package consensus

import (
	"context"
	"encoding/hex"
	"fmt"
	"math/big"
	"time"

	"github.com/goat-network/dogecoin-relayer/internal/doge"
	"github.com/goat-network/dogecoin-relayer/internal/models"
	"github.com/goat-network/dogecoin-relayer/pkg/eventbus"
	"github.com/goat-network/dogecoin-relayer/pkg/global"
	"github.com/goat-network/dogecoin-relayer/pkg/types"
	log "github.com/sirupsen/logrus"
)

// WithdrawalProcessor coordinates Dogecoin L1 payouts and triggers bridgeOutFinish on L2.
// It intentionally keeps the logic simple for regtest/local runs while leaving room for
// Fireblocks-backed signing in production.
type WithdrawalProcessor struct {
	conn           *models.DBConnection
	eventRepo      *models.EventRepository
	dogeClient     *doge.DogeClient
	utxoProcessor  *UtxoProcessor
	logger         *log.Entry
	changeAddr     string
	requiredConfs  int64
	pollInterval   time.Duration
	ctx            context.Context
	cancel         context.CancelFunc
	eventBus       *eventbus.Bus
	withdrawEnable bool
}

func NewWithdrawalProcessor(conn *models.DBConnection, up *UtxoProcessor) (*WithdrawalProcessor, error) {
	cfg := global.GetConfig()
	if cfg == nil {
		return nil, fmt.Errorf("global config not initialized")
	}

	changeAddr := cfg.Withdraw.ChangeAddress
	if changeAddr == "" && len(cfg.Doge.WatchAddresses) > 0 {
		changeAddr = cfg.Doge.WatchAddresses[0]
	}
	if changeAddr == "" {
		return nil, fmt.Errorf("withdraw change address not configured")
	}

	confs := int64(cfg.Withdraw.MinConfirmations)
	if confs <= 0 {
		confs = int64(cfg.Doge.Confirmations)
	}
	if confs <= 0 {
		confs = 1
	}

	client, err := doge.NewDogeClient(cfg.Doge)
	if err != nil {
		return nil, fmt.Errorf("create doge client for withdrawal: %w", err)
	}

	poll := time.Duration(cfg.Scan.Interval)
	if poll <= 0 {
		poll = 15
	}

	return &WithdrawalProcessor{
		conn:           conn,
		eventRepo:      models.NewEventRepository(conn.GetDB()),
		dogeClient:     client,
		utxoProcessor:  up,
		logger:         log.WithField("component", "withdrawal-processor"),
		changeAddr:     changeAddr,
		requiredConfs:  confs,
		pollInterval:   poll * time.Second,
		eventBus:       global.GetEventBus(),
		withdrawEnable: cfg.Withdraw.Enabled || cfg.Withdraw.Mode == "" || cfg.Withdraw.Mode == "local",
	}, nil
}

func (wp *WithdrawalProcessor) Start(ctx context.Context) error {
	if !wp.withdrawEnable {
		wp.logger.Info("Withdrawal processor disabled via config")
		return nil
	}
	wp.ctx, wp.cancel = context.WithCancel(ctx)
	wp.eventBus.Subscribe(eventbus.EventBridgeOutProposed, wp.handleBridgeOutProposed)
	go wp.loop()
	wp.logger.Infof("Withdrawal processor started (change=%s, confirmations=%d)", wp.changeAddr, wp.requiredConfs)
	return nil
}

func (wp *WithdrawalProcessor) Shutdown() {
	if wp.cancel != nil {
		wp.cancel()
	}
	if wp.dogeClient != nil {
		wp.dogeClient.Close()
	}
	wp.eventBus.Unsubscribe(eventbus.EventBridgeOutProposed, wp.handleBridgeOutProposed)
}

func (wp *WithdrawalProcessor) loop() {
	ticker := time.NewTicker(wp.pollInterval)
	defer ticker.Stop()

	for {
		select {
		case <-wp.ctx.Done():
			wp.logger.Info("Withdrawal processor stopped")
			return
		case <-ticker.C:
			if err := wp.processPendingWithdrawals(); err != nil {
				wp.logger.Errorf("processPendingWithdrawals error: %v", err)
			}
			if err := wp.processBroadcastedWithdrawals(); err != nil {
				wp.logger.Errorf("processBroadcastedWithdrawals error: %v", err)
			}
		}
	}
}

func (wp *WithdrawalProcessor) handleBridgeOutProposed(data any) {
	// Lightweight wake-up hook; the periodic loop will pick it up.
	if err := wp.processPendingWithdrawals(); err != nil {
		wp.logger.Errorf("handleBridgeOutProposed process error: %v", err)
	}
}

func (wp *WithdrawalProcessor) processPendingWithdrawals() error {
	if wp.utxoProcessor == nil {
		return fmt.Errorf("utxo processor not ready")
	}
	isProposer, err := wp.utxoProcessor.isCurrentProposer()
	if err != nil {
		return err
	}
	if !isProposer {
		wp.logger.Debug("Not proposer, skip pending withdrawal processing")
		return nil
	}

	pending, err := wp.eventRepo.ListWithdrawalsByStatus(nil, "init", 20)
	if err != nil {
		return fmt.Errorf("list pending withdrawals: %w", err)
	}
	for i := range pending {
		if err := wp.broadcastWithdrawal(&pending[i]); err != nil {
			wp.logger.Errorf("broadcast withdrawal %s failed: %v", pending[i].ReqTaskId, err)
		}
	}
	return nil
}

func (wp *WithdrawalProcessor) processBroadcastedWithdrawals() error {
	isProposer, err := wp.utxoProcessor.isCurrentProposer()
	if err != nil {
		return err
	}
	if !isProposer {
		return nil
	}

	broadcasted, err := wp.eventRepo.ListWithdrawalsByStatus(nil, "broadcasted", 50)
	if err != nil {
		return fmt.Errorf("list broadcasted withdrawals: %w", err)
	}

	for i := range broadcasted {
		w := &broadcasted[i]
		if err := wp.checkAndSubmit(w); err != nil {
			wp.logger.Errorf("checkAndSubmit withdrawal %s failed: %v", w.ReqTaskId, err)
		}
	}
	return nil
}

func (wp *WithdrawalProcessor) broadcastWithdrawal(w *models.Withdrawal) error {
	if w.DestAddress == "" {
		return fmt.Errorf("withdrawal %s missing dest address", w.ReqTaskId)
	}
	amountSat, err := parseAmountString(w.DestAmount)
	if err != nil || amountSat.Sign() <= 0 {
		return fmt.Errorf("withdrawal %s invalid dest amount: %v", w.ReqTaskId, err)
	}

	outputs := map[string]string{
		w.DestAddress: formatDogeAmount(amountSat),
	}

	fundedHex, err := wp.dogeClient.CreateAndFundRawTx(outputs, wp.changeAddr)
	if err != nil {
		return fmt.Errorf("create/fund raw tx: %w", err)
	}

	signedHex, err := wp.dogeClient.SignRawTransaction(fundedHex)
	if err != nil {
		return fmt.Errorf("sign raw tx: %w", err)
	}

	txid, err := wp.dogeClient.SendRawTransaction(signedHex)
	if err != nil {
		return fmt.Errorf("send raw tx: %w", err)
	}

	rawBytes, err := hex.DecodeString(signedHex)
	if err != nil {
		return fmt.Errorf("decode signed tx hex: %w", err)
	}

	// Try to locate destination vout index for bookkeeping
	voutIndex := -1
	if msgTx, err := wp.dogeClient.DecodeRawTransaction(signedHex); err == nil {
		network := types.GetDogeNetwork(global.GetConfig().Doge.NetworkType)
		for idx, out := range msgTx.TxOut {
			receiver, err := types.ExtractAddressFromScript(out.PkScript, network)
			if err == nil && receiver == w.DestAddress {
				voutIndex = idx
				break
			}
		}
	}

	updated := &models.Withdrawal{
		ReqTaskId:   w.ReqTaskId,
		ReqTxHash:   w.ReqTxHash,
		ReqBlock:    w.ReqBlock,
		ReqLogIndex: w.ReqLogIndex,
		Status:      "broadcasted",
		DestAddress: w.DestAddress,
		DestAmount:  w.DestAmount,
		TxId:        txid,
		Vout:        voutIndex,
		TxBytes:     rawBytes,
	}

	if err := wp.eventRepo.CreateOrUpdateWithdrawal(nil, updated); err != nil {
		return fmt.Errorf("update withdrawal after broadcast: %w", err)
	}

	wp.logger.Infof("Broadcasted Dogecoin withdrawal tx %s for task %s (amount=%s)", txid, w.ReqTaskId, w.DestAmount)
	return nil
}

func (wp *WithdrawalProcessor) checkAndSubmit(w *models.Withdrawal) error {
	if w.TxId == "" {
		return fmt.Errorf("withdrawal %s missing txid", w.ReqTaskId)
	}

	rawHex, confs, err := wp.dogeClient.GetRawTransactionHex(w.TxId)
	if err != nil {
		return fmt.Errorf("query tx %s: %w", w.TxId, err)
	}
	if confs < wp.requiredConfs {
		wp.logger.Debugf("Withdrawal tx %s confirmations %d/%d", w.TxId, confs, wp.requiredConfs)
		return nil
	}

	// Ensure we have tx bytes stored
	txBytes := w.TxBytes
	if len(txBytes) == 0 && rawHex != "" {
		if decoded, err := hex.DecodeString(rawHex); err == nil {
			txBytes = decoded
		}
	}
	if len(txBytes) == 0 {
		return fmt.Errorf("missing raw tx bytes for %s", w.TxId)
	}

	amountSat, err := parseAmountString(w.DestAmount)
	if err != nil {
		return fmt.Errorf("invalid dest amount for %s: %w", w.ReqTaskId, err)
	}
	taskId, ok := new(big.Int).SetString(w.ReqTaskId, 10)
	if !ok {
		return fmt.Errorf("invalid task id %s", w.ReqTaskId)
	}

	req := &withdrawalRequest{
		TxBytes:     txBytes,
		TxId:        w.TxId,
		TotalAmount: amountSat,
		TaskIds:     []*big.Int{taskId},
	}

	if err := wp.utxoProcessor.SubmitWithdrawalRequest(req); err != nil {
		return fmt.Errorf("submit withdrawal request: %w", err)
	}

	if err := wp.eventRepo.UpdateWithdrawalStatusByTask(nil, w.ReqTaskId, "pending_finish"); err != nil {
		wp.logger.Warnf("Failed to update withdrawal %s status to pending_finish: %v", w.ReqTaskId, err)
	}
	return nil
}

func parseAmountString(val string) (*big.Int, error) {
	amt := new(big.Int)
	if val == "" {
		return amt, fmt.Errorf("empty amount")
	}
	if _, ok := amt.SetString(val, 10); !ok {
		return amt, fmt.Errorf("parse amount string %s failed", val)
	}
	return amt, nil
}

func formatDogeAmount(amountSat *big.Int) string {
	rat := new(big.Rat).SetFrac(amountSat, big.NewInt(100000000))
	return rat.FloatString(8)
}
