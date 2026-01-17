package grpc

import (
	"bytes"
	"context"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"slices"
	"strings"
	"time"

	"github.com/dogecoinw/doged/chaincfg"
	"github.com/dogecoinw/doged/wire"
	"github.com/goat-network/dogecoin-relayer/internal/consensus"
	"github.com/goat-network/dogecoin-relayer/internal/doge"
	"github.com/goat-network/dogecoin-relayer/internal/models"
	"github.com/goat-network/dogecoin-relayer/internal/p2p"
	"github.com/goat-network/dogecoin-relayer/pkg/global"
	"github.com/goat-network/dogecoin-relayer/pkg/module"
	"github.com/goat-network/dogecoin-relayer/pkg/types"
	"github.com/google/uuid"
	log "github.com/sirupsen/logrus"
)

type depositVerifier struct {
	client     *doge.DogeClient
	conn       *models.DBConnection
	network    *chaincfg.Params
	magicBytes []byte
	minAmount  int64
	addresses  []string
	logger     *log.Entry
}

func newDepositVerifier(client *doge.DogeClient, conn *models.DBConnection, logger *log.Entry) (*depositVerifier, error) {
	cfg := global.GetConfig()
	if cfg == nil {
		return nil, fmt.Errorf("global config not initialized")
	}

	network := types.GetDogeNetwork(cfg.Doge.NetworkType)

	var magicBytes []byte
	if cfg.Doge.DepositMagicBytes != "" {
		magicHex := strings.TrimPrefix(cfg.Doge.DepositMagicBytes, "0x")
		var err error
		magicBytes, err = hex.DecodeString(magicHex)
		if err != nil {
			return nil, fmt.Errorf("failed to decode magic bytes: %w", err)
		}
	}

	return &depositVerifier{
		client:     client,
		conn:       conn,
		network:    network,
		magicBytes: magicBytes,
		minAmount:  cfg.Doge.MinDepositAmount,
		addresses:  cfg.Doge.WatchAddresses,
		logger:     logger,
	}, nil
}

type VerifyResult struct {
	IsValid   bool
	EvmAddr   string
	OutIndex  int
	Amount    int64
	RawTxHex  string
	Confirmed int64
}

func (v *depositVerifier) verifyDeposit(ctx context.Context, txid string, rawTxHex string, claimedEvmAddr string) (*VerifyResult, error) {
	rpcHex, confirmations, err := v.client.GetRawTransactionHex(txid)
	if err != nil {
		return nil, fmt.Errorf("failed to get transaction from RPC: %w", err)
	}

	if rawTxHex != "" && rpcHex != rawTxHex {
		return nil, fmt.Errorf("transaction data mismatch between frontend and RPC")
	}

	tx, err := v.client.DecodeRawTransaction(rpcHex)
	if err != nil {
		return nil, fmt.Errorf("failed to decode transaction: %w", err)
	}

	computedTxid := tx.TxHash().String()
	if computedTxid != txid {
		return nil, fmt.Errorf("txid mismatch: expected %s, got %s", txid, computedTxid)
	}

	isDeposit, evmAddr, err := types.IsUtxoDogeDepositV1(tx, v.addresses, v.network, v.minAmount, v.magicBytes)
	if err != nil {
		return nil, fmt.Errorf("deposit verification failed: %w", err)
	}
	if !isDeposit {
		return nil, fmt.Errorf("transaction is not a valid deposit")
	}

	if claimedEvmAddr != "" {
		normalizedClaimed := strings.ToLower(strings.TrimPrefix(claimedEvmAddr, "0x"))
		normalizedExtracted := strings.ToLower(strings.TrimPrefix(evmAddr, "0x"))
		if normalizedClaimed != normalizedExtracted {
			return nil, fmt.Errorf("EVM address mismatch: claimed %s, extracted %s", claimedEvmAddr, evmAddr)
		}
	}

	var depositOutIdx int
	var depositAmount int64
	for idx, vout := range tx.TxOut {
		recv, err := types.ExtractAddressFromScript(vout.PkScript, v.network)
		if err != nil || recv == "" {
			continue
		}
		for _, watchAddr := range v.addresses {
			if recv == watchAddr && vout.Value >= v.minAmount {
				depositOutIdx = idx
				depositAmount = vout.Value
				break
			}
		}
	}

	return &VerifyResult{
		IsValid:   true,
		EvmAddr:   evmAddr,
		OutIndex:  depositOutIdx,
		Amount:    depositAmount,
		RawTxHex:  rpcHex,
		Confirmed: confirmations,
	}, nil
}

func (v *depositVerifier) saveDeposit(ctx context.Context, txid string, result *VerifyResult) error {
	db := v.conn.GetDB()

	rawTxBytes, err := hex.DecodeString(result.RawTxHex)
	if err != nil {
		return fmt.Errorf("failed to decode raw tx hex: %w", err)
	}

	deposit := &models.Deposit{
		TxId:    txid,
		Vout:    result.OutIndex,
		EvmAddr: result.EvmAddr,
		Amount:  result.Amount,
		TxBytes: rawTxBytes,
		Status:  "pending",
	}

	for _, addr := range v.addresses {
		deposit.Address = addr
		break
	}

	if err := db.Create(deposit).Error; err != nil {
		if strings.Contains(err.Error(), "UNIQUE constraint failed") || strings.Contains(err.Error(), "duplicate key") {
			v.logger.Infof("Deposit already exists: txid=%s, vout=%d", txid, result.OutIndex)
			return nil
		}
		return fmt.Errorf("failed to save deposit: %w", err)
	}

	utxo := &models.UTXO{
		Uid:           fmt.Sprintf("%s:%d", txid, result.OutIndex),
		Txid:          txid,
		OutIndex:      result.OutIndex,
		Amount:        result.Amount,
		EvmAddr:       result.EvmAddr,
		Source:        models.UTXO_SOURCE_DEPOSIT,
		Status:        models.UTXO_STATUS_CONFIRMED,
		WalletVersion: "1",
		UpdatedAt:     time.Now(),
	}

	for _, addr := range v.addresses {
		utxo.Receiver = addr
		break
	}

	if err := db.Create(utxo).Error; err != nil {
		if strings.Contains(err.Error(), "UNIQUE constraint failed") || strings.Contains(err.Error(), "duplicate key") {
			v.logger.Infof("UTXO already exists: uid=%s", utxo.Uid)
			return nil
		}
		return fmt.Errorf("failed to save UTXO: %w", err)
	}

	v.logger.Infof("Saved deposit: txid=%s, vout=%d, amount=%d, evmAddr=%s", txid, result.OutIndex, result.Amount, result.EvmAddr)

	// Broadcast deposit notification to other nodes via P2P
	if err := v.broadcastDepositNotification(deposit); err != nil {
		v.logger.Warnf("Failed to broadcast deposit notification: %v", err)
		// Don't return error - local save was successful
	}

	return nil
}

// broadcastDepositNotification broadcasts a deposit notification to other nodes via P2P
func (v *depositVerifier) broadcastDepositNotification(deposit *models.Deposit) error {
	// Get the P2P module
	p2pMod, ok := module.GetModule("p2p")
	if !ok {
		return fmt.Errorf("P2P module not found")
	}

	p2pSender, ok := p2pMod.(p2p.P2PSender)
	if !ok {
		return fmt.Errorf("P2P module does not implement P2PSender interface")
	}

	// Create a DepositNotification with the deposit data
	sessionID := uuid.New().String()
	notification := consensus.NewDepositNotification(
		deposit.TxId,
		deposit.Vout,
		deposit.EvmAddr,
		deposit.Amount,
		deposit.TxBytes,
		deposit.Address,
		sessionID,
	)

	// Marshal the notification
	payload, err := json.Marshal(notification)
	if err != nil {
		return fmt.Errorf("failed to marshal deposit notification: %w", err)
	}

	// Broadcast via P2P
	msg := types.P2PBroadcastMessage{
		Type:      types.P2PMessageTypeDepositNotification,
		SessionID: sessionID,
		Payload:   payload,
	}

	if err := p2pSender.BroadcastP2PMessage(msg); err != nil {
		return fmt.Errorf("failed to broadcast P2P message: %w", err)
	}

	v.logger.Infof("Broadcasted deposit notification: txid=%s, vout=%d, sessionID=%s", deposit.TxId, deposit.Vout, sessionID)
	return nil
}

func splitEvmAddresses(evmAddressesStr string) ([]string, error) {
	evmAddresses := strings.Split(evmAddressesStr, ",")
	if len(evmAddresses) > 150 {
		return nil, fmt.Errorf("EVM addresses should not be more than 150")
	}

	for i, addr := range evmAddresses {
		evmAddresses[i] = strings.ToLower(strings.TrimPrefix(addr, "0x"))
	}
	slices.Sort(evmAddresses)
	return slices.Compact(evmAddresses), nil
}

func deserializeTx(rawHex string) (*wire.MsgTx, error) {
	rawBytes, err := hex.DecodeString(rawHex)
	if err != nil {
		return nil, fmt.Errorf("failed to decode hex: %w", err)
	}

	var tx wire.MsgTx
	if err := tx.Deserialize(bytes.NewReader(rawBytes)); err != nil {
		return nil, fmt.Errorf("failed to deserialize tx: %w", err)
	}

	return &tx, nil
}
