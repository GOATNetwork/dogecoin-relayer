package doge

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/dogecoinw/doged/chaincfg"
	"github.com/dogecoinw/doged/chaincfg/chainhash"
	"github.com/dogecoinw/doged/wire"
	"github.com/goat-network/dogecoin-relayer/internal/config"
	"github.com/goat-network/dogecoin-relayer/internal/metrics"
	"github.com/goat-network/dogecoin-relayer/internal/models"
	"github.com/goat-network/dogecoin-relayer/pkg/global"
	"github.com/goat-network/dogecoin-relayer/pkg/module"
	"github.com/goat-network/dogecoin-relayer/pkg/types"
	log "github.com/sirupsen/logrus"
	"gorm.io/gorm"
)

type DogeModule struct {
	cfg           config.DogeConfig
	scanCfg       config.ScanConfig
	conn          *models.DBConnection
	logger        *log.Entry
	client        *DogeClient
	state         *models.StateRepository
	eventRepo     *models.EventRepository
	utxoScanRepo  *models.UTXOScanStateRepository
	blockCh       chan *types.DogeBlockExt
	currentHeight int64
}

var _ module.Module = (*DogeModule)(nil)

// DogeClientProvider exposes the initialized Dogecoin RPC client.
type DogeClientProvider interface {
	DogeClient() *DogeClient
}

// DogeClient returns the initialized client instance for other modules.
func (m *DogeModule) DogeClient() *DogeClient {
	return m.client
}

type txProcessingResult struct {
	txid            string
	sender          string
	utxos           []*models.UTXO
	vins            []*models.VIN
	vouts           []*models.VOUT
	evmAddr         string
	hasWatchedUtxo  bool
	hasWatchedVin   bool
	isDeposit       bool
	isWithdrawal    bool
	isSafebox       bool
	isConsolidation bool
}

func classifyOrderType(order *models.SendOrder) (isWithdrawal, isSafebox, isConsolidation bool) {
	if order == nil {
		return false, false, false
	}

	switch order.OrderType {
	case models.ORDER_TYPE_WITHDRAWAL:
		isWithdrawal = true
	case models.ORDER_TYPE_SAFEBOX:
		isSafebox = true
	case models.ORDER_TYPE_CONSOLIDATION:
		isConsolidation = true
	}

	return
}

func (m *DogeModule) Name() string {
	return "doge"
}

func (m *DogeModule) Init(cfg any, conn *models.DBConnection) error {
	// Initialize configurations
	m.cfg = cfg.(config.DogeConfig)
	m.conn = conn
	m.logger = types.InitLogEntry(m.Name())

	// Initialize Dogecoin client
	client, err := NewDogeClient(m.cfg)
	if err != nil {
		metrics.RecordError("doge", "init_failed")
		return fmt.Errorf("failed to create Dogecoin client: %w", err)
	}
	m.client = client

	// Initialize state repository
	m.state = models.NewStateRepository(conn.GetDB())

	// Initialize event repository for deposit/withdrawal bookkeeping
	m.eventRepo = models.NewEventRepository(conn.GetDB())

	// Initialize UTXO scan state repository
	m.utxoScanRepo = models.NewUTXOScanStateRepository(conn.GetDB())

	// Initialize block channel
	m.blockCh = make(chan *types.DogeBlockExt, 100)

	// Set scan config from global config
	globalCfg := global.GetConfig()
	if globalCfg != nil {
		m.scanCfg = globalCfg.Scan
	}

	if err := m.initializeScanHeight(); err != nil {
		return fmt.Errorf("initialize scan height: %w", err)
	}

	return nil
}

func (m *DogeModule) Run(ctx context.Context) error {
	m.logger.Info("Doge module running")
	metrics.RecordModuleStart("doge")

	// Start block scanning loop
	go m.blockScanLoop(ctx)

	// Start block fetching loop
	go m.blockFetchLoop(ctx)

	return nil
}

func (m *DogeModule) Shutdown(ctx context.Context) error {
	m.logger.Info("Doge module shutting down")
	close(m.blockCh)
	if m.client != nil {
		m.client.Close()
	}
	metrics.RecordModuleStop("doge")
	return nil
}

// blockFetchLoop continuously fetches new blocks from Dogecoin node
func (m *DogeModule) blockFetchLoop(ctx context.Context) {
	ticker := time.NewTicker(time.Duration(m.scanCfg.Interval) * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			m.logger.Info("Block fetch loop stopping...")
			return
		case <-ticker.C:
			if err := m.fetchNewBlocks(ctx); err != nil {
				m.logger.Errorf("Failed to fetch new blocks: %v", err)
			}
		}
	}
}

// fetchNewBlocks fetches new blocks from the current height
func (m *DogeModule) fetchNewBlocks(ctx context.Context) error {
	// Get current block count from node
	currentBlockCount, err := m.client.GetBlockCount()
	if err != nil {
		return err
	}

	// Calculate end height (current height + range, but not exceeding current block count)
	endHeight := m.currentHeight + int64(m.scanCfg.Range)
	if endHeight > currentBlockCount {
		endHeight = currentBlockCount
	}

	blockCache := make(map[int64]*types.DogeBlockExt, int(endHeight-m.currentHeight+1))

	// Fetch blocks in range
	for height := m.currentHeight; height <= endHeight; height++ {
		block, ok := blockCache[height]
		if !ok {
			block, err = m.client.GetBlockByHeight(height)
			if err != nil {
				m.logger.Errorf("Failed to get block at height %d: %v", height, err)
				continue
			}
			blockCache[height] = block
		}

		// Send block to processing channel
		select {
		case m.blockCh <- block:
		case <-ctx.Done():
			return ctx.Err()
		}
	}

	// Update current height
	m.currentHeight = endHeight + 1

	return nil
}

// blockScanLoop processes blocks from the channel
func (m *DogeModule) blockScanLoop(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			m.logger.Info("Block scan loop stopping...")
			return
		case block := <-m.blockCh:
			if block != nil {
				m.processBlock(*block)
				if err := m.updateUTXOScanState(block.BlockNumber); err != nil {
					m.logger.Errorf("Failed to update UTXO scan state at height %d: %v", block.BlockNumber, err)
				}
			}
		}
	}
}

func (m *DogeModule) initializeScanHeight() error {
	if m.utxoScanRepo == nil {
		m.currentHeight = int64(m.cfg.StartHeight)
		return nil
	}

	state, err := m.utxoScanRepo.GetUTXOScanState()
	if err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			m.currentHeight = int64(m.cfg.StartHeight)
			m.logger.Infof("UTXO scan state not found, starting from configured height %d", m.currentHeight)
			return nil
		}
		return err
	}

	startFrom := int64(state.LastScannedBlock)
	if int64(m.cfg.StartHeight) > startFrom {
		startFrom = int64(m.cfg.StartHeight)
	}
	m.currentHeight = startFrom + 1
	m.logger.Infof("Resuming UTXO scan from height %d (last scanned %d)", m.currentHeight, state.LastScannedBlock)
	return nil
}

func (m *DogeModule) updateUTXOScanState(height int64) error {
	if m.utxoScanRepo == nil {
		return nil
	}
	if height < 0 {
		return nil
	}
	return m.utxoScanRepo.UpdateUTXOScanState(uint64(height))
}

// processBlock processes a single block and extracts P2PKH transactions
func (m *DogeModule) processBlock(dogeBlock types.DogeBlockExt) {
	m.logger.Debugf("Processing Dogecoin block, height: %d, hash: %s", dogeBlock.BlockNumber, dogeBlock.GetBlockHash().String())

	// Verify SPV (simplified)
	if err := types.VerifyBlockSPV(dogeBlock); err != nil {
		m.logger.Errorf("Verify block SPV err %v", err)
		return
	}

	// Get network parameters
	network := types.GetDogeNetwork(m.cfg.NetworkType)

	// Prepare watch addresses
	watchAddrSet := make(map[string]struct{})
	var pubkeyBytes []byte

	// Prefer explicit watch addresses if provided
	if len(m.cfg.WatchAddresses) > 0 {
		for _, a := range m.cfg.WatchAddresses {
			if a != "" {
				watchAddrSet[a] = struct{}{}
			}
		}
		m.logger.Debugf("Using configured watch addresses: %d", len(watchAddrSet))
	} else if m.cfg.WatchPubkeyBase64 != "" {
		// Derive standard addresses from configured pubkey
		var err error
		pubkeyBytes, err = types.DecodeBase64Pubkey(m.cfg.WatchPubkeyBase64)
		if err != nil {
			m.logger.Errorf("Failed to decode configured pubkey: %v", err)
			return
		}
		p2pkhAddress, err := types.GenerateP2PKHAddress(pubkeyBytes, network)
		if err != nil {
			m.logger.Errorf("Failed to generate P2PKH address: %v", err)
			return
		}
		watchAddrSet[p2pkhAddress] = struct{}{}
		// Dogecoin does not support segwit outputs; skip derived P2WPKH here.
		m.logger.Debugf("Using derived watch address (P2PKH only for Doge): %s", p2pkhAddress)
	} else {
		// No addresses configured; continue scanning but skip ownership-based classification
		m.logger.Debug("No watch addresses or pubkey configured; scanning without address filter")
	}

	// Process each transaction in the block
	for _, tx := range dogeBlock.Transactions {
		m.processTransaction(tx, dogeBlock, watchAddrSet, network, pubkeyBytes)
	}

	m.logger.Debugf("Processed Dogecoin block %d", dogeBlock.BlockNumber)
}

// processTransaction processes a single transaction
func (m *DogeModule) processTransaction(tx *wire.MsgTx, dogeBlock types.DogeBlockExt, watchAddrSet map[string]struct{}, network *chaincfg.Params, pubkeyBytes []byte) {
	result, err := m.analyzeTransaction(tx, dogeBlock, watchAddrSet, network)
	if err != nil {
		m.logger.Errorf("Analyze transaction %s failed: %v", tx.TxHash().String(), err)
		return
	}

	if !result.hasWatchedUtxo && !result.hasWatchedVin && !result.isWithdrawal && !result.isSafebox && !result.isConsolidation {
		return
	}

	if result.hasWatchedUtxo {
		if err := m.handleDepositUTXOs(tx, dogeBlock, result, pubkeyBytes); err != nil {
			m.logger.Errorf("Persist deposit UTXOs for %s failed: %v", result.txid, err)
		}
	}

	if result.hasWatchedVin {
		if err := m.persistVINsAndVOUTs(result, dogeBlock.BlockNumber); err != nil {
			m.logger.Errorf("Persist VIN/VOUT for %s failed: %v", result.txid, err)
		}
	}

	if result.isWithdrawal || result.isSafebox || result.isConsolidation {
		if err := m.handleSpendingTransaction(result.txid, result.vins, dogeBlock.BlockNumber); err != nil {
			m.logger.Errorf("Finalize spending transaction %s failed: %v", result.txid, err)
		}
	}
}

func (m *DogeModule) analyzeTransaction(tx *wire.MsgTx, dogeBlock types.DogeBlockExt, watchAddrSet map[string]struct{}, network *chaincfg.Params) (*txProcessingResult, error) {
	result := &txProcessingResult{
		txid:  tx.TxHash().String(),
		utxos: make([]*models.UTXO, 0),
		vins:  make([]*models.VIN, 0),
		vouts: make([]*models.VOUT, 0),
	}

	sendOrder, err := m.state.GetSendOrderByTxIdOrExternalId(result.txid)
	if err != nil {
		return nil, fmt.Errorf("query send order: %w", err)
	}
	result.isWithdrawal, result.isSafebox, result.isConsolidation = classifyOrderType(sendOrder)

	currentTime := time.Now()

	for _, vin := range tx.TxIn {
		if vin.PreviousOutPoint.Hash == (chainhash.Hash{}) && vin.PreviousOutPoint.Index == 0xffffffff {
			result.sender = "coinbase"
			m.logger.Debugf("Detect coinbase tx for %s", result.txid)
			break
		}

		if len(vin.SignatureScript) == 0 {
			continue
		}

		if len(vin.SignatureScript) >= 23 {
			sigLen := int(vin.SignatureScript[0])
			if sigLen > 0 && len(vin.SignatureScript) > sigLen+1 {
				pubKeyLen := int(vin.SignatureScript[sigLen+1])
				if pubKeyLen > 0 && len(vin.SignatureScript) >= sigLen+2+pubKeyLen {
					pubKey := vin.SignatureScript[sigLen+2 : sigLen+2+pubKeyLen]
					if addr, addrErr := types.GenerateP2PKHAddress(pubKey, network); addrErr == nil {
						result.sender = addr
					}
				}
			}
		}

		if _, ok := watchAddrSet[result.sender]; ok {
			result.hasWatchedVin = true
			if len(tx.TxOut) == 1 {
				result.isConsolidation = true
			} else {
				result.isWithdrawal = true
			}

			result.vins = append(result.vins, &models.VIN{
				OrderId:   "",
				BtcHeight: dogeBlock.BlockNumber,
				Txid:      vin.PreviousOutPoint.Hash.String(),
				OutIndex:  int(vin.PreviousOutPoint.Index),
				SigScript: vin.SignatureScript,
				Sender:    result.sender,
				Source:    models.UTXO_SOURCE_UNKNOWN,
				Status:    models.UTXO_STATUS_CONFIRMED,
				UpdatedAt: currentTime,
			})
		}
	}

	magicBytes := parseHexOrRaw(m.cfg.DepositMagicBytes)
	minDepositAmount := m.cfg.MinDepositAmount
	if minDepositAmount <= 0 {
		minDepositAmount = 1_000_000
	}

	watchList := make([]string, 0, len(watchAddrSet))
	for addr := range watchAddrSet {
		watchList = append(watchList, addr)
	}

	if len(watchList) > 0 {
		if deposit, evmAddr, derr := types.IsUtxoDogeDepositV1(tx, watchList, network, minDepositAmount, magicBytes); derr == nil {
			result.isDeposit = deposit
			result.evmAddr = evmAddr
		} else {
			m.logger.Warnf("Deposit detection failed for %s: %v", result.txid, derr)
		}
	}

	for idx, vout := range tx.TxOut {
		receiver, extractErr := types.ExtractAddressFromScript(vout.PkScript, network)
		if extractErr != nil {
			m.logger.Debugf("Failed to extract address from script: %v", extractErr)
			receiver = "unknown"
		} else if receiver == "" {
			receiver = "unknown"
		}

		receiverType := models.WALLET_TYPE_UNKNOWN
		switch {
		case types.IsP2PKHScript(vout.PkScript):
			receiverType = models.WALLET_TYPE_P2PKH
		case types.IsP2WPKHScript(vout.PkScript):
			receiverType = models.WALLET_TYPE_P2WPKH
		}

		voutModel := &models.VOUT{
			OrderId:    "",
			BtcHeight:  dogeBlock.BlockNumber,
			Txid:       result.txid,
			OutIndex:   idx,
			WithdrawId: "",
			Amount:     vout.Value,
			Receiver:   receiver,
			Sender:     result.sender,
			Source:     models.UTXO_SOURCE_UNKNOWN,
			Status:     models.UTXO_STATUS_CONFIRMED,
			UpdatedAt:  currentTime,
		}
		result.vouts = append(result.vouts, voutModel)

		if _, ok := watchAddrSet[receiver]; ok {
			result.hasWatchedUtxo = true
			result.utxos = append(result.utxos, &models.UTXO{
				Txid:          result.txid,
				PkScript:      vout.PkScript,
				OutIndex:      idx,
				Amount:        vout.Value,
				Receiver:      receiver,
				WalletVersion: "1",
				Sender:        result.sender,
				EvmAddr:       result.evmAddr,
				Source:        models.UTXO_SOURCE_UNKNOWN,
				ReceiverType:  receiverType,
				Status:        models.UTXO_STATUS_CONFIRMED,
				ReceiveBlock:  dogeBlock.BlockNumber,
				SpentBlock:    0,
				UpdatedAt:     currentTime,
			})
		}
	}

	return result, nil
}

func (m *DogeModule) handleDepositUTXOs(tx *wire.MsgTx, dogeBlock types.DogeBlockExt, result *txProcessingResult, pubkeyBytes []byte) error {
	if len(result.utxos) == 0 {
		return nil
	}

	noWitnessTx, err := types.SerializeTransactionNoWitness(tx)
	if err != nil {
		return fmt.Errorf("serialize tx without witness: %w", err)
	}

	blockHash := dogeBlock.GetBlockHash().String()

	for _, utxo := range result.utxos {
		switch {
		case result.isDeposit:
			utxo.Source = models.UTXO_SOURCE_DEPOSIT
		case result.isConsolidation:
			utxo.Source = models.UTXO_SOURCE_CONSOLIDATION
		case result.isWithdrawal || result.isSafebox:
			utxo.Source = models.UTXO_SOURCE_WITHDRAWAL
		default:
			utxo.Source = models.UTXO_SOURCE_UNKNOWN
		}

		merkleRoot, proofBytes, txIndex, proofErr := types.GenerateSPVProof(utxo.Txid, []string{result.txid})
		if proofErr != nil {
			m.logger.Errorf("Generate SPV proof err for %s: %v", utxo.Txid, proofErr)
			continue
		}

		if err := m.state.AddUtxo(utxo, pubkeyBytes, blockHash, dogeBlock.BlockNumber, noWitnessTx, merkleRoot, proofBytes, txIndex, result.isDeposit); err != nil {
			m.logger.Errorf("Add UTXO %#v err %v", utxo, err)
			continue
		}

		if result.isDeposit {
			if err := m.recordDeposit(utxo, result.txid, noWitnessTx); err != nil {
				m.logger.Errorf("Record deposit for %s:%d failed: %v", utxo.Txid, utxo.OutIndex, err)
			} else if utxo.EvmAddr == "" {
				m.logger.Warnf("Deposit %s:%d recorded with empty EVM address, marking as skipped. Transaction may lack OP_RETURN or magic bytes mismatch.", utxo.Txid, utxo.OutIndex)
				// Mark deposit as skipped in the event database
				if dep, err := m.eventRepo.GetDeposit(nil, utxo.Txid, utxo.OutIndex); err == nil {
					if err := m.eventRepo.UpdateDepositStatus(nil, dep.ID, "skipped"); err != nil {
						m.logger.Errorf("Failed to mark deposit %s:%d as skipped: %v", utxo.Txid, utxo.OutIndex, err)
					}
				}
				// Also mark UTXO as processed to prevent processor from picking it up again
				if err := m.conn.GetDB().Model(&models.UTXO{}).Where("txid = ? AND out_index = ?", utxo.Txid, utxo.OutIndex).Update("status", models.UTXO_STATUS_PROCESSED).Error; err != nil {
					m.logger.Errorf("Failed to mark UTXO %s:%d as processed: %v", utxo.Txid, utxo.OutIndex, err)
				}
			}
		}
	}

	return nil
}

func (m *DogeModule) persistVINsAndVOUTs(result *txProcessingResult, blockHeight int64) error {
	for _, vin := range result.vins {
		switch {
		case result.isDeposit:
			vin.Source = models.UTXO_SOURCE_DEPOSIT
		case result.isConsolidation:
			vin.Source = models.UTXO_SOURCE_CONSOLIDATION
		case result.isWithdrawal || result.isSafebox:
			vin.Source = models.UTXO_SOURCE_WITHDRAWAL
		default:
			vin.Source = models.UTXO_SOURCE_UNKNOWN
		}

		if err := m.state.AddOrUpdateVin(vin); err != nil {
			return fmt.Errorf("add/update vin: %w", err)
		}
	}

	for _, vout := range result.vouts {
		switch {
		case result.isDeposit:
			vout.Source = models.UTXO_SOURCE_DEPOSIT
		case result.isConsolidation:
			vout.Source = models.UTXO_SOURCE_CONSOLIDATION
		case result.isWithdrawal || result.isSafebox:
			vout.Source = models.UTXO_SOURCE_WITHDRAWAL
		default:
			vout.Source = models.UTXO_SOURCE_UNKNOWN
		}

		if err := m.state.AddOrUpdateVout(vout); err != nil {
			return fmt.Errorf("add/update vout: %w", err)
		}
	}

	return nil
}

func (m *DogeModule) handleSpendingTransaction(txid string, vins []*models.VIN, blockHeight int64) error {
	if err := m.state.UpdateSendOrderConfirmed(txid, blockHeight); err != nil {
		m.logger.Debugf("Update send order confirmed %s err %v", txid, err)
	}

	if len(vins) == 0 {
		return nil
	}

	if err := m.state.UpdateUtxoStatusSpentByVins(vins, blockHeight); err != nil {
		return fmt.Errorf("update utxo status spent: %w", err)
	}

	return nil
}

func (m *DogeModule) recordDeposit(utxo *models.UTXO, txid string, txBytes []byte) error {
	if m.eventRepo == nil {
		return nil
	}

	deposit := &models.Deposit{
		TxId:        txid,
		Vout:        utxo.OutIndex,
		Address:     utxo.Receiver,
		EvmAddr:     utxo.EvmAddr,
		Amount:      utxo.Amount,
		TxBytes:     txBytes,
		Status:      "pending",
		EvmTxHash:   "",
		EvmBlock:    0,
		EvmLogIndex: 0,
	}

	return m.eventRepo.CreateOrUpdateDeposit(nil, deposit)
}

// parseHexOrRaw parses a hex string like "0xdeadbeef" or "deadbeef" into bytes.
// If s is empty or invalid hex, it returns the raw bytes of s.
func parseHexOrRaw(s string) []byte {
	if s == "" {
		return []byte{}
	}
	// trim 0x/0X prefix
	if len(s) > 2 && (s[:2] == "0x" || s[:2] == "0X") {
		s = s[2:]
	}
	// hex decode
	dst := make([]byte, len(s)/2)
	n := 0
	for i := 0; i+1 < len(s); i += 2 {
		var b byte
		for k := 0; k < 2; k++ {
			c := s[i+k]
			var v byte
			switch {
			case '0' <= c && c <= '9':
				v = c - '0'
			case 'a' <= c && c <= 'f':
				v = c - 'a' + 10
			case 'A' <= c && c <= 'F':
				v = c - 'A' + 10
			default:
				// not hex
				return []byte(s)
			}
			b = (b << 4) | v
		}
		dst[n] = b
		n++
	}
	return dst[:n]
}

func init() {
	log.Info("Registering doge module")
	module.RegisterModule(&DogeModule{})
}
