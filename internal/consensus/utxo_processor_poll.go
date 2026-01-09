package consensus

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"math/big"
	"sort"
	"strings"
	"time"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/crypto"
	"github.com/goat-network/dogecoin-relayer/internal/models"
	"github.com/goat-network/dogecoin-relayer/internal/p2p"
	"github.com/goat-network/dogecoin-relayer/pkg/contract"
	"github.com/goat-network/dogecoin-relayer/pkg/module"
	"github.com/goat-network/dogecoin-relayer/pkg/types"
	"gorm.io/gorm"
)

// pollLoop continuously polls for new UTXOs
func (up *UtxoProcessor) pollLoop() {
	ticker := time.NewTicker(up.pollInterval)
	defer ticker.Stop()

	for {
		select {
		case <-up.ctx.Done():
			up.logger.Info("UTXO manager poll loop stopping...")
			return
		case <-ticker.C:
			if err := up.scanNewUTXOs(); err != nil {
				up.logger.Errorf("Failed to process new UTXOs: %v", err)
			}
		}
	}
}

// scanNewUTXOs checks for new deposit UTXOs and processes them
func (up *UtxoProcessor) scanNewUTXOs() error {
	up.cleanupStaleSessions()

	// Check if this node should process UTXOs
	isProposer, err := up.isCurrentProposer()
	if err != nil {
		up.logger.Errorf("Failed to check proposer status: %v", err)
		return fmt.Errorf("failed to check proposer status: %w", err)
	}

	// Add info level logging for debugging
	up.logger.Infof("UTXO scan: isProposer=%v, proposerSet=%v, currentProposer=%s",
		isProposer, up.proposerSet, up.currentProposer.Hex())

	if !isProposer {
		up.logger.Info("Not the current proposer, skipping UTXO processing")
		return nil
	}

	// Scan deposit UTXOs
	if err := up.scanDepositUTXOs(); err != nil {
		up.logger.Errorf("Failed to process deposit UTXOs: %v", err)
	}

	// Scan withdrawal UTXOs
	if err := up.scanWithdrawalUTXOs(); err != nil {
		up.logger.Errorf("Failed to process withdrawal UTXOs: %v", err)
	}

	return nil
}

// scanDepositUTXOs handles deposit UTXO processing
func (up *UtxoProcessor) scanDepositUTXOs() error {
	// Add safety check to prevent infinite recursion
	defer func() {
		if r := recover(); r != nil {
			up.logger.Errorf("Panic in scanDepositUTXOs: %v", r)
		}
	}()

	// Check if there are pending batches still awaiting TSS signature
	// to prevent TSS nonce race conditions. Only process one batch at a time.
	// We check this BEFORE fetching new UTXOs to ensure we don't advance
	// the lastProcessedId cursor if we're not ready to process.
	hasPending := false
	up.pendingBatches.Range(func(key, value interface{}) bool {
		if pending, ok := value.(*pendingBatch); ok && pending.batchType == "deposit" {
			hasPending = true
			up.logger.Infof("Skipping new deposit processing: pending batch %s awaiting TSS signature", key)
			return false // stop iteration
		}
		return true
	})
	if hasPending {
		up.logger.Info("Waiting for pending deposit batch to complete before processing new deposits")
		return nil
	}

	// Query for new unprocessed deposit UTXOs
	utxos, err := up.getUnprocessedDepositUTXOs()
	if err != nil {
		return fmt.Errorf("failed to get unprocessed deposit UTXOs: %w", err)
	}

	if len(utxos) == 0 {
		up.logger.Debug("No new deposit UTXOs to process")
		return nil // No new UTXOs to process
	}

	up.logger.Infof("Found %d new deposit UTXOs to process", len(utxos))

	// Filter UTXOs with valid EVM addresses before batching
	validUTXOs := make([]*models.UTXO, 0, len(utxos))
	for _, utxo := range utxos {
		if utxo.EvmAddr == "" {
			up.logger.Warnf("Skipping UTXO %s: missing EVM address", utxo.Uid)
			// Mark as processed to avoid reprocessing
			if err := up.markUTXOsAsProcessed([]*models.UTXO{utxo}); err != nil {
				up.logger.Errorf("Failed to mark invalid UTXO as processed: %v", err)
			}
			continue
		}
		validUTXOs = append(validUTXOs, utxo)
	}

	if len(validUTXOs) == 0 {
		up.logger.Info("No valid deposit UTXOs with EVM addresses to process")
		return nil
	}

	up.logger.Infof("Processing %d valid deposit UTXOs (filtered from %d total)", len(validUTXOs), len(utxos))

	// Group UTXOs into batches for bridge transactions
	batches := up.groupUTXOsIntoBatches(validUTXOs)
	up.logger.Infof("Created %d batches from %d UTXOs", len(batches), len(validUTXOs))

	// Process only ONE batch per poll cycle to prevent TSS nonce race conditions.
	// Each batch requires a unique TSS nonce, and nonces are fetched from on-chain state.
	// If we process multiple batches before the first tx is confirmed, they'll all use
	// the same nonce and fail with "Invalid Signer".
	if len(batches) > 0 {
		batch := batches[0]
		up.logger.Infof("Processing batch 1/%d with %d UTXOs (remaining batches will be processed in next poll cycles)", len(batches), len(batch.UTXOs))
		if err := up.processDepositBatch(batch); err != nil {
			up.logger.Errorf("Failed to process deposit batch %s: %v", batch.ID.String(), err)
			return err
		}

		// NOTE: Do NOT mark UTXOs as processed here.
		// UTXOs will be marked as processed in completeBatchWithSignature()
		// after TSS signature is successfully received and verified.
		// This ensures that failed TSS sessions don't leave UTXOs in limbo.
		up.logger.Debugf("Deposit batch %s submitted for TSS signing, UTXOs will be marked processed upon completion", batch.ID.String())

		if len(batches) > 1 {
			up.logger.Infof("Deferring %d remaining batches to next poll cycle to avoid TSS nonce race condition", len(batches)-1)
		}
	}

	return nil
}

// scanWithdrawalUTXOs handles withdrawal UTXO processing
func (up *UtxoProcessor) scanWithdrawalUTXOs() error {
	// Check if there are pending withdrawal batches still awaiting TSS signature
	hasPending := false
	up.pendingBatches.Range(func(key, value interface{}) bool {
		if pending, ok := value.(*pendingBatch); ok && pending.batchType == "withdrawal" {
			hasPending = true
			up.logger.Infof("Skipping new withdrawal processing: pending batch %s awaiting TSS signature", key)
			return false
		}
		return true
	})
	if hasPending {
		up.logger.Info("Waiting for pending withdrawal batch to complete before processing new withdrawals")
		return nil
	}

	// Query for new unprocessed withdrawal UTXOs
	utxos, err := up.getUnprocessedWithdrawalUTXOs()
	if err != nil {
		return fmt.Errorf("failed to get unprocessed withdrawal UTXOs: %w", err)
	}

	if len(utxos) == 0 {
		return nil // No new withdrawal UTXOs to process
	}

	up.logger.Infof("Found %d new withdrawal UTXOs to process", len(utxos))

	// Process only ONE withdrawal UTXO per poll cycle to prevent TSS nonce race conditions
	utxo := utxos[0]
	if err := up.processWithdrawalUTXO(utxo); err != nil {
		up.logger.Errorf("Failed to process withdrawal UTXO %s: %v", utxo.Uid, err)
		return err
	}

	up.logger.Debugf("Withdrawal UTXO %s submitted for TSS signing, will be marked processed upon completion", utxo.Uid)

	if len(utxos) > 1 {
		up.logger.Infof("Deferring %d remaining withdrawal UTXOs to next poll cycle", len(utxos)-1)
	}

	return nil
}

// processWithdrawalUTXO processes a single withdrawal UTXO with its associated outputs
func (up *UtxoProcessor) processWithdrawalUTXO(utxo *models.UTXO) error {
	up.logger.Infof("Processing withdrawal UTXO %s (txid: %s, amount: %d DOGE)",
		utxo.Uid, utxo.Txid, utxo.Amount)

	// Get all VOUT records for this transaction
	vouts, err := up.getVOUTsForTransaction(utxo.Txid)
	if err != nil {
		return fmt.Errorf("failed to get VOUTs for transaction %s: %w", utxo.Txid, err)
	}

	if len(vouts) == 0 {
		return fmt.Errorf("no VOUTs found for withdrawal transaction %s", utxo.Txid)
	}

	up.logger.Infof("Found %d outputs for withdrawal transaction %s", len(vouts), utxo.Txid)

	hasWithdrawMapping := false
	for _, v := range vouts {
		if v.WithdrawId != "" {
			hasWithdrawMapping = true
			break
		}
	}
	if !hasWithdrawMapping {
		up.logger.Debugf("Skip withdrawal tx %s: no withdraw_id mapping present on outputs", utxo.Txid)
		return nil
	}

	// Create withdrawal request with single UTXO and aligned task IDs
	request := up.createWithdrawalRequestFromUTXO(utxo, vouts)

	// Process the withdrawal request
	if err := up.processWithdrawalRequest(request); err != nil {
		return fmt.Errorf("failed to process withdrawal request: %w", err)
	}

	return nil
}

// getUnprocessedDepositUTXOs retrieves unprocessed deposit UTXOs from database
func (up *UtxoProcessor) getUnprocessedDepositUTXOs() ([]*models.UTXO, error) {
	var utxos []*models.UTXO

	// Query for deposit UTXOs that haven't been processed yet
	// We rely on status='confirmed' to find pending items. Items that are successfully
	// processed will have status='processed'. This allows automatic retry of failed items.
	err := up.conn.GetDB().Where(
		"source = ? AND status = ?",
		models.UTXO_SOURCE_DEPOSIT,
		models.UTXO_STATUS_CONFIRMED,
	).Order("id ASC").Limit(up.batchSize).Find(&utxos).Error

	if err != nil {
		return nil, err
	}

	return utxos, nil
}

// getUnprocessedWithdrawalUTXOs retrieves unprocessed withdrawal UTXOs from database
func (up *UtxoProcessor) getUnprocessedWithdrawalUTXOs() ([]*models.UTXO, error) {
	var utxos []*models.UTXO

	// Query for withdrawal UTXOs that haven't been processed yet
	err := up.conn.GetDB().Where(
		"source = ? AND status = ?",
		models.UTXO_SOURCE_WITHDRAWAL,
		models.UTXO_STATUS_CONFIRMED,
	).Order("id ASC").Limit(up.batchSize).Find(&utxos).Error

	if err != nil {
		return nil, err
	}

	return utxos, nil
}

// groupUTXOsIntoBatches groups UTXOs into batches for efficient processing
func (up *UtxoProcessor) groupUTXOsIntoBatches(utxos []*models.UTXO) []*BridgeInBatch {
	var batches []*BridgeInBatch
	currentBatch := &BridgeInBatch{
		ID:                big.NewInt(time.Now().Unix()), // Simple batch ID based on timestamp
		TransactionParams: make([]contract.BridgeTransaction, 0),
		TotalAmount:       big.NewInt(0),
		UTXOs:             make([]*models.UTXO, 0),
	}

	// Pre-fetch all deposit records in a single query to avoid per-UTXO queries
	// This prevents database lock contention
	depositMap := make(map[string]models.Deposit)
	if len(utxos) > 0 {
		var deposits []models.Deposit
		// Build condition for batch query
		var conditions []map[string]interface{}
		for _, utxo := range utxos {
			conditions = append(conditions, map[string]interface{}{
				"tx_id": utxo.Txid,
				"vout":  utxo.OutIndex,
			})
		}

		// Query all deposits in one go
		// Note: This uses OR conditions, which is less efficient but safer than N+1 queries
		if len(conditions) > 0 {
			query := up.conn.GetDB()
			for i, cond := range conditions {
				if i == 0 {
					query = query.Where("tx_id = ? AND vout = ?", cond["tx_id"], cond["vout"])
				} else {
					query = query.Or("tx_id = ? AND vout = ?", cond["tx_id"], cond["vout"])
				}
			}
			query.Find(&deposits)

			// Build map for quick lookup
			for i := range deposits {
				key := fmt.Sprintf("%s:%d", deposits[i].TxId, deposits[i].Vout)
				depositMap[key] = deposits[i]
			}
		}
	}

	for _, utxo := range utxos {
		// Create bridge transaction for this UTXO
		// EVM address validation is now done before calling this function
		// IMPORTANT: txBytes must be the raw Dogecoin transaction bytes (no-witness)
		// We persist these in deposits.tx_bytes when scanning blocks; fetch them here.
		var txBytes []byte
		{
			key := fmt.Sprintf("%s:%d", utxo.Txid, utxo.OutIndex)
			if dep, exists := depositMap[key]; exists && len(dep.TxBytes) > 0 {
				txBytes = dep.TxBytes
			}

			if len(txBytes) == 0 {
				// CRITICAL: Do not fallback to txid bytes - the bridge contract requires raw tx bytes for verification
				up.logger.Errorf("Deposit raw bytes missing for %s:%d - skipping this UTXO", utxo.Txid, utxo.OutIndex)
				continue
			}
		}

		// Convert satoshis (8 decimals) to ERC20 wei (18 decimals)
		// Dogecoin uses 8 decimals (1 DOGE = 100,000,000 satoshis)
		// ERC20 uses 18 decimals (1 DOGE = 100,000,000,000,000,000,000 wei)
		// Conversion: multiply by 10^10
		amountWei := new(big.Int).Mul(big.NewInt(utxo.Amount), big.NewInt(10000000000))
		bridgeTx := contract.BridgeTransaction{
			DestEvmAddress: common.HexToAddress(utxo.EvmAddr),
			Amount:         amountWei,
			Txout:          uint32(utxo.OutIndex),
			TxBytes:        txBytes,
		}

		currentBatch.TransactionParams = append(currentBatch.TransactionParams, bridgeTx)
		currentBatch.TotalAmount.Add(currentBatch.TotalAmount, amountWei)
		currentBatch.UTXOs = append(currentBatch.UTXOs, utxo)

		// Check if batch is full (limit to prevent large transactions)
		if len(currentBatch.TransactionParams) >= 1 {
			batches = append(batches, currentBatch)
			currentBatch = &BridgeInBatch{
				ID:                big.NewInt(time.Now().Unix() + int64(len(batches))),
				TransactionParams: make([]contract.BridgeTransaction, 0),
				TotalAmount:       big.NewInt(0),
				UTXOs:             make([]*models.UTXO, 0),
			}
		}
	}

	// Add the last batch if it has transactions
	if len(currentBatch.TransactionParams) > 0 {
		batches = append(batches, currentBatch)
	}

	return batches
}

// pendingBatchRetryLoop monitors pending batches and retries failed sessions
func (up *UtxoProcessor) pendingBatchRetryLoop() {
	interval := up.tssRequestRetryBackoff / 2
	if interval <= 0 {
		interval = 5 * time.Second
	}
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		select {
		case <-up.ctx.Done():
			up.logger.Info("Pending batch retry loop stopping...")
			return
		case <-ticker.C:
			if err := up.retryPendingBatches(); err != nil {
				up.logger.Errorf("Failed to retry pending batches: %v", err)
			}
		}
	}
}

// retryPendingBatches checks and retries pending batches that may have failed
func (up *UtxoProcessor) retryPendingBatches() error {
	isProposer, err := up.isCurrentProposer()
	if err != nil {
		return fmt.Errorf("failed to check proposer status: %w", err)
	}

	if !isProposer {
		return nil // Only the current proposer actively retries
	}

	now := time.Now()
	retries := 0

	up.pendingBatches.Range(func(key, value interface{}) bool {
		currentKey, ok := key.(string)
		if !ok {
			return true
		}

		pending, ok := value.(*pendingBatch)
		if !ok {
			return true
		}

		if pending.nextRetryAt.IsZero() || now.Before(pending.nextRetryAt) {
			return true
		}

		if up.tssRequestMaxRetries > 0 && pending.nextAttempt >= up.tssRequestMaxRetries {
			up.logger.Warnf("Max retries reached for session %s (base %s)", currentKey, pending.baseSessionID)
			return true
		}

		var retryErr error
		switch pending.batchType {
		case "deposit":
			retryErr = up.retryDepositBatch(pending)
		case "withdrawal":
			retryErr = up.retryWithdrawalBatch(pending)
		default:
			up.logger.Warnf("Unknown pending batch type %s for session %s", pending.batchType, currentKey)
			return true
		}

		if retryErr != nil {
			up.logger.Errorf("Failed to retry pending batch %s: %v", currentKey, retryErr)
		} else {
			retries++
		}

		return true
	})

	if retries > 0 {
		up.logger.Infof("Retried %d pending batches", retries)
	}

	return nil
}

func (up *UtxoProcessor) retryDepositBatch(pending *pendingBatch) error {
	if pending.depositBatch == nil {
		return fmt.Errorf("nil deposit batch")
	}
	if pending.baseSessionID == "" {
		return fmt.Errorf("missing base session id")
	}

	up.logger.Infof("Retrying deposit batch for base session %s (next attempt %d)", pending.baseSessionID, pending.nextAttempt+1)
	actualSession := up.assignNewTssSession(pending)
	up.pendingBatches.Store(pending.baseSessionID, pending)

	if err := up.updatePendingBatchInDB(pending); err != nil {
		up.logger.Warnf("Failed to update pending batch %s in database (continuing anyway): %v", pending.baseSessionID, err)
	}

	if err := up.requestTssSignature(pending.calldata, actualSession); err != nil {
		return err
	}
	return nil
}

func (up *UtxoProcessor) retryWithdrawalBatch(pending *pendingBatch) error {
	if pending.withdrawalRequest == nil {
		return fmt.Errorf("nil withdrawal request")
	}
	if pending.baseSessionID == "" {
		return fmt.Errorf("missing base session id")
	}

	up.logger.Infof("Retrying withdrawal batch for base session %s (next attempt %d)", pending.baseSessionID, pending.nextAttempt+1)
	actualSession := up.assignNewTssSession(pending)
	up.pendingBatches.Store(pending.baseSessionID, pending)

	if err := up.updatePendingBatchInDB(pending); err != nil {
		up.logger.Warnf("Failed to update pending withdrawal batch %s in database (continuing anyway): %v", pending.baseSessionID, err)
	}

	return up.requestWithdrawalTssSignature(pending.calldata, actualSession, pending.withdrawalRequest)
}

// processDepositBatch processes a batch of deposit bridge transactions
func (up *UtxoProcessor) processDepositBatch(batch *BridgeInBatch) error {
	// Create session ID for TSS signing based on the batch contents
	sessionID, err := up.generateSessionID(batch)
	if err != nil {
		return fmt.Errorf("failed to generate session ID: %w", err)
	}

	// Derive deterministic batch ID so every node rebuilds identical calldata
	derivedID, err := deriveDeterministicIDFromSession(sessionID)
	if err != nil {
		up.logger.Errorf("Failed to derive deterministic batch ID for session %s: %v", sessionID, err)
		return fmt.Errorf("failed to derive batch ID: %w", err)
	}
	batch.ID = derivedID
	up.logger.Infof("Generated session ID: %s, derived batch ID: %s", sessionID, batch.ID.String())

	up.logger.Infof("Processing deposit bridge batch %s with %d transactions, total amount: %s DOGE",
		batch.ID.String(), len(batch.TransactionParams), batch.TotalAmount.String())

	// Generate bridge transaction calldata using the deterministic batch ID
	calldata, err := up.generateBridgeInCalldata(batch)
	if err != nil {
		return fmt.Errorf("failed to generate bridge calldata: %w", err)
	}

	calldataPreview := calldata
	if len(calldata) > 64 {
		calldataPreview = calldata[:64]
	}
	up.logger.Infof("Generated bridge calldata (%d bytes): %x...", len(calldata), calldataPreview)
	up.logger.Debugf("Bridge calldata (full hex): %x", calldata)

	// Fetch current TSS nonce to include in signing payload
	tssNonce, err := up.fetchTssNonce()
	if err != nil {
		return fmt.Errorf("failed to fetch tss nonce: %w", err)
	}
	up.logger.Infof("Using TSS nonce %s for batch %s", tssNonce.String(), batch.ID.String())

	// Add safety check for calldata size
	if len(calldata) > 100000 { // 100KB limit
		return fmt.Errorf("calldata too large: %d bytes", len(calldata))
	}

	// Store the pending batch data
	pending := &pendingBatch{
		batchType:     "deposit",
		depositBatch:  batch,
		calldata:      calldata,
		utxos:         batch.UTXOs,
		baseSessionID: sessionID,
		tssNonce:      new(big.Int).Set(tssNonce),
	}
	actualSessionID := up.assignNewTssSession(pending)
	up.pendingBatches.Store(pending.baseSessionID, pending)

	if err := up.persistPendingBatch(pending); err != nil {
		up.logger.Warnf("Failed to persist pending batch %s to database (continuing anyway): %v", pending.baseSessionID, err)
	}

	// Add defer to clean up on panic
	defer func() {
		if r := recover(); r != nil {
			up.logger.Errorf("Panic in TSS signature request: %v", r)
			up.pendingBatches.Delete(pending.baseSessionID)
			up.tssSessionAliases.Delete(actualSessionID)
		}
	}()

	// Re-enable TSS with step-by-step logging and error handling
	up.logger.Infof("Starting TSS signature request for batch %s", batch.ID.String())

	// Add step-by-step logging to isolate the exact failure point
	up.logger.Infof("Step 1: About to call requestTssSignature")

	if err := up.requestTssSignature(calldata, actualSessionID); err != nil {
		up.logger.Errorf("Initial TSS signature request failed for session %s: %v", actualSessionID, err)
		now := time.Now()
		retryDelay := up.tssRequestRetryBackoff
		if retryDelay <= 0 {
			retryDelay = 15 * time.Second
		}
		pending.lastAttempt = now
		pending.nextRetryAt = now.Add(retryDelay)
		up.pendingBatches.Store(pending.baseSessionID, pending)
		return fmt.Errorf("failed to request TSS signature: %w", err)
	}

	up.logger.Infof("TSS signature requested successfully for deposit batch %s", batch.ID.String())

	return nil
}

// processWithdrawalBatch processes a batch of withdrawal bridge transactions
func (up *UtxoProcessor) processWithdrawalRequest(request *withdrawalRequest) error {
	// Create session ID for TSS signing before building calldata
	sessionID, err := up.generateWithdrawalSessionID(request)
	if err != nil {
		return fmt.Errorf("failed to generate session ID: %w", err)
	}

	// Align withdrawal request ID with session so all nodes compute identical calldata
	derivedID, err := deriveDeterministicIDFromSession(sessionID)
	if err != nil {
		up.logger.Errorf("Failed to derive deterministic withdrawal ID for session %s: %v", sessionID, err)
		return fmt.Errorf("failed to derive withdrawal ID: %w", err)
	}
	request.ID = derivedID
	up.logger.Infof("Generated withdrawal session ID: %s, derived withdrawal ID: %s", sessionID, request.ID.String())

	txRef := request.TxId
	if txRef == "" && request.UTXO != nil {
		txRef = request.UTXO.Uid
	}
	up.logger.Infof("Processing withdrawal request %s for tx %s, total amount: %s DOGE",
		request.ID.String(), txRef, request.TotalAmount.String())

	// Generate bridgeOutFinish calldata for withdrawal using the deterministic request ID
	calldata, err := up.generateBridgeOutFinishCalldata(request)
	if err != nil {
		return fmt.Errorf("failed to generate bridgeOutFinish calldata: %w", err)
	}

	up.logger.Infof("Generated bridgeOutFinish calldata: %x", calldata)

	// Fetch current TSS nonce
	tssNonce, err := up.fetchTssNonce()
	if err != nil {
		return fmt.Errorf("failed to fetch tss nonce: %w", err)
	}
	up.logger.Infof("Using TSS nonce %s for withdrawal request %s", tssNonce.String(), request.ID.String())

	// Store the pending request data
	utxos := make([]*models.UTXO, 0)
	if request.UTXO != nil {
		utxos = append(utxos, request.UTXO)
	}
	pending := &pendingBatch{
		batchType:         "withdrawal",
		withdrawalRequest: request,
		calldata:          calldata,
		utxos:             utxos,
		baseSessionID:     sessionID,
		tssNonce:          new(big.Int).Set(tssNonce),
	}
	actualSessionID := up.assignNewTssSession(pending)
	up.pendingBatches.Store(pending.baseSessionID, pending)

	if err := up.persistPendingBatch(pending); err != nil {
		up.logger.Warnf("Failed to persist pending withdrawal batch %s to database (continuing anyway): %v", pending.baseSessionID, err)
	}

	// Request TSS signature asynchronously
	if err := up.requestWithdrawalTssSignature(calldata, actualSessionID, request); err != nil {
		up.logger.Errorf("Initial withdrawal TSS request failed for session %s: %v", actualSessionID, err)
		now := time.Now()
		retryDelay := up.tssRequestRetryBackoff
		if retryDelay <= 0 {
			retryDelay = 15 * time.Second
		}
		pending.lastAttempt = now
		pending.nextRetryAt = now.Add(retryDelay)
		up.pendingBatches.Store(pending.baseSessionID, pending)
		return fmt.Errorf("failed to request TSS signature: %w", err)
	}

	// The actual signing will be handled by the event bus
	up.logger.Infof("TSS signature requested for withdrawal request %s", request.ID.String())

	return nil
}

func (up *UtxoProcessor) generateSessionID(batch *BridgeInBatch) (string, error) {
	// Create a deterministic session ID based on the UTXOs in the batch
	// This ensures all nodes generate the same session ID for the same batch

	if len(batch.UTXOs) == 0 {
		return "", fmt.Errorf("cannot generate session ID for empty batch")
	}

	// Create a list of UTXO txids and sort them for consistency
	txids := make([]string, len(batch.UTXOs))
	for i, utxo := range batch.UTXOs {
		txids[i] = utxo.Txid
	}

	// Sort to ensure consistent ordering across all nodes
	sort.Strings(txids)

	// Concatenate all txids
	var concatenated string
	for _, txid := range txids {
		concatenated += txid + "|"
	}

	// Create SHA256 hash of the concatenated txids
	hash := sha256.Sum256([]byte(concatenated))
	hashHex := hex.EncodeToString(hash[:])

	// Take the first 16 characters of the hash for a reasonably short but unique session ID
	shortHash := hashHex[:16]

	return fmt.Sprintf("session-%s", shortHash), nil
}

// generateWithdrawalSessionID generates a session ID for withdrawal requests
func (up *UtxoProcessor) generateWithdrawalSessionID(request *withdrawalRequest) (string, error) {
	// Create a deterministic session ID based on the single UTXO and its task IDs
	// This ensures all nodes generate the same session ID for the same withdrawal

	var txid string
	switch {
	case request.TxId != "":
		txid = request.TxId
	case request.UTXO != nil:
		txid = request.UTXO.Txid
	default:
		return "", fmt.Errorf("cannot generate session ID for request with no txid/UTXO")
	}

	// Create a deterministic string combining UTXO info and task IDs
	var concatenated string
	concatenated += "withdrawal|" + txid + "|"

	// Add task IDs to ensure uniqueness
	for _, taskId := range request.TaskIds {
		concatenated += taskId.String() + "|"
	}

	// Add total amount for additional uniqueness
	concatenated += request.TotalAmount.String()

	// Create SHA256 hash of the concatenated data
	hash := sha256.Sum256([]byte(concatenated))
	hashHex := hex.EncodeToString(hash[:])

	// Take the first 16 characters of the hash for a reasonably short but unique session ID
	shortHash := hashHex[:16]

	return fmt.Sprintf("session-%s", shortHash), nil
}

// generateBridgeInCalldata generates the calldata for bridge transactions
func (up *UtxoProcessor) generateBridgeInCalldata(batch *BridgeInBatch) ([]byte, error) {
	if up.contractBuilder == nil {
		return nil, fmt.Errorf("contract builder not set")
	}

	// Generate the bridge transaction calldata
	calldata, err := up.contractBuilder.GenerateBridgeInTxData(batch.TransactionParams)
	if err != nil {
		return nil, fmt.Errorf("failed to generate bridge transaction data: %w", err)
	}

	return calldata, nil
}

func deriveDeterministicIDFromSession(sessionID string) (*big.Int, error) {
	result := new(big.Int)
	if sessionID == "" {
		return result, fmt.Errorf("empty session id")
	}

	// Try to find the last "-" and extract the suffix
	idx := strings.LastIndex(sessionID, "-")
	if idx == -1 || idx+1 >= len(sessionID) {
		// If no "-" found, use the whole session ID as suffix
		suffix := sessionID
		if _, ok := result.SetString(suffix, 16); ok {
			return result, nil
		}
	} else {
		suffix := sessionID[idx+1:]
		if _, ok := result.SetString(suffix, 16); ok {
			return result, nil
		}
	}

	// Fall back to hashing the whole session ID for deterministic behavior
	hash := crypto.Keccak256Hash([]byte(sessionID))
	result.SetBytes(hash.Bytes())
	return result, nil // Return nil error since this is a valid fallback
}

// generateBridgeOutFinishCalldata generates the calldata for bridgeOutFinish function
func (up *UtxoProcessor) generateBridgeOutFinishCalldata(request *withdrawalRequest) ([]byte, error) {
	if up.contractBuilder == nil {
		return nil, fmt.Errorf("contract builder not set")
	}

	if request.TotalAmount == nil {
		return nil, fmt.Errorf("withdrawal amount not set")
	}

	var rawBytes []byte
	switch {
	case len(request.TxBytes) > 0:
		rawBytes = request.TxBytes
	case request.UTXO != nil:
		rawBytes = []byte(request.UTXO.Txid)
	default:
		return nil, fmt.Errorf("no tx bytes available for withdrawal request")
	}

	bridgeTx := contract.BridgeOutTransaction{
		Amount:  request.TotalAmount,
		TxBytes: rawBytes,
	}

	up.logger.Infof("Generating bridgeOutFinish calldata: requestId=%s, totalAmount=%s, taskIds=%d",
		request.ID.String(), request.TotalAmount.String(), len(request.TaskIds))

	// Generate the bridgeOutFinish transaction calldata
	calldata, err := up.contractBuilder.GenerateBridgeOutFinishTxData(bridgeTx, request.TaskIds)
	if err != nil {
		return nil, fmt.Errorf("failed to generate bridgeOutFinish transaction data: %w", err)
	}

	up.logger.Debugf("Generated bridgeOutFinish calldata for UTXO %s with %d task IDs: [%v]",
		request.TxId, len(request.TaskIds), request.TaskIds)

	return calldata, nil
}

// markUTXOsAsProcessed marks UTXOs as processed to avoid reprocessing
func (up *UtxoProcessor) markUTXOsAsProcessed(utxos []*models.UTXO) error {
	if len(utxos) == 0 {
		return nil
	}

	// Use a single transaction for all updates to prevent database corruption
	return up.conn.GetDB().Transaction(func(tx *gorm.DB) error {
		now := time.Now()
		for _, utxo := range utxos {
			utxo.UpdatedAt = now
			utxo.Status = models.UTXO_STATUS_PROCESSED
			if err := tx.Save(utxo).Error; err != nil {
				return fmt.Errorf("failed to mark UTXO %s as processed: %w", utxo.Uid, err)
			}
		}
		up.logger.Debugf("Marked %d UTXOs as processed in transaction", len(utxos))
		return nil
	})
}

// requestTssSignature creates a batch proposal, sends it to other nodes, and requests TSS signature
func (up *UtxoProcessor) requestTssSignature(calldata []byte, sessionID string) error {
	up.logger.Infof("Step 2: Entering requestTssSignature for session %s", sessionID)

	if up.chainID == nil {
		return fmt.Errorf("chain ID not set")
	}

	// Get the current batch from pending batches to create the proposal
	up.logger.Infof("Step 3: Loading pending batch for session %s", sessionID)
	_, pending, exists := up.loadPendingForSession(sessionID)
	if !exists {
		return fmt.Errorf("no pending batch found for session %s", sessionID)
	}

	// This function should only be called for deposit batches
	if pending.batchType != "deposit" || pending.depositBatch == nil {
		return fmt.Errorf("requestTssSignature called for non-deposit batch or nil depositBatch")
	}

	batch := pending.depositBatch
	up.logger.Infof("Step 4: Batch loaded, getting proposer address")

	// Get this node's Ethereum address (proposer)
	proposerAddress, err := up.getNodeEthereumAddress()
	if err != nil {
		return fmt.Errorf("failed to get proposer address: %w", err)
	}

	// Create batch ID from session ID for consistency
	baseSessionID := pending.baseSessionID
	if baseSessionID == "" {
		baseSessionID = sessionID
	}

	batchID := fmt.Sprintf("batch-%s", baseSessionID)
	up.logger.Infof("Step 5: Creating deposit proposal for batch %s", batchID)

	// Create the deposit proposal for the batch
	proposal := NewDepositProposal(
		batchID,
		batch.UTXOs,
		batch.TotalAmount,
		calldata,
		pending.tssNonce,
		proposerAddress.Hex(),
		sessionID,
	)

	up.logger.Infof("Step 6: Sending proposal to P2P network")
	// Send proposal to other P2P nodes
	if err := up.sendProposalToP2P(proposal); err != nil {
		up.logger.Errorf("Failed to send proposal to P2P network: %v", err)
		// Continue with TSS signing even if P2P broadcast fails
	} else {
		up.logger.Infof("Batch proposal sent to P2P network for session %s", sessionID)
	}

	up.logger.Infof("Step 7: Creating hash and calling TSS sign")
	// Create hash to sign for verifyAndCall function
	if pending.tssNonce == nil {
		return fmt.Errorf("pending batch missing tss nonce")
	}

	digest, baseHash, err := up.computeVerifyAndCallDigest(calldata, pending.tssNonce)
	if err != nil {
		return fmt.Errorf("failed to compute signing digest: %w", err)
	}

	// Print calldata hash for debugging
	up.logger.Infof("verifyAndCall base hash: %x (nonce=%s)", baseHash[:], pending.tssNonce.String())
	up.logger.Infof("Signing digest for TSS (keccak with prefix) %x", digest)
	up.logger.Infof("Calldata length: %d bytes", len(calldata))

	// Request TSS signature using the callTssSign function
	up.callTssSign(sessionID, digest)
	up.logger.Infof("TSS signing requested for batch %s with session %s (base session %s)", batchID, sessionID, baseSessionID)

	return nil
}

// sendProposalToP2P sends the batch proposal to other P2P nodes
func (up *UtxoProcessor) sendProposalToP2P(proposal *DepositProposal) error {
	up.logger.Infof("Step 6.1: Getting P2P module")
	p2pModule, ok := module.GetModule((&p2p.P2PModule{}).Name())
	if !ok {
		return fmt.Errorf("p2p module not found")
	}

	up.logger.Infof("Step 6.2: Marshaling proposal to JSON")
	payload, err := json.Marshal(proposal)
	if err != nil {
		return fmt.Errorf("failed to marshal proposal: %v", err)
	}

	up.logger.Infof("Step 6.3: Proposal marshaled successfully, size: %d bytes", len(payload))

	// Add size limit for P2P messages
	if len(payload) > 50000 { // 50KB limit
		return fmt.Errorf("P2P message too large: %d bytes", len(payload))
	}

	up.logger.Infof("Step 6.4: Broadcasting P2P message")
	err = p2pModule.(p2p.P2PSender).BroadcastP2PMessage(types.P2PBroadcastMessage{
		Type:      types.P2PMessageTypeDepositProposal,
		SessionID: proposal.SessionID,
		Payload:   payload,
	})

	if err != nil {
		return fmt.Errorf("failed to broadcast P2P message: %w", err)
	}

	up.logger.Infof("Step 6.5: P2P message broadcast completed")
	return nil
}

// requestWithdrawalTssSignature creates a withdrawal proposal and requests TSS signature
func (up *UtxoProcessor) requestWithdrawalTssSignature(calldata []byte, sessionID string, request *withdrawalRequest) error {
	if up.chainID == nil {
		return fmt.Errorf("chain ID not set")
	}

	// Get this node's Ethereum address (proposer)
	proposerAddress, err := up.getNodeEthereumAddress()
	if err != nil {
		return fmt.Errorf("failed to get proposer address: %w", err)
	}

	// Load pending to access base session ID
	_, pending, exists := up.loadPendingForSession(sessionID)
	if !exists {
		return fmt.Errorf("no pending withdrawal batch found for session %s", sessionID)
	}

	baseSessionID := pending.baseSessionID
	if baseSessionID == "" {
		baseSessionID = sessionID
	}

	// Create request ID from base session ID for consistency
	requestID := fmt.Sprintf("withdrawal-request-%s", baseSessionID)

	utxos := make([]*models.UTXO, 0)
	if request.UTXO != nil {
		utxos = append(utxos, request.UTXO)
	}

	// Create the withdrawal proposal for the request
	proposal := NewWithdrawalProposal(
		requestID,
		utxos,
		request.TotalAmount,
		calldata,
		request.TaskIds,
		pending.tssNonce,
		proposerAddress.Hex(),
		sessionID,
	)
	proposal.TxBytes = request.TxBytes
	proposal.TxId = request.TxId

	// Send proposal to other P2P nodes
	if err := up.sendWithdrawalProposalToP2P(proposal); err != nil {
		up.logger.Errorf("Failed to send withdrawal proposal to P2P network: %v", err)
		// Continue with TSS signing even if P2P broadcast fails
	} else {
		up.logger.Infof("Withdrawal proposal sent to P2P network for session %s", sessionID)
	}

	// Create hash to sign for verifyAndCall function
	if pending.tssNonce == nil {
		return fmt.Errorf("pending withdrawal batch missing tss nonce")
	}

	digest, baseHash, err := up.computeVerifyAndCallDigest(calldata, pending.tssNonce)
	if err != nil {
		return fmt.Errorf("failed to compute withdrawal signing digest: %w", err)
	}

	// Print calldata hash for debugging
	up.logger.Infof("Withdrawal verifyAndCall base hash: %x (nonce=%s)", baseHash[:], pending.tssNonce.String())
	up.logger.Infof("Withdrawal signing digest: %x", digest)
	up.logger.Infof("Withdrawal calldata length: %d bytes", len(calldata))

	// Request TSS signature using the callTssSign function
	up.callTssSign(sessionID, digest)
	up.logger.Infof("TSS signing requested for withdrawal request %s with session %s (base session %s)", requestID, sessionID, baseSessionID)

	return nil
}

// sendWithdrawalProposalToP2P sends the withdrawal proposal to other P2P nodes
func (up *UtxoProcessor) sendWithdrawalProposalToP2P(proposal *WithdrawalProposal) error {
	p2pModule, ok := module.GetModule((&p2p.P2PModule{}).Name())
	if !ok {
		return fmt.Errorf("p2p module not found")
	}

	payload, err := json.Marshal(proposal)
	if err != nil {
		return fmt.Errorf("failed to marshal withdrawal proposal: %v", err)
	}

	return p2pModule.(p2p.P2PSender).BroadcastP2PMessage(types.P2PBroadcastMessage{
		Type:      types.P2PMessageTypeBridgeOut,
		SessionID: proposal.SessionID,
		Payload:   payload,
	})
}

// GetStats returns current UTXO manager statistics
func (up *UtxoProcessor) GetStats() map[string]interface{} {
	return map[string]interface{}{
		"is_running":        up.isRunning,
		"last_processed_id": up.lastProcessedId,
		"poll_interval":     up.pollInterval.String(),
		"batch_size":        up.batchSize,
		"bridge_contract":   up.bridgeContract.Hex(),
		"current_proposer":  up.currentProposer.Hex(),
		"proposer_set":      up.proposerSet,
		"processing_model":  "deposits_batched_withdrawals_individual",
	}
}

// getNodeEthereumAddress gets this node's Ethereum address from P2P module
func (up *UtxoProcessor) getNodeEthereumAddress() (common.Address, error) {
	// Get the P2P module from the module registry
	p2pModule, ok := module.GetModule((&p2p.P2PModule{}).Name())
	if !ok {
		return common.Address{}, fmt.Errorf("P2P module not found in registry")
	}

	p2pModuleInstance, ok := p2pModule.(*p2p.P2PModule)
	if !ok {
		return common.Address{}, fmt.Errorf("failed to cast P2P module to P2PModule type")
	}

	network := p2pModuleInstance.GetNetwork()
	if network == nil {
		return common.Address{}, fmt.Errorf("P2P network not initialized")
	}

	return network.GetNodeEthereumAddress()
}

// getVOUTsForTransaction retrieves all VOUT records for a specific transaction
func (up *UtxoProcessor) getVOUTsForTransaction(txid string) ([]*models.VOUT, error) {
	var vouts []*models.VOUT

	err := up.conn.GetDB().Where(
		"txid = ? AND source = ?",
		txid,
		models.UTXO_SOURCE_WITHDRAWAL,
	).Order("out_index ASC").Find(&vouts).Error

	if err != nil {
		return nil, err
	}

	return vouts, nil
}

// createWithdrawalRequestFromUTXO creates a withdrawal request from a single UTXO and its outputs
func (up *UtxoProcessor) createWithdrawalRequestFromUTXO(utxo *models.UTXO, vouts []*models.VOUT) *withdrawalRequest {
	// Create task IDs from VOUT WithdrawId or OutIndex
	taskIds := make([]*big.Int, 0, len(vouts))
	totalAmount := big.NewInt(0)

	for _, vout := range vouts {
		// Use WithdrawId as task ID if available, otherwise use a combination of txid and out_index
		var taskId *big.Int
		if vout.WithdrawId != "" {
			// Parse WithdrawId as task ID (assuming it's numeric)
			taskId = big.NewInt(0)
			taskId.SetString(vout.WithdrawId, 10)
		} else {
			// Fallback: use VOUT ID or OutIndex as task ID
			taskId = big.NewInt(int64(vout.ID))
		}

		taskIds = append(taskIds, taskId)
		// Convert satoshis (8 decimals) to ERC20 wei (18 decimals)
		amountWei := new(big.Int).Mul(big.NewInt(vout.Amount), big.NewInt(10000000000))
		totalAmount.Add(totalAmount, amountWei)

		up.logger.Debugf("VOUT %d: amount=%d satoshis -> %s wei, receiver=%s, taskId=%s",
			vout.OutIndex, vout.Amount, amountWei.String(), vout.Receiver, taskId.String())
	}

	return &withdrawalRequest{
		ID:          big.NewInt(time.Now().Unix()),
		UTXO:        utxo,
		TxId:        utxo.Txid,
		TotalAmount: totalAmount,
		TaskIds:     taskIds,
	}
}
