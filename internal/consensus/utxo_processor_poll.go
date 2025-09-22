package consensus

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"math/big"
	"sort"
	"time"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/crypto"
	"github.com/goat-network/dogecoin-relayer/internal/models"
	"github.com/goat-network/dogecoin-relayer/internal/p2p"
	"github.com/goat-network/dogecoin-relayer/pkg/contract"
	"github.com/goat-network/dogecoin-relayer/pkg/module"
	"github.com/goat-network/dogecoin-relayer/pkg/types"
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

// pendingBatchRetryLoop monitors pending batches and retries failed sessions
func (up *UtxoProcessor) pendingBatchRetryLoop() {
	ticker := time.NewTicker(30 * time.Second) // Check every 30 seconds
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
	// Only proposer should retry and rebroadcast
	isProposer, err := up.isCurrentProposer()
	if err != nil {
		return fmt.Errorf("failed to check proposer status: %w", err)
	}

	if !isProposer {
		return nil // Only proposer retries
	}

	retryCount := 0
	up.pendingBatches.Range(func(key, value interface{}) bool {
		sessionID := key.(string)
		pending := value.(*pendingBatch)

		// Check if this is a deposit batch that needs retry
		if pending.batchType == "deposit" && pending.depositBatch != nil {
			up.logger.Infof("Retrying pending deposit batch for session %s", sessionID)

			// Recreate and send proposal
			if err := up.retryDepositBatch(sessionID, pending); err != nil {
				up.logger.Errorf("Failed to retry deposit batch %s: %v", sessionID, err)
			} else {
				retryCount++
			}
		}

		return true // Continue iteration
	})

	if retryCount > 0 {
		up.logger.Infof("Retried %d pending deposit batches", retryCount)
	}

	return nil
}

// retryDepositBatch retries a specific deposit batch
func (up *UtxoProcessor) retryDepositBatch(sessionID string, pending *pendingBatch) error {
	batch := pending.depositBatch

	// Get proposer address
	proposerAddress, err := up.getNodeEthereumAddress()
	if err != nil {
		return fmt.Errorf("failed to get proposer address: %w", err)
	}

	// Create batch ID from session ID
	batchID := fmt.Sprintf("batch-%s", sessionID)

	// Recreate the deposit proposal
	proposal := NewDepositProposal(
		batchID,
		batch.UTXOs,
		batch.TotalAmount,
		pending.calldata,
		proposerAddress.Hex(),
		sessionID,
	)

	// Rebroadcast proposal to P2P network
	if err := up.sendProposalToP2P(proposal); err != nil {
		up.logger.Errorf("Failed to rebroadcast proposal: %v", err)
	} else {
		up.logger.Infof("Rebroadcast proposal for session %s", sessionID)
	}

	// Re-request TSS signature
	hash := crypto.Keccak256(pending.calldata)
	up.callTssSign(sessionID, hash)
	up.logger.Infof("Re-requested TSS signature for session %s", sessionID)

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

	for i, batch := range batches {
		up.logger.Infof("Processing batch %d/%d with %d UTXOs", i+1, len(batches), len(batch.UTXOs))
		if err := up.processDepositBatch(batch); err != nil {
			up.logger.Errorf("Failed to process deposit batch %s: %v", batch.ID.String(), err)
			continue
		}

		// Mark UTXOs as processed
		if err := up.markUTXOsAsProcessed(batch.UTXOs); err != nil {
			up.logger.Errorf("Failed to mark deposit UTXOs as processed: %v", err)
		}
	}

	return nil
}

// scanWithdrawalUTXOs handles withdrawal UTXO processing
func (up *UtxoProcessor) scanWithdrawalUTXOs() error {
	// Query for new unprocessed withdrawal UTXOs
	utxos, err := up.getUnprocessedWithdrawalUTXOs()
	if err != nil {
		return fmt.Errorf("failed to get unprocessed withdrawal UTXOs: %w", err)
	}

	if len(utxos) == 0 {
		return nil // No new withdrawal UTXOs to process
	}

	up.logger.Infof("Found %d new withdrawal UTXOs to process", len(utxos))

	// For withdrawals, each UTXO is processed individually (not batched)
	// because each UTXO contains multiple outputs aligned with task IDs
	for _, utxo := range utxos {
		if err := up.processWithdrawalUTXO(utxo); err != nil {
			up.logger.Errorf("Failed to process withdrawal UTXO %s: %v", utxo.Uid, err)
			continue
		}

		// Mark UTXO as processed
		if err := up.markUTXOsAsProcessed([]*models.UTXO{utxo}); err != nil {
			up.logger.Errorf("Failed to mark withdrawal UTXO as processed: %v", err)
		}
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
	// NOTE: Previously this query filtered out rows with empty evm_addr.
	// That caused valid deposit UTXOs to be skipped when evm_addr was not
	// populated yet during ingestion. We now fetch by source/status/id only
	// and defer the evm address check to the batching step, where UTXOs with
	// missing EVM address are explicitly skipped with a warning.
	err := up.conn.GetDB().Where(
		"source = ? AND status = ? AND id > ?",
		models.UTXO_SOURCE_DEPOSIT,
		models.UTXO_STATUS_CONFIRMED,
		up.lastProcessedId,
	).Limit(up.batchSize).Find(&utxos).Error

	if err != nil {
		return nil, err
	}

	// Update last processed ID
	if len(utxos) > 0 {
		up.lastProcessedId = utxos[len(utxos)-1].ID
	}

	return utxos, nil
}

// getUnprocessedWithdrawalUTXOs retrieves unprocessed withdrawal UTXOs from database
func (up *UtxoProcessor) getUnprocessedWithdrawalUTXOs() ([]*models.UTXO, error) {
	var utxos []*models.UTXO

	// Query for withdrawal UTXOs that haven't been processed yet
	// For withdrawals, we typically have one UTXO that contains multiple outputs
	err := up.conn.GetDB().Where(
		"source = ? AND status = ? AND id > ?",
		models.UTXO_SOURCE_WITHDRAWAL,
		models.UTXO_STATUS_CONFIRMED,
		up.lastProcessedId,
	).Limit(up.batchSize).Find(&utxos).Error

	if err != nil {
		return nil, err
	}

	// Update last processed ID
	if len(utxos) > 0 {
		up.lastProcessedId = utxos[len(utxos)-1].ID
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

	for _, utxo := range utxos {
		// Create bridge transaction for this UTXO
		// EVM address validation is now done before calling this function
		bridgeTx := contract.BridgeTransaction{
			DestEvmAddress: common.HexToAddress(utxo.EvmAddr),
			Amount:         big.NewInt(utxo.Amount),
			TxBytes:        []byte(utxo.Txid), // Store transaction ID as bytes
		}

		currentBatch.TransactionParams = append(currentBatch.TransactionParams, bridgeTx)
		currentBatch.TotalAmount.Add(currentBatch.TotalAmount, big.NewInt(utxo.Amount))
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

// processDepositBatch processes a batch of deposit bridge transactions
func (up *UtxoProcessor) processDepositBatch(batch *BridgeInBatch) error {
	up.logger.Infof("Processing deposit bridge batch %s with %d transactions, total amount: %s DOGE",
		batch.ID.String(), len(batch.TransactionParams), batch.TotalAmount.String())

	// Generate bridge transaction calldata
	calldata, err := up.generateBridgeInCalldata(batch)
	if err != nil {
		return fmt.Errorf("failed to generate bridge calldata: %w", err)
	}

	calldataPreview := calldata
	if len(calldata) > 64 {
		calldataPreview = calldata[:64]
	}
	up.logger.Infof("Generated bridge calldata (%d bytes): %x...", len(calldata), calldataPreview)

	// Add safety check for calldata size
	if len(calldata) > 100000 { // 100KB limit
		return fmt.Errorf("calldata too large: %d bytes", len(calldata))
	}

	// Create session ID for TSS signing
	sessionID, err := up.generateSessionID(batch)
	if err != nil {
		return fmt.Errorf("failed to generate session ID: %w", err)
	}

	// Store the pending batch data
	pending := &pendingBatch{
		batchType:    "deposit",
		depositBatch: batch,
		calldata:     calldata,
		utxos:        batch.UTXOs,
	}
	up.pendingBatches.Store(sessionID, pending)

	// Add defer to clean up on panic
	defer func() {
		if r := recover(); r != nil {
			up.logger.Errorf("Panic in TSS signature request: %v", r)
			up.pendingBatches.Delete(sessionID)
		}
	}()

	// Re-enable TSS with step-by-step logging and error handling
	up.logger.Infof("Starting TSS signature request for batch %s", batch.ID.String())

	// Add step-by-step logging to isolate the exact failure point
	up.logger.Infof("Step 1: About to call requestTssSignature")

	err = up.requestTssSignature(calldata, sessionID)
	if err != nil {
		// Clean up the pending batch on error
		up.pendingBatches.Delete(sessionID)
		up.logger.Errorf("TSS signature request failed: %v", err)

		// Mark UTXOs as processed even on TSS failure to avoid infinite retry
		if markErr := up.markUTXOsAsProcessed(batch.UTXOs); markErr != nil {
			up.logger.Errorf("Failed to mark UTXOs as processed after TSS failure: %v", markErr)
		}

		return fmt.Errorf("failed to request TSS signature: %w", err)
	}

	// The actual signing will be handled by the event bus
	up.logger.Infof("TSS signature requested successfully for deposit batch %s", batch.ID.String())

	return nil
}

// processWithdrawalBatch processes a batch of withdrawal bridge transactions
func (up *UtxoProcessor) processWithdrawalRequest(request *withdrawalRequest) error {
	up.logger.Infof("Processing withdrawal request %s with single UTXO %s, total amount: %s DOGE",
		request.ID.String(), request.UTXO.Uid, request.TotalAmount.String())

	// Generate bridgeOutFinish calldata for withdrawal
	calldata, err := up.generateBridgeOutFinishCalldata(request)
	if err != nil {
		return fmt.Errorf("failed to generate bridgeOutFinish calldata: %w", err)
	}

	up.logger.Infof("Generated bridgeOutFinish calldata: %x", calldata)

	// Create session ID for TSS signing
	sessionID, err := up.generateWithdrawalSessionID(request)
	if err != nil {
		return fmt.Errorf("failed to generate session ID: %w", err)
	}

	// Store the pending request data
	pending := &pendingBatch{
		batchType:         "withdrawal",
		withdrawalRequest: request,
		calldata:          calldata,
		utxos:             []*models.UTXO{request.UTXO}, // Convert single UTXO to slice for compatibility
	}
	up.pendingBatches.Store(sessionID, pending)

	// Request TSS signature asynchronously
	err = up.requestWithdrawalTssSignature(calldata, sessionID, request)
	if err != nil {
		// Clean up the pending request on error
		up.pendingBatches.Delete(sessionID)
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

	if request.UTXO == nil {
		return "", fmt.Errorf("cannot generate session ID for request with no UTXO")
	}

	utxo := request.UTXO

	// Create a deterministic string combining UTXO info and task IDs
	var concatenated string
	concatenated += "withdrawal|" + utxo.Txid + "|"

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

	return fmt.Sprintf("withdrawal-session-%s", shortHash), nil
}

// generateBridgeInCalldata generates the calldata for bridge transactions
func (up *UtxoProcessor) generateBridgeInCalldata(batch *BridgeInBatch) ([]byte, error) {
	if up.contractBuilder == nil {
		return nil, fmt.Errorf("contract builder not set")
	}

	// Generate the bridge transaction calldata
	calldata, err := up.contractBuilder.GenerateBridgeInTxData(batch.TransactionParams, batch.ID)
	if err != nil {
		return nil, fmt.Errorf("failed to generate bridge transaction data: %w", err)
	}

	return calldata, nil
}

// generateBridgeOutFinishCalldata generates the calldata for bridgeOutFinish function
func (up *UtxoProcessor) generateBridgeOutFinishCalldata(request *withdrawalRequest) ([]byte, error) {
	if up.contractBuilder == nil {
		return nil, fmt.Errorf("contract builder not set")
	}

	if request.UTXO == nil {
		return nil, fmt.Errorf("no UTXO in withdrawal request")
	}

	// For bridge out, we process a single UTXO that contains multiple outputs
	// Each output aligns with a task ID
	utxo := request.UTXO

	// Create bridge transaction using the UTXO transaction data
	// The transaction contains multiple outputs, but we represent it as a single BridgeTransaction
	// with the full transaction data and aligned task IDs
	bridgeTx := contract.BridgeTransaction{
		DestEvmAddress: common.HexToAddress(utxo.EvmAddr), // This might be the fee recipient or contract address
		Amount:         request.TotalAmount,               // Total amount of all outputs
		TxBytes:        []byte(utxo.Txid),                 // Transaction ID as bytes (could be full tx bytes if available)
	}

	up.logger.Infof("Generating bridgeOutFinish calldata: requestId=%s, totalAmount=%s, taskIds=%d",
		request.ID.String(), request.TotalAmount.String(), len(request.TaskIds))

	// Generate the bridgeOutFinish transaction calldata
	// This creates calldata for: bridgeOutFinish(uint256 requestId, BridgeTransaction bridgeTx, uint256[] taskIds)
	calldata, err := up.contractBuilder.GenerateBridgeOutFinishTxData(request.ID, bridgeTx, request.TaskIds)
	if err != nil {
		return nil, fmt.Errorf("failed to generate bridgeOutFinish transaction data: %w", err)
	}

	up.logger.Debugf("Generated bridgeOutFinish calldata for UTXO %s with %d task IDs: [%v]",
		utxo.Uid, len(request.TaskIds), request.TaskIds)

	return calldata, nil
}

// markUTXOsAsProcessed marks UTXOs as processed to avoid reprocessing
func (up *UtxoProcessor) markUTXOsAsProcessed(utxos []*models.UTXO) error {
	// Update UTXOs to mark as processed (we could add a "processed" status or use a separate table)
	// For now, we'll update the UpdatedAt timestamp to track processing
	for _, utxo := range utxos {
		utxo.UpdatedAt = time.Now()
		if err := up.conn.GetDB().Save(utxo).Error; err != nil {
			return fmt.Errorf("failed to mark UTXO %s as processed: %w", utxo.Uid, err)
		}
	}

	up.logger.Debugf("Marked %d UTXOs as processed", len(utxos))
	return nil
}

// requestTssSignature creates a batch proposal, sends it to other nodes, and requests TSS signature
func (up *UtxoProcessor) requestTssSignature(calldata []byte, sessionID string) error {
	up.logger.Infof("Step 2: Entering requestTssSignature for session %s", sessionID)

	if up.chainID == nil {
		return fmt.Errorf("chain ID not set")
	}

	// Get the current batch from pending batches to create the proposal
	up.logger.Infof("Step 3: Loading pending batch for session %s", sessionID)
	pendingData, exists := up.pendingBatches.Load(sessionID)
	if !exists {
		return fmt.Errorf("no pending batch found for session %s", sessionID)
	}

	pending := pendingData.(*pendingBatch)

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
	batchID := fmt.Sprintf("batch-%s", sessionID)
	up.logger.Infof("Step 5: Creating deposit proposal for batch %s", batchID)

	// Create the deposit proposal for the batch
	proposal := NewDepositProposal(
		batchID,
		batch.UTXOs,
		batch.TotalAmount,
		calldata,
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
	hash := crypto.Keccak256(calldata)

	// Request TSS signature using the callTssSign function
	up.callTssSign(sessionID, hash)
	up.logger.Infof("TSS signing requested for batch %s with session ID: %s", batchID, sessionID)

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
	payload, err := proposal.MarshalJSON()
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

	// Create request ID from session ID for consistency
	requestID := fmt.Sprintf("withdrawal-request-%s", sessionID)

	// Create the withdrawal proposal for the request
	proposal := NewWithdrawalProposal(
		requestID,
		[]*models.UTXO{request.UTXO}, // Convert single UTXO to slice for proposal compatibility
		request.TotalAmount,
		calldata,
		request.TaskIds,
		proposerAddress.Hex(),
		sessionID,
	)

	// Send proposal to other P2P nodes
	if err := up.sendWithdrawalProposalToP2P(proposal); err != nil {
		up.logger.Errorf("Failed to send withdrawal proposal to P2P network: %v", err)
		// Continue with TSS signing even if P2P broadcast fails
	} else {
		up.logger.Infof("Withdrawal proposal sent to P2P network for session %s", sessionID)
	}

	// Create hash to sign for verifyAndCall function
	hash := crypto.Keccak256(calldata)

	// Request TSS signature using the callTssSign function
	up.callTssSign(sessionID, hash)
	up.logger.Infof("TSS signing requested for withdrawal request %s with session ID: %s", requestID, sessionID)

	return nil
}

// sendWithdrawalProposalToP2P sends the withdrawal proposal to other P2P nodes
func (up *UtxoProcessor) sendWithdrawalProposalToP2P(proposal *WithdrawalProposal) error {
	p2pModule, ok := module.GetModule((&p2p.P2PModule{}).Name())
	if !ok {
		return fmt.Errorf("p2p module not found")
	}

	payload, err := proposal.MarshalJSON()
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
		totalAmount.Add(totalAmount, big.NewInt(vout.Amount))

		up.logger.Debugf("VOUT %d: amount=%d, receiver=%s, taskId=%s",
			vout.OutIndex, vout.Amount, vout.Receiver, taskId.String())
	}

	return &withdrawalRequest{
		ID:          big.NewInt(time.Now().Unix()),
		UTXO:        utxo,
		TotalAmount: totalAmount,
		TaskIds:     taskIds,
	}
}
