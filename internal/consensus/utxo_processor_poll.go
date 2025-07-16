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
			if err := up.processNewUTXOs(); err != nil {
				up.logger.Errorf("Failed to process new UTXOs: %v", err)
			}
		}
	}
}

// processNewUTXOs checks for new deposit UTXOs and processes them
func (up *UtxoProcessor) processNewUTXOs() error {
	// Check if this node should process UTXOs
	isProposer, err := up.isCurrentProposer()
	if err != nil {
		return fmt.Errorf("failed to check proposer status: %w", err)
	}

	if !isProposer {
		up.logger.Debug("Not the current proposer, skipping UTXO processing")
		return nil
	}

	// Query for new unprocessed deposit UTXOs
	utxos, err := up.getUnprocessedDepositUTXOs()
	if err != nil {
		return fmt.Errorf("failed to get unprocessed deposit UTXOs: %w", err)
	}

	if len(utxos) == 0 {
		return nil // No new UTXOs to process
	}

	up.logger.Infof("Found %d new deposit UTXOs to process", len(utxos))

	// Group UTXOs into batches for bridge transactions
	batches := up.groupUTXOsIntoBatches(utxos)

	for _, batch := range batches {
		if err := up.processBatch(batch); err != nil {
			up.logger.Errorf("Failed to process batch %s: %v", batch.ID.String(), err)
			continue
		}

		// Mark UTXOs as processed
		if err := up.markUTXOsAsProcessed(batch.UTXOs); err != nil {
			up.logger.Errorf("Failed to mark UTXOs as processed: %v", err)
		}
	}

	return nil
}

// getUnprocessedDepositUTXOs retrieves unprocessed deposit UTXOs from database
func (up *UtxoProcessor) getUnprocessedDepositUTXOs() ([]*models.UTXO, error) {
	var utxos []*models.UTXO

	// Query for deposit UTXOs that haven't been processed yet
	err := up.conn.GetDB().Where(
		"source = ? AND status = ? AND id > ? AND evm_addr != ?",
		models.UTXO_SOURCE_DEPOSIT,
		models.UTXO_STATUS_CONFIRMED,
		up.lastProcessedId,
		"", // Non-empty EVM address required for deposits
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
func (up *UtxoProcessor) groupUTXOsIntoBatches(utxos []*models.UTXO) []*bridgeBatch {
	var batches []*bridgeBatch
	currentBatch := &bridgeBatch{
		ID:           big.NewInt(time.Now().Unix()), // Simple batch ID based on timestamp
		Transactions: make([]contract.BridgeTransaction, 0),
		TotalAmount:  big.NewInt(0),
		UTXOs:        make([]*models.UTXO, 0),
	}

	for _, utxo := range utxos {
		// Validate UTXO has required data
		if utxo.EvmAddr == "" {
			up.logger.Warnf("Skipping UTXO %s: missing EVM address", utxo.Uid)
			continue
		}

		// Create bridge transaction for this UTXO
		bridgeTx := contract.BridgeTransaction{
			DestEvmAddress: common.HexToAddress(utxo.EvmAddr),
			Amount:         big.NewInt(utxo.Amount),
			TxBytes:        []byte(utxo.Txid), // Store transaction ID as bytes
		}

		currentBatch.Transactions = append(currentBatch.Transactions, bridgeTx)
		currentBatch.TotalAmount.Add(currentBatch.TotalAmount, big.NewInt(utxo.Amount))
		currentBatch.UTXOs = append(currentBatch.UTXOs, utxo)

		// Check if batch is full (limit to prevent large transactions)
		if len(currentBatch.Transactions) >= 5 {
			batches = append(batches, currentBatch)
			currentBatch = &bridgeBatch{
				ID:           big.NewInt(time.Now().Unix() + int64(len(batches))),
				Transactions: make([]contract.BridgeTransaction, 0),
				TotalAmount:  big.NewInt(0),
				UTXOs:        make([]*models.UTXO, 0),
			}
		}
	}

	// Add the last batch if it has transactions
	if len(currentBatch.Transactions) > 0 {
		batches = append(batches, currentBatch)
	}

	return batches
}

// processBatch processes a batch of bridge transactions
func (up *UtxoProcessor) processBatch(batch *bridgeBatch) error {
	up.logger.Infof("Processing bridge batch %s with %d transactions, total amount: %s DOGE",
		batch.ID.String(), len(batch.Transactions), batch.TotalAmount.String())

	// Generate bridge transaction calldata
	calldata, err := up.generateBridgeInCalldata(batch)
	if err != nil {
		return fmt.Errorf("failed to generate bridge calldata: %w", err)
	}

	up.logger.Infof("Generated bridge calldata: %x", calldata)

	// Create session ID for TSS signing
	sessionID, err := up.generateSessionID(batch)
	if err != nil {
		return fmt.Errorf("failed to generate session ID: %w", err)
	}

	// Store the pending batch data
	pending := &pendingBatch{
		batch:    batch,
		calldata: calldata,
		utxos:    batch.UTXOs,
	}
	up.pendingBatches.Store(sessionID, pending)

	// Request TSS signature asynchronously
	err = up.requestTssSignature(calldata, sessionID)
	if err != nil {
		// Clean up the pending batch on error
		up.pendingBatches.Delete(sessionID)
		return fmt.Errorf("failed to request TSS signature: %w", err)
	}

	// The actual signing will be handled by the event bus
	up.logger.Infof("TSS signature requested for batch %s", batch.ID.String())

	return nil
}

func (up *UtxoProcessor) generateSessionID(batch *bridgeBatch) (string, error) {
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

// generateBridgeInCalldata generates the calldata for bridge transactions
func (up *UtxoProcessor) generateBridgeInCalldata(batch *bridgeBatch) ([]byte, error) {
	if up.contractBuilder == nil {
		return nil, fmt.Errorf("contract builder not set")
	}

	// Generate the bridge transaction calldata
	calldata, err := up.contractBuilder.GenerateBridgeInTxData(batch.Transactions, batch.ID)
	if err != nil {
		return nil, fmt.Errorf("failed to generate bridge transaction data: %w", err)
	}

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
	if up.chainID == nil {
		return fmt.Errorf("chain ID not set")
	}

	// Get the current batch from pending batches to create the proposal
	pendingData, exists := up.pendingBatches.Load(sessionID)
	if !exists {
		return fmt.Errorf("no pending batch found for session %s", sessionID)
	}

	pending := pendingData.(*pendingBatch)
	batch := pending.batch

	// Get this node's Ethereum address (proposer)
	proposerAddress, err := up.getNodeEthereumAddress()
	if err != nil {
		return fmt.Errorf("failed to get proposer address: %w", err)
	}

	// Create batch ID from session ID for consistency
	batchID := fmt.Sprintf("batch-%s", sessionID)

	// Create the deposit proposal for the batch
	proposal := NewDepositProposal(
		batchID,
		batch.UTXOs,
		batch.TotalAmount,
		calldata,
		proposerAddress.Hex(),
		sessionID,
	)

	// Send proposal to other P2P nodes
	if err := up.sendProposalToP2P(proposal); err != nil {
		up.logger.Errorf("Failed to send proposal to P2P network: %v", err)
		// Continue with TSS signing even if P2P broadcast fails
	} else {
		up.logger.Infof("Batch proposal sent to P2P network for session %s", sessionID)
	}

	// Create hash to sign for verifyAndCall function
	// This would typically be: keccak256(abi.encode(targets, calldata, tssNonce, chainID))
	// For simplicity, we'll use the calldata hash directly
	hash := crypto.Keccak256(calldata)

	// Request TSS signature using the callTssSign function
	up.callTssSign(sessionID, hash)
	up.logger.Infof("TSS signing requested for batch %s with session ID: %s", batchID, sessionID)

	return nil
}

// sendProposalToP2P sends the batch proposal to other P2P nodes
func (up *UtxoProcessor) sendProposalToP2P(proposal *DepositProposal) error {
	p2pModule, ok := module.GetModule((&p2p.P2PModule{}).Name())
	if !ok {
		return fmt.Errorf("p2p module not found")
	}

	payload, err := proposal.MarshalJSON()
	if err != nil {
		return fmt.Errorf("failed to marshal proposal: %v", err)
	}

	return p2pModule.(p2p.P2PSender).BroadcastP2PMessage(types.P2PBroadcastMessage{
		Type:      types.P2PMessageTypeDepositProposal,
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

// isCurrentProposer checks if this node is the current proposer by querying the contract
func (up *UtxoProcessor) isCurrentProposer() (bool, error) {
	// Get this node's Ethereum address
	nodeAddress, err := up.getNodeEthereumAddress()
	if err != nil {
		return false, fmt.Errorf("failed to get node address: %w", err)
	}

	// TODO: Query the contract for the current proposer
	// This is where you would make a contract call to get the current proposer address
	// Example:
	// currentProposer, err := up.getCurrentProposerFromContract()
	// if err != nil {
	//     return false, err
	// }
	// return nodeAddress == currentProposer, nil

	up.logger.Debugf("Node address: %s - ready for proposer checking", nodeAddress.Hex())

	// For now, return true as placeholder
	return true, nil
}
