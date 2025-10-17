package consensus

import (
	"bytes"
	"fmt"
	"math/big"
	"strconv"
	"strings"
	"time"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/crypto"
	"github.com/goat-network/dogecoin-relayer/internal/models"
	"github.com/goat-network/dogecoin-relayer/internal/p2p"
	"github.com/goat-network/dogecoin-relayer/pkg/contract"
	"github.com/goat-network/dogecoin-relayer/pkg/eventbus"
	"github.com/goat-network/dogecoin-relayer/pkg/module"
	"github.com/goat-network/dogecoin-relayer/pkg/types"
)

func (up *UtxoProcessor) registerP2PHandler() {
	go func() {
		// Wait for P2P network to be fully initialized
		up.logger.Infof("Waiting for P2P network initialization...")
		time.Sleep(30 * time.Second) // Wait for network to be fully ready

		// Retry registration until P2P module is available and network is ready
		maxRetries := 30
		for i := 0; i < maxRetries; i++ {
			p2pModule, ok := module.GetModule((&p2p.P2PModule{}).Name())
			if !ok {
				up.logger.Debugf("P2P module not found, retrying in 2 seconds... (%d/%d)", i+1, maxRetries)
				time.Sleep(2 * time.Second)
				continue
			}

			// Check if network is initialized
			network := p2pModule.(*p2p.P2PModule).GetNetwork()
			if network == nil {
				up.logger.Debugf("P2P network not initialized, retrying in 2 seconds... (%d/%d)", i+1, maxRetries)
				time.Sleep(2 * time.Second)
				continue
			}

			// register a handler for deposit proposal message
			err1 := p2pModule.(p2p.P2PSender).RegisterP2PHandler(types.P2PMessageTypeDepositProposal, func(msg *types.P2PBroadcastMessage) error {
				up.logger.Infof("🎯 Received P2P deposit proposal message for session %s from proposer", msg.SessionID)
				return up.handleP2PDepositProposal(msg)
			})

			// register a handler for bridge out (withdrawal) message
			err2 := p2pModule.(p2p.P2PSender).RegisterP2PHandler(types.P2PMessageTypeBridgeOut, func(msg *types.P2PBroadcastMessage) error {
				up.logger.Infof("🎯 Received P2P withdrawal proposal message for session %s", msg.SessionID)
				return up.handleP2PWithdrawalProposal(msg)
			})

			if err1 != nil || err2 != nil {
				up.logger.Errorf("Failed to register handlers: deposit=%v, withdrawal=%v", err1, err2)
				time.Sleep(2 * time.Second)
				continue
			}

			up.logger.Infof("✅ P2P handlers registered successfully for UTXO processor")
			return
		}

		up.logger.Errorf("❌ Failed to register P2P handlers after %d retries", maxRetries)
	}()
}

// handleTssSignature handles TSS signature responses from the event bus
func (up *UtxoProcessor) handleTssSignature(data any) {
	resp, ok := data.(types.TssSigResponse)
	if !ok {
		up.logger.Errorf("Invalid TSS signature response data type: %T", data)
		return
	}

	key, pending, exists := up.loadPendingForSession(resp.SessionID)
	if !exists {
		up.logger.Warnf("No pending batch found for TSS session %s", resp.SessionID)
		return
	}

	baseSessionID := pending.baseSessionID
	if baseSessionID == "" {
		baseSessionID = key
	}

	up.logger.Infof("Received TSS signature response for session %s (base session %s), success: %v", resp.SessionID, baseSessionID, resp.Success)

	if resp.Success {
		up.logger.Infof("TSS signature successful for session %s", resp.SessionID)
		// Remove from pending batches on success
		up.pendingBatches.Delete(key)
		up.tssSessionAliases.Delete(resp.SessionID)

		// Continue with transaction submission
		err := up.completeBatchWithSignature(pending, resp.RawSig)
		if err != nil {
			up.logger.Errorf("Failed to complete batch with signature: %v", err)
		}
	} else {
		up.logger.Errorf("TSS signing failed for session %s: %s", resp.SessionID, resp.Message)

		// For timeout errors, keep the batch for retry; for other errors, remove it
		if strings.Contains(resp.Message, "timeout") || strings.Contains(resp.Message, "timed out") {
			now := time.Now()
			retryDelay := up.tssRequestRetryBackoff
			if retryDelay <= 0 {
				retryDelay = 15 * time.Second
			}
			pending.lastAttempt = now
			pending.nextRetryAt = now.Add(retryDelay)
			up.pendingBatches.Store(key, pending)
			up.logger.Infof("Keeping batch %s for retry due to timeout", resp.SessionID)
		} else {
			// Remove from pending batches for non-timeout errors
			up.pendingBatches.Delete(key)
			up.logger.Infof("Removed batch %s due to non-timeout error", baseSessionID)
		}
		up.tssSessionAliases.Delete(resp.SessionID)

		// Handle failure
		up.handleBatchSigningFailure(pending, resp.Message)
	}
}

func (up *UtxoProcessor) callTssSign(sessionID string, calldata []byte) string {
	up.eventBus.Publish(eventbus.EventTssSigRequest, types.TssSigRequest{
		SessionID:  sessionID,
		UnsignHash: calldata,
	})
	up.logger.Debugf("Sent TSS sign request for session %s", sessionID)
	return sessionID
}

func (up *UtxoProcessor) loadPendingForSession(sessionID string) (string, *pendingBatch, bool) {
	if data, ok := up.pendingBatches.Load(sessionID); ok {
		return sessionID, data.(*pendingBatch), true
	}

	if alias, ok := up.tssSessionAliases.Load(sessionID); ok {
		base, ok := alias.(string)
		if ok {
			if data, ok := up.pendingBatches.Load(base); ok {
				return base, data.(*pendingBatch), true
			}
		}
	}

	if base, ok := stripRetrySuffix(sessionID); ok {
		if data, ok := up.pendingBatches.Load(base); ok {
			return base, data.(*pendingBatch), true
		}
	}

	return "", nil, false
}

func stripRetrySuffix(sessionID string) (string, bool) {
	idx := strings.LastIndex(sessionID, "-")
	if idx == -1 || idx+1 >= len(sessionID) {
		return sessionID, false
	}

	suffix := sessionID[idx+1:]
	if _, err := strconv.Atoi(suffix); err != nil {
		return sessionID, false
	}

	return sessionID[:idx], true
}

func (up *UtxoProcessor) handleP2PDepositProposal(msg *types.P2PBroadcastMessage) error {
	proposal := &DepositProposal{}
	err := proposal.UnmarshalJSON(msg.Payload)
	if err != nil {
		return fmt.Errorf("failed to unmarshal proposal: %v", err)
	}
	up.logger.Infof("Received deposit proposal for batch %s from proposer %s", proposal.BatchID, proposal.Proposer)

	// Don't convert to full UTXOs for validation - use lightweight UTXOs directly
	// This avoids loading heavy database objects into the proposal

	// Parse total amount from string for internal use
	totalAmount, ok := new(big.Int).SetString(proposal.TotalAmountStr, 10)
	if !ok {
		return fmt.Errorf("invalid total amount format: %s", proposal.TotalAmountStr)
	}
	proposal.TotalAmount = totalAmount

	// Validate the proposal
	if err := up.validateDepositProposal(proposal); err != nil {
		up.logger.Errorf("Invalid deposit proposal: %v", err)
		return fmt.Errorf("invalid deposit proposal: %w", err)
	}

	// Convert lightweight UTXOs to full UTXOs only when needed for processing
	fullUTXOs := make([]*models.UTXO, len(proposal.LightweightUTXOs))
	for i, lightUTXO := range proposal.LightweightUTXOs {
		var utxo models.UTXO
		err := up.conn.GetDB().Where("uid = ?", lightUTXO.Uid).First(&utxo).Error
		if err != nil {
			return fmt.Errorf("failed to find UTXO %s: %w", lightUTXO.Uid, err)
		}
		fullUTXOs[i] = &utxo
	}

	// Create pending batch entry for tracking
	txParams := make([]contract.BridgeTransaction, len(fullUTXOs))
	for i, utxo := range fullUTXOs {
		txParams[i] = contract.BridgeTransaction{
			DestEvmAddress: common.HexToAddress(utxo.EvmAddr),
			Amount:         big.NewInt(utxo.Amount),
			TxBytes:        []byte(utxo.Txid),
		}
	}

	batchID := new(big.Int)
	derivedID, err := deriveDeterministicIDFromSession(proposal.SessionID)
	if err != nil {
		up.logger.Errorf("Failed to derive deterministic batch ID for session %s: %v", proposal.SessionID, err)
		return fmt.Errorf("failed to derive batch ID: %w", err)
	}
	batchID = derivedID
	up.logger.Infof("Handler: derived batch ID %s from session %s", batchID.String(), proposal.SessionID)

	batch := &BridgeInBatch{
		ID:                batchID,
		UTXOs:             fullUTXOs,
		TotalAmount:       totalAmount,
		TransactionParams: txParams,
	}

	// Generate calldata locally since it's not sent via P2P
	calldata, err := up.generateBridgeInCalldata(batch)
	if err != nil {
		return fmt.Errorf("failed to generate calldata for received proposal: %w", err)
	}

	baseSessionID := proposal.SessionID
	if base, ok := stripRetrySuffix(proposal.SessionID); ok {
		baseSessionID = base
	}

	var pending *pendingBatch
	var key string
	if existingKey, existing, ok := up.loadPendingForSession(baseSessionID); ok {
		pending = existing
		key = existingKey
	} else {
		pending = &pendingBatch{
			batchType:     "deposit",
			baseSessionID: baseSessionID,
		}
		key = baseSessionID
	}

	pending.depositBatch = batch
	pending.withdrawalRequest = nil
	pending.calldata = calldata
	pending.utxos = fullUTXOs
	up.registerExistingTssSession(pending, proposal.SessionID)
	up.pendingBatches.Store(key, pending)

	// Create hash to sign for verifyAndCall function
	hash := crypto.Keccak256(calldata)

	// Request TSS signature for the proposal
	selfAddress, addrErr := up.getNodeEthereumAddress()
	if addrErr != nil {
		return fmt.Errorf("failed to get node address: %w", addrErr)
	}

	if strings.EqualFold(proposal.Proposer, selfAddress.Hex()) {
		up.logger.Debugf("Skipping TSS sign initiation for proposer node on session %s", proposal.SessionID)
		return nil
	}

	tssSessionID := pending.currentSessionID
	up.callTssSign(tssSessionID, hash)
	up.logger.Infof("Joined TSS signing for proposal batch %s with session %s (base session %s)",
		proposal.BatchID, tssSessionID, pending.baseSessionID)

	return nil
}

func (up *UtxoProcessor) handleP2PWithdrawalProposal(msg *types.P2PBroadcastMessage) error {
	proposal := &WithdrawalProposal{}
	err := proposal.UnmarshalJSON(msg.Payload)
	if err != nil {
		return fmt.Errorf("failed to unmarshal withdrawal proposal: %v", err)
	}
	up.logger.Infof("Received withdrawal proposal for batch %s from proposer %s", proposal.BatchID, proposal.Proposer)

	// Validate the withdrawal proposal
	if err := up.validateWithdrawalProposal(proposal); err != nil {
		up.logger.Errorf("Invalid withdrawal proposal: %v", err)
		return fmt.Errorf("invalid withdrawal proposal: %w", err)
	}

	// Create pending request entry for tracking
	request := &withdrawalRequest{
		ID:          big.NewInt(0),     // Will be set based on request ID
		UTXO:        proposal.UTXOs[0], // Single UTXO from proposal
		TotalAmount: proposal.TotalAmount,
		TaskIds:     proposal.TaskIds,
	}

	derivedID, err := deriveDeterministicIDFromSession(proposal.SessionID)
	if err != nil {
		up.logger.Errorf("Failed to derive deterministic withdrawal ID for session %s: %v", proposal.SessionID, err)
		return fmt.Errorf("failed to derive withdrawal ID: %w", err)
	}
	request.ID = derivedID
	up.logger.Infof("Handler: derived withdrawal ID %s from session %s", request.ID.String(), proposal.SessionID)

	calldata, err := up.generateBridgeOutFinishCalldata(request)
	if err != nil {
		return fmt.Errorf("failed to regenerate withdrawal calldata: %w", err)
	}

	if len(proposal.Calldata) > 0 && !bytes.Equal(proposal.Calldata, calldata) {
		return fmt.Errorf("withdrawal proposal calldata mismatch for session %s", proposal.SessionID)
	}

	baseSessionID := proposal.SessionID
	if base, ok := stripRetrySuffix(proposal.SessionID); ok {
		baseSessionID = base
	}

	var pending *pendingBatch
	var key string
	if existingKey, existing, ok := up.loadPendingForSession(baseSessionID); ok {
		pending = existing
		key = existingKey
	} else {
		pending = &pendingBatch{
			batchType:     "withdrawal",
			baseSessionID: baseSessionID,
		}
		key = baseSessionID
	}

	pending.depositBatch = nil
	pending.withdrawalRequest = request
	pending.calldata = calldata
	pending.utxos = proposal.UTXOs
	up.registerExistingTssSession(pending, proposal.SessionID)
	up.pendingBatches.Store(key, pending)

	// Create hash to sign for verifyAndCall function
	hash := crypto.Keccak256(calldata)

	// Request TSS signature for the proposal
	selfAddress, addrErr := up.getNodeEthereumAddress()
	if addrErr != nil {
		return fmt.Errorf("failed to get node address: %w", addrErr)
	}

	if strings.EqualFold(proposal.Proposer, selfAddress.Hex()) {
		up.logger.Debugf("Skipping TSS sign initiation for proposer node on withdrawal session %s", proposal.SessionID)
		return nil
	}

	tssSessionID := pending.currentSessionID
	up.callTssSign(tssSessionID, hash)
	up.logger.Infof("Joined TSS signing for withdrawal batch %s with session %s (base session %s)",
		proposal.BatchID, tssSessionID, pending.baseSessionID)

	return nil
}

// validateDepositProposal validates a received deposit proposal
func (up *UtxoProcessor) validateDepositProposal(proposal *DepositProposal) error {
	// Basic validation checks
	if proposal.BatchID == "" {
		return fmt.Errorf("batch ID is required")
	}
	if proposal.SessionID == "" {
		return fmt.Errorf("session ID is required")
	}
	if len(proposal.LightweightUTXOs) == 0 {
		return fmt.Errorf("UTXOs list cannot be empty")
	}
	// Parse total amount from string
	totalAmount, ok := new(big.Int).SetString(proposal.TotalAmountStr, 10)
	if !ok || totalAmount.Cmp(big.NewInt(0)) <= 0 {
		return fmt.Errorf("total amount must be a positive number, got: %s", proposal.TotalAmountStr)
	}
	if proposal.Proposer == "" {
		return fmt.Errorf("proposer address is required")
	}

	// Calculate total amount from lightweight UTXOs and verify it matches proposal
	calculatedTotal := big.NewInt(0)
	for _, lightUTXO := range proposal.LightweightUTXOs {
		if lightUTXO.Amount <= 0 {
			return fmt.Errorf("UTXO amount must be positive")
		}
		calculatedTotal.Add(calculatedTotal, big.NewInt(lightUTXO.Amount))
	}

	if calculatedTotal.Cmp(totalAmount) != 0 {
		return fmt.Errorf("calculated total amount (%s) does not match proposal total amount (%s)",
			calculatedTotal.String(), totalAmount.String())
	}

	// TODO: Add more sophisticated validation:
	// - Verify UTXOs exist and are unspent
	// - Validate calldata structure
	// - Check proposer authorization
	// - Verify session ID uniqueness

	up.logger.Debugf("Proposal validation passed for batch %s", proposal.BatchID)
	return nil
}

// validateWithdrawalProposal validates a received withdrawal proposal
func (up *UtxoProcessor) validateWithdrawalProposal(proposal *WithdrawalProposal) error {
	// Basic validation checks
	if proposal.BatchID == "" {
		return fmt.Errorf("batch ID is required")
	}
	if proposal.SessionID == "" {
		return fmt.Errorf("session ID is required")
	}
	if len(proposal.UTXOs) == 0 {
		return fmt.Errorf("UTXOs list cannot be empty")
	}
	if proposal.TotalAmount == nil || proposal.TotalAmount.Cmp(big.NewInt(0)) <= 0 {
		return fmt.Errorf("total amount must be positive")
	}
	if len(proposal.Calldata) == 0 {
		return fmt.Errorf("calldata cannot be empty")
	}
	if proposal.Proposer == "" {
		return fmt.Errorf("proposer address is required")
	}
	if len(proposal.TaskIds) == 0 {
		return fmt.Errorf("task IDs are required for withdrawals")
	}

	// Calculate total amount from UTXOs and verify it matches proposal
	calculatedTotal := big.NewInt(0)
	for _, utxo := range proposal.UTXOs {
		if utxo.Amount <= 0 {
			return fmt.Errorf("UTXO amount must be positive")
		}
		calculatedTotal.Add(calculatedTotal, big.NewInt(utxo.Amount))
	}

	if calculatedTotal.Cmp(proposal.TotalAmount) != 0 {
		return fmt.Errorf("calculated total amount (%s) does not match proposal total amount (%s)",
			calculatedTotal.String(), proposal.TotalAmount.String())
	}

	// TODO: Add more sophisticated validation:
	// - Verify UTXOs exist and are unspent
	// - Validate calldata structure matches bridgeOutFinish
	// - Check proposer authorization
	// - Verify session ID uniqueness
	// - Validate task IDs correspond to withdrawal requests

	up.logger.Debugf("Withdrawal proposal validation passed for batch %s", proposal.BatchID)
	return nil
}

// completeBatchWithSignature completes batch processing after receiving TSS signature
func (up *UtxoProcessor) completeBatchWithSignature(pending *pendingBatch, signature []byte) error {
	var batchID string
	switch pending.batchType {
	case "deposit":
		if pending.depositBatch != nil {
			batchID = pending.depositBatch.ID.String()
		} else {
			return fmt.Errorf("deposit batch is nil")
		}
	case "withdrawal":
		if pending.withdrawalRequest != nil {
			batchID = pending.withdrawalRequest.ID.String()
		} else {
			return fmt.Errorf("withdrawal request is nil")
		}
	default:
		return fmt.Errorf("unknown batch type: %s", pending.batchType)
	}

	up.logger.Infof("Completing %s batch %s with signature", pending.batchType, batchID)

	// Send the calldata to the bridge contract
	txHash, err := up.sendCalldataToBridge(pending.calldata, signature)
	if err != nil {
		return fmt.Errorf("failed to send calldata to bridge: %w", err)
	}

	up.logger.Infof("Bridge transaction sent successfully. TxHash: %s", txHash.Hex())

	// Mark UTXOs as processed
	err = up.markUTXOsAsProcessed(pending.utxos)
	if err != nil {
		up.logger.Errorf("Failed to mark UTXOs as processed: %v", err)
		return err
	}

	return nil
}

// sendCalldataToBridge sends the signed calldata to the bridge contract using the node's private key
func (up *UtxoProcessor) sendCalldataToBridge(calldata []byte, signature []byte) (common.Hash, error) {
	if up.chainID == nil {
		return common.Hash{}, fmt.Errorf("chain ID not set")
	}

	// Get P2P module to access the private key
	p2pModule, ok := module.GetModule((&p2p.P2PModule{}).Name())
	if !ok {
		return common.Hash{}, fmt.Errorf("p2p module not found")
	}

	// Get the node's ECDSA private key from P2P network
	network := p2pModule.(*p2p.P2PModule).GetNetwork()
	privateKey, err := network.GetNodeECDSAPrivateKey()
	if err != nil {
		return common.Hash{}, fmt.Errorf("failed to get node private key: %w", err)
	}

	// For verifyAndCall, we need to construct the full transaction data
	// This typically involves encoding targets, calldata array, and signature
	targets := []common.Address{up.bridgeContract}
	calldataArray := [][]byte{calldata}

	// Generate the verifyAndCall transaction data
	verifyAndCallData, err := up.contractBuilder.GenerateVerifyAndCallTxData(targets, calldataArray, signature)
	if err != nil {
		return common.Hash{}, fmt.Errorf("failed to generate verifyAndCall transaction data: %w", err)
	}

	// Send the transaction using SendTx from consensus.go
	// The transaction value is 0 since we're just calling a contract function
	tx, err := SendTx(up.ctx, privateKey, up.chainID, &up.bridgeContract, big.NewInt(0), verifyAndCallData)
	if err != nil {
		return common.Hash{}, fmt.Errorf("failed to send transaction: %w", err)
	}

	up.logger.Infof("Bridge transaction sent successfully. TxHash: %s, From: %s",
		tx.Hash().Hex(), crypto.PubkeyToAddress(privateKey.PublicKey).Hex())

	return tx.Hash(), nil
}

// handleBatchSigningFailure handles TSS signing failures
func (up *UtxoProcessor) handleBatchSigningFailure(pending *pendingBatch, errorMessage string) {
	var batchID string
	switch pending.batchType {
	case "deposit":
		if pending.depositBatch != nil {
			batchID = pending.depositBatch.ID.String()
		} else {
			batchID = "unknown-deposit-batch"
		}
	case "withdrawal":
		if pending.withdrawalRequest != nil {
			batchID = pending.withdrawalRequest.ID.String()
		} else {
			batchID = "unknown-withdrawal-request"
		}
	default:
		batchID = "unknown-batch-type"
	}

	up.logger.Errorf("Batch %s signing failed: %s", batchID, errorMessage)
	// For now, just log the failure. In production, you might want to:
	// - Retry the signing process
	// - Alert operators
	// - Store failure information for analysis
}
