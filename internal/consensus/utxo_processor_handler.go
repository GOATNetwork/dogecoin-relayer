package consensus

import (
	"fmt"
	"math/big"
	"time"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/crypto"
	"github.com/goat-network/dogecoin-relayer/internal/p2p"
	"github.com/goat-network/dogecoin-relayer/pkg/eventbus"
	"github.com/goat-network/dogecoin-relayer/pkg/global"
	"github.com/goat-network/dogecoin-relayer/pkg/module"
	"github.com/goat-network/dogecoin-relayer/pkg/types"
)

func (up *UtxoProcessor) registerP2PHandler() {
	go func() {
		p2pConfig := global.GetConfig().P2P
		time.Sleep(time.Duration(p2pConfig.ListenWaitSeconds+p2pConfig.ConnectionWaitSeconds+15) * time.Second)

		p2pModule, ok := module.GetModule((&p2p.P2PModule{}).Name())
		if !ok {
			panic(fmt.Errorf("p2p module not found"))
		}
		// register a handler for bridge in message
		p2pModule.(p2p.P2PSender).RegisterP2PHandler(types.P2PMessageTypeBridgeIn, func(msg *types.P2PBroadcastMessage) error {
			return up.handleP2PDepositProposal(msg)
		})
		// register a handler for bridge out (withdrawal) message
		p2pModule.(p2p.P2PSender).RegisterP2PHandler(types.P2PMessageTypeBridgeOut, func(msg *types.P2PBroadcastMessage) error {
			return up.handleP2PWithdrawalProposal(msg)
		})
	}()
}

// handleTssSignature handles TSS signature responses from the event bus
func (up *UtxoProcessor) handleTssSignature(data any) {
	resp, ok := data.(types.TssSigResponse)
	if !ok {
		up.logger.Errorf("Invalid TSS signature response data type: %T", data)
		return
	}

	up.logger.Infof("Received TSS signature response for session %s, success: %v", resp.SessionID, resp.Success)

	// Retrieve the pending batch
	pendingData, exists := up.pendingBatches.LoadAndDelete(resp.SessionID)
	if !exists {
		up.logger.Warnf("No pending batch found for session %s", resp.SessionID)
		return
	}

	pending := pendingData.(*pendingBatch)

	if resp.Success {
		up.logger.Infof("TSS signature successful for session %s", resp.SessionID)
		// Continue with transaction submission
		err := up.completeBatchWithSignature(pending, resp.RawSig)
		if err != nil {
			up.logger.Errorf("Failed to complete batch with signature: %v", err)
		}
	} else {
		up.logger.Errorf("TSS signing failed for session %s: %s", resp.SessionID, resp.Message)
		// Handle failure - could implement retry logic here
		up.handleBatchSigningFailure(pending, resp.Message)
	}
}

func (up *UtxoProcessor) callTssSign(sessionID string, calldata []byte) {
	up.eventBus.Publish(eventbus.EventTssSigRequest, types.TssSigRequest{
		SessionID:  sessionID,
		UnsignHash: calldata,
	})
	up.logger.Debugf("Sent TSS sign request for session %s", sessionID)
}

func (up *UtxoProcessor) handleP2PDepositProposal(msg *types.P2PBroadcastMessage) error {
	proposal := &DepositProposal{}
	err := proposal.UnmarshalJSON(msg.Payload)
	if err != nil {
		return fmt.Errorf("failed to unmarshal proposal: %v", err)
	}
	up.logger.Infof("Received deposit proposal for batch %s from proposer %s", proposal.BatchID, proposal.Proposer)

	// Validate the proposal
	if err := up.validateDepositProposal(proposal); err != nil {
		up.logger.Errorf("Invalid deposit proposal: %v", err)
		return fmt.Errorf("invalid deposit proposal: %w", err)
	}

	// Create pending batch entry for tracking
	batch := &bridgeInBatch{
		ID:                big.NewInt(0), // Will be set based on batch ID
		UTXOs:             proposal.UTXOs,
		TotalAmount:       proposal.TotalAmount,
		TransactionParams: nil, // Will be constructed from UTXOs if needed
	}

	pending := &pendingBatch{
		batchType:    "deposit",
		depositBatch: batch,
		calldata:     proposal.Calldata,
		utxos:        proposal.UTXOs,
	}

	// Store the pending batch for when signature comes back
	up.pendingBatches.Store(proposal.SessionID, pending)

	// Create hash to sign for verifyAndCall function
	hash := crypto.Keccak256(proposal.Calldata)

	// Request TSS signature for the proposal
	up.callTssSign(proposal.SessionID, hash)

	up.logger.Infof("TSS signature requested for received proposal batch %s with session ID: %s",
		proposal.BatchID, proposal.SessionID)

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

	pending := &pendingBatch{
		batchType:         "withdrawal",
		withdrawalRequest: request,
		calldata:          proposal.Calldata,
		utxos:             proposal.UTXOs,
	}

	// Store the pending batch for when signature comes back
	up.pendingBatches.Store(proposal.SessionID, pending)

	// Create hash to sign for verifyAndCall function
	hash := crypto.Keccak256(proposal.Calldata)

	// Request TSS signature for the proposal
	up.callTssSign(proposal.SessionID, hash)

	up.logger.Infof("TSS signature requested for received withdrawal proposal batch %s with session ID: %s",
		proposal.BatchID, proposal.SessionID)

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
