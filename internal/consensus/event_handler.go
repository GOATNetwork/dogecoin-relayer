package consensus

import (
	"encoding/json"
	"fmt"
	"math/big"
	"reflect"

	"github.com/ethereum/go-ethereum/common"
	"github.com/goat-network/dogecoin-relayer/internal/models"
	"github.com/goat-network/dogecoin-relayer/pkg/eventbus"
	"github.com/goat-network/dogecoin-relayer/pkg/global"
	"github.com/goat-network/dogecoin-relayer/pkg/types"
	log "github.com/sirupsen/logrus"
)

// EventHandler manages event detection and processing
type EventHandler struct {
	eventBus  *eventbus.Bus
	eventRepo *models.EventRepository
	logger    *log.Entry
}

// NewEventHandler creates a new event handler
func NewEventHandler(eventRepo *models.EventRepository) *EventHandler {
	return &EventHandler{
		eventBus:  global.GetEventBus(),
		eventRepo: eventRepo,
		logger:    log.WithField("component", "EventHandler"),
	}
}

// Start begins event processing with the provided event channel
func (eh *EventHandler) Start(eventChannel <-chan BlockchainEvent) error {
	// Start processing events
	go eh.processEvents(eventChannel)

	eh.logger.Info("Event handler started")
	return nil
}

// processEvents processes detected events from the provided channel
func (eh *EventHandler) processEvents(eventChannel <-chan BlockchainEvent) {
	for event := range eventChannel {
		if err := eh.ProcessEvent(event); err != nil {
			eh.logger.Errorf("Failed to process event %s: %v", event.EventName, err)
		}
	}
}

// ProcessEvent processes a blockchain event based on its type
func (eh *EventHandler) ProcessEvent(event BlockchainEvent) error {
	eh.logger.Debugf("Processing event: %s at block %d", event.EventName, event.BlockNumber)

	var processingErr error
	switch event.EventName {
	case types.EventNameBridgeIn:
		processingErr = eh.processBridgeIn(event)
	case types.EventNameBridgeOutProposed:
		processingErr = eh.processBridgeOutProposed(event)
	case types.EventNameBridgeOutFinished:
		processingErr = eh.processBridgeOutFinished(event)
	case types.EventNameSubmitterChosen:
		processingErr = eh.processSubmitterChosen(event)
	case types.EventNameAddProposerReq:
		processingErr = eh.processAddProposerRequested(event)
	case types.EventNameRemoveProposerReq:
		processingErr = eh.processRemoveProposerRequested(event)
	case types.EventNameProposerConfirmed:
		processingErr = eh.processProposerConfirmed(event)
	default:
		eh.logger.Warnf("Unknown event type: %s", event.EventName)
		processingErr = fmt.Errorf("unknown event type: %s", event.EventName)
	}

	if processingErr != nil {
		eh.logger.Errorf("Failed to process %s event: %v", event.EventName, processingErr)
	}

	return processingErr
}

// processBridgeIn handles BridgeIn events
// Creates or updates Deposit records when bridge in is detected from deposit UTXO,
// updates status and EVM fields when the final bridgeIn event is detected.
func (eh *EventHandler) processBridgeIn(event BlockchainEvent) error {
	logger := eh.logger.WithField("event", "BridgeIn")

	// Convert event data to JSON for logging
	eventDataJSON, err := json.MarshalIndent(event.EventData, "", "  ")
	if err != nil {
		return fmt.Errorf("failed to marshal event data: %w", err)
	}

	logger.Infof("BridgeIn event detected:\nContract: %s\nTx: %s\nBlock: %d\nData:\n%s",
		event.ContractAddress.Hex(),
		event.TxHash.Hex(),
		event.BlockNumber,
		string(eventDataJSON))

	// Extract txHash (bytes32) from event data and convert to 0x-hex string
	rawTxHash, ok := event.EventData["txHash"]
	if !ok {
		return fmt.Errorf("missing txHash in BridgeIn event data")
	}

	txIdHex, err := toHex32(rawTxHash)
	if err != nil {
		return fmt.Errorf("failed to parse BridgeIn.txHash: %w", err)
	}

	// Update all deposits with the matching TxId (vout not available in event)
	tx := eh.eventRepo.BeginTransaction()
	var matchingDeposits []models.Deposit
	if err := tx.Where("tx_id = ?", txIdHex).Find(&matchingDeposits).Error; err != nil {
		tx.Rollback()
		return fmt.Errorf("query deposits by tx_id failed: %w", err)
	}

	if len(matchingDeposits) == 0 {
		tx.Rollback()
		return fmt.Errorf("no deposit found for txId=%s; ensure UTXO detection created it earlier", txIdHex)
	}
	if len(matchingDeposits) != 1 {
		tx.Rollback()
		return fmt.Errorf("expected exactly 1 deposit for txId=%s, got %d", txIdHex, len(matchingDeposits))
	}

	dep := matchingDeposits[0]
	if err := eh.eventRepo.UpdateDepositStatus(tx, dep.ID, "confirmed"); err != nil {
		tx.Rollback()
		return fmt.Errorf("failed updating deposit status: %w", err)
	}
	// Also set EVM fields
	dep.EvmTxHash = event.TxHash.Hex()
	dep.EvmBlock = event.BlockNumber
	dep.EvmLogIndex = event.LogIndex
	if err := tx.Model(&models.Deposit{}).Where("id = ?", dep.ID).Updates(map[string]any{
		"evm_tx_hash":   dep.EvmTxHash,
		"evm_block":     dep.EvmBlock,
		"evm_log_index": dep.EvmLogIndex,
	}).Error; err != nil {
		tx.Rollback()
		return fmt.Errorf("failed updating deposit EVM fields: %w", err)
	}
	logger.Infof("Updated deposit %d for txId=%s to confirmed with EVM fields", dep.ID, txIdHex)

	if err := tx.Commit().Error; err != nil {
		tx.Rollback()
		return fmt.Errorf("failed to commit transaction: %w", err)
	}
	return nil
}

// processBridgeOutProposed handles BridgeOutProposed events
func (eh *EventHandler) processBridgeOutProposed(event BlockchainEvent) error {
	logger := eh.logger.WithField("event", "BridgeOutProposed")

	logger.Infof("BridgeOutProposed event: Contract=%s, Tx=%s, Block=%d",
		event.ContractAddress.Hex(), event.TxHash.Hex(), event.BlockNumber)

	// Extract required fields from event data
	// taskId: uint256 -> decimal string
	rawTaskId, ok := event.EventData["taskId"]
	if !ok {
		return fmt.Errorf("missing taskId in BridgeOutProposed event data")
	}
	taskId, err := toUint256String(rawTaskId)
	if err != nil {
		return fmt.Errorf("failed to parse BridgeOutProposed.taskId: %w", err)
	}

	// Create withdrawal record with explicit transaction
	tx := eh.eventRepo.BeginTransaction()
	withdrawal := &models.Withdrawal{
		ReqTaskId:   taskId,
		ReqTxHash:   event.TxHash.Hex(),
		ReqBlock:    event.BlockNumber,
		ReqLogIndex: event.LogIndex,
		Status:      "init", // Initial status when BridgeOutProposed is detected
	}

	if err := eh.eventRepo.CreateOrUpdateWithdrawal(tx, withdrawal); err != nil {
		tx.Rollback()
		return fmt.Errorf("failed to create/update withdrawal: %w", err)
	}

	if err := tx.Commit().Error; err != nil {
		tx.Rollback()
		return fmt.Errorf("failed to commit transaction: %w", err)
	}

	logger.Infof("Created/updated withdrawal record for taskId=%s", taskId)

	// Publish to event bus for other modules (like UTXO processor)
	eh.eventBus.Publish(eventbus.EventBridgeOutProposed, event)
	return nil
}

// processBridgeOutFinished handles BridgeOutFinished events
func (eh *EventHandler) processBridgeOutFinished(event BlockchainEvent) error {
	logger := eh.logger.WithField("event", "BridgeOutFinished")

	logger.Infof("BridgeOutFinished event detected: Tx %s at block %d",
		event.TxHash.Hex(), event.BlockNumber)

	// Extract taskIds: uint256[]
	rawTaskIds, ok := event.EventData["taskIds"]
	if !ok {
		return fmt.Errorf("missing taskIds in BridgeOutFinished event data")
	}
	taskIds, err := toUint256StringSlice(rawTaskIds)
	if err != nil {
		return fmt.Errorf("failed to parse BridgeOutFinished.taskIds: %w", err)
	}

	// Update withdrawal to final state with explicit transaction
	tx := eh.eventRepo.BeginTransaction()
	for _, taskId := range taskIds {
		if err := eh.eventRepo.SetWithdrawalFinishInfo(tx, taskId,
			event.TxHash.Hex(), event.BlockNumber, event.LogIndex); err != nil {
			tx.Rollback()
			return fmt.Errorf("failed to set withdrawal finish info (taskId=%s): %w", taskId, err)
		}

		withdrawal, err := eh.eventRepo.GetWithdrawalByTask(tx, taskId)
		if err != nil {
			tx.Rollback()
			return fmt.Errorf("failed to get withdrawal (taskId=%s): %w", taskId, err)
		}
		if err := eh.eventRepo.UpdateWithdrawalStatus(tx, withdrawal.ID, "confirmed"); err != nil {
			tx.Rollback()
			return fmt.Errorf("failed to update withdrawal status (taskId=%s): %w", taskId, err)
		}
		logger.Infof("Updated withdrawal %s to confirmed state", taskId)
	}

	if err := tx.Commit().Error; err != nil {
		tx.Rollback()
		return fmt.Errorf("failed to commit transaction: %w", err)
	}

	// Publish to event bus
	eh.eventBus.Publish(eventbus.EventBridgeOutFinished, event)
	return nil
}

// processSubmitterChosen handles SubmitterChosen events
func (eh *EventHandler) processSubmitterChosen(event BlockchainEvent) error {
	// TODO: handle by proposer manager
	logger := eh.logger.WithField("event", "SubmitterChosen")

	logger.Infof("SubmitterChosen event detected: Tx %s at block %d",
		event.TxHash.Hex(), event.BlockNumber)

	// Extract the chosen submitter address from event data
	var submitterAddr string
	if event.EventData != nil {
		// Log the event data for debugging
		eventDataJSON, err := json.MarshalIndent(event.EventData, "", "  ")
		if err == nil {
			logger.Debugf("SubmitterChosen event data:\n%s", string(eventDataJSON))
		}

		// Try to extract submitter address from common field names
		if addr, err := readAddress(event.EventData["newSubmitter"]); err == nil {
			submitterAddr = addr
		} else {
			// Log available fields for debugging
			var fields []string
			for key := range event.EventData {
				fields = append(fields, key)
			}
			logger.Warnf("Could not find submitter address in event data. Available fields: %v", fields)
		}
	} else {
		logger.Warn("SubmitterChosen event has no data")
	}

	if submitterAddr != "" {
		logger.Infof("New submitter chosen: %s", submitterAddr)

		// Update proposer record with explicit transaction
		tx := eh.eventRepo.BeginTransaction()
		proposer := &models.Proposers{
			Address:   submitterAddr,
			Status:    "ok", // Active submitter
			JoinBlock: event.BlockNumber,
		}

		if err := eh.eventRepo.CreateOrUpdateProposer(tx, proposer); err != nil {
			tx.Rollback()
			return fmt.Errorf("failed to create/update proposer: %w", err)
		}

		if err := tx.Commit().Error; err != nil {
			tx.Rollback()
			return fmt.Errorf("failed to commit transaction: %w", err)
		}

		logger.Infof("Updated proposer record for address=%s", submitterAddr)

		// Publish to event bus for UTXO processor to update current proposer
		eh.eventBus.Publish(eventbus.EventSubmitterChosen, event)
		return nil
	}

	// Publish to event bus anyway
	eh.eventBus.Publish(eventbus.EventSubmitterChosen, event)
	return nil
}

// processAddProposerRequested marks a proposer as pending add with the event payload recorded
func (eh *EventHandler) processAddProposerRequested(event BlockchainEvent) error {
	logger := eh.logger.WithField("event", "AddProposerRequested")
	// proposer: address in event data
	proposerAddr, err := readAddress(event.EventData["proposer"])
	if err != nil {
		return fmt.Errorf("invalid proposer in AddProposerRequested: %w", err)
	}

	// Save pending state with serialized event data
	payload, _ := json.Marshal(event.EventData)

	tx := eh.eventRepo.BeginTransaction()
	proposer := &models.Proposers{
		Address:      proposerAddr,
		Status:       "pending",
		PendingEvent: string(payload),
		JoinBlock:    0,
	}
	if err := eh.eventRepo.CreateOrUpdateProposer(tx, proposer); err != nil {
		tx.Rollback()
		return fmt.Errorf("failed to mark proposer pending add: %w", err)
	}
	if err := tx.Commit().Error; err != nil {
		tx.Rollback()
		return fmt.Errorf("failed to commit transaction: %w", err)
	}
	logger.Infof("Proposer %s marked pending add", proposerAddr)
	return nil
}

// processRemoveProposerRequested marks a proposer as pending exit
func (eh *EventHandler) processRemoveProposerRequested(event BlockchainEvent) error {
	logger := eh.logger.WithField("event", "RemoveProposerRequested")
	proposerAddr, err := readAddress(event.EventData["proposer"])
	if err != nil {
		return fmt.Errorf("invalid proposer in RemoveProposerRequested: %w", err)
	}

	payload, _ := json.Marshal(event.EventData)

	tx := eh.eventRepo.BeginTransaction()
	// keep status pending, store pending event, set exit block when confirmed later
	proposer := &models.Proposers{
		Address:      proposerAddr,
		Status:       "pending",
		PendingEvent: string(payload),
	}
	if err := eh.eventRepo.CreateOrUpdateProposer(tx, proposer); err != nil {
		tx.Rollback()
		return fmt.Errorf("failed to mark proposer pending remove: %w", err)
	}
	if err := tx.Commit().Error; err != nil {
		tx.Rollback()
		return fmt.Errorf("failed to commit transaction: %w", err)
	}
	logger.Infof("Proposer %s marked pending remove", proposerAddr)
	return nil
}

// processProposerConfirmed applies the pending action and sets final status
func (eh *EventHandler) processProposerConfirmed(event BlockchainEvent) error {
	logger := eh.logger.WithField("event", "ProposerConfirmed")
	proposerAddr, err := readAddress(event.EventData["proposer"])
	if err != nil {
		return fmt.Errorf("invalid proposer in ProposerConfirmed: %w", err)
	}

	tx := eh.eventRepo.BeginTransaction()
	rec, err := eh.eventRepo.GetProposer(tx, proposerAddr)
	if err != nil {
		tx.Rollback()
		return fmt.Errorf("failed to load proposer %s: %w", proposerAddr, err)
	}

	// Decide whether it’s confirming an add or a remove based on PendingEvent content
	finalStatus := rec.Status
	if finalStatus == "pending" {
		// If no previous status, treat as add confirmation
		finalStatus = "ok"
		if rec.JoinBlock == 0 {
			rec.JoinBlock = event.BlockNumber
		}
	} else if finalStatus == "ok" {
		// If currently ok and got a pending remove earlier, set exit
		finalStatus = "exit"
		rec.ExitBlock = event.BlockNumber
	}

	if err := tx.Model(&models.Proposers{}).Where("id = ?", rec.ID).Updates(map[string]any{
		"status":        finalStatus,
		"pending_event": "",
		"join_block":    rec.JoinBlock,
		"exit_block":    rec.ExitBlock,
	}).Error; err != nil {
		tx.Rollback()
		return fmt.Errorf("failed to confirm proposer change: %w", err)
	}

	if err := tx.Commit().Error; err != nil {
		tx.Rollback()
		return fmt.Errorf("failed to commit transaction: %w", err)
	}
	logger.Infof("Proposer %s confirmed; status=%s", proposerAddr, finalStatus)
	return nil
}

// Helpers to robustly parse ABI values into usable formats

// toUint256String parses a value that may be *big.Int, big.Int, uint64, string, etc., into a base-10 string
func toUint256String(v any) (string, error) {
	switch t := v.(type) {
	case *big.Int:
		return t.String(), nil
	case big.Int:
		return t.String(), nil
	case uint64:
		return new(big.Int).SetUint64(t).String(), nil
	case int64:
		return new(big.Int).SetInt64(t).String(), nil
	case string:
		// assume already decimal string
		return t, nil
	default:
		// try reflect for pointers to big.Int
		rv := reflect.ValueOf(v)
		if rv.IsValid() && rv.Kind() == reflect.Ptr {
			if bi, ok := rv.Interface().(*big.Int); ok && bi != nil {
				return bi.String(), nil
			}
		}
		return "", fmt.Errorf("unsupported uint256 type: %T", v)
	}
}

// toUint256StringSlice parses uint256[] variants into []string
func toUint256StringSlice(v any) ([]string, error) {
	switch t := v.(type) {
	case []interface{}:
		out := make([]string, 0, len(t))
		for _, it := range t {
			s, err := toUint256String(it)
			if err != nil {
				return nil, err
			}
			out = append(out, s)
		}
		return out, nil
	case []*big.Int:
		out := make([]string, 0, len(t))
		for _, bi := range t {
			if bi == nil {
				return nil, fmt.Errorf("nil *big.Int in slice")
			}
			out = append(out, bi.String())
		}
		return out, nil
	case []big.Int:
		out := make([]string, 0, len(t))
		for _, bi := range t {
			out = append(out, bi.String())
		}
		return out, nil
	default:
		return nil, fmt.Errorf("unsupported uint256[] type: %T", v)
	}
}

// toHex32 converts a bytes32-like value ([32]byte, []byte, common.Hash, string) to 0x-hex string
func toHex32(v any) (string, error) {
	switch t := v.(type) {
	case common.Hash:
		return t.Hex(), nil
	case [32]byte:
		h := common.BytesToHash(t[:])
		return h.Hex(), nil
	case []byte:
		if len(t) != 32 {
			return "", fmt.Errorf("expected 32 bytes, got %d", len(t))
		}
		return common.BytesToHash(t).Hex(), nil
	case string:
		// assume already hex string; normalize
		if len(t) == 66 && t[:2] == "0x" {
			return t, nil
		}
		if len(t) == 64 {
			return "0x" + t, nil
		}
		return "", fmt.Errorf("unexpected string format for bytes32: %s", t)
	default:
		return "", fmt.Errorf("unsupported bytes32 type: %T", v)
	}
}

// readAddress accepts common input types and returns 0x-hex string address
func readAddress(v any) (string, error) {
	switch t := v.(type) {
	case string:
		if t == "" {
			return "", fmt.Errorf("empty address")
		}
		return t, nil
	case common.Address:
		return t.Hex(), nil
	default:
		return "", fmt.Errorf("unsupported address type: %T", v)
	}
}
