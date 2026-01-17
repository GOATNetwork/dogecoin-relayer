package consensus

import (
	"encoding/hex"
	"encoding/json"
	"fmt"
	"math/big"
	"reflect"
	"strings"
	"time"

	"github.com/dogecoinw/doged/btcutil"
	"github.com/ethereum/go-ethereum/common"
	"github.com/goat-network/dogecoin-relayer/internal/models"
	"github.com/goat-network/dogecoin-relayer/internal/p2p"
	"github.com/goat-network/dogecoin-relayer/internal/wallet"
	"github.com/goat-network/dogecoin-relayer/pkg/eventbus"
	"github.com/goat-network/dogecoin-relayer/pkg/global"
	"github.com/goat-network/dogecoin-relayer/pkg/module"
	"github.com/goat-network/dogecoin-relayer/pkg/types"
	log "github.com/sirupsen/logrus"
	"gorm.io/gorm"
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
	case types.EventNameProposerSelected:
		processingErr = eh.processProposerSelected(event)
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

	// Extract txHash (bytes32) from event data
	rawTxHash, ok := event.EventData["txHash"]
	if !ok {
		return fmt.Errorf("missing txHash in BridgeIn event data")
	}

	txIdHex, err := toHex32(rawTxHash)
	if err != nil {
		return fmt.Errorf("failed to parse BridgeIn.txHash: %w", err)
	}

	txIdBytes, err := types.DecodeDogecoinHash(strings.TrimPrefix(txIdHex, "0x"))
	if err != nil {
		return fmt.Errorf("failed to decode Dogecoin txId: %w", err)
	}
	txId := fmt.Sprintf("%x", txIdBytes)

	logger.Infof("Processing BridgeIn for Dogecoin txId=%s", txId)

	// Use repository's transaction wrapper with automatic retry on database lock errors
	return eh.eventRepo.WithTransactionRetry(func(tx *gorm.DB) error {
		var matchingDeposits []models.Deposit
		if err := tx.Where("tx_id = ?", txId).Find(&matchingDeposits).Error; err != nil {
			return fmt.Errorf("query deposits by tx_id failed: %w", err)
		}

		if len(matchingDeposits) == 0 {
			return fmt.Errorf("no deposit found for txId=%s; ensure UTXO detection created it earlier", txId)
		}
		if len(matchingDeposits) != 1 {
			return fmt.Errorf("expected exactly 1 deposit for txId=%s, got %d", txId, len(matchingDeposits))
		}

		dep := matchingDeposits[0]
		if err := eh.eventRepo.UpdateDepositStatus(tx, dep.ID, "confirmed"); err != nil {
			return fmt.Errorf("failed updating deposit status: %w", err)
		}

		// Only update to processed if not already in a higher state (pending/spent)
		if err := tx.Model(&models.UTXO{}).
			Where("txid = ? AND out_index = ? AND status NOT IN (?, ?)",
				dep.TxId, dep.Vout,
				models.UTXO_STATUS_PENDING, models.UTXO_STATUS_SPENT).
			Update("status", models.UTXO_STATUS_PROCESSED).Error; err != nil {
			return fmt.Errorf("failed updating UTXO status: %w", err)
		}
		logger.Infof("Updated UTXO %s:%d status to processed (if not already pending/spent)", dep.TxId, dep.Vout)

		// Also set EVM fields
		dep.EvmTxHash = event.TxHash.Hex()
		dep.EvmBlock = event.BlockNumber
		dep.EvmLogIndex = event.LogIndex
		if err := tx.Model(&models.Deposit{}).Where("id = ?", dep.ID).Updates(map[string]any{
			"evm_tx_hash":   dep.EvmTxHash,
			"evm_block":     dep.EvmBlock,
			"evm_log_index": dep.EvmLogIndex,
		}).Error; err != nil {
			return fmt.Errorf("failed updating deposit EVM fields: %w", err)
		}

		logger.Infof("Updated deposit %d for txId=%s to confirmed with EVM fields", dep.ID, txIdHex)
		return nil
	})
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

	// Optional fields: destination amount/address for Dogecoin withdrawal
	var destAmountStr string
	if rawAmt, ok := event.EventData["destAmount"]; ok {
		if amtStr, err := toUint256String(rawAmt); err == nil {
			destAmountStr = amtStr
		} else {
			logger.Warnf("Failed to parse destAmount for task %s: %v", taskId, err)
		}
	}
	destDogeAddr, err := parseDestDogecoinAddress(event.EventData["destDogecoinAddress"])
	if err != nil {
		logger.Warnf("Failed to parse destDogecoinAddress for task %s: %v", taskId, err)
	}

	withdrawal := &models.Withdrawal{
		ReqTaskId:   taskId,
		ReqTxHash:   event.TxHash.Hex(),
		ReqBlock:    event.BlockNumber,
		ReqLogIndex: event.LogIndex,
		Status:      models.WITHDRAW_STATUS_CREATE,
		DestAmount:  destAmountStr,
		DestAddress: destDogeAddr,
	}

	// Check if withdrawal already exists and is in progress
	// Don't overwrite withdrawals that have already started processing
	existing, findErr := eh.eventRepo.GetWithdrawalByTask(nil, taskId)
	if findErr == nil && existing != nil {
		// Withdrawal exists - check if it's already being processed
		status := existing.Status
		if status == models.WITHDRAW_STATUS_INIT ||
			status == models.WITHDRAW_STATUS_PENDING ||
			status == models.WITHDRAW_STATUS_CONFIRMED ||
			status == models.WITHDRAW_STATUS_PROCESSED {
			// Don't overwrite - withdrawal is already in progress
			logger.Infof("Withdrawal taskId=%s already exists with status=%s, skipping update", taskId, status)
			eh.eventBus.Publish(eventbus.EventBridgeOutProposed, event)
			return nil
		}
		// Only update if status is CREATE or AGGREGATING (early stages)
		logger.Infof("Updating existing withdrawal taskId=%s (status=%s)", taskId, status)
	}

	err = eh.eventRepo.WithTransactionRetry(func(tx *gorm.DB) error {
		return eh.eventRepo.CreateOrUpdateWithdrawal(tx, withdrawal)
	})

	if err != nil {
		return fmt.Errorf("failed to create/update withdrawal: %w", err)
	}

	logger.Infof("Created/updated withdrawal record for taskId=%s", taskId)
	eh.broadcastWithdrawalStatus(withdrawal)

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

	// Collect closed orders for broadcasting after transaction commits
	var allClosedOrders []wallet.ClosedOrderInfo

	// Update withdrawal to final state using transaction wrapper with retry on lock errors
	err = eh.eventRepo.WithTransactionRetry(func(tx *gorm.DB) error {
		for _, taskId := range taskIds {
			if err := eh.eventRepo.SetWithdrawalFinishInfo(tx, taskId,
				event.TxHash.Hex(), event.BlockNumber, event.LogIndex); err != nil {
				return fmt.Errorf("failed to set withdrawal finish info (taskId=%s): %w", taskId, err)
			}

			withdrawal, err := eh.eventRepo.GetWithdrawalByTask(tx, taskId)
			if err != nil {
				return fmt.Errorf("failed to get withdrawal (taskId=%s): %w", taskId, err)
			}
			if err := eh.eventRepo.UpdateWithdrawalStatus(tx, withdrawal.ID, models.WITHDRAW_STATUS_PROCESSED); err != nil {
				return fmt.Errorf("failed to update withdrawal status (taskId=%s): %w", taskId, err)
			}
			logger.Infof("Updated withdrawal %s to processed state", taskId)

			// Close associated send_orders when withdrawal is processed
			closedOrders, closeErr := wallet.CloseSendOrdersForWithdrawal(tx, taskId)
			if closeErr != nil {
				logger.Warnf("Failed to close send_orders for withdrawal %s: %v", taskId, closeErr)
			} else if len(closedOrders) > 0 {
				allClosedOrders = append(allClosedOrders, closedOrders...)
				logger.Infof("Closed %d send_orders for withdrawal %s", len(closedOrders), taskId)
			}
		}
		return nil
	})
	if err != nil {
		return err
	}

	// Broadcast withdrawal status updates
	for _, taskId := range taskIds {
		withdrawal, err := eh.eventRepo.GetWithdrawalByTask(nil, taskId)
		if err != nil {
			logger.Warnf("Failed to load withdrawal %s for status sync: %v", taskId, err)
			continue
		}
		eh.broadcastWithdrawalStatus(withdrawal)
	}

	// Broadcast send_order status updates
	for _, closedOrder := range allClosedOrders {
		eh.broadcastSendOrderStatusUpdate(closedOrder.OrderId, closedOrder.Txid, closedOrder.OldStatus, "closed", "withdrawal_processed")
	}

	eh.eventBus.Publish(eventbus.EventBridgeOutFinished, event)
	return nil
}

// processProposerSelected handles ProposerSelected events
func (eh *EventHandler) processProposerSelected(event BlockchainEvent) error {
	logger := eh.logger.WithField("event", "ProposerSelected")

	logger.Infof("ProposerSelected event detected: Tx %s at block %d",
		event.TxHash.Hex(), event.BlockNumber)

	// Extract the chosen proposer address from event data
	var proposerAddr string
	if event.EventData != nil {
		// Log the event data for debugging
		eventDataJSON, err := json.MarshalIndent(event.EventData, "", "  ")
		if err == nil {
			logger.Debugf("ProposerSelected event data:\n%s", string(eventDataJSON))
		}

		// Try to extract proposer address from common field names
		if addr, err := readAddress(event.EventData["newProposer"]); err == nil {
			proposerAddr = addr
		} else if addr, err := readAddress(event.EventData["proposer"]); err == nil {
			proposerAddr = addr
		} else {
			// Log available fields for debugging
			var fields []string
			for key := range event.EventData {
				fields = append(fields, key)
			}
			logger.Warnf("Could not find proposer address in event data. Available fields: %v", fields)
		}
	} else {
		logger.Warn("ProposerSelected event has no data")
	}

	if proposerAddr != "" {
		logger.Infof("New proposer selected: %s", proposerAddr)

		// Update proposer record with retry transaction
		err := eh.eventRepo.WithTransactionRetry(func(tx *gorm.DB) error {
			proposer := &models.Proposers{
				Address:   proposerAddr,
				Status:    "ok", // Active proposer
				JoinBlock: event.BlockNumber,
			}
			return eh.eventRepo.CreateOrUpdateProposer(tx, proposer)
		})

		if err != nil {
			return fmt.Errorf("failed to create/update proposer: %w", err)
		}

		logger.Infof("Updated proposer record for address=%s", proposerAddr)

		// Publish to event bus for UTXO processor to update current proposer
		eh.eventBus.Publish(eventbus.EventProposerSelected, event)
		return nil
	}

	// Publish to event bus anyway
	eh.eventBus.Publish(eventbus.EventProposerSelected, event)
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

	err = eh.eventRepo.WithTransactionRetry(func(tx *gorm.DB) error {
		proposer := &models.Proposers{
			Address:      proposerAddr,
			Status:       "pending",
			PendingEvent: string(payload),
			JoinBlock:    0,
		}
		return eh.eventRepo.CreateOrUpdateProposer(tx, proposer)
	})
	if err != nil {
		return fmt.Errorf("failed to mark proposer pending add: %w", err)
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

	err = eh.eventRepo.WithTransactionRetry(func(tx *gorm.DB) error {
		// keep status pending, store pending event, set exit block when confirmed later
		proposer := &models.Proposers{
			Address:      proposerAddr,
			Status:       "pending",
			PendingEvent: string(payload),
		}
		return eh.eventRepo.CreateOrUpdateProposer(tx, proposer)
	})
	if err != nil {
		return fmt.Errorf("failed to mark proposer pending remove: %w", err)
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

	var finalStatus string
	err = eh.eventRepo.WithTransactionRetry(func(tx *gorm.DB) error {
		rec, err := eh.eventRepo.GetProposer(tx, proposerAddr)
		if err != nil {
			return fmt.Errorf("failed to load proposer %s: %w", proposerAddr, err)
		}

		// Decide whether it's confirming an add or a remove based on PendingEvent content
		finalStatus = rec.Status
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
			return fmt.Errorf("failed to confirm proposer change: %w", err)
		}
		return nil
	})
	if err != nil {
		return err
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

// parseDestDogecoinAddress attempts to convert the bytes20 payload emitted in BridgeOutProposed
// into a base58 Dogecoin address (P2PKH). Returns empty string when parsing fails.
func parseDestDogecoinAddress(raw any) (string, error) {
	if raw == nil {
		return "", fmt.Errorf("nil dest dogecoin address")
	}

	var payload []byte
	switch v := raw.(type) {
	case []byte:
		payload = v
	case [20]byte:
		payload = v[:]
	case string:
		// allow hex string input (with or without 0x)
		s := strings.TrimPrefix(v, "0x")
		if len(s) != 40 {
			return "", fmt.Errorf("unexpected string length for dest doge address: %d", len(s))
		}
		bs, err := hex.DecodeString(s)
		if err != nil {
			return "", fmt.Errorf("decode dest address hex: %w", err)
		}
		payload = bs
	default:
		return "", fmt.Errorf("unsupported dest doge address type: %T", raw)
	}

	if len(payload) != 20 {
		return "", fmt.Errorf("dest doge address payload size mismatch: %d", len(payload))
	}

	cfg := global.GetConfig()
	network := types.GetDogeNetwork(cfg.Doge.NetworkType)
	addr, err := btcutil.NewAddressPubKeyHash(payload, network)
	if err != nil {
		return "", fmt.Errorf("build doge address: %w", err)
	}
	return addr.EncodeAddress(), nil
}

func (eh *EventHandler) broadcastWithdrawalStatus(w *models.Withdrawal) {
	p2pModule, ok := module.GetModule((&p2p.P2PModule{}).Name())
	if !ok {
		return
	}
	payload := types.WithdrawalStatusPayload{
		ReqTaskId:      w.ReqTaskId,
		Status:         w.Status,
		ReqTxHash:      w.ReqTxHash,
		ReqBlock:       w.ReqBlock,
		ReqLogIndex:    w.ReqLogIndex,
		DestAddress:    w.DestAddress,
		DestAmount:     w.DestAmount,
		TxId:           w.TxId,
		ExternalId:     w.ExternalId,
		Vout:           w.Vout,
		TxBytes:        w.TxBytes,
		UnsignedTx:     w.UnsignedTx,
		FinishTxHash:   w.FinishTxHash,
		FinishBlock:    w.FinishBlock,
		FinishLogIndex: w.FinishLogIndex,
		UpdatedAt:      time.Now().Unix(),
	}
	payloadBytes, err := json.Marshal(payload)
	if err != nil {
		eh.logger.Warnf("Failed to marshal withdrawal status payload: %v", err)
		return
	}
	msg := types.P2PBroadcastMessage{
		Type:      types.P2PMessageTypeWithdrawalStatus,
		SessionID: w.ReqTaskId,
		Payload:   payloadBytes,
	}
	if err := p2pModule.(p2p.P2PSender).BroadcastP2PMessage(msg); err != nil {
		eh.logger.Warnf("Failed to broadcast withdrawal status %s: %v", w.ReqTaskId, err)
	}
}

func (eh *EventHandler) broadcastSendOrderStatusUpdate(orderId, txid, oldStatus, newStatus, reason string) {
	p2pModule, ok := module.GetModule((&p2p.P2PModule{}).Name())
	if !ok {
		return
	}
	payload := types.SendOrderStatusUpdatePayload{
		OrderId:   orderId,
		Txid:      txid,
		OldStatus: oldStatus,
		NewStatus: newStatus,
		Reason:    reason,
		UpdatedAt: time.Now().Unix(),
	}
	payloadBytes, err := json.Marshal(payload)
	if err != nil {
		eh.logger.Warnf("Failed to marshal send_order status update payload: %v", err)
		return
	}
	msg := types.P2PBroadcastMessage{
		Type:      types.P2PMessageTypeSendOrderStatusUpdate,
		SessionID: orderId,
		Payload:   payloadBytes,
	}
	if err := p2pModule.(p2p.P2PSender).BroadcastP2PMessage(msg); err != nil {
		eh.logger.Warnf("Failed to broadcast send_order status update %s: %v", orderId, err)
	} else {
		eh.logger.Debugf("Broadcast send_order %s status: %s → %s (reason: %s)", orderId, oldStatus, newStatus, reason)
	}
}
