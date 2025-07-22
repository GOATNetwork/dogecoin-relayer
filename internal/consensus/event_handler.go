package consensus

import (
	"encoding/json"
	"fmt"

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
func (eh *EventHandler) Start(eventChannel <-chan DetectedEvent) error {
	// Start processing events
	go eh.processEvents(eventChannel)

	eh.logger.Info("Event handler started")
	return nil
}

// processEvents processes detected events from the provided channel
func (eh *EventHandler) processEvents(eventChannel <-chan DetectedEvent) {
	for event := range eventChannel {
		if err := eh.ProcessEvent(event); err != nil {
			eh.logger.Errorf("Failed to process event %s: %v", event.EventName, err)
		}
	}
}

// ProcessEvent processes a detected event based on its type
func (eh *EventHandler) ProcessEvent(event DetectedEvent) error {
	eh.logger.Debugf("Processing event: %s (ID: %d)", event.EventName, event.DatabaseID)

	// Event should already be saved to database by EventDetector
	if event.DatabaseID == 0 {
		eh.logger.Errorf("Event has no database ID - this should not happen")
		return fmt.Errorf("event has no database ID")
	}

	// Log the processing step start
	processingLog := &models.EventProcessingLog{
		DetectedEventID: event.DatabaseID,
		ProcessingStep:  "event_received",
		Status:          "success",
	}
	if err := eh.eventRepo.CreateProcessingLog(processingLog); err != nil {
		eh.logger.Warnf("Failed to log processing step: %v", err)
	}

	var processingErr error
	switch event.EventName {
	case types.EventNameBridgeIn:
		processingErr = eh.processBridgeIn(event, event.DatabaseID)
	case types.EventNameBridgeOutProposed:
		processingErr = eh.processBridgeOutProposed(event, event.DatabaseID)
	case types.EventNameBridgeOutFinished:
		processingErr = eh.processBridgeOutFinished(event, event.DatabaseID)
	case types.EventNameSubmitterChosen:
		processingErr = eh.processSubmitterChosen(event, event.DatabaseID)
	default:
		eh.logger.Warnf("Unknown event type: %s", event.EventName)
		processingErr = fmt.Errorf("unknown event type: %s", event.EventName)
	}

	// Update event status based on processing result
	finalStatus := "processed"
	if processingErr != nil {
		finalStatus = "failed"
		// Log the processing failure
		failureLog := &models.EventProcessingLog{
			DetectedEventID: event.DatabaseID,
			ProcessingStep:  "event_processing",
			Status:          "failed",
			ErrorMessage:    processingErr.Error(),
		}
		if err := eh.eventRepo.CreateProcessingLog(failureLog); err != nil {
			eh.logger.Warnf("Failed to log processing failure: %v", err)
		}
	}

	// Update the event status in database
	if err := eh.eventRepo.UpdateDetectedEventStatus(event.DatabaseID, finalStatus); err != nil {
		eh.logger.Errorf("Failed to update event status: %v", err)
	}

	return processingErr
}

// processBridgeIn handles BridgeIn events
func (eh *EventHandler) processBridgeIn(event DetectedEvent, eventID uint) error {
	logger := eh.logger.WithField("event", "BridgeIn").WithField("event_id", eventID)

	// Log processing step start
	processingLog := &models.EventProcessingLog{
		DetectedEventID: eventID,
		ProcessingStep:  "bridge_in_processing",
		Status:          "success",
	}

	// Convert event data to JSON for logging
	eventDataJSON, err := json.MarshalIndent(event.EventData, "", "  ")
	if err != nil {
		processingLog.Status = "failed"
		processingLog.ErrorMessage = fmt.Sprintf("failed to marshal event data: %v", err)
		eh.eventRepo.CreateProcessingLog(processingLog)
		return fmt.Errorf("failed to marshal event data: %w", err)
	}

	logger.Infof("BridgeIn event detected:\nContract: %s\nTx: %s\nBlock: %d\nData:\n%s",
		event.ContractAddress.Hex(),
		event.TxHash.Hex(),
		event.BlockNumber,
		string(eventDataJSON))

	// TODO: Implement specific BridgeIn business logic here
	// - Validate bridge request
	// - Check token balances
	// - Initiate cross-chain transfer process

	// Log successful processing
	if err := eh.eventRepo.CreateProcessingLog(processingLog); err != nil {
		eh.logger.Warnf("Failed to log processing step: %v", err)
	}

	// Publish to event bus for other modules
	eh.eventBus.Publish(eventbus.EventBridgeInDetected, event)

	return nil
}

// processBridgeOutProposed handles BridgeOutProposed events
func (eh *EventHandler) processBridgeOutProposed(event DetectedEvent, eventID uint) error {
	logger := eh.logger.WithField("event", "BridgeOutProposed").WithField("event_id", eventID)

	// Log processing step start
	processingLog := &models.EventProcessingLog{
		DetectedEventID: eventID,
		ProcessingStep:  "bridge_out_proposed_processing",
		Status:          "success",
	}

	logger.Infof("BridgeOutProposed event detected: Tx %s at block %d",
		event.TxHash.Hex(), event.BlockNumber)

	// TODO: implement BridgeOutProposed logic
	// - Validate proposal
	// - Coordinate with TSS for signing
	// - Update proposal status in DB

	// Log successful processing
	if err := eh.eventRepo.CreateProcessingLog(processingLog); err != nil {
		eh.logger.Warnf("Failed to log processing step: %v", err)
	}

	// Publish to event bus
	eh.eventBus.Publish(eventbus.EventBridgeOutProposed, event)

	return nil
}

// processBridgeOutFinished handles BridgeOutFinished events
func (eh *EventHandler) processBridgeOutFinished(event DetectedEvent, eventID uint) error {
	logger := eh.logger.WithField("event", "BridgeOutFinished").WithField("event_id", eventID)

	// Log processing step start
	processingLog := &models.EventProcessingLog{
		DetectedEventID: eventID,
		ProcessingStep:  "bridge_out_finished_processing",
		Status:          "success",
	}

	logger.Infof("BridgeOutFinished event detected: Tx %s at block %d",
		event.TxHash.Hex(), event.BlockNumber)

	// TODO: implement BridgeOutFinished logic
	// - Mark transaction as completed
	// - Update database status
	// - Cleanup any pending states

	// Log successful processing
	if err := eh.eventRepo.CreateProcessingLog(processingLog); err != nil {
		eh.logger.Warnf("Failed to log processing step: %v", err)
	}

	// Publish to event bus
	eh.eventBus.Publish(eventbus.EventBridgeOutFinished, event)

	return nil
}

// processSubmitterChosen handles SubmitterChosen events
func (eh *EventHandler) processSubmitterChosen(event DetectedEvent, eventID uint) error {
	logger := eh.logger.WithField("event", "SubmitterChosen").WithField("event_id", eventID)

	// Log processing step start
	processingLog := &models.EventProcessingLog{
		DetectedEventID: eventID,
		ProcessingStep:  "submitter_chosen_processing",
		Status:          "success",
	}

	logger.Infof("SubmitterChosen event detected: Tx %s at block %d",
		event.TxHash.Hex(), event.BlockNumber)

	// Extract the chosen submitter address from event data
	if event.EventData != nil {
		// Log the event data for debugging
		eventDataJSON, err := json.MarshalIndent(event.EventData, "", "  ")
		if err == nil {
			logger.Debugf("SubmitterChosen event data:\n%s", string(eventDataJSON))
		}

		// Try to extract submitter address from common field names
		var submitterAddr string
		if addr, ok := event.EventData["submitter"].(string); ok {
			submitterAddr = addr
		} else if addr, ok := event.EventData["chosen"].(string); ok {
			submitterAddr = addr
		} else if addr, ok := event.EventData["newSubmitter"].(string); ok {
			submitterAddr = addr
		} else {
			// Log available fields for debugging
			var fields []string
			for key := range event.EventData {
				fields = append(fields, key)
			}
			logger.Warnf("Could not find submitter address in event data. Available fields: %v", fields)
		}

		if submitterAddr != "" {
			logger.Infof("New submitter chosen: %s", submitterAddr)
		}
	} else {
		logger.Warn("SubmitterChosen event has no data")
	}

	// Log successful processing
	if err := eh.eventRepo.CreateProcessingLog(processingLog); err != nil {
		eh.logger.Warnf("Failed to log processing step: %v", err)
	}

	// Publish to event bus for UTXO processor to update current proposer
	eh.eventBus.Publish(eventbus.EventSubmitterChosen, event)

	return nil
}
