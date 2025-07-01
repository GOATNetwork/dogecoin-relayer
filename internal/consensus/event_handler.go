package consensus

import (
	"encoding/json"
	"fmt"

	"github.com/goat-network/dogecoin-relayer/pkg/eventbus"
	"github.com/goat-network/dogecoin-relayer/pkg/global"
	"github.com/goat-network/dogecoin-relayer/pkg/types"
	log "github.com/sirupsen/logrus"
)

// EventHandler manages event detection and processing
type EventHandler struct {
	eventBus *eventbus.Bus
	logger   *log.Entry
}

// NewEventHandler creates a new event handler
func NewEventHandler() *EventHandler {
	return &EventHandler{
		eventBus: global.GetEventBus(),
		logger:   log.WithField("component", "EventHandler"),
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
	eh.logger.Debugf("Processing event: %s", event.EventName)

	switch event.EventName {
	case types.EventNameBridgeIn:
		return eh.processBridgeIn(event)
	case types.EventNameBridgeOutProposed:
		return eh.processBridgeOutProposed(event)
	case types.EventNameBridgeOutFinished:
		return eh.processBridgeOutFinished(event)
	case types.EventNameSubmitterChosen:
		return eh.processSubmitterChosen(event)
	default:
		eh.logger.Warnf("Unknown event type: %s", event.EventName)
		return fmt.Errorf("unknown event type: %s", event.EventName)
	}
}

// processBridgeIn handles BridgeIn events
func (eh *EventHandler) processBridgeIn(event DetectedEvent) error {
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

	// TODO: save event to DB

	// Publish to event bus for other modules
	eh.eventBus.Publish(eventbus.EventBridgeInDetected, event)

	return nil
}

// processBridgeOutProposed handles BridgeOutProposed events
func (eh *EventHandler) processBridgeOutProposed(event DetectedEvent) error {
	logger := eh.logger.WithField("event", "BridgeOutProposed")

	logger.Infof("BridgeOutProposed event detected: Tx %s at block %d",
		event.TxHash.Hex(), event.BlockNumber)

	// TODO: implement BridgeOutProposed logic
	// - Validate proposal
	// - Coordinate with TSS for signing
	// - Update proposal status in DB

	// Publish to event bus
	eh.eventBus.Publish(eventbus.EventBridgeOutProposed, event)

	return nil
}

// processBridgeOutFinished handles BridgeOutFinished events
func (eh *EventHandler) processBridgeOutFinished(event DetectedEvent) error {
	logger := eh.logger.WithField("event", "BridgeOutFinished")

	logger.Infof("BridgeOutFinished event detected: Tx %s at block %d",
		event.TxHash.Hex(), event.BlockNumber)

	// TODO: implement BridgeOutFinished logic
	// - Mark transaction as completed
	// - Update database status
	// - Cleanup any pending states

	// Publish to event bus
	eh.eventBus.Publish(eventbus.EventBridgeOutFinished, event)

	return nil
}

// processSubmitterChosen handles SubmitterChosen events
func (eh *EventHandler) processSubmitterChosen(event DetectedEvent) error {
	logger := eh.logger.WithField("event", "SubmitterChosen")

	logger.Infof("SubmitterChosen event detected: Tx %s at block %d",
		event.TxHash.Hex(), event.BlockNumber)

	// TODO: implement SubmitterChosen logic
	// - Check if this node is the chosen submitter
	// - Prepare submission if selected
	// - Coordinate with other modules

	// Publish to event bus
	eh.eventBus.Publish(eventbus.EventSubmitterChosen, event)

	return nil
}
