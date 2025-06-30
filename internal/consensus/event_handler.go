package consensus

import (
	"encoding/json"
	"fmt"

	log "github.com/sirupsen/logrus"
)

// EventHandler manages event detection and processing
type EventHandler struct {
	processors map[string]EventProcessor
	logger     *log.Entry
}

// EventProcessor defines how to process different types of events
type EventProcessor interface {
	ProcessEvent(event DetectedEvent) error
	GetEventName() string
}

// NewEventHandler creates a new event handler
func NewEventHandler() *EventHandler {
	return &EventHandler{
		processors: make(map[string]EventProcessor),
		logger:     log.WithField("component", "EventHandler"),
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
		if processor, exists := eh.processors[event.EventName]; exists {
			if err := processor.ProcessEvent(event); err != nil {
				eh.logger.Errorf("Failed to process event %s: %v", event.EventName, err)
			}
		} else {
			eh.logger.Warnf("No processor registered for event: %s", event.EventName)
		}
	}
}

// RegisterProcessor registers an event processor for a specific event type
func (eh *EventHandler) RegisterProcessor(processor EventProcessor) {
	eh.processors[processor.GetEventName()] = processor
	eh.logger.Infof("Registered processor for event: %s", processor.GetEventName())
}

// GenericEventProcessor processes any event and logs the data
type GenericEventProcessor struct {
	eventName string
	logger    *log.Entry
}

func NewGenericEventProcessor(eventName string) *GenericEventProcessor {
	return &GenericEventProcessor{
		eventName: eventName,
		logger:    log.WithField("processor", eventName),
	}
}

func (p *GenericEventProcessor) GetEventName() string {
	return p.eventName
}

func (p *GenericEventProcessor) ProcessEvent(event DetectedEvent) error {
	// Convert event data to JSON for logging
	eventDataJSON, err := json.MarshalIndent(event.EventData, "", "  ")
	if err != nil {
		return fmt.Errorf("failed to marshal event data: %w", err)
	}

	p.logger.Infof("Event detected: %s\nContract: %s\nTx: %s\nBlock: %d\nData:\n%s",
		event.EventName,
		event.ContractAddress.Hex(),
		event.TxHash.Hex(),
		event.BlockNumber,
		string(eventDataJSON))

	// TODO: save event to DB

	return nil
}
