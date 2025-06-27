package consensus

import (
	"encoding/json"
	"fmt"

	"github.com/ethereum/go-ethereum/common"
	log "github.com/sirupsen/logrus"
)

// EventHandler manages event detection and processing
type EventHandler struct {
	detector   *EventDetector
	processors map[string]EventProcessor
	logger     *log.Entry
}

// EventProcessor defines how to process different types of events
type EventProcessor interface {
	ProcessEvent(event DetectedEvent) error
	GetEventName() string
}

// NewEventHandler creates a new event handler
func NewEventHandler(configs []EventConfig, options ...EventDetectorOption) *EventHandler {
	return &EventHandler{
		detector:   NewEventDetector(configs, options...),
		processors: make(map[string]EventProcessor),
		logger:     log.WithField("component", "EventHandler"),
	}
}

// RegisterProcessor registers an event processor for a specific event type
func (eh *EventHandler) RegisterProcessor(processor EventProcessor) {
	eh.processors[processor.GetEventName()] = processor
	eh.logger.Infof("Registered processor for event: %s", processor.GetEventName())
}

// Start begins event detection and processing
func (eh *EventHandler) Start() error {
	// Start the detector
	if err := eh.detector.Start(); err != nil {
		return fmt.Errorf("failed to start event detector: %w", err)
	}

	// Start processing events
	go eh.processEvents()

	eh.logger.Info("Event handler started")
	return nil
}

// Stop stops event detection and processing
func (eh *EventHandler) Stop() {
	eh.detector.Stop()
	eh.logger.Info("Event handler stopped")
}

// processEvents processes detected events
func (eh *EventHandler) processEvents() {
	for event := range eh.detector.EventChannel() {
		if processor, exists := eh.processors[event.EventName]; exists {
			if err := processor.ProcessEvent(event); err != nil {
				eh.logger.Errorf("Failed to process event %s: %v", event.EventName, err)
			}
		} else {
			eh.logger.Warnf("No processor registered for event: %s", event.EventName)
		}
	}
}

// GetLastScannedBlock returns the last scanned block
func (eh *EventHandler) GetLastScannedBlock() uint64 {
	return eh.detector.GetLastScannedBlock()
}

// UpdateConfigs updates the event configurations
func (eh *EventHandler) UpdateConfigs(configs []EventConfig) {
	eh.detector.UpdateConfigs(configs)
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

// Helper functions for creating common event configurations

// CreateCustomEventConfig creates a configuration for monitoring custom events
func CreateCustomEventConfig(contractAddress common.Address, eventName, eventSignature, abi string) EventConfig {
	return EventConfig{
		ContractAddress: contractAddress,
		EventName:       eventName,
		EventSignature:  eventSignature,
		ABI:             abi,
		IsActive:        true,
	}
}
