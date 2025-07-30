package consensus

import (
	"context"
	"fmt"
	"math/big"
	"strings"
	"sync"
	"time"

	"github.com/ethereum/go-ethereum"
	"github.com/ethereum/go-ethereum/accounts/abi"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/ethclient"
	log "github.com/sirupsen/logrus"

	"github.com/goat-network/dogecoin-relayer/internal/config"
	"github.com/goat-network/dogecoin-relayer/internal/models"
	"github.com/goat-network/dogecoin-relayer/pkg/contract"
	eventTypes "github.com/goat-network/dogecoin-relayer/pkg/types"
)

// EventConfig defines which events to monitor
type EventConfig struct {
	ContractAddress common.Address `json:"contract_address"`
	EventName       string         `json:"event_name"`      // e.g., "Transfer"
	EventSignature  string         `json:"event_signature"` // e.g., "Transfer(address,address,uint256)"
	IsActive        bool           `json:"is_active"`
}

// DetectedEvent represents a processed blockchain event
type DetectedEvent struct {
	BlockNumber     uint64                 `json:"block_number"`
	TxHash          common.Hash            `json:"tx_hash"`
	LogIndex        uint                   `json:"log_index"`
	ContractAddress common.Address         `json:"contract_address"`
	EventName       string                 `json:"event_name"`
	EventData       map[string]interface{} `json:"event_data"`
	Timestamp       time.Time              `json:"timestamp"`
	DatabaseID      uint                   `json:"database_id"`
}

// EventDetector monitors blockchain events
type EventDetector struct {
	ctx                context.Context
	cancel             context.CancelFunc
	configs            []EventConfig
	ABI                string // Contract ABI JSON
	client             *ethclient.Client
	eventRepo          *models.EventRepository
	lastScannedBlock   uint64
	confirmationBlocks uint64
	batchSize          uint64
	scanInterval       time.Duration

	eventChannel chan DetectedEvent
	mu           sync.RWMutex
	isRunning    bool

	logger *log.Entry
}

// EventDetectorOption allows customization of EventDetector
type EventDetectorOption func(*EventDetector)

// NewEventDetector creates a new event detector
func NewEventDetector(rawAbiData string, configs []EventConfig, eventRepo *models.EventRepository, options ...EventDetectorOption) *EventDetector {
	ctx, cancel := context.WithCancel(context.Background())

	detector := &EventDetector{
		ctx:                ctx,
		cancel:             cancel,
		configs:            configs,
		ABI:                rawAbiData,
		client:             GetEthClient(),
		eventRepo:          eventRepo,
		lastScannedBlock:   10000,
		confirmationBlocks: 6,               // Default 6 confirmations
		batchSize:          1000,            // Default batch size
		scanInterval:       5 * time.Second, // Default 5 seconds
		eventChannel:       make(chan DetectedEvent, 1000),
		logger:             log.WithField("component", "EventDetector"),
	}

	// Apply options
	for _, option := range options {
		option(detector)
	}

	return detector
}

func SetLastScannedBlock(block uint64) EventDetectorOption {
	return func(d *EventDetector) {
		d.lastScannedBlock = block
	}
}

// SetConfirmationBlocks sets the number of confirmation blocks
func SetConfirmationBlocks(blocks uint64) EventDetectorOption {
	return func(d *EventDetector) {
		d.confirmationBlocks = blocks
	}
}

// SetBatchSize sets the batch size for scanning
func SetBatchSize(size uint64) EventDetectorOption {
	return func(d *EventDetector) {
		d.batchSize = size
	}
}

// SetScanInterval sets the scanning interval
func SetScanInterval(interval time.Duration) EventDetectorOption {
	return func(d *EventDetector) {
		d.scanInterval = interval
	}
}

// CreateRequiredEventConfigs creates EventConfig using contract utilities for multiple addresses and events
func CreateRequiredEventConfigs(cfg config.EventDetectionConfig, abiFilePath string) (string, []EventConfig, error) {
	var contractAddresses []common.Address
	var eventNames []string
	contractAddresses = append(contractAddresses, common.HexToAddress(cfg.ContractBridge))
	eventNames = append(eventNames, eventTypes.EventNameBridgeIn)
	contractAddresses = append(contractAddresses, common.HexToAddress(cfg.ContractBridge))
	eventNames = append(eventNames, eventTypes.EventNameBridgeOutProposed)
	contractAddresses = append(contractAddresses, common.HexToAddress(cfg.ContractBridge))
	eventNames = append(eventNames, eventTypes.EventNameBridgeOutFinished)
	contractAddresses = append(contractAddresses, common.HexToAddress(cfg.ContractEntryPoint))
	eventNames = append(eventNames, eventTypes.EventNameSubmitterChosen)
	rawAbiData, configs, err := CreateEventConfig(abiFilePath, contractAddresses, eventNames)
	if err != nil {
		return "", nil, fmt.Errorf("failed to create event config: %w", err)
	}
	return rawAbiData, configs, nil
}

// CreateEventConfig creates EventConfig using contract utilities for multiple addresses and events
func CreateEventConfig(abiFilePath string, contractAddresses []common.Address, eventNames []string) (string, []EventConfig, error) {
	// Use the contract package to load ABI and raw data in one read
	parsedABI, rawAbiData, err := contract.LoadABIAndRawDataFromFile(abiFilePath)
	if err != nil {
		return "", nil, fmt.Errorf("failed to load ABI from %s: %w", abiFilePath, err)
	}

	// Validate all event names exist in ABI before creating configs
	eventSignatures := make(map[string]string)
	for _, eventName := range eventNames {
		event, exists := parsedABI.Events[eventName]
		if !exists {
			return "", nil, fmt.Errorf("event %s not found in ABI", eventName)
		}
		eventSignatures[eventName] = generateEventSignature(event)
	}

	// Generate configs for all combinations of addresses and events
	var configs []EventConfig
	for _, contractAddress := range contractAddresses {
		for _, eventName := range eventNames {
			configs = append(configs, EventConfig{
				ContractAddress: contractAddress,
				EventName:       eventName,
				EventSignature:  eventSignatures[eventName],
				IsActive:        true,
			})
		}
	}

	return rawAbiData, configs, nil
}

// generateEventSignature generates event signature from ABI event
func generateEventSignature(event abi.Event) string {
	var inputs []string
	for _, input := range event.Inputs {
		inputs = append(inputs, input.Type.String())
	}
	return fmt.Sprintf("%s(%s)", event.Name, strings.Join(inputs, ","))
}

// Start begins the event detection process
func (ed *EventDetector) Start() error {
	ed.mu.Lock()
	if ed.isRunning {
		ed.mu.Unlock()
		return fmt.Errorf("event detector is already running")
	}
	ed.isRunning = true
	ed.mu.Unlock()

	ed.logger.Info("Starting event detector")

	// Initialize scan states for each contract from DB
	if err := ed.initializeScanStates(); err != nil {
		ed.logger.Errorf("Failed to initialize scan states: %v", err)
		return fmt.Errorf("failed to initialize scan states: %w", err)
	}

	// Start scanning in a goroutine
	go ed.scanLoop()

	return nil
}

// initializeScanStates initializes scan states for all configured contracts
func (ed *EventDetector) initializeScanStates() error {
	// Skip initialization if repository is not available
	if ed.eventRepo == nil {
		ed.logger.Info("Event repository not available, using default scan states")
		return nil
	}

	contractAddressesMap := make(map[string]bool)

	// Get unique contract addresses from configs
	for _, config := range ed.configs {
		if config.IsActive {
			contractAddressesMap[config.ContractAddress.Hex()] = true
		}
	}

	// Initialize scan state for each unique contract address
	for contractAddress := range contractAddressesMap {
		state, err := ed.eventRepo.GetScanState(contractAddress)
		if err != nil {
			// If scan state doesn't exist, create it with current last scanned block
			newState := &models.EventScanState{
				ContractAddress:    contractAddress,
				LastScannedBlock:   ed.lastScannedBlock,
				ConfirmationBlocks: ed.confirmationBlocks,
				IsActive:           true,
			}

			if ed.eventRepo != nil {
				if err := ed.eventRepo.CreateOrUpdateScanState(newState); err != nil {
					return fmt.Errorf("failed to create scan state for contract %s: %w", contractAddress, err)
				}
			}

			ed.logger.Infof("Created initial scan state for contract %s at block %d", contractAddress, ed.lastScannedBlock)
		} else {
			// Use the minimum last scanned block from all contracts
			if state.LastScannedBlock < ed.lastScannedBlock {
				ed.lastScannedBlock = state.LastScannedBlock
			}
			ed.logger.Infof("Loaded scan state for contract %s, last scanned block: %d", contractAddress, state.LastScannedBlock)
		}
	}

	return nil
}

// Stop stops the event detection process
func (ed *EventDetector) Stop() {
	ed.mu.Lock()
	defer ed.mu.Unlock()

	if !ed.isRunning {
		return
	}

	ed.logger.Info("Stopping event detector")
	ed.cancel()
	ed.isRunning = false
	close(ed.eventChannel)
}

// EventChannel returns the channel for receiving detected events
func (ed *EventDetector) EventChannel() <-chan DetectedEvent {
	return ed.eventChannel
}

// scanLoop is the main scanning loop
func (ed *EventDetector) scanLoop() {
	ticker := time.NewTicker(ed.scanInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ed.ctx.Done():
			ed.logger.Info("Event detector scan loop stopped")
			return
		case <-ticker.C:
			if err := ed.scanForEvents(); err != nil {
				ed.logger.Errorf("Error scanning for events: %v", err)
			}
		}
	}
}

// scanForEvents scans for new events
func (ed *EventDetector) scanForEvents() error {
	// Get latest block
	latestBlock, err := ed.client.BlockNumber(ed.ctx)
	if err != nil {
		return fmt.Errorf("failed to get latest block: %w", err)
	}

	// Apply confirmation blocks
	confirmedBlock := latestBlock - ed.confirmationBlocks
	if confirmedBlock <= ed.lastScannedBlock {
		return nil // No new blocks to scan
	}

	// Calculate scan range
	fromBlock := ed.lastScannedBlock + 1
	toBlock := fromBlock + ed.batchSize - 1
	if toBlock > confirmedBlock {
		toBlock = confirmedBlock
	}

	ed.logger.Debugf("Scanning blocks %d to %d", fromBlock, toBlock)

	// Scan each contract separately for better error handling
	contractsScanned := make(map[string]bool)
	for _, config := range ed.configs {
		if !config.IsActive {
			continue
		}

		contractAddr := config.ContractAddress.Hex()
		if contractsScanned[contractAddr] {
			continue // Skip if already scanned this contract
		}

		if err := ed.scanContractEvents(config, fromBlock, toBlock); err != nil {
			ed.logger.Errorf("Failed to scan events for contract %s: %v", contractAddr, err)
			// Continue with other contracts even if one fails
		} else {
			contractsScanned[contractAddr] = true
		}
	}

	// Update scan states for all successfully scanned contracts
	for contractAddr := range contractsScanned {
		if ed.eventRepo != nil {
			if err := ed.eventRepo.UpdateScanState(contractAddr, toBlock); err != nil {
				ed.logger.Errorf("Failed to update scan state for contract %s: %v", contractAddr, err)
			}
		} else {
			ed.logger.Debugf("Updated scan state for contract %s to block %d", contractAddr, toBlock)
		}
	}

	// Update last scanned block
	ed.lastScannedBlock = toBlock

	return nil
}

// scanContractEvents scans events for a specific contract
func (ed *EventDetector) scanContractEvents(config EventConfig, fromBlock, toBlock uint64) error {
	// Parse contract ABI
	contractABI, err := abi.JSON(strings.NewReader(ed.ABI))
	if err != nil {
		return fmt.Errorf("failed to parse ABI: %w", err)
	}

	// Get event from ABI
	event, exists := contractABI.Events[config.EventName]
	if !exists {
		return fmt.Errorf("event %s not found in ABI", config.EventName)
	}

	// Create filter query
	query := ethereum.FilterQuery{
		FromBlock: big.NewInt(int64(fromBlock)),
		ToBlock:   big.NewInt(int64(toBlock)),
		Addresses: []common.Address{config.ContractAddress},
		Topics:    [][]common.Hash{{event.ID}}, // Filter by event signature
	}

	// Get logs
	logs, err := ed.client.FilterLogs(ed.ctx, query)
	if err != nil {
		return fmt.Errorf("failed to filter logs: %w", err)
	}

	// Process each log
	for _, vlog := range logs {
		detectedEvent, err := ed.parseEvent(vlog, config, contractABI, event)
		if err != nil {
			ed.logger.Errorf("Failed to parse event: %v", err)
			continue
		}

		// Save event to database immediately upon detection
		dbEvent := &models.DetectedEvent{
			BlockNumber:     detectedEvent.BlockNumber,
			TxHash:          detectedEvent.TxHash.Hex(),
			LogIndex:        detectedEvent.LogIndex,
			ContractAddress: detectedEvent.ContractAddress.Hex(),
			EventName:       detectedEvent.EventName,
			Status:          "pending",
		}

		if ed.eventRepo != nil {
			if err := ed.eventRepo.CreateDetectedEvent(dbEvent, detectedEvent.EventData); err != nil {
				ed.logger.Errorf("Failed to save detected event to database: %v", err)
				// Continue processing other events even if one fails to save
			}
		} else {
			ed.logger.Debugf("Event detected but not persisted (database not available): %s", detectedEvent.EventName)
			// Continue processing other events even if database is not available
			continue
		}

		// Attach database ID to the detected event for processing
		detectedEvent.DatabaseID = dbEvent.ID

		// Send to channel (non-blocking)
		select {
		case ed.eventChannel <- *detectedEvent:
		default:
			ed.logger.Warn("Event channel is full, dropping event (but already saved to DB)")
		}
	}

	if len(logs) > 0 {
		ed.logger.Infof("Detected and saved %d events for contract %s in blocks %d-%d",
			len(logs), config.ContractAddress.Hex(), fromBlock, toBlock)
	}

	return nil
}

// parseEvent parses a raw log into a DetectedEvent
func (ed *EventDetector) parseEvent(vlog types.Log, config EventConfig, contractABI abi.ABI, event abi.Event) (*DetectedEvent, error) {
	// Unpack event data
	eventData := make(map[string]interface{})
	err := contractABI.UnpackIntoMap(eventData, config.EventName, vlog.Data)
	if err != nil {
		return nil, fmt.Errorf("failed to unpack event data: %w", err)
	}

	// Add indexed parameters
	for i, arg := range event.Inputs {
		if arg.Indexed && i+1 < len(vlog.Topics) {
			value, err := ed.parseTopicValue(vlog.Topics[i+1], arg.Type.String())
			if err != nil {
				ed.logger.Warnf("Failed to parse indexed parameter %s: %v", arg.Name, err)
				continue
			}
			eventData[arg.Name] = value
		}
	}

	return &DetectedEvent{
		BlockNumber:     vlog.BlockNumber,
		TxHash:          vlog.TxHash,
		LogIndex:        vlog.Index,
		ContractAddress: vlog.Address,
		EventName:       config.EventName,
		EventData:       eventData,
		Timestamp:       time.Now(),
	}, nil
}

// parseTopicValue parses a topic value based on its type
func (ed *EventDetector) parseTopicValue(topic common.Hash, typeStr string) (interface{}, error) {
	switch typeStr {
	case "address":
		return common.HexToAddress(topic.Hex()), nil
	case "uint256":
		return new(big.Int).SetBytes(topic.Bytes()), nil
	case "bytes32":
		return topic, nil
	case "bool":
		return topic.Big().Cmp(big.NewInt(0)) != 0, nil
	default:
		// For other types, return the raw topic
		return topic, nil
	}
}

// GetLastScannedBlock returns the last scanned block number
func (ed *EventDetector) GetLastScannedBlock() uint64 {
	ed.mu.RLock()
	defer ed.mu.RUnlock()
	return ed.lastScannedBlock
}

// UpdateConfigs updates the event configurations
func (ed *EventDetector) UpdateConfigs(configs []EventConfig) {
	ed.mu.Lock()
	defer ed.mu.Unlock()
	ed.configs = configs
	ed.logger.Info("Event detector configurations updated")
}
