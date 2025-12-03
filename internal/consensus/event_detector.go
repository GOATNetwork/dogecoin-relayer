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

// BlockchainEvent represents a processed blockchain event (simplified, no DB persistence)
type BlockchainEvent struct {
	BlockNumber     uint64                 `json:"block_number"`
	TxHash          common.Hash            `json:"tx_hash"`
	LogIndex        uint                   `json:"log_index"`
	ContractAddress common.Address         `json:"contract_address"`
	EventName       string                 `json:"event_name"`
	EventData       map[string]interface{} `json:"event_data"`
	Timestamp       time.Time              `json:"timestamp"`
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

	eventChannel chan BlockchainEvent
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
		eventChannel:       make(chan BlockchainEvent, 1000),
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
	eventNames = append(eventNames, eventTypes.EventNameProposerSelected)
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
	filteredEventNames := make([]string, 0, len(eventNames))
	for _, eventName := range eventNames {
		event, exists := parsedABI.Events[eventName]
		if !exists {
			log.Warnf("Event %s not found in ABI, skipping subscription", eventName)
			continue
		}
		eventSignatures[eventName] = generateEventSignature(event)
		filteredEventNames = append(filteredEventNames, eventName)
	}

	if len(filteredEventNames) == 0 {
		return "", nil, fmt.Errorf("none of the requested events exist in ABI")
	}

	// Generate configs for all combinations of addresses and events
	var configs []EventConfig
	for _, contractAddress := range contractAddresses {
		for _, eventName := range filteredEventNames {
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
func (ed *EventDetector) EventChannel() <-chan BlockchainEvent {
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

	if latestBlock <= ed.lastScannedBlock {
		return nil
	}

	// Apply confirmation blocks
	confirmedBlock := latestBlock - ed.confirmationBlocks
	if confirmedBlock <= ed.lastScannedBlock {
		return nil
	}

	// Calculate scan range
	fromBlock := ed.lastScannedBlock + 1
	toBlock := min(fromBlock+ed.batchSize-1, confirmedBlock)

	ed.logger.Debugf("Scanning blocks %d to %d", fromBlock, toBlock)

	if err := ed.scanContractEvents(ed.configs, fromBlock, toBlock); err != nil {
		return fmt.Errorf("failed to scan events: %w", err)
	}

	tx := ed.eventRepo.BeginTransaction()

	if err := ed.eventRepo.UpdateScanState(tx, toBlock); err != nil {
		return fmt.Errorf("failed to update scan state: %w", err)
	}

	if err := tx.Commit().Error; err != nil {
		tx.Rollback()
		return fmt.Errorf("failed to commit transaction: %w", err)
	}

	// Update last scanned block
	ed.lastScannedBlock = toBlock

	return nil
}

// scanContractEvents scans events for all contracts
func (ed *EventDetector) scanContractEvents(configs []EventConfig, fromBlock, toBlock uint64) error {
	filterConfigs := make([]EventConfig, 0)
	for _, config := range configs {
		if config.IsActive {
			filterConfigs = append(filterConfigs, config)
		}
	}

	if len(filterConfigs) == 0 {
		return nil
	}

	// Parse contract ABI
	contractABI, err := abi.JSON(strings.NewReader(ed.ABI))
	if err != nil {
		return fmt.Errorf("failed to parse ABI: %w", err)
	}

	// Prepare addresses, topics and router table
	//    - topics[0] contains multiple event signatures (OR)
	//    - Addresses contains multiple addresses (OR)
	//    - Router table: addrHex -> sigHex -> config
	addrSet := make(map[string]struct{})
	var addresses []common.Address

	sigSet := make(map[common.Hash]struct{})
	var sigs []common.Hash

	type cfgBySig map[string]EventConfig   // sigHex -> config
	cfgRouter := make(map[string]cfgBySig) // addrHex -> (sigHex -> config)

	// Additional: topic0Hash -> abi.Event, avoid looking up event name later
	eventsBySig := make(map[common.Hash]abi.Event)

	for _, c := range configs {
		ev, ok := contractABI.Events[c.EventName]
		if !ok {
			ed.logger.Warnf("event %s not found in ABI, skip this config (addr=%s)", c.EventName, c.ContractAddress.Hex())
			continue
		}
		sig := ev.ID
		sigHex := sig.Hex()
		addrHex := c.ContractAddress.Hex()

		// Collect addresses (deduplication)
		if _, seen := addrSet[addrHex]; !seen {
			addrSet[addrHex] = struct{}{}
			addresses = append(addresses, c.ContractAddress)
		}

		// Collect signatures (deduplication)
		if _, seen := sigSet[sig]; !seen {
			sigSet[sig] = struct{}{}
			sigs = append(sigs, sig)
			eventsBySig[sig] = ev
		}

		// Router table
		if _, ok := cfgRouter[addrHex]; !ok {
			cfgRouter[addrHex] = make(cfgBySig)
		}
		cfgRouter[addrHex][sigHex] = c
	}

	if len(addresses) == 0 || len(sigs) == 0 {
		// All events are filtered out (e.g. event name not in ABI)
		ed.logger.Warnf("no active events found in ABI, skip scanning")
		return nil
	}

	// Assemble one-time FilterQuery
	query := ethereum.FilterQuery{
		FromBlock: big.NewInt(int64(fromBlock)),
		ToBlock:   big.NewInt(int64(toBlock)),
		Addresses: addresses,
		Topics:    [][]common.Hash{sigs}, // topic0 contains multiple event signatures (OR)
	}

	// Fetch logs (one time)
	logs, err := ed.client.FilterLogs(ed.ctx, query)
	if err != nil {
		return fmt.Errorf("failed to filter logs: %w", err)
	}

	// Process logs
	var detectedCount int
	for _, vlog := range logs {
		if len(vlog.Topics) == 0 {
			continue // Non-standard event log (no topic0), skip
		}
		addrHex := vlog.Address.Hex()
		sigHex := vlog.Topics[0].Hex()

		bySig, ok := cfgRouter[addrHex]
		if !ok {
			// Not in our batch of addresses (theoretically impossible, but just in case)
			continue
		}
		cfg, ok := bySig[sigHex]
		if !ok {
			// This address's event is not configured (or you put multiple events together but only want some of them)
			continue
		}

		evABI, ok := eventsBySig[vlog.Topics[0]]
		if !ok {
			// Event not cached in ABI (theoretically impossible)
			continue
		}

		detectedEvent, err := ed.parseEvent(vlog, cfg, contractABI, evABI)
		if err != nil {
			ed.logger.Errorf("Failed to parse event (addr=%s, event=%s, tx=%s): %v",
				addrHex, cfg.EventName, vlog.TxHash.Hex(), err)
			continue
		}

		// Send event directly to channel (no DB persistence for general events)
		select {
		case ed.eventChannel <- *detectedEvent:
			detectedCount++
		default:
			ed.logger.Warn("Event channel is full, dropping event")
		}
	}

	if detectedCount > 0 {
		ed.logger.Infof(
			"Detected %d events for %d contract(s) in blocks %d-%d",
			detectedCount, len(addresses), fromBlock, toBlock,
		)
	}

	return nil
}

// parseEvent parses a raw log into a BlockchainEvent
func (ed *EventDetector) parseEvent(vlog types.Log, config EventConfig, contractABI abi.ABI, event abi.Event) (*BlockchainEvent, error) {
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

	return &BlockchainEvent{
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
