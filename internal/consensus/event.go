package consensus

import (
	"context"
	"fmt"
	"time"

	"github.com/ethereum/go-ethereum/common"
	log "github.com/sirupsen/logrus"

	"github.com/goat-network/dogecoin-relayer/internal/config"
)

// EventManager manages consensus-related events
type EventManager struct {
	handler *EventHandler
	ctx     context.Context
	cancel  context.CancelFunc
	logger  *log.Entry
	config  *config.ConsensusConfig
}

// NewEventManager creates a new consensus event manager
func NewEventManager(cfg *config.ConsensusConfig) *EventManager {
	ctx, cancel := context.WithCancel(context.Background())

	return &EventManager{
		ctx:    ctx,
		cancel: cancel,
		logger: log.WithField("component", "EventManager"),
		config: cfg,
	}
}

// StartMonitoring starts monitoring consensus-related events
func (cem *EventManager) StartMonitoring(contractAddress common.Address) error {
	// Check if event detection is enabled
	if !cem.config.EventDetection.Enabled {
		cem.logger.Info("Event detection is disabled in configuration")
		return nil
	}

	// Create event configurations for a hypothetical consensus contract
	configs := cem.createEventConfigs(contractAddress)

	// Create event handler with configuration values
	cem.handler = NewEventHandler(configs,
		SetLastScannedBlock(uint64(cem.config.EventDetection.LastScannedBlock)),
		SetConfirmationBlocks(uint64(cem.config.EventDetection.ConfirmationBlocks)),
		SetBatchSize(uint64(cem.config.EventDetection.BatchSize)),
		SetScanInterval(time.Duration(cem.config.EventDetection.ScanIntervalSec)*time.Second),
	)

	// Register processors
	cem.handler.RegisterProcessor(NewGenericEventProcessor("ConsensusUpdate"))

	// Start the handler
	if err := cem.handler.Start(); err != nil {
		return fmt.Errorf("failed to start consensus event handler: %w", err)
	}

	cem.logger.WithFields(log.Fields{
		"contract":            contractAddress.Hex(),
		"confirmation_blocks": cem.config.EventDetection.ConfirmationBlocks,
		"batch_size":          cem.config.EventDetection.BatchSize,
		"scan_interval_sec":   cem.config.EventDetection.ScanIntervalSec,
	}).Info("Started monitoring consensus events")

	return nil
}

// StopMonitoring stops event monitoring
func (cem *EventManager) StopMonitoring() {
	if cem.handler != nil {
		cem.handler.Stop()
	}
	cem.cancel()
	cem.logger.Info("Stopped monitoring consensus events")
}

// GetMonitoringStatus returns the current monitoring status
func (cem *EventManager) GetMonitoringStatus() map[string]interface{} {
	if cem.handler == nil {
		return map[string]interface{}{
			"status": "stopped",
		}
	}

	return map[string]interface{}{
		"status":             "running",
		"last_scanned_block": cem.handler.GetLastScannedBlock(),
	}
}

// createEventConfigs creates event configurations for consensus contract
func (cem *EventManager) createEventConfigs(contractAddress common.Address) []EventConfig {
	// Example ABI for a hypothetical consensus contract
	consensusABI := `[
		{
			"anonymous": false,
			"inputs": [
				{"indexed": false, "name": "epoch", "type": "uint256"},
				{"indexed": false, "name": "blockHash", "type": "bytes32"},
				{"indexed": false, "name": "timestamp", "type": "uint256"}
			],
			"name": "ConsensusUpdate",
			"type": "event"
		}
	]`

	return []EventConfig{
		{
			ContractAddress: contractAddress,
			EventName:       "ConsensusUpdate",
			EventSignature:  "ConsensusUpdate(uint256,bytes32,uint256)",
			ABI:             consensusABI,
			IsActive:        true,
		},
	}
}
