package consensus

import (
	"context"
	"fmt"
	"time"

	"github.com/ethereum/go-ethereum/common"
	log "github.com/sirupsen/logrus"

	"github.com/goat-network/dogecoin-relayer/internal/config"
	"github.com/goat-network/dogecoin-relayer/internal/models"
	"github.com/goat-network/dogecoin-relayer/pkg/module"
	"github.com/goat-network/dogecoin-relayer/pkg/types"
)

// EventManager manages consensus-related events
type EventManager struct {
	cfg     config.EventDetectionConfig
	conn    *models.DBConnection
	logger  *log.Entry
	handler *EventHandler
}

var _ module.Module = (*EventManager)(nil)

func (m *EventManager) Name() string {
	return "event_manager"
}

func (m *EventManager) Init(cfg any, conn *models.DBConnection) error {
	m.cfg = cfg.(config.EventDetectionConfig)
	m.conn = conn
	m.logger = types.InitLogEntry(m.Name())
	return nil
}

func (m *EventManager) Run(ctx context.Context) error {
	m.logger.Info("Event manager module running")

	// Check if event detection is enabled
	if !m.cfg.Enabled {
		m.logger.Info("Event detection is disabled in configuration")
		return nil
	}

	// TODO: Start event monitoring logic here
	// For now, just log that the module is running
	m.logger.WithFields(log.Fields{
		"confirmation_blocks": m.cfg.ConfirmationBlocks,
		"batch_size":          m.cfg.BatchSize,
		"scan_interval_sec":   m.cfg.ScanIntervalSec,
	}).Info("Event manager configuration loaded")

	return nil
}

func (m *EventManager) Shutdown(ctx context.Context) error {
	m.logger.Info("Event manager module shutting down")
	if m.handler != nil {
		m.handler.Stop()
	}
	return nil
}

// StartMonitoring starts monitoring consensus-related events
func (m *EventManager) StartMonitoring(contractAddress common.Address) error {
	// Check if event detection is enabled
	if !m.cfg.Enabled {
		m.logger.Info("Event detection is disabled in configuration")
		return nil
	}

	// Create event configurations for a hypothetical consensus contract
	configs := m.createEventConfigs(contractAddress)

	// Create event handler with configuration values
	m.handler = NewEventHandler(configs,
		SetLastScannedBlock(uint64(m.cfg.LastScannedBlock)),
		SetConfirmationBlocks(uint64(m.cfg.ConfirmationBlocks)),
		SetBatchSize(uint64(m.cfg.BatchSize)),
		SetScanInterval(time.Duration(m.cfg.ScanIntervalSec)*time.Second),
	)

	// Register processors
	m.handler.RegisterProcessor(NewGenericEventProcessor("ConsensusUpdate"))

	// Start the handler
	if err := m.handler.Start(); err != nil {
		return fmt.Errorf("failed to start consensus event handler: %w", err)
	}

	m.logger.WithFields(log.Fields{
		"contract":            contractAddress.Hex(),
		"confirmation_blocks": m.cfg.ConfirmationBlocks,
		"batch_size":          m.cfg.BatchSize,
		"scan_interval_sec":   m.cfg.ScanIntervalSec,
	}).Info("Started monitoring consensus events")

	return nil
}

// StopMonitoring stops event monitoring
func (m *EventManager) StopMonitoring() {
	if m.handler != nil {
		m.handler.Stop()
	}
	m.logger.Info("Stopped monitoring consensus events")
}

// GetMonitoringStatus returns the current monitoring status
func (m *EventManager) GetMonitoringStatus() map[string]interface{} {
	if m.handler == nil {
		return map[string]interface{}{
			"status": "stopped",
		}
	}

	return map[string]interface{}{
		"status":             "running",
		"last_scanned_block": m.handler.GetLastScannedBlock(),
	}
}

// createEventConfigs creates event configurations for consensus contract
func (m *EventManager) createEventConfigs(contractAddress common.Address) []EventConfig {
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

func init() {
	log.Info("Registering event manager module")
	module.RegisterModule(&EventManager{})
}
