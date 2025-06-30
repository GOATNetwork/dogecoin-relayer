package consensus

import (
	"context"
	"fmt"
	"time"

	log "github.com/sirupsen/logrus"

	"github.com/goat-network/dogecoin-relayer/internal/config"
	"github.com/goat-network/dogecoin-relayer/internal/models"
	"github.com/goat-network/dogecoin-relayer/pkg/module"
	"github.com/goat-network/dogecoin-relayer/pkg/types"
)

// EventManager manages consensus-related events
type EventManager struct {
	cfg    config.EventDetectionConfig
	conn   *models.DBConnection
	logger *log.Entry

	detector *EventDetector
	handler  *EventHandler
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

	if err := m.StartMonitoring(); err != nil {
		m.logger.Errorf("Failed to start event monitoring: %v", err)
		return err
	}

	return nil
}

func (m *EventManager) Shutdown(ctx context.Context) error {
	m.logger.Info("Event manager module shutting down")
	if m.detector != nil {
		m.detector.Stop()
	}
	return nil
}

// StartMonitoring starts monitoring consensus-related events
func (m *EventManager) StartMonitoring() error {
	// Check if event detection is enabled
	if !m.cfg.Enabled {
		m.logger.Info("Event detection is disabled in configuration")
		return nil
	}

	// Create event configurations using contract utilities
	rawAbiData, configs, err := CreateRequiredEventConfigs(m.cfg, m.cfg.AbiPath)
	if err != nil {
		m.logger.Errorf("Failed to create event config from ABI: %v", err)
		return err
	}

	// Create event detector with configuration values
	m.detector = NewEventDetector(rawAbiData, configs, SetLastScannedBlock(uint64(m.cfg.LastScannedBlock)),
		SetConfirmationBlocks(uint64(m.cfg.ConfirmationBlocks)),
		SetBatchSize(uint64(m.cfg.BatchSize)),
		SetScanInterval(time.Duration(m.cfg.ScanIntervalSec)*time.Second))

	// Create event handler
	m.handler = NewEventHandler()

	// Register processors
	m.handler.RegisterProcessor(NewGenericEventProcessor("ConsensusUpdate"))

	// Start the detector first
	if err := m.detector.Start(); err != nil {
		return fmt.Errorf("failed to start event detector: %w", err)
	}

	// Start the handler with the detector's event channel
	if err := m.handler.Start(m.detector.EventChannel()); err != nil {
		return fmt.Errorf("failed to start event handler: %w", err)
	}

	m.logger.WithFields(log.Fields{
		"contract_bridge":      m.cfg.ContractBridge,
		"contract_entry_point": m.cfg.ContractEntryPoint,
		"confirmation_blocks":  m.cfg.ConfirmationBlocks,
		"batch_size":           m.cfg.BatchSize,
		"scan_interval_sec":    m.cfg.ScanIntervalSec,
	}).Info("Started monitoring consensus events")

	return nil
}

// StopMonitoring stops event monitoring
func (m *EventManager) StopMonitoring() {
	if m.detector != nil {
		m.detector.Stop()
	}
	m.logger.Info("Stopped monitoring consensus events")
}

// GetMonitoringStatus returns the current monitoring status
func (m *EventManager) GetMonitoringStatus() map[string]interface{} {
	if m.detector == nil {
		return map[string]interface{}{
			"status": "stopped",
		}
	}

	return map[string]interface{}{
		"status":             "running",
		"last_scanned_block": m.detector.GetLastScannedBlock(),
	}
}

func init() {
	log.Info("Registering event manager module")
	module.RegisterModule(&EventManager{})
}
