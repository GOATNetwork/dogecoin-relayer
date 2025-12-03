package consensus

import (
	"context"
	"fmt"
	"time"

	"github.com/goat-network/dogecoin-relayer/internal/config"
	"github.com/goat-network/dogecoin-relayer/internal/models"
	"github.com/goat-network/dogecoin-relayer/pkg/types"
	log "github.com/sirupsen/logrus"
)

// EventManager manages consensus-related events
type EventManager struct {
	cfg       config.EventDetectionConfig
	conn      *models.DBConnection
	eventRepo *models.EventRepository
	logger    *log.Entry

	detector *EventDetector
	handler  *EventHandler
}

// EventManager initialization
func NewEventManager(cfg config.EventDetectionConfig, conn *models.DBConnection) (*EventManager, error) {
	if conn == nil || conn.GetDB() == nil {
		return nil, fmt.Errorf("database connection not initialized")
	}

	m := &EventManager{
		cfg:       cfg,
		conn:      conn,
		logger:    types.InitLogEntry("event_manager"),
		eventRepo: models.NewEventRepository(conn.GetDB()),
	}

	return m, nil
}

func (m *EventManager) Start(ctx context.Context) error {
	m.logger.Info("Event manager module running")

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

	if m.cfg.ScanIntervalSec <= 0 {
		m.logger.Fatal("Scan interval must be greater than 0")
	}

	// Check if event repository is available (database ready)
	if m.eventRepo == nil {
		m.logger.Fatal("Event repository not available, event detection will run without persistence")
	}

	// Create event configurations using contract utilities
	rawAbiData, configs, err := CreateRequiredEventConfigs(m.cfg, m.cfg.AbiPath)
	if err != nil {
		m.logger.Errorf("Failed to create event config from ABI: %v", err)
		return err
	}

	// Create event detector with configuration values and event repository
	m.detector = NewEventDetector(rawAbiData, configs, m.eventRepo,
		SetLastScannedBlock(uint64(m.cfg.LastScannedBlock)),
		SetConfirmationBlocks(uint64(m.cfg.ConfirmationBlocks)),
		SetBatchSize(uint64(m.cfg.BatchSize)),
		SetScanInterval(time.Duration(m.cfg.ScanIntervalSec)*time.Second))

	// Create event handler with event repository
	m.handler = NewEventHandler(m.eventRepo)

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
	}).Info("Started monitoring consensus events with periodic recovery")

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

// GetEventStatistics returns event processing statistics (now simplified)
func (m *EventManager) GetEventStatistics() (map[string]int64, error) {
	if m.eventRepo == nil {
		return nil, fmt.Errorf("event repository not initialized")
	}

	// Return simple statistics for the new system
	stats := make(map[string]int64)

	// Count deposits by status
	pendingDeposits, _ := m.eventRepo.ListDepositsByStatus(nil, "pending", 0)
	confirmedDeposits, _ := m.eventRepo.ListDepositsByStatus(nil, "confirmed", 0)
	stats["deposits_pending"] = int64(len(pendingDeposits))
	stats["deposits_confirmed"] = int64(len(confirmedDeposits))

	// Count proposers by status
	activeProposers, _ := m.eventRepo.ListProposersByStatus(nil, "ok")
	stats["proposers_active"] = int64(len(activeProposers))

	return stats, nil
}

// GetUtxoManagerStats returns UTXO manager statistics
// func (m *EventManager) GetUtxoManagerStats() map[string]interface{} {
// 	if m.utxoProcessor == nil {
// 		return map[string]interface{}{
// 			"status": "not_initialized",
// 		}
// 	}
// 	return m.utxoProcessor.GetStats()
// }
