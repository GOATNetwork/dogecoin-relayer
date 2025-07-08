package consensus

import (
	"context"
	"fmt"
	"time"

	log "github.com/sirupsen/logrus"

	"github.com/ethereum/go-ethereum/common"
	"github.com/goat-network/dogecoin-relayer/internal/config"
	"github.com/goat-network/dogecoin-relayer/internal/models"
	"github.com/goat-network/dogecoin-relayer/pkg/module"
	"github.com/goat-network/dogecoin-relayer/pkg/types"
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

var _ module.Module = (*EventManager)(nil)

func (m *EventManager) Name() string {
	return "event_manager"
}

func (m *EventManager) Init(cfg any, conn *models.DBConnection) error {
	m.cfg = cfg.(config.EventDetectionConfig)
	m.conn = conn
	m.eventRepo = models.NewEventRepository(conn.DB)
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

	// Start periodic recovery of failed events (every 30 minutes, max 3 retries)
	m.StartPeriodicRecovery(30, 3)

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

// GetEventStatistics returns event processing statistics
func (m *EventManager) GetEventStatistics() (map[string]int64, error) {
	if m.eventRepo == nil {
		return nil, fmt.Errorf("event repository not initialized")
	}
	return m.eventRepo.GetEventProcessingStatistics()
}

// GetRecentEvents returns recent detected events
func (m *EventManager) GetRecentEvents(status string, limit int) ([]models.DetectedEvent, error) {
	if m.eventRepo == nil {
		return nil, fmt.Errorf("event repository not initialized")
	}
	return m.eventRepo.GetDetectedEventsByStatus(status, limit)
}

// RecoverFailedEvents reprocesses failed events from database
func (m *EventManager) RecoverFailedEvents(maxRetries int) error {
	if m.eventRepo == nil {
		return fmt.Errorf("event repository not initialized")
	}

	failedEvents, err := m.eventRepo.GetDetectedEventsByStatus("failed", 100)
	if err != nil {
		return fmt.Errorf("failed to get failed events: %w", err)
	}

	if len(failedEvents) == 0 {
		return nil
	}

	m.logger.Infof("Found %d failed events to retry", len(failedEvents))

	for _, dbEvent := range failedEvents {
		// Check retry count in processing logs
		logs, err := m.eventRepo.GetProcessingLogs(dbEvent.ID)
		if err != nil {
			m.logger.Errorf("Failed to get processing logs for event %d: %v", dbEvent.ID, err)
			continue
		}

		retryCount := 0
		for _, log := range logs {
			if log.Status == "failed" {
				retryCount++
			}
		}

		if retryCount >= maxRetries {
			m.logger.Debugf("Skipping event %d - max retries exceeded (%d)", dbEvent.ID, retryCount)
			continue
		}

		// Convert back to DetectedEvent for reprocessing
		eventData, err := m.eventRepo.GetDetectedEventData(&dbEvent)
		if err != nil {
			m.logger.Errorf("Failed to parse event data for event %d: %v", dbEvent.ID, err)
			continue
		}

		detectedEvent := DetectedEvent{
			BlockNumber:     dbEvent.BlockNumber,
			TxHash:          common.HexToHash(dbEvent.TxHash),
			LogIndex:        dbEvent.LogIndex,
			ContractAddress: common.HexToAddress(dbEvent.ContractAddress),
			EventName:       dbEvent.EventName,
			EventData:       eventData,
			Timestamp:       dbEvent.ProcessedAt,
			DatabaseID:      dbEvent.ID,
		}

		// Reset status to pending for retry
		if err := m.eventRepo.UpdateDetectedEventStatus(dbEvent.ID, "pending"); err != nil {
			m.logger.Errorf("Failed to reset event status for retry: %v", err)
			continue
		}

		// Reprocess the event
		if err := m.handler.ProcessEvent(detectedEvent); err != nil {
			m.logger.Errorf("Failed to reprocess event %d: %v", dbEvent.ID, err)
		} else {
			m.logger.Infof("Successfully reprocessed event %d", dbEvent.ID)
		}
	}

	return nil
}

// StartPeriodicRecovery starts a goroutine that periodically recovers failed events
func (m *EventManager) StartPeriodicRecovery(intervalMinutes int, maxRetries int) {
	if intervalMinutes <= 0 {
		return
	}

	go func() {
		ticker := time.NewTicker(time.Duration(intervalMinutes) * time.Minute)
		defer ticker.Stop()

		for {
			select {
			case <-ticker.C:
				if err := m.RecoverFailedEvents(maxRetries); err != nil {
					m.logger.Errorf("Failed to recover failed events: %v", err)
				}
			}
		}
	}()

	m.logger.Infof("Started periodic recovery every %d minutes (max retries: %d)", intervalMinutes, maxRetries)
}

func init() {
	log.Info("Registering event manager module")
	module.RegisterModule(&EventManager{})
}
