package consensus

import (
	"context"
	"encoding/hex"
	"fmt"
	"math/big"
	"time"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/crypto"
	"github.com/goat-network/dogecoin-relayer/internal/config"
	"github.com/goat-network/dogecoin-relayer/internal/models"
	"github.com/goat-network/dogecoin-relayer/internal/tss"
	"github.com/goat-network/dogecoin-relayer/pkg/contract"
	"github.com/goat-network/dogecoin-relayer/pkg/global"
	"github.com/goat-network/dogecoin-relayer/pkg/module"
	"github.com/goat-network/dogecoin-relayer/pkg/types"
	log "github.com/sirupsen/logrus"
)

// EventManager manages consensus-related events
type EventManager struct {
	cfg           config.EventDetectionConfig
	conn          *models.DBConnection
	eventRepo     *models.EventRepository
	logger        *log.Entry
	utxoProcessor *UtxoProcessor

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
	m.eventRepo = models.NewEventRepository(conn.GetDB())
	m.logger = types.InitLogEntry(m.Name())

	// Initialize UTXO manager
	m.utxoProcessor = NewUtxoProcessor(conn, m.cfg.ContractBridge)

	return nil
}

func (m *EventManager) Run(ctx context.Context) error {
	m.logger.Info("Event manager module running")

	// Start UTXO manager for bridge operations
	if err := m.startUtxoProcessor(); err != nil {
		m.logger.Errorf("Failed to start UTXO manager: %v", err)
		return err
	}

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
	if m.utxoProcessor != nil {
		m.utxoProcessor.Stop()
	}
	return nil
}

// startUtxoProcessor starts the UTXO manager for bridge operations
func (m *EventManager) startUtxoProcessor() error {
	if m.utxoProcessor == nil {
		return fmt.Errorf("UTXO manager not initialized")
	}

	// Create contract builder for generating bridge calldata
	contractBuilder, err := contract.NewEntryPoint(
		common.HexToAddress(m.cfg.ContractBridge),
		GetEthClient(),
		m.cfg.AbiPath,
	)
	if err != nil {
		return fmt.Errorf("failed to create contract builder: %w", err)
	}

	// Set the contract builder in the UTXO manager
	m.utxoProcessor.SetContractBuilder(contractBuilder)

	// Get node's Ethereum address for proposer management
	globalCfg := global.GetConfig()

	// Create ProposerManager
	if globalCfg != nil && globalCfg.P2P.ProposerPrivateKey != "" {
		// Get node address from P2P private key
		privKeyBytes, err := hex.DecodeString(globalCfg.P2P.ProposerPrivateKey)
		if err != nil {
			return fmt.Errorf("invalid proposer private key: %w", err)
		}

		privKey := crypto.ToECDSAUnsafe(privKeyBytes)
		nodeAddress := crypto.PubkeyToAddress(privKey.PublicKey)

		// Create initial validator set (for now, just this node)
		// TODO: Get actual validator set from configuration or contract
		initialValidators := []common.Address{nodeAddress}

		// Create contract querier
		contractQuerier := NewDefaultContractQuerier(contractBuilder, common.HexToAddress(m.cfg.ContractEntryPoint))

		// Create proposer manager configuration
		pmConfig := ProposerManagerConfig{
			NodeAddress:          nodeAddress,
			VerificationInterval: 5 * time.Minute,  // Verify with contract every 5 minutes
			ProposerTimeout:      15 * time.Minute, // Timeout inactive proposer after 15 minutes
			InitialValidators:    initialValidators,
		}

		// Create and set proposer manager
		proposerManager := NewProposerManager(pmConfig, contractQuerier, global.GetEventBus())
		m.utxoProcessor.SetProposerManager(proposerManager)

		m.logger.Info("ProposerManager created and configured")
	} else {
		m.logger.Warn("ProposerManager not created - P2P private key not configured")
	}

	// Get TSS client from the TSS module if TSS is enabled
	if globalCfg != nil && globalCfg.Tss.Enabled {
		// Get the TSS module from the module registry
		tssModule, exists := module.GetModule("tss")
		if !exists {
			return fmt.Errorf("TSS module not found in registry")
		}

		// Cast to TssModule and get the sign client
		tssModuleInstance, ok := tssModule.(*tss.TssModule)
		if !ok {
			return fmt.Errorf("failed to cast TSS module to TssModule type")
		}

		tssClient := tssModuleInstance.GetSignClient()
		if tssClient == nil {
			return fmt.Errorf("TSS sign client not initialized")
		}

		m.utxoProcessor.SetTssClient(tssClient)

		// Set chain ID from consensus configuration
		if globalCfg.Consensus.ChainId > 0 {
			chainID := big.NewInt(int64(globalCfg.Consensus.ChainId))
			m.utxoProcessor.SetChainID(chainID)
		} else {
			m.logger.Warn("Chain ID not configured, TSS transaction signing may fail")
		}

		m.logger.Info("TSS client configured for UTXO processor from TSS module")
	} else {
		m.logger.Warn("TSS is not enabled, bridge transactions will not be signed")
	}

	// Start the UTXO manager
	if err := m.utxoProcessor.Start(); err != nil {
		return fmt.Errorf("failed to start UTXO manager: %w", err)
	}

	m.logger.Info("UTXO manager started successfully")
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

// GetUtxoManagerStats returns UTXO manager statistics
func (m *EventManager) GetUtxoManagerStats() map[string]interface{} {
	if m.utxoProcessor == nil {
		return map[string]interface{}{
			"status": "not_initialized",
		}
	}
	return m.utxoProcessor.GetStats()
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
