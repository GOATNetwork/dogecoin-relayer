package consensus

import (
	"context"
	"fmt"
	"time"

	"github.com/goat-network/dogecoin-relayer/internal/config"
	"github.com/goat-network/dogecoin-relayer/internal/models"
	"github.com/goat-network/dogecoin-relayer/pkg/eventbus"
	"github.com/goat-network/dogecoin-relayer/pkg/global"
	"github.com/goat-network/dogecoin-relayer/pkg/module"
	"github.com/goat-network/dogecoin-relayer/pkg/types"
	log "github.com/sirupsen/logrus"
)

// ConsensusModule manages the overall consensus functionality
type ConsensusModule struct {
	cfg           config.ConsensusConfig
	conn          *models.DBConnection
	logger        *log.Entry
	eventManager  *EventManager
	utxoProcessor *UtxoProcessor
	eventBus      *eventbus.Bus
}

// Ensure ConsensusModule implements the Module interface
var _ module.Module = (*ConsensusModule)(nil)

func (c *ConsensusModule) Name() string {
	return "consensus"
}

func (c *ConsensusModule) Init(cfg any, conn *models.DBConnection) error {
	c.cfg = cfg.(config.ConsensusConfig)
	c.conn = conn
	c.logger = types.InitLogEntry(c.Name())
	c.eventBus = global.GetEventBus()

	// Initialize the Ethereum client with the RPC URL from consensus config
	if c.cfg.Rpc != "" {
		if err := InitEthClient(c.cfg.Rpc); err != nil {
			c.logger.Errorf("Failed to initialize Ethereum client: %v", err)
			return fmt.Errorf("failed to initialize Ethereum client: %w", err)
		}

		// Verify the Ethereum client is working by getting the latest block number
		client := GetEthClient()
		if client == nil {
			c.logger.Error("Ethereum client is nil after initialization")
			return fmt.Errorf("ethereum client is nil after initialization")
		}

		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()

		latestBlock, err := client.BlockNumber(ctx)
		if err != nil {
			c.logger.Errorf("Failed to get latest block number: %v", err)
			return fmt.Errorf("failed to get latest block number: %w", err)
		}

		c.logger.Infof("Ethereum client verified - latest block number: %d", latestBlock)
	}

	// Initialize the EventManager with the EventDetectionConfig subset
	eventManager, err := NewEventManager(c.cfg.EventDetection, conn)
	if err != nil {
		c.logger.Errorf("Failed to initialize event manager: %v", err)
		return fmt.Errorf("failed to initialize event manager: %w", err)
	}
	c.eventManager = eventManager

	// Initialize the UTXO processor
	c.utxoProcessor = NewUtxoProcessor(conn, c.cfg.EventDetection.ContractBridge, c.cfg.EventDetection.AbiPath)

	c.logger.Info("Consensus module initialized successfully")
	return nil
}

func (c *ConsensusModule) Run(ctx context.Context) error {
	c.logger.Info("Consensus module running")

	// Start the event manager
	if err := c.eventManager.Run(ctx); err != nil {
		return fmt.Errorf("failed to run event manager: %w", err)
	}

	// Start UTXO manager for bridge operations
	if err := c.utxoProcessor.Start(); err != nil {
		c.logger.Errorf("Failed to start UTXO manager: %v", err)
		return err
	}

	// start health check
	go HealthCheck(ctx)

	c.logger.Info("Consensus module started successfully")
	return nil
}

func (c *ConsensusModule) Shutdown(ctx context.Context) error {
	c.logger.Info("Consensus module shutting down")

	// Shutdown the event manager
	if c.eventManager != nil {
		if err := c.eventManager.Shutdown(ctx); err != nil {
			c.logger.Errorf("Failed to shutdown event manager: %v", err)
		}
	}

	// Close the Ethereum client
	CloseEthClient()

	c.logger.Info("Consensus module shutdown complete")
	return nil
}

func init() {
	log.Info("Registering consensus module")
	module.RegisterModule(&ConsensusModule{})
}
