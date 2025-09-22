package consensus

import (
	"context"
	"fmt"
	"math/big"
	"sync"
	"time"

	"github.com/ethereum/go-ethereum/common"
	"github.com/goat-network/dogecoin-relayer/internal/models"
	"github.com/goat-network/dogecoin-relayer/internal/p2p"
	"github.com/goat-network/dogecoin-relayer/internal/tss"
	"github.com/goat-network/dogecoin-relayer/pkg/contract"
	"github.com/goat-network/dogecoin-relayer/pkg/eventbus"
	"github.com/goat-network/dogecoin-relayer/pkg/global"
	"github.com/goat-network/dogecoin-relayer/pkg/module"
	"github.com/goat-network/dogecoin-relayer/pkg/types"
	log "github.com/sirupsen/logrus"
)

// BridgeInBatch represents a batch of bridge transactions
type BridgeInBatch struct {
	ID                *big.Int
	TransactionParams []contract.BridgeTransaction
	TotalAmount       *big.Int
	UTXOs             []*models.UTXO
}

// withdrawalRequest represents a single withdrawal request with multiple outputs aligned to task IDs
type withdrawalRequest struct {
	ID          *big.Int
	UTXO        *models.UTXO // Single UTXO with multiple outputs
	TotalAmount *big.Int
	TaskIds     []*big.Int // Task IDs aligned with VOUT outputs
}

// pendingBatch stores batch data while waiting for TSS signature
type pendingBatch struct {
	batchType         string // "deposit" or "withdrawal"
	depositBatch      *BridgeInBatch
	withdrawalRequest *withdrawalRequest
	calldata          []byte
	utxos             []*models.UTXO
}

// UtxoProcessor manages UTXO processing for bridge operations
type UtxoProcessor struct {
	conn            *models.DBConnection
	state           *models.StateRepository
	logger          *log.Entry
	eventBus        *eventbus.Bus
	contractBuilder *contract.Contract
	bridgeContract  common.Address
	abiPath         string
	tssClient       *tss.SignClient
	chainID         *big.Int
	p2pModule       *p2p.P2PModule // Reference to P2P module for accessing public key

	// Current proposer state
	currentProposer common.Address // Cached from SubmitterChosen events
	proposerSet     bool           // Whether we have received proposer info

	// Pending batches waiting for TSS signatures
	pendingBatches sync.Map // sessionID -> *pendingBatch

	// Polling configuration
	pollInterval    time.Duration
	batchSize       int
	lastProcessedId uint

	// Control
	ctx       context.Context
	cancel    context.CancelFunc
	isRunning bool
}

// NewUtxoProcessor creates a new UTXO processor
func NewUtxoProcessor(conn *models.DBConnection, bridgeContractAddress, abiPath string) *UtxoProcessor {
	ctx, cancel := context.WithCancel(context.Background())

	up := &UtxoProcessor{
		conn:            conn,
		state:           models.NewStateRepository(conn.GetDB()),
		logger:          types.InitLogEntry("utxo-processor"),
		eventBus:        global.GetEventBus(),
		bridgeContract:  common.HexToAddress(bridgeContractAddress),
		abiPath:         abiPath,
		pollInterval:    10 * time.Second, // Poll every 10 seconds
		batchSize:       1,                // Process 1 UTXO at a time for debugging
		lastProcessedId: 0,
		ctx:             ctx,
		cancel:          cancel,
		isRunning:       false,
	}

	return up
}

// SetContractBuilder sets the contract builder for generating calldata
func (up *UtxoProcessor) SetContractBuilder(builder *contract.Contract) {
	up.contractBuilder = builder
}

// SetTssClient sets the TSS client for signing transactions
func (up *UtxoProcessor) SetTssClient(client *tss.SignClient) {
	up.tssClient = client
}

// SetChainID sets the chain ID for transaction signing
func (up *UtxoProcessor) SetChainID(chainID *big.Int) {
	up.chainID = chainID
}

// Start begins the UTXO monitoring process
func (up *UtxoProcessor) Start() error {
	if up.isRunning {
		return fmt.Errorf("UTXO manager is already running")
	}

	// Create contract builder for generating bridge calldata
	contractBuilder, err := contract.NewEntryPoint(
		up.bridgeContract,
		GetEthClient(),
		up.abiPath,
	)
	if err != nil {
		return fmt.Errorf("failed to create contract builder: %w", err)
	}

	// Set the contract builder in the UTXO manager
	up.SetContractBuilder(contractBuilder)
	up.logger.Info("Contract builder set successfully")

	// Get TSS client from the TSS module if TSS is enabled
	globalCfg := global.GetConfig()
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

		up.SetTssClient(tssClient)

		// Set chain ID from consensus configuration
		if globalCfg.Consensus.ChainId == 0 {
			return fmt.Errorf("invalid chain ID in consensus configuration")
		}
		chainID := big.NewInt(int64(globalCfg.Consensus.ChainId))
		up.SetChainID(chainID)
		up.logger.Infof("Chain ID set to %d", globalCfg.Consensus.ChainId)

		up.logger.Info("TSS client configured for UTXO processor")
	} else {
		up.logger.Warn("TSS is not enabled, bridge transactions will not be signed")
	}

	up.isRunning = true
	up.logger.Info("Starting UTXO manager")

	up.registerP2PHandler()
	// Subscribe to TSS signature responses
	up.eventBus.Subscribe(eventbus.EventTssSigResponse, up.handleTssSignature)
	// Subscribe to SubmitterChosen events to track current proposer
	up.eventBus.Subscribe(eventbus.EventSubmitterChosen, up.handleSubmitterChosen)

	// Start the polling loop
	go up.pollLoop()

	// Start pending batch monitoring and retry loop
	go up.pendingBatchRetryLoop()

	return nil
}

// Stop stops the UTXO manager
func (up *UtxoProcessor) Stop() {
	if !up.isRunning {
		return
	}

	up.logger.Info("Stopping UTXO manager")
	// Unsubscribe from events
	up.eventBus.Unsubscribe(eventbus.EventTssSigResponse, up.handleTssSignature)
	up.eventBus.Unsubscribe(eventbus.EventSubmitterChosen, up.handleSubmitterChosen)
	up.cancel()
	up.isRunning = false
}

// handleSubmitterChosen handles SubmitterChosen events from the event bus
func (up *UtxoProcessor) handleSubmitterChosen(data any) {
	event, ok := data.(BlockchainEvent)
	if !ok {
		up.logger.Errorf("Invalid SubmitterChosen event data type: %T", data)
		return
	}

	// Extract proposer address from event data
	if event.EventData == nil {
		up.logger.Errorf("SubmitterChosen event has no data")
		return
	}

	// Parse the submitter address from event data
	// The event data should contain the chosen submitter address
	var submitterAddr string

	// Debug: Log the actual types of event data
	for key, value := range event.EventData {
		up.logger.Infof("Event data debug: key=%s, value=%v, type=%T", key, value, value)
	}

	// Try different field names and types
	if addr, ok := event.EventData["submitter"].(string); ok {
		submitterAddr = addr
	} else if addr, ok := event.EventData["chosen"].(string); ok {
		submitterAddr = addr
	} else if addr, ok := event.EventData["newSubmitter"].(string); ok {
		submitterAddr = addr
	} else if addr, ok := event.EventData["submitter"].(common.Address); ok {
		submitterAddr = addr.Hex()
	} else if addr, ok := event.EventData["chosen"].(common.Address); ok {
		submitterAddr = addr.Hex()
	} else if addr, ok := event.EventData["newSubmitter"].(common.Address); ok {
		submitterAddr = addr.Hex()
	} else {
		up.logger.Errorf("Could not extract submitter address from SubmitterChosen event: %+v", event.EventData)
		return
	}

	if submitterAddr != "" {
		newProposer := common.HexToAddress(submitterAddr)
		up.updateCurrentProposer(newProposer)
	}
}

// updateCurrentProposer updates the current proposer and logs the change
func (up *UtxoProcessor) updateCurrentProposer(newProposer common.Address) {
	oldProposer := up.currentProposer
	up.currentProposer = newProposer
	up.proposerSet = true

	if oldProposer != newProposer {
		up.logger.Infof("Proposer updated: %s → %s", oldProposer.Hex(), newProposer.Hex())
	}
}

// isCurrentProposer checks if this node is the current proposer using cached state
func (up *UtxoProcessor) isCurrentProposer() (bool, error) {
	// Get this node's Ethereum address
	nodeAddress, err := up.getNodeEthereumAddress()
	if err != nil {
		return false, fmt.Errorf("failed to get node address: %w", err)
	}

	// If we haven't received proposer info yet, fall back to contract query
	if !up.proposerSet {
		up.logger.Debug("No proposer info from events yet, querying contract...")
		// TODO: Query contract as fallback
		// For now, return false to skip processing until we get events
		return false, nil
	}

	isProposer := up.currentProposer == nodeAddress
	up.logger.Infof("Proposer check: current=%s, node=%s, isProposer=%v",
		up.currentProposer.Hex(), nodeAddress.Hex(), isProposer)

	return isProposer, nil
}
