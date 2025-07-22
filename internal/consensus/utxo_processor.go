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
	"github.com/goat-network/dogecoin-relayer/pkg/types"
	log "github.com/sirupsen/logrus"
)

// bridgeInBatch represents a batch of bridge transactions
type bridgeInBatch struct {
	ID                *big.Int
	TransactionParams []contract.BridgeTransaction
	TotalAmount       *big.Int
	UTXOs             []*models.UTXO
}

// bridgeOutBatch represents a batch of withdrawal transactions
type bridgeOutBatch struct {
	ID          *big.Int
	UTXOs       []*models.UTXO
	TotalAmount *big.Int
	TaskIds     []*big.Int
}

// pendingBatch stores batch data while waiting for TSS signature
type pendingBatch struct {
	batchType     string // "deposit" or "withdrawal"
	depositBatch  *bridgeInBatch
	withdrawBatch *bridgeOutBatch
	calldata      []byte
	utxos         []*models.UTXO
}

// UtxoProcessor manages UTXO processing for bridge operations
type UtxoProcessor struct {
	conn            *models.DBConnection
	state           *models.StateRepository
	logger          *log.Entry
	eventBus        *eventbus.Bus
	contractBuilder *contract.Contract
	bridgeContract  common.Address
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
func NewUtxoProcessor(conn *models.DBConnection, bridgeContractAddress string) *UtxoProcessor {
	ctx, cancel := context.WithCancel(context.Background())

	up := &UtxoProcessor{
		conn:            conn,
		state:           models.NewStateRepository(conn.GetDB()),
		logger:          types.InitLogEntry("utxo-processor"),
		eventBus:        global.GetEventBus(),
		bridgeContract:  common.HexToAddress(bridgeContractAddress),
		pollInterval:    10 * time.Second, // Poll every 10 seconds
		batchSize:       10,               // Process 10 UTXOs at a time
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

	up.isRunning = true
	up.logger.Info("Starting UTXO manager")

	up.registerP2PHandler()
	// Subscribe to TSS signature responses
	up.eventBus.Subscribe(eventbus.EventTssSigResponse, up.handleTssSignature)
	// Subscribe to SubmitterChosen events to track current proposer
	up.eventBus.Subscribe(eventbus.EventSubmitterChosen, up.handleSubmitterChosen)

	// Initialize current proposer from contract
	if err := up.initializeCurrentProposer(); err != nil {
		up.logger.Warnf("Failed to initialize current proposer: %v", err)
		// Continue anyway - will be updated when events come in
	}

	// Start the polling loop
	go up.pollLoop()

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
	event, ok := data.(DetectedEvent)
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
	if submitterAddr, ok := event.EventData["submitter"].(string); ok {
		newProposer := common.HexToAddress(submitterAddr)
		up.updateCurrentProposer(newProposer)
	} else if submitterAddr, ok := event.EventData["chosen"].(string); ok {
		newProposer := common.HexToAddress(submitterAddr)
		up.updateCurrentProposer(newProposer)
	} else {
		up.logger.Errorf("Could not extract submitter address from SubmitterChosen event: %+v", event.EventData)
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

// initializeCurrentProposer queries the contract for current proposer on startup
func (up *UtxoProcessor) initializeCurrentProposer() error {
	// TODO: Query the contract for the current proposer
	// This is only called once on startup
	currentProposer, err := up.contractBuilder.GetCurrentProposer()
	if err != nil {
		return err
	}
	up.updateCurrentProposer(currentProposer)

	up.logger.Debug("Current proposer will be set from SubmitterChosen events")
	return nil
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
	up.logger.Debugf("Proposer check: current=%s, node=%s, isProposer=%v",
		up.currentProposer.Hex(), nodeAddress.Hex(), isProposer)

	return isProposer, nil
}
