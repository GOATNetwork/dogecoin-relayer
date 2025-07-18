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

// bridgeBatch represents a batch of bridge transactions
type bridgeBatch struct {
	ID           *big.Int
	Transactions []contract.BridgeTransaction
	TotalAmount  *big.Int
	UTXOs        []*models.UTXO
}

// pendingBatch stores batch data while waiting for TSS signature
type pendingBatch struct {
	batch    *bridgeBatch
	calldata []byte
	utxos    []*models.UTXO
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

	// Pending batches waiting for TSS signatures
	pendingBatches sync.Map // sessionID -> *pendingBatch

	// Polling configuration
	pollInterval    time.Duration
	batchSize       int
	lastProcessedId uint
	// activeSessions sync.Map

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
	up.cancel()
	up.isRunning = false
}
