package consensus

import (
	"context"
	"fmt"
	"math/big"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/crypto"
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
	baseSessionID     string
	currentSessionID  string
	nextAttempt       int
	lastAttempt       time.Time
	nextRetryAt       time.Time
	tssNonce          *big.Int
}

const proposerRefreshInterval = 30 * time.Second

func (up *UtxoProcessor) assignNewTssSession(pending *pendingBatch) string {
	attempt := pending.nextAttempt
	sessionID := pending.baseSessionID
	if attempt > 0 {
		sessionID = fmt.Sprintf("%s-%d", pending.baseSessionID, attempt)
	}

	// Update attempt counters for next round
	if pending.currentSessionID != "" {
		up.tssSessionAliases.Delete(pending.currentSessionID)
	}
	pending.currentSessionID = sessionID
	pending.nextAttempt++
	pending.lastAttempt = time.Now()
	retryDelay := up.tssRequestRetryBackoff
	if retryDelay <= 0 {
		retryDelay = 15 * time.Second
	}
	pending.nextRetryAt = pending.lastAttempt.Add(retryDelay)

	up.tssSessionAliases.Store(sessionID, pending.baseSessionID)
	return sessionID
}

func (up *UtxoProcessor) registerExistingTssSession(pending *pendingBatch, sessionID string) {
	base := pending.baseSessionID
	if base == "" {
		base = sessionID
		pending.baseSessionID = base
	}
	if attempt, ok := extractAttemptIndex(base, sessionID); ok {
		if attempt+1 > pending.nextAttempt {
			pending.nextAttempt = attempt + 1
		}
	}
	if pending.currentSessionID != "" && pending.currentSessionID != sessionID {
		up.tssSessionAliases.Delete(pending.currentSessionID)
	}
	pending.currentSessionID = sessionID
	up.tssSessionAliases.Store(sessionID, base)
}

// UtxoProcessor manages UTXO processing for bridge operations
type UtxoProcessor struct {
	conn               *models.DBConnection
	state              *models.StateRepository
	logger             *log.Entry
	eventBus           *eventbus.Bus
	contractBuilder    *contract.Contract
	bridgeContract     common.Address
	entryPointContract common.Address
	abiPath            string
	tssClient          *tss.SignClient
	chainID            *big.Int
	p2pModule          *p2p.P2PModule // Reference to P2P module for accessing public key

	// Current proposer state
	currentProposer     common.Address // Cached from ProposerSelected events
	proposerSet         bool           // Whether we have received proposer info
	lastProposerRefresh time.Time

	// Pending batches waiting for TSS signatures
	pendingBatches    sync.Map // base session ID -> *pendingBatch
	tssSessionAliases sync.Map // actual TSS session ID -> base session ID

	// Retry configuration
	tssRequestMaxRetries   int
	tssRequestRetryBackoff time.Duration

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
func NewUtxoProcessor(conn *models.DBConnection, bridgeContractAddress, entryPointAddress, abiPath string) *UtxoProcessor {
	ctx, cancel := context.WithCancel(context.Background())

	up := &UtxoProcessor{
		conn:                   conn,
		state:                  models.NewStateRepository(conn.GetDB()),
		logger:                 types.InitLogEntry("utxo-processor"),
		eventBus:               global.GetEventBus(),
		bridgeContract:         common.HexToAddress(bridgeContractAddress),
		entryPointContract:     common.HexToAddress(entryPointAddress),
		abiPath:                abiPath,
		pollInterval:           10 * time.Second, // Poll every 10 seconds
		batchSize:              1,                // Process 1 UTXO at a time for debugging
		lastProcessedId:        0,
		ctx:                    ctx,
		cancel:                 cancel,
		isRunning:              false,
		tssRequestMaxRetries:   3,
		tssRequestRetryBackoff: 60 * time.Second,
	}

	return up
}

func extractAttemptIndex(baseSession, currentSession string) (int, bool) {
	if baseSession == "" {
		return 0, false
	}
	if currentSession == baseSession {
		return 0, true
	}
	if !strings.HasPrefix(currentSession, baseSession+"-") {
		return 0, false
	}
	suffix := currentSession[len(baseSession)+1:]
	value, err := strconv.Atoi(suffix)
	if err != nil {
		return 0, false
	}
	return value, true
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
		up.entryPointContract,
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
	// Subscribe to proposer selection events to track current proposer
	up.eventBus.Subscribe(eventbus.EventProposerSelected, up.handleProposerSelected)

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
	up.eventBus.Unsubscribe(eventbus.EventProposerSelected, up.handleProposerSelected)
	up.cancel()
	up.isRunning = false
}

// handleProposerSelected handles ProposerSelected events from the event bus
func (up *UtxoProcessor) handleProposerSelected(data any) {
	event, ok := data.(BlockchainEvent)
	if !ok {
		up.logger.Errorf("Invalid ProposerSelected event data type: %T", data)
		return
	}

	// Extract proposer address from event data
	if event.EventData == nil {
		up.logger.Errorf("ProposerSelected event has no data")
		return
	}

	// Parse the proposer address from event data
	// The event data should contain the chosen proposer address
	var proposerAddr string

	// Debug: Log the actual types of event data
	for key, value := range event.EventData {
		up.logger.Infof("Event data debug: key=%s, value=%v, type=%T", key, value, value)
	}

	// Try different field names and types
	if addr, ok := event.EventData["proposer"].(string); ok {
		proposerAddr = addr
	} else if addr, ok := event.EventData["chosen"].(string); ok {
		proposerAddr = addr
	} else if addr, ok := event.EventData["newProposer"].(string); ok {
		proposerAddr = addr
	} else if addr, ok := event.EventData["proposer"].(common.Address); ok {
		proposerAddr = addr.Hex()
	} else if addr, ok := event.EventData["chosen"].(common.Address); ok {
		proposerAddr = addr.Hex()
	} else if addr, ok := event.EventData["newProposer"].(common.Address); ok {
		proposerAddr = addr.Hex()
	} else {
		up.logger.Errorf("Could not extract proposer address from ProposerSelected event: %+v", event.EventData)
		return
	}

	if proposerAddr != "" {
		newProposer := common.HexToAddress(proposerAddr)
		up.updateCurrentProposer(newProposer)
	}
}

// updateCurrentProposer updates the current proposer and logs the change
func (up *UtxoProcessor) updateCurrentProposer(newProposer common.Address) {
	oldProposer := up.currentProposer
	up.currentProposer = newProposer
	up.proposerSet = true
	up.lastProposerRefresh = time.Now()

	if oldProposer != newProposer {
		up.logger.Infof("Proposer updated: %s → %s", oldProposer.Hex(), newProposer.Hex())
	}
}

func (up *UtxoProcessor) refreshProposerFromContract() error {
	if up.contractBuilder == nil {
		return fmt.Errorf("contract builder not configured")
	}

	proposer, err := up.contractBuilder.GetCurrentProposer()
	if err != nil {
		return fmt.Errorf("failed to query current proposer: %w", err)
	}

	up.updateCurrentProposer(proposer)
	return nil
}

// isCurrentProposer checks if this node is the current proposer using cached state
func (up *UtxoProcessor) isCurrentProposer() (bool, error) {
	// Get this node's Ethereum address
	nodeAddress, err := up.getNodeEthereumAddress()
	if err != nil {
		return false, fmt.Errorf("failed to get node address: %w", err)
	}

	needsRefresh := !up.proposerSet || time.Since(up.lastProposerRefresh) > proposerRefreshInterval
	if needsRefresh {
		if err := up.refreshProposerFromContract(); err != nil {
			up.logger.Warnf("Failed to refresh proposer from contract: %v", err)
		}
	}

	if !up.proposerSet {
		up.logger.Debug("Proposer not set; skipping processing until refresh succeeds")
		return false, nil
	}

	isProposer := up.currentProposer == nodeAddress
	up.logger.Infof("Proposer check: current=%s, node=%s, isProposer=%v",
		up.currentProposer.Hex(), nodeAddress.Hex(), isProposer)

	return isProposer, nil
}

func (up *UtxoProcessor) fetchTssNonce() (*big.Int, error) {
	if up.contractBuilder == nil {
		return nil, fmt.Errorf("contract builder not configured")
	}

	ctx, cancel := context.WithTimeout(up.ctx, 10*time.Second)
	defer cancel()

	nonce, err := up.contractBuilder.GetTssNonce(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to fetch tss nonce: %w", err)
	}

	return nonce, nil
}

func (up *UtxoProcessor) computeVerifyAndCallDigest(calldata []byte, tssNonce *big.Int) ([]byte, [32]byte, error) {
	if up.chainID == nil {
		return nil, [32]byte{}, fmt.Errorf("chain ID not set")
	}
	if tssNonce == nil {
		return nil, [32]byte{}, fmt.Errorf("tss nonce not provided")
	}

	targets := []common.Address{up.bridgeContract}
	calldataArray := [][]byte{calldata}

	hash, err := contract.CreateVerifyAndCallHash(targets, calldataArray, tssNonce, up.chainID)
	if err != nil {
		return nil, [32]byte{}, fmt.Errorf("failed to create verifyAndCall hash: %w", err)
	}

	prefix := []byte("\x19Ethereum Signed Message:\n32")
	digest := crypto.Keccak256(prefix, hash[:])

	return digest, hash, nil
}

// cleanupStaleSessions removes pending sessions that exhausted retries or sat idle too long.
func (up *UtxoProcessor) cleanupStaleSessions() {
	now := time.Now()
	maxAge := up.tssRequestRetryBackoff * time.Duration(up.tssRequestMaxRetries+1)
	if maxAge <= 0 {
		maxAge = 10 * time.Minute
	}

	up.pendingBatches.Range(func(key, value interface{}) bool {
		baseID, ok := key.(string)
		if !ok {
			return true
		}

		pending, ok := value.(*pendingBatch)
		if !ok {
			up.pendingBatches.Delete(baseID)
			return true
		}

		// Remove batches that already hit retry ceiling.
		if up.tssRequestMaxRetries > 0 && pending.nextAttempt >= up.tssRequestMaxRetries {
			up.logger.Warnf("Removing %s session %s after exhausting retries", pending.batchType, baseID)
			up.removePendingSession(baseID, pending)
			return true
		}

		// Remove batches that have been idle for too long.
		if !pending.lastAttempt.IsZero() && now.Sub(pending.lastAttempt) > maxAge {
			up.logger.Warnf("Cleaning up stale %s session %s (last attempt %s ago)", pending.batchType, baseID, now.Sub(pending.lastAttempt))
			up.removePendingSession(baseID, pending)
		}
		return true
	})
}

func (up *UtxoProcessor) removePendingSession(baseID string, pending *pendingBatch) {
	up.pendingBatches.Delete(baseID)
	if pending == nil {
		return
	}
	if pending.currentSessionID != "" {
		up.tssSessionAliases.Delete(pending.currentSessionID)
	}
	if pending.baseSessionID != "" && pending.baseSessionID != baseID {
		up.tssSessionAliases.Delete(pending.baseSessionID)
	}
}
