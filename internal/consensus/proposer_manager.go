package consensus

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/ethereum/go-ethereum/common"
	"github.com/goat-network/dogecoin-relayer/pkg/contract"
	"github.com/goat-network/dogecoin-relayer/pkg/eventbus"
	"github.com/goat-network/dogecoin-relayer/pkg/types"
	log "github.com/sirupsen/logrus"
)

// ProposerRotationStrategy defines how proposers are rotated
type ProposerRotationStrategy interface {
	// GetCurrentProposer returns the current proposer
	GetCurrentProposer() common.Address
	// NextProposer rotates to the next proposer and returns it
	NextProposer() common.Address
	// SetProposer explicitly sets the current proposer (for contract sync)
	SetProposer(proposer common.Address) error
	// GetValidators returns the list of validators
	GetValidators() []common.Address
	// AddValidator adds a new validator to the rotation
	AddValidator(validator common.Address)
	// RemoveValidator removes a validator from the rotation
	RemoveValidator(validator common.Address) bool
}

// RoundRobinRotation implements round-robin proposer rotation
type RoundRobinRotation struct {
	validators   []common.Address
	currentIndex int
	mutex        sync.RWMutex
}

// NewRoundRobinRotation creates a new round-robin rotation strategy
func NewRoundRobinRotation(validators []common.Address) *RoundRobinRotation {
	validatorsCopy := make([]common.Address, len(validators))
	copy(validatorsCopy, validators)
	return &RoundRobinRotation{
		validators:   validatorsCopy,
		currentIndex: 0,
	}
}

func (r *RoundRobinRotation) GetCurrentProposer() common.Address {
	r.mutex.RLock()
	defer r.mutex.RUnlock()

	if len(r.validators) == 0 {
		return common.Address{}
	}
	return r.validators[r.currentIndex]
}

func (r *RoundRobinRotation) NextProposer() common.Address {
	r.mutex.Lock()
	defer r.mutex.Unlock()

	if len(r.validators) == 0 {
		return common.Address{}
	}

	r.currentIndex = (r.currentIndex + 1) % len(r.validators)
	return r.validators[r.currentIndex]
}

func (r *RoundRobinRotation) SetProposer(proposer common.Address) error {
	r.mutex.Lock()
	defer r.mutex.Unlock()

	for i, validator := range r.validators {
		if validator == proposer {
			r.currentIndex = i
			return nil
		}
	}

	return fmt.Errorf("proposer %s not found in validator set", proposer.Hex())
}

func (r *RoundRobinRotation) GetValidators() []common.Address {
	r.mutex.RLock()
	defer r.mutex.RUnlock()

	result := make([]common.Address, len(r.validators))
	copy(result, r.validators)
	return result
}

func (r *RoundRobinRotation) AddValidator(validator common.Address) {
	r.mutex.Lock()
	defer r.mutex.Unlock()

	// Check if validator already exists
	for _, v := range r.validators {
		if v == validator {
			return
		}
	}

	r.validators = append(r.validators, validator)
}

func (r *RoundRobinRotation) RemoveValidator(validator common.Address) bool {
	r.mutex.Lock()
	defer r.mutex.Unlock()

	for i, v := range r.validators {
		if v == validator {
			// Remove validator
			r.validators = append(r.validators[:i], r.validators[i+1:]...)

			// Adjust current index if necessary
			if i < r.currentIndex {
				r.currentIndex--
			} else if i == r.currentIndex && len(r.validators) > 0 {
				r.currentIndex = r.currentIndex % len(r.validators)
			}

			return true
		}
	}

	return false
}

// ProposerManager manages proposer selection with hybrid approach
type ProposerManager struct {
	// Configuration
	nodeAddress          common.Address
	verificationInterval time.Duration
	proposerTimeout      time.Duration

	// State
	rotationStrategy  ProposerRotationStrategy
	lastVerifiedBlock uint64
	lastVerification  time.Time
	lastActivity      time.Time
	initialized       bool

	// External dependencies
	contractQuerier ContractQuerier
	eventBus        *eventbus.Bus
	logger          *log.Entry

	// Synchronization
	mutex  sync.RWMutex
	ctx    context.Context
	cancel context.CancelFunc
}

// ContractQuerier interface for querying proposer from contract
type ContractQuerier interface {
	GetCurrentProposer() (common.Address, error)
	GetValidatorSet() ([]common.Address, error)
}

// ProposerManagerConfig holds configuration for ProposerManager
type ProposerManagerConfig struct {
	NodeAddress          common.Address
	VerificationInterval time.Duration // How often to verify with contract
	ProposerTimeout      time.Duration // How long to wait before rotating inactive proposer
	InitialValidators    []common.Address
}

// NewProposerManager creates a new proposer manager
func NewProposerManager(config ProposerManagerConfig, contractQuerier ContractQuerier, eventBus *eventbus.Bus) *ProposerManager {
	ctx, cancel := context.WithCancel(context.Background())

	pm := &ProposerManager{
		nodeAddress:          config.NodeAddress,
		verificationInterval: config.VerificationInterval,
		proposerTimeout:      config.ProposerTimeout,
		rotationStrategy:     NewRoundRobinRotation(config.InitialValidators),
		contractQuerier:      contractQuerier,
		eventBus:             eventBus,
		logger:               types.InitLogEntry("proposer-manager"),
		ctx:                  ctx,
		cancel:               cancel,
		initialized:          false,
	}

	// Set default values if not provided
	if pm.verificationInterval == 0 {
		pm.verificationInterval = 5 * time.Minute
	}
	if pm.proposerTimeout == 0 {
		pm.proposerTimeout = 15 * time.Minute
	}

	return pm
}

// Start initializes the proposer manager and starts background tasks
func (pm *ProposerManager) Start() error {
	pm.mutex.Lock()
	defer pm.mutex.Unlock()

	if pm.initialized {
		return fmt.Errorf("proposer manager already started")
	}

	// Initialize from contract
	if err := pm.initializeFromContract(); err != nil {
		pm.logger.Warnf("Failed to initialize from contract, using default rotation: %v", err)
	}

	// Subscribe to events
	pm.eventBus.Subscribe(eventbus.EventNetworkInitialized, pm.handleNetworkInitialized)
	pm.eventBus.Subscribe(eventbus.EventBridgeInDetected, pm.handleProposerActivity)
	pm.eventBus.Subscribe(eventbus.EventBridgeOutFinished, pm.handleProposerActivity)

	// Start background tasks
	go pm.verificationLoop()
	go pm.timeoutCheckLoop()

	pm.initialized = true
	pm.lastActivity = time.Now()

	pm.logger.Info("Proposer manager started successfully")
	return nil
}

// Stop shuts down the proposer manager
func (pm *ProposerManager) Stop() {
	pm.mutex.Lock()
	defer pm.mutex.Unlock()

	if !pm.initialized {
		return
	}

	pm.cancel()
	pm.eventBus.Unsubscribe(eventbus.EventNetworkInitialized, pm.handleNetworkInitialized)
	pm.eventBus.Unsubscribe(eventbus.EventBridgeInDetected, pm.handleProposerActivity)
	pm.eventBus.Unsubscribe(eventbus.EventBridgeOutFinished, pm.handleProposerActivity)

	pm.initialized = false
	pm.logger.Info("Proposer manager stopped")
}

// IsCurrentProposer returns whether this node is the current proposer
func (pm *ProposerManager) IsCurrentProposer() (bool, error) {
	pm.mutex.RLock()
	defer pm.mutex.RUnlock()

	if !pm.initialized {
		return false, fmt.Errorf("proposer manager not initialized")
	}

	currentProposer := pm.rotationStrategy.GetCurrentProposer()
	return currentProposer == pm.nodeAddress, nil
}

// GetCurrentProposer returns the current proposer address
func (pm *ProposerManager) GetCurrentProposer() common.Address {
	pm.mutex.RLock()
	defer pm.mutex.RUnlock()

	return pm.rotationStrategy.GetCurrentProposer()
}

// OnTransactionSuccess should be called when a transaction is successfully executed
func (pm *ProposerManager) OnTransactionSuccess() {
	pm.mutex.Lock()
	defer pm.mutex.Unlock()

	pm.lastActivity = time.Now()

	// Rotate to next proposer after successful execution
	nextProposer := pm.rotationStrategy.NextProposer()
	pm.logger.Infof("Transaction successful, rotated proposer to: %s", nextProposer.Hex())

	// Publish proposer change event
	pm.eventBus.Publish(eventbus.EventType("proposer:changed"), map[string]interface{}{
		"previous": pm.rotationStrategy.GetCurrentProposer().Hex(),
		"current":  nextProposer.Hex(),
		"reason":   "transaction_success",
	})
}

// ForceRotation forces rotation to the next proposer (for timeout scenarios)
func (pm *ProposerManager) ForceRotation(reason string) {
	pm.mutex.Lock()
	defer pm.mutex.Unlock()

	previousProposer := pm.rotationStrategy.GetCurrentProposer()
	nextProposer := pm.rotationStrategy.NextProposer()
	pm.lastActivity = time.Now()

	pm.logger.Warnf("Forced proposer rotation from %s to %s, reason: %s",
		previousProposer.Hex(), nextProposer.Hex(), reason)

	// Publish proposer change event
	pm.eventBus.Publish(eventbus.EventType("proposer:changed"), map[string]interface{}{
		"previous": previousProposer.Hex(),
		"current":  nextProposer.Hex(),
		"reason":   reason,
	})
}

// GetStats returns current proposer manager statistics
func (pm *ProposerManager) GetStats() map[string]interface{} {
	pm.mutex.RLock()
	defer pm.mutex.RUnlock()

	return map[string]interface{}{
		"initialized":           pm.initialized,
		"current_proposer":      pm.rotationStrategy.GetCurrentProposer().Hex(),
		"is_current_proposer":   pm.rotationStrategy.GetCurrentProposer() == pm.nodeAddress,
		"validators":            pm.rotationStrategy.GetValidators(),
		"last_verification":     pm.lastVerification,
		"last_activity":         pm.lastActivity,
		"verification_interval": pm.verificationInterval.String(),
		"proposer_timeout":      pm.proposerTimeout.String(),
	}
}

// initializeFromContract initializes proposer state from contract
func (pm *ProposerManager) initializeFromContract() error {
	if pm.contractQuerier == nil {
		return fmt.Errorf("contract querier not available")
	}

	// Get current proposer from contract
	currentProposer, err := pm.contractQuerier.GetCurrentProposer()
	if err != nil {
		return fmt.Errorf("failed to get current proposer from contract: %w", err)
	}

	// Get validator set from contract
	validators, err := pm.contractQuerier.GetValidatorSet()
	if err != nil {
		pm.logger.Warnf("Failed to get validator set from contract: %v", err)
		// Continue with existing validators
	} else {
		// Update validator set
		pm.rotationStrategy = NewRoundRobinRotation(validators)
	}

	// Set current proposer
	if err := pm.rotationStrategy.SetProposer(currentProposer); err != nil {
		pm.logger.Warnf("Failed to set proposer from contract: %v", err)
		// Add the proposer to validator set if not found
		pm.rotationStrategy.AddValidator(currentProposer)
		pm.rotationStrategy.SetProposer(currentProposer)
	}

	pm.lastVerification = time.Now()
	pm.logger.Infof("Initialized proposer state from contract: current=%s", currentProposer.Hex())

	return nil
}

// verificationLoop periodically verifies proposer state with contract
func (pm *ProposerManager) verificationLoop() {
	ticker := time.NewTicker(pm.verificationInterval)
	defer ticker.Stop()

	for {
		select {
		case <-pm.ctx.Done():
			return
		case <-ticker.C:
			pm.verifyWithContract()
		}
	}
}

// timeoutCheckLoop checks for proposer timeouts
func (pm *ProposerManager) timeoutCheckLoop() {
	ticker := time.NewTicker(1 * time.Minute) // Check every minute
	defer ticker.Stop()

	for {
		select {
		case <-pm.ctx.Done():
			return
		case <-ticker.C:
			pm.checkProposerTimeout()
		}
	}
}

// verifyWithContract verifies current state with contract
func (pm *ProposerManager) verifyWithContract() {
	if pm.contractQuerier == nil {
		return
	}

	contractProposer, err := pm.contractQuerier.GetCurrentProposer()
	if err != nil {
		pm.logger.Errorf("Failed to verify proposer with contract: %v", err)
		return
	}

	pm.mutex.Lock()
	defer pm.mutex.Unlock()

	currentProposer := pm.rotationStrategy.GetCurrentProposer()
	if contractProposer != currentProposer {
		pm.logger.Infof("Proposer mismatch detected. Local: %s, Contract: %s. Syncing with contract.",
			currentProposer.Hex(), contractProposer.Hex())

		// Sync with contract
		if err := pm.rotationStrategy.SetProposer(contractProposer); err != nil {
			pm.logger.Warnf("Failed to sync with contract proposer: %v", err)
			// Add to validator set if not found
			pm.rotationStrategy.AddValidator(contractProposer)
			pm.rotationStrategy.SetProposer(contractProposer)
		}

		// Publish sync event
		pm.eventBus.Publish(eventbus.EventType("proposer:synced"), map[string]interface{}{
			"local":    currentProposer.Hex(),
			"contract": contractProposer.Hex(),
		})
	}

	pm.lastVerification = time.Now()
}

// checkProposerTimeout checks if current proposer has timed out
func (pm *ProposerManager) checkProposerTimeout() {
	pm.mutex.RLock()
	timeSinceActivity := time.Since(pm.lastActivity)
	pm.mutex.RUnlock()

	if timeSinceActivity > pm.proposerTimeout {
		pm.ForceRotation("timeout")
	}
}

// Event handlers
func (pm *ProposerManager) handleNetworkInitialized(data any) {
	pm.logger.Debug("Network initialized, updating proposer state")
	pm.verifyWithContract()
}

func (pm *ProposerManager) handleProposerActivity(data any) {
	pm.mutex.Lock()
	pm.lastActivity = time.Now()
	pm.mutex.Unlock()
}

// DefaultContractQuerier implements ContractQuerier using the existing contract builder
type DefaultContractQuerier struct {
	contractBuilder *contract.Contract
	entryPointAddr  common.Address
	logger          *log.Entry
}

// NewDefaultContractQuerier creates a new default contract querier
func NewDefaultContractQuerier(contractBuilder *contract.Contract, entryPointAddr common.Address) *DefaultContractQuerier {
	return &DefaultContractQuerier{
		contractBuilder: contractBuilder,
		entryPointAddr:  entryPointAddr,
		logger:          types.InitLogEntry("contract-querier"),
	}
}

// GetCurrentProposer queries the current proposer from the contract
func (cq *DefaultContractQuerier) GetCurrentProposer() (common.Address, error) {
	// TODO: Implement contract call to get current proposer
	// This would typically call a contract method like "getCurrentProposer()"
	// For now, we'll return a placeholder

	// Example implementation:
	// result, err := cq.contractBuilder.CallMethod("getCurrentProposer")
	// if err != nil {
	//     return common.Address{}, fmt.Errorf("failed to call getCurrentProposer: %w", err)
	// }
	// return result.(common.Address), nil

	cq.logger.Debug("GetCurrentProposer called - placeholder implementation")
	return common.Address{}, fmt.Errorf("getCurrentProposer not implemented yet")
}

// GetValidatorSet queries the validator set from the contract
func (cq *DefaultContractQuerier) GetValidatorSet() ([]common.Address, error) {
	// TODO: Implement contract call to get validator set
	// This would typically call a contract method like "getValidators()"
	// For now, we'll return a placeholder

	// Example implementation:
	// result, err := cq.contractBuilder.CallMethod("getValidators")
	// if err != nil {
	//     return nil, fmt.Errorf("failed to call getValidators: %w", err)
	// }
	// return result.([]common.Address), nil

	cq.logger.Debug("GetValidatorSet called - placeholder implementation")
	return nil, fmt.Errorf("getValidatorSet not implemented yet")
}
