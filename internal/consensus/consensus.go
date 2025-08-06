package consensus

import (
	"context"
	"crypto/ecdsa"
	"fmt"
	"math/big"
	"strings"
	"sync"
	"time"

	"github.com/ethereum/go-ethereum"
	"github.com/ethereum/go-ethereum/common"
	ethtypes "github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/crypto"
	"github.com/ethereum/go-ethereum/ethclient"
	"github.com/goat-network/dogecoin-relayer/internal/config"
	"github.com/goat-network/dogecoin-relayer/internal/metrics"
	"github.com/goat-network/dogecoin-relayer/internal/models"
	"github.com/goat-network/dogecoin-relayer/pkg/eventbus"
	"github.com/goat-network/dogecoin-relayer/pkg/global"
	"github.com/goat-network/dogecoin-relayer/pkg/module"
	"github.com/goat-network/dogecoin-relayer/pkg/types"
	log "github.com/sirupsen/logrus"
)

var (
	globalClient *ethclient.Client
	clientMutex  sync.RWMutex
	clientOnce   sync.Once
)

// ConsensusModule manages the overall consensus functionality
type ConsensusModule struct {
	cfg          config.ConsensusConfig
	conn         *models.DBConnection
	logger       *log.Entry
	eventManager *EventManager
	eventBus     *eventbus.Bus
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
	c.eventManager = &EventManager{}
	if err := c.eventManager.Init(c.cfg.EventDetection, conn); err != nil {
		c.logger.Errorf("Failed to initialize event manager: %v", err)
		return fmt.Errorf("failed to initialize event manager: %w", err)
	}

	c.logger.Info("Consensus module initialized successfully")
	return nil
}

func (c *ConsensusModule) Run(ctx context.Context) error {
	c.logger.Info("Consensus module running")

	// Perform TSS consensus check before starting main operations
	if err := c.performTSSConsensusCheck(ctx); err != nil {
		c.logger.Errorf("TSS consensus check failed: %v", err)
		return fmt.Errorf("TSS consensus check failed: %w", err)
	}

	// Start the event manager
	if err := c.eventManager.Run(ctx); err != nil {
		c.logger.Errorf("Failed to run event manager: %v", err)
		return fmt.Errorf("failed to run event manager: %w", err)
	}

	c.logger.Info("Consensus module started successfully")
	return nil
}

// performTSSConsensusCheck performs a TSS consensus check during startup
func (c *ConsensusModule) performTSSConsensusCheck(ctx context.Context) error {
	c.logger.Info("Starting TSS consensus check...")

	// Create a channel to receive the TSS response
	responseChan := make(chan types.TssSigResponse, 1)
	timeoutChan := make(chan struct{}, 1)

	// Generate a test session ID and hash to sign
	testSessionID := "consensus-startup-check"

	// Use a constant test hash for all nodes to ensure TSS consensus check works
	// This is a deterministic hash that all nodes will sign the same way
	testHash := []byte{
		0x01, 0x23, 0x45, 0x67, 0x89, 0xab, 0xcd, 0xef,
		0xfe, 0xdc, 0xba, 0x98, 0x76, 0x54, 0x32, 0x10,
		0x11, 0x22, 0x33, 0x44, 0x55, 0x66, 0x77, 0x88,
		0x99, 0xaa, 0xbb, 0xcc, 0xdd, 0xee, 0xff, 0x00,
	}

	c.logger.WithFields(log.Fields{
		"session_id": testSessionID,
		"hash":       fmt.Sprintf("0x%x", testHash),
	}).Info("Generated test TSS signature request")

	// Subscribe to TSS signature response events for our session
	tssResponseHandler := func(data any) {
		if response, ok := data.(types.TssSigResponse); ok {
			if response.SessionID == testSessionID {
				c.logger.WithFields(log.Fields{
					"session_id": response.SessionID,
					"success":    response.Success,
					"message":    response.Message,
					"signature":  fmt.Sprintf("0x%x", response.RawSig),
				}).Info("Received TSS signature response")

				select {
				case responseChan <- response:
				default:
					// Channel is full, ignore duplicate responses
				}
			}
		}
	}

	c.eventBus.Subscribe(eventbus.EventTssSigResponse, tssResponseHandler)
	defer c.eventBus.Unsubscribe(eventbus.EventTssSigResponse, tssResponseHandler)

	// Set up timeout - use context deadline if shorter than default 30s
	timeout := 30 * time.Second
	if deadline, ok := ctx.Deadline(); ok {
		if timeUntilDeadline := time.Until(deadline); timeUntilDeadline < timeout {
			timeout = timeUntilDeadline
		}
	}
	go func() {
		time.Sleep(timeout)
		select {
		case timeoutChan <- struct{}{}:
		default:
		}
	}()

	// Publish the TSS signature request
	tssRequest := types.TssSigRequest{
		SessionID:  testSessionID,
		UnsignHash: testHash,
	}

	c.logger.WithFields(log.Fields{
		"session_id": tssRequest.SessionID,
		"hash":       fmt.Sprintf("0x%x", tssRequest.UnsignHash),
	}).Info("Publishing TSS signature request for consensus check")

	c.eventBus.Publish(eventbus.EventTssSigRequest, tssRequest)

	// Wait for response or timeout
	select {
	case response := <-responseChan:
		if response.Success {
			c.logger.WithFields(log.Fields{
				"session_id":     response.SessionID,
				"signature_size": len(response.RawSig),
				"duration":       fmt.Sprintf("%.2fs", time.Since(time.Unix(0, 0)).Seconds()),
			}).Info("✅ TSS consensus check PASSED - TSS system is operational")
			return nil
		} else {
			// Check if this is a connection issue vs compatibility issue
			isConnectionIssue := strings.Contains(response.Message, "connect: connection refused") ||
				strings.Contains(response.Message, "no such host") ||
				strings.Contains(response.Message, "network is unreachable")

			isCompatibilityIssue := strings.Contains(response.Message, "failed to decode") ||
				strings.Contains(response.Message, "json: cannot unmarshal") ||
				strings.Contains(response.Message, "Unsupported curve")

			if isConnectionIssue {
				c.logger.WithFields(log.Fields{
					"session_id": response.SessionID,
					"message":    response.Message,
				}).Error("❌ TSS consensus check FAILED - TSS service unreachable")
				return fmt.Errorf("TSS service unreachable: %s", response.Message)
			} else if isCompatibilityIssue {
				// For compatibility/format issues, log warning but continue
				c.logger.WithFields(log.Fields{
					"session_id": response.SessionID,
					"message":    response.Message,
				}).Warn("⚠️ TSS consensus check - compatibility issue detected, but TSS service is reachable")
				c.logger.Warn("TSS service responded but with format/compatibility issues - consensus will continue but TSS functionality may be limited")
				return nil
			} else {
				// For other unknown errors, fail the check
				c.logger.WithFields(log.Fields{
					"session_id": response.SessionID,
					"message":    response.Message,
				}).Error("❌ TSS consensus check FAILED - unknown TSS error")
				return fmt.Errorf("TSS signing failed: %s", response.Message)
			}
		}

	case <-timeoutChan:
		c.logger.WithFields(log.Fields{
			"session_id": testSessionID,
			"timeout":    timeout.String(),
		}).Error("❌ TSS consensus check FAILED - timeout waiting for TSS response")
		return fmt.Errorf("TSS consensus check timeout after %v", timeout)

	case <-ctx.Done():
		c.logger.Info("TSS consensus check cancelled due to context cancellation")
		return ctx.Err()
	}
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

// InitEthClient initializes the global Ethereum client
func InitEthClient(rpcURL string) error {
	var err error
	clientOnce.Do(func() {
		client, dialErr := ethclient.Dial(rpcURL)
		if dialErr != nil {
			err = dialErr
			metrics.RecordError("consensus", "connection_failed")
			return
		}

		clientMutex.Lock()
		globalClient = client
		clientMutex.Unlock()

		// Update connection status
		metrics.EthConnectionStatus.Set(1)
		log.Infof("Global Ethereum client initialized with RPC: %s", rpcURL)
	})
	return err
}

// GetEthClient returns the global Ethereum client
func GetEthClient() *ethclient.Client {
	clientMutex.RLock()
	defer clientMutex.RUnlock()
	return globalClient
}

// CloseEthClient closes the global Ethereum client
func CloseEthClient() {
	clientMutex.Lock()
	defer clientMutex.Unlock()
	if globalClient != nil {
		globalClient.Close()
		globalClient = nil
		metrics.EthConnectionStatus.Set(0)
		log.Info("Global Ethereum client closed")
	}
}

func CreateEIP1559Tx(privateKey *ecdsa.PrivateKey, chainID *big.Int, nonce, gasLimit uint64, to *common.Address, maxFeePerGas, maxPriorityFeePerGas, value *big.Int, data []byte) (*ethtypes.Transaction, error) {
	timer := metrics.NewTimer("consensus", "create_tx")

	unsignedTx := ethtypes.NewTx(&ethtypes.DynamicFeeTx{
		ChainID:   chainID,
		Nonce:     nonce,
		GasTipCap: maxPriorityFeePerGas,
		GasFeeCap: maxFeePerGas,
		Gas:       gasLimit,
		To:        to,
		Value:     value,
		Data:      data,
	})
	signer := ethtypes.NewLondonSigner(chainID)
	signedTx, err := ethtypes.SignTx(unsignedTx, signer, privateKey)

	if err != nil {
		timer.RecordFailure()
		metrics.RecordError("consensus", "tx_creation_failed")
		return nil, err
	}

	timer.RecordSuccess()
	return signedTx, nil
}

// SendTx creates, signs and sends an EIP-1559 transaction
func SendTx(ctx context.Context, privateKey *ecdsa.PrivateKey, chainID *big.Int, to *common.Address, value *big.Int, data []byte) (*ethtypes.Transaction, error) {
	timer := metrics.NewTimer("consensus", "send_tx")

	if globalClient == nil {
		timer.RecordFailure()
		metrics.RecordError("consensus", "client_not_initialized")
		return nil, fmt.Errorf("ethereum client not initialized")
	}

	// Calculate fromAddr from private key
	fromAddr := crypto.PubkeyToAddress(privateKey.PublicKey)

	// Get nonce from chain
	nonce, err := globalClient.PendingNonceAt(ctx, fromAddr)
	if err != nil {
		timer.RecordFailure()
		metrics.RecordError("consensus", "nonce_fetch_failed")
		return nil, fmt.Errorf("failed to get nonce: %w", err)
	}

	// Estimate gas limit
	gasLimit, err := globalClient.EstimateGas(ctx, ethereum.CallMsg{
		From:  fromAddr,
		To:    to,
		Value: value,
		Data:  data,
	})
	if err != nil {
		timer.RecordFailure()
		metrics.RecordError("consensus", "gas_estimation_failed")
		return nil, fmt.Errorf("failed to estimate gas: %w", err)
	}

	// Get suggested gas price and calculate EIP-1559 fees
	gasPrice, err := globalClient.SuggestGasPrice(ctx)
	if err != nil {
		timer.RecordFailure()
		metrics.RecordError("consensus", "gas_price_fetch_failed")
		return nil, fmt.Errorf("failed to get gas price: %w", err)
	}

	// Update gas price metric (convert to Gwei)
	gasPriceGwei := new(big.Int).Div(gasPrice, big.NewInt(1000000000))
	metrics.EthGasPrice.Set(float64(gasPriceGwei.Int64()))

	// Set maxFeePerGas slightly higher than current gas price for EIP-1559
	maxFeePerGas := new(big.Int).Mul(gasPrice, big.NewInt(2))
	// Set maxPriorityFeePerGas as a portion of maxFeePerGas
	maxPriorityFeePerGas := new(big.Int).Div(maxFeePerGas, big.NewInt(10)) // 10% of max fee

	// Create and sign the transaction
	signedTx, err := CreateEIP1559Tx(privateKey, chainID, nonce, gasLimit, to, maxFeePerGas, maxPriorityFeePerGas, value, data)
	if err != nil {
		timer.RecordFailure()
		metrics.EthTransactionsSent.WithLabelValues("failure").Inc()
		return nil, fmt.Errorf("failed to create transaction: %w", err)
	}

	// Send the transaction
	err = globalClient.SendTransaction(ctx, signedTx)
	if err != nil {
		timer.RecordFailure()
		metrics.EthTransactionsSent.WithLabelValues("failure").Inc()
		metrics.RecordError("consensus", "tx_send_failed")
		return nil, fmt.Errorf("failed to send transaction: %w", err)
	}

	// Record successful transaction
	timer.RecordSuccess()
	metrics.EthTransactionsSent.WithLabelValues("success").Inc()
	metrics.EthGasUsed.WithLabelValues("eip1559").Add(float64(gasLimit))

	log.Infof("Transaction sent successfully. From: %s, Hash: %s, Nonce: %d, Gas: %d", fromAddr.Hex(), signedTx.Hash().Hex(), nonce, gasLimit)
	return signedTx, nil
}

// HealthCheck performs a health check on the Ethereum connection
func HealthCheck(ctx context.Context) error {
	timer := metrics.NewTimer("consensus", "health_check")

	if globalClient == nil {
		timer.RecordFailure()
		metrics.EthConnectionStatus.Set(0)
		return fmt.Errorf("ethereum client not initialized")
	}

	// Try to get the latest block number
	_, err := globalClient.BlockNumber(ctx)
	if err != nil {
		timer.RecordFailure()
		metrics.EthConnectionStatus.Set(0)
		metrics.RecordError("consensus", "health_check_failed")
		return fmt.Errorf("health check failed: %w", err)
	}

	timer.RecordSuccess()
	metrics.EthConnectionStatus.Set(1)
	return nil
}

// GetBlockNumber returns the current block number
func GetBlockNumber(ctx context.Context) (uint64, error) {
	timer := metrics.NewTimer("consensus", "get_block_number")

	if globalClient == nil {
		timer.RecordFailure()
		metrics.RecordError("consensus", "client_not_initialized")
		return 0, fmt.Errorf("ethereum client not initialized")
	}

	blockNumber, err := globalClient.BlockNumber(ctx)
	if err != nil {
		timer.RecordFailure()
		metrics.RecordError("consensus", "block_number_fetch_failed")
		return 0, fmt.Errorf("failed to get block number: %w", err)
	}

	timer.RecordSuccess()
	return blockNumber, nil
}

func init() {
	log.Info("Registering consensus module")
	module.RegisterModule(&ConsensusModule{})
}
