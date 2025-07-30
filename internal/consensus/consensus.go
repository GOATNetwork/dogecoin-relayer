package consensus

import (
	"context"
	"crypto/ecdsa"
	"fmt"
	"math/big"
	"sync"

	"github.com/ethereum/go-ethereum"
	"github.com/ethereum/go-ethereum/common"
	ethtypes "github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/crypto"
	"github.com/ethereum/go-ethereum/ethclient"
	"github.com/goat-network/dogecoin-relayer/internal/config"
	"github.com/goat-network/dogecoin-relayer/internal/metrics"
	"github.com/goat-network/dogecoin-relayer/internal/models"
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

	// Initialize the Ethereum client with the RPC URL from consensus config
	if c.cfg.Rpc != "" {
		if err := InitEthClient(c.cfg.Rpc); err != nil {
			c.logger.Errorf("Failed to initialize Ethereum client: %v", err)
			return fmt.Errorf("failed to initialize Ethereum client: %w", err)
		}
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

	// Start the event manager
	if err := c.eventManager.Run(ctx); err != nil {
		c.logger.Errorf("Failed to run event manager: %v", err)
		return fmt.Errorf("failed to run event manager: %w", err)
	}

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
