package consensus

import (
	"context"
	"crypto/ecdsa"
	"fmt"
	"math/big"
	"sync"
	"time"

	"github.com/ethereum/go-ethereum"
	"github.com/ethereum/go-ethereum/common"
	ethtypes "github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/crypto"
	"github.com/ethereum/go-ethereum/ethclient"
	"github.com/goat-network/dogecoin-relayer/internal/metrics"
	log "github.com/sirupsen/logrus"
)

var (
	globalClient *ethclient.Client
	clientMutex  sync.RWMutex
	clientOnce   sync.Once
)

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
	estimatedGas, err := globalClient.EstimateGas(ctx, ethereum.CallMsg{
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
	// Add 30% buffer to estimated gas to prevent out-of-gas errors
	// Gas estimation can be inaccurate for complex contract calls
	gasLimit := estimatedGas + (estimatedGas * 30 / 100)
	log.Debugf("Gas estimation: estimated=%d, with buffer=%d (30%%)", estimatedGas, gasLimit)

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

	// Ensure minimum gas tip cap (chain requires at least 130000)
	minGasTipCap := big.NewInt(150000) // Set slightly above minimum
	if maxPriorityFeePerGas.Cmp(minGasTipCap) < 0 {
		maxPriorityFeePerGas = minGasTipCap
	}
	// Ensure maxFeePerGas is at least maxPriorityFeePerGas
	if maxFeePerGas.Cmp(maxPriorityFeePerGas) < 0 {
		maxFeePerGas = new(big.Int).Mul(maxPriorityFeePerGas, big.NewInt(2))
	}

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
func HealthCheck(ctx context.Context) {
	if globalClient == nil {
		metrics.EthConnectionStatus.Set(0)
		return
	}

	ticker := time.NewTicker(60 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if err := performHealthCheck(ctx); err != nil {
				log.Errorf("Ethereum health check failed: %v", err)
			}
		}
	}
}

func performHealthCheck(ctx context.Context) error {
	timer := metrics.NewTimer("consensus", "health_check")

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
