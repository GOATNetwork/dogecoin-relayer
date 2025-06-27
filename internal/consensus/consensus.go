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
			return
		}

		clientMutex.Lock()
		globalClient = client
		clientMutex.Unlock()

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
		log.Info("Global Ethereum client closed")
	}
}

func CreateEIP1559Tx(privateKey *ecdsa.PrivateKey, chainID *big.Int, nonce, gasLimit uint64, to *common.Address, maxFeePerGas, maxPriorityFeePerGas, value *big.Int, data []byte) (*ethtypes.Transaction, error) {
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
	return signedTx, err
}

// SendTx creates, signs and sends an EIP-1559 transaction
func SendTx(ctx context.Context, privateKey *ecdsa.PrivateKey, chainID *big.Int, to *common.Address, value *big.Int, data []byte) (*ethtypes.Transaction, error) {
	if globalClient == nil {
		return nil, fmt.Errorf("ethereum client not initialized")
	}

	// Calculate fromAddr from private key
	fromAddr := crypto.PubkeyToAddress(privateKey.PublicKey)

	// Get nonce from chain
	nonce, err := globalClient.PendingNonceAt(ctx, fromAddr)
	if err != nil {
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
		return nil, fmt.Errorf("failed to estimate gas: %w", err)
	}

	// Get suggested gas price and calculate EIP-1559 fees
	gasPrice, err := globalClient.SuggestGasPrice(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to get gas price: %w", err)
	}

	// Set maxFeePerGas slightly higher than current gas price for EIP-1559
	maxFeePerGas := new(big.Int).Mul(gasPrice, big.NewInt(2))
	// Set maxPriorityFeePerGas as a portion of maxFeePerGas
	maxPriorityFeePerGas := new(big.Int).Div(maxFeePerGas, big.NewInt(10)) // 10% of max fee

	// Create and sign the transaction
	signedTx, err := CreateEIP1559Tx(privateKey, chainID, nonce, gasLimit, to, maxFeePerGas, maxPriorityFeePerGas, value, data)
	if err != nil {
		return nil, fmt.Errorf("failed to create transaction: %w", err)
	}

	// Send the transaction
	err = globalClient.SendTransaction(ctx, signedTx)
	if err != nil {
		return nil, fmt.Errorf("failed to send transaction: %w", err)
	}

	log.Infof("Transaction sent successfully. From: %s, Hash: %s, Nonce: %d, Gas: %d", fromAddr.Hex(), signedTx.Hash().Hex(), nonce, gasLimit)
	return signedTx, nil
}
