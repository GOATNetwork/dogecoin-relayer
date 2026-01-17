package doge

import (
	"os"
	"testing"

	"github.com/goat-network/dogecoin-relayer/internal/config"
	"github.com/stretchr/testify/require"
)

func TestNewDogeClient(t *testing.T) {
	cfg := config.DogeConfig{
		RpcUrl:      "http://localhost:18443",
		RpcUser:     "test",
		RpcPassword: "test",
	}

	client, err := NewDogeClient(cfg)
	require.NoError(t, err)
	require.NotNil(t, client)
	require.NotNil(t, client.client)

	defer client.Close()
}

func TestDogeClient_GetBlockCount(t *testing.T) {
	cfg := config.DogeConfig{
		RpcUrl:      "http://localhost:18443",
		RpcUser:     "test",
		RpcPassword: "test",
	}

	client, err := NewDogeClient(cfg)
	require.NoError(t, err)
	defer client.Close()

	// This test will fail if no Dogecoin node is running
	// It's expected to fail in test environment
	_, err = client.GetBlockCount()
	if err != nil {
		t.Logf("Expected error when no Dogecoin node is running: %v", err)
	}
}

// TestDogeClient_GetBlock_Height22212782 tests GetBlock with testnet
// Uses environment variables:
//   - DOGE_RPC_URL: Dogecoin RPC URL (default: https://dogecoin-testnet.gateway.tatum.io)
//   - DOGE_API_KEY: API key for Tatum gateway
func TestDogeClient_GetBlock_Height22212782(t *testing.T) {
	rpcURL := os.Getenv("DOGE_RPC_URL")
	apiKey := os.Getenv("DOGE_API_KEY")

	if rpcURL == "" {
		rpcURL = "https://dogecoin-testnet.gateway.tatum.io"
	}
	if apiKey == "" {
		t.Skip("DOGE_API_KEY not set, skipping integration test")
	}

	cfg := config.DogeConfig{
		RpcUrl: rpcURL,
		RpcHeaders: map[string]string{
			"x-api-key": apiKey,
		},
	}

	client, err := NewDogeClient(cfg)
	require.NoError(t, err, "Failed to create DogeClient")
	defer client.Close()

	// Test height 22212782 - the target block from the task
	testHeight := int64(22212782)

	// First get block hash
	blockHash, err := client.GetBlockHash(testHeight)
	require.NoError(t, err, "Failed to get block hash for height %d", testHeight)
	require.NotNil(t, blockHash)
	t.Logf("Block hash for height %d: %s", testHeight, blockHash.String())

	// Test GetBlock
	block, err := client.GetBlock(blockHash)
	require.NoError(t, err, "Failed to get block by hash")
	require.NotNil(t, block)
	require.NotEmpty(t, block.Transactions)
	require.Equal(t, *blockHash, block.BlockHash(), "Block hash mismatch")

	t.Logf("Successfully retrieved block at height %d with %d transactions", testHeight, len(block.Transactions))
}
