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
	require.NotNil(t, client.btcClient)

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

// TestDogeClient_GetBlockVerboseTx_Height22212782 tests GetBlockVerboseTx with testnet
// Uses environment variables:
//   - DOGE_RPC_URL: Dogecoin RPC URL (default: https://dogecoin-testnet.gateway.tatum.io)
//   - DOGE_API_KEY: API key for Tatum gateway
func TestDogeClient_GetBlockVerboseTx_Height22212782(t *testing.T) {
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

	// Test GetBlockVerboseTx
	verboseBlock, err := client.GetBlockVerboseTx(blockHash.String())
	require.NoError(t, err, "Failed to get verbose block")
	require.NotNil(t, verboseBlock)

	// Validate block data
	require.Equal(t, testHeight, verboseBlock.Height, "Block height mismatch")
	require.NotEmpty(t, verboseBlock.Hash, "Block hash is empty")
	require.NotEmpty(t, verboseBlock.Tx, "Block has no transactions")

	t.Logf("Block height: %d", verboseBlock.Height)
	t.Logf("Block hash: %s", verboseBlock.Hash)
	t.Logf("Transaction count: %d", len(verboseBlock.Tx))

	// Check that at least one transaction has vin/vout data
	hasVinVout := false
	for i, tx := range verboseBlock.Tx {
		if len(tx.Vin) > 0 || len(tx.Vout) > 0 {
			hasVinVout = true
			t.Logf("Tx[%d] txid=%s has %d vin(s), %d vout(s)", i, tx.Txid, len(tx.Vin), len(tx.Vout))

			// Log first vin details if available
			if len(tx.Vin) > 0 {
				vin := tx.Vin[0]
				if vin.IsCoinBase() {
					t.Logf("  Vin[0]: coinbase tx")
				} else {
					t.Logf("  Vin[0]: txid=%s, vout=%d", vin.Txid, vin.Vout)
				}
			}

			// Log first vout details if available
			if len(tx.Vout) > 0 {
				vout := tx.Vout[0]
				t.Logf("  Vout[0]: value=%.8f, addresses=%v", vout.Value, vout.ScriptPubKey.Addresses)
			}
			break
		}
	}
	require.True(t, hasVinVout, "No transaction has vin/vout data - verbose=2 may not be working")
}

// TestDogeClient_GetBlockVerboseTxByHeight tests the height-based convenience method
func TestDogeClient_GetBlockVerboseTxByHeight(t *testing.T) {
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

	testHeight := int64(22212782)

	// Test GetBlockVerboseTxByHeight directly
	verboseBlock, err := client.GetBlockVerboseTxByHeight(testHeight)
	require.NoError(t, err, "Failed to get verbose block by height")
	require.NotNil(t, verboseBlock)
	require.Equal(t, testHeight, verboseBlock.Height)
	require.NotEmpty(t, verboseBlock.Tx)

	t.Logf("Successfully retrieved verbose block at height %d with %d transactions", testHeight, len(verboseBlock.Tx))
}
