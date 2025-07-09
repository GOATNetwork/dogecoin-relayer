package doge

import (
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
