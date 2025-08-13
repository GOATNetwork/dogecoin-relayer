package doge

import (
	"bytes"
	"encoding/hex"
	"encoding/json"
	"fmt"

	"github.com/dogecoinw/doged/btcutil"
	"github.com/dogecoinw/doged/chaincfg/chainhash"
	"github.com/dogecoinw/doged/rpcclient"
	"github.com/dogecoinw/doged/wire"
	"github.com/goat-network/dogecoin-relayer/internal/config"
	"github.com/goat-network/dogecoin-relayer/internal/metrics"
	"github.com/goat-network/dogecoin-relayer/pkg/types"
	log "github.com/sirupsen/logrus"
)

// DogeClient represents a Dogecoin RPC client using doged library
type DogeClient struct {
	cfg    config.DogeConfig
	client *rpcclient.Client
	logger *log.Entry
}

// NewDogeClient creates a new Dogecoin RPC client using doged library
func NewDogeClient(cfg config.DogeConfig) (*DogeClient, error) {
	// Create RPC client configuration
	connCfg := &rpcclient.ConnConfig{
		Host:         cfg.RpcUrl,
		User:         cfg.RpcUser,
		Pass:         cfg.RpcPassword,
		HTTPPostMode: true,
		DisableTLS:   true,
	}

	// Create the RPC client
	client, err := rpcclient.New(connCfg, nil)
	if err != nil {
		metrics.RecordError("doge", "connection_failed")
		return nil, fmt.Errorf("failed to create RPC client: %w", err)
	}

	// Update connection status
	metrics.DogeConnectionStatus.Set(1)
	metrics.DogeConfirmations.Set(float64(cfg.Confirmations))

	return &DogeClient{
		cfg:    cfg,
		client: client,
		logger: log.WithField("module", "doge-client"),
	}, nil
}

// GetBlockCount returns the current block count
func (c *DogeClient) GetBlockCount() (int64, error) {
	timer := metrics.NewTimer("doge", "get_block_count")

	result, err := c.client.GetBlockCount()
	if err != nil {
		timer.RecordFailure()
		metrics.DogeRPCCalls.WithLabelValues("GetBlockCount", "failure").Inc()
		metrics.RecordError("doge", "rpc_call_failed")
		return 0, err
	}

	timer.RecordSuccess()
	metrics.DogeRPCCalls.WithLabelValues("GetBlockCount", "success").Inc()
	metrics.DogeBlockHeight.Set(float64(result))

	return result, nil
}

// GetBlockHash returns the block hash for a given height
func (c *DogeClient) GetBlockHash(height int64) (*chainhash.Hash, error) {
	timer := metrics.NewTimer("doge", "get_block_hash")

	result, err := c.client.GetBlockHash(height)
	if err != nil {
		timer.RecordFailure()
		metrics.DogeRPCCalls.WithLabelValues("GetBlockHash", "failure").Inc()
		metrics.RecordError("doge", "rpc_call_failed")
		return nil, err
	}

	timer.RecordSuccess()
	metrics.DogeRPCCalls.WithLabelValues("GetBlockHash", "success").Inc()

	return result, nil
}

// GetBlock returns block information
func (c *DogeClient) GetBlock(blockHash *chainhash.Hash) (*wire.MsgBlock, error) {
	timer := metrics.NewTimer("doge", "get_block")

	// Preferred path: use RawRequest to call getblock with verbosity=0 and decode
	// This avoids issues where high-level GetBlock isn't registered in the rpc client.
	if c.client != nil {
		// Build params: ["<hash>", false]
		var params []json.RawMessage
		// hash string param
		params = append(params, json.RawMessage(fmt.Sprintf("\"%s\"", blockHash.String())))
		// verbosity false -> raw hex string (Dogecoin expects boolean, not integer)
		params = append(params, json.RawMessage("false"))

		if rawResp, err := c.client.RawRequest("getblock", params); err == nil {
			var hexStr string
			if uErr := json.Unmarshal(rawResp, &hexStr); uErr == nil {
				if rawBytes, dErr := hex.DecodeString(hexStr); dErr == nil {
					blk := &wire.MsgBlock{}
					if deErr := blk.Deserialize(bytes.NewReader(rawBytes)); deErr == nil {
						timer.RecordSuccess()
						metrics.DogeRPCCalls.WithLabelValues("GetBlock", "success").Inc()
						return blk, nil
					}
				}
			}
			// fallthrough to legacy path on any decode failure
		}
	}

	// Fallback: legacy high-level call (may fail if method not registered)
	result, err := c.client.GetBlock(blockHash)
	if err != nil {
		timer.RecordFailure()
		metrics.DogeRPCCalls.WithLabelValues("GetBlock", "failure").Inc()
		metrics.RecordError("doge", "rpc_call_failed")
		return nil, err
	}

	timer.RecordSuccess()
	metrics.DogeRPCCalls.WithLabelValues("GetBlock", "success").Inc()

	return result, nil
}

// GetRawTransaction returns raw transaction information
func (c *DogeClient) GetRawTransaction(txHash *chainhash.Hash) (*btcutil.Tx, error) {
	timer := metrics.NewTimer("doge", "get_raw_transaction")

	result, err := c.client.GetRawTransaction(txHash)
	if err != nil {
		timer.RecordFailure()
		metrics.DogeRPCCalls.WithLabelValues("GetRawTransaction", "failure").Inc()
		metrics.RecordError("doge", "rpc_call_failed")
		return nil, err
	}

	timer.RecordSuccess()
	metrics.DogeRPCCalls.WithLabelValues("GetRawTransaction", "success").Inc()

	return result, nil
}

// GetBlockByHeight returns a block by height
func (c *DogeClient) GetBlockByHeight(height int64) (*types.DogeBlockExt, error) {
	timer := metrics.NewTimer("doge", "get_block_by_height")

	// Get block hash
	blockHash, err := c.GetBlockHash(height)
	if err != nil {
		timer.RecordFailure()
		metrics.DogeBlocksProcessed.WithLabelValues("failure").Inc()
		return nil, fmt.Errorf("failed to get block hash for height %d: %w", height, err)
	}

	// Get block
	block, err := c.GetBlock(blockHash)
	if err != nil {
		timer.RecordFailure()
		metrics.DogeBlocksProcessed.WithLabelValues("failure").Inc()
		return nil, fmt.Errorf("failed to get block for hash %s: %w", blockHash.String(), err)
	}

	// Count transactions scanned
	metrics.DogeTransactionsScanned.Add(float64(len(block.Transactions)))

	timer.RecordSuccess()
	metrics.DogeBlocksProcessed.WithLabelValues("success").Inc()

	// Convert to our format
	return &types.DogeBlockExt{
		BlockNumber:  height,
		BlockHash:    *blockHash,
		Transactions: block.Transactions,
	}, nil
}

// Close closes the RPC client connection
func (c *DogeClient) Close() {
	if c.client != nil {
		c.client.Shutdown()
		metrics.DogeConnectionStatus.Set(0)
		c.logger.Info("Doge client connection closed")
	}
}

// HealthCheck performs a health check on the Dogecoin connection
func (c *DogeClient) HealthCheck() error {
	timer := metrics.NewTimer("doge", "health_check")

	_, err := c.GetBlockCount()
	if err != nil {
		timer.RecordFailure()
		metrics.DogeConnectionStatus.Set(0)
		return fmt.Errorf("health check failed: %w", err)
	}

	timer.RecordSuccess()
	metrics.DogeConnectionStatus.Set(1)
	return nil
}
