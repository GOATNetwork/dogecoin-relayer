package doge

import (
	"bytes"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"strings"

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
	// NOTE: doged/rpcclient.GetBlock uses an integer verbosity param (0),
	// which Dogecoin Core rejects with code -1 (expects boolean).
	// To ensure compatibility, explicitly call RawRequest with boolean false
	// and decode the returned hex ourselves.
	timer := metrics.NewTimer("doge", "get_block")

	if blockHash == nil {
		timer.RecordFailure()
		metrics.DogeRPCCalls.WithLabelValues("GetBlock", "failure").Inc()
		return nil, fmt.Errorf("nil block hash")
	}

	// Build JSON-RPC params: [hash, false]
	hashJSON, err := json.Marshal(blockHash.String())
	if err != nil {
		timer.RecordFailure()
		metrics.DogeRPCCalls.WithLabelValues("GetBlock", "failure").Inc()
		metrics.RecordError("doge", "marshal_failed")
		return nil, fmt.Errorf("marshal hash: %w", err)
	}
	// Use explicit boolean false for non-verbose (hex) response
	verboseJSON := json.RawMessage([]byte("false"))

	res, err := c.client.RawRequest("getblock", []json.RawMessage{json.RawMessage(hashJSON), verboseJSON})
	if err != nil {
		timer.RecordFailure()
		metrics.DogeRPCCalls.WithLabelValues("GetBlock", "failure").Inc()
		metrics.RecordError("doge", "rpc_call_failed")
		return nil, err
	}

	// Unmarshal result as hex string
	var blockHex string
	if err := json.Unmarshal(res, &blockHex); err != nil {
		timer.RecordFailure()
		metrics.DogeRPCCalls.WithLabelValues("GetBlock", "failure").Inc()
		metrics.RecordError("doge", "unmarshal_failed")
		return nil, fmt.Errorf("unmarshal getblock result: %w", err)
	}

	// Decode hex and deserialize to wire.MsgBlock
	serializedBlock, err := hex.DecodeString(blockHex)
	if err != nil {
		timer.RecordFailure()
		metrics.DogeRPCCalls.WithLabelValues("GetBlock", "failure").Inc()
		metrics.RecordError("doge", "hex_decode_failed")
		return nil, fmt.Errorf("decode block hex: %w", err)
	}

	var msgBlock wire.MsgBlock
	if err := msgBlock.Deserialize(bytes.NewReader(serializedBlock)); err != nil {
		timer.RecordFailure()
		metrics.DogeRPCCalls.WithLabelValues("GetBlock", "failure").Inc()
		metrics.RecordError("doge", "deserialize_failed")
		return nil, fmt.Errorf("deserialize block: %w", err)
	}

	timer.RecordSuccess()
	metrics.DogeRPCCalls.WithLabelValues("GetBlock", "success").Inc()
	return &msgBlock, nil
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

// CreateAndFundRawTx builds a raw transaction with the given outputs (amounts expressed as string DOGE values)
// and lets the node pick inputs/change. Returns the funded hex string.
func (c *DogeClient) CreateAndFundRawTx(outputs map[string]string, changeAddr string) (string, error) {
	if len(outputs) == 0 {
		return "", fmt.Errorf("no outputs provided")
	}

	outputJSON, err := json.Marshal(outputs)
	if err != nil {
		return "", fmt.Errorf("marshal outputs: %w", err)
	}

	rawCreate, err := c.client.RawRequest("createrawtransaction", []json.RawMessage{
		json.RawMessage("[]"), // empty inputs, wallet will fund
		json.RawMessage(outputJSON),
	})
	if err != nil {
		return "", fmt.Errorf("createrawtransaction rpc: %w", err)
	}

	var unsignedHex string
	if err := json.Unmarshal(rawCreate, &unsignedHex); err != nil {
		return "", fmt.Errorf("unmarshal create tx: %w", err)
	}

	options := map[string]any{
		"changeAddress": changeAddr,
	}
	optJSON, _ := json.Marshal(options)

	rawFund, err := c.client.RawRequest("fundrawtransaction", []json.RawMessage{
		json.RawMessage(fmt.Sprintf("%q", unsignedHex)),
		json.RawMessage(optJSON),
	})
	if err != nil {
		return "", fmt.Errorf("fundrawtransaction rpc: %w", err)
	}

	var fundResp struct {
		Hex    string  `json:"hex"`
		Fee    float64 `json:"fee"`
		ChgPos int     `json:"changepos"`
	}
	if err := json.Unmarshal(rawFund, &fundResp); err != nil {
		return "", fmt.Errorf("unmarshal fund tx: %w", err)
	}

	return fundResp.Hex, nil
}

// SignRawTransaction signs the provided raw tx hex using the node wallet.
func (c *DogeClient) SignRawTransaction(rawHex string) (string, error) {
	rawSign, err := c.client.RawRequest("signrawtransaction", []json.RawMessage{
		json.RawMessage(fmt.Sprintf("%q", rawHex)),
	})
	if err != nil {
		return "", fmt.Errorf("signrawtransaction rpc: %w", err)
	}
	var signResp struct {
		Hex      string `json:"hex"`
		Complete bool   `json:"complete"`
	}
	if err := json.Unmarshal(rawSign, &signResp); err != nil {
		return "", fmt.Errorf("unmarshal sign tx: %w", err)
	}
	if !signResp.Complete {
		return "", fmt.Errorf("signrawtransaction incomplete")
	}
	return signResp.Hex, nil
}

// SendRawTransaction broadcasts the given raw tx hex.
func (c *DogeClient) SendRawTransaction(rawHex string) (string, error) {
	rawSend, err := c.client.RawRequest("sendrawtransaction", []json.RawMessage{
		json.RawMessage(fmt.Sprintf("%q", rawHex)),
	})
	if err != nil {
		return "", fmt.Errorf("sendrawtransaction rpc: %w", err)
	}
	var txid string
	if err := json.Unmarshal(rawSend, &txid); err != nil {
		return "", fmt.Errorf("unmarshal send tx: %w", err)
	}
	return txid, nil
}

// GetRawTransactionHex fetches raw tx hex and confirmations (verbose) for the given txid.
func (c *DogeClient) GetRawTransactionHex(txid string) (string, int64, error) {
	rawGet, err := c.client.RawRequest("getrawtransaction", []json.RawMessage{
		json.RawMessage(fmt.Sprintf("%q", txid)),
		json.RawMessage("true"),
	})
	if err != nil {
		return "", 0, fmt.Errorf("getrawtransaction rpc: %w", err)
	}
	var resp struct {
		Hex           string  `json:"hex"`
		Confirmations float64 `json:"confirmations"`
	}
	if err := json.Unmarshal(rawGet, &resp); err != nil {
		return "", 0, fmt.Errorf("unmarshal getrawtransaction: %w", err)
	}
	return resp.Hex, int64(resp.Confirmations), nil
}

// DecodeRawTransaction decodes a hex-encoded tx into wire.MsgTx.
func (c *DogeClient) DecodeRawTransaction(rawHex string) (*wire.MsgTx, error) {
	decoded, err := hex.DecodeString(strings.TrimSpace(rawHex))
	if err != nil {
		return nil, fmt.Errorf("decode raw tx hex: %w", err)
	}
	var msg wire.MsgTx
	if err := msg.Deserialize(bytes.NewReader(decoded)); err != nil {
		return nil, fmt.Errorf("deserialize raw tx: %w", err)
	}
	return &msg, nil
}
