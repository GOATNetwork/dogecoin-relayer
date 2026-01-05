package doge

import (
	"bytes"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"net/url"
	"strings"

	"github.com/btcsuite/btcd/btcjson"
	btcrpcclient "github.com/btcsuite/btcd/rpcclient"
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
	cfg       config.DogeConfig
	client    *rpcclient.Client
	btcClient *btcrpcclient.Client // btcsuite client for GetBlockVerboseTx
	logger    *log.Entry
}

func normalizeRPCConfig(raw string) (string, bool, error) {
	rpcUrl := strings.TrimSpace(raw)
	if rpcUrl == "" {
		return "", true, fmt.Errorf("rpc_url is empty")
	}

	parsed, err := url.Parse(rpcUrl)
	if err != nil {
		return "", true, fmt.Errorf("parse rpc_url %q: %w", raw, err)
	}

	disableTLS := true
	switch parsed.Scheme {
	case "https":
		disableTLS = false
	case "http":
		disableTLS = true
	default:
		return "", true, fmt.Errorf("rpc_url must start with http:// or https:// : %q", raw)
	}

	if parsed.Host == "" {
		return "", disableTLS, fmt.Errorf("rpc_url missing host: %q", raw)
	}

	host := parsed.Host
	if reqURI := parsed.RequestURI(); reqURI != "" && reqURI != "/" {
		host += reqURI
	}

	return host, disableTLS, nil
}

// NewDogeClient creates a new Dogecoin RPC client using doged library
func NewDogeClient(cfg config.DogeConfig) (*DogeClient, error) {
	host, disableTLS, err := normalizeRPCConfig(cfg.RpcUrl)
	if err != nil {
		return nil, err
	}

	// Create RPC client configuration for doged client
	connCfg := &rpcclient.ConnConfig{
		Host:         host,
		User:         cfg.RpcUser,
		Pass:         cfg.RpcPassword,
		HTTPPostMode: true,
		DisableTLS:   disableTLS,
	}
	// If using header-based auth (like Tatum API) and no User/Pass is set,
	// set a dummy Pass to prevent the client from trying cookie authentication
	// which would fail with "stat : no such file or directory".
	if connCfg.Pass == "" && len(cfg.RpcHeaders) > 0 {
		connCfg.Pass = "__header_auth__"
	}
	if len(cfg.RpcHeaders) > 0 {
		connCfg.ExtraHeaders = make(map[string]string, len(cfg.RpcHeaders))
		for key, value := range cfg.RpcHeaders {
			trimmedKey := strings.TrimSpace(key)
			if trimmedKey == "" {
				continue
			}
			connCfg.ExtraHeaders[trimmedKey] = value
		}
	}

	// Create the doged RPC client
	client, err := rpcclient.New(connCfg, nil)
	if err != nil {
		metrics.RecordError("doge", "connection_failed")
		return nil, fmt.Errorf("failed to create RPC client: %w", err)
	}

	// Create btcsuite RPC client configuration for GetBlockVerboseTx
	btcConnCfg := &btcrpcclient.ConnConfig{
		Host:         host,
		User:         cfg.RpcUser,
		Pass:         cfg.RpcPassword,
		HTTPPostMode: true,
		DisableTLS:   disableTLS,
	}
	// If using header-based auth (like Tatum API) and no User/Pass is set,
	// set a dummy Pass to prevent the client from trying cookie authentication
	// which would fail with "stat : no such file or directory"
	if btcConnCfg.Pass == "" && len(cfg.RpcHeaders) > 0 {
		btcConnCfg.Pass = "__header_auth__"
	}
	if len(cfg.RpcHeaders) > 0 {
		btcConnCfg.ExtraHeaders = make(map[string]string, len(cfg.RpcHeaders))
		for key, value := range cfg.RpcHeaders {
			trimmedKey := strings.TrimSpace(key)
			if trimmedKey == "" {
				continue
			}
			btcConnCfg.ExtraHeaders[trimmedKey] = value
		}
	}

	// Create the btcsuite RPC client
	btcClient, err := btcrpcclient.New(btcConnCfg, nil)
	if err != nil {
		client.Shutdown()
		metrics.RecordError("doge", "btc_connection_failed")
		return nil, fmt.Errorf("failed to create btcsuite RPC client: %w", err)
	}

	// Update connection status
	metrics.DogeConnectionStatus.Set(1)
	metrics.DogeConfirmations.Set(float64(cfg.Confirmations))

	return &DogeClient{
		cfg:       cfg,
		client:    client,
		btcClient: btcClient,
		logger:    log.WithField("module", "doge-client"),
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

// GetBlockVerboseTx returns verbose block information with full transaction details using btcsuite/btcd
// This uses getblock with verbosity=2 (integer) to get complete vin/vout information
func (c *DogeClient) GetBlockVerboseTx(blockHashStr string) (*btcjson.GetBlockVerboseTxResult, error) {
	timer := metrics.NewTimer("doge", "get_block_verbose_tx")

	if blockHashStr == "" {
		timer.RecordFailure()
		metrics.DogeRPCCalls.WithLabelValues("GetBlockVerboseTx", "failure").Inc()
		return nil, fmt.Errorf("empty block hash")
	}

	// Use RawRequest with verbosity=2 to get full transaction details
	// btcsuite's GetBlockVerboseTx internally uses int verbosity which some nodes don't support
	// So we use RawRequest directly with explicit verbosity=2
	hashJSON, err := json.Marshal(blockHashStr)
	if err != nil {
		timer.RecordFailure()
		metrics.DogeRPCCalls.WithLabelValues("GetBlockVerboseTx", "failure").Inc()
		return nil, fmt.Errorf("marshal hash: %w", err)
	}

	// Use verbosity=2 (integer) to get full transaction details including vin/vout
	verboseJSON := json.RawMessage([]byte("2"))

	res, err := c.btcClient.RawRequest("getblock", []json.RawMessage{json.RawMessage(hashJSON), verboseJSON})
	if err != nil {
		timer.RecordFailure()
		metrics.DogeRPCCalls.WithLabelValues("GetBlockVerboseTx", "failure").Inc()
		metrics.RecordError("doge", "rpc_call_failed")
		return nil, fmt.Errorf("getblock rpc: %w", err)
	}

	var result btcjson.GetBlockVerboseTxResult
	if err := json.Unmarshal(res, &result); err != nil {
		timer.RecordFailure()
		metrics.DogeRPCCalls.WithLabelValues("GetBlockVerboseTx", "failure").Inc()
		metrics.RecordError("doge", "unmarshal_failed")
		return nil, fmt.Errorf("unmarshal getblock verbose result: %w", err)
	}

	timer.RecordSuccess()
	metrics.DogeRPCCalls.WithLabelValues("GetBlockVerboseTx", "success").Inc()
	return &result, nil
}

// GetBlockVerboseTxByHeight returns verbose block information with full transaction details for a given height
func (c *DogeClient) GetBlockVerboseTxByHeight(height int64) (*btcjson.GetBlockVerboseTxResult, error) {
	timer := metrics.NewTimer("doge", "get_block_verbose_tx_by_height")

	// Get block hash first
	blockHash, err := c.GetBlockHash(height)
	if err != nil {
		timer.RecordFailure()
		return nil, fmt.Errorf("failed to get block hash for height %d: %w", height, err)
	}

	// Get verbose block with transactions
	result, err := c.GetBlockVerboseTx(blockHash.String())
	if err != nil {
		timer.RecordFailure()
		return nil, fmt.Errorf("failed to get verbose block for hash %s: %w", blockHash.String(), err)
	}

	timer.RecordSuccess()
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
	}
	if c.btcClient != nil {
		c.btcClient.Shutdown()
	}
	metrics.DogeConnectionStatus.Set(0)
	c.logger.Info("Doge client connection closed")
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
