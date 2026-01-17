package doge

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"time"

	log "github.com/sirupsen/logrus"
)

// ElectrsBlock represents a block returned by electrs API
type ElectrsBlock struct {
	ID                string `json:"id"`                 // Block hash
	Height            int64  `json:"height"`             // Block height
	Version           int    `json:"version"`            // Block version
	Timestamp         int64  `json:"timestamp"`          // Block timestamp
	TxCount           int    `json:"tx_count"`           // Number of transactions in block
	Size              int    `json:"size"`               // Block size in bytes
	Weight            int    `json:"weight"`             // Block weight
	MerkleRoot        string `json:"merkle_root"`        // Merkle root
	PreviousBlockHash string `json:"previousblockhash"`  // Previous block hash
	Nonce             uint32 `json:"nonce"`              // Block nonce
	Bits              uint32 `json:"bits"`               // Block bits
}

// ElectrsClient is a client for the electrs API
type ElectrsClient struct {
	baseURL    string
	httpClient *http.Client
	logger     *log.Entry
}

// NewElectrsClient creates a new electrs client
func NewElectrsClient(baseURL string, timeout time.Duration) *ElectrsClient {
	if timeout <= 0 {
		timeout = 30 * time.Second
	}

	return &ElectrsClient{
		baseURL: baseURL,
		httpClient: &http.Client{
			Timeout: timeout,
		},
		logger: log.WithField("module", "electrs-client"),
	}
}

// GetBlocks fetches blocks starting from a specific height
// Returns up to 10 blocks from height down to height-9
// This is the behavior of /blocks/[height] endpoint
func (c *ElectrsClient) GetBlocks(height int64) ([]ElectrsBlock, error) {
	url := fmt.Sprintf("%s/blocks/%d", c.baseURL, height)
	c.logger.Debugf("Fetching blocks from electrs: %s", url)

	resp, err := c.httpClient.Get(url)
	if err != nil {
		return nil, fmt.Errorf("electrs request failed: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return nil, fmt.Errorf("electrs returned status %d: %s", resp.StatusCode, string(body))
	}

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("read response body: %w", err)
	}

	var blocks []ElectrsBlock
	if err := json.Unmarshal(body, &blocks); err != nil {
		return nil, fmt.Errorf("unmarshal blocks: %w", err)
	}

	c.logger.Debugf("Fetched %d blocks from electrs (height %d)", len(blocks), height)
	return blocks, nil
}

// GetBlockTip fetches the current tip block
func (c *ElectrsClient) GetBlockTip() (*ElectrsBlock, error) {
	url := fmt.Sprintf("%s/blocks/tip", c.baseURL)
	c.logger.Debugf("Fetching tip block from electrs: %s", url)

	resp, err := c.httpClient.Get(url)
	if err != nil {
		return nil, fmt.Errorf("electrs request failed: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return nil, fmt.Errorf("electrs returned status %d: %s", resp.StatusCode, string(body))
	}

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("read response body: %w", err)
	}

	var blocks []ElectrsBlock
	if err := json.Unmarshal(body, &blocks); err != nil {
		return nil, fmt.Errorf("unmarshal tip blocks: %w", err)
	}

	if len(blocks) == 0 {
		return nil, fmt.Errorf("no blocks returned from tip endpoint")
	}

	return &blocks[0], nil
}

// GetBlockHeight returns the current blockchain tip height
func (c *ElectrsClient) GetBlockHeight() (int64, error) {
	url := fmt.Sprintf("%s/blocks/tip/height", c.baseURL)
	c.logger.Debugf("Fetching tip height from electrs: %s", url)

	resp, err := c.httpClient.Get(url)
	if err != nil {
		return 0, fmt.Errorf("electrs request failed: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return 0, fmt.Errorf("electrs returned status %d: %s", resp.StatusCode, string(body))
	}

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return 0, fmt.Errorf("read response body: %w", err)
	}

	var height int64
	if err := json.Unmarshal(body, &height); err != nil {
		return 0, fmt.Errorf("unmarshal height: %w", err)
	}

	return height, nil
}

// FilterBlocksWithTransactions returns only blocks that have more than minTxCount transactions
// Typically minTxCount=1 means we skip blocks with only coinbase transaction
func (c *ElectrsClient) FilterBlocksWithTransactions(blocks []ElectrsBlock, minTxCount int) []ElectrsBlock {
	result := make([]ElectrsBlock, 0, len(blocks))
	for _, block := range blocks {
		if block.TxCount > minTxCount {
			result = append(result, block)
		}
	}
	return result
}
