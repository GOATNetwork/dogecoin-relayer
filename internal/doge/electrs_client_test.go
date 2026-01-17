package doge

import (
	"testing"
	"time"
)

func TestElectrsClient_GetBlocks(t *testing.T) {
	// Skip if not running integration tests
	if testing.Short() {
		t.Skip("Skipping integration test in short mode")
	}

	client := NewElectrsClient("https://doge-electrs-testnet-demo.qed.me", 30*time.Second)

	// Get tip height first
	tipHeight, err := client.GetBlockHeight()
	if err != nil {
		t.Fatalf("Failed to get tip height: %v", err)
	}
	t.Logf("Tip height: %d", tipHeight)

	// Get blocks at tip height
	blocks, err := client.GetBlocks(tipHeight)
	if err != nil {
		t.Fatalf("Failed to get blocks: %v", err)
	}

	// Should return up to 10 blocks
	if len(blocks) == 0 {
		t.Fatal("Expected at least 1 block")
	}
	if len(blocks) > 10 {
		t.Fatalf("Expected at most 10 blocks, got %d", len(blocks))
	}

	t.Logf("Got %d blocks", len(blocks))

	// First block should be at tipHeight
	if blocks[0].Height != tipHeight {
		t.Errorf("First block height %d != tip height %d", blocks[0].Height, tipHeight)
	}

	// Verify block data
	for i, block := range blocks {
		t.Logf("Block %d: height=%d, tx_count=%d, hash=%s", i, block.Height, block.TxCount, block.ID)
		if block.Height <= 0 {
			t.Errorf("Block %d has invalid height: %d", i, block.Height)
		}
		if block.TxCount < 1 {
			t.Errorf("Block %d has invalid tx_count: %d (should be at least 1 for coinbase)", i, block.TxCount)
		}
		if block.ID == "" {
			t.Errorf("Block %d has empty hash", i)
		}
	}

	// Verify heights are descending (tip down to tip-9)
	for i := 1; i < len(blocks); i++ {
		if blocks[i].Height >= blocks[i-1].Height {
			t.Errorf("Blocks not in descending order: %d >= %d", blocks[i].Height, blocks[i-1].Height)
		}
	}
}

func TestElectrsClient_FilterBlocksWithTransactions(t *testing.T) {
	client := NewElectrsClient("https://doge-electrs-testnet-demo.qed.me", 30*time.Second)

	// Create test blocks
	blocks := []ElectrsBlock{
		{Height: 100, TxCount: 1},  // Only coinbase
		{Height: 101, TxCount: 5},  // Has transactions
		{Height: 102, TxCount: 1},  // Only coinbase
		{Height: 103, TxCount: 10}, // Has transactions
	}

	// Filter blocks with more than coinbase
	filtered := client.FilterBlocksWithTransactions(blocks, 1)

	if len(filtered) != 2 {
		t.Fatalf("Expected 2 blocks with transactions, got %d", len(filtered))
	}

	if filtered[0].Height != 101 || filtered[1].Height != 103 {
		t.Errorf("Unexpected filtered blocks: %v", filtered)
	}
}
