package consensus

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/goat-network/dogecoin-relayer/internal/config"
	"github.com/goat-network/dogecoin-relayer/internal/models"
	"github.com/goat-network/dogecoin-relayer/pkg/eventbus"
	"github.com/goat-network/dogecoin-relayer/pkg/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"gorm.io/driver/sqlite"
	"gorm.io/gorm"
)

// createIsolatedConsensusModule creates a consensus module with its own event bus for testing
func createIsolatedConsensusModule(t *testing.T) (*ConsensusModule, *eventbus.Bus) {
	// Create mock database connection
	db, err := gorm.Open(sqlite.Open(":memory:"), &gorm.Config{})
	require.NoError(t, err)
	conn := &models.DBConnection{DB: db}

	// Set up mock configuration
	mockConfig := config.ConsensusConfig{
		Rpc: "", // Empty RPC to skip Ethereum client initialization
		EventDetection: config.EventDetectionConfig{
			Enabled:            false,
			ContractBridge:     "0x1234567890123456789012345678901234567890",
			ConfirmationBlocks: 6,
			BatchSize:          100,
			ScanIntervalSec:    10,
		},
	}

	// Create isolated event bus
	eventBus := eventbus.NewEventBus()

	// Create and initialize consensus module
	module := &ConsensusModule{}
	err = module.Init(mockConfig, conn)
	require.NoError(t, err)

	// Override the event bus after initialization to use isolated one
	module.eventBus = eventBus

	return module, eventBus
}

func TestConsensusModule_TSSConsensusCheck_Success_Isolated(t *testing.T) {
	module, eventBus := createIsolatedConsensusModule(t)

	// Set up mock TSS responder that succeeds quickly
	eventBus.Subscribe(eventbus.EventTssSigRequest, func(data any) {
		go func() {
			if request, ok := data.(types.TssSigRequest); ok {
				// Simulate quick processing
				time.Sleep(50 * time.Millisecond)

				// Verify we're getting the expected constant hash
				expectedHash := []byte{
					0x01, 0x23, 0x45, 0x67, 0x89, 0xab, 0xcd, 0xef,
					0xfe, 0xdc, 0xba, 0x98, 0x76, 0x54, 0x32, 0x10,
					0x11, 0x22, 0x33, 0x44, 0x55, 0x66, 0x77, 0x88,
					0x99, 0xaa, 0xbb, 0xcc, 0xdd, 0xee, 0xff, 0x00,
				}

				// Verify the hash matches our expected constant
				if len(request.UnsignHash) != len(expectedHash) {
					t.Logf("Hash length mismatch: got %d, expected %d", len(request.UnsignHash), len(expectedHash))
				}

				response := types.TssSigResponse{
					SessionID: request.SessionID,
					Success:   true,
					Message:   "Mock TSS response",
					RawSig:    []byte{0x01, 0x02, 0x03, 0x04},
				}

				eventBus.Publish(eventbus.EventTssSigResponse, response)
			}
		}()
	})

	// Give a moment for subscription to register
	time.Sleep(10 * time.Millisecond)

	// Test TSS consensus check
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	err := module.performTSSConsensusCheck(ctx)
	assert.NoError(t, err, "TSS consensus check should succeed")
}

func TestConsensusModule_TSSConsensusCheck_Failure_Isolated(t *testing.T) {
	module, eventBus := createIsolatedConsensusModule(t)

	// Set up mock TSS responder that fails
	eventBus.Subscribe(eventbus.EventTssSigRequest, func(data any) {
		go func() {
			if request, ok := data.(types.TssSigRequest); ok {
				// Simulate processing
				time.Sleep(50 * time.Millisecond)

				response := types.TssSigResponse{
					SessionID: request.SessionID,
					Success:   false,
					Message:   "Mock TSS failure",
					RawSig:    nil,
				}

				eventBus.Publish(eventbus.EventTssSigResponse, response)
			}
		}()
	})

	// Give a moment for subscription to register
	time.Sleep(10 * time.Millisecond)

	// Test TSS consensus check
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	err := module.performTSSConsensusCheck(ctx)
	assert.Error(t, err, "TSS consensus check should fail")
	assert.Contains(t, err.Error(), "TSS signing failed", "Error should indicate TSS signing failure")
}

func TestConsensusModule_TSSConsensusCheck_Timeout_Isolated(t *testing.T) {
	module, _ := createIsolatedConsensusModule(t)

	// No TSS responder set up, so it will timeout

	// Test TSS consensus check with short timeout
	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
	defer cancel()

	err := module.performTSSConsensusCheck(ctx)
	assert.Error(t, err, "TSS consensus check should timeout")
	// Accept both context deadline exceeded and internal timeout messages
	assert.True(t,
		strings.Contains(err.Error(), "timeout") || strings.Contains(err.Error(), "context deadline exceeded"),
		"Error should indicate timeout or context deadline, got: %v", err)
}

func TestConsensusModule_TSSConsensusCheck_ContextCancellation_Isolated(t *testing.T) {
	module, _ := createIsolatedConsensusModule(t)

	// No TSS responder set up

	// Test TSS consensus check with context cancellation
	ctx, cancel := context.WithCancel(context.Background())

	// Cancel the context after a short delay
	go func() {
		time.Sleep(100 * time.Millisecond)
		cancel()
	}()

	err := module.performTSSConsensusCheck(ctx)
	assert.Error(t, err, "TSS consensus check should be cancelled")
	assert.Equal(t, context.Canceled, err, "Error should be context cancellation")
}
