package consensus

import (
	"math/big"
	"testing"
	"time"

	"strings"

	"github.com/ethereum/go-ethereum/accounts/abi"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// Test helper functions
func createTestEventConfigs() []EventConfig {
	return []EventConfig{
		{
			ContractAddress: common.HexToAddress("0x1234567890123456789012345678901234567890"),
			EventName:       "Transfer",
			EventSignature:  "Transfer(address,address,uint256)",
			IsActive:        true,
		},
		{
			ContractAddress: common.HexToAddress("0xabcdefabcdefabcdefabcdefabcdefabcdefabcdef"),
			EventName:       "Approval",
			EventSignature:  "Approval(address,address,uint256)",
			IsActive:        true,
		},
	}
}

func createTestABI() string {
	return `[
		{
			"anonymous": false,
			"inputs": [
				{"indexed": true, "name": "from", "type": "address"},
				{"indexed": true, "name": "to", "type": "address"},
				{"indexed": false, "name": "value", "type": "uint256"}
			],
			"name": "Transfer",
			"type": "event"
		},
		{
			"anonymous": false,
			"inputs": [
				{"indexed": true, "name": "owner", "type": "address"},
				{"indexed": true, "name": "spender", "type": "address"},
				{"indexed": false, "name": "value", "type": "uint256"}
			],
			"name": "Approval",
			"type": "event"
		}
	]`
}

func createTestLogs() []types.Log {
	// Create Transfer event log
	transferEventID := common.HexToHash("0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef") // Transfer event signature
	fromAddr := common.HexToAddress("0x1111111111111111111111111111111111111111")
	toAddr := common.HexToAddress("0x2222222222222222222222222222222222222222")

	return []types.Log{
		{
			Address: common.HexToAddress("0x1234567890123456789012345678901234567890"),
			Topics: []common.Hash{
				transferEventID,
				common.BytesToHash(fromAddr.Bytes()),
				common.BytesToHash(toAddr.Bytes()),
			},
			Data:        common.FromHex("0x00000000000000000000000000000000000000000000000000000000000003e8"), // 1000 in hex
			BlockNumber: 12345,
			TxHash:      common.HexToHash("0xabcdef1234567890abcdef1234567890abcdef1234567890abcdef1234567890ab"),
			Index:       0,
		},
	}
}

// Note: For proper testing of the scanning functionality, we would need dependency injection
// of the ethclient.Client. The current implementation uses a global client which makes
// unit testing more difficult. This is a common issue that can be resolved by refactoring
// the EventDetector to accept a client interface as a parameter.

func TestNewEventDetector(t *testing.T) {
	testABI := createTestABI()
	testConfigs := createTestEventConfigs()

	tests := []struct {
		name     string
		abi      string
		configs  []EventConfig
		options  []EventDetectorOption
		validate func(t *testing.T, detector *EventDetector)
	}{
		{
			name:    "basic creation",
			abi:     testABI,
			configs: testConfigs,
			options: nil,
			validate: func(t *testing.T, detector *EventDetector) {
				assert.Equal(t, testABI, detector.ABI)
				assert.Equal(t, testConfigs, detector.configs)
				assert.Equal(t, uint64(10000), detector.lastScannedBlock)
				assert.Equal(t, uint64(6), detector.confirmationBlocks)
				assert.Equal(t, uint64(1000), detector.batchSize)
				assert.Equal(t, 5*time.Second, detector.scanInterval)
				assert.NotNil(t, detector.eventChannel)
				assert.False(t, detector.isRunning)
			},
		},
		{
			name:    "with options",
			abi:     testABI,
			configs: testConfigs,
			options: []EventDetectorOption{
				SetLastScannedBlock(5000),
				SetConfirmationBlocks(12),
				SetBatchSize(500),
				SetScanInterval(10 * time.Second),
			},
			validate: func(t *testing.T, detector *EventDetector) {
				assert.Equal(t, uint64(5000), detector.lastScannedBlock)
				assert.Equal(t, uint64(12), detector.confirmationBlocks)
				assert.Equal(t, uint64(500), detector.batchSize)
				assert.Equal(t, 10*time.Second, detector.scanInterval)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			detector := NewEventDetector(tt.abi, tt.configs, tt.options...)
			require.NotNil(t, detector)
			tt.validate(t, detector)
		})
	}
}

func TestEventDetector_StartStop(t *testing.T) {
	testABI := createTestABI()
	testConfigs := createTestEventConfigs()

	t.Run("successful start", func(t *testing.T) {
		detector := NewEventDetector(testABI, testConfigs)

		err := detector.Start()
		assert.NoError(t, err)
		assert.True(t, detector.isRunning)

		// Clean up
		detector.Stop()
	})

	t.Run("start already running", func(t *testing.T) {
		detector := NewEventDetector(testABI, testConfigs)

		err := detector.Start()
		assert.NoError(t, err)

		// Try to start again
		err = detector.Start()
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "already running")

		// Clean up
		detector.Stop()
	})

	t.Run("stop when not running", func(t *testing.T) {
		detector := NewEventDetector(testABI, testConfigs)

		// Stop without starting - should not panic
		detector.Stop()
		assert.False(t, detector.isRunning)
	})

	t.Run("stop after start", func(t *testing.T) {
		detector := NewEventDetector(testABI, testConfigs)

		err := detector.Start()
		assert.NoError(t, err)
		assert.True(t, detector.isRunning)

		detector.Stop()
		assert.False(t, detector.isRunning)
	})
}

func TestEventDetector_EventChannel(t *testing.T) {
	testABI := createTestABI()
	testConfigs := createTestEventConfigs()

	detector := NewEventDetector(testABI, testConfigs)

	channel := detector.EventChannel()
	assert.NotNil(t, channel)

	// Channel should be readable
	select {
	case <-channel:
		t.Fatal("Channel should be empty initially")
	default:
		// Expected - channel is empty
	}
}

func TestEventDetector_GetLastScannedBlock(t *testing.T) {
	testABI := createTestABI()
	testConfigs := createTestEventConfigs()

	detector := NewEventDetector(testABI, testConfigs, SetLastScannedBlock(5000))

	lastBlock := detector.GetLastScannedBlock()
	assert.Equal(t, uint64(5000), lastBlock)
}

func TestEventDetector_UpdateConfigs(t *testing.T) {
	testABI := createTestABI()
	testConfigs := createTestEventConfigs()

	detector := NewEventDetector(testABI, testConfigs)

	newConfigs := []EventConfig{
		{
			ContractAddress: common.HexToAddress("0x9999999999999999999999999999999999999999"),
			EventName:       "NewEvent",
			EventSignature:  "NewEvent(uint256)",
			IsActive:        true,
		},
	}

	detector.UpdateConfigs(newConfigs)
	assert.Equal(t, newConfigs, detector.configs)
}

func TestEventDetector_ScanForEvents(t *testing.T) {
	// Skip this test if there's no real client (which is expected in unit tests)
	// This test would require a running Ethereum node or a properly mocked client
	t.Skip("Skipping scanForEvents test - requires running Ethereum client or proper dependency injection for mocking")

	// In a real implementation, this test would need:
	// 1. Dependency injection of the ethclient.Client
	// 2. A mock client that implements the required interface methods
	// 3. Or integration tests with a test blockchain (like ganache)
	//
	// Example setup would be:
	// testABI := createTestABI()
	// testConfigs := createTestEventConfigs()
	// detector := NewEventDetector(testABI, testConfigs, SetLastScannedBlock(12340))
}

func TestEventDetector_ParseTopicValue(t *testing.T) {
	testABI := createTestABI()
	testConfigs := createTestEventConfigs()

	detector := NewEventDetector(testABI, testConfigs)

	tests := []struct {
		name     string
		topic    common.Hash
		typeStr  string
		expected interface{}
		hasError bool
	}{
		{
			name:     "address type",
			topic:    common.HexToHash("0x0000000000000000000000001234567890123456789012345678901234567890"),
			typeStr:  "address",
			expected: common.HexToAddress("0x1234567890123456789012345678901234567890"),
			hasError: false,
		},
		{
			name:     "uint256 type",
			topic:    common.HexToHash("0x00000000000000000000000000000000000000000000000000000000000003e8"),
			typeStr:  "uint256",
			expected: big.NewInt(1000),
			hasError: false,
		},
		{
			name:     "bytes32 type",
			topic:    common.HexToHash("0x1234567890abcdef1234567890abcdef1234567890abcdef1234567890abcdef"),
			typeStr:  "bytes32",
			expected: common.HexToHash("0x1234567890abcdef1234567890abcdef1234567890abcdef1234567890abcdef"),
			hasError: false,
		},
		{
			name:     "bool type - true",
			topic:    common.HexToHash("0x0000000000000000000000000000000000000000000000000000000000000001"),
			typeStr:  "bool",
			expected: true,
			hasError: false,
		},
		{
			name:     "bool type - false",
			topic:    common.HexToHash("0x0000000000000000000000000000000000000000000000000000000000000000"),
			typeStr:  "bool",
			expected: false,
			hasError: false,
		},
		{
			name:     "unknown type",
			topic:    common.HexToHash("0x1234567890abcdef1234567890abcdef1234567890abcdef1234567890abcdef"),
			typeStr:  "unknown",
			expected: common.HexToHash("0x1234567890abcdef1234567890abcdef1234567890abcdef1234567890abcdef"),
			hasError: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := detector.parseTopicValue(tt.topic, tt.typeStr)

			if tt.hasError {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
				assert.Equal(t, tt.expected, result)
			}
		})
	}
}

func TestEventDetector_ParseEvent(t *testing.T) {
	testABI := createTestABI()
	testConfigs := createTestEventConfigs()

	detector := NewEventDetector(testABI, testConfigs)

	// Parse the ABI
	contractABI, err := abi.JSON(strings.NewReader(testABI))
	require.NoError(t, err)

	transferEvent := contractABI.Events["Transfer"]
	config := testConfigs[0] // Transfer event config

	// Create a test log
	testLog := types.Log{
		Address: config.ContractAddress,
		Topics: []common.Hash{
			transferEvent.ID,
			common.HexToHash("0x0000000000000000000000001111111111111111111111111111111111111111"), // from
			common.HexToHash("0x0000000000000000000000002222222222222222222222222222222222222222"), // to
		},
		Data:        common.FromHex("0x00000000000000000000000000000000000000000000000000000000000003e8"), // value = 1000
		BlockNumber: 12345,
		TxHash:      common.HexToHash("0xabcdef1234567890abcdef1234567890abcdef1234567890abcdef1234567890ab"),
		Index:       0,
	}

	detectedEvent, err := detector.parseEvent(testLog, config, contractABI, transferEvent)
	require.NoError(t, err)
	require.NotNil(t, detectedEvent)

	assert.Equal(t, uint64(12345), detectedEvent.BlockNumber)
	assert.Equal(t, testLog.TxHash, detectedEvent.TxHash)
	assert.Equal(t, uint(0), detectedEvent.LogIndex)
	assert.Equal(t, config.ContractAddress, detectedEvent.ContractAddress)
	assert.Equal(t, "Transfer", detectedEvent.EventName)
	assert.NotNil(t, detectedEvent.EventData)

	// Check indexed parameters
	assert.Equal(t, common.HexToAddress("0x1111111111111111111111111111111111111111"), detectedEvent.EventData["from"])
	assert.Equal(t, common.HexToAddress("0x2222222222222222222222222222222222222222"), detectedEvent.EventData["to"])

	// Check non-indexed parameters
	assert.Equal(t, big.NewInt(1000), detectedEvent.EventData["value"])
}

func TestGenerateEventSignature(t *testing.T) {
	testABI := createTestABI()

	// Parse the ABI
	contractABI, err := abi.JSON(strings.NewReader(testABI))
	require.NoError(t, err)

	tests := []struct {
		name      string
		eventName string
		expected  string
	}{
		{
			name:      "Transfer event",
			eventName: "Transfer",
			expected:  "Transfer(address,address,uint256)",
		},
		{
			name:      "Approval event",
			eventName: "Approval",
			expected:  "Approval(address,address,uint256)",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			event := contractABI.Events[tt.eventName]
			signature := generateEventSignature(event)
			assert.Equal(t, tt.expected, signature)
		})
	}
}

func TestEventDetectorOptions(t *testing.T) {
	testABI := createTestABI()
	testConfigs := createTestEventConfigs()

	t.Run("SetLastScannedBlock", func(t *testing.T) {
		detector := NewEventDetector(testABI, testConfigs, SetLastScannedBlock(9999))
		assert.Equal(t, uint64(9999), detector.lastScannedBlock)
	})

	t.Run("SetConfirmationBlocks", func(t *testing.T) {
		detector := NewEventDetector(testABI, testConfigs, SetConfirmationBlocks(20))
		assert.Equal(t, uint64(20), detector.confirmationBlocks)
	})

	t.Run("SetBatchSize", func(t *testing.T) {
		detector := NewEventDetector(testABI, testConfigs, SetBatchSize(2000))
		assert.Equal(t, uint64(2000), detector.batchSize)
	})

	t.Run("SetScanInterval", func(t *testing.T) {
		detector := NewEventDetector(testABI, testConfigs, SetScanInterval(30*time.Second))
		assert.Equal(t, 30*time.Second, detector.scanInterval)
	})

	t.Run("multiple options", func(t *testing.T) {
		detector := NewEventDetector(testABI, testConfigs,
			SetLastScannedBlock(8888),
			SetConfirmationBlocks(15),
			SetBatchSize(1500),
			SetScanInterval(20*time.Second),
		)
		assert.Equal(t, uint64(8888), detector.lastScannedBlock)
		assert.Equal(t, uint64(15), detector.confirmationBlocks)
		assert.Equal(t, uint64(1500), detector.batchSize)
		assert.Equal(t, 20*time.Second, detector.scanInterval)
	})
}

// Integration test to verify the detector can process events end-to-end
func TestEventDetector_Integration(t *testing.T) {
	// Skip this integration test since it requires a real Ethereum client
	t.Skip("Skipping integration test - requires running Ethereum client")

	// This test would verify end-to-end functionality including:
	// 1. Starting the detector
	// 2. Processing events from a real blockchain
	// 3. Stopping the detector gracefully
	// 4. Verifying channel cleanup
	//
	// For a proper integration test, you would need:
	// - A test blockchain (like Ganache or Hardhat Network)
	// - Deployed test contracts
	// - Generated test events
}

// Benchmark tests
func BenchmarkEventDetector_ParseTopicValue(b *testing.B) {
	testABI := createTestABI()
	testConfigs := createTestEventConfigs()
	detector := NewEventDetector(testABI, testConfigs)

	topic := common.HexToHash("0x0000000000000000000000001234567890123456789012345678901234567890")

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, err := detector.parseTopicValue(topic, "address")
		if err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkGenerateEventSignature(b *testing.B) {
	testABI := createTestABI()
	contractABI, err := abi.JSON(strings.NewReader(testABI))
	if err != nil {
		b.Fatal(err)
	}

	transferEvent := contractABI.Events["Transfer"]

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = generateEventSignature(transferEvent)
	}
}
