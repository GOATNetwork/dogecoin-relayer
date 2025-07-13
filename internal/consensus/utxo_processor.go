package consensus

import (
	"context"
	"fmt"
	"math/big"
	"time"

	"github.com/ethereum/go-ethereum/common"
	"github.com/goat-network/dogecoin-relayer/internal/models"
	"github.com/goat-network/dogecoin-relayer/pkg/contract"
	"github.com/goat-network/dogecoin-relayer/pkg/eventbus"
	"github.com/goat-network/dogecoin-relayer/pkg/global"
	"github.com/goat-network/dogecoin-relayer/pkg/types"
	log "github.com/sirupsen/logrus"
)

// UtxoProcessor manages UTXO processing for bridge operations
type UtxoProcessor struct {
	conn            *models.DBConnection
	state           *models.StateRepository
	logger          *log.Entry
	eventBus        *eventbus.Bus
	contractBuilder *contract.Contract
	bridgeContract  common.Address

	// Polling configuration
	pollInterval    time.Duration
	batchSize       int
	lastProcessedId uint

	// Control
	ctx       context.Context
	cancel    context.CancelFunc
	isRunning bool
}

// UtxoEvent represents a UTXO event for bridge processing
type UtxoEvent struct {
	UTXO        *models.UTXO
	TxBytes     []byte
	MerkleProof []byte
	BlockHash   string
	EventType   string // "deposit", "withdrawal", etc.
}

// BridgeBatch represents a batch of bridge transactions
type BridgeBatch struct {
	ID           *big.Int
	Transactions []contract.BridgeTransaction
	TotalAmount  *big.Int
	UTXOs        []*models.UTXO
}

// NewUtxoManager creates a new UTXO manager
func NewUtxoManager(conn *models.DBConnection, bridgeContractAddress string) *UtxoProcessor {
	ctx, cancel := context.WithCancel(context.Background())

	return &UtxoProcessor{
		conn:            conn,
		state:           models.NewStateRepository(conn.GetDB()),
		logger:          types.InitLogEntry("utxo-manager"),
		eventBus:        global.GetEventBus(),
		bridgeContract:  common.HexToAddress(bridgeContractAddress),
		pollInterval:    10 * time.Second, // Poll every 10 seconds
		batchSize:       10,               // Process 10 UTXOs at a time
		lastProcessedId: 0,
		ctx:             ctx,
		cancel:          cancel,
		isRunning:       false,
	}
}

// SetContractBuilder sets the contract builder for generating calldata
func (um *UtxoProcessor) SetContractBuilder(builder *contract.Contract) {
	um.contractBuilder = builder
}

// Start begins the UTXO monitoring process
func (um *UtxoProcessor) Start() error {
	if um.isRunning {
		return fmt.Errorf("UTXO manager is already running")
	}

	um.isRunning = true
	um.logger.Info("Starting UTXO manager")

	// Start the polling loop
	go um.pollLoop()

	return nil
}

// Stop stops the UTXO manager
func (um *UtxoProcessor) Stop() {
	if !um.isRunning {
		return
	}

	um.logger.Info("Stopping UTXO manager")
	um.cancel()
	um.isRunning = false
}

// pollLoop continuously polls for new UTXOs
func (um *UtxoProcessor) pollLoop() {
	ticker := time.NewTicker(um.pollInterval)
	defer ticker.Stop()

	for {
		select {
		case <-um.ctx.Done():
			um.logger.Info("UTXO manager poll loop stopping...")
			return
		case <-ticker.C:
			if err := um.processNewUTXOs(); err != nil {
				um.logger.Errorf("Failed to process new UTXOs: %v", err)
			}
		}
	}
}

// processNewUTXOs checks for new deposit UTXOs and processes them
func (um *UtxoProcessor) processNewUTXOs() error {
	// Query for new unprocessed deposit UTXOs
	utxos, err := um.getUnprocessedDepositUTXOs()
	if err != nil {
		return fmt.Errorf("failed to get unprocessed deposit UTXOs: %w", err)
	}

	if len(utxos) == 0 {
		return nil // No new UTXOs to process
	}

	um.logger.Infof("Found %d new deposit UTXOs to process", len(utxos))

	// Group UTXOs into batches for bridge transactions
	batches := um.groupUTXOsIntoBatches(utxos)

	for _, batch := range batches {
		if err := um.processBatch(batch); err != nil {
			um.logger.Errorf("Failed to process batch %s: %v", batch.ID.String(), err)
			continue
		}

		// Mark UTXOs as processed
		if err := um.markUTXOsAsProcessed(batch.UTXOs); err != nil {
			um.logger.Errorf("Failed to mark UTXOs as processed: %v", err)
		}
	}

	return nil
}

// getUnprocessedDepositUTXOs retrieves unprocessed deposit UTXOs from database
func (um *UtxoProcessor) getUnprocessedDepositUTXOs() ([]*models.UTXO, error) {
	var utxos []*models.UTXO

	// Query for deposit UTXOs that haven't been processed yet
	err := um.conn.GetDB().Where(
		"source = ? AND status = ? AND id > ? AND evm_addr != ?",
		models.UTXO_SOURCE_DEPOSIT,
		models.UTXO_STATUS_CONFIRMED,
		um.lastProcessedId,
		"", // Non-empty EVM address required for deposits
	).Limit(um.batchSize).Find(&utxos).Error

	if err != nil {
		return nil, err
	}

	// Update last processed ID
	if len(utxos) > 0 {
		um.lastProcessedId = utxos[len(utxos)-1].ID
	}

	return utxos, nil
}

// groupUTXOsIntoBatches groups UTXOs into batches for efficient processing
func (um *UtxoProcessor) groupUTXOsIntoBatches(utxos []*models.UTXO) []*BridgeBatch {
	var batches []*BridgeBatch
	currentBatch := &BridgeBatch{
		ID:           big.NewInt(time.Now().Unix()), // Simple batch ID based on timestamp
		Transactions: make([]contract.BridgeTransaction, 0),
		TotalAmount:  big.NewInt(0),
		UTXOs:        make([]*models.UTXO, 0),
	}

	for _, utxo := range utxos {
		// Validate UTXO has required data
		if utxo.EvmAddr == "" {
			um.logger.Warnf("Skipping UTXO %s: missing EVM address", utxo.Uid)
			continue
		}

		// Create bridge transaction for this UTXO
		bridgeTx := contract.BridgeTransaction{
			DestEvmAddress: common.HexToAddress(utxo.EvmAddr),
			Amount:         big.NewInt(utxo.Amount),
			TxBytes:        []byte(utxo.Txid), // Store transaction ID as bytes
		}

		currentBatch.Transactions = append(currentBatch.Transactions, bridgeTx)
		currentBatch.TotalAmount.Add(currentBatch.TotalAmount, big.NewInt(utxo.Amount))
		currentBatch.UTXOs = append(currentBatch.UTXOs, utxo)

		// Check if batch is full (limit to prevent large transactions)
		if len(currentBatch.Transactions) >= 5 {
			batches = append(batches, currentBatch)
			currentBatch = &BridgeBatch{
				ID:           big.NewInt(time.Now().Unix() + int64(len(batches))),
				Transactions: make([]contract.BridgeTransaction, 0),
				TotalAmount:  big.NewInt(0),
				UTXOs:        make([]*models.UTXO, 0),
			}
		}
	}

	// Add the last batch if it has transactions
	if len(currentBatch.Transactions) > 0 {
		batches = append(batches, currentBatch)
	}

	return batches
}

// processBatch processes a batch of bridge transactions
func (um *UtxoProcessor) processBatch(batch *BridgeBatch) error {
	um.logger.Infof("Processing bridge batch %s with %d transactions, total amount: %s DOGE",
		batch.ID.String(), len(batch.Transactions), batch.TotalAmount.String())

	// Generate bridge transaction calldata
	calldata, err := um.generateBridgeInCalldata(batch)
	if err != nil {
		return fmt.Errorf("failed to generate bridge calldata: %w", err)
	}

	um.logger.Infof("Generated bridge calldata: %x", calldata)

	// TODO: tss sign the calldata
	// TODO: send the calldata to the bridge contract

	return nil
}

// generateBridgeInCalldata generates the calldata for bridge transactions
func (um *UtxoProcessor) generateBridgeInCalldata(batch *BridgeBatch) ([]byte, error) {
	if um.contractBuilder == nil {
		return nil, fmt.Errorf("contract builder not set")
	}

	// Generate the bridge transaction calldata
	calldata, err := um.contractBuilder.GenerateBridgeInTxData(batch.Transactions, batch.ID)
	if err != nil {
		return nil, fmt.Errorf("failed to generate bridge transaction data: %w", err)
	}

	return calldata, nil
}

// markUTXOsAsProcessed marks UTXOs as processed to avoid reprocessing
func (um *UtxoProcessor) markUTXOsAsProcessed(utxos []*models.UTXO) error {
	// Update UTXOs to mark as processed (we could add a "processed" status or use a separate table)
	// For now, we'll update the UpdatedAt timestamp to track processing
	for _, utxo := range utxos {
		utxo.UpdatedAt = time.Now()
		if err := um.conn.GetDB().Save(utxo).Error; err != nil {
			return fmt.Errorf("failed to mark UTXO %s as processed: %w", utxo.Uid, err)
		}
	}

	um.logger.Debugf("Marked %d UTXOs as processed", len(utxos))
	return nil
}

// GetStats returns current UTXO manager statistics
func (um *UtxoProcessor) GetStats() map[string]interface{} {
	return map[string]interface{}{
		"is_running":        um.isRunning,
		"last_processed_id": um.lastProcessedId,
		"poll_interval":     um.pollInterval.String(),
		"batch_size":        um.batchSize,
		"bridge_contract":   um.bridgeContract.Hex(),
	}
}
