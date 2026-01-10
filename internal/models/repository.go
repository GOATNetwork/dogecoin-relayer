package models

import (
	"errors"
	"fmt"
	"time"

	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

// UTXORepository handles UTXO database operations
type UTXORepository struct {
	db *gorm.DB
}

func NewUTXORepository(db *gorm.DB) *UTXORepository {
	return &UTXORepository{db: db}
}

func (r *UTXORepository) AddUTXO(utxo *UTXO, pubkeyBytes []byte, blockHash string, blockHeight int64, noWitnessTx []byte, merkleRoot string, proofBytes []byte, txIndex int, isDeposit bool) error {
	// Generate UID for UTXO
	utxo.Uid = fmt.Sprintf("%s:%d", utxo.Txid, utxo.OutIndex)

	// Set default values
	if utxo.WalletVersion == "" {
		utxo.WalletVersion = "1"
	}
	if utxo.Status == "" {
		utxo.Status = UTXO_STATUS_CONFIRMED
	}
	if utxo.Source == "" {
		utxo.Source = UTXO_SOURCE_UNKNOWN
	}

	utxo.UpdatedAt = time.Now()

	var existing UTXO
	err := r.db.Select("uid").Where("uid = ?", utxo.Uid).First(&existing).Error
	if err == nil {
		return nil
	}
	if err != nil && !errors.Is(err, gorm.ErrRecordNotFound) {
		return err
	}

	return r.db.Clauses(clause.OnConflict{
		Columns:   []clause.Column{{Name: "uid"}},
		DoNothing: true,
	}).Create(utxo).Error
}

// BatchUpdateUTXOs updates multiple UTXOs in a single transaction
func (r *UTXORepository) BatchUpdateUTXOs(utxos []*UTXO) error {
	if len(utxos) == 0 {
		return nil
	}

	return r.db.Transaction(func(tx *gorm.DB) error {
		now := time.Now()
		for _, utxo := range utxos {
			utxo.UpdatedAt = now
			if err := tx.Save(utxo).Error; err != nil {
				return fmt.Errorf("failed to update UTXO %s: %w", utxo.Uid, err)
			}
		}
		return nil
	})
}

// GetUnprocessedDepositUTXOs retrieves deposit UTXOs that haven't been processed
func (r *UTXORepository) GetUnprocessedDepositUTXOs(lastProcessedId uint, batchSize int) ([]*UTXO, error) {
	var utxos []*UTXO
	err := r.db.Where(
		"source = ? AND status = ? AND id > ?",
		UTXO_SOURCE_DEPOSIT,
		UTXO_STATUS_CONFIRMED,
		lastProcessedId,
	).Limit(batchSize).Find(&utxos).Error

	return utxos, err
}

// GetUnprocessedWithdrawalUTXOs retrieves withdrawal UTXOs that haven't been processed
func (r *UTXORepository) GetUnprocessedWithdrawalUTXOs(lastProcessedId uint, batchSize int) ([]*UTXO, error) {
	var utxos []*UTXO
	err := r.db.Where(
		"source = ? AND status = ? AND id > ?",
		UTXO_SOURCE_WITHDRAWAL,
		UTXO_STATUS_CONFIRMED,
		lastProcessedId,
	).Limit(batchSize).Find(&utxos).Error

	return utxos, err
}

func (r *UTXORepository) UpdateUTXOStatusSpentByVins(vins []*VIN, spentBlock int64) error {
	if len(vins) == 0 {
		return nil
	}

	return r.db.Transaction(func(tx *gorm.DB) error {
		for _, vin := range vins {
			err := tx.Model(&UTXO{}).
				Where("txid = ? AND out_index = ?", vin.Txid, vin.OutIndex).
				Updates(map[string]interface{}{
					"status":      UTXO_STATUS_SPENT,
					"spent_block": spentBlock,
					"updated_at":  time.Now(),
				}).Error
			if err != nil {
				return err
			}
		}
		return nil
	})
}

// VINRepository handles VIN database operations
type VINRepository struct {
	db *gorm.DB
}

func NewVINRepository(db *gorm.DB) *VINRepository {
	return &VINRepository{db: db}
}

func (r *VINRepository) AddOrUpdateVIN(vin *VIN) error {
	if vin.Status == "" {
		vin.Status = UTXO_STATUS_CONFIRMED
	}
	if vin.Source == "" {
		vin.Source = UTXO_SOURCE_UNKNOWN
	}

	vin.UpdatedAt = time.Now()

	// Use upsert logic
	var existingVIN VIN
	err := r.db.Where("txid = ? AND out_index = ?", vin.Txid, vin.OutIndex).First(&existingVIN).Error
	if err == gorm.ErrRecordNotFound {
		return r.db.Create(vin).Error
	} else if err != nil {
		return err
	}

	return r.db.Model(&existingVIN).Updates(vin).Error
}

// VOUTRepository handles VOUT database operations
type VOUTRepository struct {
	db *gorm.DB
}

func NewVOUTRepository(db *gorm.DB) *VOUTRepository {
	return &VOUTRepository{db: db}
}

func (r *VOUTRepository) AddOrUpdateVOUT(vout *VOUT) error {
	if vout.Status == "" {
		vout.Status = UTXO_STATUS_CONFIRMED
	}
	if vout.Source == "" {
		vout.Source = UTXO_SOURCE_UNKNOWN
	}

	vout.UpdatedAt = time.Now()

	// Use upsert logic
	var existingVOUT VOUT
	err := r.db.Where("txid = ? AND out_index = ?", vout.Txid, vout.OutIndex).First(&existingVOUT).Error
	if err == gorm.ErrRecordNotFound {
		return r.db.Create(vout).Error
	} else if err != nil {
		return err
	}

	return r.db.Model(&existingVOUT).Updates(vout).Error
}

// SendOrderRepository handles SendOrder database operations
type SendOrderRepository struct {
	db *gorm.DB
}

func NewSendOrderRepository(db *gorm.DB) *SendOrderRepository {
	return &SendOrderRepository{db: db}
}

func (r *SendOrderRepository) GetSendOrderByTxIdOrExternalId(txid string) (*SendOrder, error) {
	var order SendOrder
	err := r.db.Where("txid = ? OR external_id = ?", txid, txid).First(&order).Error
	if err == gorm.ErrRecordNotFound {
		return nil, nil
	}
	return &order, err
}

func (r *SendOrderRepository) UpdateSendOrderConfirmed(txid string, confirmBlock int64) error {
	return r.db.Model(&SendOrder{}).
		Where("txid = ?", txid).
		Updates(map[string]interface{}{
			"status":        "confirmed",
			"confirm_block": confirmBlock,
			"updated_at":    time.Now(),
		}).Error
}

// PendingBatchRepository handles pending batch database operations
type PendingBatchRepository struct {
	db *gorm.DB
}

func NewPendingBatchRepository(db *gorm.DB) *PendingBatchRepository {
	return &PendingBatchRepository{db: db}
}

// CreatePendingBatch creates a new pending batch
func (r *PendingBatchRepository) CreatePendingBatch(batch *PendingBatch) error {
	return r.db.Clauses(clause.OnConflict{
		Columns: []clause.Column{{Name: "base_session_id"}},
		DoUpdates: clause.AssignmentColumns([]string{
			"batch_type",
			"call_data",
			"tss_nonce",
			"next_attempt",
			"last_attempt",
			"next_retry_at",
			"batch_id",
			"total_amount",
			"withdrawal_id",
			"task_ids_json",
			"tx_id",
			"updated_at",
		}),
		Where: clause.Where{Exprs: []clause.Expression{
			clause.Expr{SQL: "status = ?", Vars: []interface{}{PENDING_BATCH_STATUS_PENDING}},
		}},
	}).Create(batch).Error
}

func (r *PendingBatchRepository) GetPendingBatchByBaseSessionID(baseSessionID string) (*PendingBatch, error) {
	var batch PendingBatch
	err := r.db.Where("base_session_id = ? AND status = ?", baseSessionID, PENDING_BATCH_STATUS_PENDING).First(&batch).Error
	if err != nil {
		return nil, err
	}
	return &batch, nil
}

func (r *PendingBatchRepository) GetAllPendingBatches() ([]*PendingBatch, error) {
	var batches []*PendingBatch
	err := r.db.Where("status = ?", PENDING_BATCH_STATUS_PENDING).Find(&batches).Error
	return batches, err
}

func (r *PendingBatchRepository) UpdatePendingBatch(batch *PendingBatch) error {
	return r.db.Clauses(clause.OnConflict{
		Columns: []clause.Column{{Name: "base_session_id"}},
		DoUpdates: clause.AssignmentColumns([]string{
			"batch_type",
			"call_data",
			"tss_nonce",
			"next_attempt",
			"last_attempt",
			"next_retry_at",
			"batch_id",
			"total_amount",
			"withdrawal_id",
			"task_ids_json",
			"tx_id",
			"updated_at",
		}),
		Where: clause.Where{Exprs: []clause.Expression{
			clause.Expr{SQL: "status = ?", Vars: []interface{}{PENDING_BATCH_STATUS_PENDING}},
		}},
	}).Create(batch).Error
}

// DeletePendingBatchByBaseSessionID deletes a pending batch by base session ID
func (r *PendingBatchRepository) DeletePendingBatchByBaseSessionID(baseSessionID string) error {
	return r.db.Where("base_session_id = ?", baseSessionID).Delete(&PendingBatch{}).Error
}

// UpdatePendingBatchStatus updates the status of a pending batch
func (r *PendingBatchRepository) UpdatePendingBatchStatus(baseSessionID, status string) error {
	return r.db.Model(&PendingBatch{}).
		Where("base_session_id = ?", baseSessionID).
		Update("status", status).Error
}

// WithPendingBatchTransactionRetry wraps a function with transaction and retry logic
func (r *PendingBatchRepository) WithPendingBatchTransactionRetry(fn func(tx *gorm.DB) error) error {
	var lastErr error
	maxRetries := 5
	retryDelay := 50 * time.Millisecond

	for i := 0; i < maxRetries; i++ {
		if i > 0 {
			time.Sleep(retryDelay)
			retryDelay *= 2
		}

		err := r.db.Transaction(func(tx *gorm.DB) error {
			return fn(tx)
		})

		if err == nil {
			return nil
		}

		lastErr = err

		if !isTransientError(err) {
			return err
		}
	}

	return lastErr
}

// isTransientError checks if an error is a transient database error that should be retried
func isTransientError(err error) bool {
	if err == nil {
		return false
	}
	errStr := err.Error()
	return containsSubstring(errStr, "database is locked") ||
		containsSubstring(errStr, "SQLITE_BUSY") ||
		containsSubstring(errStr, "database is busy")
}

func containsSubstring(s, substr string) bool {
	return len(s) >= len(substr) && (s == substr || len(s) > len(substr) && containsSubstringHelper(s, substr))
}

func containsSubstringHelper(s, substr string) bool {
	for i := 0; i <= len(s)-len(substr); i++ {
		if s[i:i+len(substr)] == substr {
			return true
		}
	}
	return false
}

// StateRepository combines all repositories for convenience
type StateRepository struct {
	UTXORepo         *UTXORepository
	VINRepo          *VINRepository
	VOUTRepo         *VOUTRepository
	SendOrderRepo    *SendOrderRepository
	PendingBatchRepo *PendingBatchRepository
}

func NewStateRepository(db *gorm.DB) *StateRepository {
	return &StateRepository{
		UTXORepo:         NewUTXORepository(db),
		VINRepo:          NewVINRepository(db),
		VOUTRepo:         NewVOUTRepository(db),
		SendOrderRepo:    NewSendOrderRepository(db),
		PendingBatchRepo: NewPendingBatchRepository(db),
	}
}

func (s *StateRepository) AddUtxo(utxo *UTXO, pubkeyBytes []byte, blockHash string, blockHeight int64, noWitnessTx []byte, merkleRoot string, proofBytes []byte, txIndex int, isDeposit bool) error {
	return s.UTXORepo.AddUTXO(utxo, pubkeyBytes, blockHash, blockHeight, noWitnessTx, merkleRoot, proofBytes, txIndex, isDeposit)
}

func (s *StateRepository) AddOrUpdateVin(vin *VIN) error {
	return s.VINRepo.AddOrUpdateVIN(vin)
}

func (s *StateRepository) AddOrUpdateVout(vout *VOUT) error {
	return s.VOUTRepo.AddOrUpdateVOUT(vout)
}

func (s *StateRepository) GetSendOrderByTxIdOrExternalId(txid string) (*SendOrder, error) {
	return s.SendOrderRepo.GetSendOrderByTxIdOrExternalId(txid)
}

func (s *StateRepository) UpdateSendOrderConfirmed(txid string, confirmBlock int64) error {
	return s.SendOrderRepo.UpdateSendOrderConfirmed(txid, confirmBlock)
}

func (s *StateRepository) UpdateUtxoStatusSpentByVins(vins []*VIN, spentBlock int64) error {
	return s.UTXORepo.UpdateUTXOStatusSpentByVins(vins, spentBlock)
}

// Batch operations delegated to repositories
func (s *StateRepository) BatchUpdateUTXOs(utxos []*UTXO) error {
	return s.UTXORepo.BatchUpdateUTXOs(utxos)
}

func (s *StateRepository) GetUnprocessedDepositUTXOs(lastProcessedId uint, batchSize int) ([]*UTXO, error) {
	return s.UTXORepo.GetUnprocessedDepositUTXOs(lastProcessedId, batchSize)
}

func (s *StateRepository) GetUnprocessedWithdrawalUTXOs(lastProcessedId uint, batchSize int) ([]*UTXO, error) {
	return s.UTXORepo.GetUnprocessedWithdrawalUTXOs(lastProcessedId, batchSize)
}

func (s *StateRepository) CreatePendingBatch(batch *PendingBatch) error {
	return s.PendingBatchRepo.CreatePendingBatch(batch)
}

func (s *StateRepository) GetPendingBatchByBaseSessionID(baseSessionID string) (*PendingBatch, error) {
	return s.PendingBatchRepo.GetPendingBatchByBaseSessionID(baseSessionID)
}

func (s *StateRepository) GetAllPendingBatches() ([]*PendingBatch, error) {
	return s.PendingBatchRepo.GetAllPendingBatches()
}

func (s *StateRepository) UpdatePendingBatch(batch *PendingBatch) error {
	return s.PendingBatchRepo.UpdatePendingBatch(batch)
}

func (s *StateRepository) DeletePendingBatchByBaseSessionID(baseSessionID string) error {
	return s.PendingBatchRepo.DeletePendingBatchByBaseSessionID(baseSessionID)
}

func (s *StateRepository) UpdatePendingBatchStatus(baseSessionID, status string) error {
	return s.PendingBatchRepo.UpdatePendingBatchStatus(baseSessionID, status)
}

func (s *StateRepository) WithPendingBatchTransactionRetry(fn func(tx *gorm.DB) error) error {
	return s.PendingBatchRepo.WithPendingBatchTransactionRetry(fn)
}
