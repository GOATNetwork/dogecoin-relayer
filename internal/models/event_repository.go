package models

import (
	"fmt"
	"strings"
	"time"

	log "github.com/sirupsen/logrus"
	"gorm.io/gorm"
)

type EventRepository struct {
	db *gorm.DB
}

func NewEventRepository(db *gorm.DB) *EventRepository {
	if db == nil {
		panic("database instance cannot be nil when creating EventRepository")
	}
	return &EventRepository{db: db}
}

// Transaction operations
func (r *EventRepository) BeginTransaction() *gorm.DB {
	return r.db.Begin()
}

// WithTransaction runs the provided function within a DB transaction.
// If the function returns an error the transaction is rolled back; otherwise it's committed.
func (r *EventRepository) WithTransaction(fn func(tx *gorm.DB) error) error {
	return r.db.Transaction(func(tx *gorm.DB) error { return fn(tx) })
}

// WithTransactionRetry runs a transaction with retry logic for database lock errors
func (r *EventRepository) WithTransactionRetry(fn func(tx *gorm.DB) error) error {
	maxRetries := 5 // Increased from 3
	for i := 0; i < maxRetries; i++ {
		err := r.db.Transaction(func(tx *gorm.DB) error {
			return fn(tx)
		})

		if err == nil {
			return nil
		}

		// Check if it's a database lock error
		if isLockError(err) && i < maxRetries-1 {
			// Exponential backoff with jitter: 50ms, 100ms, 200ms, 400ms, 800ms
			backoffMs := 50 * (1 << uint(i))
			// Add small jitter to avoid thundering herd
			jitterMs := backoffMs / 10 // 10% jitter
			waitTime := time.Duration(backoffMs+jitterMs) * time.Millisecond

			// Log on retries to help debug
			if i >= 2 {
				// Only log after 2nd retry to reduce noise
				log.Printf("WARN: Database lock detected, retry %d/%d after %v: %v",
					i+1, maxRetries, waitTime, err)
			}

			time.Sleep(waitTime)
			continue
		}

		return err
	}
	return nil
}

// isLockError checks if the error is related to database locking
func isLockError(err error) bool {
	if err == nil {
		return false
	}
	errStr := strings.ToLower(err.Error())
	return strings.Contains(errStr, "database is locked") ||
		strings.Contains(errStr, "database lock") ||
		strings.Contains(errStr, "busy") ||
		strings.Contains(errStr, "timeout") ||
		strings.Contains(errStr, "locked")
}

// getDB returns tx if provided; otherwise returns the repository base DB.
func (r *EventRepository) getDB(tx *gorm.DB) *gorm.DB {
	if tx != nil {
		return tx
	}
	return r.db
}

// EventScanState operations
func (r *EventRepository) GetScanState(contractAddress string) (*EventScanState, error) {
	var state EventScanState
	err := r.db.Where("contract_address = ?", contractAddress).First(&state).Error
	if err != nil {
		return nil, err
	}
	return &state, nil
}

func (r *EventRepository) UpdateScanState(transaction *gorm.DB, lastScannedBlock uint64) error {
	if transaction == nil {
		transaction = r.db
	}
	state := &EventScanState{
		LastScannedBlock: lastScannedBlock,
		LastScannedAt:    time.Now(),
		IsActive:         true,
	}

	// Use Upsert (create or update)
	return transaction.Save(state).Error
}

func (r *EventRepository) CreateOrUpdateScanState(state *EventScanState) error {
	state.LastScannedAt = time.Now()
	return r.db.Save(state).Error
}

// New repository operations for Deposit, Withdrawal, and Proposers

// Deposit operations

// CreateOrUpdateDeposit creates a new deposit or updates the existing one matched by (tx_id, vout) with retry on lock errors.
func (r *EventRepository) CreateOrUpdateDeposit(tx *gorm.DB, deposit *Deposit) error {
	// If we're already in a transaction, execute directly without retry
	if tx != nil {
		return r.createOrUpdateDepositImpl(tx, deposit)
	}
	
	// For non-transactional updates, use retry logic
	return r.WithTransactionRetry(func(innerTx *gorm.DB) error {
		return r.createOrUpdateDepositImpl(innerTx, deposit)
	})
}

// createOrUpdateDepositImpl is the internal implementation of CreateOrUpdateDeposit
func (r *EventRepository) createOrUpdateDepositImpl(db *gorm.DB, deposit *Deposit) error {
	var existing Deposit
	err := db.Where("tx_id = ? AND vout = ?", deposit.TxId, deposit.Vout).First(&existing).Error
	if err != nil {
		if err == gorm.ErrRecordNotFound {
			return db.Create(deposit).Error
		}
		return err
	}

	// Update mutable fields
	updates := map[string]any{
		"address":       deposit.Address,
		"evm_addr":      deposit.EvmAddr,
		"amount":        deposit.Amount,
		"tx_bytes":      deposit.TxBytes,
		"status":        deposit.Status,
		"evm_tx_hash":   deposit.EvmTxHash,
		"evm_block":     deposit.EvmBlock,
		"evm_log_index": deposit.EvmLogIndex,
		"updated_at":    time.Now(),
	}
	return db.Model(&existing).Updates(updates).Error
}

// GetDeposit returns a deposit by (tx_id, vout).
func (r *EventRepository) GetDeposit(tx *gorm.DB, txId string, vout int) (*Deposit, error) {
	db := r.getDB(tx)
	var dep Deposit
	if err := db.Where("tx_id = ? AND vout = ?", txId, vout).First(&dep).Error; err != nil {
		return nil, err
	}
	return &dep, nil
}

// ListDepositsByStatus lists deposits filtered by status with optional limit (<=0 means no limit).
func (r *EventRepository) ListDepositsByStatus(tx *gorm.DB, status string, limit int) ([]Deposit, error) {
	db := r.getDB(tx)
	var list []Deposit
	query := db.Where("status = ?", status).Order("created_at ASC")
	if limit > 0 {
		query = query.Limit(limit)
	}
	return list, query.Find(&list).Error
}

// UpdateDepositStatus updates status of a deposit by primary key ID with retry on lock errors.
func (r *EventRepository) UpdateDepositStatus(tx *gorm.DB, id uint, status string) error {
	db := r.getDB(tx)
	
	// If we're already in a transaction, don't retry (let the outer transaction handle it)
	if tx != nil {
		return db.Model(&Deposit{}).Where("id = ?", id).Updates(map[string]any{
			"status":     status,
			"updated_at": time.Now(),
		}).Error
	}
	
	// For non-transactional updates, use retry logic
	return r.WithTransactionRetry(func(innerTx *gorm.DB) error {
		return innerTx.Model(&Deposit{}).Where("id = ?", id).Updates(map[string]any{
			"status":     status,
			"updated_at": time.Now(),
		}).Error
	})
}

// BatchGetDepositsByTxIds retrieves multiple deposits in a single query
// Returns a map keyed by "txid:vout" for quick lookup
func (r *EventRepository) BatchGetDepositsByTxIds(tx *gorm.DB, txidVouts []struct{ TxId string; Vout int }) (map[string]Deposit, error) {
	db := r.getDB(tx)
	
	if len(txidVouts) == 0 {
		return make(map[string]Deposit), nil
	}

	var deposits []Deposit
	query := db
	
	// Build OR conditions for batch query
	for i, tv := range txidVouts {
		if i == 0 {
			query = query.Where("tx_id = ? AND vout = ?", tv.TxId, tv.Vout)
		} else {
			query = query.Or("tx_id = ? AND vout = ?", tv.TxId, tv.Vout)
		}
	}
	
	if err := query.Find(&deposits).Error; err != nil {
		return nil, err
	}
	
	// Build result map
	result := make(map[string]Deposit, len(deposits))
	for _, dep := range deposits {
		key := fmt.Sprintf("%s:%d", dep.TxId, dep.Vout)
		result[key] = dep
	}
	
	return result, nil
}


// Withdrawal operations

// CreateOrUpdateWithdrawal creates or updates a withdrawal matched by req_task_id.
func (r *EventRepository) CreateOrUpdateWithdrawal(tx *gorm.DB, w *Withdrawal) error {
	db := r.getDB(tx)
	var existing Withdrawal
	err := db.Where("req_task_id = ?", w.ReqTaskId).First(&existing).Error
	if err != nil {
		if err == gorm.ErrRecordNotFound {
			return db.Create(w).Error
		}
		return err
	}

	updates := map[string]any{
		"req_tx_hash":      w.ReqTxHash,
		"req_block":        w.ReqBlock,
		"req_log_index":    w.ReqLogIndex,
		"status":           w.Status,
		"tx_id":            w.TxId,
		"vout":             w.Vout,
		"tx_bytes":         w.TxBytes,
		"finish_tx_hash":   w.FinishTxHash,
		"finish_block":     w.FinishBlock,
		"finish_log_index": w.FinishLogIndex,
		"updated_at":       time.Now(),
	}
	return db.Model(&existing).Updates(updates).Error
}

// GetWithdrawalByTask returns a withdrawal by req_task_id.
func (r *EventRepository) GetWithdrawalByTask(tx *gorm.DB, reqTaskId string) (*Withdrawal, error) {
	db := r.getDB(tx)
	var w Withdrawal
	if err := db.Where("req_task_id = ?", reqTaskId).First(&w).Error; err != nil {
		return nil, err
	}
	return &w, nil
}

// UpdateWithdrawalStatus updates withdrawal status by ID.
func (r *EventRepository) UpdateWithdrawalStatus(tx *gorm.DB, id uint, status string) error {
	db := r.getDB(tx)
	return db.Model(&Withdrawal{}).Where("id = ?", id).Updates(map[string]any{
		"status":     status,
		"updated_at": time.Now(),
	}).Error
}

// SetWithdrawalFinishInfo sets finish info by req_task_id (idempotent upsert-style update).
func (r *EventRepository) SetWithdrawalFinishInfo(tx *gorm.DB, reqTaskId, finishTxHash string, finishBlock uint64, finishLogIndex uint) error {
	db := r.getDB(tx)
	return db.Model(&Withdrawal{}).Where("req_task_id = ?", reqTaskId).Updates(map[string]any{
		"finish_tx_hash":   finishTxHash,
		"finish_block":     finishBlock,
		"finish_log_index": finishLogIndex,
		"updated_at":       time.Now(),
	}).Error
}

// Proposers operations

// CreateOrUpdateProposer creates or updates a proposer matched by address.
func (r *EventRepository) CreateOrUpdateProposer(tx *gorm.DB, p *Proposers) error {
	db := r.getDB(tx)
	var existing Proposers
	err := db.Where("address = ?", p.Address).First(&existing).Error
	if err != nil {
		if err == gorm.ErrRecordNotFound {
			return db.Create(p).Error
		}
		return err
	}

	updates := map[string]any{
		"status":        p.Status,
		"pending_event": p.PendingEvent,
		"join_block":    p.JoinBlock,
		"exit_block":    p.ExitBlock,
		"updated_at":    time.Now(),
	}
	return db.Model(&existing).Updates(updates).Error
}

// UpdateProposerStatus updates a proposer status by address.
func (r *EventRepository) UpdateProposerStatus(tx *gorm.DB, address, status string) error {
	db := r.getDB(tx)
	return db.Model(&Proposers{}).Where("address = ?", address).Updates(map[string]any{
		"status":     status,
		"updated_at": time.Now(),
	}).Error
}

// GetProposer returns a proposer by address.
func (r *EventRepository) GetProposer(tx *gorm.DB, address string) (*Proposers, error) {
	db := r.getDB(tx)
	var p Proposers
	if err := db.Where("address = ?", address).First(&p).Error; err != nil {
		return nil, err
	}
	return &p, nil
}

// ListProposersByStatus lists proposers filtered by status.
func (r *EventRepository) ListProposersByStatus(tx *gorm.DB, status string) ([]Proposers, error) {
	db := r.getDB(tx)
	var list []Proposers
	err := db.Where("status = ?", status).Order("created_at ASC").Find(&list).Error
	return list, err
}
