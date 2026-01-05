package models

import (
	"time"

	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

const utxoScanStateID uint = 1

// UTXOScanStateRepository manages UTXO scan state persistence.
type UTXOScanStateRepository struct {
	db *gorm.DB
}

func NewUTXOScanStateRepository(db *gorm.DB) *UTXOScanStateRepository {
	if db == nil {
		panic("database instance cannot be nil when creating UTXOScanStateRepository")
	}
	return &UTXOScanStateRepository{db: db}
}

func (r *UTXOScanStateRepository) GetUTXOScanState() (*UTXOScanState, error) {
	var state UTXOScanState
	if err := r.db.First(&state, utxoScanStateID).Error; err != nil {
		return nil, err
	}
	return &state, nil
}

func (r *UTXOScanStateRepository) UpdateUTXOScanState(lastScannedBlock uint64) error {
	state := &UTXOScanState{
		LastScannedBlock: lastScannedBlock,
		LastScannedAt:    time.Now(),
		IsActive:         true,
	}
	state.ID = utxoScanStateID

	return r.db.Clauses(clause.OnConflict{
		Columns:   []clause.Column{{Name: "id"}},
		DoUpdates: clause.AssignmentColumns([]string{"last_scanned_block", "last_scanned_at", "is_active"}),
	}).Create(state).Error
}
