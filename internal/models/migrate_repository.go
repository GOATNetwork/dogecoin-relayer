package models

import (
	"fmt"
	"sync"

	"gorm.io/gorm"
)

type MigrateRepository struct {
	db *gorm.DB
	mu sync.Mutex

	migrateList map[uint64]func() error
}

func NewMigrateRepository(db *gorm.DB) *MigrateRepository {
	r := &MigrateRepository{db: db}
	r.initMigrateList()
	return r
}

func (r *MigrateRepository) initMigrateList() {
	r.migrateList = make(map[uint64]func() error)

	// Migration 1: Create basic migration log table
	r.migrateList[1] = r.createMigrationLogTable

	// Migration 2: Create event detection tables
	r.migrateList[2] = r.createEventDetectionTables

	// Migration 3: Create consensus-related tables
	r.migrateList[3] = r.createConsensusTables

	// Migration 4: Create scanner-related tables (UTXO/VIN/VOUT/SendOrder)
	r.migrateList[4] = r.createScannerTables

	// Migration 5: Create pending batch table for TSS signature tracking
	r.migrateList[5] = r.createPendingBatchTables

	// Migration 6: Add utxos_json to pending_batches
	r.migrateList[6] = r.addUtxosJsonColumn
}

func (r *MigrateRepository) DoMigrate() error {
	for version, migrateFunc := range r.migrateList {
		err := migrateFunc()
		if err != nil {
			return fmt.Errorf("migrate %d failed: %w", version, err)
		}
	}
	return nil
}

// Migration functions
func (r *MigrateRepository) createMigrationLogTable() error {
	return r.db.AutoMigrate(&MigrateLog{})
}

func (r *MigrateRepository) createEventDetectionTables() error {
	return r.db.AutoMigrate(
		&EventScanState{},
	)
}

func (r *MigrateRepository) createConsensusTables() error {
	return r.db.AutoMigrate(
		&Deposit{},
		&Withdrawal{},
		&Proposers{},
	)
}

// createScannerTables creates tables used by the Doge scanner/pipeline
func (r *MigrateRepository) createScannerTables() error {
	if r.db.Migrator().HasTable(&UTXO{}) {
		if err := r.db.Exec("DELETE FROM utxos WHERE id NOT IN (SELECT MIN(id) FROM utxos GROUP BY uid)").Error; err != nil {
			return fmt.Errorf("dedupe utxos by uid: %w", err)
		}
	}
	return r.db.AutoMigrate(
		&UTXOScanState{},
		&UTXO{},
		&VIN{},
		&VOUT{},
		&SendOrder{},
	)
}

func (r *MigrateRepository) createPendingBatchTables() error {
	return r.db.AutoMigrate(&PendingBatch{})
}

func (r *MigrateRepository) addUtxosJsonColumn() error {
	return r.db.AutoMigrate(&PendingBatch{})
}
