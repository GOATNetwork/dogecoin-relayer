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
		&DetectedEvent{},
		&EventScanState{},
		&EventProcessingLog{},
	)
}
