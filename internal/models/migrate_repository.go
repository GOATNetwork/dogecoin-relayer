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
