package models

import (
	"gorm.io/gorm"
)

// MigrateLog represents the migration log information for a blockchain.
// It includes the serial number and description.
type MigrateLog struct {
	gorm.Model `swaggerignore:"true"`

	Version uint64 `gorm:"uniqueIndex:idx_migrate_log_unique;type:bigint" json:"version" binding:"required"`
	Desc    string `gorm:"type:varchar(255)" json:"desc"`
}
