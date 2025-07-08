package models

import (
	"time"

	"gorm.io/gorm"
)

// MigrateLog represents the migration log information for a blockchain.
// It includes the serial number and description.
type MigrateLog struct {
	gorm.Model `swaggerignore:"true"`

	Version uint64 `gorm:"uniqueIndex:idx_migrate_log_unique;type:bigint" json:"version" binding:"required"`
	Desc    string `gorm:"type:varchar(255)" json:"desc"`
}

// DetectedEvent represents a blockchain event that has been detected and processed
type DetectedEvent struct {
	gorm.Model `swaggerignore:"true"`

	BlockNumber     uint64    `gorm:"type:bigint;not null;index" json:"block_number"`
	TxHash          string    `gorm:"type:varchar(66);not null;index" json:"tx_hash"` // 0x + 64 chars
	LogIndex        uint      `gorm:"type:int;not null" json:"log_index"`
	ContractAddress string    `gorm:"type:varchar(42);not null;index" json:"contract_address"`
	EventName       string    `gorm:"type:varchar(100);not null;index" json:"event_name"`
	EventData       string    `gorm:"type:text" json:"event_data"` // JSON string of event data
	ProcessedAt     time.Time `gorm:"type:timestamp;not null" json:"processed_at"`
	Status          string    `gorm:"type:varchar(20);default:'pending';index" json:"status"` // pending, processed, failed

	// Unique constraint to prevent duplicate event processing
	UniqueIndex string `gorm:"uniqueIndex:idx_detected_event_unique;type:varchar(150);not null"`
}

// EventScanState tracks the scanning progress for event detection
type EventScanState struct {
	gorm.Model `swaggerignore:"true"`

	ContractAddress    string    `gorm:"type:varchar(42);not null;uniqueIndex" json:"contract_address"`
	LastScannedBlock   uint64    `gorm:"type:bigint;not null;default:0" json:"last_scanned_block"`
	LastScannedAt      time.Time `gorm:"type:timestamp" json:"last_scanned_at,omitempty"`
	ConfirmationBlocks uint64    `gorm:"type:bigint;default:6" json:"confirmation_blocks"`
	IsActive           bool      `gorm:"default:true" json:"is_active"`
}

// EventProcessingLog tracks the processing status and any errors for detected events
type EventProcessingLog struct {
	gorm.Model `swaggerignore:"true"`

	DetectedEventID uint      `gorm:"not null;index" json:"detected_event_id"`
	ProcessingStep  string    `gorm:"type:varchar(100);not null" json:"processing_step"`
	Status          string    `gorm:"type:varchar(20);not null" json:"status"` // success, failed, retry
	ErrorMessage    string    `gorm:"type:text" json:"error_message,omitempty"`
	ProcessedAt     time.Time `gorm:"type:timestamp;not null" json:"processed_at"`
	RetryCount      int       `gorm:"default:0" json:"retry_count"`

	// Foreign key relationship
	DetectedEvent DetectedEvent `gorm:"foreignKey:DetectedEventID"`
}
