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

// UTXO represents an unspent transaction output
type UTXO struct {
	gorm.Model `swaggerignore:"true"`

	Uid           string    `gorm:"primaryKey;type:varchar(255)" json:"uid"`
	Txid          string    `gorm:"index;type:varchar(255)" json:"txid"`
	PkScript      []byte    `gorm:"type:blob" json:"pk_script"`
	OutIndex      int       `gorm:"index" json:"out_index"`
	Amount        int64     `gorm:"type:bigint" json:"amount"`
	Receiver      string    `gorm:"index;type:varchar(255)" json:"receiver"`
	WalletVersion string    `gorm:"type:varchar(10)" json:"wallet_version"`
	Sender        string    `gorm:"type:varchar(255)" json:"sender"`
	EvmAddr       string    `gorm:"type:varchar(255)" json:"evm_addr"`
	Source        string    `gorm:"type:varchar(50)" json:"source"`
	ReceiverType  string    `gorm:"type:varchar(20)" json:"receiver_type"`
	Status        string    `gorm:"type:varchar(20)" json:"status"`
	ReceiveBlock  int64     `gorm:"index;type:bigint" json:"receive_block"`
	SpentBlock    int64     `gorm:"type:bigint" json:"spent_block"`
	UpdatedAt     time.Time `json:"updated_at"`
}

// VIN represents a transaction input
type VIN struct {
	gorm.Model `swaggerignore:"true"`

	OrderId   string    `gorm:"type:varchar(255)" json:"order_id"`
	BtcHeight int64     `gorm:"index;type:bigint" json:"btc_height"`
	Txid      string    `gorm:"index;type:varchar(255)" json:"txid"`
	OutIndex  int       `gorm:"index" json:"out_index"`
	SigScript []byte    `gorm:"type:blob" json:"sig_script"`
	Sender    string    `gorm:"type:varchar(255)" json:"sender"`
	Source    string    `gorm:"type:varchar(50)" json:"source"`
	Status    string    `gorm:"type:varchar(20)" json:"status"`
	UpdatedAt time.Time `json:"updated_at"`
}

// VOUT represents a transaction output
type VOUT struct {
	gorm.Model `swaggerignore:"true"`

	OrderId    string    `gorm:"type:varchar(255)" json:"order_id"`
	BtcHeight  int64     `gorm:"index;type:bigint" json:"btc_height"`
	Txid       string    `gorm:"index;type:varchar(255)" json:"txid"`
	OutIndex   int       `gorm:"index" json:"out_index"`
	WithdrawId string    `gorm:"type:varchar(255)" json:"withdraw_id"`
	Amount     int64     `gorm:"type:bigint" json:"amount"`
	Receiver   string    `gorm:"type:varchar(255)" json:"receiver"`
	Sender     string    `gorm:"type:varchar(255)" json:"sender"`
	Source     string    `gorm:"type:varchar(50)" json:"source"`
	Status     string    `gorm:"type:varchar(20)" json:"status"`
	UpdatedAt  time.Time `json:"updated_at"`
}

// SendOrder represents a send order
type SendOrder struct {
	gorm.Model `swaggerignore:"true"`

	OrderId      string    `gorm:"primaryKey;type:varchar(255)" json:"order_id"`
	OrderType    string    `gorm:"type:varchar(50)" json:"order_type"`
	Txid         string    `gorm:"index;type:varchar(255)" json:"txid"`
	ExternalId   string    `gorm:"index;type:varchar(255)" json:"external_id"`
	Status       string    `gorm:"type:varchar(20)" json:"status"`
	ConfirmBlock int64     `gorm:"type:bigint" json:"confirm_block"`
	UpdatedAt    time.Time `json:"updated_at"`
}

// Constants for UTXO source types
const (
	UTXO_SOURCE_UNKNOWN       = "unknown"
	UTXO_SOURCE_DEPOSIT       = "deposit"
	UTXO_SOURCE_CONSOLIDATION = "consolidation"
	UTXO_SOURCE_WITHDRAWAL    = "withdrawal"
)

// Constants for UTXO status
const (
	UTXO_STATUS_UNCONFIRMED = "unconfirmed"
	UTXO_STATUS_CONFIRMED   = "confirmed"
	UTXO_STATUS_SPENT       = "spent"
)

// Constants for wallet types
const (
	WALLET_TYPE_UNKNOWN = "unknown"
	WALLET_TYPE_P2PKH   = "p2pkh"
	WALLET_TYPE_P2WPKH  = "p2wpkh"
	WALLET_TYPE_P2WSH   = "p2wsh"
)

// Constants for order types
const (
	ORDER_TYPE_WITHDRAWAL    = "withdrawal"
	ORDER_TYPE_CONSOLIDATION = "consolidation"
	ORDER_TYPE_SAFEBOX       = "safebox"
)
