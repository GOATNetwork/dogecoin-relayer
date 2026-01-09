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

type Deposit struct {
	gorm.Model `swaggerignore:"true"`

	TxId        string `gorm:"type:varchar(66);not null;index" json:"txid"`
	Vout        int    `gorm:"type:int;not null" json:"vout"`
	Address     string `gorm:"type:varchar(60)" json:"address"`
	EvmAddr     string `gorm:"type:varchar(255)" json:"evm_addr"`
	Amount      int64  `gorm:"type:bigint" json:"amount"`
	TxBytes     []byte `gorm:"type:blob" json:"tx_bytes"`
	Status      string `gorm:"type:varchar(20)" json:"status"`
	EvmTxHash   string `gorm:"type:varchar(66)" json:"evm_tx_hash"`
	EvmBlock    uint64 `gorm:"type:bigint" json:"evm_block"`
	EvmLogIndex uint   `gorm:"type:int" json:"evm_log_index"`
}

type Withdrawal struct {
	gorm.Model `swaggerignore:"true"`

	ReqTaskId      string `gorm:"type:varchar(255);not null;uniqueIndex" json:"req_task_id"`
	ReqTxHash      string `gorm:"type:varchar(66)" json:"req_tx_hash"`
	ReqBlock       uint64 `gorm:"type:bigint" json:"req_block"`
	ReqLogIndex    uint   `gorm:"type:int" json:"req_log_index"`
	Status         string `gorm:"type:varchar(20)" json:"status"`
	DestAddress    string `gorm:"type:varchar(100)" json:"dest_address"`
	DestAmount     string `gorm:"type:varchar(80)" json:"dest_amount"`
	TxId           string `gorm:"type:varchar(66)" json:"txid"`
	Vout           int    `gorm:"type:int" json:"vout"`
	TxBytes        []byte `gorm:"type:blob" json:"tx_bytes"`
	FinishTxHash   string `gorm:"type:varchar(66)" json:"finish_tx_hash"`
	FinishBlock    uint64 `gorm:"type:bigint" json:"finish_block"`
	FinishLogIndex uint   `gorm:"type:int" json:"finish_log_index"`
}

type Proposers struct {
	gorm.Model `swaggerignore:"true"`

	Address      string `gorm:"type:varchar(60)" json:"address"`
	Status       string `gorm:"type:varchar(20)" json:"status"`
	PendingEvent string `gorm:"type:varchar(255)" json:"pending_event"`
	JoinBlock    uint64 `gorm:"type:bigint" json:"join_block"`
	ExitBlock    uint64 `gorm:"type:bigint" json:"exit_block"`
}

// EventScanState tracks the scanning progress for event detection
type EventScanState struct {
	gorm.Model `swaggerignore:"true"`

	LastScannedBlock   uint64    `gorm:"type:bigint;not null;default:0" json:"last_scanned_block"`
	LastScannedAt      time.Time `gorm:"type:timestamp" json:"last_scanned_at,omitempty"`
	ConfirmationBlocks uint64    `gorm:"type:bigint;default:6" json:"confirmation_blocks"`
	IsActive           bool      `gorm:"default:true" json:"is_active"`
}

// UTXOScanState tracks the scanning progress for Doge UTXO scanning
type UTXOScanState struct {
	gorm.Model `swaggerignore:"true"`

	LastScannedBlock uint64    `gorm:"type:bigint;not null;default:0" json:"last_scanned_block"`
	LastScannedAt    time.Time `gorm:"type:timestamp" json:"last_scanned_at,omitempty"`
	IsActive         bool      `gorm:"default:true" json:"is_active"`
}

// UTXO represents an unspent transaction output
type UTXO struct {
	gorm.Model `swaggerignore:"true"`

	Uid           string    `gorm:"uniqueIndex:idx_utxo_uid;type:varchar(255)" json:"uid"`
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
	UTXO_STATUS_PROCESSED   = "processed"
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

// PendingBatch represents a batch waiting for TSS signature
type PendingBatch struct {
	gorm.Model `swaggerignore:"true"`

	BaseSessionID string    `gorm:"uniqueIndex:idx_pending_batch_base_session;type:varchar(255);not null" json:"base_session_id"`
	BatchType     string    `gorm:"type:varchar(20);not null" json:"batch_type"` // "deposit" or "withdrawal"
	CallData      []byte    `gorm:"type:blob" json:"calldata"`
	TssNonce      string    `gorm:"type:varchar(80)" json:"tss_nonce"`
	NextAttempt   int       `gorm:"type:int;default:0" json:"next_attempt"`
	LastAttempt   time.Time `gorm:"type:timestamp" json:"last_attempt"`
	NextRetryAt   time.Time `gorm:"type:timestamp" json:"next_retry_at"`
	Status        string    `gorm:"type:varchar(20);default:'pending'" json:"status"` // "pending", "completed", "failed"

	// Deposit-specific fields (nullable)
	BatchID     string `gorm:"type:varchar(80)" json:"batch_id,omitempty"`
	TotalAmount string `gorm:"type:varchar(80)" json:"total_amount,omitempty"`

	// Withdrawal-specific fields (nullable)
	WithdrawalID string `gorm:"type:varchar(255)" json:"withdrawal_id,omitempty"`
	TaskIdsJSON  string `gorm:"type:text" json:"task_ids_json,omitempty"`
	TxId         string `gorm:"type:varchar(66)" json:"txid,omitempty"`
}

// Constants for pending batch status
const (
	PENDING_BATCH_STATUS_PENDING   = "pending"
	PENDING_BATCH_STATUS_COMPLETED = "completed"
	PENDING_BATCH_STATUS_FAILED    = "failed"
)
