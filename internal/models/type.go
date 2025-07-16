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

// L1Block model
type L1Block struct {
	ID        uint      `gorm:"primaryKey" json:"id"`
	Height    uint64    `gorm:"not null;uniqueIndex" json:"height"`
	Hash      string    `gorm:"not null" json:"hash"`
	Status    string    `gorm:"not null;index:l1_block_status_index" json:"status"` // "unconfirm", "confirmed", "signing", "pending", "processed"
	UpdatedAt time.Time `gorm:"not null" json:"updated_at"`
}

// L1SyncStatus model
type L1SyncStatus struct {
	ID              uint      `gorm:"primaryKey" json:"id"`
	UnconfirmHeight int64     `gorm:"not null" json:"unconfirm_height"`
	ConfirmedHeight int64     `gorm:"not null" json:"confirmed_height"`
	UpdatedAt       time.Time `gorm:"not null" json:"updated_at"`
}

// Voter model for consensus voting
type Voter struct {
	ID        uint      `gorm:"primaryKey" json:"id"`
	Address   string    `gorm:"not null;uniqueIndex" json:"address"`
	Weight    uint64    `gorm:"not null" json:"weight"`
	Status    string    `gorm:"not null" json:"status"`
	CreatedAt time.Time `gorm:"not null" json:"created_at"`
	UpdatedAt time.Time `gorm:"not null" json:"updated_at"`
}

// EpochVoter model for epoch-based voting
type EpochVoter struct {
	ID        uint      `gorm:"primaryKey" json:"id"`
	Epoch     uint64    `gorm:"not null;index" json:"epoch"`
	Address   string    `gorm:"not null;index" json:"address"`
	Weight    uint64    `gorm:"not null" json:"weight"`
	Voted     bool      `gorm:"not null" json:"voted"`
	CreatedAt time.Time `gorm:"not null" json:"created_at"`
	UpdatedAt time.Time `gorm:"not null" json:"updated_at"`
}

// VoterQueue model for voting queue management
type VoterQueue struct {
	ID        uint      `gorm:"primaryKey" json:"id"`
	Address   string    `gorm:"not null;index" json:"address"`
	Priority  int       `gorm:"not null" json:"priority"`
	Status    string    `gorm:"not null" json:"status"`
	CreatedAt time.Time `gorm:"not null" json:"created_at"`
	UpdatedAt time.Time `gorm:"not null" json:"updated_at"`
}

// DepositPubKey model for deposit public keys
type DepositPubKey struct {
	ID        uint      `gorm:"primaryKey" json:"id"`
	PubKey    string    `gorm:"not null;uniqueIndex" json:"pub_key"`
	Address   string    `gorm:"not null" json:"address"`
	Status    string    `gorm:"not null" json:"status"`
	CreatedAt time.Time `gorm:"not null" json:"created_at"`
	UpdatedAt time.Time `gorm:"not null" json:"updated_at"`
}

// DogeBlock model for Dogecoin blocks
type DogeBlock struct {
	ID         uint      `gorm:"primaryKey" json:"id"`
	Height     uint64    `gorm:"not null;uniqueIndex" json:"height"`
	Hash       string    `gorm:"not null" json:"hash"`
	PrevHash   string    `gorm:"not null" json:"prev_hash"`
	MerkleRoot string    `gorm:"not null" json:"merkle_root"`
	Timestamp  int64     `gorm:"not null" json:"timestamp"`
	Status     string    `gorm:"not null" json:"status"`
	CreatedAt  time.Time `gorm:"not null" json:"created_at"`
	UpdatedAt  time.Time `gorm:"not null" json:"updated_at"`
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
	L1Height  int64     `gorm:"index;type:bigint" json:"l1_height"`
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
	L1Height   int64     `gorm:"index;type:bigint" json:"l1_height"`
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

// Withdraw model for withdrawal operations
type Withdraw struct {
	ID        uint      `gorm:"primaryKey" json:"id"`
	OrderId   string    `gorm:"not null;uniqueIndex" json:"order_id"`
	Address   string    `gorm:"not null" json:"address"`
	Amount    int64     `gorm:"not null" json:"amount"`
	Status    string    `gorm:"not null" json:"status"`
	Txid      string    `gorm:"index" json:"txid"`
	CreatedAt time.Time `gorm:"not null" json:"created_at"`
	UpdatedAt time.Time `gorm:"not null" json:"updated_at"`
}

// Deposit model (for managing deposits)
type Deposit struct {
	ID          uint      `gorm:"primaryKey" json:"id"`
	TxHash      string    `gorm:"not null;index:deposit_txhash_output_index" json:"tx_hash"`
	Amount      int64     `gorm:"not null;default:0" json:"amount"`
	RawTx       string    `gorm:"not null" json:"raw_tx"`
	EvmAddr     string    `gorm:"not null" json:"evm_addr"`
	BlockHash   string    `gorm:"not null;index:deposit_blockhash_index" json:"block_hash"`
	BlockHeight uint64    `gorm:"not null;index:deposit_blockhash_height" json:"block_height"`
	TxIndex     int       `gorm:"not null;index:deposit_txindex_index" json:"tx_index"`
	OutputIndex int       `gorm:"not null;index:deposit_txhash_output_index" json:"output_index"`
	MerkleRoot  []byte    `json:"merkle_root"`
	Proof       []byte    `json:"proof"`
	SignVersion uint32    `gorm:"not null" json:"sign_version"`
	Status      string    `gorm:"not null;index:deposit_status_index" json:"status"` // "unconfirm", "confirmed", "signing", "pending", "processed"
	CreatedAt   time.Time `gorm:"index:deposit_created_index" json:"created_at"`
	UpdatedAt   time.Time `gorm:"not null" json:"updated_at"`
}

// DepositResult model, it save deposit data from layer2 events
type DepositResult struct {
	ID                 uint   `gorm:"primaryKey" json:"id"`
	Txid               string `gorm:"uniqueIndex:idx_txid_tx_out" json:"txid"`
	TxOut              uint64 `gorm:"uniqueIndex:idx_txid_tx_out" json:"tx_out"`
	Address            string `gorm:"not null" json:"address"`
	Amount             uint64 `gorm:"not null" json:"amount"`
	BlockHash          string `gorm:"not null" json:"block_hash"`
	NeedFetchSubScript bool   `gorm:"not null;default:false;index:idx_need_fetch_sub_script" json:"need_fetch_sub_script"` // if true, need fetch sub script from dogecoin client, or fetch non-exist utxo then save
}

// DogeSyncStatus model for Dogecoin sync status
type DogeSyncStatus struct {
	ID              uint      `gorm:"primaryKey" json:"id"`
	UnconfirmHeight int64     `gorm:"not null" json:"unconfirm_height"`
	ConfirmedHeight int64     `gorm:"not null" json:"confirmed_height"`
	UpdatedAt       time.Time `gorm:"not null" json:"updated_at"`
}

// DogeBlockData model for Dogecoin block data
type DogeBlockData struct {
	ID        uint      `gorm:"primaryKey" json:"id"`
	Height    uint64    `gorm:"not null;uniqueIndex" json:"height"`
	Hash      string    `gorm:"not null" json:"hash"`
	Data      []byte    `gorm:"type:blob" json:"data"`
	Status    string    `gorm:"not null" json:"status"`
	CreatedAt time.Time `gorm:"not null" json:"created_at"`
	UpdatedAt time.Time `gorm:"not null" json:"updated_at"`
}

// DogeTXOutput model for Dogecoin transaction outputs
type DogeTXOutput struct {
	ID        uint      `gorm:"primaryKey" json:"id"`
	Txid      string    `gorm:"not null;index" json:"txid"`
	OutIndex  int       `gorm:"not null" json:"out_index"`
	Amount    int64     `gorm:"not null" json:"amount"`
	Script    []byte    `gorm:"type:blob" json:"script"`
	Address   string    `gorm:"index" json:"address"`
	Status    string    `gorm:"not null" json:"status"`
	CreatedAt time.Time `gorm:"not null" json:"created_at"`
	UpdatedAt time.Time `gorm:"not null" json:"updated_at"`
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
