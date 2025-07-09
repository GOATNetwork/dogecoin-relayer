package models

import (
	"fmt"
	"time"

	"gorm.io/gorm"
)

// UTXORepository handles UTXO database operations
type UTXORepository struct {
	db *gorm.DB
}

func NewUTXORepository(db *gorm.DB) *UTXORepository {
	return &UTXORepository{db: db}
}

func (r *UTXORepository) AddUTXO(utxo *UTXO, pubkeyBytes []byte, blockHash string, blockHeight int64, noWitnessTx []byte, merkleRoot string, proofBytes []byte, txIndex int, isDeposit bool) error {
	// Generate UID for UTXO
	utxo.Uid = fmt.Sprintf("%s:%d", utxo.Txid, utxo.OutIndex)

	// Set default values
	if utxo.WalletVersion == "" {
		utxo.WalletVersion = "1"
	}
	if utxo.Status == "" {
		utxo.Status = UTXO_STATUS_CONFIRMED
	}
	if utxo.Source == "" {
		utxo.Source = UTXO_SOURCE_UNKNOWN
	}

	utxo.UpdatedAt = time.Now()

	return r.db.Create(utxo).Error
}

func (r *UTXORepository) UpdateUTXOStatusSpentByVins(vins []*VIN, spentBlock int64) error {
	for _, vin := range vins {
		err := r.db.Model(&UTXO{}).
			Where("txid = ? AND out_index = ?", vin.Txid, vin.OutIndex).
			Updates(map[string]interface{}{
				"status":      UTXO_STATUS_SPENT,
				"spent_block": spentBlock,
				"updated_at":  time.Now(),
			}).Error
		if err != nil {
			return err
		}
	}
	return nil
}

// VINRepository handles VIN database operations
type VINRepository struct {
	db *gorm.DB
}

func NewVINRepository(db *gorm.DB) *VINRepository {
	return &VINRepository{db: db}
}

func (r *VINRepository) AddOrUpdateVIN(vin *VIN) error {
	if vin.Status == "" {
		vin.Status = UTXO_STATUS_CONFIRMED
	}
	if vin.Source == "" {
		vin.Source = UTXO_SOURCE_UNKNOWN
	}

	vin.UpdatedAt = time.Now()

	// Use upsert logic
	var existingVIN VIN
	err := r.db.Where("txid = ? AND out_index = ?", vin.Txid, vin.OutIndex).First(&existingVIN).Error
	if err == gorm.ErrRecordNotFound {
		return r.db.Create(vin).Error
	} else if err != nil {
		return err
	}

	return r.db.Model(&existingVIN).Updates(vin).Error
}

// VOUTRepository handles VOUT database operations
type VOUTRepository struct {
	db *gorm.DB
}

func NewVOUTRepository(db *gorm.DB) *VOUTRepository {
	return &VOUTRepository{db: db}
}

func (r *VOUTRepository) AddOrUpdateVOUT(vout *VOUT) error {
	if vout.Status == "" {
		vout.Status = UTXO_STATUS_CONFIRMED
	}
	if vout.Source == "" {
		vout.Source = UTXO_SOURCE_UNKNOWN
	}

	vout.UpdatedAt = time.Now()

	// Use upsert logic
	var existingVOUT VOUT
	err := r.db.Where("txid = ? AND out_index = ?", vout.Txid, vout.OutIndex).First(&existingVOUT).Error
	if err == gorm.ErrRecordNotFound {
		return r.db.Create(vout).Error
	} else if err != nil {
		return err
	}

	return r.db.Model(&existingVOUT).Updates(vout).Error
}

// SendOrderRepository handles SendOrder database operations
type SendOrderRepository struct {
	db *gorm.DB
}

func NewSendOrderRepository(db *gorm.DB) *SendOrderRepository {
	return &SendOrderRepository{db: db}
}

func (r *SendOrderRepository) GetSendOrderByTxIdOrExternalId(txid string) (*SendOrder, error) {
	var order SendOrder
	err := r.db.Where("txid = ? OR external_id = ?", txid, txid).First(&order).Error
	if err == gorm.ErrRecordNotFound {
		return nil, nil
	}
	return &order, err
}

func (r *SendOrderRepository) UpdateSendOrderConfirmed(txid string, confirmBlock int64) error {
	return r.db.Model(&SendOrder{}).
		Where("txid = ?", txid).
		Updates(map[string]interface{}{
			"status":        "confirmed",
			"confirm_block": confirmBlock,
			"updated_at":    time.Now(),
		}).Error
}

// StateRepository combines all repositories for convenience
type StateRepository struct {
	UTXORepo      *UTXORepository
	VINRepo       *VINRepository
	VOUTRepo      *VOUTRepository
	SendOrderRepo *SendOrderRepository
}

func NewStateRepository(db *gorm.DB) *StateRepository {
	return &StateRepository{
		UTXORepo:      NewUTXORepository(db),
		VINRepo:       NewVINRepository(db),
		VOUTRepo:      NewVOUTRepository(db),
		SendOrderRepo: NewSendOrderRepository(db),
	}
}

func (s *StateRepository) AddUtxo(utxo *UTXO, pubkeyBytes []byte, blockHash string, blockHeight int64, noWitnessTx []byte, merkleRoot string, proofBytes []byte, txIndex int, isDeposit bool) error {
	return s.UTXORepo.AddUTXO(utxo, pubkeyBytes, blockHash, blockHeight, noWitnessTx, merkleRoot, proofBytes, txIndex, isDeposit)
}

func (s *StateRepository) AddOrUpdateVin(vin *VIN) error {
	return s.VINRepo.AddOrUpdateVIN(vin)
}

func (s *StateRepository) AddOrUpdateVout(vout *VOUT) error {
	return s.VOUTRepo.AddOrUpdateVOUT(vout)
}

func (s *StateRepository) GetSendOrderByTxIdOrExternalId(txid string) (*SendOrder, error) {
	return s.SendOrderRepo.GetSendOrderByTxIdOrExternalId(txid)
}

func (s *StateRepository) UpdateSendOrderConfirmed(txid string, confirmBlock int64) error {
	return s.SendOrderRepo.UpdateSendOrderConfirmed(txid, confirmBlock)
}

func (s *StateRepository) UpdateUtxoStatusSpentByVins(vins []*VIN, spentBlock int64) error {
	return s.UTXORepo.UpdateUTXOStatusSpentByVins(vins, spentBlock)
}
