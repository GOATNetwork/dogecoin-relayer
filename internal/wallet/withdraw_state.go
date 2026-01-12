package wallet

import (
	"bytes"
	"fmt"
	"time"

	"github.com/dogecoinw/doged/txscript"
	"github.com/dogecoinw/doged/wire"
	"github.com/goat-network/dogecoin-relayer/internal/models"
	"github.com/goat-network/dogecoin-relayer/pkg/global"
	"github.com/goat-network/dogecoin-relayer/pkg/types"
	"gorm.io/gorm"
)

func CreateSendOrder(tx *wire.MsgTx, orderType string, selectedUtxos []*UTXOAdapter, selectedWithdrawals []*WithdrawalAdapter, changeAddr string, db *gorm.DB) (*models.SendOrder, []*models.VIN, []*models.VOUT, error) {
	noWitnessTx := new(bytes.Buffer)
	if err := tx.SerializeNoWitness(noWitnessTx); err != nil {
		return nil, nil, nil, err
	}

	net := types.GetDogeNetwork(global.GetConfig().Doge.NetworkType)

	order := &models.SendOrder{
		OrderId:      generateOrderID(),
		OrderType:    orderType,
		Txid:         tx.TxHash().String(),
		Status:       "aggregating",
		ConfirmBlock: 0,
		UpdatedAt:    time.Now(),
	}

	var vins []*models.VIN
	for _, utxo := range selectedUtxos {
		vin := &models.VIN{
			OrderId:   order.OrderId,
			BtcHeight: 0,
			Txid:      utxo.Txid,
			OutIndex:  utxo.OutIndex,
			SigScript: nil,
			Sender:    "",
			Source:    orderType,
			Status:    "aggregating",
			UpdatedAt: time.Now(),
		}
		vins = append(vins, vin)
	}

	var vouts []*models.VOUT
	for i, txOut := range tx.TxOut {
		_, addresses, _, err := txscript.ExtractPkScriptAddrs(txOut.PkScript, net)
		if err != nil {
			continue
		}

		withdrawId := ""
		if len(addresses) > 0 {
			receiver := addresses[0].EncodeAddress()
			for _, withdrawal := range selectedWithdrawals {
				if withdrawal != nil && withdrawal.DestAddress == receiver {
					withdrawId = withdrawal.ReqTaskId
					break
				}
			}
		}
		if withdrawId == "" && i < len(selectedWithdrawals) {
			withdrawId = selectedWithdrawals[i].ReqTaskId
		}

		vout := &models.VOUT{
			OrderId:    order.OrderId,
			BtcHeight:  0,
			Txid:       tx.TxHash().String(),
			OutIndex:   i,
			WithdrawId: withdrawId,
			Amount:     txOut.Value,
			Receiver:   addresses[0].EncodeAddress(),
			Sender:     "",
			Source:     orderType,
			Status:     "aggregating",
			UpdatedAt:  time.Now(),
		}
		vouts = append(vouts, vout)
	}

	if err := db.Transaction(func(tx *gorm.DB) error {
		if err := tx.Create(order).Error; err != nil {
			return err
		}

		for _, vin := range vins {
			if err := tx.Create(vin).Error; err != nil {
				return err
			}
		}

		for _, vout := range vouts {
			if err := tx.Create(vout).Error; err != nil {
				return err
			}
		}

		return nil
	}); err != nil {
		return nil, nil, nil, fmt.Errorf("save send order to db: %v", err)
	}

	return order, vins, vouts, nil
}

func UpdateSendOrderPending(txid, externalId string, db *gorm.DB, vins []*models.VIN, vouts []*models.VOUT) error {
	return db.Transaction(func(tx *gorm.DB) error {
		if err := tx.Model(&models.SendOrder{}).
			Where("txid = ?", txid).
			Updates(map[string]interface{}{
				"status":      "pending",
				"external_id": externalId,
				"updated_at":  time.Now(),
			}).Error; err != nil {
			return err
		}

		for _, vin := range vins {
			if err := tx.Model(vin).
				Where("order_id = ?", vin.OrderId).
				Updates(map[string]interface{}{
					"status":     "pending",
					"updated_at": time.Now(),
				}).Error; err != nil {
				return err
			}
		}

		for _, vout := range vouts {
			if err := tx.Model(vout).
				Where("order_id = ?", vout.OrderId).
				Updates(map[string]interface{}{
					"status":     "pending",
					"updated_at": time.Now(),
				}).Error; err != nil {
				return err
			}
		}

		return nil
	})
}

func CloseSendOrdersForWithdrawal(db *gorm.DB, withdrawId string) error {
	if withdrawId == "" {
		return nil
	}
	return db.Transaction(func(tx *gorm.DB) error {
		var orderIds []string
		if err := tx.Model(&models.VOUT{}).
			Distinct("order_id").
			Where("withdraw_id = ?", withdrawId).
			Pluck("order_id", &orderIds).Error; err != nil {
			return err
		}
		if len(orderIds) == 0 {
			return nil
		}
		now := time.Now()
		if err := tx.Model(&models.SendOrder{}).
			Where("order_id IN ? AND status IN ?", orderIds, []string{"aggregating", "init", "pending"}).
			Updates(map[string]interface{}{
				"status":     "closed",
				"updated_at": now,
			}).Error; err != nil {
			return err
		}
		if err := tx.Model(&models.VIN{}).
			Where("order_id IN ?", orderIds).
			Updates(map[string]interface{}{
				"status":     "closed",
				"updated_at": now,
			}).Error; err != nil {
			return err
		}
		if err := tx.Model(&models.VOUT{}).
			Where("order_id IN ?", orderIds).
			Updates(map[string]interface{}{
				"status":     "closed",
				"updated_at": now,
			}).Error; err != nil {
			return err
		}
		return nil
	})
}

// CleanProcessingSendOrders cleans up "aggregating" status send orders and their associated UTXOs on startup.
// This handles cases where the service crashed or restarted while processing withdrawals.
// It returns the count of reset UTXOs for logging purposes.
func CleanProcessingSendOrders(db *gorm.DB) (int64, error) {
	var resetCount int64
	err := db.Transaction(func(tx *gorm.DB) error {
		var orders []models.SendOrder
		if err := tx.Where("status = ?", "aggregating").Find(&orders).Error; err != nil {
			return err
		}
		if len(orders) == 0 {
			return nil
		}
		orderIds := make([]string, 0, len(orders))
		for _, order := range orders {
			orderIds = append(orderIds, order.OrderId)
		}

		// First, get all VINs associated with these orders to reset their corresponding UTXOs
		var vins []models.VIN
		if err := tx.Where("order_id IN ?", orderIds).Find(&vins).Error; err != nil {
			return err
		}

		// Reset UTXOs that are in PENDING status and associated with these orders
		for _, vin := range vins {
			result := tx.Model(&models.UTXO{}).
				Where("txid = ? AND out_index = ? AND status = ?", vin.Txid, vin.OutIndex, models.UTXO_STATUS_PENDING).
				Update("status", models.UTXO_STATUS_PROCESSED)
			if result.Error != nil {
				return result.Error
			}
			resetCount += result.RowsAffected
		}

		now := time.Now()
		if err := tx.Model(&models.SendOrder{}).
			Where("order_id IN ?", orderIds).
			Updates(map[string]interface{}{
				"status":     "closed",
				"updated_at": now,
			}).Error; err != nil {
			return err
		}
		if err := tx.Model(&models.VIN{}).
			Where("order_id IN ?", orderIds).
			Updates(map[string]interface{}{
				"status":     "closed",
				"updated_at": now,
			}).Error; err != nil {
			return err
		}
		if err := tx.Model(&models.VOUT{}).
			Where("order_id IN ?", orderIds).
			Updates(map[string]interface{}{
				"status":     "closed",
				"updated_at": now,
			}).Error; err != nil {
			return err
		}
		return nil
	})
	return resetCount, err
}

func generateOrderID() string {
	return fmt.Sprintf("order_%d", time.Now().UnixNano())
}

func ToUTXOAdapter(utxo *models.UTXO) *UTXOAdapter {
	receiverType := utxo.ReceiverType
	if receiverType == "" {
		receiverType = WALLET_TYPE_P2PKH
	}
	return &UTXOAdapter{
		UTXO:         utxo,
		ReceiverType: receiverType,
		SubScript:    utxo.PkScript,
	}
}

func ToWithdrawalAdapter(withdrawal *models.Withdrawal, amountSat int64, txPrice int64) *WithdrawalAdapter {
	return &WithdrawalAdapter{
		Withdrawal: withdrawal,
		Amount:     amountSat,
		TxPrice:    txPrice,
	}
}
