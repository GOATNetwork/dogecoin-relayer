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
		if i < len(selectedWithdrawals) {
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

func generateOrderID() string {
	return fmt.Sprintf("order_%d", time.Now().UnixNano())
}

func ToUTXOAdapter(utxo *models.UTXO) *UTXOAdapter {
	return &UTXOAdapter{
		UTXO:         utxo,
		ReceiverType: utxo.ReceiverType,
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
