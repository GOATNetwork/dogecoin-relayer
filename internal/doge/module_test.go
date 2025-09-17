package doge

import (
	"encoding/hex"
	"testing"

	"github.com/dogecoinw/doged/btcutil"
	"github.com/dogecoinw/doged/chaincfg/chainhash"
	"github.com/dogecoinw/doged/txscript"
	"github.com/dogecoinw/doged/wire"
	"github.com/goat-network/dogecoin-relayer/internal/config"
	"github.com/goat-network/dogecoin-relayer/internal/models"
	"github.com/goat-network/dogecoin-relayer/pkg/types"
	log "github.com/sirupsen/logrus"
	"github.com/stretchr/testify/require"
	"gorm.io/driver/sqlite"
	"gorm.io/gorm"
)

func newTestModule(t *testing.T) (*DogeModule, *gorm.DB) {
	t.Helper()

	db, err := gorm.Open(sqlite.Open(":memory:"), &gorm.Config{})
	require.NoError(t, err)

	require.NoError(t, db.AutoMigrate(
		&models.Deposit{},
		&models.UTXO{},
		&models.VIN{},
		&models.VOUT{},
		&models.SendOrder{},
	))

	module := &DogeModule{
		cfg:       config.DogeConfig{},
		logger:    log.WithField("suite", "doge-test"),
		state:     models.NewStateRepository(db),
		eventRepo: models.NewEventRepository(db),
	}

	return module, db
}

func TestRecordDepositCreatesEntry(t *testing.T) {
	module, db := newTestModule(t)

	utxo := &models.UTXO{
		Txid:     "tx1",
		OutIndex: 0,
		Receiver: "addr1",
		Amount:   1_500_000,
	}

	txBytes := []byte{0x01, 0x02}
	require.NoError(t, module.recordDeposit(utxo, utxo.Txid, txBytes))

	var dep models.Deposit
	require.NoError(t, db.Where("tx_id = ? AND vout = ?", utxo.Txid, utxo.OutIndex).First(&dep).Error)
	require.Equal(t, int64(1_500_000), dep.Amount)
	require.Equal(t, "pending", dep.Status)
	require.Equal(t, txBytes, dep.TxBytes)

	utxo.Amount = 2_000_000
	require.NoError(t, module.recordDeposit(utxo, utxo.Txid, txBytes))

	var updated models.Deposit
	require.NoError(t, db.Where("tx_id = ? AND vout = ?", utxo.Txid, utxo.OutIndex).First(&updated).Error)
	require.Equal(t, int64(2_000_000), updated.Amount)
}

func TestHandleSpendingTransactionMarksSpent(t *testing.T) {
	module, db := newTestModule(t)

	require.NoError(t, db.Create(&models.SendOrder{
		OrderId:   "order-1",
		OrderType: models.ORDER_TYPE_WITHDRAWAL,
		Txid:      "spend-tx",
		Status:    "init",
	}).Error)

	require.NoError(t, db.Create(&models.UTXO{
		Uid:      "prevtx:0",
		Txid:     "prevtx",
		OutIndex: 0,
		Amount:   1_000_000,
		Status:   models.UTXO_STATUS_CONFIRMED,
	}).Error)

	module.state = models.NewStateRepository(db)

	vin := &models.VIN{
		Txid:     "prevtx",
		OutIndex: 0,
	}

	require.NoError(t, module.handleSpendingTransaction("spend-tx", []*models.VIN{vin}, 42))

	var order models.SendOrder
	require.NoError(t, db.Where("txid = ?", "spend-tx").First(&order).Error)
	require.Equal(t, "confirmed", order.Status)
	require.Equal(t, int64(42), order.ConfirmBlock)

	var utxo models.UTXO
	require.NoError(t, db.Where("txid = ? AND out_index = ?", "prevtx", 0).First(&utxo).Error)
	require.Equal(t, models.UTXO_STATUS_SPENT, utxo.Status)
}

func TestProcessDepositTransaction(t *testing.T) {
	module, db := newTestModule(t)

	watchAddress := "n46CVKhdq21FWm7fCwmtUZVadvEMPEDqE2"
	evmAddress := "0x1234567890abcdef1234567890abcdef12345678"
	magicHex := "deadbeef"

	module.cfg = config.DogeConfig{
		NetworkType:       "regtest",
		WatchAddresses:    []string{watchAddress},
		DepositMagicBytes: "0x" + magicHex,
		MinDepositAmount:  1_000_000,
	}

	netParams := types.GetDogeNetwork(module.cfg.NetworkType)

	addr, err := btcutil.DecodeAddress(watchAddress, netParams)
	require.NoError(t, err)

	payScript, err := txscript.PayToAddrScript(addr)
	require.NoError(t, err)

	magicBytes, err := hex.DecodeString(magicHex)
	require.NoError(t, err)

	evmBytes, err := hex.DecodeString(evmAddress[2:])
	require.NoError(t, err)
	require.Len(t, evmBytes, 20)

	opReturnScript, err := txscript.NewScriptBuilder().
		AddOp(txscript.OP_RETURN).
		AddData(append(magicBytes, evmBytes...)).
		Script()
	require.NoError(t, err)

	tx := wire.NewMsgTx(wire.TxVersion)

	prevOut := &wire.TxIn{
		PreviousOutPoint: wire.OutPoint{
			Hash:  chainhash.Hash{},
			Index: 0,
		},
	}
	tx.AddTxIn(prevOut)

	tx.AddTxOut(&wire.TxOut{Value: 2_500_000, PkScript: payScript})
	tx.AddTxOut(&wire.TxOut{Value: 0, PkScript: opReturnScript})

	block := types.DogeBlockExt{
		BlockNumber:  101,
		BlockHash:    chainhash.Hash{},
		Transactions: []*wire.MsgTx{tx},
	}

	module.processBlock(block)

	var storedUTXOs []models.UTXO
	require.NoError(t, db.Find(&storedUTXOs).Error)
	require.Len(t, storedUTXOs, 1)

	utxo := storedUTXOs[0]
	require.Equal(t, tx.TxHash().String(), utxo.Txid)
	require.Equal(t, 0, utxo.OutIndex)
	require.Equal(t, int64(2_500_000), utxo.Amount)
	require.Equal(t, models.UTXO_SOURCE_DEPOSIT, utxo.Source)
	require.Equal(t, evmAddress, utxo.EvmAddr)

	var deposit models.Deposit
	require.NoError(t, db.Where("tx_id = ? AND vout = ?", tx.TxHash().String(), 0).First(&deposit).Error)
	require.Equal(t, int64(2_500_000), deposit.Amount)
	require.Equal(t, "pending", deposit.Status)
	require.NotEmpty(t, deposit.TxBytes)

}
