package wallet

import (
	"fmt"

	"github.com/dogecoinw/doged/chaincfg"
	"github.com/dogecoinw/doged/wire"
	"github.com/goat-network/dogecoin-relayer/internal/models"
)

type TransactionError struct {
	Code    TransactionErrorCode
	Message string
	Err     error
}

type TransactionErrorCode int

const (
	ErrInvalidUTXO TransactionErrorCode = iota + 1
	ErrInvalidAddress
	ErrInvalidScript
	ErrInvalidTx
	ErrWithdrawDustAmount
	ErrChangeDustAmount
	ErrTxTooLarge
	ErrTxPriceTooHigh
)

func (e *TransactionError) Error() string {
	if e.Err != nil {
		return fmt.Sprintf("%s: %v", e.Message, e.Err)
	}
	return e.Message
}

type TransactionParams struct {
	UTXOs          []*UTXOAdapter
	Withdrawals    []*WithdrawalAdapter
	ChangeAddress  string
	ChangeAmount   int64
	EstimatedFee   float64
	WitnessSize    int64
	NetworkFee     int64
	Net            *DogeNetworkParams
	UtxoAmount     int64
	WithdrawAmount int64
}

type TransactionResult struct {
	Tx        *wire.MsgTx
	ActualFee uint64
}

type UTXOAdapter struct {
	*models.UTXO
	ReceiverType string
	SubScript    []byte
}

type WithdrawalAdapter struct {
	*models.Withdrawal
	Amount  int64
	TxPrice int64
}

type DogeNetworkParams struct {
	Params *chaincfg.Params
}

const (
	SMALL_UTXO_DEFINE = 50000000
)

const (
	WALLET_TYPE_UNKNOWN = "unknown"
	WALLET_TYPE_P2PKH   = "p2pkh"
	WALLET_TYPE_P2WPKH  = "p2wpkh"
	WALLET_TYPE_P2WSH   = "p2wsh"
)

const (
	ORDER_TYPE_WITHDRAWAL    = "withdrawal"
	ORDER_TYPE_CONSOLIDATION = "consolidation"
	ORDER_TYPE_SAFEBOX       = "safebox"
)

const (
	UTXO_SOURCE_DEPOSIT       = "deposit"
	UTXO_SOURCE_CONSOLIDATION = "consolidation"
	UTXO_SOURCE_WITHDRAWAL    = "withdrawal"
)
