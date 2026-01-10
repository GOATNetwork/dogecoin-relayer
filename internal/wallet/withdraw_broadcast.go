package wallet

import (
	"bytes"

	"github.com/dogecoinw/doged/wire"
)

func SerializeTransactionNoWitness(tx *wire.MsgTx) ([]byte, error) {
	var buf bytes.Buffer
	err := tx.SerializeNoWitness(&buf)
	if err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

func DeserializeTransactionFromBytes(data []byte) (*wire.MsgTx, error) {
	var msgTx wire.MsgTx
	buf := bytes.NewReader(data)
	err := msgTx.Deserialize(buf)
	if err != nil {
		return nil, err
	}
	return &msgTx, nil
}
