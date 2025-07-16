package models

import (
	"time"

	"gorm.io/gorm"
)

func AddUtxoRecords(tx *gorm.DB) error {
	utxos := []UTXO{
		{
			Uid:  "",
			Txid: "2638a8f782ce703e59d06bb2f4b0c65153c141a2aa07f4b20df3a01f5d8a2462",
			PkScript: []byte{
				0x00, 0x14, 0x90, 0x34, 0xd0, 0xe5, 0xbc, 0xc8,
				0xe9, 0x6f, 0x6c, 0xbe, 0x89, 0xce, 0xa6, 0xe0,
				0x91, 0xae, 0xa9, 0x5d, 0xd9, 0xd8,
			},
			OutIndex:      0,
			Amount:        50000,
			Receiver:      "bc1qjq6dpeduer5k7m973882dcy34654mkwcvgpr08",
			WalletVersion: "1",
			Sender:        "",
			EvmAddr:       "0x738fe7d89c172239bf456D387Ad2c60A79087917",
			Source:        "deposit",
			ReceiverType:  "P2WPKH",
			Status:        "spent",
			ReceiveBlock:  873521,
			SpentBlock:    873752,
			UpdatedAt:     mustParseTime("2024-12-08 06:47:57.385513245+00:00"),
		},
		{
			Uid:  "",
			Txid: "451cae01882da59279fce894f9f57c5ae17f4ee10d924aab355de94e661287bc",
			PkScript: []byte{
				0x00, 0x14, 0x90, 0x34, 0xd0, 0xe5, 0xbc, 0xc8,
				0xe9, 0x6f, 0x6c, 0xbe, 0x89, 0xce, 0xa6, 0xe0,
				0x91, 0xae, 0xa9, 0x5d, 0xd9, 0xd8,
			},
			OutIndex:      0,
			Amount:        50000,
			Receiver:      "bc1qjq6dpeduer5k7m973882dcy34654mkwcvgpr08",
			WalletVersion: "1",
			Sender:        "",
			EvmAddr:       "0x738fe7d89c172239bf456D387Ad2c60A79087917",
			Source:        "deposit",
			ReceiverType:  "P2WPKH",
			Status:        "spent",
			ReceiveBlock:  873586,
			SpentBlock:    873729,
			UpdatedAt:     mustParseTime("2024-12-08 02:16:34.799847784+00:00"),
		},
	}

	return tx.Create(&utxos).Error
}

func mustParseTime(value string) time.Time {
	t, err := time.Parse("2006-01-02 15:04:05.999999999-07:00", value)
	if err != nil {
		panic(err)
	}
	return t
}
