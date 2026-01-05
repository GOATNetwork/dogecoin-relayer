package models

import (
	"testing"

	"github.com/stretchr/testify/require"
	"gorm.io/driver/sqlite"
	"gorm.io/gorm"
)

func TestUTXORepository_AddUTXO_DedupByUID(t *testing.T) {
	db, err := gorm.Open(sqlite.Open(":memory:"), &gorm.Config{})
	require.NoError(t, err)
	require.NoError(t, db.AutoMigrate(&UTXO{}))

	repo := NewUTXORepository(db)
	utxo := &UTXO{
		Txid:     "a3a3668e574ca0001a61feb1d6650f701923276a8cedf5929f6da66ea111289a",
		OutIndex: 0,
		Amount:   500000000,
		Receiver: "nsC3fq1zsBzZypN2KaawpV9B6V3CMQHwnM",
	}

	require.NoError(t, repo.AddUTXO(utxo, nil, "", 0, nil, "", nil, 0, true))
	require.NoError(t, repo.AddUTXO(utxo, nil, "", 0, nil, "", nil, 0, true))

	var count int64
	require.NoError(t, db.Model(&UTXO{}).Where("uid = ?", utxo.Uid).Count(&count).Error)
	require.Equal(t, int64(1), count)
}
