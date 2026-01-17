package models

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"gorm.io/driver/sqlite"
	"gorm.io/gorm"
)

func TestUTXOScanStateUpsertSingleRow(t *testing.T) {
	db, err := gorm.Open(sqlite.Open(":memory:"), &gorm.Config{})
	require.NoError(t, err)
	require.NoError(t, db.AutoMigrate(&UTXOScanState{}))

	repo := NewUTXOScanStateRepository(db)

	for i := 0; i < 3; i++ {
		err = repo.UpdateUTXOScanState(uint64(100 + i))
		require.NoError(t, err)

		var count int64
		require.NoError(t, db.Model(&UTXOScanState{}).Count(&count).Error)
		assert.Equal(t, int64(1), count)
	}

	state, err := repo.GetUTXOScanState()
	require.NoError(t, err)
	assert.Equal(t, uint64(102), state.LastScannedBlock)
}
