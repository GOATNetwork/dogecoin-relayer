package models

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"gorm.io/driver/sqlite"
	"gorm.io/gorm"
)

func TestEventScanStateUpsertSingleRow(t *testing.T) {
	db, err := gorm.Open(sqlite.Open(":memory:"), &gorm.Config{})
	require.NoError(t, err)
	require.NoError(t, db.AutoMigrate(&EventScanState{}))

	repo := NewEventRepository(db)

	for i := 0; i < 3; i++ {
		err = repo.UpdateScanState(nil, uint64(100+i))
		require.NoError(t, err)

		var count int64
		require.NoError(t, db.Model(&EventScanState{}).Count(&count).Error)
		assert.Equal(t, int64(1), count)
	}

	state, err := repo.GetScanState()
	require.NoError(t, err)
	assert.Equal(t, uint64(102), state.LastScannedBlock)

	err = repo.CreateOrUpdateScanState(&EventScanState{
		LastScannedBlock:   200,
		ConfirmationBlocks: 9,
		IsActive:           true,
	})
	require.NoError(t, err)

	var count int64
	require.NoError(t, db.Model(&EventScanState{}).Count(&count).Error)
	assert.Equal(t, int64(1), count)

	state, err = repo.GetScanState()
	require.NoError(t, err)
	assert.Equal(t, uint64(200), state.LastScannedBlock)
	assert.Equal(t, uint64(9), state.ConfirmationBlocks)
}
