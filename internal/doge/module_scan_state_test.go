package doge

import (
	"testing"

	"github.com/goat-network/dogecoin-relayer/internal/config"
	"github.com/goat-network/dogecoin-relayer/internal/models"
	log "github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"gorm.io/driver/sqlite"
	"gorm.io/gorm"
)

func TestInitializeScanHeightFromState(t *testing.T) {
	db, err := gorm.Open(sqlite.Open(":memory:"), &gorm.Config{})
	require.NoError(t, err)
	require.NoError(t, db.AutoMigrate(&models.UTXOScanState{}))

	repo := models.NewUTXOScanStateRepository(db)
	require.NoError(t, repo.UpdateUTXOScanState(120))

	module := &DogeModule{
		cfg:          config.DogeConfig{StartHeight: 50},
		utxoScanRepo: repo,
		logger:       log.WithField("suite", "doge-test"),
	}

	require.NoError(t, module.initializeScanHeight())
	assert.Equal(t, int64(121), module.currentHeight)
}

func TestInitializeScanHeightFromConfigWhenMissing(t *testing.T) {
	db, err := gorm.Open(sqlite.Open(":memory:"), &gorm.Config{})
	require.NoError(t, err)
	require.NoError(t, db.AutoMigrate(&models.UTXOScanState{}))

	repo := models.NewUTXOScanStateRepository(db)

	module := &DogeModule{
		cfg:          config.DogeConfig{StartHeight: 75},
		utxoScanRepo: repo,
		logger:       log.WithField("suite", "doge-test"),
	}

	require.NoError(t, module.initializeScanHeight())
	assert.Equal(t, int64(75), module.currentHeight)
}
