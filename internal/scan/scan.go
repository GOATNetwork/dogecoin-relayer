package scan

import (
	"context"

	"github.com/goat-network/dogecoin-relayer/config"
	"github.com/goat-network/dogecoin-relayer/internal/models"
	"github.com/goat-network/dogecoin-relayer/pkg/module"
	"github.com/goat-network/dogecoin-relayer/pkg/types"
	log "github.com/sirupsen/logrus"
)

type ScanModule struct {
	cfg    config.ScanConfig
	conn   *models.DBConnection
	logger *log.Entry
}

var _ module.Module = (*ScanModule)(nil)

func (m *ScanModule) Name() string {
	return "scan"
}

func (m *ScanModule) Init(cfg any, conn *models.DBConnection) error {
	m.cfg = cfg.(config.ScanConfig)
	m.conn = conn
	m.logger = types.InitLogEntry(m.Name())
	return nil
}

func (m *ScanModule) Run(ctx context.Context) error {
	m.logger.Info("Scan module running")

	return nil
}

func (m *ScanModule) Shutdown(ctx context.Context) error {
	m.logger.Info("Scan module shutting down")
	return nil
}

func init() {
	log.Info("Registering scan module")
	module.RegisterModule(&ScanModule{})
}
