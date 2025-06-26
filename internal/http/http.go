package http

import (
	"context"

	"github.com/goat-network/dogecoin-relayer/internal/config"
	"github.com/goat-network/dogecoin-relayer/internal/models"
	"github.com/goat-network/dogecoin-relayer/pkg/module"
	"github.com/goat-network/dogecoin-relayer/pkg/types"
	log "github.com/sirupsen/logrus"
)

type HttpModule struct {
	cfg    config.HttpConfig
	conn   *models.DBConnection
	logger *log.Entry
}

var _ module.Module = (*HttpModule)(nil)

func (m *HttpModule) Name() string {
	return "http"
}

func (m *HttpModule) Init(cfg any, conn *models.DBConnection) error {
	m.cfg = cfg.(config.HttpConfig)
	m.conn = conn
	m.logger = types.InitLogEntry(m.Name())
	return nil
}

func (m *HttpModule) Run(ctx context.Context) error {
	m.logger.Info("Http module running")

	// TODO: start http server

	return nil
}

func (m *HttpModule) Shutdown(ctx context.Context) error {
	m.logger.Info("Http module shutting down")
	return nil
}

func init() {
	log.Info("Registering http module")
	module.RegisterModule(&HttpModule{})
}
