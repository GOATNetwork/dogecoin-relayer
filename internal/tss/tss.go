package tss

import (
	"context"
	"sync"

	"github.com/goat-network/dogecoin-relayer/internal/config"
	"github.com/goat-network/dogecoin-relayer/internal/models"
	"github.com/goat-network/dogecoin-relayer/pkg/eventbus"
	"github.com/goat-network/dogecoin-relayer/pkg/global"
	"github.com/goat-network/dogecoin-relayer/pkg/module"
	"github.com/goat-network/dogecoin-relayer/pkg/types"
	log "github.com/sirupsen/logrus"
)

type TssModule struct {
	cfg    config.TssConfig
	conn   *models.DBConnection
	logger *log.Entry

	signClient *SignClient
	eventBus   *eventbus.Bus

	activeSessions sync.Map // key: sessionID (string), value: timestamp (time.Time)
}

var _ module.Module = (*TssModule)(nil)

func (m *TssModule) Name() string {
	return "tss"
}

func (m *TssModule) Init(cfg any, conn *models.DBConnection) error {
	m.cfg = cfg.(config.TssConfig)
	m.conn = conn
	m.logger = types.InitLogEntry(m.Name())

	m.signClient = NewSignClient(m.cfg)
	m.eventBus = global.GetEventBus()

	return nil
}

func (m *TssModule) Run(ctx context.Context) error {
	m.logger.Info("Tss module running")

	// register event bus
	m.subscribeEvent()
	// start tss handler event by event bus
	go m.checkSignHandler(ctx)

	return nil
}

func (m *TssModule) Shutdown(ctx context.Context) error {
	m.logger.Info("Tss module shutting down")
	m.unSubscribeEvent()
	return nil
}

func init() {
	log.Info("Registering tss module")
	module.RegisterModule(&TssModule{})
}
