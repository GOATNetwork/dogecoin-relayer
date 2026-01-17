package tss

import (
	"context"
	"sync"

	"github.com/goat-network/dogecoin-relayer/internal/config"
	"github.com/goat-network/dogecoin-relayer/internal/metrics"
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
	cancel         context.CancelFunc
	ctx            context.Context
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

	// Set initial connection status
	if m.cfg.Enabled {
		metrics.TSSClientStatus.Set(1)
	} else {
		metrics.TSSClientStatus.Set(0)
	}

	return nil
}

func (m *TssModule) Run(ctx context.Context) error {
	m.logger.Info("Tss module running")
	metrics.RecordModuleStart("tss")

	// Create cancellable context
	m.ctx, m.cancel = context.WithCancel(ctx)

	// register event bus
	m.subscribeEvent()

	// start tss handler event by event bus
	go m.checkSignHandler(ctx)

	// start periodic health check
	go m.healthCheck(ctx)

	// start metrics collection
	go m.collectMetrics(ctx)

	return nil
}

func (m *TssModule) Shutdown(ctx context.Context) error {
	m.logger.Info("Tss module shutting down")

	if m.cancel != nil {
		m.cancel()
	}

	m.unSubscribeEvent()
	metrics.RecordModuleStop("tss")
	metrics.TSSClientStatus.Set(0)

	return nil
}

// GetSignClient returns the sign client instance
func (m *TssModule) GetSignClient() *SignClient {
	return m.signClient
}

func init() {
	log.Info("Registering tss module")
	module.RegisterModule(&TssModule{})
}
