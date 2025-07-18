package p2p

import (
	"context"

	"github.com/goat-network/dogecoin-relayer/internal/config"
	"github.com/goat-network/dogecoin-relayer/internal/metrics"
	"github.com/goat-network/dogecoin-relayer/internal/models"
	"github.com/goat-network/dogecoin-relayer/pkg/module"
	"github.com/goat-network/dogecoin-relayer/pkg/types"
	log "github.com/sirupsen/logrus"
)

type P2PModule struct {
	cfg    config.P2PConfig
	conn   *models.DBConnection
	logger *log.Entry

	network *Network
	cancel  context.CancelFunc
}

var _ module.Module = (*P2PModule)(nil)

func (m *P2PModule) Name() string {
	return "p2p"
}

func (m *P2PModule) Init(cfg any, conn *models.DBConnection) error {
	m.cfg = cfg.(config.P2PConfig)
	m.conn = conn
	m.logger = types.InitLogEntry(m.Name())
	return nil
}

func (m *P2PModule) Run(ctx context.Context) error {
	m.logger.Info("P2P module running")
	metrics.RecordModuleStart("p2p")

	// Create cancellable context
	ctx, cancel := context.WithCancel(ctx)
	m.cancel = cancel

	network, err := NewNetwork(ctx, m.cfg)
	if err != nil {
		metrics.RecordError("p2p", "network_init_failed")
		return err
	}
	m.network = network

	// Start network in goroutine
	go func() {
		if err := m.network.Start(); err != nil {
			m.logger.Errorf("P2P network failed: %v", err)
			metrics.RecordError("p2p", "network_start_failed")
		}
	}()

	// Start metrics collection
	go m.collectMetrics(ctx)

	return nil
}

func (m *P2PModule) Shutdown(ctx context.Context) error {
	m.logger.Info("P2P module shutting down")

	if m.cancel != nil {
		m.cancel()
	}

	if m.network != nil {
		m.network.Close()
	}

	metrics.RecordModuleStop("p2p")

	return nil
}

// GetNetwork returns the network instance for accessing host and public key information
func (m *P2PModule) GetNetwork() *Network {
	return m.network
}

func init() {
	log.Info("Registering p2p module")
	module.RegisterModule(&P2PModule{})
}
