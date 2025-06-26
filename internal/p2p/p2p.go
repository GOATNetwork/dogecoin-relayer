package p2p

import (
	"context"

	"github.com/goat-network/dogecoin-relayer/internal/config"
	"github.com/goat-network/dogecoin-relayer/internal/models"
	"github.com/goat-network/dogecoin-relayer/pkg/module"
	"github.com/goat-network/dogecoin-relayer/pkg/types"
	log "github.com/sirupsen/logrus"
)

type P2PModule struct {
	cfg    config.P2PConfig
	conn   *models.DBConnection
	logger *log.Entry

	Network *Network
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

	network, err := NewNetwork(ctx, m.cfg)
	if err != nil {
		return err
	}
	m.Network = network
	go m.Network.Start()

	return nil
}

func (m *P2PModule) Shutdown(ctx context.Context) error {
	m.logger.Info("P2P module shutting down")
	return nil
}

func init() {
	log.Info("Registering p2p module")
	module.RegisterModule(&P2PModule{})
}
