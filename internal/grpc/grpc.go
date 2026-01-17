package grpc

import (
	"context"
	"fmt"
	"net"
	"time"

	"github.com/goat-network/dogecoin-relayer/internal/config"
	"github.com/goat-network/dogecoin-relayer/internal/metrics"
	"github.com/goat-network/dogecoin-relayer/internal/models"
	"github.com/goat-network/dogecoin-relayer/pkg/module"
	"github.com/goat-network/dogecoin-relayer/pkg/types"
	"github.com/goat-network/dogecoin-relayer/proto"
	log "github.com/sirupsen/logrus"
	"google.golang.org/grpc"
	"google.golang.org/grpc/keepalive"
	"google.golang.org/grpc/reflection"
)

// GrpcModule represents the gRPC module for the Dogecoin Relayer
type GrpcModule struct {
	cfg     config.GrpcConfig
	conn    *models.DBConnection
	logger  *log.Entry
	server  *grpc.Server
	grpcSrv *grpcServer
}

// GrpcConfig holds gRPC server configuration
type GrpcConfig struct {
	Enabled     bool   `yaml:"enabled"`
	Port        int    `yaml:"port"`
	UseTLS      bool   `yaml:"use_tls"`
	CertFile    string `yaml:"cert_file"`
	KeyFile     string `yaml:"key_file"`
	MaxRecvSize int    `yaml:"max_recv_size"` // in MB
	MaxSendSize int    `yaml:"max_send_size"` // in MB
	MaxConnAge  int    `yaml:"max_conn_age"`  // in seconds
	MaxConnIdle int    `yaml:"max_conn_idle"` // in seconds
	PingTime    int    `yaml:"ping_time"`     // in seconds
	Timeout     int    `yaml:"timeout"`       // in seconds
}

var _ module.Module = (*GrpcModule)(nil)

func (m *GrpcModule) Name() string {
	return "grpc"
}

func (m *GrpcModule) Init(cfg any, conn *models.DBConnection) error {
	m.cfg = cfg.(config.GrpcConfig)
	m.conn = conn
	m.logger = types.InitLogEntry(m.Name())

	// Initialize gRPC server instance
	m.grpcSrv = newGRPCServer(m.conn, m.logger)

	return nil
}

func (m *GrpcModule) Run(ctx context.Context) error {
	m.logger.Info("gRPC module running")
	metrics.RecordModuleStart("grpc")

	if !m.cfg.Enabled {
		m.logger.Info("gRPC server disabled")
		return nil
	}

	// Set default values if not configured
	if m.cfg.MaxRecvSize <= 0 {
		m.cfg.MaxRecvSize = 4 // 4MB default
	}
	if m.cfg.MaxSendSize <= 0 {
		m.cfg.MaxSendSize = 4 // 4MB default
	}
	if m.cfg.MaxConnAge <= 0 {
		m.cfg.MaxConnAge = 300 // 5 minutes default
	}
	if m.cfg.MaxConnIdle <= 0 {
		m.cfg.MaxConnIdle = 300 // 5 minutes default
	}
	if m.cfg.PingTime <= 0 {
		m.cfg.PingTime = 60 // 1 minute default
	}
	if m.cfg.Timeout <= 0 {
		m.cfg.Timeout = 10 // 10 seconds default
	}

	var opts []grpc.ServerOption

	// Configure keepalive parameters
	keepaliveParams := keepalive.ServerParameters{
		MaxConnectionIdle:     time.Duration(m.cfg.MaxConnIdle) * time.Second,
		MaxConnectionAge:      time.Duration(m.cfg.MaxConnAge) * time.Second,
		MaxConnectionAgeGrace: time.Duration(m.cfg.Timeout) * time.Second,
		Time:                  time.Duration(m.cfg.PingTime) * time.Second,
		Timeout:               time.Duration(m.cfg.Timeout) * time.Second,
	}
	opts = append(opts, grpc.KeepaliveParams(keepaliveParams))

	// Configure keepalive enforcement policy
	keepaliveEnforcement := keepalive.EnforcementPolicy{
		MinTime:             10 * time.Second,
		PermitWithoutStream: true,
	}
	opts = append(opts, grpc.KeepaliveEnforcementPolicy(keepaliveEnforcement))

	// Configure max receive and send message sizes
	maxRecvSize := int64(m.cfg.MaxRecvSize) * 1024 * 1024
	maxSendSize := int64(m.cfg.MaxSendSize) * 1024 * 1024
	opts = append(opts, grpc.MaxRecvMsgSize(int(maxRecvSize)))
	opts = append(opts, grpc.MaxSendMsgSize(int(maxSendSize)))

	// Configure TLS if enabled
	if m.cfg.UseTLS {
		creds, err := loadServerTLSCredentials(m.cfg.CertFile, m.cfg.KeyFile)
		if err != nil {
			metrics.RecordError("grpc", "tls_error")
			return fmt.Errorf("failed to load TLS credentials: %w", err)
		}
		opts = append(opts, grpc.Creds(creds))
		m.logger.Info("gRPC TLS enabled")
	}

	// Create gRPC server
	m.server = grpc.NewServer(opts...)

	// Register the BitcoinLightWallet service
	proto.RegisterBitcoinLightWalletServer(m.server, m.grpcSrv)

	// Enable reflection for development/debugging
	if m.cfg.UseTLS {
		// Reflection should only be enabled in non-production when TLS is not used
		// to avoid exposing service information
		m.logger.Warn("gRPC reflection is disabled when TLS is enabled")
	} else {
		reflection.Register(m.server)
		m.logger.Info("gRPC reflection enabled (development mode)")
	}

	// Start listening
	addr := fmt.Sprintf(":%d", m.cfg.Port)
	lis, err := net.Listen("tcp", addr)
	if err != nil {
		metrics.RecordError("grpc", "listen_error")
		return fmt.Errorf("failed to listen on %s: %w", addr, err)
	}

	// Start server in goroutine
	go func() {
		m.logger.Infof("Starting gRPC server on port %d", m.cfg.Port)
		metrics.RecordModuleStart("grpc")

		if err := m.server.Serve(lis); err != nil {
			m.logger.Errorf("gRPC server failed: %v", err)
			metrics.RecordError("grpc", "server_failed")
			metrics.RecordModuleStop("grpc")
		}
	}()

	return nil
}

func (m *GrpcModule) Shutdown(ctx context.Context) error {
	m.logger.Info("gRPC module shutting down")

	if m.server != nil {
		// Create graceful shutdown context
		_, cancel := context.WithTimeout(ctx, 15*time.Second)
		defer cancel()

		// Stop accepting new requests
		m.server.GracefulStop()

		m.logger.Info("gRPC server stopped gracefully")

		metrics.RecordModuleStop("grpc")
	}

	return nil
}

func init() {
	log.Info("Registering grpc module")
	module.RegisterModule(&GrpcModule{})
}
