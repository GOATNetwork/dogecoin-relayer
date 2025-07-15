package http

import (
	"context"
	"fmt"
	"net/http"
	"time"

	"github.com/goat-network/dogecoin-relayer/internal/config"
	"github.com/goat-network/dogecoin-relayer/internal/metrics"
	"github.com/goat-network/dogecoin-relayer/internal/models"
	"github.com/goat-network/dogecoin-relayer/pkg/module"
	"github.com/goat-network/dogecoin-relayer/pkg/types"
	log "github.com/sirupsen/logrus"
)

type HttpModule struct {
	cfg    config.HttpConfig
	conn   *models.DBConnection
	logger *log.Entry
	server *http.Server
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
	metrics.RecordModuleStart("http")

	if !m.cfg.Enabled {
		m.logger.Info("HTTP server disabled")
		return nil
	}

	// Create HTTP server
	mux := http.NewServeMux()

	m.registerHandlers(mux)

	// Create server with or without metrics middleware
	var handler http.Handler
	if metrics.IsEnabled() {
		handler = m.metricsMiddleware(mux)
	} else {
		handler = mux
	}

	server := &http.Server{
		Addr:    fmt.Sprintf(":%d", m.cfg.Port),
		Handler: handler,
	}
	m.server = server

	// Start server in goroutine
	go func() {
		m.logger.Infof("Starting HTTP server on port %d", m.cfg.Port)
		if metrics.IsEnabled() {
			metrics.HTTPServerStatus.Set(1)
		}

		if err := server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			m.logger.Errorf("HTTP server failed: %v", err)
			metrics.RecordError("http", "server_failed")
			if metrics.IsEnabled() {
				metrics.HTTPServerStatus.Set(0)
			}
		}
	}()

	// Start system metrics collection only if enabled
	if metrics.IsEnabled() && metrics.GetConfig().CollectSystemMetrics {
		systemCollector := metrics.CreateSystemMetricsCollectorFromConfig()
		systemCollector.Start(ctx)
	}

	return nil
}

func (m *HttpModule) Shutdown(ctx context.Context) error {
	m.logger.Info("Http module shutting down")

	if m.server != nil {
		shutdownCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
		defer cancel()

		if err := m.server.Shutdown(shutdownCtx); err != nil {
			m.logger.Errorf("HTTP server shutdown failed: %v", err)
		}

		if metrics.IsEnabled() {
			metrics.HTTPServerStatus.Set(0)
		}
	}

	metrics.RecordModuleStop("http")
	return nil
}

func init() {
	log.Info("Registering http module")
	module.RegisterModule(&HttpModule{})
}
