package cmd

import (
	"context"
	"flag"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/goat-network/dogecoin-relayer/internal/config"
	"github.com/goat-network/dogecoin-relayer/internal/models"
	"github.com/goat-network/dogecoin-relayer/pkg/global"
	"github.com/goat-network/dogecoin-relayer/pkg/module"
	log "github.com/sirupsen/logrus"

	// import modules
	_ "github.com/goat-network/dogecoin-relayer/internal/consensus"
	_ "github.com/goat-network/dogecoin-relayer/internal/http"
	_ "github.com/goat-network/dogecoin-relayer/internal/p2p"
	_ "github.com/goat-network/dogecoin-relayer/internal/scan"
	_ "github.com/goat-network/dogecoin-relayer/internal/tss"
)

func Run() {
	ctx, cancel := context.WithCancel(context.Background())

	go func() {
		signalChan := make(chan os.Signal, 1)
		signal.Notify(signalChan, os.Interrupt, syscall.SIGTERM)
		<-signalChan
		log.Info("Received shutdown signal, cleaning up...")
		cancel()
	}()

	configFilePath := flag.String("config", "./data/config.yaml", "Path to the configuration file")
	flag.Parse()

	cfg, err := config.LoadConfig(*configFilePath)
	if err != nil {
		log.Fatalf("Failed to load config: %v", err)
	}

	log.Debugf("cfg loaded %v", cfg)

	// Set global configuration
	global.SetConfig(cfg)

	// TODO: Initialize global client, doge and goat

	enabledModules := make([]string, 0)
	if cfg.P2P.Enabled {
		enabledModules = append(enabledModules, "p2p")
	}
	if cfg.Http.Enabled {
		enabledModules = append(enabledModules, "http")
	}
	if cfg.Scan.Enabled {
		enabledModules = append(enabledModules, "scan")
	}
	if cfg.Tss.Enabled {
		enabledModules = append(enabledModules, "tss")
	}

	log.Infof("Enabled modules: %v", enabledModules)

	conn, err := models.NewDBConnection(&cfg.Sqlite, &cfg.Gorm)
	if err != nil {
		log.Fatalf("Failed to connect to database: %v", err)
	}

	for _, moduleName := range enabledModules {
		m, exists := module.GetModule(moduleName)
		if !exists {
			log.Infof("Module %s not found, skipping...", moduleName)
			continue
		}

		var moduleConfig any
		switch moduleName {
		case "scan":
			moduleConfig = cfg.Scan
		case "p2p":
			moduleConfig = cfg.P2P
		case "http":
			moduleConfig = cfg.Http
		case "consensus":
			moduleConfig = cfg.Consensus
		case "tss":
			moduleConfig = cfg.Tss
		default:
			log.Fatalf("Module %s not found, skipping...", moduleName)
			continue
		}
		if err := m.Init(moduleConfig, conn); err != nil {
			log.Fatalf("Failed to initialize module %s: %v", moduleName, err)
		}

		go func(mod module.Module) {
			if err := mod.Run(ctx); err != nil {
				log.Fatalf("Error running module %s: %v", mod.Name(), err)
			}
		}(m)
	}

	<-ctx.Done()

	log.Info("Shutting down modules...")
	shutdownTimeout := 15 * time.Second
	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), shutdownTimeout)
	defer shutdownCancel()

	for _, moduleName := range enabledModules {
		m, exists := module.GetModule(moduleName)
		if !exists {
			log.Infof("Module %s not found, skipping...", moduleName)
			continue
		}

		if err := m.Shutdown(shutdownCtx); err != nil {
			log.Errorf("Error shutting down module %s: %v", m.Name(), err)
		}
	}

	conn.Close()
	log.Info("Database connection closed")

	log.Info("All modules shut down. Exiting.")
}
