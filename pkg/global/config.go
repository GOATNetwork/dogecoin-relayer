package global

import (
	"sync"

	"github.com/goat-network/dogecoin-relayer/internal/config"
	"github.com/goat-network/dogecoin-relayer/pkg/eventbus"
)

var (
	globalConfig *config.Config
	configMutex  sync.RWMutex

	globalEventBus *eventbus.Bus
	eventBusMutex  sync.RWMutex
)

// SetConfig sets the global configuration
func SetConfig(cfg *config.Config) {
	configMutex.Lock()
	defer configMutex.Unlock()
	globalConfig = cfg
}

// GetConfig returns the global configuration
func GetConfig() *config.Config {
	configMutex.RLock()
	defer configMutex.RUnlock()
	return globalConfig
}

func GetEventBus() *eventbus.Bus {
	eventBusMutex.Lock()
	defer eventBusMutex.Unlock()

	if globalEventBus == nil {
		globalEventBus = eventbus.NewEventBus()
	}

	return globalEventBus
}
