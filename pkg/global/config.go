package global

import (
	"sync"

	"github.com/goat-network/dogecoin-relayer/config"
)

var (
	globalConfig *config.Config
	configMutex  sync.RWMutex
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
