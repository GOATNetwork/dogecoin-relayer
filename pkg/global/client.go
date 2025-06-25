package global

import (
	"sync"
)

var (
	clientMutex sync.RWMutex
	clientOnce  sync.Once
)

// GlobalClient is a placeholder for the global client instance
