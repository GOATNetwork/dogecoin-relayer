package module

import (
	"context"

	log "github.com/sirupsen/logrus"

	"github.com/goat-network/dogecoin-relayer/internal/models"
)

type Module interface {
	Name() string
	Init(cfg any, conn *models.DBConnection) error
	Run(ctx context.Context) error
	Shutdown(ctx context.Context) error
}

var registry = make(map[string]Module)

func RegisterModule(m Module) {
	if _, exists := registry[m.Name()]; exists {
		log.Fatalf("Module %s already registered", m.Name())
	}
	registry[m.Name()] = m
}

func GetModule(name string) (Module, bool) {
	m, exists := registry[name]
	return m, exists
}
