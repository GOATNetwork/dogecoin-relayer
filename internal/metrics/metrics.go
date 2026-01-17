package metrics

import (
	"sync"
	"time"

	"github.com/goat-network/dogecoin-relayer/internal/config"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	log "github.com/sirupsen/logrus"
)

const (
	// Namespace for all metrics
	Namespace = "dogecoin_relayer"
)

// MetricsManager manages all metrics collection
type MetricsManager struct {
	registry *prometheus.Registry
	logger   *log.Entry
	mu       sync.RWMutex
	config   config.MetricsConfig
	enabled  bool
}

var (
	manager *MetricsManager
	once    sync.Once
)

// Initialize creates the global metrics manager
func Initialize(cfg config.MetricsConfig) *MetricsManager {
	once.Do(func() {
		manager = &MetricsManager{
			registry: prometheus.NewRegistry(),
			logger:   log.WithField("component", "metrics"),
			config:   cfg,
			enabled:  cfg.Enabled,
		}

		if cfg.Enabled {
			manager.logger.Info("Metrics manager initialized and enabled")
			// Initialize start time only if metrics are enabled
			StartTime.Set(float64(time.Now().Unix()))
		} else {
			manager.logger.Info("Metrics manager initialized but disabled")
		}
	})
	return manager
}

// GetManager returns the global metrics manager
func GetManager() *MetricsManager {
	return manager
}

// IsEnabled returns whether metrics collection is enabled
func IsEnabled() bool {
	if manager == nil {
		return false
	}
	return manager.enabled
}

// GetConfig returns the metrics configuration
func GetConfig() config.MetricsConfig {
	if manager == nil {
		return config.MetricsConfig{}
	}
	return manager.config
}

// GetRegistry returns the prometheus registry
func (m *MetricsManager) GetRegistry() *prometheus.Registry {
	return m.registry
}

// Common metrics that will be used across modules
var (
	// System metrics
	StartTime = promauto.NewGauge(prometheus.GaugeOpts{
		Namespace: Namespace,
		Name:      "start_time_seconds",
		Help:      "Unix timestamp of when the application started",
	})

	// Module status metrics
	ModuleStatus = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: Namespace,
		Name:      "module_status",
		Help:      "Status of each module (1=running, 0=stopped)",
	}, []string{"module"})

	// Database metrics
	DBConnections = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: Namespace,
		Name:      "database_connections",
		Help:      "Number of database connections",
	}, []string{"state"}) // active, idle, open

	// Error metrics
	ErrorsTotal = promauto.NewCounterVec(prometheus.CounterOpts{
		Namespace: Namespace,
		Name:      "errors_total",
		Help:      "Total number of errors by module and type",
	}, []string{"module", "type"})

	// Request/Operation metrics
	OperationsTotal = promauto.NewCounterVec(prometheus.CounterOpts{
		Namespace: Namespace,
		Name:      "operations_total",
		Help:      "Total number of operations by module and type",
	}, []string{"module", "operation", "status"})

	OperationDuration = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Namespace: Namespace,
		Name:      "operation_duration_seconds",
		Help:      "Duration of operations in seconds",
		Buckets:   prometheus.DefBuckets,
	}, []string{"module", "operation"})
)

// RecordModuleStart records that a module has started
func RecordModuleStart(moduleName string) {
	if !IsEnabled() {
		return
	}
	ModuleStatus.WithLabelValues(moduleName).Set(1)
	GetManager().logger.Infof("Module %s started", moduleName)
}

// RecordModuleStop records that a module has stopped
func RecordModuleStop(moduleName string) {
	if !IsEnabled() {
		return
	}
	ModuleStatus.WithLabelValues(moduleName).Set(0)
	GetManager().logger.Infof("Module %s stopped", moduleName)
}

// RecordError records an error for a specific module
func RecordError(moduleName, errorType string) {
	if !IsEnabled() {
		return
	}
	ErrorsTotal.WithLabelValues(moduleName, errorType).Inc()
}

// RecordOperation records an operation completion
func RecordOperation(moduleName, operation, status string, duration time.Duration) {
	if !IsEnabled() {
		return
	}
	OperationsTotal.WithLabelValues(moduleName, operation, status).Inc()
	OperationDuration.WithLabelValues(moduleName, operation).Observe(duration.Seconds())
}

// RecordGRPCRequest records a gRPC request
func RecordGRPCRequest(method, status string, isError bool) {
	if !IsEnabled() {
		return
	}
	operation := "grpc_" + method
	if isError {
		ErrorsTotal.WithLabelValues("rpc", operation).Inc()
	}
	OperationsTotal.WithLabelValues("rpc", operation, status).Inc()
}

// Timer helper for measuring operation duration
type Timer struct {
	start  time.Time
	module string
	op     string
}

// NewTimer creates a new timer
func NewTimer(module, operation string) *Timer {
	return &Timer{
		start:  time.Now(),
		module: module,
		op:     operation,
	}
}

// RecordSuccess records a successful operation
func (t *Timer) RecordSuccess() {
	if !IsEnabled() {
		return
	}
	duration := time.Since(t.start)
	RecordOperation(t.module, t.op, "success", duration)
}

// RecordFailure records a failed operation
func (t *Timer) RecordFailure() {
	if !IsEnabled() {
		return
	}
	duration := time.Since(t.start)
	RecordOperation(t.module, t.op, "failure", duration)
}

// ConditionallyRecord conditionally records metrics based on configuration
func ConditionallyRecord(detailedMetrics bool, fn func()) {
	if !IsEnabled() {
		return
	}

	// If detailed metrics are disabled, skip detailed recording
	if detailedMetrics && !GetConfig().EnableDetailedMetrics {
		return
	}

	fn()
}
