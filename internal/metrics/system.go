package metrics

import (
	"context"
	"runtime"
	"time"

	log "github.com/sirupsen/logrus"
)

// SystemMetricsCollector collects system-level metrics
type SystemMetricsCollector struct {
	logger   *log.Entry
	ticker   *time.Ticker
	done     chan struct{}
	interval time.Duration
}

// NewSystemMetricsCollector creates a new system metrics collector
func NewSystemMetricsCollector(interval time.Duration) *SystemMetricsCollector {
	return &SystemMetricsCollector{
		logger:   log.WithField("component", "system-metrics"),
		ticker:   time.NewTicker(interval),
		done:     make(chan struct{}),
		interval: interval,
	}
}

// Start begins collecting system metrics
func (c *SystemMetricsCollector) Start(ctx context.Context) {
	// Check if metrics and system metrics are enabled
	if !IsEnabled() {
		c.logger.Info("Metrics disabled, skipping system metrics collection")
		return
	}

	if !GetConfig().CollectSystemMetrics {
		c.logger.Info("System metrics collection disabled")
		return
	}

	c.logger.Info("Starting system metrics collection")

	// Collect metrics immediately
	c.collectMetrics()

	go func() {
		defer c.ticker.Stop()

		for {
			select {
			case <-c.ticker.C:
				c.collectMetrics()
			case <-c.done:
				c.logger.Info("System metrics collection stopped")
				return
			case <-ctx.Done():
				c.logger.Info("System metrics collection stopped by context")
				return
			}
		}
	}()
}

// Stop stops the metrics collection
func (c *SystemMetricsCollector) Stop() {
	close(c.done)
}

// collectMetrics collects all system metrics
func (c *SystemMetricsCollector) collectMetrics() {
	if !IsEnabled() {
		return
	}

	// Collect memory statistics
	var memStats runtime.MemStats
	runtime.ReadMemStats(&memStats)

	// Update memory metrics
	MemoryUsage.WithLabelValues("heap").Set(float64(memStats.HeapAlloc))
	MemoryUsage.WithLabelValues("stack").Set(float64(memStats.StackInuse))
	MemoryUsage.WithLabelValues("sys").Set(float64(memStats.Sys))

	// Update goroutine count
	GoroutineCount.Set(float64(runtime.NumGoroutine()))

	// Update GC metrics only if detailed metrics are enabled
	ConditionallyRecord(true, func() {
		GCCount.Add(float64(memStats.NumGC))
		// Note: GC duration is typically updated by runtime hooks
		// This is a simplified approach - for production, consider using runtime.GCStats
	})
}

// UpdateDBMetrics updates database connection metrics
func UpdateDBMetrics(active, idle, open int) {
	if !IsEnabled() {
		return
	}
	DBConnections.WithLabelValues("active").Set(float64(active))
	DBConnections.WithLabelValues("idle").Set(float64(idle))
	DBConnections.WithLabelValues("open").Set(float64(open))
}

// CreateSystemMetricsCollectorFromConfig creates a collector using the metrics configuration
func CreateSystemMetricsCollectorFromConfig() *SystemMetricsCollector {
	config := GetConfig()

	// Use configured interval or default to 10 seconds
	interval := 10 * time.Second
	if config.SystemCollectInterval > 0 {
		interval = time.Duration(config.SystemCollectInterval) * time.Second
	}

	return NewSystemMetricsCollector(interval)
}
