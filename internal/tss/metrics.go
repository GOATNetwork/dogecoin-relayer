package tss

import (
	"context"
	"fmt"
	"time"

	"github.com/goat-network/dogecoin-relayer/internal/metrics"
)

// collectMetrics periodically collects TSS metrics
func (m *TssModule) collectMetrics(ctx context.Context) {
	ticker := time.NewTicker(10 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			// Count active sessions
			activeCount := 0
			m.activeSessions.Range(func(key, value interface{}) bool {
				activeCount++
				return true
			})
			metrics.TSSActiveSessions.Set(float64(activeCount))
		}
	}
}

// healthCheck performs periodic health checks
func (m *TssModule) healthCheck(ctx context.Context) {
	ticker := time.NewTicker(30 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if err := m.performHealthCheck(); err != nil {
				m.logger.Errorf("TSS health check failed: %v", err)
				metrics.RecordError("tss", "health_check_failed")
				metrics.TSSClientStatus.Set(0)
			} else {
				metrics.TSSClientStatus.Set(1)
			}
		}
	}
}

// performHealthCheck checks if TSS service is healthy
func (m *TssModule) performHealthCheck() error {
	timer := metrics.NewTimer("tss", "health_check")

	// TODO: Implement actual health check logic
	// For now, just check if the client is configured
	if m.signClient == nil {
		timer.RecordFailure()
		return fmt.Errorf("TSS client not initialized")
	}

	timer.RecordSuccess()
	return nil
}

// StartSession starts a new TSS session
func (m *TssModule) StartSession(sessionID string, sessionType string) {
	m.logger.Infof("Starting TSS session: %s", sessionID)

	// Record session start
	m.activeSessions.Store(sessionID, time.Now())

	// Update metrics
	metrics.TSSActiveSessions.Set(float64(m.getActiveSessionCount()))
}

// EndSession ends a TSS session
func (m *TssModule) EndSession(sessionID string, success bool) {
	m.logger.Infof("Ending TSS session: %s, success: %v", sessionID, success)

	// Get session start time
	if startTime, exists := m.activeSessions.Load(sessionID); exists {
		duration := time.Since(startTime.(time.Time))

		// Record session metrics
		if success {
			metrics.TSSSessionsTotal.WithLabelValues("success").Inc()
		} else {
			metrics.TSSSessionsTotal.WithLabelValues("failure").Inc()
		}

		metrics.TSSSessionDuration.WithLabelValues("default").Observe(duration.Seconds())

		// Remove from active sessions
		m.activeSessions.Delete(sessionID)

		// Update active count
		metrics.TSSActiveSessions.Set(float64(m.getActiveSessionCount()))
	}
}

// RecordSigningRequest records a signing request
func (m *TssModule) RecordSigningRequest(success bool, duration time.Duration) {
	if success {
		metrics.TSSSigningRequests.WithLabelValues("success").Inc()
	} else {
		metrics.TSSSigningRequests.WithLabelValues("failure").Inc()
	}

	metrics.TSSSigningLatency.Observe(duration.Seconds())
}

// RecordTimeout records a timeout event
func (m *TssModule) RecordTimeout() {
	metrics.TSSTimeouts.Inc()
	metrics.RecordError("tss", "timeout")
}

// getActiveSessionCount returns the number of active sessions
func (m *TssModule) getActiveSessionCount() int {
	count := 0
	m.activeSessions.Range(func(key, value interface{}) bool {
		count++
		return true
	})
	return count
}
