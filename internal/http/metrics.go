package http

import (
	"net/http"
	"strconv"
	"time"

	"github.com/goat-network/dogecoin-relayer/internal/metrics"
	log "github.com/sirupsen/logrus"
)

// metricsMiddleware adds metrics collection to HTTP requests
func (m *HttpModule) metricsMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()

		// Create a response writer wrapper to capture status code
		wrappedWriter := &responseWriter{ResponseWriter: w, statusCode: 200}

		// Increment active connections
		if metrics.IsEnabled() {
			metrics.HTTPActiveConnections.Inc()
			defer metrics.HTTPActiveConnections.Dec()
		}

		// Call the next handler
		next.ServeHTTP(wrappedWriter, r)

		// Record metrics
		if metrics.IsEnabled() {
			duration := time.Since(start)
			status := strconv.Itoa(wrappedWriter.statusCode)

			requestSize := float64(0)
			if r.ContentLength > 0 {
				requestSize = float64(r.ContentLength)
			}

			// Use the helper function for conditional recording
			metrics.RecordHTTPRequest(r.Method, r.URL.Path, status, duration.Seconds(), requestSize, 0)
		}

		// Log the request
		m.logger.WithFields(log.Fields{
			"method":   r.Method,
			"path":     r.URL.Path,
			"status":   wrappedWriter.statusCode,
			"duration": time.Since(start),
			"ip":       r.RemoteAddr,
		}).Info("HTTP request processed")
	})
}
