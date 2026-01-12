package http

import (
	"fmt"
	"net/http"
	"time"

	"github.com/goat-network/dogecoin-relayer/internal/metrics"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

// responseWriter wrapper to capture status code
type responseWriter struct {
	http.ResponseWriter
	statusCode int
}

func (rw *responseWriter) WriteHeader(code int) {
	rw.statusCode = code
	rw.ResponseWriter.WriteHeader(code)
}

func (m *HttpModule) registerHandlers(mux *http.ServeMux) {
	// Add metrics endpoint only if metrics are enabled
	if metrics.IsEnabled() {
		mux.Handle("/metrics", promhttp.Handler())
		m.logger.Info("Metrics endpoint enabled at /metrics")
	} else {
		// Provide a disabled metrics endpoint
		mux.HandleFunc("/metrics", m.disabledMetricsHandler)
		m.logger.Info("Metrics endpoint disabled")
	}

	// Add health check endpoint
	mux.HandleFunc("/health", m.healthHandler)

	// Add status endpoint
	mux.HandleFunc("/status", m.statusHandler)

	// Add Fireblocks cosigner callback endpoint
	mux.HandleFunc("/api/fireblocks/cosigner/v2/tx_sign_request", m.handleFireblocksCosignerTxSign)
	m.logger.Info("Fireblocks cosigner callback endpoint enabled at /api/fireblocks/cosigner/v2/tx_sign_request")

	// Add Fireblocks webhook endpoint
	mux.HandleFunc("/api/fireblocks/webhook", m.handleFireblocksWebhook)
	m.logger.Info("Fireblocks webhook endpoint enabled at /api/fireblocks/webhook")

	// Add root endpoint
	mux.HandleFunc("/", m.rootHandler)
}

// disabledMetricsHandler handles requests to disabled metrics endpoint
func (m *HttpModule) disabledMetricsHandler(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "text/plain")
	w.WriteHeader(http.StatusServiceUnavailable)
	fmt.Fprint(w, "Metrics collection is disabled. Please enable it in the configuration.")
}

// healthHandler handles health check requests
func (m *HttpModule) healthHandler(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)

	metricsStatus := "disabled"
	if metrics.IsEnabled() {
		metricsStatus = "enabled"
	}

	// Simple JSON response
	fmt.Fprintf(w, `{
		"status": "ok",
		"timestamp": "%s",
		"service": "dogecoin-relayer",
		"metrics": "%s"
	}`, time.Now().UTC().Format(time.RFC3339), metricsStatus)
}

// statusHandler handles status requests
func (m *HttpModule) statusHandler(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)

	metricsStatus := "disabled"
	if metrics.IsEnabled() {
		metricsStatus = "enabled"
	}

	// Get module statuses (simplified)
	fmt.Fprintf(w, `{
		"status": "running",
		"timestamp": "%s",
		"modules": {
			"http": "running",
			"metrics": "%s"
		}
	}`, time.Now().UTC().Format(time.RFC3339), metricsStatus)
}

// rootHandler handles root requests
func (m *HttpModule) rootHandler(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "text/html")
	w.WriteHeader(http.StatusOK)

	metricsLink := `<li><a href="/metrics">Metrics (Prometheus)</a></li>`
	if !metrics.IsEnabled() {
		metricsLink = `<li><a href="/metrics">Metrics (Disabled)</a></li>`
	}

	html := fmt.Sprintf(`
	<!DOCTYPE html>
	<html>
	<head>
		<title>Dogecoin Relayer</title>
		<style>
			body { font-family: Arial, sans-serif; margin: 40px; }
			h1 { color: #333; }
			a { color: #0066cc; text-decoration: none; }
			a:hover { text-decoration: underline; }
			ul { line-height: 1.6; }
			.status { padding: 10px; margin: 10px 0; border-radius: 5px; }
			.enabled { background-color: #d4edda; color: #155724; }
			.disabled { background-color: #f8d7da; color: #721c24; }
		</style>
	</head>
	<body>
		<h1>Dogecoin Relayer</h1>
		<p>Welcome to the Dogecoin Relayer service.</p>
		<div class="status %s">
			<strong>Metrics Status:</strong> %s
		</div>
		<h2>Available Endpoints:</h2>
		<ul>
			<li><a href="/health">Health Check</a></li>
			<li><a href="/status">Status</a></li>
			%s
			<li>POST /api/fireblocks/cosigner/v2/tx_sign_request - Fireblocks Cosigner Callback</li>
			<li>POST /api/fireblocks/webhook - Fireblocks Webhook</li>
		</ul>
	</body>
	</html>
	`, func() string {
		if metrics.IsEnabled() {
			return "enabled"
		}
		return "disabled"
	}(), func() string {
		if metrics.IsEnabled() {
			return "Enabled"
		}
		return "Disabled"
	}(), metricsLink)

	fmt.Fprint(w, html)
}
