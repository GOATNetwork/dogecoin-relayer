package metrics

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

// Dogecoin module metrics
var (
	// Block metrics
	DogeBlockHeight = promauto.NewGauge(prometheus.GaugeOpts{
		Namespace: Namespace,
		Subsystem: "doge",
		Name:      "block_height",
		Help:      "Current Dogecoin block height",
	})

	DogeBlocksProcessed = promauto.NewCounterVec(prometheus.CounterOpts{
		Namespace: Namespace,
		Subsystem: "doge",
		Name:      "blocks_processed_total",
		Help:      "Total number of blocks processed",
	}, []string{"status"}) // success, failure

	// RPC metrics
	DogeRPCCalls = promauto.NewCounterVec(prometheus.CounterOpts{
		Namespace: Namespace,
		Subsystem: "doge",
		Name:      "rpc_calls_total",
		Help:      "Total number of RPC calls to Dogecoin node",
	}, []string{"method", "status"})

	DogeRPCDuration = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Namespace: Namespace,
		Subsystem: "doge",
		Name:      "rpc_duration_seconds",
		Help:      "Duration of RPC calls to Dogecoin node",
		Buckets:   prometheus.DefBuckets,
	}, []string{"method"})

	// Connection metrics
	DogeConnectionStatus = promauto.NewGauge(prometheus.GaugeOpts{
		Namespace: Namespace,
		Subsystem: "doge",
		Name:      "connection_status",
		Help:      "Dogecoin node connection status (1=connected, 0=disconnected)",
	})

	// Transaction metrics
	DogeTransactionsScanned = promauto.NewCounter(prometheus.CounterOpts{
		Namespace: Namespace,
		Subsystem: "doge",
		Name:      "transactions_scanned_total",
		Help:      "Total number of transactions scanned",
	})

	DogeConfirmations = promauto.NewGauge(prometheus.GaugeOpts{
		Namespace: Namespace,
		Subsystem: "doge",
		Name:      "confirmations_required",
		Help:      "Number of confirmations required",
	})
)

// Consensus module metrics
var (
	// Ethereum connection metrics
	EthConnectionStatus = promauto.NewGauge(prometheus.GaugeOpts{
		Namespace: Namespace,
		Subsystem: "consensus",
		Name:      "eth_connection_status",
		Help:      "Ethereum connection status (1=connected, 0=disconnected)",
	})

	// Transaction metrics
	EthTransactionsSent = promauto.NewCounterVec(prometheus.CounterOpts{
		Namespace: Namespace,
		Subsystem: "consensus",
		Name:      "eth_transactions_sent_total",
		Help:      "Total number of Ethereum transactions sent",
	}, []string{"status"}) // success, failure

	EthGasUsed = promauto.NewCounterVec(prometheus.CounterOpts{
		Namespace: Namespace,
		Subsystem: "consensus",
		Name:      "eth_gas_used_total",
		Help:      "Total gas used in Ethereum transactions",
	}, []string{"tx_type"})

	EthGasPrice = promauto.NewGauge(prometheus.GaugeOpts{
		Namespace: Namespace,
		Subsystem: "consensus",
		Name:      "eth_gas_price_gwei",
		Help:      "Current Ethereum gas price in Gwei",
	})

	// Event detection metrics
	EthEventsDetected = promauto.NewCounterVec(prometheus.CounterOpts{
		Namespace: Namespace,
		Subsystem: "consensus",
		Name:      "eth_events_detected_total",
		Help:      "Total number of events detected",
	}, []string{"event_type", "contract"})

	EthLastScannedBlock = promauto.NewGauge(prometheus.GaugeOpts{
		Namespace: Namespace,
		Subsystem: "consensus",
		Name:      "eth_last_scanned_block",
		Help:      "Last scanned Ethereum block number",
	})

	EthBlockProcessingTime = promauto.NewHistogram(prometheus.HistogramOpts{
		Namespace: Namespace,
		Subsystem: "consensus",
		Name:      "eth_block_processing_seconds",
		Help:      "Time taken to process Ethereum blocks",
		Buckets:   prometheus.DefBuckets,
	})
)

// P2P module metrics
var (
	// Peer metrics
	P2PPeerCount = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: Namespace,
		Subsystem: "p2p",
		Name:      "peer_count",
		Help:      "Number of connected peers",
	}, []string{"status"}) // connected, connecting, disconnected

	P2PMessagesSent = promauto.NewCounterVec(prometheus.CounterOpts{
		Namespace: Namespace,
		Subsystem: "p2p",
		Name:      "messages_sent_total",
		Help:      "Total number of P2P messages sent",
	}, []string{"message_type", "status"})

	P2PMessagesReceived = promauto.NewCounterVec(prometheus.CounterOpts{
		Namespace: Namespace,
		Subsystem: "p2p",
		Name:      "messages_received_total",
		Help:      "Total number of P2P messages received",
	}, []string{"message_type", "status"})

	P2PMessageSize = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Namespace: Namespace,
		Subsystem: "p2p",
		Name:      "message_size_bytes",
		Help:      "Size of P2P messages in bytes",
		Buckets:   prometheus.ExponentialBuckets(64, 2, 10), // 64B to 32KB
	}, []string{"message_type", "direction"}) // sent, received

	// Network metrics
	P2PNetworkLatency = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Namespace: Namespace,
		Subsystem: "p2p",
		Name:      "network_latency_seconds",
		Help:      "Network latency to peers",
		Buckets:   prometheus.DefBuckets,
	}, []string{"peer_id"})

	P2PConnectionDuration = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Namespace: Namespace,
		Subsystem: "p2p",
		Name:      "connection_duration_seconds",
		Help:      "Duration of peer connections",
		Buckets:   prometheus.ExponentialBuckets(60, 2, 10), // 1 minute to ~17 hours
	}, []string{"peer_id"})
)

// TSS module metrics
var (
	// Session metrics
	TSSActiveSessions = promauto.NewGauge(prometheus.GaugeOpts{
		Namespace: Namespace,
		Subsystem: "tss",
		Name:      "active_sessions",
		Help:      "Number of active TSS sessions",
	})

	TSSSessionsTotal = promauto.NewCounterVec(prometheus.CounterOpts{
		Namespace: Namespace,
		Subsystem: "tss",
		Name:      "sessions_total",
		Help:      "Total number of TSS sessions",
	}, []string{"status"}) // success, failure, timeout

	TSSSessionDuration = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Namespace: Namespace,
		Subsystem: "tss",
		Name:      "session_duration_seconds",
		Help:      "Duration of TSS sessions",
		Buckets:   prometheus.DefBuckets,
	}, []string{"session_type"})

	// Signing metrics
	TSSSigningRequests = promauto.NewCounterVec(prometheus.CounterOpts{
		Namespace: Namespace,
		Subsystem: "tss",
		Name:      "signing_requests_total",
		Help:      "Total number of signing requests",
	}, []string{"status"}) // success, failure

	TSSSigningLatency = promauto.NewHistogram(prometheus.HistogramOpts{
		Namespace: Namespace,
		Subsystem: "tss",
		Name:      "signing_latency_seconds",
		Help:      "Latency of signing operations",
		Buckets:   prometheus.DefBuckets,
	})

	// TSS client metrics
	TSSClientStatus = promauto.NewGauge(prometheus.GaugeOpts{
		Namespace: Namespace,
		Subsystem: "tss",
		Name:      "client_status",
		Help:      "TSS client connection status (1=connected, 0=disconnected)",
	})

	TSSTimeouts = promauto.NewCounter(prometheus.CounterOpts{
		Namespace: Namespace,
		Subsystem: "tss",
		Name:      "timeouts_total",
		Help:      "Total number of TSS timeouts",
	})
)

// HTTP module metrics
var (
	// Request metrics
	HTTPRequests = promauto.NewCounterVec(prometheus.CounterOpts{
		Namespace: Namespace,
		Subsystem: "http",
		Name:      "requests_total",
		Help:      "Total number of HTTP requests",
	}, []string{"method", "endpoint", "status"})

	HTTPRequestDuration = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Namespace: Namespace,
		Subsystem: "http",
		Name:      "request_duration_seconds",
		Help:      "Duration of HTTP requests",
		Buckets:   prometheus.DefBuckets,
	}, []string{"method", "endpoint"})

	HTTPRequestSize = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Namespace: Namespace,
		Subsystem: "http",
		Name:      "request_size_bytes",
		Help:      "Size of HTTP requests",
		Buckets:   prometheus.ExponentialBuckets(64, 2, 10),
	}, []string{"method", "endpoint"})

	HTTPResponseSize = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Namespace: Namespace,
		Subsystem: "http",
		Name:      "response_size_bytes",
		Help:      "Size of HTTP responses",
		Buckets:   prometheus.ExponentialBuckets(64, 2, 10),
	}, []string{"method", "endpoint"})

	// Server metrics
	HTTPServerStatus = promauto.NewGauge(prometheus.GaugeOpts{
		Namespace: Namespace,
		Subsystem: "http",
		Name:      "server_status",
		Help:      "HTTP server status (1=running, 0=stopped)",
	})

	HTTPActiveConnections = promauto.NewGauge(prometheus.GaugeOpts{
		Namespace: Namespace,
		Subsystem: "http",
		Name:      "active_connections",
		Help:      "Number of active HTTP connections",
	})
)

// System-level metrics
var (
	// Memory metrics
	MemoryUsage = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: Namespace,
		Subsystem: "system",
		Name:      "memory_usage_bytes",
		Help:      "Memory usage in bytes",
	}, []string{"type"}) // heap, stack, sys

	// Goroutine metrics
	GoroutineCount = promauto.NewGauge(prometheus.GaugeOpts{
		Namespace: Namespace,
		Subsystem: "system",
		Name:      "goroutine_count",
		Help:      "Number of goroutines",
	})

	// GC metrics
	GCDuration = promauto.NewHistogram(prometheus.HistogramOpts{
		Namespace: Namespace,
		Subsystem: "system",
		Name:      "gc_duration_seconds",
		Help:      "Duration of garbage collection",
		Buckets:   prometheus.DefBuckets,
	})

	GCCount = promauto.NewCounter(prometheus.CounterOpts{
		Namespace: Namespace,
		Subsystem: "system",
		Name:      "gc_count_total",
		Help:      "Total number of GC runs",
	})
)

// Helper functions for conditional metric recording

// RecordDogeRPCCall records a Dogecoin RPC call with conditional detailed metrics
func RecordDogeRPCCall(method, status string, duration float64) {
	if !IsEnabled() {
		return
	}

	DogeRPCCalls.WithLabelValues(method, status).Inc()

	// Only record detailed duration if enabled
	ConditionallyRecord(true, func() {
		DogeRPCDuration.WithLabelValues(method).Observe(duration)
	})
}

// RecordP2PMessage records a P2P message with conditional detailed metrics
func RecordP2PMessage(messageType, direction, status string, size int) {
	if !IsEnabled() {
		return
	}

	if direction == "sent" {
		P2PMessagesSent.WithLabelValues(messageType, status).Inc()
	} else {
		P2PMessagesReceived.WithLabelValues(messageType, status).Inc()
	}

	// Only record message size if detailed metrics are enabled
	ConditionallyRecord(true, func() {
		P2PMessageSize.WithLabelValues(messageType, direction).Observe(float64(size))
	})
}

// RecordHTTPRequest records an HTTP request with conditional detailed metrics
func RecordHTTPRequest(method, endpoint, status string, duration, requestSize, responseSize float64) {
	if !IsEnabled() {
		return
	}

	HTTPRequests.WithLabelValues(method, endpoint, status).Inc()
	HTTPRequestDuration.WithLabelValues(method, endpoint).Observe(duration)

	// Only record size metrics if detailed metrics are enabled
	ConditionallyRecord(true, func() {
		if requestSize > 0 {
			HTTPRequestSize.WithLabelValues(method, endpoint).Observe(requestSize)
		}
		if responseSize > 0 {
			HTTPResponseSize.WithLabelValues(method, endpoint).Observe(responseSize)
		}
	})
}
