package p2p

import (
	"context"
	"time"

	"github.com/goat-network/dogecoin-relayer/internal/metrics"
)

// collectMetrics periodically collects P2P metrics
func (m *P2PModule) collectMetrics(ctx context.Context) {
	ticker := time.NewTicker(10 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if m.network != nil {
				// Collect peer metrics
				peers := m.network.GetPeers()
				metrics.P2PPeerCount.WithLabelValues("connected").Set(float64(len(peers)))

				// TODO: Add more detailed metrics collection
				// This would require extending the Network interface
			}
		}
	}
}

// RecordMessageSent records a sent message
func (m *P2PModule) RecordMessageSent(messageType string, success bool, size int) {
	status := "success"
	if !success {
		status = "failure"
	}

	metrics.P2PMessagesSent.WithLabelValues(messageType, status).Inc()
	metrics.P2PMessageSize.WithLabelValues(messageType, "sent").Observe(float64(size))
}

// RecordMessageReceived records a received message
func (m *P2PModule) RecordMessageReceived(messageType string, success bool, size int) {
	status := "success"
	if !success {
		status = "failure"
	}

	metrics.P2PMessagesReceived.WithLabelValues(messageType, status).Inc()
	metrics.P2PMessageSize.WithLabelValues(messageType, "received").Observe(float64(size))
}

// RecordPeerLatency records network latency to a peer
func (m *P2PModule) RecordPeerLatency(peerID string, latency time.Duration) {
	metrics.P2PNetworkLatency.WithLabelValues(peerID).Observe(latency.Seconds())
}

// RecordPeerConnection records a peer connection event
func (m *P2PModule) RecordPeerConnection(peerID string, duration time.Duration) {
	metrics.P2PConnectionDuration.WithLabelValues(peerID).Observe(duration.Seconds())
}
