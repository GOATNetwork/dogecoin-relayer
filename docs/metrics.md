# Dogecoin Relayer Metrics System

## Overview

This document describes the comprehensive metrics collection system implemented for the Dogecoin Relayer. The system uses Prometheus metrics format and provides detailed monitoring capabilities for all modules.

## Configuration

The metrics system can be configured through the `metrics` section in your `config.yaml` file:

```yaml
metrics:
  # Enable/disable metrics collection
  enabled: true
  
  # Enable/disable system metrics collection (memory, CPU, etc.)
  collect_system_metrics: true
  
  # System metrics collection interval in seconds
  system_collect_interval: 10
  
  # Enable/disable detailed metrics (message sizes, latencies, etc.)
  # Detailed metrics can increase memory usage and processing overhead
  enable_detailed_metrics: true
```

### Configuration Options

- **`enabled`** (boolean): Master switch for metrics collection. When `false`, no metrics are collected and the `/metrics` endpoint returns a disabled message.
- **`collect_system_metrics`** (boolean): Controls whether system-level metrics (memory, goroutines, GC) are collected.
- **`system_collect_interval`** (integer): Interval in seconds for system metrics collection. Default: 10 seconds.
- **`enable_detailed_metrics`** (boolean): Controls whether detailed metrics (message sizes, latencies, etc.) are collected. Disabling this can reduce memory usage.

### Performance Considerations

- When `enabled: false`, metrics collection has near-zero overhead
- When `enable_detailed_metrics: false`, only basic counters and gauges are collected
- System metrics collection can be disabled to reduce background processing
- Larger collection intervals reduce CPU usage but provide less granular data

## Architecture

The metrics system is built around the following components:

- **Metrics Manager**: Central coordinator for all metrics collection
- **Module-specific Collectors**: Each module has its own metrics collectors
- **System Metrics**: Runtime and system-level metrics
- **HTTP Endpoint**: Prometheus-compatible metrics exposure

## Available Metrics

### System Metrics

- `dogecoin_relayer_start_time_seconds`: Application start time
- `dogecoin_relayer_module_status`: Module status (1=running, 0=stopped)
- `dogecoin_relayer_errors_total`: Total errors by module and type
- `dogecoin_relayer_operations_total`: Total operations by module/operation/status
- `dogecoin_relayer_operation_duration_seconds`: Operation duration histogram
- `dogecoin_relayer_system_memory_usage_bytes`: Memory usage by type
- `dogecoin_relayer_system_goroutine_count`: Number of goroutines
- `dogecoin_relayer_system_gc_duration_seconds`: GC duration
- `dogecoin_relayer_system_gc_count_total`: Total GC runs

### Dogecoin Module Metrics

- `dogecoin_relayer_doge_block_height`: Current block height
- `dogecoin_relayer_doge_blocks_processed_total`: Blocks processed counter
- `dogecoin_relayer_doge_rpc_calls_total`: RPC calls by method and status
- `dogecoin_relayer_doge_rpc_duration_seconds`: RPC call duration
- `dogecoin_relayer_doge_connection_status`: Connection status
- `dogecoin_relayer_doge_transactions_scanned_total`: Transactions scanned
- `dogecoin_relayer_doge_confirmations_required`: Required confirmations

### Consensus Module Metrics

- `dogecoin_relayer_consensus_eth_connection_status`: Ethereum connection status
- `dogecoin_relayer_consensus_eth_transactions_sent_total`: Transactions sent
- `dogecoin_relayer_consensus_eth_gas_used_total`: Gas used by transaction type
- `dogecoin_relayer_consensus_eth_gas_price_gwei`: Current gas price in Gwei
- `dogecoin_relayer_consensus_eth_events_detected_total`: Events detected
- `dogecoin_relayer_consensus_eth_last_scanned_block`: Last scanned block
- `dogecoin_relayer_consensus_eth_block_processing_seconds`: Block processing time

### P2P Module Metrics

- `dogecoin_relayer_p2p_peer_count`: Connected peers by status
- `dogecoin_relayer_p2p_messages_sent_total`: Messages sent by type/status
- `dogecoin_relayer_p2p_messages_received_total`: Messages received by type/status
- `dogecoin_relayer_p2p_message_size_bytes`: Message size distribution
- `dogecoin_relayer_p2p_network_latency_seconds`: Network latency to peers
- `dogecoin_relayer_p2p_connection_duration_seconds`: Connection duration

### TSS Module Metrics

- `dogecoin_relayer_tss_active_sessions`: Active TSS sessions
- `dogecoin_relayer_tss_sessions_total`: Total sessions by status
- `dogecoin_relayer_tss_session_duration_seconds`: Session duration
- `dogecoin_relayer_tss_signing_requests_total`: Signing requests by status
- `dogecoin_relayer_tss_signing_latency_seconds`: Signing latency
- `dogecoin_relayer_tss_client_status`: TSS client connection status
- `dogecoin_relayer_tss_timeouts_total`: Total timeouts

### HTTP Module Metrics

- `dogecoin_relayer_http_requests_total`: HTTP requests by method/endpoint/status
- `dogecoin_relayer_http_request_duration_seconds`: Request duration
- `dogecoin_relayer_http_request_size_bytes`: Request size
- `dogecoin_relayer_http_response_size_bytes`: Response size
- `dogecoin_relayer_http_server_status`: HTTP server status
- `dogecoin_relayer_http_active_connections`: Active HTTP connections

## Usage

### Basic Setup

1. **Enable metrics in configuration**:
   ```yaml
   metrics:
     enabled: true
     collect_system_metrics: true
     system_collect_interval: 10
     enable_detailed_metrics: true
   ```

2. **Enable HTTP module**:
   ```yaml
   http:
     enabled: true
     port: 8080
   ```

### Access Metrics

When metrics are enabled, the following endpoints are available:

- **Metrics endpoint**: `http://localhost:8080/metrics`
- **Health check**: `http://localhost:8080/health`
- **Status**: `http://localhost:8080/status`
- **Root**: `http://localhost:8080/`

When metrics are disabled:
- The `/metrics` endpoint returns a 503 status with an explanatory message
- Health and status endpoints include metrics status information
- The root page shows metrics status visually

### Prometheus Configuration

Add the following to your Prometheus configuration:

```yaml
scrape_configs:
  - job_name: 'dogecoin-relayer'
    static_configs:
      - targets: ['localhost:8080']
    metrics_path: /metrics
    scrape_interval: 15s
```

### Production Recommendations

For production environments, consider these configurations:

#### High Performance (Minimal Metrics)
```yaml
metrics:
  enabled: true
  collect_system_metrics: false
  enable_detailed_metrics: false
```

#### Balanced (Recommended)
```yaml
metrics:
  enabled: true
  collect_system_metrics: true
  system_collect_interval: 30
  enable_detailed_metrics: false
```

#### Full Monitoring (Development/Debug)
```yaml
metrics:
  enabled: true
  collect_system_metrics: true
  system_collect_interval: 10
  enable_detailed_metrics: true
```

## Grafana Dashboard

### Key Panels to Create

1. **System Overview**
   - Module status
   - Memory usage
   - Goroutine count
   - Error rates

2. **Dogecoin Module**
   - Block height over time
   - RPC call success rate
   - Connection status
   - Transaction processing rate

3. **Consensus Module**
   - Ethereum connection status
   - Gas price trends
   - Transaction success rate
   - Event detection rate

4. **P2P Module**
   - Connected peers
   - Message throughput
   - Network latency
   - Connection duration

5. **TSS Module**
   - Active sessions
   - Signing success rate
   - Session duration
   - Timeout frequency

6. **HTTP Module**
   - Request rate
   - Response time
   - Error rate
   - Active connections

### Sample Queries

```promql
# Module status
dogecoin_relayer_module_status

# Error rate by module
rate(dogecoin_relayer_errors_total[5m])

# RPC call success rate
rate(dogecoin_relayer_doge_rpc_calls_total{status="success"}[5m]) / rate(dogecoin_relayer_doge_rpc_calls_total[5m])

# Memory usage
dogecoin_relayer_system_memory_usage_bytes

# HTTP request rate
rate(dogecoin_relayer_http_requests_total[5m])
```

## Alerting

### Recommended Alerts

1. **Module Down**
   ```promql
   dogecoin_relayer_module_status == 0
   ```

2. **High Error Rate**
   ```promql
   rate(dogecoin_relayer_errors_total[5m]) > 0.1
   ```

3. **RPC Connection Issues**
   ```promql
   dogecoin_relayer_doge_connection_status == 0
   ```

4. **High Memory Usage**
   ```promql
   dogecoin_relayer_system_memory_usage_bytes{type="heap"} > 1000000000
   ```

5. **TSS Timeout Issues**
   ```promql
   rate(dogecoin_relayer_tss_timeouts_total[5m]) > 0.01
   ```

## Development

### Adding New Metrics

1. Add metric definitions to `internal/metrics/collectors.go`
2. Update the relevant module to record metrics
3. Add documentation to this file

### Example: Adding a New Counter

```go
// In collectors.go
var MyNewCounter = promauto.NewCounter(prometheus.CounterOpts{
    Namespace: Namespace,
    Subsystem: "mymodule",
    Name:      "my_counter_total",
    Help:      "Description of my counter",
})

// In your module
if metrics.IsEnabled() {
    metrics.MyNewCounter.Inc()
}
```

### Testing Metrics

Use `curl` to test metrics endpoint:

```bash
curl http://localhost:8080/metrics
curl http://localhost:8080/health
curl http://localhost:8080/status
```

## Troubleshooting

### Common Issues

1. **Metrics endpoint not accessible**
   - Check if metrics are enabled in configuration
   - Check if HTTP module is enabled
   - Verify port configuration
   - Check firewall settings

2. **Missing metrics**
   - Ensure metrics are enabled in configuration
   - Ensure modules are enabled
   - Check for initialization errors
   - Verify metric registration

3. **High cardinality warnings**
   - Review label usage
   - Consider aggregating labels
   - Use appropriate metric types
   - Disable detailed metrics if not needed

4. **Performance issues**
   - Disable detailed metrics
   - Increase system collection interval
   - Disable system metrics collection
   - Consider disabling metrics entirely

### Debug Information

Enable debug logging to see metrics collection:

```yaml
log:
  level: debug
```

This will show detailed information about metrics collection and module operations.

### Metrics Status Check

You can check the current metrics status by visiting:
- `http://localhost:8080/health` - JSON response with metrics status
- `http://localhost:8080/status` - JSON response with module status
- `http://localhost:8080/` - HTML page with visual status indicators 