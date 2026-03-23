---
sidebar_position: 4
title: Monitoring
description: Prometheus metrics, health checks, and audit logging
---

# Monitoring

Graphmind exposes Prometheus metrics, a health check endpoint, and structured logging for observability.

## Health Check

```bash
curl http://localhost:8080/api/status
```

```json
{
  "status": "healthy",
  "version": "0.6.1",
  "storage": {
    "nodes": 1042,
    "edges": 3891
  }
}
```

Use this endpoint for load balancer health checks, Docker `HEALTHCHECK`, or Kubernetes liveness probes.

RESP-based health check:

```bash
redis-cli -p 6379 PING
# PONG
```

## Prometheus Metrics

Graphmind exposes metrics at `GET /metrics` in Prometheus text format.

```bash
curl http://localhost:8080/metrics
```

### Available Metrics

| Metric | Type | Description |
|--------|------|-------------|
| `graphmind_nodes_total` | Gauge | Total number of nodes |
| `graphmind_edges_total` | Gauge | Total number of edges |
| `graphmind_queries_total` | Counter | Total queries executed (labels: `type=read\|write`) |
| `graphmind_query_duration_ms` | Histogram | Query execution time in ms (labels: `type=read\|write`) |
| `graphmind_resp_connections_active` | Gauge | Currently active RESP connections |
| `graphmind_resp_connections_total` | Counter | Total RESP connections since startup |
| `graphmind_script_statements_total` | Counter | Total script statements executed |
| `graphmind_script_errors_total` | Counter | Script statement errors |

### Prometheus Configuration

Add Graphmind as a scrape target in `prometheus.yml`:

```yaml
scrape_configs:
  - job_name: 'graphmind'
    scrape_interval: 15s
    static_configs:
      - targets: ['localhost:8080']
```

### Grafana Dashboard

With Prometheus scraping metrics, you can create Grafana dashboards. Useful panels:

- **Query rate**: `rate(graphmind_queries_total[5m])`
- **Query latency (p95)**: `histogram_quantile(0.95, rate(graphmind_query_duration_ms_bucket[5m]))`
- **Active connections**: `graphmind_resp_connections_active`
- **Graph size**: `graphmind_nodes_total` and `graphmind_edges_total`
- **Write vs read ratio**: `rate(graphmind_queries_total{type="write"}[5m]) / rate(graphmind_queries_total[5m])`

## Logging

Graphmind uses structured logging via the `tracing` crate. Control the log level with:

```bash
# Environment variable
RUST_LOG=info graphmind

# Config file
[logging]
level = "debug"

# CLI flag
graphmind --log-level debug
```

Log levels: `error`, `warn`, `info`, `debug`, `trace`

### Log output

Logs are written to stdout in a human-readable format by default. For JSON-structured output suitable for log aggregation (ELK, Loki, etc.), set:

```bash
RUST_LOG=info graphmind
```

### What gets logged

| Level | Events |
|-------|--------|
| `error` | Failed queries, persistence errors, startup failures |
| `warn` | Authentication failures, deprecated features, config issues |
| `info` | Server start/stop, recovery, connection events |
| `debug` | Individual query execution, plan details |
| `trace` | RESP protocol frames, operator-level execution |

## Docker Health Check

The Graphmind Docker image includes a built-in health check:

```bash
# Check container health
docker inspect --format='{{.State.Health.Status}}' graphmind
```

The health check sends a RESP `PING` every 30 seconds and expects `PONG`.

## Kubernetes Probes

```yaml
livenessProbe:
  httpGet:
    path: /api/status
    port: 8080
  initialDelaySeconds: 10
  periodSeconds: 30

readinessProbe:
  httpGet:
    path: /api/status
    port: 8080
  initialDelaySeconds: 5
  periodSeconds: 10
```
