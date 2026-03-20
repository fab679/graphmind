---
sidebar_position: 10
title: "ADR-010: Observability Stack"
---

# ADR-010: Use Prometheus + OpenTelemetry for Observability

## Status
**Accepted**

## Date
2025-10-14

## Context

Graphmind needs comprehensive observability for health monitoring, issue debugging, capacity planning, and SLA compliance.

## Decision

**We will use Prometheus for metrics, OpenTelemetry for tracing, and structured JSON logging.**

### Observability Stack

- **Prometheus**: Time-series metrics (pull-based, PromQL query language)
- **OpenTelemetry**: Distributed request tracing (vendor-neutral, integrates with Jaeger/Zipkin/Datadog)
- **Structured Logging**: JSON format via the `tracing` crate
- **Grafana**: Unified dashboards

### Key Metrics

- `graph_queries_total{type="read|write"}`
- `graph_query_duration_seconds{quantile="0.5|0.95|0.99"}`
- `graph_nodes_total`
- `graph_edges_total`
- `graph_memory_bytes`

## Consequences

### Positive

- **Comprehensive Observability**: Metrics + Traces + Logs
- **Industry Standard**: Prometheus/OTEL used by 80% of Cloud Native projects
- **Great Tooling**: Grafana dashboards, alert manager
- **Low Overhead**: Less than 1% performance impact

### Negative

- **Complexity**: Multiple systems to manage (mitigated by managed services)
- **Storage Cost**: Metrics/logs can be large (mitigated by retention policies, sampling)

## Alternatives Considered

- **Datadog/New Relic**: Expensive, vendor lock-in
- **Custom Metrics**: Reinventing the wheel
- **ELK Stack Only**: No metrics, expensive

## Related Decisions

- [ADR-001](./001-rust.md): Rust has excellent tracing crates
- [ADR-006](./006-tokio.md): Tokio integrates with tracing

---

**Last Updated**: 2025-10-14
**Status**: Accepted and Implemented
