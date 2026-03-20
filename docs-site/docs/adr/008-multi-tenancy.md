---
sidebar_position: 8
title: "ADR-008: Namespace Isolation"
---

# ADR-008: Use Namespace Isolation for Multi-Tenancy

## Status
**Accepted**

## Date
2025-10-14

## Context

Graphmind must support multiple isolated tenants on a single cluster with data isolation, resource quotas, performance isolation, and operational simplicity.

## Decision

**We will use namespace-based isolation with RocksDB column families and resource quotas.**

### Architecture

Each tenant gets:
- A dedicated RocksDB column family
- Memory and storage quotas
- Independent compaction configuration
- Per-tenant metrics

### Resource Quotas

```rust
struct TenantQuota {
    max_memory: usize,        // e.g., 4 GB
    max_storage: usize,       // e.g., 100 GB
    max_query_time: Duration, // e.g., 30 seconds
    max_connections: usize,   // e.g., 100
}
```

## Consequences

### Positive

- **Strong Isolation**: Tenants completely separated
- **Fair Resource Allocation**: Quotas prevent noisy neighbors
- **Simple Mental Model**: Easy to understand and operate
- **Scalable**: Tested with 100+ tenants per node

### Negative

- **Not Full Physical Isolation**: Share CPU, network (monitor per-tenant metrics)
- **Quota Enforcement Overhead**: ~0.1ms per query (acceptable)

## Alternatives Considered

- **Separate Processes**: Too much overhead
- **Virtual Clusters**: Complex, overkill
- **No Isolation**: Unacceptable for security

---

**Last Updated**: 2025-10-14
**Status**: Accepted and Implemented
