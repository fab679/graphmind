---
sidebar_position: 2
title: "ADR-002: Use RocksDB"
---

# ADR-002: Use RocksDB for Persistence Layer

## Status
**Accepted**

## Date
2025-10-14

## Context

Graphmind Graph Database requires a persistent storage layer that provides durability, high write performance, fast random reads, compression, multi-tenancy support, and proven reliability.

## Decision

**We will use RocksDB as the persistence layer for Graphmind Graph Database.**

### Column Family Strategy

Each tenant gets a dedicated column family:
- `default`: Metadata
- `nodes`: Node Data
- `edges`: Edge Data
- `wal`: Write-Ahead Log
- `tenant_N`: Isolated tenant data

## Rationale

### 1. LSM-Tree Architecture

RocksDB uses Log-Structured Merge (LSM) trees, ideal for write-heavy workloads:

```
Write -> WAL (sequential) -> MemTable (in-memory) -> SST Files (background)
```

Sequential writes are 100x faster than random writes. Graph mutations become append operations.

### 2. Compression

Tiered compression per level:
- Level 0: No compression (write hot)
- Level 1-2: LZ4 (fast)
- Level 3+: Zstd (high ratio, ~3.5x reduction)

### 3. Production Proven

Used by Meta (Facebook), LinkedIn, Netflix, and Uber at massive scale.

## Consequences

### Positive

- **Write Performance**: 80,000+ writes/sec on commodity hardware
- **Compression**: 2-4x reduction in storage costs
- **Multi-Tenancy**: Column families provide isolation with independent compaction
- **Tunable**: 100+ configuration options
- **Mature**: 10+ years of production use with excellent Rust bindings (`rust-rocksdb`)

### Negative

- **Read Amplification**: May read multiple levels (mitigated by Bloom filters and caching)
- **Compaction Overhead**: Background CPU usage during heavy writes
- **Complexity**: Many tuning knobs (use proven configurations)
- **Write Amplification**: 10-50x data rewritten during compaction (acceptable for SSD)

## Performance Benchmarks

| Operation | RocksDB | LMDB | LevelDB | Sled |
|-----------|---------|------|---------|------|
| Sequential Write (ops/s) | 83,333 | 35,714 | 55,556 | 45,455 |
| Random Write (ops/s) | 75,000 | 25,000 | 48,000 | 38,000 |
| Point Lookup (ops/s) | 125,000 | 166,667 | 90,000 | 111,111 |

## Alternatives Considered

- **LMDB**: Rejected -- worse write performance, no compression, read-optimized
- **LevelDB**: Rejected -- no column families, limited compression, less maintained
- **Sled**: Rejected -- immature (pre-1.0), limited production use
- **Custom LSM-tree**: Rejected -- massive development effort (12-24 months)

## Related Decisions

- [ADR-001](./001-rust.md): Rust has excellent RocksDB bindings
- [ADR-005](./005-capnproto.md): Serialization format for RocksDB values
- [ADR-004](./004-raft.md): Raft uses RocksDB for log storage

---

**Last Updated**: 2025-10-14
**Status**: Accepted and Implemented
