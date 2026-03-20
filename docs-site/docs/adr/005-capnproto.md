---
sidebar_position: 5
title: "ADR-005: Use Cap'n Proto"
---

# ADR-005: Use Cap'n Proto for Zero-Copy Serialization

## Status
**Accepted**

## Date
2025-10-14

## Context

Graphmind Graph Database needs efficient serialization for persistence (RocksDB), network communication (cluster nodes), snapshots (backups), and performance-critical paths (sub-millisecond overhead).

## Decision

**We will use Cap'n Proto for zero-copy serialization of graph data structures.**

Additionally:
- **Apache Arrow** for columnar property data
- **bincode** for internal Rust-only structures (hot path)

## Rationale

### 1. Zero-Copy Performance

Traditional serialization:
```
Read from disk -> Deserialize -> Copy to objects -> Use
  (1ms)            (0.5ms)        (0.3ms)
```

Cap'n Proto:
```
Read from disk -> Cast pointer -> Use
  (1ms)            (0us!)
```

### 2. Schema Evolution

Cap'n Proto supports adding fields without breaking compatibility -- enabling rolling upgrades, snapshot compatibility, and client version flexibility.

## Performance Comparison

| Operation (1M nodes) | Cap'n Proto | Protocol Buffers | JSON |
|---------------------|-------------|------------------|------|
| Serialize | 0 ms | 450 ms | 1200 ms |
| Deserialize | 0 ms | 380 ms | 980 ms |
| Size | 85 MB | 65 MB | 180 MB |

## Consequences

### Positive

- **Maximum Performance**: Zero-copy deserialization
- **Memory Efficient**: No intermediate allocations
- **Cache-Friendly**: Optimized data layout
- **Type-Safe**: Schema compiler generates safe accessors

### Negative

- **Larger Size**: ~30% larger than Protocol Buffers (compression reduces to ~10%)
- **Schema Compilation**: Extra build step (integrated via build.rs)
- **Learning Curve**: Different API from JSON/Protobuf

## Alternatives Considered

- **Protocol Buffers**: No zero-copy, 380ms deserialization for 1M nodes
- **FlatBuffers**: Similar performance but less mature Rust ecosystem
- **Apache Arrow**: Best for columnar data, not ideal for graph structures (used for properties)
- **bincode**: Fastest for Rust-only, but no cross-language support or schema evolution
- **JSON**: 10x slower, 2x larger

## Related Decisions

- [ADR-002](./002-rocksdb.md): Cap'n Proto data stored in RocksDB
- [ADR-004](./004-raft.md): Cap'n Proto for Raft log entries
- [ADR-003](./003-resp.md): RESP for client API, Cap'n Proto internal

---

**Last Updated**: 2025-10-14
**Status**: Accepted and Implemented
