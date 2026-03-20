---
sidebar_position: 9
title: "ADR-009: Graph Partitioning"
---

# ADR-009: Graph-Aware Partitioning for Distributed Mode

## Status
**Proposed** (Phase 4+)

## Date
2025-10-14

## Context

For distributed scaling beyond replication (Phase 4+), we need to partition graph data across nodes. Graph partitioning is fundamentally harder than key-value partitioning due to graph connectivity.

## Decision (Proposed)

**We will use a hybrid approach: Replication for hot data, graph-aware partitioning for cold data.**

### The Graph Partitioning Problem

**Goal**: Minimize edge cuts (edges crossing partitions)

Hash partitioning destroys graph locality -- random NodeId distribution means most edges cross partitions and every traversal requires a network hop.

**Performance Impact**:

| Partitioning | Local Edges | Network Hops | Query Latency |
|--------------|-------------|--------------|---------------|
| Hash | 20% | 80% | **250ms** |
| Graph-Aware | 85% | 15% | **45ms** |

### Graph-Aware Algorithms

- **METIS** (Offline): Near-optimal partitioning, minutes for billion-node graphs, used for batch rebalancing
- **Streaming Partitioner** (Online): Real-time incremental partitioning for growing graphs

## Consequences

### Positive

- **Better Locality**: 85% edges stay within partition
- **Faster Queries**: Fewer network hops
- **Scalability**: Linear scaling for partitionable graphs

### Negative

- **Complexity**: Significantly more complex than replication
- **Rebalancing**: Expensive to repartition
- **Skew**: Some partitions may be larger (hotspots)

**Risk Level**: **VERY HIGH** -- This is a research-level problem. Only implement if Phase 3 (replication) is not sufficient.

## Go/No-Go Decision

**After Phase 3, evaluate**: Can we handle 95% of use cases with replication? Do we have distributed systems experts? Is the complexity worth it?

## Related Decisions

- [ADR-004](./004-raft.md): Raft for each partition
- [ADR-002](./002-rocksdb.md): Each partition has RocksDB instance

---

**Last Updated**: 2025-10-14
**Status**: Proposed (Phase 4+, High Risk)
