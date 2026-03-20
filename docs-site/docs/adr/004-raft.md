---
sidebar_position: 4
title: "ADR-004: Use Raft Consensus"
---

# ADR-004: Use Raft Consensus for Distributed Coordination

## Status
**Accepted** (Phase 3+)

## Date
2025-10-14

## Context

For distributed deployment (Phase 3+), Graphmind needs a consensus algorithm providing strong consistency, fault tolerance, leader election, log replication, and understandability.

## Decision

**We will use Raft consensus protocol via the `openraft` Rust library for distributed coordination.**

### Write Flow

1. Client sends write to Leader
2. Leader appends to local log (uncommitted)
3. Leader replicates to Followers in parallel
4. Followers acknowledge
5. Leader commits once quorum achieved (2/3)
6. Leader applies to state machine and responds to client

## Rationale

### 1. Understandability (vs Paxos)

Raft is designed for understandability with a strong leader, randomized election timeouts, leader-only log appends, and election restrictions.

### 2. Production Proven

Used by etcd (Kubernetes control plane), CockroachDB, TiKV (petabyte-scale), and Consul.

### 3. Excellent Rust Implementation

The `openraft` library is actively developed, feature-complete, well-documented, and used in production.

## Consequences

### Positive

- **Strong Consistency**: Linearizable reads and writes
- **Fault Tolerance**: 3 nodes tolerates 1 failure, 5 nodes tolerates 2
- **Automatic Failover**: Leader election in 150-300ms
- **No Data Loss**: Committed data replicated to quorum

### Negative

- **Write Latency**: 1.5-2x higher than single-node (network round-trip)
- **Leader Bottleneck**: All writes go through leader (mitigated by read replicas)
- **Complexity**: Network partitions, clock skew, distributed debugging

### CAP Theorem

Raft chooses **CP** (Consistency + Partition Tolerance). During network partition, minority partition rejects writes to ensure consistency.

## Performance

| Setup | Write Latency (P50) | Write Latency (P99) |
|-------|-------------------|-------------------|
| Single Node | 1.2ms | 4.5ms |
| 3-Node Raft (same DC) | 2.8ms | 8.2ms |
| 3-Node Raft (cross-DC) | 15ms | 45ms |

## Alternatives Considered

- **Multi-Paxos**: Rejected -- much harder to understand and implement correctly
- **EPaxos**: Rejected -- research-level complexity, almost no production implementations
- **ZAB**: Rejected -- tied to ZooKeeper, no standalone Rust implementation
- **Two-Phase Commit**: Rejected -- blocking protocol, not fault-tolerant

## Related Decisions

- [ADR-002](./002-rocksdb.md): RocksDB stores Raft log
- [ADR-009](./009-partitioning.md): Raft for each partition

---

**Last Updated**: 2025-10-14
**Status**: Accepted (Phase 3+)
