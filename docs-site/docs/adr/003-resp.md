---
sidebar_position: 3
title: "ADR-003: Use RESP Protocol"
---

# ADR-003: Use RESP Protocol for Network Communication

## Status
**Accepted**

## Date
2025-10-14

## Context

Graphmind Graph Database needs a network protocol with wide client compatibility, simplicity, low overhead, proven reliability, and extensibility for custom graph commands.

## Decision

**We will use RESP (Redis Serialization Protocol) as the primary network protocol, with custom GRAPH.* commands for graph operations.**

### Command Namespace

```
GRAPH.QUERY <graph-name> <cypher-query>
GRAPH.RO_QUERY <graph-name> <cypher-query>
GRAPH.DELETE <graph-name>
GRAPH.LIST
GRAPH.INFO <graph-name>
GRAPH.EXPLAIN <graph-name> <cypher-query>
GRAPH.SLOWLOG
GRAPH.CONFIG GET/SET
```

## Rationale

### 1. Ecosystem Compatibility

Existing Redis clients in every language: Python (`redis-py`), Java (`Jedis`, `Lettuce`), JavaScript (`node-redis`, `ioredis`), Go (`go-redis`), .NET, Ruby. No need to write client libraries from scratch.

### 2. Protocol Simplicity

Human-readable (easy debugging with `redis-cli`), simple state machine parsing, efficient binary encoding, self-describing types.

### 3. RedisGraph Precedent

RedisGraph successfully implemented graph database using RESP -- proven architecture with known limitations and workarounds.

## Consequences

### Positive

- **Instant Client Support**: Works with all Redis clients
- **Tooling**: Works with `redis-cli`, `redis-benchmark`, monitoring tools
- **Pipelining**: Batch multiple queries in a single round-trip
- **Low Overhead**: ~0.3ms parsing overhead

### Negative

- **Not Truly Redis Compatible**: Custom commands, different data model
- **Complex Results Encoding**: Graph results must be encoded as RESP arrays/maps
- **No Streaming**: Complete response before returning (use LIMIT, add HTTP streaming later)

## Protocol Benchmarks

| Protocol | Latency (P50) | Latency (P99) | Throughput |
|----------|---------------|---------------|------------|
| RESP | 0.8ms | 3.2ms | 450K req/s |
| HTTP/1.1 | 2.1ms | 8.5ms | 180K req/s |
| HTTP/2 | 1.5ms | 5.2ms | 320K req/s |
| gRPC | 1.2ms | 4.1ms | 380K req/s |

## Alternatives Considered

- **gRPC**: Use for internal cluster communication (Phase 3+), not client-facing
- **HTTP REST API Only**: Higher latency, no pipelining -- provide as secondary option
- **Custom Binary Protocol**: Rejected -- need client libraries for every language

## Related Decisions

- [ADR-006](./006-tokio.md): Tokio enables async TCP server
- [ADR-005](./005-capnproto.md): Internal serialization (not RESP)

---

**Last Updated**: 2025-10-14
**Status**: Accepted and Implemented
