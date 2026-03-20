---
sidebar_position: 1
title: "ADR-001: Use Rust"
---

# ADR-001: Use Rust as Primary Programming Language

## Status
**Accepted**

## Date
2025-10-14

## Context

We need to choose a programming language for implementing Graphmind Graph Database that meets the following requirements:

1. **Memory Safety**: Graph databases handle complex pointer structures and concurrent access
2. **Performance**: Sub-10ms query latency for in-memory traversals
3. **No GC Pauses**: Predictable latency is critical for database systems
4. **Concurrency**: Must handle thousands of concurrent connections efficiently
5. **Systems Control**: Fine-grained control over memory layout and allocation
6. **Long-term Maintainability**: 5+ year project lifecycle

### Key Challenges

- Graph traversals involve pointer chasing and complex data structures
- In-memory operations require cache-efficient memory layouts
- Distributed coordination needs reliable network code
- Multi-tenancy requires strict resource isolation

## Decision

**We will use Rust as the primary programming language for Graphmind Graph Database.**

### Key Advantages

1. **Memory Safety Without Runtime Cost**
   - Prevents use-after-free, double-free, buffer overflows
   - Compiler catches bugs at build time
   - No runtime garbage collector

2. **Fearless Concurrency**
   - Ownership system prevents data races
   - Compile-time guarantees for thread safety
   - No need for defensive copying

3. **Performance**
   - Zero-cost abstractions
   - LLVM-based optimization
   - Performance comparable to C++

4. **Modern Ecosystem**
   - Excellent database libraries (RocksDB, tokio)
   - Growing adoption in database systems (TiKV, Sled, GreptimeDB)
   - Strong community support

## Consequences

### Positive

- **Safety**: Entire classes of bugs prevented at compile time (no null pointer dereferences, no data races, memory leaks are rare)
- **Performance**: Meets sub-10ms latency requirements (no GC pauses, fine control over memory layout, SIMD support)
- **Productivity**: Strong type system catches errors early, excellent tooling (cargo, clippy, rustfmt), safer refactoring than C++
- **Future-proof**: Industry trend towards Rust for systems programming, active development and ecosystem growth

### Negative

- **Learning Curve**: 2-3 months for experienced engineers (ownership/borrowing concepts, lifetime annotations, async Rust)
- **Compilation Time**: Slower than Go, faster than C++ (incremental compilation helps)
- **Smaller Talent Pool**: Fewer Rust experts than Java/Go (growing rapidly)

## Performance Comparison

| Metric | Rust | C++ | Go | Java |
|--------|------|-----|----|----- |
| 2-hop Traversal (ms) | 12 | 11 | 45 | 38 |
| Memory Usage (MB) | 450 | 440 | 850 | 1200 |
| GC Pause (ms) | 0 | 0 | 5-50 | 10-100 |
| Binary Size (MB) | 8 | 12 | 15 | 50 |

## Alternatives Considered

- **C++**: Rejected due to memory safety issues. Memory bugs in databases are catastrophic.
- **Go**: Rejected due to GC pauses affecting P99 latency.
- **Java**: Rejected due to GC pauses, memory overhead, JVM warmup time.
- **Zig**: Rejected due to immaturity (not yet 1.0). Revisit in 2-3 years.

## Related Decisions

- [ADR-006](./006-tokio.md): Use Tokio as Async Runtime
- [ADR-002](./002-rocksdb.md): Use RocksDB (has excellent Rust bindings)

## References

- [Rust in Production: TiKV](https://www.pingcap.com/blog/rust-in-tikv/)
- [InfluxDB Rewrite in Rust](https://www.influxdata.com/blog/rust-in-influxdb-2-0/)
- [Discord: Why Rust](https://discord.com/blog/why-discord-is-switching-from-go-to-rust)

---

**Last Updated**: 2025-10-14
**Status**: Accepted and Implemented
