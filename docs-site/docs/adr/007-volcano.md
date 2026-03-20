---
sidebar_position: 7
title: "ADR-007: Volcano Iterator Model"
---

# ADR-007: Use Volcano Iterator Model for Query Execution

## Status
**Accepted**

## Date
2025-10-14

## Context

We need a query execution model for OpenCypher queries that is composable, efficient, debuggable, and well-understood.

## Decision

**We will use the Volcano Iterator Model (also called "Pipeline Model") for query execution.**

### Iterator Protocol

```rust
trait PhysicalOperator {
    fn next(&mut self) -> Option<Record>;
    fn reset(&mut self);
}
```

Operators chain together:
```
Project -> Filter -> Expand -> Scan
```

Each operator pulls one record at a time from its child via `next()`.

## Rationale

- **Lazy Evaluation**: Process one row at a time, no need to materialize entire result set
- **Composability**: Chain operators like building blocks
- **Pipelining**: First result returned quickly (good for LIMIT queries)
- **Standard Pattern**: Used by PostgreSQL, MySQL, SQL Server

## Consequences

### Positive

- **Easy Optimization**: Operator reordering, filter pushdown
- **Memory Efficient**: O(1) memory per operator
- **Debuggable**: Easy to reason about execution flow

### Negative

- **Not Ideal for Joins**: Nested loop joins can be slow (mitigated by hash joins in Phase 2)

## Alternatives Considered

- **Vectorized Execution**: Process batches (used by ClickHouse) -- better for analytics, overkill for OLTP
- **Compilation (JIT)**: Compile query to machine code -- complex, months of work

---

**Last Updated**: 2025-10-14
**Status**: Accepted and Implemented
