---
sidebar_position: 12
title: "ADR-012: Late Materialization"
---

# ADR-012: Late Materialization with NodeRef/EdgeRef

## Status
**Accepted**

## Date
2025-12-15

## Context

Query execution was cloning full `Node` and `Edge` objects at scan time, causing excessive memory allocation for large graphs. A query like `MATCH (n:Person) WHERE n.age > 30 RETURN n.name` would clone every Person node (including all properties), filter most out, then only use the `name` property.

For a graph with 1M Person nodes each carrying 10 properties, this meant cloning ~1M full node objects just to return a single property from the ~10K that pass the filter.

## Decision

**We will use late materialization with reference-based values throughout the query pipeline.**

### Reference Types

```rust
// Before: Full clone at scan time
Value::Node(NodeId, Node)        // Clones entire node

// After: Reference only
Value::NodeRef(NodeId)           // 8 bytes
Value::EdgeRef(EdgeId, NodeId, NodeId, EdgeType)  // ~40 bytes
```

### Lazy Property Resolution

Properties are resolved on demand via `resolve_property()`:

| Operator | Materializes? |
|----------|---------------|
| `NodeScanOperator` | No -- produces `NodeRef(id)` |
| `ExpandOperator` | No -- produces `EdgeRef(...)` |
| `FilterOperator` | No -- pass-through |
| `JoinOperator` | No |
| `ProjectOperator` (RETURN n) | Yes -- materializes full node |
| `ProjectOperator` (RETURN n.name) | No -- uses `resolve_property` |

### Identity-Based Equality

`PartialEq` and `Hash` compare by ID only:
```rust
Value::NodeRef(42) == Value::Node(42, any_node)  // true
```

## Consequences

### Positive

- Reduced memory allocation by ~60% for typical queries
- O(1) property access via store lookup
- Efficient `JoinOperator` with `HashSet`-based matching
- `LIMIT` queries benefit most -- only N nodes ever materialized
- Pipeline stays lazy end-to-end (consistent with Volcano model)

### Negative

- Tests must use `resolve_property(prop, store)` instead of direct property access
- Store reference must be threaded through operators
- Debugging slightly harder (NodeRef values don't show property data)

## Related Decisions

- [ADR-007](./007-volcano.md): Late materialization preserves the lazy pull-based model
- [ADR-001](./001-rust.md): Rust's ownership model makes reference threading safe

---

**Last Updated**: 2025-12-15
**Status**: Accepted and Implemented
