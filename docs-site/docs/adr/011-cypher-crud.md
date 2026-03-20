---
sidebar_position: 11
title: "ADR-011: Cypher CRUD Operations"
---

# ADR-011: Implement Cypher CRUD Operations (DELETE, SET, REMOVE)

## Status
**Proposed**

## Date
2025-12-27

## Context

Graphmind's OpenCypher query engine currently supports read operations and node/edge creation, but lacks essential mutation operations:

| Operation | Neo4j | Graphmind (Before) | Gap |
|-----------|-------|-------------------|-----|
| `DELETE` | Yes | No | Critical |
| `DETACH DELETE` | Yes | No | Critical |
| `SET` properties | Yes | No | Critical |
| `REMOVE` properties | Yes | No | Important |
| `MERGE` | Yes | No | Future |

## Decision

**We will implement essential CRUD operations in three phases:**

### Phase 1: DELETE Operations
```cypher
DELETE n                -- Strict delete (fails if node has edges)
DETACH DELETE n         -- Cascade delete (removes connected edges)
```

### Phase 2: SET Operations
```cypher
SET n.prop = value      -- Individual property
SET n = {props}         -- Replace all properties
SET n += {props}        -- Merge properties
```

### Phase 3: REMOVE Operations
```cypher
REMOVE n.prop1, n.prop2 -- Remove properties
```

### Architecture

We extend the existing Volcano iterator model (ADR-007) with `MutQueryExecutor` that takes `&mut GraphStore`. Execution order follows Neo4j clause ordering:
```
MATCH -> WHERE -> SET -> REMOVE -> DELETE -> CREATE -> RETURN
```

All mutations are logged to WAL before applying.

## Consequences

### Positive

- Complete CRUD capability
- Neo4j-compatible Cypher semantics
- Full mutations via RESP protocol
- Durability via WAL

### Negative

- ~1,300 new LOC across grammar, parser, operators
- ~125 new tests needed
- Mutation operators require mutable store access

## Related Decisions

- [ADR-007](./007-volcano.md): Execution model extended for mutations
- [ADR-002](./002-rocksdb.md): Persistence layer for mutation durability

---

**Last Updated**: 2025-12-27
**Status**: Proposed
