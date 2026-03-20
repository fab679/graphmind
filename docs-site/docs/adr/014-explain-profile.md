---
sidebar_position: 14
title: "ADR-014: EXPLAIN and PROFILE"
---

# ADR-014: EXPLAIN and PROFILE Query Plan Visualization

## Status
**Accepted**

## Date
2026-02-16

## Context

As Graphmind's query engine grows in complexity, users need visibility into how their queries are executed. Without this, query optimization is a guessing game.

## Decision

**We will add EXPLAIN and PROFILE query prefixes that display the execution plan as a formatted operator tree.**

### EXPLAIN: Plan Without Execution

```cypher
EXPLAIN MATCH (n:Person)-[:KNOWS]->(m:Person) WHERE n.age > 30 RETURN m.name
```

Output:
```
+----------------------------------+----------------+
| Operator                         | Estimated Rows |
+----------------------------------+----------------+
| ProjectOperator (m.name)         |             50 |
|   FilterOperator (n.age > 30)    |             50 |
|     ExpandOperator (-[:KNOWS]->) |            500 |
|       NodeScanOperator (:Person) |            100 |
+----------------------------------+----------------+
```

### PROFILE: Plan With Runtime Statistics

Executes the query and collects per-operator statistics (actual rows, elapsed time).

### Profiling Wrapper Operator

For PROFILE, each operator is wrapped in a `ProfileOperator` that collects timing and row counts with minimal overhead (~5-10% for small queries).

### Row Estimation

| Operator | Estimation Rule |
|----------|----------------|
| `NodeScanOperator` | Count of nodes with matching label |
| `ExpandOperator` | Input rows * average edge degree |
| `FilterOperator` | Input rows * default selectivity (0.5) |
| `ProjectOperator` | Same as input rows |
| `LimitOperator` | min(input estimate, limit value) |

## Consequences

### Positive

- Users can inspect execution plans before running expensive queries
- Per-operator timing enables precise bottleneck identification
- Foundation for a future cost-based optimizer
- EXPLAIN has zero execution cost -- safe for production
- Consistent with Neo4j/Memgraph conventions

### Negative

- PROFILE adds timing overhead per operator
- Row estimation heuristics may be inaccurate without statistics collection

## Alternatives Considered

- **Logging-Only**: Not interactive, cannot be used from RESP clients
- **Graphical Visualization**: Too complex for current phase
- **Separate PLAN Command**: Inconsistent with Neo4j/Memgraph conventions

## Related Decisions

- [ADR-007](./007-volcano.md): EXPLAIN/PROFILE visualizes the Volcano operator tree
- [ADR-012](./012-late-materialization.md): Profile stats show materialization costs
- [ADR-013](./013-peg-grammar.md): EXPLAIN/PROFILE keywords use atomic rules

---

**Last Updated**: 2026-02-16
**Status**: Accepted
