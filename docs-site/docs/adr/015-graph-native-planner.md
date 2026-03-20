---
sidebar_position: 15
title: "ADR-015: Graph-Native Query Planning"
---

# ADR-015: Graph-Native Query Planning

## Status
**Accepted**

## Date
2026-03-06

## Context

Graphmind's query planner (v0.6.0) is a greedy single-plan generator. It always starts from the leftmost node in the MATCH pattern, follows edge direction as written, and never compares alternative plans.

**This is the wrong model for a graph database.** The "join" in a graph database is the adjacency list lookup -- an inherent index-nested-loop join. The dominant cost factors are:

1. **Which node to start from** -- scanning 100 Company nodes vs 1M Person nodes is a 10,000x difference
2. **Which direction to traverse** -- a Company may have 1,100 incoming WORKS_AT edges but a Person has 1 outgoing
3. **Whether both endpoints are bound** -- checking edge existence is O(min_degree), not O(degree)

## Decision

**We will replace the greedy single-plan planner with a graph-native plan enumeration pipeline that selects the optimal starting point, traversal direction, and operator placement using triple-level statistics.**

### New Planner Architecture

```
AST -> Pattern Analyzer -> Plan Enumerator -> Cost Estimator -> Logical Optimizer -> Physical Planner -> ExecutionPlan
```

### Key Components

1. **GraphCatalog** -- triple-level statistics `(:Label, :TYPE, :Label)` with avg degrees, counts, and percentiles. Maintained incrementally.

2. **Plan Enumeration** -- for each node in the pattern, generate a candidate plan. For each edge, consider both forward and reverse traversal. Score by estimated intermediate rows.

3. **ExpandIntoOperator** -- when both endpoints are bound, check edge existence via O(min_degree) scan.

4. **Logical Plan IR** -- separating plan structure from physical operators.

5. **Feature-flagged rollout** -- old planner remains default until proven equivalent.

### Phased Implementation

| Phase | Scope | Impact |
|-------|-------|--------|
| 0: Foundation | GraphCatalog, edges_between(), LogicalPlan IR | Zero behavioral change |
| 1: Starting Point | Plan enumeration, direction reversal, cost comparison | 10-100x for asymmetric patterns |
| 2: Expand-Into | Detect bound endpoints, fuse multi-path pipelines | Eliminate hash join materialization |
| 3: Predicate Placement | Optimal filter position | Reduced intermediate cardinality |
| 4: VarLength Paths | BFS-based expansion with memoization | Better *1..N path queries |
| 5: WCOJ | Sorted adjacency lists, IntersectionOperator | Optimal triangle/clique performance |

## Rationale

### Why Triple-Level Statistics

Global statistics (`avg_out_degree = 5.0`) are useless. The same graph might have:
- `(:Person, :KNOWS, :Person)` avg out-degree = 5.0
- `(:Person, :WORKS_AT, :Company)` avg out-degree = 1.0
- `(:Company, :WORKS_AT, :Person)` avg in-degree = 1,100

### Why Plan Enumeration over DP

For graph patterns, candidate plans are manageable: `|nodes| x 2^|edges|`. For typical Cypher patterns (3-6 nodes), this is at most ~200 candidates. Full DP enumeration of join orders is overkill.

### Why Expand-Into Matters

Without Expand-Into: Scan + Expand + Filter = O(degree)
With Expand-Into: IndexScan + IndexScan + EdgeCheck = O(1)

## Consequences

### Positive

- 10-100x performance improvement for asymmetric patterns
- Planner adapts to data distribution, not just query syntax
- Foundation for WCOJ (Phase 5)
- EXPLAIN shows all candidate plans with costs
- Feature-flagged rollout eliminates regression risk

### Negative

- Increased planner complexity (5-stage pipeline)
- Memory overhead for GraphCatalog (typically < 1000 entries)
- Two code paths during transition

## Alternatives Considered

- **Relational-Style DP**: Rejected -- graph queries dominated by adjacency traversal, not arbitrary joins
- **Greedy Heuristic Improvements**: Rejected -- cannot capture traversal direction impact
- **Adaptive Query Execution**: Rejected -- too complex, can be added later as Phase 6

## Related Decisions

- [ADR-007](./007-volcano.md): New operators implement PhysicalOperator trait
- [ADR-012](./012-late-materialization.md): New operators produce Value::NodeRef/EdgeRef
- [ADR-014](./014-explain-profile.md): EXPLAIN extended to show candidate plans

---

**Last Updated**: 2026-03-06
**Status**: Accepted
