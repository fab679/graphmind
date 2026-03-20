---
sidebar_position: 3
title: Cypher Compatibility
description: OpenCypher query language support
---

# Cypher Compatibility Matrix

**Last Updated:** 2026-03-04
**Version:** Graphmind v0.5.12

This document tracks the compatibility of Graphmind's OpenCypher implementation against the industry standard (Neo4j) and modern competitors (FalkorDB).

## Summary

Graphmind provides **~90% OpenCypher coverage** with pattern matching, CRUD operations, aggregations, subqueries, and extensive function support. Features unique to Graphmind include native vector search, graph algorithms, and optimization solvers accessible via Cypher.

- **Supported:** MATCH, OPTIONAL MATCH, CREATE, DELETE, SET, REMOVE, MERGE (with ON CREATE/ON MATCH SET), WITH, UNWIND, UNION/UNION ALL, RETURN DISTINCT, ORDER BY, SKIP, LIMIT, EXPLAIN, EXISTS subqueries, aggregations (COUNT/SUM/AVG/MIN/MAX/COLLECT), 30+ built-in functions, cross-type coercion, Null propagation.
- **Remaining gaps:** list slicing, pattern comprehensions, named paths, `collect(DISTINCT x)`.

## Feature Matrix

| Feature Category | Feature | Graphmind | FalkorDB | Neo4j | Notes |
| :--- | :--- | :---: | :---: | :---: | :--- |
| **Read** | `MATCH` | Supported | Supported | Supported | Single and multi-hop patterns, variable-length paths |
| | `OPTIONAL MATCH` | Supported | Supported | Supported | Returns null for unmatched patterns via LeftOuterJoin |
| | `WHERE` | Supported | Supported | Supported | Full predicate support with precedence |
| | `RETURN` | Supported | Supported | Supported | Projections, aliases, expressions |
| | `RETURN DISTINCT` | Supported | Supported | Supported | Deduplication supported |
| | `ORDER BY` | Supported | Supported | Supported | ASC/DESC, multi-column |
| | `SKIP` / `LIMIT` | Supported | Supported | Supported | Both supported |
| | `EXPLAIN` | Supported | Supported | Supported | Query plan visualization without execution |
| **Write** | `CREATE` | Supported | Supported | Supported | Nodes, edges, chained patterns with properties |
| | `DELETE` / `DETACH DELETE` | Supported | Supported | Supported | Node and edge deletion |
| | `SET` | Supported | Supported | Supported | Property updates, label addition |
| | `REMOVE` | Supported | Supported | Supported | Property and label removal |
| | `MERGE` | Supported | Supported | Supported | Upsert with ON CREATE SET / ON MATCH SET |
| **Aggregation** | `count()` | Supported | Supported | Supported | Global and grouped |
| | `sum()` / `avg()` | Supported | Supported | Supported | Numeric aggregation |
| | `min()` / `max()` | Supported | Supported | Supported | Comparable types |
| | `collect()` | Supported | Supported | Supported | List aggregation |
| | Implicit `GROUP BY` | Supported | Supported | Supported | Non-aggregated return items become grouping keys |
| **Structure** | `WITH` | Supported | Supported | Supported | Full projection barrier (v0.5.10) |
| | `UNWIND` | Supported | Supported | Supported | List expansion |
| | `UNION` / `UNION ALL` | Supported | Supported | Supported | Combining result sets |
| | `EXISTS` subquery | Supported | Supported | Supported | Existence check in WHERE |
| **String Functions** | `toUpper`, `toLower` | Supported | Supported | Supported | |
| | `trim`, `replace` | Supported | Supported | Supported | |
| | `substring`, `left`, `right` | Supported | Supported | Supported | |
| | `reverse`, `toString` | Supported | Supported | Supported | |
| | `split` | Not yet | Supported | Supported | |
| **Numeric Functions** | `abs`, `ceil`, `floor`, `round` | Supported | Supported | Supported | |
| | `sqrt`, `sign` | Supported | Supported | Supported | |
| | `toInteger`, `toFloat` | Supported | Supported | Supported | |
| | `rand`, `log`, `exp` | Not yet | Supported | Supported | |
| **Collection Functions** | `size`, `length` | Supported | Supported | Supported | |
| | `head`, `last`, `tail` | Supported | Supported | Supported | |
| | `keys` | Supported | Supported | Supported | |
| | `range` | Supported | Supported | Supported | |
| | `nodes()`, `relationships()` | Not yet | Supported | Supported | Path functions |
| **Graph Functions** | `id()` | Supported | Supported | Supported | |
| | `labels()`, `type()` | Supported | Supported | Supported | |
| | `exists()`, `coalesce()` | Supported | Supported | Supported | |
| **Expressions** | `CASE WHEN ... THEN ... END` | Supported | Supported | Supported | Simple and searched forms |
| **Predicates** | `STARTS WITH`, `ENDS WITH`, `CONTAINS` | Supported | Supported | Supported | |
| | `=~` (regex) | Supported | Supported | Supported | |
| | `IN` (list membership) | Supported | Supported | Supported | |
| | `IS NULL`, `IS NOT NULL` | Supported | Supported | Supported | |
| | `AND`, `OR`, `NOT`, `XOR` | Supported | Supported | Supported | Atomic keyword rules prevent false matches |
| **Type Handling** | Integer/Float coercion | Supported | Supported | Supported | Automatic promotion in comparisons |
| | Null propagation | Supported | Supported | Supported | Three-valued logic (Null comparisons return Null) |
| | String/Boolean coercion | Supported | No | No | LLM-friendly: `prop = 'true'` matches Boolean |
| **Extensions** | `CREATE VECTOR INDEX` | Supported | Partial | Partial | Native HNSW indexing |
| | `CALL db.index.vector...` | Supported | Partial | Partial | Vector similarity search |
| | `algo.pageRank` | Supported | Supported | Supported | Iterative ranking |
| | `algo.wcc` / `algo.scc` | Supported | Supported | Supported | Connected components |
| | `algo.bfs` / `algo.dijkstra` | Supported | Supported | Supported | Shortest path algorithms |
| | `algo.maxFlow` | Supported | No | No | Edmonds-Karp Max Flow |
| | `algo.mst` | Supported | No | No | Prim's Minimum Spanning Tree |
| | `algo.triangleCount` | Supported | No | No | Topology analysis |
| | `algo.or.solve` | Supported | No | No | In-database optimization (15+ solvers) |

## Remaining Gaps

1. **List slicing**: `list[0..3]` syntax not yet supported.
2. **Pattern comprehensions**: `[(a)-[:KNOWS]->(b) | b.name]` not yet supported.
3. **Named paths**: `p = (a)-[:KNOWS]->(b)` path assignment not yet supported.
4. **Some functions**: `split`, `rand`, `log`, `exp`, `nodes()`, `relationships()`, `timestamp()`.
5. **`collect(DISTINCT x)`**: DISTINCT modifier inside aggregate functions not yet supported.

## Recently Resolved (formerly listed as gaps)

- ~~**CASE expressions**~~: Fully supported as of v0.5.5 (simple and searched forms).
- ~~**WITH projection barrier**~~: Fully enforced as of v0.5.10.
