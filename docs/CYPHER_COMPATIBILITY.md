# Cypher Compatibility Matrix

**Last Updated:** 2026-03-20
**Version:** Graphmind v0.6.4

This document tracks the compatibility of Graphmind's OpenCypher implementation against the industry standard (Neo4j) and modern competitors (FalkorDB).

## Summary

Graphmind provides **~90% OpenCypher coverage** with pattern matching, CRUD operations, aggregations, subqueries, and extensive function support. Features unique to Graphmind include native vector search, graph algorithms, and optimization solvers accessible via Cypher.

- **Supported:** MATCH, OPTIONAL MATCH, CREATE, DELETE, SET, REMOVE, MERGE (with ON CREATE/ON MATCH SET), WITH, UNWIND, UNION/UNION ALL, RETURN DISTINCT, ORDER BY, SKIP, LIMIT, EXPLAIN, EXISTS subqueries, aggregations (COUNT/SUM/AVG/MIN/MAX/COLLECT), 30+ built-in functions, cross-type coercion, Null propagation.
- **Remaining gaps:** `XOR` operator, `split()` function, `nodes()`/`relationships()` path functions.

## Feature Matrix

| Feature Category | Feature | Graphmind | FalkorDB | Neo4j | Notes |
| :--- | :--- | :---: | :---: | :---: | :--- |
| **Read** | `MATCH` | ✅ | ✅ | ✅ | Single and multi-hop patterns, variable-length paths |
| | `OPTIONAL MATCH` | ✅ | ✅ | ✅ | Returns null for unmatched patterns via LeftOuterJoin |
| | `WHERE` | ✅ | ✅ | ✅ | Full predicate support with precedence |
| | `RETURN` | ✅ | ✅ | ✅ | Projections, aliases, expressions |
| | `RETURN DISTINCT` | ✅ | ✅ | ✅ | Deduplication supported |
| | `ORDER BY` | ✅ | ✅ | ✅ | ASC/DESC, multi-column |
| | `SKIP` / `LIMIT` | ✅ | ✅ | ✅ | Both supported |
| | `EXPLAIN` | ✅ | ✅ | ✅ | Query plan visualization without execution |
| **Write** | `CREATE` | ✅ | ✅ | ✅ | Nodes, edges, chained patterns with properties |
| | `DELETE` / `DETACH DELETE` | ✅ | ✅ | ✅ | Node and edge deletion |
| | `SET` | ✅ | ✅ | ✅ | Property updates, label addition |
| | `REMOVE` | ✅ | ✅ | ✅ | Property and label removal |
| | `MERGE` | ✅ | ✅ | ✅ | Upsert with ON CREATE SET / ON MATCH SET |
| **Aggregation** | `count()` | ✅ | ✅ | ✅ | Global and grouped |
| | `sum()` / `avg()` | ✅ | ✅ | ✅ | Numeric aggregation |
| | `min()` / `max()` | ✅ | ✅ | ✅ | Comparable types |
| | `collect()` | ✅ | ✅ | ✅ | List aggregation |
| | Implicit `GROUP BY` | ✅ | ✅ | ✅ | Non-aggregated return items become grouping keys |
| **Structure** | `WITH` | ✅ | ✅ | ✅ | Full projection barrier (v0.5.10) |
| | `UNWIND` | ✅ | ✅ | ✅ | List expansion |
| | `UNION` / `UNION ALL` | ✅ | ✅ | ✅ | Combining result sets |
| | `EXISTS` subquery | ✅ | ✅ | ✅ | Existence check in WHERE |
| **String Functions** | `toUpper`, `toLower` | ✅ | ✅ | ✅ | |
| | `trim`, `replace` | ✅ | ✅ | ✅ | |
| | `substring`, `left`, `right` | ✅ | ✅ | ✅ | |
| | `reverse`, `toString` | ✅ | ✅ | ✅ | |
| | `split` | ❌ | ✅ | ✅ | |
| **Numeric Functions** | `abs`, `ceil`, `floor`, `round` | ✅ | ✅ | ✅ | |
| | `sqrt`, `sign` | ✅ | ✅ | ✅ | |
| | `toInteger`, `toFloat` | ✅ | ✅ | ✅ | |
| | `rand`, `log`, `exp` | ✅ | ✅ | ✅ | |
| | `timestamp` | ✅ | ✅ | ✅ | Epoch milliseconds |
| **Collection Functions** | `size`, `length` | ✅ | ✅ | ✅ | |
| | `head`, `last`, `tail` | ✅ | ✅ | ✅ | |
| | `keys` | ✅ | ✅ | ✅ | |
| | `range` | ✅ | ✅ | ✅ | |
| | `nodes()`, `relationships()` | ❌ | ✅ | ✅ | Path functions |
| **Graph Functions** | `id()` | ✅ | ✅ | ✅ | |
| | `labels()`, `type()` | ✅ | ✅ | ✅ | |
| | `exists()`, `coalesce()` | ✅ | ✅ | ✅ | |
| **Expressions** | `CASE WHEN ... THEN ... END` | ✅ | ✅ | ✅ | Simple and searched forms |
| **Predicates** | `STARTS WITH`, `ENDS WITH`, `CONTAINS` | ✅ | ✅ | ✅ | |
| | `=~` (regex) | ✅ | ✅ | ✅ | |
| | `IN` (list membership) | ✅ | ✅ | ✅ | |
| | `IS NULL`, `IS NOT NULL` | ✅ | ✅ | ✅ | |
| | `AND`, `OR`, `NOT` | ✅ | ✅ | ✅ | Atomic keyword rules prevent false matches |
| | `XOR` | ❌ | ✅ | ✅ | Not implemented |
| **Type Handling** | Integer/Float coercion | ✅ | ✅ | ✅ | Automatic promotion in comparisons |
| | Null propagation | ✅ | ✅ | ✅ | Three-valued logic (Null comparisons return Null) |
| | String/Boolean coercion | ✅ | ❌ | ❌ | LLM-friendly: `prop = 'true'` matches Boolean |
| **Extensions** | `CREATE VECTOR INDEX` | ✅ | ⚠️ | ⚠️ | Native HNSW indexing |
| | `CALL db.index.vector...` | ✅ | ⚠️ | ⚠️ | Vector similarity search |
| | `algo.pageRank` | ✅ | ✅ | ✅ | Iterative ranking |
| | `algo.wcc` / `algo.scc` | ✅ | ✅ | ✅ | Connected components |
| | `algo.bfs` / `algo.dijkstra` | ✅ | ✅ | ✅ | Shortest path algorithms |
| | `algo.maxFlow` | ✅ | ❌ | ❌ | Edmonds-Karp Max Flow |
| | `algo.mst` | ✅ | ❌ | ❌ | Prim's Minimum Spanning Tree |
| | `algo.triangleCount` | ✅ | ❌ | ❌ | Topology analysis |
| | `algo.or.solve` | ✅ | ❌ | ❌ | In-database optimization (15+ solvers) |

## Multi-Statement and Schema Commands

| Feature | Graphmind | FalkorDB | Neo4j | Notes |
| :--- | :---: | :---: | :---: | :--- |
| Multi-statement queries (`;` separator) | ✅ | ✅ | ✅ | Splits on `;` respecting quoted strings, executes sequentially |
| Multi-CREATE with shared variables | ✅ | ❌ | ✅ | `CREATE (a:P) CREATE (b:P) CREATE (a)-[:R]->(b)` — auto WITH insertion |
| `SHOW LABELS` | ✅ | ❌ | ✅ | Lists all node labels in the graph |
| `SHOW RELATIONSHIP TYPES` | ✅ | ❌ | ✅ | Lists all relationship types |
| `SHOW PROPERTY KEYS` | ✅ | ❌ | ✅ | Lists all property keys |
| `CALL db.schema.visualization()` | ✅ | ❌ | ✅ | Returns schema as nodes/edges for visualization |

## Remaining Gaps

1. **`XOR` operator**: Boolean XOR not implemented.
2. **`split()` function**: String splitting not yet supported.
3. **`nodes()`, `relationships()`**: Path decomposition functions not yet supported.

## Recently Resolved (formerly listed as gaps)

- ~~**List slicing**~~: `list[0..3]`, `list[..2]`, `list[3..]`, `list[-2..]` fully supported.
- ~~**Pattern comprehensions**~~: `[(a)-[:REL]->(b) | expr]` fully supported with optional WHERE filter.
- ~~**Named paths**~~: `p = (a)-[:KNOWS]->(b)` path assignment supported (CY-04).
- ~~**`collect(DISTINCT x)`**~~: DISTINCT modifier inside `collect()` fully supported.
- ~~**`rand`, `log`, `exp`, `timestamp`**~~: All implemented.
- ~~**CASE expressions**~~: Fully supported as of v0.5.5 (simple and searched forms).
- ~~**WITH projection barrier**~~: Fully enforced as of v0.5.10.
- ~~**Multi-statement queries**~~: Supported as of v0.6.4 (semicolon splitting + multi-CREATE rewriting).
- ~~**Schema introspection**~~: `SHOW LABELS`, `SHOW RELATIONSHIP TYPES`, `SHOW PROPERTY KEYS`, and `CALL db.schema.visualization()` supported.
