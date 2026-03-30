# Graphmind Bug Fixes

This document describes database bugs that were identified and fixed in the query engine.

---

## Issue 0 (CRITICAL): MERGE does not update property indexes

**Status:** Fixed

### Problem

When a property index exists on a label (e.g., `CREATE INDEX FOR (p:Project) ON (p.name)`), nodes created via `MERGE` were not added to the index. Subsequent `MATCH` queries using property filters returned zero results, even though the node existed.

```cypher
CREATE INDEX idx_project_name IF NOT EXISTS FOR (p:Project) ON (p.name)
MERGE (p:Project {name: 'my-project'})
MATCH (p:Project {name: 'my-project'}) RETURN p.name
-- Returned: [] (EMPTY)
```

Nodes created via `CREATE` were findable; nodes created via `MERGE` were not.

### Root Cause

Both `MergeOperator` and `PerRowMergeOperator` in `src/query/executor/operator.rs` used `node.set_property()` (direct struct mutation on the `Node`) instead of `store.set_node_property()`. The store method triggers index events (property index updates, vector index updates, constraint checks) and columnar storage updates. The direct mutation bypassed all of this.

The same issue affected:
- Initial property setting during node creation
- `ON CREATE SET` properties
- `ON MATCH SET` properties
- Target node creation in relationship segments

### Fix

Replaced all `node.set_property()` calls with `store.set_node_property()` and `node.labels.insert()` with `store.add_label_to_node()` in both `MergeOperator` and `PerRowMergeOperator`, matching the pattern used by `CreateNodeOperator`.

**Files changed:** `src/query/executor/operator.rs`

### Tests Added

- `test_merge_updates_property_index` — MERGE-created nodes are findable via property index
- `test_merge_on_create_set_updates_index` — ON CREATE SET properties are indexed
- `test_merge_on_match_set_updates_index` — ON MATCH SET property changes update the index

---

## Issue 1 & 2: `WITH collect() ... WITH carry_forward + collect()` causes "Ambiguous aggregation expression"

**Status:** Fixed

### Problem

When using `WITH` to collect results and then referencing that collected variable in a subsequent `WITH` clause alongside new aggregate functions, the planner rejected the query:

```cypher
MATCH (a:Person {team: 'engineering'})
WITH collect(a) AS engineers
MATCH (b:Person {team: 'design'})
WITH engineers + collect(b) AS allPeople    -- ERROR
UNWIND allPeople AS person
RETURN person.name
```

Error: `Planning error: Ambiguous aggregation expression: 'engineers' is not a grouping key`

### Root Cause

The `validate_aggregation_mixing()` function in `src/query/executor/planner.rs` only recognized grouping keys from standalone non-aggregate WITH items. Carry-forward variables (like `engineers` in `WITH engineers + collect(b) AS allPeople`) appeared inside aggregate-containing expressions but weren't recognized as valid. The validator treated them as ambiguous because they were neither explicit grouping keys nor aggregate function calls.

### Fix

Added logic to scan aggregate-containing expressions for variable references that appear *outside* aggregate function calls, and treat them as implicit grouping keys. These are carry-forward variables from prior WITH scope that act as constants in the current aggregation context. This matches the OpenCypher specification and Neo4j behavior.

Also extended the validation to cover `extra_with_stages` (multi-WITH queries) which were previously unvalidated.

**Files changed:** `src/query/executor/planner.rs`

### Tests Added

- `test_with_carry_forward_plus_collect` — Validates the planner accepts carry-forward variables alongside aggregation

---

## Issue 3: `MATCH` + `MERGE` in the same query causes "Variable already declared"

**Status:** Fixed

### Problem

When a query uses `MATCH` to find an existing node and then `MERGE` to introduce a new node, the planner incorrectly rejected it:

```cypher
MATCH (t:DecisionTrace) WHERE id(t) = $traceId
MERGE (p:Project {name: $project})
ON CREATE SET p.createdAt = $createdAt
CREATE (t)-[:BELONGS_TO_PROJECT]->(p)
```

Error: `Planning error: Variable 'p' already declared`

### Root Cause

The MERGE re-binding validation in `src/query/executor/planner.rs` collected `bound_node_vars` from MATCH clauses **and** CREATE clauses, then checked MERGE patterns against them. When `CREATE (t)-[:BELONGS_TO_PROJECT]->(p)` referenced variable `p` (which MERGE introduces), the validator saw `p` as "already declared" before the MERGE check.

The problem was that CREATE clauses reference variables that MERGE defines — including CREATE variables in the bound set was incorrect because CREATE comes *after* MERGE in the execution pipeline.

### Fix

Changed the MERGE re-binding validation to only collect bound variables from MATCH and WITH clauses (prior scope), not from CREATE clauses. MERGE introduces new variables that CREATE may subsequently reference — this is valid OpenCypher.

**Files changed:** `src/query/executor/planner.rs`

### Tests Added

- `test_match_then_merge_different_variables` — Validates MATCH + MERGE with different variables is accepted

---

## Issue 4: `$params` not supported in UNION ALL queries

**Status:** Fixed

### Problem

Query parameters (`$name`, `$agentName`, etc.) were not resolved in UNION ALL subqueries:

```cypher
MATCH (a:Agent) WHERE a.name = $name RETURN a.name
UNION ALL
MATCH (p:Project) WHERE p.name = $name RETURN p.name
```

With params `{ name: "code-reviewer" }`, the second subquery failed with: `Runtime error: Unresolved parameter: $name`

### Root Cause

The `substitute_params()` function in `src/query/executor/mod.rs` handled MATCH, CREATE, MERGE, WHERE, RETURN, WITH, ORDER BY, SET, UNWIND, and multi-part stages — but did not recurse into `query.union_queries`. Each UNION subquery is a separate `Query` struct that also needs parameter substitution.

### Fix

Added a recursive `substitute_params()` call for each subquery in `query.union_queries` before processing multi-part stages.

**Files changed:** `src/query/executor/mod.rs`

### Tests Added

- `test_union_all_with_params` — Parameters resolve correctly across UNION ALL boundaries

---

## Issue 6 (CRITICAL): Vector indexes never populated — SEARCH returns 0 results

**Status:** Fixed

### Problem

Vector indexes created via `CREATE VECTOR INDEX` were never populated with data. `SHOW VECTOR INDEXES` always showed 0 vectors, and `SEARCH ... IN (VECTOR INDEX ...)` always returned empty results, even when nodes had valid embedding properties.

### Root Cause

Two separate paths were broken:

1. **JSON parameter path**: When embedding vectors were sent via the SDK/API as JSON parameters (e.g., `{embedding: [0.1, 0.2, ...]}`), the `json_params_to_property_values()` function in `src/http/handler.rs` converted all-numeric arrays to `PropertyValue::Array` instead of `PropertyValue::Vector`. The index event handlers only checked for `PropertyValue::Vector`, so the data was never indexed.

2. **Array fallback**: Even for data stored as `PropertyValue::Array` (which can happen through various code paths), all four index event handlers (`NodeCreated`, `PropertySet`, `LabelAdded` — in both synchronous and background indexer) only checked `PropertyValue::Vector`, missing the `PropertyValue::Array` variant.

### Fix

- **`json_params_to_property_values()`**: All-numeric JSON arrays are now converted to `PropertyValue::Vector(Vec<f32>)` instead of `PropertyValue::Array`.
- **All 8 index event handlers**: Added `PropertyValue::Array` matching as a fallback — numeric arrays are converted to `Vec<f32>` and indexed just like `PropertyValue::Vector`.

**Files changed:** `src/http/handler.rs`, `src/graph/store.rs`

### Tests Added

- `test_vector_index_populated_after_create_node` — Core engine: vector index populated after CREATE
- `test_vector_index_populated_via_http_flow` — HTTP layer: vector index populated across separate lock acquisitions

---

## Issue 7: `DROP VECTOR INDEX` classified as read-only query

**Status:** Fixed

### Problem

`DROP VECTOR INDEX` and `DROP INDEX` were routed to the read-only query executor, causing them to fail with "Cannot execute write query with read-only executor."

### Root Cause

The write-query classifier in `query_handler` checked for `CREATE`, `DELETE`, `SET`, `MERGE`, `REMOVE`, `CALL` keywords but not `DROP`. Queries starting with `DROP` were classified as read-only.

### Fix

Added `DROP` to the write-keyword list: `trimmed_upper.starts_with("DROP")`, `trimmed_upper.contains(" DROP ")`, and `trimmed_upper.contains("\nDROP ")`.

**Files changed:** `src/http/handler.rs`

---

## Issue 5 (UI): Legend panel clips buttons when label names are long

**Status:** Fixed

### Problem

In the Legend panel, long node label names (e.g. `DecisionTrace`, `ClinicalTrialOutcome`) pushed the Color/Icon/Image mode buttons off the right edge of the fixed-width panel, making them inaccessible.

### Fix

- Changed the panel from fixed `width: 280` to `minWidth: 280; maxWidth: 420; width: auto` so it grows with content up to a reasonable limit.
- Added `overflow: hidden; textOverflow: ellipsis; whiteSpace: nowrap` to label text so long names truncate with ellipsis instead of overflowing.
- Made the mode button group `flexShrink: 0` so it never gets compressed.

**Files changed:** `ui/src/components/graph/LegendPanel.tsx`

---

## Issue 6 (UI): Nodes without relationships stack together instead of spreading apart

**Status:** Fixed

### Problem

When nodes are loaded without relationships (e.g., `MATCH (n) RETURN n` on a disconnected graph), all nodes pile up in the center of the canvas instead of spreading apart into a readable layout.

### Root Cause

The D3 force simulation's charge strength and collide radius were tuned for graphs with link forces pulling nodes into structure. Without links, the weak repulsion (`-200`) combined with `forceCenter` pulling everything to the middle resulted in nodes settling into a dense cluster with only minimal `radius + 4` collision avoidance.

### Fix

Detect sparse/disconnected graphs (link-to-node ratio < 0.5) and boost the repulsion strength (doubled) and collision radius (from `radius + 4` to `radius + 30`) so isolated nodes spread into a readable arrangement.

**Files changed:** `ui/src/components/graph/ForceGraph.tsx`

---

## Issue 7 (UI): Connected section in property inspector can grow out of hand

**Status:** Fixed

### Problem

The "Connected" section in the property inspector listed up to 20 connected edges for the selected node. For highly connected hub nodes, this dominated the inspector panel and pushed the more useful Properties and Stats sections out of view.

### Fix

Removed the "Connected" section entirely. The degree count is still shown in the Stats section. Connected edges can be explored via the graph canvas (right-click > expand neighbors).

**Files changed:** `ui/src/components/inspector/PropertyInspector.tsx`
