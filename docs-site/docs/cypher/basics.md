---
sidebar_position: 1
title: Cypher Basics
description: Introduction to the Cypher query language for Graphmind
---

# Cypher Basics

Cypher is a declarative graph query language. It uses ASCII-art patterns to describe nodes and relationships in a property graph.

Graphmind supports ~90% of the OpenCypher specification.

## Nodes

Nodes are written in parentheses:

```cypher
()                           -- Any node
(n)                          -- Any node, bound to variable n
(n:Person)                   -- Node with label Person
(n:Person:Employee)          -- Node with multiple labels
(n {name: "Alice"})          -- Any node with property filter
(n:Person {name: "Alice"})   -- Node with label and property filter
```

Nodes can have:
- **Labels** (e.g., `:Person`, `:Company`) -- categories used for filtering and indexing. Nodes can have multiple labels (e.g., `:Person:Employee`).
- **Properties** -- key-value pairs (strings, integers, floats, booleans, datetimes, arrays, maps)

## Relationships

Relationships are written as arrows between nodes:

```cypher
(a)-[r]->(b)                -- Directed relationship from a to b
(a)<-[r]-(b)                -- Directed relationship from b to a
(a)-[r]->(b)                -- Undirected (either direction)
(a)-[r:KNOWS]->(b)          -- Relationship with type KNOWS
(a)-[r:KNOWS|WORKS_WITH]->(b) -- Multiple relationship types (OR)
(a)-[r:KNOWS {since: 2020}]->(b)  -- Relationship with properties
```

### Anonymous Patterns

You can omit the relationship variable for simpler patterns:

```cypher
(a)-->(b)                   -- Any directed relationship
(a)<--(b)                   -- Any incoming relationship
(a)--(b)                    -- Any relationship (undirected)
(a)-[:KNOWS]->(b)           -- Typed, no variable binding
```

### Named Paths

Bind an entire path to a variable:

```cypher
p = (a:Person)-[:KNOWS]->(b:Person)
```

### Variable-Length Paths

Match paths of varying length:

```cypher
(a)-[*]->(b)                -- Any length (1 or more hops)
(a)-[*2]->(b)               -- Exactly 2 hops
(a)-[*1..5]->(b)            -- Between 1 and 5 hops
(a)-[*..3]->(b)             -- Up to 3 hops
(a)-[:KNOWS*1..3]->(b)      -- 1 to 3 hops of type KNOWS
(a)-[*0..1]->(b)            -- Zero-length paths (includes self)
```

Relationships always have:
- A **direction** (source to target)
- A **type** (e.g., `KNOWS`, `WORKS_AT`)
- Optional **properties**

## MATCH and RETURN

`MATCH` finds patterns in the graph. `RETURN` specifies what to output.

```cypher
-- Find all Person nodes
MATCH (p:Person)
RETURN p.name, p.age
```

| p.name | p.age |
|--------|-------|
| Alice  | 30    |
| Bob    | 25    |

```cypher
-- Find who Alice knows
MATCH (a:Person {name: "Alice"})-[:KNOWS]->(friend)
RETURN friend.name
```

| friend.name |
|-------------|
| Bob         |
| Carol       |

## OPTIONAL MATCH

`OPTIONAL MATCH` works like `MATCH`, but returns `null` for missing patterns instead of filtering out the row:

```cypher
MATCH (p:Person)
OPTIONAL MATCH (p)-[:WORKS_AT]->(c:Company)
RETURN p.name, c.name AS company
```

If a person has no `WORKS_AT` relationship, they still appear with `null` for the company column.

## WHERE Clause

Filter results with `WHERE`:

```cypher
MATCH (p:Person)
WHERE p.age > 25
RETURN p.name, p.age
```

### Comparison Operators

| Operator | Meaning |
|----------|---------|
| `=` | Equal |
| `<>` | Not equal |
| `<`, `>`, `<=`, `>=` | Comparisons |
| `IS NULL`, `IS NOT NULL` | Null checks |

### Chained Comparisons

You can chain comparisons for range checks:

```cypher
MATCH (p:Person)
WHERE 18 < p.age < 65
RETURN p.name
```

### Logical Operators

```cypher
MATCH (p:Person)
WHERE p.age > 25 AND p.name <> "Bob"
RETURN p.name
```

| Operator | Meaning |
|----------|---------|
| `AND` | Both conditions must be true |
| `OR` | Either condition must be true |
| `NOT` | Negate a condition |
| `XOR` | Exactly one condition must be true |

### Pattern Predicates in WHERE

Test for the existence of a pattern:

```cypher
MATCH (p:Person)
WHERE (p)-[:KNOWS]->()
RETURN p.name
```

### EXISTS Subqueries

Use `EXISTS` for more complex pattern checks:

```cypher
MATCH (p:Person)
WHERE EXISTS { MATCH (p)-[:KNOWS]->(:Person {name: "Alice"}) }
RETURN p.name
```

### String Predicates

```cypher
MATCH (p:Person)
WHERE p.name STARTS WITH "Al"
RETURN p.name

MATCH (p:Person)
WHERE p.name CONTAINS "ob"
RETURN p.name

MATCH (p:Person)
WHERE p.name ENDS WITH "ce"
RETURN p.name

-- Regex match
MATCH (p:Person)
WHERE p.name =~ "A.*"
RETURN p.name
```

### List Membership

```cypher
MATCH (p:Person)
WHERE p.name IN ["Alice", "Bob", "Carol"]
RETURN p.name
```

## Aliases

Use `AS` to rename output columns:

```cypher
MATCH (p:Person)
RETURN p.name AS person_name, p.age AS person_age
```

## DISTINCT

Remove duplicate rows:

```cypher
MATCH (p:Person)-[:LIVES_IN]->(c:City)
RETURN DISTINCT c.name
```

## ORDER BY, SKIP, LIMIT

Sort and paginate results:

```cypher
MATCH (p:Person)
RETURN p.name, p.age
ORDER BY p.age DESC, p.name ASC
SKIP 5
LIMIT 10
```

- `ORDER BY` -- sort by one or more columns (default `ASC`, or specify `DESC`). Multiple columns are separated by commas.
- `SKIP` -- skip the first N rows. Accepts expressions: `SKIP toInteger(rand() * 9) + 1`
- `LIMIT` -- return at most N rows. Accepts expressions: `LIMIT toInteger($pageSize)`

## RETURN *

Return all bound variables without listing them explicitly:

```cypher
MATCH (p:Person)-[r:KNOWS]->(f:Person)
RETURN *
```

This expands to return `p`, `r`, and `f` with all their properties.

## CASE Expressions

Conditional logic in expressions:

```cypher
MATCH (p:Person)
RETURN p.name,
  CASE
    WHEN p.age < 18 THEN "minor"
    WHEN p.age < 65 THEN "adult"
    ELSE "senior"
  END AS category
```

## List Comprehension

Filter and transform list elements:

```cypher
WITH [1, 2, 3, 4, 5] AS nums
RETURN [x IN nums WHERE x > 2 | x * 10] AS result
-- [30, 40, 50]
```

## Pattern Comprehension

Extract data from patterns into a list:

```cypher
MATCH (p:Person)
RETURN p.name, [(p)-[:KNOWS]->(f) | f.name] AS friend_names
```

## Quantified Predicates

Test conditions across list elements:

```cypher
WITH [1, 2, 3, 4, 5] AS nums
RETURN all(x IN nums WHERE x > 0) AS all_positive,
       any(x IN nums WHERE x > 4) AS has_large,
       none(x IN nums WHERE x < 0) AS none_negative,
       single(x IN nums WHERE x = 3) AS exactly_one_three
```

## WITH Clause

`WITH` acts as a pipeline barrier -- it projects columns between query parts. Variables not listed in `WITH` are not available in subsequent clauses.

```cypher
MATCH (p:Person)-[:KNOWS]->(f)
WITH p, count(f) AS friend_count
WHERE friend_count > 3
RETURN p.name, friend_count
ORDER BY friend_count DESC
```

## UNWIND

Expand a list into individual rows:

```cypher
UNWIND [1, 2, 3] AS x
RETURN x
```

```cypher
MATCH (p:Person)
WITH collect(p.name) AS names
UNWIND names AS name
RETURN name
```

## UNION and UNION ALL

Combine results from multiple queries:

```cypher
MATCH (p:Person) RETURN p.name AS name
UNION
MATCH (c:Company) RETURN c.name AS name
```

`UNION` removes duplicates; `UNION ALL` keeps them.

## EXPLAIN and PROFILE

See the query execution plan without running the query:

```cypher
EXPLAIN MATCH (p:Person)-[:KNOWS]->(f)
WHERE p.age > 25
RETURN f.name
```

This shows the operator tree (NodeScan, Filter, Expand, Project) which is useful for understanding query performance.

`PROFILE` executes the query and returns operator-level timing and row counts alongside results.

## Null Handling

Graphmind uses three-valued logic. Comparing anything with `null` produces `null` (not `true` or `false`):

```cypher
-- This will NOT match nodes where age is null
MATCH (p:Person) WHERE p.age > 25 RETURN p.name

-- Explicitly check for null
MATCH (p:Person) WHERE p.age IS NOT NULL RETURN p.name
```

## Multi-Statement Queries

You can execute multiple statements in a single query. Use semicolons to separate independent statements:

```cypher
CREATE (a:Person {name: 'Alice'});
CREATE (b:Person {name: 'Bob'});
MATCH (a:Person {name: 'Alice'}), (b:Person {name: 'Bob'})
CREATE (a)-[:KNOWS]->(b)
```

Or use consecutive CREATE statements that share variables (no semicolons needed):

```cypher
CREATE (a:Person {name: 'Alice', age: 30})
CREATE (b:Person {name: 'Bob', age: 25})
CREATE (a)-[:KNOWS {since: 2020}]->(b)
```

The second form automatically inserts WITH clauses to carry variables between CREATE statements.

This works everywhere -- the UI editor, REST API, RESP protocol, and all SDKs. Semicolons inside quoted strings are handled correctly:

```cypher
CREATE (n:Note {text: 'Use semicolons; they work!'});
CREATE (m:Note {text: 'Another note'})
```

## Schema Commands

### Create Index

Speed up property lookups:

```cypher
-- Single property index
CREATE INDEX ON :Person(name)

-- Composite index
CREATE INDEX ON :Person(name, age)

-- Drop an index
DROP INDEX ON :Person(name)
```

### Create Constraint

Enforce uniqueness:

```cypher
CREATE CONSTRAINT ON (p:Person) ASSERT p.email IS UNIQUE
```

### Create Vector Index

For similarity search:

```cypher
CREATE VECTOR INDEX person_embed
  FOR (n:Person) ON (n.embedding)
  OPTIONS {dimensions: 128, similarity: 'cosine'}
```

### Schema Introspection

```cypher
-- List all indexes
SHOW INDEXES

-- List all constraints
SHOW CONSTRAINTS

-- List all node labels
SHOW LABELS

-- List all relationship types
SHOW RELATIONSHIP TYPES

-- List all property keys
SHOW PROPERTY KEYS

-- Visualize schema as a graph
CALL db.schema.visualization()
```

See [Indexes & Constraints](indexes) for full details.

## Type Coercion

Graphmind automatically coerces types in comparisons:
- Integer and Float values are promoted for comparison (`30 = 30.0` is true)
- String `"true"` matches Boolean `true` (useful for LLM-generated queries)
- Null propagates through expressions (`null + 1` is `null`)
