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
(n:Person {name: "Alice"})   -- Node with label and property filter
```

Nodes can have:
- **Labels** (e.g., `:Person`, `:Company`) -- categories used for filtering and indexing
- **Properties** -- key-value pairs (strings, integers, floats, booleans)

## Relationships

Relationships are written as arrows between nodes:

```cypher
(a)-[r]->(b)                -- Directed relationship from a to b
(a)<-[r]-(b)                -- Directed relationship from b to a
(a)-[r:KNOWS]->(b)          -- Relationship with type KNOWS
(a)-[r:KNOWS {since: 2020}]->(b)  -- Relationship with properties
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
ORDER BY p.age DESC
SKIP 5
LIMIT 10
```

- `ORDER BY` -- sort by one or more columns (default `ASC`, or specify `DESC`)
- `SKIP` -- skip the first N rows
- `LIMIT` -- return at most N rows

## EXPLAIN

See the query execution plan without running the query:

```cypher
EXPLAIN MATCH (p:Person)-[:KNOWS]->(f)
WHERE p.age > 25
RETURN f.name
```

This shows the operator tree (NodeScan, Filter, Expand, Project) which is useful for understanding query performance.

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

## Type Coercion

Graphmind automatically coerces types in comparisons:
- Integer and Float values are promoted for comparison (`30 = 30.0` is true)
- String `"true"` matches Boolean `true` (useful for LLM-generated queries)
- Null propagates through expressions (`null + 1` is `null`)
