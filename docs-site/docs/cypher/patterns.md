---
sidebar_position: 3
title: Pattern Matching
description: Multi-hop patterns, variable-length paths, OPTIONAL MATCH, WITH, UNWIND, and UNION
---

# Pattern Matching

Cypher's power comes from pattern matching. This page covers patterns beyond single-hop relationships.

## Single-Hop Patterns

```cypher
-- Outgoing relationship
MATCH (a:Person)-[:KNOWS]->(b:Person)
RETURN a.name, b.name

-- Incoming relationship
MATCH (a:Person)<-[:KNOWS]-(b:Person)
RETURN a.name, b.name

-- Any direction (undirected match)
MATCH (a:Person)-[:KNOWS]-(b:Person)
RETURN a.name, b.name
```

## Multi-Hop Patterns

Chain relationships to traverse multiple hops:

```cypher
-- Friends of friends (2 hops)
MATCH (a:Person {name: "Alice"})-[:KNOWS]->(b)-[:KNOWS]->(c)
WHERE a <> c
RETURN DISTINCT c.name AS friend_of_friend
```

```cypher
-- 3-hop chain across different relationship types
MATCH (p:Person)-[:WORKS_AT]->(c:Company)-[:LOCATED_IN]->(city:City)
RETURN p.name, c.name, city.name
```

## Variable-Length Paths

Match paths of variable length using `*min..max`:

```cypher
-- 1 to 3 hops along KNOWS edges
MATCH (a:Person {name: "Alice"})-[:KNOWS*1..3]->(b:Person)
RETURN DISTINCT b.name
```

```cypher
-- Exactly 2 hops
MATCH (a:Person)-[:KNOWS*2]->(b:Person)
RETURN a.name, b.name

-- Any number of hops (no upper bound)
MATCH (a:Person {name: "Alice"})-[:KNOWS*1..]->(b:Person)
RETURN DISTINCT b.name
```

## OPTIONAL MATCH

Like a SQL LEFT JOIN. Returns `null` for parts of the pattern that do not match:

```cypher
-- Return all people, even those with no company
MATCH (p:Person)
OPTIONAL MATCH (p)-[:WORKS_AT]->(c:Company)
RETURN p.name, c.name AS company
```

| p.name | company    |
|--------|------------|
| Alice  | Acme Corp  |
| Bob    | null       |
| Carol  | DataStream |

## Multiple MATCH Clauses

Use separate `MATCH` clauses to combine independent patterns:

```cypher
MATCH (p:Person {name: "Alice"})
MATCH (c:Company {name: "Acme Corp"})
RETURN p.name, c.name
```

This is a cross product unless the patterns share variables:

```cypher
MATCH (p:Person)-[:WORKS_AT]->(c:Company)
MATCH (p)-[:LIVES_IN]->(city:City)
RETURN p.name, c.name, city.name
```

## WITH Clause

`WITH` is a projection barrier. It pipes results from one part of a query to the next, like a subquery:

```cypher
-- Find people with more than 5 friends
MATCH (p:Person)-[:KNOWS]->(f)
WITH p, count(f) AS friend_count
WHERE friend_count > 5
RETURN p.name, friend_count
ORDER BY friend_count DESC
```

```cypher
-- Chain aggregations
MATCH (p:Person)-[:WORKS_AT]->(c:Company)
WITH c.name AS company, count(p) AS employees
WHERE employees > 10
RETURN company, employees
ORDER BY employees DESC
```

Key points:
- Only variables listed in `WITH` are visible to subsequent clauses
- Aggregations in `WITH` work the same as in `RETURN`
- `WHERE` after `WITH` filters the piped results

## UNWIND

Expand a list into individual rows:

```cypher
-- Create multiple nodes from a list
UNWIND ["Alice", "Bob", "Carol"] AS name
CREATE (p:Person {name: name})
```

```cypher
-- Use UNWIND with MATCH
UNWIND [1, 2, 3] AS level
MATCH (p:Person)
WHERE p.age > level * 10
RETURN level, count(p)
```

## UNION and UNION ALL

Combine results from multiple queries:

```cypher
-- UNION removes duplicates
MATCH (p:Person) WHERE p.age > 30 RETURN p.name AS name
UNION
MATCH (p:Person)-[:WORKS_AT]->(:Company {name: "Acme Corp"}) RETURN p.name AS name
```

```cypher
-- UNION ALL keeps duplicates
MATCH (p:Person) WHERE p.age > 30 RETURN p.name AS name
UNION ALL
MATCH (p:Person)-[:WORKS_AT]->(:Company {name: "Acme Corp"}) RETURN p.name AS name
```

Both queries must return the same column names.

## EXISTS Subqueries

Check if a pattern exists in a `WHERE` clause:

```cypher
-- Find people who know someone at Acme Corp
MATCH (p:Person)
WHERE EXISTS {
  MATCH (p)-[:KNOWS]->(f)-[:WORKS_AT]->(:Company {name: "Acme Corp"})
}
RETURN p.name
```

## CASE Expressions

Conditional logic within queries:

```cypher
-- Simple CASE
MATCH (p:Person)
RETURN p.name,
  CASE
    WHEN p.age < 25 THEN "young"
    WHEN p.age < 35 THEN "mid"
    ELSE "senior"
  END AS category
```
