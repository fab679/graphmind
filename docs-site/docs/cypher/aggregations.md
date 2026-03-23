---
sidebar_position: 4
title: Aggregations
description: Counting, grouping, collecting, and pagination
---

# Aggregations

Graphmind supports standard aggregation functions. Non-aggregated columns in `RETURN` become implicit grouping keys (like SQL `GROUP BY`).

## Aggregate Functions

### count()

```cypher
-- Count all nodes
MATCH (n) RETURN count(n) AS total

-- Count by label
MATCH (n) RETURN labels(n) AS label, count(n) AS total
ORDER BY total DESC
```

| label      | total |
|------------|-------|
| [Person]   | 200   |
| [Post]     | 2000  |
| [Comment]  | 3000  |

### sum() and avg()

```cypher
MATCH (p:Person)
RETURN sum(p.age) AS total_age, avg(p.age) AS average_age
```

### min() and max()

```cypher
MATCH (p:Person)
RETURN min(p.age) AS youngest, max(p.age) AS oldest
```

### collect()

Aggregate values into a list:

```cypher
MATCH (p:Person)-[:WORKS_AT]->(c:Company {name: "Acme Corp"})
RETURN collect(p.name) AS employees
```

| employees                       |
|---------------------------------|
| ["Alice", "Bob", "Carol", ...] |

## Implicit GROUP BY

When you mix aggregated and non-aggregated expressions in `RETURN`, the non-aggregated ones become grouping keys:

```cypher
-- Group by company, count employees per company
MATCH (p:Person)-[:WORKS_AT]->(c:Company)
RETURN c.name AS company, count(p) AS headcount, avg(p.age) AS avg_age
ORDER BY headcount DESC
```

| company    | headcount | avg_age |
|------------|-----------|---------|
| Acme Corp  | 42        | 31.5    |
| GlobalBank | 38        | 29.8    |
| DataStream | 12        | 27.3    |

```cypher
-- Group by city, find the oldest person per city
MATCH (p:Person)-[:LIVES_IN]->(c:City)
RETURN c.name AS city, max(p.age) AS max_age, count(p) AS population
ORDER BY population DESC
LIMIT 5
```

## DISTINCT

Remove duplicate rows from results:

```cypher
-- Unique cities where people live
MATCH (p:Person)-[:LIVES_IN]->(c:City)
RETURN DISTINCT c.name
```

DISTINCT applies to the entire row, not individual columns.

## Combining Aggregations with WITH

Use `WITH` to aggregate first, then filter:

```cypher
-- Companies with more than 10 employees
MATCH (p:Person)-[:WORKS_AT]->(c:Company)
WITH c.name AS company, count(p) AS employees
WHERE employees > 10
RETURN company, employees
ORDER BY employees DESC
```

```cypher
-- Most connected people (top 10 by friend count)
MATCH (p:Person)-[:KNOWS]->(f)
WITH p.name AS person, count(f) AS friends
ORDER BY friends DESC
LIMIT 10
RETURN person, friends
```

## ORDER BY

Sort results by one or more columns:

```cypher
-- Ascending (default)
MATCH (p:Person) RETURN p.name, p.age ORDER BY p.age

-- Descending
MATCH (p:Person) RETURN p.name, p.age ORDER BY p.age DESC

-- Multiple columns
MATCH (p:Person)-[:LIVES_IN]->(c:City)
RETURN c.name, p.name, p.age
ORDER BY c.name ASC, p.age DESC
```

## SKIP and LIMIT

Paginate through results:

```cypher
-- First page (10 results)
MATCH (p:Person) RETURN p.name ORDER BY p.name LIMIT 10

-- Second page
MATCH (p:Person) RETURN p.name ORDER BY p.name SKIP 10 LIMIT 10

-- Third page
MATCH (p:Person) RETURN p.name ORDER BY p.name SKIP 20 LIMIT 10
```

## Counting Relationships

```cypher
-- Degree of each node (number of outgoing KNOWS edges)
MATCH (p:Person)-[r:KNOWS]->()
RETURN p.name, count(r) AS out_degree
ORDER BY out_degree DESC
LIMIT 5
```

| p.name | out_degree |
|--------|------------|
| Alice  | 8          |
| Bob    | 7          |
| Carol  | 6          |
