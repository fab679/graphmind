---
sidebar_position: 6
title: Indexes & Constraints
description: Create indexes and constraints for performance and data integrity
---

# Indexes & Constraints

## Property Indexes

Indexes speed up `MATCH` queries that filter on specific properties.

### Create Index

Both modern (Neo4j 4.x/5.x) and classic (openCypher 9) syntax are supported:

```cypher
-- Modern syntax (recommended)
CREATE INDEX person_name IF NOT EXISTS FOR (p:Person) ON (p.name)

-- Classic syntax
CREATE INDEX ON :Person(name)
```

After creating this index, queries like `MATCH (p:Person {name: 'Alice'})` will use the index instead of scanning all Person nodes.

### Composite Index

Index multiple properties together:

```cypher
-- Modern syntax
CREATE INDEX person_name_age IF NOT EXISTS FOR (p:Person) ON (p.name, p.age)

-- Classic syntax
CREATE INDEX ON :Person(name, age)
```

### Drop Index

```cypher
-- Modern syntax (by name)
DROP INDEX person_name IF EXISTS

-- Classic syntax
DROP INDEX ON :Person(name)
```

### Show Indexes

```cypher
SHOW INDEXES
```

## Unique Constraints

Constraints enforce data rules at the database level.

### Create Unique Constraint

```cypher
-- Modern syntax (recommended)
CREATE CONSTRAINT person_email IF NOT EXISTS FOR (p:Person) REQUIRE p.email IS UNIQUE

-- Classic syntax
CREATE CONSTRAINT ON (p:Person) ASSERT p.email IS UNIQUE
```

After this, attempting to create two Person nodes with the same email will fail.

### Create NOT NULL Constraint

```cypher
CREATE CONSTRAINT person_name_required IF NOT EXISTS FOR (p:Person) REQUIRE p.name IS NOT NULL
```

### Drop Constraint

```cypher
DROP CONSTRAINT person_email IF EXISTS
```

### Show Constraints

```cypher
SHOW CONSTRAINTS
```

## Vector Indexes

For similarity search on embedding vectors.

### Create Vector Index

```cypher
CREATE VECTOR INDEX product_embed
  FOR (p:Product) ON (p.embedding)
  OPTIONS {dimensions: 384, similarity: 'cosine'}
```

Options:
- `dimensions` (required): Vector dimensionality
- `similarity`: `'cosine'` (default), `'euclidean'`, or `'dot_product'`

### Query Vector Index

```cypher
CALL db.index.vector.queryNodes('Product', 'embedding', [0.1, 0.2, ...], 10)
YIELD node, score
RETURN node.name, score
```

## Schema Introspection

```cypher
-- All labels in use
SHOW LABELS

-- All relationship types
SHOW RELATIONSHIP TYPES

-- All property keys
SHOW PROPERTY KEYS

-- Visual schema (returns nodes/edges representing the schema)
CALL db.schema.visualization()
```

## Index Usage in Queries

The query planner automatically uses indexes when available:

```cypher
-- With index on :Person(name), this uses IndexScan instead of NodeScan
MATCH (p:Person {name: 'Alice'}) RETURN p

-- Check with EXPLAIN
EXPLAIN MATCH (p:Person {name: 'Alice'}) RETURN p
-- Output shows: IndexScan instead of NodeScan+Filter
```
