---
sidebar_position: 2
title: CRUD Operations
description: Create, read, update, and delete nodes and relationships
---

# CRUD Operations

This page covers the full lifecycle: creating data, reading it, updating properties, and deleting nodes and relationships.

## CREATE

### Create Nodes

```cypher
-- Node with one label and properties
CREATE (p:Person {name: 'Alice', age: 30, active: true})

-- Node with no properties
CREATE (c:City)

-- Multiple nodes using semicolons
CREATE (a:Person {name: 'Alice', age: 30});
CREATE (b:Person {name: 'Bob', age: 25})
```

:::tip
Use semicolons to separate multiple statements. Each statement sees the effects of previous ones, so you can create nodes first, then match and link them:
```cypher
CREATE (a:Person {name: 'Alice'});
CREATE (b:Person {name: 'Bob'});
MATCH (a:Person {name: 'Alice'}), (b:Person {name: 'Bob'})
CREATE (a)-[:KNOWS]->(b)
```
:::

### Multi-CREATE with Shared Variables

You can use consecutive CREATE statements that share variables without semicolons. Graphmind automatically inserts `WITH` clauses to carry variables between CREATE statements:

```cypher
CREATE (a:Person {name: 'Alice', age: 30})
CREATE (b:Person {name: 'Bob', age: 25})
CREATE (a)-[:KNOWS {since: 2020}]->(b)
```

### CREATE with RETURN

Return newly created nodes or relationships:

```cypher
CREATE (p:Person {name: 'Carol', age: 28})
RETURN p.name, p.age
```

### Create Relationships

Relationships are created between existing or newly created nodes:

```cypher
-- Create two nodes and a relationship in one statement
CREATE (a:Person {name: "Alice"})-[:KNOWS {since: 2020}]->(b:Person {name: "Bob"})
```

Create a relationship between existing nodes:

```cypher
MATCH (a:Person {name: "Alice"}), (b:Person {name: "Bob"})
CREATE (a)-[:WORKS_WITH {project: "GraphDB"}]->(b)
```

## MATCH (Read)

### Find all nodes of a label

```cypher
MATCH (p:Person)
RETURN p.name, p.age
```

### Find by property

```cypher
MATCH (p:Person {name: "Alice"})
RETURN p
```

### Find relationships

```cypher
MATCH (a:Person)-[r:KNOWS]->(b:Person)
RETURN a.name, b.name, r.since
```

### Find with conditions

```cypher
MATCH (p:Person)
WHERE p.age >= 25 AND p.age <= 35
RETURN p.name, p.age
ORDER BY p.age
```

## SET (Update)

### Update properties

```cypher
MATCH (p:Person {name: "Alice"})
SET p.age = 31, p.title = "Engineer"
```

### Add a label

```cypher
MATCH (p:Person {name: "Alice"})
SET p:Employee
```

### Replace all properties (map replace)

Replace all properties on a node with a new map. Existing properties not in the map are removed:

```cypher
MATCH (p:Person {name: "Alice"})
SET p = {name: "Alice", age: 31, title: "Engineer"}
```

### Merge properties (map merge)

Add or update properties without removing existing ones:

```cypher
MATCH (p:Person {name: "Alice"})
SET p += {title: "Engineer", department: "R&D"}
```

### Update relationship properties

```cypher
MATCH (a:Person {name: "Alice"})-[r:KNOWS]->(b:Person {name: "Bob"})
SET r.strength = "close"
```

## REMOVE

### Remove a property

```cypher
MATCH (p:Person {name: "Alice"})
REMOVE p.title
```

### Remove a label

```cypher
MATCH (p:Person {name: "Alice"})
REMOVE p:Employee
```

### Remove multiple labels

```cypher
MATCH (p:Person {name: "Alice"})
REMOVE p:Employee:Contractor
```

## DELETE

### Delete a node (with no relationships)

```cypher
MATCH (p:Person {name: "Carol"})
DELETE p
```

This fails if the node has any relationships. Use `DETACH DELETE` instead.

### Delete a node and all its relationships

```cypher
MATCH (p:Person {name: "Carol"})
DETACH DELETE p
```

### Delete a specific relationship

```cypher
MATCH (a:Person {name: "Alice"})-[r:KNOWS]->(b:Person {name: "Bob"})
DELETE r
```

### Delete all data

```cypher
MATCH (n) DETACH DELETE n
```

## MERGE (Upsert)

`MERGE` creates a node or relationship only if it does not already exist. If it exists, it matches it instead.

### Basic MERGE

```cypher
-- Creates the node if no Person with name "Dave" exists; otherwise matches it
MERGE (p:Person {name: "Dave"})
```

### MERGE with ON CREATE SET / ON MATCH SET

```cypher
MERGE (p:Person {name: "Dave"})
ON CREATE SET p.age = 35, p.created = true
ON MATCH SET p.lastSeen = 2024
```

- `ON CREATE SET` -- runs only when a new node is created
- `ON MATCH SET` -- runs only when an existing node is matched

### MERGE relationships

```cypher
MATCH (a:Person {name: "Alice"}), (b:Person {name: "Dave"})
MERGE (a)-[:KNOWS]->(b)
```

## Full CRUD Lifecycle Example

Here is a complete example that creates, reads, updates, and deletes data:

```cypher
-- 1. Create
CREATE (a:Person {name: "Alice", age: 30})
CREATE (b:Person {name: "Bob", age: 25})
CREATE (a)-[:KNOWS {since: 2020}]->(b)
```

```cypher
-- 2. Read
MATCH (p:Person)-[r:KNOWS]->(friend)
RETURN p.name, friend.name, r.since
```

| p.name | friend.name | r.since |
|--------|-------------|---------|
| Alice  | Bob         | 2020    |

```cypher
-- 3. Update
MATCH (p:Person {name: "Alice"})
SET p.age = 31, p.email = "alice@example.com"
```

```cypher
-- 4. Upsert
MERGE (c:Person {name: "Carol"})
ON CREATE SET c.age = 28
```

```cypher
-- 5. Delete a relationship
MATCH (a:Person {name: "Alice"})-[r:KNOWS]->(b:Person {name: "Bob"})
DELETE r
```

```cypher
-- 6. Delete a node and its remaining relationships
MATCH (p:Person {name: "Bob"})
DETACH DELETE p
```
