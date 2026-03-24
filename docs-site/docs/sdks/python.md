---
sidebar_position: 3
title: Python SDK
description: Embedded and remote Python client for the Graphmind graph database
---

# Python SDK

The Python SDK provides native bindings via PyO3, giving you both embedded mode (in-process, no server) and remote mode (connects to a running Graphmind server over HTTP).

## Installation

### From PyPI

```bash
pip install graphmind
```

### From Source (with maturin)

```bash
cd sdk/python
pip install maturin
maturin develop --release
```

This compiles the Rust bindings and installs the `graphmind` module into your active Python environment.

## Quick Start

### Embedded Mode

No server needed. The graph database runs inside the Python process.

```python
from graphmind import GraphmindClient

client = GraphmindClient.embedded()

# Create data (semicolons separate multiple statements)
client.query("""
    CREATE (a:Person {name: 'Alice', age: 30});
    CREATE (b:Person {name: 'Bob', age: 25});
    MATCH (a:Person {name: 'Alice'}), (b:Person {name: 'Bob'})
    CREATE (a)-[:KNOWS {since: 2020}]->(b)
""")

# Query data
result = client.query_readonly("MATCH (p:Person) RETURN p.name, p.age ORDER BY p.age")
print(result.columns)   # ['p.name', 'p.age']
print(result.records)   # [['Alice', 30], ['Bob', 25]]
print(len(result))      # 2
```

### Remote Mode

Connect to a running Graphmind server.

```python
from graphmind import GraphmindClient

client = GraphmindClient.connect("http://localhost:8080")

result = client.query_readonly("MATCH (n) RETURN count(n) AS total")
print(result.records[0][0])  # total node count
```

## API Reference

### Client Methods

| Method | Description |
|--------|-------------|
| `GraphmindClient.embedded()` | Create an in-process client |
| `GraphmindClient.connect(url)` | Connect to a remote server |
| `query(cypher, graph="default", params=None)` | Execute a read/write query |
| `query_readonly(cypher, graph="default", params=None)` | Execute a read-only query |
| `explain(cypher, graph="default")` | Show execution plan |
| `profile(cypher, graph="default")` | Execute with profiling stats |
| `execute_script(script, graph="default")` | Run multi-statement script |
| `schema(graph="default")` | Get schema summary |
| `status()` | Server health, stats, and version |
| `ping()` | Connectivity check |
| `list_graphs()` | List graph namespaces |
| `delete_graph(graph="default")` | Delete a graph |

### QueryResult

| Attribute | Type | Description |
|-----------|------|-------------|
| `columns` | `list[str]` | Column names |
| `records` | `list[list]` | Row data (native Python types) |
| `nodes` | `list[dict]` | Nodes with `id`, `labels`, `properties` |
| `edges` | `list[dict]` | Edges with `id`, `source`, `target`, `type`, `properties` |

## Query Parameters

Use parameterized queries to safely pass dynamic values. Parameters prevent injection and are ideal for strings with special characters (quotes, semicolons).

```python
# Pass parameters as a dict
result = client.query_readonly(
    "MATCH (p:Person) WHERE p.name = $name AND p.age > $minAge RETURN p.name, p.age",
    params={"name": "Alice", "minAge": 25}
)

for row in result.records:
    print(f"{row[0]} is {row[1]} years old")
```

### Write queries with parameters

```python
client.query(
    "MERGE (p:Person {name: $name}) SET p.age = $age, p.bio = $bio",
    params={
        "name": "Carol",
        "age": 28,
        "bio": 'She said "hello" and it\'s fine — special chars work!'
    }
)
```

### Supported parameter types

| Python type | Cypher type |
|-------------|-------------|
| `str` | String |
| `int` | Integer |
| `float` | Float |
| `bool` | Boolean |
| `None` | Null |
| `list` | Array |

Parameters work in both embedded and remote mode.

## CRUD Operations

### CREATE

```python
# Nodes
client.query('CREATE (p:Person {name: "Carol", age: 28, active: true})')

# Edges
client.query('''
    MATCH (a:Person {name: "Alice"}), (c:Person {name: "Carol"})
    CREATE (a)-[:WORKS_WITH {project: "GraphDB"}]->(c)
''')
```

### MATCH with WHERE

```python
result = client.query_readonly('''
    MATCH (p:Person)
    WHERE p.age > 25 AND p.active = true
    RETURN p.name, p.age
    ORDER BY p.age DESC
    LIMIT 10
''')

for row in result.records:
    print(f"{row[0]} is {row[1]} years old")
```

### SET (update)

```python
client.query('MATCH (p:Person {name: "Alice"}) SET p.age = 31, p.title = "Engineer"')
```

### DELETE

```python
client.query('MATCH (p:Person {name: "Bob"}) DELETE p')
```

### MERGE (upsert)

```python
client.query('MERGE (p:Person {name: "Dave"}) SET p.age = 35')
```

## Aggregations

```python
result = client.query_readonly('''
    MATCH (p:Person)
    RETURN count(p) AS total,
           avg(p.age) AS avg_age,
           min(p.age) AS youngest,
           max(p.age) AS oldest,
           collect(p.name) AS names
''')

row = result.records[0]
print(f"Total: {row[0]}, Avg age: {row[1]:.1f}")
```

### GROUP BY

```python
result = client.query_readonly('''
    MATCH (p:Person)-[:WORKS_AT]->(c:Company)
    RETURN c.name, count(p) AS employees, avg(p.age) AS avg_age
    ORDER BY employees DESC
''')

for name, count, avg_age in result.records:
    print(f"{name}: {count} employees, avg age {avg_age:.1f}")
```

## Multi-hop Traversals

```python
# Friends of friends
result = client.query_readonly('''
    MATCH (a:Person {name: "Alice"})-[:KNOWS]->(b)-[:KNOWS]->(c)
    WHERE a <> c
    RETURN DISTINCT c.name AS friend_of_friend
''')

# Variable-length paths
result = client.query_readonly('''
    MATCH (a:Person {name: "Alice"})-[:KNOWS*1..3]->(b:Person)
    RETURN DISTINCT b.name
''')
```

## Schema Introspection

```python
schema = client.schema()
print(schema)
# Node labels: Person (42), Company (5)
# Edge types: KNOWS (120), WORKS_AT (42)
```

## EXPLAIN and PROFILE

```python
# See the query plan without executing
plan = client.explain("MATCH (p:Person)-[:KNOWS]->(f) RETURN f.name")
for row in plan.records:
    print(row)

# Execute with profiling (operator timing and row counts)
profile = client.profile("MATCH (p:Person)-[:KNOWS]->(f) RETURN f.name")
for row in profile.records:
    print(row)
```

## Script Execution

Execute multiple semicolon-separated statements in order:

```python
results = client.execute_script('''
    CREATE (a:City {name: "Paris"});
    CREATE (b:City {name: "London"});
    MATCH (a:City {name: "Paris"}), (b:City {name: "London"})
    CREATE (a)-[:CONNECTED_TO {distance: 450}]->(b)
''')

print(f"Executed {len(results)} statements")
```

## Server Version

```python
status = client.status()
print(status.version)  # e.g. "0.8.0-beta"
```

## Multi-tenancy

```python
# Separate graph namespaces
client.query('CREATE (n:User {name: "Acme User"})', graph="tenant_acme")
client.query('CREATE (n:User {name: "Globex User"})', graph="tenant_globex")

# Queries are scoped
acme_count = client.query_readonly("MATCH (n) RETURN count(n)", graph="tenant_acme")
globex_count = client.query_readonly("MATCH (n) RETURN count(n)", graph="tenant_globex")

# List and delete
graphs = client.list_graphs()
client.delete_graph("tenant_acme")
```

## Graph Algorithms (Embedded Only)

Algorithm methods are available only in embedded mode. Calling them on a remote client raises `RuntimeError`.

### PageRank

```python
scores = client.page_rank(label="Person", edge_type="KNOWS", damping=0.85, iterations=20)
# Returns: {node_id: score, ...}

for node_id, score in sorted(scores.items(), key=lambda x: -x[1])[:10]:
    print(f"Node {node_id}: {score:.4f}")
```

### Connected Components

```python
# Weakly connected components
wcc = client.wcc(label="Person", edge_type="KNOWS")
print(f"{wcc['component_count']} components")

# Strongly connected components
scc = client.scc()
```

### Shortest Path

```python
# BFS (unweighted)
path = client.bfs(source=1, target=42)
if path:
    print(f"Path: {path['path']}, cost: {path['cost']}")

# Dijkstra (weighted)
path = client.dijkstra(source=1, target=42, weight_property="distance")
```

### PCA

```python
result = client.pca(properties=["age", "income", "score"], label="Person", n_components=2)
print(f"Explained variance ratio: {result['explained_variance_ratio']}")
print(f"Components: {result['components']}")
```

### Triangle Count

```python
count = client.triangle_count(label="Person", edge_type="KNOWS")
print(f"Found {count} triangles")
```

## Vector Search (Embedded Only)

```python
# Create an HNSW index
client.create_vector_index("Document", "embedding", dimensions=384, metric="cosine")

# Add vectors (node_id must exist)
client.add_vector("Document", "embedding", node_id=1, vector=[0.1, 0.2, ...])

# k-NN search
results = client.vector_search("Document", "embedding", query_vector=[0.15, 0.25, ...], k=10)
for node_id, distance in results:
    print(f"Node {node_id}: distance {distance:.4f}")
```

Supported distance metrics: `"cosine"`, `"l2"`, `"dot"`.

## Error Handling

SDK methods raise `RuntimeError` on failure:

```python
try:
    client.query("INVALID CYPHER SYNTAX")
except RuntimeError as e:
    print(f"Query failed: {e}")
```

Common error scenarios:
- Invalid Cypher syntax
- Referencing non-existent nodes in DELETE/SET
- Calling algorithm/vector methods on a remote client
- Network errors in remote mode

## Integration with pandas

Convert query results to a pandas DataFrame:

```python
import pandas as pd

result = client.query_readonly("MATCH (p:Person) RETURN p.name, p.age, p.active")
df = pd.DataFrame(result.records, columns=result.columns)

print(df.describe())
print(df.groupby("p.active")["p.age"].mean())
```

## Integration with NetworkX

Build a NetworkX graph from query results:

```python
import networkx as nx

result = client.query_readonly('''
    MATCH (a:Person)-[r:KNOWS]->(b:Person)
    RETURN a.name, b.name, r.since
''')

G = nx.DiGraph()
for src, dst, since in result.records:
    G.add_edge(src, dst, since=since)

print(f"NetworkX graph: {G.number_of_nodes()} nodes, {G.number_of_edges()} edges")
print(f"Density: {nx.density(G):.4f}")

# Use NetworkX algorithms
print(f"Clustering: {nx.average_clustering(G.to_undirected()):.4f}")
```

## Use in Jupyter Notebooks

The `QueryResult` repr works well in notebooks:

```python
# In a Jupyter cell
client = GraphmindClient.embedded()
client.query('CREATE (a:Person {name: "Alice", age: 30})')

result = client.query_readonly("MATCH (p:Person) RETURN p.name, p.age")
result  # displays: QueryResult(columns=['p.name', 'p.age'], records=1)
```

Combine with pandas for rich table display:

```python
import pandas as pd

result = client.query_readonly("MATCH (p:Person) RETURN p.name, p.age")
pd.DataFrame(result.records, columns=result.columns)
# Renders as a formatted HTML table in Jupyter
```

## Performance Tips

- **Use embedded mode** for data pipelines and batch operations -- eliminates network roundtrips.
- **Use `execute_script`** to batch multiple CREATE statements in one call instead of separate `query()` calls.
- **Use `query_readonly`** for read operations -- it allows concurrent readers.
- **Use `execute_script`** to send multiple statements in one call to the remote server.
- **Reuse the client** instance rather than creating a new one per operation.
