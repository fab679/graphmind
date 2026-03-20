# Graphmind Python SDK

Python SDK for [Graphmind](https://github.com/fab679/graphmind) — a high-performance graph database with OpenCypher support.

## Installation

```bash
pip install graphmind
```

## Quick Start

### Embedded Mode (No Server Required)

```python
from graphmind import GraphmindClient

db = GraphmindClient.embedded()

# Create data (semicolons separate multiple statements)
db.query("""
    CREATE (a:Person {name: 'Alice', age: 30});
    CREATE (b:Person {name: 'Bob', age: 25});
    MATCH (a:Person {name: 'Alice'}), (b:Person {name: 'Bob'})
    CREATE (a)-[:KNOWS {since: 2020}]->(b)
""")

# Query
result = db.query_readonly("MATCH (n:Person) RETURN n.name, n.age ORDER BY n.age")
for row in result.rows:
    print(row)

# Schema introspection
schema = db.schema()
print(schema)
```

### Remote Mode (Connect to Server)

```python
from graphmind import GraphmindClient

db = GraphmindClient.remote("localhost", 8080)

result = db.query_readonly("MATCH (n) RETURN labels(n), count(n)")
print(result)
```

## API Reference

| Method | Description |
|--------|-------------|
| `GraphmindClient.embedded()` | Create embedded database (no server) |
| `GraphmindClient.remote(host, port)` | Connect to running server |
| `query(cypher, graph?)` | Execute read/write Cypher query |
| `query_readonly(cypher, graph?)` | Execute read-only query |
| `schema(graph?)` | Get schema (labels, types, properties) |
| `explain(cypher, graph?)` | Show query execution plan |
| `profile(cypher, graph?)` | Execute with profiling stats |
| `execute_script(script, graph?)` | Execute multi-statement script |
| `status()` | Server health check |
| `ping()` | Connectivity test |

## Multi-Tenancy

```python
# Queries run against isolated graph namespaces
db.query("CREATE (n:User {name: 'Alice'})", graph="production")
db.query("CREATE (n:User {name: 'Test'})", graph="staging")
```

## Requirements

- Python 3.8+
- No external dependencies (Rust-native via PyO3)

## License

Apache-2.0 — see [LICENSE](../../LICENSE)
