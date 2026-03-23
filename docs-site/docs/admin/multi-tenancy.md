---
sidebar_position: 2
title: Multi-Tenancy
description: Isolate data with named graph namespaces
---

# Multi-Tenancy

Graphmind supports multiple isolated graph namespaces (tenants) within a single server instance. Each graph has its own nodes, edges, indexes, and data -- completely separated from other graphs.

## How It Works

Every query targets a specific graph by name. The default graph is called `default`. Graphs are created automatically on first write -- no explicit setup needed.

## Creating and Using Graphs

### HTTP API

Specify the graph name in the `graph` field:

```bash
# Write to a graph named "analytics"
curl -X POST http://localhost:8080/api/query \
  -H 'Content-Type: application/json' \
  -d '{"query": "CREATE (n:User {name: \"Alice\"})", "graph": "analytics"}'

# Write to a different graph
curl -X POST http://localhost:8080/api/query \
  -H 'Content-Type: application/json' \
  -d '{"query": "CREATE (n:User {name: \"Bob\"})", "graph": "marketing"}'
```

If you omit the `graph` field, it defaults to `"default"`.

### RESP Protocol

The graph name is the first argument to `GRAPH.QUERY`:

```bash
redis-cli -p 6379
127.0.0.1:6379> GRAPH.QUERY analytics "CREATE (n:User {name: 'Alice'})"
127.0.0.1:6379> GRAPH.QUERY marketing "CREATE (n:User {name: 'Bob'})"
```

### Python SDK

```python
from graphmind import GraphmindClient

client = GraphmindClient.embedded()  # or .connect(...)

# Write to different graphs
client.query('CREATE (n:User {name: "Alice"})', graph="analytics")
client.query('CREATE (n:User {name: "Bob"})', graph="marketing")

# Queries are scoped to their graph
a = client.query_readonly("MATCH (n) RETURN count(n)", graph="analytics")
m = client.query_readonly("MATCH (n) RETURN count(n)", graph="marketing")
# a and m are independent counts
```

### TypeScript SDK

```typescript
// Pass graph name as second argument
await client.query('CREATE (n:User {name: "Alice"})', "analytics");
await client.query('CREATE (n:User {name: "Bob"})', "marketing");
```

### Rust SDK

```rust
client.query("analytics", r#"CREATE (n:User {name: "Alice"})"#).await?;
client.query("marketing", r#"CREATE (n:User {name: "Bob"})"#).await?;
```

## Listing Graphs

### HTTP API

```bash
curl http://localhost:8080/api/graphs
```

```json
{"graphs": ["default", "analytics", "marketing"]}
```

### RESP Protocol

```
127.0.0.1:6379> GRAPH.LIST
1) "default"
2) "analytics"
3) "marketing"
```

### SDKs

```python
graphs = client.list_graphs()
# ["default", "analytics", "marketing"]
```

## Deleting a Graph

Deletes all data (nodes, edges, indexes) in a graph namespace.

### HTTP API

```bash
curl -X DELETE http://localhost:8080/api/graphs/marketing
```

```json
{"status": "ok", "deleted": "marketing"}
```

### RESP Protocol

```
127.0.0.1:6379> GRAPH.DELETE marketing
OK
```

### SDKs

```python
client.delete_graph("marketing")
```

## Embedded Mode (Rust)

In embedded mode, you can use `TenantStoreManager` for direct access to isolated graph stores:

```rust
use graphmind::TenantStoreManager;

let mgr = TenantStoreManager::new();

// Stores are created automatically on first access
let production = mgr.get_store("production").await;
let staging = mgr.get_store("staging").await;

// Each store is fully isolated
{
    let mut store = production.write().await;
    engine.execute_mut("CREATE (n:User {name: 'Alice'})", &mut store, "production")?;
}

// List all tenant graphs
let graphs = mgr.list_graphs().await; // ["default", "production", "staging"]
```

## Embedded Mode (Python)

The Python SDK supports multi-tenancy in both embedded and remote modes:

```python
client = GraphmindClient.embedded()

# Write to isolated graphs
client.query('CREATE (n:User {name: "Alice"})', graph="production")
client.query('CREATE (n:User {name: "Bob"})', graph="staging")

# Read-only queries scoped to a graph
result = client.query_readonly("MATCH (n) RETURN n", graph="staging")

# List all graphs
graphs = client.list_graphs()  # ["default", "production", "staging"]
```

## Isolation Guarantees

- Each graph has its own node ID space, label indexes, and property indexes
- A query on graph A cannot see or affect data in graph B
- Persistence uses RocksDB column families with tenant-prefixed keys
- The `default` graph exists automatically and cannot be deleted

## Web Visualizer

The web UI at `http://localhost:8080` includes a graph selector dropdown. Use it to switch between graphs. The graph name is included in all API calls the UI makes.

## Use Cases

| Pattern | Example |
|---------|---------|
| **Per-customer isolation** | `tenant_acme`, `tenant_globex` |
| **Environment separation** | `production`, `staging`, `test` |
| **Domain separation** | `social_graph`, `product_catalog`, `fraud_detection` |
| **Temporary workspaces** | `import_20240315`, `experiment_a` (delete when done) |
