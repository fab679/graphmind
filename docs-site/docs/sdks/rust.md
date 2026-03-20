---
sidebar_position: 2
title: Rust SDK
description: Embedded and remote Rust client for the Graphmind graph database
---

# Rust SDK

The Rust SDK (`graphmind-sdk`) provides both embedded and remote access to Graphmind. In embedded mode the database runs in-process with zero network overhead. In remote mode it connects to a running server over HTTP.

## Installation

Add to your `Cargo.toml`:

```toml
[dependencies]
graphmind-sdk = "0.6.4"
tokio = { version = "1", features = ["full"] }
```

## Quick Start -- Embedded Mode

No server required. The graph lives in your process memory.

```rust
use graphmind_sdk::{EmbeddedClient, GraphmindClient};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let client = EmbeddedClient::new();

    // Create nodes and edges (semicolons separate statements)
    client.query("default", r#"
        CREATE (a:Person {name: "Alice", age: 30});
        CREATE (b:Person {name: "Bob", age: 25});
        MATCH (a:Person {name: "Alice"}), (b:Person {name: "Bob"})
        CREATE (a)-[:KNOWS {since: 2020}]->(b)
    "#).await?;

    // Query
    let result = client.query_readonly("default",
        "MATCH (p:Person) RETURN p.name, p.age ORDER BY p.age"
    ).await?;

    for record in &result.records {
        println!("{:?}", record);
    }

    Ok(())
}
```

## Quick Start -- Remote Mode

Connect to a running Graphmind server (RESP on :6379, HTTP on :8080).

```rust
use graphmind_sdk::{RemoteClient, GraphmindClient};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let client = RemoteClient::new("http://localhost:8080");

    let result = client.query("default",
        "MATCH (p:Person)-[:KNOWS]->(f) RETURN p.name, f.name"
    ).await?;

    println!("Columns: {:?}", result.columns);
    println!("Found {} records", result.records.len());

    Ok(())
}
```

## The `GraphmindClient` Trait

Both `EmbeddedClient` and `RemoteClient` implement the `GraphmindClient` trait, so you can write generic code:

```rust
use graphmind_sdk::{GraphmindClient, GraphmindResult, QueryResult};

async fn count_nodes(client: &dyn GraphmindClient) -> GraphmindResult<u64> {
    let result = client.query_readonly("default",
        "MATCH (n) RETURN count(n) AS total"
    ).await?;
    // Extract count from first record
    let count = result.records[0][0].as_u64().unwrap_or(0);
    Ok(count)
}
```

### Trait Methods

| Method | Description |
|--------|-------------|
| `query(graph, cypher)` | Execute a read/write Cypher query |
| `query_readonly(graph, cypher)` | Execute a read-only query |
| `explain(graph, cypher)` | Return the EXPLAIN plan without executing |
| `profile(graph, cypher)` | Execute with PROFILE instrumentation |
| `schema(graph)` | Return a schema summary string |
| `status()` | Server health, version, node/edge counts |
| `ping()` | Connectivity check (returns `"PONG"`) |
| `list_graphs()` | List all graph namespaces |
| `delete_graph(graph)` | Delete all data in a graph |

## CRUD Operations

### CREATE

```rust
// Nodes with properties
client.query("default", r#"
    CREATE (p:Person {name: "Carol", age: 28, active: true})
"#).await?;

// Edges with properties
client.query("default", r#"
    MATCH (a:Person {name: "Alice"}), (c:Person {name: "Carol"})
    CREATE (a)-[:WORKS_WITH {project: "GraphDB", since: 2023}]->(c)
"#).await?;
```

### MATCH with WHERE

```rust
let result = client.query_readonly("default", r#"
    MATCH (p:Person)
    WHERE p.age > 25 AND p.active = true
    RETURN p.name, p.age
    ORDER BY p.age DESC
    LIMIT 10
"#).await?;
```

### SET (update properties)

```rust
client.query("default", r#"
    MATCH (p:Person {name: "Alice"})
    SET p.age = 31, p.title = "Engineer"
"#).await?;
```

### DELETE

```rust
// Delete specific nodes and their edges
client.query("default", r#"
    MATCH (p:Person {name: "Bob"})
    DELETE p
"#).await?;
```

### MERGE (upsert)

```rust
client.query("default", r#"
    MERGE (p:Person {name: "Dave"})
    SET p.age = 35
"#).await?;
```

## Aggregations

```rust
let result = client.query_readonly("default", r#"
    MATCH (p:Person)
    RETURN count(p) AS total,
           avg(p.age) AS avg_age,
           min(p.age) AS youngest,
           max(p.age) AS oldest,
           collect(p.name) AS names
"#).await?;
```

### GROUP BY

```rust
let result = client.query_readonly("default", r#"
    MATCH (p:Person)-[:WORKS_AT]->(c:Company)
    RETURN c.name, count(p) AS employees, avg(p.age) AS avg_age
    ORDER BY employees DESC
"#).await?;
```

## Traversals

### Multi-hop Patterns

```rust
let result = client.query_readonly("default", r#"
    MATCH (a:Person {name: "Alice"})-[:KNOWS]->(b)-[:KNOWS]->(c)
    WHERE a <> c
    RETURN DISTINCT c.name AS friend_of_friend
"#).await?;
```

### Variable-length Paths

```rust
let result = client.query_readonly("default", r#"
    MATCH (a:Person {name: "Alice"})-[:KNOWS*1..3]->(b:Person)
    RETURN DISTINCT b.name, length(b) AS distance
"#).await?;
```

## Schema Introspection

```rust
let schema = client.schema("default").await?;
println!("{}", schema);
// Output:
// Node labels: Person (42), Company (5), City (8)
// Edge types: KNOWS (120), WORKS_AT (42), LIVES_IN (42)
```

## EXPLAIN and PROFILE

```rust
// EXPLAIN -- show the plan without executing
let plan = client.explain("default",
    "MATCH (p:Person)-[:KNOWS]->(f) WHERE p.age > 25 RETURN f.name"
).await?;
for record in &plan.records {
    println!("{:?}", record);
}

// PROFILE -- execute and show operator-level timing/row counts
let profile = client.profile("default",
    "MATCH (p:Person)-[:KNOWS]->(f) RETURN f.name"
).await?;
for record in &profile.records {
    println!("{:?}", record);
}
```

## Multi-tenancy

Use different graph names to isolate data:

```rust
// Each graph name is a separate namespace
client.query("tenant_acme", r#"CREATE (n:User {name: "Acme User"})"#).await?;
client.query("tenant_globex", r#"CREATE (n:User {name: "Globex User"})"#).await?;

// Queries are scoped to their graph
let acme = client.query_readonly("tenant_acme", "MATCH (n) RETURN count(n)").await?;
let globex = client.query_readonly("tenant_globex", "MATCH (n) RETURN count(n)").await?;

// List all graphs
let graphs = client.list_graphs().await?;

// Delete a tenant's graph
client.delete_graph("tenant_acme").await?;
```

## Extension: Graph Algorithms (Embedded Only)

The `AlgorithmClient` trait adds algorithm methods to `EmbeddedClient`:

```rust
use graphmind_sdk::{EmbeddedClient, AlgorithmClient, PageRankConfig};

let client = EmbeddedClient::new();
// ... populate graph ...

// PageRank
let config = PageRankConfig { damping_factor: 0.85, iterations: 20, ..Default::default() };
let scores = client.page_rank(config, Some("Person"), Some("KNOWS")).await;
for (node_id, score) in &scores {
    println!("Node {} -> {:.4}", node_id, score);
}

// Shortest path (BFS)
let path = client.bfs(source_id, target_id, None, None).await;
if let Some(p) = path {
    println!("Path: {:?}, cost: {}", p.path, p.cost);
}

// Dijkstra (weighted)
let path = client.dijkstra(src, dst, None, Some("ROAD"), Some("distance")).await;

// Weakly connected components
let wcc = client.weakly_connected_components(None, None).await;
println!("{} components found", wcc.components.len());

// Strongly connected components
let scc = client.strongly_connected_components(None, None).await;
```

## Extension: Vector Search (Embedded Only)

The `VectorClient` trait adds HNSW vector index operations:

```rust
use graphmind_sdk::{EmbeddedClient, VectorClient, DistanceMetric};

let client = EmbeddedClient::new();

// Create a vector index
client.create_vector_index("Document", "embedding", 384, DistanceMetric::Cosine).await?;

// Add vectors
client.add_vector("Document", "embedding", node_id, &embedding_vec).await?;

// k-NN search
let results = client.vector_search("Document", "embedding", &query_vec, 10).await?;
for (node_id, distance) in results {
    println!("Node {:?} at distance {:.4}", node_id, distance);
}
```

## Error Handling

All SDK methods return `GraphmindResult<T>`, which wraps `GraphmindError`:

```rust
use graphmind_sdk::{GraphmindError, GraphmindResult};

match client.query("default", "INVALID CYPHER").await {
    Ok(result) => println!("Success: {} records", result.records.len()),
    Err(GraphmindError::QueryError(msg)) => eprintln!("Bad query: {}", msg),
    Err(GraphmindError::ConnectionError(msg)) => eprintln!("Network: {}", msg),
    Err(e) => eprintln!("Other error: {}", e),
}
```

## Thread Safety

`EmbeddedClient` uses `Arc<RwLock<GraphStore>>` internally, so it is safe to clone and share across threads:

```rust
let client = EmbeddedClient::new();
let c1 = client.clone();
let c2 = client.clone();

let h1 = tokio::spawn(async move {
    c1.query_readonly("default", "MATCH (n) RETURN count(n)").await
});
let h2 = tokio::spawn(async move {
    c2.query_readonly("default", "MATCH (n) RETURN count(n)").await
});

let (r1, r2) = tokio::join!(h1, h2);
```

## Working with Query Results

The `QueryResult` struct contains:

| Field | Type | Description |
|-------|------|-------------|
| `columns` | `Vec<String>` | Column names from RETURN clause |
| `records` | `Vec<Vec<serde_json::Value>>` | Row data as JSON values |
| `nodes` | `Vec<SdkNode>` | Graph nodes touched by the query |
| `edges` | `Vec<SdkEdge>` | Graph edges touched by the query |

```rust
let result = client.query_readonly("default",
    "MATCH (p:Person) RETURN p.name, p.age"
).await?;

// Iterate records
for row in &result.records {
    let name = row[0].as_str().unwrap_or("?");
    let age = row[1].as_i64().unwrap_or(0);
    println!("{} is {} years old", name, age);
}

// Inspect returned nodes
for node in &result.nodes {
    println!("Node {} labels={:?} props={:?}", node.id, node.labels, node.properties);
}
```

## Re-exported Types

The SDK re-exports key types from the core crate so you do not need to depend on `graphmind` directly:

- **Graph types:** `GraphStore`, `Node`, `Edge`, `NodeId`, `EdgeId`, `PropertyValue`, `Label`, `EdgeType`
- **Query types:** `QueryEngine`, `RecordBatch`, `CacheStats`
- **Algorithm types:** `PageRankConfig`, `PathResult`, `WccResult`, `SccResult`, `FlowResult`, `MSTResult`
- **Vector types:** `VectorIndex`, `VectorIndexManager`, `DistanceMetric`
- **Persistence types:** `PersistenceManager`, `PersistentStorage`, `Wal`
- **NLQ types:** `NLQPipeline`, `NLQConfig`, `LLMProvider`
- **Optimization types:** `PSOSolver`, `GASolver`, `DESolver`, `SASolver`, `NSGA2Solver`, and more
