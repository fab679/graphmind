# Graphmind Rust SDK

Rust SDK for [Graphmind](https://github.com/fab679/graphmind) — use as an embedded graph database or connect to a remote server.

## Installation

```toml
[dependencies]
graphmind-sdk = "0.6.2"
```

## Embedded Mode

```rust
use graphmind_sdk::EmbeddedClient;

let mut client = EmbeddedClient::new();

// Create data
client.query("default", "CREATE (n:Person {name: 'Alice', age: 30})")?;

// Query
let result = client.query_readonly("default", "MATCH (n:Person) RETURN n.name, n.age")?;
println!("{:?}", result);

// Schema
let schema = client.schema("default")?;
println!("{}", schema);
```

## Remote Mode

```rust
use graphmind_sdk::RemoteClient;

let client = RemoteClient::new("http://localhost:8080");

let result = client.query("default", "MATCH (n) RETURN n LIMIT 10").await?;
```

## API

Both `EmbeddedClient` and `RemoteClient` implement the `GraphmindClient` trait:

| Method | Description |
|--------|-------------|
| `query(graph, cypher)` | Execute read/write query |
| `query_readonly(graph, cypher)` | Execute read-only query |
| `schema(graph)` | Schema introspection |
| `explain(graph, cypher)` | Show execution plan |
| `profile(graph, cypher)` | Execute with profiling |
| `status()` | Server health |
| `ping()` | Connectivity test |
| `list_graphs()` | List graph namespaces |
| `delete_graph(graph)` | Delete a namespace |

## Feature Flags

The parent `graphmind` crate supports feature flags for minimal builds:

```toml
# Minimal embedded engine (no server, no persistence)
graphmind = { version = "0.6.2", default-features = false }

# With persistence and vector search
graphmind = { version = "0.6.2", default-features = false, features = ["persistence", "vector"] }
```

## License

Apache-2.0 — see [LICENSE](../../LICENSE)
