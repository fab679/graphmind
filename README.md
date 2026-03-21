# Graphmind Graph Database

![Version](https://img.shields.io/badge/version-0.6.5-blue)
![Rust](https://img.shields.io/badge/rust-1.85-orange)
![License](https://img.shields.io/badge/license-Apache_2.0-blue)

**Graphmind** is a high-performance, distributed graph database written in Rust with ~90% OpenCypher support, Redis protocol compatibility, and a built-in web visualizer. It combines property graph storage, vector search, graph algorithms, and natural language querying in a single binary.

[![Graphmind Graph Simulation](https://github.com/fab679/graphmind/releases/download/kg-snapshots-v2/simulation-preview.gif)](https://github.com/fab679/graphmind/releases/download/kg-snapshots-v2/graphmind-cricket-demo.mp4)

## Install

### Docker (recommended)

```bash
docker run -d --name graphmind \
  -p 6379:6379 -p 8080:8080 \
  -v graphmind-data:/data \
  fabischk/graphmind:latest
```

### Binary

```bash
# Quick install script (Linux/macOS)
curl -sSL https://raw.githubusercontent.com/fab679/graphmind/main/dist/install.sh | bash

# Or download directly from GitHub Releases:
# https://github.com/fab679/graphmind/releases/latest
# Linux:  graphmind-v0.6.5-x86_64-unknown-linux-gnu.tar.gz
# macOS:  graphmind-v0.6.5-aarch64-apple-darwin.tar.gz
# Intel:  graphmind-v0.6.5-x86_64-apple-darwin.tar.gz
```

### Cargo

```bash
cargo install graphmind
```

### From Source

```bash
git clone https://github.com/fab679/graphmind.git
cd graphmind
cd ui && npm install && npm run build && cd ..
cargo build --release
./target/release/graphmind
```

## Quick Start

```bash
# Start the server (RESP on :6379, HTTP on :8080)
graphmind

# Open the web visualizer
open http://localhost:8080
```

### Create data and query

```bash
redis-cli -p 6379

# Create nodes and relationships
GRAPH.QUERY default "CREATE (a:Person {name: 'Alice', age: 30})"
GRAPH.QUERY default "CREATE (b:Person {name: 'Bob', age: 25})"
GRAPH.QUERY default "MATCH (a:Person {name: 'Alice'}), (b:Person {name: 'Bob'}) CREATE (a)-[:KNOWS]->(b)"

# Query
GRAPH.QUERY default "MATCH (p:Person)-[:KNOWS]->(friend) RETURN p.name, friend.name"
```

### Load demo data

```bash
curl -X POST http://localhost:8080/api/script \
  -H 'Content-Type: application/json' \
  --data-binary @scripts/social_network_demo.cypher
```

## SDK Examples

### Python

```bash
pip install graphmind
```

```python
from graphmind import GraphmindClient

db = GraphmindClient.embedded()  # or .remote("localhost", 8080)

db.query("""
    CREATE (a:Person {name: 'Alice', age: 30});
    CREATE (b:Person {name: 'Bob', age: 25});
    MATCH (a:Person {name: 'Alice'}), (b:Person {name: 'Bob'})
    CREATE (a)-[:KNOWS]->(b)
""")

result = db.query_readonly("MATCH (n:Person) RETURN n.name, n.age")
print(result)
```

### TypeScript

```bash
npm install graphmind-sdk
```

```typescript
import { GraphmindClient } from 'graphmind-sdk';

const client = new GraphmindClient({ url: 'http://localhost:8080' });

await client.query(`
  CREATE (a:Person {name: "Alice", age: 30});
  CREATE (b:Person {name: "Bob", age: 25});
  MATCH (a:Person {name: "Alice"}), (b:Person {name: "Bob"})
  CREATE (a)-[:KNOWS]->(b)
`);

const result = await client.query('MATCH (n:Person) RETURN n.name, n.age');
```

### Rust

```toml
[dependencies]
graphmind-sdk = "0.6.5"
```

```rust
use graphmind_sdk::EmbeddedClient;

let mut client = EmbeddedClient::new();
client.query("default", "
    CREATE (a:Person {name: 'Alice', age: 30});
    CREATE (b:Person {name: 'Bob', age: 25});
    MATCH (a:Person {name: 'Alice'}), (b:Person {name: 'Bob'})
    CREATE (a)-[:KNOWS]->(b)
")?;

let result = client.query_readonly("default", "MATCH (n:Person) RETURN n.name, n.age")?;
```

### redis-cli / Any Redis Client

```bash
redis-cli -p 6379
> GRAPH.QUERY default "MATCH (n) RETURN labels(n), count(n)"
```

```python
import redis
r = redis.Redis(port=6379)
r.execute_command('GRAPH.QUERY', 'default', 'MATCH (n:Person) RETURN n.name')
```

### REST API

```bash
curl -X POST http://localhost:8080/api/query \
  -H 'Content-Type: application/json' \
  -d '{"query": "MATCH (n) RETURN n LIMIT 10"}'
```

## Configuration

| Variable | Default | Description |
|----------|---------|-------------|
| `GRAPHMIND_HOST` | `127.0.0.1` | RESP server bind address |
| `GRAPHMIND_PORT` | `6379` | RESP server port |
| `GRAPHMIND_HTTP_PORT` | `8080` | HTTP/visualizer port |
| `GRAPHMIND_DATA_DIR` | `./graphmind_data` | Data directory |
| `GRAPHMIND_AUTH_TOKEN` | *(none)* | Enable auth with this token |
| `GRAPHMIND_LOG_LEVEL` | `info` | Log level |

See [`dist/config.toml`](dist/config.toml) for a full config file example.

## Key Features

- **OpenCypher** -- ~90% coverage (MATCH, CREATE, MERGE, WITH, UNWIND, UNION, 30+ functions)
- **RESP Protocol** -- Works with any Redis client
- **Vector Search** -- Built-in HNSW indexing
- **NLQ** -- Natural language to Cypher via OpenAI, Gemini, Ollama, or Claude
- **Graph Algorithms** -- PageRank, BFS, Dijkstra, WCC, SCC, and more
- **Multi-Tenancy** -- Isolated graph namespaces with per-tenant quotas
- **High Availability** -- Raft consensus for cluster replication
- **Web Visualizer** -- Interactive graph explorer with D3.js force graph
- **MCP Server** -- Auto-generate AI agent tools from graph schema

## Documentation

- [Full Documentation](https://fab679.github.io/graphmind/)
- [Releases & Changelog](https://github.com/fab679/graphmind/releases)

## License

Apache License 2.0
