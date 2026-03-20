# Graphmind SDK for TypeScript/Node.js

TypeScript/Node.js client SDK for [Graphmind](https://github.com/fab679/graphmind) — a high-performance graph database with OpenCypher support.

## Installation

```bash
npm install graphmind-sdk
```

## Quick Start

```typescript
import { GraphmindClient } from 'graphmind-sdk';

const client = new GraphmindClient({ url: 'http://localhost:8080' });

// Create data
await client.query('CREATE (a:Person {name: "Alice", age: 30})');
await client.query('CREATE (b:Person {name: "Bob", age: 25})');

// Query
const result = await client.query('MATCH (n:Person) RETURN n.name, n.age');
console.log(result);

// Schema
const schema = await client.schema();
console.log(schema);
```

## Authentication

```typescript
const client = new GraphmindClient({
  url: 'http://localhost:8080',
  token: 'my-secret-token',
});
```

## API Reference

| Method | Description |
|--------|-------------|
| `query(cypher, graph?)` | Execute Cypher query (read or write) |
| `schema(graph?)` | Get schema introspection |
| `explain(cypher, graph?)` | Show execution plan |
| `profile(cypher, graph?)` | Execute with profiling |
| `executeScript(script, graph?)` | Multi-statement execution |
| `nlq(question, graph?)` | Natural language to Cypher |
| `status()` | Server health check |
| `ping()` | Connectivity test |
| `listGraphs()` | List all graph namespaces |
| `deleteGraph(name)` | Delete a graph namespace |

## Multi-Tenancy

```typescript
// Queries target isolated graph namespaces
await client.query('CREATE (n:User {name: "Alice"})', 'production');
await client.query('CREATE (n:User {name: "Test"})', 'staging');

const graphs = await client.listGraphs();
// ['default', 'production', 'staging']
```

## Requirements

- Node.js 18+
- A running Graphmind server

## License

Apache-2.0 — see [LICENSE](../../LICENSE)
