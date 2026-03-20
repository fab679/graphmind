---
sidebar_position: 1
title: SDK Overview
description: Client libraries for Graphmind in Rust, Python, TypeScript, and any Redis-compatible language
---

# SDKs & Client Libraries

Graphmind provides native SDKs for three languages plus compatibility with any Redis client library.

| SDK | Install | Mode | Status |
|-----|---------|------|--------|
| **[Rust](rust)** | `graphmind-sdk = "0.6.2"` | Embedded + Remote | Stable |
| **[Python](python)** | `pip install graphmind` | Embedded + Remote | Stable |
| **[TypeScript](typescript)** | `npm install graphmind-sdk` | Remote only | Stable |
| **[REST API](rest-api)** | Any HTTP client | Remote | Stable |
| **[RESP Protocol](resp-protocol)** | Any Redis client | Remote | Stable |

## Choosing a Connection Method

### Embedded Mode (Rust, Python)

The database runs inside your application process. No server needed.

```python
from graphmind import GraphmindClient
client = GraphmindClient.embedded()
client.query('CREATE (n:Person {name: "Alice"})')
```

Best for: CLI tools, data pipelines, testing, single-process applications.

### Remote Mode (all SDKs)

Connect to a running Graphmind server over HTTP.

```python
client = GraphmindClient.connect("http://localhost:8080")
```

Best for: web applications, microservices, multi-client scenarios.

### RESP Protocol (any language)

Use any Redis client library. No Graphmind-specific SDK needed.

```bash
redis-cli -p 6379
127.0.0.1:6379> GRAPH.QUERY default "MATCH (n) RETURN count(n)"
```

Best for: languages without a native SDK, existing Redis infrastructure.

### REST API (any language)

Plain HTTP with JSON. Works from curl, Postman, or any HTTP client.

```bash
curl -X POST http://localhost:8080/api/query \
  -H 'Content-Type: application/json' \
  -d '{"query": "MATCH (n) RETURN count(n)"}'
```

Best for: serverless functions, shell scripts, quick testing.

## Common API Surface

All SDKs implement the same core operations:

| Operation | Description |
|-----------|-------------|
| `query(cypher)` | Execute a read/write Cypher query |
| `query_readonly(cypher)` | Execute a read-only query |
| `schema()` | Introspect labels, edge types, properties |
| `explain(cypher)` | Show the query execution plan |
| `profile(cypher)` | Execute with timing and row stats |
| `execute_script(script)` | Run multiple statements |
| `status()` | Server health and graph stats |
| `ping()` | Connectivity check |
| `list_graphs()` | List all graph namespaces |
| `delete_graph(name)` | Delete a graph namespace |
