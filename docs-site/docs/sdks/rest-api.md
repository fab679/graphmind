---
sidebar_position: 5
title: REST API
description: HTTP REST API reference for the Graphmind graph database
---

# REST API

Graphmind exposes a REST API on port 8080 (default). Every operation available through the SDKs is also available as an HTTP endpoint.

## Base URL

```
http://localhost:8080
```

Configure with the `--http-port` flag or `GRAPHMIND_HTTP_PORT` environment variable.

## Authentication

When authentication is enabled, include a bearer token in the `Authorization` header:

```
Authorization: Bearer <token>
```

## Endpoints

### POST /api/query

Execute a Cypher query (read or write).

**Request:**
```bash
curl -X POST http://localhost:8080/api/query \
  -H 'Content-Type: application/json' \
  -d '{"query": "MATCH (p:Person) RETURN p.name, p.age", "graph": "default"}'
```

**Response:**
```json
{
  "columns": ["p.name", "p.age"],
  "records": [["Alice", 30], ["Bob", 25]],
  "nodes": [
    {"id": "1", "labels": ["Person"], "properties": {"name": "Alice", "age": 30}},
    {"id": "2", "labels": ["Person"], "properties": {"name": "Bob", "age": 25}}
  ],
  "edges": []
}
```

The `graph` field is optional and defaults to `"default"`.

### POST /api/script

Execute a multi-statement Cypher script. Statements are separated by newlines.

**Request:**
```bash
curl -X POST http://localhost:8080/api/script \
  -H 'Content-Type: application/json' \
  -d '{
    "query": "CREATE (a:Person {name: '\''Alice'\''})\nCREATE (b:Person {name: '\''Bob'\''})"
  }'
```

**Response:**
```json
{
  "status": "ok",
  "executed": 2,
  "errors": [],
  "storage": {"nodes": 2, "edges": 0}
}
```

### POST /api/nlq

Translate a natural-language question to Cypher and execute it. Requires an LLM provider to be configured on the server.

**Request:**
```bash
curl -X POST http://localhost:8080/api/nlq \
  -H 'Content-Type: application/json' \
  -d '{"query": "Who are Alice'\''s friends?", "graph": "default"}'
```

**Response:**
```json
{
  "cypher": "MATCH (a:Person {name:'Alice'})-[:KNOWS]-(b) RETURN a, b",
  "provider": "OpenAI",
  "model": "gpt-4o-mini",
  "columns": ["a", "b"],
  "records": [["Alice", "Bob"]],
  "nodes": [...],
  "edges": [...]
}
```

### GET /api/status

Server health, version, and graph statistics.

**Request:**
```bash
curl http://localhost:8080/api/status
```

**Response:**
```json
{
  "status": "healthy",
  "version": "0.6.2",
  "storage": {
    "nodes": 1042,
    "edges": 3891
  }
}
```

### GET /api/schema

Introspect node labels, edge types, properties, indexes, and statistics.

**Request:**
```bash
curl http://localhost:8080/api/schema
```

**Response:**
```json
{
  "node_types": [
    {"label": "Person", "count": 42, "properties": {"name": "String", "age": "Integer"}},
    {"label": "Company", "count": 5, "properties": {"name": "String"}}
  ],
  "edge_types": [
    {"type": "KNOWS", "count": 120, "source_labels": ["Person"], "target_labels": ["Person"],
     "properties": {"since": "Integer"}}
  ],
  "indexes": [
    {"label": "Person", "property": "name", "type": "btree"}
  ],
  "constraints": [],
  "statistics": {
    "total_nodes": 47,
    "total_edges": 120,
    "avg_out_degree": 2.55
  }
}
```

### POST /api/sample

Sample a subgraph for visualization. Returns a proportional subset of nodes and edges.

**Request:**
```bash
curl -X POST http://localhost:8080/api/sample \
  -H 'Content-Type: application/json' \
  -d '{"max_nodes": 200, "labels": ["Person", "Company"], "graph": "default"}'
```

**Response:**
```json
{
  "nodes": [
    {"id": 1, "label": "Person", "name": "Alice", "properties": {"age": 30}},
    {"id": 2, "label": "Company", "name": "Acme", "properties": {}}
  ],
  "edges": [
    {"id": 100, "source": 1, "target": 2, "type": "WORKS_AT", "properties": {}}
  ],
  "total_nodes": 1042,
  "total_edges": 3891,
  "sampled_nodes": 200,
  "sampled_edges": 312
}
```

All fields in the request body are optional. `max_nodes` defaults to 200 (max 1000).

### POST /api/import/csv

Import nodes from CSV content.

**Request:**
```bash
curl -X POST http://localhost:8080/api/import/csv \
  -H 'Content-Type: application/json' \
  -d '{
    "csv": "name,age,city\nAlice,30,Paris\nBob,25,London",
    "label": "Person",
    "id_column": "name",
    "delimiter": ","
  }'
```

**Response:**
```json
{
  "status": "ok",
  "nodes_created": 2,
  "label": "Person",
  "columns": ["name", "age", "city"]
}
```

### POST /api/import/json

Import nodes from JSON objects.

**Request:**
```bash
curl -X POST http://localhost:8080/api/import/json \
  -H 'Content-Type: application/json' \
  -d '{
    "label": "Person",
    "nodes": [
      {"name": "Alice", "age": 30},
      {"name": "Bob", "age": 25}
    ]
  }'
```

**Response:**
```json
{
  "status": "ok",
  "nodes_created": 2,
  "label": "Person"
}
```

### POST /api/snapshot/export

Export the entire graph as a portable `.sgsnap` snapshot.

**Request:**
```bash
curl -X POST http://localhost:8080/api/snapshot/export \
  -H 'Content-Type: application/json' \
  -d '{"graph": "default"}' \
  --output graph.sgsnap
```

The response body is the binary snapshot file.

### POST /api/snapshot/import

Import a previously exported `.sgsnap` snapshot.

**Request:**
```bash
curl -X POST http://localhost:8080/api/snapshot/import \
  -H 'Content-Type: application/octet-stream' \
  --data-binary @graph.sgsnap
```

**Response:**
```json
{
  "status": "ok",
  "nodes_imported": 1042,
  "edges_imported": 3891
}
```

### GET /api/graphs

List all graph namespaces.

**Request:**
```bash
curl http://localhost:8080/api/graphs
```

**Response:**
```json
{
  "graphs": ["default", "tenant_acme", "tenant_globex"]
}
```

### DELETE /api/graphs/:name

Delete a graph namespace and all its data.

**Request:**
```bash
curl -X DELETE http://localhost:8080/api/graphs/tenant_acme
```

**Response:**
```json
{
  "status": "ok",
  "deleted": "tenant_acme"
}
```

### GET /metrics

Prometheus-compatible metrics endpoint.

**Request:**
```bash
curl http://localhost:8080/metrics
```

**Response (text/plain):**
```
# HELP graphmind_nodes_total Total number of nodes
# TYPE graphmind_nodes_total gauge
graphmind_nodes_total 1042
# HELP graphmind_edges_total Total number of edges
# TYPE graphmind_edges_total gauge
graphmind_edges_total 3891
# HELP graphmind_query_duration_seconds Query execution time
# TYPE graphmind_query_duration_seconds histogram
...
```

## Error Responses

All endpoints return errors in a consistent format:

```json
{
  "error": "Parse error: unexpected token at line 1, column 5"
}
```

HTTP status codes:

| Code | Meaning |
|------|---------|
| 200 | Success |
| 400 | Bad request (invalid Cypher, missing fields) |
| 401 | Unauthorized (missing or invalid token) |
| 404 | Not found (unknown endpoint or graph) |
| 500 | Internal server error |

## Examples in Python (requests)

```python
import requests

BASE = "http://localhost:8080"

# Query
resp = requests.post(f"{BASE}/api/query", json={
    "query": "MATCH (p:Person) RETURN p.name, p.age",
    "graph": "default"
})
data = resp.json()
print(data["records"])

# Status
status = requests.get(f"{BASE}/api/status").json()
print(f"Nodes: {status['storage']['nodes']}")

# Schema
schema = requests.get(f"{BASE}/api/schema").json()
for nt in schema["node_types"]:
    print(f"{nt['label']}: {nt['count']} nodes")

# Import CSV
csv_data = "name,age\nAlice,30\nBob,25"
resp = requests.post(f"{BASE}/api/import/csv", json={
    "csv": csv_data,
    "label": "Person"
})
print(resp.json())
```

## Examples in JavaScript (fetch)

```javascript
const BASE = "http://localhost:8080";

// Query
const result = await fetch(`${BASE}/api/query`, {
  method: "POST",
  headers: { "Content-Type": "application/json" },
  body: JSON.stringify({
    query: "MATCH (p:Person) RETURN p.name, p.age",
    graph: "default",
  }),
}).then((r) => r.json());

console.log(result.records);

// Status
const status = await fetch(`${BASE}/api/status`).then((r) => r.json());
console.log(`Nodes: ${status.storage.nodes}`);

// Schema
const schema = await fetch(`${BASE}/api/schema`).then((r) => r.json());
schema.node_types.forEach((nt) => console.log(`${nt.label}: ${nt.count}`));

// NLQ
const nlq = await fetch(`${BASE}/api/nlq`, {
  method: "POST",
  headers: { "Content-Type": "application/json" },
  body: JSON.stringify({ query: "Who knows Alice?" }),
}).then((r) => r.json());

console.log("Generated Cypher:", nlq.cypher);
```
