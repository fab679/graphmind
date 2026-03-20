---
sidebar_position: 4
title: API Reference
description: HTTP REST API endpoints
---

# API Reference

Graphmind exposes a REST API on port 8080.

## Endpoints

| Method | Path | Description |
|--------|------|-------------|
| POST | `/api/query` | Execute a single Cypher query |
| POST | `/api/script` | Execute multi-statement Cypher script |
| POST | `/api/nlq` | Natural language to Cypher translation |
| GET | `/api/status` | Server health and statistics |
| GET | `/api/schema` | Schema introspection |
| POST | `/api/sample` | Sample a subgraph |
| POST | `/api/import/csv` | Import from CSV |
| POST | `/api/import/json` | Import from JSON |
| POST | `/api/snapshot/export` | Export snapshot |
| POST | `/api/snapshot/import` | Import snapshot |

## POST /api/query

Execute a single Cypher query.

**Request:**
```json
{
  "query": "MATCH (n:Person) RETURN n.name, n.age",
  "graph": "default"
}
```

**Response:**
```json
{
  "nodes": [],
  "edges": [],
  "columns": ["n.name", "n.age"],
  "records": [["Alice", 30], ["Bob", 25]]
}
```

## POST /api/script

Execute multiple Cypher statements. Splits on newlines, skips comments (`//`, `--`) and blank lines.

**Request:**
```json
{
  "query": "CREATE (a:Person {name: 'Alice'})\nCREATE (b:Person {name: 'Bob'})",
  "graph": "default"
}
```

**Response:**
```json
{
  "status": "ok",
  "executed": 2,
  "errors": [],
  "storage": { "nodes": 2, "edges": 0 }
}
```

## POST /api/nlq

Translate natural language to Cypher using an LLM provider.

Requires environment variable: `OPENAI_API_KEY`, `GEMINI_API_KEY`, or `CLAUDE_CODE_NLQ=1`.

**Request:**
```json
{ "query": "Who are Alice's friends?" }
```

**Response:**
```json
{
  "cypher": "MATCH (a:Person {name: 'Alice'})-[:FRIENDS_WITH]-(b) RETURN a, b",
  "provider": "OpenAI",
  "model": "gpt-4o-mini"
}
```

## GET /api/status

**Response:**
```json
{
  "status": "healthy",
  "version": "0.6.1",
  "storage": { "nodes": 52, "edges": 142 },
  "cache": { "hits": 10, "misses": 5, "size": 5 }
}
```

## GET /api/schema

**Response:**
```json
{
  "node_types": [{ "label": "Person", "count": 16, "properties": { "name": "String", "age": "Integer" } }],
  "edge_types": [{ "type": "KNOWS", "count": 18, "source_labels": ["Person"], "target_labels": ["Person"] }],
  "indexes": [],
  "constraints": [],
  "statistics": { "total_nodes": 52, "total_edges": 142, "avg_out_degree": 2.73 }
}
```
