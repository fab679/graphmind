---
sidebar_position: 1
title: SDK Overview
description: Client libraries for Graphmind in Rust, Python, TypeScript, and any Redis-compatible language
---

# SDKs & Client Libraries

Graphmind provides native SDKs for three languages plus compatibility with any Redis client library.

| SDK | Install | Mode | Status |
|-----|---------|------|--------|
| **Rust** | `graphmind-sdk = "0.6.2"` | Embedded + Remote | Stable |
| **Python** | `pip install graphmind` | Embedded + Remote | Stable |
| **TypeScript** | `npm install graphmind-sdk` | Remote only | Stable |
| **Any language** | Any Redis client | Remote (RESP) | Stable |
| **Any language** | HTTP client | Remote (REST) | Stable |

## Choosing an SDK

- **Embedded mode** (Rust, Python): The database runs inside your application process. No server needed. Best for: CLI tools, data pipelines, testing, single-process apps.
- **Remote mode** (all SDKs): Connects to a running Graphmind server via HTTP or RESP. Best for: web apps, microservices, multi-client scenarios.
- **RESP protocol**: Any Redis client library works. Best for: languages without a native SDK, existing Redis infrastructure.
- **REST API**: Plain HTTP. Best for: serverless functions, shell scripts, any HTTP client.

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
