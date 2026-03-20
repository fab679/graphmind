---
sidebar_position: 100
title: Glossary
description: Key terms and concepts used in Graphmind
---

# Glossary

## Core Concepts

### Property Graph
A graph data model where data is organized as nodes (entities) and edges (relationships). Both nodes and edges can have properties (key-value pairs) and labels/types.

### Node
The fundamental unit of data in a property graph, representing an entity (e.g., Person, Product). Nodes can have multiple **Labels** and **Properties**.

### Edge
A directed connection between two nodes, representing a relationship (e.g., KNOWS, BOUGHT). Edges always have a single **Edge Type** and can have **Properties**.

### Label
A tag applied to a node to categorize it (e.g., `:Person`, `:Vehicle`). Nodes can have multiple labels. Used for indexing and query filtering.

### Properties
Key-value pairs attached to nodes or edges. Keys are strings, and values can be strings, integers, floats, booleans, vectors, etc.

## Architecture

### Raft Consensus
A distributed consensus algorithm used by Graphmind to ensure data consistency and fault tolerance across the cluster. It manages leader election and log replication.

### WAL (Write-Ahead Log)
A persistence technique where modifications are written to a log file before they are applied to the database. Ensures durability and crash recovery.

### RocksDB
An embeddable persistent key-value store used by Graphmind as the underlying storage engine for graph data.

## Multi-Tenancy

### Tenant
A logical isolation boundary within the database. Each tenant (graph namespace) has its own data, indices, and isolation from other tenants.

### Graph Namespace
A named container for graph data. The default namespace is `default`. Create additional namespaces by specifying a different graph name in queries.

## AI & Vector Search

### Vector Embedding
A list of floating-point numbers (e.g., `[0.1, 0.5, -0.9]`) representing the semantic meaning of text, images, or other data. Used for similarity search.

### HNSW (Hierarchical Navigable Small World)
A graph-based index structure used for efficient Approximate Nearest Neighbor (ANN) search on vector embeddings. Provides sub-millisecond search latency.

### NLQ (Natural Language Querying)
A feature allowing users to query the graph using plain language. The system uses an LLM to translate the request into an executable Cypher query.

### GAK (Generation-Augmented Knowledge)
The inverse of RAG -- using LLMs to *build* the database rather than *query* it. The database acts as an agent to fetch, structure, and persist missing information on-demand.

## Query Languages

### OpenCypher
A declarative graph query language that uses ASCII-art style patterns (e.g., `(a)-[:KNOWS]->(b)`) to describe and query graph structures.

## Protocols

### RESP (Redis Serialization Protocol)
The wire protocol used by Redis. Graphmind implements RESP3, allowing any Redis client to connect and issue graph commands.

## Performance & Internals

### Late Materialization
An optimization where operators pass lightweight references (`NodeRef(NodeId)`) through the pipeline instead of full node copies. Properties are resolved lazily only when needed. Yields 4-5x improvement in multi-hop query latency.

### Volcano Iterator Model
A query execution model where each operator (scan, filter, expand, project) implements a `next()` method. Operators pull records one at a time from their children, avoiding large intermediate materializations.

### MVCC (Multi-Version Concurrency Control)
A concurrency control method where each node/edge maintains version history. Readers access consistent snapshots via `get_node_at_version()` without blocking writers.

### CSR (Compressed Sparse Row)
A memory-efficient representation for graph adjacency. Stores all edge targets in a single contiguous array with an offset index per node, improving cache locality for traversals.
