---
sidebar_position: 1
title: Vector Search
description: HNSW vector indexes for similarity search
---

# Vector Search

Graphmind includes a built-in HNSW (Hierarchical Navigable Small World) vector index for approximate nearest neighbor search. This lets you store embeddings alongside graph data and combine vector similarity with graph traversal.

## Creating a Vector Index

Create an index on a specific label and property, specifying the vector dimensionality and distance metric:

```cypher
CREATE VECTOR INDEX ON :Document(embedding)
OPTIONS {dimension: 384, metric: 'cosine'}
```

Supported distance metrics:
- `cosine` -- cosine similarity (most common for text embeddings)
- `l2` -- Euclidean distance
- `dot` -- dot product

### Via SDK (Embedded Mode)

**Python:**

```python
client.create_vector_index("Document", "embedding", dimensions=384, metric="cosine")
```

**Rust:**

```rust
use graphmind_sdk::{EmbeddedClient, VectorClient, DistanceMetric};

let client = EmbeddedClient::new();
client.create_vector_index("Document", "embedding", 384, DistanceMetric::Cosine).await?;
```

## Inserting Vectors

After creating nodes, add vectors to them:

### Via SDK

**Python:**

```python
# Create a node first
client.query('CREATE (d:Document {title: "Graph Databases"})')

# Get the node ID
result = client.query_readonly('MATCH (d:Document {title: "Graph Databases"}) RETURN id(d)')
node_id = result.records[0][0]

# Add the vector
embedding = [0.1, 0.2, -0.3, ...]  # 384 dimensions
client.add_vector("Document", "embedding", node_id=node_id, vector=embedding)
```

**Rust:**

```rust
client.add_vector("Document", "embedding", node_id, &embedding_vec).await?;
```

## k-NN Search

Find the k nearest neighbors to a query vector:

### Via Cypher

```cypher
CALL db.index.vector.queryNodes('Document', 'embedding', 10, [0.15, 0.25, ...])
YIELD node, score
RETURN node.title, score
ORDER BY score ASC
```

### Via SDK

**Python:**

```python
query_vector = [0.15, 0.25, ...]  # same dimensionality as the index
results = client.vector_search("Document", "embedding", query_vector=query_vector, k=10)
for node_id, distance in results:
    print(f"Node {node_id}: distance {distance:.4f}")
```

**Rust:**

```rust
let results = client.vector_search("Document", "embedding", &query_vec, 10).await?;
for (node_id, distance) in results {
    println!("Node {:?} at distance {:.4}", node_id, distance);
}
```

## Hybrid Queries

Combine vector search with Cypher graph traversal for powerful hybrid queries:

```cypher
-- Find similar documents, then traverse to their authors
CALL db.index.vector.queryNodes('Document', 'embedding', 5, [0.15, 0.25, ...])
YIELD node, score
MATCH (node)<-[:WROTE]-(author:Person)
RETURN node.title, score, author.name
ORDER BY score ASC
```

```cypher
-- Find documents similar to a query, then find related documents by shared tags
CALL db.index.vector.queryNodes('Document', 'embedding', 3, [0.15, 0.25, ...])
YIELD node, score
MATCH (node)-[:HAS_TAG]->(t:Tag)<-[:HAS_TAG]-(related:Document)
WHERE related <> node
RETURN DISTINCT related.title, collect(t.name) AS shared_tags
```

## Use Cases

| Use Case | How |
|----------|-----|
| **Semantic search** | Embed document text, search by meaning |
| **Recommendation** | Find similar users/products by embedding features |
| **RAG (Retrieval-Augmented Generation)** | Store knowledge base embeddings, retrieve context for LLMs |
| **Deduplication** | Find near-duplicate records by vector distance |
| **Image similarity** | Store image embeddings, find visually similar items |

## Performance

The HNSW index provides sub-millisecond search latency on datasets up to 1M vectors. Index construction time scales linearly with the number of vectors.

## Limitations

- Vector search methods (`create_vector_index`, `add_vector`, `vector_search`) are available only in **embedded mode** (Rust and Python SDKs). The HTTP/RESP protocols support vector queries via Cypher `CALL` syntax.
- Vectors must all have the same dimensionality within an index.
- The index is held in memory. Memory usage is approximately `dimension * 4 bytes * num_vectors` plus HNSW graph overhead.
