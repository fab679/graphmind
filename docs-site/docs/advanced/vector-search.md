---
sidebar_position: 1
title: Vector Search
description: HNSW vector indexes for similarity search
---

# Vector Search

Graphmind includes a built-in HNSW (Hierarchical Navigable Small World) vector index for approximate nearest neighbor search. This lets you store embeddings alongside graph data and combine vector similarity with graph traversal.

## Creating a Vector Index

Create an index on a specific label and property, specifying the vector dimensionality and similarity metric:

```cypher
CREATE VECTOR INDEX myIdx FOR (n:Document) ON (n.embedding) OPTIONS {dimensions: 384, similarity: 'cosine'}
```

- **index name** -- a unique identifier for the index (e.g., `myIdx`)
- **FOR (variable:Label)** -- the node label to index
- **ON (variable.property)** -- the property containing vector embeddings
- **OPTIONS** -- `dimensions` (integer) and `similarity` (`'cosine'` or `'l2'`)

Supported similarity metrics:
- `cosine` -- cosine similarity (most common for text embeddings)
- `l2` -- Euclidean distance

## Listing Vector Indexes

```cypher
-- Show only vector indexes (with dimensions, similarity, and vector count)
SHOW VECTOR INDEXES

-- Show all indexes (property + vector)
SHOW INDEXES
```

`SHOW VECTOR INDEXES` returns: `name`, `label`, `property`, `dimensions`, `similarity`, `vectors`, `type`.

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

## SEARCH Clause (Recommended)

The `SEARCH` clause is used within `MATCH` and `OPTIONAL MATCH` to constrain patterns using approximate nearest neighbor (ANN) vector search. This is the recommended way to do vector search in Graphmind.

### Syntax

```cypher
[OPTIONAL] MATCH pattern
  SEARCH binding_variable IN (
    VECTOR INDEX index_name
    FOR query_vector
    [WHERE filter_predicate]
    LIMIT top_k
  ) [SCORE AS score_alias]
```

- **binding_variable** -- must match a node variable from the MATCH pattern
- **index_name** -- name of an existing vector index (or the label it indexes)
- **query_vector** -- a vector literal like `[1, 2, 3]`, a parameter `$embedding`, or a property reference
- **WHERE** (optional) -- in-index filter applied during the search (only property predicates with AND)
- **LIMIT** -- number of approximate nearest neighbors to return
- **SCORE AS** (optional) -- returns the similarity score (0.0 to 1.0, higher = more similar) as a named column

### Basic Vector Search

```cypher
-- Find the 4 most similar movies to the query vector
MATCH (movie:Movie)
  SEARCH movie IN (
    VECTOR INDEX moviePlots
    FOR [1, 2, 3]
    LIMIT 4
  )
RETURN movie.title AS title
```

### With Similarity Score

```cypher
MATCH (movie:Movie)
  SEARCH movie IN (
    VECTOR INDEX moviePlots
    FOR [1, 2, 3]
    LIMIT 4
  ) SCORE AS similarityScore
RETURN movie.title AS title, similarityScore
```

Similarity scores are `FLOAT` values between 0.0 and 1.0. A score of 1.0 means the vectors are identical.

### With In-Index Filtering

Filter during the vector search itself (more efficient than post-filtering):

```cypher
MATCH (movie:Movie)
  SEARCH movie IN (
    VECTOR INDEX moviePlots
    FOR [1, 2, 3]
    WHERE movie.rating > 7.5
    LIMIT 4
  )
RETURN movie.title, movie.rating
```

### With Post-Filtering (WHERE outside SEARCH)

Post-filter results after the vector search:

```cypher
MATCH (movie:Movie)
  SEARCH movie IN (
    VECTOR INDEX moviePlots
    FOR [1, 2, 3]
    LIMIT 10
  )
  WHERE movie.rating > 8.0
RETURN movie.title, movie.rating
```

### Using a Property as Query Vector

```cypher
MATCH (snowWhite:Movie {title: 'Snow White'})
MATCH (movie:Movie)
  SEARCH movie IN (
    VECTOR INDEX moviePlots
    FOR snowWhite.embedding
    LIMIT 4
  )
RETURN movie.title AS title
```

### OPTIONAL MATCH with SEARCH

If the query vector evaluates to null (e.g., a non-existing property), `OPTIONAL MATCH` returns null rows:

```cypher
MATCH (m:Movie {title: 'Snow White'})
OPTIONAL MATCH (movie:Movie)
  SEARCH movie IN (
    VECTOR INDEX moviePlots
    FOR m.nonExistentProp
    LIMIT 4
  )
RETURN movie.title
```

## CALL Procedure (Legacy)

The `CALL db.index.vector.queryNodes` procedure is still supported but the `SEARCH` clause is preferred:

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

Combine vector search with Cypher graph traversal:

```cypher
-- Find similar documents, then traverse to their authors
MATCH (doc:Document)
  SEARCH doc IN (
    VECTOR INDEX doc_embeddings
    FOR [0.15, 0.25, 0.35]
    LIMIT 5
  ) SCORE AS similarity
MATCH (doc)<-[:WROTE]-(author:Person)
RETURN doc.title, similarity, author.name
ORDER BY similarity DESC
```

```cypher
-- Find documents similar to a query, then find related documents by shared tags
MATCH (doc:Document)
  SEARCH doc IN (
    VECTOR INDEX doc_embeddings
    FOR [0.15, 0.25, 0.35]
    LIMIT 3
  )
MATCH (doc)-[:HAS_TAG]->(t:Tag)<-[:HAS_TAG]-(related:Document)
WHERE related <> doc
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

- Vectors must all have the same dimensionality within an index.
- The index is held in memory. Memory usage is approximately `dimensions * 4 bytes * num_vectors` plus HNSW graph overhead.
- The `SEARCH` clause's in-index `WHERE` filter only supports property predicates joined with `AND`. `OR`, `NOT`, `IN`, and string operators are not supported in the in-index filter (use post-filtering with an outer `WHERE` instead).
- The `SEARCH` clause pattern must have exactly one bound variable (the binding variable).
