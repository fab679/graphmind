# Vector Search

Graphmind supports HNSW vector indexes for approximate nearest neighbor (ANN) search. This enables semantic search, recommendation systems, and similarity matching directly in your graph queries.

## Creating a Vector Index

```cypher
CREATE VECTOR INDEX movieEmbed
  FOR (n:Movie) ON (n.embedding)
  OPTIONS {dimensions: 384, similarity: 'cosine'}
```

**Options:**
| Option | Description | Values |
|--------|-------------|--------|
| `dimensions` | Vector dimensionality | Any positive integer |
| `similarity` | Distance metric | `cosine`, `euclidean`, `l2` |

## Adding Vectors

Vectors are stored as array properties on nodes:

```cypher
CREATE (m:Movie {
  title: 'The Matrix',
  embedding: [0.12, -0.45, 0.78, ...]
})
```

Vectors are automatically indexed when a matching vector index exists.

## Querying Vectors

### SEARCH Syntax (Recommended)

```cypher
MATCH (movie:Movie)
  SEARCH movie IN (
    VECTOR INDEX movieEmbed
    FOR [0.12, -0.45, 0.78]
    LIMIT 10
  )
RETURN movie.title AS title
```

### CALL Procedure Syntax

```cypher
CALL db.index.vector.queryNodes('Movie', 'embedding', [0.12, -0.45, 0.78], 10)
  YIELD node, score
RETURN node.title, score
ORDER BY score DESC
```

## Managing Vector Indexes

### Show All Vector Indexes

```cypher
SHOW VECTOR INDEXES
```

Returns: `name`, `label`, `property`, `dimensions`, `similarity`, `vectors`, `type`

### Drop a Vector Index

```cypher
DROP VECTOR INDEX Movie_embedding
```

The index name format is `{Label}_{property}` as shown by `SHOW VECTOR INDEXES`.

## Using with Parameters

```cypher
// From application code, pass the query vector as a parameter
CALL db.index.vector.queryNodes('Movie', 'embedding', $queryVector, $k)
  YIELD node, score
RETURN node.title, score
```

## Best Practices

- **Dimensionality**: Match the dimensions to your embedding model (e.g., 384 for `all-MiniLM-L6-v2`, 1536 for OpenAI `text-embedding-3-small`)
- **Cosine similarity** is best for normalized embeddings (most LLM outputs)
- **Euclidean/L2** is better for unnormalized vectors
- Index vectors at creation time for best performance
- Use `LIMIT` in SEARCH to control result set size
