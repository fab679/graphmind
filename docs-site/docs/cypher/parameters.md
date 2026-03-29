# Query Parameters

Parameters allow you to safely pass values into Cypher queries without string concatenation. This prevents injection attacks and enables query plan caching.

## Syntax

Use `$paramName` in your query to reference a parameter:

```cypher
MATCH (p:Person {name: $name})
WHERE p.age > $minAge
RETURN p.name, p.age
```

## Supported Locations

Parameters can be used in:

| Location | Example |
|----------|---------|
| Node properties | `MATCH (n:Person {name: $name})` |
| WHERE clauses | `WHERE n.age > $minAge` |
| CREATE properties | `CREATE (n:Person {name: $name, age: $age})` |
| SET values | `SET n.name = $newName` |
| LIMIT/SKIP | `RETURN n LIMIT $limit` |
| Function arguments | `CALL db.index.vector.queryNodes('Movie', 'embedding', $vector, $k)` |

### Edge Properties with Parameters

Use parameters in WHERE clauses to filter edge properties:

```cypher
MATCH (p:Person)-[r:LIVES_IN]->(l:Location)
WHERE r.since = $year
RETURN p.name, l.name
```

> **Note**: Parameters in edge property maps within MATCH patterns (`-[:REL {prop: $val}]->`) are not supported. Use WHERE clauses instead.

## Using Parameters from SDKs

### TypeScript

```typescript
import { GraphmindClient } from 'graphmind-sdk';

const client = new GraphmindClient({ url: 'http://localhost:8080' });

const result = await client.query(
  'MATCH (p:Person {name: $name}) RETURN p.age',
  'default',
  { name: 'Alice' }
);
```

### Python

```python
import graphmind

client = graphmind.GraphmindClient.embedded()

result = client.query_readonly(
    "MATCH (p:Person {name: $name}) RETURN p.age",
    params={"name": "Alice"}
)
```

### Rust SDK

```rust
use graphmind_sdk::RemoteClient;
use std::collections::HashMap;

let client = RemoteClient::new("http://localhost:8080");
let mut params = HashMap::new();
params.insert("name".to_string(), serde_json::json!("Alice"));

let result = client.query_with_params("default",
    "MATCH (p:Person {name: $name}) RETURN p.age",
    params
).await?;
```

### HTTP API (curl)

```bash
curl -X POST http://localhost:8080/api/query \
  -H 'Content-Type: application/json' \
  -d '{
    "query": "MATCH (p:Person {name: $name}) RETURN p.age",
    "params": {"name": "Alice"}
  }'
```

## Parameter Types

| JSON Type | Cypher Type | Example |
|-----------|-------------|---------|
| `string` | STRING | `"Alice"` |
| `number` (int) | INTEGER | `42` |
| `number` (float) | FLOAT | `3.14` |
| `boolean` | BOOLEAN | `true` |
| `null` | NULL | `null` |
| `array` | LIST | `[1, 2, 3]` |

## Common Errors

### MATCH without RETURN

```
MATCH query requires a RETURN clause. Add RETURN at the end.
```

Every `MATCH` query must include a `RETURN` clause (or an updating clause like `CREATE`, `DELETE`, `SET`, or `MERGE`).

```cypher
-- Wrong:
MATCH (p:Person {name: $name})

-- Correct:
MATCH (p:Person {name: $name}) RETURN p
```
