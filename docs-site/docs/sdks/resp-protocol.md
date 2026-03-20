---
sidebar_position: 6
title: RESP Protocol
description: Use Graphmind with any Redis client via the RESP wire protocol
---

# RESP Protocol

Graphmind implements the Redis Serialization Protocol (RESP), so any Redis client library can connect to it. The RESP server runs on port 6379 by default alongside the HTTP API on port 8080.

## What is RESP?

RESP is the wire protocol used by Redis. Graphmind supports RESP3, which means you can use standard Redis clients (redis-cli, redis-py, ioredis, go-redis, Jedis, etc.) to send graph commands without installing a Graphmind-specific SDK.

## Connecting

### redis-cli

```bash
redis-cli -p 6379
127.0.0.1:6379> PING
PONG
127.0.0.1:6379> GRAPH.QUERY default "MATCH (n) RETURN count(n)"
```

### Connection URL

```
redis://localhost:6379
```

## Commands

### PING

Health check.

```
PING
-> PONG
```

### INFO

Server information.

```
INFO
-> (server info string)
```

### GRAPH.QUERY

Execute a read/write Cypher query against a named graph.

```
GRAPH.QUERY <graph_name> "<cypher_query>"
```

**Example:**
```
GRAPH.QUERY default "CREATE (n:Person {name: 'Alice', age: 30})"
GRAPH.QUERY default "MATCH (p:Person) RETURN p.name, p.age"
```

The response is a RESP array containing:
1. Column headers
2. Result rows
3. Query statistics

### GRAPH.RO_QUERY

Execute a read-only Cypher query. Identical to `GRAPH.QUERY` but the server may optimize for concurrent reads.

```
GRAPH.RO_QUERY <graph_name> "<cypher_query>"
```

**Example:**
```
GRAPH.RO_QUERY default "MATCH (p:Person) WHERE p.age > 25 RETURN p.name"
```

### GRAPH.DELETE

Delete all data in a named graph.

```
GRAPH.DELETE <graph_name>
```

**Example:**
```
GRAPH.DELETE test_graph
-> OK
```

### GRAPH.LIST

List all graph namespaces.

```
GRAPH.LIST
-> 1) "default"
   2) "tenant_acme"
```

## Client Examples

### Python (redis-py)

```python
import redis

r = redis.Redis(host="localhost", port=6379, decode_responses=True)

# Ping
print(r.ping())  # True

# Create data
r.execute_command("GRAPH.QUERY", "default",
    "CREATE (a:Person {name: 'Alice', age: 30})")
r.execute_command("GRAPH.QUERY", "default",
    "CREATE (b:Person {name: 'Bob', age: 25})")
r.execute_command("GRAPH.QUERY", "default",
    "MATCH (a:Person {name: 'Alice'}), (b:Person {name: 'Bob'}) "
    "CREATE (a)-[:KNOWS]->(b)")

# Query
result = r.execute_command("GRAPH.RO_QUERY", "default",
    "MATCH (p:Person) RETURN p.name, p.age")
print(result)

# Delete graph
r.execute_command("GRAPH.DELETE", "test_graph")
```

### Node.js (ioredis)

```javascript
import Redis from "ioredis";

const redis = new Redis(6379, "localhost");

// Create data
await redis.call("GRAPH.QUERY", "default",
  "CREATE (a:Person {name: 'Alice', age: 30})");
await redis.call("GRAPH.QUERY", "default",
  "CREATE (b:Person {name: 'Bob', age: 25})");

// Query
const result = await redis.call("GRAPH.RO_QUERY", "default",
  "MATCH (p:Person) RETURN p.name, p.age ORDER BY p.age");
console.log(result);

// List graphs
const graphs = await redis.call("GRAPH.LIST");
console.log(graphs);

await redis.quit();
```

### Go (go-redis)

```go
package main

import (
    "context"
    "fmt"
    "github.com/redis/go-redis/v9"
)

func main() {
    ctx := context.Background()
    rdb := redis.NewClient(&redis.Options{
        Addr: "localhost:6379",
    })

    // Create data
    rdb.Do(ctx, "GRAPH.QUERY", "default",
        "CREATE (a:Person {name: 'Alice', age: 30})")

    // Query
    result, err := rdb.Do(ctx, "GRAPH.RO_QUERY", "default",
        "MATCH (p:Person) RETURN p.name, p.age").Result()
    if err != nil {
        panic(err)
    }
    fmt.Println(result)

    // List graphs
    graphs, _ := rdb.Do(ctx, "GRAPH.LIST").StringSlice()
    fmt.Println(graphs)
}
```

### Java (Jedis)

```java
import redis.clients.jedis.Jedis;
import java.util.List;

public class GraphmindExample {
    public static void main(String[] args) {
        try (Jedis jedis = new Jedis("localhost", 6379)) {
            // Create data
            jedis.sendCommand(() -> "GRAPH.QUERY".getBytes(),
                "default", "CREATE (a:Person {name: 'Alice', age: 30})");

            // Query
            Object result = jedis.sendCommand(() -> "GRAPH.RO_QUERY".getBytes(),
                "default", "MATCH (p:Person) RETURN p.name, p.age");
            System.out.println(result);

            // List graphs
            Object graphs = jedis.sendCommand(() -> "GRAPH.LIST".getBytes());
            System.out.println(graphs);
        }
    }
}
```

### Ruby

```ruby
require "redis"

r = Redis.new(host: "localhost", port: 6379)

# Create data
r.call("GRAPH.QUERY", "default",
  "CREATE (a:Person {name: 'Alice', age: 30})")

# Query
result = r.call("GRAPH.RO_QUERY", "default",
  "MATCH (p:Person) RETURN p.name, p.age")
puts result

# List graphs
puts r.call("GRAPH.LIST")
```

### C# (.NET)

```csharp
using StackExchange.Redis;

var redis = ConnectionMultiplexer.Connect("localhost:6379");
var db = redis.GetDatabase();

// Create data
db.Execute("GRAPH.QUERY", "default",
    "CREATE (a:Person {name: 'Alice', age: 30})");

// Query
var result = db.Execute("GRAPH.RO_QUERY", "default",
    "MATCH (p:Person) RETURN p.name, p.age");
Console.WriteLine(result);

// List graphs
var graphs = db.Execute("GRAPH.LIST");
Console.WriteLine(graphs);
```

## When to Use RESP vs REST

| Criteria | RESP (port 6379) | REST (port 8080) |
|----------|------------------|-------------------|
| Latency | Lower (binary protocol, persistent TCP) | Higher (HTTP overhead per request) |
| Client support | Any Redis client in any language | Any HTTP client |
| Connection pooling | Built into most Redis clients | Must be managed manually |
| Streaming | Persistent connection | Request/response |
| Import/Export | Not supported | CSV, JSON, snapshot import/export |
| NLQ | Not supported | POST /api/nlq |
| Schema introspection | Not supported | GET /api/schema |
| Sampling | Not supported | POST /api/sample |
| Monitoring | INFO command | GET /metrics (Prometheus) |

**Use RESP when:** You need the lowest latency, already have Redis infrastructure, or your language lacks a native Graphmind SDK.

**Use REST when:** You need import/export, NLQ, schema introspection, sampling, or Prometheus metrics. Also the better choice for serverless or short-lived processes where persistent TCP connections are impractical.
