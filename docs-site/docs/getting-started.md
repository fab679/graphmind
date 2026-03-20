---
sidebar_position: 1
title: Quick Start
description: Get Graphmind running and execute your first query in 60 seconds
---

# Quick Start

This page gets you from zero to a running Graphmind instance with data you can query.

## 1. Start Graphmind

The fastest way is Docker:

```bash
docker run -d --name graphmind \
  -p 6379:6379 \
  -p 8080:8080 \
  fabischk/graphmind:latest
```

This starts two servers:
- **RESP** on port 6379 (Redis-compatible protocol)
- **HTTP + Web Visualizer** on port 8080

See [Installation](installation/docker) for other options (binary download, building from source).

## 2. Open the Visualizer

Go to [http://localhost:8080](http://localhost:8080) in your browser. You will see the Graphmind web UI with a Cypher editor and an empty graph canvas.

## 3. Run Your First Query

Paste this into the Cypher editor and press **Ctrl+Enter** (or click the Run button):

```cypher
CREATE (alice:Person {name: "Alice", age: 30})
CREATE (bob:Person {name: "Bob", age: 25})
CREATE (carol:Person {name: "Carol", age: 28})
CREATE (alice)-[:KNOWS {since: 2020}]->(bob)
CREATE (bob)-[:KNOWS {since: 2022}]->(carol)
CREATE (alice)-[:KNOWS {since: 2021}]->(carol)
```

Then query the data:

```cypher
MATCH (p:Person)-[:KNOWS]->(friend)
RETURN p.name, friend.name
```

Expected output:

| p.name | friend.name |
|--------|-------------|
| Alice  | Bob         |
| Alice  | Carol       |
| Bob    | Carol       |

## 4. Load Demo Data

Graphmind ships with a social network demo script (52 nodes, 142 edges) that gives you a richer dataset to explore.

**Option A: Via the Web UI**

Click the upload button in the editor toolbar, then select `scripts/social_network_demo.cypher` from the repository.

**Option B: Via the API**

```bash
curl -X POST http://localhost:8080/api/script \
  -H 'Content-Type: application/json' \
  --data-binary @scripts/social_network_demo.cypher
```

**Option C: Via redis-cli**

```bash
redis-cli -p 6379
127.0.0.1:6379> GRAPH.QUERY default "MATCH (n) RETURN labels(n), count(n)"
```

## 5. Explore

After loading the demo data, try these queries:

```cypher
-- Find all people and who they know
MATCH (p:Person)-[:KNOWS]->(friend:Person)
RETURN p.name, collect(friend.name) AS friends

-- Count nodes by label
MATCH (n) RETURN labels(n), count(n) ORDER BY count(n) DESC

-- Find friends-of-friends
MATCH (a:Person {name: "Alice"})-[:KNOWS]->(b)-[:KNOWS]->(c)
WHERE a <> c
RETURN DISTINCT c.name AS friend_of_friend
```

## Next Steps

- **[Cypher Guide](cypher/basics)** -- Learn the query language
- **[Installation](installation/docker)** -- Production setup with persistent storage
- **[Web Visualizer](visualizer/index)** -- Graph exploration features
- **[SDKs](sdks/index)** -- Connect from Rust, Python, TypeScript, or any Redis client
- **[Administration](admin/authentication)** -- Authentication, multi-tenancy, backups
