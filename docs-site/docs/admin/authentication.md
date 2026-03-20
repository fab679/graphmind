---
sidebar_position: 1
title: Authentication
description: Configure token-based authentication for Graphmind
---

# Authentication

Graphmind supports token-based authentication for both the HTTP API and the RESP protocol. Authentication is disabled by default.

## Enabling Authentication

### Via Environment Variable

The simplest way -- set a single token:

```bash
GRAPHMIND_AUTH_TOKEN=my-secret-token graphmind
```

Or with Docker:

```bash
docker run -d -p 6379:6379 -p 8080:8080 \
  -e GRAPHMIND_AUTH_TOKEN=my-secret-token \
  fabischk/graphmind:latest
```

### Via Config File

For multiple tokens, use the config file:

```toml
[auth]
enabled = true
tokens = [
  "token-for-admin",
  "token-for-app-server",
  "token-for-analytics",
]
```

## Using Authentication

### HTTP API

Include the token as a Bearer token in the `Authorization` header:

```bash
curl -X POST http://localhost:8080/api/query \
  -H 'Authorization: Bearer my-secret-token' \
  -H 'Content-Type: application/json' \
  -d '{"query": "MATCH (n) RETURN count(n)"}'
```

Without a valid token, you get a `401 Unauthorized` response:

```json
{"error": "Unauthorized"}
```

### RESP Protocol

Authenticate after connecting with the `AUTH` command:

```bash
redis-cli -p 6379
127.0.0.1:6379> AUTH my-secret-token
OK
127.0.0.1:6379> GRAPH.QUERY default "MATCH (n) RETURN count(n)"
```

Without authentication, commands return an error:

```
(error) NOAUTH Authentication required
```

### Web Visualizer

When auth is enabled, the web UI at `http://localhost:8080` shows a token entry dialog on first load. Enter your token to authenticate. The token is stored in the browser's localStorage.

### Python SDK

```python
from graphmind import GraphmindClient

client = GraphmindClient.connect("http://localhost:8080", token="my-secret-token")
result = client.query_readonly("MATCH (n) RETURN count(n)")
```

### TypeScript SDK

```typescript
import { GraphmindClient } from "graphmind-sdk";

const client = new GraphmindClient({
  url: "http://localhost:8080",
  token: "my-secret-token",
});
```

### Rust SDK

```rust
use graphmind_sdk::RemoteClient;

let client = RemoteClient::new("http://localhost:8080")
    .with_token("my-secret-token");
```

## Security Notes

- Tokens are compared as plain strings. Use long, random tokens (e.g., `openssl rand -hex 32`).
- Tokens are sent in HTTP headers and RESP commands. Use TLS (a reverse proxy like nginx or Caddy) in production to encrypt traffic.
- When `auth.enabled = false` (the default), all requests are accepted without a token.
- The `GRAPHMIND_AUTH_TOKEN` environment variable takes precedence: if set, it enables auth with that single token regardless of the config file.
