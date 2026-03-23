---
sidebar_position: 1
title: Authentication
description: Configure authentication and user roles for Graphmind
---

# Authentication

Graphmind supports two authentication modes: **token-based** and **username/password**. Authentication is disabled by default — enable it when deploying to production.

## Enabling Authentication

### Username/Password (Recommended)

Set an admin user during startup:

```bash
GRAPHMIND_ADMIN_USER=admin GRAPHMIND_ADMIN_PASSWORD=secret graphmind
```

Or with Docker:

```bash
docker run -d -p 6379:6379 -p 8080:8080 \
  -e GRAPHMIND_ADMIN_USER=admin \
  -e GRAPHMIND_ADMIN_PASSWORD=secret \
  fabischk/graphmind:latest
```

### Token-Based (Simple)

For scripts and CI/CD, set a single token:

```bash
GRAPHMIND_AUTH_TOKEN=my-secret-token graphmind
```

### Combined

Both can be used together:

```bash
GRAPHMIND_ADMIN_USER=admin GRAPHMIND_ADMIN_PASSWORD=secret \
GRAPHMIND_AUTH_TOKEN=api-token-for-scripts \
graphmind
```

## Roles

| Role | Read | Write | Admin |
|------|------|-------|-------|
| `Admin` | Yes | Yes | Yes (manage users, delete graphs) |
| `ReadWrite` | Yes | Yes | No |
| `ReadOnly` | Yes | No | No |

The initial user created via `GRAPHMIND_ADMIN_USER` is always an `Admin`.

## Using Authentication

### HTTP API — Basic Auth

```bash
curl -u admin:secret -X POST http://localhost:8080/api/query \
  -H 'Content-Type: application/json' \
  -d '{"query": "MATCH (n) RETURN count(n)"}'
```

### HTTP API — Bearer Token

```bash
curl -X POST http://localhost:8080/api/query \
  -H 'Authorization: Bearer my-secret-token' \
  -H 'Content-Type: application/json' \
  -d '{"query": "MATCH (n) RETURN count(n)"}'
```

### HTTP API — Login Endpoint

For sessions, use the login endpoint:

```bash
curl -X POST http://localhost:8080/api/auth/login \
  -H 'Content-Type: application/json' \
  -d '{"username": "admin", "password": "secret"}'
```

Response:
```json
{"authenticated": true, "role": "Admin", "username": "admin"}
```

### RESP Protocol

Single-arg (token):
```bash
redis-cli -p 6379
127.0.0.1:6379> AUTH my-secret-token
OK
```

Two-arg (username/password):
```bash
127.0.0.1:6379> AUTH admin secret
OK
127.0.0.1:6379> GRAPH.QUERY default "MATCH (n) RETURN count(n)"
```

### Web Visualizer

When auth is enabled, the UI shows a login screen on first load:

1. Enter the server URL (default: `http://localhost:8080`)
2. Enter username and password
3. Click **Connect**
4. Credentials are stored in the browser for subsequent requests

If auth is not enabled, click **Skip** to connect directly.

### Python SDK

```python
from graphmind import GraphmindClient

# Username/password
client = GraphmindClient.connect("http://localhost:8080",
    username="admin", password="secret")

# Or token
client = GraphmindClient.connect("http://localhost:8080",
    token="my-secret-token")
```

### TypeScript SDK

```typescript
import { GraphmindClient } from "graphmind-sdk";

const client = new GraphmindClient({
  url: "http://localhost:8080",
  token: "my-secret-token",  // or use Basic auth
});
```

### Rust SDK

```rust
use graphmind_sdk::RemoteClient;

let client = RemoteClient::new("http://localhost:8080")
    .with_token("my-secret-token");
```

## Managing Users

### List Users (Admin only)

```bash
curl -u admin:secret http://localhost:8080/api/auth/users
```

```json
[["admin", "Admin"]]
```

### Create Users (Admin only)

```bash
curl -u admin:secret -X POST http://localhost:8080/api/auth/users \
  -H 'Content-Type: application/json' \
  -d '{"username": "analyst", "password": "pass123", "role": "ReadOnly"}'
```

Roles: `Admin`, `ReadWrite`, `ReadOnly`.

## Security Notes

- Passwords are hashed before storage (not stored in plaintext)
- Use TLS in production (reverse proxy with nginx or Caddy) to encrypt credentials in transit
- When auth is disabled (default), all requests are accepted without credentials
- The `GRAPHMIND_AUTH_TOKEN` and `GRAPHMIND_ADMIN_USER` environment variables activate auth on startup
