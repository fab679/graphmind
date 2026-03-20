---
sidebar_position: 4
title: Configuration
description: Configuration file, environment variables, and CLI flags
---

# Configuration

Graphmind is configured through three layers (later layers override earlier ones):

1. **Config file** (TOML) -- defaults to `graphmind.toml` in the working directory
2. **CLI flags** -- override config file values
3. **Environment variables** -- override everything

## Config File

Create a `graphmind.toml`:

```toml
[server]
host = "127.0.0.1"       # Bind address
resp_port = 6379          # RESP protocol port
http_port = 8080          # HTTP API and Visualizer port
data_dir = "./graphmind_data"  # RocksDB data directory

[logging]
level = "info"            # debug, info, warn, error

[auth]
enabled = false           # Enable token-based authentication
tokens = [                # Allowed bearer tokens
  "my-secret-token-1",
  "my-secret-token-2",
]
```

Specify a config file path:

```bash
graphmind --config /etc/graphmind/graphmind.toml
```

If the file does not exist, Graphmind starts with defaults (no error).

## CLI Flags

| Flag | Config key | Default | Description |
|------|-----------|---------|-------------|
| `--config <path>` | -- | `graphmind.toml` | Config file path |
| `--host <addr>` | `server.host` | `127.0.0.1` | Bind address |
| `--port <port>` | `server.resp_port` | `6379` | RESP server port |
| `--http-port <port>` | `server.http_port` | `8080` | HTTP server port |
| `--data-dir <path>` | `server.data_dir` | `./graphmind_data` | Data directory |
| `--log-level <level>` | `logging.level` | `info` | Log level |
| `--demo <mode>` | -- | (none) | Load demo data on startup |

## Environment Variables

| Variable | Config key | Description |
|----------|-----------|-------------|
| `GRAPHMIND_HOST` | `server.host` | Bind address |
| `GRAPHMIND_PORT` | `server.resp_port` | RESP port |
| `GRAPHMIND_HTTP_PORT` | `server.http_port` | HTTP port |
| `GRAPHMIND_DATA_DIR` | `server.data_dir` | Data directory |
| `GRAPHMIND_AUTH_TOKEN` | `auth.tokens` | Single auth token (enables auth) |
| `RUST_LOG` | `logging.level` | Log level filter |

### NLQ Provider Variables

These enable natural language query translation. Set one:

| Variable | Provider | Default Model |
|----------|----------|---------------|
| `OPENAI_API_KEY` | OpenAI | `gpt-4o-mini` (override with `OPENAI_MODEL`) |
| `GEMINI_API_KEY` | Google Gemini | `gemini-2.0-flash` (override with `GEMINI_MODEL`) |
| `CLAUDE_CODE_NLQ=1` | Claude Code CLI | Uses local Claude Code |

## Example Configurations

### Development

```toml
[server]
host = "127.0.0.1"
resp_port = 6379
http_port = 8080

[logging]
level = "debug"
```

### Production (Single Node)

```toml
[server]
host = "0.0.0.0"
resp_port = 6379
http_port = 8080
data_dir = "/var/lib/graphmind/data"

[logging]
level = "info"

[auth]
enabled = true
tokens = ["prod-token-abc123"]
```

### Docker / Kubernetes

When running in a container, use environment variables instead of a config file:

```bash
GRAPHMIND_HOST=0.0.0.0
GRAPHMIND_PORT=6379
GRAPHMIND_HTTP_PORT=8080
GRAPHMIND_DATA_DIR=/data
GRAPHMIND_AUTH_TOKEN=prod-token-abc123
RUST_LOG=info
```

## Precedence

If the same setting is specified in multiple places, the order is:

```
Environment variable > CLI flag > Config file > Default
```

For example, if the config file says `resp_port = 6379`, the CLI flag `--port 7379` overrides it, and `GRAPHMIND_PORT=8379` overrides both.
