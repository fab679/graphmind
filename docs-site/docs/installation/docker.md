---
sidebar_position: 1
title: Docker
description: Run Graphmind with Docker or Docker Compose
---

# Docker Installation

## Quick Start

```bash
docker run -d --name graphmind \
  -p 6379:6379 \
  -p 8080:8080 \
  fabischk/graphmind:latest
```

Ports:
- `6379` -- RESP server (Redis-compatible)
- `8080` -- HTTP API and Web Visualizer

## Persistent Storage

Mount a volume so data survives container restarts:

```bash
docker run -d --name graphmind \
  -p 6379:6379 \
  -p 8080:8080 \
  -v graphmind_data:/data \
  fabischk/graphmind:latest
```

Graphmind stores its RocksDB data in `/data` inside the container.

## Environment Variables

| Variable | Default | Description |
|----------|---------|-------------|
| `GRAPHMIND_HOST` | `0.0.0.0` | Bind address |
| `GRAPHMIND_PORT` | `6379` | RESP server port |
| `GRAPHMIND_HTTP_PORT` | `8080` | HTTP/Visualizer port |
| `GRAPHMIND_DATA_DIR` | `/data` | Data directory |
| `GRAPHMIND_AUTH_TOKEN` | (none) | Enable auth with this bearer token |
| `RUST_LOG` | `info` | Log level (`debug`, `info`, `warn`, `error`) |
| `OPENAI_API_KEY` | (none) | Enable NLQ with OpenAI |
| `GEMINI_API_KEY` | (none) | Enable NLQ with Google Gemini |

Example with authentication and NLQ enabled:

```bash
docker run -d --name graphmind \
  -p 6379:6379 \
  -p 8080:8080 \
  -v graphmind_data:/data \
  -e GRAPHMIND_AUTH_TOKEN=my-secret-token \
  -e OPENAI_API_KEY=sk-... \
  fabischk/graphmind:latest
```

## Docker Compose

Create a `docker-compose.yml`:

```yaml
services:
  graphmind:
    image: fabischk/graphmind:latest
    ports:
      - "6379:6379"
      - "8080:8080"
    volumes:
      - graphmind_data:/data
    environment:
      - RUST_LOG=info
      # - GRAPHMIND_AUTH_TOKEN=my-secret-token
      # - OPENAI_API_KEY=sk-...
    healthcheck:
      test: ["CMD", "sh", "-c", "echo PING | nc -w1 localhost 6379 | grep -q PONG"]
      interval: 30s
      timeout: 5s
      start_period: 10s
      retries: 3
    restart: unless-stopped

volumes:
  graphmind_data:
```

Start it:

```bash
docker compose up -d
```

## Load Demo Data on Startup

Use the `--demo` flag to start with a pre-built social network:

```bash
docker run -d --name graphmind \
  -p 6379:6379 \
  -p 8080:8080 \
  fabischk/graphmind:latest \
  --host 0.0.0.0 --demo social
```

This creates ~5,250 nodes and ~10,000 edges across 6 node labels (Person, Company, City, Post, Comment, Tag) with 8 edge types.

## Health Check

The Docker image includes a built-in health check via RESP PING:

```bash
docker inspect --format='{{.State.Health.Status}}' graphmind
# healthy
```

You can also check via HTTP:

```bash
curl http://localhost:8080/api/status
# {"status":"healthy","version":"0.6.1","storage":{"nodes":0,"edges":0}}
```

## Building the Image Locally

```bash
git clone https://github.com/graphmind-ai/graphmind.git
cd graphmind
docker build -t graphmind .
docker run -d -p 6379:6379 -p 8080:8080 graphmind
```
