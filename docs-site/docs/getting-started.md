---
sidebar_position: 1
title: Getting Started
description: Quick start guide for Graphmind graph database
---

# Getting Started

## Prerequisites
- Rust 1.75+ with cargo
- Node.js 20+ (for the web visualizer)

## Build & Run
```bash
# Clone and build
git clone https://github.com/graphmind-ai/graphmind.git
cd graphmind
cargo build --release

# Start the server
cargo run
# RESP server: 127.0.0.1:6379
# Web visualizer: http://localhost:8080
```

## Load Demo Data
```bash
# Via the web UI: click the Upload button and select scripts/social_network_demo.cypher
# Or via API:
curl -X POST http://localhost:8080/api/script \
  -H 'Content-Type: application/json' \
  --data-binary @scripts/social_network_demo.cypher
```

## Connect via Redis CLI
```bash
redis-cli
> GRAPH.QUERY default "MATCH (n) RETURN labels(n), count(n)"
```

## Web Visualizer
Open http://localhost:8080 for the built-in graph explorer with:
- Cypher editor with syntax highlighting and autocomplete
- Interactive D3.js force-directed graph visualization
- Fullscreen explorer with search, legend, and minimap
- Graph layouts: force, circular, hierarchical, grid
- Dark/Light theme toggle
- Export as PNG, CSV, or JSON

## Frontend Development
```bash
cd ui && npm install && npm run dev   # Dev server on :5173
cd ui && npm run build                # Build → src/http/static/
```
