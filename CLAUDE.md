# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

Graphmind is a high-performance distributed graph database written in Rust with ~90% OpenCypher query support, Redis protocol (RESP) compatibility, multi-tenancy, vector search, NLQ, and graph algorithms. Currently at Phase 4 (High Availability Foundation), version v0.6.1.

## Build & Development Commands

```bash
# Build
cargo build                    # Debug build
cargo build --release          # Release build (optimized)

# Run tests (1842 unit tests)
cargo test                     # All tests
cargo test graph::node         # Specific module tests
cargo test -- --nocapture      # Tests with output

# Benchmarks (all in benches/)
cargo bench                                    # All benchmarks
cargo bench --bench graph_benchmarks           # Criterion micro-benchmarks (15 benches)
cargo bench --bench full_benchmark             # Full suite (ingestion, vector, traversal, algorithms)
cargo bench --bench vector_benchmark           # HNSW vector search
cargo bench --bench graphalytics_benchmark     # LDBC Graphalytics algorithms
cargo bench --bench mvcc_benchmark             # MVCC & arena allocation
cargo bench --bench late_materialization_bench  # Late materialization traversal
cargo bench --bench graph_optimization_benchmark # Metaheuristic optimization solvers
cargo bench --bench ldbc_benchmark             # LDBC SNB Interactive queries (needs data)
cargo bench --bench ldbc_bi_benchmark          # LDBC SNB BI queries (needs data)
cargo bench --bench finbench_benchmark         # LDBC FinBench queries (synthetic data)

# Run examples
cargo run --example banking_demo              # Banking fraud detection + NLQ
cargo run --example clinical_trials_demo      # Clinical trials + vector search
cargo run --example supply_chain_demo         # Supply chain + optimization
cargo run --example smart_manufacturing_demo  # Digital twin + scheduling
cargo run --example social_network_demo       # Social network analysis
cargo run --example knowledge_graph_demo      # Enterprise knowledge graph
cargo run --example enterprise_soc_demo       # Security operations center
cargo run --example agentic_enrichment_demo   # GAK (Generation-Augmented Knowledge)
cargo run --example persistence_demo          # Persistence & multi-tenancy
cargo run --example cluster_demo              # Raft clustering
cargo run --example ldbc_loader               # Load LDBC SNB SF1 dataset
cargo run --example finbench_loader           # Load/generate FinBench dataset
cargo run --release --example cricket_loader  # Load 21K Cricsheet matches
cargo run --release --example aact_loader     # Load AACT clinical trials dataset

# Start RESP server
cargo run                      # RESP on 127.0.0.1:6379, HTTP on :8080

# Code quality
cargo fmt -- --check           # Check formatting
cargo clippy -- -D warnings    # Lint checks

# Integration tests (requires running server)
cd tests/integration
python3 test_resp_basic.py
python3 test_resp_visual.py

# Frontend UI (React)
cd ui && npm install       # Install frontend dependencies
cd ui && npm run dev       # Dev server on :5173 (proxies /api to :8080)
cd ui && npm run build     # Production build → ui/dist/
```

## Architecture

### Module Structure

```
src/
├── graph/           # Property Graph Model
│   ├── store.rs     # GraphStore - in-memory storage with indices + cardinality stats
│   ├── node.rs      # Node with labels and properties
│   ├── edge.rs      # Directed edges with types
│   ├── property.rs  # PropertyValue (String, Integer, Float, Boolean, DateTime, Array, Map, Null)
│   └── types.rs     # NodeId, EdgeId, Label, EdgeType
│
├── query/           # OpenCypher Query Engine (~90% coverage)
│   ├── parser.rs    # Pest-based OpenCypher parser
│   ├── cypher.pest  # PEG grammar (atomic keyword rules for word boundaries)
│   ├── ast.rs       # Query AST
│   └── executor/
│       ├── planner.rs   # Query planner (AST → ExecutionPlan)
│       ├── operator.rs  # Physical operators (Volcano iterator model)
│       └── record.rs    # Record, RecordBatch, Value (with late materialization)
│
├── protocol/        # RESP Protocol
│   ├── resp.rs      # RESP3 encoder/decoder
│   ├── server.rs    # Tokio TCP server
│   └── command.rs   # GRAPH.* command handler
│
├── persistence/     # Persistence & Multi-Tenancy
│   ├── storage.rs   # RocksDB with column families
│   ├── wal.rs       # Write-Ahead Log
│   └── tenant.rs    # Multi-tenancy & resource quotas
│
├── raft/            # High Availability
│   ├── node.rs      # RaftNode using openraft
│   ├── state_machine.rs  # GraphStateMachine
│   ├── cluster.rs   # ClusterConfig, ClusterManager
│   ├── network.rs   # Inter-node communication
│   └── storage.rs   # Raft log storage
│
├── nlq/             # Natural Language Query Pipeline
│   ├── mod.rs       # NLQPipeline (text_to_cypher, extract_cypher, is_safe_query)
│   └── client.rs    # NLQClient (OpenAI, Gemini, Ollama, Claude Code providers)
│
├── vector/          # HNSW Vector Index
├── snapshot/        # Portable .sgsnap export/import
└── sharding/        # Tenant-level sharding
```

### Key Architectural Patterns

1. **Volcano Iterator Model (ADR-007)**: Lazy, pull-based operators:
   - `NodeScanOperator` → `FilterOperator` → `ExpandOperator` → `ProjectOperator` → `LimitOperator`

2. **Late Materialization (ADR-012)**: Scan produces `Value::NodeRef(id)` not full clones. Properties resolved on demand via `resolve_property()`.

3. **In-Memory Graph Storage**: O(1) lookups via HashMaps with adjacency lists for traversal.

4. **Multi-Tenancy**: RocksDB column families with tenant-prefixed keys, per-tenant quotas.

5. **Raft Consensus**: Uses `openraft` crate with custom `GraphStateMachine`.

6. **Cross-Type Coercion**: Integer/Float promotion, String/Boolean coercion, Null propagation (three-valued logic).

### Frontend (React UI)

The web-based visualizer is a React 19 + TypeScript application in `ui/`.

```
ui/src/
├── api/client.ts          # Typed fetch client for all API endpoints
├── stores/                # Zustand state management
│   ├── graphStore.ts      # Nodes, edges, selection, multi-select
│   ├── queryStore.ts      # Query execution, history, saved queries
│   ├── uiStore.ts         # Connection status, schema, panel state
│   └── graphSettingsStore.ts  # Colors, icons, captions (persisted)
├── components/
│   ├── editor/            # CodeMirror 6 Cypher editor, query templates, saved queries
│   ├── graph/             # D3 force graph, fullscreen explorer, toolbar, stats, minimap
│   ├── inspector/         # Property inspector, schema browser with color/icon pickers
│   ├── layout/            # AppShell, Navbar, resizable panels
│   ├── results/           # TanStack Table for query results
│   └── ui/                # Reusable components (button, badge, tabs, resizable, theme toggle, icon picker)
├── lib/
│   ├── cypher.ts          # Cypher keywords/functions/procedures for autocomplete
│   ├── colors.ts          # Node label color mapping with custom override support
│   ├── icons.ts           # 55+ SVG icon catalog for node visualization
│   ├── shortcuts.ts       # Keyboard shortcuts system
│   └── utils.ts           # Tailwind merge utility
└── types/api.ts           # TypeScript interfaces matching backend API
```

**Tech stack:** React 19, Vite 6, TypeScript, Tailwind CSS v4, shadcn/ui, D3.js (d3-force), CodeMirror 6, TanStack Table, Zustand

**Dev commands:**
```bash
cd ui
npm install                # Install dependencies
npm run dev                # Dev server on :5173 (proxies /api to :8080)
npm run build              # Production build → ui/dist/
```

## Cypher Support

**Supported clauses:** MATCH, OPTIONAL MATCH, CREATE, DELETE, SET, REMOVE, MERGE, WITH, UNWIND, UNION, RETURN DISTINCT, ORDER BY, SKIP, LIMIT, EXPLAIN, EXISTS subqueries.

**Supported functions (30+):** toUpper, toLower, trim, replace, substring, left, right, reverse, toString, toInteger, toFloat, abs, ceil, floor, round, sqrt, sign, count, sum, avg, min, max, collect, size, length, head, last, tail, keys, id, labels, type, exists, coalesce.

**Remaining gaps:** list slicing, pattern comprehensions, named paths, collect(DISTINCT x).

## API Patterns

### Query Engine
```rust
// Read-only queries
let executor = QueryExecutor::new(&store);
let result: RecordBatch = executor.execute(&query)?;

// Write queries (CREATE, DELETE, SET, MERGE)
let mut executor = MutQueryExecutor::new(&mut store, tenant_id);
executor.execute(&query)?;

// EXPLAIN (no execution)
// Returns plan as RecordBatch with operator descriptions
```

### NLQ Pipeline
```rust
let pipeline = NLQPipeline::new(nlq_config)?;
let cypher = pipeline.text_to_cypher("Who knows Alice?", &schema_summary).await?;
// Returns clean Cypher with markdown fences stripped and safety validation
```

### Graph Store
```rust
let mut graph = GraphStore::new();
let node_id = graph.create_node("Person");
graph.get_node_mut(node_id)?.set_property("name", "Alice");
graph.create_edge(source_id, target_id, "KNOWS")?;
```

### HTTP API Endpoints

The Axum HTTP server runs on port 8080 alongside the RESP server on port 6379.

| Method | Path | Description |
|--------|------|-------------|
| POST | `/api/query` | Execute a single Cypher query |
| POST | `/api/script` | Execute multi-statement Cypher script (line-by-line) |
| POST | `/api/nlq` | Translate natural language to Cypher via LLM |
| GET | `/api/status` | Server health, node/edge counts, cache stats |
| GET | `/api/schema` | Schema introspection (labels, types, properties, stats) |
| POST | `/api/sample` | Sample a subgraph for visualization |
| POST | `/api/import/csv` | Import nodes from CSV |
| POST | `/api/import/json` | Import nodes from JSON |
| POST | `/api/snapshot/export` | Export graph as .sgsnap |
| POST | `/api/snapshot/import` | Import .sgsnap snapshot |

#### Script Endpoint
```bash
# Execute a .cypher file with multiple statements
curl -X POST http://localhost:8080/api/script \
  -H 'Content-Type: application/json' \
  -d '{"query": "CREATE (a:Person {name: '\''Alice'\''})\nCREATE (b:Person {name: '\''Bob'\''})"}'
# Response: {"status":"ok","executed":2,"errors":[],"storage":{"nodes":2,"edges":0}}
```

#### NLQ Endpoint (Natural Language Queries)
Requires an LLM provider configured via environment variables:
- `OPENAI_API_KEY` — Uses OpenAI (model: `OPENAI_MODEL` or `gpt-4o-mini`)
- `GEMINI_API_KEY` — Uses Google Gemini (model: `GEMINI_MODEL` or `gemini-2.0-flash`)
- `CLAUDE_CODE_NLQ=1` — Uses Claude Code CLI

```bash
OPENAI_API_KEY=sk-... cargo run
# Then:
curl -X POST http://localhost:8080/api/nlq \
  -H 'Content-Type: application/json' \
  -d '{"query": "Who are Alice'\''s friends?"}'
# Response: {"cypher":"MATCH (a:Person {name:'Alice'})-[:FRIENDS_WITH]-(b) RETURN a,b","provider":"OpenAI","model":"gpt-4o-mini"}
```

## Testing

- **1842 unit tests** across all modules (89.7% coverage)
- **10 benchmark binaries** in `benches/` (Criterion micro-benchmarks + domain benchmarks)
- **Integration tests**: Python scripts in `tests/integration/`
- **8 domain-specific example demos** with NLQ integration
- **4 data loaders** (LDBC SNB, FinBench, Cricket, AACT) in `examples/`
- **28 CRUD integration tests** in `tests/cypher_crud_test.rs` (CREATE, MATCH, SET, DELETE, MERGE, WITH, UNWIND, OPTIONAL MATCH, EXPLAIN)

## Demo Scripts

```bash
# Social Network Demo — 52 nodes, 142 edges
# Load via UI script button or API:
curl -X POST http://localhost:8080/api/script \
  -H 'Content-Type: application/json' \
  -d @scripts/social_network_demo.cypher

# Or paste into the UI editor and use the Upload button
```

The demo creates: 16 Person, 6 City, 5 Company, 7 Property, 6 Car, 5 Pet, 8 Hobby, 4 University nodes with MARRIED_TO, LIVES_IN, FRIENDS_WITH, WORKS_AT, OWNS, ATTENDED, ENJOYS, INVESTED_IN, HEADQUARTERED_IN relationships. Includes 25 test queries (Q1-Q25).

## UI Features (Visualizer)

The web visualizer at `http://localhost:8080` provides:

- **Cypher Editor**: CodeMirror 6 with syntax highlighting, autocomplete (keywords, functions, schema labels/types, variables), Ctrl+Enter to execute
- **D3 Force Graph**: Canvas-based rendering with zoom/pan, node dragging, click selection, right-click context menu (expand neighbors, load all relationships)
- **Graph Layouts**: Force-directed (default), circular, hierarchical, grid
- **Fullscreen Explorer**: Immersive mode with floating glassmorphism legend, search, minimap, and property inspector
- **Query Templates**: Pre-built queries organized by category (Exploration, Pathfinding, Analytics, Social Network, Algorithms)
- **Saved Queries**: Name and save queries, persisted to localStorage
- **NLQ Mode**: Natural language to Cypher translation (requires LLM API key)
- **Search**: Type to highlight matching nodes on canvas, dims non-matching
- **Shortest Path**: Click two nodes to find and visualize shortest path (BFS)
- **Highlight Mode**: Select a node to highlight connected nodes, dim the rest
- **Multi-select**: Shift+click to select multiple nodes
- **Node Icons**: 55+ built-in SVG icons assignable per label, auto-detect image URLs
- **Color Customization**: Per-label node colors and per-type edge colors (persisted)
- **Caption Property**: Choose which property displays on each node label
- **Export**: PNG screenshot, CSV data, JSON graph
- **Graph Stats**: Floating panel with node/edge distribution and degree metrics
- **Dark/Light Theme**: Toggle with system preference support
- **Keyboard Shortcuts**: F5 (run), Escape (clear), Delete (remove from canvas), ? (help)
- **Script Loader**: Upload .cypher files to execute all statements
- **Query History**: With play/delete buttons and timeline scrubber
