---
sidebar_position: 3
title: Build from Source
description: Clone, build, and run Graphmind from source
---

# Build from Source

## Prerequisites

- **Rust 1.75+** with cargo ([install](https://rustup.rs))
- **Node.js 20+** (for the web visualizer UI)
- **C++ compiler** and **CMake** (for RocksDB)

```bash
# Ubuntu/Debian
sudo apt-get install -y clang libclang-dev cmake

# macOS
brew install cmake llvm

# Fedora
sudo dnf install clang clang-devel cmake
```

## Clone and Build

```bash
git clone https://github.com/graphmind-ai/graphmind.git
cd graphmind
```

Build the web UI first (it gets embedded into the server binary):

```bash
cd ui && npm install && npm run build && cd ..
```

Build the server:

```bash
cargo build --release
```

The binary is at `target/release/graphmind`.

## Run

```bash
# Start the server
cargo run --release

# Or run the binary directly
./target/release/graphmind
```

The server starts two listeners:
- RESP on `127.0.0.1:6379`
- HTTP + Visualizer on `127.0.0.1:8080`

## Development Mode

For faster iteration during development, skip the release build:

```bash
# Run in debug mode
cargo run

# Run with demo data
cargo run -- --demo social
```

For the UI, run the Vite dev server separately (hot-reload on changes):

```bash
cd ui && npm run dev
```

This starts a dev server on `http://localhost:5173` that proxies API requests to the Graphmind HTTP server on `:8080`.

## Run Tests

```bash
# All tests (1842 unit tests)
cargo test

# Specific module
cargo test graph::node

# With output
cargo test -- --nocapture

# Check formatting and linting
cargo fmt -- --check
cargo clippy -- -D warnings
```

## Run Benchmarks

```bash
# All benchmarks
cargo bench

# Specific benchmark
cargo bench --bench graph_benchmarks
cargo bench --bench vector_benchmark
cargo bench --bench full_benchmark
```

## Run Examples

```bash
cargo run --example banking_demo
cargo run --example clinical_trials_demo
cargo run --example supply_chain_demo
cargo run --example social_network_demo
```

## Integration Tests

Start the server first, then run the Python integration tests:

```bash
# Terminal 1: start the server
cargo run

# Terminal 2: run integration tests
cd tests/integration
python3 test_resp_basic.py
python3 test_resp_visual.py
```
