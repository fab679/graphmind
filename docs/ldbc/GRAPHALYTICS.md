# LDBC Graphalytics Benchmark — Samyama v0.5.8

## Overview

[LDBC Graphalytics](https://ldbcouncil.org/benchmarks/graphalytics/) is a benchmark for graph analysis platforms. It defines 6 standard graph algorithms that must be implemented and validated against reference outputs on standard datasets.

**Result: 12/12 validations passed (100%), all 6 algorithms execute correctly**

## Test Environment

- **Hardware:** Mac Mini M2 Pro, 16GB RAM
- **OS:** macOS Sonoma
- **Build:** `cargo build --release` (Rust 1.83, LTO enabled)
- **Date:** 2026-02-26

## Algorithms

| Algorithm | Abbreviation | Description | Implementation |
|-----------|-------------|-------------|----------------|
| Breadth-First Search | BFS | Single-source shortest path (unweighted) | `samyama_graph_algorithms::bfs()` |
| PageRank | PR | Iterative link analysis ranking | `samyama_graph_algorithms::page_rank()` |
| Weakly Connected Components | WCC | Find connected components (ignoring edge direction) | `samyama_graph_algorithms::weakly_connected_components()` |
| Community Detection (Label Propagation) | CDLP | Assign community labels via neighbor voting | `samyama_graph_algorithms::cdlp()` |
| Local Clustering Coefficient | LCC | Measure of neighborhood connectivity per node | `samyama_graph_algorithms::local_clustering_coefficient_directed()` |
| Single-Source Shortest Path | SSSP | Weighted shortest path from source | `samyama_graph_algorithms::dijkstra()` |

## Datasets

### XS-size (included in repo)

| Dataset | Vertices | Edges | Directed | Source |
|---------|----------|-------|----------|--------|
| example-directed | 10 | 17 | Yes | LDBC reference (XS) |
| example-undirected | 9 | 24 (12 bidirectional) | No | LDBC reference (XS) |

### S-size (downloaded separately)

| Dataset | Vertices | Edges | Directed | Source |
|---------|----------|-------|----------|--------|
| wiki-Talk | ~2.4M | ~5.0M | Yes | LDBC Graphalytics S |
| cit-Patents | ~3.8M | ~16.5M | Yes | LDBC Graphalytics S |
| datagen-7_5-fb | ~633K | ~34.2M | No | LDBC Graphalytics S |

## Results (XS-size)

### example-directed (10 vertices, 17 edges)

| Algorithm | Time | Result | Validation |
|-----------|------|--------|------------|
| BFS | 4us | source=1, reachable=6, max_depth=2 | **PASS** |
| PR | 3us | converged with tolerance=1e-7 | **PASS** |
| WCC | 9us | components=1, largest=10 | **PASS** |
| CDLP | 23us | communities=4, largest=4, iters=2 | **PASS** |
| LCC | 16us | avg_cc (directed mode) | **PASS** |
| SSSP | 4us | source=1, reachable=6, max_dist=1.020 | **PASS** |

### example-undirected (9 vertices, 24 edges)

| Algorithm | Time | Result | Validation |
|-----------|------|--------|------------|
| BFS | 4us | source=2, reachable=9, max_depth=4 | **PASS** |
| PR | 3us | converged with tolerance=1e-7 | **PASS** |
| WCC | 12us | components=1, largest=9 | **PASS** |
| CDLP | 13us | communities=4, largest=4, iters=2 | **PASS** |
| LCC | 13us | avg_cc=0.652, non_zero=8 | **PASS** |
| SSSP | 4us | source=2, reachable=9, max_dist=2.410 | **PASS** |

### Summary

| Algorithm | Directed | Undirected | Overall |
|-----------|----------|------------|---------|
| BFS | PASS | PASS | 2/2 |
| PR | PASS | PASS | 2/2 |
| WCC | PASS | PASS | 2/2 |
| CDLP | PASS | PASS | 2/2 |
| LCC | PASS | PASS | 2/2 |
| SSSP | PASS | PASS | 2/2 |
| **Total** | **6/6** | **6/6** | **12/12** |

## Fixes Applied (v0.5.8)

### PageRank Convergence (previously FAIL)

**Previous issue:** Benchmark used `tolerance: 0.0` with only `max_iterations` from the properties file (typically 2), so PageRank never converged.

**Fix:** Changed to `tolerance: 1e-7` with `iterations: max(props, 100)`. PageRank now runs to convergence, matching LDBC reference outputs.

### Directed LCC (previously FAIL on directed datasets)

**Previous issue:** The LCC algorithm treated all edges as undirected, using `d*(d-1)/2` as the divisor. LDBC expects directed triangle semantics for directed graphs.

**Fix:** Added `local_clustering_coefficient_directed(view, directed)` which counts directed edges among neighbors and uses `d*(d-1)` divisor when `directed=true`. The benchmark auto-detects directedness from the dataset properties file.

## GPU Acceleration (Enterprise)

The enterprise edition supports GPU-accelerated PageRank and LCC via wgpu compute shaders:

- **PageRank:** GPU iterations with periodic CPU-side convergence checking (tolerance-based)
- **LCC:** GPU kernel with directed/undirected mode, binary search in sorted CSR adjacency
- **Auto-dispatch:** Graphs with >1000 nodes automatically use GPU when available
- GPU path is taken for S-size datasets (633K+ vertices)

## Algorithm Details

### BFS (Breadth-First Search)
- Single-source BFS from a given start vertex
- Returns distance (hop count) to every reachable vertex
- Validates against reference output: exact match on all distances

### PageRank
- Iterative computation: `PR(v) = (1-d)/N + d * sum(PR(u)/out_degree(u))` for each neighbor u
- Configurable: damping factor (default 0.85), max iterations, convergence tolerance
- Validates against reference: tolerance of 1e-4 per node

### Weakly Connected Components (WCC)
- Union-Find based: treats all edges as undirected
- Returns component ID for each vertex (minimum vertex ID in component)

### Community Detection via Label Propagation (CDLP)
- Synchronous label propagation: each node adopts most frequent neighbor label
- Ties broken by smallest label value
- Configurable iteration count (default from properties file)

### Local Clustering Coefficient (LCC)
- For each node: `LCC(v) = 2 * triangles(v) / (degree(v) * (degree(v) - 1))`
- Reports per-node coefficient and average

### Single-Source Shortest Path (SSSP)
- Dijkstra's algorithm from a given source vertex
- Edge weights from dataset property file
- Returns shortest distance to every reachable vertex

## Running

```bash
# Download XS datasets (included by default)
bash scripts/download_graphalytics.sh

# Download S-size datasets
bash scripts/download_graphalytics.sh --size S

# Run all algorithms on XS datasets
cargo bench --release --bench graphalytics_benchmark -- --all

# Run on S-size datasets
cargo bench --release --bench graphalytics_benchmark -- --size S --all

# Run all sizes
cargo bench --release --bench graphalytics_benchmark -- --size all --all

# Run specific algorithm
cargo bench --release --bench graphalytics_benchmark -- --algo BFS

# Run on specific dataset
cargo bench --release --bench graphalytics_benchmark -- --dataset example-directed

# Custom data directory
cargo bench --release --bench graphalytics_benchmark -- --data-dir /path/to/data --all
```

## Dataset Sizes

- **XS:** example-directed (10V), example-undirected (9V) — sub-millisecond
- **S:** wiki-Talk (~2.4M V), cit-Patents (~3.8M V), datagen-7_5-fb (~633K V) — seconds
- **M/L:** Requires more memory; performance scales linearly with edge count
