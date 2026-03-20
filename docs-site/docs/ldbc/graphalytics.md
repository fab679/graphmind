---
sidebar_position: 2
title: Graphalytics
description: LDBC Graphalytics benchmark results
---

# LDBC Graphalytics Benchmark -- Graphmind v0.6.0

## Overview

[LDBC Graphalytics](https://ldbcouncil.org/benchmarks/graphalytics/) is a benchmark for graph analysis platforms. It defines 6 standard graph algorithms that must be implemented and validated against reference outputs on standard datasets.

**Result: 28/28 validations passed (100%) -- XS 12/12, S-size 16/16**

## Test Environment

- **Hardware:** Mac Mini M4 (10-core: 4P+6E), 16GB RAM
- **OS:** macOS Tahoe 26.2
- **Build:** `cargo build --release` (Rust 1.85, LTO enabled)
- **Date:** 2026-03-07

## Algorithms

| Algorithm | Abbreviation | Description |
|-----------|-------------|-------------|
| Breadth-First Search | BFS | Single-source shortest path (unweighted) |
| PageRank | PR | Iterative link analysis ranking |
| Weakly Connected Components | WCC | Find connected components (ignoring direction) |
| Community Detection (Label Propagation) | CDLP | Community labels via neighbor voting |
| Local Clustering Coefficient | LCC | Neighborhood connectivity per node |
| Single-Source Shortest Path | SSSP | Weighted shortest path from source |

## Datasets

### XS-size (included in repo)

| Dataset | Vertices | Edges | Directed |
|---------|----------|-------|----------|
| example-directed | 10 | 17 | Yes |
| example-undirected | 9 | 24 (12 bidirectional) | No |

### S-size (downloaded separately)

| Dataset | Vertices | Edges | Directed |
|---------|----------|-------|----------|
| wiki-Talk | ~2.4M | ~5.0M | Yes |
| cit-Patents | ~3.8M | ~16.5M | Yes |
| datagen-7_5-fb | ~633K | ~68.4M (34.2M bidirectional) | No |

## Results (S-size)

### cit-Patents (3,774,768 vertices, 16,518,947 edges, directed)

Load time: 8.1s

| Algorithm | Time | Validation |
|-----------|------|------------|
| BFS | 71ms | **PASS** |
| PR | 791ms | **PASS** |
| WCC | 376ms | **PASS** |
| CDLP | 9.5s | **PASS** |
| LCC | 9.6s | **PASS** |

### datagen-7_5-fb (633,432 vertices, 68,371,494 edges, undirected)

Load time: 8.6s

| Algorithm | Time | Validation |
|-----------|------|------------|
| BFS | 170ms | **PASS** |
| PR | 879ms | **PASS** |
| WCC | 285ms | **PASS** |
| CDLP | 15.5s | **PASS** |
| LCC | 167s | **PASS** |
| SSSP | 304ms | **PASS** |

### wiki-Talk (2,394,385 vertices, 5,021,410 edges, directed)

Load time: 1.4s

| Algorithm | Time | Validation |
|-----------|------|------------|
| BFS | 148ms | **PASS** |
| PR | 280ms | **PASS** |
| WCC | 265ms | **PASS** |
| CDLP | 2.5s | **PASS** |
| LCC | 41.5s | **PASS** |

### Overall Summary (XS + S-size)

| Size | Datasets | Validations | Passed | Rate |
|------|----------|-------------|--------|------|
| XS | 2 | 12 | 12 | 100% |
| S | 3 | 16 | 16 | 100% |
| **Total** | **5** | **28** | **28** | **100%** |

## Running

```bash
# Download datasets
bash scripts/download_graphalytics.sh          # XS
bash scripts/download_graphalytics.sh --size S  # S-size

# Run all algorithms
cargo bench --release --bench graphalytics_benchmark -- --all

# Run specific algorithm or dataset
cargo bench --release --bench graphalytics_benchmark -- --algo BFS
cargo bench --release --bench graphalytics_benchmark -- --dataset example-directed
```
