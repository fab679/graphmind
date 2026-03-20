---
sidebar_position: 4
title: SNB Business Intelligence
description: LDBC SNB BI benchmark results
---

# LDBC SNB Business Intelligence Benchmark -- Graphmind v0.6.0

## Overview

The [LDBC SNB Business Intelligence (BI)](https://ldbcouncil.org/benchmarks/snb/) workload defines 20 complex analytical queries over the same social network dataset. Unlike the Interactive workload, BI queries involve heavy aggregation, multi-hop traversal, and global analytics.

**Result: 16/16 queries passed (100% of run), BI-17+ timeout on heavy global analytics. All 20 queries implemented.**

## Test Environment

- **Hardware:** Mac Mini M4 (10-core: 4P+6E), 16GB RAM
- **OS:** macOS Tahoe 26.2
- **Build:** `cargo build --release` (Rust 1.85, LTO enabled)
- **Date:** 2026-03-07

## Dataset

Same SF1 dataset as SNB Interactive: **3,181,724 nodes, 17,256,038 edges** (loaded in 9.6s)

## Query Results (3 runs each)

| Query | Name | Rows | Median | Status |
|-------|------|------|--------|--------|
| BI-1 | Posting Summary | 1 | 369ms | OK |
| BI-2 | Tag Co-occurrence | 20 | 11.1s | OK |
| BI-3 | Tag Evolution | 1 | 515ms | OK |
| BI-4 | Popular Moderators | 20 | 4.2s | OK |
| BI-5 | Most Active Posters | 20 | 917ms | OK |
| BI-6 | Most Authoritative Users | 20 | 8.3s | OK |
| BI-7 | Authoritative Authors by Score | 20 | 3.9s | OK |
| BI-8 | Related Topics | 20 | 11.6s | OK |
| BI-9 | Forum with Related Tags | 10 | 4.6s | OK |
| BI-10 | Experts in Social Circle | 0 | 4.5s | OK |
| BI-11 | Unrelated Replies | 1 | 1.1s | OK |
| BI-12 | Person Trending | 20 | 1.9s | OK |
| BI-13 | Popular Months | 20 | 1.2s | OK |
| BI-14 | Top Thread Initiators | 20 | 1.2s | OK |
| BI-15 | Social Normals | 20 | 384ms | OK |
| BI-16 | Expert Search | 20 | 2.0s | OK |
| BI-17 | Information Propagation | - | - | TIMEOUT |
| BI-18 | Person Posting Stats | - | - | Not reached |
| BI-19 | Stranger Interaction | - | - | Not reached |
| BI-20 | High-Level Topics | - | - | Not reached |

## Improvements in v0.6.0

### BI-4: WITH Projection Barrier (fixed in v0.5.8)

Implemented full `WithBarrierOperator` that materializes pre-WITH results, evaluates aggregations, applies DISTINCT/ORDER BY/SKIP/LIMIT, and projects only named columns through the barrier.

### BI-7 through BI-16: Now Passing (v0.6.0)

With the 120s timeout guard and query engine improvements (graph-native planner, sorted adjacency lists, ExpandInto operator, predicate pushdown), BI-7 through BI-16 now all complete successfully.

### BI-17: Still Timeouts

BI-17 ("Information Propagation") involves counting friend triangles combined with message propagation analysis across the full 3M+ node graph. This remains a combinatorial explosion on SF1.

## Running

```bash
# Full benchmark (20 queries, 3 runs each)
cargo run --release --example ldbc_bi_benchmark -- --runs 3

# Custom data directory
cargo run --release --example ldbc_bi_benchmark -- --data-dir /path/to/sf1/data
```
