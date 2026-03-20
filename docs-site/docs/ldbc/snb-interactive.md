---
sidebar_position: 3
title: SNB Interactive
description: LDBC SNB Interactive benchmark results
---

# LDBC SNB Interactive Benchmark -- Graphmind v0.6.0

## Overview

The [LDBC Social Network Benchmark (SNB) Interactive](https://ldbcouncil.org/benchmarks/snb/) workload defines parameterized queries over a synthetic social network. It tests transactional read and write patterns typical of social networking applications.

**Result: 21/21 read queries passed (100%)**

## Test Environment

- **Hardware:** Mac Mini M4 (10-core: 4P+6E), 16GB RAM
- **OS:** macOS Tahoe 26.2
- **Build:** `cargo build --release` (Rust 1.85, LTO enabled)
- **Date:** 2026-03-07

## Dataset: Scale Factor 1 (SF1)

| Entity | Count |
|--------|-------|
| Person | 9,892 |
| Forum | 90,492 |
| Post | 1,003,605 |
| Comment | 2,052,169 |
| **Total Nodes** | **3,181,724** |
| **Total Edges** | **17,256,038** |

**Load time:** 9.2s (single-threaded CSV parsing)

## Read Query Results (3 runs each)

### Short Reads (IS1-IS7)

| Query | Name | Median | Status |
|-------|------|--------|--------|
| IS1 | Person Profile | 17.8ms | OK |
| IS2 | Recent Posts by Person | 18.0ms | OK |
| IS3 | Friends of Person | 17.6ms | OK |
| IS4 | Post Content | 336ms | OK |
| IS5 | Post Creator | 316ms | OK |
| IS6 | Forum of Post | 314ms | OK |
| IS7 | Replies to Post | 5.8s | OK |

### Complex Reads (IC1-IC14)

| Query | Name | Median | Status |
|-------|------|--------|--------|
| IC1 | Transitive Friends by Name | 17.0ms | OK |
| IC2 | Recent Friend Posts | 24.4ms | OK |
| IC3 | Friends in Countries | 5.5s | OK |
| IC4 | Popular Tags in Period | 24.2ms | OK |
| IC5 | New Forum Members | 4.4s | OK |
| IC6 | Tag Co-occurrence | 6.6s | OK |
| IC7 | Recent Likers | 17.6ms | OK |
| IC8 | Recent Replies | 17.7ms | OK |
| IC9 | Recent FoF Posts | 26.9ms | OK |
| IC10 | Friend Recommendation | 4.2s | OK |
| IC11 | Job Referral | 21.1ms | OK |
| IC12 | Expert Reply | 64.9ms | OK |
| IC13 | Single Shortest Path | 18.2ms | OK |
| IC14 | Trusted Connection Paths | 19.4ms | OK |

### Performance Summary

| Category | Queries | Median Range |
|----------|---------|--------------|
| Point lookups | 6 | 17.0ms - 21.1ms |
| 1-hop with filters | 4 | 24.2ms - 64.9ms |
| Multi-hop (FoF) | 4 | 4.2s - 6.6s |
| Full-graph scan | 4 | 314ms - 5.8s |
| Path finding | 2 | 18.2ms - 19.4ms |

**Total benchmark time:** 111.9s

## Update and Delete Operations

8 update operations (INS1-INS8) and 8 delete operations (DEL1-DEL8) are defined following the LDBC SNB specification.

Run with: `cargo bench --release --bench ldbc_benchmark -- --updates --deletes`

## Running

```bash
# Full benchmark (21 queries, 3 runs each)
cargo run --release --example ldbc_benchmark -- --runs 3

# Single query
cargo run --release --example ldbc_benchmark -- --query IC13

# With update operations
cargo run --release --example ldbc_benchmark -- --runs 3 --updates
```
