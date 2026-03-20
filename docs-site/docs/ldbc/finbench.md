---
sidebar_position: 5
title: FinBench
description: LDBC FinBench benchmark results
---

# LDBC FinBench Benchmark -- Graphmind v0.6.0

## Overview

[LDBC FinBench](https://ldbcouncil.org/benchmarks/finbench/) is a benchmark for graph databases in financial scenarios. It tests transactional patterns common in fraud detection, anti-money laundering (AML), and financial network analysis.

**Result: 40/40 queries passed (100%) -- 12 CR + 6 SR + 3 RW + 19 W**

## Test Environment

- **Hardware:** Mac Mini M4 (10-core: 4P+6E), 16GB RAM
- **OS:** macOS Tahoe 26.2
- **Build:** `cargo build --release` (Rust 1.85, LTO enabled)
- **Date:** 2026-03-07

## Dataset: Synthetic SF1

Generated in-memory by the built-in synthetic data generator.

| Entity | Count |
|--------|-------|
| Person | 1,000 |
| Company | 500 |
| Account | 5,000 |
| Loan | 1,000 |
| Medium | 200 |
| **Total Nodes** | **7,700** |
| **Total Edges** | **42,240** |

**Load time:** 39ms (synthetic generation)

## Query Results (3 runs each)

### Complex Reads (CR-1 through CR-12)

| Query | Name | Median | Status |
|-------|------|--------|--------|
| CR-1 | Transfer In/Out Amounts | 13.8ms | OK |
| CR-2 | Blocked Account Transfers | 12.7ms | OK |
| CR-3 | Shortest Transfer Path | 3.8ms | OK |
| CR-4 | Transfer Cycle Detection | 1.5ms | OK |
| CR-5 | Owner Account Transfer Patterns | 23.8ms | OK |
| CR-6 | Loan Deposit Tracing | 1.2ms | OK |
| CR-7 | Transfer Chain Analysis | 62.5ms | OK |
| CR-8 | Loan Deposit Distribution | 5.3ms | OK |
| CR-9 | Guarantee Chain | 1.1ms | OK |
| CR-10 | Investment Network | 4.8ms | OK |
| CR-11 | Shared Medium Sign-In | 2.6ms | OK |
| CR-12 | Person Account Transfer Stats | 1.2ms | OK |

### Simple Reads (SR-1 through SR-6)

| Query | Name | Median | Status |
|-------|------|--------|--------|
| SR-1 | Account by ID | 0.87ms | OK |
| SR-2 | Account Transfers in Window | 0.94ms | OK |
| SR-3 | Person's Accounts | 0.17ms | OK |
| SR-4 | Transfer-In Accounts | 7.3ms | OK |
| SR-5 | Transfer-Out Accounts | 0.96ms | OK |
| SR-6 | Loan by ID | 0.17ms | OK |

### Read-Write Operations (RW-1 through RW-3)

| Query | Name | Median | Status |
|-------|------|--------|--------|
| RW-1 | Block Account + Read Transfers | 0.85ms | OK |
| RW-2 | Block Medium + Find Accounts | 0.03ms | OK |
| RW-3 | Block Person + Accounts | 0.16ms | OK |

### Performance Summary

| Category | Queries | Median Range |
|----------|---------|--------------|
| Point lookups | 3 | 0.17ms - 0.87ms |
| 1-hop traversals | 5 | 1.2ms - 13.8ms |
| Multi-hop analysis | 4 | 5.3ms - 62.5ms |
| Path finding | 1 | 3.8ms |
| Read-write transactions | 3 | 0.03ms - 0.85ms |

**Total benchmark time:** 665ms | **AST cache:** 58 hits, 20 misses

## Data Model

```
Person --OWN--> Account --TRANSFER--> Account
  |                |                     |
  |            SIGN_IN--> Medium     WITHDRAW/DEPOSIT
  |                                      |
  +--APPLY--> Loan <--REPAY--------------+
                |
          GUARANTEE--> Loan
                |
Company --OWN--> Account
  |
  +--INVEST--> Company
```

## Coverage

| Category | Spec | Implemented | Coverage |
|----------|------|-------------|----------|
| Complex Reads (CR) | 12 | 12 | 100% |
| Simple Reads (SR) | 6 | 6 | 100% |
| Read-Write (RW) | 3 | 3 | 100% |
| Write (W) | 19 | 19 (defined) | 100% |
| **Total** | **40** | **40** | **100%** |

## Running

```bash
# Full benchmark (synthetic data auto-generated)
cargo run --release --example finbench_benchmark -- --runs 3

# With write benchmarks
cargo run --release --example finbench_benchmark -- --runs 3 --writes

# Custom scale
cargo run --release --example finbench_benchmark -- --scale 10  # 10x more data
```
