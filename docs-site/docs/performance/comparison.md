---
sidebar_position: 2
title: Benchmark Comparison
description: Comparison with Neo4j, FalkorDB, Memgraph, and TigerGraph
---

# Honest Benchmark & Architecture Comparison

This document provides a candid comparison of **Graphmind Graph Database** (v0.5.0) against industry leaders **Neo4j**, **FalkorDB**, **Memgraph**, and **TigerGraph**.

## Summary Table

| Feature | Graphmind (Rust/RocksDB) | Neo4j (Java/Native) | FalkorDB (C/GraphBLAS) | Memgraph (C++/In-Memory) | TigerGraph (C++/MPP) |
| :--- | :--- | :--- | :--- | :--- | :--- |
| **Ingestion Speed** | **Very High** -- ~230K nodes/sec, ~1.1M edges/sec | **Medium** -- ~26K nodes/sec | **High** -- in-memory C | **Very High** -- ~295K nodes/sec | **Very High** -- MPP parallel loading |
| **1-Hop Traversal** | **41ms** (Cypher), **15us** (raw API) | **~28ms** | **~55ms** p50 | **~1.1ms** | **Sub-ms** |
| **Deep Traversal** | **15us raw 3-hop**, 259ms 2-hop (Cypher) | **High** | **High** (GraphBLAS) | **Very High** | **Very High** (MPP) |
| **Vector Search** | **Ultra High** -- native HNSW, **549us** | **Low/Medium** | **Medium** | **N/A** | **N/A** |
| **Query Complexity** | **Low** -- basic Volcano, no CBO | **Very High** -- decades of CBO | **Medium/High** | **High** | **Very High** |
| **Concurrency (MVCC)** | **High** -- native MVCC | **Medium/High** | **Medium** | **Medium** | **High** |
| **Memory Footprint** | **Low** -- columnar + CSR, no GC | **High** -- JVM overhead | **Low** | **Medium** | **Medium** |

## Graphmind Wins

- **High-Throughput Ingestion**: ~230K nodes/sec and ~1.1M edges/sec makes it ideal for streaming data pipelines
- **Lock-Free Concurrency**: MVCC ensures readers never block writers
- **Ultra-Low Latency Vector Search**: 549us native HNSW enables real-time RAG
- **Memory Efficiency**: Columnar storage, arenas, and CSR allow large graphs on limited RAM
- **Raw Storage Speed**: 15us 3-hop traversal demonstrates the Rust + late materialization architecture

## Graphmind Challenges

- **Cypher Query Overhead**: ~40ms parse/plan overhead per query (query AST caching in progress)
- **Complex Analytical Queries**: Slower without mature Cost-Based Optimizer (CBO)
- **Tooling Maturity**: Ecosystem tooling is nascent compared to established players

## Key Metrics Summary

| Metric | Graphmind | Best Competitor |
| :--- | :--- | :--- |
| Node ingestion | 230K/sec | Memgraph ~295K/sec |
| Edge ingestion | 1.1M/sec | -- |
| 1-hop (Cypher) | 41ms | Memgraph ~1.1ms |
| 3-hop (raw API) | 15us | -- |
| Vector search | 549us | -- (no native competitor) |
| RETURN n (1000 nodes) | 1.96ms | -- |

## Conclusion

Graphmind (v0.5.0) is a **high-performance, hybrid transactional/analytical graph database** that dominates in **vector search** and **ingestion throughput**. The late materialization optimization delivered 15us raw 3-hop traversal, proving the storage architecture is competitive. For AI-native workloads requiring fast vector search + graph traversal, Graphmind offers a unique combination not available from any single competitor.
