# Graphmind Graph Algorithms

Graph algorithm library for [Graphmind](https://github.com/fab679/graphmind).

## Algorithms

- **PageRank** — Iterative link analysis
- **BFS/DFS** — Breadth/depth-first traversal
- **Shortest Path** — Dijkstra's and unweighted BFS
- **Connected Components** — Weakly and strongly connected
- **Community Detection** — CDLP (label propagation), Louvain
- **Local Clustering Coefficient** — Triangle-based clustering
- **Minimum Spanning Tree** — Kruskal's algorithm
- **Max Flow** — Ford-Fulkerson
- **PCA** — Principal Component Analysis (randomized SVD)

## Usage

```rust
use graphmind_graph_algorithms::pagerank;

let result = pagerank::compute(&graph, 0.85, 100, 1e-6);
```

## License

Apache-2.0
