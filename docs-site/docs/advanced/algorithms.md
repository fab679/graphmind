---
sidebar_position: 2
title: Graph Algorithms
description: Built-in graph algorithms accessible via Cypher and SDKs
---

# Graph Algorithms

Graphmind includes built-in implementations of common graph algorithms. They can be invoked via Cypher `CALL` syntax or through the SDK (embedded mode).

## Available Algorithms

| Algorithm | Description | Cypher Procedure |
|-----------|-------------|------------------|
| **PageRank** | Iterative node importance ranking | `algo.pageRank` |
| **BFS** | Unweighted shortest path | `algo.bfs` |
| **Dijkstra** | Weighted shortest path | `algo.dijkstra` |
| **WCC** | Weakly connected components | `algo.wcc` |
| **SCC** | Strongly connected components | `algo.scc` |
| **CDLP** | Community detection (label propagation) | `algo.cdlp` |
| **LCC** | Local clustering coefficient | `algo.lcc` |
| **MST** | Minimum spanning tree (Prim's) | `algo.mst` |
| **Max Flow** | Edmonds-Karp maximum flow | `algo.maxFlow` |
| **Triangle Count** | Count triangles in the graph | `algo.triangleCount` |

## PageRank

Compute the importance of each node based on the link structure.

### Cypher

```cypher
CALL algo.pageRank('Person', 'KNOWS', {dampingFactor: 0.85, iterations: 20})
YIELD nodeId, score
RETURN nodeId, score
ORDER BY score DESC
LIMIT 10
```

### Python SDK (Embedded)

```python
scores = client.page_rank(label="Person", edge_type="KNOWS", damping=0.85, iterations=20)
for node_id, score in sorted(scores.items(), key=lambda x: -x[1])[:10]:
    print(f"Node {node_id}: {score:.4f}")
```

### Rust SDK (Embedded)

```rust
use graphmind_sdk::{EmbeddedClient, AlgorithmClient, PageRankConfig};

let config = PageRankConfig {
    damping_factor: 0.85,
    iterations: 20,
    ..Default::default()
};
let scores = client.page_rank(config, Some("Person"), Some("KNOWS")).await;
```

## Shortest Path (BFS)

Find the shortest unweighted path between two nodes.

```cypher
CALL algo.bfs(1, 42, 'Person', 'KNOWS')
YIELD path, cost
RETURN path, cost
```

```python
path = client.bfs(source=1, target=42, label="Person", edge_type="KNOWS")
if path:
    print(f"Path: {path['path']}, cost: {path['cost']}")
```

## Shortest Path (Dijkstra)

Find the shortest weighted path.

```cypher
CALL algo.dijkstra(1, 42, 'City', 'ROAD', 'distance')
YIELD path, cost
RETURN path, cost
```

```python
path = client.dijkstra(source=1, target=42, weight_property="distance")
if path:
    print(f"Path: {path['path']}, cost: {path['cost']}")
```

## Weakly Connected Components

Find groups of nodes that are reachable from each other (ignoring edge direction).

```cypher
CALL algo.wcc('Person', 'KNOWS')
YIELD componentId, nodeCount
RETURN componentId, nodeCount
ORDER BY nodeCount DESC
```

```python
wcc = client.wcc(label="Person", edge_type="KNOWS")
print(f"{wcc['component_count']} components found")
```

## Strongly Connected Components

Find groups of nodes where every node is reachable from every other node following edge direction.

```cypher
CALL algo.scc('Person', 'KNOWS')
YIELD componentId, nodeCount
RETURN componentId, nodeCount
```

```python
scc = client.scc(label="Person", edge_type="KNOWS")
```

## Community Detection (Label Propagation)

Detect communities by propagating labels through the graph.

```cypher
CALL algo.cdlp('Person', 'KNOWS', {iterations: 10})
YIELD nodeId, communityId
RETURN communityId, count(*) AS size
ORDER BY size DESC
```

## Local Clustering Coefficient

Measure how clustered each node's neighborhood is (0.0 to 1.0).

```cypher
CALL algo.lcc('Person', 'KNOWS')
YIELD nodeId, coefficient
RETURN nodeId, coefficient
ORDER BY coefficient DESC
LIMIT 10
```

## Minimum Spanning Tree

Find the minimum cost tree connecting all nodes (Prim's algorithm).

```cypher
CALL algo.mst('City', 'ROAD', 'distance')
YIELD edges, totalWeight
RETURN edges, totalWeight
```

## Maximum Flow

Compute the maximum flow between a source and sink node (Edmonds-Karp).

```cypher
CALL algo.maxFlow(1, 42, 'PIPE', 'capacity')
YIELD maxFlow, flowEdges
RETURN maxFlow
```

## Triangle Count

Count the number of triangles in the graph.

```cypher
CALL algo.triangleCount('Person', 'KNOWS')
YIELD count
RETURN count
```

```python
count = client.triangle_count(label="Person", edge_type="KNOWS")
print(f"Found {count} triangles")
```

## Notes

- Algorithm methods on the SDK are available only in **embedded mode** (Rust and Python). For remote mode, use the Cypher `CALL` syntax through the query API.
- All algorithms operate on the in-memory graph and return results without modifying the stored data.
- For large graphs, algorithm execution time scales with the number of nodes and edges involved. PageRank with 20 iterations over 1M nodes typically completes in under a second.
