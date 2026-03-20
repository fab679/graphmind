//! Local Clustering Coefficient (LCC)
//!
//! Computes the local clustering coefficient for each node.
//!
//! Undirected: LCC(v) = 2 * T(v) / (deg(v) * (deg(v) - 1))
//! Directed:   LCC(v) = T(v) / (deg(v) * (deg(v) - 1))
//!
//! where T(v) is the number of triangles (edges among neighbors) containing v,
//! and deg(v) is the undirected degree (union of successors + predecessors) for
//! undirected mode, or the number of distinct neighbors for directed mode.

use super::common::{GraphView, NodeId};
use std::collections::{HashMap, HashSet};

/// Result of LCC computation
#[derive(Debug, Clone)]
pub struct LccResult {
    /// Clustering coefficient per node
    pub coefficients: HashMap<NodeId, f64>,
    /// Global average clustering coefficient
    pub average: f64,
}

/// Compute local clustering coefficients for all nodes (undirected mode).
///
/// Uses undirected edges (union of successors + predecessors).
/// This is the backward-compatible entry point.
pub fn local_clustering_coefficient(view: &GraphView) -> LccResult {
    local_clustering_coefficient_directed(view, false)
}

/// Compute local clustering coefficients for all nodes.
///
/// When `directed=false`: uses undirected neighbor sets (union of successors +
/// predecessors), counts undirected edges among neighbors, divides by
/// `d*(d-1)/2`.
///
/// When `directed=true`: uses undirected neighbor sets for neighborhood
/// discovery, but counts *directed* edges (u→w) among neighbors, divides by
/// `d*(d-1)` (the maximum number of directed edges among d nodes).
pub fn local_clustering_coefficient_directed(view: &GraphView, directed: bool) -> LccResult {
    let n = view.node_count;
    if n == 0 {
        return LccResult {
            coefficients: HashMap::new(),
            average: 0.0,
        };
    }

    // Build undirected neighbor sets for each node (used for neighborhood)
    let mut neighbors: Vec<HashSet<usize>> = Vec::with_capacity(n);
    for idx in 0..n {
        let mut set = HashSet::new();
        for &s in view.successors(idx) {
            if s != idx {
                set.insert(s);
            }
        }
        for &p in view.predecessors(idx) {
            if p != idx {
                set.insert(p);
            }
        }
        neighbors.push(set);
    }

    // For directed mode, also build successor sets for fast directed edge lookup
    let successor_sets: Option<Vec<HashSet<usize>>> = if directed {
        let mut sets = Vec::with_capacity(n);
        for idx in 0..n {
            let set: HashSet<usize> = view.successors(idx).iter().copied().collect();
            sets.push(set);
        }
        Some(sets)
    } else {
        None
    };

    let mut coefficients = HashMap::with_capacity(n);
    let mut sum = 0.0;

    for idx in 0..n {
        let deg = neighbors[idx].len();
        if deg < 2 {
            coefficients.insert(view.index_to_node[idx], 0.0);
            continue;
        }

        let neighbor_vec: Vec<usize> = neighbors[idx].iter().cloned().collect();

        if directed {
            // Directed mode: count directed edges u→w among neighbors
            let succ_sets = successor_sets.as_ref().unwrap();
            let mut directed_edges = 0usize;
            for i in 0..neighbor_vec.len() {
                for j in 0..neighbor_vec.len() {
                    if i != j && succ_sets[neighbor_vec[i]].contains(&neighbor_vec[j]) {
                        directed_edges += 1;
                    }
                }
            }
            let max_edges = deg * (deg - 1); // d*(d-1) for directed
            let cc = directed_edges as f64 / max_edges as f64;
            coefficients.insert(view.index_to_node[idx], cc);
            sum += cc;
        } else {
            // Undirected mode: count undirected edges among neighbors
            let mut triangle_edges = 0usize;
            for i in 0..neighbor_vec.len() {
                for j in (i + 1)..neighbor_vec.len() {
                    if neighbors[neighbor_vec[i]].contains(&neighbor_vec[j]) {
                        triangle_edges += 1;
                    }
                }
            }
            let max_edges = deg * (deg - 1) / 2; // d*(d-1)/2 for undirected
            let cc = triangle_edges as f64 / max_edges as f64;
            coefficients.insert(view.index_to_node[idx], cc);
            sum += cc;
        }
    }

    let average = if n > 0 { sum / n as f64 } else { 0.0 };

    LccResult {
        coefficients,
        average,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::common::GraphView;
    use std::collections::HashMap;

    #[test]
    fn test_lcc_triangle() {
        // Complete triangle: 1-2, 2-3, 1-3
        let index_to_node = vec![1, 2, 3];
        let mut node_to_index = HashMap::new();
        node_to_index.insert(1, 0);
        node_to_index.insert(2, 1);
        node_to_index.insert(3, 2);

        let outgoing = vec![vec![1, 2], vec![0, 2], vec![0, 1]];
        let incoming = vec![vec![1, 2], vec![0, 2], vec![0, 1]];

        let view = GraphView::from_adjacency_list(
            3,
            index_to_node,
            node_to_index,
            outgoing,
            incoming,
            None,
        );
        let result = local_clustering_coefficient(&view);

        // All nodes in a complete triangle have LCC = 1.0
        for (_node, cc) in &result.coefficients {
            assert!(
                (cc - 1.0).abs() < 1e-10,
                "Complete triangle LCC should be 1.0, got {}",
                cc
            );
        }
        assert!((result.average - 1.0).abs() < 1e-10);
    }

    #[test]
    fn test_lcc_star() {
        // Star: center 1 connected to 2, 3, 4 (no edges among 2,3,4)
        let index_to_node = vec![1, 2, 3, 4];
        let mut node_to_index = HashMap::new();
        for (i, &id) in index_to_node.iter().enumerate() {
            node_to_index.insert(id, i);
        }

        let outgoing = vec![vec![1, 2, 3], vec![0], vec![0], vec![0]];
        let incoming = vec![vec![1, 2, 3], vec![0], vec![0], vec![0]];

        let view = GraphView::from_adjacency_list(
            4,
            index_to_node,
            node_to_index,
            outgoing,
            incoming,
            None,
        );
        let result = local_clustering_coefficient(&view);

        // Center node: 3 neighbors, no edges among them -> LCC = 0
        assert!((result.coefficients[&1] - 0.0).abs() < 1e-10);
        // Leaf nodes: degree 1 -> LCC = 0
        assert!((result.coefficients[&2] - 0.0).abs() < 1e-10);
    }

    #[test]
    fn test_lcc_empty() {
        let view = GraphView::from_adjacency_list(0, vec![], HashMap::new(), vec![], vec![], None);
        let result = local_clustering_coefficient(&view);
        assert!(result.coefficients.is_empty());
        assert!((result.average - 0.0).abs() < 1e-10);
    }

    #[test]
    fn test_lcc_directed_triangle() {
        // Directed triangle: 1->2, 2->3, 3->1 (cycle)
        // Each node has 2 neighbors (undirected), and there is exactly 1 directed
        // edge among those 2 neighbors. max_edges = 2*(2-1) = 2.
        // LCC = 1/2 = 0.5 for each node.
        let index_to_node = vec![1, 2, 3];
        let mut node_to_index = HashMap::new();
        node_to_index.insert(1, 0);
        node_to_index.insert(2, 1);
        node_to_index.insert(3, 2);

        // 0->1, 1->2, 2->0
        let outgoing = vec![vec![1], vec![2], vec![0]];
        let incoming = vec![vec![2], vec![0], vec![1]];

        let view = GraphView::from_adjacency_list(
            3,
            index_to_node,
            node_to_index,
            outgoing,
            incoming,
            None,
        );
        let result = local_clustering_coefficient_directed(&view, true);

        // Node 0 (id=1): neighbors are {1, 2}. Directed edges among them: 1->2 = 1.
        // max = 2*1 = 2.  LCC = 1/2 = 0.5
        for (&_node, &cc) in &result.coefficients {
            assert!(
                (cc - 0.5).abs() < 1e-10,
                "Directed cycle triangle LCC should be 0.5, got {}",
                cc
            );
        }
    }

    #[test]
    fn test_lcc_directed_complete_triangle() {
        // Fully connected directed triangle: all 6 directed edges present
        // Each node has 2 neighbors, 2 directed edges among them, max = 2.
        // LCC = 2/2 = 1.0
        let index_to_node = vec![1, 2, 3];
        let mut node_to_index = HashMap::new();
        node_to_index.insert(1, 0);
        node_to_index.insert(2, 1);
        node_to_index.insert(3, 2);

        let outgoing = vec![vec![1, 2], vec![0, 2], vec![0, 1]];
        let incoming = vec![vec![1, 2], vec![0, 2], vec![0, 1]];

        let view = GraphView::from_adjacency_list(
            3,
            index_to_node,
            node_to_index,
            outgoing,
            incoming,
            None,
        );
        let result = local_clustering_coefficient_directed(&view, true);

        for (&_node, &cc) in &result.coefficients {
            assert!(
                (cc - 1.0).abs() < 1e-10,
                "Fully connected directed triangle LCC should be 1.0, got {}",
                cc
            );
        }
    }
}
