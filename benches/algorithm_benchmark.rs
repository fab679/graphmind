use criterion::{black_box, criterion_group, criterion_main, BenchmarkId, Criterion};
use graphmind::graph::GraphStore;
use graphmind::query::executor::QueryExecutor;
use graphmind::query::parser::parse_query;

/// Build a connected graph with `n` nodes and ~3 edges per node.
fn build_graph(n: usize) -> GraphStore {
    let mut store = GraphStore::new();

    let ids: Vec<_> = (0..n)
        .map(|i| {
            let id = store.create_node("Node");
            if let Some(node) = store.get_node_mut(id) {
                node.set_property("id", i as i64);
            }
            id
        })
        .collect();

    for i in 0..n {
        for offset in [1, 2, 5] {
            let j = (i + offset) % n;
            if i != j {
                let _ = store.create_edge(ids[i], ids[j], "EDGE");
            }
        }
    }

    store
}

fn bench_pagerank(c: &mut Criterion) {
    let mut group = c.benchmark_group("pagerank");

    for size in [50, 200, 500] {
        let store = build_graph(size);
        let query = parse_query("CALL algo.pageRank('Node', 'EDGE') YIELD node, score").unwrap();

        group.bench_with_input(BenchmarkId::new("nodes", size), &size, |b, _| {
            b.iter(|| {
                let executor = QueryExecutor::new(&store);
                executor.execute(black_box(&query)).unwrap()
            });
        });
    }

    group.finish();
}

fn bench_wcc(c: &mut Criterion) {
    let mut group = c.benchmark_group("wcc");

    for size in [50, 200, 500] {
        let store = build_graph(size);
        let query = parse_query("CALL algo.wcc('Node') YIELD node, componentId").unwrap();

        group.bench_with_input(BenchmarkId::new("nodes", size), &size, |b, _| {
            b.iter(|| {
                let executor = QueryExecutor::new(&store);
                executor.execute(black_box(&query)).unwrap()
            });
        });
    }

    group.finish();
}

fn bench_shortest_path(c: &mut Criterion) {
    let store = build_graph(200);

    // Find node IDs 1 and last
    let node_ids: Vec<_> = store
        .get_nodes_by_label(&graphmind::graph::Label::new("Node"))
        .iter()
        .map(|n| n.id)
        .collect();
    let start = node_ids[0];
    let end = node_ids[node_ids.len() - 1];

    let query = parse_query(&format!(
        "CALL algo.shortestPath({}, {}) YIELD path, cost",
        start.0, end.0
    ))
    .unwrap();

    c.bench_function("shortest_path_200", |b| {
        b.iter(|| {
            let executor = QueryExecutor::new(&store);
            executor.execute(black_box(&query)).unwrap()
        });
    });
}

criterion_group!(benches, bench_pagerank, bench_wcc, bench_shortest_path);
criterion_main!(benches);
