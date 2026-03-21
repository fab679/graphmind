use criterion::{black_box, criterion_group, criterion_main, BenchmarkId, Criterion};
use graphmind::graph::GraphStore;
use graphmind::query::executor::{MutQueryExecutor, QueryExecutor};
use graphmind::query::parser::parse_query;

/// Build a graph with `n` Person nodes and ~3 KNOWS edges per node.
fn setup_graph(n: usize) -> GraphStore {
    let mut store = GraphStore::new();

    // Create Person nodes
    let ids: Vec<_> = (0..n)
        .map(|i| {
            let id = store.create_node("Person");
            if let Some(node) = store.get_node_mut(id) {
                node.set_property("name", format!("Person{}", i));
                node.set_property("age", (20 + (i % 50)) as i64);
            }
            id
        })
        .collect();

    // Each person KNOWS ~3 others
    for i in 0..n {
        for offset in [1, 7, 13] {
            let j = (i + offset) % n;
            if i != j {
                let _ = store.create_edge(ids[i], ids[j], "KNOWS");
            }
        }
    }

    store
}

// ---------------------------------------------------------------------------
// Parse benchmarks
// ---------------------------------------------------------------------------

fn bench_parse(c: &mut Criterion) {
    let queries = vec![
        ("simple_match", "MATCH (n:Person) RETURN n"),
        (
            "match_where",
            "MATCH (n:Person) WHERE n.age > 30 RETURN n.name, n.age",
        ),
        (
            "match_pattern",
            "MATCH (a:Person)-[:KNOWS]->(b:Person) RETURN a.name, b.name",
        ),
        (
            "aggregation",
            "MATCH (n:Person) RETURN count(n), avg(n.age), max(n.age)",
        ),
        (
            "multi_hop",
            "MATCH (a:Person)-[:KNOWS]->(b)-[:KNOWS]->(c) RETURN a.name, c.name",
        ),
    ];

    let mut group = c.benchmark_group("parse");
    for (name, query) in &queries {
        group.bench_with_input(BenchmarkId::new("query", name), query, |b, q| {
            b.iter(|| parse_query(black_box(q)).unwrap());
        });
    }
    group.finish();
}

// ---------------------------------------------------------------------------
// Execute benchmarks
// ---------------------------------------------------------------------------

fn bench_execute(c: &mut Criterion) {
    let store = setup_graph(100);

    let cases: Vec<(&str, &str)> = vec![
        ("scan_all", "MATCH (n:Person) RETURN n"),
        ("filter", "MATCH (n:Person) WHERE n.age > 40 RETURN n.name"),
        (
            "pattern_1hop",
            "MATCH (a:Person)-[:KNOWS]->(b) RETURN a.name, b.name",
        ),
        (
            "pattern_2hop",
            "MATCH (a:Person {name: 'Person0'})-[:KNOWS]->(b)-[:KNOWS]->(c) RETURN c.name",
        ),
        (
            "aggregation",
            "MATCH (n:Person) RETURN count(n), avg(n.age)",
        ),
        (
            "order_limit",
            "MATCH (n:Person) RETURN n.name ORDER BY n.age DESC LIMIT 10",
        ),
    ];

    let mut group = c.benchmark_group("execute");
    for (name, cypher) in &cases {
        let query = parse_query(cypher).unwrap();
        group.bench_function(*name, |b| {
            b.iter(|| {
                let executor = QueryExecutor::new(&store);
                executor.execute(black_box(&query)).unwrap()
            });
        });
    }
    group.finish();
}

// ---------------------------------------------------------------------------
// Write benchmarks
// ---------------------------------------------------------------------------

fn bench_write(c: &mut Criterion) {
    let mut group = c.benchmark_group("write");

    group.bench_function("create_node", |b| {
        let mut store = GraphStore::new();
        let mut i = 0u64;
        b.iter(|| {
            let q = parse_query(&format!("CREATE (n:Bench {{id: {}}})", i)).unwrap();
            let mut executor = MutQueryExecutor::new(&mut store, "default".to_string());
            executor.execute(&q).unwrap();
            i += 1;
        });
    });

    group.bench_function("create_edge", |b| {
        let mut store = GraphStore::new();
        let q_a = parse_query("CREATE (a:X {id: 1})").unwrap();
        let q_b = parse_query("CREATE (b:X {id: 2})").unwrap();
        let mut executor = MutQueryExecutor::new(&mut store, "default".to_string());
        executor.execute(&q_a).unwrap();
        executor.execute(&q_b).unwrap();

        let q_edge =
            parse_query("MATCH (a:X {id: 1}), (b:X {id: 2}) CREATE (a)-[:R]->(b)").unwrap();
        b.iter(|| {
            let mut executor = MutQueryExecutor::new(&mut store, "default".to_string());
            executor.execute(black_box(&q_edge)).unwrap();
        });
    });

    group.finish();
}

// ---------------------------------------------------------------------------
// Scale benchmarks
// ---------------------------------------------------------------------------

fn bench_scale(c: &mut Criterion) {
    let mut group = c.benchmark_group("scale");

    for size in [10, 100, 500, 1000] {
        let store = setup_graph(size);
        let query = parse_query("MATCH (n:Person) RETURN count(n)").unwrap();
        group.bench_with_input(BenchmarkId::new("scan", size), &size, |b, _| {
            b.iter(|| {
                let executor = QueryExecutor::new(&store);
                executor.execute(black_box(&query)).unwrap()
            });
        });
    }

    group.finish();
}

criterion_group!(
    benches,
    bench_parse,
    bench_execute,
    bench_write,
    bench_scale
);
criterion_main!(benches);
