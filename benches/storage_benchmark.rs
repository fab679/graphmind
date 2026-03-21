use criterion::{black_box, criterion_group, criterion_main, Criterion};
use graphmind::graph::{GraphStore, Label};

fn bench_node_creation(c: &mut Criterion) {
    c.bench_function("create_1000_nodes", |b| {
        b.iter(|| {
            let mut store = GraphStore::new();
            for i in 0..1000 {
                let id = store.create_node("Person");
                if let Some(node) = store.get_node_mut(id) {
                    node.set_property("name", format!("Node{}", i));
                    node.set_property("age", (i % 80) as i64);
                }
            }
            black_box(store.node_count())
        });
    });
}

fn bench_edge_creation(c: &mut Criterion) {
    c.bench_function("create_1000_edges", |b| {
        b.iter_with_setup(
            || {
                let mut store = GraphStore::new();
                let ids: Vec<_> = (0..100).map(|_| store.create_node("N")).collect();
                (store, ids)
            },
            |(mut store, ids)| {
                for i in 0..1000 {
                    let src = ids[i % ids.len()];
                    let tgt = ids[(i * 7 + 3) % ids.len()];
                    let _ = store.create_edge(src, tgt, "R");
                }
                black_box(store.edge_count())
            },
        );
    });
}

fn bench_node_lookup(c: &mut Criterion) {
    let mut store = GraphStore::new();
    let ids: Vec<_> = (0..10_000)
        .map(|i| {
            let id = store.create_node("Person");
            if let Some(node) = store.get_node_mut(id) {
                node.set_property("name", format!("P{}", i));
            }
            id
        })
        .collect();

    c.bench_function("node_lookup_10k", |b| {
        let mut i = 0usize;
        b.iter(|| {
            let id = ids[i % ids.len()];
            black_box(store.get_node(id));
            i += 1;
        });
    });
}

fn bench_label_index(c: &mut Criterion) {
    let mut store = GraphStore::new();
    for _ in 0..5000 {
        store.create_node("Person");
    }
    for _ in 0..3000 {
        store.create_node("Company");
    }
    for _ in 0..2000 {
        store.create_node("City");
    }

    c.bench_function("label_scan_5000", |b| {
        b.iter(|| black_box(store.get_nodes_by_label(&Label::new("Person")).len()));
    });
}

fn bench_adjacency(c: &mut Criterion) {
    let mut store = GraphStore::new();
    let center = store.create_node("Hub");
    for _ in 0..100 {
        let spoke = store.create_node("Spoke");
        store.create_edge(center, spoke, "CONNECTED").unwrap();
    }

    c.bench_function("outgoing_edges_100", |b| {
        b.iter(|| black_box(store.get_outgoing_edges(center).len()));
    });
}

fn bench_property_access(c: &mut Criterion) {
    let mut store = GraphStore::new();
    let id = store.create_node("Person");
    if let Some(node) = store.get_node_mut(id) {
        node.set_property("name", "Alice");
        node.set_property("age", 30i64);
        node.set_property("city", "Portland");
        node.set_property("active", true);
    }

    c.bench_function("get_property", |b| {
        b.iter(|| {
            let node = store.get_node(id).unwrap();
            black_box(node.get_property("name"));
            black_box(node.get_property("age"));
        });
    });
}

criterion_group!(
    benches,
    bench_node_creation,
    bench_edge_creation,
    bench_node_lookup,
    bench_label_index,
    bench_adjacency,
    bench_property_access,
);
criterion_main!(benches);
