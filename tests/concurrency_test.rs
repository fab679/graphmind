//! Concurrency and thread safety tests
//!
//! Tests concurrent read access to a shared GraphStore and large graph operations.

use graphmind::{GraphStore, QueryEngine};
use std::sync::Arc;
use std::thread;

/// Helper: run a mutating query, panic on failure.
fn exec_mut(engine: &QueryEngine, store: &mut GraphStore, q: &str) {
    engine
        .execute_mut(q, store, "default")
        .unwrap_or_else(|e| panic!("execute_mut failed for: {q}\n  error: {e}"));
}

/// Helper: run a read-only query, panic on failure.
fn exec(
    engine: &QueryEngine,
    store: &GraphStore,
    q: &str,
) -> graphmind::query::executor::RecordBatch {
    engine
        .execute(q, store)
        .unwrap_or_else(|e| panic!("execute failed for: {q}\n  error: {e}"))
}

/// Helper: extract an integer value from a column in the first record.
fn first_int(batch: &graphmind::query::executor::RecordBatch, col: &str) -> i64 {
    batch.records[0]
        .get(col)
        .unwrap_or_else(|| panic!("column '{col}' not found"))
        .as_property()
        .unwrap_or_else(|| panic!("column '{col}' is not a Property value"))
        .as_integer()
        .unwrap_or_else(|| panic!("column '{col}' is not an Integer"))
}

// ---------------------------------------------------------------------------
// Concurrent read tests
// ---------------------------------------------------------------------------

#[test]
fn test_concurrent_reads() {
    let mut store = GraphStore::new();
    let engine = QueryEngine::new();
    for i in 0..100 {
        exec_mut(&engine, &mut store, &format!("CREATE (n:N {{id: {}}})", i));
    }
    let store = Arc::new(store);
    let engine = Arc::new(engine);
    let mut handles = vec![];
    for _ in 0..10 {
        let s = Arc::clone(&store);
        let e = Arc::clone(&engine);
        handles.push(thread::spawn(move || {
            let r = e.execute("MATCH (n:N) RETURN count(n) AS cnt", &s).unwrap();
            assert_eq!(
                r.records[0]
                    .get("cnt")
                    .unwrap()
                    .as_property()
                    .unwrap()
                    .as_integer(),
                Some(100)
            );
        }));
    }
    for h in handles {
        h.join().unwrap();
    }
}

#[test]
fn test_concurrent_reads_different_queries() {
    let mut store = GraphStore::new();
    let engine = QueryEngine::new();
    for i in 0..50 {
        exec_mut(
            &engine,
            &mut store,
            &format!("CREATE (n:Person {{name: 'Person{}', age: {}}})", i, 20 + i),
        );
    }
    let store = Arc::new(store);
    let engine = Arc::new(engine);
    let mut handles = vec![];

    // Thread 1: count all
    {
        let s = Arc::clone(&store);
        let e = Arc::clone(&engine);
        handles.push(thread::spawn(move || {
            let r = e
                .execute("MATCH (n:Person) RETURN count(n) AS cnt", &s)
                .unwrap();
            assert_eq!(
                r.records[0]
                    .get("cnt")
                    .unwrap()
                    .as_property()
                    .unwrap()
                    .as_integer(),
                Some(50)
            );
        }));
    }

    // Thread 2: filter by age
    {
        let s = Arc::clone(&store);
        let e = Arc::clone(&engine);
        handles.push(thread::spawn(move || {
            let r = e
                .execute(
                    "MATCH (n:Person) WHERE n.age >= 50 RETURN count(n) AS cnt",
                    &s,
                )
                .unwrap();
            let count = r.records[0]
                .get("cnt")
                .unwrap()
                .as_property()
                .unwrap()
                .as_integer()
                .unwrap();
            assert!(count > 0);
        }));
    }

    // Thread 3: order by
    {
        let s = Arc::clone(&store);
        let e = Arc::clone(&engine);
        handles.push(thread::spawn(move || {
            let r = e
                .execute("MATCH (n:Person) RETURN n.name ORDER BY n.age LIMIT 5", &s)
                .unwrap();
            assert_eq!(r.len(), 5);
        }));
    }

    for h in handles {
        h.join().unwrap();
    }
}

// ---------------------------------------------------------------------------
// Large graph tests
// ---------------------------------------------------------------------------

#[test]
fn test_large_graph_1000_nodes() {
    let mut store = GraphStore::new();
    let engine = QueryEngine::new();
    for i in 0..1000 {
        exec_mut(
            &engine,
            &mut store,
            &format!("CREATE (n:Big {{id: {}}})", i),
        );
    }
    let r = exec(&engine, &store, "MATCH (n:Big) RETURN count(n) AS cnt");
    assert_eq!(first_int(&r, "cnt"), 1000);
}

#[test]
fn test_large_graph_with_edges() {
    let mut store = GraphStore::new();
    let engine = QueryEngine::new();

    // Create a chain: 0 -> 1 -> 2 -> ... -> 99
    for i in 0..100 {
        exec_mut(
            &engine,
            &mut store,
            &format!("CREATE (n:Chain {{idx: {}}})", i),
        );
    }
    for i in 0..99 {
        exec_mut(
            &engine,
            &mut store,
            &format!(
                "MATCH (a:Chain {{idx: {}}}), (b:Chain {{idx: {}}}) CREATE (a)-[:NEXT]->(b)",
                i,
                i + 1
            ),
        );
    }

    // Count edges
    let r = exec(
        &engine,
        &store,
        "MATCH ()-[r:NEXT]->() RETURN count(r) AS cnt",
    );
    assert_eq!(first_int(&r, "cnt"), 99);

    // Traverse 3 hops from start
    let r = exec(
        &engine,
        &store,
        "MATCH (a:Chain {idx: 0})-[:NEXT]->(b)-[:NEXT]->(c)-[:NEXT]->(d) RETURN d.idx",
    );
    assert_eq!(r.len(), 1);
    assert_eq!(first_int(&r, "d.idx"), 3);
}

#[test]
fn test_many_labels() {
    let mut store = GraphStore::new();
    let engine = QueryEngine::new();

    for i in 0..20 {
        let label = format!("Label{}", i);
        exec_mut(
            &engine,
            &mut store,
            &format!("CREATE (n:{} {{idx: {}}})", label, i),
        );
    }

    // Each label should have exactly 1 node
    let r = exec(&engine, &store, "MATCH (n:Label5) RETURN count(n) AS cnt");
    assert_eq!(first_int(&r, "cnt"), 1);

    let r = exec(&engine, &store, "MATCH (n:Label19) RETURN count(n) AS cnt");
    assert_eq!(first_int(&r, "cnt"), 1);
}

#[test]
fn test_query_engine_shared_across_threads() {
    // Verify QueryEngine can be safely shared across threads
    let engine = Arc::new(QueryEngine::new());
    let mut handles = vec![];
    for _ in 0..5 {
        let e = Arc::clone(&engine);
        handles.push(thread::spawn(move || {
            let mut store = GraphStore::new();
            e.execute_mut("CREATE (n:T {v: 1})", &mut store, "default")
                .unwrap();
            let r = e
                .execute("MATCH (n:T) RETURN count(n) AS cnt", &store)
                .unwrap();
            assert_eq!(
                r.records[0]
                    .get("cnt")
                    .unwrap()
                    .as_property()
                    .unwrap()
                    .as_integer(),
                Some(1)
            );
        }));
    }
    for h in handles {
        h.join().unwrap();
    }
}
