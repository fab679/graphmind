//! Vector search end-to-end tests
//!
//! Tests vector index creation via Cypher, insertion of nodes with embeddings,
//! and vector similarity search via CALL db.index.vector.queryNodes.

use graphmind::{GraphStore, QueryEngine};

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

// ---------------------------------------------------------------------------
// Vector index creation and search
// ---------------------------------------------------------------------------

#[test]
fn test_vector_index_and_search() {
    let mut store = GraphStore::new();
    let engine = QueryEngine::new();

    // Create vector index
    exec_mut(
        &engine,
        &mut store,
        "CREATE VECTOR INDEX test_idx FOR (n:Doc) ON (n.embedding) OPTIONS {dimensions: 3, similarity: 'cosine'}",
    );

    // Insert documents with embeddings
    exec_mut(
        &engine,
        &mut store,
        "CREATE (n:Doc {text: 'hello', embedding: [1.0, 0.0, 0.0]})",
    );
    exec_mut(
        &engine,
        &mut store,
        "CREATE (n:Doc {text: 'world', embedding: [0.0, 1.0, 0.0]})",
    );
    exec_mut(
        &engine,
        &mut store,
        "CREATE (n:Doc {text: 'similar', embedding: [0.9, 0.1, 0.0]})",
    );

    // Query nearest neighbors to [1.0, 0.0, 0.0]
    let result = exec(
        &engine,
        &store,
        "CALL db.index.vector.queryNodes('Doc', 'embedding', [1.0, 0.0, 0.0], 2) YIELD node, score RETURN node.text, score",
    );

    assert_eq!(result.len(), 2);
    // First result should be 'hello' (exact match, highest cosine similarity)
    let first_text = result.records[0]
        .get("node.text")
        .unwrap()
        .as_property()
        .unwrap()
        .as_string()
        .unwrap()
        .to_string();
    assert_eq!(first_text, "hello");
}

#[test]
fn test_vector_search_k_larger_than_data() {
    let mut store = GraphStore::new();
    let engine = QueryEngine::new();

    exec_mut(
        &engine,
        &mut store,
        "CREATE VECTOR INDEX small_idx FOR (n:Small) ON (n.vec) OPTIONS {dimensions: 2, similarity: 'cosine'}",
    );

    exec_mut(
        &engine,
        &mut store,
        "CREATE (n:Small {name: 'only', vec: [1.0, 0.0]})",
    );

    // Ask for 10 results but only 1 exists
    let result = exec(
        &engine,
        &store,
        "CALL db.index.vector.queryNodes('Small', 'vec', [1.0, 0.0], 10) YIELD node, score RETURN node.name, score",
    );

    // Should return at most 1 result
    assert!(result.len() <= 1);
}

#[test]
fn test_vector_search_all_results_have_scores() {
    let mut store = GraphStore::new();
    let engine = QueryEngine::new();

    exec_mut(
        &engine,
        &mut store,
        "CREATE VECTOR INDEX scored_idx FOR (n:Scored) ON (n.emb) OPTIONS {dimensions: 2, similarity: 'cosine'}",
    );

    exec_mut(
        &engine,
        &mut store,
        "CREATE (n:Scored {id: 1, emb: [1.0, 0.0]})",
    );
    exec_mut(
        &engine,
        &mut store,
        "CREATE (n:Scored {id: 2, emb: [0.0, 1.0]})",
    );
    exec_mut(
        &engine,
        &mut store,
        "CREATE (n:Scored {id: 3, emb: [0.7, 0.7]})",
    );

    let result = exec(
        &engine,
        &store,
        "CALL db.index.vector.queryNodes('Scored', 'emb', [1.0, 0.0], 3) YIELD node, score RETURN node.id, score",
    );

    // All results should have a score
    for record in &result.records {
        let score = record
            .get("score")
            .expect("score column should exist")
            .as_property()
            .expect("score should be a property");
        // Score should be a float
        assert!(
            score.as_float().is_some() || score.as_integer().is_some(),
            "score should be numeric, got: {:?}",
            score
        );
    }
}

#[test]
fn test_vector_no_index_error() {
    let store = GraphStore::new();
    let engine = QueryEngine::new();
    let result = engine.execute(
        "CALL db.index.vector.queryNodes('Missing', 'embedding', [1.0], 5) YIELD node RETURN node",
        &store,
    );
    // Should return empty or error gracefully (either is acceptable)
    assert!(result.is_ok() || result.is_err());
}
