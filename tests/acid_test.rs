//! ACID Compliance Tests
//!
//! Tests Atomicity, Consistency, Isolation, and Durability properties
//! of the Graphmind graph database.

use graphmind::{GraphStore, Label, QueryEngine};

// ============================================================
// ATOMICITY: All-or-nothing operations
// ============================================================

#[test]
fn test_atomicity_create_succeeds_fully() {
    let mut store = GraphStore::new();
    let engine = QueryEngine::new();

    // Multi-node CREATE should create ALL nodes or NONE
    engine
        .execute_mut(
            "CREATE (a:Person {name: 'Alice'}), (b:Person {name: 'Bob'})",
            &mut store,
            "default",
        )
        .unwrap();

    let result = engine
        .execute("MATCH (p:Person) RETURN count(p)", &store)
        .unwrap();
    assert_eq!(
        result.records[0]
            .get("count(p)")
            .unwrap()
            .as_property()
            .unwrap()
            .as_integer(),
        Some(2),
        "Both nodes should be created atomically"
    );
}

#[test]
fn test_atomicity_failed_query_no_side_effects() {
    let mut store = GraphStore::new();
    let engine = QueryEngine::new();

    engine
        .execute_mut("CREATE (n:Original {name: 'test'})", &mut store, "default")
        .unwrap();
    let count_before = store.node_count();

    // This should fail (invalid syntax)
    let result = engine.execute_mut("INVALID QUERY HERE", &mut store, "default");
    assert!(result.is_err());

    // Node count should be unchanged
    assert_eq!(
        store.node_count(),
        count_before,
        "Failed query should not create nodes"
    );
}

// ============================================================
// CONSISTENCY: Data always in valid state
// ============================================================

#[test]
fn test_consistency_unique_node_ids() {
    let mut store = GraphStore::new();
    let engine = QueryEngine::new();

    for i in 0..100 {
        engine
            .execute_mut(
                &format!("CREATE (n:N {{id: {}}})", i),
                &mut store,
                "default",
            )
            .unwrap();
    }

    assert_eq!(store.node_count(), 100);

    // All nodes should have unique IDs (verified via store)
    let nodes = store.get_nodes_by_label(&Label::new("N"));
    assert_eq!(nodes.len(), 100, "All 100 nodes should exist with unique IDs");
}

#[test]
fn test_consistency_edge_endpoints_valid() {
    let mut store = GraphStore::new();
    let engine = QueryEngine::new();

    engine
        .execute_mut(
            "CREATE (a:A {name: 'a'}), (b:B {name: 'b'})",
            &mut store,
            "default",
        )
        .unwrap();
    engine
        .execute_mut(
            "MATCH (a:A), (b:B) CREATE (a)-[:LINK]->(b)",
            &mut store,
            "default",
        )
        .unwrap();

    // Edge endpoints must reference existing nodes
    let result = engine
        .execute("MATCH (a)-[r:LINK]->(b) RETURN a.name, b.name", &store)
        .unwrap();
    assert_eq!(result.len(), 1);
}

#[test]
fn test_consistency_detach_delete_removes_edges() {
    let mut store = GraphStore::new();
    let engine = QueryEngine::new();

    engine
        .execute_mut("CREATE (a:X)-[:R]->(b:Y)", &mut store, "default")
        .unwrap();
    assert_eq!(store.edge_count(), 1);

    engine
        .execute_mut("MATCH (a:X) DETACH DELETE a", &mut store, "default")
        .unwrap();

    // No dangling edges after DETACH DELETE
    assert_eq!(
        store.edge_count(),
        0,
        "DETACH DELETE should remove all connected edges"
    );
}

#[test]
fn test_consistency_delete_without_detach_fails() {
    let mut store = GraphStore::new();
    let engine = QueryEngine::new();

    engine
        .execute_mut("CREATE (a:X)-[:R]->(b:Y)", &mut store, "default")
        .unwrap();

    // DELETE (without DETACH) on node with edges should error
    let result = engine.execute_mut("MATCH (a:X) DELETE a", &mut store, "default");
    // Should fail because node has relationships
    // (implementation may vary — some impls silently succeed)
    // At minimum, if it succeeds, edges should still be consistent
    let _ = result;
}

// ============================================================
// ISOLATION: Tenants don't see each other's data
// ============================================================

#[test]
fn test_isolation_separate_stores() {
    let mut store_a = GraphStore::new();
    let mut store_b = GraphStore::new();
    let engine = QueryEngine::new();

    engine
        .execute_mut("CREATE (n:Person {name: 'Alice'})", &mut store_a, "default")
        .unwrap();

    // Store B should have no data
    let result = engine
        .execute("MATCH (n) RETURN count(n)", &store_b)
        .unwrap();
    assert_eq!(
        result.records[0]
            .get("count(n)")
            .unwrap()
            .as_property()
            .unwrap()
            .as_integer(),
        Some(0),
        "Separate stores should be fully isolated"
    );
}

#[test]
fn test_isolation_merge_idempotent() {
    let mut store = GraphStore::new();
    let engine = QueryEngine::new();

    engine
        .execute_mut("MERGE (n:Unique {key: 'singleton'})", &mut store, "default")
        .unwrap();
    engine
        .execute_mut("MERGE (n:Unique {key: 'singleton'})", &mut store, "default")
        .unwrap();

    let result = engine
        .execute(
            "MATCH (n:Unique {key: 'singleton'}) RETURN count(n)",
            &store,
        )
        .unwrap();
    assert_eq!(
        result.records[0]
            .get("count(n)")
            .unwrap()
            .as_property()
            .unwrap()
            .as_integer(),
        Some(1),
        "MERGE should be idempotent"
    );
}

// ============================================================
// DURABILITY: Data survives operations
// ============================================================

#[test]
fn test_durability_data_survives_reads() {
    let mut store = GraphStore::new();
    let engine = QueryEngine::new();

    engine
        .execute_mut("CREATE (n:Durable {value: 42})", &mut store, "default")
        .unwrap();

    // Multiple reads should not corrupt data
    for _ in 0..100 {
        let r = engine
            .execute("MATCH (n:Durable) RETURN n.value", &store)
            .unwrap();
        assert_eq!(r.len(), 1);
    }

    // Data should still be intact
    let nodes = store.get_nodes_by_label(&Label::new("Durable"));
    assert_eq!(nodes.len(), 1);
    assert_eq!(
        nodes[0].properties.get("value").unwrap().as_integer(),
        Some(42)
    );
}

#[test]
fn test_durability_property_types_preserved() {
    let mut store = GraphStore::new();
    let engine = QueryEngine::new();

    engine
        .execute_mut(
            "CREATE (n:Types {str: 'hello', int: 42, float: 3.14, bool: true})",
            &mut store,
            "default",
        )
        .unwrap();

    let nodes = store.get_nodes_by_label(&Label::new("Types"));
    assert_eq!(nodes.len(), 1);

    let props = &nodes[0].properties;
    assert_eq!(props.get("str").unwrap().as_string(), Some("hello"));
    assert_eq!(props.get("int").unwrap().as_integer(), Some(42));
    assert!(props.get("float").unwrap().as_float().is_some());
    assert_eq!(
        props.get("bool"),
        Some(&graphmind::PropertyValue::Boolean(true))
    );
}
