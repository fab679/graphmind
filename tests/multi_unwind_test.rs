#[test]
fn test_merge_node_idempotent() {
    let mut store = graphmind::GraphStore::new();
    let engine = graphmind::QueryEngine::new();
    engine.execute_mut("MERGE (p:Person {id: 'p1', name: 'Alice'})", &mut store, "default").unwrap();
    engine.execute_mut("MERGE (p:Person {id: 'p1', name: 'Alice'})", &mut store, "default").unwrap();
    assert_eq!(store.node_count(), 1, "MERGE should be idempotent for nodes");
}

#[test]
fn test_merge_full_pattern_creates_all() {
    let mut store = graphmind::GraphStore::new();
    let engine = graphmind::QueryEngine::new();
    engine.execute_mut(
        "MERGE (a:Person {name: 'Alice'})-[:KNOWS]->(b:Person {name: 'Bob'})",
        &mut store, "default"
    ).unwrap();
    assert_eq!(store.node_count(), 2);
    assert_eq!(store.edge_count(), 1);
    
    // Second MERGE — idempotent
    engine.execute_mut(
        "MERGE (a:Person {name: 'Alice'})-[:KNOWS]->(b:Person {name: 'Bob'})",
        &mut store, "default"
    ).unwrap();
    assert_eq!(store.node_count(), 2, "No new nodes");
    assert_eq!(store.edge_count(), 1, "No new edges");
}

#[test]
fn test_merge_on_create_on_match() {
    let mut store = graphmind::GraphStore::new();
    let engine = graphmind::QueryEngine::new();
    engine.execute_mut(
        "MERGE (p:Person {id: 'p1'}) ON CREATE SET p.name = 'Created'",
        &mut store, "default"
    ).unwrap();
    let nodes = store.get_nodes_by_label(&graphmind::Label::new("Person"));
    assert_eq!(nodes[0].properties.get("name").and_then(|v| v.as_string()), Some("Created"));
    
    engine.execute_mut(
        "MERGE (p:Person {id: 'p1'}) ON MATCH SET p.name = 'Matched'",
        &mut store, "default"
    ).unwrap();
    let nodes = store.get_nodes_by_label(&graphmind::Label::new("Person"));
    assert_eq!(nodes[0].properties.get("name").and_then(|v| v.as_string()), Some("Matched"));
    assert_eq!(store.node_count(), 1);
}
