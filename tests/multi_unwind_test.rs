#[test]
fn test_inline_property_match() {
    let mut store = graphmind::GraphStore::new();
    let engine = graphmind::QueryEngine::new();
    engine
        .execute_mut(
            "CREATE (:A)<-[:KNOWS {name: 'monkey'}]-()-[:KNOWS {name: 'woot'}]->(:B)",
            &mut store,
            "default",
        )
        .unwrap();
    eprintln!(
        "nodes: {}, edges: {}",
        store.node_count(),
        store.edge_count()
    );
    let result = engine
        .execute(
            "MATCH (node)-[r:KNOWS {name: 'monkey'}]->(a) RETURN a",
            &store,
        )
        .unwrap();
    eprintln!("rows: {}", result.len());
    for r in &result.records {
        eprintln!("  a = {:?}", r.get("a"));
    }
    assert_eq!(result.len(), 1, "Expected 1 match for directed monkey edge");
}
