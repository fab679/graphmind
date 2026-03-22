#[test]
fn test_multi_with_create() {
    let mut store = graphmind::GraphStore::new();
    let engine = graphmind::QueryEngine::new();
    let q = "CREATE (a) WITH a WITH * CREATE (b) CREATE (a)<-[:T]-(b)";
    match engine.execute_mut(q, &mut store, "default") {
        Ok(_) => eprintln!(
            "OK: {} nodes, {} edges",
            store.node_count(),
            store.edge_count()
        ),
        Err(e) => eprintln!("ERROR: {}", e),
    }
}
