#[test]
fn test_rewrite_doesnt_break_match_create() {
    let mut store = graphmind::GraphStore::new();
    let engine = graphmind::QueryEngine::new();

    // Create people via the SAME execute_mut call (semicolons)
    let full_script = "CREATE (p1:Person {id: 'p1', name: 'Alice'});
CREATE (p2:Person {id: 'p2', name: 'Bob'});
CREATE (p3:Person {id: 'p3', name: 'Charlie'});
// ─── Social graph ──────────────────
MATCH (a:Person {id:'p1'}), (b:Person {id:'p2'}) CREATE (a)-[:FOLLOWS]->(b);
MATCH (a:Person {id:'p1'}), (b:Person {id:'p3'}) CREATE (a)-[:FOLLOWS]->(b)";

    match engine.execute_mut(full_script, &mut store, "default") {
        Ok(_) => eprintln!("OK"),
        Err(e) => eprintln!("ERROR: {}", e),
    }
    eprintln!(
        "Nodes: {}, Edges: {}",
        store.node_count(),
        store.edge_count()
    );
    assert_eq!(store.node_count(), 3);
    assert_eq!(store.edge_count(), 2, "Should have 2 FOLLOWS edges");
}
