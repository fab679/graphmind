use graphmind::{GraphStore, QueryEngine};

#[test]
fn test_multi_with_unwind_create_stages() {
    let mut store = GraphStore::new();
    let engine = QueryEngine::new();

    let q = r#"CREATE (t:TVShow {id: 1399})
SET t.name = "Game of Thrones"
WITH t
UNWIND ["Drama", "Sci-Fi", "Adventure"] AS genre_name
CREATE (g:Genre {name: genre_name})
CREATE (t)-[:IN_GENRE]->(g)
WITH t
UNWIND ["HBO"] AS network_name
CREATE (n:Network {name: network_name})
CREATE (t)-[:AIRED_ON]->(n)"#;

    match engine.execute_mut(q, &mut store, "default") {
        Ok(_) => eprintln!("OK: {} nodes, {} edges", store.node_count(), store.edge_count()),
        Err(e) => eprintln!("ERROR: {}", e),
    }
    
    // Expected: 1 show + 3 genres + 1 network = 5 nodes
    // Expected: 3 IN_GENRE + 1 AIRED_ON = 4 edges
    assert_eq!(store.node_count(), 5, "Expected 5 nodes");
    assert_eq!(store.edge_count(), 4, "Expected 4 edges");
}

#[test]
fn test_parse_multi_part_stages() {
    let q = r#"CREATE (t:TVShow {id: 1399})
SET t.name = "Test"
WITH t
UNWIND ["A", "B"] AS x
CREATE (g:Genre {name: x})
CREATE (t)-[:IN_GENRE]->(g)
WITH t
UNWIND ["N1"] AS y
CREATE (n:Network {name: y})
CREATE (t)-[:ON]->(n)"#;

    let query = graphmind::query::parser::parse_query(q).unwrap();
    eprintln!("multi_part_stages: {}", query.multi_part_stages.len());
    for (i, stage) in query.multi_part_stages.iter().enumerate() {
        eprintln!("  stage[{}]: with_items={}, unwinds={}, creates={}, sets={}", 
            i, stage.with_clause.items.len(), stage.unwind_clauses.len(), 
            stage.create_clauses.len(), stage.set_clauses.len());
    }
    
    assert!(query.multi_part_stages.len() >= 1, "Should have at least 1 multi-part stage");
}
