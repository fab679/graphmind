#[test]
fn test_unique_constraint_enforcement() {
    let mut store = graphmind::GraphStore::new();
    let engine = graphmind::QueryEngine::new();

    engine
        .execute_mut(
            "CREATE CONSTRAINT person_id IF NOT EXISTS FOR (p:Person) REQUIRE p.id IS UNIQUE",
            &mut store,
            "default",
        )
        .unwrap();

    eprintln!(
        "Has constraint: {}",
        store
            .property_index
            .has_unique_constraint(&graphmind::Label::new("Person"), "id")
    );

    engine
        .execute_mut(
            "CREATE (:Person {id: 'p1', name: 'Alice'})",
            &mut store,
            "default",
        )
        .unwrap();
    eprintln!("After first: {} nodes", store.node_count());

    let result = engine.execute_mut(
        "CREATE (:Person {id: 'p1', name: 'Duplicate'})",
        &mut store,
        "default",
    );
    eprintln!("Duplicate result: {:?}", result.is_err());
    if let Err(e) = &result {
        eprintln!("Error: {}", e);
    }
    eprintln!("After duplicate: {} nodes", store.node_count());
}
