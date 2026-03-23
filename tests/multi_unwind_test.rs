#[test]
fn test_schema_from_indexes_only() {
    let mut store = graphmind::GraphStore::new();
    let engine = graphmind::QueryEngine::new();

    // Create constraints/indexes with NO data
    engine
        .execute_mut(
            "CREATE CONSTRAINT person_id IF NOT EXISTS FOR (p:Person) REQUIRE p.id IS UNIQUE",
            &mut store,
            "default",
        )
        .unwrap();
    engine
        .execute_mut(
            "CREATE INDEX person_name IF NOT EXISTS FOR (p:Person) ON (p.name)",
            &mut store,
            "default",
        )
        .unwrap();
    engine
        .execute_mut(
            "CREATE INDEX movie_year IF NOT EXISTS FOR (m:Movie) ON (m.year)",
            &mut store,
            "default",
        )
        .unwrap();

    // Zero nodes
    assert_eq!(store.node_count(), 0);

    // But indexes should define schema labels
    let indexed = store.property_index.indexed_labels();
    let label_names: Vec<String> = indexed
        .iter()
        .map(|(l, _)| l.as_str().to_string())
        .collect();
    eprintln!("Indexed labels: {:?}", label_names);
    assert!(
        label_names.contains(&"Person".to_string()),
        "Person should be in indexed labels"
    );
    assert!(
        label_names.contains(&"Movie".to_string()),
        "Movie should be in indexed labels"
    );

    // SHOW INDEXES should show all
    let r = engine.execute("SHOW INDEXES", &store).unwrap();
    assert!(r.len() >= 2, "Should show at least 2 indexes");

    // SHOW CONSTRAINTS should show the unique constraint
    let r = engine.execute("SHOW CONSTRAINTS", &store).unwrap();
    assert!(r.len() >= 1, "Should show at least 1 constraint");
}
