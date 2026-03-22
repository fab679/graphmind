#[test]
fn test_basic_match() {
    let mut store = graphmind::GraphStore::new();
    let engine = graphmind::QueryEngine::new();
    engine
        .execute_mut("CREATE (a:A {name: 'test'})", &mut store, "default")
        .unwrap();
    let result = engine.execute("MATCH (a:A) RETURN a.name", &store);
    match &result {
        Ok(r) => eprintln!("OK: {} rows", r.len()),
        Err(e) => eprintln!("ERROR: {}", e),
    }
    assert!(result.is_ok());
    assert_eq!(result.unwrap().len(), 1);
}
