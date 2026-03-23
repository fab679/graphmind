#[test]
fn test_bug4_exact_python_bytes() {
    let mut store = graphmind::GraphStore::new();
    let engine = graphmind::QueryEngine::new();

    // This is what Python sends when user writes: "CREATE (n:Test {val: 'it\\'s a test'})"
    // Python \\ becomes \ so the string is: CREATE (n:Test {val: 'it\'s a test'})
    let query = "CREATE (n:Test {val: 'it\\'s a test'})";
    eprintln!("Query bytes: {:?}", query);

    let r = engine.execute_mut(query, &mut store, "default");
    match &r {
        Ok(_) => eprintln!("OK: {} nodes", store.node_count()),
        Err(e) => eprintln!("ERROR: {}", e),
    }
    assert!(r.is_ok(), "Should parse single-quote escape");
}
