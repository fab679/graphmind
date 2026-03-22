#[test]
fn test_multi_with_stages() {
    let mut store = graphmind::GraphStore::new();
    let engine = graphmind::QueryEngine::new();
    engine
        .execute_mut("CREATE (a:A {name: 'Alice'})", &mut store, "default")
        .unwrap();
    let q = "MATCH (a:A) WITH a WITH a.name AS name RETURN name";
    let r = engine.execute(q, &store).unwrap();
    assert_eq!(r.len(), 1);
    eprintln!("Multi-WITH fixed: {} rows", r.len());
}
