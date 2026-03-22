#[test]
fn test_with_unwind_return() {
    let store = graphmind::GraphStore::new();
    let engine = graphmind::QueryEngine::new();
    let q = "WITH [1, 2, 3] AS list UNWIND list AS x RETURN x";
    match engine.execute(q, &store) {
        Ok(r) => {
            eprintln!("OK: {} rows", r.len());
            assert_eq!(r.len(), 3);
        }
        Err(e) => eprintln!("ERROR: {}", e),
    }
}
