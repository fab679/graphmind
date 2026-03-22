#[test]
fn test_i64_min() {
    let store = graphmind::GraphStore::new();
    let engine = graphmind::QueryEngine::new();
    match engine.execute("RETURN -9223372036854775808 AS literal", &store) {
        Ok(r) => eprintln!(
            "OK: {} rows, val={:?}",
            r.len(),
            r.records.get(0).and_then(|r| r.get("literal"))
        ),
        Err(e) => eprintln!("ERROR: {}", e),
    }
}
