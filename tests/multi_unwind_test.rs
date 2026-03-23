#[test]
fn test_unicode_comment_no_panic() {
    let mut store = graphmind::GraphStore::new();
    let engine = graphmind::QueryEngine::new();
    // This should NOT panic even with unicode box-drawing characters
    let q = "// ─── Create constraints ───\nCREATE (n:Test {name: 'test'})";
    let result = engine.execute_mut(q, &mut store, "default");
    // Should either succeed or return a parse error, NOT panic
    match result {
        Ok(_) => eprintln!("OK"),
        Err(e) => eprintln!("Error (not panic): {}", e),
    }
}
