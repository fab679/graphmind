#[test]
fn test_comments_with_constraints() {
    let mut store = graphmind::GraphStore::new();
    let engine = graphmind::QueryEngine::new();

    let q = "// ─── Create constraints & indexes ───────────────────────────────────────────
CREATE CONSTRAINT person_id IF NOT EXISTS FOR (p:Person) REQUIRE p.id IS UNIQUE;
CREATE CONSTRAINT movie_id IF NOT EXISTS FOR (m:Movie) REQUIRE m.id IS UNIQUE;
CREATE INDEX person_name IF NOT EXISTS FOR (p:Person) ON (p.name)";

    match engine.execute_mut(q, &mut store, "default") {
        Ok(_) => eprintln!("OK"),
        Err(e) => eprintln!("ERROR: {}", e),
    }
}
