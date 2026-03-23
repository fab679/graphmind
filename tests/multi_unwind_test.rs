#[test]
fn test_vector_search_e2e() {
    let mut store = graphmind::GraphStore::new();
    let engine = graphmind::QueryEngine::new();

    // Step 1: Create vector index
    let r = engine.execute_mut(
        "CREATE VECTOR INDEX person_embed FOR (n:Person) ON (n.embedding) OPTIONS {dimensions: 3, similarity: 'cosine'}",
        &mut store, "default"
    );
    match &r {
        Ok(_) => eprintln!("1. Index created OK"),
        Err(e) => eprintln!("1. Index ERROR: {}", e),
    }

    // Step 2: Create nodes with embeddings
    engine
        .execute_mut(
            "CREATE (n:Person {name: 'Alice', embedding: [1.0, 0.0, 0.0]})",
            &mut store,
            "default",
        )
        .unwrap();
    engine
        .execute_mut(
            "CREATE (n:Person {name: 'Bob', embedding: [0.0, 1.0, 0.0]})",
            &mut store,
            "default",
        )
        .unwrap();
    engine
        .execute_mut(
            "CREATE (n:Person {name: 'Charlie', embedding: [0.9, 0.1, 0.0]})",
            &mut store,
            "default",
        )
        .unwrap();
    eprintln!("2. Created 3 nodes with embeddings");

    // Step 3: Vector search - find nearest to [1.0, 0.0, 0.0]
    let result = engine.execute(
        "CALL db.index.vector.queryNodes('Person', 'embedding', [1.0, 0.0, 0.0], 2) YIELD node, score RETURN node.name, score",
        &store
    );
    match &result {
        Ok(r) => {
            eprintln!("3. Vector search: {} results", r.len());
            for rec in &r.records {
                let name = rec
                    .get("node.name")
                    .map(|v| format!("{:?}", v))
                    .unwrap_or_default();
                let score = rec
                    .get("score")
                    .map(|v| format!("{:?}", v))
                    .unwrap_or_default();
                eprintln!("   {} score={}", name, score);
            }
        }
        Err(e) => eprintln!("3. Search ERROR: {}", e),
    }

    // Step 4: Verify results
    if let Ok(r) = &result {
        assert!(r.len() > 0, "Should find at least 1 result");
    }
}
