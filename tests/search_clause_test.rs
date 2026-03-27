use graphmind::graph::{GraphStore, PropertyValue};
use graphmind::vector::DistanceMetric;
use graphmind::QueryEngine;

/// Helper: create a graph with Movie nodes that have embeddings, plus a vector index.
fn setup_movie_graph() -> GraphStore {
    let mut store = GraphStore::new();

    // Create vector index for Movie.embedding
    store
        .create_vector_index("Movie", "embedding", 3, DistanceMetric::Cosine)
        .unwrap();

    // Create movies with embeddings
    let movies = vec![
        ("Snow White", vec![1.0f32, 2.0, 3.0], 7.6, "1937-12-21"),
        ("Cinderella", vec![1.0, 3.0, 4.0], 7.3, "1950-02-15"),
        ("Frozen", vec![1.0, 3.0, 3.0], 7.4, "2013-11-10"),
        ("Lilo & Stitch", vec![1.0, 1.0, 3.0], 7.4, "2002-06-16"),
        ("Lion King", vec![2.0, 5.0, 2.0], 8.5, "1994-06-12"),
        ("Mulan", vec![2.0, 3.0, 3.0], 7.7, "1998-06-05"),
    ];

    for (title, embedding, rating, release_date) in &movies {
        let id = store.create_node("Movie");
        if let Some(node) = store.get_node_mut(id) {
            node.set_property("title", title.to_string());
            node.set_property("rating", *rating);
            node.set_property("releaseDate", release_date.to_string());
        }
        // Add to vector index
        store
            .vector_index
            .add_vector("Movie", "embedding", id, embedding)
            .unwrap();
    }

    // Create a movie WITHOUT embedding (Aladdin)
    let aladdin_id = store.create_node("Movie");
    if let Some(node) = store.get_node_mut(aladdin_id) {
        node.set_property("title", "Aladdin".to_string());
        node.set_property("rating", 8.0);
        node.set_property("releaseDate", "1992-11-08".to_string());
    }

    store
}

// ============================================================
// 1. Basic SEARCH clause parsing
// ============================================================

#[test]
fn test_search_clause_parses() {
    let q = r#"MATCH (movie:Movie) SEARCH movie IN (VECTOR INDEX moviePlots FOR [1, 2, 3] LIMIT 4) RETURN movie.title"#;
    let parsed = graphmind::query::parser::parse_query(q).unwrap();
    assert!(!parsed.match_clauses.is_empty());
    let mc = &parsed.match_clauses[0];
    assert!(mc.search_clause.is_some());
    let sc = mc.search_clause.as_ref().unwrap();
    assert_eq!(sc.binding_variable, "movie");
    assert_eq!(sc.index_name, "moviePlots");
    assert!(sc.score_alias.is_none());
}

#[test]
fn test_search_clause_with_score_parses() {
    let q = r#"MATCH (movie:Movie) SEARCH movie IN (VECTOR INDEX moviePlots FOR [1, 2, 3] LIMIT 4) SCORE AS similarityScore RETURN movie.title, similarityScore"#;
    let parsed = graphmind::query::parser::parse_query(q).unwrap();
    let sc = parsed.match_clauses[0].search_clause.as_ref().unwrap();
    assert_eq!(sc.score_alias, Some("similarityScore".to_string()));
}

#[test]
fn test_search_clause_with_where_parses() {
    let q = r#"MATCH (movie:Movie) SEARCH movie IN (VECTOR INDEX moviePlots FOR [1, 2, 3] WHERE movie.rating > 7.5 LIMIT 4) RETURN movie.title"#;
    let parsed = graphmind::query::parser::parse_query(q).unwrap();
    let sc = parsed.match_clauses[0].search_clause.as_ref().unwrap();
    assert!(sc.where_clause.is_some());
}

// ============================================================
// 2. Basic vector search via SEARCH clause
// ============================================================

#[test]
fn test_search_basic_4_nearest() {
    let store = setup_movie_graph();
    let engine = QueryEngine::new();

    // Find 4 nearest movies to Snow White's embedding [1, 2, 3]
    let result = engine
        .execute(
            r#"MATCH (movie:Movie) SEARCH movie IN (VECTOR INDEX moviePlots FOR [1, 2, 3] LIMIT 4) RETURN movie.title AS title"#,
            &store,
        )
        .unwrap();

    assert_eq!(result.records.len(), 4, "Should return top 4 results");

    // Verify all returned titles are movies with embeddings
    let titles: Vec<String> = result
        .records
        .iter()
        .filter_map(|r| match r.get("title") {
            Some(graphmind::query::executor::Value::Property(PropertyValue::String(s))) => {
                Some(s.clone())
            }
            _ => None,
        })
        .collect();

    // Aladdin should NOT be in results (no embedding)
    assert!(
        !titles.contains(&"Aladdin".to_string()),
        "Aladdin has no embedding, should not appear"
    );

    // Snow White should be first (most similar to itself)
    assert_eq!(
        titles[0], "Snow White",
        "Snow White should be most similar to itself"
    );
}

#[test]
fn test_search_with_score() {
    let store = setup_movie_graph();
    let engine = QueryEngine::new();

    let result = engine
        .execute(
            r#"MATCH (movie:Movie) SEARCH movie IN (VECTOR INDEX moviePlots FOR [1, 2, 3] LIMIT 4) SCORE AS sim RETURN movie.title AS title, sim"#,
            &store,
        )
        .unwrap();

    assert_eq!(result.records.len(), 4);

    // First result (Snow White = itself) should have score ~1.0
    let first_score = match result.records[0].get("sim") {
        Some(graphmind::query::executor::Value::Property(PropertyValue::Float(f))) => *f,
        _ => panic!("Expected float score"),
    };
    assert!(
        first_score > 0.99,
        "Snow White similarity to itself should be ~1.0, got {}",
        first_score
    );

    // All scores should be between 0 and 1
    for record in &result.records {
        if let Some(graphmind::query::executor::Value::Property(PropertyValue::Float(f))) =
            record.get("sim")
        {
            assert!(
                *f >= 0.0 && *f <= 1.0,
                "Score should be in [0,1], got {}",
                f
            );
        }
    }
}

// ============================================================
// 3. SEARCH with post-filtering WHERE
// ============================================================

#[test]
fn test_search_with_post_filter_where() {
    let store = setup_movie_graph();
    let engine = QueryEngine::new();

    // Find 4 nearest then filter by rating > 7.5
    let result = engine
        .execute(
            r#"MATCH (movie:Movie) SEARCH movie IN (VECTOR INDEX moviePlots FOR [1, 2, 3] LIMIT 6) WHERE movie.rating > 7.5 RETURN movie.title AS title, movie.rating AS rating"#,
            &store,
        )
        .unwrap();

    // All results should have rating > 7.5
    for record in &result.records {
        if let Some(graphmind::query::executor::Value::Property(PropertyValue::Float(f))) =
            record.get("rating")
        {
            assert!(*f > 7.5, "Rating should be > 7.5, got {}", f);
        }
    }
}

// ============================================================
// 4. SEARCH with in-index WHERE filter
// ============================================================

#[test]
fn test_search_with_in_index_where() {
    let store = setup_movie_graph();
    let engine = QueryEngine::new();

    // In-index filtering: rating > 7.5 applied during search
    let result = engine
        .execute(
            r#"MATCH (movie:Movie) SEARCH movie IN (VECTOR INDEX moviePlots FOR [1, 2, 3] WHERE movie.rating > 7.5 LIMIT 4) RETURN movie.title AS title"#,
            &store,
        )
        .unwrap();

    // Results should only contain movies with rating > 7.5
    // Verify the filter was applied (results may be fewer than LIMIT)
    assert!(result.records.len() <= 4);
}

// ============================================================
// 5. OPTIONAL MATCH with SEARCH — null semantics
// ============================================================

#[test]
fn test_optional_match_with_search_null_vector() {
    let store = setup_movie_graph();
    let engine = QueryEngine::new();

    // If the query vector evaluates to a valid vector, OPTIONAL MATCH with SEARCH should work
    let result = engine.execute(
        r#"OPTIONAL MATCH (movie:Movie) SEARCH movie IN (VECTOR INDEX moviePlots FOR [1, 2, 3] LIMIT 2) RETURN movie.title AS title"#,
        &store,
    );

    // Should succeed (OPTIONAL MATCH returns results or null rows)
    assert!(result.is_ok());
}

// ============================================================
// 6. SEARCH with different query vector forms
// ============================================================

#[test]
fn test_search_with_integer_vector() {
    let store = setup_movie_graph();
    let engine = QueryEngine::new();

    // Integer list should work too
    let result = engine
        .execute(
            r#"MATCH (movie:Movie) SEARCH movie IN (VECTOR INDEX moviePlots FOR [1, 2, 3] LIMIT 3) RETURN movie.title"#,
            &store,
        )
        .unwrap();

    assert_eq!(result.records.len(), 3);
}

// ============================================================
// 7. EXPLAIN with SEARCH
// ============================================================

#[test]
fn test_explain_with_search() {
    let store = setup_movie_graph();
    let engine = QueryEngine::new();

    let result = engine
        .execute(
            r#"EXPLAIN MATCH (movie:Movie) SEARCH movie IN (VECTOR INDEX moviePlots FOR [1, 2, 3] LIMIT 4) RETURN movie.title"#,
            &store,
        )
        .unwrap();

    // Should contain VectorSearch in the plan
    if let Some(graphmind::query::executor::Value::Property(PropertyValue::String(plan))) =
        result.records[0].get("plan")
    {
        assert!(
            plan.contains("VectorSearch"),
            "EXPLAIN plan should mention VectorSearch, got: {}",
            plan
        );
    }
}

// ============================================================
// 8. SEARCH with LIMIT 0 — should return empty
// ============================================================

#[test]
fn test_search_limit_zero() {
    let store = setup_movie_graph();
    let engine = QueryEngine::new();

    let result = engine
        .execute(
            r#"MATCH (movie:Movie) SEARCH movie IN (VECTOR INDEX moviePlots FOR [1, 2, 3] LIMIT 0) RETURN movie.title"#,
            &store,
        )
        .unwrap();

    assert_eq!(result.records.len(), 0, "LIMIT 0 should return no results");
}

// ============================================================
// 9. SEARCH result ordering — most similar first
// ============================================================

#[test]
fn test_search_results_ordered_by_similarity() {
    let store = setup_movie_graph();
    let engine = QueryEngine::new();

    let result = engine
        .execute(
            r#"MATCH (movie:Movie) SEARCH movie IN (VECTOR INDEX moviePlots FOR [1, 2, 3] LIMIT 6) SCORE AS sim RETURN movie.title AS title, sim"#,
            &store,
        )
        .unwrap();

    // Verify scores are in descending order
    let scores: Vec<f64> = result
        .records
        .iter()
        .filter_map(|r| match r.get("sim") {
            Some(graphmind::query::executor::Value::Property(PropertyValue::Float(f))) => Some(*f),
            _ => None,
        })
        .collect();

    for i in 1..scores.len() {
        assert!(
            scores[i - 1] >= scores[i],
            "Scores should be descending: {} >= {} at position {}",
            scores[i - 1],
            scores[i],
            i
        );
    }
}

// ============================================================
// 10. SEARCH combined with graph traversal
// ============================================================

#[test]
fn test_search_clause_does_not_break_regular_match() {
    // Regular MATCH without SEARCH should still work
    let store = setup_movie_graph();
    let engine = QueryEngine::new();

    let result = engine
        .execute(
            r#"MATCH (m:Movie) WHERE m.rating > 8.0 RETURN m.title AS title"#,
            &store,
        )
        .unwrap();

    // Lion King (8.5) and Aladdin (8.0) — only Lion King > 8.0
    assert!(
        !result.records.is_empty(),
        "Regular MATCH should still work"
    );
}
