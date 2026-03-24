use graphmind::{GraphStore, QueryEngine};

#[test]
fn test_multi_with_unwind_create_stages() {
    let mut store = GraphStore::new();
    let engine = QueryEngine::new();

    let q = r#"CREATE (t:TVShow {id: 1399})
SET t.name = "Game of Thrones"
WITH t
UNWIND ["Drama", "Sci-Fi", "Adventure"] AS genre_name
CREATE (g:Genre {name: genre_name})
CREATE (t)-[:IN_GENRE]->(g)
WITH t
UNWIND ["HBO"] AS network_name
CREATE (n:Network {name: network_name})
CREATE (t)-[:AIRED_ON]->(n)"#;

    engine.execute_mut(q, &mut store, "default").unwrap();

    // Expected: 1 show + 3 genres + 1 network = 5 nodes
    // Expected: 3 IN_GENRE + 1 AIRED_ON = 4 edges
    assert_eq!(store.node_count(), 5, "Expected 5 nodes");
    assert_eq!(store.edge_count(), 4, "Expected 4 edges");
}

#[test]
fn test_full_tvshow_create_query() {
    // The full Neo4j-style query from the user's example
    let mut store = GraphStore::new();
    let engine = QueryEngine::new();

    let q = r#"CREATE (t:TVShow {id: 1399})
    SET t.name = "Game of Thrones",
        t.seasons = 8,
        t.episodes = 73,
        t.language = "en"

    WITH t

    UNWIND ["Sci-Fi & Fantasy", "Drama", "Action & Adventure"] AS genre_name
    CREATE (g:Genre {name: genre_name})
    CREATE (t)-[:IN_GENRE]->(g)

    WITH t
    UNWIND ["David Benioff", "D.B. Weiss"] AS creator_name
    CREATE (c:Creator {name: creator_name})
    CREATE (t)-[:CREATED_BY]->(c)

    WITH t
    UNWIND ["HBO"] AS network_name
    CREATE (n:Network {name: network_name})
    CREATE (t)-[:AIRED_ON]->(n)

    WITH t
    UNWIND ["US"] AS country_name
    CREATE (co:Country {name: country_name})
    CREATE (t)-[:ORIGINATED_IN]->(co)

    WITH t
    UNWIND ["English"] AS lang_name
    CREATE (l:Language {name: lang_name})
    CREATE (t)-[:SPOKEN_IN]->(l)

    WITH t
    UNWIND ["Revolution Sun Studios", "Television 360", "Generator Entertainment", "Bighead Littlehead"] AS comp_name
    CREATE (pc:Company {name: comp_name})
    CREATE (t)-[:PRODUCED_BY]->(pc)"#;

    engine.execute_mut(q, &mut store, "default").unwrap();

    // 1 TVShow + 3 genres + 2 creators + 1 network + 1 country + 1 language + 4 companies = 13 nodes
    assert_eq!(store.node_count(), 13, "Expected 13 nodes");

    // 3 IN_GENRE + 2 CREATED_BY + 1 AIRED_ON + 1 ORIGINATED_IN + 1 SPOKEN_IN + 4 PRODUCED_BY = 12 edges
    assert_eq!(store.edge_count(), 12, "Expected 12 edges");

    // Verify TVShow properties
    let tvshow_nodes: Vec<_> = store.get_nodes_by_label(&graphmind::graph::Label::new("TVShow"));
    assert_eq!(tvshow_nodes.len(), 1);
    assert_eq!(
        tvshow_nodes[0].properties.get("name"),
        Some(&graphmind::graph::PropertyValue::String(
            "Game of Thrones".to_string()
        ))
    );
    assert_eq!(
        tvshow_nodes[0].properties.get("seasons"),
        Some(&graphmind::graph::PropertyValue::Integer(8))
    );

    // Verify genres
    let genre_nodes: Vec<_> = store.get_nodes_by_label(&graphmind::graph::Label::new("Genre"));
    assert_eq!(genre_nodes.len(), 3);

    // Verify companies
    let company_nodes: Vec<_> = store.get_nodes_by_label(&graphmind::graph::Label::new("Company"));
    assert_eq!(company_nodes.len(), 4);
}

#[test]
fn test_multi_part_merge_query() {
    // Test MERGE variant — should find-or-create nodes
    let mut store = GraphStore::new();
    let engine = QueryEngine::new();

    let q = r#"CREATE (t:TVShow {id: 1399})
SET t.name = "Game of Thrones"
WITH t
UNWIND ["Drama", "Fantasy"] AS genre_name
MERGE (g:Genre {name: genre_name})
CREATE (t)-[:IN_GENRE]->(g)
WITH t
UNWIND ["HBO"] AS network_name
MERGE (n:Network {name: network_name})
CREATE (t)-[:AIRED_ON]->(n)"#;

    engine.execute_mut(q, &mut store, "default").unwrap();

    // 1 TVShow + 2 genres + 1 network = 4 nodes
    assert_eq!(store.node_count(), 4, "Expected 4 nodes");
    assert_eq!(store.edge_count(), 3, "Expected 3 edges");

    // Run again — MERGE should find existing genres/networks, not create duplicates
    let q2 = r#"CREATE (t2:TVShow {id: 1400})
SET t2.name = "Breaking Bad"
WITH t2
UNWIND ["Drama", "Thriller"] AS genre_name
MERGE (g:Genre {name: genre_name})
CREATE (t2)-[:IN_GENRE]->(g)
WITH t2
UNWIND ["HBO"] AS network_name
MERGE (n:Network {name: network_name})
CREATE (t2)-[:AIRED_ON]->(n)"#;

    engine.execute_mut(q2, &mut store, "default").unwrap();

    // 2 TVShows + 3 genres (Drama reused, Fantasy, Thriller) + 1 network (HBO reused) = 6 nodes
    let genre_nodes: Vec<_> = store.get_nodes_by_label(&graphmind::graph::Label::new("Genre"));
    assert_eq!(
        genre_nodes.len(),
        3,
        "Drama should be reused, Thriller is new"
    );

    let network_nodes: Vec<_> = store.get_nodes_by_label(&graphmind::graph::Label::new("Network"));
    assert_eq!(network_nodes.len(), 1, "HBO should be reused");
}

#[test]
fn test_parse_multi_part_stages() {
    let q = r#"CREATE (t:TVShow {id: 1399})
SET t.name = "Test"
WITH t
UNWIND ["A", "B"] AS x
CREATE (g:Genre {name: x})
CREATE (t)-[:IN_GENRE]->(g)
WITH t
UNWIND ["N1"] AS y
CREATE (n:Network {name: y})
CREATE (t)-[:ON]->(n)"#;

    let query = graphmind::query::parser::parse_query(q).unwrap();

    assert_eq!(
        query.multi_part_stages.len(),
        2,
        "Should have 2 multi-part stages"
    );

    // Stage 0: CREATE + SET + WITH t
    assert_eq!(query.multi_part_stages[0].create_clauses.len(), 1);
    assert_eq!(query.multi_part_stages[0].set_clauses.len(), 1);
    assert_eq!(query.multi_part_stages[0].unwind_clauses.len(), 0);

    // Stage 1: UNWIND + 2 CREATEs + WITH t
    assert_eq!(query.multi_part_stages[1].create_clauses.len(), 2);
    assert_eq!(query.multi_part_stages[1].unwind_clauses.len(), 1);
}

#[test]
fn test_simple_create_with_create() {
    // Simple multi-CREATE without UNWIND
    let mut store = GraphStore::new();
    let engine = QueryEngine::new();

    let q = r#"CREATE (a:Person {name: "Alice"})
WITH a
CREATE (b:Person {name: "Bob"})
CREATE (a)-[:KNOWS]->(b)"#;

    engine.execute_mut(q, &mut store, "default").unwrap();

    assert_eq!(store.node_count(), 2, "Expected 2 nodes");
    assert_eq!(store.edge_count(), 1, "Expected 1 edge");
}

#[test]
fn test_expression_properties_in_create_with_multipart() {
    // Verify that variable references in CREATE properties are resolved at runtime
    // Uses multi-part query pattern (WITH) to trigger the new pipeline
    let mut store = GraphStore::new();
    let engine = QueryEngine::new();

    let q = r#"CREATE (root:Root {id: 1})
WITH root
UNWIND ["Alice", "Bob", "Charlie"] AS name
CREATE (p:Person {name: name})"#;

    engine.execute_mut(q, &mut store, "default").unwrap();

    let persons: Vec<_> = store.get_nodes_by_label(&graphmind::graph::Label::new("Person"));
    assert_eq!(persons.len(), 3, "Expected 3 Person nodes");

    let names: Vec<String> = persons
        .iter()
        .filter_map(|n| n.properties.get("name"))
        .filter_map(|v| match v {
            graphmind::graph::PropertyValue::String(s) => Some(s.clone()),
            _ => None,
        })
        .collect();
    assert!(names.contains(&"Alice".to_string()));
    assert!(names.contains(&"Bob".to_string()));
    assert!(names.contains(&"Charlie".to_string()));
}

#[test]
fn test_read_only_multi_with_still_works() {
    // Ensure read-only multi-WITH queries are not broken
    let mut store = GraphStore::new();
    let engine = QueryEngine::new();

    // Create test data
    engine
        .execute_mut(
            r#"CREATE (a:Person {name: "Alice", age: 30})
CREATE (b:Person {name: "Bob", age: 25})
CREATE (c:Person {name: "Charlie", age: 35})"#,
            &mut store,
            "default",
        )
        .unwrap();

    let result = engine.execute(
        "MATCH (p:Person) WITH p.name AS name, p.age AS age WHERE age > 26 RETURN name ORDER BY name",
        &store,
    ).unwrap();

    assert_eq!(result.records.len(), 2);
}
