use graphmind::graph::{Label, PropertyValue};
use graphmind::{GraphStore, QueryEngine};

#[test]
fn test_full_merge_tvshow_query() {
    let mut store = GraphStore::new();
    let engine = QueryEngine::new();

    let q = r#"MERGE (t:TVShow {id: 1399})
    SET t.name = "Game of Thrones",
        t.seasons = 8,
        t.episodes = 73,
        t.language = "en",
        t.vote_count = 21857,
        t.vote_average = 8.442,
        t.overview = "Seven noble families fight for control of the mythical land of Westeros. Friction between the houses leads to full-scale war. All while a very ancient evil awakens in the farthest north. Amidst the war, a neglected military order of misfits, the Night's Watch, is all that stands between the realms of men and icy horrors beyond.",
        t.tagline = "Winter Is Coming"

    WITH t

    UNWIND ["Sci-Fi & Fantasy", "Drama", "Action & Adventure"] AS genre_name
    MERGE (g:Genre {name: genre_name})
    MERGE (t)-[:IN_GENRE]->(g)

    WITH t
    UNWIND ["David Benioff", "D.B. Weiss"] AS creator_name
    MERGE (c:Creator {name: creator_name})
    MERGE (t)-[:CREATED_BY]->(c)

    WITH t
    UNWIND ["HBO"] AS network_name
    MERGE (n:Network {name: network_name})
    MERGE (t)-[:AIRED_ON]->(n)

    WITH t
    UNWIND ["US"] AS country_name
    MERGE (co:Country {name: country_name})
    MERGE (t)-[:ORIGINATED_IN]->(co)

    WITH t
    UNWIND ["English"] AS lang_name
    MERGE (l:Language {name: lang_name})
    MERGE (t)-[:SPOKEN_IN]->(l)

    WITH t
    UNWIND ["Revolution Sun Studios", "Television 360", "Generator Entertainment", "Bighead Littlehead"] AS comp_name
    MERGE (pc:Company {name: comp_name})
    MERGE (t)-[:PRODUCED_BY]->(pc)"#;

    let result = engine.execute_mut(q, &mut store, "default");
    assert!(result.is_ok(), "Query failed: {:?}", result.err());

    // Dump all nodes for debugging
    eprintln!("\n=== NODES ({}) ===", store.node_count());
    for node in store.all_nodes() {
        let labels: Vec<String> = node.labels.iter().map(|l| l.as_str().to_string()).collect();
        eprintln!(
            "  {:?} [{}] props={:?}",
            node.id,
            labels.join(", "),
            node.properties
        );
    }
    eprintln!("\n=== EDGES ({}) ===", store.edge_count());
    for edge in store.all_edges() {
        eprintln!(
            "  {:?} -[:{}]-> {:?}",
            edge.source,
            edge.edge_type.as_str(),
            edge.target
        );
    }

    // === Verify node counts per label ===
    let tvshow = store.get_nodes_by_label(&Label::new("TVShow"));
    assert_eq!(tvshow.len(), 1, "Expected 1 TVShow");

    let genres = store.get_nodes_by_label(&Label::new("Genre"));
    assert_eq!(genres.len(), 3, "Expected 3 Genres");

    let creators = store.get_nodes_by_label(&Label::new("Creator"));
    assert_eq!(creators.len(), 2, "Expected 2 Creators");

    let networks = store.get_nodes_by_label(&Label::new("Network"));
    assert_eq!(networks.len(), 1, "Expected 1 Network");

    let countries = store.get_nodes_by_label(&Label::new("Country"));
    assert_eq!(countries.len(), 1, "Expected 1 Country");

    let languages = store.get_nodes_by_label(&Label::new("Language"));
    assert_eq!(languages.len(), 1, "Expected 1 Language");

    let companies = store.get_nodes_by_label(&Label::new("Company"));
    assert_eq!(companies.len(), 4, "Expected 4 Companies");

    // Total: 1+3+2+1+1+1+4 = 13 nodes
    assert_eq!(store.node_count(), 13, "Expected 13 total nodes");

    // Total: 3+2+1+1+1+4 = 12 edges
    assert_eq!(store.edge_count(), 12, "Expected 12 total edges");

    // === Verify TVShow properties — NO nulls ===
    let tv = &tvshow[0];
    assert_eq!(
        tv.properties.get("name"),
        Some(&PropertyValue::String("Game of Thrones".to_string())),
        "TVShow name"
    );
    assert_eq!(
        tv.properties.get("id"),
        Some(&PropertyValue::Integer(1399)),
        "TVShow id"
    );
    assert_eq!(
        tv.properties.get("seasons"),
        Some(&PropertyValue::Integer(8)),
        "TVShow seasons"
    );
    assert_eq!(
        tv.properties.get("episodes"),
        Some(&PropertyValue::Integer(73)),
        "TVShow episodes"
    );
    assert_eq!(
        tv.properties.get("language"),
        Some(&PropertyValue::String("en".to_string())),
        "TVShow language"
    );
    assert_eq!(
        tv.properties.get("vote_count"),
        Some(&PropertyValue::Integer(21857)),
        "TVShow vote_count"
    );
    assert_eq!(
        tv.properties.get("vote_average"),
        Some(&PropertyValue::Float(8.442)),
        "TVShow vote_average"
    );
    assert!(
        tv.properties.get("overview").is_some(),
        "TVShow overview should exist"
    );
    assert_eq!(
        tv.properties.get("tagline"),
        Some(&PropertyValue::String("Winter Is Coming".to_string())),
        "TVShow tagline"
    );

    // === Check NO null property values anywhere ===
    for node in store.all_nodes() {
        for (key, value) in &node.properties {
            assert!(
                !matches!(value, PropertyValue::Null),
                "NULL property found: {:?}.{} = Null",
                node.id,
                key
            );
        }
    }

    // === Verify Genre names ===
    let genre_names: Vec<String> = genres
        .iter()
        .filter_map(|n| match n.properties.get("name") {
            Some(PropertyValue::String(s)) => Some(s.clone()),
            _ => None,
        })
        .collect();
    assert!(genre_names.contains(&"Sci-Fi & Fantasy".to_string()));
    assert!(genre_names.contains(&"Drama".to_string()));
    assert!(genre_names.contains(&"Action & Adventure".to_string()));

    // === Verify Creator names ===
    let creator_names: Vec<String> = creators
        .iter()
        .filter_map(|n| match n.properties.get("name") {
            Some(PropertyValue::String(s)) => Some(s.clone()),
            _ => None,
        })
        .collect();
    assert!(creator_names.contains(&"David Benioff".to_string()));
    assert!(creator_names.contains(&"D.B. Weiss".to_string()));

    // === Verify Company names ===
    let company_names: Vec<String> = companies
        .iter()
        .filter_map(|n| match n.properties.get("name") {
            Some(PropertyValue::String(s)) => Some(s.clone()),
            _ => None,
        })
        .collect();
    assert!(company_names.contains(&"Revolution Sun Studios".to_string()));
    assert!(company_names.contains(&"Television 360".to_string()));
    assert!(company_names.contains(&"Generator Entertainment".to_string()));
    assert!(company_names.contains(&"Bighead Littlehead".to_string()));

    // === Verify idempotency: running again should NOT create duplicates ===
    let result2 = engine.execute_mut(q, &mut store, "default");
    assert!(result2.is_ok(), "Second run failed: {:?}", result2.err());

    // MERGE should find existing nodes, not create new ones
    assert_eq!(
        store.get_nodes_by_label(&Label::new("TVShow")).len(),
        1,
        "TVShow should still be 1 after re-run"
    );
    assert_eq!(
        store.get_nodes_by_label(&Label::new("Genre")).len(),
        3,
        "Genres should still be 3 after re-run"
    );
    assert_eq!(
        store.get_nodes_by_label(&Label::new("Company")).len(),
        4,
        "Companies should still be 4 after re-run"
    );
    // Node count should remain 13
    assert_eq!(
        store.node_count(),
        13,
        "Total nodes should remain 13 after idempotent re-run"
    );
}

#[test]
fn test_explain_multi_part_merge() {
    let store = GraphStore::new();
    let engine = QueryEngine::new();

    let q = r#"EXPLAIN MERGE (t:TVShow {id: 1399})
    SET t.name = "Game of Thrones"
    WITH t
    UNWIND ["Drama", "Fantasy"] AS genre_name
    MERGE (g:Genre {name: genre_name})
    MERGE (t)-[:IN_GENRE]->(g)
    WITH t
    UNWIND ["HBO"] AS network_name
    MERGE (n:Network {name: network_name})
    MERGE (t)-[:AIRED_ON]->(n)"#;

    let result = engine.execute(q, &store).unwrap();
    let plan_text = match &result.records[0].get("plan") {
        Some(graphmind::query::executor::Value::Property(PropertyValue::String(s))) => s.clone(),
        other => panic!("Expected plan string, got {:?}", other),
    };
    eprintln!("EXPLAIN output:\n{}", plan_text);
    assert!(
        !plan_text.contains("Unknown"),
        "Plan should not contain 'Unknown' operators"
    );
}
