use graphmind::graph::Label;
use graphmind::{GraphStore, QueryEngine};

#[test]
fn test_merge_idempotency_detailed() {
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

    // === RUN 1 ===
    engine.execute_mut(q, &mut store, "default").unwrap();
    let run1_nodes = store.node_count();
    let run1_edges = store.edge_count();
    eprintln!("=== RUN 1: {} nodes, {} edges ===", run1_nodes, run1_edges);

    let labels = [
        "TVShow", "Genre", "Creator", "Network", "Country", "Language", "Company",
    ];
    for label in &labels {
        let count = store.get_nodes_by_label(&Label::new(*label)).len();
        eprintln!("  {}: {}", label, count);
    }

    // === RUN 2 ===
    engine.execute_mut(q, &mut store, "default").unwrap();
    let run2_nodes = store.node_count();
    let run2_edges = store.edge_count();
    eprintln!(
        "\n=== RUN 2: {} nodes, {} edges ===",
        run2_nodes, run2_edges
    );

    for label in &labels {
        let count = store.get_nodes_by_label(&Label::new(*label)).len();
        eprintln!("  {}: {}", label, count);
    }

    // Check for duplicate edges
    let tvshow = store.get_nodes_by_label(&Label::new("TVShow"));
    if !tvshow.is_empty() {
        let tv_id = tvshow[0].id;
        let outgoing = store.get_outgoing_edges(tv_id);
        eprintln!("\n=== TVShow edges ({}) ===", outgoing.len());
        for edge in &outgoing {
            let target_labels: Vec<String> = store
                .get_node(edge.target)
                .map(|n| n.labels.iter().map(|l| l.as_str().to_string()).collect())
                .unwrap_or_default();
            let target_name = store
                .get_node(edge.target)
                .and_then(|n| n.properties.get("name"))
                .map(|v| format!("{:?}", v))
                .unwrap_or_else(|| "?".to_string());
            eprintln!(
                "  -[:{}]-> {:?} [{}] {}",
                edge.edge_type.as_str(),
                edge.target,
                target_labels.join(":"),
                target_name
            );
        }
    }

    // === RUN 3 ===
    engine.execute_mut(q, &mut store, "default").unwrap();
    let run3_nodes = store.node_count();
    let run3_edges = store.edge_count();
    eprintln!(
        "\n=== RUN 3: {} nodes, {} edges ===",
        run3_nodes, run3_edges
    );

    // === ASSERTIONS ===
    assert_eq!(run1_nodes, 13, "Run 1: expected 13 nodes");
    assert_eq!(run1_edges, 12, "Run 1: expected 12 edges");

    assert_eq!(
        run2_nodes, run1_nodes,
        "Run 2 should NOT create new nodes (got {} vs {})",
        run2_nodes, run1_nodes
    );
    assert_eq!(
        run2_edges, run1_edges,
        "Run 2 should NOT create new edges (got {} vs {})",
        run2_edges, run1_edges
    );

    assert_eq!(
        run3_nodes, run1_nodes,
        "Run 3 should NOT create new nodes (got {} vs {})",
        run3_nodes, run1_nodes
    );
    assert_eq!(
        run3_edges, run1_edges,
        "Run 3 should NOT create new edges (got {} vs {})",
        run3_edges, run1_edges
    );
}
