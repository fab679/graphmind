use graphmind::{GraphStore, QueryEngine};

fn build_graph() -> (GraphStore, QueryEngine) {
    let mut store = GraphStore::new();
    let engine = QueryEngine::new();
    for i in 1..=6 {
        engine.execute_mut(&format!("CREATE (n:Node {{id: {}, name: 'N{}'}})", i, i), &mut store, "default").unwrap();
    }
    engine.execute_mut("MATCH (a:Node {id:1}),(b:Node {id:2}) CREATE (a)-[:LINK]->(b)", &mut store, "default").unwrap();
    engine.execute_mut("MATCH (a:Node {id:2}),(b:Node {id:3}) CREATE (a)-[:LINK]->(b)", &mut store, "default").unwrap();
    engine.execute_mut("MATCH (a:Node {id:3}),(b:Node {id:4}) CREATE (a)-[:LINK]->(b)", &mut store, "default").unwrap();
    engine.execute_mut("MATCH (a:Node {id:4}),(b:Node {id:5}) CREATE (a)-[:LINK]->(b)", &mut store, "default").unwrap();
    engine.execute_mut("MATCH (a:Node {id:5}),(b:Node {id:1}) CREATE (a)-[:LINK]->(b)", &mut store, "default").unwrap();
    engine.execute_mut("MATCH (a:Node {id:1}),(b:Node {id:4}) CREATE (a)-[:LINK]->(b)", &mut store, "default").unwrap();
    (store, engine)
}

#[test]
fn test_pagerank() {
    let (store, engine) = build_graph();
    let r = engine.execute("CALL algo.pageRank() YIELD node, score RETURN node, score", &store).unwrap();
    eprintln!("PageRank: {} rows", r.len());
    assert!(r.len() >= 1, "PageRank should return results");
}

#[test]
fn test_wcc() {
    let (store, engine) = build_graph();
    let r = engine.execute("CALL algo.wcc() YIELD node, componentId RETURN node, componentId", &store).unwrap();
    eprintln!("WCC: {} rows", r.len());
    assert!(r.len() >= 1);
}

#[test]
fn test_scc() {
    let (store, engine) = build_graph();
    let r = engine.execute("CALL algo.scc() YIELD node, componentId RETURN node, componentId", &store).unwrap();
    eprintln!("SCC: {} rows", r.len());
    assert!(r.len() >= 1);
}

#[test]
fn test_triangle_count() {
    let (store, engine) = build_graph();
    let r = engine.execute("CALL algo.triangleCount() YIELD triangles RETURN triangles", &store).unwrap();
    eprintln!("TriangleCount: {} rows", r.len());
    assert!(r.len() >= 1);
}

#[test]
fn test_weighted_path() {
    let (store, engine) = build_graph();
    let r = engine.execute("CALL algo.weightedPath(0, 3, 'weight') YIELD node, cost RETURN node, cost", &store);
    match &r {
        Ok(batch) => eprintln!("WeightedPath: {} rows", batch.len()),
        Err(e) => eprintln!("WeightedPath: {}", e),
    }
}

#[test]
fn test_mst() {
    let (store, engine) = build_graph();
    let r = engine.execute("CALL algo.mst() YIELD source, target, weight RETURN source, target, weight", &store);
    match &r {
        Ok(batch) => eprintln!("MST: {} rows", batch.len()),
        Err(e) => eprintln!("MST: {}", e),
    }
}
