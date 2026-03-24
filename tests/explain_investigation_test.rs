use graphmind::graph::PropertyValue;
use graphmind::{GraphStore, QueryEngine};

fn get_plan(engine: &QueryEngine, store: &GraphStore, q: &str) -> String {
    let result = engine.execute(q, store).unwrap();
    match result.records[0].get("plan") {
        Some(graphmind::query::executor::Value::Property(PropertyValue::String(s))) => s.clone(),
        _ => "???".to_string(),
    }
}

#[test]
fn investigate_explain_shapes() {
    let mut store = GraphStore::new();
    let engine = QueryEngine::new();

    engine
        .execute_mut(
            r#"CREATE (a:Person {name: "Alice", age: 30})
               CREATE (b:Person {name: "Bob", age: 25})
               CREATE (c:Person {name: "Charlie", age: 35})
               CREATE (d:Company {name: "Acme"})"#,
            &mut store,
            "default",
        )
        .unwrap();
    engine
        .execute_mut(
            r#"MATCH (a:Person {name: "Alice"}), (b:Person {name: "Bob"}) CREATE (a)-[:KNOWS]->(b)"#,
            &mut store,
            "default",
        )
        .unwrap();
    engine
        .execute_mut(
            r#"MATCH (a:Person {name: "Alice"}), (d:Company {name: "Acme"}) CREATE (a)-[:WORKS_AT]->(d)"#,
            &mut store,
            "default",
        )
        .unwrap();

    eprintln!("=== 1. Simple scan (linear) ===");
    eprintln!(
        "{}",
        get_plan(&engine, &store, "EXPLAIN MATCH (n:Person) RETURN n.name")
    );

    eprintln!("=== 2. Multi-pattern MATCH with JOIN (BRANCHING tree!) ===");
    eprintln!(
        "{}",
        get_plan(
            &engine,
            &store,
            "EXPLAIN MATCH (a:Person)-[:KNOWS]->(b:Person), (c:Company) RETURN a.name, b.name, c.name"
        )
    );

    eprintln!("=== 3. OPTIONAL MATCH (BRANCHING — LeftOuterJoin) ===");
    eprintln!(
        "{}",
        get_plan(
            &engine,
            &store,
            "EXPLAIN MATCH (a:Person) OPTIONAL MATCH (a)-[:WORKS_AT]->(c:Company) RETURN a.name, c.name"
        )
    );

    eprintln!("=== 4. Multi-part write EXPLAIN ===");
    eprintln!(
        "{}",
        get_plan(
            &engine,
            &store,
            r#"EXPLAIN CREATE (t:TVShow {id: 1}) SET t.name = "X" WITH t UNWIND ["A","B"] AS g CREATE (x:Genre {name: g}) CREATE (t)-[:HAS]->(x)"#
        )
    );

    eprintln!("=== 5. PROFILE ===");
    eprintln!(
        "{}",
        get_plan(&engine, &store, "PROFILE MATCH (n:Person) RETURN n.name")
    );

    eprintln!("=== 6. Aggregation with ORDER BY ===");
    eprintln!(
        "{}",
        get_plan(
            &engine,
            &store,
            "EXPLAIN MATCH (n:Person) RETURN n.age, count(n) AS cnt ORDER BY cnt DESC LIMIT 10"
        )
    );
}
