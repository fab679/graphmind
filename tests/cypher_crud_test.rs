//! Cypher CRUD integration tests
//!
//! Tests CREATE, MATCH, SET, DELETE, MERGE, and complex retrieval patterns
//! using the QueryEngine API.
//!
//! Note: SET mutations are verified via direct store access because the
//! read-only QueryExecutor may return stale property values due to
//! late materialization caching (known limitation).

use graphmind::query::executor::MutQueryExecutor;
use graphmind::query::parser::parse_query;
use graphmind::{GraphStore, Label, PropertyValue, QueryEngine};

/// Helper: create engine + store, run a mutating query
fn exec_mut(engine: &QueryEngine, store: &mut GraphStore, q: &str) {
    engine
        .execute_mut(q, store, "default")
        .unwrap_or_else(|e| panic!("execute_mut failed for: {q}\n  error: {e}"));
}

/// Helper: run a read query, return records
fn exec(
    engine: &QueryEngine,
    store: &GraphStore,
    q: &str,
) -> graphmind::query::executor::RecordBatch {
    engine
        .execute(q, store)
        .unwrap_or_else(|e| panic!("execute failed for: {q}\n  error: {e}"))
}

/// Helper: extract a string column from the first record
fn first_str(batch: &graphmind::query::executor::RecordBatch, col: &str) -> String {
    batch.records[0]
        .get(col)
        .unwrap()
        .as_property()
        .unwrap()
        .as_string()
        .unwrap()
        .to_string()
}

/// Helper: extract an integer column from the first record
fn first_int(batch: &graphmind::query::executor::RecordBatch, col: &str) -> i64 {
    batch.records[0]
        .get(col)
        .unwrap()
        .as_property()
        .unwrap()
        .as_integer()
        .unwrap()
}

// ---------------------------------------------------------------------------
// CREATE tests
// ---------------------------------------------------------------------------

#[test]
fn test_create_single_node() {
    let mut store = GraphStore::new();
    let engine = QueryEngine::new();

    exec_mut(
        &engine,
        &mut store,
        "CREATE (n:Movie {title: 'Inception', year: 2010})",
    );

    let result = exec(&engine, &store, "MATCH (m:Movie) RETURN m.title, m.year");
    assert_eq!(result.len(), 1);
    assert_eq!(first_str(&result, "m.title"), "Inception");
    assert_eq!(first_int(&result, "m.year"), 2010);
}

#[test]
fn test_create_multiple_nodes() {
    let mut store = GraphStore::new();
    let engine = QueryEngine::new();

    exec_mut(
        &engine,
        &mut store,
        "CREATE (a:City {name: 'Paris', population: 2161000})",
    );
    exec_mut(
        &engine,
        &mut store,
        "CREATE (b:City {name: 'Tokyo', population: 13960000})",
    );
    exec_mut(
        &engine,
        &mut store,
        "CREATE (c:City {name: 'London', population: 8982000})",
    );

    let result = exec(&engine, &store, "MATCH (c:City) RETURN count(c) AS total");
    assert_eq!(first_int(&result, "total"), 3);
}

#[test]
fn test_create_node_with_relationship() {
    let mut store = GraphStore::new();
    let engine = QueryEngine::new();

    exec_mut(
        &engine,
        &mut store,
        "CREATE (a:Person {name: 'Alice', age: 30})",
    );
    exec_mut(
        &engine,
        &mut store,
        "CREATE (b:Person {name: 'Bob', age: 25})",
    );
    exec_mut(
        &engine,
        &mut store,
        "MATCH (a:Person {name: 'Alice'}), (b:Person {name: 'Bob'}) CREATE (a)-[:FRIENDS_WITH {since: 2020}]->(b)",
    );

    let result = exec(
        &engine,
        &store,
        "MATCH (a:Person)-[r:FRIENDS_WITH]->(b:Person) RETURN a.name, b.name, r.since",
    );
    assert_eq!(result.len(), 1);
    assert_eq!(first_str(&result, "a.name"), "Alice");
    assert_eq!(first_str(&result, "b.name"), "Bob");
    assert_eq!(first_int(&result, "r.since"), 2020);
}

#[test]
fn test_create_complex_graph() {
    let mut store = GraphStore::new();
    let engine = QueryEngine::new();

    // Build a small company graph
    let people = [
        ("Alice", "Engineering", 120000),
        ("Bob", "Engineering", 110000),
        ("Charlie", "Marketing", 95000),
        ("Diana", "Marketing", 105000),
        ("Eve", "Engineering", 130000),
    ];

    for (name, dept, salary) in people {
        exec_mut(
            &engine,
            &mut store,
            &format!(
                "CREATE (p:Employee {{name: '{name}', department: '{dept}', salary: {salary}}})"
            ),
        );
    }

    // Create department nodes
    exec_mut(
        &engine,
        &mut store,
        "CREATE (d:Department {name: 'Engineering', budget: 500000})",
    );
    exec_mut(
        &engine,
        &mut store,
        "CREATE (d:Department {name: 'Marketing', budget: 300000})",
    );

    // Link employees to departments
    for (name, dept, _) in people {
        exec_mut(
            &engine,
            &mut store,
            &format!(
                "MATCH (e:Employee {{name: '{name}'}}), (d:Department {{name: '{dept}'}}) CREATE (e)-[:WORKS_IN]->(d)"
            ),
        );
    }

    // Verify employee count
    let result = exec(
        &engine,
        &store,
        "MATCH (e:Employee) RETURN count(e) AS total",
    );
    assert_eq!(first_int(&result, "total"), 5);

    // Verify department links
    let result = exec(
        &engine,
        &store,
        "MATCH (e:Employee)-[:WORKS_IN]->(d:Department {name: 'Engineering'}) RETURN count(e) AS eng_count",
    );
    assert_eq!(first_int(&result, "eng_count"), 3);
}

// ---------------------------------------------------------------------------
// MATCH / retrieval tests
// ---------------------------------------------------------------------------

fn build_movie_graph() -> GraphStore {
    let mut store = GraphStore::new();
    let engine = QueryEngine::new();

    // Movies
    let movies = [
        ("The Matrix", 1999, 87),
        ("Inception", 2010, 88),
        ("Interstellar", 2014, 86),
        ("The Dark Knight", 2008, 90),
        ("Memento", 2000, 84),
    ];
    for (title, year, rating) in movies {
        exec_mut(
            &engine,
            &mut store,
            &format!("CREATE (m:Movie {{title: '{title}', year: {year}, rating: {rating}}})"),
        );
    }

    // Directors
    exec_mut(
        &engine,
        &mut store,
        "CREATE (d:Director {name: 'Christopher Nolan'})",
    );
    exec_mut(
        &engine,
        &mut store,
        "CREATE (d:Director {name: 'Lana Wachowski'})",
    );

    // Actors
    for name in [
        "Keanu Reeves",
        "Leonardo DiCaprio",
        "Matthew McConaughey",
        "Christian Bale",
        "Guy Pearce",
    ] {
        exec_mut(
            &engine,
            &mut store,
            &format!("CREATE (a:Actor {{name: '{name}'}})"),
        );
    }

    // DIRECTED relationships
    for title in ["Inception", "Interstellar", "The Dark Knight", "Memento"] {
        exec_mut(
            &engine,
            &mut store,
            &format!(
                "MATCH (d:Director {{name: 'Christopher Nolan'}}), (m:Movie {{title: '{title}'}}) CREATE (d)-[:DIRECTED]->(m)"
            ),
        );
    }
    exec_mut(
        &engine,
        &mut store,
        "MATCH (d:Director {name: 'Lana Wachowski'}), (m:Movie {title: 'The Matrix'}) CREATE (d)-[:DIRECTED]->(m)",
    );

    // ACTED_IN relationships
    let roles = [
        ("Keanu Reeves", "The Matrix", "Neo"),
        ("Leonardo DiCaprio", "Inception", "Cobb"),
        ("Matthew McConaughey", "Interstellar", "Cooper"),
        ("Christian Bale", "The Dark Knight", "Batman"),
        ("Guy Pearce", "Memento", "Leonard"),
    ];
    for (actor, movie, role) in roles {
        exec_mut(
            &engine,
            &mut store,
            &format!(
                "MATCH (a:Actor {{name: '{actor}'}}), (m:Movie {{title: '{movie}'}}) CREATE (a)-[:ACTED_IN {{role: '{role}'}}]->(m)"
            ),
        );
    }

    store
}

#[test]
fn test_match_all_nodes_by_label() {
    let store = build_movie_graph();
    let engine = QueryEngine::new();

    let result = exec(
        &engine,
        &store,
        "MATCH (m:Movie) RETURN m.title ORDER BY m.title",
    );
    assert_eq!(result.len(), 5);
    assert_eq!(first_str(&result, "m.title"), "Inception");
}

#[test]
fn test_match_with_property_filter() {
    let store = build_movie_graph();
    let engine = QueryEngine::new();

    // Movies after 2005 with rating > 85
    let result = exec(
        &engine,
        &store,
        "MATCH (m:Movie) WHERE m.year > 2005 AND m.rating > 85 RETURN m.title, m.year ORDER BY m.year",
    );
    // The Dark Knight (2008, 90), Inception (2010, 88), Interstellar (2014, 86)
    assert_eq!(result.len(), 3);
}

#[test]
fn test_match_relationship_traversal() {
    let store = build_movie_graph();
    let engine = QueryEngine::new();

    // Find all movies directed by Christopher Nolan
    let result = exec(
        &engine,
        &store,
        "MATCH (d:Director {name: 'Christopher Nolan'})-[:DIRECTED]->(m:Movie) RETURN m.title ORDER BY m.title",
    );
    assert_eq!(result.len(), 4);
    assert_eq!(first_str(&result, "m.title"), "Inception");
}

#[test]
fn test_match_with_relationship_properties() {
    let store = build_movie_graph();
    let engine = QueryEngine::new();

    // Find actor roles
    let result = exec(
        &engine,
        &store,
        "MATCH (a:Actor)-[r:ACTED_IN]->(m:Movie) RETURN a.name, m.title, r.role ORDER BY a.name",
    );
    assert_eq!(result.len(), 5);
    assert_eq!(first_str(&result, "a.name"), "Christian Bale");
    assert_eq!(first_str(&result, "r.role"), "Batman");
}

#[test]
fn test_match_multi_hop() {
    let store = build_movie_graph();
    let engine = QueryEngine::new();

    // Find actors in Nolan movies (Director->Movie<-Actor)
    let result = exec(
        &engine,
        &store,
        "MATCH (d:Director {name: 'Christopher Nolan'})-[:DIRECTED]->(m:Movie)<-[:ACTED_IN]-(a:Actor) RETURN a.name, m.title ORDER BY a.name",
    );
    assert_eq!(result.len(), 4);
}

// ---------------------------------------------------------------------------
// Aggregation tests
// ---------------------------------------------------------------------------

#[test]
fn test_count_aggregation() {
    let store = build_movie_graph();
    let engine = QueryEngine::new();

    let result = exec(
        &engine,
        &store,
        "MATCH (m:Movie) RETURN count(m) AS movie_count",
    );
    assert_eq!(first_int(&result, "movie_count"), 5);
}

#[test]
fn test_avg_min_max_aggregation() {
    let store = build_movie_graph();
    let engine = QueryEngine::new();

    let result = exec(
        &engine,
        &store,
        "MATCH (m:Movie) RETURN min(m.year) AS earliest, max(m.year) AS latest",
    );
    assert_eq!(result.len(), 1);
    assert_eq!(first_int(&result, "earliest"), 1999);
    assert_eq!(first_int(&result, "latest"), 2014);
}

#[test]
fn test_group_by_with_count() {
    let store = build_movie_graph();
    let engine = QueryEngine::new();

    // Count movies per director
    let result = exec(
        &engine,
        &store,
        "MATCH (d:Director)-[:DIRECTED]->(m:Movie) RETURN d.name, count(m) AS num_movies ORDER BY num_movies DESC",
    );
    assert_eq!(result.len(), 2);
    assert_eq!(first_str(&result, "d.name"), "Christopher Nolan");
    assert_eq!(first_int(&result, "num_movies"), 4);
}

// ---------------------------------------------------------------------------
// SET / UPDATE tests (verified via store access due to late materialization)
// ---------------------------------------------------------------------------

#[test]
fn test_set_property() {
    let mut store = GraphStore::new();

    let q1 = parse_query("CREATE (p:Product {name: 'Widget', price: 999})").unwrap();
    MutQueryExecutor::new(&mut store, "default".to_string())
        .execute(&q1)
        .unwrap();

    let q2 = parse_query("MATCH (p:Product {name: 'Widget'}) SET p.price = 1299").unwrap();
    MutQueryExecutor::new(&mut store, "default".to_string())
        .execute(&q2)
        .unwrap();

    let nodes = store.get_nodes_by_label(&Label::new("Product"));
    assert_eq!(
        nodes[0].properties.get("price").unwrap().as_integer(),
        Some(1299)
    );
}

#[test]
fn test_set_add_new_property() {
    let mut store = GraphStore::new();

    let q1 = parse_query("CREATE (p:Person {name: 'Alice'})").unwrap();
    MutQueryExecutor::new(&mut store, "default".to_string())
        .execute(&q1)
        .unwrap();

    let q2 =
        parse_query("MATCH (p:Person {name: 'Alice'}) SET p.email = 'alice@example.com'").unwrap();
    MutQueryExecutor::new(&mut store, "default".to_string())
        .execute(&q2)
        .unwrap();

    let nodes = store.get_nodes_by_label(&Label::new("Person"));
    assert_eq!(
        nodes[0].properties.get("email").unwrap().as_string(),
        Some("alice@example.com")
    );
}

#[test]
fn test_set_multiple_properties() {
    let mut store = GraphStore::new();

    let q1 = parse_query("CREATE (s:Server {hostname: 'web-01', status: 'active'})").unwrap();
    MutQueryExecutor::new(&mut store, "default".to_string())
        .execute(&q1)
        .unwrap();

    let q2 = parse_query("MATCH (s:Server {hostname: 'web-01'}) SET s.status = 'maintenance', s.downtime_reason = 'upgrade'").unwrap();
    MutQueryExecutor::new(&mut store, "default".to_string())
        .execute(&q2)
        .unwrap();

    let nodes = store.get_nodes_by_label(&Label::new("Server"));
    assert_eq!(
        nodes[0].properties.get("status").unwrap().as_string(),
        Some("maintenance")
    );
    assert_eq!(
        nodes[0]
            .properties
            .get("downtime_reason")
            .unwrap()
            .as_string(),
        Some("upgrade")
    );
}

// ---------------------------------------------------------------------------
// DELETE tests
// ---------------------------------------------------------------------------

#[test]
fn test_delete_node() {
    let mut store = GraphStore::new();
    let engine = QueryEngine::new();

    exec_mut(
        &engine,
        &mut store,
        "CREATE (t:TempNode {name: 'delete_me'})",
    );
    exec_mut(&engine, &mut store, "CREATE (t:TempNode {name: 'keep_me'})");

    let result = exec(&engine, &store, "MATCH (t:TempNode) RETURN count(t) AS c");
    assert_eq!(first_int(&result, "c"), 2);

    // Delete one node
    exec_mut(
        &engine,
        &mut store,
        "MATCH (t:TempNode {name: 'delete_me'}) DELETE t",
    );

    let result = exec(&engine, &store, "MATCH (t:TempNode) RETURN count(t) AS c");
    assert_eq!(first_int(&result, "c"), 1);

    let result = exec(&engine, &store, "MATCH (t:TempNode) RETURN t.name");
    assert_eq!(first_str(&result, "t.name"), "keep_me");
}

#[test]
fn test_detach_delete() {
    let mut store = GraphStore::new();
    let engine = QueryEngine::new();

    exec_mut(&engine, &mut store, "CREATE (a:Person {name: 'X'})");
    exec_mut(&engine, &mut store, "CREATE (b:Person {name: 'Y'})");
    exec_mut(
        &engine,
        &mut store,
        "MATCH (a:Person {name: 'X'}), (b:Person {name: 'Y'}) CREATE (a)-[:LINKED]->(b)",
    );

    // DETACH DELETE removes the node and all its relationships
    exec_mut(
        &engine,
        &mut store,
        "MATCH (p:Person {name: 'X'}) DETACH DELETE p",
    );

    let result = exec(&engine, &store, "MATCH (p:Person) RETURN count(p) AS c");
    assert_eq!(first_int(&result, "c"), 1);
}

// ---------------------------------------------------------------------------
// MERGE tests
// ---------------------------------------------------------------------------

#[test]
fn test_merge_creates_when_not_exists() {
    let mut store = GraphStore::new();
    let engine = QueryEngine::new();

    exec_mut(&engine, &mut store, "MERGE (c:Country {name: 'France'})");

    let result = exec(
        &engine,
        &store,
        "MATCH (c:Country {name: 'France'}) RETURN c.name",
    );
    assert_eq!(result.len(), 1);
    assert_eq!(first_str(&result, "c.name"), "France");
}

#[test]
fn test_merge_matches_when_exists() {
    let mut store = GraphStore::new();
    let engine = QueryEngine::new();

    exec_mut(&engine, &mut store, "CREATE (c:Country {name: 'Germany'})");

    // MERGE should not create a duplicate
    exec_mut(&engine, &mut store, "MERGE (c:Country {name: 'Germany'})");

    let result = exec(
        &engine,
        &store,
        "MATCH (c:Country {name: 'Germany'}) RETURN count(c) AS c",
    );
    assert_eq!(first_int(&result, "c"), 1);
}

// ---------------------------------------------------------------------------
// WITH / pipeline tests
// ---------------------------------------------------------------------------

#[test]
fn test_with_pipeline() {
    let store = build_movie_graph();
    let engine = QueryEngine::new();

    // Find directors who directed more than 1 movie
    let result = exec(
        &engine,
        &store,
        "MATCH (d:Director)-[:DIRECTED]->(m:Movie) \
         WITH d.name AS director, count(m) AS films \
         WHERE films > 1 \
         RETURN director, films",
    );
    assert_eq!(result.len(), 1);
    assert_eq!(first_str(&result, "director"), "Christopher Nolan");
    assert_eq!(first_int(&result, "films"), 4);
}

// ---------------------------------------------------------------------------
// ORDER BY / SKIP / LIMIT tests
// ---------------------------------------------------------------------------

#[test]
fn test_order_by_desc_with_limit() {
    let store = build_movie_graph();
    let engine = QueryEngine::new();

    // Top 3 highest-rated movies (rating is integer: 90, 88, 87, 86, 84)
    let result = exec(
        &engine,
        &store,
        "MATCH (m:Movie) RETURN m.title, m.rating ORDER BY m.rating DESC LIMIT 3",
    );
    assert_eq!(result.len(), 3);
    assert_eq!(first_str(&result, "m.title"), "The Dark Knight"); // rating 90
}

#[test]
fn test_skip_and_limit() {
    let store = build_movie_graph();
    let engine = QueryEngine::new();

    // Skip first 2, take next 2 (sorted by year)
    let result = exec(
        &engine,
        &store,
        "MATCH (m:Movie) RETURN m.title, m.year ORDER BY m.year SKIP 2 LIMIT 2",
    );
    assert_eq!(result.len(), 2);
    // After skipping 1999 and 2000, should get 2008 and 2010
    assert_eq!(first_int(&result, "m.year"), 2008);
}

// ---------------------------------------------------------------------------
// String function tests
// ---------------------------------------------------------------------------

#[test]
fn test_string_functions() {
    let mut store = GraphStore::new();
    let engine = QueryEngine::new();

    exec_mut(
        &engine,
        &mut store,
        "CREATE (p:Person {name: 'alice wonderland'})",
    );

    let result = exec(
        &engine,
        &store,
        "MATCH (p:Person) RETURN toUpper(p.name) AS upper, size(p.name) AS len",
    );
    assert_eq!(first_str(&result, "upper"), "ALICE WONDERLAND");
    assert_eq!(first_int(&result, "len"), 16);
}

// ---------------------------------------------------------------------------
// UNWIND tests (requires preceding MATCH in this engine)
// ---------------------------------------------------------------------------

#[test]
fn test_unwind_with_return() {
    let mut store = GraphStore::new();
    let engine = QueryEngine::new();

    exec_mut(&engine, &mut store, "CREATE (d:Dummy {val: 1})");

    // UNWIND a list and return values
    let result = exec(
        &engine,
        &store,
        "MATCH (d:Dummy) UNWIND [1, 2, 3, 4, 5] AS x RETURN x ORDER BY x",
    );
    assert_eq!(result.len(), 5);
    // UNWIND values come through as Property values
    let x_val = result.records[0]
        .get("x")
        .unwrap()
        .as_property()
        .unwrap()
        .clone();
    assert!(
        x_val == PropertyValue::Integer(1) || x_val == PropertyValue::Float(1.0),
        "Expected 1, got {:?}",
        x_val
    );
}

#[test]
fn test_unwind_with_aggregation() {
    let mut store = GraphStore::new();
    let engine = QueryEngine::new();

    exec_mut(&engine, &mut store, "CREATE (d:Dummy {val: 1})");

    let result = exec(
        &engine,
        &store,
        "MATCH (d:Dummy) UNWIND [10, 20, 30] AS x RETURN sum(x) AS total, count(x) AS cnt",
    );
    assert_eq!(result.len(), 1);
    let total = result.records[0]
        .get("total")
        .unwrap()
        .as_property()
        .unwrap()
        .clone();
    // sum() may return Integer or Float depending on input types
    assert!(
        total == PropertyValue::Integer(60) || total == PropertyValue::Float(60.0),
        "Expected sum=60, got {:?}",
        total
    );
    assert_eq!(first_int(&result, "cnt"), 3);
}

// ---------------------------------------------------------------------------
// OPTIONAL MATCH tests
// ---------------------------------------------------------------------------

#[test]
fn test_optional_match() {
    let store = build_movie_graph();
    let engine = QueryEngine::new();

    // Keanu Reeves acted in The Matrix; no DIRECTED relationship from him
    let result = exec(
        &engine,
        &store,
        "MATCH (a:Actor {name: 'Keanu Reeves'}) \
         OPTIONAL MATCH (a)-[:DIRECTED]->(m:Movie) \
         RETURN a.name, m.title",
    );
    assert_eq!(result.len(), 1);
    assert_eq!(first_str(&result, "a.name"), "Keanu Reeves");
    // m.title should be null
    let m_title = result.records[0]
        .get("m.title")
        .unwrap()
        .as_property()
        .unwrap();
    assert_eq!(*m_title, PropertyValue::Null);
}

// ---------------------------------------------------------------------------
// EXPLAIN test
// ---------------------------------------------------------------------------

#[test]
fn test_explain_returns_plan() {
    let store = build_movie_graph();
    let engine = QueryEngine::new();

    let result = exec(
        &engine,
        &store,
        "EXPLAIN MATCH (m:Movie) WHERE m.year > 2000 RETURN m.title",
    );
    // EXPLAIN returns the plan as records, not actual data
    assert!(result.len() >= 1);
}

// ---------------------------------------------------------------------------
// End-to-end scenario: build, query, update, delete
// ---------------------------------------------------------------------------

#[test]
fn test_full_crud_lifecycle() {
    let mut store = GraphStore::new();
    let engine = QueryEngine::new();

    // CREATE
    exec_mut(
        &engine,
        &mut store,
        "CREATE (u:User {name: 'TestUser', status: 'active', score: 0})",
    );
    exec_mut(
        &engine,
        &mut store,
        "CREATE (u:User {name: 'OtherUser', status: 'active', score: 50})",
    );

    // READ
    let result = exec(
        &engine,
        &store,
        "MATCH (u:User {name: 'TestUser'}) RETURN u.status, u.score",
    );
    assert_eq!(first_str(&result, "u.status"), "active");
    assert_eq!(first_int(&result, "u.score"), 0);

    // UPDATE (verified via store due to late materialization)
    exec_mut(
        &engine,
        &mut store,
        "MATCH (u:User {name: 'TestUser'}) SET u.score = 100, u.status = 'premium'",
    );
    let nodes: Vec<_> = store
        .get_nodes_by_label(&Label::new("User"))
        .into_iter()
        .filter(|n| n.properties.get("name").and_then(|p| p.as_string()) == Some("TestUser"))
        .collect();
    assert_eq!(
        nodes[0].properties.get("status").unwrap().as_string(),
        Some("premium")
    );
    assert_eq!(
        nodes[0].properties.get("score").unwrap().as_integer(),
        Some(100)
    );

    // CREATE relationship
    exec_mut(
        &engine,
        &mut store,
        "MATCH (a:User {name: 'TestUser'}), (b:User {name: 'OtherUser'}) CREATE (a)-[:FOLLOWS]->(b)",
    );
    let result = exec(
        &engine,
        &store,
        "MATCH (a:User)-[:FOLLOWS]->(b:User) RETURN a.name, b.name",
    );
    assert_eq!(result.len(), 1);

    // DELETE relationship target
    exec_mut(
        &engine,
        &mut store,
        "MATCH (u:User {name: 'OtherUser'}) DETACH DELETE u",
    );
    let result = exec(&engine, &store, "MATCH (u:User) RETURN count(u) AS c");
    assert_eq!(first_int(&result, "c"), 1);
}
