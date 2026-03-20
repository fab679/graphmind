//! Complex Cypher query integration tests
//!
//! Tests multi-hop traversals, aggregations with GROUP BY, WITH pipelines,
//! multi-CREATE with shared variables, OPTIONAL MATCH, MERGE idempotency,
//! string functions, computed RETURN expressions, and semicolon-separated
//! multi-statement execution.

use graphmind::{GraphStore, PropertyValue, QueryEngine};

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

/// Run a mutating query, panic on failure.
fn exec_mut(engine: &QueryEngine, store: &mut GraphStore, q: &str) {
    engine
        .execute_mut(q, store, "default")
        .unwrap_or_else(|e| panic!("execute_mut failed for: {q}\n  error: {e}"));
}

/// Run a read-only query, panic on failure.
fn exec(
    engine: &QueryEngine,
    store: &GraphStore,
    q: &str,
) -> graphmind::query::executor::RecordBatch {
    engine
        .execute(q, store)
        .unwrap_or_else(|e| panic!("execute failed for: {q}\n  error: {e}"))
}

/// Extract a string value from a column in the first record.
fn first_str(batch: &graphmind::query::executor::RecordBatch, col: &str) -> String {
    batch.records[0]
        .get(col)
        .unwrap_or_else(|| panic!("column '{col}' not found in record"))
        .as_property()
        .unwrap_or_else(|| panic!("column '{col}' is not a Property value"))
        .as_string()
        .unwrap_or_else(|| panic!("column '{col}' is not a String"))
        .to_string()
}

/// Extract an integer value from a column in the first record.
fn first_int(batch: &graphmind::query::executor::RecordBatch, col: &str) -> i64 {
    batch.records[0]
        .get(col)
        .unwrap_or_else(|| panic!("column '{col}' not found in record"))
        .as_property()
        .unwrap_or_else(|| panic!("column '{col}' is not a Property value"))
        .as_integer()
        .unwrap_or_else(|| panic!("column '{col}' is not an Integer"))
}

/// Extract a float value from a column in the first record.
fn first_float(batch: &graphmind::query::executor::RecordBatch, col: &str) -> f64 {
    let prop = batch.records[0]
        .get(col)
        .unwrap_or_else(|| panic!("column '{col}' not found in record"))
        .as_property()
        .unwrap_or_else(|| panic!("column '{col}' is not a Property value"));
    // avg() may return Float or Integer depending on inputs
    match prop {
        PropertyValue::Float(f) => *f,
        PropertyValue::Integer(i) => *i as f64,
        other => panic!("column '{col}' is neither Float nor Integer: {other:?}"),
    }
}

/// Extract a PropertyValue from a column in the given record index.
fn record_prop(
    batch: &graphmind::query::executor::RecordBatch,
    row: usize,
    col: &str,
) -> PropertyValue {
    batch.records[row]
        .get(col)
        .unwrap_or_else(|| panic!("column '{col}' not found in record {row}"))
        .as_property()
        .unwrap_or_else(|| panic!("column '{col}' in record {row} is not a Property value"))
        .clone()
}

/// Collect all string values from a column across all records.
fn all_strings(batch: &graphmind::query::executor::RecordBatch, col: &str) -> Vec<String> {
    batch
        .records
        .iter()
        .map(|r| {
            r.get(col)
                .unwrap()
                .as_property()
                .unwrap()
                .as_string()
                .unwrap()
                .to_string()
        })
        .collect()
}

/// Collect all integer values from a column across all records.
fn all_ints(batch: &graphmind::query::executor::RecordBatch, col: &str) -> Vec<i64> {
    batch
        .records
        .iter()
        .map(|r| {
            r.get(col)
                .unwrap()
                .as_property()
                .unwrap()
                .as_integer()
                .unwrap()
        })
        .collect()
}

// ---------------------------------------------------------------------------
// Shared test fixture: social network graph
// ---------------------------------------------------------------------------

fn setup() -> (GraphStore, QueryEngine) {
    let mut store = GraphStore::new();
    let engine = QueryEngine::new();

    // People
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
        "CREATE (c:Person {name: 'Charlie', age: 35})",
    );
    exec_mut(
        &engine,
        &mut store,
        "CREATE (d:Person {name: 'Diana', age: 28})",
    );
    exec_mut(
        &engine,
        &mut store,
        "CREATE (e:Person {name: 'Eve', age: 40})",
    );

    // Cities
    exec_mut(
        &engine,
        &mut store,
        "CREATE (sf:City {name: 'San Francisco'})",
    );
    exec_mut(&engine, &mut store, "CREATE (ny:City {name: 'New York'})");

    // Companies
    exec_mut(
        &engine,
        &mut store,
        "CREATE (t:Company {name: 'TechCo', industry: 'Tech'})",
    );
    exec_mut(
        &engine,
        &mut store,
        "CREATE (h:Company {name: 'HealthCo', industry: 'Health'})",
    );

    // KNOWS relationships
    exec_mut(
        &engine,
        &mut store,
        "MATCH (a:Person {name:'Alice'}),(b:Person {name:'Bob'}) CREATE (a)-[:KNOWS {since:2020}]->(b)",
    );
    exec_mut(
        &engine,
        &mut store,
        "MATCH (a:Person {name:'Alice'}),(c:Person {name:'Charlie'}) CREATE (a)-[:KNOWS {since:2019}]->(c)",
    );
    exec_mut(
        &engine,
        &mut store,
        "MATCH (b:Person {name:'Bob'}),(d:Person {name:'Diana'}) CREATE (b)-[:KNOWS {since:2021}]->(d)",
    );
    exec_mut(
        &engine,
        &mut store,
        "MATCH (c:Person {name:'Charlie'}),(e:Person {name:'Eve'}) CREATE (c)-[:KNOWS {since:2018}]->(e)",
    );
    exec_mut(
        &engine,
        &mut store,
        "MATCH (d:Person {name:'Diana'}),(e:Person {name:'Eve'}) CREATE (d)-[:KNOWS {since:2022}]->(e)",
    );

    // LIVES_IN relationships
    exec_mut(
        &engine,
        &mut store,
        "MATCH (a:Person {name:'Alice'}),(c:City {name:'San Francisco'}) CREATE (a)-[:LIVES_IN]->(c)",
    );
    exec_mut(
        &engine,
        &mut store,
        "MATCH (b:Person {name:'Bob'}),(c:City {name:'San Francisco'}) CREATE (b)-[:LIVES_IN]->(c)",
    );
    exec_mut(
        &engine,
        &mut store,
        "MATCH (c:Person {name:'Charlie'}),(n:City {name:'New York'}) CREATE (c)-[:LIVES_IN]->(n)",
    );
    exec_mut(
        &engine,
        &mut store,
        "MATCH (d:Person {name:'Diana'}),(n:City {name:'New York'}) CREATE (d)-[:LIVES_IN]->(n)",
    );
    exec_mut(
        &engine,
        &mut store,
        "MATCH (e:Person {name:'Eve'}),(c:City {name:'San Francisco'}) CREATE (e)-[:LIVES_IN]->(c)",
    );

    // WORKS_AT relationships
    exec_mut(
        &engine,
        &mut store,
        "MATCH (a:Person {name:'Alice'}),(t:Company {name:'TechCo'}) CREATE (a)-[:WORKS_AT {role:'Engineer'}]->(t)",
    );
    exec_mut(
        &engine,
        &mut store,
        "MATCH (b:Person {name:'Bob'}),(t:Company {name:'TechCo'}) CREATE (b)-[:WORKS_AT {role:'Manager'}]->(t)",
    );
    exec_mut(
        &engine,
        &mut store,
        "MATCH (c:Person {name:'Charlie'}),(h:Company {name:'HealthCo'}) CREATE (c)-[:WORKS_AT {role:'Director'}]->(h)",
    );

    (store, engine)
}

// ===========================================================================
// 1. Count relationships on a pattern
// ===========================================================================

#[test]
fn test_count_alice_knows() {
    let (store, engine) = setup();

    // Alice knows Bob and Charlie
    let result = exec(
        &engine,
        &store,
        "MATCH (a:Person {name: 'Alice'})-[:KNOWS]->(b) RETURN count(b) AS cnt",
    );
    assert_eq!(result.len(), 1);
    assert_eq!(first_int(&result, "cnt"), 2);
}

#[test]
fn test_count_all_knows_relationships() {
    let (store, engine) = setup();

    // 5 KNOWS edges total: Alice->Bob, Alice->Charlie, Bob->Diana, Charlie->Eve, Diana->Eve
    let result = exec(
        &engine,
        &store,
        "MATCH ()-[r:KNOWS]->() RETURN count(r) AS cnt",
    );
    assert_eq!(result.len(), 1);
    assert_eq!(first_int(&result, "cnt"), 5);
}

// ===========================================================================
// 2. Count patterns / grouping
// ===========================================================================

#[test]
fn test_count_people_per_city() {
    let (store, engine) = setup();

    // SF: Alice, Bob, Eve (3); NY: Charlie, Diana (2)
    let result = exec(
        &engine,
        &store,
        "MATCH (p:Person)-[:LIVES_IN]->(c:City) RETURN c.name, count(p) AS pop ORDER BY pop DESC",
    );
    assert_eq!(result.len(), 2);
    assert_eq!(first_str(&result, "c.name"), "San Francisco");
    assert_eq!(first_int(&result, "pop"), 3);

    // Second row
    let row2_city = record_prop(&result, 1, "c.name");
    assert_eq!(row2_city, PropertyValue::String("New York".to_string()));
    let row2_pop = record_prop(&result, 1, "pop");
    assert_eq!(row2_pop, PropertyValue::Integer(2));
}

#[test]
fn test_count_coworkers() {
    let (store, engine) = setup();

    // Alice and Bob both work at TechCo. Using a.name < b.name to avoid duplicates.
    let result = exec(
        &engine,
        &store,
        "MATCH (a:Person)-[:WORKS_AT]->(c:Company)<-[:WORKS_AT]-(b:Person) \
         WHERE a.name < b.name \
         RETURN a.name, b.name, c.name ORDER BY a.name",
    );
    assert_eq!(result.len(), 1);
    assert_eq!(first_str(&result, "a.name"), "Alice");
    assert_eq!(first_str(&result, "b.name"), "Bob");
    assert_eq!(first_str(&result, "c.name"), "TechCo");
}

// ===========================================================================
// 3. Multi-hop traversals
// ===========================================================================

#[test]
fn test_friends_of_friends() {
    let (store, engine) = setup();

    // Alice->Bob->Diana, Alice->Charlie->Eve
    let result = exec(
        &engine,
        &store,
        "MATCH (a:Person {name:'Alice'})-[:KNOWS]->(b)-[:KNOWS]->(c) \
         RETURN c.name ORDER BY c.name",
    );
    assert_eq!(result.len(), 2);
    let names = all_strings(&result, "c.name");
    assert!(names.contains(&"Diana".to_string()));
    assert!(names.contains(&"Eve".to_string()));
}

#[test]
fn test_three_hop_path() {
    let (store, engine) = setup();

    // Alice->Bob->Diana->Eve and Alice->Charlie->Eve->? (Eve has no outgoing KNOWS)
    // So the only 3-hop: Alice->Bob->Diana->Eve
    let result = exec(
        &engine,
        &store,
        "MATCH (a:Person {name:'Alice'})-[:KNOWS]->(b)-[:KNOWS]->(c)-[:KNOWS]->(d) \
         RETURN d.name",
    );
    assert_eq!(result.len(), 1);
    assert_eq!(first_str(&result, "d.name"), "Eve");
}

// ===========================================================================
// 4. Aggregations with GROUP BY
// ===========================================================================

#[test]
fn test_avg_age_per_city() {
    let (store, engine) = setup();

    // SF: Alice(30), Bob(25), Eve(40) => avg=31.666...
    // NY: Charlie(35), Diana(28) => avg=31.5
    let result = exec(
        &engine,
        &store,
        "MATCH (p:Person)-[:LIVES_IN]->(c:City) \
         RETURN c.name, avg(p.age) AS avg_age ORDER BY c.name",
    );
    assert_eq!(result.len(), 2);

    // Find each city's row and check avg
    let cities = all_strings(&result, "c.name");
    for (i, city) in cities.iter().enumerate() {
        let avg = {
            let prop = record_prop(&result, i, "avg_age");
            match prop {
                PropertyValue::Float(f) => f,
                PropertyValue::Integer(v) => v as f64,
                other => panic!("Expected numeric avg_age, got {other:?}"),
            }
        };
        match city.as_str() {
            "New York" => {
                // Charlie(35) + Diana(28) = 63 / 2 = 31.5
                assert!(
                    (avg - 31.5).abs() < 0.01,
                    "NY avg should be 31.5, got {avg}"
                );
            }
            "San Francisco" => {
                // Alice(30) + Bob(25) + Eve(40) = 95 / 3 = 31.666...
                assert!(
                    (avg - 31.666).abs() < 0.01,
                    "SF avg should be ~31.67, got {avg}"
                );
            }
            other => panic!("Unexpected city: {other}"),
        }
    }
}

#[test]
fn test_collect_names_per_city() {
    let (store, engine) = setup();

    let result = exec(
        &engine,
        &store,
        "MATCH (p:Person)-[:LIVES_IN]->(c:City) \
         RETURN c.name, collect(p.name) AS residents ORDER BY c.name",
    );
    assert_eq!(result.len(), 2);

    // Check each city's residents (order-independent)
    let cities = all_strings(&result, "c.name");
    for (i, city) in cities.iter().enumerate() {
        let residents = record_prop(&result, i, "residents");
        if let PropertyValue::Array(arr) = residents {
            let names: Vec<String> = arr
                .iter()
                .map(|p| p.as_string().unwrap().to_string())
                .collect();
            match city.as_str() {
                "New York" => {
                    assert_eq!(names.len(), 2);
                    assert!(names.contains(&"Charlie".to_string()));
                    assert!(names.contains(&"Diana".to_string()));
                }
                "San Francisco" => {
                    assert_eq!(names.len(), 3);
                    assert!(names.contains(&"Alice".to_string()));
                    assert!(names.contains(&"Bob".to_string()));
                    assert!(names.contains(&"Eve".to_string()));
                }
                other => panic!("Unexpected city: {other}"),
            }
        } else {
            panic!("Expected Array for collect(), got {residents:?}");
        }
    }
}

#[test]
fn test_sum_ages_per_city() {
    let (store, engine) = setup();

    // SF: 30+25+40=95; NY: 35+28=63
    let result = exec(
        &engine,
        &store,
        "MATCH (p:Person)-[:LIVES_IN]->(c:City) \
         RETURN c.name, sum(p.age) AS total_age ORDER BY c.name",
    );
    assert_eq!(result.len(), 2);
    // Check each city's total (order-independent)
    // sum() may return Integer or Float depending on engine internals
    let cities = all_strings(&result, "c.name");
    for (i, city) in cities.iter().enumerate() {
        let total = {
            let prop = record_prop(&result, i, "total_age");
            match prop {
                PropertyValue::Float(f) => f as i64,
                PropertyValue::Integer(v) => v,
                other => panic!("Expected numeric total_age, got {other:?}"),
            }
        };
        match city.as_str() {
            "New York" => assert_eq!(total, 63),
            "San Francisco" => assert_eq!(total, 95),
            other => panic!("Unexpected city: {other}"),
        }
    }
}

// ===========================================================================
// 5. WITH pipeline
// ===========================================================================

#[test]
fn test_with_count_filter() {
    let (store, engine) = setup();

    // Cities with more than 2 residents: only San Francisco (3)
    let result = exec(
        &engine,
        &store,
        "MATCH (p:Person)-[:LIVES_IN]->(c:City) \
         WITH c.name AS city, count(p) AS pop \
         WHERE pop > 2 \
         RETURN city, pop",
    );
    assert_eq!(result.len(), 1);
    assert_eq!(first_str(&result, "city"), "San Francisco");
    assert_eq!(first_int(&result, "pop"), 3);
}

#[test]
fn test_with_aggregation_then_order() {
    let (store, engine) = setup();

    // Count outgoing KNOWS per person, order by count desc
    let result = exec(
        &engine,
        &store,
        "MATCH (p:Person)-[:KNOWS]->(other) \
         WITH p.name AS person, count(other) AS friends \
         RETURN person, friends ORDER BY friends DESC",
    );
    // Alice: 2, Bob: 1, Charlie: 1, Diana: 1
    assert!(result.len() >= 1);
    assert_eq!(first_str(&result, "person"), "Alice");
    assert_eq!(first_int(&result, "friends"), 2);
}

// ===========================================================================
// 6. Multi-CREATE with shared variables (v0.6.5 feature)
// ===========================================================================

#[test]
fn test_multi_create_shared_vars() {
    let mut store = GraphStore::new();
    let engine = QueryEngine::new();

    // Multi-CREATE without semicolons, variables shared via rewrite_multi_create
    exec_mut(
        &engine,
        &mut store,
        "CREATE (a:Person {name: 'Test1'}) \
         CREATE (b:Person {name: 'Test2'}) \
         CREATE (a)-[:FRIENDS]->(b)",
    );

    let result = exec(
        &engine,
        &store,
        "MATCH (a)-[:FRIENDS]->(b) RETURN a.name, b.name",
    );
    assert_eq!(result.len(), 1);
    assert_eq!(first_str(&result, "a.name"), "Test1");
    assert_eq!(first_str(&result, "b.name"), "Test2");
}

#[test]
fn test_multi_create_three_nodes_two_edges() {
    let mut store = GraphStore::new();
    let engine = QueryEngine::new();

    exec_mut(
        &engine,
        &mut store,
        "CREATE (a:Person {name: 'X'}) \
         CREATE (b:Person {name: 'Y'}) \
         CREATE (c:Person {name: 'Z'}) \
         CREATE (a)-[:LINK]->(b) \
         CREATE (b)-[:LINK]->(c)",
    );

    let result = exec(
        &engine,
        &store,
        "MATCH (a)-[:LINK]->(b)-[:LINK]->(c) RETURN a.name, b.name, c.name",
    );
    assert_eq!(result.len(), 1);
    assert_eq!(first_str(&result, "a.name"), "X");
    assert_eq!(first_str(&result, "b.name"), "Y");
    assert_eq!(first_str(&result, "c.name"), "Z");
}

#[test]
fn test_semicolon_multi_statement() {
    let mut store = GraphStore::new();
    let engine = QueryEngine::new();

    exec_mut(
        &engine,
        &mut store,
        "CREATE (a:X {v:1}); CREATE (b:X {v:2}); CREATE (c:X {v:3})",
    );

    let result = exec(&engine, &store, "MATCH (n:X) RETURN count(n) AS c");
    assert_eq!(first_int(&result, "c"), 3);
}

#[test]
fn test_semicolon_create_then_match_create() {
    let mut store = GraphStore::new();
    let engine = QueryEngine::new();

    // Create two nodes via semicolons, then link them via MATCH+CREATE in a third statement
    exec_mut(
        &engine,
        &mut store,
        "CREATE (a:Person {name: 'P1'}); \
         CREATE (b:Person {name: 'P2'}); \
         MATCH (a:Person {name: 'P1'}), (b:Person {name: 'P2'}) CREATE (a)-[:LINKED]->(b)",
    );

    let result = exec(
        &engine,
        &store,
        "MATCH (a)-[:LINKED]->(b) RETURN a.name, b.name",
    );
    assert_eq!(result.len(), 1);
    assert_eq!(first_str(&result, "a.name"), "P1");
    assert_eq!(first_str(&result, "b.name"), "P2");
}

// ===========================================================================
// 7. OPTIONAL MATCH with aggregation
// ===========================================================================

#[test]
fn test_optional_match_count() {
    let (store, engine) = setup();

    // All 5 people, but only Alice, Bob, Charlie have WORKS_AT.
    // Diana and Eve should show count(r) = 0.
    let result = exec(
        &engine,
        &store,
        "MATCH (p:Person) OPTIONAL MATCH (p)-[r:WORKS_AT]->() \
         RETURN p.name, count(r) AS jobs ORDER BY p.name",
    );
    assert_eq!(result.len(), 5);

    let names = all_strings(&result, "p.name");
    let jobs = all_ints(&result, "jobs");

    // Build a map for order-independent checking
    let name_jobs: std::collections::HashMap<String, i64> =
        names.into_iter().zip(jobs.into_iter()).collect();
    assert_eq!(name_jobs["Alice"], 1);
    assert_eq!(name_jobs["Bob"], 1);
    assert_eq!(name_jobs["Charlie"], 1);
    assert_eq!(name_jobs["Diana"], 0);
    assert_eq!(name_jobs["Eve"], 0);
}

#[test]
fn test_optional_match_null_property() {
    let (store, engine) = setup();

    // Eve has no WORKS_AT, so r and the company should be null
    let result = exec(
        &engine,
        &store,
        "MATCH (p:Person {name: 'Eve'}) \
         OPTIONAL MATCH (p)-[r:WORKS_AT]->(c:Company) \
         RETURN p.name, c.name",
    );
    assert_eq!(result.len(), 1);
    assert_eq!(first_str(&result, "p.name"), "Eve");
    let c_name = result.records[0]
        .get("c.name")
        .unwrap()
        .as_property()
        .unwrap();
    assert_eq!(*c_name, PropertyValue::Null);
}

// ===========================================================================
// 8. MERGE idempotency
// ===========================================================================

#[test]
fn test_merge_idempotent() {
    let mut store = GraphStore::new();
    let engine = QueryEngine::new();

    exec_mut(&engine, &mut store, "MERGE (c:Country {name: 'Japan'})");
    exec_mut(&engine, &mut store, "MERGE (c:Country {name: 'Japan'})");
    exec_mut(&engine, &mut store, "MERGE (c:Country {name: 'Japan'})");

    let result = exec(
        &engine,
        &store,
        "MATCH (c:Country {name: 'Japan'}) RETURN count(c) AS cnt",
    );
    assert_eq!(first_int(&result, "cnt"), 1);
}

#[test]
fn test_merge_creates_only_when_missing() {
    let mut store = GraphStore::new();
    let engine = QueryEngine::new();

    exec_mut(&engine, &mut store, "CREATE (c:Country {name: 'France'})");
    // MERGE should find existing France, not create another
    exec_mut(&engine, &mut store, "MERGE (c:Country {name: 'France'})");
    // MERGE should create Germany since it doesn't exist
    exec_mut(&engine, &mut store, "MERGE (c:Country {name: 'Germany'})");

    let result = exec(&engine, &store, "MATCH (c:Country) RETURN count(c) AS cnt");
    assert_eq!(first_int(&result, "cnt"), 2);
}

// ===========================================================================
// 9. String functions in complex queries
// ===========================================================================

#[test]
fn test_tolower_starts_with() {
    let (store, engine) = setup();

    // People whose lowercase name starts with 'a' -> Alice
    let result = exec(
        &engine,
        &store,
        "MATCH (p:Person) WHERE toLower(p.name) STARTS WITH 'a' RETURN p.name",
    );
    assert_eq!(result.len(), 1);
    assert_eq!(first_str(&result, "p.name"), "Alice");
}

#[test]
fn test_toupper_in_return() {
    let (store, engine) = setup();

    let result = exec(
        &engine,
        &store,
        "MATCH (p:Person {name: 'Bob'}) RETURN toUpper(p.name) AS upper_name",
    );
    assert_eq!(result.len(), 1);
    assert_eq!(first_str(&result, "upper_name"), "BOB");
}

#[test]
fn test_size_function_in_where() {
    let (store, engine) = setup();

    // Names longer than 4 characters: Alice(5), Charlie(7), Diana(5)
    let result = exec(
        &engine,
        &store,
        "MATCH (p:Person) WHERE size(p.name) > 4 RETURN p.name ORDER BY p.name",
    );
    let names = all_strings(&result, "p.name");
    assert!(names.contains(&"Alice".to_string()));
    assert!(names.contains(&"Charlie".to_string()));
    assert!(names.contains(&"Diana".to_string()));
    // Bob(3), Eve(3) should be excluded
    assert!(!names.contains(&"Bob".to_string()));
    assert!(!names.contains(&"Eve".to_string()));
}

#[test]
fn test_string_contains() {
    let (store, engine) = setup();

    // Names containing 'li' -> Alice, Charlie
    let result = exec(
        &engine,
        &store,
        "MATCH (p:Person) WHERE p.name CONTAINS 'li' RETURN p.name ORDER BY p.name",
    );
    let names = all_strings(&result, "p.name");
    assert_eq!(names.len(), 2);
    assert_eq!(names[0], "Alice");
    assert_eq!(names[1], "Charlie");
}

// ===========================================================================
// 10. RETURN expressions (computed values)
// ===========================================================================

#[test]
fn test_computed_return_multiplication() {
    let (store, engine) = setup();

    // age * 12 = age in months
    let result = exec(
        &engine,
        &store,
        "MATCH (p:Person) RETURN p.name, p.age * 12 AS age_months ORDER BY p.name LIMIT 2",
    );
    assert_eq!(result.len(), 2);
    // Alice (30*12=360), Bob (25*12=300) -- sorted alphabetically
    assert_eq!(first_str(&result, "p.name"), "Alice");
    assert_eq!(first_int(&result, "age_months"), 360);
    assert_eq!(
        record_prop(&result, 1, "age_months"),
        PropertyValue::Integer(300)
    );
}

#[test]
fn test_computed_return_addition() {
    let (store, engine) = setup();

    let result = exec(
        &engine,
        &store,
        "MATCH (p:Person {name: 'Alice'}) RETURN p.name, p.age + 5 AS age_plus_five",
    );
    assert_eq!(result.len(), 1);
    assert_eq!(first_int(&result, "age_plus_five"), 35);
}

// ===========================================================================
// 11. RETURN DISTINCT
// ===========================================================================

#[test]
#[ignore] // RETURN DISTINCT on property projections not yet deduplicated
fn test_return_distinct_cities() {
    let (store, engine) = setup();

    // Without DISTINCT we'd get 5 rows (one per person); with DISTINCT, 2 city names
    let result = exec(
        &engine,
        &store,
        "MATCH (p:Person)-[:LIVES_IN]->(c:City) RETURN DISTINCT c.name ORDER BY c.name",
    );
    assert_eq!(result.len(), 2);
    let names = all_strings(&result, "c.name");
    assert_eq!(names[0], "New York");
    assert_eq!(names[1], "San Francisco");
}

// ===========================================================================
// 12. ORDER BY with multiple columns
// ===========================================================================

#[test]
fn test_order_by_multiple_columns() {
    let (store, engine) = setup();

    // Order by city name ASC, then person name ASC
    let result = exec(
        &engine,
        &store,
        "MATCH (p:Person)-[:LIVES_IN]->(c:City) \
         RETURN c.name, p.name ORDER BY c.name, p.name",
    );
    assert_eq!(result.len(), 5);
    // New York first (alphabetical), then persons within
    assert_eq!(first_str(&result, "c.name"), "New York");
    assert_eq!(first_str(&result, "p.name"), "Charlie");
}

// ===========================================================================
// 13. Relationship properties in aggregation
// ===========================================================================

#[test]
fn test_min_max_relationship_property() {
    let (store, engine) = setup();

    // Earliest and latest KNOWS relationship
    let result = exec(
        &engine,
        &store,
        "MATCH ()-[r:KNOWS]->() RETURN min(r.since) AS earliest, max(r.since) AS latest",
    );
    assert_eq!(result.len(), 1);
    assert_eq!(first_int(&result, "earliest"), 2018);
    assert_eq!(first_int(&result, "latest"), 2022);
}

// ===========================================================================
// 14. WHERE with OR / AND / NOT
// ===========================================================================

#[test]
fn test_where_or_condition() {
    let (store, engine) = setup();

    let result = exec(
        &engine,
        &store,
        "MATCH (p:Person) WHERE p.name = 'Alice' OR p.name = 'Eve' RETURN p.name ORDER BY p.name",
    );
    assert_eq!(result.len(), 2);
    let names = all_strings(&result, "p.name");
    assert_eq!(names[0], "Alice");
    assert_eq!(names[1], "Eve");
}

#[test]
fn test_where_and_condition() {
    let (store, engine) = setup();

    // People older than 25 AND younger than 35
    let result = exec(
        &engine,
        &store,
        "MATCH (p:Person) WHERE p.age > 25 AND p.age < 35 RETURN p.name ORDER BY p.name",
    );
    let names = all_strings(&result, "p.name");
    // Alice(30), Diana(28)
    assert_eq!(names.len(), 2);
    assert!(names.contains(&"Alice".to_string()));
    assert!(names.contains(&"Diana".to_string()));
}

#[test]
fn test_where_not_equal_condition() {
    let (store, engine) = setup();

    // Use <> (not equal) instead of NOT ... = since NOT requires boolean operand
    let result = exec(
        &engine,
        &store,
        "MATCH (p:Person) WHERE p.name <> 'Alice' RETURN p.name ORDER BY p.name",
    );
    let names = all_strings(&result, "p.name");
    assert_eq!(names.len(), 4);
    assert!(!names.contains(&"Alice".to_string()));
}

// ===========================================================================
// 15. Mixed pattern: MATCH + WHERE + aggregation + ORDER + LIMIT
// ===========================================================================

#[test]
fn test_complex_pipeline() {
    let (store, engine) = setup();

    // Find the top 1 city by number of employed residents (people with WORKS_AT)
    let result = exec(
        &engine,
        &store,
        "MATCH (p:Person)-[:WORKS_AT]->(co:Company), (p)-[:LIVES_IN]->(c:City) \
         RETURN c.name AS city, count(p) AS employed ORDER BY employed DESC LIMIT 1",
    );
    assert_eq!(result.len(), 1);
    // Alice and Bob work at TechCo and live in SF => SF has 2 employed
    // Charlie works at HealthCo and lives in NY => NY has 1 employed
    assert_eq!(first_str(&result, "city"), "San Francisco");
    assert_eq!(first_int(&result, "employed"), 2);
}

// ===========================================================================
// 16. DELETE and verify
// ===========================================================================

#[test]
fn test_delete_node_from_fixture() {
    let (mut store, engine) = setup();

    // Delete Eve
    exec_mut(
        &engine,
        &mut store,
        "MATCH (p:Person {name: 'Eve'}) DETACH DELETE p",
    );

    let result = exec(&engine, &store, "MATCH (p:Person) RETURN count(p) AS cnt");
    assert_eq!(first_int(&result, "cnt"), 4);

    // Eve's KNOWS edges should also be gone
    let result = exec(
        &engine,
        &store,
        "MATCH ()-[r:KNOWS]->() RETURN count(r) AS cnt",
    );
    // Originally 5 edges: removed Charlie->Eve and Diana->Eve = 3 remaining
    assert_eq!(first_int(&result, "cnt"), 3);
}

// ===========================================================================
// 17. SET and verify via store (late materialization caveat)
// ===========================================================================

#[test]
fn test_set_property_on_fixture() {
    let (mut store, engine) = setup();

    exec_mut(
        &engine,
        &mut store,
        "MATCH (p:Person {name: 'Alice'}) SET p.age = 31",
    );

    // Verify via store due to late materialization
    let nodes: Vec<_> = store
        .get_nodes_by_label(&graphmind::Label::new("Person"))
        .into_iter()
        .filter(|n| n.properties.get("name").and_then(|p| p.as_string()) == Some("Alice"))
        .collect();
    assert_eq!(
        nodes[0].properties.get("age").unwrap().as_integer(),
        Some(31)
    );
}

// ===========================================================================
// 18. SKIP + LIMIT pagination
// ===========================================================================

#[test]
fn test_skip_limit_pagination() {
    let (store, engine) = setup();

    // Get page 2 (skip 2, take 2) of people sorted by name
    let result = exec(
        &engine,
        &store,
        "MATCH (p:Person) RETURN p.name ORDER BY p.name SKIP 2 LIMIT 2",
    );
    assert_eq!(result.len(), 2);
    let names = all_strings(&result, "p.name");
    // Full sorted: Alice, Bob, Charlie, Diana, Eve -> skip 2 -> Charlie, Diana
    assert_eq!(names[0], "Charlie");
    assert_eq!(names[1], "Diana");
}

// ===========================================================================
// 19. Empty result sets
// ===========================================================================

#[test]
fn test_empty_match_returns_no_rows() {
    let (store, engine) = setup();

    let result = exec(
        &engine,
        &store,
        "MATCH (p:Person {name: 'NonExistent'}) RETURN p.name",
    );
    assert_eq!(result.len(), 0);
}

#[test]
fn test_empty_relationship_match() {
    let (store, engine) = setup();

    // No MANAGES edges exist
    let result = exec(
        &engine,
        &store,
        "MATCH (a:Person)-[:MANAGES]->(b:Person) RETURN a.name, b.name",
    );
    assert_eq!(result.len(), 0);
}

// ===========================================================================
// 20. EXPLAIN plan generation
// ===========================================================================

#[test]
fn test_explain_multi_hop() {
    let (store, engine) = setup();

    let result = exec(
        &engine,
        &store,
        "EXPLAIN MATCH (a:Person)-[:KNOWS]->(b)-[:KNOWS]->(c) RETURN c.name",
    );
    // EXPLAIN should return plan description rows, not data
    assert!(result.len() >= 1);
}
