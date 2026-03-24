use graphmind::graph::{Label, PropertyValue};
use graphmind::{GraphStore, QueryEngine};
use std::collections::HashMap;

#[test]
fn test_params_in_match_where() {
    let mut store = GraphStore::new();
    let engine = QueryEngine::new();

    engine
        .execute_mut(
            r#"CREATE (a:Person {name: "Alice", age: 30})
               CREATE (b:Person {name: "Bob", age: 25})"#,
            &mut store,
            "default",
        )
        .unwrap();

    let mut params = HashMap::new();
    params.insert(
        "name".to_string(),
        PropertyValue::String("Alice".to_string()),
    );

    let result = engine
        .execute_with_params(
            "MATCH (n:Person) WHERE n.name = $name RETURN n.name AS name, n.age AS age",
            &store,
            &params,
        )
        .unwrap();

    assert_eq!(result.records.len(), 1);
}

#[test]
fn test_params_in_write() {
    let mut store = GraphStore::new();
    let engine = QueryEngine::new();

    // Create a node first
    engine
        .execute_mut(
            r#"CREATE (n:Person {name: "placeholder"})"#,
            &mut store,
            "default",
        )
        .unwrap();

    // Update with params via MATCH + SET
    let mut params = HashMap::new();
    params.insert(
        "newName".to_string(),
        PropertyValue::String("Charlie".to_string()),
    );
    params.insert("age".to_string(), PropertyValue::Integer(40));

    engine
        .execute_mut_with_params(
            "MATCH (n:Person) SET n.name = $newName, n.age = $age",
            &mut store,
            "default",
            &params,
        )
        .unwrap();

    let persons = store.get_nodes_by_label(&Label::new("Person"));
    assert_eq!(persons.len(), 1);
    assert_eq!(
        persons[0].properties.get("name"),
        Some(&PropertyValue::String("Charlie".to_string()))
    );
    assert_eq!(
        persons[0].properties.get("age"),
        Some(&PropertyValue::Integer(40))
    );
}

#[test]
fn test_params_multiple_types() {
    let mut store = GraphStore::new();
    let engine = QueryEngine::new();

    engine
        .execute_mut(
            r#"CREATE (a:Item {name: "X", price: 9.99, active: true, count: 5})"#,
            &mut store,
            "default",
        )
        .unwrap();

    // Match with integer param
    let mut params = HashMap::new();
    params.insert("min_count".to_string(), PropertyValue::Integer(3));

    let result = engine
        .execute_with_params(
            "MATCH (n:Item) WHERE n.count > $min_count RETURN n.name AS name",
            &store,
            &params,
        )
        .unwrap();

    assert_eq!(result.records.len(), 1);

    // Match with float param
    let mut params = HashMap::new();
    params.insert("max_price".to_string(), PropertyValue::Float(10.0));

    let result = engine
        .execute_with_params(
            "MATCH (n:Item) WHERE n.price < $max_price RETURN n.name AS name",
            &store,
            &params,
        )
        .unwrap();

    assert_eq!(result.records.len(), 1);
}

#[test]
fn test_unresolved_param_error() {
    let mut store = GraphStore::new();
    let engine = QueryEngine::new();

    // Create data so the filter actually evaluates
    engine
        .execute_mut(
            r#"CREATE (n:Person {name: "Alice"})"#,
            &mut store,
            "default",
        )
        .unwrap();

    // Using $name without providing it should error
    let result = engine.execute_with_params(
        "MATCH (n:Person) WHERE n.name = $name RETURN n",
        &store,
        &HashMap::new(),
    );

    assert!(result.is_err(), "Should fail with unresolved parameter");
    let err = result.unwrap_err().to_string();
    assert!(
        err.contains("Unresolved parameter") || err.contains("$name"),
        "Error should mention unresolved parameter, got: {}",
        err
    );
}

#[test]
fn test_params_for_text_with_quotes() {
    // This is the real use case: overview text contains "meta-human" with literal quotes
    let mut store = GraphStore::new();
    let engine = QueryEngine::new();

    let overview = r#"After a particle accelerator causes a freak storm, CSI Investigator Barry Allen is struck by lightning and falls into a coma. Months later he awakens with the power of super speed. Barry is shocked to discover he is not the only "meta-human" who was created in the wake of the accelerator explosion -- and not everyone is using their new powers for good."#;

    let mut params = HashMap::new();
    params.insert(
        "show_name".to_string(),
        PropertyValue::String("The Flash".to_string()),
    );
    params.insert(
        "overview".to_string(),
        PropertyValue::String(overview.to_string()),
    );
    params.insert(
        "tagline".to_string(),
        PropertyValue::String("The fastest man alive.".to_string()),
    );
    params.insert("seasons".to_string(), PropertyValue::Integer(9));
    params.insert("vote_avg".to_string(), PropertyValue::Float(7.797));

    // Use params to avoid quote escaping issues
    engine
        .execute_mut_with_params(
            r#"MERGE (t:TVShow {id: 60735})
               SET t.name = $show_name,
                   t.seasons = $seasons,
                   t.overview = $overview,
                   t.tagline = $tagline,
                   t.vote_average = $vote_avg"#,
            &mut store,
            "default",
            &params,
        )
        .unwrap();

    let shows = store.get_nodes_by_label(&Label::new("TVShow"));
    assert_eq!(shows.len(), 1);
    assert_eq!(
        shows[0].properties.get("name"),
        Some(&PropertyValue::String("The Flash".to_string()))
    );
    // Verify the overview with quotes is stored correctly
    let stored_overview = match shows[0].properties.get("overview") {
        Some(PropertyValue::String(s)) => s.clone(),
        _ => panic!("overview not found"),
    };
    assert!(
        stored_overview.contains("\"meta-human\""),
        "Overview should contain quoted meta-human"
    );
    assert_eq!(
        shows[0].properties.get("vote_average"),
        Some(&PropertyValue::Float(7.797))
    );
}

#[test]
fn test_single_quotes_for_strings_with_double_quotes() {
    // Alternative: use single quotes to wrap strings containing double quotes
    let mut store = GraphStore::new();
    let engine = QueryEngine::new();

    let q = r#"CREATE (n:Note {text: 'She said "hello" to everyone'})"#;
    engine.execute_mut(q, &mut store, "default").unwrap();

    let notes = store.get_nodes_by_label(&Label::new("Note"));
    assert_eq!(notes.len(), 1);
    let text = match notes[0].properties.get("text") {
        Some(PropertyValue::String(s)) => s.clone(),
        _ => panic!("text not found"),
    };
    assert!(text.contains("\"hello\""));
}

#[test]
fn test_escaped_quotes_in_strings() {
    // Alternative: escape inner quotes with backslash
    let mut store = GraphStore::new();
    let engine = QueryEngine::new();

    let q = r#"CREATE (n:Note {text: "She said \"hello\" to everyone"})"#;
    engine.execute_mut(q, &mut store, "default").unwrap();

    let notes = store.get_nodes_by_label(&Label::new("Note"));
    assert_eq!(notes.len(), 1);
    let text = match notes[0].properties.get("text") {
        Some(PropertyValue::String(s)) => s.clone(),
        _ => panic!("text not found"),
    };
    assert!(text.contains("\"hello\""));
}

#[test]
fn test_params_in_merge_property_map() {
    // This is the exact pattern from the TMDB import notebook:
    // MERGE (t:TVShow {id: $id}) SET t.name = $name ...
    let mut store = GraphStore::new();
    let engine = QueryEngine::new();

    let mut params = HashMap::new();
    params.insert("id".to_string(), PropertyValue::Integer(1399));
    params.insert(
        "name".to_string(),
        PropertyValue::String("Game of Thrones".to_string()),
    );
    params.insert("seasons".to_string(), PropertyValue::Integer(8));

    let q = r#"MERGE (t:TVShow {id: $id}) SET t.name = $name, t.seasons = $seasons"#;
    engine
        .execute_mut_with_params(q, &mut store, "default", &params)
        .unwrap();

    let shows = store.get_nodes_by_label(&Label::new("TVShow"));
    assert_eq!(shows.len(), 1, "Should create 1 TVShow node");
    assert_eq!(
        shows[0].properties.get("id"),
        Some(&PropertyValue::Integer(1399))
    );
    assert_eq!(
        shows[0].properties.get("name"),
        Some(&PropertyValue::String("Game of Thrones".to_string()))
    );
    assert_eq!(
        shows[0].properties.get("seasons"),
        Some(&PropertyValue::Integer(8))
    );

    // Run again — MERGE should be idempotent
    engine
        .execute_mut_with_params(q, &mut store, "default", &params)
        .unwrap();
    let shows2 = store.get_nodes_by_label(&Label::new("TVShow"));
    assert_eq!(shows2.len(), 1, "MERGE should not create duplicate");
}

#[test]
fn test_params_in_create_property_map() {
    let mut store = GraphStore::new();
    let engine = QueryEngine::new();

    let mut params = HashMap::new();
    params.insert(
        "name".to_string(),
        PropertyValue::String("Alice".to_string()),
    );
    params.insert("age".to_string(), PropertyValue::Integer(30));

    let q = r#"CREATE (p:Person {name: $name, age: $age})"#;
    engine
        .execute_mut_with_params(q, &mut store, "default", &params)
        .unwrap();

    let people = store.get_nodes_by_label(&Label::new("Person"));
    assert_eq!(people.len(), 1);
    assert_eq!(
        people[0].properties.get("name"),
        Some(&PropertyValue::String("Alice".to_string()))
    );
    assert_eq!(
        people[0].properties.get("age"),
        Some(&PropertyValue::Integer(30))
    );
}

#[test]
fn test_params_in_match_property_map() {
    let mut store = GraphStore::new();
    let engine = QueryEngine::new();

    engine
        .execute_mut(
            r#"CREATE (a:Person {name: "Alice", age: 30})"#,
            &mut store,
            "default",
        )
        .unwrap();

    let mut params = HashMap::new();
    params.insert(
        "name".to_string(),
        PropertyValue::String("Alice".to_string()),
    );

    let q = r#"MATCH (p:Person {name: $name}) RETURN p.age"#;
    let result = engine.execute_with_params(q, &store, &params).unwrap();
    assert_eq!(result.records.len(), 1, "Should find Alice by param");
}
