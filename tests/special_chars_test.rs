use graphmind::graph::{Label, PropertyValue};
use graphmind::{GraphStore, QueryEngine};

// === Semicolons inside strings ===

#[test]
fn test_semicolon_in_double_quoted_string() {
    let mut store = GraphStore::new();
    let engine = QueryEngine::new();

    let q = r#"CREATE (n:Note {text: "hello; world"})"#;
    engine.execute_mut(q, &mut store, "default").unwrap();

    let nodes = store.get_nodes_by_label(&Label::new("Note"));
    assert_eq!(nodes.len(), 1);
    assert_eq!(
        nodes[0].properties.get("text"),
        Some(&PropertyValue::String("hello; world".to_string()))
    );
}

#[test]
fn test_semicolon_in_single_quoted_string() {
    let mut store = GraphStore::new();
    let engine = QueryEngine::new();

    let q = "CREATE (n:Note {text: 'hello; world'})";
    engine.execute_mut(q, &mut store, "default").unwrap();

    let nodes = store.get_nodes_by_label(&Label::new("Note"));
    assert_eq!(nodes.len(), 1);
    assert_eq!(
        nodes[0].properties.get("text"),
        Some(&PropertyValue::String("hello; world".to_string()))
    );
}

#[test]
fn test_semicolon_split_respects_strings() {
    // Two statements separated by ; but first has ; inside a string
    let mut store = GraphStore::new();
    let engine = QueryEngine::new();

    let q = r#"CREATE (a:A {val: "x;y"}); CREATE (b:B {val: "z"})"#;
    engine.execute_mut(q, &mut store, "default").unwrap();

    assert_eq!(store.get_nodes_by_label(&Label::new("A")).len(), 1);
    assert_eq!(store.get_nodes_by_label(&Label::new("B")).len(), 1);
    assert_eq!(
        store.get_nodes_by_label(&Label::new("A"))[0]
            .properties
            .get("val"),
        Some(&PropertyValue::String("x;y".to_string()))
    );
}

// === Escape sequences ===

#[test]
fn test_newline_escape_in_string() {
    let mut store = GraphStore::new();
    let engine = QueryEngine::new();

    let q = r#"CREATE (n:Note {text: "line1\nline2"})"#;
    engine.execute_mut(q, &mut store, "default").unwrap();

    let nodes = store.get_nodes_by_label(&Label::new("Note"));
    assert_eq!(nodes.len(), 1);
    assert_eq!(
        nodes[0].properties.get("text"),
        Some(&PropertyValue::String("line1\nline2".to_string()))
    );
}

#[test]
fn test_tab_escape_in_string() {
    let mut store = GraphStore::new();
    let engine = QueryEngine::new();

    let q = r#"CREATE (n:Note {text: "col1\tcol2"})"#;
    engine.execute_mut(q, &mut store, "default").unwrap();

    let nodes = store.get_nodes_by_label(&Label::new("Note"));
    assert_eq!(nodes.len(), 1);
    assert_eq!(
        nodes[0].properties.get("text"),
        Some(&PropertyValue::String("col1\tcol2".to_string()))
    );
}

#[test]
fn test_escaped_double_quote_in_double_quoted_string() {
    let mut store = GraphStore::new();
    let engine = QueryEngine::new();

    let q = r#"CREATE (n:Note {text: "she said \"hello\""})"#;
    engine.execute_mut(q, &mut store, "default").unwrap();

    let nodes = store.get_nodes_by_label(&Label::new("Note"));
    assert_eq!(nodes.len(), 1);
    assert_eq!(
        nodes[0].properties.get("text"),
        Some(&PropertyValue::String("she said \"hello\"".to_string()))
    );
}

#[test]
fn test_escaped_single_quote_in_single_quoted_string() {
    let mut store = GraphStore::new();
    let engine = QueryEngine::new();

    let q = r"CREATE (n:Note {text: 'it\'s fine'})";
    engine.execute_mut(q, &mut store, "default").unwrap();

    let nodes = store.get_nodes_by_label(&Label::new("Note"));
    assert_eq!(nodes.len(), 1);
    assert_eq!(
        nodes[0].properties.get("text"),
        Some(&PropertyValue::String("it's fine".to_string()))
    );
}

#[test]
fn test_backslash_in_string() {
    let mut store = GraphStore::new();
    let engine = QueryEngine::new();

    let q = r#"CREATE (n:Note {path: "C:\\Users\\admin"})"#;
    engine.execute_mut(q, &mut store, "default").unwrap();

    let nodes = store.get_nodes_by_label(&Label::new("Note"));
    assert_eq!(nodes.len(), 1);
    assert_eq!(
        nodes[0].properties.get("path"),
        Some(&PropertyValue::String("C:\\Users\\admin".to_string()))
    );
}

// === Special characters in strings ===

#[test]
fn test_curly_braces_in_string() {
    let mut store = GraphStore::new();
    let engine = QueryEngine::new();

    let q = r#"CREATE (n:Note {json: "{\"key\": \"value\"}"})"#;
    engine.execute_mut(q, &mut store, "default").unwrap();

    let nodes = store.get_nodes_by_label(&Label::new("Note"));
    assert_eq!(nodes.len(), 1);
    let text = match nodes[0].properties.get("json") {
        Some(PropertyValue::String(s)) => s.clone(),
        _ => panic!("Expected string"),
    };
    assert!(text.contains("key"));
}

#[test]
fn test_parentheses_in_string() {
    let mut store = GraphStore::new();
    let engine = QueryEngine::new();

    let q = r#"CREATE (n:Note {text: "func(arg1, arg2)"})"#;
    engine.execute_mut(q, &mut store, "default").unwrap();

    let nodes = store.get_nodes_by_label(&Label::new("Note"));
    assert_eq!(nodes.len(), 1);
    assert_eq!(
        nodes[0].properties.get("text"),
        Some(&PropertyValue::String("func(arg1, arg2)".to_string()))
    );
}

#[test]
fn test_brackets_and_arrows_in_string() {
    let mut store = GraphStore::new();
    let engine = QueryEngine::new();

    let q = r#"CREATE (n:Note {text: "list[0] -> value <-- back"})"#;
    engine.execute_mut(q, &mut store, "default").unwrap();

    let nodes = store.get_nodes_by_label(&Label::new("Note"));
    assert_eq!(nodes.len(), 1);
    assert_eq!(
        nodes[0].properties.get("text"),
        Some(&PropertyValue::String(
            "list[0] -> value <-- back".to_string()
        ))
    );
}

#[test]
fn test_unicode_in_string() {
    let mut store = GraphStore::new();
    let engine = QueryEngine::new();

    // Direct UTF-8 characters
    let q = r#"CREATE (n:Note {text: "café résumé naïve"})"#;
    engine.execute_mut(q, &mut store, "default").unwrap();

    let nodes = store.get_nodes_by_label(&Label::new("Note"));
    assert_eq!(nodes.len(), 1);
    assert_eq!(
        nodes[0].properties.get("text"),
        Some(&PropertyValue::String("café résumé naïve".to_string()))
    );
}

#[test]
fn test_emoji_in_string() {
    let mut store = GraphStore::new();
    let engine = QueryEngine::new();

    let q = "CREATE (n:Note {text: \"hello 🌍🚀\"})";
    engine.execute_mut(q, &mut store, "default").unwrap();

    let nodes = store.get_nodes_by_label(&Label::new("Note"));
    assert_eq!(nodes.len(), 1);
    let text = match nodes[0].properties.get("text") {
        Some(PropertyValue::String(s)) => s.clone(),
        _ => panic!("Expected string"),
    };
    assert!(text.contains("🌍"));
}

#[test]
fn test_multiline_string_content() {
    let mut store = GraphStore::new();
    let engine = QueryEngine::new();

    // Newlines via escape sequences
    let q = r#"CREATE (n:Note {text: "line1\nline2\nline3"})"#;
    engine.execute_mut(q, &mut store, "default").unwrap();

    let nodes = store.get_nodes_by_label(&Label::new("Note"));
    assert_eq!(nodes.len(), 1);
    let text = match nodes[0].properties.get("text") {
        Some(PropertyValue::String(s)) => s.clone(),
        _ => panic!("Expected string"),
    };
    assert_eq!(text.lines().count(), 3);
}

// === Expression properties: randomUUID() in CREATE ===

#[test]
fn test_random_uuid_in_create_via_multipart() {
    let mut store = GraphStore::new();
    let engine = QueryEngine::new();

    // Use multi-part pattern so expression properties are evaluated
    let q = r#"CREATE (root:Root {id: 1})
WITH root
CREATE (n:Person {id: randomUUID(), name: "Alice"})"#;

    engine.execute_mut(q, &mut store, "default").unwrap();

    let persons = store.get_nodes_by_label(&Label::new("Person"));
    assert_eq!(persons.len(), 1);

    let id_val = persons[0].properties.get("id");
    eprintln!("Person id = {:?}", id_val);

    // id should be a non-null UUID string
    match id_val {
        Some(PropertyValue::String(s)) => {
            assert!(!s.is_empty(), "UUID should not be empty");
            // UUID v4 format: 8-4-4-4-12 hex chars
            assert_eq!(s.len(), 36, "UUID should be 36 chars: {}", s);
            assert_eq!(
                s.chars().filter(|c| *c == '-').count(),
                4,
                "UUID should have 4 dashes"
            );
        }
        other => panic!("Expected UUID string, got: {:?}", other),
    }

    // name should be set normally
    assert_eq!(
        persons[0].properties.get("name"),
        Some(&PropertyValue::String("Alice".to_string()))
    );
}

// === Expression properties: randomUUID() in MATCH (should NOT work — MATCH uses static matching) ===

#[test]
fn test_random_uuid_in_match_is_static_not_dynamic() {
    // MATCH (c:Person {id: randomUUID()}) would need to match an existing node
    // whose id property equals a freshly generated UUID — which is always different.
    // This should match NOTHING (not crash), because expression_properties in NodePattern
    // only affect CREATE/MERGE, not MATCH.
    let mut store = GraphStore::new();
    let engine = QueryEngine::new();

    engine
        .execute_mut(
            r#"CREATE (p:Person {id: "fixed-uuid", name: "Alice"})"#,
            &mut store,
            "default",
        )
        .unwrap();

    // This MATCH should return 0 results — randomUUID() generates a new UUID each time,
    // which won't match "fixed-uuid"
    let result = engine.execute(
        r#"MATCH (c:Person {id: randomUUID()}) RETURN c.name"#,
        &store,
    );

    // Should either return empty results or parse error — NOT crash
    match result {
        Ok(batch) => {
            // If it parses, it should match nothing
            assert_eq!(
                batch.records.len(),
                0,
                "randomUUID() in MATCH should match nothing"
            );
        }
        Err(_) => {
            // Parse error is also acceptable — randomUUID() in MATCH property map
            // is not standard Cypher
        }
    }
}
