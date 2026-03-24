use graphmind::graph::{Label, PropertyValue};
use graphmind::{GraphStore, QueryEngine};

#[test]
fn test_create_with_random_uuid() {
    let mut store = GraphStore::new();
    let engine = QueryEngine::new();

    let q = r#"CREATE (p:Person {name: "fabisch", id: randomUUID()})"#;
    engine.execute_mut(q, &mut store, "default").unwrap();

    let persons = store.get_nodes_by_label(&Label::new("Person"));
    assert_eq!(persons.len(), 1, "Should create 1 Person");

    eprintln!("Person properties: {:?}", persons[0].properties);

    // Check name
    assert_eq!(
        persons[0].properties.get("name"),
        Some(&PropertyValue::String("fabisch".to_string())),
        "name should be set"
    );

    // Check id exists and is a UUID string
    let id = persons[0].properties.get("id");
    eprintln!("id value: {:?}", id);
    match id {
        Some(PropertyValue::String(s)) => {
            assert_eq!(s.len(), 36, "UUID should be 36 chars");
            assert_eq!(s.chars().filter(|c| *c == '-').count(), 4);
            eprintln!("UUID: {}", s);
        }
        Some(PropertyValue::Null) => {
            panic!("id is Null — randomUUID() was not evaluated!");
        }
        None => {
            panic!("id property is missing entirely!");
        }
        other => {
            panic!("id has unexpected type: {:?}", other);
        }
    }
}
