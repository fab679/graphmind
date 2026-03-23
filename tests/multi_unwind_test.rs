use graphmind::{GraphStore, Label, QueryEngine};

#[test]
fn test_random_uuid() {
    let store = GraphStore::new();
    let engine = QueryEngine::new();

    let r = engine
        .execute("RETURN randomUUID() AS uuid", &store)
        .unwrap();
    let uuid = r.records[0]
        .get("uuid")
        .unwrap()
        .as_property()
        .unwrap()
        .as_string()
        .unwrap();
    eprintln!("UUID: {}", uuid);
    assert_eq!(uuid.len(), 36);
    assert_eq!(uuid.chars().nth(14), Some('4'), "Version 4");

    // Two UUIDs should differ
    let r2 = engine
        .execute("RETURN randomUUID() AS u1, randomUUID() AS u2", &store)
        .unwrap();
    let u1 = r2.records[0]
        .get("u1")
        .unwrap()
        .as_property()
        .unwrap()
        .as_string()
        .unwrap();
    let u2 = r2.records[0]
        .get("u2")
        .unwrap()
        .as_property()
        .unwrap()
        .as_string()
        .unwrap();
    assert_ne!(u1, u2);
}

#[test]
fn test_uuid_with_create() {
    let mut store = GraphStore::new();
    let engine = QueryEngine::new();

    // Use WITH to pass UUID into CREATE
    engine
        .execute_mut(
            "WITH randomUUID() AS uid CREATE (n:Person {id: uid, name: 'Alice'})",
            &mut store,
            "default",
        )
        .unwrap();

    let nodes = store.get_nodes_by_label(&Label::new("Person"));
    let id = nodes[0].properties.get("id").unwrap().as_string().unwrap();
    eprintln!("Person id: {}", id);
    assert_eq!(id.len(), 36);
}
