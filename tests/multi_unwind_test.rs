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
fn test_uuid_uniqueness() {
    let store = GraphStore::new();
    let engine = QueryEngine::new();

    // Generate 10 UUIDs and verify all unique
    let mut uuids = Vec::new();
    for _ in 0..10 {
        let r = engine
            .execute("RETURN randomUUID() AS uuid", &store)
            .unwrap();
        let uuid = r.records[0]
            .get("uuid")
            .unwrap()
            .as_property()
            .unwrap()
            .as_string()
            .unwrap()
            .to_string();
        assert_eq!(uuid.len(), 36, "UUID should be 36 chars");
        uuids.push(uuid);
    }
    let unique: std::collections::HashSet<_> = uuids.iter().collect();
    assert_eq!(unique.len(), 10, "All 10 UUIDs should be unique");
}
