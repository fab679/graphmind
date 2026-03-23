#[test]
fn test_new_constraint_syntax() {
    let mut store = graphmind::GraphStore::new();
    let engine = graphmind::QueryEngine::new();

    // New syntax: CREATE CONSTRAINT name IF NOT EXISTS FOR (n:Label) REQUIRE n.prop IS UNIQUE
    let r = engine.execute_mut(
        "CREATE CONSTRAINT person_id IF NOT EXISTS FOR (p:Person) REQUIRE p.id IS UNIQUE",
        &mut store,
        "default",
    );
    match &r {
        Ok(_) => eprintln!("New constraint syntax: OK"),
        Err(e) => eprintln!("New constraint syntax ERROR: {}", e),
    }
    assert!(r.is_ok(), "New constraint syntax should work");
}

#[test]
fn test_old_constraint_syntax() {
    let mut store = graphmind::GraphStore::new();
    let engine = graphmind::QueryEngine::new();

    // Old syntax: CREATE CONSTRAINT ON (n:Label) ASSERT n.prop IS UNIQUE
    let r = engine.execute_mut(
        "CREATE CONSTRAINT ON (p:Person) ASSERT p.id IS UNIQUE",
        &mut store,
        "default",
    );
    assert!(r.is_ok(), "Old constraint syntax should still work");
}

#[test]
fn test_new_index_syntax() {
    let mut store = graphmind::GraphStore::new();
    let engine = graphmind::QueryEngine::new();

    // New syntax: CREATE INDEX name IF NOT EXISTS FOR (n:Label) ON (n.prop)
    let r = engine.execute_mut(
        "CREATE INDEX person_name IF NOT EXISTS FOR (p:Person) ON (p.name)",
        &mut store,
        "default",
    );
    match &r {
        Ok(_) => eprintln!("New index syntax: OK"),
        Err(e) => eprintln!("New index syntax ERROR: {}", e),
    }
    assert!(r.is_ok(), "New index syntax should work");
}

#[test]
fn test_old_index_syntax() {
    let mut store = graphmind::GraphStore::new();
    let engine = graphmind::QueryEngine::new();

    // Old syntax: CREATE INDEX ON :Label(prop)
    let r = engine.execute_mut("CREATE INDEX ON :Person(name)", &mut store, "default");
    assert!(r.is_ok(), "Old index syntax should still work");
}
