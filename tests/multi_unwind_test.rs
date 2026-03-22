#[test]
fn test_create_rebind() {
    let q = "CREATE (n:Foo) CREATE (n:Bar)-[:OWNS]->(:Dog)";
    let query = graphmind::query::parser::parse_query(q).unwrap();
    eprintln!("create_clause: {:?}", query.create_clause.is_some());
    eprintln!("create_clauses: {}", query.create_clauses.len());
    if let Some(cc) = &query.create_clause {
        for path in &cc.pattern.paths {
            eprintln!("  path: start={:?}", path.start.variable);
        }
    }
    for (i, cc) in query.create_clauses.iter().enumerate() {
        for path in &cc.pattern.paths {
            eprintln!("  clauses[{}]: start={:?}", i, path.start.variable);
        }
    }
}
