//! Entertainment Graph Integration Test
//!
//! Tests the full social network + entertainment graph from the user's
//! comprehensive Cypher script — constraints, data insertion, relationships,
//! retrieval queries, updates, and deletions.

use graphmind::{GraphStore, QueryEngine};

fn setup_graph() -> (GraphStore, QueryEngine) {
    let mut store = GraphStore::new();
    let engine = QueryEngine::new();

    // ─── Block 1: Schema setup ───────────────────────────────────────
    let constraints = r#"
CREATE CONSTRAINT person_id   IF NOT EXISTS FOR (p:Person)  REQUIRE p.id  IS UNIQUE;
CREATE CONSTRAINT movie_id    IF NOT EXISTS FOR (m:Movie)   REQUIRE m.id  IS UNIQUE;
CREATE CONSTRAINT genre_name  IF NOT EXISTS FOR (g:Genre)   REQUIRE g.name IS UNIQUE;
CREATE CONSTRAINT city_name   IF NOT EXISTS FOR (c:City)    REQUIRE c.name IS UNIQUE;
CREATE CONSTRAINT company_id  IF NOT EXISTS FOR (co:Company) REQUIRE co.id IS UNIQUE;
CREATE CONSTRAINT award_id    IF NOT EXISTS FOR (a:Award)   REQUIRE a.id  IS UNIQUE;
CREATE CONSTRAINT tag_name    IF NOT EXISTS FOR (t:Tag)     REQUIRE t.name IS UNIQUE;
CREATE INDEX person_name IF NOT EXISTS FOR (p:Person) ON (p.name);
CREATE INDEX movie_year  IF NOT EXISTS FOR (m:Movie)  ON (m.year)
"#;
    engine
        .execute_mut(constraints, &mut store, "default")
        .unwrap();

    // ─── Block 2: People ─────────────────────────────────────────────
    let people = r#"
CREATE (:Person {id:'p1', name:'Amara Osei',      age:34, email:'amara@mail.com',  followers:12000}),
       (:Person {id:'p2', name:'Lena Fischer',    age:29, email:'lena@mail.com',   followers:8400}),
       (:Person {id:'p3', name:'Kenji Watanabe',  age:41, email:'kenji@mail.com',  followers:31000}),
       (:Person {id:'p4', name:'Sofia Reyes',     age:26, email:'sofia@mail.com',  followers:5500}),
       (:Person {id:'p5', name:'David Nkosi',     age:38, email:'david@mail.com',  followers:19800}),
       (:Person {id:'p6', name:'Priya Menon',     age:31, email:'priya@mail.com',  followers:7200}),
       (:Person {id:'p7', name:'Carlos Vega',     age:45, email:'carlos@mail.com', followers:22000}),
       (:Person {id:'p8', name:'Mei Lin',         age:27, email:'mei@mail.com',    followers:9300})
"#;
    engine.execute_mut(people, &mut store, "default").unwrap();

    // ─── Block 3: Movies ─────────────────────────────────────────────
    let movies = r#"
CREATE (:Movie {id:'m1', title:'Echoes of Tomorrow',  year:2021, runtime:118, rating:8.2, budget:15000000}),
       (:Movie {id:'m2', title:'The Silent Current',   year:2019, runtime:105, rating:7.6, budget:8000000}),
       (:Movie {id:'m3', title:'Neon Horizon',          year:2023, runtime:132, rating:9.1, budget:45000000}),
       (:Movie {id:'m4', title:'Dust & Stars',          year:2020, runtime:95,  rating:7.1, budget:5000000}),
       (:Movie {id:'m5', title:'Parallel Lives',        year:2022, runtime:110, rating:8.8, budget:22000000}),
       (:Movie {id:'m6', title:'The Glass Labyrinth',   year:2018, runtime:124, rating:7.9, budget:12000000})
"#;
    engine.execute_mut(movies, &mut store, "default").unwrap();

    // ─── Block 4: Genres, Cities, Companies, Tags, Awards ────────────
    engine
        .execute_mut(
            r#"
CREATE (:Genre {name:'Sci-Fi'}),   (:Genre {name:'Drama'}),
       (:Genre {name:'Thriller'}), (:Genre {name:'Mystery'})
"#,
            &mut store,
            "default",
        )
        .unwrap();

    engine
        .execute_mut(
            r#"
CREATE (:City {name:'Nairobi',  country:'Kenya',  population:4400000}),
       (:City {name:'Berlin',   country:'Germany',population:3700000}),
       (:City {name:'Tokyo',    country:'Japan',  population:13960000}),
       (:City {name:'Mumbai',   country:'India',  population:20700000})
"#,
            &mut store,
            "default",
        )
        .unwrap();

    engine
        .execute_mut(
            r#"
CREATE (:Company {id:'c1', name:'Stellar Films',  founded:2010, employees:320}),
       (:Company {id:'c2', name:'NovaCinema',     founded:2005, employees:850}),
       (:Company {id:'c3', name:'Apex Studios',   founded:2015, employees:210})
"#,
            &mut store,
            "default",
        )
        .unwrap();

    engine
        .execute_mut(
            r#"
CREATE (:Tag {name:'mind-bending'}), (:Tag {name:'visually-stunning'}),
       (:Tag {name:'slow-burn'}),    (:Tag {name:'cult-classic'}),
       (:Tag {name:'thought-provoking'})
"#,
            &mut store,
            "default",
        )
        .unwrap();

    engine
        .execute_mut(
            r#"
CREATE (:Award {id:'a1', name:'Golden Lens',       category:'Best Director',    year:2022}),
       (:Award {id:'a2', name:'Aurora Prize',       category:'Best Actress',     year:2021}),
       (:Award {id:'a3', name:'Nebula Film Award',  category:'Best Picture',     year:2023}),
       (:Award {id:'a4', name:'Meridian Award',     category:'Best Screenplay',  year:2020})
"#,
            &mut store,
            "default",
        )
        .unwrap();

    // ─── Block 5: Reviews ────────────────────────────────────────────
    engine
        .execute_mut(
            r#"
CREATE (:Review {id:'r1', score:9,   body:'A masterpiece.',     date:'2021-11-01'}),
       (:Review {id:'r2', score:7,   body:'Slow but rewarding.', date:'2019-05-14'}),
       (:Review {id:'r3', score:10,  body:'Absolutely epic.',    date:'2023-03-22'}),
       (:Review {id:'r4', score:6,   body:'Forgettable.',        date:'2020-08-09'}),
       (:Review {id:'r5', score:8,   body:'Deeply moving.',      date:'2022-07-31'}),
       (:Review {id:'r6', score:9,   body:'Criminally underrated.', date:'2018-12-05'})
"#,
            &mut store,
            "default",
        )
        .unwrap();

    // ─── Block 6: Social graph (FOLLOWS) ─────────────────────────────
    let follows = r#"
MATCH (a:Person {id:'p1'}), (b:Person {id:'p2'}) CREATE (a)-[:FOLLOWS]->(b);
MATCH (a:Person {id:'p1'}), (b:Person {id:'p3'}) CREATE (a)-[:FOLLOWS]->(b);
MATCH (a:Person {id:'p2'}), (b:Person {id:'p4'}) CREATE (a)-[:FOLLOWS]->(b);
MATCH (a:Person {id:'p3'}), (b:Person {id:'p5'}) CREATE (a)-[:FOLLOWS]->(b);
MATCH (a:Person {id:'p4'}), (b:Person {id:'p1'}) CREATE (a)-[:FOLLOWS]->(b);
MATCH (a:Person {id:'p5'}), (b:Person {id:'p7'}) CREATE (a)-[:FOLLOWS]->(b);
MATCH (a:Person {id:'p6'}), (b:Person {id:'p3'}) CREATE (a)-[:FOLLOWS]->(b);
MATCH (a:Person {id:'p7'}), (b:Person {id:'p8'}) CREATE (a)-[:FOLLOWS]->(b);
MATCH (a:Person {id:'p8'}), (b:Person {id:'p2'}) CREATE (a)-[:FOLLOWS]->(b)
"#;
    engine.execute_mut(follows, &mut store, "default").unwrap();

    // ─── Block 7: ACTED_IN ───────────────────────────────────────────
    let acted = r#"
MATCH (p:Person {id:'p1'}), (m:Movie {id:'m1'}) CREATE (p)-[:ACTED_IN {role:'Dr. Yara Cole', fee:200000}]->(m);
MATCH (p:Person {id:'p2'}), (m:Movie {id:'m1'}) CREATE (p)-[:ACTED_IN {role:'Commander Hess', fee:150000}]->(m);
MATCH (p:Person {id:'p4'}), (m:Movie {id:'m3'}) CREATE (p)-[:ACTED_IN {role:'Nova', fee:500000}]->(m);
MATCH (p:Person {id:'p5'}), (m:Movie {id:'m3'}) CREATE (p)-[:ACTED_IN {role:'The Architect', fee:620000}]->(m);
MATCH (p:Person {id:'p6'}), (m:Movie {id:'m5'}) CREATE (p)-[:ACTED_IN {role:'Leila', fee:310000}]->(m);
MATCH (p:Person {id:'p1'}), (m:Movie {id:'m6'}) CREATE (p)-[:ACTED_IN {role:'Detective Marsh', fee:180000}]->(m);
MATCH (p:Person {id:'p8'}), (m:Movie {id:'m2'}) CREATE (p)-[:ACTED_IN {role:'Jin', fee:120000}]->(m)
"#;
    engine.execute_mut(acted, &mut store, "default").unwrap();

    // ─── Block 8: DIRECTED ───────────────────────────────────────────
    let directed = r#"
MATCH (p:Person {id:'p3'}), (m:Movie {id:'m1'}) CREATE (p)-[:DIRECTED {fee:400000}]->(m);
MATCH (p:Person {id:'p7'}), (m:Movie {id:'m3'}) CREATE (p)-[:DIRECTED {fee:900000}]->(m);
MATCH (p:Person {id:'p3'}), (m:Movie {id:'m5'}) CREATE (p)-[:DIRECTED {fee:500000}]->(m);
MATCH (p:Person {id:'p5'}), (m:Movie {id:'m6'}) CREATE (p)-[:DIRECTED {fee:350000}]->(m)
"#;
    engine.execute_mut(directed, &mut store, "default").unwrap();

    // ─── Block 9: IN_GENRE ───────────────────────────────────────────
    let genres = r#"
MATCH (m:Movie {id:'m1'}), (g:Genre {name:'Sci-Fi'})   CREATE (m)-[:IN_GENRE]->(g);
MATCH (m:Movie {id:'m1'}), (g:Genre {name:'Drama'})    CREATE (m)-[:IN_GENRE]->(g);
MATCH (m:Movie {id:'m2'}), (g:Genre {name:'Mystery'})  CREATE (m)-[:IN_GENRE]->(g);
MATCH (m:Movie {id:'m3'}), (g:Genre {name:'Sci-Fi'})   CREATE (m)-[:IN_GENRE]->(g);
MATCH (m:Movie {id:'m4'}), (g:Genre {name:'Drama'})    CREATE (m)-[:IN_GENRE]->(g);
MATCH (m:Movie {id:'m5'}), (g:Genre {name:'Drama'})    CREATE (m)-[:IN_GENRE]->(g);
MATCH (m:Movie {id:'m6'}), (g:Genre {name:'Thriller'}) CREATE (m)-[:IN_GENRE]->(g);
MATCH (m:Movie {id:'m6'}), (g:Genre {name:'Mystery'})  CREATE (m)-[:IN_GENRE]->(g)
"#;
    engine.execute_mut(genres, &mut store, "default").unwrap();

    // ─── Block 10: LIVES_IN, WORKS_AT, PRODUCED_BY ───────────────────
    let locations = r#"
MATCH (p:Person {id:'p1'}), (c:City {name:'Nairobi'}) CREATE (p)-[:LIVES_IN]->(c);
MATCH (p:Person {id:'p2'}), (c:City {name:'Berlin'})  CREATE (p)-[:LIVES_IN]->(c);
MATCH (p:Person {id:'p3'}), (c:City {name:'Tokyo'})   CREATE (p)-[:LIVES_IN]->(c);
MATCH (p:Person {id:'p5'}), (c:City {name:'Nairobi'}) CREATE (p)-[:LIVES_IN]->(c);
MATCH (p:Person {id:'p6'}), (c:City {name:'Mumbai'})  CREATE (p)-[:LIVES_IN]->(c);
MATCH (p:Person {id:'p3'}), (co:Company {id:'c1'}) CREATE (p)-[:WORKS_AT {since:2015, title:'Senior Director'}]->(co);
MATCH (p:Person {id:'p7'}), (co:Company {id:'c2'}) CREATE (p)-[:WORKS_AT {since:2008, title:'Executive Producer'}]->(co);
MATCH (p:Person {id:'p5'}), (co:Company {id:'c3'}) CREATE (p)-[:WORKS_AT {since:2018, title:'Director'}]->(co);
MATCH (m:Movie {id:'m1'}), (co:Company {id:'c1'}) CREATE (m)-[:PRODUCED_BY]->(co);
MATCH (m:Movie {id:'m3'}), (co:Company {id:'c2'}) CREATE (m)-[:PRODUCED_BY]->(co);
MATCH (m:Movie {id:'m5'}), (co:Company {id:'c1'}) CREATE (m)-[:PRODUCED_BY]->(co);
MATCH (m:Movie {id:'m6'}), (co:Company {id:'c3'}) CREATE (m)-[:PRODUCED_BY]->(co)
"#;
    engine
        .execute_mut(locations, &mut store, "default")
        .unwrap();

    // ─── Block 11: Tags & Awards ─────────────────────────────────────
    let tags = r#"
MATCH (p:Person {id:'p3'}), (a:Award {id:'a1'}) CREATE (p)-[:WON {ceremony:'Berlin Film Fest'}]->(a);
MATCH (p:Person {id:'p1'}), (a:Award {id:'a2'}) CREATE (p)-[:WON {ceremony:'Aurora Gala'}]->(a);
MATCH (m:Movie {id:'m3'}), (a:Award {id:'a3'}) CREATE (m)-[:WON]->(a);
MATCH (p:Person {id:'p7'}), (a:Award {id:'a4'}) CREATE (p)-[:WON {ceremony:'Meridian Ceremony'}]->(a);
MATCH (m:Movie {id:'m1'}), (t:Tag {name:'mind-bending'})      CREATE (m)-[:TAGGED]->(t);
MATCH (m:Movie {id:'m3'}), (t:Tag {name:'visually-stunning'}) CREATE (m)-[:TAGGED]->(t);
MATCH (m:Movie {id:'m3'}), (t:Tag {name:'mind-bending'})      CREATE (m)-[:TAGGED]->(t);
MATCH (m:Movie {id:'m6'}), (t:Tag {name:'slow-burn'})         CREATE (m)-[:TAGGED]->(t);
MATCH (m:Movie {id:'m6'}), (t:Tag {name:'cult-classic'})      CREATE (m)-[:TAGGED]->(t);
MATCH (m:Movie {id:'m5'}), (t:Tag {name:'thought-provoking'}) CREATE (m)-[:TAGGED]->(t)
"#;
    engine.execute_mut(tags, &mut store, "default").unwrap();

    (store, engine)
}

// ============================================================
// Data verification
// ============================================================

#[test]
fn test_data_creation_counts() {
    let (store, engine) = setup_graph();
    // 8 persons + 6 movies + 4 genres + 4 cities + 3 companies + 5 tags + 4 awards + 6 reviews = 40
    eprintln!(
        "Nodes: {}, Edges: {}",
        store.node_count(),
        store.edge_count()
    );
    assert!(
        store.node_count() >= 36,
        "Expected at least 36 nodes, got {}",
        store.node_count()
    );
    assert!(
        store.edge_count() >= 40,
        "Expected at least 40 edges, got {}",
        store.edge_count()
    );
}

#[test]
fn test_constraint_prevents_duplicates() {
    let (mut store, engine) = setup_graph();
    let result = engine.execute_mut(
        "CREATE (:Person {id:'p1', name:'Duplicate'})",
        &mut store,
        "default",
    );
    assert!(result.is_err(), "Should fail: duplicate p1");
}

// ============================================================
// Retrieval queries
// ============================================================

#[test]
fn test_actors_and_roles() {
    let (store, engine) = setup_graph();
    let r = engine.execute(
        "MATCH (p:Person)-[r:ACTED_IN]->(m:Movie) RETURN p.name, r.role, m.title ORDER BY m.year DESC",
        &store,
    ).unwrap();
    assert!(
        r.len() >= 7,
        "Expected at least 7 ACTED_IN relationships, got {}",
        r.len()
    );
}

#[test]
fn test_director_and_cast() {
    let (store, engine) = setup_graph();
    let r = engine.execute(
        "MATCH (d:Person)-[:DIRECTED]->(m:Movie)<-[:ACTED_IN]-(a:Person) RETURN d.name, m.title, collect(a.name) AS cast",
        &store,
    ).unwrap();
    assert!(r.len() >= 1, "Should find directors with casts");
}

#[test]
fn test_top_rated_with_genres() {
    let (store, engine) = setup_graph();
    let r = engine.execute(
        "MATCH (m:Movie)-[:IN_GENRE]->(g:Genre) RETURN m.title, m.rating, collect(g.name) AS genres ORDER BY m.rating DESC LIMIT 3",
        &store,
    ).unwrap();
    assert_eq!(r.len(), 3, "Should return top 3 movies");
}

#[test]
fn test_follows_exist() {
    let (store, engine) = setup_graph();
    let r = engine
        .execute(
            "MATCH (a:Person)-[:FOLLOWS]->(b:Person) RETURN count(*)",
            &store,
        )
        .unwrap();
    assert_eq!(r.len(), 1);
}

#[test]
fn test_production_companies() {
    let (store, engine) = setup_graph();
    let r = engine.execute(
        "MATCH (m:Movie)-[:PRODUCED_BY]->(co:Company) RETURN co.name, collect(m.title) AS movies",
        &store,
    ).unwrap();
    assert!(r.len() >= 2, "Should have at least 2 production companies");
}

// ============================================================
// Updates & deletions
// ============================================================

#[test]
fn test_update_follower_count() {
    let (mut store, engine) = setup_graph();
    engine
        .execute_mut(
            "MATCH (p:Person {id:'p1'}) SET p.verified = true",
            &mut store,
            "default",
        )
        .unwrap();
    // Verify via store
    let nodes = store.get_nodes_by_label(&graphmind::Label::new("Person"));
    let p1 = nodes
        .iter()
        .find(|n| n.properties.get("id").and_then(|v| v.as_string()) == Some("p1"))
        .unwrap();
    assert_eq!(
        p1.properties.get("verified"),
        Some(&graphmind::PropertyValue::Boolean(true))
    );
}

#[test]
fn test_delete_review() {
    let (mut store, engine) = setup_graph();
    let before = store.node_count();
    engine
        .execute_mut(
            "MATCH (r:Review {id:'r4'}) DETACH DELETE r",
            &mut store,
            "default",
        )
        .unwrap();
    assert_eq!(
        store.node_count(),
        before - 1,
        "One review should be deleted"
    );
}
