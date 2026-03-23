//! Multi-tenancy isolation tests
//!
//! Tests that TenantStoreManager provides full data isolation between tenants,
//! auto-creates tenants on access, protects the default tenant from deletion,
//! and provides correct statistics.

use graphmind::tenant_store::TenantStoreManager;
use graphmind::QueryEngine;

// ---------------------------------------------------------------------------
// Data isolation
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_tenant_data_isolation() {
    let mgr = TenantStoreManager::new();
    let engine = QueryEngine::new();

    // Create data in tenant A
    {
        let store = mgr.get_store("tenant_a").await;
        let mut guard = store.write().await;
        engine
            .execute_mut("CREATE (n:Person {name: 'Alice'})", &mut guard, "tenant_a")
            .unwrap();
    }

    // Query tenant B -- should be empty
    {
        let store = mgr.get_store("tenant_b").await;
        let guard = store.read().await;
        let result = engine
            .execute("MATCH (n:Person) RETURN count(n) AS cnt", &guard)
            .unwrap();
        let count = result.records[0]
            .get("cnt")
            .unwrap()
            .as_property()
            .unwrap()
            .as_integer()
            .unwrap();
        assert_eq!(count, 0, "Tenant B should have no data");
    }

    // Query tenant A -- should have data
    {
        let store = mgr.get_store("tenant_a").await;
        let guard = store.read().await;
        let result = engine
            .execute("MATCH (n:Person) RETURN count(n) AS cnt", &guard)
            .unwrap();
        let count = result.records[0]
            .get("cnt")
            .unwrap()
            .as_property()
            .unwrap()
            .as_integer()
            .unwrap();
        assert_eq!(count, 1, "Tenant A should have 1 person");
    }
}

#[tokio::test]
async fn test_tenant_isolation_edges() {
    let mgr = TenantStoreManager::new();
    let engine = QueryEngine::new();

    // Create nodes and edges in tenant X
    {
        let store = mgr.get_store("tenant_x").await;
        let mut guard = store.write().await;
        engine
            .execute_mut("CREATE (a:Node {id: 1})", &mut guard, "tenant_x")
            .unwrap();
        engine
            .execute_mut("CREATE (b:Node {id: 2})", &mut guard, "tenant_x")
            .unwrap();
        engine
            .execute_mut(
                "MATCH (a:Node {id: 1}), (b:Node {id: 2}) CREATE (a)-[:LINK]->(b)",
                &mut guard,
                "tenant_x",
            )
            .unwrap();
    }

    // Tenant Y should have no edges
    {
        let store = mgr.get_store("tenant_y").await;
        let guard = store.read().await;
        assert_eq!(guard.node_count(), 0);
        assert_eq!(guard.edge_count(), 0);
    }

    // Tenant X should have 2 nodes and 1 edge
    {
        let store = mgr.get_store("tenant_x").await;
        let guard = store.read().await;
        assert_eq!(guard.node_count(), 2);
        assert_eq!(guard.edge_count(), 1);
    }
}

// ---------------------------------------------------------------------------
// Auto-creation and listing
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_tenant_auto_creation() {
    let mgr = TenantStoreManager::new();
    let graphs = mgr.list_graphs().await;
    assert!(graphs.contains(&"default".to_string()));

    // Access new tenant -- auto-creates
    let _store = mgr.get_store("new_tenant").await;
    let graphs = mgr.list_graphs().await;
    assert!(graphs.contains(&"new_tenant".to_string()));
}

#[tokio::test]
async fn test_same_store_returned() {
    let mgr = TenantStoreManager::new();
    let s1 = mgr.get_store("mydb").await;
    let s2 = mgr.get_store("mydb").await;
    assert!(std::sync::Arc::ptr_eq(&s1, &s2));
}

// ---------------------------------------------------------------------------
// Deletion
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_tenant_deletion() {
    let mgr = TenantStoreManager::new();
    let _store = mgr.get_store("to_delete").await;
    assert!(mgr.delete_graph("to_delete").await);
    let graphs = mgr.list_graphs().await;
    assert!(!graphs.contains(&"to_delete".to_string()));
}

#[tokio::test]
async fn test_default_tenant_protected() {
    let mgr = TenantStoreManager::new();
    assert!(
        !mgr.delete_graph("default").await,
        "Should not delete default"
    );
}

#[tokio::test]
async fn test_delete_nonexistent_tenant() {
    let mgr = TenantStoreManager::new();
    assert!(
        !mgr.delete_graph("does_not_exist").await,
        "Deleting nonexistent tenant should return false"
    );
}

// ---------------------------------------------------------------------------
// Stats
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_tenant_stats() {
    let mgr = TenantStoreManager::new();

    // Add data to default tenant
    {
        let store = mgr.get_store("default").await;
        let mut guard = store.write().await;
        let a = guard.create_node("Person");
        let b = guard.create_node("Person");
        guard.create_edge(a, b, "KNOWS").unwrap();
    }

    // Add data to another tenant
    {
        let store = mgr.get_store("stats_test").await;
        let mut guard = store.write().await;
        guard.create_node("Item");
    }

    let stats = mgr.stats().await;
    let default_stats = stats.iter().find(|(name, _, _)| name == "default").unwrap();
    assert_eq!(default_stats.1, 2); // 2 nodes
    assert_eq!(default_stats.2, 1); // 1 edge

    let other_stats = stats
        .iter()
        .find(|(name, _, _)| name == "stats_test")
        .unwrap();
    assert_eq!(other_stats.1, 1); // 1 node
    assert_eq!(other_stats.2, 0); // 0 edges
}

// ---------------------------------------------------------------------------
// Multiple tenants with independent mutations
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_multiple_tenants_independent_writes() {
    let mgr = TenantStoreManager::new();
    let engine = QueryEngine::new();

    // Create 5 tenants, each with a different number of nodes
    for i in 1..=5 {
        let tenant = format!("tenant_{}", i);
        let store = mgr.get_store(&tenant).await;
        let mut guard = store.write().await;
        for j in 0..i {
            engine
                .execute_mut(
                    &format!("CREATE (n:Item {{idx: {}}})", j),
                    &mut guard,
                    &tenant,
                )
                .unwrap();
        }
    }

    // Verify each tenant has the correct count
    for i in 1..=5 {
        let tenant = format!("tenant_{}", i);
        let store = mgr.get_store(&tenant).await;
        let guard = store.read().await;
        assert_eq!(
            guard.node_count(),
            i,
            "tenant_{} should have {} nodes",
            i,
            i
        );
    }
}
