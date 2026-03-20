//! Multi-tenant graph store manager
//!
//! Maintains a separate GraphStore per tenant/graph name, providing
//! true data isolation between tenants.

use crate::graph::GraphStore;
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::RwLock;

/// Thread-safe multi-tenant store that holds one GraphStore per graph name.
#[derive(Clone)]
pub struct TenantStoreManager {
    stores: Arc<RwLock<HashMap<String, Arc<RwLock<GraphStore>>>>>,
}

impl TenantStoreManager {
    pub fn new() -> Self {
        let mut stores = HashMap::new();
        stores.insert(
            "default".to_string(),
            Arc::new(RwLock::new(GraphStore::new())),
        );
        Self {
            stores: Arc::new(RwLock::new(stores)),
        }
    }

    /// Create a TenantStoreManager with an existing store as the "default" tenant.
    pub fn with_default(store: Arc<RwLock<GraphStore>>) -> Self {
        let mut stores = HashMap::new();
        stores.insert("default".to_string(), store);
        Self {
            stores: Arc::new(RwLock::new(stores)),
        }
    }

    /// Get or create a store for a tenant/graph name.
    pub async fn get_store(&self, graph: &str) -> Arc<RwLock<GraphStore>> {
        // Fast path: read lock
        {
            let stores = self.stores.read().await;
            if let Some(store) = stores.get(graph) {
                return Arc::clone(store);
            }
        }
        // Slow path: write lock, create new store
        let mut stores = self.stores.write().await;
        stores
            .entry(graph.to_string())
            .or_insert_with(|| Arc::new(RwLock::new(GraphStore::new())))
            .clone()
    }

    /// List all tenant/graph names
    pub async fn list_graphs(&self) -> Vec<String> {
        let stores = self.stores.read().await;
        stores.keys().cloned().collect()
    }

    /// Delete a tenant/graph (returns true if it existed)
    pub async fn delete_graph(&self, graph: &str) -> bool {
        if graph == "default" {
            return false;
        } // protect default
        let mut stores = self.stores.write().await;
        stores.remove(graph).is_some()
    }

    /// Get stats for all tenants
    pub async fn stats(&self) -> Vec<(String, usize, usize)> {
        let stores = self.stores.read().await;
        let mut result = Vec::new();
        for (name, store) in stores.iter() {
            let s = store.read().await;
            result.push((name.clone(), s.node_count(), s.edge_count()));
        }
        result
    }
}

impl Default for TenantStoreManager {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_new_has_default() {
        let mgr = TenantStoreManager::new();
        let graphs = mgr.list_graphs().await;
        assert!(graphs.contains(&"default".to_string()));
    }

    #[tokio::test]
    async fn test_get_store_creates_on_demand() {
        let mgr = TenantStoreManager::new();
        let store = mgr.get_store("tenant_a").await;
        let guard = store.read().await;
        assert_eq!(guard.node_count(), 0);

        let graphs = mgr.list_graphs().await;
        assert!(graphs.contains(&"tenant_a".to_string()));
    }

    #[tokio::test]
    async fn test_get_store_returns_same_instance() {
        let mgr = TenantStoreManager::new();
        let s1 = mgr.get_store("mydb").await;
        let s2 = mgr.get_store("mydb").await;
        assert!(Arc::ptr_eq(&s1, &s2));
    }

    #[tokio::test]
    async fn test_tenant_isolation() {
        let mgr = TenantStoreManager::new();

        // Write to tenant_a
        {
            let store = mgr.get_store("tenant_a").await;
            let mut guard = store.write().await;
            guard.create_node("Person");
        }

        // tenant_b should be empty
        {
            let store = mgr.get_store("tenant_b").await;
            let guard = store.read().await;
            assert_eq!(guard.node_count(), 0);
        }

        // tenant_a should have 1 node
        {
            let store = mgr.get_store("tenant_a").await;
            let guard = store.read().await;
            assert_eq!(guard.node_count(), 1);
        }
    }

    #[tokio::test]
    async fn test_delete_graph() {
        let mgr = TenantStoreManager::new();
        mgr.get_store("temp").await;
        assert!(mgr.delete_graph("temp").await);
        assert!(!mgr.delete_graph("temp").await); // already gone
    }

    #[tokio::test]
    async fn test_cannot_delete_default() {
        let mgr = TenantStoreManager::new();
        assert!(!mgr.delete_graph("default").await);
    }

    #[tokio::test]
    async fn test_stats() {
        let mgr = TenantStoreManager::new();
        {
            let store = mgr.get_store("default").await;
            let mut guard = store.write().await;
            let a = guard.create_node("Person");
            let b = guard.create_node("Person");
            guard.create_edge(a, b, "KNOWS").unwrap();
        }
        let stats = mgr.stats().await;
        let default_stats = stats.iter().find(|(name, _, _)| name == "default").unwrap();
        assert_eq!(default_stats.1, 2); // 2 nodes
        assert_eq!(default_stats.2, 1); // 1 edge
    }

    #[tokio::test]
    async fn test_with_default() {
        let store = Arc::new(RwLock::new(GraphStore::new()));
        {
            let mut guard = store.write().await;
            guard.create_node("Test");
        }
        let mgr = TenantStoreManager::with_default(store);
        let default_store = mgr.get_store("default").await;
        let guard = default_store.read().await;
        assert_eq!(guard.node_count(), 1);
    }
}
