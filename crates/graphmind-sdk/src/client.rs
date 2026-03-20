//! GraphmindClient trait — the unified interface for embedded and remote modes

use crate::error::GraphmindResult;
use crate::models::{QueryResult, ServerStatus};
use async_trait::async_trait;

/// Unified client interface for the Graphmind graph database.
///
/// Implemented by:
/// - `EmbeddedClient` — in-process, no network (for examples, tests, embedded use)
/// - `RemoteClient` — connects to a running Graphmind server via HTTP
#[async_trait]
pub trait GraphmindClient: Send + Sync {
    /// Execute a read-write Cypher query
    async fn query(&self, graph: &str, cypher: &str) -> GraphmindResult<QueryResult>;

    /// Execute a read-only Cypher query
    async fn query_readonly(&self, graph: &str, cypher: &str) -> GraphmindResult<QueryResult>;

    /// Delete a graph
    async fn delete_graph(&self, graph: &str) -> GraphmindResult<()>;

    /// List all graphs
    async fn list_graphs(&self) -> GraphmindResult<Vec<String>>;

    /// Get server status for a specific graph
    async fn status(&self, graph: &str) -> GraphmindResult<ServerStatus>;

    /// Ping the server
    async fn ping(&self) -> GraphmindResult<String>;

    /// Return a schema summary (node types, edge types, counts)
    async fn schema(&self, graph: &str) -> GraphmindResult<String>;

    /// Return the EXPLAIN plan for a Cypher query without executing it
    async fn explain(&self, graph: &str, cypher: &str) -> GraphmindResult<QueryResult>;

    /// Execute a Cypher query with PROFILE instrumentation
    async fn profile(&self, graph: &str, cypher: &str) -> GraphmindResult<QueryResult>;
}
