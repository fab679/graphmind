//! RemoteClient — network client for a running Graphmind server
//!
//! Connects via HTTP to the Graphmind HTTP API.

use async_trait::async_trait;
use reqwest::Client;

use crate::client::GraphmindClient;
use crate::error::{GraphmindError, GraphmindResult};
use crate::models::{QueryResult, ServerStatus};

/// Network client that connects to a running Graphmind server.
///
/// Uses HTTP transport for `/api/query` and `/api/status` endpoints.
pub struct RemoteClient {
    http_base_url: String,
    http_client: Client,
}

impl RemoteClient {
    /// Create a new RemoteClient connecting to the given HTTP base URL.
    ///
    /// # Example
    /// ```no_run
    /// # use graphmind_sdk::RemoteClient;
    /// let client = RemoteClient::new("http://localhost:8080");
    /// ```
    pub fn new(http_base_url: &str) -> Self {
        Self {
            http_base_url: http_base_url.trim_end_matches('/').to_string(),
            http_client: Client::new(),
        }
    }

    /// Execute a POST request to /api/query
    async fn post_query(&self, graph: &str, cypher: &str) -> GraphmindResult<QueryResult> {
        let url = format!("{}/api/query", self.http_base_url);
        let body = serde_json::json!({ "query": cypher, "graph": graph });

        let response = self.http_client.post(&url).json(&body).send().await?;

        if response.status().is_success() {
            let result: QueryResult = response.json().await?;
            Ok(result)
        } else {
            let error_body: serde_json::Value = response
                .json()
                .await
                .unwrap_or_else(|_| serde_json::json!({"error": "Unknown error"}));
            let msg = error_body
                .get("error")
                .and_then(|v| v.as_str())
                .unwrap_or("Unknown error")
                .to_string();
            Err(GraphmindError::QueryError(msg))
        }
    }
}

#[async_trait]
impl GraphmindClient for RemoteClient {
    async fn query(&self, graph: &str, cypher: &str) -> GraphmindResult<QueryResult> {
        self.post_query(graph, cypher).await
    }

    async fn query_readonly(&self, graph: &str, cypher: &str) -> GraphmindResult<QueryResult> {
        self.post_query(graph, cypher).await
    }

    async fn delete_graph(&self, graph: &str) -> GraphmindResult<()> {
        // The HTTP API doesn't expose GRAPH.DELETE directly.
        // We can execute a Cypher that deletes all nodes/edges.
        self.post_query(graph, "MATCH (n) DELETE n").await?;
        Ok(())
    }

    async fn list_graphs(&self) -> GraphmindResult<Vec<String>> {
        // Single-graph mode in OSS
        Ok(vec!["default".to_string()])
    }

    async fn status(&self) -> GraphmindResult<ServerStatus> {
        let url = format!("{}/api/status", self.http_base_url);
        let response = self.http_client.get(&url).send().await?;

        if response.status().is_success() {
            let status: ServerStatus = response.json().await?;
            Ok(status)
        } else {
            Err(GraphmindError::ConnectionError(format!(
                "Status endpoint returned {}",
                response.status()
            )))
        }
    }

    async fn ping(&self) -> GraphmindResult<String> {
        let status = self.status().await?;
        if status.status == "healthy" {
            Ok("PONG".to_string())
        } else {
            Err(GraphmindError::ConnectionError(format!(
                "Server unhealthy: {}",
                status.status
            )))
        }
    }

    async fn schema(&self, _graph: &str) -> GraphmindResult<String> {
        let url = format!("{}/api/schema", self.http_base_url);
        let response = self.http_client.get(&url).send().await?;

        if response.status().is_success() {
            let body: serde_json::Value = response.json().await?;
            Ok(serde_json::to_string_pretty(&body).unwrap_or_else(|_| body.to_string()))
        } else {
            Err(GraphmindError::ConnectionError(format!(
                "Schema endpoint returned {}",
                response.status()
            )))
        }
    }

    async fn explain(&self, graph: &str, cypher: &str) -> GraphmindResult<QueryResult> {
        let prefixed = if cypher.trim().to_uppercase().starts_with("EXPLAIN") {
            cypher.to_string()
        } else {
            format!("EXPLAIN {}", cypher)
        };
        self.post_query(graph, &prefixed).await
    }

    async fn profile(&self, graph: &str, cypher: &str) -> GraphmindResult<QueryResult> {
        let prefixed = if cypher.trim().to_uppercase().starts_with("PROFILE") {
            cypher.to_string()
        } else {
            format!("PROFILE {}", cypher)
        };
        self.post_query(graph, &prefixed).await
    }
}
