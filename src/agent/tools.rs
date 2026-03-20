use crate::agent::{AgentError, AgentResult, Tool};
use async_trait::async_trait;
use reqwest::Client;
use serde_json::{json, Value};

pub struct WebSearchTool {
    #[allow(dead_code)]
    api_key: String, // Google Custom Search API Key (or SerpApi)
    #[allow(dead_code)]
    client: Client,
}

impl WebSearchTool {
    pub fn new(api_key: String) -> Self {
        Self {
            api_key,
            client: Client::new(),
        }
    }
}

#[async_trait]
impl Tool for WebSearchTool {
    fn name(&self) -> &str {
        "web_search"
    }

    fn description(&self) -> &str {
        "Search the web for information using Google Custom Search."
    }

    fn parameters(&self) -> Value {
        json!({
            "type": "object",
            "properties": {
                "query": {
                    "type": "string",
                    "description": "The search query"
                }
            },
            "required": ["query"]
        })
    }

    async fn execute(&self, args: Value) -> AgentResult<Value> {
        let query = args
            .get("query")
            .and_then(|v| v.as_str())
            .ok_or_else(|| AgentError::ToolError("Missing 'query' parameter".to_string()))?;

        // Mock implementation for demo/prototype to avoid needing another real API key immediately
        // In production, call: https://www.googleapis.com/customsearch/v1?key={}&cx={}&q={}

        println!("Available for search: {}", query);

        // Return dummy data
        Ok(json!({
            "results": [
                { "title": "Graphmind Graph Database", "snippet": "Graphmind is a high-performance distributed graph database..." },
                { "title": "Graph Database - Wikipedia", "snippet": "A graph database is a database that uses graph structures..." }
            ]
        }))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_web_search_tool_name() {
        let tool = WebSearchTool::new("test-key".to_string());
        assert_eq!(tool.name(), "web_search");
    }

    #[test]
    fn test_web_search_tool_description() {
        let tool = WebSearchTool::new("test-key".to_string());
        assert!(!tool.description().is_empty());
    }

    #[test]
    fn test_web_search_tool_parameters() {
        let tool = WebSearchTool::new("test-key".to_string());
        let params = tool.parameters();
        assert_eq!(params["type"], "object");
        assert!(params["properties"]["query"].is_object());
    }

    #[tokio::test]
    async fn test_web_search_tool_execute() {
        let tool = WebSearchTool::new("test-key".to_string());
        let args = json!({"query": "graph database"});
        let result = tool.execute(args).await;
        assert!(result.is_ok());
        let value = result.unwrap();
        assert!(value["results"].is_array());
    }

    #[tokio::test]
    async fn test_web_search_tool_missing_query() {
        let tool = WebSearchTool::new("test-key".to_string());
        let args = json!({});
        let result = tool.execute(args).await;
        assert!(result.is_err());
    }
}
