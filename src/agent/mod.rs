//! Agentic Enrichment
//!
//! Implements agents that can use tools to enrich the graph.

pub mod tools;

use crate::persistence::tenant::{AgentConfig, NLQConfig};
use crate::nlq::client::NLQClient;
use serde_json::Value;
use std::collections::HashMap;
use std::sync::Arc;
use thiserror::Error;
use async_trait::async_trait;

#[derive(Error, Debug)]
pub enum AgentError {
    #[error("Configuration error: {0}")]
    ConfigError(String),
    #[error("Tool error: {0}")]
    ToolError(String),
    #[error("LLM error: {0}")]
    LLMError(String),
    #[error("Execution error: {0}")]
    ExecutionError(String),
}

pub type AgentResult<T> = Result<T, AgentError>;

/// Trait for agent tools
#[async_trait]
pub trait Tool: Send + Sync {
    fn name(&self) -> &str;
    fn description(&self) -> &str;
    fn parameters(&self) -> Value;
    async fn execute(&self, args: Value) -> AgentResult<Value>;
}

/// Runtime for executing agents
pub struct AgentRuntime {
    config: AgentConfig,
    tools: HashMap<String, Arc<dyn Tool>>,
}

impl AgentRuntime {
    pub fn new(config: AgentConfig) -> Self {
        Self {
            config,
            tools: HashMap::new(),
        }
    }

    pub fn register_tool(&mut self, tool: Arc<dyn Tool>) {
        self.tools.insert(tool.name().to_string(), tool);
    }

    /// Convert AgentConfig to NLQConfig for reusing the NLQ client
    fn to_nlq_config(config: &AgentConfig) -> NLQConfig {
        NLQConfig {
            enabled: config.enabled,
            provider: config.provider.clone(),
            model: config.model.clone(),
            api_key: config.api_key.clone(),
            api_base_url: config.api_base_url.clone(),
            system_prompt: config.system_prompt.clone(),
        }
    }

    /// Process a trigger (e.g., "Enrich Company node X")
    pub async fn process_trigger(&self, prompt: &str, _context: &str) -> AgentResult<String> {
        let nlq_config = Self::to_nlq_config(&self.config);
        let client = NLQClient::new(&nlq_config)
            .map_err(|e| AgentError::ConfigError(e.to_string()))?;
        let response = client.generate_cypher(prompt).await
            .map_err(|e| AgentError::LLMError(e.to_string()))?;
        Ok(response)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::persistence::tenant::LLMProvider;

    fn mock_agent_config() -> AgentConfig {
        AgentConfig {
            enabled: true,
            provider: LLMProvider::Mock,
            model: "mock-model".to_string(),
            api_key: None,
            api_base_url: None,
            system_prompt: None,
            tools: vec![],
            policies: std::collections::HashMap::new(),
        }
    }

    #[test]
    fn test_agent_runtime_new() {
        let config = mock_agent_config();
        let runtime = AgentRuntime::new(config);
        assert!(runtime.tools.is_empty());
    }

    #[test]
    fn test_register_tool() {
        let config = mock_agent_config();
        let mut runtime = AgentRuntime::new(config);

        // Create and register WebSearchTool
        let tool = Arc::new(tools::WebSearchTool::new("test-key".to_string()));
        runtime.register_tool(tool);
        assert_eq!(runtime.tools.len(), 1);
        assert!(runtime.tools.contains_key("web_search"));
    }

    #[test]
    fn test_to_nlq_config() {
        let config = mock_agent_config();
        let nlq_config = AgentRuntime::to_nlq_config(&config);
        assert!(nlq_config.enabled);
        assert_eq!(nlq_config.provider, LLMProvider::Mock);
        assert_eq!(nlq_config.model, "mock-model");
    }

    #[tokio::test]
    async fn test_process_trigger_mock() {
        let config = mock_agent_config();
        let runtime = AgentRuntime::new(config);
        let result = runtime.process_trigger("Find all persons", "context").await;
        assert!(result.is_ok());
        let cypher = result.unwrap();
        assert!(cypher.contains("MATCH")); // Mock returns "MATCH (n) RETURN n LIMIT 10"
    }
}
