//! Authentication module for Graphmind
//!
//! Simple token-based authentication for RESP and HTTP servers.
//! Configure via config file or environment variable GRAPHMIND_AUTH_TOKEN.

use std::collections::HashSet;
use std::sync::Arc;

#[derive(Clone, Debug)]
pub struct AuthConfig {
    pub enabled: bool,
    pub tokens: HashSet<String>,
}

impl AuthConfig {
    pub fn new(enabled: bool, tokens: Vec<String>) -> Self {
        Self {
            enabled,
            tokens: tokens.into_iter().collect(),
        }
    }

    pub fn disabled() -> Self {
        Self {
            enabled: false,
            tokens: HashSet::new(),
        }
    }

    /// Create an AuthConfig from the GRAPHMIND_AUTH_TOKEN environment variable.
    /// If the variable is set, auth is enabled with that single token.
    /// If unset, auth is disabled.
    pub fn from_env() -> Self {
        if let Ok(token) = std::env::var("GRAPHMIND_AUTH_TOKEN") {
            if !token.is_empty() {
                return Self::new(true, vec![token]);
            }
        }
        Self::disabled()
    }

    /// Check if a token is valid. Returns true if auth is disabled or token matches.
    pub fn validate(&self, token: &str) -> bool {
        if !self.enabled {
            return true;
        }
        self.tokens.contains(token)
    }

    /// Check if auth is required
    pub fn is_required(&self) -> bool {
        self.enabled
    }
}

/// Shared auth config (thread-safe)
pub type SharedAuthConfig = Arc<AuthConfig>;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_auth_disabled() {
        let config = AuthConfig::disabled();
        assert!(!config.enabled);
        assert!(!config.is_required());
        assert!(config.validate("anything"));
        assert!(config.validate(""));
    }

    #[test]
    fn test_auth_enabled_valid_token() {
        let config = AuthConfig::new(true, vec!["secret123".to_string()]);
        assert!(config.enabled);
        assert!(config.is_required());
        assert!(config.validate("secret123"));
    }

    #[test]
    fn test_auth_enabled_invalid_token() {
        let config = AuthConfig::new(true, vec!["secret123".to_string()]);
        assert!(!config.validate("wrong"));
        assert!(!config.validate(""));
    }

    #[test]
    fn test_auth_multiple_tokens() {
        let config = AuthConfig::new(true, vec!["token_a".to_string(), "token_b".to_string()]);
        assert!(config.validate("token_a"));
        assert!(config.validate("token_b"));
        assert!(!config.validate("token_c"));
    }

    #[test]
    fn test_auth_from_env_unset() {
        // When env var is not set, auth should be disabled
        // (We can't reliably test this if the var happens to be set,
        // but in normal test environments it won't be.)
        std::env::remove_var("GRAPHMIND_AUTH_TOKEN");
        let config = AuthConfig::from_env();
        assert!(!config.enabled);
    }

    #[test]
    fn test_shared_auth_config() {
        let config = Arc::new(AuthConfig::new(true, vec!["tok".to_string()]));
        let shared: SharedAuthConfig = config;
        assert!(shared.validate("tok"));
        assert!(!shared.validate("bad"));
    }
}
