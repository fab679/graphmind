//! Authentication module for Graphmind
//!
//! Supports both token-based and username/password authentication with roles.
//! Configure via environment variables:
//!   - GRAPHMIND_AUTH_TOKEN: Legacy token-based auth
//!   - GRAPHMIND_ADMIN_USER + GRAPHMIND_ADMIN_PASSWORD: Username/password auth

use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::Arc;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum Role {
    Admin,
    ReadWrite,
    ReadOnly,
}

impl Role {
    pub fn can_write(&self) -> bool {
        matches!(self, Role::Admin | Role::ReadWrite)
    }
    pub fn can_admin(&self) -> bool {
        matches!(self, Role::Admin)
    }
}

impl std::fmt::Display for Role {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Role::Admin => write!(f, "Admin"),
            Role::ReadWrite => write!(f, "ReadWrite"),
            Role::ReadOnly => write!(f, "ReadOnly"),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct User {
    pub username: String,
    pub password_hash: String,
    pub role: Role,
}

#[derive(Clone, Debug)]
pub struct AuthManager {
    enabled: bool,
    users: HashMap<String, User>,    // username -> User
    tokens: HashMap<String, String>, // token -> username (for backward compat)
}

impl AuthManager {
    pub fn new() -> Self {
        Self {
            enabled: false,
            users: HashMap::new(),
            tokens: HashMap::new(),
        }
    }

    /// Create a disabled auth manager (backward compat alias for new())
    pub fn disabled() -> Self {
        Self::new()
    }

    /// Create from environment variables. Sets up auth if GRAPHMIND_AUTH_TOKEN
    /// or GRAPHMIND_ADMIN_USER is set.
    pub fn from_env() -> Self {
        let mut mgr = Self::new();

        // Legacy token auth
        if let Ok(token) = std::env::var("GRAPHMIND_AUTH_TOKEN") {
            if !token.is_empty() {
                mgr.enabled = true;
                mgr.tokens.insert(token, "admin".to_string());
                // Create implicit admin user for token
                mgr.users.insert(
                    "admin".to_string(),
                    User {
                        username: "admin".to_string(),
                        password_hash: String::new(), // token-only, no password
                        role: Role::Admin,
                    },
                );
            }
        }

        // Username/password auth
        if let Ok(admin_user) = std::env::var("GRAPHMIND_ADMIN_USER") {
            if !admin_user.is_empty() {
                let admin_pass = std::env::var("GRAPHMIND_ADMIN_PASSWORD").unwrap_or_default();
                mgr.enabled = true;
                mgr.users.insert(
                    admin_user.clone(),
                    User {
                        username: admin_user,
                        password_hash: Self::hash_password(&admin_pass),
                        role: Role::Admin,
                    },
                );
            }
        }

        mgr
    }

    pub fn is_enabled(&self) -> bool {
        self.enabled
    }

    /// Check if auth is required (alias for is_enabled, backward compat)
    pub fn is_required(&self) -> bool {
        self.enabled
    }

    /// Validate a Bearer token. Returns the role if valid.
    pub fn validate_token(&self, token: &str) -> Option<Role> {
        if !self.enabled {
            return Some(Role::Admin);
        }
        self.tokens
            .get(token)
            .and_then(|username| self.users.get(username))
            .map(|u| u.role)
    }

    /// Validate username/password. Returns the role if valid.
    pub fn validate_credentials(&self, username: &str, password: &str) -> Option<Role> {
        if !self.enabled {
            return Some(Role::Admin);
        }
        self.users
            .get(username)
            .filter(|u| {
                !u.password_hash.is_empty() && u.password_hash == Self::hash_password(password)
            })
            .map(|u| u.role)
    }

    /// Validate either token or credentials from an Authorization header value.
    /// Supports "Bearer <token>" and "Basic <base64(user:pass)>".
    pub fn validate(&self, auth_header: &str) -> Option<Role> {
        if !self.enabled {
            return Some(Role::Admin);
        }

        // Bearer token
        if let Some(token) = auth_header.strip_prefix("Bearer ") {
            return self.validate_token(token.trim());
        }

        // Basic auth (base64 encoded username:password)
        if let Some(encoded) = auth_header.strip_prefix("Basic ") {
            if let Ok(decoded) = base64_decode(encoded.trim()) {
                if let Some((user, pass)) = decoded.split_once(':') {
                    return self.validate_credentials(user, pass);
                }
            }
        }

        None
    }

    /// Legacy validate for a bare token string (backward compat with RESP AUTH <token>)
    pub fn validate_bare_token(&self, token: &str) -> bool {
        if !self.enabled {
            return true;
        }
        self.tokens.contains_key(token)
    }

    /// Add a user (admin operation)
    pub fn add_user(&mut self, username: String, password: String, role: Role) {
        self.users.insert(
            username.clone(),
            User {
                username,
                password_hash: Self::hash_password(&password),
                role,
            },
        );
        self.enabled = true;
    }

    /// List users (admin operation)
    pub fn list_users(&self) -> Vec<(&str, Role)> {
        self.users
            .values()
            .map(|u| (u.username.as_str(), u.role))
            .collect()
    }

    /// Remove a user
    pub fn remove_user(&mut self, username: &str) -> bool {
        self.users.remove(username).is_some()
    }

    /// Simple password hashing using DefaultHasher with a salt prefix
    fn hash_password(password: &str) -> String {
        use std::collections::hash_map::DefaultHasher;
        use std::hash::{Hash, Hasher};
        let mut hasher = DefaultHasher::new();
        format!("graphmind:{}", password).hash(&mut hasher);
        format!("{:016x}", hasher.finish())
    }
}

// Backward-compatible type aliases
pub type AuthConfig = AuthManager;
pub type SharedAuthConfig = Arc<AuthManager>;
pub type SharedAuthManager = Arc<AuthManager>;

/// Decode a base64 string into a UTF-8 string.
fn base64_decode(input: &str) -> Result<String, ()> {
    let bytes = base64_decode_bytes(input)?;
    String::from_utf8(bytes).map_err(|_| ())
}

/// Minimal base64 decoder (no external dependency needed).
fn base64_decode_bytes(input: &str) -> Result<Vec<u8>, ()> {
    let input = input.trim_end_matches('=');
    let mut output = Vec::new();
    let mut buf: u32 = 0;
    let mut bits: u32 = 0;
    for c in input.chars() {
        let val = match c {
            'A'..='Z' => c as u32 - 'A' as u32,
            'a'..='z' => c as u32 - 'a' as u32 + 26,
            '0'..='9' => c as u32 - '0' as u32 + 52,
            '+' => 62,
            '/' => 63,
            _ => return Err(()),
        };
        buf = (buf << 6) | val;
        bits += 6;
        if bits >= 8 {
            bits -= 8;
            output.push((buf >> bits) as u8);
            buf &= (1 << bits) - 1;
        }
    }
    Ok(output)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_auth_disabled() {
        let config = AuthManager::new();
        assert!(!config.is_enabled());
        assert!(!config.is_required());
        assert!(config.validate("Bearer anything").is_some());
        assert!(config.validate_token("anything").is_some());
        assert!(config.validate_credentials("anyone", "pass").is_some());
    }

    #[test]
    fn test_auth_disabled_alias() {
        let config = AuthConfig::new();
        assert!(!config.is_enabled());
    }

    #[test]
    fn test_role_permissions() {
        assert!(Role::Admin.can_write());
        assert!(Role::Admin.can_admin());
        assert!(Role::ReadWrite.can_write());
        assert!(!Role::ReadWrite.can_admin());
        assert!(!Role::ReadOnly.can_write());
        assert!(!Role::ReadOnly.can_admin());
    }

    #[test]
    fn test_role_display() {
        assert_eq!(format!("{}", Role::Admin), "Admin");
        assert_eq!(format!("{}", Role::ReadWrite), "ReadWrite");
        assert_eq!(format!("{}", Role::ReadOnly), "ReadOnly");
    }

    #[test]
    fn test_add_user_and_validate() {
        let mut mgr = AuthManager::new();
        mgr.add_user("alice".to_string(), "secret123".to_string(), Role::Admin);

        assert!(mgr.is_enabled());
        assert_eq!(
            mgr.validate_credentials("alice", "secret123"),
            Some(Role::Admin)
        );
        assert!(mgr.validate_credentials("alice", "wrong").is_none());
        assert!(mgr.validate_credentials("bob", "secret123").is_none());
    }

    #[test]
    fn test_validate_basic_auth() {
        let mut mgr = AuthManager::new();
        mgr.add_user("alice".to_string(), "pass".to_string(), Role::ReadWrite);

        // "alice:pass" base64 = "YWxpY2U6cGFzcw=="
        let role = mgr.validate("Basic YWxpY2U6cGFzcw==");
        assert_eq!(role, Some(Role::ReadWrite));

        // Wrong password
        // "alice:wrong" base64 = "YWxpY2U6d3Jvbmc="
        let role = mgr.validate("Basic YWxpY2U6d3Jvbmc=");
        assert!(role.is_none());
    }

    #[test]
    fn test_validate_bearer_token() {
        let mut mgr = AuthManager::new();
        mgr.enabled = true;
        mgr.users.insert(
            "admin".to_string(),
            User {
                username: "admin".to_string(),
                password_hash: String::new(),
                role: Role::Admin,
            },
        );
        mgr.tokens
            .insert("mytoken".to_string(), "admin".to_string());

        assert_eq!(mgr.validate("Bearer mytoken"), Some(Role::Admin));
        assert!(mgr.validate("Bearer badtoken").is_none());
    }

    #[test]
    fn test_list_users() {
        let mut mgr = AuthManager::new();
        mgr.add_user("alice".to_string(), "p1".to_string(), Role::Admin);
        mgr.add_user("bob".to_string(), "p2".to_string(), Role::ReadOnly);

        let users = mgr.list_users();
        assert_eq!(users.len(), 2);
    }

    #[test]
    fn test_remove_user() {
        let mut mgr = AuthManager::new();
        mgr.add_user("alice".to_string(), "p1".to_string(), Role::Admin);
        assert!(mgr.remove_user("alice"));
        assert!(!mgr.remove_user("alice"));
        assert!(mgr.validate_credentials("alice", "p1").is_none());
    }

    #[test]
    fn test_bare_token_validation() {
        let mut mgr = AuthManager::new();
        mgr.enabled = true;
        mgr.tokens.insert("secret".to_string(), "admin".to_string());
        mgr.users.insert(
            "admin".to_string(),
            User {
                username: "admin".to_string(),
                password_hash: String::new(),
                role: Role::Admin,
            },
        );

        assert!(mgr.validate_bare_token("secret"));
        assert!(!mgr.validate_bare_token("wrong"));
    }

    #[test]
    fn test_base64_decode() {
        assert_eq!(base64_decode("YWxpY2U6cGFzcw==").unwrap(), "alice:pass");
        assert_eq!(base64_decode("aGVsbG8=").unwrap(), "hello");
        assert_eq!(base64_decode("dGVzdA==").unwrap(), "test");
    }

    #[test]
    fn test_from_env_unset() {
        std::env::remove_var("GRAPHMIND_AUTH_TOKEN");
        std::env::remove_var("GRAPHMIND_ADMIN_USER");
        std::env::remove_var("GRAPHMIND_ADMIN_PASSWORD");
        let config = AuthManager::from_env();
        assert!(!config.is_enabled());
    }

    #[test]
    fn test_shared_auth_manager() {
        let mut mgr = AuthManager::new();
        mgr.add_user("test".to_string(), "pass".to_string(), Role::Admin);
        let shared: SharedAuthManager = Arc::new(mgr);
        assert_eq!(
            shared.validate_credentials("test", "pass"),
            Some(Role::Admin)
        );
    }

    #[test]
    fn test_multiple_roles() {
        let mut mgr = AuthManager::new();
        mgr.add_user("admin".to_string(), "a".to_string(), Role::Admin);
        mgr.add_user("writer".to_string(), "w".to_string(), Role::ReadWrite);
        mgr.add_user("reader".to_string(), "r".to_string(), Role::ReadOnly);

        let admin_role = mgr.validate_credentials("admin", "a").unwrap();
        assert!(admin_role.can_write());
        assert!(admin_role.can_admin());

        let writer_role = mgr.validate_credentials("writer", "w").unwrap();
        assert!(writer_role.can_write());
        assert!(!writer_role.can_admin());

        let reader_role = mgr.validate_credentials("reader", "r").unwrap();
        assert!(!reader_role.can_write());
        assert!(!reader_role.can_admin());
    }
}
