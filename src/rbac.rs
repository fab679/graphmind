//! Role-Based Access Control (RBAC) for Graphmind
//!
//! Defines roles, permissions, and user management.
//! Phase 1: simple role model. Phase 2: per-tenant scoping.

use serde::{Deserialize, Serialize};
use std::collections::HashMap;

/// User roles with increasing privilege levels
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum Role {
    /// Can only read data (MATCH, RETURN, EXPLAIN)
    ReadOnly,
    /// Can read and write data (CREATE, DELETE, SET, MERGE)
    ReadWrite,
    /// Full access including admin operations (schema changes, user management)
    Admin,
}

impl Role {
    /// Check if this role can execute a write operation
    pub fn can_write(&self) -> bool {
        matches!(self, Role::ReadWrite | Role::Admin)
    }

    /// Check if this role can perform admin operations
    pub fn can_admin(&self) -> bool {
        matches!(self, Role::Admin)
    }

    /// Check if this role can read data
    pub fn can_read(&self) -> bool {
        true // all roles can read
    }
}

/// A user with authentication credentials and role
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct User {
    pub username: String,
    pub token: String, // hashed or plain token
    pub role: Role,
    pub tenant: Option<String>, // None = all tenants
    pub created_at: String,
    pub enabled: bool,
}

/// User store (in-memory, can be persisted to RocksDB later)
#[derive(Debug, Default)]
pub struct UserStore {
    users: HashMap<String, User>,     // token -> User
    by_name: HashMap<String, String>, // username -> token
}

impl UserStore {
    pub fn new() -> Self {
        Self::default()
    }

    /// Add a user
    pub fn add_user(&mut self, user: User) {
        self.by_name
            .insert(user.username.clone(), user.token.clone());
        self.users.insert(user.token.clone(), user);
    }

    /// Remove a user by username
    pub fn remove_user(&mut self, username: &str) -> Option<User> {
        if let Some(token) = self.by_name.remove(username) {
            self.users.remove(&token)
        } else {
            None
        }
    }

    /// Look up a user by token
    pub fn get_by_token(&self, token: &str) -> Option<&User> {
        self.users.get(token).filter(|u| u.enabled)
    }

    /// Look up a user by username
    pub fn get_by_name(&self, username: &str) -> Option<&User> {
        self.by_name
            .get(username)
            .and_then(|token| self.users.get(token))
            .filter(|u| u.enabled)
    }

    /// List all users
    pub fn list_users(&self) -> Vec<&User> {
        self.users.values().collect()
    }

    /// Check if a token has write permission
    pub fn can_write(&self, token: &str) -> bool {
        self.get_by_token(token)
            .map(|u| u.role.can_write())
            .unwrap_or(false)
    }

    /// Check if a token has admin permission
    pub fn can_admin(&self, token: &str) -> bool {
        self.get_by_token(token)
            .map(|u| u.role.can_admin())
            .unwrap_or(false)
    }
}
