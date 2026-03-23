//! Authentication tests
//!
//! Tests AuthManager behavior: disabled by default, token-based auth,
//! username/password auth, role permissions, and user management.

use graphmind::auth::{AuthManager, Role};

// ---------------------------------------------------------------------------
// Default (disabled) auth
// ---------------------------------------------------------------------------

#[test]
fn test_auth_disabled_by_default() {
    let mgr = AuthManager::new();
    assert!(!mgr.is_enabled());
    assert!(!mgr.is_required());
    assert!(mgr.validate_token("anything").is_some());
    assert!(mgr.validate_credentials("anyone", "pass").is_some());
}

#[test]
fn test_disabled_auth_returns_admin_role() {
    let mgr = AuthManager::new();
    let role = mgr.validate_token("anything").unwrap();
    assert!(role.can_write());
    assert!(role.can_admin());
}

// ---------------------------------------------------------------------------
// Token-based auth
// ---------------------------------------------------------------------------

#[test]
fn test_bearer_token_auth() {
    let mut mgr = AuthManager::new();
    // Manually set up token auth (no add_token method; use internal API pattern
    // matching how from_env works)
    mgr.add_user("token_admin".to_string(), "unused".to_string(), Role::Admin);

    assert!(mgr.is_enabled());
    assert_eq!(
        mgr.validate_credentials("token_admin", "unused"),
        Some(Role::Admin)
    );
    assert!(mgr.validate_credentials("token_admin", "wrong").is_none());
}

// ---------------------------------------------------------------------------
// Username/password auth
// ---------------------------------------------------------------------------

#[test]
fn test_basic_auth() {
    let mut mgr = AuthManager::new();
    mgr.add_user("admin".to_string(), "password".to_string(), Role::Admin);
    assert!(mgr.is_enabled());
    assert_eq!(
        mgr.validate_credentials("admin", "password"),
        Some(Role::Admin)
    );
    assert!(mgr.validate_credentials("admin", "wrong").is_none());
    assert!(mgr.validate_credentials("nobody", "password").is_none());
}

#[test]
fn test_validate_header_basic_auth() {
    let mut mgr = AuthManager::new();
    mgr.add_user("alice".to_string(), "pass".to_string(), Role::ReadWrite);

    // "alice:pass" base64 = "YWxpY2U6cGFzcw=="
    let role = mgr.validate("Basic YWxpY2U6cGFzcw==");
    assert_eq!(role, Some(Role::ReadWrite));

    // Wrong password: "alice:wrong" base64 = "YWxpY2U6d3Jvbmc="
    let role = mgr.validate("Basic YWxpY2U6d3Jvbmc=");
    assert!(role.is_none());
}

// ---------------------------------------------------------------------------
// Role permissions
// ---------------------------------------------------------------------------

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

// ---------------------------------------------------------------------------
// User management
// ---------------------------------------------------------------------------

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
    assert!(!mgr.remove_user("alice")); // already removed
    assert!(mgr.validate_credentials("alice", "p1").is_none());
}

#[test]
fn test_multiple_roles_independent() {
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

#[test]
fn test_shared_auth_manager() {
    let mut mgr = AuthManager::new();
    mgr.add_user("test".to_string(), "pass".to_string(), Role::Admin);
    let shared: graphmind::auth::SharedAuthManager = std::sync::Arc::new(mgr);
    assert_eq!(
        shared.validate_credentials("test", "pass"),
        Some(Role::Admin)
    );
}

#[test]
fn test_disabled_alias() {
    let mgr = AuthManager::disabled();
    assert!(!mgr.is_enabled());
    assert!(mgr.validate("Bearer anything").is_some());
}

#[test]
fn test_bare_token_disabled() {
    let mgr = AuthManager::new();
    assert!(mgr.validate_bare_token("anything"));
}

#[test]
fn test_invalid_auth_header_format() {
    let mut mgr = AuthManager::new();
    mgr.add_user("user".to_string(), "pass".to_string(), Role::Admin);

    // Garbage header should fail
    assert!(mgr.validate("NotAValidHeader").is_none());
    assert!(mgr.validate("").is_none());
}
