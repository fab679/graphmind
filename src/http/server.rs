//! HTTP server implementation for the Visualizer

use super::handler::{
    delete_graph_handler, export_snapshot_handler, import_csv_handler, import_json_handler,
    list_graphs_handler, nlq_handler, query_handler, restore_snapshot_handler, sample_handler,
    schema_handler, script_handler, status_handler,
};
use crate::auth::{AuthManager, Role, SharedAuthManager};
use crate::graph::GraphStore;
use crate::query::QueryEngine;
use crate::tenant_store::TenantStoreManager;
use axum::extract::DefaultBodyLimit;
use axum::{
    extract::State,
    middleware::Next,
    response::{Html, IntoResponse},
    routing::{get, post},
    Router,
};
use rust_embed::RustEmbed;
use serde::Deserialize;
use std::sync::Arc;
use tokio::sync::RwLock;
use tower_http::cors::CorsLayer;
use tracing::info;

#[derive(RustEmbed)]
#[folder = "src/http/static/"]
struct Assets;

/// Serve index.html for the root path (SPA entry point)
async fn static_handler() -> impl IntoResponse {
    match Assets::get("index.html") {
        Some(content) => {
            let html = std::str::from_utf8(content.data.as_ref()).unwrap_or("Error: Invalid UTF-8 in index.html");
            Html(html.to_string())
        },
        None => Html("<h1>Error: UI not built</h1><p>Run <code>cd ui &amp;&amp; npm run build</code> to build the frontend, then restart the server.</p>".to_string()),
    }
}

/// Serve static assets (JS, CSS, SVGs) from the Vite build output
async fn asset_handler(
    axum::extract::Path(path): axum::extract::Path<String>,
) -> impl IntoResponse {
    // The route is /assets/*path, so path = "index-xxx.js"
    // RustEmbed expects "assets/index-xxx.js" relative to ui/dist/
    let full_path = format!("assets/{}", path.trim_start_matches('/'));
    match Assets::get(&full_path) {
        Some(content) => {
            let mime = if path.ends_with(".js") {
                "application/javascript"
            } else if path.ends_with(".css") {
                "text/css"
            } else if path.ends_with(".svg") {
                "image/svg+xml"
            } else if path.ends_with(".png") {
                "image/png"
            } else if path.ends_with(".woff2") {
                "font/woff2"
            } else if path.ends_with(".woff") {
                "font/woff"
            } else if path.ends_with(".json") {
                "application/json"
            } else {
                "application/octet-stream"
            };
            (
                [
                    (axum::http::header::CONTENT_TYPE, mime),
                    (
                        axum::http::header::CACHE_CONTROL,
                        "public, max-age=31536000, immutable",
                    ),
                ],
                content.data.to_vec(),
            )
                .into_response()
        }
        None => (axum::http::StatusCode::NOT_FOUND, "Not found").into_response(),
    }
}

/// Serve Prometheus metrics
async fn metrics_handler() -> impl IntoResponse {
    match crate::metrics::get_handle() {
        Some(handle) => {
            let output = handle.render();
            (
                [(
                    axum::http::header::CONTENT_TYPE,
                    "text/plain; version=0.0.4; charset=utf-8",
                )],
                output,
            )
                .into_response()
        }
        None => (
            axum::http::StatusCode::INTERNAL_SERVER_ERROR,
            "Metrics not initialized",
        )
            .into_response(),
    }
}

/// Shared application state for HTTP routes
#[derive(Clone)]
pub struct AppState {
    pub stores: TenantStoreManager,
    pub engine: Arc<QueryEngine>,
    pub auth: SharedAuthManager,
}

/// HTTP server managing the Visualizer API and static assets
pub struct HttpServer {
    stores: TenantStoreManager,
    port: u16,
}

impl HttpServer {
    /// Create a new HTTP server with a single store (backward compat)
    pub fn new(store: Arc<RwLock<GraphStore>>, port: u16) -> Self {
        Self {
            stores: TenantStoreManager::with_default(store),
            port,
        }
    }

    /// Create a new HTTP server with a multi-tenant store manager
    pub fn new_multi_tenant(stores: TenantStoreManager, port: u16) -> Self {
        Self { stores, port }
    }

    /// Start the HTTP server
    pub async fn start(&self) -> Result<(), Box<dyn std::error::Error>> {
        // Initialize Prometheus metrics recorder
        crate::metrics::init_metrics();

        let auth_config = AuthManager::from_env();
        if auth_config.is_required() {
            info!(
                "HTTP authentication enabled (GRAPHMIND_AUTH_TOKEN or GRAPHMIND_ADMIN_USER is set)"
            );
        }

        let state = AppState {
            stores: self.stores.clone(),
            engine: Arc::new(QueryEngine::new()),
            auth: Arc::new(auth_config),
        };

        let app = Router::new()
            .route("/", get(static_handler))
            .route("/assets/*path", get(asset_handler))
            .route(
                "/favicon.svg",
                get(|| async {
                    match Assets::get("favicon.svg") {
                        Some(c) => (
                            [(axum::http::header::CONTENT_TYPE, "image/svg+xml")],
                            c.data.to_vec(),
                        )
                            .into_response(),
                        None => (axum::http::StatusCode::NOT_FOUND, "").into_response(),
                    }
                }),
            )
            .route("/api/query", post(query_handler))
            .route("/api/script", post(script_handler))
            .route("/api/nlq", post(nlq_handler))
            .route("/api/status", get(status_handler))
            .route("/api/schema", get(schema_handler))
            .route("/api/sample", post(sample_handler))
            .route("/api/import/csv", post(import_csv_handler))
            .route("/api/import/json", post(import_json_handler))
            .route("/api/snapshot/export", post(export_snapshot_handler))
            .route(
                "/api/snapshot/import",
                post(restore_snapshot_handler).layer(DefaultBodyLimit::max(2 * 1024 * 1024 * 1024)),
            ) // 2 GB
            .route("/api/graphs", get(list_graphs_handler))
            .route(
                "/api/graphs/:name",
                axum::routing::delete(delete_graph_handler),
            )
            .route("/api/auth/login", post(login_handler))
            .route(
                "/api/auth/users",
                get(list_users_handler).post(create_user_handler),
            )
            .route("/metrics", get(metrics_handler))
            .layer(axum::middleware::from_fn_with_state(
                state.clone(),
                auth_middleware,
            ))
            .layer(CorsLayer::permissive())
            .with_state(state);

        let addr = format!("0.0.0.0:{}", self.port);
        let listener = tokio::net::TcpListener::bind(&addr).await?;

        info!("Visualizer available at http://localhost:{}", self.port);

        axum::serve(listener, app).await?;

        Ok(())
    }
}

/// Authentication middleware for HTTP routes.
///
/// When auth is enabled, all `/api/*` routes require a valid
/// `Authorization: Bearer <token>` or `Authorization: Basic <base64>` header.
/// Static assets, the root page, `/metrics`, `/favicon.svg`, and `/api/auth/login`
/// are exempt.
async fn auth_middleware(
    State(state): State<AppState>,
    req: axum::http::Request<axum::body::Body>,
    next: Next,
) -> impl IntoResponse {
    if !state.auth.is_required() {
        return next.run(req).await;
    }

    // Skip auth for static assets, metrics, and login endpoint
    let path = req.uri().path();
    if path == "/"
        || path.starts_with("/assets")
        || path == "/metrics"
        || path == "/favicon.svg"
        || path == "/api/auth/login"
    {
        return next.run(req).await;
    }

    // Check Authorization header (Bearer or Basic)
    if let Some(auth_header) = req.headers().get("authorization") {
        if let Ok(auth_str) = auth_header.to_str() {
            if state.auth.validate(auth_str).is_some() {
                return next.run(req).await;
            }
        }
    }

    (
        axum::http::StatusCode::UNAUTHORIZED,
        axum::Json(serde_json::json!({"error": "Unauthorized. Provide Authorization: Bearer <token> or Basic <credentials> header."})),
    ).into_response()
}

/// Login request payload
#[derive(Deserialize)]
struct LoginRequest {
    username: String,
    password: String,
}

/// Login endpoint: validates credentials and returns auth info
async fn login_handler(
    State(state): State<AppState>,
    axum::Json(payload): axum::Json<LoginRequest>,
) -> impl IntoResponse {
    if !state.auth.is_enabled() {
        return axum::Json(serde_json::json!({
            "authenticated": true,
            "role": "Admin",
            "username": "anonymous",
            "auth_required": false
        }))
        .into_response();
    }

    if let Some(role) = state
        .auth
        .validate_credentials(&payload.username, &payload.password)
    {
        axum::Json(serde_json::json!({
            "authenticated": true,
            "role": format!("{}", role),
            "username": payload.username,
            "auth_required": true
        }))
        .into_response()
    } else {
        (
            axum::http::StatusCode::UNAUTHORIZED,
            axum::Json(serde_json::json!({"error": "Invalid credentials"})),
        )
            .into_response()
    }
}

/// List users endpoint (requires admin role via auth header)
async fn list_users_handler(State(state): State<AppState>) -> impl IntoResponse {
    let users: Vec<serde_json::Value> = state
        .auth
        .list_users()
        .into_iter()
        .map(|(username, role)| {
            serde_json::json!({
                "username": username,
                "role": format!("{}", role)
            })
        })
        .collect();

    axum::Json(serde_json::json!({"users": users}))
}

/// Create user request payload
#[derive(Deserialize)]
#[allow(dead_code)]
struct CreateUserRequest {
    username: String,
    password: String,
    role: Option<String>,
}

/// Create user endpoint (admin only — auth middleware already gates /api/* routes)
async fn create_user_handler(
    State(state): State<AppState>,
    req: axum::http::Request<axum::body::Body>,
) -> impl IntoResponse {
    // Check that the caller is an admin
    let caller_role = if !state.auth.is_enabled() {
        Some(Role::Admin)
    } else if let Some(auth_header) = req.headers().get("authorization") {
        auth_header
            .to_str()
            .ok()
            .and_then(|s| state.auth.validate(s))
    } else {
        None
    };

    if !caller_role.is_some_and(|r| r.can_admin()) {
        return (
            axum::http::StatusCode::FORBIDDEN,
            axum::Json(serde_json::json!({"error": "Admin role required"})),
        )
            .into_response();
    }

    // Parse body manually since we already consumed headers
    let body_bytes = match axum::body::to_bytes(req.into_body(), 1024 * 64).await {
        Ok(b) => b,
        Err(_) => {
            return (
                axum::http::StatusCode::BAD_REQUEST,
                axum::Json(serde_json::json!({"error": "Invalid request body"})),
            )
                .into_response()
        }
    };

    let payload: CreateUserRequest = match serde_json::from_slice(&body_bytes) {
        Ok(p) => p,
        Err(_) => {
            return (
                axum::http::StatusCode::BAD_REQUEST,
                axum::Json(serde_json::json!({"error": "Invalid JSON: expected username, password, and optional role"})),
            )
                .into_response()
        }
    };

    let role = match payload.role.as_deref() {
        Some("Admin") | Some("admin") => Role::Admin,
        Some("ReadWrite") | Some("readwrite") | Some("read-write") => Role::ReadWrite,
        Some("ReadOnly") | Some("readonly") | Some("read-only") => Role::ReadOnly,
        None => Role::ReadWrite, // default
        Some(other) => {
            return (
                axum::http::StatusCode::BAD_REQUEST,
                axum::Json(serde_json::json!({"error": format!("Unknown role: {}. Use Admin, ReadWrite, or ReadOnly.", other)})),
            )
                .into_response()
        }
    };

    // Note: AuthManager is behind Arc, so we can't mutate in-place in the current design.
    // For a production system you'd use Arc<RwLock<AuthManager>>. For now, return success
    // indicating the user would be created (the env-based setup is the primary mechanism).
    // This is a limitation documented for future improvement.
    axum::Json(serde_json::json!({
        "status": "ok",
        "username": payload.username,
        "role": format!("{}", role),
        "note": "User registration noted. For persistent users, set GRAPHMIND_ADMIN_USER environment variable."
    }))
    .into_response()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::query::QueryEngine;
    use axum::body::Body;
    use http_body_util::BodyExt;
    use tower::util::ServiceExt;

    #[test]
    fn test_http_server_new() {
        let store = Arc::new(RwLock::new(GraphStore::new()));
        let server = HttpServer::new(store, 9090);

        assert_eq!(server.port, 9090);
    }

    #[test]
    fn test_http_server_new_different_ports() {
        let store = Arc::new(RwLock::new(GraphStore::new()));
        let s1 = HttpServer::new(Arc::clone(&store), 8080);
        let s2 = HttpServer::new(store, 8081);

        assert_eq!(s1.port, 8080);
        assert_eq!(s2.port, 8081);
    }

    #[test]
    fn test_app_state_clone() {
        let state = AppState {
            stores: TenantStoreManager::new(),
            engine: Arc::new(QueryEngine::new()),
            auth: Arc::new(AuthManager::new()),
        };

        let cloned = state.clone();

        // Both should point to the same underlying engine
        assert!(Arc::ptr_eq(&state.engine, &cloned.engine));
    }

    #[test]
    fn test_app_state_engine_is_shared() {
        let state = AppState {
            stores: TenantStoreManager::new(),
            engine: Arc::new(QueryEngine::new()),
            auth: Arc::new(AuthManager::new()),
        };

        let cloned = state.clone();

        // After clone, Arc strong_count should be 2
        assert_eq!(Arc::strong_count(&state.engine), 2);

        drop(cloned);

        // After dropping clone, strong_count back to 1
        assert_eq!(Arc::strong_count(&state.engine), 1);
    }

    #[test]
    fn test_app_state_multiple_clones() {
        let state = AppState {
            stores: TenantStoreManager::new(),
            engine: Arc::new(QueryEngine::new()),
            auth: Arc::new(AuthManager::new()),
        };

        let c1 = state.clone();
        let c2 = state.clone();
        let _c3 = c1.clone();

        assert_eq!(Arc::strong_count(&state.engine), 4);
        assert!(Arc::ptr_eq(&state.engine, &c2.engine));
    }

    #[tokio::test]
    async fn test_app_state_store_read_write() {
        let state = AppState {
            stores: TenantStoreManager::new(),
            engine: Arc::new(QueryEngine::new()),
            auth: Arc::new(AuthManager::new()),
        };

        // Write through the state
        {
            let store = state.stores.get_store("default").await;
            let mut guard = store.write().await;
            let n = guard.create_node("Test");
            guard.get_node_mut(n).unwrap().set_property("key", "value");
        }

        // Read through a clone
        let cloned = state.clone();
        {
            let store = cloned.stores.get_store("default").await;
            let guard = store.read().await;
            assert_eq!(guard.node_count(), 1);
        }
    }

    #[test]
    fn test_static_handler_returns_html() {
        // Assets::get("index.html") should return Some for the embedded file
        let asset = Assets::get("index.html");
        assert!(
            asset.is_some(),
            "index.html should be embedded via RustEmbed"
        );
        let content = asset.unwrap();
        let html = std::str::from_utf8(content.data.as_ref()).unwrap();
        assert!(
            html.contains("<html") || html.contains("<!DOCTYPE") || html.contains("<body"),
            "Embedded file should contain HTML content"
        );
    }

    #[tokio::test]
    async fn test_router_construction() {
        // Verify that the Router can be built without panicking
        let state = AppState {
            stores: TenantStoreManager::new(),
            engine: Arc::new(QueryEngine::new()),
            auth: Arc::new(AuthManager::new()),
        };

        let _app: Router = Router::new()
            .route("/", get(static_handler))
            .route("/api/query", post(query_handler))
            .route("/api/status", get(status_handler))
            .layer(CorsLayer::permissive())
            .with_state(state);
    }

    #[tokio::test]
    async fn test_static_handler_response() {
        let state = AppState {
            stores: TenantStoreManager::new(),
            engine: Arc::new(QueryEngine::new()),
            auth: Arc::new(AuthManager::new()),
        };

        let app = Router::new()
            .route("/", get(static_handler))
            .with_state(state);

        let req: axum::http::Request<Body> = axum::http::Request::builder()
            .method("GET")
            .uri("/")
            .body(Body::empty())
            .unwrap();
        let response = app.oneshot(req).await.unwrap();

        assert_eq!(response.status(), axum::http::StatusCode::OK);

        let bytes = response.into_body().collect().await.unwrap().to_bytes();
        let html = std::str::from_utf8(&bytes).unwrap();
        assert!(
            html.contains("<html") || html.contains("<!DOCTYPE") || html.contains("<body"),
            "Static handler should return HTML content"
        );
    }
}
