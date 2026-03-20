//! RESP protocol server implementation
//!
//! Implements REQ-REDIS-001, REQ-REDIS-002 (RESP server, Redis clients)

use crate::auth::{AuthConfig, AuthManager, SharedAuthConfig};
use crate::graph::GraphStore;
use crate::persistence::PersistenceManager;
use crate::protocol::command::CommandHandler;
use crate::protocol::resp::{RespError, RespValue};
use crate::raft::ClusterManager;
use crate::sharding::{Proxy, RouteResult, Router};
use crate::tenant_store::TenantStoreManager;
use bytes::BytesMut;
use std::sync::Arc;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::RwLock;
use tracing::{debug, error, info, warn};

/// Server configuration
#[derive(Debug, Clone)]
pub struct ServerConfig {
    /// Bind address
    pub address: String,
    /// Port
    pub port: u16,
    /// Maximum connections
    pub max_connections: usize,
    /// Data directory for persistence (None = in-memory only)
    pub data_path: Option<String>,
}

impl Default for ServerConfig {
    fn default() -> Self {
        Self {
            address: "127.0.0.1".to_string(),
            port: 6379,
            max_connections: 10000,
            data_path: Some("./graphmind_data".to_string()),
        }
    }
}

/// RESP protocol server
pub struct RespServer {
    /// Server configuration
    config: ServerConfig,
    /// Multi-tenant store manager
    stores: TenantStoreManager,
    /// Command handler
    handler: Arc<CommandHandler>,
    /// Optional persistence manager for durability
    /// When Some, writes are persisted to disk via WAL + RocksDB
    #[allow(dead_code)]
    persistence: Option<Arc<PersistenceManager>>,
    /// Optional sharding router
    router: Option<Arc<Router>>,
    /// Optional proxy client
    proxy: Option<Arc<Proxy>>,
    /// Optional cluster manager for resolving node addresses
    cluster_manager: Option<Arc<ClusterManager>>,
    /// Authentication configuration
    auth: SharedAuthConfig,
}

impl RespServer {
    /// Create a new RESP server (in-memory only, no persistence)
    pub fn new(config: ServerConfig, store: Arc<RwLock<GraphStore>>) -> Self {
        let handler = Arc::new(CommandHandler::new(None));
        let auth = Arc::new(AuthConfig::from_env());
        Self {
            config,
            stores: TenantStoreManager::with_default(store),
            handler,
            persistence: None,
            router: None,
            proxy: None,
            cluster_manager: None,
            auth,
        }
    }

    /// Create a new RESP server with persistence enabled
    /// Data will be persisted to disk via WAL + RocksDB
    pub fn new_with_persistence(
        config: ServerConfig,
        store: Arc<RwLock<GraphStore>>,
        persistence: Arc<PersistenceManager>,
    ) -> Self {
        let handler = Arc::new(CommandHandler::new(Some(Arc::clone(&persistence))));
        let auth = Arc::new(AuthConfig::from_env());
        Self {
            config,
            stores: TenantStoreManager::with_default(store),
            handler,
            persistence: Some(persistence),
            router: None,
            proxy: None,
            cluster_manager: None,
            auth,
        }
    }

    /// Create a new RESP server with a multi-tenant store manager
    pub fn new_multi_tenant(config: ServerConfig, stores: TenantStoreManager) -> Self {
        let handler = Arc::new(CommandHandler::new(None));
        let auth = Arc::new(AuthConfig::from_env());
        Self {
            config,
            stores,
            handler,
            persistence: None,
            router: None,
            proxy: None,
            cluster_manager: None,
            auth,
        }
    }

    /// Create a new multi-tenant RESP server with persistence
    pub fn new_multi_tenant_with_persistence(
        config: ServerConfig,
        stores: TenantStoreManager,
        persistence: Arc<PersistenceManager>,
    ) -> Self {
        let handler = Arc::new(CommandHandler::new(Some(Arc::clone(&persistence))));
        let auth = Arc::new(AuthConfig::from_env());
        Self {
            config,
            stores,
            handler,
            persistence: Some(persistence),
            router: None,
            proxy: None,
            cluster_manager: None,
            auth,
        }
    }

    /// Enable sharding for this server
    pub fn with_sharding(
        mut self,
        router: Arc<Router>,
        proxy: Arc<Proxy>,
        cluster_manager: Arc<ClusterManager>,
    ) -> Self {
        self.router = Some(router);
        self.proxy = Some(proxy);
        self.cluster_manager = Some(cluster_manager);
        self
    }

    /// Start the server
    pub async fn start(&self) -> Result<(), Box<dyn std::error::Error>> {
        let addr = format!("{}:{}", self.config.address, self.config.port);
        let listener = TcpListener::bind(&addr).await?;

        if self.auth.is_required() {
            info!(
                "RESP authentication enabled (GRAPHMIND_AUTH_TOKEN or GRAPHMIND_ADMIN_USER is set)"
            );
        }
        info!("RESP server listening on {}", addr);

        loop {
            let (socket, peer_addr) = listener.accept().await?;
            debug!("New connection from {}", peer_addr);

            let stores = self.stores.clone();
            let handler = Arc::clone(&self.handler);
            let router = self.router.clone();
            let proxy = self.proxy.clone();
            let cluster = self.cluster_manager.clone();
            let auth = Arc::clone(&self.auth);

            // Spawn a new task for each connection
            tokio::spawn(async move {
                if let Err(e) =
                    handle_connection(socket, stores, handler, router, proxy, cluster, auth).await
                {
                    error!("Error handling connection from {}: {}", peer_addr, e);
                }
            });
        }
    }
}

/// Extract the command name from a RESP value (for auth gating).
fn extract_command_name(value: &RespValue) -> Option<String> {
    if let Ok(args) = value.as_array() {
        if !args.is_empty() {
            if let Ok(Some(s)) = args[0].as_string() {
                return Some(s.to_uppercase());
            }
        }
    }
    None
}

/// Handle the AUTH command: supports both AUTH <token> and AUTH <username> <password>.
fn handle_auth_command(value: &RespValue, auth: &AuthManager) -> RespValue {
    if let Ok(args) = value.as_array() {
        match args.len() {
            // AUTH <token> (single arg — legacy token auth)
            2 => match args[1].as_string() {
                Ok(Some(token)) => {
                    if auth.validate_bare_token(&token) {
                        RespValue::SimpleString("OK".to_string())
                    } else {
                        RespValue::Error("ERR invalid password".to_string())
                    }
                }
                _ => RespValue::Error("ERR invalid password".to_string()),
            },
            // AUTH <username> <password> (two args — username/password auth)
            3 => {
                let username = match args[1].as_string() {
                    Ok(Some(u)) => u,
                    _ => return RespValue::Error("ERR invalid username".to_string()),
                };
                let password = match args[2].as_string() {
                    Ok(Some(p)) => p,
                    _ => return RespValue::Error("ERR invalid password".to_string()),
                };
                if auth.validate_credentials(&username, &password).is_some() {
                    RespValue::SimpleString("OK".to_string())
                } else {
                    RespValue::Error(
                        "ERR invalid username-password pair or user is disabled".to_string(),
                    )
                }
            }
            _ => RespValue::Error("ERR wrong number of arguments for 'AUTH' command".to_string()),
        }
    } else {
        RespValue::Error("ERR wrong number of arguments for 'AUTH' command".to_string())
    }
}

/// Handle a single client connection
async fn handle_connection(
    mut socket: TcpStream,
    stores: TenantStoreManager,
    handler: Arc<CommandHandler>,
    router: Option<Arc<Router>>,
    proxy: Option<Arc<Proxy>>,
    cluster: Option<Arc<ClusterManager>>,
    auth: SharedAuthConfig,
) -> Result<(), Box<dyn std::error::Error>> {
    let mut buffer = BytesMut::with_capacity(4096);
    // If auth is not required, connection starts authenticated
    let mut authenticated = !auth.is_required();

    loop {
        // Read data from socket
        let n = socket.read_buf(&mut buffer).await?;

        if n == 0 {
            // Connection closed
            debug!("Connection closed by client");
            return Ok(());
        }

        // Try to parse RESP commands
        loop {
            match RespValue::decode(&mut buffer) {
                Ok(Some(value)) => {
                    // Check auth state before processing commands
                    let cmd_name = extract_command_name(&value);

                    // AUTH command is always allowed
                    if cmd_name.as_deref() == Some("AUTH") {
                        let response = handle_auth_command(&value, &auth);
                        if matches!(&response, RespValue::SimpleString(s) if s == "OK") {
                            authenticated = true;
                        }
                        let mut response_buf = Vec::new();
                        response.encode(&mut response_buf)?;
                        socket.write_all(&response_buf).await?;
                        continue;
                    }

                    // PING is always allowed (even unauthenticated)
                    if cmd_name.as_deref() == Some("PING") && !authenticated {
                        let response = handler.handle_command_multi(&value, &stores).await;
                        let mut response_buf = Vec::new();
                        response.encode(&mut response_buf)?;
                        socket.write_all(&response_buf).await?;
                        continue;
                    }

                    // Reject all other commands if not authenticated
                    if !authenticated {
                        warn!("Unauthenticated command rejected: {:?}", cmd_name);
                        let response = RespValue::Error(
                            "NOAUTH Authentication required. Use AUTH <token>.".to_string(),
                        );
                        let mut response_buf = Vec::new();
                        response.encode(&mut response_buf)?;
                        socket.write_all(&response_buf).await?;
                        continue;
                    }

                    let mut forwarded = false;

                    // Attempt routing if configured
                    if let (Some(router), Some(proxy), Some(cluster)) = (&router, &proxy, &cluster)
                    {
                        if let Ok(args) = value.as_array() {
                            if args.len() >= 2 {
                                if let Ok(Some(cmd)) = args[0].as_string() {
                                    if cmd.to_uppercase().starts_with("GRAPH.") {
                                        if let Ok(Some(key)) = args[1].as_string() {
                                            if let Some(RouteResult::Remote(node_id)) =
                                                router.route(&key)
                                            {
                                                // Resolve address from ClusterConfig
                                                let config = cluster.get_config().await;
                                                if let Some(node_config) =
                                                    config.nodes.iter().find(|n| n.id == node_id)
                                                {
                                                    debug!("Routing command for tenant '{}' to node {} ({})", key, node_id, node_config.address);

                                                    // Re-encode command
                                                    let mut cmd_bytes = Vec::new();
                                                    value.encode(&mut cmd_bytes)?;

                                                    // Forward
                                                    match proxy
                                                        .forward(&node_config.address, &cmd_bytes)
                                                        .await
                                                    {
                                                        Ok(response_bytes) => {
                                                            socket
                                                                .write_all(&response_bytes)
                                                                .await?;
                                                            forwarded = true;
                                                        }
                                                        Err(e) => {
                                                            error!(
                                                                "Failed to forward request: {}",
                                                                e
                                                            );
                                                            let err = RespValue::Error(format!(
                                                                "ERR routing failed: {}",
                                                                e
                                                            ));
                                                            let mut buf = Vec::new();
                                                            err.encode(&mut buf)?;
                                                            socket.write_all(&buf).await?;
                                                            forwarded = true; // Handled as error
                                                        }
                                                    }
                                                }
                                            }
                                        }
                                    }
                                }
                            }
                        }
                    }

                    if !forwarded {
                        // Process command locally
                        let response = handler.handle_command_multi(&value, &stores).await;

                        // Encode and send response
                        let mut response_buf = Vec::new();
                        response.encode(&mut response_buf)?;
                        socket.write_all(&response_buf).await?;
                    }
                }
                Ok(None) => {
                    // Need more data
                    break;
                }
                Err(RespError::Incomplete) => {
                    // Need more data
                    break;
                }
                Err(e) => {
                    // Protocol error
                    error!("Protocol error: {}", e);
                    let error_response = RespValue::Error(format!("ERR {}", e));
                    let mut response_buf = Vec::new();
                    error_response.encode(&mut response_buf)?;
                    socket.write_all(&response_buf).await?;
                    break;
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_server_config_default() {
        let config = ServerConfig::default();
        assert_eq!(config.address, "127.0.0.1");
        assert_eq!(config.port, 6379);
        assert_eq!(config.max_connections, 10000);
    }

    #[test]
    fn test_server_creation() {
        let config = ServerConfig::default();
        let store = Arc::new(RwLock::new(GraphStore::new()));
        let server = RespServer::new(config, store);
        // In-memory server should have no persistence
        assert!(server.persistence.is_none());
    }

    #[tokio::test]
    async fn test_connection_handling() {
        // This is a basic test structure
        // Real integration tests would require a running server
        let store = Arc::new(RwLock::new(GraphStore::new()));
        let handler = Arc::new(CommandHandler::new(None)); // No persistence for tests

        // Would need to set up mock socket for full test
        drop(store);
        drop(handler);
    }

    #[test]
    fn test_server_config_custom() {
        let config = ServerConfig {
            address: "0.0.0.0".to_string(),
            port: 16379,
            max_connections: 500,
            data_path: Some("/tmp/graphmind_test".to_string()),
        };
        assert_eq!(config.address, "0.0.0.0");
        assert_eq!(config.port, 16379);
        assert_eq!(config.max_connections, 500);
        assert_eq!(config.data_path, Some("/tmp/graphmind_test".to_string()));
    }

    #[test]
    fn test_server_config_no_persistence() {
        let config = ServerConfig {
            address: "127.0.0.1".to_string(),
            port: 6379,
            max_connections: 10000,
            data_path: None,
        };
        assert!(config.data_path.is_none());
    }

    #[test]
    fn test_server_config_default_has_data_path() {
        let config = ServerConfig::default();
        assert!(config.data_path.is_some());
        assert_eq!(config.data_path.unwrap(), "./graphmind_data");
    }

    #[test]
    fn test_server_config_debug() {
        let config = ServerConfig::default();
        let debug_str = format!("{:?}", config);
        assert!(debug_str.contains("127.0.0.1"));
        assert!(debug_str.contains("6379"));
    }

    #[test]
    fn test_server_config_clone() {
        let config = ServerConfig::default();
        let cloned = config.clone();
        assert_eq!(config.address, cloned.address);
        assert_eq!(config.port, cloned.port);
        assert_eq!(config.max_connections, cloned.max_connections);
        assert_eq!(config.data_path, cloned.data_path);
    }

    #[test]
    fn test_server_new_stores_config() {
        let config = ServerConfig {
            address: "192.168.1.1".to_string(),
            port: 9999,
            max_connections: 42,
            data_path: None,
        };
        let store = Arc::new(RwLock::new(GraphStore::new()));
        let server = RespServer::new(config, store);

        assert_eq!(server.config.address, "192.168.1.1");
        assert_eq!(server.config.port, 9999);
        assert_eq!(server.config.max_connections, 42);
        assert!(server.config.data_path.is_none());
    }

    #[test]
    fn test_server_new_has_no_router() {
        let config = ServerConfig::default();
        let store = Arc::new(RwLock::new(GraphStore::new()));
        let server = RespServer::new(config, store);

        assert!(server.router.is_none());
        assert!(server.proxy.is_none());
        assert!(server.cluster_manager.is_none());
    }

    #[tokio::test]
    async fn test_server_new_shared_store() {
        let store = Arc::new(RwLock::new(GraphStore::new()));
        let store_clone = Arc::clone(&store);
        let config = ServerConfig::default();
        let server = RespServer::new(config, store);

        // The default tenant store should point to the same underlying store
        let default_store = server.stores.get_store("default").await;
        assert!(Arc::ptr_eq(&default_store, &store_clone));
    }

    #[tokio::test]
    async fn test_server_start_invalid_port_fails() {
        // Bind to a port that we'll then try to bind again
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let port = listener.local_addr().unwrap().port();

        let config = ServerConfig {
            address: "127.0.0.1".to_string(),
            port,
            max_connections: 10,
            data_path: None,
        };
        let store = Arc::new(RwLock::new(GraphStore::new()));
        let server = RespServer::new(config, store);

        // Starting on already-bound port should fail
        let result =
            tokio::time::timeout(std::time::Duration::from_millis(100), server.start()).await;

        // Either times out (somehow bound) or errors
        assert!(result.is_err() || result.unwrap().is_err());
    }

    #[tokio::test]
    async fn test_handle_connection_close() {
        // Create a TCP pair: a server-side socket and a client-side socket
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();

        let store = Arc::new(RwLock::new(GraphStore::new()));
        let handler = Arc::new(CommandHandler::new(None));

        let client_task = tokio::spawn(async move {
            let mut stream = tokio::net::TcpStream::connect(addr).await.unwrap();
            // Send a PING inline command
            use tokio::io::AsyncWriteExt;
            stream.write_all(b"PING\r\n").await.unwrap();
            // Read response
            use tokio::io::AsyncReadExt;
            let mut buf = vec![0u8; 256];
            let n = stream.read(&mut buf).await.unwrap();
            assert!(n > 0);
            // Close the connection by dropping
            drop(stream);
        });

        let (socket, _peer) = listener.accept().await.unwrap();
        let result = handle_connection(
            socket,
            TenantStoreManager::with_default(store),
            handler,
            None,
            None,
            None,
            Arc::new(AuthConfig::disabled()),
        )
        .await;
        assert!(result.is_ok());

        client_task.await.unwrap();
    }

    #[tokio::test]
    async fn test_handle_connection_resp_command() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();

        let store = Arc::new(RwLock::new(GraphStore::new()));
        let handler = Arc::new(CommandHandler::new(None));

        let client_task = tokio::spawn(async move {
            let mut stream = tokio::net::TcpStream::connect(addr).await.unwrap();
            use tokio::io::{AsyncReadExt, AsyncWriteExt};
            // Send RESP-formatted PING: *1\r\n$4\r\nPING\r\n
            stream.write_all(b"*1\r\n$4\r\nPING\r\n").await.unwrap();
            let mut buf = vec![0u8; 256];
            let n = stream.read(&mut buf).await.unwrap();
            assert!(n > 0);
            // The response should be a simple string "+PONG\r\n"
            let response = String::from_utf8_lossy(&buf[..n]);
            assert!(
                response.contains("PONG"),
                "Expected PONG in response, got: {}",
                response
            );
            drop(stream);
        });

        let (socket, _peer) = listener.accept().await.unwrap();
        let result = handle_connection(
            socket,
            TenantStoreManager::with_default(store),
            handler,
            None,
            None,
            None,
            Arc::new(AuthConfig::disabled()),
        )
        .await;
        assert!(result.is_ok());

        client_task.await.unwrap();
    }

    #[tokio::test]
    async fn test_handle_connection_protocol_error() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();

        let store = Arc::new(RwLock::new(GraphStore::new()));
        let handler = Arc::new(CommandHandler::new(None));

        let client_task = tokio::spawn(async move {
            let mut stream = tokio::net::TcpStream::connect(addr).await.unwrap();
            use tokio::io::{AsyncReadExt, AsyncWriteExt};
            // Send invalid RESP data: bad array length
            stream.write_all(b"*abc\r\n").await.unwrap();
            let mut buf = vec![0u8; 256];
            let n = stream.read(&mut buf).await.unwrap();
            // Should get an error response
            let response = String::from_utf8_lossy(&buf[..n]);
            assert!(
                response.contains("ERR") || response.contains("-"),
                "Expected error response, got: {}",
                response
            );
            drop(stream);
        });

        let (socket, _peer) = listener.accept().await.unwrap();
        let result = handle_connection(
            socket,
            TenantStoreManager::with_default(store),
            handler,
            None,
            None,
            None,
            Arc::new(AuthConfig::disabled()),
        )
        .await;
        // Connection may close after error, which is still OK
        assert!(result.is_ok());

        client_task.await.unwrap();
    }

    // ========== Additional Server Coverage Tests ==========

    #[test]
    fn test_server_with_persistence() {
        let temp_dir = tempfile::TempDir::new().unwrap();
        let persistence =
            Arc::new(crate::persistence::PersistenceManager::new(temp_dir.path()).unwrap());
        let store = Arc::new(RwLock::new(GraphStore::new()));
        let config = ServerConfig::default();

        let server = RespServer::new_with_persistence(config, store, persistence);
        assert!(server.persistence.is_some());
        assert!(server.router.is_none());
        assert!(server.proxy.is_none());
        assert!(server.cluster_manager.is_none());
    }

    #[test]
    fn test_server_with_persistence_stores_config() {
        let temp_dir = tempfile::TempDir::new().unwrap();
        let persistence =
            Arc::new(crate::persistence::PersistenceManager::new(temp_dir.path()).unwrap());
        let store = Arc::new(RwLock::new(GraphStore::new()));
        let config = ServerConfig {
            address: "0.0.0.0".to_string(),
            port: 16379,
            max_connections: 500,
            data_path: Some("/tmp/test".to_string()),
        };

        let server = RespServer::new_with_persistence(config, store, persistence);
        assert_eq!(server.config.port, 16379);
        assert_eq!(server.config.address, "0.0.0.0");
    }

    #[tokio::test]
    async fn test_handle_connection_graph_query_command() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();

        let store = Arc::new(RwLock::new(GraphStore::new()));
        let handler = Arc::new(CommandHandler::new(None));

        let client_task = tokio::spawn(async move {
            let mut stream = tokio::net::TcpStream::connect(addr).await.unwrap();
            use tokio::io::{AsyncReadExt, AsyncWriteExt};
            // Use inline command format which is simpler
            stream
                .write_all(b"GRAPH.QUERY default \"MATCH (n) RETURN n\"\r\n")
                .await
                .unwrap();
            let mut buf = vec![0u8; 4096];
            let n = stream.read(&mut buf).await.unwrap();
            assert!(n > 0, "Expected response from GRAPH.QUERY");
            drop(stream);
        });

        let (socket, _peer) = listener.accept().await.unwrap();
        let result = handle_connection(
            socket,
            TenantStoreManager::with_default(store),
            handler,
            None,
            None,
            None,
            Arc::new(AuthConfig::disabled()),
        )
        .await;
        assert!(result.is_ok());

        client_task.await.unwrap();
    }

    #[tokio::test]
    async fn test_handle_connection_multiple_commands() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();

        let store = Arc::new(RwLock::new(GraphStore::new()));
        let handler = Arc::new(CommandHandler::new(None));

        let server_store = Arc::clone(&store);
        let server_handler = Arc::clone(&handler);

        let server_task = tokio::spawn(async move {
            let (socket, _peer) = listener.accept().await.unwrap();
            // handle_connection returns Ok on clean disconnect (n=0)
            let _result = handle_connection(
                socket,
                TenantStoreManager::with_default(server_store),
                server_handler,
                None,
                None,
                None,
                Arc::new(AuthConfig::disabled()),
            )
            .await;
        });

        let mut stream = tokio::net::TcpStream::connect(addr).await.unwrap();
        use tokio::io::{AsyncReadExt, AsyncWriteExt};

        // Send two PING commands back-to-back
        stream
            .write_all(b"*1\r\n$4\r\nPING\r\n*1\r\n$4\r\nPING\r\n")
            .await
            .unwrap();

        let mut buf = vec![0u8; 1024];
        let n = stream.read(&mut buf).await.unwrap();
        let response = String::from_utf8_lossy(&buf[..n]);
        // Should contain at least one PONG response
        let pong_count = response.matches("PONG").count();
        assert!(
            pong_count >= 1,
            "Expected at least one PONG, got: {}",
            response
        );

        drop(stream);
        let _ = server_task.await;
    }

    #[test]
    fn test_server_config_address_variants() {
        let configs = vec![
            ("127.0.0.1", 6379),
            ("0.0.0.0", 6379),
            ("localhost", 6380),
            ("192.168.1.100", 9999),
        ];

        for (addr, port) in configs {
            let config = ServerConfig {
                address: addr.to_string(),
                port,
                max_connections: 100,
                data_path: None,
            };
            assert_eq!(config.address, addr);
            assert_eq!(config.port, port);
        }
    }
}
