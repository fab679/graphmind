//! Graphmind SDK — Client library for the Graphmind Graph Database
//!
//! Provides two client implementations:
//!
//! - **`EmbeddedClient`** — In-process, no network. Uses `GraphStore` and `QueryEngine`
//!   directly. Ideal for tests, examples, and embedded applications.
//!
//! - **`RemoteClient`** — Connects to a running Graphmind server via HTTP.
//!   For production client applications.
//!
//! Both implement the `GraphmindClient` trait for a unified API.
//!
//! Extension traits (EmbeddedClient only):
//! - **`AlgorithmClient`** — PageRank, WCC, SCC, BFS, Dijkstra, and more
//! - **`VectorClient`** — Vector index creation, insertion, and k-NN search
//!
//! # Quick Start
//!
//! ```rust
//! use graphmind_sdk::{EmbeddedClient, GraphmindClient};
//!
//! #[tokio::main]
//! async fn main() {
//!     let client = EmbeddedClient::new();
//!
//!     // Create data
//!     client.query("default", r#"CREATE (n:Person {name: "Alice"})"#)
//!         .await.unwrap();
//!
//!     // Query data
//!     let result = client.query_readonly("default", "MATCH (n:Person) RETURN n.name")
//!         .await.unwrap();
//!     println!("Found {} records", result.len());
//! }
//! ```

pub mod algo;
pub mod client;
pub mod embedded;
pub mod error;
pub mod models;
pub mod remote;
pub mod vector_ext;

// ============================================================
// Core SDK types
// ============================================================

pub use client::GraphmindClient;
pub use embedded::EmbeddedClient;
pub use error::{GraphmindError, GraphmindResult};
pub use models::{QueryResult, SdkEdge, SdkNode, ServerStatus, StorageStats};
pub use remote::RemoteClient;

// ============================================================
// Extension traits (EmbeddedClient only)
// ============================================================

pub use algo::AlgorithmClient;
pub use vector_ext::VectorClient;

// ============================================================
// Graph types (re-exported from graphmind core)
// ============================================================

pub use graphmind::graph::{
    Edge, EdgeId, EdgeType, GraphError, GraphResult, GraphStore, Label, Node, NodeId, PropertyMap,
    PropertyValue,
};
pub use graphmind::query::{CacheStats, QueryEngine, RecordBatch};

// ============================================================
// Algorithm types (re-exported from graphmind-graph-algorithms)
// ============================================================

pub use graphmind::algo::{
    bfs, build_view, count_triangles, dijkstra, edmonds_karp, page_rank, pca, prim_mst,
    strongly_connected_components, weakly_connected_components, FlowResult, MSTResult,
    PageRankConfig, PathResult, PcaConfig, PcaResult, PcaSolver, SccResult, WccResult,
};
pub use graphmind_graph_algorithms::GraphView;

// ============================================================
// Vector types (re-exported from graphmind core)
// ============================================================

pub use graphmind::vector::{
    DistanceMetric, IndexKey, VectorError, VectorIndex, VectorIndexManager, VectorResult,
};

// ============================================================
// NLQ types (re-exported from graphmind core)
// ============================================================

pub use graphmind::{NLQError, NLQPipeline, NLQResult};

// ============================================================
// Agent types (re-exported from graphmind core)
// ============================================================

pub use graphmind::agent::AgentRuntime;

// ============================================================
// Persistence & Multi-Tenancy types (re-exported from graphmind core)
// ============================================================

pub use graphmind::persistence::tenant::{AgentConfig, ToolConfig};
pub use graphmind::{
    AutoEmbedConfig, LLMProvider, NLQConfig, PersistenceError, PersistenceManager,
    PersistenceResult, PersistentStorage, StorageError, StorageResult, Wal, WalEntry, WalError,
    WalResult,
};

// ============================================================
// Optimization types (re-exported from graphmind-optimization)
// ============================================================

pub use graphmind_optimization::algorithms::{
    ABCSolver, BatSolver, CuckooSolver, DESolver, FireflySolver, GASolver, GWOSolver, JayaSolver,
    NSGA2Solver, PSOSolver, RaoSolver, SASolver, TLBOSolver,
};
pub use graphmind_optimization::{
    Individual, MultiObjectiveIndividual, MultiObjectiveProblem, MultiObjectiveResult,
    OptimizationResult, Problem, SimpleProblem, SolverConfig,
};

// ============================================================
// ndarray (re-exported for optimization problem definitions)
// ============================================================

pub use ndarray::Array1;

// ============================================================
// Version
// ============================================================

pub use graphmind::VERSION;
