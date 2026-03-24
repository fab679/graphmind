//! Python bindings for the Graphmind Graph Database SDK
//!
//! Exposes GraphmindClient with both embedded and remote modes to Python.

use pyo3::prelude::*;
use pyo3::exceptions::PyRuntimeError;
use pyo3::types::PyDict;
use graphmind_sdk::{
    EmbeddedClient, RemoteClient, GraphmindClient as GraphmindClientTrait,
    QueryResult as SdkQueryResult,
    AlgorithmClient, PageRankConfig, PcaConfig,
    VectorClient, DistanceMetric, NodeId,
};
use std::collections::HashMap;
use std::sync::Arc;
use tokio::runtime::Runtime;

/// Create a shared tokio runtime for all async operations
fn get_runtime() -> &'static Runtime {
    use std::sync::OnceLock;
    static RT: OnceLock<Runtime> = OnceLock::new();
    RT.get_or_init(|| {
        Runtime::new().expect("Failed to create tokio runtime")
    })
}

/// Query result returned from Cypher queries
#[pyclass]
struct QueryResult {
    #[pyo3(get)]
    columns: Vec<String>,
    records_json: Vec<Vec<serde_json::Value>>,
    nodes_json: Vec<serde_json::Value>,
    edges_json: Vec<serde_json::Value>,
}

#[pymethods]
impl QueryResult {
    fn __len__(&self) -> usize {
        self.records_json.len()
    }

    fn __repr__(&self) -> String {
        format!("QueryResult(columns={:?}, records={})", self.columns, self.records_json.len())
    }

    #[getter]
    fn records(&self, py: Python<'_>) -> PyResult<PyObject> {
        json_to_py(py, &serde_json::Value::Array(
            self.records_json.iter().map(|row| serde_json::Value::Array(row.clone())).collect()
        ))
    }

    #[getter]
    fn nodes(&self, py: Python<'_>) -> PyResult<PyObject> {
        json_to_py(py, &serde_json::Value::Array(self.nodes_json.clone()))
    }

    #[getter]
    fn edges(&self, py: Python<'_>) -> PyResult<PyObject> {
        json_to_py(py, &serde_json::Value::Array(self.edges_json.clone()))
    }
}

/// Server status information
#[pyclass]
#[derive(Clone)]
struct ServerStatus {
    #[pyo3(get)]
    status: String,
    #[pyo3(get)]
    version: String,
    #[pyo3(get)]
    nodes: u64,
    #[pyo3(get)]
    edges: u64,
}

#[pymethods]
impl ServerStatus {
    fn __repr__(&self) -> String {
        format!(
            "ServerStatus(status='{}', version='{}', nodes={}, edges={})",
            self.status, self.version, self.nodes, self.edges
        )
    }
}

/// Convert SDK QueryResult to Python QueryResult
fn convert_query_result(result: SdkQueryResult) -> PyResult<QueryResult> {
    let nodes_json: Vec<serde_json::Value> = result.nodes.iter().map(|n| {
        serde_json::json!({
            "id": n.id,
            "labels": n.labels,
            "properties": n.properties,
        })
    }).collect();

    let edges_json: Vec<serde_json::Value> = result.edges.iter().map(|e| {
        serde_json::json!({
            "id": e.id,
            "source": e.source,
            "target": e.target,
            "type": e.edge_type,
            "properties": e.properties,
        })
    }).collect();

    Ok(QueryResult {
        columns: result.columns,
        records_json: result.records,
        nodes_json,
        edges_json,
    })
}

/// Convert a serde_json::Value to a Python object
fn json_to_py(py: Python<'_>, value: &serde_json::Value) -> PyResult<PyObject> {
    match value {
        serde_json::Value::Null => Ok(py.None()),
        serde_json::Value::Bool(b) => Ok(b.to_object(py)),
        serde_json::Value::Number(n) => {
            if let Some(i) = n.as_i64() {
                Ok(i.to_object(py))
            } else if let Some(f) = n.as_f64() {
                Ok(f.to_object(py))
            } else {
                Ok(py.None())
            }
        }
        serde_json::Value::String(s) => Ok(s.to_object(py)),
        serde_json::Value::Array(arr) => {
            let list: Vec<PyObject> = arr.iter()
                .map(|v| json_to_py(py, v))
                .collect::<PyResult<_>>()?;
            Ok(list.to_object(py))
        }
        serde_json::Value::Object(map) => {
            let dict = PyDict::new_bound(py);
            for (k, v) in map {
                dict.set_item(k, json_to_py(py, v)?)?;
            }
            Ok(dict.to_object(py))
        }
    }
}

/// Convert a Python dict to a HashMap<String, serde_json::Value> for query parameters
fn py_dict_to_json_map(dict: &Bound<'_, PyDict>) -> PyResult<HashMap<String, serde_json::Value>> {
    let mut map = HashMap::new();
    for (key, value) in dict.iter() {
        let k: String = key.extract()?;
        let v = py_to_json(value)?;
        map.insert(k, v);
    }
    Ok(map)
}

/// Convert a Python object to serde_json::Value
fn py_to_json(obj: pyo3::Bound<'_, pyo3::PyAny>) -> PyResult<serde_json::Value> {
    if obj.is_none() {
        Ok(serde_json::Value::Null)
    } else if let Ok(b) = obj.extract::<bool>() {
        Ok(serde_json::Value::Bool(b))
    } else if let Ok(i) = obj.extract::<i64>() {
        Ok(serde_json::json!(i))
    } else if let Ok(f) = obj.extract::<f64>() {
        Ok(serde_json::json!(f))
    } else if let Ok(s) = obj.extract::<String>() {
        Ok(serde_json::Value::String(s))
    } else if let Ok(list) = obj.extract::<Vec<pyo3::Bound<'_, pyo3::PyAny>>>() {
        let arr: Vec<serde_json::Value> = list
            .into_iter()
            .map(py_to_json)
            .collect::<PyResult<_>>()?;
        Ok(serde_json::Value::Array(arr))
    } else {
        // Fallback: convert to string
        Ok(serde_json::Value::String(obj.str()?.to_string()))
    }
}

/// Internal enum to hold either embedded or remote client
enum ClientInner {
    Embedded(EmbeddedClient),
    Remote(RemoteClient),
}

/// Python client for the Graphmind Graph Database.
///
/// Create with `GraphmindClient.embedded()` for in-process mode
/// or `GraphmindClient.connect(url)` for remote mode.
#[pyclass]
struct GraphmindClient {
    inner: Arc<ClientInner>,
}

impl GraphmindClient {
    fn require_embedded(&self) -> PyResult<&EmbeddedClient> {
        match &*self.inner {
            ClientInner::Embedded(c) => Ok(c),
            ClientInner::Remote(_) => Err(PyRuntimeError::new_err(
                "Algorithm methods are only available in embedded mode. Use GraphmindClient.embedded()."
            )),
        }
    }
}

#[pymethods]
impl GraphmindClient {
    /// Create an in-process embedded client (no server needed)
    #[staticmethod]
    fn embedded() -> PyResult<Self> {
        Ok(GraphmindClient {
            inner: Arc::new(ClientInner::Embedded(EmbeddedClient::new())),
        })
    }

    /// Connect to a running Graphmind server via HTTP
    #[staticmethod]
    fn connect(url: &str) -> PyResult<Self> {
        Ok(GraphmindClient {
            inner: Arc::new(ClientInner::Remote(RemoteClient::new(url))),
        })
    }

    /// Execute a Cypher query
    #[pyo3(signature = (cypher, graph="default", params=None))]
    fn query(&self, cypher: &str, graph: &str, params: Option<&Bound<'_, PyDict>>) -> PyResult<QueryResult> {
        let rt = get_runtime();
        let result = if let Some(py_params) = params {
            let json_params = py_dict_to_json_map(py_params)?;
            match &*self.inner {
                ClientInner::Embedded(c) => rt.block_on(c.query_with_params(graph, cypher, json_params)),
                ClientInner::Remote(c) => rt.block_on(c.query_with_params(graph, cypher, json_params)),
            }
        } else {
            match &*self.inner {
                ClientInner::Embedded(c) => rt.block_on(c.query(graph, cypher)),
                ClientInner::Remote(c) => rt.block_on(c.query(graph, cypher)),
            }
        };
        match result {
            Ok(r) => convert_query_result(r),
            Err(e) => Err(PyRuntimeError::new_err(e.to_string())),
        }
    }

    /// Execute a read-only Cypher query
    #[pyo3(signature = (cypher, graph="default", params=None))]
    fn query_readonly(&self, cypher: &str, graph: &str, params: Option<&Bound<'_, PyDict>>) -> PyResult<QueryResult> {
        let rt = get_runtime();
        let result = if let Some(py_params) = params {
            let json_params = py_dict_to_json_map(py_params)?;
            match &*self.inner {
                ClientInner::Embedded(c) => rt.block_on(c.query_with_params(graph, cypher, json_params)),
                ClientInner::Remote(c) => rt.block_on(c.query_with_params(graph, cypher, json_params)),
            }
        } else {
            match &*self.inner {
                ClientInner::Embedded(c) => rt.block_on(c.query_readonly(graph, cypher)),
                ClientInner::Remote(c) => rt.block_on(c.query_readonly(graph, cypher)),
            }
        };
        match result {
            Ok(r) => convert_query_result(r),
            Err(e) => Err(PyRuntimeError::new_err(e.to_string())),
        }
    }

    /// Get server status
    #[pyo3(signature = (graph="default"))]
    fn status(&self, graph: &str) -> PyResult<ServerStatus> {
        let rt = get_runtime();
        let result = match &*self.inner {
            ClientInner::Embedded(c) => rt.block_on(c.status(graph)),
            ClientInner::Remote(c) => rt.block_on(c.status(graph)),
        };
        match result {
            Ok(s) => Ok(ServerStatus {
                status: s.status,
                version: s.version,
                nodes: s.storage.nodes,
                edges: s.storage.edges,
            }),
            Err(e) => Err(PyRuntimeError::new_err(e.to_string())),
        }
    }

    /// Ping the server
    fn ping(&self) -> PyResult<String> {
        let rt = get_runtime();
        let result = match &*self.inner {
            ClientInner::Embedded(c) => rt.block_on(c.ping()),
            ClientInner::Remote(c) => rt.block_on(c.ping()),
        };
        result.map_err(|e| PyRuntimeError::new_err(e.to_string()))
    }

    /// Delete a graph
    #[pyo3(signature = (graph="default"))]
    fn delete_graph(&self, graph: &str) -> PyResult<()> {
        let rt = get_runtime();
        let result = match &*self.inner {
            ClientInner::Embedded(c) => rt.block_on(c.delete_graph(graph)),
            ClientInner::Remote(c) => rt.block_on(c.delete_graph(graph)),
        };
        result.map_err(|e| PyRuntimeError::new_err(e.to_string()))
    }

    /// List graphs
    fn list_graphs(&self) -> PyResult<Vec<String>> {
        let rt = get_runtime();
        let result = match &*self.inner {
            ClientInner::Embedded(c) => rt.block_on(c.list_graphs()),
            ClientInner::Remote(c) => rt.block_on(c.list_graphs()),
        };
        result.map_err(|e| PyRuntimeError::new_err(e.to_string()))
    }

    // ========================================================================
    // Algorithm methods (embedded mode only)
    // ========================================================================

    /// Run PageRank on the graph.
    /// Returns dict mapping node_id -> score.
    #[pyo3(signature = (label=None, edge_type=None, damping=0.85, iterations=20, tolerance=1e-6))]
    fn page_rank(
        &self,
        py: Python<'_>,
        label: Option<&str>,
        edge_type: Option<&str>,
        damping: f64,
        iterations: usize,
        tolerance: f64,
    ) -> PyResult<PyObject> {
        let client = self.require_embedded()?;
        let rt = get_runtime();
        let config = PageRankConfig {
            damping_factor: damping,
            iterations,
            tolerance,
            ..Default::default()
        };
        let scores: HashMap<u64, f64> = rt.block_on(client.page_rank(config, label, edge_type));
        let dict = PyDict::new_bound(py);
        for (k, v) in &scores {
            dict.set_item(k, v)?;
        }
        Ok(dict.to_object(py))
    }

    /// Detect weakly connected components.
    /// Returns dict with 'components' (dict of component_id -> list of node IDs) and 'component_count'.
    #[pyo3(signature = (label=None, edge_type=None))]
    fn wcc(&self, py: Python<'_>, label: Option<&str>, edge_type: Option<&str>) -> PyResult<PyObject> {
        let client = self.require_embedded()?;
        let rt = get_runtime();
        let result = rt.block_on(client.weakly_connected_components(label, edge_type));
        let dict = PyDict::new_bound(py);
        let component_count = result.components.len();
        let components_dict = PyDict::new_bound(py);
        for (k, v) in &result.components {
            components_dict.set_item(k, v.to_object(py))?;
        }
        dict.set_item("components", components_dict)?;
        dict.set_item("component_count", component_count)?;
        Ok(dict.to_object(py))
    }

    /// Detect strongly connected components.
    /// Returns dict with 'components' and 'component_count'.
    #[pyo3(signature = (label=None, edge_type=None))]
    fn scc(&self, py: Python<'_>, label: Option<&str>, edge_type: Option<&str>) -> PyResult<PyObject> {
        let client = self.require_embedded()?;
        let rt = get_runtime();
        let result = rt.block_on(client.strongly_connected_components(label, edge_type));
        let dict = PyDict::new_bound(py);
        let component_count = result.components.len();
        let components_dict = PyDict::new_bound(py);
        for (k, v) in &result.components {
            components_dict.set_item(k, v.to_object(py))?;
        }
        dict.set_item("components", components_dict)?;
        dict.set_item("component_count", component_count)?;
        Ok(dict.to_object(py))
    }

    /// Breadth-first search from source to target.
    /// Returns dict with 'path' (list of node IDs) and 'distance', or None if no path.
    #[pyo3(signature = (source, target, label=None, edge_type=None))]
    fn bfs(
        &self,
        py: Python<'_>,
        source: u64,
        target: u64,
        label: Option<&str>,
        edge_type: Option<&str>,
    ) -> PyResult<PyObject> {
        let client = self.require_embedded()?;
        let rt = get_runtime();
        let result = rt.block_on(client.bfs(source, target, label, edge_type));
        match result {
            Some(path) => {
                let dict = PyDict::new_bound(py);
                dict.set_item("path", path.path.to_object(py))?;
                dict.set_item("cost", path.cost)?;
                Ok(dict.to_object(py))
            }
            None => Ok(py.None()),
        }
    }

    /// Dijkstra's shortest path from source to target (weighted).
    /// Returns dict with 'path' and 'distance', or None if no path.
    #[pyo3(signature = (source, target, label=None, edge_type=None, weight_property=None))]
    fn dijkstra(
        &self,
        py: Python<'_>,
        source: u64,
        target: u64,
        label: Option<&str>,
        edge_type: Option<&str>,
        weight_property: Option<&str>,
    ) -> PyResult<PyObject> {
        let client = self.require_embedded()?;
        let rt = get_runtime();
        let result = rt.block_on(client.dijkstra(source, target, label, edge_type, weight_property));
        match result {
            Some(path) => {
                let dict = PyDict::new_bound(py);
                dict.set_item("path", path.path.to_object(py))?;
                dict.set_item("cost", path.cost)?;
                Ok(dict.to_object(py))
            }
            None => Ok(py.None()),
        }
    }

    /// Run PCA on node numeric properties.
    /// Returns dict with 'components', 'explained_variance', 'explained_variance_ratio',
    /// 'mean', 'std_dev', 'n_samples', 'n_features'.
    #[pyo3(signature = (properties, label=None, n_components=2))]
    fn pca(
        &self,
        py: Python<'_>,
        properties: Vec<String>,
        label: Option<&str>,
        n_components: usize,
    ) -> PyResult<PyObject> {
        let client = self.require_embedded()?;
        let rt = get_runtime();
        let config = PcaConfig {
            n_components,
            ..PcaConfig::default()
        };
        let props_refs: Vec<&str> = properties.iter().map(|s| s.as_str()).collect();
        let result = rt.block_on(client.pca(label, &props_refs, config));
        let dict = PyDict::new_bound(py);
        // Convert components (Vec<Vec<f64>>) to list of lists
        let components: Vec<Vec<f64>> = result.components;
        dict.set_item("components", components.to_object(py))?;
        dict.set_item("explained_variance", result.explained_variance.to_object(py))?;
        dict.set_item("explained_variance_ratio", result.explained_variance_ratio.to_object(py))?;
        dict.set_item("mean", result.mean.to_object(py))?;
        dict.set_item("std_dev", result.std_dev.to_object(py))?;
        dict.set_item("n_samples", result.n_samples)?;
        dict.set_item("n_features", result.n_features)?;
        Ok(dict.to_object(py))
    }

    /// Count triangles in the graph.
    #[pyo3(signature = (label=None, edge_type=None))]
    fn triangle_count(&self, label: Option<&str>, edge_type: Option<&str>) -> PyResult<usize> {
        let client = self.require_embedded()?;
        let rt = get_runtime();
        Ok(rt.block_on(client.count_triangles(label, edge_type)))
    }

    // ========================================================================
    // Vector methods (embedded mode only)
    // ========================================================================

    /// Create a vector index for a label/property.
    /// metric: "cosine", "l2", or "dot"
    #[pyo3(signature = (label, property, dimensions, metric="cosine"))]
    fn create_vector_index(
        &self,
        label: &str,
        property: &str,
        dimensions: usize,
        metric: &str,
    ) -> PyResult<()> {
        let client = self.require_embedded()?;
        let rt = get_runtime();
        let dist = match metric {
            "l2" => DistanceMetric::L2,
            "dot" | "inner_product" => DistanceMetric::InnerProduct,
            _ => DistanceMetric::Cosine,
        };
        rt.block_on(client.create_vector_index(label, property, dimensions, dist))
            .map_err(|e| PyRuntimeError::new_err(e.to_string()))
    }

    /// Add a vector for a node in a vector index.
    fn add_vector(
        &self,
        label: &str,
        property: &str,
        node_id: u64,
        vector: Vec<f32>,
    ) -> PyResult<()> {
        let client = self.require_embedded()?;
        let rt = get_runtime();
        rt.block_on(client.add_vector(label, property, NodeId(node_id), &vector))
            .map_err(|e| PyRuntimeError::new_err(e.to_string()))
    }

    /// Search for k nearest neighbors. Returns list of (node_id, distance) tuples.
    #[pyo3(signature = (label, property, query_vector, k=10))]
    fn vector_search(
        &self,
        label: &str,
        property: &str,
        query_vector: Vec<f32>,
        k: usize,
    ) -> PyResult<Vec<(u64, f32)>> {
        let client = self.require_embedded()?;
        let rt = get_runtime();
        let results = rt.block_on(client.vector_search(label, property, &query_vector, k))
            .map_err(|e| PyRuntimeError::new_err(e.to_string()))?;
        Ok(results.into_iter().map(|(nid, dist)| (nid.0, dist)).collect())
    }

    /// Get schema summary for the graph.
    /// In embedded mode, returns a string with node/edge type counts.
    /// In remote mode, fetches from the /api/schema endpoint.
    #[pyo3(signature = (graph="default"))]
    fn schema(&self, graph: &str) -> PyResult<String> {
        let rt = get_runtime();
        let result = match &*self.inner {
            ClientInner::Embedded(c) => rt.block_on(c.schema(graph)),
            ClientInner::Remote(c) => rt.block_on(c.schema(graph)),
        };
        result.map_err(|e| PyRuntimeError::new_err(e.to_string()))
    }

    /// Return the EXPLAIN plan for a Cypher query without executing it.
    #[pyo3(signature = (cypher, graph="default"))]
    fn explain(&self, cypher: &str, graph: &str) -> PyResult<QueryResult> {
        let rt = get_runtime();
        let result = match &*self.inner {
            ClientInner::Embedded(c) => rt.block_on(c.explain(graph, cypher)),
            ClientInner::Remote(c) => rt.block_on(c.explain(graph, cypher)),
        };
        match result {
            Ok(r) => convert_query_result(r),
            Err(e) => Err(PyRuntimeError::new_err(e.to_string())),
        }
    }

    /// Execute a Cypher query with PROFILE instrumentation.
    #[pyo3(signature = (cypher, graph="default"))]
    fn profile(&self, cypher: &str, graph: &str) -> PyResult<QueryResult> {
        let rt = get_runtime();
        let result = match &*self.inner {
            ClientInner::Embedded(c) => rt.block_on(c.profile(graph, cypher)),
            ClientInner::Remote(c) => rt.block_on(c.profile(graph, cypher)),
        };
        match result {
            Ok(r) => convert_query_result(r),
            Err(e) => Err(PyRuntimeError::new_err(e.to_string())),
        }
    }

    /// Execute a multi-statement Cypher script.
    /// Splits on semicolons and executes each statement in order.
    /// Returns a list of QueryResult objects.
    #[pyo3(signature = (script, graph="default"))]
    fn execute_script(&self, script: &str, graph: &str) -> PyResult<Vec<QueryResult>> {
        let rt = get_runtime();
        let statements: Vec<&str> = script.split(';')
            .map(|s| s.trim())
            .filter(|s| !s.is_empty())
            .collect();
        let mut results = Vec::new();
        for stmt in statements {
            let result = match &*self.inner {
                ClientInner::Embedded(c) => rt.block_on(c.query(graph, stmt)),
                ClientInner::Remote(c) => rt.block_on(c.query(graph, stmt)),
            };
            match result {
                Ok(r) => results.push(convert_query_result(r)?),
                Err(e) => return Err(PyRuntimeError::new_err(e.to_string())),
            }
        }
        Ok(results)
    }

    fn __repr__(&self) -> String {
        match &*self.inner {
            ClientInner::Embedded(_) => "GraphmindClient(mode='embedded')".to_string(),
            ClientInner::Remote(_) => "GraphmindClient(mode='remote')".to_string(),
        }
    }
}

/// Python module for the Graphmind Graph Database
#[pymodule]
fn graphmind(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_class::<GraphmindClient>()?;
    m.add_class::<QueryResult>()?;
    m.add_class::<ServerStatus>()?;
    Ok(())
}
