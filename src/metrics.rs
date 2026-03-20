//! Prometheus metrics for Graphmind
//!
//! Exposes metrics at GET /metrics in Prometheus text format.

use metrics::{counter, gauge, histogram};
use metrics_exporter_prometheus::{PrometheusBuilder, PrometheusHandle};
use std::sync::OnceLock;

static HANDLE: OnceLock<PrometheusHandle> = OnceLock::new();

/// Initialize the Prometheus metrics recorder. Call once at startup.
pub fn init_metrics() {
    let handle = PrometheusBuilder::new()
        .install_recorder()
        .expect("Failed to install Prometheus recorder");
    HANDLE.set(handle).ok();
}

/// Get the global Prometheus handle (for rendering metrics)
pub fn get_handle() -> Option<&'static PrometheusHandle> {
    HANDLE.get()
}

// --- Metric recording functions ---

/// Record a query execution
pub fn record_query(duration_ms: f64, is_write: bool) {
    let qtype = if is_write { "write" } else { "read" };
    counter!("graphmind_queries_total", "type" => qtype).increment(1);
    histogram!("graphmind_query_duration_ms", "type" => qtype).record(duration_ms);
}

/// Update storage gauge metrics
pub fn update_storage_gauges(nodes: u64, edges: u64) {
    gauge!("graphmind_nodes_total").set(nodes as f64);
    gauge!("graphmind_edges_total").set(edges as f64);
}

/// Record a RESP connection
pub fn record_resp_connection(active: bool) {
    if active {
        gauge!("graphmind_resp_connections_active").increment(1.0);
        counter!("graphmind_resp_connections_total").increment(1);
    } else {
        gauge!("graphmind_resp_connections_active").decrement(1.0);
    }
}

/// Record script execution
pub fn record_script_execution(statements: u64, errors: u64) {
    counter!("graphmind_script_statements_total").increment(statements);
    counter!("graphmind_script_errors_total").increment(errors);
}
