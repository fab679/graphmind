pub mod algorithms;
pub mod common;

/// Re-export common types
pub use common::*;

/// Initialize the optimization engine
pub fn init() {
    tracing::info!("Graphmind Optimization Engine Initialized");
}
