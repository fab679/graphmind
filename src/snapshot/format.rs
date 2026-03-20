//! Snapshot format types for `.sgsnap` files.
//!
//! Format: gzip-compressed JSON-lines (one JSON object per line).
//! Line 0 is the header, lines 1..N are nodes, lines N+1..M are edges.

use serde::{Deserialize, Serialize};
use std::collections::HashMap;

/// Header line (line 0) of a .sgsnap file
#[derive(Debug, Serialize, Deserialize)]
pub struct SnapshotHeader {
    pub format: String, // Always "sgsnap"
    pub version: u32,   // Format version (currently 1)
    pub tenant: String, // Tenant ID that was exported
    pub node_count: u64,
    pub edge_count: u64,
    pub labels: Vec<String>,
    pub edge_types: Vec<String>,
    pub created_at: String, // ISO 8601
    pub graphmind_version: String,
}

/// A node record in the snapshot
#[derive(Debug, Serialize, Deserialize)]
pub struct SnapshotNode {
    pub t: String, // Always "n"
    pub id: u64,   // Original NodeId
    pub labels: Vec<String>,
    pub props: HashMap<String, serde_json::Value>,
}

/// An edge record in the snapshot
#[derive(Debug, Serialize, Deserialize)]
pub struct SnapshotEdge {
    pub t: String, // Always "e"
    pub id: u64,   // Original EdgeId
    pub src: u64,  // Source NodeId
    pub tgt: u64,  // Target NodeId
    #[serde(rename = "type")]
    pub edge_type: String,
    pub props: HashMap<String, serde_json::Value>,
}

/// Stats returned from export
#[derive(Debug)]
pub struct ExportStats {
    pub node_count: u64,
    pub edge_count: u64,
    pub labels: Vec<String>,
    pub edge_types: Vec<String>,
    pub bytes_written: u64,
}

/// Stats returned from import
#[derive(Debug)]
pub struct ImportStats {
    pub node_count: u64,
    pub edge_count: u64,
    pub labels: Vec<String>,
    pub edge_types: Vec<String>,
}
