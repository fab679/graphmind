//! Audit logging for Graphmind
//!
//! Records all mutating operations (CREATE, DELETE, SET, MERGE) in a structured
//! JSON-lines format for compliance and forensics.

use chrono::Utc;
use serde::Serialize;
use std::fs::{File, OpenOptions};
use std::io::Write;
use std::path::PathBuf;
use std::sync::Mutex;

#[derive(Serialize)]
pub struct AuditEntry {
    pub timestamp: String,
    pub operation: String, // "QUERY", "SCRIPT", "NLQ"
    pub query: String,
    pub user: Option<String>, // auth token or "anonymous"
    pub tenant: String,
    pub is_write: bool,
    pub duration_ms: f64,
    pub result: AuditResult,
}

#[derive(Serialize)]
pub enum AuditResult {
    #[serde(rename = "success")]
    Success { rows: usize },
    #[serde(rename = "error")]
    Error { message: String },
}

pub struct AuditLogger {
    file: Option<Mutex<File>>,
    enabled: bool,
}

impl AuditLogger {
    /// Create a new audit logger. Pass None for path to disable.
    pub fn new(path: Option<PathBuf>) -> Self {
        match path {
            Some(p) => {
                let file = OpenOptions::new()
                    .create(true)
                    .append(true)
                    .open(&p)
                    .ok()
                    .map(Mutex::new);
                Self {
                    file,
                    enabled: true,
                }
            }
            None => Self {
                file: None,
                enabled: false,
            },
        }
    }

    pub fn disabled() -> Self {
        Self {
            file: None,
            enabled: false,
        }
    }

    pub fn is_enabled(&self) -> bool {
        self.enabled && self.file.is_some()
    }

    /// Log an audit entry
    pub fn log(&self, entry: &AuditEntry) {
        if !self.is_enabled() {
            return;
        }
        if let Some(ref file) = self.file {
            if let Ok(mut f) = file.lock() {
                if let Ok(json) = serde_json::to_string(entry) {
                    let _ = writeln!(f, "{}", json);
                }
            }
        }
    }

    /// Convenience: log a query execution
    pub fn log_query(
        &self,
        query: &str,
        user: Option<&str>,
        tenant: &str,
        is_write: bool,
        duration_ms: f64,
        result: Result<usize, &str>,
    ) {
        if !self.is_enabled() {
            return;
        }

        let entry = AuditEntry {
            timestamp: Utc::now().to_rfc3339(),
            operation: "QUERY".to_string(),
            query: query.to_string(),
            user: user.map(|s| s.to_string()),
            tenant: tenant.to_string(),
            is_write,
            duration_ms,
            result: match result {
                Ok(rows) => AuditResult::Success { rows },
                Err(msg) => AuditResult::Error {
                    message: msg.to_string(),
                },
            },
        };
        self.log(&entry);
    }
}

// Mutex<File> already provides Send + Sync, so AuditLogger is safe
unsafe impl Send for AuditLogger {}
unsafe impl Sync for AuditLogger {}
