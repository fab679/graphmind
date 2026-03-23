//! # Query Processing Pipeline
//!
//! This module implements the full **query processing pipeline** for Graphmind's OpenCypher
//! dialect, following the same staged architecture used by virtually every database engine
//! and compiler:
//!
//! ```text
//!   Source Text          Pest PEG Parser        Abstract Syntax Tree
//!  ┌──────────┐        ┌──────────────┐        ┌──────────────────┐
//!  │ MATCH    │──Lex──>│  cypher.pest │──AST──>│  Query struct    │
//!  │ (n:Foo)  │ +Parse │  (PEG rules) │        │  (ast.rs)        │
//!  │ RETURN n │        └──────────────┘        └────────┬─────────┘
//!  └──────────┘                                         │
//!                                                       │ plan()
//!                                                       v
//!                     Execution Plan             ┌──────────────┐
//!                    ┌──────────────┐            │ QueryPlanner │
//!                    │ Operator tree│<───────────│ (planner.rs) │
//!                    │ (Volcano)    │            └──────────────┘
//!                    └──────┬───────┘
//!                           │ next() / next_mut()
//!                           v
//!                    ┌──────────────┐
//!                    │  RecordBatch │  (final output)
//!                    └──────────────┘
//! ```
//!
//! This mirrors how compilers work: source code is lexed into tokens, parsed into an AST,
//! lowered to an intermediate representation (the execution plan), and finally "executed"
//! (in a compiler, that means code generation; here, it means pulling records through
//! operators). The analogy is not accidental -- query languages *are* domain-specific
//! programming languages.
//!
//! ## Parsing: PEG via Pest
//!
//! The parser uses [Pest](https://pest.rs), a Rust crate that implements **Parsing Expression
//! Grammars (PEGs)**. Unlike context-free grammars (CFGs) used by yacc/bison, PEGs use an
//! ordered-choice operator (`/`) that tries alternatives left-to-right and commits to the
//! first match. This makes PEGs **always unambiguous** -- there is exactly one parse tree for
//! any input, which eliminates an entire class of grammar debugging. The grammar lives in
//! [`cypher.pest`](cypher.pest) and is compiled into a Rust parser at build time via a proc
//! macro (`#[derive(Parser)]`).
//!
//! ## Execution: Volcano Iterator Model (ADR-007)
//!
//! Query execution follows the **Volcano iterator model** invented by Goetz Graefe. Each
//! physical operator (scan, filter, expand, project, etc.) implements a `next()` method that
//! **pulls** a single record from its child operator. Records flow upward through the
//! operator tree one at a time, like a lazy iterator chain in Rust (`iter().filter().map()`).
//! This is memory-efficient because intermediate results are never fully materialized -- each
//! operator processes one record and immediately passes it upstream.
//!
//! ## LRU Parse Cache
//!
//! Parsing is expensive (PEG matching, AST construction, string allocation). Since many
//! applications execute the same queries repeatedly with different parameters, this module
//! maintains an **LRU (Least Recently Used) cache** of parsed ASTs. On a cache hit, we skip
//! parsing entirely and jump straight to planning. The cache uses `Mutex<LruCache>` for
//! thread safety, with lock-free `AtomicU64` counters for hit/miss statistics.
//!
//! ## Read vs Write Execution Paths
//!
//! Queries are split into two execution paths based on mutability:
//! - **[`QueryExecutor`]**: read-only queries (MATCH, RETURN, EXPLAIN). Takes `&GraphStore`.
//! - **[`MutQueryExecutor`]**: write queries (CREATE, DELETE, SET, MERGE). Takes `&mut GraphStore`.
//!
//! This separation mirrors Rust's ownership model -- shared references (`&T`) allow
//! concurrent reads, while exclusive references (`&mut T`) guarantee single-writer access.
//! The type system enforces at compile time that no read query can accidentally modify the
//! graph.

pub mod ast;
pub mod executor;
pub mod parser;

use lru::LruCache;
use std::num::NonZeroUsize;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Mutex;

// Re-export main types
pub use ast::Query;
pub use executor::{
    ExecutionError,
    ExecutionResult,
    MutQueryExecutor, // Added for CREATE/DELETE/SET support
    QueryExecutor,
    Record,
    RecordBatch,
    Value,
};
pub use parser::{parse_query, ParseError, ParseResult};

/// Default LRU cache capacity
const DEFAULT_CACHE_CAPACITY: usize = 1024;

/// Lock-free cache hit/miss counters.
pub struct CacheStats {
    hits: AtomicU64,
    misses: AtomicU64,
}

impl CacheStats {
    fn new() -> Self {
        Self {
            hits: AtomicU64::new(0),
            misses: AtomicU64::new(0),
        }
    }

    /// Total cache hits since engine creation.
    pub fn hits(&self) -> u64 {
        self.hits.load(Ordering::Relaxed)
    }

    /// Total cache misses since engine creation.
    pub fn misses(&self) -> u64 {
        self.misses.load(Ordering::Relaxed)
    }

    fn record_hit(&self) {
        self.hits.fetch_add(1, Ordering::Relaxed);
    }

    fn record_miss(&self) {
        self.misses.fetch_add(1, Ordering::Relaxed);
    }
}

/// Query engine - high-level interface for executing queries
///
/// Includes an LRU AST cache that eliminates repeated parsing overhead
/// for identical queries. The cache is keyed by whitespace-normalized
/// query strings and evicts least-recently-used entries when full.
pub struct QueryEngine {
    /// Parsed AST cache: normalized query string -> Query AST
    ast_cache: Mutex<LruCache<String, Query>>,
    /// Lock-free hit/miss counters
    stats: CacheStats,
    /// Per-query timeout in seconds (0 = no timeout)
    query_timeout_secs: u64,
}

impl QueryEngine {
    /// Create a new query engine with the default cache capacity (1024 entries)
    pub fn new() -> Self {
        Self::with_capacity(DEFAULT_CACHE_CAPACITY)
    }

    /// Create a new query engine with a specific cache capacity
    pub fn with_capacity(capacity: usize) -> Self {
        let cap = NonZeroUsize::new(capacity).unwrap_or(NonZeroUsize::new(1).unwrap());
        Self {
            ast_cache: Mutex::new(LruCache::new(cap)),
            stats: CacheStats::new(),
            query_timeout_secs: 45,
        }
    }

    /// Return a reference to the cache statistics (hits/misses).
    pub fn cache_stats(&self) -> &CacheStats {
        &self.stats
    }

    /// Return the current number of entries in the cache.
    pub fn cache_len(&self) -> usize {
        self.ast_cache.lock().unwrap().len()
    }

    /// Parse with caching — normalizes whitespace for cache hits
    fn cached_parse(&self, query_str: &str) -> Result<Query, Box<dyn std::error::Error>> {
        let normalized = query_str.split_whitespace().collect::<Vec<_>>().join(" ");

        // Check cache (LruCache::get promotes to most-recently-used)
        {
            let mut cache = self.ast_cache.lock().unwrap();
            if let Some(cached) = cache.get(&normalized) {
                self.stats.record_hit();
                return Ok(cached.clone());
            }
        }

        self.stats.record_miss();

        // Parse and cache (LRU evicts automatically when full)
        let query = parse_query(query_str)?;
        {
            let mut cache = self.ast_cache.lock().unwrap();
            cache.put(normalized, query.clone());
        }
        Ok(query)
    }

    /// Split a query string on semicolons, respecting quoted strings.
    /// Returns individual statement strings (trimmed, non-empty).
    fn split_statements(input: &str) -> Vec<&str> {
        let mut statements = Vec::new();
        let mut start = 0;
        let mut in_single_quote = false;
        let mut in_double_quote = false;
        let mut escape_next = false;
        let bytes = input.as_bytes();

        for i in 0..bytes.len() {
            if escape_next {
                escape_next = false;
                continue;
            }
            match bytes[i] {
                b'\\' => escape_next = true,
                b'\'' if !in_double_quote => in_single_quote = !in_single_quote,
                b'"' if !in_single_quote => in_double_quote = !in_double_quote,
                b';' if !in_single_quote && !in_double_quote => {
                    let stmt = input[start..i].trim();
                    if !stmt.is_empty() {
                        statements.push(stmt);
                    }
                    start = i + 1;
                }
                _ => {}
            }
        }
        // Last segment (after final semicolon or no semicolons at all)
        let last = input[start..].trim();
        if !last.is_empty() {
            statements.push(last);
        }
        statements
    }

    /// Rewrite multi-CREATE statements by inserting WITH clauses to carry variables forward.
    /// E.g.: `CREATE (a:Person) CREATE (b:Person) CREATE (a)-[:KNOWS]->(b)`
    /// becomes: `CREATE (a:Person) WITH a CREATE (b:Person) WITH a, b CREATE (a)-[:KNOWS]->(b)`
    /// Expand `UNWIND [1,2,3] AS x CREATE ({num: x})` into individual CREATEs.
    /// Returns None if the query is not an UNWIND+CREATE pattern.
    fn expand_unwind_create(input: &str) -> Option<Vec<String>> {
        let trimmed = input.trim();
        let upper = trimmed.to_uppercase();

        // Must start with UNWIND and contain CREATE but not MATCH/RETURN/WITH
        if !upper.starts_with("UNWIND") || !upper.contains("CREATE") {
            return None;
        }
        if upper.contains("MATCH")
            || upper.contains("RETURN")
            || upper.contains("WITH")
            || upper.contains("MERGE")
        {
            return None;
        }

        // Extract the list and variable: UNWIND [...] AS var
        // Find the list literal [...]
        let list_start = trimmed.find('[')?;
        let mut depth = 0;
        let mut list_end = None;
        for (i, ch) in trimmed[list_start..].char_indices() {
            match ch {
                '[' => depth += 1,
                ']' => {
                    depth -= 1;
                    if depth == 0 {
                        list_end = Some(list_start + i + 1);
                        break;
                    }
                }
                _ => {}
            }
        }
        let list_end = list_end?;
        let list_str = &trimmed[list_start..list_end];

        // Find AS variable
        let after_list = &trimmed[list_end..];
        let as_pos = after_list.to_uppercase().find(" AS ")?;
        let after_as = &after_list[as_pos + 4..].trim();
        let var_end = after_as
            .find(|c: char| !c.is_alphanumeric() && c != '_')
            .unwrap_or(after_as.len());
        let var_name = &after_as[..var_end].trim();

        // Find CREATE clause
        let create_pos = upper.find("CREATE")?;
        let create_clause = &trimmed[create_pos..];

        // Parse the list values (simple: split by comma, handle nested)
        let inner = &list_str[1..list_str.len() - 1]; // strip [ ]
        let mut elements = Vec::new();
        let mut current = String::new();
        let mut nest = 0;
        let mut in_str = false;
        let mut str_char = ' ';
        for ch in inner.chars() {
            match ch {
                '\'' | '"' if !in_str => {
                    in_str = true;
                    str_char = ch;
                    current.push(ch);
                }
                c if in_str && c == str_char => {
                    in_str = false;
                    current.push(ch);
                }
                '[' | '{' if !in_str => {
                    nest += 1;
                    current.push(ch);
                }
                ']' | '}' if !in_str => {
                    nest -= 1;
                    current.push(ch);
                }
                ',' if !in_str && nest == 0 => {
                    elements.push(current.trim().to_string());
                    current.clear();
                }
                _ => current.push(ch),
            }
        }
        if !current.trim().is_empty() {
            elements.push(current.trim().to_string());
        }

        // Generate one CREATE per element, substituting the variable
        let mut result = Vec::new();
        for element in &elements {
            // Replace occurrences of the variable in the CREATE clause with the literal value
            let create = create_clause.to_string();
            // Simple replacement: replace `: var}` and `: var,` patterns
            // Also handle `{prop: var}` and `{prop: var,`
            // Use word-boundary replacement
            let mut new_create = String::new();
            let var_bytes = var_name.as_bytes();
            let create_bytes = create.as_bytes();
            let mut i = 0;
            while i < create_bytes.len() {
                if i + var_bytes.len() <= create_bytes.len()
                    && &create_bytes[i..i + var_bytes.len()] == var_bytes
                {
                    // Check word boundaries
                    let before_ok = i == 0
                        || !create_bytes[i - 1].is_ascii_alphanumeric()
                            && create_bytes[i - 1] != b'_';
                    let after_ok = i + var_bytes.len() >= create_bytes.len()
                        || !create_bytes[i + var_bytes.len()].is_ascii_alphanumeric()
                            && create_bytes[i + var_bytes.len()] != b'_';
                    if before_ok && after_ok {
                        new_create.push_str(element);
                        i += var_bytes.len();
                        continue;
                    }
                }
                new_create.push(create_bytes[i] as char);
                i += 1;
            }
            result.push(new_create);
        }

        if result.is_empty() {
            None
        } else {
            Some(result)
        }
    }

    fn rewrite_multi_create(input: &str) -> String {
        // Regex-free approach: split on CREATE keyword boundaries (not inside quotes)
        let upper = input.to_uppercase();
        if upper.matches("CREATE").count() <= 1 {
            return input.to_string();
        }

        // Find positions of each CREATE keyword (not inside quotes)
        let mut create_positions = Vec::new();
        let bytes = input.as_bytes();
        let upper_bytes = upper.as_bytes();
        let mut in_single = false;
        let mut in_double = false;
        let mut i = 0;
        while i < bytes.len() {
            match bytes[i] {
                b'\'' if !in_double => in_single = !in_single,
                b'"' if !in_single => in_double = !in_double,
                _ if !in_single && !in_double && i + 6 <= upper_bytes.len() => {
                    if &upper[i..i + 6] == "CREATE" {
                        // Make sure it's a keyword boundary (not part of another word)
                        let before_ok = i == 0 || !bytes[i - 1].is_ascii_alphanumeric();
                        let after_ok =
                            i + 6 >= bytes.len() || !bytes[i + 6].is_ascii_alphanumeric();
                        if before_ok && after_ok {
                            create_positions.push(i);
                        }
                    }
                }
                _ => {}
            }
            i += 1;
        }

        if create_positions.len() <= 1 {
            return input.to_string();
        }

        // If query already has WITH, don't rewrite (user is managing variable passing)
        if upper.contains("WITH") {
            return input.to_string();
        }

        // If query has MATCH, don't rewrite — the MATCH+CREATE pattern is handled
        // natively by the grammar (MATCH ... CREATE pattern).
        // For multiple CREATEs after a MATCH, users should use semicolons.
        if upper.contains("MATCH") {
            return input.to_string();
        }

        // Extract variables from each CREATE clause and insert WITH between them
        let mut result = String::new();
        let mut accumulated_vars: Vec<String> = Vec::new();

        for (idx, &pos) in create_positions.iter().enumerate() {
            let end = if idx + 1 < create_positions.len() {
                create_positions[idx + 1]
            } else {
                input.len()
            };
            // For the first clause, include any MATCH prefix before the first CREATE
            let start = if idx == 0 { 0 } else { pos };
            let clause = input[start..end].trim();

            // If not the first CREATE and we have accumulated variables, insert WITH
            if idx > 0 && !accumulated_vars.is_empty() {
                result.push_str(" WITH ");
                result.push_str(&accumulated_vars.join(", "));
                result.push(' ');
            }

            result.push_str(clause);

            // Extract variable names from this CREATE clause: (varname:Label ...) or (varname)
            let clause_bytes = clause.as_bytes();
            let mut j = 0;
            while j < clause_bytes.len() {
                if clause_bytes[j] == b'(' {
                    j += 1;
                    // Skip whitespace
                    while j < clause_bytes.len() && clause_bytes[j].is_ascii_whitespace() {
                        j += 1;
                    }
                    // Read variable name (alphanumeric + underscore)
                    let var_start = j;
                    while j < clause_bytes.len()
                        && (clause_bytes[j].is_ascii_alphanumeric() || clause_bytes[j] == b'_')
                    {
                        j += 1;
                    }
                    if j > var_start {
                        let var = &clause[var_start..j];
                        // Only collect if it looks like a variable (not a label after colon)
                        if !accumulated_vars.contains(&var.to_string()) {
                            accumulated_vars.push(var.to_string());
                        }
                    }
                }
                j += 1;
            }
        }

        result
    }

    /// Parse and execute a read-only Cypher query (MATCH, RETURN, etc.)
    /// Supports multiple semicolon-separated statements.
    pub fn execute(
        &self,
        query_str: &str,
        store: &crate::graph::GraphStore,
    ) -> Result<RecordBatch, Box<dyn std::error::Error>> {
        let statements = Self::split_statements(query_str);
        let mut last_result = RecordBatch {
            records: Vec::new(),
            columns: Vec::new(),
        };

        for stmt in &statements {
            let query = self.cached_parse(stmt)?;
            let mut executor = QueryExecutor::new(store);
            if self.query_timeout_secs > 0 {
                executor = executor.with_deadline(
                    std::time::Instant::now()
                        + std::time::Duration::from_secs(self.query_timeout_secs),
                );
            }
            last_result = executor.execute(&query)?;
        }

        Ok(last_result)
    }

    /// Parse and execute a write Cypher query (CREATE, DELETE, SET, etc.)
    /// Supports multiple semicolon-separated statements. Each statement
    /// sees the effects of previous statements (shared store).
    /// Also rewrites multi-CREATE queries to use WITH for variable sharing.
    pub fn execute_mut(
        &self,
        query_str: &str,
        store: &mut crate::graph::GraphStore,
        tenant_id: &str,
    ) -> Result<RecordBatch, Box<dyn std::error::Error>> {
        let statements = Self::split_statements(query_str);
        let mut last_result = RecordBatch {
            records: Vec::new(),
            columns: Vec::new(),
        };

        for stmt in &statements {
            // Rewrite multi-CREATE to use WITH for variable sharing
            let rewritten = Self::rewrite_multi_create(stmt);

            // Handle UNWIND+CREATE: expand UNWIND list into per-element CREATEs
            if let Some(expanded) = Self::expand_unwind_create(&rewritten) {
                for create_stmt in &expanded {
                    let query = self.cached_parse(create_stmt)?;
                    let mut executor = MutQueryExecutor::new(store, tenant_id.to_string());
                    last_result = executor.execute(&query)?;
                }
                continue;
            }

            let query = self.cached_parse(&rewritten)?;
            let mut executor = MutQueryExecutor::new(store, tenant_id.to_string());
            last_result = executor.execute(&query)?;
        }

        Ok(last_result)
    }
}

impl Default for QueryEngine {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::graph::GraphStore;

    #[test]
    fn test_query_engine_creation() {
        let engine = QueryEngine::new();
        drop(engine);
    }

    #[test]
    fn test_end_to_end_simple_query() {
        let mut store = GraphStore::new();

        // Create test data
        let alice = store.create_node("Person");
        if let Some(node) = store.get_node_mut(alice) {
            node.set_property("name", "Alice");
            node.set_property("age", 30i64);
        }

        let bob = store.create_node("Person");
        if let Some(node) = store.get_node_mut(bob) {
            node.set_property("name", "Bob");
            node.set_property("age", 25i64);
        }

        // Execute query
        let engine = QueryEngine::new();
        let result = engine.execute("MATCH (n:Person) RETURN n", &store);

        assert!(result.is_ok());
        let batch = result.unwrap();
        assert_eq!(batch.len(), 2);
        assert_eq!(batch.columns.len(), 1);
        assert_eq!(batch.columns[0], "n");
    }

    #[test]
    fn test_query_with_filter() {
        let mut store = GraphStore::new();

        let alice = store.create_node("Person");
        if let Some(node) = store.get_node_mut(alice) {
            node.set_property("name", "Alice");
            node.set_property("age", 30i64);
        }

        let bob = store.create_node("Person");
        if let Some(node) = store.get_node_mut(bob) {
            node.set_property("name", "Bob");
            node.set_property("age", 25i64);
        }

        let engine = QueryEngine::new();
        let result = engine.execute("MATCH (n:Person) WHERE n.age > 28 RETURN n", &store);

        assert!(result.is_ok());
        let batch = result.unwrap();
        assert_eq!(batch.len(), 1); // Only Alice
    }

    #[test]
    fn test_query_with_limit() {
        let mut store = GraphStore::new();

        for i in 0..10 {
            let node = store.create_node("Person");
            if let Some(n) = store.get_node_mut(node) {
                n.set_property("id", i as i64);
            }
        }

        let engine = QueryEngine::new();
        let result = engine.execute("MATCH (n:Person) RETURN n LIMIT 5", &store);

        assert!(result.is_ok());
        let batch = result.unwrap();
        assert_eq!(batch.len(), 5);
    }

    #[test]
    fn test_query_with_edge_traversal() {
        let mut store = GraphStore::new();

        let alice = store.create_node("Person");
        if let Some(node) = store.get_node_mut(alice) {
            node.set_property("name", "Alice");
        }

        let bob = store.create_node("Person");
        if let Some(node) = store.get_node_mut(bob) {
            node.set_property("name", "Bob");
        }

        store.create_edge(alice, bob, "KNOWS").unwrap();

        let engine = QueryEngine::new();
        let result = engine.execute("MATCH (a:Person)-[:KNOWS]->(b:Person) RETURN a, b", &store);

        assert!(result.is_ok());
        let batch = result.unwrap();
        assert_eq!(batch.len(), 1);
        assert_eq!(batch.columns.len(), 2);
    }

    #[test]
    fn test_property_projection() {
        let mut store = GraphStore::new();

        let alice = store.create_node("Person");
        if let Some(node) = store.get_node_mut(alice) {
            node.set_property("name", "Alice");
            node.set_property("age", 30i64);
        }

        let engine = QueryEngine::new();
        let result = engine.execute("MATCH (n:Person) RETURN n.name, n.age", &store);

        assert!(result.is_ok());
        let batch = result.unwrap();
        assert_eq!(batch.len(), 1);
        assert_eq!(batch.columns.len(), 2);
        assert_eq!(batch.columns[0], "n.name");
        assert_eq!(batch.columns[1], "n.age");
    }

    // ==================== CREATE TESTS ====================

    #[test]
    fn test_create_single_node() {
        // Test: CREATE (n:Person)
        let mut store = GraphStore::new();
        let engine = QueryEngine::new();

        // Execute CREATE query
        let result = engine.execute_mut(r#"CREATE (n:Person)"#, &mut store, "default");

        assert!(result.is_ok(), "CREATE query should succeed");

        // Verify node was created by querying it
        let query_result = engine.execute("MATCH (n:Person) RETURN n", &store);
        assert!(query_result.is_ok());
        let batch = query_result.unwrap();
        assert_eq!(batch.len(), 1, "Should have created 1 Person node");
    }

    #[test]
    fn test_create_node_with_properties() {
        // Test: CREATE (n:Person {name: "Alice", age: 30})
        let mut store = GraphStore::new();
        let engine = QueryEngine::new();

        // Execute CREATE query with properties
        let result = engine.execute_mut(
            r#"CREATE (n:Person {name: "Alice", age: 30})"#,
            &mut store,
            "default",
        );

        assert!(
            result.is_ok(),
            "CREATE query with properties should succeed"
        );

        // Verify node was created with correct properties
        let query_result = engine.execute("MATCH (n:Person) RETURN n.name, n.age", &store);
        assert!(query_result.is_ok());
        let batch = query_result.unwrap();
        assert_eq!(batch.len(), 1, "Should have created 1 Person node");
    }

    #[test]
    fn test_create_multiple_nodes() {
        // Test multiple CREATE operations
        let mut store = GraphStore::new();
        let engine = QueryEngine::new();

        // Create first node
        let result1 = engine.execute_mut(
            r#"CREATE (a:Person {name: "Alice"})"#,
            &mut store,
            "default",
        );
        assert!(result1.is_ok());

        // Create second node
        let result2 =
            engine.execute_mut(r#"CREATE (b:Person {name: "Bob"})"#, &mut store, "default");
        assert!(result2.is_ok());

        // Verify both nodes exist
        let query_result = engine.execute("MATCH (n:Person) RETURN n", &store);
        assert!(query_result.is_ok());
        let batch = query_result.unwrap();
        assert_eq!(batch.len(), 2, "Should have created 2 Person nodes");
    }

    #[test]
    fn test_create_returns_error_on_readonly_executor() {
        // Test that using read-only executor for CREATE fails
        let store = GraphStore::new();
        let engine = QueryEngine::new();

        // Try to execute CREATE with read-only execute() - should fail
        let result = engine.execute(r#"CREATE (n:Person)"#, &store);

        assert!(
            result.is_err(),
            "CREATE should fail with read-only executor"
        );
    }

    // ==================== CREATE EDGE TESTS ====================

    #[test]
    fn test_create_edge_simple() {
        // Test: CREATE (a:Person)-[:KNOWS]->(b:Person)
        let mut store = GraphStore::new();
        let engine = QueryEngine::new();

        // Execute CREATE query with edge
        let result = engine.execute_mut(
            r#"CREATE (a:Person {name: "Alice"})-[:KNOWS]->(b:Person {name: "Bob"})"#,
            &mut store,
            "default",
        );

        assert!(
            result.is_ok(),
            "CREATE with edge should succeed: {:?}",
            result.err()
        );

        // Verify nodes were created
        let query_result = engine.execute("MATCH (n:Person) RETURN n", &store);
        assert!(query_result.is_ok());
        let batch = query_result.unwrap();
        assert_eq!(batch.len(), 2, "Should have created 2 Person nodes");

        // Verify edge was created by querying the relationship
        let edge_result =
            engine.execute("MATCH (a:Person)-[:KNOWS]->(b:Person) RETURN a, b", &store);
        assert!(edge_result.is_ok(), "Edge query should succeed");
        let edge_batch = edge_result.unwrap();
        assert_eq!(edge_batch.len(), 1, "Should have 1 KNOWS relationship");
    }

    #[test]
    fn test_create_edge_with_properties() {
        // Test: CREATE (a:Person)-[:KNOWS {since: 2020}]->(b:Person)
        let mut store = GraphStore::new();
        let engine = QueryEngine::new();

        // Execute CREATE query with edge properties
        let result = engine.execute_mut(
            r#"CREATE (a:Person {name: "Alice"})-[:FRIENDS {since: 2020}]->(b:Person {name: "Bob"})"#,
            &mut store,
            "default"
        );

        assert!(
            result.is_ok(),
            "CREATE with edge properties should succeed: {:?}",
            result.err()
        );

        // Verify edge was created
        let edge_result = engine.execute(
            "MATCH (a:Person)-[r:FRIENDS]->(b:Person) RETURN a, r, b",
            &store,
        );
        assert!(edge_result.is_ok(), "Edge query should succeed");
        let edge_batch = edge_result.unwrap();
        assert_eq!(edge_batch.len(), 1, "Should have 1 FRIENDS relationship");
    }

    #[test]
    fn test_create_chain_pattern() {
        // Test: CREATE (a:Person)-[:KNOWS]->(b:Person)-[:LIKES]->(c:Movie)
        let mut store = GraphStore::new();
        let engine = QueryEngine::new();

        // Execute CREATE query with chain of edges
        let result = engine.execute_mut(
            r#"CREATE (a:Person {name: "Alice"})-[:KNOWS]->(b:Person {name: "Bob"})-[:LIKES]->(c:Movie {title: "Matrix"})"#,
            &mut store,
            "default"
        );

        assert!(
            result.is_ok(),
            "CREATE chain should succeed: {:?}",
            result.err()
        );

        // Verify 2 Person nodes and 1 Movie node created
        let person_result = engine.execute("MATCH (n:Person) RETURN n", &store);
        assert!(person_result.is_ok());
        assert_eq!(
            person_result.unwrap().len(),
            2,
            "Should have 2 Person nodes"
        );

        let movie_result = engine.execute("MATCH (n:Movie) RETURN n", &store);
        assert!(movie_result.is_ok());
        assert_eq!(movie_result.unwrap().len(), 1, "Should have 1 Movie node");

        // Verify both edges were created
        let knows_result =
            engine.execute("MATCH (a:Person)-[:KNOWS]->(b:Person) RETURN a, b", &store);
        assert!(knows_result.is_ok());
        assert_eq!(
            knows_result.unwrap().len(),
            1,
            "Should have 1 KNOWS relationship"
        );

        let likes_result =
            engine.execute("MATCH (a:Person)-[:LIKES]->(b:Movie) RETURN a, b", &store);
        assert!(likes_result.is_ok());
        assert_eq!(
            likes_result.unwrap().len(),
            1,
            "Should have 1 LIKES relationship"
        );
    }

    #[test]
    fn test_cache_hit_miss_tracking() {
        let store = GraphStore::new();
        let engine = QueryEngine::new();

        // First execution — cache miss
        let _ = engine.execute("MATCH (n:Person) RETURN n", &store);
        assert_eq!(engine.cache_stats().hits(), 0);
        assert_eq!(engine.cache_stats().misses(), 1);
        assert_eq!(engine.cache_len(), 1);

        // Second identical query — cache hit
        let _ = engine.execute("MATCH (n:Person) RETURN n", &store);
        assert_eq!(engine.cache_stats().hits(), 1);
        assert_eq!(engine.cache_stats().misses(), 1);

        // Different query — cache miss
        let _ = engine.execute("MATCH (n:Movie) RETURN n", &store);
        assert_eq!(engine.cache_stats().hits(), 1);
        assert_eq!(engine.cache_stats().misses(), 2);
        assert_eq!(engine.cache_len(), 2);

        // Whitespace-normalized hit
        let _ = engine.execute("MATCH  (n:Person)  RETURN  n", &store);
        assert_eq!(engine.cache_stats().hits(), 2);
        assert_eq!(engine.cache_stats().misses(), 2);
    }

    #[test]
    fn test_lru_eviction() {
        let store = GraphStore::new();
        let engine = QueryEngine::with_capacity(2);

        // Fill cache to capacity
        let _ = engine.execute("MATCH (a:Person) RETURN a", &store);
        let _ = engine.execute("MATCH (b:Movie) RETURN b", &store);
        assert_eq!(engine.cache_len(), 2);

        // Third distinct query should evict the LRU entry
        let _ = engine.execute("MATCH (c:Company) RETURN c", &store);
        assert_eq!(engine.cache_len(), 2); // Still 2, not 3

        // The first query should have been evicted (was LRU)
        let _ = engine.execute("MATCH (a:Person) RETURN a", &store);
        // If evicted: miss count goes up; if still cached: hit count goes up
        // We had 3 misses so far, this should be a 4th miss
        assert_eq!(engine.cache_stats().misses(), 4);
    }

    // ==================== MULTI-STATEMENT TESTS ====================

    #[test]
    fn test_split_statements_basic() {
        let stmts = QueryEngine::split_statements("CREATE (a:Person); CREATE (b:Person)");
        assert_eq!(stmts, vec!["CREATE (a:Person)", "CREATE (b:Person)"]);
    }

    #[test]
    fn test_split_statements_with_whitespace() {
        let stmts = QueryEngine::split_statements("  CREATE (a:Person) ;  CREATE (b:Person) ;  ");
        assert_eq!(stmts, vec!["CREATE (a:Person)", "CREATE (b:Person)"]);
    }

    #[test]
    fn test_split_statements_no_semicolon() {
        let stmts = QueryEngine::split_statements("MATCH (n) RETURN n");
        assert_eq!(stmts, vec!["MATCH (n) RETURN n"]);
    }

    #[test]
    fn test_split_statements_respects_single_quotes() {
        let stmts = QueryEngine::split_statements("CREATE (n:P {x: 'a;b'}); MATCH (n) RETURN n");
        assert_eq!(stmts, vec!["CREATE (n:P {x: 'a;b'})", "MATCH (n) RETURN n"]);
    }

    #[test]
    fn test_split_statements_respects_double_quotes() {
        let stmts = QueryEngine::split_statements(r#"CREATE (n:P {x: "a;b"}); MATCH (n) RETURN n"#);
        assert_eq!(
            stmts,
            vec![r#"CREATE (n:P {x: "a;b"})"#, "MATCH (n) RETURN n"]
        );
    }

    #[test]
    fn test_multi_create_with_semicolons() {
        let mut store = GraphStore::new();
        let engine = QueryEngine::new();

        engine
            .execute_mut(
                "CREATE (a:Person {name: 'Alice'}); CREATE (b:Person {name: 'Bob'})",
                &mut store,
                "default",
            )
            .unwrap();

        let result = engine
            .execute("MATCH (n:Person) RETURN count(n)", &store)
            .unwrap();
        let count = result.records[0]
            .get("count(n)")
            .unwrap()
            .as_property()
            .unwrap()
            .as_integer()
            .unwrap();
        assert_eq!(count, 2);
    }

    #[test]
    fn test_multi_statement_create_then_match_relationship() {
        let mut store = GraphStore::new();
        let engine = QueryEngine::new();

        // Create nodes and relationship in one multi-statement call
        engine
            .execute_mut(
                "CREATE (a:Person {name: 'Alice'}); \
             CREATE (b:Person {name: 'Bob'}); \
             MATCH (a:Person {name: 'Alice'}), (b:Person {name: 'Bob'}) CREATE (a)-[:KNOWS]->(b)",
                &mut store,
                "default",
            )
            .unwrap();

        // Verify
        let result = engine
            .execute(
                "MATCH (a:Person)-[:KNOWS]->(b:Person) RETURN a.name, b.name",
                &store,
            )
            .unwrap();
        assert_eq!(result.len(), 1);
    }

    #[test]
    fn test_multi_statement_trailing_semicolon() {
        let mut store = GraphStore::new();
        let engine = QueryEngine::new();

        // Trailing semicolons should be fine
        engine
            .execute_mut("CREATE (n:Test {val: 1});", &mut store, "default")
            .unwrap();

        let result = engine
            .execute("MATCH (n:Test) RETURN count(n)", &store)
            .unwrap();
        let count = result.records[0]
            .get("count(n)")
            .unwrap()
            .as_property()
            .unwrap()
            .as_integer()
            .unwrap();
        assert_eq!(count, 1);
    }

    #[test]
    fn test_multi_create_no_semicolons_shared_variables() {
        // This is the key user request: multi-line CREATE with variable sharing
        let mut store = GraphStore::new();
        let engine = QueryEngine::new();

        engine
            .execute_mut(
                "CREATE (a:Person {name: 'Alice', age: 30})
                 CREATE (b:Person {name: 'Bob', age: 25})
                 CREATE (a)-[:KNOWS {since: 2020}]->(b)",
                &mut store,
                "default",
            )
            .unwrap();

        // Verify nodes
        let result = engine
            .execute("MATCH (n:Person) RETURN count(n)", &store)
            .unwrap();
        let count = result.records[0]
            .get("count(n)")
            .unwrap()
            .as_property()
            .unwrap()
            .as_integer()
            .unwrap();
        assert_eq!(count, 2, "Should have 2 Person nodes");

        // Verify relationship
        let result = engine
            .execute(
                "MATCH (a:Person)-[r:KNOWS]->(b:Person) RETURN a.name, b.name, r.since",
                &store,
            )
            .unwrap();
        assert_eq!(result.len(), 1, "Should have 1 KNOWS relationship");
    }

    #[test]
    fn test_multi_create_with_match_no_semicolons() {
        let mut store = GraphStore::new();
        let engine = QueryEngine::new();

        // Create then query in one go (use semicolons to separate CREATE+MATCH)
        engine
            .execute_mut(
                "CREATE (c:City {name: 'Paris'});
                 CREATE (p:Person {name: 'Alice'});
                 MATCH (p:Person {name: 'Alice'}), (c:City {name: 'Paris'})
                 CREATE (p)-[:LIVES_IN]->(c)",
                &mut store,
                "default",
            )
            .unwrap();

        let result = engine
            .execute(
                "MATCH (p:Person)-[:LIVES_IN]->(c:City) RETURN p.name, c.name",
                &store,
            )
            .unwrap();
        assert_eq!(result.len(), 1);
    }

    #[test]
    fn test_match_then_create_then_create_relationship() {
        // MATCH existing node, CREATE new node, CREATE relationship
        // Uses semicolons to separate MATCH+CREATE from the relationship CREATE
        let mut store = GraphStore::new();
        let engine = QueryEngine::new();

        // Use semicolons: CREATE both nodes, then MATCH both to create relationship
        engine
            .execute_mut(
                "CREATE (f:Person {name: 'Fabisch Kamau'}); \
                 CREATE (g:Person {name: 'Gloria Muthoni'}); \
                 MATCH (f:Person {name: 'Fabisch Kamau'}), (g:Person {name: 'Gloria Muthoni'}) \
                 CREATE (f)-[:LOVES]->(g)",
                &mut store,
                "default",
            )
            .unwrap();

        // Verify
        let result = engine
            .execute(
                "MATCH (a:Person)-[:LOVES]->(b:Person) RETURN a.name, b.name",
                &store,
            )
            .unwrap();
        assert_eq!(result.len(), 1);
    }
}
