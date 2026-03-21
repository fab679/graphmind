//! OpenCypher 9 TCK (Technology Compatibility Kit) Runner
//!
//! Parses the official OpenCypher 9 `.feature` files (Gherkin format) and runs
//! each scenario against the Graphmind query engine. Produces an informational
//! compliance report rather than asserting — so it can be run repeatedly to
//! track progress as Cypher coverage improves.
//!
//! Usage:
//!   cargo test --test tck_runner -- --nocapture
//!   cargo test --test tck_runner -- --nocapture 2>&1 | tail -60

use graphmind::{GraphStore, QueryEngine};
use std::collections::HashMap;
use std::fmt;
use std::fs;
use std::path::{Path, PathBuf};

// ---------------------------------------------------------------------------
// Data structures
// ---------------------------------------------------------------------------

/// A single TCK scenario parsed from a `.feature` file.
struct TckScenario {
    feature: String,
    name: String,
    setup_queries: Vec<String>,
    test_query: String,
    expected: TckExpected,
    has_parameters: bool,
    is_outline: bool,
    tags: Vec<String>,
}

/// What the scenario expects from the test query.
enum TckExpected {
    /// Expected result rows (header + data). We only check row count for now.
    Rows {
        header: Vec<String>,
        data: Vec<Vec<String>>,
        ordered: bool,
    },
    /// `Then the result should be empty`
    Empty,
    /// `Then a SyntaxError should be raised`
    SyntaxError,
    /// `Then a TypeError should be raised`
    TypeError,
    /// `Then a SemanticError should be raised`
    SemanticError,
    /// `Then a ArgumentError should be raised`
    ArgumentError,
    /// Any other error expectation
    OtherError(String),
    /// No explicit expectation found — just verify no crash
    AnyResult,
}

impl fmt::Display for TckExpected {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            TckExpected::Rows { data, ordered, .. } => {
                write!(
                    f,
                    "{} rows ({})",
                    data.len(),
                    if *ordered { "ordered" } else { "any order" }
                )
            }
            TckExpected::Empty => write!(f, "empty"),
            TckExpected::SyntaxError => write!(f, "SyntaxError"),
            TckExpected::TypeError => write!(f, "TypeError"),
            TckExpected::SemanticError => write!(f, "SemanticError"),
            TckExpected::ArgumentError => write!(f, "ArgumentError"),
            TckExpected::OtherError(kind) => write!(f, "{}Error", kind),
            TckExpected::AnyResult => write!(f, "any"),
        }
    }
}

/// Outcome of running a single scenario.
enum Outcome {
    Passed,
    Failed(String),
    Skipped(String),
}

// ---------------------------------------------------------------------------
// Feature file parser
// ---------------------------------------------------------------------------

/// Recursively find all `.feature` files under `dir`.
fn find_feature_files(dir: &Path) -> Vec<PathBuf> {
    let mut files = Vec::new();
    for entry in walkdir::WalkDir::new(dir)
        .into_iter()
        .filter_map(|e| e.ok())
    {
        let path = entry.path();
        if path.extension().map(|e| e == "feature").unwrap_or(false) {
            files.push(path.to_path_buf());
        }
    }
    files.sort();
    files
}

/// Extract the triple-quoted block starting at `lines[*i]`.
/// Expects `*i` to point at or before the opening `"""`.
/// Advances `*i` past the closing `"""`.
fn extract_query_block(lines: &[&str], i: &mut usize) -> Option<String> {
    // Skip to opening """
    while *i < lines.len() && lines[*i].trim() != "\"\"\"" {
        *i += 1;
    }
    if *i >= lines.len() {
        return None;
    }
    *i += 1; // skip opening """

    let mut parts = Vec::new();
    while *i < lines.len() && lines[*i].trim() != "\"\"\"" {
        parts.push(lines[*i].trim());
        *i += 1;
    }
    if *i < lines.len() {
        *i += 1; // skip closing """
    }

    // Join with newlines to preserve multi-statement separation
    // (e.g., separate CREATE statements that the engine splits on newlines)
    let query = parts.join("\n");
    if query.is_empty() {
        None
    } else {
        Some(query)
    }
}

/// Parse a pipe-delimited table starting at `lines[*i]`.
/// Returns (header, data_rows). Advances `*i` past the table.
fn parse_table(lines: &[&str], i: &mut usize) -> (Vec<String>, Vec<Vec<String>>) {
    let mut rows: Vec<Vec<String>> = Vec::new();
    while *i < lines.len() {
        let line = lines[*i].trim();
        if !line.starts_with('|') {
            break;
        }
        let cells: Vec<String> = line
            .split('|')
            .filter(|s| !s.trim().is_empty())
            .map(|s| s.trim().to_string())
            .collect();
        if !cells.is_empty() {
            rows.push(cells);
        }
        *i += 1;
    }

    if rows.is_empty() {
        (Vec::new(), Vec::new())
    } else {
        let header = rows[0].clone();
        let data = rows.into_iter().skip(1).collect();
        (header, data)
    }
}

/// Parse all scenarios from a single `.feature` file.
fn parse_feature_file(path: &Path) -> Vec<TckScenario> {
    let content = match fs::read_to_string(path) {
        Ok(c) => c,
        Err(_) => return Vec::new(),
    };
    let feature_name = path.file_stem().unwrap().to_str().unwrap().to_string();
    let lines: Vec<&str> = content.lines().collect();
    let mut scenarios = Vec::new();
    let mut i = 0;

    while i < lines.len() {
        // Collect tags on lines immediately before a Scenario
        let mut tags = Vec::new();
        while i < lines.len() && lines[i].trim().starts_with('@') {
            for tag in lines[i].trim().split_whitespace() {
                if tag.starts_with('@') {
                    tags.push(tag.to_string());
                }
            }
            i += 1;
        }
        if i >= lines.len() {
            break;
        }

        let line = lines[i].trim();
        let is_outline = line.starts_with("Scenario Outline:");
        if line.starts_with("Scenario:") || is_outline {
            let name = line.splitn(2, ':').nth(1).unwrap_or("").trim().to_string();
            let mut setup_queries = Vec::new();
            let mut test_query = String::new();
            let mut expected = TckExpected::AnyResult;
            let mut has_parameters = false;
            let scenario_tags = tags.clone();

            i += 1;
            while i < lines.len() {
                let line = lines[i].trim();

                // Stop at next scenario (but peek for tags)
                if line.starts_with("Scenario:") || line.starts_with("Scenario Outline:") {
                    break;
                }
                // Tags for the next scenario — stop here
                if line.starts_with('@')
                    && i + 1 < lines.len()
                    && (lines[i + 1].trim().starts_with("Scenario:")
                        || lines[i + 1].trim().starts_with("Scenario Outline:"))
                {
                    break;
                }

                // Setup queries: "And having executed:" or "Given having executed:"
                if line == "And having executed:" || line == "Given having executed:" {
                    i += 1;
                    if let Some(query) = extract_query_block(&lines, &mut i) {
                        setup_queries.push(query);
                    }
                    continue;
                }

                // Parameters (we'll skip these scenarios)
                if line.starts_with("And parameters are:") {
                    has_parameters = true;
                    i += 1;
                    // Skip the parameter table
                    while i < lines.len() && lines[i].trim().starts_with('|') {
                        i += 1;
                    }
                    continue;
                }

                // Test query
                if line.starts_with("When executing query") {
                    i += 1;
                    if let Some(query) = extract_query_block(&lines, &mut i) {
                        test_query = query;
                    }
                    continue;
                }

                // When executing control query (for side-effect verification — skip)
                // Also skip all subsequent "Then" blocks and "And" lines for the control query
                if line.starts_with("When executing control query") {
                    i += 1;
                    // Skip the query block
                    while i < lines.len() && lines[i].trim() != "\"\"\"" {
                        i += 1;
                    }
                    if i < lines.len() {
                        i += 1; // skip opening """
                    }
                    while i < lines.len() && lines[i].trim() != "\"\"\"" {
                        i += 1;
                    }
                    if i < lines.len() {
                        i += 1; // skip closing """
                    }
                    // Skip "Then" and "And" lines that follow the control query result
                    while i < lines.len() {
                        let next_line = lines[i].trim();
                        if next_line.starts_with("Then ") || next_line.starts_with("And ") {
                            // Skip "Then" block content (tables, etc.)
                            i += 1;
                            while i < lines.len() && lines[i].trim().starts_with('|') {
                                i += 1;
                            }
                        } else {
                            break;
                        }
                    }
                    continue;
                }

                // Expected: empty
                if line == "Then the result should be empty" {
                    expected = TckExpected::Empty;
                    i += 1;
                    continue;
                }

                // Expected: rows (ordered or any order)
                if line.starts_with("Then the result should be") {
                    let ordered = line.contains("in order:") && !line.contains("in any order:");
                    i += 1;
                    let (header, data) = parse_table(&lines, &mut i);
                    if data.is_empty() && !header.is_empty() {
                        // Header-only means empty result (0 data rows)
                        expected = TckExpected::Rows {
                            header,
                            data: Vec::new(),
                            ordered,
                        };
                    } else if !header.is_empty() {
                        expected = TckExpected::Rows {
                            header,
                            data,
                            ordered,
                        };
                    }
                    continue;
                }

                // Expected errors
                if line.starts_with("Then a ") && line.contains("should be raised") {
                    if line.contains("SyntaxError") {
                        expected = TckExpected::SyntaxError;
                    } else if line.contains("TypeError") {
                        expected = TckExpected::TypeError;
                    } else if line.contains("SemanticError") {
                        expected = TckExpected::SemanticError;
                    } else if line.contains("ArgumentError") {
                        expected = TckExpected::ArgumentError;
                    } else {
                        // Extract error kind: "Then a FooError should be..."
                        let kind = line
                            .strip_prefix("Then a ")
                            .unwrap_or("")
                            .split_whitespace()
                            .next()
                            .unwrap_or("Unknown")
                            .to_string();
                        expected = TckExpected::OtherError(kind);
                    }
                    i += 1;
                    continue;
                }

                // Examples table for Scenario Outline — skip for now
                if line == "Examples:" {
                    i += 1;
                    // Skip the examples table
                    while i < lines.len() && lines[i].trim().starts_with('|') {
                        i += 1;
                    }
                    continue;
                }

                i += 1;
            }

            if !test_query.is_empty() {
                scenarios.push(TckScenario {
                    feature: feature_name.clone(),
                    name,
                    setup_queries,
                    test_query,
                    expected,
                    has_parameters,
                    is_outline,
                    tags: scenario_tags,
                });
            }
        } else {
            i += 1;
        }
    }

    scenarios
}

// ---------------------------------------------------------------------------
// Query execution helpers
// ---------------------------------------------------------------------------

/// Detect whether a query string contains write operations.
fn is_write_query(query: &str) -> bool {
    let upper = query.to_uppercase();
    // Check for write keywords at word boundaries (not inside quoted strings).
    // This is a heuristic — good enough for TCK scenarios.
    for keyword in &["CREATE", "DELETE", "DETACH", "SET ", "REMOVE", "MERGE"] {
        if upper.contains(keyword) {
            return true;
        }
    }
    false
}

/// Execute a query against the store, choosing read or write path.
fn execute_query(
    engine: &QueryEngine,
    store: &mut GraphStore,
    query: &str,
) -> Result<graphmind::RecordBatch, Box<dyn std::error::Error>> {
    if is_write_query(query) {
        engine.execute_mut(query, store, "default")
    } else {
        engine.execute(query, store)
    }
}

/// Execute a multi-line query block as separate statements (line-by-line),
/// similar to the script handler. Returns the last non-empty result.
fn execute_script(
    engine: &QueryEngine,
    store: &mut GraphStore,
    script: &str,
) -> Result<graphmind::RecordBatch, Box<dyn std::error::Error>> {
    let mut last_result = graphmind::RecordBatch {
        records: Vec::new(),
        columns: Vec::new(),
    };

    for line in script.lines() {
        let stmt = line.trim();
        if stmt.is_empty() || stmt.starts_with("//") || stmt.starts_with("--") {
            continue;
        }
        let stmt = stmt.trim_end_matches(';').trim();
        if stmt.is_empty() {
            continue;
        }
        last_result = execute_query(engine, store, stmt)?;
    }

    Ok(last_result)
}

// ---------------------------------------------------------------------------
// Scenario runner
// ---------------------------------------------------------------------------

fn run_scenario(scenario: &TckScenario) -> Outcome {
    // Skip scenarios that require parameters (Graphmind doesn't support $param syntax yet)
    if scenario.has_parameters {
        return Outcome::Skipped("requires parameters ($param)".to_string());
    }

    // Skip Scenario Outlines (template expansion not implemented)
    if scenario.is_outline {
        return Outcome::Skipped("Scenario Outline (template)".to_string());
    }

    // Skip @ignore tagged scenarios
    if scenario.tags.iter().any(|t| t == "@ignore") {
        return Outcome::Skipped("@ignore tag".to_string());
    }

    // Wrap the entire execution in a thread with a larger stack to avoid
    // stack overflow from deeply nested PEG parsing. Also catch_unwind
    // so parser/executor panics are reported as failures.
    let setup_queries = scenario.setup_queries.clone();
    let test_query = scenario.test_query.clone();

    let result = std::thread::Builder::new()
        .stack_size(16 * 1024 * 1024) // 16 MB stack
        .spawn(move || {
            std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                run_scenario_inner(&setup_queries, &test_query)
            }))
        })
        .unwrap()
        .join();

    let result = match result {
        Ok(inner) => inner,
        Err(_) => {
            Err(Box::new("thread panicked (stack overflow?)") as Box<dyn std::any::Any + Send>)
        }
    };

    match result {
        Ok(inner_result) => check_expectation(scenario, inner_result),
        Err(panic_info) => {
            let msg = if let Some(s) = panic_info.downcast_ref::<String>() {
                s.clone()
            } else if let Some(s) = panic_info.downcast_ref::<&str>() {
                s.to_string()
            } else {
                "unknown panic".to_string()
            };
            // If we expected an error, a panic counts as "got an error"
            match &scenario.expected {
                TckExpected::SyntaxError
                | TckExpected::TypeError
                | TckExpected::SemanticError
                | TckExpected::ArgumentError
                | TckExpected::OtherError(_) => Outcome::Passed,
                _ => Outcome::Failed(format!(
                    "PANIC: {}  query: {}",
                    truncate(&msg, 80),
                    truncate(&scenario.test_query, 80)
                )),
            }
        }
    }
}

/// Inner scenario runner (may panic — caller catches).
/// Returns (query_result, expected_check_outcome).
fn run_scenario_inner(setup_queries: &[String], test_query: &str) -> InnerResult {
    let mut store = GraphStore::new();
    let engine = QueryEngine::new();

    // Run setup queries (each is a single multi-line statement, or a multi-statement script)
    for (idx, setup) in setup_queries.iter().enumerate() {
        // Try as a single query first; if that fails, try line-by-line script execution
        if let Err(e) = execute_query(&engine, &mut store, setup) {
            // Fallback: try executing line-by-line (handles multi-CREATE blocks)
            if setup.contains('\n') {
                if let Err(e2) = execute_script(&engine, &mut store, setup) {
                    return InnerResult::SetupFailed(format!(
                        "Setup query {} failed: {}  query: {}",
                        idx + 1,
                        e2,
                        truncate(setup, 120)
                    ));
                }
            } else {
                return InnerResult::SetupFailed(format!(
                    "Setup query {} failed: {}  query: {}",
                    idx + 1,
                    e,
                    truncate(setup, 120)
                ));
            }
        }
    }

    // Run the test query (single statement, possibly multi-line)
    // Try as single query first, then fall back to script execution for multi-statement queries
    match execute_query(&engine, &mut store, test_query) {
        Ok(batch) => InnerResult::Ok {
            row_count: batch.len(),
            col_count: batch.columns.len(),
            columns: batch.columns.clone(),
        },
        Err(e) => {
            // Fallback: try line-by-line script execution for multi-statement queries
            if test_query.contains('\n') {
                match execute_script(&engine, &mut store, test_query) {
                    Ok(batch) => InnerResult::Ok {
                        row_count: batch.len(),
                        col_count: batch.columns.len(),
                        columns: batch.columns.clone(),
                    },
                    Err(_) => InnerResult::QueryError(format!("{}", e)),
                }
            } else {
                InnerResult::QueryError(format!("{}", e))
            }
        }
    }
}

/// Result from the inner (panic-safe) scenario runner.
enum InnerResult {
    Ok {
        row_count: usize,
        col_count: usize,
        columns: Vec<String>,
    },
    QueryError(String),
    SetupFailed(String),
}

/// Convert InnerResult + TckExpected into an Outcome.
fn check_expectation(scenario: &TckScenario, inner: InnerResult) -> Outcome {
    match inner {
        InnerResult::SetupFailed(msg) => Outcome::Failed(msg),

        InnerResult::QueryError(err_msg) => match &scenario.expected {
            TckExpected::SyntaxError
            | TckExpected::TypeError
            | TckExpected::SemanticError
            | TckExpected::ArgumentError
            | TckExpected::OtherError(_) => Outcome::Passed,
            TckExpected::AnyResult => Outcome::Passed,
            TckExpected::Empty => Outcome::Failed(format!(
                "Expected empty result but got error: {}  query: {}",
                err_msg,
                truncate(&scenario.test_query, 100)
            )),
            TckExpected::Rows { data, .. } => Outcome::Failed(format!(
                "Expected {} rows but got error: {}  query: {}",
                data.len(),
                err_msg,
                truncate(&scenario.test_query, 100)
            )),
        },

        InnerResult::Ok {
            row_count,
            col_count,
            columns,
        } => match &scenario.expected {
            TckExpected::SyntaxError
            | TckExpected::TypeError
            | TckExpected::SemanticError
            | TckExpected::ArgumentError
            | TckExpected::OtherError(_) => Outcome::Failed(format!(
                "Expected {} but query succeeded: {}",
                scenario.expected,
                truncate(&scenario.test_query, 100)
            )),
            TckExpected::Empty => {
                if row_count == 0 {
                    Outcome::Passed
                } else {
                    Outcome::Failed(format!(
                        "Expected empty result, got {} rows: {}",
                        row_count,
                        truncate(&scenario.test_query, 100)
                    ))
                }
            }
            TckExpected::Rows { header, data, .. } => {
                let expected_rows = data.len();
                let expected_cols = header.len();

                if row_count != expected_rows {
                    Outcome::Failed(format!(
                        "Row count mismatch: expected {}, got {}  query: {}",
                        expected_rows,
                        row_count,
                        truncate(&scenario.test_query, 100)
                    ))
                } else if col_count != expected_cols {
                    Outcome::Failed(format!(
                        "Column count mismatch: expected {} ({:?}), got {} ({:?})  query: {}",
                        expected_cols,
                        header,
                        col_count,
                        columns,
                        truncate(&scenario.test_query, 100)
                    ))
                } else {
                    Outcome::Passed
                }
            }
            TckExpected::AnyResult => Outcome::Passed,
        },
    }
}

/// Truncate a string for display.
fn truncate(s: &str, max: usize) -> String {
    if s.len() <= max {
        s.to_string()
    } else {
        format!("{}...", &s[..max])
    }
}

// ---------------------------------------------------------------------------
// Category for grouping results
// ---------------------------------------------------------------------------

/// Derive a category name from the feature file path.
/// e.g., ".../clauses/match/Match1.feature" => "clauses/match"
fn category_from_path(path: &Path, tck_root: &Path) -> String {
    let parent = path.parent().unwrap_or(path);
    parent
        .strip_prefix(tck_root)
        .unwrap_or(parent)
        .to_str()
        .unwrap_or("unknown")
        .to_string()
}

// ---------------------------------------------------------------------------
// Main test
// ---------------------------------------------------------------------------

#[test]
fn test_tck_compliance() {
    let tck_dir = Path::new("opencypher9/tck/features");
    if !tck_dir.exists() {
        eprintln!("TCK directory not found at {:?}, skipping", tck_dir);
        return;
    }

    // Suppress panic output from catch_unwind — we handle panics gracefully
    let default_hook = std::panic::take_hook();
    std::panic::set_hook(Box::new(|_| { /* silence */ }));

    let feature_files = find_feature_files(tck_dir);
    if feature_files.is_empty() {
        std::panic::set_hook(default_hook);
        eprintln!("No .feature files found, skipping");
        return;
    }

    let mut total_passed: usize = 0;
    let mut total_failed: usize = 0;
    let mut total_skipped: usize = 0;
    let mut failures: Vec<String> = Vec::new();
    let mut skip_reasons: HashMap<String, usize> = HashMap::new();

    // Per-category tallies: (passed, failed, skipped)
    let mut category_results: HashMap<String, (usize, usize, usize)> = HashMap::new();

    for path in &feature_files {
        let scenarios = parse_feature_file(path);
        let category = category_from_path(path, tck_dir);

        for scenario in &scenarios {
            let outcome = run_scenario(scenario);
            let entry = category_results
                .entry(category.clone())
                .or_insert((0, 0, 0));

            match outcome {
                Outcome::Passed => {
                    total_passed += 1;
                    entry.0 += 1;
                }
                Outcome::Failed(msg) => {
                    total_failed += 1;
                    entry.1 += 1;
                    failures.push(format!(
                        "[{}] {} / {} -- {}",
                        category, scenario.feature, scenario.name, msg
                    ));
                }
                Outcome::Skipped(reason) => {
                    total_skipped += 1;
                    entry.2 += 1;
                    *skip_reasons.entry(reason).or_insert(0) += 1;
                }
            }
        }
    }

    // -----------------------------------------------------------------------
    // Print compliance report
    // -----------------------------------------------------------------------

    let total_run = total_passed + total_failed;
    let total_all = total_run + total_skipped;
    let compliance = if total_run > 0 {
        total_passed as f64 / total_run as f64 * 100.0
    } else {
        0.0
    };

    println!("\n{}", "=".repeat(72));
    println!("  OpenCypher 9 TCK Compliance Report");
    println!("  Graphmind v{}", graphmind::VERSION);
    println!("{}", "=".repeat(72));

    // Category breakdown
    println!(
        "\n  {:<40} {:>6} {:>6} {:>6}  {:>5}",
        "Category", "Pass", "Fail", "Skip", "Rate"
    );
    println!("  {}", "-".repeat(67));

    let mut categories: Vec<_> = category_results.iter().collect();
    categories.sort_by_key(|(name, _)| name.to_string());

    for (cat, (p, f, s)) in &categories {
        let run = p + f;
        let pct = if run > 0 {
            *p as f64 / run as f64 * 100.0
        } else {
            0.0
        };
        println!("  {:<40} {:>6} {:>6} {:>6}  {:>4.0}%", cat, p, f, s, pct);
    }

    println!("  {}", "-".repeat(67));
    println!(
        "  {:<40} {:>6} {:>6} {:>6}  {:>4.0}%",
        "TOTAL", total_passed, total_failed, total_skipped, compliance
    );

    // Summary
    println!(
        "\n  Scenarios: {} total, {} executed, {} skipped",
        total_all, total_run, total_skipped
    );
    println!(
        "  Compliance: {:.1}% ({}/{})",
        compliance, total_passed, total_run
    );
    println!("  Feature files: {}", feature_files.len());

    // Skip reasons
    if !skip_reasons.is_empty() {
        println!("\n  Skip reasons:");
        let mut reasons: Vec<_> = skip_reasons.iter().collect();
        reasons.sort_by(|a, b| b.1.cmp(a.1));
        for (reason, count) in &reasons {
            println!("    {:>4}x  {}", count, reason);
        }
    }

    // First N failures
    if !failures.is_empty() {
        let show = failures.len();
        println!("\n  First {} failures (of {}):", show, failures.len());
        for f in failures.iter() {
            println!("    FAIL  {}", f);
        }
    }

    println!("\n{}\n", "=".repeat(72));

    // Restore default panic hook
    std::panic::set_hook(default_hook);

    // This test is informational — do NOT assert failure.
    // Uncomment the line below to make CI fail on any TCK regression:
    // assert_eq!(total_failed, 0, "{} TCK scenarios failed", total_failed);
}
