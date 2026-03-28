//! # Query Planner: From Declarative Query to Imperative Execution
//!
//! The planner is the heart of the query engine. It transforms a **declarative** query
//! ("find all people who know Alice") into an **imperative** execution plan ("scan Person
//! nodes, expand along KNOWS edges, filter where name = 'Alice'"). This transformation is
//! the most important optimization opportunity in any database -- the same query can have
//! dozens of valid execution plans, and the best one can be orders of magnitude faster
//! than the worst.
//!
//! ## Cost-Based Optimization (ADR-015)
//!
//! Like PostgreSQL, MySQL, and other mature databases, Graphmind uses **cost-based
//! optimization**. The planner:
//! 1. **Enumerates** candidate plans (different join orders, scan strategies, traversal
//!    directions)
//! 2. **Estimates** the cost of each plan using **cardinality estimation** -- statistical
//!    models that predict how many records each operator will produce (e.g., "there are
//!    10,000 Person nodes, 0.1% have name = 'Alice', so an equality filter produces ~10
//!    records")
//! 3. **Picks** the cheapest plan
//!
//! The statistics come from [`GraphStore::compute_statistics()`] which samples property
//! distributions, counts labels, and measures average degree.
//!
//! ## Key Optimization Techniques
//!
//! - **Predicate pushdown**: move WHERE filters as close to the scan as possible. Filtering
//!   1 million nodes down to 100 *before* expanding edges is vastly cheaper than expanding
//!   all edges and filtering afterward.
//! - **Index selection**: when a WHERE clause matches an indexed property (`WHERE n.email = $x`
//!   and an index exists on `:Person(email)`), use `IndexScanOperator` instead of
//!   `NodeScanOperator + FilterOperator`. This turns O(n) scans into O(log n) lookups.
//! - **Join ordering**: for multi-pattern MATCH clauses, the order in which patterns are
//!   joined matters enormously. Joining a 10-row result with a 1M-row result is fast;
//!   joining two 1M-row results is catastrophic.
//! - **Early LIMIT propagation**: push LIMIT down into the operator tree so that scans
//!   stop after producing enough records.
//!
//! ## Plan Cache
//!
//! Planning is not free -- enumerating plans and computing cost estimates takes time. For
//! repeated queries (common in applications), the planner caches planning metadata (index
//! hints, cost estimates) keyed by a hash of the query string. A **generation counter**
//! (`AtomicU64`) is incremented on schema changes (CREATE INDEX, DROP INDEX) to invalidate
//! stale cache entries. This uses `AtomicU64` with `Ordering::Relaxed` because exact
//! ordering is not required -- a stale read just causes one extra re-plan.
//!
//! ## Rust Concepts
//!
//! - **`Mutex<HashMap<u64, PlanCacheEntry>>`**: the plan cache is shared across threads
//!   (the query engine is `Send + Sync`). `Mutex` provides mutual exclusion -- only one
//!   thread can read/write the cache at a time. `HashMap<u64, _>` uses a pre-computed hash
//!   of the query string as the key.
//! - **`AtomicU64`**: a lock-free atomic integer for the generation counter. Atomics are
//!   cheaper than mutexes for simple counters because they use CPU-level atomic instructions
//!   (e.g., `LOCK CMPXCHG` on x86) instead of OS-level locks.

use crate::graph::EdgeType; // Added for CREATE edge support
use crate::graph::GraphStore;
use crate::graph::{Label, PropertyValue}; // Added for CREATE support
use crate::query::ast::*;
use crate::query::executor::{
    // Added CreateNodeOperator and CreateNodesAndEdgesOperator for CREATE statement support
    operator::{
        AggregateFunction, AggregateOperator, AggregateType, AlgorithmOperator,
        CartesianProductOperator, CompositeCreateIndexOperator, CreateConstraintOperator,
        CreateEdgeOperator, CreateIndexOperator, CreateNodeOperator, CreateNodesAndEdgesOperator,
        CreateVectorIndexOperator, DeleteOperator, DropIndexOperator, ExpandOperator,
        FilterOperator, ForeachOperator, IndexScanOperator, JoinOperator, LeftOuterJoinOperator,
        LimitOperator, MatchCreateEdgeOperator, MergeOperator, MockProcedureOperator,
        NodeScanOperator, PerRowCreateOperator, PerRowMergeOperator, ProjectOperator,
        RemovePropertyOperator, SchemaVisualizationOperator, SetPropertyOperator,
        ShortestPathOperator, ShowConstraintsOperator, ShowIndexesOperator, ShowLabelsOperator,
        ShowPropertyKeysOperator, ShowRelationshipTypesOperator, ShowVectorIndexesOperator,
        SingleRowOperator, SkipOperator, SortOperator, DistinctOperator, UnwindOperator, VarLengthExpandOperator,
        VectorSearchOperator, WithBarrierOperator,
    },
    ExecutionError,
    ExecutionResult,
    OperatorBox,
};
use std::collections::{HashMap, HashSet};
use std::sync::Mutex; // Added for CREATE properties and JOIN logic

/// Recursively extract aggregate function calls (sum, avg, count, min, max, collect)
/// from an expression tree, replacing each with a `Variable("__agg_N")` reference.
///
/// Returns the rewritten expression and the list of extracted aggregates.
/// Check if an expression contains any aggregation function call
fn contains_aggregation(expr: &Expression) -> bool {
    match expr {
        Expression::Function { name, args, .. } => {
            let agg_names = ["count", "sum", "avg", "min", "max", "collect"];
            if agg_names.contains(&name.to_lowercase().as_str()) {
                return true;
            }
            args.iter().any(contains_aggregation)
        }
        Expression::Binary { left, right, .. } => {
            contains_aggregation(left) || contains_aggregation(right)
        }
        Expression::Unary { expr, .. } => contains_aggregation(expr),
        _ => false,
    }
}

/// This enables expressions like `round(sum(b.runs) * 100 / sum(b.balls))` where
/// aggregate calls are nested inside arithmetic or scalar function calls.
fn extract_nested_aggregates(
    expr: &Expression,
    counter: &mut usize,
) -> (Expression, Vec<AggregateFunction>) {
    let mut aggregates = Vec::new();
    let rewritten = extract_agg_inner(expr, counter, &mut aggregates);
    (rewritten, aggregates)
}

fn extract_agg_inner(
    expr: &Expression,
    counter: &mut usize,
    aggs: &mut Vec<AggregateFunction>,
) -> Expression {
    match expr {
        Expression::Function {
            name,
            args,
            distinct,
        } => {
            let func_type = match name.to_lowercase().as_str() {
                "count" => Some(AggregateType::Count),
                "sum" => Some(AggregateType::Sum),
                "avg" => Some(AggregateType::Avg),
                "min" => Some(AggregateType::Min),
                "max" => Some(AggregateType::Max),
                "collect" => Some(AggregateType::Collect),
                _ => None,
            };

            if let Some(func) = func_type {
                let alias = format!("__agg_{}", *counter);
                *counter += 1;

                let arg_expr = if matches!(func, AggregateType::Count) && args.is_empty() {
                    Expression::Literal(PropertyValue::Integer(1))
                } else {
                    args.first()
                        .cloned()
                        .unwrap_or(Expression::Literal(PropertyValue::Null))
                };

                aggs.push(AggregateFunction {
                    func,
                    expr: arg_expr,
                    alias: alias.clone(),
                    distinct: *distinct,
                });

                Expression::Variable(alias)
            } else {
                // Non-aggregate function — recurse into args
                Expression::Function {
                    name: name.clone(),
                    args: args
                        .iter()
                        .map(|a| extract_agg_inner(a, counter, aggs))
                        .collect(),
                    distinct: *distinct,
                }
            }
        }
        Expression::Binary { left, op, right } => Expression::Binary {
            left: Box::new(extract_agg_inner(left, counter, aggs)),
            op: op.clone(),
            right: Box::new(extract_agg_inner(right, counter, aggs)),
        },
        Expression::Unary { op, expr: inner } => Expression::Unary {
            op: op.clone(),
            expr: Box::new(extract_agg_inner(inner, counter, aggs)),
        },
        Expression::Case {
            operand,
            when_clauses,
            else_result,
        } => Expression::Case {
            operand: operand
                .as_ref()
                .map(|e| Box::new(extract_agg_inner(e, counter, aggs))),
            when_clauses: when_clauses
                .iter()
                .map(|(cond, then)| {
                    (
                        extract_agg_inner(cond, counter, aggs),
                        extract_agg_inner(then, counter, aggs),
                    )
                })
                .collect(),
            else_result: else_result
                .as_ref()
                .map(|e| Box::new(extract_agg_inner(e, counter, aggs))),
        },
        // Leaf expressions and others — no aggregates possible
        other => other.clone(),
    }
}

/// An execution plan: a tree of physical operators ready to execute.
///
/// The `root` field holds the top-level operator (typically a `ProjectOperator` or
/// `LimitOperator`). Calling `root.next(store)` begins the Volcano pull-based execution,
/// cascading `next()` calls down the operator tree until a leaf scan produces a record.
///
/// `output_columns` lists the variable names that appear in the RETURN clause, used to
/// construct the final `RecordBatch` column headers.
///
/// `is_write` distinguishes read plans from write plans. When `true`, the executor must
/// use `next_mut(&mut store)` instead of `next(&store)`, and the caller must hold an
/// exclusive (`&mut`) reference to the `GraphStore`. This flag is set by the planner
/// when it encounters CREATE, DELETE, SET, MERGE, or schema-modification clauses.
pub struct ExecutionPlan {
    /// Root operator
    pub root: OperatorBox,
    /// Output column names
    pub output_columns: Vec<String>,
    /// Whether this plan contains write operations (CREATE/DELETE/SET)
    /// If true, executor must use next_mut() with mutable GraphStore
    pub is_write: bool,
}

/// Simple plan cache entry storing planning metadata
struct PlanCacheEntry {
    /// Timestamp when entry was created
    #[allow(dead_code)]
    created_at: std::time::Instant,
    /// Which index to use (if any): (label, property, op)
    #[allow(dead_code)]
    index_hint: Option<(Label, String)>,
}

/// Configuration for the query planner (ADR-015)
#[derive(Debug, Clone)]
pub struct PlannerConfig {
    /// Enable the graph-native planner (default: false, uses legacy planner)
    pub graph_native: bool,
    /// Maximum number of candidate plans to evaluate (default: 64)
    pub max_candidate_plans: usize,
}

impl Default for PlannerConfig {
    fn default() -> Self {
        Self {
            graph_native: false,
            max_candidate_plans: 64,
        }
    }
}

/// Query planner
pub struct QueryPlanner {
    /// Enable optimization
    _optimize: bool,
    /// Plan cache: query string hash → planning metadata
    plan_cache: Mutex<HashMap<u64, PlanCacheEntry>>,
    /// Cache generation counter (incremented on schema changes)
    cache_generation: std::sync::atomic::AtomicU64,
    /// Planner configuration (ADR-015)
    config: PlannerConfig,
}

impl QueryPlanner {
    /// Create a new query planner
    pub fn new() -> Self {
        Self {
            _optimize: true,
            plan_cache: Mutex::new(HashMap::new()),
            cache_generation: std::sync::atomic::AtomicU64::new(0),
            config: PlannerConfig::default(),
        }
    }

    /// Create a new query planner with configuration
    pub fn with_config(config: PlannerConfig) -> Self {
        Self {
            _optimize: true,
            plan_cache: Mutex::new(HashMap::new()),
            cache_generation: std::sync::atomic::AtomicU64::new(0),
            config,
        }
    }

    /// Get the current planner configuration
    pub fn config(&self) -> &PlannerConfig {
        &self.config
    }

    /// Invalidate the plan cache (e.g., after CREATE INDEX or schema change)
    pub fn invalidate_cache(&self) {
        self.cache_generation
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        self.plan_cache.lock().unwrap().clear();
    }

    /// Plan a query
    pub fn plan(&self, query: &Query, store: &GraphStore) -> ExecutionResult<ExecutionPlan> {
        // Validate duplicate column names in RETURN
        if let Some(rc) = &query.return_clause {
            let mut seen = std::collections::HashSet::new();
            for item in &rc.items {
                let alias = item
                    .alias
                    .clone()
                    .unwrap_or_else(|| match &item.expression {
                        Expression::Variable(v) => v.clone(),
                        Expression::Property { variable, property } => {
                            format!("{}.{}", variable, property)
                        }
                        _ => String::new(),
                    });
                if !alias.is_empty() && !seen.insert(alias.clone()) {
                    return Err(ExecutionError::PlanningError(format!(
                        "Column name '{}' appears more than once in RETURN",
                        alias
                    )));
                }
            }
        }

        // Validate CREATE patterns
        if let Some(cc) = &query.create_clause {
            for path in &cc.pattern.paths {
                for seg in &path.segments {
                    // CREATE relationship must have exactly one type
                    if seg.edge.types.is_empty() {
                        return Err(ExecutionError::PlanningError(
                            "Relationships must have exactly one type when created".to_string(),
                        ));
                    }
                    if seg.edge.types.len() > 1 {
                        return Err(ExecutionError::PlanningError(
                            "A single relationship type must be specified for CREATE".to_string(),
                        ));
                    }
                    // CREATE relationship cannot have variable-length pattern
                    if seg.edge.length.is_some() {
                        return Err(ExecutionError::PlanningError(
                            "Variable length relationships cannot be used in CREATE".to_string(),
                        ));
                    }
                }
            }
        }
        for cc in &query.create_clauses {
            for path in &cc.pattern.paths {
                for seg in &path.segments {
                    if seg.edge.types.is_empty() {
                        return Err(ExecutionError::PlanningError(
                            "Relationships must have exactly one type when created".to_string(),
                        ));
                    }
                    if seg.edge.types.len() > 1 {
                        return Err(ExecutionError::PlanningError(
                            "A single relationship type must be specified for CREATE".to_string(),
                        ));
                    }
                    if seg.edge.length.is_some() {
                        return Err(ExecutionError::PlanningError(
                            "Variable length relationships cannot be used in CREATE".to_string(),
                        ));
                    }
                }
            }
        }

        // Validate MATCH: duplicate edge variables in same pattern
        for mc in &query.match_clauses {
            let mut edge_vars = HashSet::new();
            for path in &mc.pattern.paths {
                for seg in &path.segments {
                    if let Some(v) = &seg.edge.variable {
                        if !edge_vars.insert(v.clone()) {
                            return Err(ExecutionError::PlanningError(format!(
                                "Cannot use the same relationship variable '{}' for multiple patterns",
                                v
                            )));
                        }
                    }
                }
            }
        }

        // Validate CREATE: re-binding already-bound node variables
        if !query.match_clauses.is_empty() {
            let mut match_node_vars = HashSet::new();
            let mut match_edge_vars = HashSet::new();
            for mc in &query.match_clauses {
                for path in &mc.pattern.paths {
                    if let Some(v) = &path.start.variable {
                        match_node_vars.insert(v.clone());
                    }
                    for seg in &path.segments {
                        if let Some(v) = &seg.node.variable {
                            match_node_vars.insert(v.clone());
                        }
                        if let Some(v) = &seg.edge.variable {
                            match_edge_vars.insert(v.clone());
                        }
                    }
                }
            }

            // Check CREATE patterns for re-bound MATCH variables
            let check_create = |cc: &crate::query::ast::CreateClause| -> ExecutionResult<()> {
                for path in &cc.pattern.paths {
                    if let Some(v) = &path.start.variable {
                        if match_node_vars.contains(v)
                            && (path.segments.is_empty()
                                || !path.start.labels.is_empty()
                                || path.start.properties.is_some())
                        {
                            return Err(ExecutionError::PlanningError(format!(
                                "Variable '{}' already declared in MATCH",
                                v
                            )));
                        }
                    }
                    for seg in &path.segments {
                        if let Some(v) = &seg.edge.variable {
                            if match_edge_vars.contains(v) {
                                return Err(ExecutionError::PlanningError(format!(
                                    "Variable '{}' already declared as a relationship",
                                    v
                                )));
                            }
                        }
                    }
                }
                Ok(())
            };

            if let Some(cc) = &query.create_clause {
                check_create(cc)?;
            }
            for cc in &query.create_clauses {
                check_create(cc)?;
            }
        }

        // Validate CREATE: re-binding within same CREATE pattern
        let validate_create_rebind = |cc: &crate::query::ast::CreateClause| -> ExecutionResult<()> {
            let mut seen_node_vars: HashMap<String, bool> = HashMap::new(); // var -> has_labels_or_props
            for path in &cc.pattern.paths {
                // Check start node
                if let Some(v) = &path.start.variable {
                    let has_label_or_prop =
                        !path.start.labels.is_empty() || path.start.properties.is_some();
                    if let Some(&prev_had) = seen_node_vars.get(v) {
                        // Second occurrence: BOTH must have labels/props to be a conflict
                        // Self-referencing (a:A)-[:R]->(a) is OK (second has no labels)
                        if has_label_or_prop && prev_had {
                            return Err(ExecutionError::PlanningError(format!(
                                "Can't create node '{}' with labels or properties here — already declared in this CREATE", v
                            )));
                        }
                    }
                    seen_node_vars.insert(v.clone(), has_label_or_prop);
                }
                // Check segment nodes
                for seg in &path.segments {
                    if let Some(v) = &seg.node.variable {
                        let has_label_or_prop =
                            !seg.node.labels.is_empty() || seg.node.properties.is_some();
                        if let Some(&prev_had) = seen_node_vars.get(v) {
                            if has_label_or_prop && prev_had {
                                return Err(ExecutionError::PlanningError(format!(
                                    "Can't create node '{}' with labels or properties here — already declared in this CREATE", v
                                )));
                            }
                        }
                        seen_node_vars.insert(v.clone(), has_label_or_prop);
                    }
                }
            }
            Ok(())
        };
        if let Some(cc) = &query.create_clause {
            validate_create_rebind(cc)?;
        }
        for cc in &query.create_clauses {
            validate_create_rebind(cc)?;
        }

        // Validate WITH: no duplicate aliases
        if let Some(wc) = &query.with_clause {
            let mut aliases = HashSet::new();
            for item in &wc.items {
                let alias = item
                    .alias
                    .clone()
                    .unwrap_or_else(|| match &item.expression {
                        Expression::Variable(v) => v.clone(),
                        _ => String::new(),
                    });
                if !alias.is_empty() && !aliases.insert(alias.clone()) {
                    return Err(ExecutionError::PlanningError(format!(
                        "Multiple result columns with the same name '{}' are not supported",
                        alias
                    )));
                }
            }
        }

        // Validate RETURN: no duplicate aliases
        if let Some(rc) = &query.return_clause {
            let mut aliases = HashSet::new();
            for item in &rc.items {
                let alias = item
                    .alias
                    .clone()
                    .unwrap_or_else(|| match &item.expression {
                        Expression::Variable(v) => v.clone(),
                        Expression::Property { variable, property } => {
                            format!("{}.{}", variable, property)
                        }
                        _ => String::new(),
                    });
                if !alias.is_empty() && !aliases.insert(alias.clone()) {
                    return Err(ExecutionError::PlanningError(format!(
                        "Multiple result columns with the same name '{}' are not supported",
                        alias
                    )));
                }
            }
        }

        // Validate DELETE expressions
        if let Some(dc) = &query.delete_clause {
            for expr in &dc.expressions {
                match expr {
                    Expression::Variable(v) => {
                        // Check that the variable is defined in MATCH scope
                        let mut defined = HashSet::new();
                        for mc in &query.match_clauses {
                            for path in &mc.pattern.paths {
                                if let Some(pv) = &path.start.variable { defined.insert(pv.clone()); }
                                if let Some(pv) = &path.path_variable { defined.insert(pv.clone()); }
                                for seg in &path.segments {
                                    if let Some(sv) = &seg.node.variable { defined.insert(sv.clone()); }
                                    if let Some(ev) = &seg.edge.variable { defined.insert(ev.clone()); }
                                }
                            }
                        }
                        if !defined.is_empty() && !defined.contains(v) {
                            return Err(ExecutionError::PlanningError(format!(
                                "Variable `{}` not defined", v
                            )));
                        }
                    }
                    Expression::Function { name, .. } if name == "$hasLabel" => {
                        return Err(ExecutionError::PlanningError(
                            "Invalid DELETE of label — use REMOVE instead".to_string(),
                        ));
                    }
                    Expression::Binary { .. } | Expression::Literal(_) => {
                        return Err(ExecutionError::PlanningError(
                            "Type mismatch: expected Node or Relationship but was Integer, Float, Boolean, or String".to_string(),
                        ));
                    }
                    _ => {}
                }
            }
        }

        // Validate DELETE of connected nodes (non-DETACH)
        // Note: actual constraint check happens at runtime in DeleteOperator

        // Validate MERGE with null node properties
        if let Some(mc) = &query.merge_clause {
            for path in &mc.pattern.paths {
                if let Some(ref props) = path.start.properties {
                    for (k, v) in props {
                        if matches!(v, PropertyValue::Null) {
                            return Err(ExecutionError::RuntimeError(format!(
                                "Cannot merge node using null property value for '{}'", k
                            )));
                        }
                    }
                }
            }
        }
        for mc in &query.all_merge_clauses {
            for path in &mc.pattern.paths {
                if let Some(ref props) = path.start.properties {
                    for (k, v) in props {
                        if matches!(v, PropertyValue::Null) {
                            return Err(ExecutionError::RuntimeError(format!(
                                "Cannot merge node using null property value for '{}'", k
                            )));
                        }
                    }
                }
            }
        }

        // Validate MERGE variable-length relationships
        for mc in &query.all_merge_clauses {
            for path in &mc.pattern.paths {
                for seg in &path.segments {
                    if seg.edge.length.is_some() {
                        return Err(ExecutionError::PlanningError(
                            "Variable length relationships cannot be used in MERGE".to_string(),
                        ));
                    }
                }
            }
        }

        // Validate UNION column consistency — only check explicit aliases
        if !query.union_queries.is_empty() {
            if let Some(rc) = &query.return_clause {
                // Only validate when RETURN items have explicit AS aliases
                let main_aliases: Vec<Option<&str>> = rc.items.iter()
                    .map(|item| item.alias.as_deref())
                    .collect();
                let all_have_aliases = main_aliases.iter().all(|a| a.is_some());
                if all_have_aliases {
                    let main_cols: Vec<&str> = main_aliases.iter().map(|a| a.unwrap()).collect();
                    for (union_q, _is_all) in &query.union_queries {
                        if let Some(urc) = &union_q.return_clause {
                            let union_aliases: Vec<Option<&str>> = urc.items.iter()
                                .map(|item| item.alias.as_deref())
                                .collect();
                            if union_aliases.iter().all(|a| a.is_some()) {
                                let union_cols: Vec<&str> = union_aliases.iter().map(|a| a.unwrap()).collect();
                                if main_cols.len() == union_cols.len() {
                                    for (mc, uc) in main_cols.iter().zip(union_cols.iter()) {
                                        if mc != uc {
                                            return Err(ExecutionError::PlanningError(
                                                "All sub queries in an UNION must have the same column names".to_string(),
                                            ));
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }

        // Validate: $param used as pattern predicate in MATCH/MERGE (not in property map)
        for mc in &query.match_clauses {
            for path in &mc.pattern.paths {
                if !path.start.expression_properties.is_empty() {
                    for (_, expr) in &path.start.expression_properties {
                        if matches!(expr, Expression::Parameter(_)) {
                            return Err(ExecutionError::PlanningError(
                                "Parameter maps are not allowed in MATCH".to_string(),
                            ));
                        }
                    }
                }
            }
        }

        // Validate: unaliased non-variable expressions in WITH (WITH a, count(*) is invalid)
        if let Some(wc) = &query.with_clause {
            for item in &wc.items {
                if item.alias.is_none() {
                    match &item.expression {
                        Expression::Variable(_) => {} // OK: WITH a
                        Expression::Property { .. } => {} // OK: WITH a.name
                        _ => {
                            return Err(ExecutionError::PlanningError(
                                "Expression in WITH must be aliased (use AS)".to_string(),
                            ));
                        }
                    }
                }
            }
        }

        // Validate: nested aggregation (count(count(*)))
        fn contains_nested_agg(expr: &Expression, depth: usize) -> bool {
            match expr {
                Expression::Function { name, args, .. } => {
                    let is_agg = matches!(name.to_lowercase().as_str(), "count" | "sum" | "avg" | "min" | "max" | "collect");
                    if is_agg && depth > 0 { return true; }
                    let next_depth = if is_agg { depth + 1 } else { depth };
                    args.iter().any(|a| contains_nested_agg(a, next_depth))
                }
                Expression::Binary { left, right, .. } => {
                    contains_nested_agg(left, depth) || contains_nested_agg(right, depth)
                }
                Expression::Unary { expr, .. } => contains_nested_agg(expr, depth),
                _ => false,
            }
        }
        if let Some(rc) = &query.return_clause {
            for item in &rc.items {
                if contains_nested_agg(&item.expression, 0) {
                    return Err(ExecutionError::PlanningError(
                        "Cannot nest aggregate functions".to_string(),
                    ));
                }
            }
        }

        // Validate: unknown function names in RETURN
        fn is_known_function(name: &str) -> bool {
            matches!(name.to_lowercase().as_str(),
                "toupper" | "tolower" | "trim" | "ltrim" | "rtrim" | "replace" | "substring" |
                "left" | "right" | "reverse" | "tostring" | "tointeger" | "toint" | "tofloat" |
                "toboolean" | "abs" | "ceil" | "floor" | "round" | "sqrt" | "sign" | "rand" |
                "log" | "log10" | "exp" | "e" | "pi" | "sin" | "cos" | "tan" |
                "asin" | "acos" | "atan" | "atan2" | "degrees" | "radians" | "haversin" |
                "count" | "sum" | "avg" | "min" | "max" | "collect" |
                "size" | "length" | "head" | "last" | "tail" | "keys" | "id" | "labels" | "type" |
                "exists" | "coalesce" | "range" | "nodes" | "relationships" | "rels" |
                "split" | "timestamp" | "randomuuid" | "properties" | "startnode" | "endnode" |
                "date" | "localtime" | "time" | "localdatetime" | "datetime" | "duration" |
                "datetime.fromepoch" | "datetime.fromepochmillis" |
                "duration.between" | "duration.inseconds" | "duration.inmonths" |
                "percentiledisc" | "percentilecont" | "stdev" | "stdevp" |
                "point" | "distance" |
                "none" | "any" | "all" | "single" |
                "reduce" | "extract" | "filter" |
                "$patternpredicate" | "$haslabel"
            )
        }
        fn check_unknown_functions(expr: &Expression) -> Option<String> {
            match expr {
                Expression::Function { name, args, .. } => {
                    if !is_known_function(name) {
                        return Some(name.clone());
                    }
                    for arg in args {
                        if let Some(n) = check_unknown_functions(arg) { return Some(n); }
                    }
                    None
                }
                Expression::Binary { left, right, .. } => {
                    check_unknown_functions(left).or_else(|| check_unknown_functions(right))
                }
                Expression::Unary { expr, .. } => check_unknown_functions(expr),
                _ => None,
            }
        }
        if let Some(rc) = &query.return_clause {
            for item in &rc.items {
                if let Some(bad_fn) = check_unknown_functions(&item.expression) {
                    return Err(ExecutionError::PlanningError(format!(
                        "Unknown function '{}'", bad_fn
                    )));
                }
            }
        }

        // Validate: undefined variable in SET value
        if !query.set_clauses.is_empty() && !query.match_clauses.is_empty() {
            let mut defined = HashSet::new();
            for mc in &query.match_clauses {
                for path in &mc.pattern.paths {
                    if let Some(v) = &path.start.variable { defined.insert(v.clone()); }
                    for seg in &path.segments {
                        if let Some(v) = &seg.node.variable { defined.insert(v.clone()); }
                        if let Some(v) = &seg.edge.variable { defined.insert(v.clone()); }
                    }
                }
            }
            for sc in &query.set_clauses {
                for item in &sc.items {
                    // Check if the value expression references an undefined variable
                    if let Expression::Variable(v) = &item.value {
                        if !defined.contains(v) && !v.starts_with('$') {
                            return Err(ExecutionError::PlanningError(format!(
                                "Variable `{}` not defined", v
                            )));
                        }
                    }
                }
            }
        }

        // Validate: undefined variable in MERGE ON CREATE SET / ON MATCH SET
        if let Some(mc) = &query.merge_clause {
            let mut merge_vars = HashSet::new();
            for path in &mc.pattern.paths {
                if let Some(v) = &path.start.variable { merge_vars.insert(v.clone()); }
                for seg in &path.segments {
                    if let Some(v) = &seg.node.variable { merge_vars.insert(v.clone()); }
                    if let Some(v) = &seg.edge.variable { merge_vars.insert(v.clone()); }
                }
            }
            for item in &mc.on_create_set {
                if !merge_vars.contains(&item.variable) {
                    return Err(ExecutionError::PlanningError(format!(
                        "Variable `{}` not defined", item.variable
                    )));
                }
            }
            for item in &mc.on_match_set {
                if !merge_vars.contains(&item.variable) {
                    return Err(ExecutionError::PlanningError(format!(
                        "Variable `{}` not defined", item.variable
                    )));
                }
            }
        }

        // Validate MERGE: relationship constraints
        if let Some(mc) = &query.merge_clause {
            for path in &mc.pattern.paths {
                for seg in &path.segments {
                    if seg.edge.types.is_empty() {
                        return Err(ExecutionError::PlanningError(
                            "Relationships must have exactly one type when used in MERGE"
                                .to_string(),
                        ));
                    }
                    if seg.edge.types.len() > 1 {
                        return Err(ExecutionError::PlanningError(
                            "A single relationship type must be specified for MERGE".to_string(),
                        ));
                    }
                    if seg.edge.length.is_some() {
                        return Err(ExecutionError::PlanningError(
                            "Variable length relationships cannot be used in MERGE".to_string(),
                        ));
                    }
                    // Undirected MERGE is valid — defaults to outgoing when creating
                }
            }
        }

        // Validate MERGE: reject literal null in edge properties
        if let Some(mc) = &query.merge_clause {
            for path in &mc.pattern.paths {
                for seg in &path.segments {
                    if let Some(ref props) = seg.edge.properties {
                        for (k, v) in props {
                            if matches!(v, PropertyValue::Null) {
                                return Err(ExecutionError::RuntimeError(format!(
                                    "MERGE does not support null property value for '{}'",
                                    k
                                )));
                            }
                        }
                    }
                }
            }
        }

        // Validate MERGE re-binding: MATCH (a) MERGE (a) should fail
        if let Some(mc) = &query.merge_clause {
            let m_node_vars: HashSet<String> = query
                .match_clauses
                .iter()
                .flat_map(|m| m.pattern.paths.iter())
                .filter_map(|p| p.start.variable.clone())
                .collect();
            for path in &mc.pattern.paths {
                if let Some(v) = &path.start.variable {
                    if m_node_vars.contains(v) && path.segments.is_empty() {
                        return Err(ExecutionError::PlanningError(format!(
                            "Variable '{}' already declared in MATCH",
                            v
                        )));
                    }
                }
            }
        }

        // Validate: aggregation in WHERE clause
        if let Some(wc) = &query.where_clause {
            if contains_aggregation(&wc.predicate) {
                return Err(ExecutionError::PlanningError(
                    "Aggregation expressions are not allowed in WHERE".to_string(),
                ));
            }
        }

        // Validate: RETURN * without named variables in scope
        if let Some(rc) = &query.return_clause {
            if rc.star {
                let has_named_vars = query.match_clauses.iter().any(|mc| {
                    mc.pattern.paths.iter().any(|p| {
                        p.start.variable.is_some()
                            || p.segments
                                .iter()
                                .any(|s| s.edge.variable.is_some() || s.node.variable.is_some())
                    })
                }) || query.with_clause.is_some()
                    || query.unwind_clause.is_some()
                    || query.create_clause.is_some();
                if !has_named_vars && !query.match_clauses.is_empty() {
                    return Err(ExecutionError::PlanningError(
                        "RETURN * is not allowed when there are no variables in scope".to_string(),
                    ));
                }
            }

            // Validate: RETURN references only defined variables
            if !rc.star && !query.match_clauses.is_empty() {
                let mut defined_vars: HashSet<String> = HashSet::new();
                for mc in &query.match_clauses {
                    for path in &mc.pattern.paths {
                        if let Some(v) = &path.start.variable {
                            defined_vars.insert(v.clone());
                        }
                        if let Some(v) = &path.path_variable {
                            defined_vars.insert(v.clone());
                        }
                        for seg in &path.segments {
                            if let Some(v) = &seg.edge.variable {
                                defined_vars.insert(v.clone());
                            }
                            if let Some(v) = &seg.node.variable {
                                defined_vars.insert(v.clone());
                            }
                        }
                    }
                    // SEARCH clause SCORE AS alias introduces a new variable
                    if let Some(ref sc) = mc.search_clause {
                        if let Some(ref alias) = sc.score_alias {
                            defined_vars.insert(alias.clone());
                        }
                    }
                }
                if let Some(wc) = &query.with_clause {
                    for item in &wc.items {
                        if let Some(a) = &item.alias {
                            defined_vars.insert(a.clone());
                        } else if let Expression::Variable(v) = &item.expression {
                            defined_vars.insert(v.clone());
                        }
                    }
                }
                if let Some(uc) = &query.unwind_clause {
                    defined_vars.insert(uc.variable.clone());
                }
                for u in &query.additional_unwinds {
                    defined_vars.insert(u.variable.clone());
                }
                if let Some(mc) = &query.merge_clause {
                    for path in &mc.pattern.paths {
                        if let Some(v) = &path.start.variable {
                            defined_vars.insert(v.clone());
                        }
                        if let Some(v) = &path.path_variable {
                            defined_vars.insert(v.clone());
                        }
                        for seg in &path.segments {
                            if let Some(v) = &seg.edge.variable {
                                defined_vars.insert(v.clone());
                            }
                            if let Some(v) = &seg.node.variable {
                                defined_vars.insert(v.clone());
                            }
                        }
                    }
                }
                if let Some(cc) = &query.create_clause {
                    for path in &cc.pattern.paths {
                        if let Some(v) = &path.start.variable {
                            defined_vars.insert(v.clone());
                        }
                        for seg in &path.segments {
                            if let Some(v) = &seg.edge.variable {
                                defined_vars.insert(v.clone());
                            }
                            if let Some(v) = &seg.node.variable {
                                defined_vars.insert(v.clone());
                            }
                        }
                    }
                }

                for item in &rc.items {
                    if let Expression::Variable(v) = &item.expression {
                        if !defined_vars.contains(v) {
                            return Err(ExecutionError::PlanningError(format!(
                                "Variable `{}` not defined",
                                v
                            )));
                        }
                    }
                }
            }
        }

        // Handle SHOW VECTOR INDEXES
        if query.show_vector_indexes {
            return Ok(ExecutionPlan {
                root: Box::new(ShowVectorIndexesOperator::new()),
                output_columns: vec![
                    "name".to_string(),
                    "label".to_string(),
                    "property".to_string(),
                    "dimensions".to_string(),
                    "similarity".to_string(),
                    "vectors".to_string(),
                    "type".to_string(),
                ],
                is_write: false,
            });
        }

        // Handle SHOW INDEXES
        if query.show_indexes {
            return Ok(ExecutionPlan {
                root: Box::new(ShowIndexesOperator::new()),
                output_columns: vec![
                    "label".to_string(),
                    "property".to_string(),
                    "type".to_string(),
                ],
                is_write: false,
            });
        }

        // Handle SHOW CONSTRAINTS
        if query.show_constraints {
            return Ok(ExecutionPlan {
                root: Box::new(ShowConstraintsOperator::new()),
                output_columns: vec![
                    "label".to_string(),
                    "property".to_string(),
                    "type".to_string(),
                ],
                is_write: false,
            });
        }

        // Handle CREATE CONSTRAINT
        if let Some(clause) = &query.create_constraint_clause {
            return Ok(ExecutionPlan {
                root: Box::new(CreateConstraintOperator::new(
                    clause.label.clone(),
                    clause.property.clone(),
                )),
                output_columns: vec![],
                is_write: true,
            });
        }

        // Handle DROP INDEX
        if let Some(clause) = &query.drop_index_clause {
            return Ok(ExecutionPlan {
                root: Box::new(DropIndexOperator::new(
                    clause.label.clone(),
                    clause.property.clone(),
                )),
                output_columns: vec![],
                is_write: true,
            });
        }

        // Handle CREATE VECTOR INDEX
        if let Some(clause) = &query.create_vector_index_clause {
            return Ok(ExecutionPlan {
                root: Box::new(CreateVectorIndexOperator::new(
                    clause.label.clone(),
                    clause.property_key.clone(),
                    clause.dimensions,
                    clause.similarity.clone(),
                    clause.if_not_exists,
                )),
                output_columns: vec![],
                is_write: true,
            });
        }

        // Handle CREATE INDEX (supports composite indexes)
        if let Some(clause) = &query.create_index_clause {
            // For composite indexes, create individual indexes for each property
            // The first property gets a dedicated CreateIndexOperator
            // Additional properties are also indexed
            if clause.additional_properties.is_empty() {
                return Ok(ExecutionPlan {
                    root: Box::new(CreateIndexOperator::new(
                        clause.label.clone(),
                        clause.property.clone(),
                    )),
                    output_columns: vec![],
                    is_write: true,
                });
            } else {
                // Composite index: create operator for first property
                // Additional properties are created in sequence
                return Ok(ExecutionPlan {
                    root: Box::new(CompositeCreateIndexOperator::new(
                        clause.label.clone(),
                        std::iter::once(clause.property.clone())
                            .chain(clause.additional_properties.iter().cloned())
                            .collect(),
                    )),
                    output_columns: vec![],
                    is_write: true,
                });
            }
        }

        // Handle multi-part write queries (CREATE...SET...WITH...UNWIND...CREATE pattern)
        // Each MultiPartStage represents clauses between two WITH barriers.
        // The pipeline: Stage1 ops → WithBarrier → Stage2 ops → WithBarrier → ... → final stage
        if !query.multi_part_stages.is_empty() {
            return self.plan_multi_part_write(query, store);
        }

        // Handle MERGE-only statement (no MATCH needed)
        if query.match_clauses.is_empty()
            && query.call_clause.is_none()
            && !query.all_merge_clauses.is_empty()
        {
            // First MERGE uses MergeOperator (produces its own records)
            let first_merge = &query.all_merge_clauses[0];
            let on_create: Vec<(String, String, Expression)> = first_merge
                .on_create_set
                .iter()
                .map(|s| (s.variable.clone(), s.property.clone(), s.value.clone()))
                .collect();
            let on_match: Vec<(String, String, Expression)> = first_merge
                .on_match_set
                .iter()
                .map(|s| (s.variable.clone(), s.property.clone(), s.value.clone()))
                .collect();

            let mut operator: OperatorBox = Box::new(MergeOperator::new(
                first_merge.pattern.clone(),
                on_create,
                on_match,
            ));

            // Chain remaining MERGEs with PerRowMergeOperator (shares variable bindings)
            for merge_clause in &query.all_merge_clauses[1..] {
                let oc: Vec<(String, String, Expression)> = merge_clause
                    .on_create_set
                    .iter()
                    .map(|s| (s.variable.clone(), s.property.clone(), s.value.clone()))
                    .collect();
                let om: Vec<(String, String, Expression)> = merge_clause
                    .on_match_set
                    .iter()
                    .map(|s| (s.variable.clone(), s.property.clone(), s.value.clone()))
                    .collect();
                operator = Box::new(PerRowMergeOperator::new(
                    operator,
                    merge_clause.pattern.clone(),
                    oc,
                    om,
                ));
            }

            // Apply CREATE clauses after MERGE (Bug 8 fix)
            if query.create_clause.is_some() || !query.create_clauses.is_empty() {
                let mut edges = Vec::new();
                for cc in query
                    .create_clause
                    .iter()
                    .chain(query.create_clauses.iter())
                {
                    for path in &cc.pattern.paths {
                        for seg in &path.segments {
                            let sv = path.start.variable.clone().unwrap_or_default();
                            let tv = seg.node.variable.clone().unwrap_or_default();
                            let et = seg
                                .edge
                                .types
                                .first()
                                .cloned()
                                .unwrap_or_else(|| EdgeType::new("RELATED"));
                            let ep = seg.edge.properties.clone().unwrap_or_default();
                            let ev = seg.edge.variable.clone();
                            let eep = seg.edge.expression_properties.clone();
                            let (s, t) = match seg.edge.direction {
                                Direction::Incoming => (tv, sv),
                                _ => (sv, tv),
                            };
                            edges.push((s, t, et, ep, ev, eep));
                        }
                    }
                }
                if !edges.is_empty() {
                    operator = Box::new(MatchCreateEdgeOperator::new(operator, edges));
                }
            }

            // Apply SET clauses after MERGE (Bug 6 fix)
            if !query.set_clauses.is_empty() {
                let mut items = Vec::new();
                for set_clause in &query.set_clauses {
                    for item in &set_clause.items {
                        items.push((
                            item.variable.clone(),
                            item.property.clone(),
                            item.value.clone(),
                        ));
                    }
                }
                operator = Box::new(SetPropertyOperator::new(operator, items));
            }

            let mut output_columns = Vec::new();
            if let Some(return_clause) = &query.return_clause {
                let projections: Vec<(Expression, String)> = return_clause
                    .items
                    .iter()
                    .enumerate()
                    .map(|(i, item)| {
                        let alias = item
                            .alias
                            .clone()
                            .unwrap_or_else(|| match &item.expression {
                                Expression::Variable(v) => v.clone(),
                                Expression::Property { variable, property } => {
                                    format!("{}.{}", variable, property)
                                }
                                _ => format!("col_{}", i),
                            });
                        output_columns.push(alias.clone());
                        (item.expression.clone(), alias)
                    })
                    .collect();
                operator = Box::new(ProjectOperator::new(operator, projections));
            }

            return Ok(ExecutionPlan {
                root: operator,
                output_columns,
                is_write: true,
            });
        }

        // Handle CREATE-only queries (no MATCH/CALL required)
        let has_unwind = query.unwind_clause.is_some() || !query.additional_unwinds.is_empty();
        if query.match_clauses.is_empty() && query.call_clause.is_none() {
            if !has_unwind && !query.create_clauses.is_empty() {
                let mut plan = self.plan_create_only_multi(&query.create_clauses)?;
                // Apply RETURN, SKIP, LIMIT if present
                if let Some(rc) = &query.return_clause {
                    let projections: Vec<(Expression, String)> = rc
                        .items
                        .iter()
                        .enumerate()
                        .map(|(i, item)| {
                            let alias =
                                item.alias
                                    .clone()
                                    .unwrap_or_else(|| match &item.expression {
                                        Expression::Variable(v) => v.clone(),
                                        Expression::Property { variable, property } => {
                                            format!("{}.{}", variable, property)
                                        }
                                        _ => format!("col_{}", i),
                                    });
                            (item.expression.clone(), alias)
                        })
                        .collect();
                    plan.output_columns = projections.iter().map(|(_, a)| a.clone()).collect();
                    plan.root = Box::new(ProjectOperator::new(plan.root, projections));
                }
                if let Some(skip) = query.skip {
                    plan.root = Box::new(SkipOperator::new(plan.root, skip));
                }
                if let Some(limit) = query.limit {
                    plan.root = Box::new(LimitOperator::new(plan.root, limit));
                }
                return Ok(plan);
            }
            if !has_unwind && query.create_clause.is_some() {
                let create_clause = query.create_clause.as_ref().unwrap();
                let mut plan = self.plan_create_only_multi(std::slice::from_ref(create_clause))?;
                if let Some(rc) = &query.return_clause {
                    let projections: Vec<(Expression, String)> = rc
                        .items
                        .iter()
                        .enumerate()
                        .map(|(i, item)| {
                            let alias =
                                item.alias
                                    .clone()
                                    .unwrap_or_else(|| match &item.expression {
                                        Expression::Variable(v) => v.clone(),
                                        Expression::Property { variable, property } => {
                                            format!("{}.{}", variable, property)
                                        }
                                        _ => format!("col_{}", i),
                                    });
                            (item.expression.clone(), alias)
                        })
                        .collect();
                    plan.output_columns = projections.iter().map(|(_, a)| a.clone()).collect();
                    plan.root = Box::new(ProjectOperator::new(plan.root, projections));
                }
                if let Some(skip) = query.skip {
                    plan.root = Box::new(SkipOperator::new(plan.root, skip));
                }
                if let Some(limit) = query.limit {
                    plan.root = Box::new(LimitOperator::new(plan.root, limit));
                }
                return Ok(plan);
            }

            // Handle UNWIND+CREATE(+WITH+RETURN) as a per-row pipeline
            if (query.unwind_clause.is_some() || !query.additional_unwinds.is_empty())
                && query.create_clause.is_some()
            {
                let mut operator: OperatorBox = Box::new(SingleRowOperator::new());

                // Build UNWIND chain
                for unwind in &query.additional_unwinds {
                    operator = Box::new(UnwindOperator::new(
                        operator,
                        unwind.expression.clone(),
                        unwind.variable.clone(),
                    ));
                }
                if let Some(unwind) = &query.unwind_clause {
                    operator = Box::new(UnwindOperator::new(
                        operator,
                        unwind.expression.clone(),
                        unwind.variable.clone(),
                    ));
                }

                // Build per-row CREATE from the create clause
                let create_clause = match query.create_clause.as_ref() {
                    Some(c) => c,
                    None => unreachable!(),
                };
                let mut node_specs = Vec::new();
                let mut edge_specs = Vec::new();

                for path in &create_clause.pattern.paths {
                    let labels = path.start.labels.clone();
                    let static_props = path.start.properties.clone().unwrap_or_default();
                    let expr_props = path.start.expression_properties.clone();
                    // Always generate a name for anonymous nodes so edges can reference them
                    let effective_start = path.start.variable.clone()
                        .unwrap_or_else(|| format!("__pcreate_anon_{}", node_specs.len() + 1));
                    node_specs.push((labels, static_props, expr_props, Some(effective_start.clone())));

                    let mut current_var = effective_start;

                    for seg in &path.segments {
                        let seg_labels = seg.node.labels.clone();
                        let seg_props = seg.node.properties.clone().unwrap_or_default();
                        let seg_expr_props = seg.node.expression_properties.clone();
                        let effective_target = seg.node.variable.clone()
                            .unwrap_or_else(|| format!("__pcreate_anon_{}", node_specs.len() + 1));
                        node_specs.push((
                            seg_labels,
                            seg_props,
                            seg_expr_props,
                            Some(effective_target.clone()),
                        ));

                        let target_name = effective_target;

                        if let Some(et) = seg.edge.types.first() {
                            let edge_props = seg.edge.properties.clone().unwrap_or_default();
                            let (src, tgt) = if matches!(seg.edge.direction, Direction::Incoming) {
                                (target_name.clone(), current_var.clone())
                            } else {
                                (current_var.clone(), target_name.clone())
                            };
                            edge_specs.push((
                                src,
                                tgt,
                                et.clone(),
                                edge_props,
                                seg.edge.variable.clone(),
                                seg.edge.expression_properties.clone(),
                            ));
                        }
                        current_var = target_name;
                    }
                }

                operator = Box::new(PerRowCreateOperator::new(operator, node_specs, edge_specs));
                let _ = true; // is_write set in plan

                // Handle WITH clause (aggregation, projection, filtering)
                if let Some(with_cl) = &query.with_clause {
                    let mut agg_counter = 0usize;
                    let mut has_aggregation = false;
                    let mut with_item_info: Vec<(String, Expression, Vec<AggregateFunction>)> =
                        Vec::new();
                    for (idx, item) in with_cl.items.iter().enumerate() {
                        let alias = item
                            .alias
                            .clone()
                            .unwrap_or_else(|| match &item.expression {
                                Expression::Variable(v) => v.clone(),
                                _ => format!("col_{}", idx),
                            });
                        let (rewritten, extracted) =
                            extract_nested_aggregates(&item.expression, &mut agg_counter);
                        if !extracted.is_empty() {
                            has_aggregation = true;
                        }
                        with_item_info.push((alias, rewritten, extracted));
                    }

                    if has_aggregation {
                        let mut agg_funcs = Vec::new();
                        let mut group_by = Vec::new();
                        let mut post_projections = Vec::new();
                        for (alias, rewritten, extracted) in with_item_info {
                            if !extracted.is_empty() {
                                agg_funcs.extend(extracted);
                                post_projections.push((rewritten, alias));
                            } else {
                                group_by.push((Expression::Variable(alias.clone()), alias.clone()));
                                post_projections.push((Expression::Variable(alias.clone()), alias));
                            }
                        }
                        operator = Box::new(AggregateOperator::new(operator, group_by, agg_funcs));
                        operator = Box::new(ProjectOperator::new(operator, post_projections));
                    } else {
                        let with_projections: Vec<(Expression, String)> = with_cl
                            .items
                            .iter()
                            .enumerate()
                            .map(|(i, item)| {
                                let alias =
                                    item.alias
                                        .clone()
                                        .unwrap_or_else(|| match &item.expression {
                                            Expression::Variable(v) => v.clone(),
                                            _ => format!("col_{}", i),
                                        });
                                (item.expression.clone(), alias)
                            })
                            .collect();
                        operator = Box::new(ProjectOperator::new(operator, with_projections));
                    }

                    if let Some(wc) = &with_cl.where_clause {
                        operator = Box::new(FilterOperator::new(operator, wc.predicate.clone()));
                    }
                }

                // Handle RETURN (with aggregation detection)
                let mut output_columns = Vec::new();
                if let Some(rc) = &query.return_clause {
                    let mut agg_counter = 0usize;
                    let mut has_aggregation = false;
                    let mut ret_item_info: Vec<(String, Expression, Vec<AggregateFunction>)> = Vec::new();
                    for (idx, item) in rc.items.iter().enumerate() {
                        let alias = item.alias.clone().unwrap_or_else(|| match &item.expression {
                            Expression::Variable(v) => v.clone(),
                            Expression::Property { variable, property } => format!("{}.{}", variable, property),
                            _ => format!("col_{}", idx),
                        });
                        let (rewritten, extracted) = extract_nested_aggregates(&item.expression, &mut agg_counter);
                        if !extracted.is_empty() { has_aggregation = true; }
                        output_columns.push(alias.clone());
                        ret_item_info.push((alias, rewritten, extracted));
                    }

                    if has_aggregation {
                        let mut agg_funcs = Vec::new();
                        let mut group_by = Vec::new();
                        let mut post_projections = Vec::new();
                        for (alias, rewritten, extracted) in ret_item_info {
                            if !extracted.is_empty() {
                                agg_funcs.extend(extracted);
                                post_projections.push((rewritten, alias));
                            } else {
                                group_by.push((Expression::Variable(alias.clone()), alias.clone()));
                                post_projections.push((Expression::Variable(alias.clone()), alias));
                            }
                        }
                        operator = Box::new(AggregateOperator::new(operator, group_by, agg_funcs));
                        operator = Box::new(ProjectOperator::new(operator, post_projections));
                    } else {
                        let projections: Vec<(Expression, String)> = rc
                            .items
                            .iter()
                            .enumerate()
                            .map(|(i, item)| {
                                let alias = item.alias.clone().unwrap_or_else(|| match &item.expression {
                                    Expression::Variable(v) => v.clone(),
                                    Expression::Property { variable, property } => format!("{}.{}", variable, property),
                                    _ => format!("col_{}", i),
                                });
                                (item.expression.clone(), alias)
                            })
                            .collect();
                        operator = Box::new(ProjectOperator::new(operator, projections));
                    }

                    if let Some(skip) = query.skip {
                        operator = Box::new(SkipOperator::new(operator, skip));
                    }
                    if let Some(limit) = query.limit {
                        operator = Box::new(LimitOperator::new(operator, limit));
                    }
                }

                return Ok(ExecutionPlan {
                    root: operator,
                    output_columns,
                    is_write: true,
                });
            }

            // Handle standalone RETURN (no MATCH/CREATE): e.g. RETURN 1+2 AS x
            // Also handles standalone UNWIND ... RETURN ...
            if let Some(return_clause) = &query.return_clause {
                let mut operator: OperatorBox = Box::new(SingleRowOperator::new());

                // Apply UNWINDs. Split into pre-WITH and post-WITH groups.
                // An UNWIND referencing a variable defined by WITH must go after the WITH.
                let with_vars: HashSet<String> = query.with_clause.as_ref()
                    .map(|wc| wc.items.iter().map(|item| {
                        item.alias.clone().unwrap_or_else(|| match &item.expression {
                            Expression::Variable(v) => v.clone(),
                            _ => String::new(),
                        })
                    }).filter(|s| !s.is_empty()).collect())
                    .unwrap_or_default();

                let refs_with_var = |expr: &Expression| -> bool {
                    match expr {
                        Expression::Variable(v) => with_vars.contains(v),
                        _ => false,
                    }
                };

                let mut post_with_unwinds = Vec::new();
                for unwind in &query.additional_unwinds {
                    if refs_with_var(&unwind.expression) {
                        post_with_unwinds.push(unwind.clone());
                    } else {
                        operator = Box::new(UnwindOperator::new(
                            operator,
                            unwind.expression.clone(),
                            unwind.variable.clone(),
                        ));
                    }
                }
                if let Some(unwind) = &query.unwind_clause {
                    if refs_with_var(&unwind.expression) {
                        post_with_unwinds.push(unwind.clone());
                    } else {
                        operator = Box::new(UnwindOperator::new(
                            operator,
                            unwind.expression.clone(),
                            unwind.variable.clone(),
                        ));
                    }
                }

                // Handle extra WITH stages
                for (with_cl, unwind_opt, _post_matches, _post_where) in &query.extra_with_stages {
                    let with_projections: Vec<(Expression, String)> = with_cl
                        .items
                        .iter()
                        .enumerate()
                        .map(|(i, item)| {
                            let alias =
                                item.alias
                                    .clone()
                                    .unwrap_or_else(|| match &item.expression {
                                        Expression::Variable(v) => v.clone(),
                                        _ => format!("col_{}", i),
                                    });
                            (item.expression.clone(), alias)
                        })
                        .collect();
                    operator = Box::new(ProjectOperator::new(operator, with_projections));

                    if let Some(unwind) = unwind_opt {
                        operator = Box::new(UnwindOperator::new(
                            operator,
                            unwind.expression.clone(),
                            unwind.variable.clone(),
                        ));
                    }
                }

                // Handle WITH clause (if there's a WITH before RETURN)
                if let Some(with_cl) = &query.with_clause {
                    let mut agg_counter = 0usize;
                    let mut has_aggregation = false;

                    // Check for aggregates
                    let mut with_item_info: Vec<(String, Expression, Vec<AggregateFunction>)> =
                        Vec::new();
                    for (idx, item) in with_cl.items.iter().enumerate() {
                        let alias = item
                            .alias
                            .clone()
                            .unwrap_or_else(|| match &item.expression {
                                Expression::Variable(v) => v.clone(),
                                _ => format!("col_{}", idx),
                            });
                        let (rewritten, extracted) =
                            extract_nested_aggregates(&item.expression, &mut agg_counter);
                        if !extracted.is_empty() {
                            has_aggregation = true;
                        }
                        with_item_info.push((alias, rewritten, extracted));
                    }

                    if has_aggregation {
                        let mut agg_funcs = Vec::new();
                        let mut group_by = Vec::new();
                        let mut post_projections = Vec::new();

                        for (alias, rewritten, extracted) in with_item_info {
                            if !extracted.is_empty() {
                                agg_funcs.extend(extracted);
                                post_projections.push((rewritten, alias));
                            } else {
                                let orig_expr = with_cl.items.iter()
                                    .find(|i| i.alias.as_deref() == Some(&alias) || matches!(&i.expression, Expression::Variable(v) if v == &alias))
                                    .map(|i| i.expression.clone())
                                    .unwrap_or(Expression::Variable(alias.clone()));
                                group_by.push((orig_expr, alias.clone()));
                                post_projections.push((Expression::Variable(alias.clone()), alias));
                            }
                        }

                        operator = Box::new(AggregateOperator::new(operator, group_by, agg_funcs));
                        operator = Box::new(ProjectOperator::new(operator, post_projections));
                    } else {
                        let with_projections: Vec<(Expression, String)> = with_cl
                            .items
                            .iter()
                            .enumerate()
                            .map(|(i, item)| {
                                let alias =
                                    item.alias
                                        .clone()
                                        .unwrap_or_else(|| match &item.expression {
                                            Expression::Variable(v) => v.clone(),
                                            _ => format!("col_{}", i),
                                        });
                                (item.expression.clone(), alias)
                            })
                            .collect();
                        operator = Box::new(ProjectOperator::new(operator, with_projections));
                    }

                    // Apply WITH WHERE clause as a filter
                    if let Some(wc) = &with_cl.where_clause {
                        operator = Box::new(FilterOperator::new(operator, wc.predicate.clone()));
                    }

                    // Apply WITH ORDER BY
                    if let Some(ob) = &with_cl.order_by {
                        let sort_items: Vec<(Expression, bool)> = ob
                            .items
                            .iter()
                            .map(|si| (si.expression.clone(), si.ascending))
                            .collect();
                        operator = Box::new(SortOperator::new(operator, sort_items));
                    }

                    // Apply WITH SKIP
                    if let Some(skip) = with_cl.skip {
                        operator = Box::new(SkipOperator::new(operator, skip));
                    }

                    // Apply WITH LIMIT
                    if let Some(limit) = with_cl.limit {
                        operator = Box::new(LimitOperator::new(operator, limit));
                    }

                    // Apply post-WITH UNWINDs (those that reference WITH-defined variables)
                    for unwind in &post_with_unwinds {
                        operator = Box::new(UnwindOperator::new(
                            operator,
                            unwind.expression.clone(),
                            unwind.variable.clone(),
                        ));
                    }
                }

                // Expand RETURN * — collect vars from UNWIND and WITH clauses
                let effective_items = if return_clause.star {
                    let mut star_vars: Vec<String> = Vec::new();
                    // Add WITH-defined vars
                    if let Some(wc) = &query.with_clause {
                        for item in &wc.items {
                            let alias = item.alias.clone().unwrap_or_else(|| match &item.expression {
                                Expression::Variable(v) => v.clone(),
                                _ => String::new(),
                            });
                            if !alias.is_empty() {
                                star_vars.push(alias);
                            }
                        }
                    }
                    // Add UNWIND-defined vars
                    for u in &query.additional_unwinds {
                        if !star_vars.contains(&u.variable) {
                            star_vars.push(u.variable.clone());
                        }
                    }
                    if let Some(u) = &query.unwind_clause {
                        if !star_vars.contains(&u.variable) {
                            star_vars.push(u.variable.clone());
                        }
                    }
                    star_vars.sort();
                    let mut items: Vec<crate::query::ast::ReturnItem> = star_vars
                        .into_iter()
                        .map(|v| crate::query::ast::ReturnItem {
                            expression: Expression::Variable(v),
                            alias: None,
                        })
                        .collect();
                    items.extend(return_clause.items.iter().cloned());
                    items
                } else {
                    return_clause.items.clone()
                };

                let mut output_columns = Vec::new();
                let projections: Vec<(Expression, String)> = effective_items
                    .iter()
                    .enumerate()
                    .map(|(i, item)| {
                        let alias = item
                            .alias
                            .clone()
                            .unwrap_or_else(|| match &item.expression {
                                Expression::Variable(v) => v.clone(),
                                Expression::Property { variable, property } => {
                                    format!("{}.{}", variable, property)
                                }
                                _ => format!("col_{}", i),
                            });
                        output_columns.push(alias.clone());
                        (item.expression.clone(), alias)
                    })
                    .collect();

                // Check if RETURN has aggregation
                let mut agg_counter = 0usize;
                let mut has_return_agg = false;
                for item in &return_clause.items {
                    let (_, extracted) =
                        extract_nested_aggregates(&item.expression, &mut agg_counter);
                    if !extracted.is_empty() {
                        has_return_agg = true;
                        break;
                    }
                }

                if has_return_agg {
                    let mut agg_funcs = Vec::new();
                    let mut group_by_exprs = Vec::new();
                    let mut post_projections = Vec::new();
                    let mut agg_counter2 = 0usize;

                    for (idx, item) in return_clause.items.iter().enumerate() {
                        let alias = item
                            .alias
                            .clone()
                            .unwrap_or_else(|| match &item.expression {
                                Expression::Variable(v) => v.clone(),
                                _ => format!("col_{}", idx),
                            });
                        let (rewritten, extracted) =
                            extract_nested_aggregates(&item.expression, &mut agg_counter2);
                        if !extracted.is_empty() {
                            agg_funcs.extend(extracted);
                            post_projections.push((rewritten, alias));
                        } else {
                            group_by_exprs.push((item.expression.clone(), alias.clone()));
                            post_projections.push((Expression::Variable(alias.clone()), alias));
                        }
                    }

                    operator =
                        Box::new(AggregateOperator::new(operator, group_by_exprs, agg_funcs));
                    operator = Box::new(ProjectOperator::new(operator, post_projections));
                } else {
                    operator = Box::new(ProjectOperator::new(operator, projections));
                }

                // ORDER BY
                if let Some(order_by) = &query.order_by {
                    let sort_exprs: Vec<(Expression, bool)> = order_by
                        .items
                        .iter()
                        .map(|item| (item.expression.clone(), item.ascending))
                        .collect();
                    operator = Box::new(SortOperator::new(operator, sort_exprs));
                }

                // SKIP
                if let Some(skip) = query.skip {
                    operator = Box::new(SkipOperator::new(operator, skip));
                }

                // LIMIT
                if let Some(limit) = query.limit {
                    operator = Box::new(LimitOperator::new(operator, limit));
                }

                return Ok(ExecutionPlan {
                    root: operator,
                    output_columns,
                    is_write: false,
                });
            }

            // Handle standalone UNWIND without RETURN (shouldn't happen per grammar but safety)
            return Err(ExecutionError::PlanningError(
                "Query must have at least one MATCH, CALL or CREATE clause".to_string(),
            ));
        }

        let mut operator: Option<OperatorBox> = None;
        let mut known_vars: HashSet<String> = HashSet::new();

        // Determine split point for WITH barrier
        let split = query.with_split_index.unwrap_or(query.match_clauses.len());
        let pre_with_clauses = &query.match_clauses[..split];
        let post_with_clauses = &query.match_clauses[split..];

        // Pre-compute variable sets for each pre-WITH MATCH clause
        let pre_match_var_sets: Vec<HashSet<String>> = pre_with_clauses
            .iter()
            .map(|mc| {
                let mut vars = HashSet::new();
                for path in &mc.pattern.paths {
                    if let Some(v) = &path.start.variable {
                        vars.insert(v.clone());
                    }
                    for seg in &path.segments {
                        if let Some(v) = &seg.node.variable {
                            vars.insert(v.clone());
                        }
                        if let Some(v) = &seg.edge.variable {
                            vars.insert(v.clone());
                        }
                    }
                    if let Some(v) = &path.path_variable {
                        vars.insert(v.clone());
                    }
                }
                vars
            })
            .collect();

        // Decompose WHERE clause: assign predicates to MATCH clauses or cross-MATCH
        let pre_where_preds = query
            .where_clause
            .as_ref()
            .map(|wc| flatten_and_predicates(&wc.predicate))
            .unwrap_or_default();
        let mut per_match_where: Vec<Option<WhereClause>> = vec![None; pre_with_clauses.len()];
        let mut cross_match_predicates: Vec<Expression> = Vec::new();

        for pred in pre_where_preds {
            let mut pred_vars = HashSet::new();
            Self::collect_expression_variables(&pred, &mut pred_vars);

            let target = pre_match_var_sets.iter().position(|match_vars| {
                pred_vars.is_empty() || pred_vars.iter().all(|v| match_vars.contains(v))
            });
            if let Some(i) = target {
                match &mut per_match_where[i] {
                    Some(wc) => {
                        wc.predicate = Expression::Binary {
                            left: Box::new(wc.predicate.clone()),
                            op: BinaryOp::And,
                            right: Box::new(pred),
                        };
                    }
                    None => {
                        per_match_where[i] = Some(WhereClause { predicate: pred });
                    }
                }
            } else {
                cross_match_predicates.push(pred);
            }
        }

        // 1a. Handle pre-WITH MATCH clauses
        for (match_idx, match_clause) in pre_with_clauses.iter().enumerate() {
            let match_op =
                self.dispatch_plan_match(match_clause, per_match_where[match_idx].as_ref(), store)?;

            let clause_vars = pre_match_var_sets[match_idx].clone();

            operator = Some(match operator {
                Some(existing) => {
                    let shared: Vec<String> =
                        known_vars.intersection(&clause_vars).cloned().collect();
                    if !shared.is_empty() {
                        if match_clause.optional {
                            let right_only: Vec<String> =
                                clause_vars.difference(&known_vars).cloned().collect();
                            // Extract cross-match predicates that reference OPTIONAL MATCH vars
                            // and attach them as post-join filter (preserving null rows)
                            let mut join_filter: Option<Expression> = None;
                            cross_match_predicates.retain(|pred| {
                                let mut pred_vars = HashSet::new();
                                Self::collect_expression_variables(pred, &mut pred_vars);
                                let refs_optional = pred_vars.iter().any(|v| clause_vars.contains(v) && !known_vars.contains(v));
                                if refs_optional {
                                    join_filter = Some(match join_filter.take() {
                                        Some(existing_f) => Expression::Binary {
                                            left: Box::new(existing_f),
                                            op: BinaryOp::And,
                                            right: Box::new(pred.clone()),
                                        },
                                        None => pred.clone(),
                                    });
                                    false
                                } else {
                                    true
                                }
                            });
                            let mut join_op = LeftOuterJoinOperator::new(
                                existing,
                                match_op,
                                shared[0].clone(),
                                right_only,
                            );
                            if let Some(filter) = join_filter {
                                join_op = join_op.with_filter(filter);
                            }
                            Box::new(join_op) as OperatorBox
                        } else {
                            Box::new(JoinOperator::new(existing, match_op, shared[0].clone()))
                                as OperatorBox
                        }
                    } else {
                        if match_clause.optional {
                            let right_only: Vec<String> = clause_vars.iter().cloned().collect();
                            Box::new(LeftOuterJoinOperator::new(
                                existing,
                                match_op,
                                String::new(),
                                right_only,
                            )) as OperatorBox
                        } else {
                            Box::new(CartesianProductOperator::new(existing, match_op))
                                as OperatorBox
                        }
                    }
                }
                None => {
                    if match_clause.optional {
                        // Standalone OPTIONAL MATCH: wrap with LeftOuterJoin so
                        // we get a null row if nothing matches
                        let right_only: Vec<String> = clause_vars.iter().cloned().collect();
                        let single_row: OperatorBox = Box::new(SingleRowOperator::new());
                        Box::new(LeftOuterJoinOperator::new(
                            single_row,
                            match_op,
                            String::new(),
                            right_only,
                        )) as OperatorBox
                    } else {
                        match_op
                    }
                }
            });
            known_vars.extend(clause_vars);
        }

        // Apply cross-MATCH predicates after all pre-WITH MATCH clauses are joined
        if !cross_match_predicates.is_empty() {
            if let Some(op) = operator {
                let filter_expr = cross_match_predicates
                    .into_iter()
                    .reduce(|acc, pred| Expression::Binary {
                        left: Box::new(acc),
                        op: BinaryOp::And,
                        right: Box::new(pred),
                    })
                    .unwrap();
                operator = Some(Box::new(FilterOperator::new(op, filter_expr)));
            }
        }

        // 1b. Insert WITH barrier if WITH clause is present and has post-WITH clauses
        if let Some(with_clause) = &query.with_clause {
            if let Some(op) = operator {
                // Parse WITH items into projections and aggregations
                // Uses extract_nested_aggregates to handle aggregates nested in expressions
                // e.g. round(sum(b.runs) * 100 / sum(b.balls)) / 100 AS strike_rate
                let mut items = Vec::new();
                let mut aggregates = Vec::new();
                let mut group_by = Vec::new();
                let mut has_aggregation = false;
                let mut agg_counter = 0usize;

                // First pass: detect aggregates
                struct WithItemInfo {
                    alias: String,
                    original_expr: Expression,
                    rewritten_expr: Expression,
                    extracted_aggs: Vec<AggregateFunction>,
                }
                let mut item_infos = Vec::new();

                for (idx, item) in with_clause.items.iter().enumerate() {
                    let alias = item
                        .alias
                        .clone()
                        .unwrap_or_else(|| match &item.expression {
                            Expression::Variable(var) => var.clone(),
                            Expression::Property { variable, property } => {
                                format!("{}.{}", variable, property)
                            }
                            Expression::Function {
                                name,
                                args,
                                distinct,
                            } => {
                                let arg_strs: Vec<String> = args
                                    .iter()
                                    .map(|a| match a {
                                        Expression::Variable(v) => v.clone(),
                                        Expression::Property { variable, property } => {
                                            format!("{}.{}", variable, property)
                                        }
                                        _ => "?".to_string(),
                                    })
                                    .collect();
                                if *distinct {
                                    format!("{}(DISTINCT {})", name, arg_strs.join(", "))
                                } else {
                                    format!("{}({})", name, arg_strs.join(", "))
                                }
                            }
                            _ => format!("col_{}", idx),
                        });

                    let (rewritten, extracted) =
                        extract_nested_aggregates(&item.expression, &mut agg_counter);
                    if !extracted.is_empty() {
                        has_aggregation = true;
                    }
                    item_infos.push(WithItemInfo {
                        alias,
                        original_expr: item.expression.clone(),
                        rewritten_expr: rewritten,
                        extracted_aggs: extracted,
                    });
                }

                // Second pass: build items, group_by, aggregates
                for info in item_infos {
                    if has_aggregation {
                        // Aggregation mode: items get post-projection expressions
                        if !info.extracted_aggs.is_empty() {
                            aggregates.extend(info.extracted_aggs);
                            items.push((info.rewritten_expr, info.alias.clone()));
                        } else {
                            group_by.push((info.original_expr, info.alias.clone()));
                            // Use Variable(alias) since after aggregation only aliases exist
                            items.push((
                                Expression::Variable(info.alias.clone()),
                                info.alias.clone(),
                            ));
                        }
                    } else {
                        // No aggregation: items keep original expressions
                        items.push((info.original_expr, info.alias.clone()));
                    }
                }

                // Parse WITH ORDER BY
                let sort_items: Vec<(Expression, bool)> = with_clause
                    .order_by
                    .as_ref()
                    .map(|ob| {
                        ob.items
                            .iter()
                            .map(|i| (i.expression.clone(), i.ascending))
                            .collect()
                    })
                    .unwrap_or_default();

                // Parse WITH WHERE
                let where_predicate = with_clause
                    .where_clause
                    .as_ref()
                    .map(|wc| wc.predicate.clone());

                operator = Some(Box::new(WithBarrierOperator::new(
                    op,
                    items.clone(),
                    aggregates,
                    group_by,
                    has_aggregation,
                    with_clause.distinct,
                    where_predicate,
                    sort_items,
                    with_clause.skip,
                    with_clause.limit,
                )));

                // Reset known_vars to only WITH output aliases
                known_vars.clear();
                for (_, alias) in &items {
                    known_vars.insert(alias.clone());
                }
            }
        }

        // 1c. Handle post-WITH MATCH clauses (join on variables from WITH output)
        // Pre-compute variable sets for post-WITH MATCH clauses
        let post_match_var_sets: Vec<HashSet<String>> = post_with_clauses
            .iter()
            .map(|mc| {
                let mut vars = HashSet::new();
                for path in &mc.pattern.paths {
                    if let Some(v) = &path.start.variable {
                        vars.insert(v.clone());
                    }
                    for seg in &path.segments {
                        if let Some(v) = &seg.node.variable {
                            vars.insert(v.clone());
                        }
                        if let Some(v) = &seg.edge.variable {
                            vars.insert(v.clone());
                        }
                    }
                }
                vars
            })
            .collect();

        // Decompose post-WITH WHERE clause: assign to MATCH clauses or cross-MATCH
        let post_where_preds = query
            .post_with_where_clause
            .as_ref()
            .map(|wc| flatten_and_predicates(&wc.predicate))
            .unwrap_or_default();
        let mut post_per_match_where: Vec<Option<WhereClause>> =
            vec![None; post_with_clauses.len()];
        let mut post_cross_match_preds: Vec<Expression> = Vec::new();

        for pred in post_where_preds {
            let mut pred_vars = HashSet::new();
            Self::collect_expression_variables(&pred, &mut pred_vars);

            let target = post_match_var_sets.iter().position(|match_vars| {
                pred_vars.is_empty() || pred_vars.iter().all(|v| match_vars.contains(v))
            });
            if let Some(i) = target {
                match &mut post_per_match_where[i] {
                    Some(wc) => {
                        wc.predicate = Expression::Binary {
                            left: Box::new(wc.predicate.clone()),
                            op: BinaryOp::And,
                            right: Box::new(pred),
                        };
                    }
                    None => {
                        post_per_match_where[i] = Some(WhereClause { predicate: pred });
                    }
                }
            } else {
                post_cross_match_preds.push(pred);
            }
        }

        for (match_idx, match_clause) in post_with_clauses.iter().enumerate() {
            let match_op = self.dispatch_plan_match(
                match_clause,
                post_per_match_where[match_idx].as_ref(),
                store,
            )?;

            let clause_vars = post_match_var_sets[match_idx].clone();

            operator = Some(match operator {
                Some(existing) => {
                    let shared: Vec<String> =
                        known_vars.intersection(&clause_vars).cloned().collect();
                    if !shared.is_empty() {
                        if match_clause.optional {
                            let right_only: Vec<String> =
                                clause_vars.difference(&known_vars).cloned().collect();
                            Box::new(LeftOuterJoinOperator::new(
                                existing,
                                match_op,
                                shared[0].clone(),
                                right_only,
                            )) as OperatorBox
                        } else {
                            Box::new(JoinOperator::new(existing, match_op, shared[0].clone()))
                                as OperatorBox
                        }
                    } else {
                        if match_clause.optional {
                            let right_only: Vec<String> = clause_vars.iter().cloned().collect();
                            Box::new(LeftOuterJoinOperator::new(
                                existing,
                                match_op,
                                String::new(),
                                right_only,
                            )) as OperatorBox
                        } else {
                            Box::new(CartesianProductOperator::new(existing, match_op))
                                as OperatorBox
                        }
                    }
                }
                None => {
                    if match_clause.optional {
                        let right_only: Vec<String> = clause_vars.iter().cloned().collect();
                        let single_row: OperatorBox = Box::new(SingleRowOperator::new());
                        Box::new(LeftOuterJoinOperator::new(
                            single_row,
                            match_op,
                            String::new(),
                            right_only,
                        )) as OperatorBox
                    } else {
                        match_op
                    }
                }
            });
            known_vars.extend(clause_vars);
        }

        // Apply post-WITH cross-MATCH predicates after all post-WITH MATCH clauses are joined
        if !post_cross_match_preds.is_empty() {
            if let Some(op) = operator {
                let filter_expr = post_cross_match_preds
                    .into_iter()
                    .reduce(|acc, pred| Expression::Binary {
                        left: Box::new(acc),
                        op: BinaryOp::And,
                        right: Box::new(pred),
                    })
                    .unwrap();
                operator = Some(Box::new(FilterOperator::new(op, filter_expr)));
            }
        }

        // 2. Handle CALL if present
        if let Some(call_clause) = &query.call_clause {
            let call_op = self.plan_call(call_clause)?;
            if let Some(existing_op) = operator {
                // Check for shared variables to decide between Join and Cartesian Product
                let mut shared_vars = Vec::new();

                // Collect variables from all MATCH clauses
                let mut match_vars = HashSet::new();
                for mc in &query.match_clauses {
                    for path in &mc.pattern.paths {
                        if let Some(v) = &path.start.variable {
                            match_vars.insert(v.clone());
                        }
                        for seg in &path.segments {
                            if let Some(v) = &seg.node.variable {
                                match_vars.insert(v.clone());
                            }
                            if let Some(v) = &seg.edge.variable {
                                match_vars.insert(v.clone());
                            }
                        }
                    }
                }

                // Check against CALL yield items
                for item in &call_clause.yield_items {
                    let var_name = item.alias.as_ref().unwrap_or(&item.name);
                    if match_vars.contains(var_name) {
                        shared_vars.push(var_name.clone());
                    }
                }

                if !shared_vars.is_empty() {
                    // Use JoinOperator on the first shared variable
                    operator = Some(Box::new(JoinOperator::new(
                        existing_op,
                        call_op,
                        shared_vars[0].clone(),
                    )));
                } else {
                    // Fallback to Cartesian Product
                    operator = Some(Box::new(CartesianProductOperator::new(
                        existing_op,
                        call_op,
                    )));
                }
            } else {
                operator = Some(call_op);
            }
        }

        let mut operator = operator.unwrap();

        // Add WHERE clause if present.
        // When a WITH clause exists, WHERE predicates were already decomposed and
        // pushed into per-MATCH/cross-MATCH filters above.
        // When MATCH clauses exist (pre_with_clauses), predicates were also decomposed
        // into per-match and cross-match filters — don't apply again (breaks OPTIONAL MATCH null rows).
        if query.with_clause.is_none() && pre_with_clauses.is_empty() {
            if let Some(where_clause) = &query.where_clause {
                operator = Box::new(FilterOperator::new(
                    operator,
                    where_clause.predicate.clone(),
                ));
            }
        }

        // Process extra WITH stages (multi-WITH support)
        for (extra_with, extra_unwind, extra_matches, extra_where) in &query.extra_with_stages {
            // Create WithBarrier for this stage
            operator = self.build_with_barrier(operator, extra_with, store)?;

            // Apply UNWIND for this stage (e.g., UNWIND top_players AS player)
            if let Some(unwind) = extra_unwind {
                operator = Box::new(UnwindOperator::new(
                    operator,
                    unwind.expression.clone(),
                    unwind.variable.clone(),
                ));
                known_vars.insert(unwind.variable.clone());
            }

            // Process post-WITH MATCH clauses for this stage
            for mc in extra_matches {
                let call_op = self.plan_match(mc, None, store)?;
                let call_vars: HashSet<String> = self.extract_match_vars(mc);
                let shared_vars: Vec<String> =
                    known_vars.intersection(&call_vars).cloned().collect();
                if !shared_vars.is_empty() {
                    operator =
                        Box::new(JoinOperator::new(operator, call_op, shared_vars[0].clone()));
                } else {
                    operator = Box::new(CartesianProductOperator::new(operator, call_op));
                }
                for v in call_vars {
                    known_vars.insert(v);
                }
            }

            // Apply post-WITH WHERE for this stage
            if let Some(where_clause) = extra_where {
                operator = Box::new(FilterOperator::new(
                    operator,
                    where_clause.predicate.clone(),
                ));
            }

            // Update known_vars to only include this WITH's outputs
            known_vars.clear();
            for item in &extra_with.items {
                let alias = item
                    .alias
                    .clone()
                    .unwrap_or_else(|| match &item.expression {
                        Expression::Variable(v) => v.clone(),
                        Expression::Property { variable, property } => {
                            format!("{}.{}", variable, property)
                        }
                        _ => "?".to_string(),
                    });
                known_vars.insert(alias);
            }
        }

        // Process main WITH clause (last WITH before RETURN in multi-part queries)
        if let Some(with_cl) = &query.with_clause {
            if !query.extra_with_stages.is_empty() {
                // This is a multi-WITH query — process the final WITH as a barrier
                operator = self.build_with_barrier(operator, with_cl, store)?;
                known_vars.clear();
                for item in &with_cl.items {
                    let alias = item
                        .alias
                        .clone()
                        .unwrap_or_else(|| match &item.expression {
                            Expression::Variable(v) => v.clone(),
                            Expression::Property { variable, property } => {
                                format!("{}.{}", variable, property)
                            }
                            _ => "?".to_string(),
                        });
                    known_vars.insert(alias);
                }
            }
        }

        // Add UNWIND clause if present
        if let Some(unwind_clause) = &query.unwind_clause {
            operator = Box::new(UnwindOperator::new(
                operator,
                unwind_clause.expression.clone(),
                unwind_clause.variable.clone(),
            ));
        }

        // Determine output columns
        let mut output_columns = Vec::new();

        // Check if this is a MATCH...CREATE query (create nodes/edges from CREATE pattern)
        let is_write = if let Some(create_clause) = &query.create_clause {
            let create_pattern = &create_clause.pattern;

            // First pass: collect new nodes to create (not in known_vars)
            // and edges to create between them
            let mut new_node_specs: Vec<(
                String,                         // variable
                Vec<Label>,                     // labels
                HashMap<String, PropertyValue>, // properties
                Vec<(String, Expression)>,      // expression_properties
            )> = Vec::new();
            let mut edges_to_create: Vec<(
                String,
                String,
                EdgeType,
                HashMap<String, PropertyValue>,
                Option<String>,
                Vec<(String, Expression)>,
            )> = Vec::new();

            for path in &create_pattern.paths {
                // Check start node
                if let Some(ref var) = path.start.variable {
                    if !known_vars.contains(var) && !path.start.labels.is_empty() {
                        new_node_specs.push((
                            var.clone(),
                            path.start.labels.clone(),
                            path.start.properties.clone().unwrap_or_default(),
                            path.start.expression_properties.clone(),
                        ));
                        known_vars.insert(var.clone());
                    }
                }

                let mut current_var = path.start.variable.clone();

                for segment in &path.segments {
                    let target_var = segment.node.variable.clone();
                    let edge = &segment.edge;

                    // Check if target is a new node
                    if let Some(ref tgt) = target_var {
                        if !known_vars.contains(tgt) && !segment.node.labels.is_empty() {
                            new_node_specs.push((
                                tgt.clone(),
                                segment.node.labels.clone(),
                                segment.node.properties.clone().unwrap_or_default(),
                                segment.node.expression_properties.clone(),
                            ));
                            known_vars.insert(tgt.clone());
                        }
                    }

                    let edge_type = edge
                        .types
                        .first()
                        .cloned()
                        .unwrap_or_else(|| EdgeType::new("RELATED_TO"));
                    let edge_properties = edge.properties.clone().unwrap_or_default();
                    let edge_variable = edge.variable.clone();
                    let edge_expr_props = edge.expression_properties.clone();

                    if let (Some(src), Some(tgt)) = (&current_var, &target_var) {
                        edges_to_create.push((
                            src.clone(),
                            tgt.clone(),
                            edge_type,
                            edge_properties,
                            edge_variable,
                            edge_expr_props,
                        ));
                    }

                    current_var = target_var;
                }
            }

            // Create new nodes first (via PerRowCreateOperator)
            if !new_node_specs.is_empty() {
                let node_specs: Vec<(
                    Vec<Label>,
                    HashMap<String, PropertyValue>,
                    Vec<(String, Expression)>,
                    Option<String>,
                )> = new_node_specs
                    .into_iter()
                    .map(|(var, labels, props, expr_props)| (labels, props, expr_props, Some(var)))
                    .collect();
                operator = Box::new(PerRowCreateOperator::new(operator, node_specs, Vec::new()));
            }

            // Then create edges
            if !edges_to_create.is_empty() {
                use crate::query::executor::operator::MatchCreateEdgeOperator;
                operator = Box::new(MatchCreateEdgeOperator::new(operator, edges_to_create));
            }

            true
        } else {
            false
        };

        // Handle DELETE clause
        let is_write = if let Some(delete_clause) = &query.delete_clause {
            let vars: Vec<String> = delete_clause
                .expressions
                .iter()
                .filter_map(|e| {
                    if let Expression::Variable(v) = e {
                        Some(v.clone())
                    } else {
                        None
                    }
                })
                .collect();
            operator = Box::new(DeleteOperator::new(operator, vars, delete_clause.detach));
            true
        } else {
            is_write
        };

        // Handle MERGE clauses (after MATCH)
        let is_write = if !query.all_merge_clauses.is_empty() {
            for merge_clause in &query.all_merge_clauses {
                let on_create: Vec<(String, String, Expression)> = merge_clause
                    .on_create_set
                    .iter()
                    .map(|s| (s.variable.clone(), s.property.clone(), s.value.clone()))
                    .collect();
                let on_match: Vec<(String, String, Expression)> = merge_clause
                    .on_match_set
                    .iter()
                    .map(|s| (s.variable.clone(), s.property.clone(), s.value.clone()))
                    .collect();
                let merge_vars = self.extract_pattern_vars(&merge_clause.pattern);
                operator = Box::new(PerRowMergeOperator::new(
                    operator,
                    merge_clause.pattern.clone(),
                    on_create,
                    on_match,
                ));
                for v in merge_vars {
                    known_vars.insert(v);
                }
            }
            true
        } else {
            is_write
        };

        // Handle SET clauses
        let is_write = if !query.set_clauses.is_empty() {
            let mut items = Vec::new();
            for set_clause in &query.set_clauses {
                for item in &set_clause.items {
                    items.push((
                        item.variable.clone(),
                        item.property.clone(),
                        item.value.clone(),
                    ));
                }
            }
            operator = Box::new(SetPropertyOperator::new(operator, items));
            true
        } else {
            is_write
        };

        // Handle REMOVE clauses
        let is_write = if !query.remove_clauses.is_empty() {
            let mut items = Vec::new();
            for remove_clause in &query.remove_clauses {
                for item in &remove_clause.items {
                    if let RemoveItem::Property { variable, property } = item {
                        items.push((variable.clone(), property.clone()));
                    }
                }
            }
            if !items.is_empty() {
                operator = Box::new(RemovePropertyOperator::new(operator, items));
            }
            true
        } else {
            is_write
        };

        // Handle FOREACH clause
        let is_write = if let Some(foreach_clause) = &query.foreach_clause {
            let mut set_items = Vec::new();
            for set_clause in &foreach_clause.set_clauses {
                for item in &set_clause.items {
                    set_items.push((
                        item.variable.clone(),
                        item.property.clone(),
                        item.value.clone(),
                    ));
                }
            }
            let create_patterns: Vec<Pattern> = foreach_clause
                .create_clauses
                .iter()
                .map(|c| c.pattern.clone())
                .collect();
            operator = Box::new(ForeachOperator::new(
                operator,
                foreach_clause.variable.clone(),
                foreach_clause.expression.clone(),
                set_items,
                create_patterns,
            ));
            true
        } else {
            is_write
        };

        // Add RETURN clause if present
        if let Some(return_clause) = &query.return_clause {
            let mut aggregates = Vec::new();
            let mut group_by = Vec::new();
            // Expand RETURN * to all known variables
            let effective_return_items = if return_clause.star {
                let mut star_items: Vec<crate::query::ast::ReturnItem> = known_vars
                    .iter()
                    .filter(|v| !v.starts_with("_anon_") && !v.starts_with("_create_anon_"))
                    .map(|v| crate::query::ast::ReturnItem {
                        expression: Expression::Variable(v.clone()),
                        alias: None,
                    })
                    .collect();
                star_items.sort_by(|a, b| {
                    let va = match &a.expression {
                        Expression::Variable(v) => v.clone(),
                        _ => String::new(),
                    };
                    let vb = match &b.expression {
                        Expression::Variable(v) => v.clone(),
                        _ => String::new(),
                    };
                    va.cmp(&vb)
                });
                star_items.extend(return_clause.items.iter().cloned());
                star_items
            } else {
                return_clause.items.clone()
            };

            let mut projections = Vec::new();
            let mut has_aggregation = false;
            let mut agg_counter = 0usize;
            let mut post_projections: Vec<(Expression, String)> = Vec::new();

            for (idx, item) in effective_return_items.iter().enumerate() {
                let alias = item
                    .alias
                    .clone()
                    .unwrap_or_else(|| match &item.expression {
                        Expression::Variable(var) => var.clone(),
                        Expression::Property { variable, property } => {
                            format!("{}.{}", variable, property)
                        }
                        Expression::Function {
                            name,
                            args,
                            distinct,
                        } => {
                            let arg_strs: Vec<String> = args
                                .iter()
                                .map(|a| match a {
                                    Expression::Variable(v) => v.clone(),
                                    Expression::Property { variable, property } => {
                                        format!("{}.{}", variable, property)
                                    }
                                    _ => "?".to_string(),
                                })
                                .collect();
                            if *distinct {
                                format!("{}(DISTINCT {})", name, arg_strs.join(", "))
                            } else {
                                format!("{}({})", name, arg_strs.join(", "))
                            }
                        }
                        _ => format!("col_{}", idx),
                    });

                output_columns.push(alias.clone());

                // Extract nested aggregates from expressions like round(sum(x) / sum(y))
                let (rewritten, extracted) =
                    extract_nested_aggregates(&item.expression, &mut agg_counter);

                if !extracted.is_empty() {
                    has_aggregation = true;
                    aggregates.extend(extracted);
                    post_projections.push((rewritten, alias.clone()));
                } else {
                    group_by.push((item.expression.clone(), alias.clone()));
                    projections.push((item.expression.clone(), alias.clone()));
                    // Use Variable(alias) for post-projection since after aggregation
                    // the record only has the alias bound, not the original expression
                    post_projections.push((Expression::Variable(alias.clone()), alias.clone()));
                }
            }

            if has_aggregation {
                operator = Box::new(AggregateOperator::new(operator, group_by, aggregates));
                // Post-aggregation projection: compute final expressions from aggregate aliases
                operator = Box::new(ProjectOperator::new(operator, post_projections));

                // Sort after aggregation + projection
                if let Some(order_by) = &query.order_by {
                    let mut sort_items = Vec::new();
                    for item in &order_by.items {
                        sort_items.push((item.expression.clone(), item.ascending));
                    }
                    operator = Box::new(SortOperator::new(operator, sort_items));
                }
            } else {
                // Non-aggregation: Sort -> Project
                if let Some(order_by) = &query.order_by {
                    let mut sort_items = Vec::new();
                    for item in &order_by.items {
                        sort_items.push((item.expression.clone(), item.ascending));
                    }
                    operator = Box::new(SortOperator::new(operator, sort_items));
                }

                operator = Box::new(ProjectOperator::new(operator, projections));
            }

            // Apply RETURN DISTINCT
            if return_clause.distinct {
                operator = Box::new(DistinctOperator::new(operator));
            }
        } else {
            // No explicit RETURN - return all matched/yielded variables
            for mc in &query.match_clauses {
                for path in &mc.pattern.paths {
                    if let Some(var) = &path.start.variable {
                        output_columns.push(var.clone());
                    }
                    for segment in &path.segments {
                        if let Some(var) = &segment.node.variable {
                            output_columns.push(var.clone());
                        }
                    }
                }
            }

            if let Some(call_clause) = &query.call_clause {
                for item in &call_clause.yield_items {
                    output_columns.push(item.alias.clone().unwrap_or_else(|| item.name.clone()));
                }
            }
        }

        // Add SKIP if present
        if let Some(skip) = query.skip {
            operator = Box::new(SkipOperator::new(operator, skip));
        }

        // Add LIMIT if present
        if let Some(limit) = query.limit {
            operator = Box::new(LimitOperator::new(operator, limit));
        }

        // QP-01: Predicate pushdown is handled inline during plan_match() via AND-chain decomposition
        // QP-02: Cost-based plan selection uses GraphStatistics to pick indexes over scans
        // QP-04: Early LIMIT propagation — done when NodeScanOperator gets early_limit set

        // For standalone CALL without RETURN, use YIELD items as output columns
        if output_columns.is_empty() {
            if let Some(call_clause) = &query.call_clause {
                output_columns = call_clause
                    .yield_items
                    .iter()
                    .map(|y| y.alias.clone().unwrap_or_else(|| y.name.clone()))
                    .collect();
            }
        }

        // Return execution plan
        Ok(ExecutionPlan {
            root: operator,
            output_columns,
            is_write,
        })
    }

    fn plan_call(&self, call_clause: &CallClause) -> ExecutionResult<OperatorBox> {
        if call_clause.procedure_name == "db.index.vector.queryNodes" {
            // CALL db.index.vector.queryNodes(label, property, vector, k) YIELD node, score
            if call_clause.arguments.len() < 4 {
                return Err(ExecutionError::PlanningError(
                    "db.index.vector.queryNodes requires 4 arguments: (label, property, query_vector, k)".to_string()
                ));
            }

            let label = match &call_clause.arguments[0] {
                Expression::Literal(PropertyValue::String(s)) => s.clone(),
                _ => {
                    return Err(ExecutionError::PlanningError(
                        "First argument (label) must be a string literal".to_string(),
                    ))
                }
            };

            let property = match &call_clause.arguments[1] {
                Expression::Literal(PropertyValue::String(s)) => s.clone(),
                _ => {
                    return Err(ExecutionError::PlanningError(
                        "Second argument (property) must be a string literal".to_string(),
                    ))
                }
            };

            let query_vector = match &call_clause.arguments[2] {
                Expression::Literal(PropertyValue::Vector(v)) => v.clone(),
                _ => {
                    return Err(ExecutionError::PlanningError(
                        "Third argument (vector) must be a vector literal".to_string(),
                    ))
                }
            };

            let k = match &call_clause.arguments[3] {
                Expression::Literal(PropertyValue::Integer(i)) => *i as usize,
                _ => {
                    return Err(ExecutionError::PlanningError(
                        "Fourth argument (k) must be an integer literal".to_string(),
                    ))
                }
            };

            let mut node_var = "node".to_string();
            let mut score_var = None;

            for item in &call_clause.yield_items {
                if item.name == "node" {
                    node_var = item.alias.clone().unwrap_or_else(|| item.name.clone());
                } else if item.name == "score" {
                    score_var = Some(item.alias.clone().unwrap_or_else(|| item.name.clone()));
                }
            }

            Ok(Box::new(VectorSearchOperator::new(
                label,
                property,
                query_vector,
                k,
                node_var,
                score_var,
            )))
        } else if call_clause.procedure_name == "db.labels" {
            Ok(Box::new(ShowLabelsOperator::new()))
        } else if call_clause.procedure_name == "db.relationshipTypes" {
            Ok(Box::new(ShowRelationshipTypesOperator::new()))
        } else if call_clause.procedure_name == "db.propertyKeys" {
            Ok(Box::new(ShowPropertyKeysOperator::new()))
        } else if call_clause.procedure_name == "db.schema.visualization" {
            Ok(Box::new(SchemaVisualizationOperator::new()))
        } else if call_clause.procedure_name.starts_with("algo.") {
            Ok(Box::new(AlgorithmOperator::new(
                call_clause.procedure_name.clone(),
                call_clause.arguments.clone(),
            )))
        } else if call_clause.procedure_name.starts_with("test.") {
            // TCK mock procedures
            let yield_vars: Vec<String> = if call_clause.yield_items.is_empty() {
                // Default output columns based on procedure name
                if call_clause.procedure_name == "test.my.proc" {
                    if call_clause.arguments.len() >= 2 {
                        vec!["city".to_string(), "country_code".to_string()]
                    } else {
                        vec!["out".to_string()]
                    }
                } else if call_clause.procedure_name == "test.labels" {
                    vec!["label".to_string()]
                } else {
                    vec![]
                }
            } else {
                call_clause
                    .yield_items
                    .iter()
                    .map(|y| y.name.clone())
                    .collect()
            };
            Ok(Box::new(MockProcedureOperator::new(
                call_clause.procedure_name.clone(),
                call_clause.arguments.clone(),
                yield_vars,
            )))
        } else {
            Err(ExecutionError::PlanningError(format!(
                "Unknown procedure: {}",
                call_clause.procedure_name
            )))
        }
    }

    /// Dispatch to graph-native or legacy planner based on configuration
    fn dispatch_plan_match(
        &self,
        match_clause: &MatchClause,
        where_clause: Option<&WhereClause>,
        store: &GraphStore,
    ) -> ExecutionResult<OperatorBox> {
        if self.config.graph_native {
            self.plan_match_native(match_clause, where_clause, store)
        } else {
            self.plan_match(match_clause, where_clause, store)
        }
    }

    /// Graph-native planner (ADR-015): enumerate candidate plans, choose cheapest
    fn plan_match_native(
        &self,
        match_clause: &MatchClause,
        where_clause: Option<&WhereClause>,
        store: &GraphStore,
    ) -> ExecutionResult<OperatorBox> {
        use super::logical_plan::PatternGraph;
        use super::physical_planner::logical_to_physical;
        use super::plan_enumerator::{enumerate_plans, EnumerationConfig};

        let pattern = &match_clause.pattern;
        if pattern.paths.is_empty() {
            return Err(ExecutionError::PlanningError(
                "Match pattern has no paths".to_string(),
            ));
        }

        let pg = PatternGraph::from_match_clause(match_clause);
        let catalog = store.catalog();
        let config = EnumerationConfig {
            max_candidate_plans: self.config.max_candidate_plans,
        };

        let candidates = enumerate_plans(&pg, where_clause, catalog, &config);
        if candidates.is_empty() {
            return Err(ExecutionError::PlanningError(
                "No valid plans enumerated".to_string(),
            ));
        }

        // Pick the cheapest plan (first one — already sorted)
        let (best_plan, _best_cost) = candidates.into_iter().next().unwrap();
        let physical = logical_to_physical(&best_plan);

        Ok(physical)
    }

    fn plan_match(
        &self,
        match_clause: &MatchClause,
        where_clause: Option<&WhereClause>,
        store: &GraphStore,
    ) -> ExecutionResult<OperatorBox> {
        let pattern = &match_clause.pattern;

        if pattern.paths.is_empty() {
            return Err(ExecutionError::PlanningError(
                "Match pattern has no paths".to_string(),
            ));
        }

        // QP-02/QP-03: Cost-based optimization — reorder paths by estimated cardinality (smallest first)
        let stats = store.compute_statistics();
        let mut paths_with_cost: Vec<(usize, f64)> = pattern
            .paths
            .iter()
            .enumerate()
            .map(|(i, path)| {
                let cost = if let Some(label) = path.start.labels.first() {
                    stats.estimate_label_scan(label) as f64
                } else {
                    f64::MAX // All-nodes scan is most expensive
                };
                (i, cost)
            })
            .collect();
        paths_with_cost.sort_by(|a, b| a.1.partial_cmp(&b.1).unwrap_or(std::cmp::Ordering::Equal));

        // Handle multiple paths — use JoinOperator when paths share variables,
        // CartesianProductOperator otherwise.
        let mut operators: Vec<OperatorBox> = Vec::new();
        let mut path_vars: Vec<HashSet<String>> = Vec::new();

        // Pre-compute variable sets for each path
        let path_var_sets: Vec<HashSet<String>> = pattern
            .paths
            .iter()
            .map(|path| {
                let mut vars = HashSet::new();
                if let Some(v) = &path.start.variable {
                    vars.insert(v.clone());
                }
                for seg in &path.segments {
                    if let Some(v) = &seg.node.variable {
                        vars.insert(v.clone());
                    }
                    if let Some(v) = &seg.edge.variable {
                        vars.insert(v.clone());
                    }
                }
                vars
            })
            .collect();

        // Decompose WHERE clause: assign each predicate to the first path that contains
        // all its referenced variables. Cross-path predicates are applied after path join.
        let all_where_preds = where_clause
            .map(|wc| flatten_and_predicates(&wc.predicate))
            .unwrap_or_default();
        let mut per_path_preds: Vec<Vec<Expression>> = vec![Vec::new(); pattern.paths.len()];
        let mut cross_path_predicates: Vec<Expression> = Vec::new();

        for pred in all_where_preds {
            let mut pred_vars = HashSet::new();
            Self::collect_expression_variables(&pred, &mut pred_vars);

            let target_path = path_var_sets.iter().position(|pvars| {
                pred_vars.is_empty() || pred_vars.iter().all(|v| pvars.contains(v))
            });
            if let Some(i) = target_path {
                per_path_preds[i].push(pred);
            } else {
                cross_path_predicates.push(pred);
            }
        }

        let mut anon_counter: usize = 0;

        for &(path_idx, _) in &paths_with_cost {
            let path = &pattern.paths[path_idx];
            // Start with node scan for this path
            // Auto-generate variable names for anonymous nodes (e.g., `()` in patterns)
            let start_var = path.start.variable.clone().unwrap_or_else(|| {
                let name = format!("_anon_{}", anon_counter);
                anon_counter += 1;
                name
            });

            // Merge inline start node properties into predicates for index selection.
            // Without this, {prop: val} in MATCH patterns falls back to NodeScan + Filter
            // instead of IndexScan. See ADR-015 for context.
            if let Some(ref props) = path.start.properties {
                for (prop_name, prop_value) in props {
                    per_path_preds[path_idx].push(Expression::Binary {
                        left: Box::new(Expression::Property {
                            variable: start_var.clone(),
                            property: prop_name.clone(),
                        }),
                        op: BinaryOp::Eq,
                        right: Box::new(Expression::Literal(prop_value.clone())),
                    });
                }
            }

            // Also convert expression properties (e.g., {id: randomUUID()}) to predicates.
            // These are non-literal expressions that need runtime evaluation.
            for (prop_name, expr) in &path.start.expression_properties {
                per_path_preds[path_idx].push(Expression::Binary {
                    left: Box::new(Expression::Property {
                        variable: start_var.clone(),
                        property: prop_name.clone(),
                    }),
                    op: BinaryOp::Eq,
                    right: Box::new(expr.clone()),
                });
            }

            // SEARCH clause: if present, use VectorSearchScanOperator instead of NodeScan
            let mut search_op: Option<OperatorBox> = None;
            if let Some(ref search) = match_clause.search_clause {
                if search.binding_variable == start_var {
                    // Evaluate the query vector expression
                    let query_vector_expr = &search.query_vector;
                    // Evaluate limit expression
                    let k = match &search.limit {
                        Expression::Literal(PropertyValue::Integer(i)) => *i as usize,
                        _ => {
                            return Err(ExecutionError::PlanningError(
                                "SEARCH LIMIT must be a literal integer".to_string(),
                            ))
                        }
                    };

                    // Get the label from the vector index (needed for the search)
                    // The index name is used to find the correct vector index
                    let index_label = path
                        .start
                        .labels
                        .first()
                        .map(|l| l.as_str().to_string())
                        .unwrap_or_default();

                    // Look up the vector index to find the property key
                    let index_keys = store.vector_index.list_indices();
                    let matching_index = index_keys.iter().find(|ik| {
                        // Match by index name convention: label_property or by label
                        ik.label == index_label
                            || format!("{}_{}", ik.label, ik.property_key) == search.index_name
                    });

                    let (search_label, search_property) = if let Some(ik) = matching_index {
                        (ik.label.clone(), ik.property_key.clone())
                    } else {
                        // Use the index_name as label and "embedding" as default property
                        (index_label.clone(), "embedding".to_string())
                    };

                    // Extract query vector from expression
                    let score_var = search.score_alias.clone();
                    let in_index_where = search.where_clause.clone();

                    // Try to resolve the query vector at plan time (literal/parameter).
                    // If it's a property access or other expression, defer to runtime.
                    let query_vec_opt: Option<Vec<f32>> = match query_vector_expr {
                        Expression::Literal(PropertyValue::Vector(v)) => Some(v.clone()),
                        Expression::Literal(PropertyValue::Array(arr)) => Some(
                            arr.iter()
                                .map(|v| match v {
                                    PropertyValue::Float(f) => *f as f32,
                                    PropertyValue::Integer(i) => *i as f32,
                                    _ => 0.0,
                                })
                                .collect(),
                        ),
                        _ => None, // Defer to runtime (e.g., property access, function call)
                    };

                    if let Some(query_vec) = query_vec_opt {
                        search_op = Some(Box::new(VectorSearchOperator::new(
                            search_label,
                            search_property,
                            query_vec,
                            k,
                            start_var.clone(),
                            score_var,
                        )));
                    } else {
                        // Deferred: the query vector expression (e.g., snowWhite.embedding)
                        // will be resolved at runtime from the prior MATCH result.
                        // We need an input operator to feed the prior record.
                        // Use a no-op placeholder; the Apply operator in multi-part queries
                        // will pipe the prior result in. For single-query, use NodeScan as input.
                        let input_op: OperatorBox =
                            Box::new(NodeScanOperator::new(start_var.clone(), vec![]));
                        search_op = Some(Box::new(VectorSearchOperator::new_deferred(
                            search_label,
                            search_property,
                            query_vector_expr.clone(),
                            k,
                            start_var.clone(),
                            score_var,
                            input_op,
                        )));
                    }

                    // If there's an in-index WHERE clause, we need to over-fetch and filter
                    // We store the in-index filter for post-search filtering
                    if let Some(ref wc) = in_index_where {
                        per_path_preds[path_idx].push(wc.predicate.clone());
                    }
                }
            }

            // Optimization: Check for index usage (using this path's assigned predicates)
            let mut index_op: Option<OperatorBox> = None;
            let mut remaining_predicates: Vec<Expression> = Vec::new();
            {
                let predicates = &per_path_preds[path_idx];
                let mut used_index = false;

                for pred in predicates {
                    if used_index {
                        // Already found an index — push remaining predicates to filter
                        remaining_predicates.push(pred.clone());
                        continue;
                    }
                    if let Expression::Binary { left, op, right } = pred {
                        if let (
                            Expression::Property { variable, property },
                            Expression::Literal(val),
                        ) = (left.as_ref(), right.as_ref())
                        {
                            if variable == &start_var {
                                for label in &path.start.labels {
                                    if store.property_index.has_index(label, property) {
                                        match op {
                                            BinaryOp::Eq
                                            | BinaryOp::Gt
                                            | BinaryOp::Ge
                                            | BinaryOp::Lt
                                            | BinaryOp::Le => {
                                                index_op = Some(Box::new(IndexScanOperator::new(
                                                    start_var.clone(),
                                                    label.clone(),
                                                    property.clone(),
                                                    op.clone(),
                                                    val.clone(),
                                                )));
                                                used_index = true;
                                            }
                                            _ => {}
                                        }
                                        if used_index {
                                            break;
                                        }
                                    }
                                }
                            }
                        }
                    }
                    if !used_index {
                        remaining_predicates.push(pred.clone());
                    }
                }
            }

            let mut path_operator = search_op.or(index_op).unwrap_or_else(|| {
                Box::new(NodeScanOperator::new(
                    start_var.clone(),
                    path.start.labels.clone(),
                ))
            });

            // Note: start node inline properties are already merged into per_path_preds
            // above, so they're handled via IndexScan or remaining_predicates Filter.
            // No separate FilterOperator needed here.

            // Split remaining predicates: those referencing only start_var can be pushed
            // down now; those referencing later-path variables must be deferred until
            // after all ExpandOperators have materialized those variables.
            let mut early_predicates: Vec<Expression> = Vec::new();
            let mut deferred_predicates: Vec<Expression> = Vec::new();
            for pred in remaining_predicates {
                let mut pred_vars = HashSet::new();
                Self::collect_expression_variables(&pred, &mut pred_vars);
                // Push down only if predicate references exclusively the start variable
                // (or no variables at all, e.g., literal expressions)
                if pred_vars.is_empty() || pred_vars.iter().all(|v| v == &start_var) {
                    early_predicates.push(pred);
                } else {
                    deferred_predicates.push(pred);
                }
            }
            if !early_predicates.is_empty() {
                let filter_expr = early_predicates
                    .into_iter()
                    .reduce(|acc, pred| Expression::Binary {
                        left: Box::new(acc),
                        op: BinaryOp::And,
                        right: Box::new(pred),
                    })
                    .unwrap();
                path_operator = Box::new(FilterOperator::new(path_operator, filter_expr));
            }

            // Check for shortestPath / allShortestPaths
            if matches!(path.path_type, PathType::Shortest | PathType::AllShortest)
                && !path.segments.is_empty()
            {
                // shortestPath: use BFS-based ShortestPathOperator
                let last_segment = path.segments.last().unwrap();
                let target_var = last_segment
                    .node
                    .variable
                    .as_ref()
                    .ok_or_else(|| {
                        ExecutionError::PlanningError(
                            "shortestPath target must have a variable".to_string(),
                        )
                    })?
                    .clone();
                let edge_types: Vec<String> = last_segment
                    .edge
                    .types
                    .iter()
                    .map(|t| t.as_str().to_string())
                    .collect();
                let all_paths = matches!(path.path_type, PathType::AllShortest);

                // We need the target node to be scanned too — create a CartesianProduct with target scan
                let target_scan: OperatorBox = Box::new(NodeScanOperator::new(
                    target_var.clone(),
                    last_segment.node.labels.clone(),
                ));
                // Add property filter for target node
                let target_op = if let Some(ref props) = last_segment.node.properties {
                    if !props.is_empty() {
                        let filter_expr = self.build_property_filter(&target_var, props);
                        Box::new(FilterOperator::new(target_scan, filter_expr)) as OperatorBox
                    } else {
                        target_scan
                    }
                } else {
                    target_scan
                };

                let combined = Box::new(CartesianProductOperator::new(path_operator, target_op));
                path_operator = Box::new(ShortestPathOperator::new(
                    combined,
                    start_var.clone(),
                    target_var.clone(),
                    path.path_variable.clone(),
                    edge_types,
                    last_segment.edge.direction.clone(),
                    all_paths,
                ));
            } else {
                // Normal path: use ExpandOperator for each segment
                let mut current_var = start_var.clone();
                for (seg_idx, segment) in path.segments.iter().enumerate() {
                    let target_var = segment.node.variable.clone().unwrap_or_else(|| {
                        let name = format!("_anon_{}", anon_counter);
                        anon_counter += 1;
                        name
                    });

                    // Always bind edge variable (needed for edge uniqueness)
                    let edge_var = Some(
                        segment
                            .edge
                            .variable
                            .clone()
                            .unwrap_or_else(|| format!("__anon_edge_{}", seg_idx)),
                    );
                    let edge_types: Vec<String> = segment
                        .edge
                        .types
                        .iter()
                        .map(|t| t.as_str().to_string())
                        .collect();

                    // Check for variable-length path
                    if let Some(ref length) = segment.edge.length {
                        // VLP: use BFS-based expansion
                        let min_hops = length.min.unwrap_or(1);
                        let max_hops = length.max.unwrap_or(15); // cap at 15 for safety
                        let target_labels = segment.node.labels.clone();
                        let path_var = path.path_variable.clone();

                        path_operator = Box::new(VarLengthExpandOperator::new(
                            path_operator,
                            current_var.clone(),
                            target_var.clone(),
                            edge_var,
                            edge_types,
                            segment.edge.direction.clone(),
                            min_hops,
                            max_hops,
                            target_labels,
                            path_var,
                        ));
                    } else {
                        let mut expand = ExpandOperator::new(
                            path_operator,
                            current_var.clone(),
                            target_var.clone(),
                            edge_var,
                            edge_types,
                            segment.edge.direction.clone(),
                        );

                        // CY-04: Set path variable for named path materialization
                        if let Some(ref pv) = path.path_variable {
                            expand = expand.with_path_variable(pv.clone());
                        }

                        // Add target label filter if labels specified on target node
                        path_operator = if !segment.node.labels.is_empty() {
                            Box::new(expand.with_target_labels(segment.node.labels.clone()))
                        } else {
                            Box::new(expand)
                        };
                    }

                    // Add property filter for edge if properties specified AND edge has a variable
                    if let Some(ref edge_props) = segment.edge.properties {
                        if !edge_props.is_empty() {
                            if let Some(ref edge_var_name) = segment.edge.variable {
                                let filter_expr =
                                    self.build_property_filter(edge_var_name, edge_props);
                                path_operator =
                                    Box::new(FilterOperator::new(path_operator, filter_expr));
                            }
                        }
                    }

                    // Add property filter for target node if properties specified
                    if let Some(ref props) = segment.node.properties {
                        if !props.is_empty() {
                            let filter_expr = self.build_property_filter(&target_var, props);
                            path_operator =
                                Box::new(FilterOperator::new(path_operator, filter_expr));
                        }
                    }

                    current_var = target_var;
                }
            }

            // Apply deferred WHERE predicates after all path expansions
            if !deferred_predicates.is_empty() {
                let filter_expr = deferred_predicates
                    .into_iter()
                    .reduce(|acc, pred| Expression::Binary {
                        left: Box::new(acc),
                        op: BinaryOp::And,
                        right: Box::new(pred),
                    })
                    .unwrap();
                path_operator = Box::new(FilterOperator::new(path_operator, filter_expr));
            }

            // Edge uniqueness: within a single path, no edge can appear twice
            // Collect all edge variables (named and auto-generated for anonymous)
            let named_edges: Vec<String> = path
                .segments
                .iter()
                .enumerate()
                .filter_map(|(i, seg)| {
                    if seg.edge.variable.is_some() {
                        seg.edge.variable.clone()
                    } else if seg.edge.length.is_none() {
                        // Anonymous fixed-length edge — use internal name
                        Some(format!("__anon_edge_{}", i))
                    } else {
                        None // Skip VLP anonymous edges
                    }
                })
                .collect();
            if named_edges.len() >= 2 {
                let mut uniqueness_predicates = Vec::new();
                for i in 0..named_edges.len() {
                    for j in (i + 1)..named_edges.len() {
                        uniqueness_predicates.push(Expression::Binary {
                            left: Box::new(Expression::Variable(named_edges[i].clone())),
                            op: BinaryOp::Ne,
                            right: Box::new(Expression::Variable(named_edges[j].clone())),
                        });
                    }
                }
                if !uniqueness_predicates.is_empty() {
                    let filter_expr = uniqueness_predicates
                        .into_iter()
                        .reduce(|acc, pred| Expression::Binary {
                            left: Box::new(acc),
                            op: BinaryOp::And,
                            right: Box::new(pred),
                        })
                        .unwrap();
                    path_operator = Box::new(FilterOperator::new(path_operator, filter_expr));
                }
            }

            // Zero-length path: p = (a) — bind path variable to single-node path
            if path.segments.is_empty() {
                if let (Some(pv), Some(sv)) = (&path.path_variable, &path.start.variable) {
                    // Create a projection that adds the path variable binding
                    let mut projections = vec![(Expression::Variable(sv.clone()), sv.clone())];
                    // Use a function expression that creates a single-node path
                    projections.push((
                        Expression::Function {
                            name: "$singleNodePath".to_string(),
                            args: vec![Expression::Variable(sv.clone())],
                            distinct: false,
                        },
                        pv.clone(),
                    ));
                    path_operator = Box::new(ProjectOperator::new(path_operator, projections));
                }
            }

            // Collect variables used in this path for join detection
            let mut vars = HashSet::new();
            if let Some(v) = &path.start.variable {
                vars.insert(v.clone());
            }
            for seg in &path.segments {
                if let Some(v) = &seg.node.variable {
                    vars.insert(v.clone());
                }
                if let Some(v) = &seg.edge.variable {
                    vars.insert(v.clone());
                }
            }
            if let Some(v) = &path.path_variable {
                vars.insert(v.clone());
            }
            path_vars.push(vars);

            operators.push(path_operator);
        }

        // Combine operators: use JoinOperator when paths share a variable, CartesianProduct otherwise
        let mut result = operators.remove(0);
        let mut combined_vars = path_vars.remove(0);
        for (op, vars) in operators.into_iter().zip(path_vars.into_iter()) {
            let shared: Vec<String> = combined_vars.intersection(&vars).cloned().collect();
            if !shared.is_empty() {
                result = Box::new(JoinOperator::new(result, op, shared[0].clone()));
            } else {
                result = Box::new(CartesianProductOperator::new(result, op));
            }
            combined_vars.extend(vars);
        }

        // Apply cross-path predicates after all paths are joined
        if !cross_path_predicates.is_empty() {
            let filter_expr = cross_path_predicates
                .into_iter()
                .reduce(|acc, pred| Expression::Binary {
                    left: Box::new(acc),
                    op: BinaryOp::And,
                    right: Box::new(pred),
                })
                .unwrap();
            result = Box::new(FilterOperator::new(result, filter_expr));
        }

        Ok(result)
    }

    /// Build a filter expression from node properties.
    /// Converts {name: "Alice", age: 30} into (n.name = "Alice" AND n.age = 30)
    fn build_property_filter(
        &self,
        var: &str,
        props: &HashMap<String, PropertyValue>,
    ) -> Expression {
        let mut conditions: Vec<Expression> = Vec::new();

        for (prop_name, prop_value) in props {
            let condition = Expression::Binary {
                left: Box::new(Expression::Property {
                    variable: var.to_string(),
                    property: prop_name.clone(),
                }),
                op: BinaryOp::Eq,
                right: Box::new(Expression::Literal(prop_value.clone())),
            };
            conditions.push(condition);
        }

        // Combine with AND if multiple properties
        if conditions.len() == 1 {
            conditions.remove(0)
        } else {
            let mut result = conditions.remove(0);
            for condition in conditions {
                result = Expression::Binary {
                    left: Box::new(result),
                    op: BinaryOp::And,
                    right: Box::new(condition),
                };
            }
            result
        }
    }

    /// Collect variables referenced by an expression
    fn collect_expression_variables(expr: &Expression, vars: &mut HashSet<String>) {
        match expr {
            Expression::Variable(v) => {
                vars.insert(v.clone());
            }
            Expression::Property { variable, .. } => {
                vars.insert(variable.clone());
            }
            Expression::Binary { left, right, .. } => {
                Self::collect_expression_variables(left, vars);
                Self::collect_expression_variables(right, vars);
            }
            Expression::Unary { expr: e, .. } => {
                Self::collect_expression_variables(e, vars);
            }
            Expression::Function { name, args, .. } => {
                if name.eq_ignore_ascii_case("$patternPredicate") {
                    // Extract variable names from the string literal arguments
                    // arg[0] = source var name, arg[1] = pattern text with target var
                    if let Some(Expression::Literal(PropertyValue::String(src))) = args.first() {
                        vars.insert(src.clone());
                    }
                    if let Some(Expression::Literal(PropertyValue::String(pattern))) = args.get(1) {
                        // Extract variable names from parenthesized nodes in pattern text
                        let chars: Vec<char> = pattern.chars().collect();
                        let mut i = 0;
                        while i < chars.len() {
                            if chars[i] == '(' {
                                let start = i + 1;
                                let mut j = start;
                                while j < chars.len() && chars[j] != ')' && chars[j] != ':' && chars[j] != '{' {
                                    j += 1;
                                }
                                let var = pattern[start..j].trim().to_string();
                                if !var.is_empty() {
                                    vars.insert(var);
                                }
                                while j < chars.len() && chars[j] != ')' { j += 1; }
                                i = j + 1;
                            } else {
                                i += 1;
                            }
                        }
                    }
                } else {
                    for arg in args {
                        Self::collect_expression_variables(arg, vars);
                    }
                }
            }
            _ => {}
        }
    }

    /// Plan multiple CREATE-only clauses, sharing variables across them.
    ///
    /// E.g.: `CREATE (a:Person) CREATE (b:Person) CREATE (a)-[:KNOWS]->(b)`
    fn plan_create_only_multi(
        &self,
        create_clauses: &[CreateClause],
    ) -> ExecutionResult<ExecutionPlan> {
        let mut all_nodes: Vec<(Vec<Label>, HashMap<String, PropertyValue>, Option<String>)> =
            Vec::new();
        let mut all_edges: Vec<(
            String,
            String,
            EdgeType,
            HashMap<String, PropertyValue>,
            Option<String>,
        )> = Vec::new();
        let mut output_columns: Vec<String> = Vec::new();
        let mut create_anon_counter = 0usize;

        // Check if any node or edge has expression properties (e.g., {id: randomUUID()})
        // If so, use PerRowCreateOperator which can evaluate expressions at runtime
        let has_expr_props = create_clauses.iter().any(|cc| {
            cc.pattern.paths.iter().any(|p| {
                !p.start.expression_properties.is_empty()
                    || p.segments.iter().any(|s| {
                        !s.node.expression_properties.is_empty()
                            || !s.edge.expression_properties.is_empty()
                    })
            })
        });
        if has_expr_props {
            let mut known_vars: HashSet<String> = HashSet::new();
            let mut anon_counter = 0usize;
            let (node_specs, edge_specs) =
                self.extract_create_specs(create_clauses, &mut anon_counter, &mut known_vars);
            let output_columns: Vec<String> = known_vars.into_iter().collect();
            let operator: OperatorBox = Box::new(PerRowCreateOperator::new(
                Box::new(SingleRowOperator::new()),
                node_specs,
                edge_specs,
            ));
            return Ok(ExecutionPlan {
                root: operator,
                output_columns,
                is_write: true,
            });
        }

        for create_clause in create_clauses {
            let pattern = &create_clause.pattern;
            for path in &pattern.paths {
                let start = &path.start;
                let labels: Vec<Label> = start.labels.clone();
                let properties: HashMap<String, PropertyValue> =
                    start.properties.clone().unwrap_or_default();
                // Generate anonymous variable if none specified and path has edges
                let variable = start.variable.clone().or_else(|| {
                    if !path.segments.is_empty() {
                        let name = format!("_create_anon_{}", create_anon_counter);
                        create_anon_counter += 1;
                        Some(name)
                    } else {
                        None
                    }
                });

                if let Some(ref var) = start.variable {
                    // Only add to nodes if not already seen (avoid duplicate node creation)
                    if !all_nodes
                        .iter()
                        .any(|(_, _, v)| v.as_deref() == Some(var.as_str()))
                    {
                        output_columns.push(var.clone());
                        all_nodes.push((labels, properties, variable.clone()));
                    }
                } else {
                    all_nodes.push((labels, properties, variable.clone()));
                }

                let mut current_source_var = variable;

                for segment in &path.segments {
                    let node = &segment.node;
                    let node_labels: Vec<Label> = node.labels.clone();
                    let node_properties: HashMap<String, PropertyValue> =
                        node.properties.clone().unwrap_or_default();
                    // Generate anonymous variable if none specified (needed for edge linking)
                    let node_variable = node.variable.clone().or_else(|| {
                        let name = format!("_create_anon_{}", create_anon_counter);
                        create_anon_counter += 1;
                        Some(name)
                    });

                    if let Some(ref var) = node.variable {
                        if !all_nodes
                            .iter()
                            .any(|(_, _, v)| v.as_deref() == Some(var.as_str()))
                        {
                            output_columns.push(var.clone());
                            all_nodes.push((node_labels, node_properties, node_variable.clone()));
                        }
                    } else {
                        all_nodes.push((node_labels, node_properties, node_variable.clone()));
                    }

                    let edge = &segment.edge;
                    let edge_type = edge
                        .types
                        .first()
                        .cloned()
                        .unwrap_or_else(|| EdgeType::new("RELATED_TO"));
                    let edge_properties: HashMap<String, PropertyValue> =
                        edge.properties.clone().unwrap_or_default();
                    let edge_variable = edge.variable.clone();

                    // Reject bidirectional edges in CREATE
                    if matches!(edge.direction, Direction::Both) {
                        return Err(ExecutionError::PlanningError(
                            "Cannot create relationship with bidirectional direction. Use -> or <- instead.".to_string(),
                        ));
                    }

                    if let (Some(source_var), Some(target_var)) =
                        (&current_source_var, &node_variable)
                    {
                        // For incoming edges (<-[:R]-), swap source and target
                        let (actual_source, actual_target) =
                            if matches!(edge.direction, Direction::Incoming) {
                                (target_var.clone(), source_var.clone())
                            } else {
                                (source_var.clone(), target_var.clone())
                            };
                        all_edges.push((
                            actual_source,
                            actual_target,
                            edge_type,
                            edge_properties,
                            edge_variable,
                        ));
                    }

                    current_source_var = node_variable;
                }
            }
        }

        let mut operator: OperatorBox = Box::new(CreateNodeOperator::new(all_nodes));

        // Chain edge creation operators on top of the node creation
        for (source_var, target_var, edge_type, edge_properties, edge_variable) in all_edges {
            operator = Box::new(CreateEdgeOperator::new(
                Some(operator),
                source_var,
                target_var,
                edge_type,
                edge_properties,
                edge_variable,
            ));
        }

        Ok(ExecutionPlan {
            root: operator,
            output_columns,
            is_write: true,
        })
    }

    #[allow(dead_code)]
    fn plan_create_only(&self, create_clause: &CreateClause) -> ExecutionResult<ExecutionPlan> {
        let pattern = &create_clause.pattern;

        // Collect all nodes to create from the pattern
        // Each node has: (labels, properties, variable_name)
        let mut nodes_to_create: Vec<(Vec<Label>, HashMap<String, PropertyValue>, Option<String>)> =
            Vec::new();
        let mut output_columns: Vec<String> = Vec::new();

        // Collect edges to create: (source_var, target_var, edge_type, properties, edge_var)
        let mut edges_to_create: Vec<(
            String,
            String,
            EdgeType,
            HashMap<String, PropertyValue>,
            Option<String>,
            Vec<(String, Expression)>,
        )> = Vec::new();

        // Counter for generating anonymous variable names for CREATE patterns
        let mut create_anon_counter = 0usize;

        for path in &pattern.paths {
            // Add start node
            let start = &path.start;
            let labels: Vec<Label> = start.labels.clone();
            let properties: HashMap<String, PropertyValue> =
                start.properties.clone().unwrap_or_default();
            // Generate anonymous variable if none specified (needed for edge linking)
            let variable = start.variable.clone().or_else(|| {
                if !path.segments.is_empty() {
                    let name = format!("_create_anon_{}", create_anon_counter);
                    create_anon_counter += 1;
                    Some(name)
                } else {
                    None
                }
            });

            // Track output column if variable exists and was user-specified
            if start.variable.is_some() {
                if let Some(ref var) = variable {
                    output_columns.push(var.clone());
                }
            }

            nodes_to_create.push((labels, properties, variable.clone()));

            // Track current source variable for edge creation
            let mut current_source_var = variable;

            // Add nodes and edges from path segments (if any)
            // Example: CREATE (a:Person)-[:KNOWS]->(b:Person)
            for segment in &path.segments {
                let node = &segment.node;
                let node_labels: Vec<Label> = node.labels.clone();
                let node_properties: HashMap<String, PropertyValue> =
                    node.properties.clone().unwrap_or_default();
                // Generate anonymous variable if none specified (needed for edge linking)
                let node_variable = node.variable.clone().or_else(|| {
                    let name = format!("_create_anon_{}", create_anon_counter);
                    create_anon_counter += 1;
                    Some(name)
                });

                if node.variable.is_some() {
                    if let Some(ref var) = node_variable {
                        output_columns.push(var.clone());
                    }
                }

                nodes_to_create.push((node_labels, node_properties, node_variable.clone()));

                // Extract edge information
                let edge = &segment.edge;
                let edge_type = edge
                    .types
                    .first()
                    .cloned()
                    .unwrap_or_else(|| EdgeType::new("RELATED_TO"));
                let edge_properties: HashMap<String, PropertyValue> =
                    edge.properties.clone().unwrap_or_default();
                let edge_variable = edge.variable.clone();

                // Create edge between source and target nodes
                // Both will have variables (either user-specified or generated)
                // For incoming edges (<-[:R]-), swap source and target
                if let (Some(source_var), Some(target_var)) = (&current_source_var, &node_variable)
                {
                    let (actual_source, actual_target) =
                        if matches!(edge.direction, Direction::Incoming) {
                            (target_var.clone(), source_var.clone())
                        } else {
                            (source_var.clone(), target_var.clone())
                        };
                    edges_to_create.push((
                        actual_source,
                        actual_target,
                        edge_type,
                        edge_properties,
                        edge_variable,
                        edge.expression_properties.clone(),
                    ));
                }

                // Update source variable for next segment
                current_source_var = node_variable;
            }
        }

        // Build the operator chain
        // First: CreateNodeOperator to create all nodes
        let node_operator: OperatorBox = Box::new(CreateNodeOperator::new(nodes_to_create));

        // If there are edges to create, chain CreateEdgeOperator
        let final_operator: OperatorBox = if edges_to_create.is_empty() {
            node_operator
        } else {
            // Create edges after nodes are created
            // We need a special combined operator that creates nodes first, then edges
            Box::new(CreateNodesAndEdgesOperator::new(
                node_operator,
                edges_to_create,
            ))
        };

        // Return execution plan with is_write: true (this mutates the graph)
        Ok(ExecutionPlan {
            root: final_operator,
            output_columns,
            is_write: true,
        })
    }
}

impl Default for QueryPlanner {
    fn default() -> Self {
        Self::new()
    }
}

/// Flatten an AND-chain expression into a list of individual predicates.
/// E.g., `a AND b AND c` → `[a, b, c]`
fn flatten_and_predicates(expr: &Expression) -> Vec<Expression> {
    match expr {
        Expression::Binary {
            left,
            op: BinaryOp::And,
            right,
        } => {
            let mut result = flatten_and_predicates(left);
            result.extend(flatten_and_predicates(right));
            result
        }
        _ => vec![expr.clone()],
    }
}

impl QueryPlanner {
    /// Build a WithBarrier operator from a WithClause (extracted for multi-WITH reuse)
    /// Plan a multi-part write query with WITH barriers between stages.
    /// Each stage can contain CREATE, MERGE, SET, DELETE, UNWIND clauses.
    /// The pipeline: Stage1 → WithBarrier → Stage2 → WithBarrier → ... → final single_part_query
    fn plan_multi_part_write(
        &self,
        query: &Query,
        store: &GraphStore,
    ) -> ExecutionResult<ExecutionPlan> {
        let mut operator: OperatorBox = Box::new(SingleRowOperator::new());
        let mut known_vars: HashSet<String> = HashSet::new();
        let mut create_anon_counter = 0usize;

        // Process each multi-part stage
        for stage in &query.multi_part_stages {
            // 1. Apply MATCH clauses for this stage
            for mc in &stage.match_clauses {
                let match_op = self.plan_match(mc, stage.where_clause.as_ref(), store)?;
                let match_vars = self.extract_match_vars(mc);
                let shared: Vec<String> = known_vars.intersection(&match_vars).cloned().collect();
                if !shared.is_empty() {
                    operator = Box::new(JoinOperator::new(operator, match_op, shared[0].clone()));
                } else if known_vars.is_empty() {
                    operator = match_op;
                } else {
                    operator = Box::new(CartesianProductOperator::new(operator, match_op));
                }
                for v in match_vars {
                    known_vars.insert(v);
                }
            }

            // 2. Apply UNWIND clauses for this stage
            for unwind in &stage.unwind_clauses {
                operator = Box::new(UnwindOperator::new(
                    operator,
                    unwind.expression.clone(),
                    unwind.variable.clone(),
                ));
                known_vars.insert(unwind.variable.clone());
            }

            // 3. Apply MERGE clauses for this stage (before CREATE, since MERGE may bind vars used by CREATE)
            for merge_clause in &stage.merge_clauses {
                let on_create: Vec<(String, String, Expression)> = merge_clause
                    .on_create_set
                    .iter()
                    .map(|s| (s.variable.clone(), s.property.clone(), s.value.clone()))
                    .collect();
                let on_match: Vec<(String, String, Expression)> = merge_clause
                    .on_match_set
                    .iter()
                    .map(|s| (s.variable.clone(), s.property.clone(), s.value.clone()))
                    .collect();

                // MERGE always uses PerRowMergeOperator which handles both node and edge patterns.
                // It checks for existing nodes/edges before creating, ensuring idempotency.
                let merge_vars = self.extract_pattern_vars(&merge_clause.pattern);
                operator = Box::new(PerRowMergeOperator::new(
                    operator,
                    merge_clause.pattern.clone(),
                    on_create,
                    on_match,
                ));
                for v in merge_vars {
                    known_vars.insert(v);
                }
            }

            // 4. Apply CREATE clauses for this stage (after MERGE, since MERGE binds vars used by CREATE)
            if !stage.create_clauses.is_empty() {
                let (node_specs, edge_specs) = self.extract_create_specs(
                    &stage.create_clauses,
                    &mut create_anon_counter,
                    &mut known_vars,
                );
                operator = Box::new(PerRowCreateOperator::new(operator, node_specs, edge_specs));
            }

            // 5. Apply SET clauses for this stage
            if !stage.set_clauses.is_empty() {
                let mut items = Vec::new();
                for set_clause in &stage.set_clauses {
                    for item in &set_clause.items {
                        items.push((
                            item.variable.clone(),
                            item.property.clone(),
                            item.value.clone(),
                        ));
                    }
                }
                operator = Box::new(SetPropertyOperator::new(operator, items));
            }

            // 6. Apply DELETE clause for this stage
            if let Some(delete_clause) = &stage.delete_clause {
                let vars: Vec<String> = delete_clause
                    .expressions
                    .iter()
                    .filter_map(|e| {
                        if let Expression::Variable(v) = e {
                            Some(v.clone())
                        } else {
                            None
                        }
                    })
                    .collect();
                operator = Box::new(DeleteOperator::new(operator, vars, delete_clause.detach));
            }

            // 7. Apply REMOVE clauses for this stage
            if !stage.remove_clauses.is_empty() {
                let mut items = Vec::new();
                for remove_clause in &stage.remove_clauses {
                    for item in &remove_clause.items {
                        if let RemoveItem::Property { variable, property } = item {
                            items.push((variable.clone(), property.clone()));
                        }
                    }
                }
                if !items.is_empty() {
                    operator = Box::new(RemovePropertyOperator::new(operator, items));
                }
            }

            // 8. Apply WITH barrier to materialize results and scope variables.
            // For multi-part write queries, use DISTINCT semantics when the WITH
            // only passes through simple variables — this matches Neo4j's behavior
            // where WITH t between CREATE stages collapses duplicate rows.
            let mut with_for_barrier = stage.with_clause.clone();
            let is_simple_passthrough = !with_for_barrier.distinct
                && with_for_barrier.where_clause.is_none()
                && with_for_barrier.order_by.is_none()
                && with_for_barrier.skip.is_none()
                && with_for_barrier.limit.is_none()
                && with_for_barrier.items.iter().all(|item| {
                    matches!(item.expression, Expression::Variable(_)) && item.alias.is_none()
                });
            if is_simple_passthrough && !stage.unwind_clauses.is_empty() {
                // After UNWIND+CREATE, WITH t produces N copies of t.
                // Apply DISTINCT to collapse back to unique rows.
                with_for_barrier.distinct = true;
            }
            operator = self.build_with_barrier(operator, &with_for_barrier, store)?;

            // Update known_vars to only include this WITH's projected outputs
            known_vars.clear();
            for item in &stage.with_clause.items {
                let alias = item
                    .alias
                    .clone()
                    .unwrap_or_else(|| match &item.expression {
                        Expression::Variable(v) => v.clone(),
                        Expression::Property { variable, property } => {
                            format!("{}.{}", variable, property)
                        }
                        _ => "?".to_string(),
                    });
                known_vars.insert(alias);
            }
        }

        // Now handle the final single_part_query's clauses (the "tail" after the last WITH)
        // These are in the flat query fields: create_clause, unwind_clause, merge_clause, etc.

        // Final UNWIND
        for unwind in &query.additional_unwinds {
            operator = Box::new(UnwindOperator::new(
                operator,
                unwind.expression.clone(),
                unwind.variable.clone(),
            ));
            known_vars.insert(unwind.variable.clone());
        }
        if let Some(unwind) = &query.unwind_clause {
            operator = Box::new(UnwindOperator::new(
                operator,
                unwind.expression.clone(),
                unwind.variable.clone(),
            ));
            known_vars.insert(unwind.variable.clone());
        }

        // Final MERGE clauses (before CREATE, since MERGE binds vars that CREATE may reference)
        // Always use PerRowMergeOperator which checks for existing nodes/edges (idempotent).
        for merge_clause in &query.all_merge_clauses {
            let on_create: Vec<(String, String, Expression)> = merge_clause
                .on_create_set
                .iter()
                .map(|s| (s.variable.clone(), s.property.clone(), s.value.clone()))
                .collect();
            let on_match: Vec<(String, String, Expression)> = merge_clause
                .on_match_set
                .iter()
                .map(|s| (s.variable.clone(), s.property.clone(), s.value.clone()))
                .collect();

            let merge_vars = self.extract_pattern_vars(&merge_clause.pattern);
            operator = Box::new(PerRowMergeOperator::new(
                operator,
                merge_clause.pattern.clone(),
                on_create,
                on_match,
            ));
            for v in merge_vars {
                known_vars.insert(v);
            }
        }

        // Final CREATE (after MERGE, since MERGE binds vars that CREATE may reference)
        if !query.create_clauses.is_empty() {
            let (node_specs, edge_specs) = self.extract_create_specs(
                &query.create_clauses,
                &mut create_anon_counter,
                &mut known_vars,
            );
            operator = Box::new(PerRowCreateOperator::new(operator, node_specs, edge_specs));
        } else if let Some(create_clause) = &query.create_clause {
            let (node_specs, edge_specs) = self.extract_create_specs(
                std::slice::from_ref(create_clause),
                &mut create_anon_counter,
                &mut known_vars,
            );
            operator = Box::new(PerRowCreateOperator::new(operator, node_specs, edge_specs));
        }

        // Final SET
        if !query.set_clauses.is_empty() {
            let mut items = Vec::new();
            for set_clause in &query.set_clauses {
                for item in &set_clause.items {
                    items.push((
                        item.variable.clone(),
                        item.property.clone(),
                        item.value.clone(),
                    ));
                }
            }
            operator = Box::new(SetPropertyOperator::new(operator, items));
        }

        // Final DELETE
        if let Some(delete_clause) = &query.delete_clause {
            let vars: Vec<String> = delete_clause
                .expressions
                .iter()
                .filter_map(|e| {
                    if let Expression::Variable(v) = e {
                        Some(v.clone())
                    } else {
                        None
                    }
                })
                .collect();
            operator = Box::new(DeleteOperator::new(operator, vars, delete_clause.detach));
        }

        // Final RETURN
        let mut output_columns = Vec::new();
        if let Some(return_clause) = &query.return_clause {
            let effective_items = if return_clause.star {
                let mut star_items: Vec<crate::query::ast::ReturnItem> = known_vars
                    .iter()
                    .filter(|v| !v.starts_with("_anon_") && !v.starts_with("_create_anon_"))
                    .map(|v| crate::query::ast::ReturnItem {
                        expression: Expression::Variable(v.clone()),
                        alias: None,
                    })
                    .collect();
                star_items.sort_by(|a, b| {
                    let va = match &a.expression {
                        Expression::Variable(v) => v.clone(),
                        _ => String::new(),
                    };
                    let vb = match &b.expression {
                        Expression::Variable(v) => v.clone(),
                        _ => String::new(),
                    };
                    va.cmp(&vb)
                });
                star_items.extend(return_clause.items.iter().cloned());
                star_items
            } else {
                return_clause.items.clone()
            };

            let projections: Vec<(Expression, String)> = effective_items
                .iter()
                .enumerate()
                .map(|(i, item)| {
                    let alias = item
                        .alias
                        .clone()
                        .unwrap_or_else(|| match &item.expression {
                            Expression::Variable(v) => v.clone(),
                            Expression::Property { variable, property } => {
                                format!("{}.{}", variable, property)
                            }
                            _ => format!("col_{}", i),
                        });
                    output_columns.push(alias.clone());
                    (item.expression.clone(), alias)
                })
                .collect();
            operator = Box::new(ProjectOperator::new(operator, projections));
        }

        // Final ORDER BY, SKIP, LIMIT
        if let Some(order_by) = &query.order_by {
            let sort_items: Vec<(Expression, bool)> = order_by
                .items
                .iter()
                .map(|i| (i.expression.clone(), i.ascending))
                .collect();
            operator = Box::new(SortOperator::new(operator, sort_items));
        }
        if let Some(skip) = query.skip {
            operator = Box::new(SkipOperator::new(operator, skip));
        }
        if let Some(limit) = query.limit {
            operator = Box::new(LimitOperator::new(operator, limit));
        }

        Ok(ExecutionPlan {
            root: operator,
            output_columns,
            is_write: true,
        })
    }

    /// Extract node and edge specs from CREATE clauses for PerRowCreateOperator.
    fn extract_create_specs(
        &self,
        create_clauses: &[CreateClause],
        anon_counter: &mut usize,
        known_vars: &mut HashSet<String>,
    ) -> (
        Vec<(
            Vec<Label>,
            HashMap<String, PropertyValue>,
            Vec<(String, Expression)>,
            Option<String>,
        )>,
        Vec<(
            String,
            String,
            EdgeType,
            HashMap<String, PropertyValue>,
            Option<String>,
            Vec<(String, Expression)>,
        )>,
    ) {
        let mut node_specs = Vec::new();
        let mut edge_specs = Vec::new();

        for create_clause in create_clauses {
            for path in &create_clause.pattern.paths {
                let start_var = path.start.variable.clone();
                let labels = path.start.labels.clone();
                let static_props = path.start.properties.clone().unwrap_or_default();
                let expr_props = path.start.expression_properties.clone();

                // Only create node if variable is new (not already bound)
                let var_is_new = start_var
                    .as_ref()
                    .map(|v| !known_vars.contains(v))
                    .unwrap_or(true);
                if var_is_new {
                    node_specs.push((labels, static_props, expr_props, start_var.clone()));
                    if let Some(ref v) = start_var {
                        known_vars.insert(v.clone());
                    }
                }

                let current_var = start_var.unwrap_or_else(|| {
                    let name = format!("_create_anon_{}", *anon_counter);
                    *anon_counter += 1;
                    name
                });

                let mut prev_var = current_var;

                for seg in &path.segments {
                    let target_var = seg.node.variable.clone();
                    let seg_labels = seg.node.labels.clone();
                    let seg_props = seg.node.properties.clone().unwrap_or_default();
                    let seg_expr_props = seg.node.expression_properties.clone();

                    let target_is_new = target_var
                        .as_ref()
                        .map(|v| !known_vars.contains(v))
                        .unwrap_or(true);
                    if target_is_new {
                        node_specs.push((
                            seg_labels,
                            seg_props,
                            seg_expr_props,
                            target_var.clone(),
                        ));
                        if let Some(ref v) = target_var {
                            known_vars.insert(v.clone());
                        }
                    }

                    let target_name = target_var.unwrap_or_else(|| {
                        let name = format!("_create_anon_{}", *anon_counter);
                        *anon_counter += 1;
                        name
                    });

                    if let Some(et) = seg.edge.types.first() {
                        let edge_props = seg.edge.properties.clone().unwrap_or_default();
                        let (src, tgt) = if matches!(seg.edge.direction, Direction::Incoming) {
                            (target_name.clone(), prev_var.clone())
                        } else {
                            (prev_var.clone(), target_name.clone())
                        };
                        edge_specs.push((
                            src,
                            tgt,
                            et.clone(),
                            edge_props,
                            seg.edge.variable.clone(),
                            seg.edge.expression_properties.clone(),
                        ));
                    }
                    prev_var = target_name;
                }
            }
        }

        (node_specs, edge_specs)
    }

    /// Extract variable names from a pattern
    fn extract_pattern_vars(&self, pattern: &Pattern) -> HashSet<String> {
        let mut vars = HashSet::new();
        for path in &pattern.paths {
            if let Some(v) = &path.start.variable {
                vars.insert(v.clone());
            }
            for seg in &path.segments {
                if let Some(v) = &seg.node.variable {
                    vars.insert(v.clone());
                }
                if let Some(v) = &seg.edge.variable {
                    vars.insert(v.clone());
                }
            }
        }
        vars
    }

    fn build_with_barrier(
        &self,
        input: OperatorBox,
        with_clause: &WithClause,
        _store: &GraphStore,
    ) -> ExecutionResult<OperatorBox> {
        let mut items = Vec::new();
        let mut aggregates = Vec::new();
        let mut group_by = Vec::new();
        let mut has_aggregation = false;
        let mut agg_counter = 0usize;

        struct WithItemInfo {
            alias: String,
            original_expr: Expression,
            rewritten_expr: Expression,
            extracted_aggs: Vec<AggregateFunction>,
        }
        let mut item_infos = Vec::new();

        for (idx, item) in with_clause.items.iter().enumerate() {
            let alias = item
                .alias
                .clone()
                .unwrap_or_else(|| match &item.expression {
                    Expression::Variable(var) => var.clone(),
                    Expression::Property { variable, property } => {
                        format!("{}.{}", variable, property)
                    }
                    Expression::Function {
                        name,
                        args,
                        distinct,
                    } => {
                        let arg_strs: Vec<String> = args
                            .iter()
                            .map(|a| match a {
                                Expression::Variable(v) => v.clone(),
                                Expression::Property { variable, property } => {
                                    format!("{}.{}", variable, property)
                                }
                                _ => "?".to_string(),
                            })
                            .collect();
                        if *distinct {
                            format!("{}(DISTINCT {})", name, arg_strs.join(", "))
                        } else {
                            format!("{}({})", name, arg_strs.join(", "))
                        }
                    }
                    _ => format!("col_{}", idx),
                });

            let (rewritten, extracted) =
                extract_nested_aggregates(&item.expression, &mut agg_counter);
            if !extracted.is_empty() {
                has_aggregation = true;
            }
            item_infos.push(WithItemInfo {
                alias,
                original_expr: item.expression.clone(),
                rewritten_expr: rewritten,
                extracted_aggs: extracted,
            });
        }

        for info in item_infos {
            if has_aggregation {
                if !info.extracted_aggs.is_empty() {
                    aggregates.extend(info.extracted_aggs);
                    items.push((info.rewritten_expr, info.alias.clone()));
                } else {
                    group_by.push((info.original_expr, info.alias.clone()));
                    items.push((Expression::Variable(info.alias.clone()), info.alias.clone()));
                }
            } else {
                items.push((info.original_expr, info.alias.clone()));
            }
        }

        let sort_items: Vec<(Expression, bool)> = with_clause
            .order_by
            .as_ref()
            .map(|ob| {
                ob.items
                    .iter()
                    .map(|i| (i.expression.clone(), i.ascending))
                    .collect()
            })
            .unwrap_or_default();

        let where_predicate = with_clause
            .where_clause
            .as_ref()
            .map(|wc| wc.predicate.clone());

        Ok(Box::new(WithBarrierOperator::new(
            input,
            items,
            aggregates,
            group_by,
            has_aggregation,
            with_clause.distinct,
            where_predicate,
            sort_items,
            with_clause.skip,
            with_clause.limit,
        )))
    }

    /// Extract variable names from a MATCH clause
    fn extract_match_vars(&self, mc: &MatchClause) -> HashSet<String> {
        let mut vars = HashSet::new();
        for path in &mc.pattern.paths {
            if let Some(v) = &path.start.variable {
                vars.insert(v.clone());
            }
            for seg in &path.segments {
                if let Some(v) = &seg.node.variable {
                    vars.insert(v.clone());
                }
                if let Some(v) = &seg.edge.variable {
                    vars.insert(v.clone());
                }
            }
        }
        vars
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::query::parser::parse_query;

    #[test]
    fn test_plan_simple_match() {
        let store = GraphStore::new();
        let planner = QueryPlanner::new();

        let query = parse_query("MATCH (n:Person) RETURN n").unwrap();
        let result = planner.plan(&query, &store);

        assert!(result.is_ok());
        let plan = result.unwrap();
        assert_eq!(plan.output_columns.len(), 1);
        assert_eq!(plan.output_columns[0], "n");
    }

    #[test]
    fn test_plan_with_where() {
        let store = GraphStore::new();
        let planner = QueryPlanner::new();

        let query = parse_query("MATCH (n:Person) WHERE n.age > 30 RETURN n").unwrap();
        let result = planner.plan(&query, &store);

        assert!(result.is_ok());
    }

    #[test]
    fn test_plan_with_limit() {
        let store = GraphStore::new();
        let planner = QueryPlanner::new();

        let query = parse_query("MATCH (n:Person) RETURN n LIMIT 10").unwrap();
        let result = planner.plan(&query, &store);

        assert!(result.is_ok());
    }

    #[test]
    fn test_plan_with_edge() {
        let store = GraphStore::new();
        let planner = QueryPlanner::new();

        let query = parse_query("MATCH (a:Person)-[:KNOWS]->(b:Person) RETURN a, b").unwrap();
        let result = planner.plan(&query, &store);

        assert!(result.is_ok());
        let plan = result.unwrap();
        assert_eq!(plan.output_columns.len(), 2);
    }

    // ========== Batch 5: Additional Planner Tests ==========

    #[test]
    fn test_plan_create_only() {
        let store = GraphStore::new();
        let planner = QueryPlanner::new();

        let query = parse_query("CREATE (n:Person {name: 'Alice'})").unwrap();
        let result = planner.plan(&query, &store);
        assert!(
            result.is_ok(),
            "Planner failed for CREATE: {:?}",
            result.err()
        );
    }

    #[test]
    fn test_plan_delete() {
        let store = GraphStore::new();
        let planner = QueryPlanner::new();

        let query = parse_query("MATCH (n:Person) DELETE n").unwrap();
        let result = planner.plan(&query, &store);
        assert!(
            result.is_ok(),
            "Planner failed for DELETE: {:?}",
            result.err()
        );
    }

    #[test]
    fn test_plan_set() {
        let store = GraphStore::new();
        let planner = QueryPlanner::new();

        let query = parse_query("MATCH (n:Person) SET n.age = 30 RETURN n").unwrap();
        let result = planner.plan(&query, &store);
        assert!(result.is_ok(), "Planner failed for SET: {:?}", result.err());
    }

    #[test]
    fn test_plan_merge() {
        let store = GraphStore::new();
        let planner = QueryPlanner::new();

        let query = parse_query("MERGE (n:Person {name: 'Alice'})").unwrap();
        let result = planner.plan(&query, &store);
        assert!(
            result.is_ok(),
            "Planner failed for MERGE: {:?}",
            result.err()
        );
    }

    #[test]
    fn test_plan_unwind() {
        let store = GraphStore::new();
        let planner = QueryPlanner::new();

        let query = parse_query("MATCH (n) UNWIND [1,2,3] AS x RETURN x").unwrap();
        let result = planner.plan(&query, &store);
        assert!(
            result.is_ok(),
            "Planner failed for UNWIND: {:?}",
            result.err()
        );
    }

    #[test]
    fn test_plan_union() {
        let store = GraphStore::new();
        let planner = QueryPlanner::new();

        let query =
            parse_query("MATCH (n:Person) RETURN n.name UNION ALL MATCH (m:Company) RETURN m.name")
                .unwrap();
        let result = planner.plan(&query, &store);
        assert!(
            result.is_ok(),
            "Planner failed for UNION: {:?}",
            result.err()
        );
    }

    #[test]
    fn test_plan_optional_match() {
        let store = GraphStore::new();
        let planner = QueryPlanner::new();

        let query =
            parse_query("MATCH (n:Person) OPTIONAL MATCH (n)-[:KNOWS]->(m) RETURN n, m").unwrap();
        let result = planner.plan(&query, &store);
        assert!(
            result.is_ok(),
            "Planner failed for OPTIONAL MATCH: {:?}",
            result.err()
        );
    }

    #[test]
    fn test_plan_explain() {
        let store = GraphStore::new();
        let planner = QueryPlanner::new();

        let query = parse_query("EXPLAIN MATCH (n:Person) RETURN n").unwrap();
        let result = planner.plan(&query, &store);
        assert!(
            result.is_ok(),
            "Planner failed for EXPLAIN: {:?}",
            result.err()
        );
    }

    #[test]
    fn test_plan_profile() {
        let store = GraphStore::new();
        let planner = QueryPlanner::new();

        let query = parse_query("PROFILE MATCH (n:Person) RETURN n").unwrap();
        let result = planner.plan(&query, &store);
        assert!(
            result.is_ok(),
            "Planner failed for PROFILE: {:?}",
            result.err()
        );
    }

    #[test]
    fn test_plan_aggregation() {
        let store = GraphStore::new();
        let planner = QueryPlanner::new();

        let query = parse_query("MATCH (n:Person) RETURN n.city, count(n) AS cnt").unwrap();
        let result = planner.plan(&query, &store);
        assert!(
            result.is_ok(),
            "Planner failed for aggregation: {:?}",
            result.err()
        );
    }

    #[test]
    fn test_plan_order_by_limit() {
        let store = GraphStore::new();
        let planner = QueryPlanner::new();

        let query = parse_query("MATCH (n:Person) RETURN n ORDER BY n.name LIMIT 5").unwrap();
        let result = planner.plan(&query, &store);
        assert!(
            result.is_ok(),
            "Planner failed for ORDER BY + LIMIT: {:?}",
            result.err()
        );
    }

    #[test]
    fn test_plan_distinct() {
        let store = GraphStore::new();
        let planner = QueryPlanner::new();

        let query = parse_query("MATCH (n:Person) RETURN DISTINCT n.name").unwrap();
        let result = planner.plan(&query, &store);
        assert!(
            result.is_ok(),
            "Planner failed for DISTINCT: {:?}",
            result.err()
        );
    }

    #[test]
    fn test_plan_with_clause() {
        let store = GraphStore::new();
        let planner = QueryPlanner::new();

        let query = parse_query("MATCH (n:Person) WITH n.name AS name RETURN name").unwrap();
        let result = planner.plan(&query, &store);
        assert!(
            result.is_ok(),
            "Planner failed for WITH: {:?}",
            result.err()
        );
    }

    #[test]
    fn test_plan_create_index() {
        let store = GraphStore::new();
        let planner = QueryPlanner::new();

        let query = parse_query("CREATE INDEX ON :Person(name)").unwrap();
        let result = planner.plan(&query, &store);
        assert!(
            result.is_ok(),
            "Planner failed for CREATE INDEX: {:?}",
            result.err()
        );
    }

    #[test]
    fn test_plan_drop_index() {
        let store = GraphStore::new();
        let planner = QueryPlanner::new();

        let query = parse_query("DROP INDEX ON :Person(name)").unwrap();
        let result = planner.plan(&query, &store);
        assert!(
            result.is_ok(),
            "Planner failed for DROP INDEX: {:?}",
            result.err()
        );
    }

    #[test]
    fn test_plan_show_indexes() {
        let store = GraphStore::new();
        let planner = QueryPlanner::new();

        let query = parse_query("SHOW INDEXES").unwrap();
        let result = planner.plan(&query, &store);
        assert!(
            result.is_ok(),
            "Planner failed for SHOW INDEXES: {:?}",
            result.err()
        );
    }

    #[test]
    fn test_plan_show_constraints() {
        let store = GraphStore::new();
        let planner = QueryPlanner::new();

        let query = parse_query("SHOW CONSTRAINTS").unwrap();
        let result = planner.plan(&query, &store);
        assert!(
            result.is_ok(),
            "Planner failed for SHOW CONSTRAINTS: {:?}",
            result.err()
        );
    }

    #[test]
    fn test_plan_create_constraint() {
        let store = GraphStore::new();
        let planner = QueryPlanner::new();

        let query =
            parse_query("CREATE CONSTRAINT ON (n:Person) ASSERT n.email IS UNIQUE").unwrap();
        let result = planner.plan(&query, &store);
        assert!(
            result.is_ok(),
            "Planner failed for CREATE CONSTRAINT: {:?}",
            result.err()
        );
    }

    #[test]
    fn test_plan_call_algorithm() {
        let store = GraphStore::new();
        let planner = QueryPlanner::new();

        let query =
            parse_query("CALL algo.pageRank({maxIterations: 20}) YIELD node, score").unwrap();
        let result = planner.plan(&query, &store);
        assert!(
            result.is_ok(),
            "Planner failed for CALL algo: {:?}",
            result.err()
        );
    }

    #[test]
    fn test_plan_multiple_return_items() {
        let store = GraphStore::new();
        let planner = QueryPlanner::new();

        let query = parse_query("MATCH (n:Person) RETURN n.name, n.age, id(n)").unwrap();
        let result = planner.plan(&query, &store);
        assert!(result.is_ok());
        let plan = result.unwrap();
        assert_eq!(plan.output_columns.len(), 3);
    }

    #[test]
    fn test_plan_with_populated_store() {
        let mut store = GraphStore::new();
        // Populate with data so statistics-based planning kicks in
        for i in 0..100 {
            let id = store.create_node("Person");
            store.get_node_mut(id).unwrap().set_property(
                "name".to_string(),
                crate::graph::PropertyValue::String(format!("Person{}", i)),
            );
        }
        for i in 0..20 {
            let id = store.create_node("Company");
            store.get_node_mut(id).unwrap().set_property(
                "name".to_string(),
                crate::graph::PropertyValue::String(format!("Company{}", i)),
            );
        }

        let planner = QueryPlanner::new();
        let query = parse_query("MATCH (n:Person) WHERE n.name = 'Person50' RETURN n").unwrap();
        let result = planner.plan(&query, &store);
        assert!(result.is_ok());
    }

    #[test]
    fn test_plan_detach_delete() {
        let store = GraphStore::new();
        let planner = QueryPlanner::new();

        let query = parse_query("MATCH (n:Person) DETACH DELETE n").unwrap();
        let result = planner.plan(&query, &store);
        assert!(
            result.is_ok(),
            "Planner failed for DETACH DELETE: {:?}",
            result.err()
        );
    }

    // ========== Coverage Enhancement Tests ==========

    #[test]
    fn test_planner_default_impl() {
        let planner = QueryPlanner::default();
        let store = GraphStore::new();
        let query = parse_query("MATCH (n) RETURN n").unwrap();
        assert!(planner.plan(&query, &store).is_ok());
    }

    #[test]
    fn test_plan_cache_invalidation() {
        let planner = QueryPlanner::new();
        let store = GraphStore::new();
        // Plan a query to populate cache
        let query = parse_query("MATCH (n:Person) RETURN n").unwrap();
        planner.plan(&query, &store).unwrap();
        // Invalidate should not cause errors
        planner.invalidate_cache();
        // Re-planning should still work
        let result = planner.plan(&query, &store);
        assert!(result.is_ok());
    }

    #[test]
    fn test_plan_create_is_write() {
        let store = GraphStore::new();
        let planner = QueryPlanner::new();

        let query = parse_query("CREATE (n:Person {name: 'Alice'})").unwrap();
        let plan = planner.plan(&query, &store).unwrap();
        assert!(plan.is_write, "CREATE should be a write plan");
    }

    #[test]
    fn test_plan_delete_is_write() {
        let store = GraphStore::new();
        let planner = QueryPlanner::new();

        let query = parse_query("MATCH (n:Person) DELETE n").unwrap();
        let plan = planner.plan(&query, &store).unwrap();
        assert!(plan.is_write, "DELETE should be a write plan");
    }

    #[test]
    fn test_plan_set_is_write() {
        let store = GraphStore::new();
        let planner = QueryPlanner::new();

        let query = parse_query("MATCH (n:Person) SET n.age = 30 RETURN n").unwrap();
        let plan = planner.plan(&query, &store).unwrap();
        assert!(plan.is_write, "SET should be a write plan");
    }

    #[test]
    fn test_plan_merge_is_write() {
        let store = GraphStore::new();
        let planner = QueryPlanner::new();

        let query = parse_query("MERGE (n:Person {name: 'Alice'})").unwrap();
        let plan = planner.plan(&query, &store).unwrap();
        assert!(plan.is_write, "MERGE should be a write plan");
    }

    #[test]
    fn test_plan_read_is_not_write() {
        let store = GraphStore::new();
        let planner = QueryPlanner::new();

        let query = parse_query("MATCH (n:Person) RETURN n").unwrap();
        let plan = planner.plan(&query, &store).unwrap();
        assert!(!plan.is_write, "MATCH...RETURN should not be a write plan");
    }

    #[test]
    fn test_plan_create_index_is_write() {
        let store = GraphStore::new();
        let planner = QueryPlanner::new();

        let query = parse_query("CREATE INDEX ON :Person(name)").unwrap();
        let plan = planner.plan(&query, &store).unwrap();
        assert!(plan.is_write, "CREATE INDEX should be a write plan");
    }

    #[test]
    fn test_plan_drop_index_is_write() {
        let store = GraphStore::new();
        let planner = QueryPlanner::new();

        let query = parse_query("DROP INDEX ON :Person(name)").unwrap();
        let plan = planner.plan(&query, &store).unwrap();
        assert!(plan.is_write, "DROP INDEX should be a write plan");
    }

    #[test]
    fn test_plan_show_indexes_not_write() {
        let store = GraphStore::new();
        let planner = QueryPlanner::new();

        let query = parse_query("SHOW INDEXES").unwrap();
        let plan = planner.plan(&query, &store).unwrap();
        assert!(!plan.is_write, "SHOW INDEXES should not be a write plan");
        assert!(plan.output_columns.contains(&"label".to_string()));
        assert!(plan.output_columns.contains(&"property".to_string()));
        assert!(plan.output_columns.contains(&"type".to_string()));
    }

    #[test]
    fn test_plan_show_constraints_not_write() {
        let store = GraphStore::new();
        let planner = QueryPlanner::new();

        let query = parse_query("SHOW CONSTRAINTS").unwrap();
        let plan = planner.plan(&query, &store).unwrap();
        assert!(
            !plan.is_write,
            "SHOW CONSTRAINTS should not be a write plan"
        );
    }

    #[test]
    fn test_plan_constraint_is_write() {
        let store = GraphStore::new();
        let planner = QueryPlanner::new();

        let query =
            parse_query("CREATE CONSTRAINT ON (n:Person) ASSERT n.email IS UNIQUE").unwrap();
        let plan = planner.plan(&query, &store).unwrap();
        assert!(plan.is_write, "CREATE CONSTRAINT should be a write plan");
    }

    #[test]
    fn test_plan_create_with_edge() {
        let store = GraphStore::new();
        let planner = QueryPlanner::new();

        let query =
            parse_query("CREATE (a:Person {name: 'Alice'})-[:KNOWS]->(b:Person {name: 'Bob'})")
                .unwrap();
        let plan = planner.plan(&query, &store).unwrap();
        assert!(plan.is_write);
        // Both variables should appear in output columns
        assert!(plan.output_columns.contains(&"a".to_string()));
        assert!(plan.output_columns.contains(&"b".to_string()));
    }

    #[test]
    fn test_plan_match_create_edge() {
        let store = GraphStore::new();
        let planner = QueryPlanner::new();

        let query =
            parse_query("MATCH (a:Person), (b:Company) CREATE (a)-[:WORKS_AT]->(b)").unwrap();
        let result = planner.plan(&query, &store);
        assert!(
            result.is_ok(),
            "MATCH...CREATE should plan: {:?}",
            result.err()
        );
        let plan = result.unwrap();
        assert!(plan.is_write);
    }

    #[test]
    fn test_plan_skip() {
        let store = GraphStore::new();
        let planner = QueryPlanner::new();

        let query = parse_query("MATCH (n:Person) RETURN n SKIP 5").unwrap();
        let result = planner.plan(&query, &store);
        assert!(result.is_ok(), "SKIP should plan: {:?}", result.err());
    }

    #[test]
    fn test_plan_skip_and_limit() {
        let store = GraphStore::new();
        let planner = QueryPlanner::new();

        let query = parse_query("MATCH (n:Person) RETURN n SKIP 5 LIMIT 10").unwrap();
        let result = planner.plan(&query, &store);
        assert!(
            result.is_ok(),
            "SKIP + LIMIT should plan: {:?}",
            result.err()
        );
    }

    #[test]
    fn test_plan_remove_property() {
        let store = GraphStore::new();
        let planner = QueryPlanner::new();

        let query = parse_query("MATCH (n:Person) REMOVE n.age RETURN n").unwrap();
        let result = planner.plan(&query, &store);
        assert!(result.is_ok(), "REMOVE should plan: {:?}", result.err());
        let plan = result.unwrap();
        assert!(plan.is_write, "REMOVE should be a write plan");
    }

    #[test]
    fn test_plan_index_scan_selection() {
        let mut store = GraphStore::new();
        // Create nodes and an index so the planner can choose IndexScan
        for i in 0..100 {
            let id = store.create_node("Person");
            store
                .set_node_property(
                    "default",
                    id,
                    "name",
                    crate::graph::PropertyValue::String(format!("Person{}", i)),
                )
                .unwrap();
        }
        // Create a property index
        store
            .property_index
            .create_index(crate::graph::Label::new("Person"), "name".to_string());

        let planner = QueryPlanner::new();
        let query = parse_query("MATCH (n:Person) WHERE n.name = 'Person50' RETURN n").unwrap();
        let result = planner.plan(&query, &store);
        assert!(
            result.is_ok(),
            "Index scan planning failed: {:?}",
            result.err()
        );
    }

    #[test]
    fn test_plan_composite_create_index() {
        let store = GraphStore::new();
        let planner = QueryPlanner::new();

        let query = parse_query("CREATE INDEX ON :Person(name, age)").unwrap();
        let result = planner.plan(&query, &store);
        assert!(
            result.is_ok(),
            "Composite CREATE INDEX should plan: {:?}",
            result.err()
        );
        let plan = result.unwrap();
        assert!(plan.is_write);
    }

    #[test]
    fn test_plan_multiple_match_cartesian_product() {
        let store = GraphStore::new();
        let planner = QueryPlanner::new();

        // Two independent patterns produce CartesianProduct
        let query = parse_query("MATCH (a:Person), (b:Company) RETURN a, b").unwrap();
        let result = planner.plan(&query, &store);
        assert!(
            result.is_ok(),
            "Multiple MATCH patterns should plan: {:?}",
            result.err()
        );
        let plan = result.unwrap();
        assert_eq!(plan.output_columns.len(), 2);
    }

    #[test]
    fn test_plan_optional_match_output_columns() {
        let store = GraphStore::new();
        let planner = QueryPlanner::new();

        let query =
            parse_query("MATCH (n:Person) OPTIONAL MATCH (n)-[:KNOWS]->(m) RETURN n, m").unwrap();
        let plan = planner.plan(&query, &store).unwrap();
        assert_eq!(plan.output_columns.len(), 2);
        assert!(plan.output_columns.contains(&"n".to_string()));
        assert!(plan.output_columns.contains(&"m".to_string()));
    }

    #[test]
    fn test_plan_with_aggregation() {
        let store = GraphStore::new();
        let planner = QueryPlanner::new();

        let query =
            parse_query("MATCH (n:Person) WITH n.city AS city, count(n) AS cnt RETURN city, cnt")
                .unwrap();
        let result = planner.plan(&query, &store);
        assert!(
            result.is_ok(),
            "WITH + aggregation should plan: {:?}",
            result.err()
        );
    }

    #[test]
    fn test_plan_with_order_by_limit() {
        let store = GraphStore::new();
        let planner = QueryPlanner::new();

        let query =
            parse_query("MATCH (n:Person) WITH n ORDER BY n.name LIMIT 10 RETURN n").unwrap();
        let result = planner.plan(&query, &store);
        assert!(
            result.is_ok(),
            "WITH ORDER BY LIMIT should plan: {:?}",
            result.err()
        );
    }

    #[test]
    fn test_plan_with_distinct() {
        let store = GraphStore::new();
        let planner = QueryPlanner::new();

        let query =
            parse_query("MATCH (n:Person) WITH DISTINCT n.city AS city RETURN city").unwrap();
        let result = planner.plan(&query, &store);
        assert!(
            result.is_ok(),
            "WITH DISTINCT should plan: {:?}",
            result.err()
        );
    }

    #[test]
    fn test_plan_multiple_aggregations() {
        let store = GraphStore::new();
        let planner = QueryPlanner::new();

        let query = parse_query("MATCH (n:Person) RETURN count(n) AS cnt, sum(n.age) AS total_age, avg(n.age) AS avg_age").unwrap();
        let result = planner.plan(&query, &store);
        assert!(
            result.is_ok(),
            "Multiple aggregations should plan: {:?}",
            result.err()
        );
        let plan = result.unwrap();
        assert_eq!(plan.output_columns.len(), 3);
        assert!(plan.output_columns.contains(&"cnt".to_string()));
        assert!(plan.output_columns.contains(&"total_age".to_string()));
        assert!(plan.output_columns.contains(&"avg_age".to_string()));
    }

    #[test]
    fn test_plan_collect_aggregation() {
        let store = GraphStore::new();
        let planner = QueryPlanner::new();

        let query = parse_query("MATCH (n:Person) RETURN collect(n.name) AS names").unwrap();
        let result = planner.plan(&query, &store);
        assert!(
            result.is_ok(),
            "collect() aggregation should plan: {:?}",
            result.err()
        );
    }

    #[test]
    fn test_plan_min_max_aggregation() {
        let store = GraphStore::new();
        let planner = QueryPlanner::new();

        let query =
            parse_query("MATCH (n:Person) RETURN min(n.age) AS youngest, max(n.age) AS oldest")
                .unwrap();
        let result = planner.plan(&query, &store);
        assert!(
            result.is_ok(),
            "min/max aggregation should plan: {:?}",
            result.err()
        );
    }

    #[test]
    fn test_plan_where_complex_and_chain() {
        let store = GraphStore::new();
        let planner = QueryPlanner::new();

        let query = parse_query(
            "MATCH (n:Person) WHERE n.age > 18 AND n.city = 'NYC' AND n.active = true RETURN n",
        )
        .unwrap();
        let result = planner.plan(&query, &store);
        assert!(
            result.is_ok(),
            "Complex AND chain WHERE should plan: {:?}",
            result.err()
        );
    }

    #[test]
    fn test_plan_where_or_predicate() {
        let store = GraphStore::new();
        let planner = QueryPlanner::new();

        let query =
            parse_query("MATCH (n:Person) WHERE n.age > 18 OR n.name = 'Admin' RETURN n").unwrap();
        let result = planner.plan(&query, &store);
        assert!(
            result.is_ok(),
            "OR predicate should plan: {:?}",
            result.err()
        );
    }

    #[test]
    fn test_plan_no_match_no_create_errors() {
        let store = GraphStore::new();
        let planner = QueryPlanner::new();

        // Build a query manually with no MATCH and no CREATE
        let query = crate::query::ast::Query {
            match_clauses: vec![],
            where_clause: None,
            return_clause: None,
            create_clause: None,
            create_clauses: vec![],
            order_by: None,
            limit: None,
            skip: None,
            call_clause: None,
            call_subquery: None,
            delete_clause: None,
            set_clauses: vec![],
            remove_clauses: vec![],
            with_clause: None,
            create_vector_index_clause: None,
            create_index_clause: None,
            drop_index_clause: None,
            create_constraint_clause: None,
            show_indexes: false,
            show_vector_indexes: false,
            show_constraints: false,
            profile: false,
            params: std::collections::HashMap::new(),
            foreach_clause: None,
            unwind_clause: None,
            additional_unwinds: Vec::new(),
            merge_clause: None,
            all_merge_clauses: vec![],
            union_queries: vec![],
            explain: false,
            with_split_index: None,
            post_with_where_clause: None,
            extra_with_stages: vec![],
            multi_part_stages: vec![],
        };
        let result = planner.plan(&query, &store);
        assert!(result.is_err());
        if let Err(e) = result {
            let msg = format!("{}", e);
            assert!(
                msg.contains("MATCH") || msg.contains("CALL") || msg.contains("CREATE"),
                "Error should mention required clauses: {}",
                msg
            );
        }
    }

    #[test]
    fn test_plan_match_with_edge_variable() {
        let store = GraphStore::new();
        let planner = QueryPlanner::new();

        let query = parse_query("MATCH (a:Person)-[r:KNOWS]->(b:Person) RETURN a, r, b").unwrap();
        let result = planner.plan(&query, &store);
        assert!(
            result.is_ok(),
            "Edge variable should plan: {:?}",
            result.err()
        );
        let plan = result.unwrap();
        assert_eq!(plan.output_columns.len(), 3);
    }

    #[test]
    fn test_plan_return_expressions() {
        let store = GraphStore::new();
        let planner = QueryPlanner::new();

        let query =
            parse_query("MATCH (n:Person) RETURN n.name AS name, n.age AS age, id(n) AS node_id")
                .unwrap();
        let plan = planner.plan(&query, &store).unwrap();
        assert_eq!(plan.output_columns, vec!["name", "age", "node_id"]);
    }

    #[test]
    fn test_plan_return_without_alias() {
        let store = GraphStore::new();
        let planner = QueryPlanner::new();

        let query = parse_query("MATCH (n:Person) RETURN n.name, n.age").unwrap();
        let plan = planner.plan(&query, &store).unwrap();
        // Without alias, the output column should be "variable.property"
        assert!(plan.output_columns.contains(&"n.name".to_string()));
        assert!(plan.output_columns.contains(&"n.age".to_string()));
    }

    #[test]
    fn test_plan_no_return_clause() {
        let store = GraphStore::new();
        let planner = QueryPlanner::new();

        // DELETE without RETURN — should still plan successfully
        let query = parse_query("MATCH (n:Person) DELETE n").unwrap();
        let plan = planner.plan(&query, &store).unwrap();
        // Output columns come from MATCH variables
        assert!(plan.output_columns.contains(&"n".to_string()));
    }

    #[test]
    fn test_plan_order_by_with_aggregation() {
        let store = GraphStore::new();
        let planner = QueryPlanner::new();

        let query =
            parse_query("MATCH (n:Person) RETURN n.city, count(n) AS cnt ORDER BY cnt").unwrap();
        let result = planner.plan(&query, &store);
        assert!(
            result.is_ok(),
            "ORDER BY with aggregation should plan: {:?}",
            result.err()
        );
    }

    #[test]
    fn test_plan_unwind_with_return() {
        let store = GraphStore::new();
        let planner = QueryPlanner::new();

        let query = parse_query("MATCH (n) UNWIND [1, 2, 3] AS x RETURN x, n").unwrap();
        let result = planner.plan(&query, &store);
        assert!(
            result.is_ok(),
            "UNWIND with RETURN should plan: {:?}",
            result.err()
        );
        let plan = result.unwrap();
        assert!(plan.output_columns.contains(&"x".to_string()));
        assert!(plan.output_columns.contains(&"n".to_string()));
    }

    #[test]
    fn test_plan_merge_with_return() {
        let store = GraphStore::new();
        let planner = QueryPlanner::new();

        let query = parse_query("MERGE (n:Person {name: 'Alice'}) RETURN n").unwrap();
        let result = planner.plan(&query, &store);
        assert!(
            result.is_ok(),
            "MERGE with RETURN should plan: {:?}",
            result.err()
        );
        let plan = result.unwrap();
        assert!(plan.is_write);
        assert!(plan.output_columns.contains(&"n".to_string()));
    }

    #[test]
    fn test_plan_with_where_filter() {
        let store = GraphStore::new();
        let planner = QueryPlanner::new();

        let query = parse_query("MATCH (n:Person) WITH n WHERE n.age > 30 RETURN n").unwrap();
        let result = planner.plan(&query, &store);
        assert!(result.is_ok(), "WITH WHERE should plan: {:?}", result.err());
    }

    #[test]
    fn test_plan_with_skip() {
        let store = GraphStore::new();
        let planner = QueryPlanner::new();

        let query = parse_query("MATCH (n:Person) WITH n SKIP 5 RETURN n").unwrap();
        let result = planner.plan(&query, &store);
        assert!(result.is_ok(), "WITH SKIP should plan: {:?}", result.err());
    }

    #[test]
    fn test_plan_with_resets_known_vars() {
        let store = GraphStore::new();
        let planner = QueryPlanner::new();

        // WITH clause should project only selected variables
        let query = parse_query("MATCH (n:Person) WITH n.name AS name RETURN name").unwrap();
        let plan = planner.plan(&query, &store).unwrap();
        assert!(plan.output_columns.contains(&"name".to_string()));
    }

    #[test]
    fn test_plan_match_with_node_properties() {
        let store = GraphStore::new();
        let planner = QueryPlanner::new();

        let query = parse_query("MATCH (n:Person {name: 'Alice'}) RETURN n").unwrap();
        let result = planner.plan(&query, &store);
        assert!(
            result.is_ok(),
            "Node with inline properties should plan: {:?}",
            result.err()
        );
    }

    #[test]
    fn test_inline_properties_trigger_index_scan() {
        // Inline properties like {name: 'Alice'} should use IndexScan when an index exists,
        // not fall back to NodeScan + Filter (O(n)).
        let mut store = GraphStore::new();
        for i in 0..100 {
            let id = store.create_node("Person");
            store
                .set_node_property(
                    "default",
                    id,
                    "name",
                    crate::graph::PropertyValue::String(format!("Person{}", i)),
                )
                .unwrap();
        }
        store
            .property_index
            .create_index(crate::graph::Label::new("Person"), "name".to_string());

        // Both forms should produce the same plan with IndexScan
        use crate::graph::PropertyValue;
        use crate::query::executor::record::Value;

        // WHERE form (already works)
        let q_where =
            parse_query("EXPLAIN MATCH (n:Person) WHERE n.name = 'Person50' RETURN n").unwrap();
        let executor_where = crate::query::executor::QueryExecutor::new(&store);
        let r_where = executor_where.execute(&q_where).unwrap();
        let plan_where = if let Some(Value::Property(PropertyValue::String(s))) =
            r_where.records[0].get("plan")
        {
            s.clone()
        } else {
            panic!("Expected plan text");
        };

        // Inline form (was broken, should now use IndexScan)
        let q_inline = parse_query("EXPLAIN MATCH (n:Person {name: 'Person50'}) RETURN n").unwrap();
        let executor_inline = crate::query::executor::QueryExecutor::new(&store);
        let r_inline = executor_inline.execute(&q_inline).unwrap();
        let plan_inline = if let Some(Value::Property(PropertyValue::String(s))) =
            r_inline.records[0].get("plan")
        {
            s.clone()
        } else {
            panic!("Expected plan text");
        };

        assert!(
            plan_where.contains("IndexScan"),
            "WHERE form should use IndexScan: {}",
            plan_where
        );
        assert!(
            plan_inline.contains("IndexScan"),
            "Inline properties should use IndexScan: {}",
            plan_inline
        );
        assert!(
            !plan_inline.contains("NodeScan"),
            "Inline properties should NOT use NodeScan when index exists: {}",
            plan_inline
        );
    }

    #[test]
    fn test_plan_edge_direction() {
        let store = GraphStore::new();
        let planner = QueryPlanner::new();

        // Forward direction
        let query = parse_query("MATCH (a:Person)-[:KNOWS]->(b:Person) RETURN a, b").unwrap();
        assert!(planner.plan(&query, &store).is_ok());

        // Backward direction
        let query = parse_query("MATCH (a:Person)<-[:KNOWS]-(b:Person) RETURN a, b").unwrap();
        assert!(planner.plan(&query, &store).is_ok());
    }

    #[test]
    fn test_plan_multi_hop_path() {
        let store = GraphStore::new();
        let planner = QueryPlanner::new();

        let query = parse_query(
            "MATCH (a:Person)-[:KNOWS]->(b:Person)-[:LIVES_IN]->(c:City) RETURN a, b, c",
        )
        .unwrap();
        let result = planner.plan(&query, &store);
        assert!(
            result.is_ok(),
            "Multi-hop path should plan: {:?}",
            result.err()
        );
        let plan = result.unwrap();
        assert_eq!(plan.output_columns.len(), 3);
    }

    #[test]
    fn test_plan_index_scan_with_gt_operator() {
        let mut store = GraphStore::new();
        for i in 0..50 {
            let id = store.create_node("Person");
            store
                .set_node_property(
                    "default",
                    id,
                    "age",
                    crate::graph::PropertyValue::Integer(i as i64),
                )
                .unwrap();
        }
        store
            .property_index
            .create_index(crate::graph::Label::new("Person"), "age".to_string());

        let planner = QueryPlanner::new();
        let query = parse_query("MATCH (n:Person) WHERE n.age > 25 RETURN n").unwrap();
        let result = planner.plan(&query, &store);
        assert!(
            result.is_ok(),
            "Index scan with > should plan: {:?}",
            result.err()
        );
    }

    #[test]
    fn test_plan_index_scan_with_lt_operator() {
        let mut store = GraphStore::new();
        for i in 0..50 {
            let id = store.create_node("Person");
            store
                .set_node_property(
                    "default",
                    id,
                    "age",
                    crate::graph::PropertyValue::Integer(i as i64),
                )
                .unwrap();
        }
        store
            .property_index
            .create_index(crate::graph::Label::new("Person"), "age".to_string());

        let planner = QueryPlanner::new();
        let query = parse_query("MATCH (n:Person) WHERE n.age < 25 RETURN n").unwrap();
        let result = planner.plan(&query, &store);
        assert!(
            result.is_ok(),
            "Index scan with < should plan: {:?}",
            result.err()
        );
    }

    #[test]
    fn test_plan_cross_match_where_predicate() {
        let store = GraphStore::new();
        let planner = QueryPlanner::new();

        // WHERE predicate references variables from different MATCH patterns
        let query =
            parse_query("MATCH (a:Person), (b:Company) WHERE a.company = b.name RETURN a, b")
                .unwrap();
        let result = planner.plan(&query, &store);
        assert!(
            result.is_ok(),
            "Cross-match WHERE should plan: {:?}",
            result.err()
        );
    }

    #[test]
    fn test_plan_match_all_nodes() {
        let store = GraphStore::new();
        let planner = QueryPlanner::new();

        // Match without label — all node scan
        let query = parse_query("MATCH (n) RETURN n").unwrap();
        let result = planner.plan(&query, &store);
        assert!(
            result.is_ok(),
            "All-node scan should plan: {:?}",
            result.err()
        );
    }

    #[test]
    fn test_plan_function_alias_generation() {
        let store = GraphStore::new();
        let planner = QueryPlanner::new();

        // Function without alias should auto-generate column name
        let query = parse_query("MATCH (n:Person) RETURN count(n)").unwrap();
        let plan = planner.plan(&query, &store).unwrap();
        assert_eq!(plan.output_columns.len(), 1);
        // Auto-generated alias should be like "count(n)"
        assert!(plan.output_columns[0].contains("count"));
    }

    #[test]
    fn test_plan_collect_distinct() {
        let store = GraphStore::new();
        let planner = QueryPlanner::new();

        let query = parse_query("MATCH (n:Person) RETURN collect(DISTINCT n.name) AS unique_names")
            .unwrap();
        let result = planner.plan(&query, &store);
        assert!(
            result.is_ok(),
            "collect(DISTINCT) should plan: {:?}",
            result.err()
        );
    }

    #[test]
    fn test_plan_with_multiple_aggregations() {
        let store = GraphStore::new();
        let planner = QueryPlanner::new();

        let query = parse_query("MATCH (n:Person) WITH n.city AS city, count(n) AS cnt, collect(n.name) AS names RETURN city, cnt, names").unwrap();
        let result = planner.plan(&query, &store);
        assert!(
            result.is_ok(),
            "WITH multiple aggregations should plan: {:?}",
            result.err()
        );
    }

    #[test]
    fn test_where_not_duplicated_after_with_barrier() {
        // Regression test: WHERE predicates referencing variables that are projected
        // away by WITH should not cause "Variable not found" errors. The WHERE must
        // only be applied before the WithBarrier, not after it.
        let mut store = GraphStore::new();
        let n1 = store.create_node("Team");
        store
            .get_node_mut(n1)
            .unwrap()
            .set_property("name", PropertyValue::String("India".into()));
        let n2 = store.create_node("Match");
        let n3 = store.create_node("Tournament");
        store
            .get_node_mut(n3)
            .unwrap()
            .set_property("name", PropertyValue::String("IPL".into()));
        store.create_edge(n1, n2, "COMPETED_IN").unwrap();
        store.create_edge(n2, n3, "PART_OF").unwrap();

        let query = parse_query(
            "MATCH (t:Team)-[:COMPETED_IN]->(m:Match)-[:PART_OF]->(trn:Tournament) \
             WHERE trn.name = 'IPL' \
             WITH t, count(m) AS played \
             RETURN t.name AS team, played",
        )
        .unwrap();

        let planner = QueryPlanner::new();
        let plan = planner.plan(&query, &store).unwrap();
        use crate::query::QueryExecutor;
        let executor = QueryExecutor::new(&store);
        let result = executor.execute_plan(plan);
        assert!(
            result.is_ok(),
            "WHERE + WITH should not fail: {:?}",
            result.err()
        );
        let batch = result.unwrap();
        assert_eq!(batch.records.len(), 1);
    }

    #[test]
    fn test_node_identity_comparison() {
        let mut store = GraphStore::new();
        let n1 = store.create_node("Team");
        store
            .get_node_mut(n1)
            .unwrap()
            .set_property("name", PropertyValue::String("India".into()));
        let n2 = store.create_node("Team");
        store
            .get_node_mut(n2)
            .unwrap()
            .set_property("name", PropertyValue::String("Australia".into()));
        let m1 = store.create_node("Match");
        store.create_edge(n1, m1, "COMPETED_IN").unwrap();
        store.create_edge(n2, m1, "COMPETED_IN").unwrap();

        // Test: t1 <> t2 (node inequality comparison)
        let query = parse_query(
            "MATCH (t1:Team)-[:COMPETED_IN]->(m:Match)<-[:COMPETED_IN]-(t2:Team) \
             WHERE t1 <> t2 \
             RETURN t1.name AS team1, t2.name AS team2",
        )
        .unwrap();

        let planner = QueryPlanner::new();
        let plan = planner.plan(&query, &store).unwrap();
        use crate::query::QueryExecutor;
        let executor = QueryExecutor::new(&store);
        let result = executor.execute_plan(plan);
        assert!(
            result.is_ok(),
            "Node identity comparison should work: {:?}",
            result.err()
        );
        let batch = result.unwrap();
        // Should get 2 rows: (India, Australia) and (Australia, India)
        assert_eq!(batch.records.len(), 2);
    }

    // ============================
    // ADR-015: Graph-native planner integration tests
    // ============================

    #[test]
    fn test_planner_config_default() {
        let config = PlannerConfig::default();
        assert!(!config.graph_native);
        assert_eq!(config.max_candidate_plans, 64);
    }

    #[test]
    fn test_planner_with_config() {
        let config = PlannerConfig {
            graph_native: true,
            max_candidate_plans: 32,
        };
        let planner = QueryPlanner::with_config(config);
        assert!(planner.config().graph_native);
        assert_eq!(planner.config().max_candidate_plans, 32);
    }

    #[test]
    fn test_plan_match_native_simple() {
        let mut store = GraphStore::new();
        let n1 = store.create_node("Person");
        store
            .get_node_mut(n1)
            .unwrap()
            .set_property("name", PropertyValue::String("Alice".to_string()));

        let planner = QueryPlanner::with_config(PlannerConfig {
            graph_native: true,
            max_candidate_plans: 64,
        });
        let query = parse_query("MATCH (n:Person) RETURN n").unwrap();
        let result = planner.plan(&query, &store);
        assert!(
            result.is_ok(),
            "Graph-native planner should handle simple MATCH: {:?}",
            result.err()
        );
    }

    #[test]
    fn test_plan_match_native_with_expand() {
        let mut store = GraphStore::new();
        let a = store.create_node("Person");
        let b = store.create_node("Person");
        store.create_edge(a, b, "KNOWS").unwrap();

        let planner = QueryPlanner::with_config(PlannerConfig {
            graph_native: true,
            max_candidate_plans: 64,
        });
        let query = parse_query("MATCH (a:Person)-[:KNOWS]->(b:Person) RETURN a, b").unwrap();
        let result = planner.plan(&query, &store);
        assert!(
            result.is_ok(),
            "Graph-native planner should handle expand: {:?}",
            result.err()
        );
    }

    #[test]
    fn test_ab_correctness_simple_scan() {
        // A/B test: both planners should produce identical results
        let mut store = GraphStore::new();
        let n1 = store.create_node("Person");
        store
            .get_node_mut(n1)
            .unwrap()
            .set_property("name", PropertyValue::String("Alice".to_string()));
        let n2 = store.create_node("Person");
        store
            .get_node_mut(n2)
            .unwrap()
            .set_property("name", PropertyValue::String("Bob".to_string()));
        store.create_node("Company"); // should not appear

        let query = parse_query("MATCH (n:Person) RETURN n.name").unwrap();

        // Legacy planner
        let legacy = QueryPlanner::new();
        let legacy_plan = legacy.plan(&query, &store).unwrap();
        let mut legacy_op = legacy_plan.root;
        let mut legacy_results = Vec::new();
        while let Some(record) = legacy_op.next(&store).unwrap() {
            if let Some(val) = record.get("n.name") {
                legacy_results.push(format!("{:?}", val));
            }
        }
        legacy_results.sort();

        // Graph-native planner
        let native = QueryPlanner::with_config(PlannerConfig {
            graph_native: true,
            max_candidate_plans: 64,
        });
        let native_plan = native.plan(&query, &store).unwrap();
        let mut native_op = native_plan.root;
        let mut native_results = Vec::new();
        while let Some(record) = native_op.next(&store).unwrap() {
            if let Some(val) = record.get("n.name") {
                native_results.push(format!("{:?}", val));
            }
        }
        native_results.sort();

        assert_eq!(legacy_results, native_results,
            "Legacy and native planners must produce identical results.\nLegacy: {:?}\nNative: {:?}", legacy_results, native_results);
    }

    #[test]
    fn test_ab_correctness_expand() {
        // A/B: ALL candidate plans must produce identical results to legacy
        let mut store = GraphStore::new();
        let a = store.create_node("Person");
        store
            .get_node_mut(a)
            .unwrap()
            .set_property("name", PropertyValue::String("Alice".to_string()));
        let b = store.create_node("Person");
        store
            .get_node_mut(b)
            .unwrap()
            .set_property("name", PropertyValue::String("Bob".to_string()));
        let c = store.create_node("Person");
        store
            .get_node_mut(c)
            .unwrap()
            .set_property("name", PropertyValue::String("Charlie".to_string()));
        store.create_edge(a, b, "KNOWS").unwrap();
        store.create_edge(a, c, "KNOWS").unwrap();

        let query =
            parse_query("MATCH (a:Person)-[:KNOWS]->(b:Person) RETURN a.name, b.name").unwrap();

        // Legacy planner results
        let legacy = QueryPlanner::new();
        let legacy_plan = legacy.plan(&query, &store).unwrap();
        let mut legacy_results: Vec<String> = Vec::new();
        let mut op = legacy_plan.root;
        while let Some(record) = op.next(&store).unwrap() {
            let a_name = record
                .get("a.name")
                .map(|v| format!("{:?}", v))
                .unwrap_or_default();
            let b_name = record
                .get("b.name")
                .map(|v| format!("{:?}", v))
                .unwrap_or_default();
            legacy_results.push(format!("{}->{}", a_name, b_name));
        }
        legacy_results.sort();

        // Graph-native planner — verify ALL candidate plans produce correct results
        use super::super::logical_plan::PatternGraph;
        use super::super::physical_planner::logical_to_physical;
        use super::super::plan_enumerator::{enumerate_plans, EnumerationConfig};

        let match_clause = &query.match_clauses[0];
        let pg = PatternGraph::from_match_clause(match_clause);
        let catalog = store.catalog();
        let config = EnumerationConfig {
            max_candidate_plans: 64,
        };
        let candidates = enumerate_plans(&pg, query.where_clause.as_ref(), catalog, &config);
        assert!(
            candidates.len() >= 2,
            "Should have at least 2 candidate plans"
        );

        for (plan_idx, (logical_plan, cost)) in candidates.iter().enumerate() {
            let physical = logical_to_physical(logical_plan);
            let projections = vec![
                (
                    Expression::Property {
                        variable: "a".to_string(),
                        property: "name".to_string(),
                    },
                    "a.name".to_string(),
                ),
                (
                    Expression::Property {
                        variable: "b".to_string(),
                        property: "name".to_string(),
                    },
                    "b.name".to_string(),
                ),
            ];
            let mut op: OperatorBox = Box::new(super::super::operator::ProjectOperator::new(
                physical,
                projections,
            ));

            let mut native_results: Vec<String> = Vec::new();
            while let Some(record) = op.next(&store).unwrap() {
                let a_name = record
                    .get("a.name")
                    .map(|v| format!("{:?}", v))
                    .unwrap_or_default();
                let b_name = record
                    .get("b.name")
                    .map(|v| format!("{:?}", v))
                    .unwrap_or_default();
                native_results.push(format!("{}->{}", a_name, b_name));
            }
            native_results.sort();

            assert_eq!(legacy_results, native_results,
                "Plan candidate #{} (cost={}) produces different results.\nLegacy: {:?}\nNative: {:?}",
                plan_idx, cost, legacy_results, native_results);
        }
    }

    #[test]
    fn test_ab_correctness_with_where() {
        // A/B: MATCH with WHERE filter
        let mut store = GraphStore::new();
        let n1 = store.create_node("Person");
        store
            .get_node_mut(n1)
            .unwrap()
            .set_property("age", PropertyValue::Integer(25));
        store
            .get_node_mut(n1)
            .unwrap()
            .set_property("name", PropertyValue::String("Alice".to_string()));
        let n2 = store.create_node("Person");
        store
            .get_node_mut(n2)
            .unwrap()
            .set_property("age", PropertyValue::Integer(35));
        store
            .get_node_mut(n2)
            .unwrap()
            .set_property("name", PropertyValue::String("Bob".to_string()));
        let n3 = store.create_node("Person");
        store
            .get_node_mut(n3)
            .unwrap()
            .set_property("age", PropertyValue::Integer(45));
        store
            .get_node_mut(n3)
            .unwrap()
            .set_property("name", PropertyValue::String("Charlie".to_string()));

        let query = parse_query("MATCH (n:Person) WHERE n.age > 30 RETURN n.name").unwrap();

        let legacy = QueryPlanner::new();
        let native = QueryPlanner::with_config(PlannerConfig {
            graph_native: true,
            max_candidate_plans: 64,
        });

        let legacy_plan = legacy.plan(&query, &store).unwrap();
        let native_plan = native.plan(&query, &store).unwrap();

        let mut legacy_results: Vec<String> = Vec::new();
        let mut op = legacy_plan.root;
        while let Some(record) = op.next(&store).unwrap() {
            if let Some(val) = record.get("n.name") {
                legacy_results.push(format!("{:?}", val));
            }
        }
        legacy_results.sort();

        let mut native_results: Vec<String> = Vec::new();
        let mut op = native_plan.root;
        while let Some(record) = op.next(&store).unwrap() {
            if let Some(val) = record.get("n.name") {
                native_results.push(format!("{:?}", val));
            }
        }
        native_results.sort();

        assert_eq!(
            legacy_results, native_results,
            "WHERE filter results differ.\nLegacy: {:?}\nNative: {:?}",
            legacy_results, native_results
        );
    }
}
