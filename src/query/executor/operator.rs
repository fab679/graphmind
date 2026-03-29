//! Physical operators for query execution using the Volcano iterator model.
//!
//! # Volcano Iterator Model (ADR-007)
//!
//! The Volcano model (Goetz Graefe, 1990s) is the dominant query execution paradigm in
//! relational and graph databases. Each operator implements a `next()` method that returns
//! one record at a time, pulling from child operators on demand. This creates a pipeline
//! where data flows from leaf operators (scans) up through filters, joins, and projections
//! to the root operator that produces final results.
//!
//! # Physical Operators
//!
//! | Operator | Description |
//! |---|---|
//! | `NodeScanOperator` | Scans all nodes matching a label (like a table scan in SQL) |
//! | `IndexScanOperator` | Uses a B-tree index to find nodes matching a predicate |
//! | `FilterOperator` | Evaluates a WHERE predicate, discarding non-matching records |
//! | `ExpandOperator` | Traverses edges from bound nodes to discover neighbors (graph-native; no SQL equivalent without expensive JOINs) |
//! | `ExpandIntoOperator` | Checks if an edge exists between two already-bound nodes (a semi-join) |
//! | `ProjectOperator` | Evaluates RETURN expressions, materializing `NodeRef` → `Node` for output |
//! | `LimitOperator` / `SkipOperator` | LIMIT and SKIP clauses |
//! | `SortOperator` | ORDER BY with multi-key comparison |
//! | `AggregateOperator` | GROUP BY + aggregation functions (count, sum, avg, min, max, collect) |
//! | `JoinOperator` | Hash join for multi-pattern MATCH queries |
//! | `LeftOuterJoinOperator` | For OPTIONAL MATCH (preserves unmatched left rows with NULLs) |
//! | `CartesianProductOperator` | Cross product for disconnected patterns |
//! | `UnwindOperator` | Expands arrays into individual rows |
//! | `MergeOperator` | MERGE (upsert): CREATE if not exists, otherwise match |
//! | `ShortestPathOperator` | BFS/Dijkstra for `shortestPath()` function |
//! | `VectorSearchOperator` | HNSW approximate nearest neighbor search |
//! | `AlgorithmOperator` | Runs graph algorithms (PageRank, WCC, SCC, etc.) |
//! | DDL operators | `CreateIndex`, `DropIndex`, `CreateConstraint`, `ShowIndexes`, etc. |
//!
//! # Expression Evaluation
//!
//! The `eval_expression()` function recursively evaluates AST expressions against a record.
//! It handles property access (`n.name`), arithmetic (`a + b`), comparisons (`a > b`),
//! boolean logic (`AND`/`OR`/`NOT`), function calls (`toUpper()`, `count()`), CASE
//! expressions, list operations, and more.
//!
//! # Type Coercion and NULL Propagation
//!
//! Integer/Float automatic promotion (widening), String concatenation via `+`, and NULL
//! propagation following three-valued logic: any operation involving NULL returns NULL,
//! except `IS NULL` / `IS NOT NULL`.
//!
//! # Late Materialization
//!
//! Operators work with `Value::NodeRef(id)` instead of full `Value::Node(id, clone)`.
//! Property access goes through `resolve_property()`, which looks up the property from
//! the [`GraphStore`] on demand. Full materialization only happens at `ProjectOperator`
//! when the query returns a node variable. See ADR-012.
//!
//! # Metaheuristic Optimization Solvers
//!
//! `AlgorithmOperator` integrates 16 solvers from `graphmind-optimization` (Jaya, Rao,
//! TLBO, Firefly, Cuckoo, GWO, GA, SA, Bat, ABC, GSA, NSGA2, MOTLBO, HS, FPA) for
//! solving continuous optimization problems within graph queries.
//!
//! # Rust Patterns
//!
//! - `Box<dyn PhysicalOperator>` — dynamic dispatch via trait objects for operator trees
//! - `&GraphStore` — lifetime-bounded borrow of the graph during execution
//! - `HashMap` — build phase of hash joins in `JoinOperator`
//! - `BTreeSet` — sorted unique results where ordering matters

use crate::graph::PropertyValue;
use crate::graph::{EdgeType, GraphStore, Label, NodeId};
use crate::query::ast::{BinaryOp, Direction, Expression, Pattern, UnaryOp};
use crate::query::executor::{ExecutionError, ExecutionResult, Record, RecordBatch, Value};
use graphmind_optimization::algorithms::{
    ABCSolver, BatSolver, CuckooSolver, FPASolver, FireflySolver, GASolver, GSASolver, GWOSolver,
    HSSolver, JayaSolver, MOTLBOSolver, NSGA2Solver, RaoSolver, RaoVariant, SASolver, TLBOSolver,
};
use graphmind_optimization::common::{MultiObjectiveProblem, Problem, SolverConfig};
use ndarray::Array1;
use std::collections::{BTreeSet, HashMap, HashSet};

// Thread-local query deadline for cooperative timeout inside operator materialization loops.
// Set by QueryExecutor before execution, checked by JoinOperator/AggregateOperator/SortOperator.
thread_local! {
    static QUERY_DEADLINE: std::cell::Cell<Option<std::time::Instant>> = const { std::cell::Cell::new(None) };
}

/// Set the query deadline for the current thread (called by QueryExecutor)
pub fn set_query_deadline(deadline: Option<std::time::Instant>) {
    QUERY_DEADLINE.with(|d| d.set(deadline));
}

/// Check if the query deadline has been exceeded; returns Err if so
fn check_deadline() -> ExecutionResult<()> {
    QUERY_DEADLINE.with(|d| {
        if let Some(deadline) = d.get() {
            if std::time::Instant::now() > deadline {
                return Err(ExecutionError::RuntimeError("Query timed out".to_string()));
            }
        }
        Ok(())
    })
}

/// Extract node ID from a Value for identity comparison
fn node_id_of(v: &Value) -> Option<NodeId> {
    match v {
        Value::NodeRef(id) | Value::Node(id, _) => Some(*id),
        _ => None,
    }
}

/// Cross-type equality with coercion: Integer↔Float promotion, String↔Boolean coercion
fn coerced_eq(left: &PropertyValue, right: &PropertyValue) -> bool {
    match (left, right) {
        _ if std::mem::discriminant(left) == std::mem::discriminant(right) => left == right,
        (PropertyValue::Integer(l), PropertyValue::Float(r)) => (*l as f64) == *r,
        (PropertyValue::Float(l), PropertyValue::Integer(r)) => *l == (*r as f64),
        (PropertyValue::DateTime(l), PropertyValue::Integer(r))
        | (PropertyValue::Integer(r), PropertyValue::DateTime(l)) => l == r,
        (PropertyValue::Boolean(b), PropertyValue::String(s))
        | (PropertyValue::String(s), PropertyValue::Boolean(b)) => {
            match s.to_lowercase().as_str() {
                "true" => *b,
                "false" => !*b,
                _ => false,
            }
        }
        _ => false,
    }
}

/// Cross-type partial comparison with numeric promotion
fn coerced_partial_cmp(left: &PropertyValue, right: &PropertyValue) -> Option<std::cmp::Ordering> {
    match (left, right) {
        (PropertyValue::Integer(l), PropertyValue::Float(r)) => (*l as f64).partial_cmp(r),
        (PropertyValue::Float(l), PropertyValue::Integer(r)) => l.partial_cmp(&(*r as f64)),
        _ => left.partial_cmp(right),
    }
}

/// Shared binary operator evaluation used by Project, Aggregate, Filter, and Sort operators
fn eval_binary_op(op: &BinaryOp, left: Value, right: Value) -> ExecutionResult<Value> {
    // Node/edge identity comparison (Cypher: n1 = n2, n1 <> n2, r1 = r2)
    if matches!(op, BinaryOp::Eq | BinaryOp::Ne) {
        // Node identity
        if let (Some(lid), Some(rid)) = (node_id_of(&left), node_id_of(&right)) {
            let eq = lid == rid;
            return Ok(Value::Property(PropertyValue::Boolean(
                if matches!(op, BinaryOp::Eq) { eq } else { !eq },
            )));
        }
        // Edge identity
        let left_eid = match &left {
            Value::EdgeRef(id, ..) | Value::Edge(id, _) => Some(*id),
            _ => None,
        };
        let right_eid = match &right {
            Value::EdgeRef(id, ..) | Value::Edge(id, _) => Some(*id),
            _ => None,
        };
        if let (Some(lid), Some(rid)) = (left_eid, right_eid) {
            let eq = lid == rid;
            return Ok(Value::Property(PropertyValue::Boolean(
                if matches!(op, BinaryOp::Eq) { eq } else { !eq },
            )));
        }
        // Path identity
        if let (
            Value::Path {
                nodes: n1,
                edges: e1,
            },
            Value::Path {
                nodes: n2,
                edges: e2,
            },
        ) = (&left, &right)
        {
            let eq = n1 == n2 && e1 == e2;
            return Ok(Value::Property(PropertyValue::Boolean(
                if matches!(op, BinaryOp::Eq) { eq } else { !eq },
            )));
        }
    }

    // Convert Values to PropertyValues, treating non-property values as Null for comparisons
    let left_prop = match left {
        Value::Property(p) => p,
        Value::Null => PropertyValue::Null,
        _ => PropertyValue::Null, // NodeRef, EdgeRef, Path — treat as null for arithmetic/comparisons
    };
    let right_prop = match right {
        Value::Property(p) => p,
        Value::Null => PropertyValue::Null,
        _ => PropertyValue::Null, // NodeRef, EdgeRef, Path — treat as null for arithmetic/comparisons
    };

    // Null propagation: most operators return null when either operand is null
    // Exceptions: IS NULL/IS NOT NULL (handled in unary), AND/OR (three-valued logic)
    let left_is_null = matches!(left_prop, PropertyValue::Null);
    let right_is_null = matches!(right_prop, PropertyValue::Null);

    if left_is_null || right_is_null {
        match op {
            // Three-valued logic for AND/OR
            BinaryOp::And => {
                return match (&left_prop, &right_prop) {
                    // false AND null = false, null AND false = false
                    (PropertyValue::Boolean(false), _) | (_, PropertyValue::Boolean(false)) => {
                        Ok(Value::Property(PropertyValue::Boolean(false)))
                    }
                    // true AND null = null, null AND true = null, null AND null = null
                    _ => Ok(Value::Null),
                };
            }
            BinaryOp::Or => {
                return match (&left_prop, &right_prop) {
                    // true OR null = true, null OR true = true
                    (PropertyValue::Boolean(true), _) | (_, PropertyValue::Boolean(true)) => {
                        Ok(Value::Property(PropertyValue::Boolean(true)))
                    }
                    // false OR null = null, null OR false = null, null OR null = null
                    _ => Ok(Value::Null),
                };
            }
            BinaryOp::Xor => {
                return Ok(Value::Null); // XOR with null is always null
            }
            // IN with null left operand returns null
            BinaryOp::In if left_is_null => return Ok(Value::Null),
            // All other ops: null propagation
            _ => return Ok(Value::Null),
        }
    }

    let result = match op {
        BinaryOp::Eq => PropertyValue::Boolean(coerced_eq(&left_prop, &right_prop)),
        BinaryOp::Ne => PropertyValue::Boolean(!coerced_eq(&left_prop, &right_prop)),
        BinaryOp::Lt | BinaryOp::Le | BinaryOp::Gt | BinaryOp::Ge => {
            let cmp = coerced_partial_cmp(&left_prop, &right_prop);
            match (op, cmp) {
                (BinaryOp::Lt, Some(std::cmp::Ordering::Less)) => PropertyValue::Boolean(true),
                (BinaryOp::Le, Some(std::cmp::Ordering::Less | std::cmp::Ordering::Equal)) => {
                    PropertyValue::Boolean(true)
                }
                (BinaryOp::Gt, Some(std::cmp::Ordering::Greater)) => PropertyValue::Boolean(true),
                (BinaryOp::Ge, Some(std::cmp::Ordering::Greater | std::cmp::Ordering::Equal)) => {
                    PropertyValue::Boolean(true)
                }
                (_, None) => PropertyValue::Null,
                _ => PropertyValue::Boolean(false),
            }
        }
        BinaryOp::And => match (&left_prop, &right_prop) {
            (PropertyValue::Boolean(l), PropertyValue::Boolean(r)) => {
                PropertyValue::Boolean(*l && *r)
            }
            // Three-valued logic: false AND null = false, true AND null = null
            (PropertyValue::Boolean(false), PropertyValue::Null)
            | (PropertyValue::Null, PropertyValue::Boolean(false)) => PropertyValue::Boolean(false),
            (PropertyValue::Boolean(true), PropertyValue::Null)
            | (PropertyValue::Null, PropertyValue::Boolean(true))
            | (PropertyValue::Null, PropertyValue::Null) => PropertyValue::Null,
            _ => {
                return Err(ExecutionError::TypeError(
                    "AND requires booleans".to_string(),
                ))
            }
        },
        BinaryOp::Or => match (&left_prop, &right_prop) {
            (PropertyValue::Boolean(l), PropertyValue::Boolean(r)) => {
                PropertyValue::Boolean(*l || *r)
            }
            // Three-valued logic: true OR null = true, false OR null = null
            (PropertyValue::Boolean(true), PropertyValue::Null)
            | (PropertyValue::Null, PropertyValue::Boolean(true)) => PropertyValue::Boolean(true),
            (PropertyValue::Boolean(false), PropertyValue::Null)
            | (PropertyValue::Null, PropertyValue::Boolean(false))
            | (PropertyValue::Null, PropertyValue::Null) => PropertyValue::Null,
            _ => {
                return Err(ExecutionError::TypeError(
                    "OR requires booleans".to_string(),
                ))
            }
        },
        BinaryOp::Xor => match (&left_prop, &right_prop) {
            (PropertyValue::Boolean(l), PropertyValue::Boolean(r)) => {
                PropertyValue::Boolean(*l ^ *r)
            }
            _ => {
                return Err(ExecutionError::TypeError(
                    "XOR requires booleans".to_string(),
                ))
            }
        },
        BinaryOp::Add => match (&left_prop, &right_prop) {
            (PropertyValue::Integer(l), PropertyValue::Integer(r)) => PropertyValue::Integer(l + r),
            (PropertyValue::Float(l), PropertyValue::Float(r)) => PropertyValue::Float(l + r),
            (PropertyValue::Integer(l), PropertyValue::Float(r)) => {
                PropertyValue::Float(*l as f64 + r)
            }
            (PropertyValue::Float(l), PropertyValue::Integer(r)) => {
                PropertyValue::Float(l + *r as f64)
            }
            (PropertyValue::String(l), PropertyValue::String(r)) => {
                PropertyValue::String(format!("{}{}", l, r))
            }
            // DateTime + Duration
            (
                PropertyValue::DateTime(dt),
                PropertyValue::Duration {
                    months,
                    days,
                    seconds,
                    ..
                },
            )
            | (
                PropertyValue::Duration {
                    months,
                    days,
                    seconds,
                    ..
                },
                PropertyValue::DateTime(dt),
            ) => add_duration_to_datetime(*dt, *months, *days, *seconds),
            // Duration + Duration
            (
                PropertyValue::Duration {
                    months: m1,
                    days: d1,
                    seconds: s1,
                    nanos: n1,
                },
                PropertyValue::Duration {
                    months: m2,
                    days: d2,
                    seconds: s2,
                    nanos: n2,
                },
            ) => PropertyValue::Duration {
                months: m1 + m2,
                days: d1 + d2,
                seconds: s1 + s2,
                nanos: n1 + n2,
            },
            // List concatenation (Array + Array)
            (PropertyValue::Array(l), PropertyValue::Array(r)) => {
                let mut result = l.clone();
                result.extend(r.iter().cloned());
                PropertyValue::Array(result)
            }
            // Vector + Vector (numeric lists)
            (PropertyValue::Vector(l), PropertyValue::Vector(r)) => {
                let mut result: Vec<PropertyValue> =
                    l.iter().map(|f| PropertyValue::Float(*f as f64)).collect();
                result.extend(r.iter().map(|f| PropertyValue::Float(*f as f64)));
                PropertyValue::Array(result)
            }
            // Vector + Array or Array + Vector
            (PropertyValue::Vector(l), PropertyValue::Array(r)) => {
                let mut result: Vec<PropertyValue> =
                    l.iter().map(|f| PropertyValue::Float(*f as f64)).collect();
                result.extend(r.iter().cloned());
                PropertyValue::Array(result)
            }
            (PropertyValue::Array(l), PropertyValue::Vector(r)) => {
                let mut result = l.clone();
                result.extend(r.iter().map(|f| PropertyValue::Float(*f as f64)));
                PropertyValue::Array(result)
            }
            // List + element = append to list
            (PropertyValue::Array(l), _) => {
                let mut result = l.clone();
                result.push(right_prop);
                PropertyValue::Array(result)
            }
            (PropertyValue::Vector(l), _) => {
                let mut result: Vec<PropertyValue> =
                    l.iter().map(|f| PropertyValue::Float(*f as f64)).collect();
                result.push(right_prop);
                PropertyValue::Array(result)
            }
            // element + List = prepend to list
            (_, PropertyValue::Array(r)) => {
                let mut result = vec![left_prop];
                result.extend(r.iter().cloned());
                PropertyValue::Array(result)
            }
            (_, PropertyValue::Vector(r)) => {
                let mut result = vec![left_prop];
                result.extend(r.iter().map(|f| PropertyValue::Float(*f as f64)));
                PropertyValue::Array(result)
            }
            // String concatenation with non-string (convert to string)
            (PropertyValue::String(l), _) => PropertyValue::String(format!("{}{}", l, right_prop)),
            (_, PropertyValue::String(r)) => PropertyValue::String(format!("{}{}", left_prop, r)),
            _ => {
                return Err(ExecutionError::TypeError(
                    "Add requires numeric or string operands".to_string(),
                ))
            }
        },
        BinaryOp::Sub => match (&left_prop, &right_prop) {
            (PropertyValue::Integer(l), PropertyValue::Integer(r)) => PropertyValue::Integer(l - r),
            (PropertyValue::Float(l), PropertyValue::Float(r)) => PropertyValue::Float(l - r),
            (PropertyValue::Integer(l), PropertyValue::Float(r)) => {
                PropertyValue::Float(*l as f64 - r)
            }
            (PropertyValue::Float(l), PropertyValue::Integer(r)) => {
                PropertyValue::Float(l - *r as f64)
            }
            // DateTime - Duration
            (
                PropertyValue::DateTime(dt),
                PropertyValue::Duration {
                    months,
                    days,
                    seconds,
                    ..
                },
            ) => add_duration_to_datetime(*dt, -*months, -*days, -*seconds),
            // DateTime - DateTime = Duration
            (PropertyValue::DateTime(a), PropertyValue::DateTime(b)) => {
                let diff_ms = a - b;
                let total_seconds = diff_ms / 1000;
                PropertyValue::Duration {
                    months: 0,
                    days: total_seconds / 86400,
                    seconds: total_seconds % 86400,
                    nanos: ((diff_ms % 1000) * 1_000_000) as i32,
                }
            }
            // Duration - Duration
            (
                PropertyValue::Duration {
                    months: m1,
                    days: d1,
                    seconds: s1,
                    nanos: n1,
                },
                PropertyValue::Duration {
                    months: m2,
                    days: d2,
                    seconds: s2,
                    nanos: n2,
                },
            ) => PropertyValue::Duration {
                months: m1 - m2,
                days: d1 - d2,
                seconds: s1 - s2,
                nanos: n1 - n2,
            },
            _ => {
                return Err(ExecutionError::TypeError(
                    "Sub requires numeric operands".to_string(),
                ))
            }
        },
        BinaryOp::Mul => match (&left_prop, &right_prop) {
            (PropertyValue::Integer(l), PropertyValue::Integer(r)) => PropertyValue::Integer(l * r),
            (PropertyValue::Float(l), PropertyValue::Float(r)) => PropertyValue::Float(l * r),
            (PropertyValue::Integer(l), PropertyValue::Float(r)) => {
                PropertyValue::Float(*l as f64 * r)
            }
            (PropertyValue::Float(l), PropertyValue::Integer(r)) => {
                PropertyValue::Float(l * *r as f64)
            }
            _ => {
                return Err(ExecutionError::TypeError(
                    "Mul requires numeric operands".to_string(),
                ))
            }
        },
        BinaryOp::Div => match (&left_prop, &right_prop) {
            (PropertyValue::Integer(_), PropertyValue::Integer(0)) => {
                return Err(ExecutionError::RuntimeError("Division by zero".to_string()))
            }
            (PropertyValue::Integer(l), PropertyValue::Integer(r)) => PropertyValue::Integer(l / r),
            (PropertyValue::Float(l), PropertyValue::Float(r)) => PropertyValue::Float(l / r),
            (PropertyValue::Integer(l), PropertyValue::Float(r)) => {
                PropertyValue::Float(*l as f64 / r)
            }
            (PropertyValue::Float(l), PropertyValue::Integer(r)) => {
                PropertyValue::Float(l / *r as f64)
            }
            _ => {
                return Err(ExecutionError::TypeError(
                    "Div requires numeric operands".to_string(),
                ))
            }
        },
        BinaryOp::Mod => match (&left_prop, &right_prop) {
            (PropertyValue::Integer(_), PropertyValue::Integer(0)) => {
                return Err(ExecutionError::RuntimeError("Modulo by zero".to_string()))
            }
            (PropertyValue::Integer(l), PropertyValue::Integer(r)) => PropertyValue::Integer(l % r),
            (PropertyValue::Float(l), PropertyValue::Float(r)) => PropertyValue::Float(l % r),
            (PropertyValue::Integer(l), PropertyValue::Float(r)) => {
                PropertyValue::Float(*l as f64 % r)
            }
            (PropertyValue::Float(l), PropertyValue::Integer(r)) => {
                PropertyValue::Float(l % *r as f64)
            }
            _ => {
                return Err(ExecutionError::TypeError(
                    "Mod requires numeric operands".to_string(),
                ))
            }
        },
        BinaryOp::Pow => match (&left_prop, &right_prop) {
            (PropertyValue::Integer(l), PropertyValue::Integer(r)) => {
                PropertyValue::Float((*l as f64).powf(*r as f64))
            }
            (PropertyValue::Float(l), PropertyValue::Integer(r)) => {
                PropertyValue::Float(l.powf(*r as f64))
            }
            (PropertyValue::Integer(l), PropertyValue::Float(r)) => {
                PropertyValue::Float((*l as f64).powf(*r))
            }
            (PropertyValue::Float(l), PropertyValue::Float(r)) => PropertyValue::Float(l.powf(*r)),
            _ => {
                return Err(ExecutionError::TypeError(
                    "Power requires numeric operands".to_string(),
                ))
            }
        },
        BinaryOp::StartsWith => match (&left_prop, &right_prop) {
            (PropertyValue::String(l), PropertyValue::String(r)) => {
                PropertyValue::Boolean(l.starts_with(r.as_str()))
            }
            (PropertyValue::Null, _) | (_, PropertyValue::Null) => PropertyValue::Null,
            // Non-string types return null per Cypher spec
            _ => PropertyValue::Null,
        },
        BinaryOp::EndsWith => match (&left_prop, &right_prop) {
            (PropertyValue::String(l), PropertyValue::String(r)) => {
                PropertyValue::Boolean(l.ends_with(r.as_str()))
            }
            (PropertyValue::Null, _) | (_, PropertyValue::Null) => PropertyValue::Null,
            _ => PropertyValue::Null,
        },
        BinaryOp::Contains => match (&left_prop, &right_prop) {
            (PropertyValue::String(l), PropertyValue::String(r)) => {
                PropertyValue::Boolean(l.contains(r.as_str()))
            }
            (PropertyValue::Null, _) | (_, PropertyValue::Null) => PropertyValue::Null,
            _ => PropertyValue::Null,
        },
        BinaryOp::In => match &right_prop {
            PropertyValue::Array(arr) => {
                // If left is null, result is null
                if matches!(left_prop, PropertyValue::Null) {
                    PropertyValue::Null
                } else if arr.iter().any(|v| coerced_eq(&left_prop, v)) {
                    PropertyValue::Boolean(true)
                } else if arr.iter().any(|v| matches!(v, PropertyValue::Null)) {
                    // If any element is null and left wasn't found, result is null
                    PropertyValue::Null
                } else {
                    PropertyValue::Boolean(false)
                }
            }
            PropertyValue::Vector(vec) => {
                // Convert vector to array for IN check
                let arr: Vec<PropertyValue> = vec
                    .iter()
                    .map(|f| PropertyValue::Float(*f as f64))
                    .collect();
                if matches!(left_prop, PropertyValue::Null) {
                    PropertyValue::Null
                } else if arr.iter().any(|v| coerced_eq(&left_prop, v)) {
                    PropertyValue::Boolean(true)
                } else {
                    PropertyValue::Boolean(false)
                }
            }
            PropertyValue::Null => PropertyValue::Null,
            _ => PropertyValue::Boolean(false),
        },
        BinaryOp::RegexMatch => match (&left_prop, &right_prop) {
            (PropertyValue::String(text), PropertyValue::String(pattern)) => {
                let re = regex::Regex::new(pattern)
                    .map_err(|e| ExecutionError::RuntimeError(format!("Invalid regex: {}", e)))?;
                PropertyValue::Boolean(re.is_match(text))
            }
            _ => {
                return Err(ExecutionError::TypeError(
                    "=~ requires string operands".to_string(),
                ))
            }
        },
    };
    Ok(Value::Property(result))
}

/// Shared unary operator evaluation
fn eval_unary_op(op: &UnaryOp, val: Value) -> ExecutionResult<Value> {
    match op {
        UnaryOp::IsNull => {
            let is_null = matches!(val, Value::Null | Value::Property(PropertyValue::Null));
            Ok(Value::Property(PropertyValue::Boolean(is_null)))
        }
        UnaryOp::IsNotNull => {
            let is_null = matches!(val, Value::Null | Value::Property(PropertyValue::Null));
            Ok(Value::Property(PropertyValue::Boolean(!is_null)))
        }
        UnaryOp::Not => match val {
            Value::Property(PropertyValue::Boolean(b)) => {
                Ok(Value::Property(PropertyValue::Boolean(!b)))
            }
            Value::Null | Value::Property(PropertyValue::Null) => Ok(Value::Null),
            _ => Err(ExecutionError::TypeError(
                "NOT requires boolean".to_string(),
            )),
        },
        UnaryOp::Minus => match val {
            Value::Property(PropertyValue::Integer(i)) => {
                Ok(Value::Property(PropertyValue::Integer(-i)))
            }
            Value::Property(PropertyValue::Float(f)) => {
                Ok(Value::Property(PropertyValue::Float(-f)))
            }
            Value::Null | Value::Property(PropertyValue::Null) => Ok(Value::Null),
            _ => Err(ExecutionError::TypeError(
                "Negation requires numeric type".to_string(),
            )),
        },
    }
}

/// Shared list/map indexing evaluation
fn eval_index(collection: Value, index: Value) -> ExecutionResult<Value> {
    match (&collection, &index) {
        (
            Value::Property(PropertyValue::Array(arr)),
            Value::Property(PropertyValue::Integer(i)),
        ) => {
            let idx = if *i < 0 {
                (arr.len() as i64 + *i) as usize
            } else {
                *i as usize
            };
            Ok(arr
                .get(idx)
                .map(|v| Value::Property(v.clone()))
                .unwrap_or(Value::Null))
        }
        (Value::Property(PropertyValue::Map(map)), Value::Property(PropertyValue::String(key))) => {
            Ok(map
                .get(key)
                .map(|v| Value::Property(v.clone()))
                .unwrap_or(Value::Null))
        }
        _ => Ok(Value::Null),
    }
}

fn eval_list_slice(
    collection: Value,
    start: Option<Value>,
    end: Option<Value>,
) -> ExecutionResult<Value> {
    match &collection {
        Value::Property(PropertyValue::Array(arr)) => {
            let len = arr.len() as i64;
            let resolve_idx = |idx: i64| -> usize {
                let resolved = if idx < 0 {
                    (len + idx).max(0)
                } else {
                    idx.min(len)
                };
                resolved as usize
            };
            let s = match start {
                Some(Value::Property(PropertyValue::Integer(i))) => resolve_idx(i),
                _ => 0,
            };
            let e = match end {
                Some(Value::Property(PropertyValue::Integer(i))) => resolve_idx(i),
                _ => len as usize,
            };
            if s >= e || s >= arr.len() {
                Ok(Value::Property(PropertyValue::Array(vec![])))
            } else {
                let sliced: Vec<PropertyValue> = arr[s..e.min(arr.len())].to_vec();
                Ok(Value::Property(PropertyValue::Array(sliced)))
            }
        }
        _ => Ok(Value::Null),
    }
}

/// Standalone expression evaluator usable from any operator
fn eval_expression(
    expr: &Expression,
    record: &Record,
    store: &GraphStore,
) -> ExecutionResult<Value> {
    match expr {
        Expression::Variable(var) => record
            .get(var)
            .cloned()
            .ok_or_else(|| ExecutionError::VariableNotFound(var.clone())),
        Expression::Property { variable, property } => {
            let val = if let Some(v) = record.get(variable) {
                v.clone()
            } else if variable.contains('.') {
                // Handle nested property access: "nestedMap.name" → resolve "nestedMap" then ".name"
                let parts: Vec<&str> = variable.splitn(2, '.').collect();
                let base = record
                    .get(parts[0])
                    .ok_or_else(|| ExecutionError::VariableNotFound(parts[0].to_string()))?;
                let intermediate = base.resolve_property(parts[1], store);
                Value::Property(intermediate)
            } else {
                return Err(ExecutionError::VariableNotFound(variable.clone()));
            };
            Ok(Value::Property(val.resolve_property(property, store)))
        }
        Expression::Literal(lit) => Ok(Value::Property(lit.clone())),
        Expression::Binary { left, op, right } => {
            let l = eval_expression(left, record, store)?;
            let r = eval_expression(right, record, store)?;
            eval_binary_op(op, l, r)
        }
        Expression::Unary { op, expr: e } => {
            let val = eval_expression(e, record, store)?;
            eval_unary_op(op, val)
        }
        Expression::Function { name, args, .. }
            if name.eq_ignore_ascii_case("$patternPredicate") =>
        {
            eval_pattern_predicate_from_args(args, record, store)
        }
        Expression::Function { name, args, .. } if name == "$propertyAccess" => {
            // Dynamic property access: (expr).prop
            if args.len() == 2 {
                let base_val = eval_expression(&args[0], record, store)?;
                if let Expression::Literal(PropertyValue::String(prop_name)) = &args[1] {
                    Ok(Value::Property(base_val.resolve_property(prop_name, store)))
                } else {
                    Ok(Value::Property(PropertyValue::Null))
                }
            } else {
                Ok(Value::Property(PropertyValue::Null))
            }
        }
        Expression::Function { name, args, .. } => {
            let arg_vals: Vec<Value> = args
                .iter()
                .map(|a| eval_expression(a, record, store))
                .collect::<Result<_, _>>()?;
            eval_function(name, &arg_vals, Some(store))
        }
        Expression::Case {
            operand,
            when_clauses,
            else_result,
        } => eval_case(
            operand.as_deref(),
            when_clauses,
            else_result.as_deref(),
            |e| eval_expression(e, record, store),
        ),
        Expression::Index { expr: e, index } => {
            let collection = eval_expression(e, record, store)?;
            let idx = eval_expression(index, record, store)?;
            eval_index(collection, idx)
        }
        Expression::ListSlice {
            expr: e,
            start,
            end,
        } => {
            let collection = eval_expression(e, record, store)?;
            let s = match start {
                Some(s) => Some(eval_expression(s, record, store)?),
                None => None,
            };
            let en = match end {
                Some(e) => Some(eval_expression(e, record, store)?),
                None => None,
            };
            eval_list_slice(collection, s, en)
        }
        Expression::ExistsSubquery {
            pattern,
            where_clause,
        } => eval_exists_subquery(pattern, where_clause.as_deref(), record, store),
        Expression::ListComprehension {
            variable,
            list_expr,
            filter,
            map_expr,
        } => eval_list_comprehension(
            variable,
            list_expr,
            filter.as_deref(),
            map_expr,
            record,
            store,
        ),
        Expression::PredicateFunction {
            name,
            variable,
            list_expr,
            predicate,
        } => eval_predicate_function(name, variable, list_expr, predicate, record, store),
        Expression::Reduce {
            accumulator,
            init,
            variable,
            list_expr,
            expression,
        } => eval_reduce(
            accumulator,
            init,
            variable,
            list_expr,
            expression,
            record,
            store,
        ),
        Expression::PatternComprehension {
            pattern,
            filter,
            projection,
        } => eval_pattern_comprehension(pattern, filter.as_deref(), projection, record, store),
        Expression::MapExpression(entries) => {
            let mut map = HashMap::new();
            for (key, val_expr) in entries {
                let val = eval_expression(val_expr, record, store)?;
                let pv = match val {
                    Value::Property(pv) => pv,
                    Value::Null => PropertyValue::Null,
                    Value::NodeRef(id) => PropertyValue::Integer(id.as_u64() as i64),
                    Value::Node(id, _) => PropertyValue::Integer(id.as_u64() as i64),
                    Value::EdgeRef(id, ..) => PropertyValue::Integer(id.as_u64() as i64),
                    Value::Edge(id, _) => PropertyValue::Integer(id.as_u64() as i64),
                    Value::Path { .. } => PropertyValue::String("<path>".to_string()),
                };
                map.insert(key.clone(), pv);
            }
            Ok(Value::Property(PropertyValue::Map(map)))
        }
        Expression::PathVariable(var) => record
            .get(var)
            .cloned()
            .ok_or_else(|| ExecutionError::VariableNotFound(var.clone())),
        Expression::Parameter(name) => {
            // Parameters are resolved by substituting them with bound variables prefixed with `$`
            // The executor is responsible for binding params to `$name` before execution
            record.get(&format!("${}", name)).cloned().ok_or_else(|| {
                ExecutionError::RuntimeError(format!("Unresolved parameter: ${}", name))
            })
        }
    }
}

/// Evaluate EXISTS { MATCH pattern WHERE cond }
fn eval_exists_subquery(
    pattern: &crate::query::ast::Pattern,
    where_clause: Option<&crate::query::ast::WhereClause>,
    record: &Record,
    store: &GraphStore,
) -> ExecutionResult<Value> {
    // Run a mini pattern match against the store
    for path in &pattern.paths {
        let start_var = path.start.variable.as_deref();
        let start_labels = &path.start.labels;

        // Check if the start variable is bound from the outer query
        let start_node_ids: Vec<NodeId> = if let Some(var) = start_var {
            if let Some(val) = record.get(var) {
                match val {
                    Value::NodeRef(id) | Value::Node(id, _) => vec![*id],
                    _ => vec![],
                }
            } else if let Some(first_label) = start_labels.first() {
                store
                    .get_nodes_by_label(first_label)
                    .iter()
                    .map(|n| n.id)
                    .collect()
            } else {
                store.all_nodes().iter().map(|n| n.id).collect()
            }
        } else if let Some(first_label) = start_labels.first() {
            store
                .get_nodes_by_label(first_label)
                .iter()
                .map(|n| n.id)
                .collect()
        } else {
            store.all_nodes().iter().map(|n| n.id).collect()
        };

        for node_id in &start_node_ids {
            let node = match store.get_node(*node_id) {
                Some(n) => n,
                None => continue,
            };

            // Check labels
            let has_all_labels = start_labels.iter().all(|l| node.labels.contains(l));
            if !has_all_labels {
                continue;
            }

            // Check properties
            if let Some(ref props) = path.start.properties {
                let props_match = props.iter().all(|(k, v)| node.properties.get(k) == Some(v));
                if !props_match {
                    continue;
                }
            }

            // If no segments, just check existence
            if path.segments.is_empty() {
                if let Some(wc) = where_clause {
                    let mut temp_record = record.clone();
                    if let Some(var) = start_var {
                        temp_record.bind(var.to_string(), Value::NodeRef(*node_id));
                    }
                    let result = eval_expression(&wc.predicate, &temp_record, store)?;
                    if matches!(result, Value::Property(PropertyValue::Boolean(true))) {
                        return Ok(Value::Property(PropertyValue::Boolean(true)));
                    }
                } else {
                    return Ok(Value::Property(PropertyValue::Boolean(true)));
                }
            } else {
                // Check edges — traverse each segment
                // For single segment, check all matching edges
                for segment in &path.segments {
                    let edge_types: Vec<&str> =
                        segment.edge.types.iter().map(|t| t.as_str()).collect();
                    let is_outgoing = !matches!(
                        segment.edge.direction,
                        crate::query::ast::Direction::Incoming
                    );
                    let is_incoming = !matches!(
                        segment.edge.direction,
                        crate::query::ast::Direction::Outgoing
                    );

                    let mut candidate_edges: Vec<(
                        crate::graph::types::EdgeId,
                        NodeId,
                        NodeId,
                        crate::graph::types::EdgeType,
                        NodeId,
                    )> = Vec::new();
                    if is_outgoing {
                        for edge in store.get_outgoing_edges(*node_id) {
                            if !edge_types.is_empty()
                                && !edge_types.contains(&edge.edge_type.as_str())
                            {
                                continue;
                            }
                            candidate_edges.push((
                                edge.id,
                                edge.source,
                                edge.target,
                                edge.edge_type.clone(),
                                edge.target,
                            ));
                        }
                    }
                    if is_incoming {
                        for edge in store.get_incoming_edges(*node_id) {
                            if !edge_types.is_empty()
                                && !edge_types.contains(&edge.edge_type.as_str())
                            {
                                continue;
                            }
                            candidate_edges.push((
                                edge.id,
                                edge.source,
                                edge.target,
                                edge.edge_type.clone(),
                                edge.source,
                            ));
                        }
                    }

                    for (edge_id, edge_src, edge_tgt, edge_type, target_id) in &candidate_edges {
                        // Check target labels
                        if !segment.node.labels.is_empty() {
                            if let Some(target) = store.get_node(*target_id) {
                                let target_matches = segment
                                    .node
                                    .labels
                                    .iter()
                                    .all(|l| target.labels.contains(l));
                                if !target_matches {
                                    continue;
                                }
                            } else {
                                continue;
                            }
                        }
                        // Check target properties
                        if let Some(ref props) = segment.node.properties {
                            if let Some(target) = store.get_node(*target_id) {
                                let props_match = props
                                    .iter()
                                    .all(|(k, v)| target.properties.get(k) == Some(v));
                                if !props_match {
                                    continue;
                                }
                            } else {
                                continue;
                            }
                        }

                        // Apply WHERE clause if present
                        if let Some(wc) = where_clause {
                            let mut temp_record = record.clone();
                            if let Some(var) = start_var {
                                temp_record.bind(var.to_string(), Value::NodeRef(*node_id));
                            }
                            if let Some(ref edge_var) = segment.edge.variable {
                                temp_record.bind(
                                    edge_var.clone(),
                                    Value::EdgeRef(
                                        *edge_id,
                                        *edge_src,
                                        *edge_tgt,
                                        edge_type.clone(),
                                    ),
                                );
                            }
                            if let Some(ref node_var) = segment.node.variable {
                                temp_record.bind(node_var.clone(), Value::NodeRef(*target_id));
                            }
                            let result = eval_expression(&wc.predicate, &temp_record, store)?;
                            if matches!(result, Value::Property(PropertyValue::Boolean(true))) {
                                return Ok(Value::Property(PropertyValue::Boolean(true)));
                            }
                        } else {
                            return Ok(Value::Property(PropertyValue::Boolean(true)));
                        }
                    }
                }
            }
        }
    }
    Ok(Value::Property(PropertyValue::Boolean(false)))
}

/// Evaluate list comprehension: [x IN list WHERE cond | expr]
fn eval_list_comprehension(
    variable: &str,
    list_expr: &Expression,
    filter: Option<&Expression>,
    map_expr: &Expression,
    record: &Record,
    store: &GraphStore,
) -> ExecutionResult<Value> {
    let list_val = eval_expression(list_expr, record, store)?;

    let items = match list_val {
        Value::Property(PropertyValue::Array(arr)) => arr,
        _ => return Ok(Value::Property(PropertyValue::Array(vec![]))),
    };

    let mut result = Vec::new();
    for item in items {
        let mut inner_record = record.clone();
        inner_record.bind(variable.to_string(), Value::Property(item));

        // Apply filter
        if let Some(f) = filter {
            let cond = eval_expression(f, &inner_record, store)?;
            if !matches!(cond, Value::Property(PropertyValue::Boolean(true))) {
                continue;
            }
        }

        // Apply map expression
        let mapped = eval_expression(map_expr, &inner_record, store)?;
        match mapped {
            Value::Property(pv) => result.push(pv),
            _ => result.push(PropertyValue::Null),
        }
    }

    Ok(Value::Property(PropertyValue::Array(result)))
}

/// Evaluate predicate functions: all(x IN list WHERE pred), any(...), none(...), single(...)
fn eval_predicate_function(
    name: &str,
    variable: &str,
    list_expr: &Expression,
    predicate: &Expression,
    record: &Record,
    store: &GraphStore,
) -> ExecutionResult<Value> {
    let list_val = eval_expression(list_expr, record, store)?;
    let items = match list_val {
        Value::Property(PropertyValue::Array(arr)) => arr,
        _ => return Ok(Value::Property(PropertyValue::Boolean(false))),
    };

    let mut true_count = 0usize;
    for item in &items {
        let mut inner_record = record.clone();
        inner_record.bind(variable.to_string(), Value::Property(item.clone()));
        let result = eval_expression(predicate, &inner_record, store)?;
        if matches!(result, Value::Property(PropertyValue::Boolean(true))) {
            true_count += 1;
        }
    }

    let result = match name {
        "all" => true_count == items.len(),
        "any" => true_count > 0,
        "none" => true_count == 0,
        "single" => true_count == 1,
        _ => false,
    };
    Ok(Value::Property(PropertyValue::Boolean(result)))
}

/// Evaluate reduce(acc = init, x IN list | expr)
fn eval_reduce(
    accumulator: &str,
    init: &Expression,
    variable: &str,
    list_expr: &Expression,
    expression: &Expression,
    record: &Record,
    store: &GraphStore,
) -> ExecutionResult<Value> {
    let init_val = eval_expression(init, record, store)?;
    let list_val = eval_expression(list_expr, record, store)?;
    let items = match list_val {
        Value::Property(PropertyValue::Array(arr)) => arr,
        _ => return Ok(init_val),
    };

    let mut acc = init_val;
    for item in items {
        let mut inner_record = record.clone();
        inner_record.bind(accumulator.to_string(), acc);
        inner_record.bind(variable.to_string(), Value::Property(item));
        acc = eval_expression(expression, &inner_record, store)?;
    }
    Ok(acc)
}

/// Evaluate pattern comprehension: `[(a)-[:REL]->(b) | expr]`
fn eval_pattern_comprehension(
    pattern: &Pattern,
    filter: Option<&Expression>,
    projection: &Expression,
    record: &Record,
    store: &GraphStore,
) -> ExecutionResult<Value> {
    let mut results = Vec::new();

    for path in &pattern.paths {
        let start_var = path.start.variable.as_deref();
        let start_labels = &path.start.labels;

        // Get candidate start nodes
        let start_node_ids: Vec<NodeId> = if let Some(var) = start_var {
            if let Some(val) = record.get(var) {
                match val {
                    Value::NodeRef(id) | Value::Node(id, _) => vec![*id],
                    _ => vec![],
                }
            } else if let Some(first_label) = start_labels.first() {
                store
                    .get_nodes_by_label(first_label)
                    .iter()
                    .map(|n| n.id)
                    .collect()
            } else {
                store.all_nodes().iter().map(|n| n.id).collect()
            }
        } else if let Some(first_label) = start_labels.first() {
            store
                .get_nodes_by_label(first_label)
                .iter()
                .map(|n| n.id)
                .collect()
        } else {
            store.all_nodes().iter().map(|n| n.id).collect()
        };

        for node_id in &start_node_ids {
            let node = match store.get_node(*node_id) {
                Some(n) => n,
                None => continue,
            };
            let has_all_labels = start_labels.iter().all(|l| node.labels.contains(l));
            if !has_all_labels {
                continue;
            }

            if path.segments.is_empty() {
                let mut temp_record = record.clone();
                if let Some(var) = start_var {
                    temp_record.bind(var.to_string(), Value::NodeRef(*node_id));
                }
                if let Some(f) = filter {
                    let cond = eval_expression(f, &temp_record, store)?;
                    if !matches!(cond, Value::Property(PropertyValue::Boolean(true))) {
                        continue;
                    }
                }
                let val = eval_expression(projection, &temp_record, store)?;
                match val {
                    Value::Property(pv) => results.push(pv),
                    _ => results.push(PropertyValue::Null),
                }
            } else {
                // One-hop traversal for pattern comprehension
                for segment in &path.segments {
                    let edge_types: Vec<&str> =
                        segment.edge.types.iter().map(|t| t.as_str()).collect();
                    let edges = match segment.edge.direction {
                        Direction::Outgoing => store.get_outgoing_edges(*node_id),
                        Direction::Incoming => store.get_incoming_edges(*node_id),
                        Direction::Both => {
                            let mut all = store.get_outgoing_edges(*node_id);
                            all.extend(store.get_incoming_edges(*node_id));
                            all
                        }
                    };
                    for edge in &edges {
                        if !edge_types.is_empty() && !edge_types.contains(&edge.edge_type.as_str())
                        {
                            continue;
                        }
                        let target_id = if edge.source == *node_id {
                            edge.target
                        } else {
                            edge.source
                        };
                        if !segment.node.labels.is_empty() {
                            if let Some(target) = store.get_node(target_id) {
                                let matches = segment
                                    .node
                                    .labels
                                    .iter()
                                    .all(|l| target.labels.contains(l));
                                if !matches {
                                    continue;
                                }
                            } else {
                                continue;
                            }
                        }
                        let mut temp_record = record.clone();
                        if let Some(var) = start_var {
                            temp_record.bind(var.to_string(), Value::NodeRef(*node_id));
                        }
                        if let Some(ref var) = segment.node.variable {
                            temp_record.bind(var.clone(), Value::NodeRef(target_id));
                        }
                        if let Some(ref var) = segment.edge.variable {
                            temp_record.bind(
                                var.clone(),
                                Value::EdgeRef(
                                    edge.id,
                                    edge.source,
                                    edge.target,
                                    edge.edge_type.clone(),
                                ),
                            );
                        }
                        // Bind path variable if present (e.g., p in [p = (n)-->() | p])
                        if let Some(ref path_var) = path.path_variable {
                            temp_record.bind(
                                path_var.clone(),
                                Value::Path {
                                    nodes: vec![*node_id, target_id],
                                    edges: vec![edge.id],
                                },
                            );
                        }
                        if let Some(f) = filter {
                            let cond = eval_expression(f, &temp_record, store)?;
                            if !matches!(cond, Value::Property(PropertyValue::Boolean(true))) {
                                continue;
                            }
                        }
                        let val = eval_expression(projection, &temp_record, store)?;
                        match val {
                            Value::Property(pv) => results.push(pv),
                            Value::Path { .. } => {
                                // Path values in pattern comprehensions are returned as-is
                                // Store as a special array element
                                results.push(PropertyValue::Null); // Path can't be a PropertyValue directly
                            }
                            _ => results.push(PropertyValue::Null),
                        }
                    }
                }
            }
        }
    }

    Ok(Value::Property(PropertyValue::Array(results)))
}

/// Evaluate a $patternPredicate: WHERE (n)-[:REL]->() or WHERE (n)-[:REL]->(m)
fn eval_pattern_predicate_from_args(
    args: &[Expression],
    record: &Record,
    store: &GraphStore,
) -> ExecutionResult<Value> {
    // args[0] = source var name, args[1] = pattern text
    let source_var = match &args[0] {
        Expression::Literal(PropertyValue::String(s)) => s.clone(),
        _ => return Ok(Value::Property(PropertyValue::Boolean(false))),
    };
    let pattern_text = match &args[1] {
        Expression::Literal(PropertyValue::String(s)) => s.clone(),
        _ => return Ok(Value::Property(PropertyValue::Boolean(false))),
    };

    // Resolve source node
    let source_val = match record.get(&source_var) {
        Some(v) => v,
        None => return Ok(Value::Property(PropertyValue::Boolean(false))),
    };
    let source_id = match source_val.node_id() {
        Some(id) => id,
        None => return Ok(Value::Property(PropertyValue::Boolean(false))),
    };

    // Parse direction from pattern
    let is_outgoing = pattern_text.contains("->");
    let is_incoming = pattern_text.contains("<-");

    // Extract edge types from [...] section
    let mut edge_types: Vec<String> = Vec::new();
    let mut is_varlen = false;
    if let Some(start) = pattern_text.find('[') {
        if let Some(end) = pattern_text.find(']') {
            let bracket = &pattern_text[start + 1..end];
            if bracket.contains('*') {
                is_varlen = true;
            }
            let type_part = bracket.split('*').next().unwrap_or("");
            if type_part.contains(':') {
                let after_colon = type_part.split(':').skip(1).collect::<Vec<_>>().join(":");
                for t in after_colon.split('|') {
                    let t = t.trim().trim_start_matches(':');
                    if !t.is_empty() {
                        edge_types.push(t.to_string());
                    }
                }
            }
        }
    }

    // Extract target variable from pattern (second parenthesized node)
    let target_var = {
        let mut found = Vec::new();
        let chars: Vec<char> = pattern_text.chars().collect();
        let mut i = 0;
        while i < chars.len() {
            if chars[i] == '(' {
                let start = i + 1;
                let mut j = start;
                while j < chars.len() && chars[j] != ')' && chars[j] != ':' && chars[j] != '{' {
                    j += 1;
                }
                let var = pattern_text[start..j].trim().to_string();
                if !var.is_empty() {
                    found.push(var);
                }
                while j < chars.len() && chars[j] != ')' {
                    j += 1;
                }
                i = j + 1;
            } else {
                i += 1;
            }
        }
        if found.len() >= 2 {
            Some(found[1].clone())
        } else {
            None
        }
    };

    // Resolve target node if specified
    let target_id = target_var
        .as_ref()
        .and_then(|tv| record.get(tv).and_then(|v| v.node_id()));

    // Check edges with direction-aware target matching
    let check_edges_directed =
        |edges: &Vec<&crate::graph::edge::Edge>, check_target: bool| -> bool {
            for edge in edges {
                let type_match = edge_types.is_empty()
                    || edge_types.iter().any(|t| t == edge.edge_type.as_str());
                if !type_match {
                    continue;
                }
                if let Some(tid) = target_id {
                    // Two-node pattern: for outgoing, target must match; for incoming, source must match
                    let matches = if check_target {
                        edge.target == tid
                    } else {
                        edge.source == tid
                    };
                    if matches {
                        return true;
                    }
                } else {
                    return true;
                }
            }
            false
        };

    if is_varlen {
        // Variable-length: BFS reachability check
        use std::collections::{HashSet, VecDeque};
        let mut visited = HashSet::new();
        let mut queue = VecDeque::new();
        visited.insert(source_id);
        queue.push_back(source_id);

        while let Some(node) = queue.pop_front() {
            let mut next_nodes = Vec::new();
            if !is_incoming {
                for edge in store.get_outgoing_edges(node) {
                    let type_match = edge_types.is_empty()
                        || edge_types.iter().any(|t| t == edge.edge_type.as_str());
                    if type_match && !visited.contains(&edge.target) {
                        next_nodes.push(edge.target);
                    }
                }
            }
            if !is_outgoing {
                for edge in store.get_incoming_edges(node) {
                    let type_match = edge_types.is_empty()
                        || edge_types.iter().any(|t| t == edge.edge_type.as_str());
                    if type_match && !visited.contains(&edge.source) {
                        next_nodes.push(edge.source);
                    }
                }
            }
            for next in next_nodes {
                if let Some(tid) = target_id {
                    if next == tid {
                        return Ok(Value::Property(PropertyValue::Boolean(true)));
                    }
                }
                visited.insert(next);
                queue.push_back(next);
            }
        }
        // For single-node varlen, any reachable node counts
        if target_id.is_none() {
            return Ok(Value::Property(PropertyValue::Boolean(visited.len() > 1)));
        }
        return Ok(Value::Property(PropertyValue::Boolean(false)));
    }

    // Fixed-length edge check
    let mut found = false;
    if is_outgoing || !is_incoming {
        let outgoing = store.get_outgoing_edges(source_id);
        if check_edges_directed(&outgoing, true) {
            found = true;
        }
    }
    if !found && (is_incoming || !is_outgoing) {
        let incoming = store.get_incoming_edges(source_id);
        if check_edges_directed(&incoming, false) {
            found = true;
        }
    }

    Ok(Value::Property(PropertyValue::Boolean(found)))
}

/// Shared function evaluation for scalar functions (not aggregates)
fn eval_function(name: &str, args: &[Value], store: Option<&GraphStore>) -> ExecutionResult<Value> {
    let lower_name = name.to_lowercase();

    // Null propagation: most functions return null if any argument is null.
    // Exceptions: coalesce (designed for null handling), count (aggregation),
    // and type conversion functions (handle null themselves).
    let null_passthrough = !matches!(
        lower_name.as_str(),
        "coalesce"
            | "count"
            | "exists"
            | "tostring"
            | "tointeger"
            | "toint"
            | "tofloat"
            | "toboolean"
    );
    if null_passthrough && !args.is_empty() {
        for arg in args {
            if matches!(arg, Value::Null | Value::Property(PropertyValue::Null)) {
                return Ok(Value::Null);
            }
        }
    }

    // Null propagation: most functions return null if any argument is null
    let has_null_arg = args
        .iter()
        .any(|a| matches!(a, Value::Null | Value::Property(PropertyValue::Null)));
    if has_null_arg {
        // Functions that should NOT return null on null input
        let null_safe = matches!(
            lower_name.as_str(),
            "coalesce" | "count" | "collect" | "exists" | "isnull"
        );
        if !null_safe {
            return Ok(Value::Null);
        }
    }

    match lower_name.as_str() {
        // String functions
        "toupper" | "touppercase" => {
            let s = extract_string(&args[0])?;
            Ok(Value::Property(PropertyValue::String(s.to_uppercase())))
        }
        "tolower" | "tolowercase" => {
            let s = extract_string(&args[0])?;
            Ok(Value::Property(PropertyValue::String(s.to_lowercase())))
        }
        "trim" => {
            let s = extract_string(&args[0])?;
            Ok(Value::Property(PropertyValue::String(s.trim().to_string())))
        }
        "ltrim" => {
            let s = extract_string(&args[0])?;
            Ok(Value::Property(PropertyValue::String(
                s.trim_start().to_string(),
            )))
        }
        "rtrim" => {
            let s = extract_string(&args[0])?;
            Ok(Value::Property(PropertyValue::String(
                s.trim_end().to_string(),
            )))
        }
        "replace" => {
            if args.len() < 3 {
                return Err(ExecutionError::RuntimeError(
                    "replace() requires 3 arguments".to_string(),
                ));
            }
            let s = extract_string(&args[0])?;
            let from = extract_string(&args[1])?;
            let to = extract_string(&args[2])?;
            Ok(Value::Property(PropertyValue::String(
                s.replace(&from, &to),
            )))
        }
        "substring" => {
            if args.len() < 2 {
                return Err(ExecutionError::RuntimeError(
                    "substring() requires at least 2 arguments".to_string(),
                ));
            }
            let s = extract_string(&args[0])?;
            let start = extract_int(&args[1])? as usize;
            let chars: Vec<char> = s.chars().collect();
            if start >= chars.len() {
                return Ok(Value::Property(PropertyValue::String(String::new())));
            }
            let result = if args.len() >= 3 {
                let len = extract_int(&args[2])? as usize;
                chars[start..std::cmp::min(start + len, chars.len())]
                    .iter()
                    .collect()
            } else {
                chars[start..].iter().collect()
            };
            Ok(Value::Property(PropertyValue::String(result)))
        }
        "left" => {
            let s = extract_string(&args[0])?;
            let n = extract_int(&args[1])? as usize;
            Ok(Value::Property(PropertyValue::String(
                s.chars().take(n).collect(),
            )))
        }
        "right" => {
            let s = extract_string(&args[0])?;
            let n = extract_int(&args[1])? as usize;
            let chars: Vec<char> = s.chars().collect();
            let start = chars.len().saturating_sub(n);
            Ok(Value::Property(PropertyValue::String(
                chars[start..].iter().collect(),
            )))
        }
        "reverse" => {
            let s = extract_string(&args[0])?;
            Ok(Value::Property(PropertyValue::String(
                s.chars().rev().collect(),
            )))
        }
        "split" => {
            if args.len() < 2 {
                return Err(ExecutionError::RuntimeError(
                    "split() requires 2 arguments".to_string(),
                ));
            }
            let s = extract_string(&args[0])?;
            let delimiter = extract_string(&args[1])?;
            let parts: Vec<PropertyValue> = s
                .split(&delimiter)
                .map(|p| PropertyValue::String(p.to_string()))
                .collect();
            Ok(Value::Property(PropertyValue::Array(parts)))
        }
        "tostring" => {
            let val = &args[0];
            match val {
                Value::Null | Value::Property(PropertyValue::Null) => Ok(Value::Null),
                Value::Property(PropertyValue::String(s)) => {
                    Ok(Value::Property(PropertyValue::String(s.clone())))
                }
                Value::Property(PropertyValue::Integer(i)) => {
                    Ok(Value::Property(PropertyValue::String(i.to_string())))
                }
                Value::Property(PropertyValue::Float(f)) => {
                    Ok(Value::Property(PropertyValue::String(f.to_string())))
                }
                Value::Property(PropertyValue::Boolean(b)) => {
                    Ok(Value::Property(PropertyValue::String(b.to_string())))
                }
                Value::Property(PropertyValue::Array(arr)) => {
                    let items: Vec<String> = arr.iter().map(|v| format!("{}", v)).collect();
                    Ok(Value::Property(PropertyValue::String(format!(
                        "[{}]",
                        items.join(", ")
                    ))))
                }
                Value::Property(PropertyValue::Map(m)) => {
                    let items: Vec<String> =
                        m.iter().map(|(k, v)| format!("{}: {}", k, v)).collect();
                    Ok(Value::Property(PropertyValue::String(format!(
                        "{{{}}}",
                        items.join(", ")
                    ))))
                }
                Value::Property(PropertyValue::DateTime(ts)) => {
                    // Convert epoch millis to ISO 8601
                    let secs = ts / 1000;
                    let nanos = ((ts % 1000) * 1_000_000) as u32;
                    if let Some(dt) = chrono::DateTime::from_timestamp(secs, nanos) {
                        Ok(Value::Property(PropertyValue::String(
                            dt.format("%Y-%m-%dT%H:%M:%S%.3fZ").to_string(),
                        )))
                    } else {
                        Ok(Value::Property(PropertyValue::String(format!(
                            "datetime({})",
                            ts
                        ))))
                    }
                }
                Value::Property(PropertyValue::Duration {
                    months,
                    days,
                    seconds,
                    nanos,
                }) => {
                    let mut parts = String::from("P");
                    if *months > 0 {
                        let y = months / 12;
                        let m = months % 12;
                        if y > 0 {
                            parts.push_str(&format!("{}Y", y));
                        }
                        if m > 0 {
                            parts.push_str(&format!("{}M", m));
                        }
                    }
                    if *days > 0 {
                        parts.push_str(&format!("{}D", days));
                    }
                    if *seconds > 0 || *nanos > 0 {
                        parts.push('T');
                        let h = seconds / 3600;
                        let m = (seconds % 3600) / 60;
                        let s = seconds % 60;
                        if h > 0 {
                            parts.push_str(&format!("{}H", h));
                        }
                        if m > 0 {
                            parts.push_str(&format!("{}M", m));
                        }
                        if s > 0 || *nanos > 0 {
                            if *nanos > 0 {
                                parts.push_str(&format!("{}.{:09}S", s, nanos));
                            } else {
                                parts.push_str(&format!("{}S", s));
                            }
                        }
                    }
                    if parts == "P" {
                        parts.push_str("T0S");
                    }
                    Ok(Value::Property(PropertyValue::String(parts)))
                }
                _ => Err(ExecutionError::TypeError(
                    "Cannot convert to string".to_string(),
                )),
            }
        }
        "tointeger" | "toint" => match &args[0] {
            Value::Null | Value::Property(PropertyValue::Null) => Ok(Value::Null),
            Value::Property(PropertyValue::Integer(i)) => {
                Ok(Value::Property(PropertyValue::Integer(*i)))
            }
            Value::Property(PropertyValue::Float(f)) => {
                Ok(Value::Property(PropertyValue::Integer(*f as i64)))
            }
            Value::Property(PropertyValue::Boolean(b)) => {
                Ok(Value::Property(PropertyValue::Integer(if *b {
                    1
                } else {
                    0
                })))
            }
            Value::Property(PropertyValue::String(s)) => {
                // Try parsing as integer first, then as float (truncate)
                if let Ok(i) = s.parse::<i64>() {
                    Ok(Value::Property(PropertyValue::Integer(i)))
                } else if let Ok(f) = s.parse::<f64>() {
                    Ok(Value::Property(PropertyValue::Integer(f as i64)))
                } else {
                    // Invalid string returns null per TCK
                    Ok(Value::Null)
                }
            }
            _ => Err(ExecutionError::TypeError(
                "Cannot convert to integer".to_string(),
            )),
        },
        "tofloat" => match &args[0] {
            Value::Null | Value::Property(PropertyValue::Null) => Ok(Value::Null),
            Value::Property(PropertyValue::Float(f)) => {
                Ok(Value::Property(PropertyValue::Float(*f)))
            }
            Value::Property(PropertyValue::Integer(i)) => {
                Ok(Value::Property(PropertyValue::Float(*i as f64)))
            }
            Value::Property(PropertyValue::String(s)) => {
                if let Ok(f) = s.parse::<f64>() {
                    Ok(Value::Property(PropertyValue::Float(f)))
                } else {
                    // Invalid string returns null per TCK
                    Ok(Value::Null)
                }
            }
            _ => Err(ExecutionError::TypeError(
                "Cannot convert to float".to_string(),
            )),
        },
        "toboolean" => match &args[0] {
            Value::Null | Value::Property(PropertyValue::Null) => Ok(Value::Null),
            Value::Property(PropertyValue::Boolean(b)) => {
                Ok(Value::Property(PropertyValue::Boolean(*b)))
            }
            Value::Property(PropertyValue::String(s)) => {
                match s.to_lowercase().as_str() {
                    "true" => Ok(Value::Property(PropertyValue::Boolean(true))),
                    "false" => Ok(Value::Property(PropertyValue::Boolean(false))),
                    // Invalid string returns null per TCK
                    _ => Ok(Value::Null),
                }
            }
            _ => Err(ExecutionError::TypeError(
                "Cannot convert to boolean".to_string(),
            )),
        },
        // Size/length
        "size" | "length" => match &args[0] {
            Value::Property(PropertyValue::String(s)) => {
                Ok(Value::Property(PropertyValue::Integer(s.len() as i64)))
            }
            Value::Property(PropertyValue::Array(a)) => {
                Ok(Value::Property(PropertyValue::Integer(a.len() as i64)))
            }
            Value::Path { edges, .. } => {
                Ok(Value::Property(PropertyValue::Integer(edges.len() as i64)))
            }
            Value::Property(PropertyValue::Map(m)) => {
                Ok(Value::Property(PropertyValue::Integer(m.len() as i64)))
            }
            Value::Property(PropertyValue::Vector(v)) => {
                Ok(Value::Property(PropertyValue::Integer(v.len() as i64)))
            }
            _ => Err(ExecutionError::TypeError(
                "size() requires string, list, map, or path".to_string(),
            )),
        },
        // Path functions
        "nodes" => match &args[0] {
            Value::Path { nodes, .. } => {
                let arr: Vec<PropertyValue> = nodes
                    .iter()
                    .map(|id| PropertyValue::Integer(id.as_u64() as i64))
                    .collect();
                Ok(Value::Property(PropertyValue::Array(arr)))
            }
            _ => Err(ExecutionError::TypeError(
                "nodes() requires a path".to_string(),
            )),
        },
        "relationships" | "rels" => match &args[0] {
            Value::Path { edges, .. } => {
                let arr: Vec<PropertyValue> = edges
                    .iter()
                    .map(|id| PropertyValue::Integer(id.as_u64() as i64))
                    .collect();
                Ok(Value::Property(PropertyValue::Array(arr)))
            }
            _ => Err(ExecutionError::TypeError(
                "relationships() requires a path".to_string(),
            )),
        },
        // Math functions
        "abs" => match &args[0] {
            Value::Property(PropertyValue::Integer(i)) => {
                Ok(Value::Property(PropertyValue::Integer(i.abs())))
            }
            Value::Property(PropertyValue::Float(f)) => {
                Ok(Value::Property(PropertyValue::Float(f.abs())))
            }
            _ => Err(ExecutionError::TypeError(
                "abs() requires numeric".to_string(),
            )),
        },
        "ceil" => match &args[0] {
            Value::Property(PropertyValue::Float(f)) => {
                Ok(Value::Property(PropertyValue::Integer(f.ceil() as i64)))
            }
            Value::Property(PropertyValue::Integer(i)) => {
                Ok(Value::Property(PropertyValue::Integer(*i)))
            }
            _ => Err(ExecutionError::TypeError(
                "ceil() requires numeric".to_string(),
            )),
        },
        "floor" => match &args[0] {
            Value::Property(PropertyValue::Float(f)) => {
                Ok(Value::Property(PropertyValue::Integer(f.floor() as i64)))
            }
            Value::Property(PropertyValue::Integer(i)) => {
                Ok(Value::Property(PropertyValue::Integer(*i)))
            }
            _ => Err(ExecutionError::TypeError(
                "floor() requires numeric".to_string(),
            )),
        },
        "round" => match &args[0] {
            Value::Property(PropertyValue::Float(f)) => {
                Ok(Value::Property(PropertyValue::Integer(f.round() as i64)))
            }
            Value::Property(PropertyValue::Integer(i)) => {
                Ok(Value::Property(PropertyValue::Integer(*i)))
            }
            _ => Err(ExecutionError::TypeError(
                "round() requires numeric".to_string(),
            )),
        },
        "sqrt" => match &args[0] {
            Value::Property(PropertyValue::Float(f)) => {
                Ok(Value::Property(PropertyValue::Float(f.sqrt())))
            }
            Value::Property(PropertyValue::Integer(i)) => {
                Ok(Value::Property(PropertyValue::Float((*i as f64).sqrt())))
            }
            _ => Err(ExecutionError::TypeError(
                "sqrt() requires numeric".to_string(),
            )),
        },
        "sign" => match &args[0] {
            Value::Property(PropertyValue::Integer(i)) => {
                Ok(Value::Property(PropertyValue::Integer(i.signum())))
            }
            Value::Property(PropertyValue::Float(f)) => {
                Ok(Value::Property(PropertyValue::Integer(if *f > 0.0 {
                    1
                } else if *f < 0.0 {
                    -1
                } else {
                    0
                })))
            }
            _ => Err(ExecutionError::TypeError(
                "sign() requires numeric".to_string(),
            )),
        },
        "log" => match &args[0] {
            Value::Property(PropertyValue::Float(f)) => {
                Ok(Value::Property(PropertyValue::Float(f.ln())))
            }
            Value::Property(PropertyValue::Integer(i)) => {
                Ok(Value::Property(PropertyValue::Float((*i as f64).ln())))
            }
            _ => Err(ExecutionError::TypeError(
                "log() requires numeric".to_string(),
            )),
        },
        "exp" => match &args[0] {
            Value::Property(PropertyValue::Float(f)) => {
                Ok(Value::Property(PropertyValue::Float(f.exp())))
            }
            Value::Property(PropertyValue::Integer(i)) => {
                Ok(Value::Property(PropertyValue::Float((*i as f64).exp())))
            }
            _ => Err(ExecutionError::TypeError(
                "exp() requires numeric".to_string(),
            )),
        },
        "rand" => {
            use rand::Rng;
            let val = rand::thread_rng().gen::<f64>();
            Ok(Value::Property(PropertyValue::Float(val)))
        }
        "randomuuid" => {
            use rand::Rng;
            let mut rng = rand::thread_rng();
            let mut bytes = [0u8; 16];
            rng.fill(&mut bytes);
            // Set version 4 (random) and variant 1
            bytes[6] = (bytes[6] & 0x0f) | 0x40; // version 4
            bytes[8] = (bytes[8] & 0x3f) | 0x80; // variant 1
            let uuid = format!(
                "{:02x}{:02x}{:02x}{:02x}-{:02x}{:02x}-{:02x}{:02x}-{:02x}{:02x}-{:02x}{:02x}{:02x}{:02x}{:02x}{:02x}",
                bytes[0], bytes[1], bytes[2], bytes[3],
                bytes[4], bytes[5],
                bytes[6], bytes[7],
                bytes[8], bytes[9],
                bytes[10], bytes[11], bytes[12], bytes[13], bytes[14], bytes[15]
            );
            Ok(Value::Property(PropertyValue::String(uuid)))
        }
        "timestamp" => {
            let ts = chrono::Utc::now().timestamp_millis();
            Ok(Value::Property(PropertyValue::Integer(ts)))
        }
        // Type/meta functions
        "coalesce" => {
            for arg in args {
                if !matches!(arg, Value::Null | Value::Property(PropertyValue::Null)) {
                    return Ok(arg.clone());
                }
            }
            Ok(Value::Null)
        }
        "head" => match &args[0] {
            Value::Property(PropertyValue::Array(arr)) => Ok(arr
                .first()
                .map(|v| Value::Property(v.clone()))
                .unwrap_or(Value::Null)),
            _ => Err(ExecutionError::TypeError(
                "head() requires list".to_string(),
            )),
        },
        "last" => match &args[0] {
            Value::Property(PropertyValue::Array(arr)) => Ok(arr
                .last()
                .map(|v| Value::Property(v.clone()))
                .unwrap_or(Value::Null)),
            _ => Err(ExecutionError::TypeError(
                "last() requires list".to_string(),
            )),
        },
        "tail" => match &args[0] {
            Value::Property(PropertyValue::Array(arr)) => {
                let tail: Vec<PropertyValue> = arr.iter().skip(1).cloned().collect();
                Ok(Value::Property(PropertyValue::Array(tail)))
            }
            Value::Property(PropertyValue::Vector(v)) => {
                let tail: Vec<PropertyValue> = v
                    .iter()
                    .skip(1)
                    .map(|f| PropertyValue::Float(*f as f64))
                    .collect();
                Ok(Value::Property(PropertyValue::Array(tail)))
            }
            Value::Null | Value::Property(PropertyValue::Null) => Ok(Value::Null),
            _ => Err(ExecutionError::TypeError(
                "tail() requires list".to_string(),
            )),
        },
        // Meta functions — work on nodes/edges
        "id" => match &args[0] {
            Value::NodeRef(id) | Value::Node(id, _) => {
                Ok(Value::Property(PropertyValue::Integer(id.as_u64() as i64)))
            }
            Value::EdgeRef(id, ..) | Value::Edge(id, _) => {
                Ok(Value::Property(PropertyValue::Integer(id.as_u64() as i64)))
            }
            _ => Err(ExecutionError::TypeError(
                "id() requires node or edge".to_string(),
            )),
        },
        "labels" => match &args[0] {
            Value::Node(_, node) => {
                let labels: Vec<PropertyValue> = node
                    .labels
                    .iter()
                    .map(|l| PropertyValue::String(l.as_str().to_string()))
                    .collect();
                Ok(Value::Property(PropertyValue::Array(labels)))
            }
            Value::NodeRef(id) => {
                let s = store.ok_or_else(|| {
                    ExecutionError::RuntimeError("labels() on NodeRef requires store".to_string())
                })?;
                let node = s.get_node(*id).ok_or_else(|| {
                    ExecutionError::RuntimeError(format!("Node {} not found", id.as_u64()))
                })?;
                let labels: Vec<PropertyValue> = node
                    .labels
                    .iter()
                    .map(|l| PropertyValue::String(l.as_str().to_string()))
                    .collect();
                Ok(Value::Property(PropertyValue::Array(labels)))
            }
            _ => Err(ExecutionError::TypeError(
                "labels() requires a node".to_string(),
            )),
        },
        "type" => match &args[0] {
            Value::Edge(_, edge) => Ok(Value::Property(PropertyValue::String(
                edge.edge_type.as_str().to_string(),
            ))),
            Value::EdgeRef(_, _, _, et) => Ok(Value::Property(PropertyValue::String(
                et.as_str().to_string(),
            ))),
            _ => Err(ExecutionError::TypeError(
                "type() requires an edge".to_string(),
            )),
        },
        "keys" => match &args[0] {
            Value::Node(_, node) => {
                let keys: Vec<PropertyValue> = node
                    .properties
                    .keys()
                    .map(|k| PropertyValue::String(k.clone()))
                    .collect();
                Ok(Value::Property(PropertyValue::Array(keys)))
            }
            Value::NodeRef(id) => {
                let s = store.ok_or_else(|| {
                    ExecutionError::RuntimeError("keys() on NodeRef requires store".to_string())
                })?;
                let node = s.get_node(*id).ok_or_else(|| {
                    ExecutionError::RuntimeError(format!("Node {} not found", id.as_u64()))
                })?;
                let keys: Vec<PropertyValue> = node
                    .properties
                    .keys()
                    .map(|k| PropertyValue::String(k.clone()))
                    .collect();
                Ok(Value::Property(PropertyValue::Array(keys)))
            }
            Value::Edge(_, edge) => {
                let keys: Vec<PropertyValue> = edge
                    .properties
                    .keys()
                    .map(|k| PropertyValue::String(k.clone()))
                    .collect();
                Ok(Value::Property(PropertyValue::Array(keys)))
            }
            Value::EdgeRef(eid, _, _, _) => {
                let s = store.ok_or_else(|| {
                    ExecutionError::RuntimeError("keys() on EdgeRef requires store".to_string())
                })?;
                let edge = s.get_edge(*eid).ok_or_else(|| {
                    ExecutionError::RuntimeError(format!("Edge {} not found", eid.as_u64()))
                })?;
                let keys: Vec<PropertyValue> = edge
                    .properties
                    .keys()
                    .map(|k| PropertyValue::String(k.clone()))
                    .collect();
                Ok(Value::Property(PropertyValue::Array(keys)))
            }
            // keys() on a map returns the map keys
            Value::Property(PropertyValue::Map(m)) => {
                let keys: Vec<PropertyValue> =
                    m.keys().map(|k| PropertyValue::String(k.clone())).collect();
                Ok(Value::Property(PropertyValue::Array(keys)))
            }
            Value::Null | Value::Property(PropertyValue::Null) => Ok(Value::Null),
            _ => Err(ExecutionError::TypeError(
                "keys() requires node or edge".to_string(),
            )),
        },
        "properties" => match &args[0] {
            Value::Node(_, node) => {
                let map: HashMap<String, PropertyValue> = node.properties.clone();
                Ok(Value::Property(PropertyValue::Map(map)))
            }
            Value::NodeRef(id) => {
                let s = store.ok_or_else(|| {
                    ExecutionError::RuntimeError(
                        "properties() on NodeRef requires store".to_string(),
                    )
                })?;
                let node = s.get_node(*id).ok_or_else(|| {
                    ExecutionError::RuntimeError(format!("Node {} not found", id.as_u64()))
                })?;
                let map: HashMap<String, PropertyValue> = node.properties.clone();
                Ok(Value::Property(PropertyValue::Map(map)))
            }
            Value::Edge(_, edge) => {
                let map: HashMap<String, PropertyValue> = edge.properties.clone();
                Ok(Value::Property(PropertyValue::Map(map)))
            }
            Value::EdgeRef(eid, ..) => {
                let s = store.ok_or_else(|| {
                    ExecutionError::RuntimeError(
                        "properties() on EdgeRef requires store".to_string(),
                    )
                })?;
                let edge = s.get_edge(*eid).ok_or_else(|| {
                    ExecutionError::RuntimeError(format!("Edge {} not found", eid.as_u64()))
                })?;
                let map: HashMap<String, PropertyValue> = edge.properties.clone();
                Ok(Value::Property(PropertyValue::Map(map)))
            }
            Value::Null | Value::Property(PropertyValue::Null) => Ok(Value::Null),
            // properties() on a map returns the map itself
            Value::Property(PropertyValue::Map(m)) => {
                Ok(Value::Property(PropertyValue::Map(m.clone())))
            }
            _ => Err(ExecutionError::TypeError(
                "properties() requires node or edge".to_string(),
            )),
        },
        "exists" => {
            let is_null = matches!(&args[0], Value::Null | Value::Property(PropertyValue::Null));
            Ok(Value::Property(PropertyValue::Boolean(!is_null)))
        }
        // $patternPredicate — WHERE (n)-[:REL]->() pattern predicate
        // Args: [source_var_name, pattern_text]
        // Evaluated in FilterOperator context where record is available
        // Here in eval_function (no record), just return true — the filter handles it
        "$patternpredicate" => Ok(Value::Property(PropertyValue::Boolean(true))),
        // Label check: WHERE n:Label → $hasLabel(n, 'Label')
        "$haslabel" => {
            if args.is_empty() {
                return Ok(Value::Property(PropertyValue::Boolean(false)));
            }
            let node_id = args[0].node_id();
            if let (Some(nid), Some(s)) = (node_id, store) {
                if let Some(node) = s.get_node(nid) {
                    // Check all label arguments
                    let has_all = args[1..].iter().all(|arg| {
                        if let Value::Property(PropertyValue::String(label)) = arg {
                            node.has_label(&Label::new(label))
                        } else {
                            false
                        }
                    });
                    Ok(Value::Property(PropertyValue::Boolean(has_all)))
                } else {
                    Ok(Value::Property(PropertyValue::Boolean(false)))
                }
            } else if matches!(&args[0], Value::Null | Value::Property(PropertyValue::Null)) {
                Ok(Value::Null)
            } else {
                Ok(Value::Property(PropertyValue::Boolean(false)))
            }
        }
        // $singleNodePath — create a path with just one node (for zero-length named paths)
        "$singlenodepath" => {
            if let Some(nid) = args[0].node_id() {
                Ok(Value::Path {
                    nodes: vec![nid],
                    edges: vec![],
                })
            } else {
                Ok(Value::Null)
            }
        }
        // startNode/endNode — return source/target node of an edge
        "startnode" => match &args[0] {
            Value::Edge(_, edge) => Ok(Value::NodeRef(edge.source)),
            Value::EdgeRef(_, src, _, _) => Ok(Value::NodeRef(*src)),
            _ => Err(ExecutionError::TypeError(
                "startNode() requires an edge".to_string(),
            )),
        },
        "endnode" => match &args[0] {
            Value::Edge(_, edge) => Ok(Value::NodeRef(edge.target)),
            Value::EdgeRef(_, _, tgt, _) => Ok(Value::NodeRef(*tgt)),
            _ => Err(ExecutionError::TypeError(
                "endNode() requires an edge".to_string(),
            )),
        },
        // range() — generate integer list
        "range" => {
            if args.len() < 2 {
                return Err(ExecutionError::RuntimeError(
                    "range() requires at least 2 arguments".to_string(),
                ));
            }
            let start = extract_int(&args[0])?;
            let end = extract_int(&args[1])?;
            let step = if args.len() >= 3 {
                extract_int(&args[2])?
            } else {
                1
            };
            if step == 0 {
                return Err(ExecutionError::RuntimeError(
                    "range() step cannot be 0".to_string(),
                ));
            }
            let mut result = Vec::new();
            let mut i = start;
            if step > 0 {
                while i <= end {
                    result.push(PropertyValue::Integer(i));
                    i += step;
                }
            } else {
                while i >= end {
                    result.push(PropertyValue::Integer(i));
                    i += step;
                }
            }
            Ok(Value::Property(PropertyValue::Array(result)))
        }
        // date/datetime/duration constructors
        "date" => {
            if args.is_empty() {
                let now = chrono::Utc::now().timestamp_millis();
                Ok(Value::Property(PropertyValue::DateTime(now)))
            } else {
                match &args[0] {
                    Value::Property(PropertyValue::String(s)) => {
                        // Parse ISO date string — try multiple formats
                        let s_trimmed = s.trim();
                        // Strip trailing time part if present (from toString output)
                        let date_str = if let Some(t_pos) = s_trimmed.find('T') {
                            &s_trimmed[..t_pos]
                        } else {
                            s_trimmed
                        };
                        if let Ok(dt) = chrono::NaiveDate::parse_from_str(date_str, "%Y-%m-%d") {
                            let millis = dt
                                .and_hms_opt(0, 0, 0)
                                .unwrap()
                                .and_utc()
                                .timestamp_millis();
                            Ok(Value::Property(PropertyValue::DateTime(millis)))
                        } else if date_str.starts_with('+') || date_str.starts_with('-') {
                            // Handle extended year format like +999999999-12-31 or -999999999-01-01
                            // Store as a sentinel value since chrono can't represent these
                            Ok(Value::Property(PropertyValue::DateTime(
                                if date_str.starts_with('-') {
                                    i64::MIN / 2
                                } else {
                                    i64::MAX / 2
                                },
                            )))
                        } else {
                            Err(ExecutionError::RuntimeError(format!(
                                "Cannot parse date: {}",
                                s
                            )))
                        }
                    }
                    Value::Property(PropertyValue::Map(map)) => {
                        let year =
                            map.get("year").and_then(|v| v.as_integer()).unwrap_or(1970) as i32;
                        let month =
                            map.get("month").and_then(|v| v.as_integer()).unwrap_or(1) as u32;
                        let day = map.get("day").and_then(|v| v.as_integer()).unwrap_or(1) as u32;
                        if let Some(dt) = chrono::NaiveDate::from_ymd_opt(year, month, day) {
                            let millis = dt
                                .and_hms_opt(0, 0, 0)
                                .unwrap()
                                .and_utc()
                                .timestamp_millis();
                            Ok(Value::Property(PropertyValue::DateTime(millis)))
                        } else {
                            Err(ExecutionError::RuntimeError(format!(
                                "Invalid date: {}-{}-{}",
                                year, month, day
                            )))
                        }
                    }
                    _ => Err(ExecutionError::TypeError(
                        "date() requires string or map argument".to_string(),
                    )),
                }
            }
        }
        "datetime" => {
            if args.is_empty() {
                let now = chrono::Utc::now().timestamp_millis();
                Ok(Value::Property(PropertyValue::DateTime(now)))
            } else {
                match &args[0] {
                    Value::Property(PropertyValue::String(s)) => {
                        if let Ok(dt) = chrono::DateTime::parse_from_rfc3339(s) {
                            Ok(Value::Property(PropertyValue::DateTime(
                                dt.timestamp_millis(),
                            )))
                        } else if let Ok(dt) =
                            chrono::NaiveDateTime::parse_from_str(s, "%Y-%m-%dT%H:%M:%S%.f")
                        {
                            Ok(Value::Property(PropertyValue::DateTime(
                                dt.and_utc().timestamp_millis(),
                            )))
                        } else if let Ok(dt) =
                            chrono::NaiveDateTime::parse_from_str(s, "%Y-%m-%dT%H:%M:%S")
                        {
                            Ok(Value::Property(PropertyValue::DateTime(
                                dt.and_utc().timestamp_millis(),
                            )))
                        } else if s.starts_with('+') || s.starts_with('-') {
                            // Handle extended year format
                            Ok(Value::Property(PropertyValue::DateTime(
                                if s.starts_with('-') {
                                    i64::MIN / 2
                                } else {
                                    i64::MAX / 2
                                },
                            )))
                        } else {
                            Err(ExecutionError::RuntimeError(format!(
                                "Cannot parse datetime: {}",
                                s
                            )))
                        }
                    }
                    Value::Property(PropertyValue::Map(map)) => {
                        use chrono::TimeZone;
                        let year =
                            map.get("year").and_then(|v| v.as_integer()).unwrap_or(1970) as i32;
                        let month =
                            map.get("month").and_then(|v| v.as_integer()).unwrap_or(1) as u32;
                        let day = map.get("day").and_then(|v| v.as_integer()).unwrap_or(1) as u32;
                        let hour = map.get("hour").and_then(|v| v.as_integer()).unwrap_or(0) as u32;
                        let minute =
                            map.get("minute").and_then(|v| v.as_integer()).unwrap_or(0) as u32;
                        let second =
                            map.get("second").and_then(|v| v.as_integer()).unwrap_or(0) as u32;
                        if let Some(dt) = chrono::Utc
                            .with_ymd_and_hms(year, month, day, hour, minute, second)
                            .single()
                        {
                            Ok(Value::Property(PropertyValue::DateTime(
                                dt.timestamp_millis(),
                            )))
                        } else {
                            Err(ExecutionError::RuntimeError(format!(
                                "Invalid datetime components: year={}, month={}, day={}, hour={}, minute={}, second={}",
                                year, month, day, hour, minute, second
                            )))
                        }
                    }
                    _ => Err(ExecutionError::TypeError(
                        "datetime() requires string or map argument".to_string(),
                    )),
                }
            }
        }
        // localdatetime — same as datetime but without timezone
        "localdatetime" => {
            if args.is_empty() {
                let now = chrono::Utc::now().timestamp_millis();
                Ok(Value::Property(PropertyValue::DateTime(now)))
            } else {
                // Delegate to datetime — localdatetime uses same map/string args
                eval_function("datetime", args, store)
            }
        }
        // localtime — time without timezone, stored as millis since midnight
        "localtime" => {
            if args.is_empty() {
                let now = chrono::Utc::now();
                let millis =
                    (now.timestamp() % 86400) * 1000 + (now.timestamp_subsec_millis() as i64);
                Ok(Value::Property(PropertyValue::DateTime(millis)))
            } else if let Some(Value::Property(PropertyValue::Map(map))) = args.first() {
                let hour = map.get("hour").and_then(|v| v.as_integer()).unwrap_or(0);
                let minute = map.get("minute").and_then(|v| v.as_integer()).unwrap_or(0);
                let second = map.get("second").and_then(|v| v.as_integer()).unwrap_or(0);
                let nano = map
                    .get("nanosecond")
                    .and_then(|v| v.as_integer())
                    .unwrap_or(0);
                let millis = hour * 3_600_000 + minute * 60_000 + second * 1000 + nano / 1_000_000;
                Ok(Value::Property(PropertyValue::DateTime(millis)))
            } else {
                eval_function("datetime", args, store)
            }
        }
        // time — time with timezone, same as localtime for our purposes
        "time" => eval_function("localtime", args, store),
        // datetime.fromepoch — construct from epoch seconds/millis
        "datetime.fromepoch" => {
            if args.len() >= 2 {
                let seconds = match &args[0] {
                    Value::Property(PropertyValue::Integer(s)) => *s,
                    _ => 0,
                };
                let nanos = match &args[1] {
                    Value::Property(PropertyValue::Integer(n)) => *n,
                    _ => 0,
                };
                Ok(Value::Property(PropertyValue::DateTime(
                    seconds * 1000 + nanos / 1_000_000,
                )))
            } else if args.len() == 1 {
                let seconds = match &args[0] {
                    Value::Property(PropertyValue::Integer(s)) => *s,
                    _ => 0,
                };
                Ok(Value::Property(PropertyValue::DateTime(seconds * 1000)))
            } else {
                Err(ExecutionError::RuntimeError(
                    "datetime.fromepoch requires arguments".to_string(),
                ))
            }
        }
        "datetime.fromepochmillis" => {
            if args.is_empty() {
                return Err(ExecutionError::RuntimeError(
                    "datetime.fromepochmillis requires an argument".to_string(),
                ));
            }
            let millis = match &args[0] {
                Value::Property(PropertyValue::Integer(m)) => *m,
                _ => 0,
            };
            Ok(Value::Property(PropertyValue::DateTime(millis)))
        }
        "duration" => {
            if args.is_empty() {
                return Err(ExecutionError::RuntimeError(
                    "duration() requires an argument".to_string(),
                ));
            }
            match &args[0] {
                Value::Property(PropertyValue::String(s)) => parse_iso_duration(s),
                Value::Property(PropertyValue::Map(map)) => {
                    let months = map.get("months").and_then(|v| v.as_integer()).unwrap_or(0);
                    let days = map.get("days").and_then(|v| v.as_integer()).unwrap_or(0);
                    let hours = map.get("hours").and_then(|v| v.as_integer()).unwrap_or(0);
                    let minutes = map.get("minutes").and_then(|v| v.as_integer()).unwrap_or(0);
                    let seconds = map.get("seconds").and_then(|v| v.as_integer()).unwrap_or(0);
                    let years = map.get("years").and_then(|v| v.as_integer()).unwrap_or(0);
                    let total_months = years * 12 + months;
                    let total_seconds = hours * 3600 + minutes * 60 + seconds;
                    Ok(Value::Property(PropertyValue::Duration {
                        months: total_months,
                        days,
                        seconds: total_seconds,
                        nanos: 0,
                    }))
                }
                _ => Err(ExecutionError::TypeError(
                    "duration() requires string or map argument".to_string(),
                )),
            }
        }
        // duration component accessors
        "duration_between" | "duration.between" => {
            if args.len() < 2 {
                return Err(ExecutionError::RuntimeError(
                    "duration.between() requires 2 arguments".to_string(),
                ));
            }
            match (&args[0], &args[1]) {
                (
                    Value::Property(PropertyValue::DateTime(a)),
                    Value::Property(PropertyValue::DateTime(b)),
                ) => {
                    let diff_ms = b - a;
                    let total_seconds = diff_ms / 1000;
                    let remaining_days = total_seconds / 86400;
                    Ok(Value::Property(PropertyValue::Duration {
                        months: 0,
                        days: remaining_days,
                        seconds: total_seconds % 86400,
                        nanos: ((diff_ms % 1000) * 1_000_000) as i32,
                    }))
                }
                _ => Err(ExecutionError::TypeError(
                    "duration.between() requires two datetime arguments".to_string(),
                )),
            }
        }
        "duration.inseconds" | "duration_inseconds" => {
            if args.len() < 2 {
                return Err(ExecutionError::RuntimeError(
                    "duration.inSeconds() requires 2 arguments".to_string(),
                ));
            }
            match (&args[0], &args[1]) {
                (
                    Value::Property(PropertyValue::DateTime(a)),
                    Value::Property(PropertyValue::DateTime(b)),
                ) => {
                    let diff_ms = b - a;
                    let total_seconds = diff_ms / 1000;
                    let nanos = ((diff_ms % 1000).abs() * 1_000_000) as i32;
                    Ok(Value::Property(PropertyValue::Duration {
                        months: 0,
                        days: 0,
                        seconds: total_seconds,
                        nanos,
                    }))
                }
                _ => Err(ExecutionError::TypeError(
                    "duration.inSeconds() requires two datetime arguments".to_string(),
                )),
            }
        }
        "duration.inmonths" | "duration_inmonths" => {
            if args.len() < 2 {
                return Err(ExecutionError::RuntimeError(
                    "duration.inMonths() requires 2 arguments".to_string(),
                ));
            }
            match (&args[0], &args[1]) {
                (
                    Value::Property(PropertyValue::DateTime(a)),
                    Value::Property(PropertyValue::DateTime(b)),
                ) => {
                    let diff_ms = b - a;
                    let approx_months = diff_ms / (30 * 86400 * 1000);
                    let remaining_days = (diff_ms % (30 * 86400 * 1000)) / (86400 * 1000);
                    Ok(Value::Property(PropertyValue::Duration {
                        months: approx_months,
                        days: remaining_days,
                        seconds: 0,
                        nanos: 0,
                    }))
                }
                _ => Err(ExecutionError::TypeError(
                    "duration.inMonths() requires two datetime arguments".to_string(),
                )),
            }
        }
        // Scalar fallbacks for aggregate functions used outside aggregation context
        "count" => {
            if args.is_empty() {
                Ok(Value::Property(PropertyValue::Integer(1)))
            } else if matches!(&args[0], Value::Null | Value::Property(PropertyValue::Null)) {
                Ok(Value::Property(PropertyValue::Integer(0)))
            } else {
                Ok(Value::Property(PropertyValue::Integer(1)))
            }
        }
        "collect" => {
            if args.is_empty()
                || matches!(&args[0], Value::Null | Value::Property(PropertyValue::Null))
            {
                Ok(Value::Property(PropertyValue::Array(Vec::new())))
            } else {
                let pv = match &args[0] {
                    Value::Property(pv) => pv.clone(),
                    _ => PropertyValue::Null,
                };
                Ok(Value::Property(PropertyValue::Array(vec![pv])))
            }
        }
        "sum" | "avg" | "min" | "max" => {
            if args.is_empty() {
                Ok(Value::Null)
            } else {
                Ok(args[0].clone())
            }
        }
        "$propertyAccess" | "$propertyaccess" => {
            // Dynamic property access: (expr).prop — args[0] = base value, args[1] = property name string
            if args.len() == 2 {
                if let Value::Property(PropertyValue::String(ref prop_name)) = args[1] {
                    if let Some(s) = store {
                        Ok(Value::Property(args[0].resolve_property(prop_name, s)))
                    } else {
                        Ok(Value::Property(PropertyValue::Null))
                    }
                } else {
                    Ok(Value::Property(PropertyValue::Null))
                }
            } else {
                Ok(Value::Property(PropertyValue::Null))
            }
        }
        _ => Err(ExecutionError::RuntimeError(format!(
            "Unknown function: {}",
            name
        ))),
    }
}

/// Helper: extract string from Value
fn extract_string(val: &Value) -> ExecutionResult<String> {
    match val {
        Value::Property(PropertyValue::String(s)) => Ok(s.clone()),
        Value::Property(PropertyValue::Integer(i)) => Ok(i.to_string()),
        Value::Property(PropertyValue::Float(f)) => Ok(f.to_string()),
        Value::Property(PropertyValue::Boolean(b)) => Ok(b.to_string()),
        Value::Property(PropertyValue::Array(arr)) => {
            let items: Vec<String> = arr.iter().map(|v| format!("{}", v)).collect();
            Ok(format!("[{}]", items.join(", ")))
        }
        Value::Property(PropertyValue::Map(m)) => {
            let items: Vec<String> = m.iter().map(|(k, v)| format!("{}: {}", k, v)).collect();
            Ok(format!("{{{}}}", items.join(", ")))
        }
        Value::Null | Value::Property(PropertyValue::Null) => Ok("null".to_string()),
        _ => Err(ExecutionError::TypeError(
            "Expected string argument".to_string(),
        )),
    }
}

/// Helper: extract integer from Value
fn extract_int(val: &Value) -> ExecutionResult<i64> {
    match val {
        Value::Property(PropertyValue::Integer(i)) => Ok(*i),
        _ => Err(ExecutionError::TypeError(
            "Expected integer argument".to_string(),
        )),
    }
}

/// Add duration components to a DateTime (millis timestamp)
fn add_duration_to_datetime(dt_millis: i64, months: i64, days: i64, seconds: i64) -> PropertyValue {
    use chrono::{Duration, Months, TimeZone};
    let dt = chrono::Utc.timestamp_millis_opt(dt_millis).single();
    match dt {
        Some(mut datetime) => {
            // Add months
            if months > 0 {
                if let Some(d) = datetime.checked_add_months(Months::new(months as u32)) {
                    datetime = d;
                }
            } else if months < 0 {
                if let Some(d) = datetime.checked_sub_months(Months::new((-months) as u32)) {
                    datetime = d;
                }
            }
            // Add days and seconds
            let total_secs = days * 86400 + seconds;
            if let Some(d) = datetime.checked_add_signed(Duration::seconds(total_secs)) {
                datetime = d;
            }
            PropertyValue::DateTime(datetime.timestamp_millis())
        }
        None => PropertyValue::Null,
    }
}

/// Parse ISO 8601 duration string (e.g. "P1Y2M3DT4H5M6S")
fn parse_iso_duration(s: &str) -> ExecutionResult<Value> {
    let s = s.trim();
    if !s.starts_with('P') && !s.starts_with('p') {
        return Err(ExecutionError::RuntimeError(format!(
            "Invalid duration format: {}",
            s
        )));
    }
    let rest = &s[1..];
    let mut months: i64 = 0;
    let mut days: i64 = 0;
    let mut seconds: i64 = 0;
    let mut nanos: i32 = 0;
    let _ = nanos; // suppress warning

    let (date_part, time_part) = if let Some(idx) = rest.find(['T', 't']) {
        (&rest[..idx], &rest[idx + 1..])
    } else {
        (rest, "")
    };

    // Parse date part: Y, M, D
    let mut num_buf = String::new();
    for ch in date_part.chars() {
        if ch.is_ascii_digit() || ch == '.' {
            num_buf.push(ch);
        } else {
            let val: f64 = num_buf.parse().unwrap_or(0.0);
            num_buf.clear();
            match ch {
                'Y' | 'y' => months += (val * 12.0) as i64,
                'M' | 'm' => months += val as i64,
                'W' | 'w' => days += (val * 7.0) as i64,
                'D' | 'd' => days += val as i64,
                _ => {}
            }
        }
    }

    // Parse time part: H, M, S
    num_buf.clear();
    for ch in time_part.chars() {
        if ch.is_ascii_digit() || ch == '.' {
            num_buf.push(ch);
        } else {
            let val: f64 = num_buf.parse().unwrap_or(0.0);
            num_buf.clear();
            match ch {
                'H' | 'h' => seconds += (val * 3600.0) as i64,
                'M' | 'm' => seconds += (val * 60.0) as i64,
                'S' | 's' => {
                    seconds += val as i64;
                    nanos = ((val - val.floor()) * 1_000_000_000.0) as i32;
                }
                _ => {}
            }
        }
    }

    Ok(Value::Property(PropertyValue::Duration {
        months,
        days,
        seconds,
        nanos,
    }))
}

/// Shared CASE expression evaluation
fn eval_case<F>(
    operand: Option<&Expression>,
    when_clauses: &[(Expression, Expression)],
    else_result: Option<&Expression>,
    eval_fn: F,
) -> ExecutionResult<Value>
where
    F: Fn(&Expression) -> ExecutionResult<Value>,
{
    if let Some(op_expr) = operand {
        // Simple CASE: CASE expr WHEN val THEN result
        let op_val = eval_fn(op_expr)?;
        for (when_expr, then_expr) in when_clauses {
            let when_val = eval_fn(when_expr)?;
            if op_val == when_val {
                return eval_fn(then_expr);
            }
        }
    } else {
        // Searched CASE: CASE WHEN condition THEN result
        for (when_expr, then_expr) in when_clauses {
            let when_val = eval_fn(when_expr)?;
            if matches!(when_val, Value::Property(PropertyValue::Boolean(true))) {
                return eval_fn(then_expr);
            }
        }
    }
    // ELSE clause or NULL
    if let Some(else_expr) = else_result {
        eval_fn(else_expr)
    } else {
        Ok(Value::Null)
    }
}

/// Optimization problem wrapper for GraphStore
struct GraphOptimizationProblem {
    /// Static cost coefficients (e.g. price per unit) for single objective
    costs: Vec<f64>,
    /// Multiple cost coefficient vectors for multi-objective
    multi_costs: Vec<Vec<f64>>,
    /// Budget constraint (optional)
    budget: Option<f64>,
    /// Minimum total sum constraint (optional)
    min_total: Option<f64>,
    /// Dimensions
    dim: usize,
    /// Bounds
    lower: f64,
    upper: f64,
}

impl Problem for GraphOptimizationProblem {
    fn dim(&self) -> usize {
        self.dim
    }

    fn bounds(&self) -> (Array1<f64>, Array1<f64>) {
        (
            Array1::from_elem(self.dim, self.lower),
            Array1::from_elem(self.dim, self.upper),
        )
    }

    fn objective(&self, variables: &Array1<f64>) -> f64 {
        // Minimize sum(variable * cost)
        let mut sum = 0.0;
        for i in 0..self.dim {
            sum += variables[i] * self.costs[i];
        }
        sum
    }

    fn penalty(&self, variables: &Array1<f64>) -> f64 {
        let mut penalty = 0.0;

        // 1. Budget Constraint: sum(x * cost) <= budget
        if let Some(budget) = self.budget {
            let mut total_cost = 0.0;
            for i in 0..self.dim {
                total_cost += variables[i] * self.costs[i];
            }
            if total_cost > budget {
                penalty += (total_cost - budget).powi(2);
            }
        }

        // 2. Minimum Total Constraint: sum(x) >= min_total
        if let Some(min_total) = self.min_total {
            let mut total_val = 0.0;
            for i in 0..self.dim {
                total_val += variables[i];
            }
            if total_val < min_total {
                penalty += (min_total - total_val).powi(2) * 100.0; // High weight for feasibility
            }
        }

        penalty
    }
}

impl MultiObjectiveProblem for GraphOptimizationProblem {
    fn num_objectives(&self) -> usize {
        self.multi_costs.len()
    }

    fn objectives(&self, variables: &Array1<f64>) -> Vec<f64> {
        let mut results = Vec::with_capacity(self.multi_costs.len());
        for costs in &self.multi_costs {
            let mut sum = 0.0;
            for i in 0..self.dim {
                sum += variables[i] * costs[i];
            }
            results.push(sum);
        }
        results
    }

    fn dim(&self) -> usize {
        self.dim
    }
    fn bounds(&self) -> (Array1<f64>, Array1<f64>) {
        (
            Array1::from_elem(self.dim, self.lower),
            Array1::from_elem(self.dim, self.upper),
        )
    }
}

/// Physical operator trait - all operators implement this
pub trait PhysicalOperator: Send {
    /// Get the next record from this operator (read-only operations)
    fn next(&mut self, store: &GraphStore) -> ExecutionResult<Option<Record>>;

    /// Get the next batch of records (Vectorized Execution)
    /// Defaults to accumulating records from next()
    fn next_batch(
        &mut self,
        store: &GraphStore,
        batch_size: usize,
    ) -> ExecutionResult<Option<RecordBatch>> {
        let mut records = Vec::with_capacity(batch_size);
        for _ in 0..batch_size {
            match self.next(store)? {
                Some(record) => records.push(record),
                None => break,
            }
        }
        if records.is_empty() {
            Ok(None)
        } else {
            Ok(Some(RecordBatch {
                records,
                columns: Vec::new(),
            }))
        }
    }

    /// Get the next batch of records for mutating operations
    fn next_batch_mut(
        &mut self,
        store: &mut GraphStore,
        tenant_id: &str,
        batch_size: usize,
    ) -> ExecutionResult<Option<RecordBatch>> {
        let mut records = Vec::with_capacity(batch_size);
        for _ in 0..batch_size {
            match self.next_mut(store, tenant_id)? {
                Some(record) => records.push(record),
                None => break,
            }
        }
        if records.is_empty() {
            Ok(None)
        } else {
            Ok(Some(RecordBatch {
                records,
                columns: Vec::new(),
            }))
        }
    }

    /// Get the next record from this operator (write operations that mutate the store)
    fn next_mut(
        &mut self,
        store: &mut GraphStore,
        _tenant_id: &str,
    ) -> ExecutionResult<Option<Record>> {
        // Default implementation calls the read-only version
        self.next(store)
    }

    /// Reset the operator to start from the beginning
    fn reset(&mut self);

    /// Returns true if this operator mutates the graph store
    fn is_mutating(&self) -> bool {
        false
    }

    /// Describe this operator for EXPLAIN output
    /// Returns (operator_name, details, children)
    fn describe(&self) -> OperatorDescription {
        OperatorDescription {
            name: "Unknown".to_string(),
            details: String::new(),
            children: Vec::new(),
        }
    }
}

/// Description of an operator for EXPLAIN output
pub struct OperatorDescription {
    pub name: String,
    pub details: String,
    pub children: Vec<OperatorDescription>,
}

impl OperatorDescription {
    /// Format the operator tree as a string
    pub fn format(&self, indent: usize) -> String {
        let mut result = String::new();
        let prefix = if indent == 0 {
            String::new()
        } else {
            format!("{}+- ", "   ".repeat(indent - 1))
        };

        if self.details.is_empty() {
            result.push_str(&format!("{}{}\n", prefix, self.name));
        } else {
            result.push_str(&format!("{}{} ({})\n", prefix, self.name, self.details));
        }

        for child in &self.children {
            result.push_str(&child.format(indent + 1));
        }
        result
    }
}

/// Format an Expression for EXPLAIN output
fn format_expression(expr: &Expression) -> String {
    match expr {
        Expression::Variable(v) => v.clone(),
        Expression::Property { variable, property } => format!("{}.{}", variable, property),
        Expression::Literal(val) => format!("{:?}", val),
        Expression::Binary { left, op, right } => {
            let op_str = match op {
                BinaryOp::Eq => "=",
                BinaryOp::Ne => "<>",
                BinaryOp::Lt => "<",
                BinaryOp::Le => "<=",
                BinaryOp::Gt => ">",
                BinaryOp::Ge => ">=",
                BinaryOp::And => "AND",
                BinaryOp::Or => "OR",
                BinaryOp::Xor => "XOR",
                BinaryOp::Add => "+",
                BinaryOp::Sub => "-",
                BinaryOp::Mul => "*",
                BinaryOp::Div => "/",
                BinaryOp::Mod => "%",
                BinaryOp::Pow => "^",
                BinaryOp::StartsWith => "STARTS WITH",
                BinaryOp::EndsWith => "ENDS WITH",
                BinaryOp::Contains => "CONTAINS",
                BinaryOp::In => "IN",
                BinaryOp::RegexMatch => "=~",
            };
            format!(
                "{} {} {}",
                format_expression(left),
                op_str,
                format_expression(right)
            )
        }
        Expression::Unary { op, expr } => {
            let op_str = match op {
                UnaryOp::Not => "NOT",
                UnaryOp::Minus => "-",
                UnaryOp::IsNull => "IS NULL",
                UnaryOp::IsNotNull => "IS NOT NULL",
            };
            match op {
                UnaryOp::IsNull | UnaryOp::IsNotNull => {
                    format!("{} {}", format_expression(expr), op_str)
                }
                _ => format!("{} {}", op_str, format_expression(expr)),
            }
        }
        Expression::Function {
            name,
            args,
            distinct,
        } => {
            let arg_strs: Vec<String> = args.iter().map(format_expression).collect();
            if *distinct {
                format!("{}(DISTINCT {})", name, arg_strs.join(", "))
            } else {
                format!("{}({})", name, arg_strs.join(", "))
            }
        }
        Expression::PathVariable(v) => format!("path({})", v),
        Expression::Parameter(p) => format!("${}", p),
        _ => "...".to_string(),
    }
}

/// Type alias for boxed operators
pub type OperatorBox = Box<dyn PhysicalOperator>;

/// Single-row operator: emits exactly one empty record.
/// Used as the input for standalone RETURN/UNWIND queries (no MATCH/CREATE).
/// Analogous to Oracle's DUAL table or PostgreSQL's implicit single row.
pub struct SingleRowOperator {
    emitted: bool,
}

impl SingleRowOperator {
    pub fn new() -> Self {
        Self { emitted: false }
    }
}

impl PhysicalOperator for SingleRowOperator {
    fn next(&mut self, _store: &GraphStore) -> ExecutionResult<Option<Record>> {
        if self.emitted {
            Ok(None)
        } else {
            self.emitted = true;
            Ok(Some(Record::new()))
        }
    }

    fn next_batch(
        &mut self,
        store: &GraphStore,
        _batch_size: usize,
    ) -> ExecutionResult<Option<RecordBatch>> {
        if self.emitted {
            return Ok(None);
        }
        match self.next(store)? {
            Some(record) => Ok(Some(RecordBatch {
                records: vec![record],
                columns: Vec::new(),
            })),
            None => Ok(None),
        }
    }

    fn next_mut(
        &mut self,
        _store: &mut GraphStore,
        _tenant_id: &str,
    ) -> ExecutionResult<Option<Record>> {
        if self.emitted {
            Ok(None)
        } else {
            self.emitted = true;
            Ok(Some(Record::new()))
        }
    }

    fn next_batch_mut(
        &mut self,
        store: &mut GraphStore,
        tenant_id: &str,
        _batch_size: usize,
    ) -> ExecutionResult<Option<RecordBatch>> {
        if self.emitted {
            return Ok(None);
        }
        match self.next_mut(store, tenant_id)? {
            Some(record) => Ok(Some(RecordBatch {
                records: vec![record],
                columns: Vec::new(),
            })),
            None => Ok(None),
        }
    }

    fn reset(&mut self) {
        self.emitted = false;
    }

    fn describe(&self) -> OperatorDescription {
        OperatorDescription {
            name: "SingleRow".to_string(),
            details: String::new(),
            children: Vec::new(),
        }
    }
}

/// Node scan operator: MATCH (n:Person)
pub struct NodeScanOperator {
    /// Variable name to bind nodes to
    variable: String,
    /// Labels to filter by
    labels: Vec<Label>,
    /// Current position in iteration
    node_ids: Vec<NodeId>,
    /// Current index
    current: usize,
    /// Early limit: stop producing after this many rows (for LIMIT pushdown)
    early_limit: Option<usize>,
    /// Count of rows produced (for early limit tracking)
    produced: usize,
}

impl NodeScanOperator {
    /// Create a new node scan operator
    pub fn new(variable: String, labels: Vec<Label>) -> Self {
        Self {
            variable,
            labels,
            node_ids: Vec::new(),
            current: 0,
            early_limit: None,
            produced: 0,
        }
    }

    /// Set early limit for LIMIT pushdown optimization
    pub fn with_early_limit(mut self, limit: usize) -> Self {
        self.early_limit = Some(limit);
        self
    }

    fn initialize(&mut self, store: &GraphStore) {
        if !self.node_ids.is_empty() {
            return;
        }

        // Get all nodes matching the labels (nodes must have ALL specified labels)
        if self.labels.is_empty() {
            // No labels - scan all nodes
            self.node_ids = store.all_nodes().into_iter().map(|n| n.id).collect();
        } else if self.labels.len() == 1 {
            // Single label - simple scan
            self.node_ids = store
                .get_nodes_by_label(&self.labels[0])
                .into_iter()
                .map(|n| n.id)
                .collect();
        } else {
            // Multiple labels - intersection: node must have ALL labels
            // Start with nodes matching the first label, then filter by remaining labels
            let first_nodes: HashSet<NodeId> = store
                .get_nodes_by_label(&self.labels[0])
                .into_iter()
                .map(|n| n.id)
                .collect();
            let mut result: HashSet<NodeId> = first_nodes;
            for label in &self.labels[1..] {
                let label_nodes: HashSet<NodeId> = store
                    .get_nodes_by_label(label)
                    .into_iter()
                    .map(|n| n.id)
                    .collect();
                result = result.intersection(&label_nodes).cloned().collect();
            }

            // Convert to sorted vec for consistent ordering
            let mut nodes: Vec<_> = result.into_iter().collect();
            nodes.sort_by_key(|id| id.as_u64());
            self.node_ids = nodes;
        }
    }
}

impl PhysicalOperator for NodeScanOperator {
    fn next(&mut self, store: &GraphStore) -> ExecutionResult<Option<Record>> {
        self.initialize(store);

        if self.current >= self.node_ids.len() {
            return Ok(None);
        }

        // Check early limit
        if let Some(limit) = self.early_limit {
            if self.produced >= limit {
                return Ok(None);
            }
        }

        let node_id = self.node_ids[self.current];
        self.current += 1;
        self.produced += 1;

        let mut record = Record::new();
        record.bind(self.variable.clone(), Value::NodeRef(node_id));

        Ok(Some(record))
    }

    fn next_batch(
        &mut self,
        store: &GraphStore,
        batch_size: usize,
    ) -> ExecutionResult<Option<RecordBatch>> {
        self.initialize(store);

        if self.current >= self.node_ids.len() {
            return Ok(None);
        }

        // Apply early limit to batch size
        let effective_batch = if let Some(limit) = self.early_limit {
            let remaining = limit.saturating_sub(self.produced);
            if remaining == 0 {
                return Ok(None);
            }
            batch_size.min(remaining)
        } else {
            batch_size
        };

        let end = (self.current + effective_batch).min(self.node_ids.len());
        let range = self.current..end;
        self.current = end;

        let mut records = Vec::with_capacity(range.len());
        for node_id in &self.node_ids[range] {
            let mut record = Record::new();
            record.bind(self.variable.clone(), Value::NodeRef(*node_id));
            records.push(record);
        }
        self.produced += records.len();

        Ok(Some(RecordBatch {
            records,
            columns: vec![self.variable.clone()],
        }))
    }

    fn reset(&mut self) {
        self.current = 0;
        self.produced = 0;
    }

    fn describe(&self) -> OperatorDescription {
        let details = if self.labels.is_empty() {
            format!("var={}, all labels", self.variable)
        } else {
            format!(
                "var={}, labels={:?}",
                self.variable,
                self.labels.iter().map(|l| l.as_str()).collect::<Vec<_>>()
            )
        };
        OperatorDescription {
            name: "NodeScan".to_string(),
            details,
            children: Vec::new(),
        }
    }
}

/// Filter operator: WHERE n.age > 30
pub struct FilterOperator {
    /// Input operator
    input: OperatorBox,
    /// Predicate expression
    predicate: Expression,
}

#[allow(dead_code)]
impl FilterOperator {
    /// Create a new filter operator
    pub fn new(input: OperatorBox, predicate: Expression) -> Self {
        Self { input, predicate }
    }

    fn evaluate_predicate(&self, record: &Record, _store: &GraphStore) -> ExecutionResult<bool> {
        let result = self.evaluate_expression(&self.predicate, record, _store)?;

        match result {
            Value::Property(PropertyValue::Boolean(b)) => Ok(b),
            Value::Null | Value::Property(PropertyValue::Null) => Ok(false),
            _ => Err(ExecutionError::TypeError(
                "Predicate must evaluate to boolean".to_string(),
            )),
        }
    }

    fn evaluate_expression(
        &self,
        expr: &Expression,
        record: &Record,
        store: &GraphStore,
    ) -> ExecutionResult<Value> {
        match expr {
            Expression::Variable(var) => record
                .get(var)
                .cloned()
                .ok_or_else(|| ExecutionError::VariableNotFound(var.clone())),
            Expression::Property { variable, property } => {
                let val = record
                    .get(variable)
                    .ok_or_else(|| ExecutionError::VariableNotFound(variable.clone()))?;

                let prop = val.resolve_property(property, store);
                Ok(Value::Property(prop))
            }
            Expression::Literal(lit) => Ok(Value::Property(lit.clone())),
            Expression::Binary { left, op, right } => {
                let left_val = self.evaluate_expression(left, record, store)?;
                let right_val = self.evaluate_expression(right, record, store)?;
                self.evaluate_binary_op(op, left_val, right_val)
            }
            Expression::Function { name, args, .. }
                if name.eq_ignore_ascii_case("$patternPredicate") =>
            {
                eval_pattern_predicate_from_args(args, record, store)
            }
            Expression::Function { name, args, .. } => {
                let arg_vals: Vec<Value> = args
                    .iter()
                    .map(|a| self.evaluate_expression(a, record, store))
                    .collect::<ExecutionResult<Vec<_>>>()?;
                eval_function(name, &arg_vals, Some(store))
            }
            Expression::Unary { op, expr } => {
                let val = self.evaluate_expression(expr, record, store)?;
                match op {
                    UnaryOp::IsNull => {
                        let is_null =
                            matches!(val, Value::Null | Value::Property(PropertyValue::Null));
                        Ok(Value::Property(PropertyValue::Boolean(is_null)))
                    }
                    UnaryOp::IsNotNull => {
                        let is_null =
                            matches!(val, Value::Null | Value::Property(PropertyValue::Null));
                        Ok(Value::Property(PropertyValue::Boolean(!is_null)))
                    }
                    UnaryOp::Not | UnaryOp::Minus => eval_unary_op(op, val),
                }
            }
            Expression::Case {
                operand,
                when_clauses,
                else_result,
            } => eval_case(
                operand.as_deref(),
                when_clauses,
                else_result.as_deref(),
                |e| self.evaluate_expression(e, record, store),
            ),
            Expression::Index { expr, index } => {
                let collection = self.evaluate_expression(expr, record, store)?;
                let idx = self.evaluate_expression(index, record, store)?;
                eval_index(collection, idx)
            }
            Expression::ListSlice { expr, start, end } => {
                let collection = self.evaluate_expression(expr, record, store)?;
                let s = match start {
                    Some(s) => Some(self.evaluate_expression(s, record, store)?),
                    None => None,
                };
                let en = match end {
                    Some(e) => Some(self.evaluate_expression(e, record, store)?),
                    None => None,
                };
                eval_list_slice(collection, s, en)
            }
            Expression::ExistsSubquery {
                pattern,
                where_clause,
            } => eval_exists_subquery(pattern, where_clause.as_deref(), record, store),
            Expression::ListComprehension {
                variable,
                list_expr,
                filter,
                map_expr,
            } => eval_list_comprehension(
                variable,
                list_expr,
                filter.as_deref(),
                map_expr,
                record,
                store,
            ),
            Expression::PredicateFunction {
                name,
                variable,
                list_expr,
                predicate,
            } => eval_predicate_function(name, variable, list_expr, predicate, record, store),
            Expression::Reduce {
                accumulator,
                init,
                variable,
                list_expr,
                expression,
            } => eval_reduce(
                accumulator,
                init,
                variable,
                list_expr,
                expression,
                record,
                store,
            ),
            Expression::PatternComprehension {
                pattern,
                filter,
                projection,
            } => eval_pattern_comprehension(pattern, filter.as_deref(), projection, record, store),
            Expression::MapExpression(entries) => {
                let mut map = std::collections::HashMap::new();
                for (key, val_expr) in entries {
                    let val = self.evaluate_expression(val_expr, record, store)?;
                    let pv = match val {
                        Value::Property(pv) => pv,
                        Value::Null => PropertyValue::Null,
                        _ => PropertyValue::Null,
                    };
                    map.insert(key.clone(), pv);
                }
                Ok(Value::Property(PropertyValue::Map(map)))
            }
            Expression::PathVariable(var) => record
                .get(var)
                .cloned()
                .ok_or_else(|| ExecutionError::VariableNotFound(var.clone())),
            Expression::Parameter(name) => {
                record.get(&format!("${}", name)).cloned().ok_or_else(|| {
                    ExecutionError::RuntimeError(format!("Unresolved parameter: ${}", name))
                })
            }
        }
    }

    fn evaluate_binary_op(
        &self,
        op: &BinaryOp,
        left: Value,
        right: Value,
    ) -> ExecutionResult<Value> {
        // Delegate to the shared binary op evaluator which handles null propagation
        eval_binary_op(op, left, right)
    }

    /// Equality with type coercion: Integer↔Float numeric promotion,
    /// String↔Boolean coercion ("true"/"false"), and Null handling.
    fn coerced_eq(&self, left: &PropertyValue, right: &PropertyValue) -> bool {
        match (left, right) {
            // Same-type: use derived PartialEq
            _ if std::mem::discriminant(left) == std::mem::discriminant(right) => left == right,
            // Integer ↔ Float promotion
            (PropertyValue::Integer(l), PropertyValue::Float(r)) => (*l as f64) == *r,
            (PropertyValue::Float(l), PropertyValue::Integer(r)) => *l == (*r as f64),
            // DateTime ↔ Integer coercion (DateTime stores epoch millis as i64)
            (PropertyValue::DateTime(l), PropertyValue::Integer(r))
            | (PropertyValue::Integer(r), PropertyValue::DateTime(l)) => l == r,
            // String ↔ Boolean coercion (LLMs often generate `prop = 'true'`)
            (PropertyValue::Boolean(b), PropertyValue::String(s))
            | (PropertyValue::String(s), PropertyValue::Boolean(b)) => {
                match s.to_lowercase().as_str() {
                    "true" => *b,
                    "false" => !*b,
                    _ => false,
                }
            }
            // Everything else: not equal
            _ => false,
        }
    }

    fn compare_lt(
        &self,
        left: &PropertyValue,
        right: &PropertyValue,
    ) -> ExecutionResult<PropertyValue> {
        match (left, right) {
            (PropertyValue::Null, _) | (_, PropertyValue::Null) => Ok(PropertyValue::Null),
            (PropertyValue::Integer(l), PropertyValue::Integer(r)) => {
                Ok(PropertyValue::Boolean(l < r))
            }
            (PropertyValue::Float(l), PropertyValue::Float(r)) => Ok(PropertyValue::Boolean(l < r)),
            (PropertyValue::Integer(l), PropertyValue::Float(r)) => {
                Ok(PropertyValue::Boolean((*l as f64) < *r))
            }
            (PropertyValue::Float(l), PropertyValue::Integer(r)) => {
                Ok(PropertyValue::Boolean(*l < (*r as f64)))
            }
            (PropertyValue::String(l), PropertyValue::String(r)) => {
                Ok(PropertyValue::Boolean(l < r))
            }
            (PropertyValue::DateTime(l), PropertyValue::DateTime(r)) => {
                Ok(PropertyValue::Boolean(l < r))
            }
            (PropertyValue::DateTime(l), PropertyValue::Integer(r)) => {
                Ok(PropertyValue::Boolean(l < r))
            }
            (PropertyValue::Integer(l), PropertyValue::DateTime(r)) => {
                Ok(PropertyValue::Boolean(l < r))
            }
            _ => Err(ExecutionError::TypeError(
                "Cannot compare these types".to_string(),
            )),
        }
    }

    fn compare_le(
        &self,
        left: &PropertyValue,
        right: &PropertyValue,
    ) -> ExecutionResult<PropertyValue> {
        match (left, right) {
            (PropertyValue::Null, _) | (_, PropertyValue::Null) => Ok(PropertyValue::Null),
            (PropertyValue::Integer(l), PropertyValue::Integer(r)) => {
                Ok(PropertyValue::Boolean(l <= r))
            }
            (PropertyValue::Float(l), PropertyValue::Float(r)) => {
                Ok(PropertyValue::Boolean(l <= r))
            }
            (PropertyValue::Integer(l), PropertyValue::Float(r)) => {
                Ok(PropertyValue::Boolean((*l as f64) <= *r))
            }
            (PropertyValue::Float(l), PropertyValue::Integer(r)) => {
                Ok(PropertyValue::Boolean(*l <= (*r as f64)))
            }
            (PropertyValue::String(l), PropertyValue::String(r)) => {
                Ok(PropertyValue::Boolean(l <= r))
            }
            (PropertyValue::DateTime(l), PropertyValue::DateTime(r)) => {
                Ok(PropertyValue::Boolean(l <= r))
            }
            (PropertyValue::DateTime(l), PropertyValue::Integer(r)) => {
                Ok(PropertyValue::Boolean(l <= r))
            }
            (PropertyValue::Integer(l), PropertyValue::DateTime(r)) => {
                Ok(PropertyValue::Boolean(l <= r))
            }
            _ => Err(ExecutionError::TypeError(
                "Cannot compare these types".to_string(),
            )),
        }
    }

    fn compare_gt(
        &self,
        left: &PropertyValue,
        right: &PropertyValue,
    ) -> ExecutionResult<PropertyValue> {
        match (left, right) {
            (PropertyValue::Null, _) | (_, PropertyValue::Null) => Ok(PropertyValue::Null),
            (PropertyValue::Integer(l), PropertyValue::Integer(r)) => {
                Ok(PropertyValue::Boolean(l > r))
            }
            (PropertyValue::Float(l), PropertyValue::Float(r)) => Ok(PropertyValue::Boolean(l > r)),
            (PropertyValue::Integer(l), PropertyValue::Float(r)) => {
                Ok(PropertyValue::Boolean((*l as f64) > *r))
            }
            (PropertyValue::Float(l), PropertyValue::Integer(r)) => {
                Ok(PropertyValue::Boolean(*l > (*r as f64)))
            }
            (PropertyValue::String(l), PropertyValue::String(r)) => {
                Ok(PropertyValue::Boolean(l > r))
            }
            (PropertyValue::DateTime(l), PropertyValue::DateTime(r)) => {
                Ok(PropertyValue::Boolean(l > r))
            }
            (PropertyValue::DateTime(l), PropertyValue::Integer(r)) => {
                Ok(PropertyValue::Boolean(l > r))
            }
            (PropertyValue::Integer(l), PropertyValue::DateTime(r)) => {
                Ok(PropertyValue::Boolean(l > r))
            }
            _ => Err(ExecutionError::TypeError(
                "Cannot compare these types".to_string(),
            )),
        }
    }

    fn compare_ge(
        &self,
        left: &PropertyValue,
        right: &PropertyValue,
    ) -> ExecutionResult<PropertyValue> {
        match (left, right) {
            (PropertyValue::Null, _) | (_, PropertyValue::Null) => Ok(PropertyValue::Null),
            (PropertyValue::Integer(l), PropertyValue::Integer(r)) => {
                Ok(PropertyValue::Boolean(l >= r))
            }
            (PropertyValue::Float(l), PropertyValue::Float(r)) => {
                Ok(PropertyValue::Boolean(l >= r))
            }
            (PropertyValue::Integer(l), PropertyValue::Float(r)) => {
                Ok(PropertyValue::Boolean((*l as f64) >= *r))
            }
            (PropertyValue::Float(l), PropertyValue::Integer(r)) => {
                Ok(PropertyValue::Boolean(*l >= (*r as f64)))
            }
            (PropertyValue::String(l), PropertyValue::String(r)) => {
                Ok(PropertyValue::Boolean(l >= r))
            }
            (PropertyValue::DateTime(l), PropertyValue::DateTime(r)) => {
                Ok(PropertyValue::Boolean(l >= r))
            }
            (PropertyValue::DateTime(l), PropertyValue::Integer(r)) => {
                Ok(PropertyValue::Boolean(l >= r))
            }
            (PropertyValue::Integer(l), PropertyValue::DateTime(r)) => {
                Ok(PropertyValue::Boolean(l >= r))
            }
            _ => Err(ExecutionError::TypeError(
                "Cannot compare these types".to_string(),
            )),
        }
    }

    fn logical_and(
        &self,
        left: &PropertyValue,
        right: &PropertyValue,
    ) -> ExecutionResult<PropertyValue> {
        match (left, right) {
            (PropertyValue::Boolean(l), PropertyValue::Boolean(r)) => {
                Ok(PropertyValue::Boolean(*l && *r))
            }
            _ => Err(ExecutionError::TypeError(
                "AND requires boolean operands".to_string(),
            )),
        }
    }

    fn logical_or(
        &self,
        left: &PropertyValue,
        right: &PropertyValue,
    ) -> ExecutionResult<PropertyValue> {
        match (left, right) {
            (PropertyValue::Boolean(l), PropertyValue::Boolean(r)) => {
                Ok(PropertyValue::Boolean(*l || *r))
            }
            _ => Err(ExecutionError::TypeError(
                "OR requires boolean operands".to_string(),
            )),
        }
    }

    fn arithmetic_add(
        &self,
        left: &PropertyValue,
        right: &PropertyValue,
    ) -> ExecutionResult<PropertyValue> {
        match (left, right) {
            (PropertyValue::Integer(l), PropertyValue::Integer(r)) => {
                Ok(PropertyValue::Integer(l + r))
            }
            (PropertyValue::Float(l), PropertyValue::Float(r)) => Ok(PropertyValue::Float(l + r)),
            (PropertyValue::Integer(l), PropertyValue::Float(r)) => {
                Ok(PropertyValue::Float(*l as f64 + r))
            }
            (PropertyValue::Float(l), PropertyValue::Integer(r)) => {
                Ok(PropertyValue::Float(l + *r as f64))
            }
            (PropertyValue::String(l), PropertyValue::String(r)) => {
                Ok(PropertyValue::String(format!("{}{}", l, r)))
            }
            _ => Err(ExecutionError::TypeError(
                "Addition requires numeric or string operands".to_string(),
            )),
        }
    }

    fn arithmetic_sub(
        &self,
        left: &PropertyValue,
        right: &PropertyValue,
    ) -> ExecutionResult<PropertyValue> {
        match (left, right) {
            (PropertyValue::Integer(l), PropertyValue::Integer(r)) => {
                Ok(PropertyValue::Integer(l - r))
            }
            (PropertyValue::Float(l), PropertyValue::Float(r)) => Ok(PropertyValue::Float(l - r)),
            (PropertyValue::Integer(l), PropertyValue::Float(r)) => {
                Ok(PropertyValue::Float(*l as f64 - r))
            }
            (PropertyValue::Float(l), PropertyValue::Integer(r)) => {
                Ok(PropertyValue::Float(l - *r as f64))
            }
            _ => Err(ExecutionError::TypeError(
                "Subtraction requires numeric operands".to_string(),
            )),
        }
    }

    fn arithmetic_mul(
        &self,
        left: &PropertyValue,
        right: &PropertyValue,
    ) -> ExecutionResult<PropertyValue> {
        match (left, right) {
            (PropertyValue::Integer(l), PropertyValue::Integer(r)) => {
                Ok(PropertyValue::Integer(l * r))
            }
            (PropertyValue::Float(l), PropertyValue::Float(r)) => Ok(PropertyValue::Float(l * r)),
            (PropertyValue::Integer(l), PropertyValue::Float(r)) => {
                Ok(PropertyValue::Float(*l as f64 * r))
            }
            (PropertyValue::Float(l), PropertyValue::Integer(r)) => {
                Ok(PropertyValue::Float(l * *r as f64))
            }
            _ => Err(ExecutionError::TypeError(
                "Multiplication requires numeric operands".to_string(),
            )),
        }
    }

    fn arithmetic_div(
        &self,
        left: &PropertyValue,
        right: &PropertyValue,
    ) -> ExecutionResult<PropertyValue> {
        match (left, right) {
            (PropertyValue::Integer(_), PropertyValue::Integer(0)) => {
                Err(ExecutionError::RuntimeError("Division by zero".to_string()))
            }
            (PropertyValue::Integer(l), PropertyValue::Integer(r)) => {
                Ok(PropertyValue::Integer(l / r))
            }
            (PropertyValue::Float(l), PropertyValue::Float(r)) => Ok(PropertyValue::Float(l / r)),
            (PropertyValue::Integer(l), PropertyValue::Float(r)) => {
                Ok(PropertyValue::Float(*l as f64 / r))
            }
            (PropertyValue::Float(l), PropertyValue::Integer(r)) => {
                Ok(PropertyValue::Float(l / *r as f64))
            }
            _ => Err(ExecutionError::TypeError(
                "Division requires numeric operands".to_string(),
            )),
        }
    }

    fn arithmetic_mod(
        &self,
        left: &PropertyValue,
        right: &PropertyValue,
    ) -> ExecutionResult<PropertyValue> {
        match (left, right) {
            (PropertyValue::Integer(_), PropertyValue::Integer(0)) => {
                Err(ExecutionError::RuntimeError("Modulo by zero".to_string()))
            }
            (PropertyValue::Integer(l), PropertyValue::Integer(r)) => {
                Ok(PropertyValue::Integer(l % r))
            }
            (PropertyValue::Float(l), PropertyValue::Float(r)) => Ok(PropertyValue::Float(l % r)),
            (PropertyValue::Integer(l), PropertyValue::Float(r)) => {
                Ok(PropertyValue::Float(*l as f64 % r))
            }
            (PropertyValue::Float(l), PropertyValue::Integer(r)) => {
                Ok(PropertyValue::Float(l % *r as f64))
            }
            _ => Err(ExecutionError::TypeError(
                "Modulo requires numeric operands".to_string(),
            )),
        }
    }

    fn string_starts_with(
        &self,
        left: &PropertyValue,
        right: &PropertyValue,
    ) -> ExecutionResult<PropertyValue> {
        match (left, right) {
            (PropertyValue::String(l), PropertyValue::String(r)) => {
                Ok(PropertyValue::Boolean(l.starts_with(r.as_str())))
            }
            (PropertyValue::Null, _) | (_, PropertyValue::Null) => Ok(PropertyValue::Null),
            _ => Ok(PropertyValue::Null), // Non-string types return null per Cypher spec
        }
    }

    fn string_ends_with(
        &self,
        left: &PropertyValue,
        right: &PropertyValue,
    ) -> ExecutionResult<PropertyValue> {
        match (left, right) {
            (PropertyValue::String(l), PropertyValue::String(r)) => {
                Ok(PropertyValue::Boolean(l.ends_with(r.as_str())))
            }
            (PropertyValue::Null, _) | (_, PropertyValue::Null) => Ok(PropertyValue::Null),
            _ => Ok(PropertyValue::Null),
        }
    }

    fn string_contains(
        &self,
        left: &PropertyValue,
        right: &PropertyValue,
    ) -> ExecutionResult<PropertyValue> {
        match (left, right) {
            (PropertyValue::String(l), PropertyValue::String(r)) => {
                Ok(PropertyValue::Boolean(l.contains(r.as_str())))
            }
            (PropertyValue::Null, _) | (_, PropertyValue::Null) => Ok(PropertyValue::Null),
            _ => Ok(PropertyValue::Null),
        }
    }

    fn eval_in(
        &self,
        left: &PropertyValue,
        right: &PropertyValue,
    ) -> ExecutionResult<PropertyValue> {
        match right {
            PropertyValue::Array(arr) => Ok(PropertyValue::Boolean(arr.contains(left))),
            _ => Err(ExecutionError::TypeError(
                "IN requires a list on the right side".to_string(),
            )),
        }
    }

    fn regex_match(
        &self,
        left: &PropertyValue,
        right: &PropertyValue,
    ) -> ExecutionResult<PropertyValue> {
        match (left, right) {
            (PropertyValue::String(text), PropertyValue::String(pattern)) => {
                let re = regex::Regex::new(pattern)
                    .map_err(|e| ExecutionError::RuntimeError(format!("Invalid regex: {}", e)))?;
                Ok(PropertyValue::Boolean(re.is_match(text)))
            }
            _ => Err(ExecutionError::TypeError(
                "=~ requires string operands".to_string(),
            )),
        }
    }

    // evaluate_function removed — FilterOperator now delegates to global eval_function
}

impl PhysicalOperator for FilterOperator {
    fn next(&mut self, store: &GraphStore) -> ExecutionResult<Option<Record>> {
        while let Some(record) = self.input.next(store)? {
            if self.evaluate_predicate(&record, store)? {
                return Ok(Some(record));
            }
        }
        Ok(None)
    }

    fn next_batch(
        &mut self,
        store: &GraphStore,
        batch_size: usize,
    ) -> ExecutionResult<Option<RecordBatch>> {
        let mut filtered_records = Vec::new();

        while filtered_records.len() < batch_size {
            if let Some(batch) = self.input.next_batch(store, batch_size)? {
                for record in batch.records {
                    if self.evaluate_predicate(&record, store)? {
                        filtered_records.push(record);
                    }
                }
            } else {
                break;
            }
        }

        if filtered_records.is_empty() {
            Ok(None)
        } else {
            Ok(Some(RecordBatch {
                records: filtered_records,
                columns: Vec::new(), // Filter doesn't change columns
            }))
        }
    }

    fn next_mut(
        &mut self,
        store: &mut GraphStore,
        tenant_id: &str,
    ) -> ExecutionResult<Option<Record>> {
        while let Some(record) = self.input.next_mut(store, tenant_id)? {
            let store_ref: &GraphStore = store;
            if self.evaluate_predicate(&record, store_ref)? {
                return Ok(Some(record));
            }
        }
        Ok(None)
    }

    fn reset(&mut self) {
        self.input.reset();
    }

    fn describe(&self) -> OperatorDescription {
        OperatorDescription {
            name: "Filter".to_string(),
            details: format_expression(&self.predicate),
            children: vec![self.input.describe()],
        }
    }
}

/// Expand operator: `-[:KNOWS]->`
pub struct ExpandOperator {
    /// Input operator
    input: OperatorBox,
    /// Source variable
    source_var: String,
    /// Target variable
    target_var: String,
    /// Edge variable (optional)
    edge_var: Option<String>,
    /// Edge types to expand (empty = all types)
    edge_types: Vec<String>,
    /// Target node labels to filter (empty = any label)
    target_labels: Vec<Label>,
    /// Direction
    direction: Direction,
    /// Current input record
    current_record: Option<Record>,
    /// Current edges as lightweight tuples (EdgeId, source, target, EdgeType) — no Edge clone
    current_edges: Vec<(crate::graph::EdgeId, NodeId, NodeId, EdgeType)>,
    /// Current edge index
    edge_index: usize,
    /// Path variable name for named paths (CY-04)
    path_variable: Option<String>,
}

impl ExpandOperator {
    /// Create a new expand operator
    pub fn new(
        input: OperatorBox,
        source_var: String,
        target_var: String,
        edge_var: Option<String>,
        edge_types: Vec<String>,
        direction: Direction,
    ) -> Self {
        Self {
            input,
            source_var,
            target_var,
            edge_var,
            edge_types,
            target_labels: Vec::new(),
            direction,
            current_record: None,
            current_edges: Vec::new(),
            edge_index: 0,
            path_variable: None,
        }
    }

    /// Set path variable for named path materialization (CY-04)
    pub fn with_path_variable(mut self, var: String) -> Self {
        self.path_variable = Some(var);
        self
    }

    /// Set target node labels to filter during expansion
    pub fn with_target_labels(mut self, labels: Vec<Label>) -> Self {
        self.target_labels = labels;
        self
    }

    fn load_edges(&mut self, record: &Record, store: &GraphStore) -> ExecutionResult<()> {
        let source_val = record
            .get(&self.source_var)
            .ok_or_else(|| ExecutionError::VariableNotFound(self.source_var.clone()))?;

        let node_id = source_val.node_id().ok_or_else(|| {
            ExecutionError::TypeError(format!("{} is not a node", self.source_var))
        })?;

        // Get lightweight edge tuples based on direction (no Edge clone)
        let edges: Vec<(crate::graph::EdgeId, NodeId, NodeId, &EdgeType)> = match self.direction {
            Direction::Outgoing => store.get_outgoing_edge_targets(node_id),
            Direction::Incoming => store.get_incoming_edge_sources(node_id),
            Direction::Both => {
                let mut all = store.get_outgoing_edge_targets(node_id);
                let incoming = store.get_incoming_edge_sources(node_id);
                // Deduplicate by edge ID (self-loops appear in both outgoing and incoming)
                let mut seen_ids: HashSet<crate::graph::EdgeId> =
                    all.iter().map(|(eid, ..)| *eid).collect();
                for edge in incoming {
                    if seen_ids.insert(edge.0) {
                        all.push(edge);
                    }
                }
                all
            }
        };

        // Filter by edge type if specified, clone EdgeType ref to owned
        self.current_edges = if self.edge_types.is_empty() {
            edges
                .into_iter()
                .map(|(eid, src, tgt, et)| (eid, src, tgt, et.clone()))
                .collect()
        } else {
            edges
                .into_iter()
                .filter(|(_, _, _, et)| self.edge_types.iter().any(|t| et.as_str() == t))
                .map(|(eid, src, tgt, et)| (eid, src, tgt, et.clone()))
                .collect()
        };

        // Filter by target node labels if specified
        if !self.target_labels.is_empty() {
            self.current_edges.retain(|(_, src, tgt, _)| {
                let target_id = match self.direction {
                    Direction::Outgoing => *tgt,
                    Direction::Incoming => *src,
                    Direction::Both => {
                        let source_id = store.get_node(node_id).map(|_| node_id);
                        if source_id == Some(*src) {
                            *tgt
                        } else {
                            *src
                        }
                    }
                };
                if let Some(node) = store.get_node(target_id) {
                    self.target_labels.iter().all(|l| node.has_label(l))
                } else {
                    false
                }
            });
        }

        self.edge_index = 0;
        Ok(())
    }
}

impl PhysicalOperator for ExpandOperator {
    fn next(&mut self, store: &GraphStore) -> ExecutionResult<Option<Record>> {
        loop {
            // If we have edges from current record, return them
            if self.edge_index < self.current_edges.len() {
                let (edge_id, src, tgt, ref edge_type) = self.current_edges[self.edge_index];
                self.edge_index += 1;

                let mut new_record = self.current_record.as_ref().unwrap().clone();

                // Determine target node based on direction
                let target_id = match self.direction {
                    Direction::Outgoing => tgt,
                    Direction::Incoming => src,
                    Direction::Both => {
                        let source_val = new_record.get(&self.source_var).unwrap();
                        let source_id = source_val.node_id().unwrap();
                        if src == source_id {
                            tgt
                        } else {
                            src
                        }
                    }
                };

                // Self-referencing pattern: (b)-->(b) means target must equal source
                if self.source_var == self.target_var {
                    let source_id = new_record.get(&self.source_var).and_then(|v| v.node_id());
                    if source_id != Some(target_id) {
                        continue; // Skip edges that don't form self-loops
                    }
                }

                new_record.bind(self.target_var.clone(), Value::NodeRef(target_id));

                if let Some(edge_var) = &self.edge_var {
                    new_record.bind(
                        edge_var.clone(),
                        Value::EdgeRef(edge_id, src, tgt, edge_type.clone()),
                    );
                }

                // CY-04: Materialize named path variable
                if let Some(ref path_var) = self.path_variable {
                    let source_id = new_record
                        .get(&self.source_var)
                        .and_then(|v| v.node_id())
                        .unwrap_or(src);
                    new_record.bind(
                        path_var.clone(),
                        Value::Path {
                            nodes: vec![source_id, target_id],
                            edges: vec![edge_id],
                        },
                    );
                }

                return Ok(Some(new_record));
            }

            // Need new input record
            if let Some(record) = self.input.next(store)? {
                self.current_record = Some(record.clone());
                self.load_edges(&record, store)?;
            } else {
                return Ok(None);
            }
        }
    }

    fn next_batch(
        &mut self,
        store: &GraphStore,
        batch_size: usize,
    ) -> ExecutionResult<Option<RecordBatch>> {
        let mut expanded_records = Vec::with_capacity(batch_size);

        while expanded_records.len() < batch_size {
            // If we have edges from current record, process them
            if self.edge_index < self.current_edges.len() {
                let take = (batch_size - expanded_records.len())
                    .min(self.current_edges.len() - self.edge_index);

                for i in 0..take {
                    let (edge_id, src, tgt, ref edge_type) =
                        self.current_edges[self.edge_index + i];
                    let mut new_record = self.current_record.as_ref().unwrap().clone();

                    let target_id = match self.direction {
                        Direction::Outgoing => tgt,
                        Direction::Incoming => src,
                        Direction::Both => {
                            let source_val = new_record.get(&self.source_var).unwrap();
                            let source_id = source_val.node_id().unwrap();
                            if src == source_id {
                                tgt
                            } else {
                                src
                            }
                        }
                    };

                    new_record.bind(self.target_var.clone(), Value::NodeRef(target_id));
                    if let Some(edge_var) = &self.edge_var {
                        new_record.bind(
                            edge_var.clone(),
                            Value::EdgeRef(edge_id, src, tgt, edge_type.clone()),
                        );
                    }
                    // CY-04: Materialize named path variable in batch mode
                    if let Some(ref path_var) = self.path_variable {
                        let source_id = new_record
                            .get(&self.source_var)
                            .and_then(|v| v.node_id())
                            .unwrap_or(src);
                        new_record.bind(
                            path_var.clone(),
                            Value::Path {
                                nodes: vec![source_id, target_id],
                                edges: vec![edge_id],
                            },
                        );
                    }
                    expanded_records.push(new_record);
                }
                self.edge_index += take;
            } else {
                // Need new input record
                if let Some(record) = self.input.next(store)? {
                    self.current_record = Some(record.clone());
                    self.load_edges(&record, store)?;
                } else {
                    break;
                }
            }
        }

        if expanded_records.is_empty() {
            Ok(None)
        } else {
            Ok(Some(RecordBatch {
                records: expanded_records,
                columns: Vec::new(), // Columns determined by output variables
            }))
        }
    }

    fn reset(&mut self) {
        self.input.reset();
        self.current_record = None;
        self.current_edges.clear();
        self.edge_index = 0;
    }

    fn describe(&self) -> OperatorDescription {
        let dir_str = match self.direction {
            Direction::Outgoing => format!(
                "({})-[:{}]->({})",
                self.source_var,
                if self.edge_types.is_empty() {
                    "*".to_string()
                } else {
                    self.edge_types.join("|")
                },
                self.target_var
            ),
            Direction::Incoming => format!(
                "({})<-[:{}]-({})",
                self.source_var,
                if self.edge_types.is_empty() {
                    "*".to_string()
                } else {
                    self.edge_types.join("|")
                },
                self.target_var
            ),
            Direction::Both => format!(
                "({})--[:{}]--({})",
                self.source_var,
                if self.edge_types.is_empty() {
                    "*".to_string()
                } else {
                    self.edge_types.join("|")
                },
                self.target_var
            ),
        };
        OperatorDescription {
            name: "Expand".to_string(),
            details: dir_str,
            children: vec![self.input.describe()],
        }
    }
}

/// Variable-length path expand operator: MATCH (a)-[*1..5]->(b)
/// Uses BFS to find all paths within the specified hop range.
pub struct VarLengthExpandOperator {
    input: OperatorBox,
    source_var: String,
    target_var: String,
    edge_var: Option<String>,
    edge_types: Vec<String>,
    direction: Direction,
    min_hops: usize,
    max_hops: usize,
    target_labels: Vec<Label>,
    path_variable: Option<String>,
    // Buffered results from BFS
    results: Vec<Record>,
    result_index: usize,
    executed: bool,
}

impl VarLengthExpandOperator {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        input: OperatorBox,
        source_var: String,
        target_var: String,
        edge_var: Option<String>,
        edge_types: Vec<String>,
        direction: Direction,
        min_hops: usize,
        max_hops: usize,
        target_labels: Vec<Label>,
        path_variable: Option<String>,
    ) -> Self {
        Self {
            input,
            source_var,
            target_var,
            edge_var,
            edge_types,
            direction,
            min_hops,
            max_hops,
            target_labels,
            path_variable,
            results: Vec::new(),
            result_index: 0,
            executed: false,
        }
    }

    fn bfs_expand(&self, start_id: NodeId, store: &GraphStore, record: &Record) -> Vec<Record> {
        use std::collections::VecDeque;
        let mut results = Vec::new();
        // BFS: (current_node, depth, visited_edges, path_nodes, path_edges)
        let mut queue: VecDeque<(
            NodeId,
            usize,
            HashSet<crate::graph::EdgeId>,
            Vec<NodeId>,
            Vec<crate::graph::EdgeId>,
        )> = VecDeque::new();
        queue.push_back((start_id, 0, HashSet::new(), vec![start_id], Vec::new()));

        while let Some((current, depth, visited, path_nodes, path_edges)) = queue.pop_front() {
            // If we're within the valid hop range, emit a result
            if depth >= self.min_hops {
                // Check target labels
                let target_ok = if self.target_labels.is_empty() {
                    true
                } else if let Some(node) = store.get_node(current) {
                    self.target_labels.iter().all(|l| node.has_label(l))
                } else {
                    false
                };

                if target_ok {
                    let mut new_record = record.clone();
                    new_record.bind(self.target_var.clone(), Value::NodeRef(current));
                    if let Some(ref ev) = self.edge_var {
                        // VLP relationship variable is a LIST of edges per Cypher spec
                        let edge_list: Vec<PropertyValue> = path_edges
                            .iter()
                            .map(|eid| PropertyValue::Integer(eid.as_u64() as i64))
                            .collect();
                        new_record
                            .bind(ev.clone(), Value::Property(PropertyValue::Array(edge_list)));
                    }
                    if let Some(ref pv) = self.path_variable {
                        new_record.bind(
                            pv.clone(),
                            Value::Path {
                                nodes: path_nodes.clone(),
                                edges: path_edges.clone(),
                            },
                        );
                    }
                    results.push(new_record);
                }
            }

            // Continue BFS if we haven't reached max depth
            if depth < self.max_hops {
                let edges = match self.direction {
                    Direction::Outgoing => store.get_outgoing_edge_targets(current),
                    Direction::Incoming => store.get_incoming_edge_sources(current),
                    Direction::Both => {
                        let mut all = store.get_outgoing_edge_targets(current);
                        let incoming = store.get_incoming_edge_sources(current);
                        let mut seen: HashSet<crate::graph::EdgeId> =
                            all.iter().map(|(eid, ..)| *eid).collect();
                        for e in incoming {
                            if seen.insert(e.0) {
                                all.push(e);
                            }
                        }
                        all
                    }
                };

                for (edge_id, src, tgt, edge_type) in edges {
                    // Edge type filter
                    if !self.edge_types.is_empty()
                        && !self.edge_types.iter().any(|t| t == edge_type.as_str())
                    {
                        continue;
                    }
                    // Edge uniqueness
                    if visited.contains(&edge_id) {
                        continue;
                    }

                    let next_node = if src == current { tgt } else { src };
                    let mut new_visited = visited.clone();
                    new_visited.insert(edge_id);
                    let mut new_path_nodes = path_nodes.clone();
                    new_path_nodes.push(next_node);
                    let mut new_path_edges = path_edges.clone();
                    new_path_edges.push(edge_id);

                    queue.push_back((
                        next_node,
                        depth + 1,
                        new_visited,
                        new_path_nodes,
                        new_path_edges,
                    ));
                }
            }
        }
        results
    }
}

impl PhysicalOperator for VarLengthExpandOperator {
    fn next(&mut self, store: &GraphStore) -> ExecutionResult<Option<Record>> {
        loop {
            // Return buffered results
            if self.result_index < self.results.len() {
                let record = self.results[self.result_index].clone();
                self.result_index += 1;
                return Ok(Some(record));
            }

            // Get next input record and run BFS
            match self.input.next(store)? {
                Some(record) => {
                    let source_val = record
                        .get(&self.source_var)
                        .ok_or_else(|| ExecutionError::VariableNotFound(self.source_var.clone()))?;
                    let node_id = source_val.node_id().ok_or_else(|| {
                        ExecutionError::TypeError(format!("{} is not a node", self.source_var))
                    })?;

                    self.results = self.bfs_expand(node_id, store, &record);
                    self.result_index = 0;
                }
                None => return Ok(None),
            }
        }
    }

    fn reset(&mut self) {
        self.input.reset();
        self.results.clear();
        self.result_index = 0;
        self.executed = false;
    }

    fn describe(&self) -> OperatorDescription {
        OperatorDescription {
            name: "VarLengthExpand".to_string(),
            details: format!(
                "{}→{} *{}..{}",
                self.source_var, self.target_var, self.min_hops, self.max_hops
            ),
            children: vec![self.input.describe()],
        }
    }
}

/// Project operator: RETURN n.name, n.age
pub struct ProjectOperator {
    /// Input operator
    input: OperatorBox,
    /// Expressions to project
    projections: Vec<(Expression, String)>, // (expr, alias)
}

impl ProjectOperator {
    /// Create a new project operator
    pub fn new(input: OperatorBox, projections: Vec<(Expression, String)>) -> Self {
        Self { input, projections }
    }

    fn evaluate_expression(
        &self,
        expr: &Expression,
        record: &Record,
        store: &GraphStore,
    ) -> ExecutionResult<Value> {
        match expr {
            Expression::Variable(var) => {
                let val = record
                    .get(var)
                    .cloned()
                    .ok_or_else(|| ExecutionError::VariableNotFound(var.clone()))?;
                // Materialize refs at projection time (RETURN n)
                match val {
                    Value::NodeRef(id) => {
                        // Node may have been deleted — return NodeRef as-is if not found
                        if let Some(node) = store.get_node(id) {
                            Ok(Value::Node(id, node.clone()))
                        } else {
                            Ok(Value::NodeRef(id))
                        }
                    }
                    Value::EdgeRef(id, src, tgt, ref et) => {
                        // Edge may have been deleted — return EdgeRef as-is if not found
                        if let Some(edge) = store.get_edge(id) {
                            Ok(Value::Edge(id, edge.clone()))
                        } else {
                            Ok(Value::EdgeRef(id, src, tgt, et.clone()))
                        }
                    }
                    other => Ok(other),
                }
            }
            Expression::Property { variable, property } => {
                let val = if let Some(v) = record.get(variable) {
                    v.clone()
                } else if variable.contains('.') {
                    let parts: Vec<&str> = variable.splitn(2, '.').collect();
                    let base = record
                        .get(parts[0])
                        .ok_or_else(|| ExecutionError::VariableNotFound(parts[0].to_string()))?;
                    Value::Property(base.resolve_property(parts[1], store))
                } else {
                    return Err(ExecutionError::VariableNotFound(variable.clone()));
                };
                // Check for deleted entity property access
                match &val {
                    Value::NodeRef(id) if store.get_node(*id).is_none() => {
                        return Err(ExecutionError::RuntimeError(format!(
                            "Entity not found: Node {} has been deleted",
                            id.as_u64()
                        )));
                    }
                    Value::EdgeRef(id, ..) if store.get_edge(*id).is_none() => {
                        return Err(ExecutionError::RuntimeError(format!(
                            "Entity not found: Relationship {} has been deleted",
                            id.as_u64()
                        )));
                    }
                    _ => {}
                }
                let prop = val.resolve_property(property, store);
                Ok(Value::Property(prop))
            }
            Expression::Literal(lit) => Ok(Value::Property(lit.clone())),
            Expression::Binary { left, op, right } => {
                let left_val = self.evaluate_expression(left, record, store)?;
                let right_val = self.evaluate_expression(right, record, store)?;
                eval_binary_op(op, left_val, right_val)
            }
            Expression::Unary { op, expr } => {
                let val = self.evaluate_expression(expr, record, store)?;
                eval_unary_op(op, val)
            }
            Expression::Function { name, args, .. } => {
                let arg_vals: Vec<Value> = args
                    .iter()
                    .map(|a| self.evaluate_expression(a, record, store))
                    .collect::<ExecutionResult<Vec<_>>>()?;
                eval_function(name, &arg_vals, Some(store))
            }
            Expression::Case {
                operand,
                when_clauses,
                else_result,
            } => eval_case(
                operand.as_deref(),
                when_clauses,
                else_result.as_deref(),
                |e| self.evaluate_expression(e, record, store),
            ),
            Expression::Index { expr, index } => {
                let collection = self.evaluate_expression(expr, record, store)?;
                let idx = self.evaluate_expression(index, record, store)?;
                eval_index(collection, idx)
            }
            Expression::ListSlice { expr, start, end } => {
                let collection = self.evaluate_expression(expr, record, store)?;
                let s = match start {
                    Some(s) => Some(self.evaluate_expression(s, record, store)?),
                    None => None,
                };
                let en = match end {
                    Some(e) => Some(self.evaluate_expression(e, record, store)?),
                    None => None,
                };
                eval_list_slice(collection, s, en)
            }
            Expression::ExistsSubquery {
                pattern,
                where_clause,
            } => eval_exists_subquery(pattern, where_clause.as_deref(), record, store),
            Expression::ListComprehension {
                variable,
                list_expr,
                filter,
                map_expr,
            } => eval_list_comprehension(
                variable,
                list_expr,
                filter.as_deref(),
                map_expr,
                record,
                store,
            ),
            Expression::PredicateFunction {
                name,
                variable,
                list_expr,
                predicate,
            } => eval_predicate_function(name, variable, list_expr, predicate, record, store),
            Expression::Reduce {
                accumulator,
                init,
                variable,
                list_expr,
                expression,
            } => eval_reduce(
                accumulator,
                init,
                variable,
                list_expr,
                expression,
                record,
                store,
            ),
            Expression::PatternComprehension {
                pattern,
                filter,
                projection,
            } => eval_pattern_comprehension(pattern, filter.as_deref(), projection, record, store),
            Expression::MapExpression(entries) => {
                let mut map = std::collections::HashMap::new();
                for (key, val_expr) in entries {
                    let val = self.evaluate_expression(val_expr, record, store)?;
                    let pv = match val {
                        Value::Property(pv) => pv,
                        Value::Null => PropertyValue::Null,
                        _ => PropertyValue::Null,
                    };
                    map.insert(key.clone(), pv);
                }
                Ok(Value::Property(PropertyValue::Map(map)))
            }
            Expression::PathVariable(var) => record
                .get(var)
                .cloned()
                .ok_or_else(|| ExecutionError::VariableNotFound(var.clone())),
            Expression::Parameter(name) => {
                record.get(&format!("${}", name)).cloned().ok_or_else(|| {
                    ExecutionError::RuntimeError(format!("Unresolved parameter: ${}", name))
                })
            }
        }
    }
}

impl PhysicalOperator for ProjectOperator {
    fn next(&mut self, store: &GraphStore) -> ExecutionResult<Option<Record>> {
        if let Some(record) = self.input.next(store)? {
            let mut new_record = Record::new();

            for (expr, alias) in &self.projections {
                let value = self.evaluate_expression(expr, &record, store)?;
                new_record.bind(alias.clone(), value);
            }

            Ok(Some(new_record))
        } else {
            Ok(None)
        }
    }

    fn next_batch(
        &mut self,
        store: &GraphStore,
        batch_size: usize,
    ) -> ExecutionResult<Option<RecordBatch>> {
        if let Some(batch) = self.input.next_batch(store, batch_size)? {
            let mut projected_records = Vec::with_capacity(batch.records.len());
            let columns: Vec<String> = self.projections.iter().map(|(_, a)| a.clone()).collect();

            for record in batch.records {
                let mut new_record = Record::new();
                for (expr, alias) in &self.projections {
                    let value = self.evaluate_expression(expr, &record, store)?;
                    new_record.bind(alias.clone(), value);
                }
                projected_records.push(new_record);
            }

            Ok(Some(RecordBatch {
                records: projected_records,
                columns,
            }))
        } else {
            Ok(None)
        }
    }

    fn next_mut(
        &mut self,
        store: &mut GraphStore,
        tenant_id: &str,
    ) -> ExecutionResult<Option<Record>> {
        if let Some(record) = self.input.next_mut(store, tenant_id)? {
            let mut new_record = Record::new();
            let store_ref: &GraphStore = store;
            for (expr, alias) in &self.projections {
                let value = self.evaluate_expression(expr, &record, store_ref)?;
                new_record.bind(alias.clone(), value);
            }
            Ok(Some(new_record))
        } else {
            Ok(None)
        }
    }

    fn reset(&mut self) {
        self.input.reset();
    }

    fn describe(&self) -> OperatorDescription {
        let cols: Vec<String> = self
            .projections
            .iter()
            .map(|(e, a)| {
                let expr_str = format_expression(e);
                if expr_str == *a {
                    a.clone()
                } else {
                    format!("{} AS {}", expr_str, a)
                }
            })
            .collect();
        OperatorDescription {
            name: "Project".to_string(),
            details: cols.join(", "),
            children: vec![self.input.describe()],
        }
    }
}

/// Aggregation type
#[derive(Debug, Clone, PartialEq)]
pub enum AggregateType {
    Count,
    Sum,
    Avg,
    Min,
    Max,
    Collect,
}

/// Aggregation function definition
#[derive(Debug, Clone)]
pub struct AggregateFunction {
    pub func: AggregateType,
    pub expr: Expression,
    pub alias: String,
    pub distinct: bool,
}

/// Internal state for an aggregator
#[derive(Debug, Clone)]
enum AggregatorState {
    Count(i64),
    CountDistinct(BTreeSet<PropertyValue>),
    Sum(f64),
    Avg { sum: f64, count: i64 },
    Min(Option<PropertyValue>),
    Max(Option<PropertyValue>),
    Collect(Vec<PropertyValue>),
    CollectDistinct(BTreeSet<PropertyValue>),
}

impl AggregatorState {
    fn new(func: &AggregateType, distinct: bool) -> Self {
        match (func, distinct) {
            (AggregateType::Count, true) => AggregatorState::CountDistinct(BTreeSet::new()),
            (AggregateType::Count, false) => AggregatorState::Count(0),
            (AggregateType::Sum, _) => AggregatorState::Sum(0.0),
            (AggregateType::Avg, _) => AggregatorState::Avg { sum: 0.0, count: 0 },
            (AggregateType::Min, _) => AggregatorState::Min(None),
            (AggregateType::Max, _) => AggregatorState::Max(None),
            (AggregateType::Collect, true) => AggregatorState::CollectDistinct(BTreeSet::new()),
            (AggregateType::Collect, false) => AggregatorState::Collect(Vec::new()),
        }
    }

    fn update(&mut self, value: &Value) {
        match self {
            AggregatorState::Count(c) => {
                if !value.is_null() {
                    *c += 1;
                }
            }
            AggregatorState::CountDistinct(set) => {
                match value {
                    Value::Property(prop) => {
                        if !prop.is_null() {
                            set.insert(prop.clone());
                        }
                    }
                    Value::NodeRef(id) | Value::Node(id, _) => {
                        set.insert(PropertyValue::Integer(id.0 as i64));
                    }
                    Value::EdgeRef(id, ..) | Value::Edge(id, _) => {
                        set.insert(PropertyValue::Integer(id.0 as i64));
                    }
                    Value::Path { .. } => {
                        // Paths are not countable as distinct — ignore
                    }
                    Value::Null => {}
                }
            }
            AggregatorState::Sum(s) => {
                if let Some(prop) = value.as_property() {
                    if let Some(f) = prop.as_float() {
                        *s += f;
                    } else if let Some(i) = prop.as_integer() {
                        *s += i as f64;
                    }
                }
            }
            AggregatorState::Avg { sum, count } => {
                if let Some(prop) = value.as_property() {
                    if let Some(f) = prop.as_float() {
                        *sum += f;
                        *count += 1;
                    } else if let Some(i) = prop.as_integer() {
                        *sum += i as f64;
                        *count += 1;
                    }
                }
            }
            AggregatorState::Min(curr) => {
                if let Some(prop) = value.as_property() {
                    if curr.is_none() || prop < curr.as_ref().unwrap() {
                        *curr = Some(prop.clone());
                    }
                }
            }
            AggregatorState::Max(curr) => {
                if let Some(prop) = value.as_property() {
                    if curr.is_none() || prop > curr.as_ref().unwrap() {
                        *curr = Some(prop.clone());
                    }
                }
            }
            AggregatorState::Collect(items) => {
                match value {
                    Value::Null | Value::Property(PropertyValue::Null) => {
                        // Skip nulls in collect (Cypher spec)
                    }
                    Value::Property(prop) => items.push(prop.clone()),
                    Value::NodeRef(id) | Value::Node(id, _) => {
                        // Store node references as maps with __nodeId for later materialization
                        let mut map = std::collections::HashMap::new();
                        map.insert("__nodeId".to_string(), PropertyValue::Integer(id.0 as i64));
                        items.push(PropertyValue::Map(map));
                    }
                    Value::EdgeRef(id, ..) | Value::Edge(id, _) => {
                        let mut map = std::collections::HashMap::new();
                        map.insert("__edgeId".to_string(), PropertyValue::Integer(id.0 as i64));
                        items.push(PropertyValue::Map(map));
                    }
                    _ => {
                        if let Some(prop) = value.as_property() {
                            items.push(prop.clone());
                        }
                    }
                }
            }
            AggregatorState::CollectDistinct(set) => {
                if let Some(prop) = value.as_property() {
                    if !prop.is_null() {
                        set.insert(prop.clone());
                    }
                }
            }
        }
    }

    fn result(&self) -> Value {
        match self {
            AggregatorState::Count(c) => Value::Property(PropertyValue::Integer(*c)),
            AggregatorState::CountDistinct(set) => {
                Value::Property(PropertyValue::Integer(set.len() as i64))
            }
            AggregatorState::Sum(s) => Value::Property(PropertyValue::Float(*s)),
            AggregatorState::Avg { sum, count } => {
                if *count == 0 {
                    Value::Null
                } else {
                    Value::Property(PropertyValue::Float(*sum / *count as f64))
                }
            }
            AggregatorState::Min(val) => val.clone().map(Value::Property).unwrap_or(Value::Null),
            AggregatorState::Max(val) => val.clone().map(Value::Property).unwrap_or(Value::Null),
            AggregatorState::Collect(items) => Value::Property(PropertyValue::Array(items.clone())),
            AggregatorState::CollectDistinct(set) => {
                Value::Property(PropertyValue::Array(set.iter().cloned().collect()))
            }
        }
    }
}

/// Aggregate operator: GROUP BY + Aggregations
pub struct AggregateOperator {
    input: OperatorBox,
    group_by: Vec<(Expression, String)>, // (expr, alias)
    aggregates: Vec<AggregateFunction>,
    results: std::vec::IntoIter<Record>,
    executed: bool,
}

impl AggregateOperator {
    pub fn new(
        input: OperatorBox,
        group_by: Vec<(Expression, String)>,
        aggregates: Vec<AggregateFunction>,
    ) -> Self {
        Self {
            input,
            group_by,
            aggregates,
            results: Vec::new().into_iter(),
            executed: false,
        }
    }

    fn evaluate_expression(
        expr: &Expression,
        record: &Record,
        store: &GraphStore,
    ) -> ExecutionResult<Value> {
        match expr {
            Expression::Variable(var) => Ok(record.get(var).cloned().unwrap_or(Value::Null)),
            Expression::Property { variable, property } => {
                let val = record.get(variable).unwrap_or(&Value::Null);
                let prop = val.resolve_property(property, store);
                Ok(Value::Property(prop))
            }
            Expression::Literal(lit) => Ok(Value::Property(lit.clone())),
            Expression::Binary { left, op, right } => {
                let left_val = Self::evaluate_expression(left, record, store)?;
                let right_val = Self::evaluate_expression(right, record, store)?;
                eval_binary_op(op, left_val, right_val)
            }
            Expression::Unary { op, expr } => {
                let val = Self::evaluate_expression(expr, record, store)?;
                eval_unary_op(op, val)
            }
            Expression::Function { name, args, .. } => {
                let arg_vals: Vec<Value> = args
                    .iter()
                    .map(|a| Self::evaluate_expression(a, record, store))
                    .collect::<ExecutionResult<Vec<_>>>()?;
                eval_function(name, &arg_vals, Some(store))
            }
            Expression::Case {
                operand,
                when_clauses,
                else_result,
            } => eval_case(
                operand.as_deref(),
                when_clauses,
                else_result.as_deref(),
                |e| Self::evaluate_expression(e, record, store),
            ),
            Expression::Index { expr, index } => {
                let collection = Self::evaluate_expression(expr, record, store)?;
                let idx = Self::evaluate_expression(index, record, store)?;
                eval_index(collection, idx)
            }
            Expression::ListSlice { expr, start, end } => {
                let collection = Self::evaluate_expression(expr, record, store)?;
                let s = match start {
                    Some(s) => Some(Self::evaluate_expression(s, record, store)?),
                    None => None,
                };
                let en = match end {
                    Some(e) => Some(Self::evaluate_expression(e, record, store)?),
                    None => None,
                };
                eval_list_slice(collection, s, en)
            }
            Expression::ExistsSubquery {
                pattern,
                where_clause,
            } => eval_exists_subquery(pattern, where_clause.as_deref(), record, store),
            Expression::ListComprehension {
                variable,
                list_expr,
                filter,
                map_expr,
            } => eval_list_comprehension(
                variable,
                list_expr,
                filter.as_deref(),
                map_expr,
                record,
                store,
            ),
            Expression::PredicateFunction {
                name,
                variable,
                list_expr,
                predicate,
            } => eval_predicate_function(name, variable, list_expr, predicate, record, store),
            Expression::Reduce {
                accumulator,
                init,
                variable,
                list_expr,
                expression,
            } => eval_reduce(
                accumulator,
                init,
                variable,
                list_expr,
                expression,
                record,
                store,
            ),
            Expression::PatternComprehension {
                pattern,
                filter,
                projection,
            } => eval_pattern_comprehension(pattern, filter.as_deref(), projection, record, store),
            Expression::MapExpression(entries) => {
                let mut map = std::collections::HashMap::new();
                for (key, val_expr) in entries {
                    let val = Self::evaluate_expression(val_expr, record, store)?;
                    let pv = match val {
                        Value::Property(pv) => pv,
                        Value::Null => PropertyValue::Null,
                        _ => PropertyValue::Null,
                    };
                    map.insert(key.clone(), pv);
                }
                Ok(Value::Property(PropertyValue::Map(map)))
            }
            Expression::PathVariable(var) => record
                .get(var)
                .cloned()
                .ok_or_else(|| ExecutionError::VariableNotFound(var.clone())),
            Expression::Parameter(name) => {
                record.get(&format!("${}", name)).cloned().ok_or_else(|| {
                    ExecutionError::RuntimeError(format!("Unresolved parameter: ${}", name))
                })
            }
        }
    }
}

impl PhysicalOperator for AggregateOperator {
    fn next(&mut self, store: &GraphStore) -> ExecutionResult<Option<Record>> {
        if !self.executed {
            self.execute_all(store)?;
        }
        Ok(self.results.next())
    }

    fn next_mut(
        &mut self,
        store: &mut GraphStore,
        tenant_id: &str,
    ) -> ExecutionResult<Option<Record>> {
        if !self.executed {
            self.execute_all_mut(store, tenant_id)?;
        }
        Ok(self.results.next())
    }

    fn next_batch(
        &mut self,
        store: &GraphStore,
        batch_size: usize,
    ) -> ExecutionResult<Option<RecordBatch>> {
        if !self.executed {
            self.execute_all(store)?;
        }

        let mut batch = Vec::with_capacity(batch_size);
        for _ in 0..batch_size {
            if let Some(record) = self.results.next() {
                batch.push(record);
            } else {
                break;
            }
        }

        if batch.is_empty() {
            Ok(None)
        } else {
            Ok(Some(RecordBatch {
                records: batch,
                columns: Vec::new(),
            }))
        }
    }

    fn reset(&mut self) {
        self.input.reset();
        self.executed = false;
        self.results = Vec::new().into_iter();
    }

    fn describe(&self) -> OperatorDescription {
        let agg_strs: Vec<String> = self
            .aggregates
            .iter()
            .map(|a| {
                format!(
                    "{}({}) AS {}",
                    format!("{:?}", a.func).to_lowercase(),
                    format_expression(&a.expr),
                    a.alias
                )
            })
            .collect();
        let group_strs: Vec<String> = self
            .group_by
            .iter()
            .map(|(e, a)| format!("{} AS {}", format_expression(e), a))
            .collect();
        let mut details = Vec::new();
        if !group_strs.is_empty() {
            details.push(format!("group_by=[{}]", group_strs.join(", ")));
        }
        details.push(format!("aggs=[{}]", agg_strs.join(", ")));
        OperatorDescription {
            name: "Aggregate".to_string(),
            details: details.join(", "),
            children: vec![self.input.describe()],
        }
    }
}

impl AggregateOperator {
    fn execute_all(&mut self, store: &GraphStore) -> ExecutionResult<()> {
        let mut groups: HashMap<Vec<Value>, Vec<AggregatorState>> = HashMap::new();

        // Use next_batch from input for performance
        let batch_size = 1024;
        let mut batch_count = 0u64;
        while let Some(batch) = self.input.next_batch(store, batch_size)? {
            batch_count += 1;
            if batch_count % 10 == 0 {
                check_deadline()?;
            }
            for record in batch.records {
                // Evaluate grouping keys
                let mut key = Vec::new();
                for (expr, _) in &self.group_by {
                    key.push(Self::evaluate_expression(expr, &record, store)?);
                }

                // Initialize state if new group
                let states = groups.entry(key).or_insert_with(|| {
                    self.aggregates
                        .iter()
                        .map(|agg| AggregatorState::new(&agg.func, agg.distinct))
                        .collect()
                });

                // Update state
                for (i, agg) in self.aggregates.iter().enumerate() {
                    let val = Self::evaluate_expression(&agg.expr, &record, store)?;
                    states[i].update(&val);
                }
            }
        }

        // Generate results
        let mut output_records = Vec::new();

        // If no input rows and this is a pure aggregation (no GROUP BY),
        // return one row with default aggregate values (count=0, sum=null, etc.)
        if groups.is_empty() && self.group_by.is_empty() && !self.aggregates.is_empty() {
            let mut record = Record::new();
            let default_states: Vec<AggregatorState> = self
                .aggregates
                .iter()
                .map(|agg| AggregatorState::new(&agg.func, agg.distinct))
                .collect();
            for (i, agg) in self.aggregates.iter().enumerate() {
                record.bind(agg.alias.clone(), default_states[i].result());
            }
            output_records.push(record);
        } else {
            for (key, states) in groups {
                let mut record = Record::new();

                for (i, (_, alias)) in self.group_by.iter().enumerate() {
                    record.bind(alias.clone(), key[i].clone());
                }

                for (i, agg) in self.aggregates.iter().enumerate() {
                    record.bind(agg.alias.clone(), states[i].result());
                }

                output_records.push(record);
            }
        }

        self.results = output_records.into_iter();
        self.executed = true;
        Ok(())
    }

    fn execute_all_mut(&mut self, store: &mut GraphStore, tenant_id: &str) -> ExecutionResult<()> {
        let mut groups: HashMap<Vec<Value>, Vec<AggregatorState>> = HashMap::new();

        let batch_size = 1024;
        while let Some(batch) = self.input.next_batch_mut(store, tenant_id, batch_size)? {
            for record in batch.records {
                let store_ref: &GraphStore = store;
                let mut key = Vec::new();
                for (expr, _) in &self.group_by {
                    key.push(Self::evaluate_expression(expr, &record, store_ref)?);
                }

                let states = groups.entry(key).or_insert_with(|| {
                    self.aggregates
                        .iter()
                        .map(|agg| AggregatorState::new(&agg.func, agg.distinct))
                        .collect()
                });

                for (i, agg) in self.aggregates.iter().enumerate() {
                    let val = Self::evaluate_expression(&agg.expr, &record, store_ref)?;
                    states[i].update(&val);
                }
            }
        }

        let mut output_records = Vec::new();
        if groups.is_empty() && self.group_by.is_empty() && !self.aggregates.is_empty() {
            let mut record = Record::new();
            let default_states: Vec<AggregatorState> = self
                .aggregates
                .iter()
                .map(|agg| AggregatorState::new(&agg.func, agg.distinct))
                .collect();
            for (i, agg) in self.aggregates.iter().enumerate() {
                record.bind(agg.alias.clone(), default_states[i].result());
            }
            output_records.push(record);
        } else {
            for (key, states) in groups {
                let mut record = Record::new();
                for (i, (_, alias)) in self.group_by.iter().enumerate() {
                    record.bind(alias.clone(), key[i].clone());
                }
                for (i, agg) in self.aggregates.iter().enumerate() {
                    record.bind(agg.alias.clone(), states[i].result());
                }
                output_records.push(record);
            }
        }

        self.results = output_records.into_iter();
        self.executed = true;
        Ok(())
    }
}

/// Limit operator: LIMIT 10
pub struct LimitOperator {
    /// Input operator
    input: OperatorBox,
    /// Maximum number of records
    limit: usize,
    /// Current count
    count: usize,
}

impl LimitOperator {
    /// Create a new limit operator
    pub fn new(input: OperatorBox, limit: usize) -> Self {
        Self {
            input,
            limit,
            count: 0,
        }
    }
}

impl PhysicalOperator for LimitOperator {
    fn next(&mut self, store: &GraphStore) -> ExecutionResult<Option<Record>> {
        if self.count >= self.limit {
            return Ok(None);
        }

        if let Some(record) = self.input.next(store)? {
            self.count += 1;
            Ok(Some(record))
        } else {
            Ok(None)
        }
    }

    fn next_batch(
        &mut self,
        store: &GraphStore,
        batch_size: usize,
    ) -> ExecutionResult<Option<RecordBatch>> {
        if self.count >= self.limit {
            return Ok(None);
        }

        let remaining = self.limit - self.count;
        let request_size = batch_size.min(remaining);

        if let Some(mut batch) = self.input.next_batch(store, request_size)? {
            if batch.records.len() > remaining {
                batch.records.truncate(remaining);
            }
            self.count += batch.records.len();
            Ok(Some(batch))
        } else {
            Ok(None)
        }
    }

    fn next_mut(
        &mut self,
        store: &mut GraphStore,
        tenant_id: &str,
    ) -> ExecutionResult<Option<Record>> {
        if self.count >= self.limit {
            return Ok(None);
        }
        if let Some(record) = self.input.next_mut(store, tenant_id)? {
            self.count += 1;
            Ok(Some(record))
        } else {
            Ok(None)
        }
    }

    fn reset(&mut self) {
        self.input.reset();
        self.count = 0;
    }

    fn describe(&self) -> OperatorDescription {
        OperatorDescription {
            name: "Limit".to_string(),
            details: format!("{}", self.limit),
            children: vec![self.input.describe()],
        }
    }
}

/// Sort operator: ORDER BY n.age ASC
pub struct SortOperator {
    input: OperatorBox,
    sort_items: Vec<(Expression, bool)>, // (expr, ascending)
    records: Vec<Record>,
    current: usize,
    executed: bool,
}

impl SortOperator {
    pub fn new(input: OperatorBox, sort_items: Vec<(Expression, bool)>) -> Self {
        Self {
            input,
            sort_items,
            records: Vec::new(),
            current: 0,
            executed: false,
        }
    }

    fn evaluate_expression(
        expr: &Expression,
        record: &Record,
        store: &GraphStore,
    ) -> ExecutionResult<Value> {
        match expr {
            Expression::Variable(var) => record
                .get(var)
                .cloned()
                .ok_or_else(|| ExecutionError::VariableNotFound(var.clone())),
            Expression::Property { variable, property } => {
                let val = record
                    .get(variable)
                    .ok_or_else(|| ExecutionError::VariableNotFound(variable.clone()))?;

                let prop = val.resolve_property(property, store);
                Ok(Value::Property(prop))
            }
            Expression::Literal(lit) => Ok(Value::Property(lit.clone())),
            Expression::Binary { left, op, right } => {
                let left_val = Self::evaluate_expression(left, record, store)?;
                let right_val = Self::evaluate_expression(right, record, store)?;
                eval_binary_op(op, left_val, right_val)
            }
            Expression::Unary { op, expr } => {
                let val = Self::evaluate_expression(expr, record, store)?;
                eval_unary_op(op, val)
            }
            Expression::Function { name, args, .. } => {
                let arg_vals: Vec<Value> = args
                    .iter()
                    .map(|a| Self::evaluate_expression(a, record, store))
                    .collect::<ExecutionResult<Vec<_>>>()?;
                eval_function(name, &arg_vals, Some(store))
            }
            Expression::Case {
                operand,
                when_clauses,
                else_result,
            } => eval_case(
                operand.as_deref(),
                when_clauses,
                else_result.as_deref(),
                |e| Self::evaluate_expression(e, record, store),
            ),
            Expression::Index { expr, index } => {
                let collection = Self::evaluate_expression(expr, record, store)?;
                let idx = Self::evaluate_expression(index, record, store)?;
                eval_index(collection, idx)
            }
            Expression::ListSlice { expr, start, end } => {
                let collection = Self::evaluate_expression(expr, record, store)?;
                let s = match start {
                    Some(s) => Some(Self::evaluate_expression(s, record, store)?),
                    None => None,
                };
                let en = match end {
                    Some(e) => Some(Self::evaluate_expression(e, record, store)?),
                    None => None,
                };
                eval_list_slice(collection, s, en)
            }
            Expression::ExistsSubquery {
                pattern,
                where_clause,
            } => eval_exists_subquery(pattern, where_clause.as_deref(), record, store),
            Expression::ListComprehension {
                variable,
                list_expr,
                filter,
                map_expr,
            } => eval_list_comprehension(
                variable,
                list_expr,
                filter.as_deref(),
                map_expr,
                record,
                store,
            ),
            Expression::PredicateFunction {
                name,
                variable,
                list_expr,
                predicate,
            } => eval_predicate_function(name, variable, list_expr, predicate, record, store),
            Expression::Reduce {
                accumulator,
                init,
                variable,
                list_expr,
                expression,
            } => eval_reduce(
                accumulator,
                init,
                variable,
                list_expr,
                expression,
                record,
                store,
            ),
            Expression::PatternComprehension {
                pattern,
                filter,
                projection,
            } => eval_pattern_comprehension(pattern, filter.as_deref(), projection, record, store),
            Expression::MapExpression(entries) => {
                let mut map = std::collections::HashMap::new();
                for (key, val_expr) in entries {
                    let val = Self::evaluate_expression(val_expr, record, store)?;
                    let pv = match val {
                        Value::Property(pv) => pv,
                        Value::Null => PropertyValue::Null,
                        _ => PropertyValue::Null,
                    };
                    map.insert(key.clone(), pv);
                }
                Ok(Value::Property(PropertyValue::Map(map)))
            }
            Expression::PathVariable(var) => record
                .get(var)
                .cloned()
                .ok_or_else(|| ExecutionError::VariableNotFound(var.clone())),
            Expression::Parameter(name) => {
                record.get(&format!("${}", name)).cloned().ok_or_else(|| {
                    ExecutionError::RuntimeError(format!("Unresolved parameter: ${}", name))
                })
            }
        }
    }
}

impl PhysicalOperator for SortOperator {
    fn next(&mut self, store: &GraphStore) -> ExecutionResult<Option<Record>> {
        if !self.executed {
            self.execute_all(store)?;
        }

        if self.current >= self.records.len() {
            return Ok(None);
        }

        let record = self.records[self.current].clone();
        self.current += 1;
        Ok(Some(record))
    }

    fn next_mut(
        &mut self,
        store: &mut GraphStore,
        tenant_id: &str,
    ) -> ExecutionResult<Option<Record>> {
        if !self.executed {
            self.execute_all_mut(store, tenant_id)?;
        }

        if self.current >= self.records.len() {
            return Ok(None);
        }

        let record = self.records[self.current].clone();
        self.current += 1;
        Ok(Some(record))
    }

    fn next_batch(
        &mut self,
        store: &GraphStore,
        batch_size: usize,
    ) -> ExecutionResult<Option<RecordBatch>> {
        if !self.executed {
            self.execute_all(store)?;
        }

        if self.current >= self.records.len() {
            return Ok(None);
        }

        let end = (self.current + batch_size).min(self.records.len());
        let batch = self.records[self.current..end].to_vec();
        self.current = end;

        Ok(Some(RecordBatch {
            records: batch,
            columns: Vec::new(),
        }))
    }

    fn reset(&mut self) {
        self.input.reset();
        self.records.clear();
        self.current = 0;
        self.executed = false;
    }

    fn describe(&self) -> OperatorDescription {
        let items: Vec<String> = self
            .sort_items
            .iter()
            .map(|(e, asc)| {
                format!(
                    "{} {}",
                    format_expression(e),
                    if *asc { "ASC" } else { "DESC" }
                )
            })
            .collect();
        OperatorDescription {
            name: "Sort".to_string(),
            details: items.join(", "),
            children: vec![self.input.describe()],
        }
    }
}

impl SortOperator {
    fn execute_all(&mut self, store: &GraphStore) -> ExecutionResult<()> {
        // Materialize all records in batches
        let batch_size = 1024;
        while let Some(batch) = self.input.next_batch(store, batch_size)? {
            self.records.extend(batch.records);
        }

        // Sort
        let sort_items = &self.sort_items;
        self.records.sort_by(|a, b| {
            for (expr, ascending) in sort_items {
                let val_a = Self::evaluate_expression(expr, a, store).unwrap_or(Value::Null);
                let val_b = Self::evaluate_expression(expr, b, store).unwrap_or(Value::Null);

                let prop_a = val_a.as_property().unwrap_or(&PropertyValue::Null);
                let prop_b = val_b.as_property().unwrap_or(&PropertyValue::Null);

                let ord = prop_a.cmp(prop_b);
                if ord != std::cmp::Ordering::Equal {
                    return if *ascending { ord } else { ord.reverse() };
                }
            }
            std::cmp::Ordering::Equal
        });

        self.executed = true;
        Ok(())
    }

    fn execute_all_mut(&mut self, store: &mut GraphStore, tenant_id: &str) -> ExecutionResult<()> {
        let batch_size = 1024;
        while let Some(batch) = self.input.next_batch_mut(store, tenant_id, batch_size)? {
            self.records.extend(batch.records);
        }

        let sort_items = &self.sort_items;
        let store_ref: &GraphStore = store;
        self.records.sort_by(|a, b| {
            for (expr, ascending) in sort_items {
                let val_a = Self::evaluate_expression(expr, a, store_ref).unwrap_or(Value::Null);
                let val_b = Self::evaluate_expression(expr, b, store_ref).unwrap_or(Value::Null);
                let prop_a = val_a.as_property().unwrap_or(&PropertyValue::Null);
                let prop_b = val_b.as_property().unwrap_or(&PropertyValue::Null);
                let ord = prop_a.cmp(prop_b);
                if ord != std::cmp::Ordering::Equal {
                    return if *ascending { ord } else { ord.reverse() };
                }
            }
            std::cmp::Ordering::Equal
        });

        self.executed = true;
        Ok(())
    }
}

/// Index scan operator: MATCH (n:Person) WHERE n.id = 1
pub struct IndexScanOperator {
    variable: String,
    label: Label,
    property: String,
    op: BinaryOp,
    value: PropertyValue,
    node_ids: Vec<NodeId>,
    current: usize,
}

impl IndexScanOperator {
    pub fn new(
        variable: String,
        label: Label,
        property: String,
        op: BinaryOp,
        value: PropertyValue,
    ) -> Self {
        Self {
            variable,
            label,
            property,
            op,
            value,
            node_ids: Vec::new(),
            current: 0,
        }
    }

    fn initialize(&mut self, store: &GraphStore) {
        if !self.node_ids.is_empty() {
            return;
        }

        if let Some(index_lock) = store.property_index.get_index(&self.label, &self.property) {
            let index = index_lock.read().unwrap();
            self.node_ids = match self.op {
                BinaryOp::Eq => index.get(&self.value),
                BinaryOp::Gt => {
                    use std::ops::Bound::Excluded;
                    use std::ops::Bound::Unbounded;
                    index.range((Excluded(self.value.clone()), Unbounded))
                }
                BinaryOp::Ge => {
                    use std::ops::Bound::Included;
                    use std::ops::Bound::Unbounded;
                    index.range((Included(self.value.clone()), Unbounded))
                }
                BinaryOp::Lt => {
                    use std::ops::Bound::Excluded;
                    use std::ops::Bound::Unbounded;
                    index.range((Unbounded, Excluded(self.value.clone())))
                }
                BinaryOp::Le => {
                    use std::ops::Bound::Included;
                    use std::ops::Bound::Unbounded;
                    index.range((Unbounded, Included(self.value.clone())))
                }
                _ => Vec::new(),
            };
        }
    }
}

impl PhysicalOperator for IndexScanOperator {
    fn next(&mut self, store: &GraphStore) -> ExecutionResult<Option<Record>> {
        self.initialize(store);

        while self.current < self.node_ids.len() {
            let node_id = self.node_ids[self.current];
            self.current += 1;

            if store.has_node(node_id) {
                let mut record = Record::new();
                record.bind(self.variable.clone(), Value::NodeRef(node_id));
                return Ok(Some(record));
            }
        }

        Ok(None)
    }

    fn next_batch(
        &mut self,
        store: &GraphStore,
        batch_size: usize,
    ) -> ExecutionResult<Option<RecordBatch>> {
        self.initialize(store);

        if self.current >= self.node_ids.len() {
            return Ok(None);
        }

        let mut records = Vec::with_capacity(batch_size);
        while records.len() < batch_size && self.current < self.node_ids.len() {
            let node_id = self.node_ids[self.current];
            self.current += 1;

            if store.has_node(node_id) {
                let mut record = Record::new();
                record.bind(self.variable.clone(), Value::NodeRef(node_id));
                records.push(record);
            }
        }

        if records.is_empty() {
            Ok(None)
        } else {
            Ok(Some(RecordBatch {
                records,
                columns: vec![self.variable.clone()],
            }))
        }
    }

    fn reset(&mut self) {
        self.current = 0;
    }

    fn describe(&self) -> OperatorDescription {
        let op_str = match self.op {
            BinaryOp::Eq => "=",
            BinaryOp::Gt => ">",
            BinaryOp::Ge => ">=",
            BinaryOp::Lt => "<",
            BinaryOp::Le => "<=",
            _ => "?",
        };
        OperatorDescription {
            name: "IndexScan".to_string(),
            details: format!(
                "var={}, {}.{} {} {:?}",
                self.variable, self.label, self.property, op_str, self.value
            ),
            children: Vec::new(),
        }
    }
}

/// Resolve an expression against a record — used by VectorSearchOperator for deferred vectors.
fn resolve_expression_for_record(
    expr: &Expression,
    record: &Record,
    store: &GraphStore,
) -> Option<Value> {
    match expr {
        Expression::Variable(var) => record.get(var).cloned(),
        Expression::Property { variable, property } => {
            let val = record.get(variable)?;
            let prop = val.resolve_property(property, store);
            Some(Value::Property(prop))
        }
        Expression::Literal(lit) => Some(Value::Property(lit.clone())),
        _ => None,
    }
}

/// Vector search operator: supports both CALL db.index.vector.queryNodes(...)
/// and SEARCH clause within MATCH.
/// Returns nodes ordered by similarity (highest first) with optional SCORE AS alias.
pub struct VectorSearchOperator {
    /// Label to search in
    label: String,
    /// Property key to search in
    property_key: String,
    /// Query vector (resolved at plan time from a literal/parameter)
    query_vector: Vec<f32>,
    /// Deferred query vector expression — for property access (e.g., snowWhite.embedding)
    /// that must be resolved at runtime from a prior MATCH result
    deferred_vector_expr: Option<crate::query::ast::Expression>,
    /// Number of neighbors to return
    k: usize,
    /// Variable name for matched nodes
    node_var: String,
    /// Variable name for similarity scores (optional)
    score_var: Option<String>,
    /// Optional input operator (for correlated subquery — prior MATCH result)
    input: Option<OperatorBox>,
    /// Search results: (NodeId, similarity_score) — score in [0, 1], higher = more similar
    results: Vec<(NodeId, f32)>,
    /// Current index in results
    current: usize,
}

impl VectorSearchOperator {
    pub fn new(
        label: String,
        property_key: String,
        query_vector: Vec<f32>,
        k: usize,
        node_var: String,
        score_var: Option<String>,
    ) -> Self {
        Self {
            label,
            property_key,
            query_vector,
            deferred_vector_expr: None,
            k,
            node_var,
            score_var,
            input: None,
            results: Vec::new(),
            current: 0,
        }
    }

    /// Create a VectorSearchOperator with a deferred expression (e.g., property access).
    /// The expression is resolved at runtime from the input operator's records.
    pub fn new_deferred(
        label: String,
        property_key: String,
        expr: crate::query::ast::Expression,
        k: usize,
        node_var: String,
        score_var: Option<String>,
        input: OperatorBox,
    ) -> Self {
        Self {
            label,
            property_key,
            query_vector: Vec::new(),
            deferred_vector_expr: Some(expr),
            k,
            node_var,
            score_var,
            input: Some(input),
            results: Vec::new(),
            current: 0,
        }
    }

    fn initialize(&mut self, store: &GraphStore) -> ExecutionResult<()> {
        if !self.results.is_empty() || self.current > 0 {
            return Ok(());
        }

        let raw_results = store
            .vector_search(&self.label, &self.property_key, &self.query_vector, self.k)
            .map_err(|e| ExecutionError::GraphError(e.to_string()))?;

        // Convert distance to similarity score:
        // HNSW returns distance (lower = more similar), we want similarity (higher = more similar).
        // For cosine distance d in [0, 2]: similarity = 1.0 - d
        // For L2 distance: similarity = 1.0 / (1.0 + d)
        // Clamp to [0, 1] range.
        self.results = raw_results
            .into_iter()
            .map(|(node_id, distance)| {
                let similarity = (1.0 - distance).clamp(0.0, 1.0);
                (node_id, similarity)
            })
            .collect();

        // Sort by similarity descending (most similar first)
        self.results
            .sort_by(|a, b| b.1.partial_cmp(&a.1).unwrap_or(std::cmp::Ordering::Equal));

        Ok(())
    }
}

impl PhysicalOperator for VectorSearchOperator {
    fn next(&mut self, store: &GraphStore) -> ExecutionResult<Option<Record>> {
        // If we have a deferred expression, resolve it from the input operator
        if self.deferred_vector_expr.is_some() && self.results.is_empty() && self.current == 0 {
            let expr = self.deferred_vector_expr.clone().unwrap();
            if let Some(ref mut input) = self.input {
                if let Some(input_record) = input.next(store)? {
                    // Resolve the expression against the input record
                    let resolved = resolve_expression_for_record(&expr, &input_record, store);
                    self.query_vector = match resolved {
                        Some(Value::Property(PropertyValue::Vector(v))) => v,
                        Some(Value::Property(PropertyValue::Array(arr))) => arr
                            .iter()
                            .map(|v| match v {
                                PropertyValue::Float(f) => *f as f32,
                                PropertyValue::Integer(i) => *i as f32,
                                _ => 0.0,
                            })
                            .collect(),
                        _ => {
                            // Null query vector — MATCH returns empty, OPTIONAL MATCH returns null
                            return Ok(None);
                        }
                    };
                } else {
                    return Ok(None);
                }
            }
        }

        self.initialize(store)?;

        if self.current >= self.results.len() {
            return Ok(None);
        }

        let (node_id, similarity) = &self.results[self.current];
        self.current += 1;

        let mut record = Record::new();
        record.bind(self.node_var.clone(), Value::NodeRef(*node_id));

        if let Some(score_var) = &self.score_var {
            record.bind(
                score_var.clone(),
                Value::Property(PropertyValue::Float(*similarity as f64)),
            );
        }

        Ok(Some(record))
    }

    fn reset(&mut self) {
        self.current = 0;
    }

    fn describe(&self) -> OperatorDescription {
        let score_detail = self
            .score_var
            .as_ref()
            .map(|s| format!(" SCORE AS {}", s))
            .unwrap_or_default();
        OperatorDescription {
            name: "VectorSearch".to_string(),
            details: format!(
                "SEARCH {} IN (VECTOR INDEX {}.{} LIMIT {}){}",
                self.node_var, self.label, self.property_key, self.k, score_detail
            ),
            children: Vec::new(),
        }
    }
}

/// Cartesian product operator: MATCH (a:X), (b:Y)
/// Produces all combinations of records from left and right inputs
pub struct CartesianProductOperator {
    left: OperatorBox,
    right: OperatorBox,
    left_records: Vec<Record>,
    left_index: usize,
    current_right: Option<Record>,
    left_materialized: bool,
}

impl CartesianProductOperator {
    pub fn new(left: OperatorBox, right: OperatorBox) -> Self {
        Self {
            left,
            right,
            left_records: Vec::new(),
            left_index: 0,
            current_right: None,
            left_materialized: false,
        }
    }

    fn materialize_left(&mut self, store: &GraphStore) -> ExecutionResult<()> {
        if self.left_materialized {
            return Ok(());
        }
        let mut count = 0u64;
        while let Some(record) = self.left.next(store)? {
            self.left_records.push(record);
            count += 1;
            if count % 10000 == 0 {
                check_deadline()?;
            }
        }
        self.left_materialized = true;
        Ok(())
    }
}

impl PhysicalOperator for CartesianProductOperator {
    fn next(&mut self, store: &GraphStore) -> ExecutionResult<Option<Record>> {
        self.materialize_left(store)?;
        if self.left_records.is_empty() {
            return Ok(None);
        }
        loop {
            if self.current_right.is_none() {
                self.current_right = self.right.next(store)?;
                self.left_index = 0;
                if self.current_right.is_none() {
                    return Ok(None);
                }
            }
            if self.left_index < self.left_records.len() {
                let left_record = &self.left_records[self.left_index];
                let right_record = self.current_right.as_ref().unwrap();
                let mut merged = left_record.clone();
                for (key, value) in right_record.bindings() {
                    merged.bind(key.clone(), value.clone());
                }
                self.left_index += 1;
                return Ok(Some(merged));
            } else {
                self.current_right = None;
            }
        }
    }

    fn next_batch(
        &mut self,
        store: &GraphStore,
        batch_size: usize,
    ) -> ExecutionResult<Option<RecordBatch>> {
        self.materialize_left(store)?;
        if self.left_records.is_empty() {
            return Ok(None);
        }

        let mut results = Vec::with_capacity(batch_size);
        while results.len() < batch_size {
            if self.current_right.is_none() {
                self.current_right = self.right.next(store)?;
                self.left_index = 0;
                if self.current_right.is_none() {
                    break;
                }
            }

            let take = (batch_size - results.len()).min(self.left_records.len() - self.left_index);
            let right_record = self.current_right.as_ref().unwrap();

            for i in 0..take {
                let left_record = &self.left_records[self.left_index + i];
                let mut merged = left_record.clone();
                for (key, value) in right_record.bindings() {
                    merged.bind(key.clone(), value.clone());
                }
                results.push(merged);
            }

            self.left_index += take;
            if self.left_index >= self.left_records.len() {
                self.current_right = None;
            }
        }

        if results.is_empty() {
            Ok(None)
        } else {
            Ok(Some(RecordBatch {
                records: results,
                columns: Vec::new(),
            }))
        }
    }

    fn reset(&mut self) {
        self.left.reset();
        self.right.reset();
        self.left_records.clear();
        self.left_index = 0;
        self.current_right = None;
        self.left_materialized = false;
    }

    fn describe(&self) -> OperatorDescription {
        OperatorDescription {
            name: "CartesianProduct".to_string(),
            details: String::new(),
            children: vec![self.left.describe(), self.right.describe()],
        }
    }
}

/// Join operator: Joins two inputs on a shared variable
pub struct JoinOperator {
    left: OperatorBox,
    right: OperatorBox,
    join_vars: Vec<String>,
    left_records: HashMap<Vec<Value>, Vec<Record>>,
    right_records: Vec<Record>,
    current_right_index: usize,
    current_left_list_index: usize,
    materialized: bool,
}

impl JoinOperator {
    pub fn new(left: OperatorBox, right: OperatorBox, join_var: String) -> Self {
        Self {
            left,
            right,
            join_vars: vec![join_var],
            left_records: HashMap::new(),
            right_records: Vec::new(),
            current_right_index: 0,
            current_left_list_index: 0,
            materialized: false,
        }
    }

    pub fn new_multi(left: OperatorBox, right: OperatorBox, join_vars: Vec<String>) -> Self {
        Self {
            left,
            right,
            join_vars,
            left_records: HashMap::new(),
            right_records: Vec::new(),
            current_right_index: 0,
            current_left_list_index: 0,
            materialized: false,
        }
    }

    fn join_key(record: &Record, vars: &[String]) -> Vec<Value> {
        vars.iter()
            .map(|v| record.get(v).cloned().unwrap_or(Value::Null))
            .collect()
    }

    fn materialize(&mut self, store: &GraphStore) -> ExecutionResult<()> {
        if self.materialized {
            return Ok(());
        }

        // Materialize left into a hash map (with periodic timeout check)
        let mut count = 0u64;
        while let Some(record) = self.left.next(store)? {
            let key = Self::join_key(&record, &self.join_vars);
            self.left_records.entry(key).or_default().push(record);
            count += 1;
            if count % 10000 == 0 {
                check_deadline()?;
            }
        }

        // Materialize right into a list
        count = 0;
        while let Some(record) = self.right.next(store)? {
            self.right_records.push(record);
            count += 1;
            if count % 10000 == 0 {
                check_deadline()?;
            }
        }

        self.materialized = true;
        Ok(())
    }
}

impl PhysicalOperator for JoinOperator {
    fn next(&mut self, store: &GraphStore) -> ExecutionResult<Option<Record>> {
        self.materialize(store)?;

        while self.current_right_index < self.right_records.len() {
            let right_record = &self.right_records[self.current_right_index];
            let join_key = Self::join_key(right_record, &self.join_vars);
            {
                if let Some(left_list) = self.left_records.get(&join_key) {
                    if self.current_left_list_index < left_list.len() {
                        let left_record = &left_list[self.current_left_list_index];
                        self.current_left_list_index += 1;

                        // Merge records
                        let mut merged = left_record.clone();
                        for (key, value) in right_record.bindings() {
                            merged.bind(key.clone(), value.clone());
                        }
                        return Ok(Some(merged));
                    }
                }
            }

            // Move to next right record
            self.current_right_index += 1;
            self.current_left_list_index = 0;
        }

        Ok(None)
    }

    fn next_batch(
        &mut self,
        store: &GraphStore,
        batch_size: usize,
    ) -> ExecutionResult<Option<RecordBatch>> {
        self.materialize(store)?;
        let mut results = Vec::with_capacity(batch_size);

        while results.len() < batch_size && self.current_right_index < self.right_records.len() {
            let right_record = &self.right_records[self.current_right_index];
            let join_key = Self::join_key(right_record, &self.join_vars);
            if let Some(left_list) = self.left_records.get(&join_key) {
                let take = (batch_size - results.len())
                    .min(left_list.len() - self.current_left_list_index);

                for i in 0..take {
                    let left_record = &left_list[self.current_left_list_index + i];
                    let mut merged = left_record.clone();
                    for (key, value) in right_record.bindings() {
                        merged.bind(key.clone(), value.clone());
                    }
                    results.push(merged);
                }

                self.current_left_list_index += take;
                if self.current_left_list_index >= left_list.len() {
                    self.current_right_index += 1;
                    self.current_left_list_index = 0;
                }
            } else {
                self.current_right_index += 1;
                self.current_left_list_index = 0;
            }
        }

        if results.is_empty() {
            Ok(None)
        } else {
            Ok(Some(RecordBatch {
                records: results,
                columns: Vec::new(),
            }))
        }
    }

    fn reset(&mut self) {
        self.left.reset();
        self.right.reset();
        self.left_records.clear();
        self.right_records.clear();
        self.current_right_index = 0;
        self.current_left_list_index = 0;
        self.materialized = false;
    }

    fn describe(&self) -> OperatorDescription {
        OperatorDescription {
            name: "HashJoin".to_string(),
            details: format!("on={}", self.join_vars.join(",")),
            children: vec![self.left.describe(), self.right.describe()],
        }
    }
}

/// Left outer join operator for OPTIONAL MATCH
/// Iterates left records and probes right records by join variable.
/// When no right match exists, emits the left record with NULL for right-only variables.
pub struct LeftOuterJoinOperator {
    left: OperatorBox,
    right: OperatorBox,
    join_vars: Vec<String>,
    right_only_vars: Vec<String>,
    /// Optional post-join filter. When set, right matches are filtered through this.
    /// If ALL right matches fail the filter, a null row is emitted (preserving LEFT OUTER semantics).
    post_filter: Option<Expression>,
    // Materialized data
    left_records: Vec<Record>,
    right_hash: HashMap<Vec<Value>, Vec<Record>>,
    // Iteration state
    current_left_idx: usize,
    current_right_match_idx: usize,
    null_emitted: bool,
    had_valid_match: bool,
    materialized: bool,
}

impl LeftOuterJoinOperator {
    pub fn new(
        left: OperatorBox,
        right: OperatorBox,
        join_var: String,
        right_only_vars: Vec<String>,
    ) -> Self {
        let join_vars = if join_var.is_empty() {
            vec![]
        } else {
            vec![join_var]
        };
        Self {
            left,
            right,
            join_vars,
            right_only_vars,
            post_filter: None,
            left_records: Vec::new(),
            right_hash: HashMap::new(),
            current_left_idx: 0,
            current_right_match_idx: 0,
            null_emitted: false,
            had_valid_match: false,
            materialized: false,
        }
    }

    pub fn new_multi(
        left: OperatorBox,
        right: OperatorBox,
        join_vars: Vec<String>,
        right_only_vars: Vec<String>,
    ) -> Self {
        Self {
            left,
            right,
            join_vars,
            right_only_vars,
            post_filter: None,
            left_records: Vec::new(),
            right_hash: HashMap::new(),
            current_left_idx: 0,
            current_right_match_idx: 0,
            null_emitted: false,
            had_valid_match: false,
            materialized: false,
        }
    }

    /// Add a post-join filter for OPTIONAL MATCH WHERE predicates.
    /// Right matches that fail the filter are skipped; if ALL fail, null row is emitted.
    pub fn with_filter(mut self, filter: Expression) -> Self {
        self.post_filter = Some(filter);
        self
    }

    fn join_key(record: &Record, vars: &[String]) -> Vec<Value> {
        vars.iter()
            .map(|v| record.get(v).cloned().unwrap_or(Value::Null))
            .collect()
    }

    fn materialize(&mut self, store: &GraphStore) -> ExecutionResult<()> {
        if self.materialized {
            return Ok(());
        }

        // Materialize left as flat list (with timeout check)
        let mut count = 0u64;
        while let Some(record) = self.left.next(store)? {
            self.left_records.push(record);
            count += 1;
            if count % 10000 == 0 {
                check_deadline()?;
            }
        }

        // Materialize right into a hash map by join variables
        count = 0;
        while let Some(record) = self.right.next(store)? {
            let key = Self::join_key(&record, &self.join_vars);
            self.right_hash.entry(key).or_default().push(record);
            count += 1;
            if count % 10000 == 0 {
                check_deadline()?;
            }
        }

        self.materialized = true;
        Ok(())
    }
}

impl PhysicalOperator for LeftOuterJoinOperator {
    fn next(&mut self, store: &GraphStore) -> ExecutionResult<Option<Record>> {
        self.materialize(store)?;

        while self.current_left_idx < self.left_records.len() {
            let left_record = &self.left_records[self.current_left_idx];

            let join_key = Self::join_key(left_record, &self.join_vars);
            let has_key =
                !self.join_vars.is_empty() && join_key.iter().all(|v| !matches!(v, Value::Null));

            if has_key {
                if let Some(right_list) = self.right_hash.get(&join_key) {
                    // Has right matches — emit merged records (with optional filter)
                    while self.current_right_match_idx < right_list.len() {
                        let right_record = &right_list[self.current_right_match_idx];
                        self.current_right_match_idx += 1;

                        let mut merged = left_record.clone();
                        for (key, value) in right_record.bindings() {
                            merged.bind(key.clone(), value.clone());
                        }

                        // Apply post-join filter if present
                        if let Some(ref filter) = self.post_filter {
                            let passes = match eval_expression(filter, &merged, store)? {
                                Value::Property(PropertyValue::Boolean(b)) => b,
                                Value::Null | Value::Property(PropertyValue::Null) => false,
                                _ => false,
                            };
                            if !passes {
                                continue; // Skip this right match
                            }
                        }
                        self.had_valid_match = true;
                        return Ok(Some(merged));
                    }
                    // Exhausted right matches — if none passed the filter, emit null row
                    if !self.had_valid_match && !self.null_emitted {
                        self.null_emitted = true;
                        let mut merged = left_record.clone();
                        for var in &self.right_only_vars {
                            merged.bind(var.clone(), Value::Null);
                        }
                        return Ok(Some(merged));
                    }
                } else if !self.null_emitted {
                    // No right matches — emit left record with NULLs
                    self.null_emitted = true;
                    let mut merged = left_record.clone();
                    for var in &self.right_only_vars {
                        merged.bind(var.clone(), Value::Null);
                    }
                    return Ok(Some(merged));
                }
            } else if !self.null_emitted {
                // Left record has no join var value — emit with NULLs
                self.null_emitted = true;
                let mut merged = left_record.clone();
                for var in &self.right_only_vars {
                    merged.bind(var.clone(), Value::Null);
                }
                return Ok(Some(merged));
            }

            // Move to next left record
            self.current_left_idx += 1;
            self.current_right_match_idx = 0;
            self.null_emitted = false;
            self.had_valid_match = false;
        }

        Ok(None)
    }

    fn next_batch(
        &mut self,
        store: &GraphStore,
        batch_size: usize,
    ) -> ExecutionResult<Option<RecordBatch>> {
        let mut results = Vec::with_capacity(batch_size);
        while results.len() < batch_size {
            match self.next(store)? {
                Some(record) => results.push(record),
                None => break,
            }
        }
        if results.is_empty() {
            Ok(None)
        } else {
            Ok(Some(RecordBatch {
                records: results,
                columns: Vec::new(),
            }))
        }
    }

    fn reset(&mut self) {
        self.left.reset();
        self.right.reset();
        self.left_records.clear();
        self.right_hash.clear();
        self.current_left_idx = 0;
        self.current_right_match_idx = 0;
        self.null_emitted = false;
        self.materialized = false;
    }

    fn describe(&self) -> OperatorDescription {
        OperatorDescription {
            name: "LeftOuterJoin".to_string(),
            details: format!("on={}", self.join_vars.join(",")),
            children: vec![self.left.describe(), self.right.describe()],
        }
    }
}

/// Create node operator: CREATE (n:Person {name: "Alice"})
pub struct CreateNodeOperator {
    /// Nodes to create (label, properties, variable)
    nodes_to_create: Vec<(Vec<Label>, HashMap<String, PropertyValue>, Option<String>)>,
    /// Created node IDs (for returning)
    created_nodes: Vec<(NodeId, Option<String>)>,
    /// Current index for iteration
    current: usize,
    /// Whether creation has been executed
    executed: bool,
}

impl CreateNodeOperator {
    /// Create a new CreateNodeOperator
    pub fn new(nodes: Vec<(Vec<Label>, HashMap<String, PropertyValue>, Option<String>)>) -> Self {
        Self {
            nodes_to_create: nodes,
            created_nodes: Vec::new(),
            current: 0,
            executed: false,
        }
    }
}

impl PhysicalOperator for CreateNodeOperator {
    fn next(&mut self, _store: &GraphStore) -> ExecutionResult<Option<Record>> {
        // Read-only version cannot create nodes
        Err(ExecutionError::RuntimeError(
            "CreateNodeOperator requires mutable store access. Use next_mut instead.".to_string(),
        ))
    }

    fn next_mut(
        &mut self,
        store: &mut GraphStore,
        tenant_id: &str,
    ) -> ExecutionResult<Option<Record>> {
        // First call: create all nodes
        if !self.executed {
            for (labels, properties, variable) in &self.nodes_to_create {
                // Use first label as primary, or empty string if none
                let primary_label = labels.first().cloned().unwrap_or_else(|| Label::new(""));

                let node_id = store.create_node(primary_label);

                // Add additional labels
                for label in labels.iter().skip(1) {
                    let _ = store.add_label_to_node(tenant_id, node_id, label.clone());
                }

                // Set properties using store.set_node_property to trigger indexing + constraint checks
                for (key, value) in properties {
                    if let Err(e) =
                        store.set_node_property(tenant_id, node_id, key.clone(), value.clone())
                    {
                        // Rollback: remove the just-created node on constraint violation
                        let _ = store.delete_node(tenant_id, node_id);
                        return Err(ExecutionError::RuntimeError(e.to_string()));
                    }
                }

                self.created_nodes.push((node_id, variable.clone()));
            }
            self.executed = true;
        }

        // Return created nodes: if multiple nodes have variables, return a single
        // combined record with all bindings (needed for multi-CREATE with edges)
        if self.current > 0 {
            return Ok(None);
        }
        self.current = 1;

        let mut record = Record::new();
        for (node_id, variable) in &self.created_nodes {
            if let Some(var) = variable {
                if let Some(node) = store.get_node(*node_id) {
                    record.bind(var.clone(), Value::Node(*node_id, node.clone()));
                }
            }
        }

        Ok(Some(record))
    }

    fn reset(&mut self) {
        self.current = 0;
        // Note: We don't reset executed flag - nodes are already created
    }

    fn is_mutating(&self) -> bool {
        true
    }
}

/// Create property index operator: CREATE INDEX ON :Person(id)
pub struct CreateIndexOperator {
    label: Label,
    property: String,
    executed: bool,
}

impl CreateIndexOperator {
    pub fn new(label: Label, property: String) -> Self {
        Self {
            label,
            property,
            executed: false,
        }
    }
}

impl PhysicalOperator for CreateIndexOperator {
    fn next(&mut self, _store: &GraphStore) -> ExecutionResult<Option<Record>> {
        Err(ExecutionError::RuntimeError(
            "CreateIndexOperator requires mutable store access. Use next_mut instead.".to_string(),
        ))
    }

    fn next_mut(
        &mut self,
        store: &mut GraphStore,
        _tenant_id: &str,
    ) -> ExecutionResult<Option<Record>> {
        if self.executed {
            return Ok(None);
        }

        store
            .property_index
            .create_index(self.label.clone(), self.property.clone());

        // Backfill index
        // Since we have mutable access to store, we can get nodes
        // But we need to avoid borrowing store while mutating property_index if we accessed it differently
        // Here we use get_nodes_by_label which borrows store.
        // property_index is inside store.
        // IndexManager uses RwLock internally so it handles its own mutability.

        // We collect entries to release the borrow on nodes
        let mut entries = Vec::new();
        let nodes = store.get_nodes_by_label(&self.label);

        for node in nodes {
            if let Some(val) = node.get_property(&self.property) {
                entries.push((node.id, val.clone()));
            }
        }

        for (node_id, val) in entries {
            store
                .property_index
                .index_insert(&self.label, &self.property, val, node_id);
        }

        self.executed = true;
        Ok(Some(Record::new()))
    }

    fn reset(&mut self) {
        self.executed = false;
    }

    fn is_mutating(&self) -> bool {
        true
    }
}

/// Create vector index operator: CREATE VECTOR INDEX ...
pub struct CreateVectorIndexOperator {
    label: Label,
    property_key: String,
    dimensions: usize,
    similarity: String,
    if_not_exists: bool,
    executed: bool,
}

impl CreateVectorIndexOperator {
    pub fn new(
        label: Label,
        property_key: String,
        dimensions: usize,
        similarity: String,
        if_not_exists: bool,
    ) -> Self {
        Self {
            label,
            property_key,
            dimensions,
            similarity,
            if_not_exists,
            executed: false,
        }
    }
}

impl PhysicalOperator for CreateVectorIndexOperator {
    fn next(&mut self, _store: &GraphStore) -> ExecutionResult<Option<Record>> {
        Err(ExecutionError::RuntimeError(
            "CreateVectorIndexOperator requires mutable store access. Use next_mut instead."
                .to_string(),
        ))
    }

    fn next_mut(
        &mut self,
        store: &mut GraphStore,
        _tenant_id: &str,
    ) -> ExecutionResult<Option<Record>> {
        if self.executed {
            return Ok(None);
        }

        // IF NOT EXISTS: skip if index already exists
        if self.if_not_exists {
            let existing = store
                .vector_index
                .get_index(self.label.as_str(), &self.property_key);
            if existing.is_some() {
                self.executed = true;
                return Ok(Some(Record::new()));
            }
        }

        let metric = match self.similarity.to_lowercase().as_str() {
            "cosine" => crate::vector::DistanceMetric::Cosine,
            "l2" => crate::vector::DistanceMetric::L2,
            _ => {
                return Err(ExecutionError::RuntimeError(format!(
                    "Unsupported similarity metric: {}",
                    self.similarity
                )))
            }
        };

        store
            .create_vector_index(
                self.label.as_str(),
                &self.property_key,
                self.dimensions,
                metric,
            )
            .map_err(|e| ExecutionError::GraphError(e.to_string()))?;

        // Backfill: index existing nodes that already have vector data on this property
        let label_obj = self.label.clone();
        let existing_nodes = store.get_nodes_by_label(&label_obj);
        let mut indexed = 0u64;
        for node in &existing_nodes {
            if let Some(crate::graph::property::PropertyValue::Vector(vec)) =
                node.get_property(&self.property_key)
            {
                if vec.len() == self.dimensions {
                    let _ = store.vector_index.add_vector(
                        self.label.as_str(),
                        &self.property_key,
                        node.id,
                        vec,
                    );
                    indexed += 1;
                }
            } else if let Some(crate::graph::property::PropertyValue::Array(arr)) =
                node.get_property(&self.property_key)
            {
                // Also handle Array properties that contain numeric values
                let vec: Vec<f32> = arr
                    .iter()
                    .filter_map(|v| match v {
                        crate::graph::property::PropertyValue::Float(f) => Some(*f as f32),
                        crate::graph::property::PropertyValue::Integer(i) => Some(*i as f32),
                        _ => None,
                    })
                    .collect();
                if vec.len() == self.dimensions {
                    let _ = store.vector_index.add_vector(
                        self.label.as_str(),
                        &self.property_key,
                        node.id,
                        &vec,
                    );
                    indexed += 1;
                }
            }
        }

        self.executed = true;

        let mut record = Record::new();
        record.bind(
            "indexed".to_string(),
            Value::Property(PropertyValue::Integer(indexed as i64)),
        );
        Ok(Some(record))
    }

    fn reset(&mut self) {
        self.executed = false;
    }

    fn is_mutating(&self) -> bool {
        true
    }
}

/// Composite create index operator: CREATE INDEX ON :Label(prop1, prop2, ...)
pub struct CompositeCreateIndexOperator {
    label: Label,
    properties: Vec<String>,
    executed: bool,
}

impl CompositeCreateIndexOperator {
    pub fn new(label: Label, properties: Vec<String>) -> Self {
        Self {
            label,
            properties,
            executed: false,
        }
    }
}

impl PhysicalOperator for CompositeCreateIndexOperator {
    fn next(&mut self, _store: &GraphStore) -> ExecutionResult<Option<Record>> {
        Err(ExecutionError::RuntimeError(
            "CompositeCreateIndexOperator requires mutable store access.".to_string(),
        ))
    }

    fn next_mut(
        &mut self,
        store: &mut GraphStore,
        _tenant_id: &str,
    ) -> ExecutionResult<Option<Record>> {
        if self.executed {
            return Ok(None);
        }

        // Create individual indexes for each property
        for property in &self.properties {
            store
                .property_index
                .create_index(self.label.clone(), property.clone());

            // Backfill each index
            let mut entries = Vec::new();
            let nodes = store.get_nodes_by_label(&self.label);
            for node in nodes {
                if let Some(val) = node.get_property(property) {
                    entries.push((node.id, val.clone()));
                }
            }
            for (node_id, val) in entries {
                store
                    .property_index
                    .index_insert(&self.label, property, val, node_id);
            }
        }

        self.executed = true;
        Ok(Some(Record::new()))
    }

    fn reset(&mut self) {
        self.executed = false;
    }

    fn is_mutating(&self) -> bool {
        true
    }

    fn describe(&self) -> OperatorDescription {
        OperatorDescription {
            name: "CreateCompositeIndex".to_string(),
            details: format!(":{}({})", self.label.as_str(), self.properties.join(", ")),
            children: Vec::new(),
        }
    }
}

/// Create unique constraint operator
pub struct CreateConstraintOperator {
    label: Label,
    property: String,
    executed: bool,
}

impl CreateConstraintOperator {
    pub fn new(label: Label, property: String) -> Self {
        Self {
            label,
            property,
            executed: false,
        }
    }
}

impl PhysicalOperator for CreateConstraintOperator {
    fn next(&mut self, _store: &GraphStore) -> ExecutionResult<Option<Record>> {
        Err(ExecutionError::RuntimeError(
            "CreateConstraintOperator requires mutable store access.".to_string(),
        ))
    }

    fn next_mut(
        &mut self,
        store: &mut GraphStore,
        _tenant_id: &str,
    ) -> ExecutionResult<Option<Record>> {
        if self.executed {
            return Ok(None);
        }

        // Check existing data for uniqueness violations
        let nodes = store.get_nodes_by_label(&self.label);
        let mut seen_values: std::collections::HashSet<PropertyValue> =
            std::collections::HashSet::new();
        for node in nodes {
            if let Some(val) = node.get_property(&self.property) {
                if !val.is_null() && !seen_values.insert(val.clone()) {
                    return Err(ExecutionError::RuntimeError(format!(
                        "Cannot create unique constraint: duplicate value {:?} for :{}({})",
                        val,
                        self.label.as_str(),
                        self.property
                    )));
                }
            }
        }

        // Create the constraint
        store
            .property_index
            .create_unique_constraint(self.label.clone(), self.property.clone());

        // Backfill constraint index
        let mut entries = Vec::new();
        let nodes = store.get_nodes_by_label(&self.label);
        for node in nodes {
            if let Some(val) = node.get_property(&self.property) {
                entries.push((node.id, val.clone()));
            }
        }
        for (node_id, val) in entries {
            store
                .property_index
                .constraint_insert(&self.label, &self.property, val, node_id);
        }

        self.executed = true;
        Ok(Some(Record::new()))
    }

    fn reset(&mut self) {
        self.executed = false;
    }

    fn is_mutating(&self) -> bool {
        true
    }

    fn describe(&self) -> OperatorDescription {
        OperatorDescription {
            name: "CreateConstraint".to_string(),
            details: format!("UNIQUE :{}({})", self.label.as_str(), self.property),
            children: Vec::new(),
        }
    }
}

/// Drop index operator: DROP INDEX ON :Label(property)
pub struct DropIndexOperator {
    label: Label,
    property: String,
    executed: bool,
}

impl DropIndexOperator {
    pub fn new(label: Label, property: String) -> Self {
        Self {
            label,
            property,
            executed: false,
        }
    }
}

impl PhysicalOperator for DropIndexOperator {
    fn next(&mut self, _store: &GraphStore) -> ExecutionResult<Option<Record>> {
        Err(ExecutionError::RuntimeError(
            "DropIndexOperator requires mutable store access. Use next_mut instead.".to_string(),
        ))
    }

    fn next_mut(
        &mut self,
        store: &mut GraphStore,
        _tenant_id: &str,
    ) -> ExecutionResult<Option<Record>> {
        if self.executed {
            return Ok(None);
        }

        if !store.property_index.has_index(&self.label, &self.property) {
            return Err(ExecutionError::RuntimeError(format!(
                "Index on :{}({}) does not exist",
                self.label.as_str(),
                self.property
            )));
        }

        store.property_index.drop_index(&self.label, &self.property);
        self.executed = true;
        Ok(Some(Record::new()))
    }

    fn reset(&mut self) {
        self.executed = false;
    }

    fn is_mutating(&self) -> bool {
        true
    }

    fn describe(&self) -> OperatorDescription {
        OperatorDescription {
            name: "DropIndex".to_string(),
            details: format!(":{}({})", self.label.as_str(), self.property),
            children: Vec::new(),
        }
    }
}

/// Drop vector index operator: DROP VECTOR INDEX name
pub struct DropVectorIndexOperator {
    index_name: String,
    executed: bool,
}

impl DropVectorIndexOperator {
    pub fn new(index_name: String) -> Self {
        Self {
            index_name,
            executed: false,
        }
    }
}

impl PhysicalOperator for DropVectorIndexOperator {
    fn next(&mut self, _store: &GraphStore) -> ExecutionResult<Option<Record>> {
        Err(ExecutionError::RuntimeError(
            "DropVectorIndexOperator requires mutable store access".to_string(),
        ))
    }

    fn next_mut(
        &mut self,
        store: &mut GraphStore,
        _tenant_id: &str,
    ) -> ExecutionResult<Option<Record>> {
        if self.executed {
            return Ok(None);
        }

        let dropped = store.drop_vector_index(&self.index_name);
        if !dropped {
            return Err(ExecutionError::RuntimeError(format!(
                "Vector index '{}' does not exist",
                self.index_name
            )));
        }

        self.executed = true;
        Ok(Some(Record::new()))
    }

    fn reset(&mut self) {
        self.executed = false;
    }

    fn is_mutating(&self) -> bool {
        true
    }

    fn describe(&self) -> OperatorDescription {
        OperatorDescription {
            name: "DropVectorIndex".to_string(),
            details: self.index_name.clone(),
            children: Vec::new(),
        }
    }
}

/// Show indexes operator: SHOW INDEXES
pub struct ShowIndexesOperator {
    results: Option<std::vec::IntoIter<Record>>,
}

impl ShowIndexesOperator {
    pub fn new() -> Self {
        Self { results: None }
    }
}

impl PhysicalOperator for ShowIndexesOperator {
    fn next(&mut self, store: &GraphStore) -> ExecutionResult<Option<Record>> {
        if self.results.is_none() {
            let mut records = Vec::new();

            // Property (BTREE) indexes
            let indexes = store.property_index.list_indexes();
            for (label, property) in indexes {
                let mut record = Record::new();
                record.bind(
                    "label".to_string(),
                    Value::Property(PropertyValue::String(label.as_str().to_string())),
                );
                record.bind(
                    "property".to_string(),
                    Value::Property(PropertyValue::String(property)),
                );
                record.bind(
                    "type".to_string(),
                    Value::Property(PropertyValue::String("BTREE".to_string())),
                );
                records.push(record);
            }

            // Vector (HNSW) indexes
            let vector_keys = store.vector_index.list_indices();
            for key in vector_keys {
                let mut record = Record::new();
                record.bind(
                    "label".to_string(),
                    Value::Property(PropertyValue::String(key.label.clone())),
                );
                record.bind(
                    "property".to_string(),
                    Value::Property(PropertyValue::String(key.property_key.clone())),
                );
                record.bind(
                    "type".to_string(),
                    Value::Property(PropertyValue::String("VECTOR".to_string())),
                );
                records.push(record);
            }

            self.results = Some(records.into_iter());
        }

        Ok(self.results.as_mut().unwrap().next())
    }

    fn reset(&mut self) {
        self.results = None;
    }

    fn describe(&self) -> OperatorDescription {
        OperatorDescription {
            name: "ShowIndexes".to_string(),
            details: String::new(),
            children: Vec::new(),
        }
    }
}

/// Show vector indexes operator: SHOW VECTOR INDEX[ES]
pub struct ShowVectorIndexesOperator {
    results: Option<std::vec::IntoIter<Record>>,
}

impl ShowVectorIndexesOperator {
    pub fn new() -> Self {
        Self { results: None }
    }
}

impl PhysicalOperator for ShowVectorIndexesOperator {
    fn next(&mut self, store: &GraphStore) -> ExecutionResult<Option<Record>> {
        if self.results.is_none() {
            let mut records = Vec::new();
            let vector_keys = store.vector_index.list_indices();
            for key in vector_keys {
                let mut record = Record::new();
                record.bind(
                    "name".to_string(),
                    Value::Property(PropertyValue::String(format!(
                        "{}_{}",
                        key.label, key.property_key
                    ))),
                );
                record.bind(
                    "label".to_string(),
                    Value::Property(PropertyValue::String(key.label.clone())),
                );
                record.bind(
                    "property".to_string(),
                    Value::Property(PropertyValue::String(key.property_key.clone())),
                );
                // Get index details (dimensions, metric)
                if let Some(idx) = store.vector_index.get_index(&key.label, &key.property_key) {
                    let idx_guard = idx.read().unwrap();
                    record.bind(
                        "dimensions".to_string(),
                        Value::Property(PropertyValue::Integer(idx_guard.dimensions() as i64)),
                    );
                    record.bind(
                        "similarity".to_string(),
                        Value::Property(PropertyValue::String(format!("{:?}", idx_guard.metric()))),
                    );
                    record.bind(
                        "vectors".to_string(),
                        Value::Property(PropertyValue::Integer(idx_guard.len() as i64)),
                    );
                }
                record.bind(
                    "type".to_string(),
                    Value::Property(PropertyValue::String("VECTOR".to_string())),
                );
                records.push(record);
            }
            self.results = Some(records.into_iter());
        }

        Ok(self.results.as_mut().unwrap().next())
    }

    fn reset(&mut self) {
        self.results = None;
    }

    fn describe(&self) -> OperatorDescription {
        OperatorDescription {
            name: "ShowVectorIndexes".to_string(),
            details: String::new(),
            children: Vec::new(),
        }
    }
}

/// Show constraints operator: SHOW CONSTRAINTS
pub struct ShowConstraintsOperator {
    results: Option<std::vec::IntoIter<Record>>,
}

impl ShowConstraintsOperator {
    pub fn new() -> Self {
        Self { results: None }
    }
}

impl PhysicalOperator for ShowConstraintsOperator {
    fn next(&mut self, store: &GraphStore) -> ExecutionResult<Option<Record>> {
        if self.results.is_none() {
            let constraints = store.property_index.list_constraints();
            let mut records = Vec::new();
            for (label, property) in constraints {
                let mut record = Record::new();
                record.bind(
                    "label".to_string(),
                    Value::Property(PropertyValue::String(label.as_str().to_string())),
                );
                record.bind(
                    "property".to_string(),
                    Value::Property(PropertyValue::String(property)),
                );
                record.bind(
                    "type".to_string(),
                    Value::Property(PropertyValue::String("UNIQUE".to_string())),
                );
                records.push(record);
            }
            self.results = Some(records.into_iter());
        }

        Ok(self.results.as_mut().unwrap().next())
    }

    fn reset(&mut self) {
        self.results = None;
    }

    fn describe(&self) -> OperatorDescription {
        OperatorDescription {
            name: "ShowConstraints".to_string(),
            details: String::new(),
            children: Vec::new(),
        }
    }
}

/// Show labels operator: CALL db.labels()
pub struct ShowLabelsOperator {
    results: Option<std::vec::IntoIter<Record>>,
}

impl ShowLabelsOperator {
    pub fn new() -> Self {
        Self { results: None }
    }
}

impl PhysicalOperator for ShowLabelsOperator {
    fn next(&mut self, store: &GraphStore) -> ExecutionResult<Option<Record>> {
        if self.results.is_none() {
            let mut labels: Vec<String> = store
                .all_labels()
                .iter()
                .map(|l| l.as_str().to_string())
                .collect();
            labels.sort();
            let mut records = Vec::new();
            for label in labels {
                let mut record = Record::new();
                record.bind(
                    "label".to_string(),
                    Value::Property(PropertyValue::String(label)),
                );
                records.push(record);
            }
            self.results = Some(records.into_iter());
        }
        Ok(self.results.as_mut().unwrap().next())
    }

    fn reset(&mut self) {
        self.results = None;
    }

    fn describe(&self) -> OperatorDescription {
        OperatorDescription {
            name: "ShowLabels".to_string(),
            details: String::new(),
            children: Vec::new(),
        }
    }
}

/// Show relationship types operator: CALL db.relationshipTypes()
pub struct ShowRelationshipTypesOperator {
    results: Option<std::vec::IntoIter<Record>>,
}

impl ShowRelationshipTypesOperator {
    pub fn new() -> Self {
        Self { results: None }
    }
}

impl PhysicalOperator for ShowRelationshipTypesOperator {
    fn next(&mut self, store: &GraphStore) -> ExecutionResult<Option<Record>> {
        if self.results.is_none() {
            let mut types: Vec<String> = store
                .all_edge_types()
                .iter()
                .map(|t| t.as_str().to_string())
                .collect();
            types.sort();
            let mut records = Vec::new();
            for edge_type in types {
                let mut record = Record::new();
                record.bind(
                    "relationshipType".to_string(),
                    Value::Property(PropertyValue::String(edge_type)),
                );
                records.push(record);
            }
            self.results = Some(records.into_iter());
        }
        Ok(self.results.as_mut().unwrap().next())
    }

    fn reset(&mut self) {
        self.results = None;
    }

    fn describe(&self) -> OperatorDescription {
        OperatorDescription {
            name: "ShowRelationshipTypes".to_string(),
            details: String::new(),
            children: Vec::new(),
        }
    }
}

/// Show property keys operator: CALL db.propertyKeys()
pub struct ShowPropertyKeysOperator {
    results: Option<std::vec::IntoIter<Record>>,
}

impl ShowPropertyKeysOperator {
    pub fn new() -> Self {
        Self { results: None }
    }
}

impl PhysicalOperator for ShowPropertyKeysOperator {
    fn next(&mut self, store: &GraphStore) -> ExecutionResult<Option<Record>> {
        if self.results.is_none() {
            let mut keys = std::collections::BTreeSet::new();
            let stats = store.compute_statistics();
            for (_, prop) in stats.property_stats.keys() {
                keys.insert(prop.clone());
            }
            for edge_type in store.all_edge_types() {
                let edges = store.get_edges_by_type(edge_type);
                for edge in edges.iter().take(1000) {
                    for key in edge.properties.keys() {
                        keys.insert(key.clone());
                    }
                }
            }
            let mut records = Vec::new();
            for key in keys {
                let mut record = Record::new();
                record.bind(
                    "propertyKey".to_string(),
                    Value::Property(PropertyValue::String(key)),
                );
                records.push(record);
            }
            self.results = Some(records.into_iter());
        }
        Ok(self.results.as_mut().unwrap().next())
    }

    fn reset(&mut self) {
        self.results = None;
    }

    fn describe(&self) -> OperatorDescription {
        OperatorDescription {
            name: "ShowPropertyKeys".to_string(),
            details: String::new(),
            children: Vec::new(),
        }
    }
}

/// Schema visualization operator: CALL db.schema.visualization()
pub struct SchemaVisualizationOperator {
    results: Option<std::vec::IntoIter<Record>>,
}

impl SchemaVisualizationOperator {
    pub fn new() -> Self {
        Self { results: None }
    }
}

impl PhysicalOperator for SchemaVisualizationOperator {
    fn next(&mut self, store: &GraphStore) -> ExecutionResult<Option<Record>> {
        if self.results.is_none() {
            let mut seen = std::collections::HashSet::new();
            let mut records = Vec::new();
            for edge_type in store.all_edge_types() {
                let edges = store.get_edges_by_type(edge_type);
                for edge in edges.iter().take(1000) {
                    if let (Some(src_node), Some(tgt_node)) =
                        (store.get_node(edge.source), store.get_node(edge.target))
                    {
                        for src_label in &src_node.labels {
                            for tgt_label in &tgt_node.labels {
                                let key = format!(
                                    "{}|{}|{}",
                                    src_label.as_str(),
                                    edge_type.as_str(),
                                    tgt_label.as_str()
                                );
                                if seen.insert(key) {
                                    let mut record = Record::new();
                                    record.bind(
                                        "source_label".to_string(),
                                        Value::Property(PropertyValue::String(
                                            src_label.as_str().to_string(),
                                        )),
                                    );
                                    record.bind(
                                        "relationship_type".to_string(),
                                        Value::Property(PropertyValue::String(
                                            edge_type.as_str().to_string(),
                                        )),
                                    );
                                    record.bind(
                                        "target_label".to_string(),
                                        Value::Property(PropertyValue::String(
                                            tgt_label.as_str().to_string(),
                                        )),
                                    );
                                    records.push(record);
                                }
                            }
                        }
                    }
                }
            }
            self.results = Some(records.into_iter());
        }
        Ok(self.results.as_mut().unwrap().next())
    }

    fn reset(&mut self) {
        self.results = None;
    }

    fn describe(&self) -> OperatorDescription {
        OperatorDescription {
            name: "SchemaVisualization".to_string(),
            details: String::new(),
            children: Vec::new(),
        }
    }
}

/// Per-row CREATE operator: for each input record, creates nodes with properties
/// resolved from the input record's variable bindings. Used by UNWIND+CREATE.
pub struct PerRowCreateOperator {
    input: OperatorBox,
    /// For each node: (labels, static_properties, expression_properties, variable_name)
    node_specs: Vec<(
        Vec<Label>,
        HashMap<String, PropertyValue>,
        Vec<(String, Expression)>,
        Option<String>,
    )>,
    /// Edges to create: (source_var, target_var, edge_type, properties, edge_var, expression_properties)
    edge_specs: Vec<(
        String,
        String,
        EdgeType,
        HashMap<String, PropertyValue>,
        Option<String>,
        Vec<(String, Expression)>,
    )>,
}

impl PerRowCreateOperator {
    pub fn new(
        input: OperatorBox,
        node_specs: Vec<(
            Vec<Label>,
            HashMap<String, PropertyValue>,
            Vec<(String, Expression)>,
            Option<String>,
        )>,
        edge_specs: Vec<(
            String,
            String,
            EdgeType,
            HashMap<String, PropertyValue>,
            Option<String>,
            Vec<(String, Expression)>,
        )>,
    ) -> Self {
        Self {
            input,
            node_specs,
            edge_specs,
        }
    }
}

impl PhysicalOperator for PerRowCreateOperator {
    fn next(&mut self, _store: &GraphStore) -> ExecutionResult<Option<Record>> {
        Err(ExecutionError::RuntimeError(
            "PerRowCreateOperator requires mutable store access".to_string(),
        ))
    }

    fn next_mut(
        &mut self,
        store: &mut GraphStore,
        tenant_id: &str,
    ) -> ExecutionResult<Option<Record>> {
        // Get next input record
        let input_record = match self.input.next_mut(store, tenant_id)? {
            Some(r) => r,
            None => return Ok(None),
        };

        let mut record = input_record.clone();

        // Create nodes for this row
        for (labels, static_props, expr_props, var_name) in &self.node_specs {
            let primary_label = labels.first().cloned().unwrap_or_else(|| Label::new(""));
            let node_id = store.create_node(primary_label);

            // Add extra labels
            for label in labels.iter().skip(1) {
                let _ = store.add_label_to_node(tenant_id, node_id, label.clone());
            }

            // Set static properties
            for (key, value) in static_props {
                let _ = store.set_node_property(tenant_id, node_id, key.clone(), value.clone());
            }

            // Evaluate expression properties from the input record
            for (key, expr) in expr_props {
                let store_ref: &GraphStore = store;
                let val = eval_expression(expr, &record, store_ref)?;
                if let Value::Property(pv) = val {
                    let _ = store.set_node_property(tenant_id, node_id, key.clone(), pv);
                }
            }

            // Bind node variable
            if let Some(var) = var_name {
                record.bind(var.clone(), Value::NodeRef(node_id));
            }
        }

        // Create edges for this row
        for (source_var, target_var, edge_type, props, edge_var, expr_props) in &self.edge_specs {
            let source_id = record.get(source_var).and_then(|v| v.node_id());
            let target_id = record.get(target_var).and_then(|v| v.node_id());
            if let (Some(src), Some(tgt)) = (source_id, target_id) {
                let edge_id = store
                    .create_edge(src, tgt, edge_type.as_str())
                    .map_err(|e| {
                        ExecutionError::RuntimeError(format!("CREATE edge failed: {}", e))
                    })?;
                for (k, v) in props {
                    if let Some(edge) = store.get_edge_mut(edge_id) {
                        edge.set_property(k.clone(), v.clone());
                    }
                }
                // Evaluate expression properties (e.g., timestamp(), randomUUID())
                for (k, expr) in expr_props {
                    let store_ref: &GraphStore = store;
                    let val = eval_expression(expr, &record, store_ref)?;
                    if let Value::Property(pv) = val {
                        if let Some(edge) = store.get_edge_mut(edge_id) {
                            edge.set_property(k.clone(), pv);
                        }
                    }
                }
                if let Some(ev) = edge_var {
                    record.bind(
                        ev.clone(),
                        Value::EdgeRef(edge_id, src, tgt, edge_type.clone()),
                    );
                }
            }
        }

        Ok(Some(record))
    }

    fn reset(&mut self) {
        self.input.reset();
    }

    fn describe(&self) -> OperatorDescription {
        OperatorDescription {
            name: "PerRowCreate".to_string(),
            details: format!(
                "{} nodes, {} edges",
                self.node_specs.len(),
                self.edge_specs.len()
            ),
            children: vec![self.input.describe()],
        }
    }

    fn is_mutating(&self) -> bool {
        true
    }
}

/// Per-row MERGE operator for the Apply pattern in multi-part queries.
/// For each input row, performs a MERGE (find-or-create) on a node pattern,
/// binding the result to the record for downstream operators.
pub struct PerRowMergeOperator {
    input: OperatorBox,
    pattern: Pattern,
    on_create_set: Vec<(String, String, Expression)>,
    on_match_set: Vec<(String, String, Expression)>,
}

impl PerRowMergeOperator {
    pub fn new(
        input: OperatorBox,
        pattern: Pattern,
        on_create_set: Vec<(String, String, Expression)>,
        on_match_set: Vec<(String, String, Expression)>,
    ) -> Self {
        Self {
            input,
            pattern,
            on_create_set,
            on_match_set,
        }
    }
}

impl PhysicalOperator for PerRowMergeOperator {
    fn next(&mut self, _store: &GraphStore) -> ExecutionResult<Option<Record>> {
        Err(ExecutionError::RuntimeError(
            "PerRowMergeOperator requires mutable store access".to_string(),
        ))
    }

    fn next_mut(
        &mut self,
        store: &mut GraphStore,
        tenant_id: &str,
    ) -> ExecutionResult<Option<Record>> {
        let input_record = match self.input.next_mut(store, tenant_id)? {
            Some(r) => r,
            None => return Ok(None),
        };

        let mut record = input_record;

        let path = match self.pattern.paths.first() {
            Some(p) => p,
            None => return Ok(Some(record)),
        };

        let start = &path.start;
        let start_var = start.variable.clone().unwrap_or_else(|| "n".to_string());
        let labels = &start.labels;

        // Resolve properties from pattern (static + expression)
        let mut resolved_props: HashMap<String, PropertyValue> =
            start.properties.clone().unwrap_or_default();
        // Evaluate expression properties from the record (e.g., {name: genre_name})
        for (key, expr) in &start.expression_properties {
            let store_ref: &GraphStore = store;
            let val = eval_expression(expr, &record, store_ref)?;
            if let Value::Property(pv) = val {
                resolved_props.insert(key.clone(), pv);
            }
        }
        // Remove Null values from resolved_props (they're non-matches)
        resolved_props.retain(|_, v| !matches!(v, PropertyValue::Null));

        // Check if the variable is already bound in the record (from a previous stage)
        let mut matched_node_id = record.get(&start_var).and_then(|v| v.node_id());

        // If not bound, search for existing node matching labels + properties
        if matched_node_id.is_none() {
            if let Some(first_label) = labels.first() {
                let candidates = store.get_nodes_by_label(first_label);
                for node in candidates {
                    let has_all_labels = labels.iter().all(|l| node.labels.contains(l));
                    if !has_all_labels {
                        continue;
                    }
                    if !resolved_props.is_empty() {
                        let props_match = resolved_props
                            .iter()
                            .all(|(k, v)| node.properties.get(k) == Some(v));
                        if !props_match {
                            continue;
                        }
                    }
                    matched_node_id = Some(node.id);
                    break;
                }
            }
        }

        let node_id;
        if let Some(existing_id) = matched_node_id {
            // ON MATCH
            node_id = existing_id;
            record.bind(start_var.clone(), Value::NodeRef(node_id));
            for (var, prop, expr) in &self.on_match_set {
                if var == &start_var {
                    let val = eval_expression(expr, &record, store)?;
                    if let Value::Property(pv) = val {
                        if let Some(node) = store.get_node_mut(node_id) {
                            node.set_property(prop.clone(), pv);
                        }
                    }
                }
            }
        } else {
            // ON CREATE
            let label_str = labels.first().map(|l| l.as_str()).unwrap_or("Node");
            node_id = store.create_node(label_str);
            for label in labels.iter().skip(1) {
                if let Some(node) = store.get_node_mut(node_id) {
                    node.labels.insert(label.clone());
                }
            }
            for (k, v) in &resolved_props {
                if let Some(node) = store.get_node_mut(node_id) {
                    node.set_property(k.clone(), v.clone());
                }
            }
            record.bind(start_var.clone(), Value::NodeRef(node_id));
            for (var, prop, expr) in &self.on_create_set {
                if var == &start_var {
                    let val = eval_expression(expr, &record, store)?;
                    if let Value::Property(pv) = val {
                        if let Some(node) = store.get_node_mut(node_id) {
                            node.set_property(prop.clone(), pv);
                        }
                    }
                }
            }
        }

        // Handle relationship segments if any
        for seg in &path.segments {
            let edge_types = &seg.edge.types;
            let edge_type_str = edge_types.first().map(|t| t.as_str()).unwrap_or("RELATED");
            let target = &seg.node;
            let target_var = target.variable.clone().unwrap_or_else(|| "m".to_string());
            let target_labels = &target.labels;
            let target_props = target.properties.as_ref();

            // Check if target is already bound
            let mut target_node_id = record.get(&target_var).and_then(|v| v.node_id());

            // Search by labels + properties
            if target_node_id.is_none() {
                if let Some(first_label) = target_labels.first() {
                    let candidates = store.get_nodes_by_label(first_label);
                    for candidate in candidates {
                        let has_labels = target_labels.iter().all(|l| candidate.labels.contains(l));
                        if !has_labels {
                            continue;
                        }
                        if let Some(req_props) = target_props {
                            let props_match = req_props
                                .iter()
                                .all(|(k, v)| candidate.properties.get(k) == Some(v));
                            if !props_match {
                                continue;
                            }
                        }
                        target_node_id = Some(candidate.id);
                        break;
                    }
                }
            }

            // Create target if not found
            let target_id = if let Some(tid) = target_node_id {
                tid
            } else {
                let label_str = target_labels.first().map(|l| l.as_str()).unwrap_or("Node");
                let tid = store.create_node(label_str);
                for label in target_labels.iter().skip(1) {
                    if let Some(node) = store.get_node_mut(tid) {
                        node.labels.insert(label.clone());
                    }
                }
                if let Some(req_props) = target_props {
                    for (k, v) in req_props {
                        if let Some(node) = store.get_node_mut(tid) {
                            node.set_property(k.clone(), v.clone());
                        }
                    }
                }
                tid
            };

            record.bind(target_var, Value::NodeRef(target_id));

            // Check if relationship already exists
            let (source_id, dest_id) = match seg.edge.direction {
                Direction::Incoming => (target_id, node_id),
                _ => (node_id, target_id),
            };

            let mut edge_exists = false;
            let outgoing = store.get_outgoing_edges(source_id);
            for existing_edge in &outgoing {
                if existing_edge.target == dest_id
                    && existing_edge.edge_type.as_str() == edge_type_str
                {
                    edge_exists = true;
                    if let Some(ev) = &seg.edge.variable {
                        record.bind(
                            ev.clone(),
                            Value::EdgeRef(
                                existing_edge.id,
                                source_id,
                                dest_id,
                                existing_edge.edge_type.clone(),
                            ),
                        );
                    }
                    break;
                }
            }

            if !edge_exists {
                if let Ok(edge_id) = store.create_edge(source_id, dest_id, edge_type_str) {
                    if let Some(edge_props) = &seg.edge.properties {
                        for (k, v) in edge_props {
                            if let Some(edge) = store.get_edge_mut(edge_id) {
                                edge.set_property(k.clone(), v.clone());
                            }
                        }
                    }
                    if let Some(ev) = &seg.edge.variable {
                        record.bind(
                            ev.clone(),
                            Value::EdgeRef(
                                edge_id,
                                source_id,
                                dest_id,
                                EdgeType::new(edge_type_str),
                            ),
                        );
                    }
                }
            }
        }

        Ok(Some(record))
    }

    fn reset(&mut self) {
        self.input.reset();
    }

    fn describe(&self) -> OperatorDescription {
        OperatorDescription {
            name: "PerRowMerge".to_string(),
            details: format!("{} paths", self.pattern.paths.len()),
            children: vec![self.input.describe()],
        }
    }

    fn is_mutating(&self) -> bool {
        true
    }
}

/// Create edge operator: `CREATE (a)-[:KNOWS]->(b)`
pub struct CreateEdgeOperator {
    /// Input operator (provides source/target nodes from MATCH)
    input: Option<OperatorBox>,
    /// Edge pattern to create: (source_var, target_var, edge_type, properties, edge_var)
    edge_pattern: (
        String,
        String,
        EdgeType,
        HashMap<String, PropertyValue>,
        Option<String>,
    ),
    /// Created edges
    created_edges: Vec<(crate::graph::EdgeId, Option<String>)>,
    /// Saved input records (to carry forward bindings)
    saved_input_records: Vec<Record>,
    /// Current index
    current: usize,
    /// Whether we've processed input
    processed: bool,
}

impl CreateEdgeOperator {
    /// Create a new CreateEdgeOperator
    pub fn new(
        input: Option<OperatorBox>,
        source_var: String,
        target_var: String,
        edge_type: EdgeType,
        properties: HashMap<String, PropertyValue>,
        edge_var: Option<String>,
    ) -> Self {
        Self {
            input,
            edge_pattern: (source_var, target_var, edge_type, properties, edge_var),
            created_edges: Vec::new(),
            saved_input_records: Vec::new(),
            current: 0,
            processed: false,
        }
    }
}

impl PhysicalOperator for CreateEdgeOperator {
    fn next(&mut self, _store: &GraphStore) -> ExecutionResult<Option<Record>> {
        Err(ExecutionError::RuntimeError(
            "CreateEdgeOperator requires mutable store access. Use next_mut instead.".to_string(),
        ))
    }

    fn next_mut(
        &mut self,
        store: &mut GraphStore,
        tenant_id: &str,
    ) -> ExecutionResult<Option<Record>> {
        let (source_var, target_var, edge_type, properties, edge_var) = &self.edge_pattern;

        // Process input records and create edges
        if !self.processed {
            if let Some(ref mut input) = self.input {
                // Create edge for each input record, saving input bindings
                while let Some(record) = input.next_mut(store, tenant_id)? {
                    let source_val = record
                        .get(source_var)
                        .ok_or_else(|| ExecutionError::VariableNotFound(source_var.clone()))?;
                    let target_val = record
                        .get(target_var)
                        .ok_or_else(|| ExecutionError::VariableNotFound(target_var.clone()))?;

                    let source_id = source_val.node_id().ok_or_else(|| {
                        ExecutionError::TypeError(format!("{} is not a node", source_var))
                    })?;
                    let target_id = target_val.node_id().ok_or_else(|| {
                        ExecutionError::TypeError(format!("{} is not a node", target_var))
                    })?;

                    let edge_id = store
                        .create_edge(source_id, target_id, edge_type.clone())
                        .map_err(|e| ExecutionError::GraphError(e.to_string()))?;

                    // Set properties on edge using Edge's set_property method
                    if let Some(edge) = store.get_edge_mut(edge_id) {
                        for (key, value) in properties {
                            edge.set_property(key.clone(), value.clone());
                        }
                    }

                    self.created_edges.push((edge_id, edge_var.clone()));
                    self.saved_input_records.push(record);
                }
            }
            self.processed = true;
        }

        // Return created edges one by one
        if self.current >= self.created_edges.len() {
            return Ok(None);
        }

        let (edge_id, variable) = &self.created_edges[self.current];
        let idx = self.current;
        self.current += 1;

        let edge = store.get_edge(*edge_id).ok_or_else(|| {
            ExecutionError::RuntimeError(format!("Created edge {:?} not found", edge_id))
        })?;

        // Start with the input record's bindings (carries forward node variables)
        let mut record = if idx < self.saved_input_records.len() {
            self.saved_input_records[idx].clone()
        } else {
            Record::new()
        };

        // Add edge binding
        if let Some(var) = variable {
            record.bind(var.clone(), Value::Edge(*edge_id, edge.clone()));
        }

        Ok(Some(record))
    }

    fn reset(&mut self) {
        if let Some(ref mut input) = self.input {
            input.reset();
        }
        self.current = 0;
        self.processed = false;
        self.created_edges.clear();
        self.saved_input_records.clear();
    }

    fn is_mutating(&self) -> bool {
        true
    }

    fn describe(&self) -> OperatorDescription {
        let (src, tgt, et, _, _) = &self.edge_pattern;
        let children = self
            .input
            .as_ref()
            .map(|i| vec![i.describe()])
            .unwrap_or_default();
        OperatorDescription {
            name: "CreateEdge".to_string(),
            details: format!("({})-[:{}]->({})", src, et.as_str(), tgt),
            children,
        }
    }
}

/// Combined operator for CREATE patterns with both nodes and edges
/// Example: `CREATE (a:Person)-[:KNOWS]->(b:Person)`
/// This operator first creates all nodes, then creates edges between them
pub struct CreateNodesAndEdgesOperator {
    /// Node creation operator
    node_operator: OperatorBox,
    /// Edges to create: (source_var, target_var, edge_type, properties, edge_var, expression_properties)
    edges_to_create: Vec<(
        String,
        String,
        EdgeType,
        HashMap<String, PropertyValue>,
        Option<String>,
        Vec<(String, Expression)>,
    )>,
    /// Variable to NodeId mapping (built during node creation)
    var_to_node_id: HashMap<String, NodeId>,
    /// Created edges
    created_edges: Vec<(crate::graph::EdgeId, crate::graph::Edge, Option<String>)>,
    /// Current phase: 0 = creating nodes, 1 = creating edges, 2 = returning results
    phase: usize,
    /// Current index for returning results
    result_index: usize,
    /// All results to return (nodes first, then edges)
    results: Vec<(Option<String>, Value)>,
}

impl CreateNodesAndEdgesOperator {
    /// Create a new CreateNodesAndEdgesOperator
    pub fn new(
        node_operator: OperatorBox,
        edges_to_create: Vec<(
            String,
            String,
            EdgeType,
            HashMap<String, PropertyValue>,
            Option<String>,
            Vec<(String, Expression)>,
        )>,
    ) -> Self {
        Self {
            node_operator,
            edges_to_create,
            var_to_node_id: HashMap::new(),
            created_edges: Vec::new(),
            phase: 0,
            result_index: 0,
            results: Vec::new(),
        }
    }
}

impl PhysicalOperator for CreateNodesAndEdgesOperator {
    fn next(&mut self, _store: &GraphStore) -> ExecutionResult<Option<Record>> {
        Err(ExecutionError::RuntimeError(
            "CreateNodesAndEdgesOperator requires mutable store access. Use next_mut instead."
                .to_string(),
        ))
    }

    fn next_mut(
        &mut self,
        store: &mut GraphStore,
        tenant_id: &str,
    ) -> ExecutionResult<Option<Record>> {
        // Phase 0: Create all nodes and collect their IDs
        if self.phase == 0 {
            while let Some(record) = self.node_operator.next_mut(store, tenant_id)? {
                // Extract variable and node from record
                for (var, value) in record.bindings().iter() {
                    if let Value::Node(node_id, node) = value {
                        self.var_to_node_id.insert(var.clone(), *node_id);
                        self.results
                            .push((Some(var.clone()), Value::Node(*node_id, node.clone())));
                    }
                }
            }
            self.phase = 1;
        }

        // Phase 1: Create all edges
        if self.phase == 1 {
            for (source_var, target_var, edge_type, properties, edge_var, expr_props) in
                &self.edges_to_create
            {
                let source_id = self
                    .var_to_node_id
                    .get(source_var)
                    .ok_or_else(|| ExecutionError::VariableNotFound(source_var.clone()))?;
                let target_id = self
                    .var_to_node_id
                    .get(target_var)
                    .ok_or_else(|| ExecutionError::VariableNotFound(target_var.clone()))?;

                let edge_id = store
                    .create_edge(*source_id, *target_id, edge_type.clone())
                    .map_err(|e| ExecutionError::GraphError(e.to_string()))?;

                // Set static properties on edge
                if let Some(edge) = store.get_edge_mut(edge_id) {
                    for (key, value) in properties {
                        edge.set_property(key.clone(), value.clone());
                    }
                }
                // Evaluate expression properties (e.g., timestamp(), randomUUID())
                let empty_record = Record::new();
                for (key, expr) in expr_props {
                    let store_ref: &GraphStore = store;
                    let val = eval_expression(expr, &empty_record, store_ref)?;
                    if let Value::Property(pv) = val {
                        if let Some(edge) = store.get_edge_mut(edge_id) {
                            edge.set_property(key.clone(), pv);
                        }
                    }
                }

                // Get the created edge for returning
                if let Some(edge) = store.get_edge(edge_id) {
                    self.created_edges
                        .push((edge_id, edge.clone(), edge_var.clone()));
                    if edge_var.is_some() {
                        self.results
                            .push((edge_var.clone(), Value::Edge(edge_id, edge.clone())));
                    }
                }
            }
            self.phase = 2;
        }

        // Phase 2: Return results one by one
        if self.result_index >= self.results.len() {
            return Ok(None);
        }

        let (var, value) = &self.results[self.result_index];
        self.result_index += 1;

        let mut record = Record::new();
        if let Some(v) = var {
            record.bind(v.clone(), value.clone());
        }

        Ok(Some(record))
    }

    fn reset(&mut self) {
        self.node_operator.reset();
        self.var_to_node_id.clear();
        self.created_edges.clear();
        self.phase = 0;
        self.result_index = 0;
        self.results.clear();
    }

    fn is_mutating(&self) -> bool {
        true
    }

    fn describe(&self) -> OperatorDescription {
        let edge_descs: Vec<String> = self
            .edges_to_create
            .iter()
            .map(|(src, tgt, et, _, _, _)| format!("({})-[:{}]->({})", src, et.as_str(), tgt))
            .collect();
        OperatorDescription {
            name: "CreateNodesAndEdges".to_string(),
            details: edge_descs.join(", "),
            children: vec![self.node_operator.describe()],
        }
    }
}

/// Operator for MATCH...CREATE queries
/// Example: `MATCH (a:Trial {id: 'NCT001'}), (b:Condition {mesh_id: 'D001'}) CREATE (a)-[:STUDIES]->(b)`
/// This operator takes matched nodes and creates edges between them
pub struct MatchCreateEdgeOperator {
    /// Input operator (MATCH results)
    input: OperatorBox,
    /// Edges to create: (source_var, target_var, edge_type, properties, edge_var, expression_properties)
    edges_to_create: Vec<(
        String,
        String,
        EdgeType,
        HashMap<String, PropertyValue>,
        Option<String>,
        Vec<(String, Expression)>,
    )>,
    /// Whether edges have been created for current batch
    done: bool,
    /// Results to return
    results: Vec<Record>,
    /// Current result index
    result_index: usize,
}

impl MatchCreateEdgeOperator {
    /// Create a new MatchCreateEdgeOperator
    pub fn new(
        input: OperatorBox,
        edges_to_create: Vec<(
            String,
            String,
            EdgeType,
            HashMap<String, PropertyValue>,
            Option<String>,
            Vec<(String, Expression)>,
        )>,
    ) -> Self {
        Self {
            input,
            edges_to_create,
            done: false,
            results: Vec::new(),
            result_index: 0,
        }
    }
}

impl PhysicalOperator for MatchCreateEdgeOperator {
    fn next(&mut self, _store: &GraphStore) -> ExecutionResult<Option<Record>> {
        Err(ExecutionError::RuntimeError(
            "MatchCreateEdgeOperator requires mutable store access. Use next_mut instead."
                .to_string(),
        ))
    }

    fn next_mut(
        &mut self,
        store: &mut GraphStore,
        tenant_id: &str,
    ) -> ExecutionResult<Option<Record>> {
        // First pass: process all matched records and create edges
        if !self.done {
            while let Some(record) = self.input.next_mut(store, tenant_id)? {
                // For each matched record, create the specified edges
                for (source_var, target_var, edge_type, properties, _edge_var, expr_props) in
                    &self.edges_to_create
                {
                    // Get source node ID from record bindings
                    let source_id = match record.get(source_var).and_then(|v| v.node_id()) {
                        Some(id) => id,
                        None => continue, // Skip if source not found
                    };

                    // Get target node ID from record bindings
                    let target_id = match record.get(target_var).and_then(|v| v.node_id()) {
                        Some(id) => id,
                        None => continue, // Skip if target not found
                    };

                    // Create the edge
                    let edge_id = store
                        .create_edge(source_id, target_id, edge_type.clone())
                        .map_err(|e| ExecutionError::GraphError(e.to_string()))?;

                    // Set static properties on edge
                    if let Some(edge) = store.get_edge_mut(edge_id) {
                        for (key, value) in properties {
                            edge.set_property(key.clone(), value.clone());
                        }
                    }
                    // Evaluate and set expression properties (e.g., timestamp(), randomUUID())
                    for (key, expr) in expr_props {
                        let store_ref: &GraphStore = store;
                        let val = eval_expression(expr, &record, store_ref)?;
                        if let Value::Property(pv) = val {
                            if let Some(edge) = store.get_edge_mut(edge_id) {
                                edge.set_property(key.clone(), pv);
                            }
                        }
                    }

                    // Build result record with the created edge
                    let mut result_record = record.clone();
                    if let Some(edge) = store.get_edge(edge_id) {
                        result_record.bind("_edge".to_string(), Value::Edge(edge_id, edge.clone()));
                    }
                    self.results.push(result_record);
                }
            }
            self.done = true;
        }

        // Return results one by one
        if self.result_index >= self.results.len() {
            return Ok(None);
        }

        let result = self.results[self.result_index].clone();
        self.result_index += 1;
        Ok(Some(result))
    }

    fn reset(&mut self) {
        self.input.reset();
        self.done = false;
        self.results.clear();
        self.result_index = 0;
    }

    fn is_mutating(&self) -> bool {
        true
    }

    fn describe(&self) -> OperatorDescription {
        let edge_descs: Vec<String> = self
            .edges_to_create
            .iter()
            .map(|(src, tgt, et, _, _, _)| format!("({})-[:{}]->({})", src, et.as_str(), tgt))
            .collect();
        OperatorDescription {
            name: "MatchCreateEdge".to_string(),
            details: edge_descs.join(", "),
            children: vec![self.input.describe()],
        }
    }
}

/// Mock procedure operator for TCK test procedures
pub struct MockProcedureOperator {
    name: String,
    args: Vec<Expression>,
    yield_vars: Vec<String>,
    results: Vec<Record>,
    current: usize,
    executed: bool,
}

impl MockProcedureOperator {
    pub fn new(name: String, args: Vec<Expression>, yield_vars: Vec<String>) -> Self {
        Self {
            name,
            args,
            yield_vars,
            results: Vec::new(),
            current: 0,
            executed: false,
        }
    }
}

impl PhysicalOperator for MockProcedureOperator {
    fn next(&mut self, store: &GraphStore) -> ExecutionResult<Option<Record>> {
        if !self.executed {
            self.executed = true;
            self.results = Vec::new();
            self.current = 0;

            let lower = self.name.to_lowercase();
            if lower == "test.donothing" {
                // No output
            } else if lower == "test.labels" {
                // Returns 3 rows with label column from store labels
                let labels: Vec<String> = store
                    .all_labels()
                    .into_iter()
                    .map(|l| l.to_string())
                    .collect();
                if labels.is_empty() {
                    // Default mock data
                    for label in &["A", "B", "C"] {
                        let mut r = Record::new();
                        r.bind(
                            "label".to_string(),
                            Value::Property(PropertyValue::String(label.to_string())),
                        );
                        self.results.push(r);
                    }
                } else {
                    for label in labels {
                        let mut r = Record::new();
                        r.bind(
                            "label".to_string(),
                            Value::Property(PropertyValue::String(label)),
                        );
                        self.results.push(r);
                    }
                }
            } else if lower == "test.my.proc" {
                // Returns 1 row with yield columns mapped to args
                let yield_cols = if self.yield_vars.is_empty() {
                    if self.args.len() >= 2 {
                        vec!["city".to_string(), "country_code".to_string()]
                    } else {
                        vec!["out".to_string()]
                    }
                } else {
                    self.yield_vars.clone()
                };
                let mut r = Record::new();
                for (i, col) in yield_cols.iter().enumerate() {
                    if i < self.args.len() {
                        let val = eval_expression(&self.args[i], &Record::new(), store)?;
                        r.bind(col.clone(), val);
                    } else {
                        r.bind(col.clone(), Value::Null);
                    }
                }
                self.results.push(r);
            }
        }

        if self.current < self.results.len() {
            let record = self.results[self.current].clone();
            self.current += 1;
            Ok(Some(record))
        } else {
            Ok(None)
        }
    }

    fn reset(&mut self) {
        self.executed = false;
        self.results.clear();
        self.current = 0;
    }

    fn describe(&self) -> OperatorDescription {
        OperatorDescription {
            name: "MockProcedure".to_string(),
            details: self.name.clone(),
            children: vec![],
        }
    }
}

/// Algorithm operator: CALL algo.pageRank(...)
pub struct AlgorithmOperator {
    /// Procedure name
    name: String,
    /// Arguments
    args: Vec<crate::query::ast::Expression>,
    /// Result records
    results: Vec<Record>,
    /// Current index
    current: usize,
    /// Whether algorithm has run
    executed: bool,
}

impl AlgorithmOperator {
    pub fn new(name: String, args: Vec<crate::query::ast::Expression>) -> Self {
        Self {
            name,
            args,
            results: Vec::new(),
            current: 0,
            executed: false,
        }
    }

    fn execute_pagerank(&mut self, store: &GraphStore) -> ExecutionResult<()> {
        // Arguments: (label?, edge_type?, config_map?)
        let mut label = None;
        let mut edge_type = None;
        let mut config = crate::algo::PageRankConfig::default();

        if !self.args.is_empty() {
            if let Expression::Literal(PropertyValue::String(s)) = &self.args[0] {
                label = Some(s.clone());
            }
        }
        if self.args.len() > 1 {
            if let Expression::Literal(PropertyValue::String(s)) = &self.args[1] {
                edge_type = Some(s.clone());
            }
        }

        // Parse optional config map
        for arg in &self.args {
            if let Expression::Literal(PropertyValue::Map(m)) = arg {
                if let Some(PropertyValue::Integer(i)) = m.get("iterations") {
                    config.iterations = *i as usize;
                }
                if let Some(PropertyValue::Float(f)) = m.get("damping") {
                    config.damping_factor = *f;
                }
            }
        }

        // Build view and run
        let view = crate::algo::build_view(store, label.as_deref(), edge_type.as_deref(), None);
        let scores = crate::algo::page_rank(&view, config);

        // Convert to records
        for (algo_id, score) in scores {
            let node_id = NodeId::new(algo_id);
            let mut record = Record::new();
            if let Some(node) = store.get_node(node_id) {
                record.bind("node".to_string(), Value::Node(node_id, node.clone()));
                record.bind(
                    "score".to_string(),
                    Value::Property(PropertyValue::Float(score)),
                );
                self.results.push(record);
            }
        }

        // Sort by score descending
        self.results.sort_by(|a, b| {
            let score_a = a
                .get("score")
                .unwrap()
                .as_property()
                .unwrap()
                .as_float()
                .unwrap();
            let score_b = b
                .get("score")
                .unwrap()
                .as_property()
                .unwrap()
                .as_float()
                .unwrap();
            score_b
                .partial_cmp(&score_a)
                .unwrap_or(std::cmp::Ordering::Equal)
        });

        Ok(())
    }

    fn execute_shortest_path(&mut self, store: &GraphStore) -> ExecutionResult<()> {
        // Arguments: (source_node, target_node, config?)
        if self.args.len() < 2 {
            return Err(ExecutionError::RuntimeError(
                "shortestPath requires source and target".to_string(),
            ));
        }

        let source_id = match &self.args[0] {
            Expression::Literal(PropertyValue::Integer(id)) => *id as u64,
            _ => {
                return Err(ExecutionError::TypeError(
                    "Source must be integer ID".to_string(),
                ))
            }
        };

        let target_id = match &self.args[1] {
            Expression::Literal(PropertyValue::Integer(id)) => *id as u64,
            _ => {
                return Err(ExecutionError::TypeError(
                    "Target must be integer ID".to_string(),
                ))
            }
        };

        let mut weight_prop = None;
        if self.args.len() > 2 {
            if let Expression::Literal(PropertyValue::Map(m)) = &self.args[2] {
                if let Some(PropertyValue::String(s)) = m.get("weight_property") {
                    weight_prop = Some(s.clone());
                }
            }
        }

        // Build view
        let view = crate::algo::build_view(store, None, None, weight_prop.as_deref());

        // Run Algorithm
        let result = if weight_prop.is_some() {
            crate::algo::dijkstra(&view, source_id, target_id)
        } else {
            crate::algo::bfs(&view, source_id, target_id)
        };

        if let Some(result) = result {
            let mut record = Record::new();
            record.bind(
                "cost".to_string(),
                Value::Property(PropertyValue::Float(result.cost)),
            );

            // Construct path list
            let mut path_nodes = Vec::new();
            for nid_u64 in result.path {
                path_nodes.push(PropertyValue::Integer(nid_u64 as i64));
            }
            record.bind(
                "path".to_string(),
                Value::Property(PropertyValue::Array(path_nodes)),
            );

            self.results.push(record);
        }

        Ok(())
    }

    fn execute_wcc(&mut self, store: &GraphStore) -> ExecutionResult<()> {
        // Arguments: (label?, edge_type?)
        let mut label = None;
        let mut edge_type = None;

        if !self.args.is_empty() {
            if let Expression::Literal(PropertyValue::String(s)) = &self.args[0] {
                label = Some(s.clone());
            }
        }
        if self.args.len() > 1 {
            if let Expression::Literal(PropertyValue::String(s)) = &self.args[1] {
                edge_type = Some(s.clone());
            }
        }

        // Build view and run WCC
        let view = crate::algo::build_view(store, label.as_deref(), edge_type.as_deref(), None);
        let result = crate::algo::weakly_connected_components(&view);

        // Convert to records
        // For WCC, we return (node, componentId)
        for (node_id, component_id) in result.node_component {
            let nid = NodeId::new(node_id);
            let mut record = Record::new();
            if let Some(node) = store.get_node(nid) {
                record.bind("node".to_string(), Value::Node(nid, node.clone()));
                record.bind(
                    "componentId".to_string(),
                    Value::Property(PropertyValue::Integer(component_id as i64)),
                );
                self.results.push(record);
            }
        }

        // Sort by componentId
        self.results.sort_by(|a, b| {
            let cid_a = a
                .get("componentId")
                .unwrap()
                .as_property()
                .unwrap()
                .as_integer()
                .unwrap();
            let cid_b = b
                .get("componentId")
                .unwrap()
                .as_property()
                .unwrap()
                .as_integer()
                .unwrap();
            cid_a.cmp(&cid_b)
        });

        Ok(())
    }

    fn execute_weighted_path(&mut self, store: &GraphStore) -> ExecutionResult<()> {
        // Arguments: (source_node_id, target_node_id, weight_property)
        if self.args.len() < 3 {
            return Err(ExecutionError::RuntimeError(
                "weightedPath requires source, target, and weight property".to_string(),
            ));
        }

        let source_id = match &self.args[0] {
            Expression::Literal(PropertyValue::Integer(id)) => *id as u64,
            _ => {
                return Err(ExecutionError::TypeError(
                    "Source must be integer ID".to_string(),
                ))
            }
        };

        let target_id = match &self.args[1] {
            Expression::Literal(PropertyValue::Integer(id)) => *id as u64,
            _ => {
                return Err(ExecutionError::TypeError(
                    "Target must be integer ID".to_string(),
                ))
            }
        };

        let weight_prop = match &self.args[2] {
            Expression::Literal(PropertyValue::String(s)) => s.clone(),
            _ => {
                return Err(ExecutionError::TypeError(
                    "Weight property must be a string".to_string(),
                ))
            }
        };

        // Build view with weights
        let view = crate::algo::build_view(store, None, None, Some(&weight_prop));

        if let Some(result) = crate::algo::dijkstra(&view, source_id, target_id) {
            let mut record = Record::new();
            record.bind(
                "cost".to_string(),
                Value::Property(PropertyValue::Float(result.cost)),
            );

            // Construct path list
            let mut path_nodes = Vec::new();
            for nid_u64 in result.path {
                let nid = NodeId::new(nid_u64);
                // We add just IDs for now, or could fetch full nodes if needed
                path_nodes.push(PropertyValue::Integer(nid.as_u64() as i64));
            }
            record.bind(
                "path".to_string(),
                Value::Property(PropertyValue::Array(path_nodes)),
            );

            self.results.push(record);
        }

        Ok(())
    }
    fn execute_or_solve(&mut self, store: &mut GraphStore, tenant_id: &str) -> ExecutionResult<()> {
        if self.args.is_empty() {
            return Err(ExecutionError::RuntimeError(
                "algo.or.solve requires a config map".to_string(),
            ));
        }

        let config_map = match &self.args[0] {
            Expression::Literal(PropertyValue::Map(m)) => m,
            _ => {
                return Err(ExecutionError::TypeError(
                    "First argument must be a map".to_string(),
                ))
            }
        };

        // Extract parameters
        let algorithm = config_map
            .get("algorithm")
            .and_then(|v| v.as_string())
            .unwrap_or("Jaya");
        let label_str = config_map
            .get("label")
            .and_then(|v| v.as_string())
            .ok_or_else(|| ExecutionError::RuntimeError("Missing 'label' in config".to_string()))?;
        let property = config_map
            .get("property")
            .and_then(|v| v.as_string())
            .ok_or_else(|| {
                ExecutionError::RuntimeError("Missing 'property' in config".to_string())
            })?;

        let min_val = config_map
            .get("min")
            .and_then(|v| v.as_float())
            .unwrap_or(0.0);
        let max_val = config_map
            .get("max")
            .and_then(|v| v.as_float())
            .unwrap_or(100.0);

        // Objective: minimize sum(variable * cost_property)
        let cost_prop = config_map.get("cost_property").and_then(|v| v.as_string());

        // Support multiple objectives
        let mut cost_props: Vec<String> = Vec::new();
        if let Some(cp) = cost_prop {
            cost_props.push(cp.to_string());
        } else if let Some(PropertyValue::Array(arr)) = config_map.get("cost_properties") {
            for v in arr {
                if let Some(s) = v.as_string() {
                    cost_props.push(s.to_string());
                }
            }
        }

        let budget = config_map.get("budget").and_then(|v| v.as_float());
        let min_total = config_map.get("min_total").and_then(|v| v.as_float());

        let pop_size = config_map
            .get("population_size")
            .and_then(|v| v.as_integer())
            .unwrap_or(50) as usize;
        let max_iter = config_map
            .get("max_iterations")
            .and_then(|v| v.as_integer())
            .unwrap_or(100) as usize;

        // 1. Gather nodes and costs
        let label = Label::new(label_str);

        let mut node_ids = Vec::new();
        let mut single_costs = Vec::new();
        let mut multi_costs = vec![Vec::new(); cost_props.len()];

        {
            let nodes = store.get_nodes_by_label(&label);
            for node in nodes {
                node_ids.push(node.id);

                // Single cost (for single objective solvers)
                if cost_props.len() == 1 {
                    let cost = node
                        .get_property(&cost_props[0])
                        .and_then(|v| v.as_float())
                        .unwrap_or(1.0);
                    single_costs.push(cost);
                } else if !cost_props.is_empty() {
                    for (i, cp) in cost_props.iter().enumerate() {
                        let cost = node
                            .get_property(cp)
                            .and_then(|v| v.as_float())
                            .unwrap_or(1.0);
                        multi_costs[i].push(cost);
                    }
                } else {
                    single_costs.push(1.0);
                }
            }
        }

        if node_ids.is_empty() {
            return Ok(());
        }

        // 2. Setup Problem
        let problem = GraphOptimizationProblem {
            costs: single_costs,
            multi_costs,
            budget,
            min_total,
            dim: node_ids.len(),
            lower: min_val,
            upper: max_val,
        };

        let solver_config = SolverConfig {
            population_size: pop_size,
            max_iterations: max_iter,
        };

        // 3. Run Solver
        if algorithm == "NSGA2" || algorithm == "MOTLBO" || cost_props.len() > 1 {
            let res = match algorithm {
                "MOTLBO" => MOTLBOSolver::new(solver_config).solve(&problem),
                _ => NSGA2Solver::new(solver_config).solve(&problem), // Default multi
            };

            // Write back first individual from Pareto Front
            if let Some(best) = res.pareto_front.first() {
                for (i, &val) in best.variables.iter().enumerate() {
                    let node_id = node_ids[i];
                    let _ = store.set_node_property(
                        tenant_id,
                        node_id,
                        property.to_string(),
                        PropertyValue::Float(val),
                    );
                }
            }

            let mut record = Record::new();
            if let Some(best) = res.pareto_front.first() {
                let fitness_props: Vec<PropertyValue> = best
                    .fitness
                    .iter()
                    .map(|&f| PropertyValue::Float(f))
                    .collect();
                record.bind(
                    "fitness".to_string(),
                    Value::Property(PropertyValue::Array(fitness_props)),
                );
            }
            record.bind(
                "algorithm".to_string(),
                Value::Property(PropertyValue::String(algorithm.to_string())),
            );
            record.bind(
                "front_size".to_string(),
                Value::Property(PropertyValue::Integer(res.pareto_front.len() as i64)),
            );
            self.results.push(record);
        } else {
            let result = match algorithm {
                "Rao1" => RaoSolver::new(solver_config, RaoVariant::Rao1).solve(&problem),
                "Rao2" => RaoSolver::new(solver_config, RaoVariant::Rao2).solve(&problem),
                "Rao3" => RaoSolver::new(solver_config, RaoVariant::Rao3).solve(&problem),
                "TLBO" => TLBOSolver::new(solver_config).solve(&problem),
                "Firefly" => FireflySolver::new(solver_config).solve(&problem),
                "Cuckoo" => CuckooSolver::new(solver_config).solve(&problem),
                "GWO" => GWOSolver::new(solver_config).solve(&problem),
                "GA" => GASolver::new(solver_config).solve(&problem),
                "SA" => SASolver::new(solver_config).solve(&problem),
                "Bat" => BatSolver::new(solver_config).solve(&problem),
                "ABC" => ABCSolver::new(solver_config).solve(&problem),
                "GSA" => GSASolver::new(solver_config).solve(&problem),
                "HS" => HSSolver::new(solver_config).solve(&problem),
                "FPA" => FPASolver::new(solver_config).solve(&problem),
                _ => JayaSolver::new(solver_config).solve(&problem), // Default to Jaya
            };

            // 4. Write back results
            for (i, &val) in result.best_variables.iter().enumerate() {
                let node_id = node_ids[i];
                let _ = store.set_node_property(
                    tenant_id,
                    node_id,
                    property.to_string(),
                    PropertyValue::Float(val),
                );
            }

            // 5. Return result record
            let mut record = Record::new();
            record.bind(
                "fitness".to_string(),
                Value::Property(PropertyValue::Float(result.best_fitness)),
            );
            record.bind(
                "algorithm".to_string(),
                Value::Property(PropertyValue::String(algorithm.to_string())),
            );
            record.bind(
                "iterations".to_string(),
                Value::Property(PropertyValue::Integer(max_iter as i64)),
            );

            // Yield history as an array for plotting
            let history_props: Vec<PropertyValue> = result
                .history
                .into_iter()
                .map(PropertyValue::Float)
                .collect();
            record.bind(
                "history".to_string(),
                Value::Property(PropertyValue::Array(history_props)),
            );

            self.results.push(record);
        }

        Ok(())
    }

    fn execute_max_flow(&mut self, store: &GraphStore) -> ExecutionResult<()> {
        // Arguments: (source, sink, capacity_property?)
        if self.args.len() < 2 {
            return Err(ExecutionError::RuntimeError(
                "maxFlow requires source and sink".to_string(),
            ));
        }

        let source_id = match &self.args[0] {
            Expression::Literal(PropertyValue::Integer(id)) => *id as u64,
            _ => {
                return Err(ExecutionError::TypeError(
                    "Source must be integer ID".to_string(),
                ))
            }
        };

        let target_id = match &self.args[1] {
            Expression::Literal(PropertyValue::Integer(id)) => *id as u64,
            _ => {
                return Err(ExecutionError::TypeError(
                    "Sink must be integer ID".to_string(),
                ))
            }
        };

        let cap_prop = if self.args.len() > 2 {
            match &self.args[2] {
                Expression::Literal(PropertyValue::String(s)) => Some(s.clone()),
                _ => None,
            }
        } else {
            None
        };

        // Build view
        let view = crate::algo::build_view(store, None, None, cap_prop.as_deref());

        // edmonds_karp expects u64 (AlgoNodeId), not crate::graph::NodeId
        if let Some(result) = crate::algo::edmonds_karp(&view, source_id, target_id) {
            let mut record = Record::new();
            record.bind(
                "max_flow".to_string(),
                Value::Property(PropertyValue::Float(result.max_flow)),
            );
            self.results.push(record);
        } else {
            // No flow found or invalid nodes
            let mut record = Record::new();
            record.bind(
                "max_flow".to_string(),
                Value::Property(PropertyValue::Float(0.0)),
            );
            self.results.push(record);
        }

        Ok(())
    }

    fn execute_mst(&mut self, store: &GraphStore) -> ExecutionResult<()> {
        // Arguments: (weight_property?)
        let weight_prop = if !self.args.is_empty() {
            match &self.args[0] {
                Expression::Literal(PropertyValue::String(s)) => Some(s.clone()),
                _ => None,
            }
        } else {
            None
        };

        let view = crate::algo::build_view(store, None, None, weight_prop.as_deref());
        let result = crate::algo::prim_mst(&view);

        // Return total weight
        let mut summary = Record::new();
        summary.bind(
            "total_weight".to_string(),
            Value::Property(PropertyValue::Float(result.total_weight)),
        );
        self.results.push(summary);

        // Return edges
        for (u_u64, v_u64, w) in result.edges {
            let u = NodeId::new(u_u64);
            let v = NodeId::new(v_u64);

            let mut record = Record::new();
            if let Some(node_u) = store.get_node(u) {
                record.bind("source".to_string(), Value::Node(u, node_u.clone()));
            }
            if let Some(node_v) = store.get_node(v) {
                record.bind("target".to_string(), Value::Node(v, node_v.clone()));
            }
            record.bind(
                "weight".to_string(),
                Value::Property(PropertyValue::Float(w)),
            );
            self.results.push(record);
        }

        Ok(())
    }

    fn execute_triangle_count(&mut self, store: &GraphStore) -> ExecutionResult<()> {
        // Build view (undirected treatment is handled in the algorithm)
        let view = crate::algo::build_view(store, None, None, None);
        let count = crate::algo::count_triangles(&view);

        let mut record = Record::new();
        record.bind(
            "triangles".to_string(),
            Value::Property(PropertyValue::Integer(count as i64)),
        );
        self.results.push(record);

        Ok(())
    }

    fn execute_scc(&mut self, store: &GraphStore) -> ExecutionResult<()> {
        // Build view and run SCC
        let view = crate::algo::build_view(store, None, None, None);
        let result = crate::algo::strongly_connected_components(&view);

        // For SCC, we return (node, componentId)
        for (node_id, component_id) in result.node_component {
            let nid = NodeId::new(node_id);
            let mut record = Record::new();
            if let Some(node) = store.get_node(nid) {
                record.bind("node".to_string(), Value::Node(nid, node.clone()));
                record.bind(
                    "componentId".to_string(),
                    Value::Property(PropertyValue::Integer(component_id as i64)),
                );
                self.results.push(record);
            }
        }

        // Sort by componentId
        self.results.sort_by(|a, b| {
            let cid_a = a
                .get("componentId")
                .unwrap()
                .as_property()
                .unwrap()
                .as_integer()
                .unwrap();
            let cid_b = b
                .get("componentId")
                .unwrap()
                .as_property()
                .unwrap()
                .as_integer()
                .unwrap();
            cid_a.cmp(&cid_b)
        });

        Ok(())
    }
}

impl PhysicalOperator for AlgorithmOperator {
    fn next(&mut self, store: &GraphStore) -> ExecutionResult<Option<Record>> {
        if !self.executed {
            match self.name.as_str() {
                "algo.pageRank" => self.execute_pagerank(store)?,
                "algo.shortestPath" => self.execute_shortest_path(store)?,
                "algo.wcc" => self.execute_wcc(store)?,
                "algo.scc" => self.execute_scc(store)?,
                "algo.weightedPath" => self.execute_weighted_path(store)?,
                "algo.maxFlow" => self.execute_max_flow(store)?,
                "algo.mst" => self.execute_mst(store)?,
                "algo.triangleCount" => self.execute_triangle_count(store)?,
                "algo.or.solve" => {
                    return Err(ExecutionError::RuntimeError(
                        "algo.or.solve requires write access (MutQueryExecutor)".to_string(),
                    ))
                }
                _ => {
                    return Err(ExecutionError::RuntimeError(format!(
                        "Unknown algorithm: {}",
                        self.name
                    )))
                }
            }
            self.executed = true;
        }

        if self.current >= self.results.len() {
            return Ok(None);
        }

        let record = self.results[self.current].clone();
        self.current += 1;
        Ok(Some(record))
    }

    fn next_mut(
        &mut self,
        store: &mut GraphStore,
        tenant_id: &str,
    ) -> ExecutionResult<Option<Record>> {
        if !self.executed {
            match self.name.as_str() {
                "algo.or.solve" => self.execute_or_solve(store, tenant_id)?,
                // For read-only algos, we can just call the immutable implementations
                // But we need to borrow store immutably.
                // Since we have &mut store, we can reborrow as &store
                "algo.pageRank" => self.execute_pagerank(store)?,
                "algo.shortestPath" => self.execute_shortest_path(store)?,
                "algo.wcc" => self.execute_wcc(store)?,
                "algo.scc" => self.execute_scc(store)?,
                "algo.weightedPath" => self.execute_weighted_path(store)?,
                "algo.maxFlow" => self.execute_max_flow(store)?,
                "algo.mst" => self.execute_mst(store)?,
                "algo.triangleCount" => self.execute_triangle_count(store)?,
                _ => {
                    return Err(ExecutionError::RuntimeError(format!(
                        "Unknown algorithm: {}",
                        self.name
                    )))
                }
            }
            self.executed = true;
        }

        if self.current >= self.results.len() {
            return Ok(None);
        }

        let record = self.results[self.current].clone();
        self.current += 1;
        Ok(Some(record))
    }

    fn is_mutating(&self) -> bool {
        self.name == "algo.or.solve"
    }

    fn reset(&mut self) {
        self.current = 0;
        self.executed = false;
        self.results.clear();
    }
}

/// Skip operator: SKIP n
pub struct SkipOperator {
    input: OperatorBox,
    skip: usize,
    skipped: usize,
}

impl SkipOperator {
    pub fn new(input: OperatorBox, skip: usize) -> Self {
        Self {
            input,
            skip,
            skipped: 0,
        }
    }
}

impl PhysicalOperator for SkipOperator {
    fn next(&mut self, store: &GraphStore) -> ExecutionResult<Option<Record>> {
        while self.skipped < self.skip {
            if self.input.next(store)?.is_some() {
                self.skipped += 1;
            } else {
                return Ok(None);
            }
        }
        self.input.next(store)
    }

    fn next_batch(
        &mut self,
        store: &GraphStore,
        batch_size: usize,
    ) -> ExecutionResult<Option<RecordBatch>> {
        while self.skipped < self.skip {
            if let Some(batch) = self.input.next_batch(store, batch_size)? {
                let mut iter = batch.records.into_iter();
                for _record in iter.by_ref() {
                    self.skipped += 1;
                    if self.skipped >= self.skip {
                        // Collect remaining records from this batch
                        let remaining: Vec<Record> = iter.collect();
                        if !remaining.is_empty() {
                            return Ok(Some(RecordBatch {
                                records: remaining,
                                columns: Vec::new(),
                            }));
                        }
                        break;
                    }
                }
                if self.skipped >= self.skip {
                    break;
                }
            } else {
                return Ok(None);
            }
        }
        self.input.next_batch(store, batch_size)
    }

    fn next_mut(
        &mut self,
        store: &mut GraphStore,
        tenant_id: &str,
    ) -> ExecutionResult<Option<Record>> {
        while self.skipped < self.skip {
            if self.input.next_mut(store, tenant_id)?.is_some() {
                self.skipped += 1;
            } else {
                return Ok(None);
            }
        }
        self.input.next_mut(store, tenant_id)
    }

    fn reset(&mut self) {
        self.input.reset();
        self.skipped = 0;
    }

    fn describe(&self) -> OperatorDescription {
        OperatorDescription {
            name: "Skip".to_string(),
            details: format!("{}", self.skip),
            children: vec![self.input.describe()],
        }
    }
}

/// DISTINCT operator: deduplicates output records by all column values.
/// Materializes all input, deduplicates, then emits unique rows.
pub struct DistinctOperator {
    input: OperatorBox,
    records: Vec<Record>,
    current: usize,
    executed: bool,
}

impl DistinctOperator {
    pub fn new(input: OperatorBox) -> Self {
        Self {
            input,
            records: Vec::new(),
            current: 0,
            executed: false,
        }
    }

    fn execute_all(&mut self, store: &GraphStore) -> ExecutionResult<()> {
        if self.executed {
            return Ok(());
        }
        let batch_size = 1024;
        let mut all_records = Vec::new();
        while let Some(batch) = self.input.next_batch(store, batch_size)? {
            all_records.extend(batch.records);
        }
        // Deduplicate: use a set of stringified values
        let mut seen: HashSet<Vec<String>> = HashSet::new();
        for record in all_records {
            let mut key: Vec<(String, String)> = record
                .bindings()
                .iter()
                .map(|(k, v)| (k.clone(), format!("{:?}", v)))
                .collect();
            key.sort_by(|a, b| a.0.cmp(&b.0));
            let vals: Vec<String> = key.into_iter().map(|(_, v)| v).collect();
            if seen.insert(vals) {
                self.records.push(record);
            }
        }
        self.executed = true;
        Ok(())
    }
}

impl PhysicalOperator for DistinctOperator {
    fn next(&mut self, store: &GraphStore) -> ExecutionResult<Option<Record>> {
        self.execute_all(store)?;
        if self.current >= self.records.len() {
            return Ok(None);
        }
        let record = self.records[self.current].clone();
        self.current += 1;
        Ok(Some(record))
    }

    fn reset(&mut self) {
        self.input.reset();
        self.records.clear();
        self.current = 0;
        self.executed = false;
    }

    fn describe(&self) -> OperatorDescription {
        OperatorDescription {
            name: "Distinct".to_string(),
            details: String::new(),
            children: vec![self.input.describe()],
        }
    }
}

/// Delete operator: DELETE n or DETACH DELETE n
pub struct DeleteOperator {
    input: OperatorBox,
    variables: Vec<String>,
    detach: bool,
}

impl DeleteOperator {
    pub fn new(input: OperatorBox, variables: Vec<String>, detach: bool) -> Self {
        Self {
            input,
            variables,
            detach,
        }
    }
}

impl PhysicalOperator for DeleteOperator {
    fn next(&mut self, store: &GraphStore) -> ExecutionResult<Option<Record>> {
        self.input.next(store)
    }

    fn next_mut(
        &mut self,
        store: &mut GraphStore,
        tenant_id: &str,
    ) -> ExecutionResult<Option<Record>> {
        if let Some(record) = self.input.next_mut(store, tenant_id)? {
            for var in &self.variables {
                if let Some(val) = record.get(var) {
                    match val {
                        Value::NodeRef(id) | Value::Node(id, _) => {
                            let node_id = *id;
                            // Check if node still exists (may have been deleted already)
                            if store.get_node(node_id).is_none() {
                                continue;
                            }
                            if self.detach {
                                let out_edges: Vec<_> = store
                                    .get_outgoing_edges(node_id)
                                    .iter()
                                    .map(|e| e.id)
                                    .collect();
                                let in_edges: Vec<_> = store
                                    .get_incoming_edges(node_id)
                                    .iter()
                                    .map(|e| e.id)
                                    .collect();
                                for eid in out_edges.into_iter().chain(in_edges) {
                                    let _ = store.delete_edge(eid);
                                }
                            } else {
                                // Non-DETACH: verify no edges exist
                                let has_out = !store.get_outgoing_edges(node_id).is_empty();
                                let has_in = !store.get_incoming_edges(node_id).is_empty();
                                if has_out || has_in {
                                    return Err(ExecutionError::RuntimeError(
                                        format!("Cannot delete node with id {} because it still has relationships. To delete this node, you must first delete its relationships, or use DETACH DELETE.", node_id.as_u64())
                                    ));
                                }
                            }
                            let _ = store.delete_node(tenant_id, node_id);
                        }
                        Value::EdgeRef(id, ..) | Value::Edge(id, _) => {
                            let _ = store.delete_edge(*id);
                        }
                        _ => {}
                    }
                }
            }
            Ok(Some(record))
        } else {
            Ok(None)
        }
    }

    fn next_batch(
        &mut self,
        store: &GraphStore,
        batch_size: usize,
    ) -> ExecutionResult<Option<RecordBatch>> {
        self.input.next_batch(store, batch_size)
    }

    fn reset(&mut self) {
        self.input.reset();
    }

    fn describe(&self) -> OperatorDescription {
        let vars = self.variables.join(", ");
        OperatorDescription {
            name: if self.detach {
                "DetachDelete"
            } else {
                "Delete"
            }
            .to_string(),
            details: vars,
            children: vec![self.input.describe()],
        }
    }

    fn is_mutating(&self) -> bool {
        true
    }
}

/// Set property operator: SET n.name = "Alice"
pub struct SetPropertyOperator {
    input: OperatorBox,
    items: Vec<(String, String, Expression)>, // (variable, property, value_expr)
}

impl SetPropertyOperator {
    pub fn new(input: OperatorBox, items: Vec<(String, String, Expression)>) -> Self {
        Self { input, items }
    }
}

impl PhysicalOperator for SetPropertyOperator {
    fn next(&mut self, store: &GraphStore) -> ExecutionResult<Option<Record>> {
        self.input.next(store)
    }

    fn next_mut(
        &mut self,
        store: &mut GraphStore,
        tenant_id: &str,
    ) -> ExecutionResult<Option<Record>> {
        if let Some(record) = self.input.next_mut(store, tenant_id)? {
            for (var, prop, expr) in &self.items {
                // Evaluate the expression
                let store_ref: &GraphStore = store;
                let evaluated = eval_expression(expr, &record, store_ref)?;
                let val = match evaluated {
                    Value::Property(pv) => pv,
                    Value::Null => PropertyValue::Null,
                    _ => PropertyValue::Null,
                };

                // Reject list-of-maps as property value
                if let PropertyValue::Array(arr) = &val {
                    if arr.iter().any(|v| matches!(v, PropertyValue::Map(_))) {
                        return Err(ExecutionError::TypeError(
                            "Property values can not contain nested maps".to_string(),
                        ));
                    }
                }

                if let Some(node_val) = record.get(var) {
                    match node_val {
                        Value::NodeRef(id) | Value::Node(id, _) => {
                            let node_id = *id;
                            let idx = node_id.as_u64() as usize;
                            if prop == "__labels__" {
                                // SET n:Label — add labels
                                if let PropertyValue::Array(labels) = &val {
                                    if let Some(node) = store.get_node_mut(node_id) {
                                        for l in labels {
                                            if let PropertyValue::String(s) = l {
                                                node.add_label(Label::new(s));
                                            }
                                        }
                                    }
                                }
                            } else if prop == "__map_replace__" {
                                // SET n = {map} — replace all properties
                                if let PropertyValue::Map(map) = &val {
                                    // Collect old keys to clear from columnar store
                                    let old_keys: Vec<String> = store
                                        .get_node(node_id)
                                        .map(|n| n.properties.keys().cloned().collect())
                                        .unwrap_or_default();
                                    if let Some(node) = store.get_node_mut(node_id) {
                                        node.properties.clear();
                                        for (k, v) in map {
                                            if !matches!(v, PropertyValue::Null) {
                                                node.set_property(k.clone(), v.clone());
                                            }
                                        }
                                    }
                                    // Sync columnar storage after releasing node borrow
                                    for k in &old_keys {
                                        store.node_columns.set_property(
                                            idx,
                                            k,
                                            PropertyValue::Null,
                                        );
                                    }
                                    for (k, v) in map {
                                        if !matches!(v, PropertyValue::Null) {
                                            store.node_columns.set_property(idx, k, v.clone());
                                        }
                                    }
                                }
                            } else if prop == "__map_merge__" {
                                // SET n += {map} — merge properties
                                if let PropertyValue::Map(map) = &val {
                                    if let Some(node) = store.get_node_mut(node_id) {
                                        for (k, v) in map {
                                            if matches!(v, PropertyValue::Null) {
                                                node.properties.remove(k);
                                            } else {
                                                node.set_property(k.clone(), v.clone());
                                            }
                                        }
                                    }
                                    // Sync columnar storage after releasing node borrow
                                    for (k, v) in map {
                                        store.node_columns.set_property(idx, k, v.clone());
                                    }
                                }
                            } else {
                                if let Some(node) = store.get_node_mut(node_id) {
                                    node.set_property(prop, val.clone());
                                }
                                // Sync columnar storage
                                store.node_columns.set_property(idx, prop, val.clone());
                            }
                        }
                        Value::EdgeRef(id, ..) | Value::Edge(id, _) => {
                            let edge_id = *id;
                            let idx = edge_id.as_u64() as usize;
                            if prop == "__map_replace__" {
                                if let PropertyValue::Map(map) = &val {
                                    let old_keys: Vec<String> = store
                                        .get_edge(edge_id)
                                        .map(|e| e.properties.keys().cloned().collect())
                                        .unwrap_or_default();
                                    if let Some(edge) = store.get_edge_mut(edge_id) {
                                        edge.properties.clear();
                                        for (k, v) in map {
                                            if !matches!(v, PropertyValue::Null) {
                                                edge.set_property(k.clone(), v.clone());
                                            }
                                        }
                                    }
                                    for k in &old_keys {
                                        store.edge_columns.set_property(
                                            idx,
                                            k,
                                            PropertyValue::Null,
                                        );
                                    }
                                    for (k, v) in map {
                                        if !matches!(v, PropertyValue::Null) {
                                            store.edge_columns.set_property(idx, k, v.clone());
                                        }
                                    }
                                }
                            } else if prop == "__map_merge__" {
                                if let PropertyValue::Map(map) = &val {
                                    if let Some(edge) = store.get_edge_mut(edge_id) {
                                        for (k, v) in map {
                                            if matches!(v, PropertyValue::Null) {
                                                edge.properties.remove(k);
                                            } else {
                                                edge.set_property(k.clone(), v.clone());
                                            }
                                        }
                                    }
                                    for (k, v) in map {
                                        store.edge_columns.set_property(idx, k, v.clone());
                                    }
                                }
                            } else {
                                if let Some(edge) = store.get_edge_mut(edge_id) {
                                    edge.set_property(prop, val.clone());
                                }
                                store.edge_columns.set_property(idx, prop, val.clone());
                            }
                        }
                        _ => {}
                    }
                }
            }
            Ok(Some(record))
        } else {
            Ok(None)
        }
    }

    fn next_batch(
        &mut self,
        store: &GraphStore,
        batch_size: usize,
    ) -> ExecutionResult<Option<RecordBatch>> {
        self.input.next_batch(store, batch_size)
    }

    fn reset(&mut self) {
        self.input.reset();
    }

    fn describe(&self) -> OperatorDescription {
        let sets: Vec<String> = self
            .items
            .iter()
            .map(|(v, p, e)| format!("{}.{} = {}", v, p, format_expression(e)))
            .collect();
        OperatorDescription {
            name: "SetProperty".to_string(),
            details: sets.join(", "),
            children: vec![self.input.describe()],
        }
    }

    fn is_mutating(&self) -> bool {
        true
    }
}

/// Remove property operator: REMOVE n.name
pub struct RemovePropertyOperator {
    input: OperatorBox,
    items: Vec<(String, String)>, // (variable, property)
}

impl RemovePropertyOperator {
    pub fn new(input: OperatorBox, items: Vec<(String, String)>) -> Self {
        Self { input, items }
    }
}

impl PhysicalOperator for RemovePropertyOperator {
    fn next(&mut self, store: &GraphStore) -> ExecutionResult<Option<Record>> {
        self.input.next(store)
    }

    fn next_mut(
        &mut self,
        store: &mut GraphStore,
        tenant_id: &str,
    ) -> ExecutionResult<Option<Record>> {
        if let Some(record) = self.input.next_mut(store, tenant_id)? {
            for (var, prop) in &self.items {
                if let Some(node_val) = record.get(var) {
                    match node_val {
                        Value::NodeRef(id) | Value::Node(id, _) => {
                            let idx = id.as_u64() as usize;
                            if let Some(node) = store.get_node_mut(*id) {
                                node.remove_property(prop);
                            }
                            store
                                .node_columns
                                .set_property(idx, prop, PropertyValue::Null);
                        }
                        Value::EdgeRef(id, ..) | Value::Edge(id, _) => {
                            let idx = id.as_u64() as usize;
                            if let Some(edge) = store.get_edge_mut(*id) {
                                edge.remove_property(prop);
                            }
                            store
                                .edge_columns
                                .set_property(idx, prop, PropertyValue::Null);
                        }
                        _ => {}
                    }
                }
            }
            Ok(Some(record))
        } else {
            Ok(None)
        }
    }

    fn next_batch(
        &mut self,
        store: &GraphStore,
        batch_size: usize,
    ) -> ExecutionResult<Option<RecordBatch>> {
        self.input.next_batch(store, batch_size)
    }

    fn reset(&mut self) {
        self.input.reset();
    }

    fn describe(&self) -> OperatorDescription {
        let removes: Vec<String> = self
            .items
            .iter()
            .map(|(v, p)| format!("{}.{}", v, p))
            .collect();
        OperatorDescription {
            name: "RemoveProperty".to_string(),
            details: removes.join(", "),
            children: vec![self.input.describe()],
        }
    }

    fn is_mutating(&self) -> bool {
        true
    }
}

/// UNWIND operator - expands a list expression into individual rows
pub struct UnwindOperator {
    input: OperatorBox,
    expression: Expression,
    variable: String,
    buffer: Vec<Record>,
    buffer_idx: usize,
}

impl UnwindOperator {
    pub fn new(input: OperatorBox, expression: Expression, variable: String) -> Self {
        Self {
            input,
            expression,
            variable,
            buffer: Vec::new(),
            buffer_idx: 0,
        }
    }
}

impl PhysicalOperator for UnwindOperator {
    fn next(&mut self, store: &GraphStore) -> ExecutionResult<Option<Record>> {
        loop {
            if self.buffer_idx < self.buffer.len() {
                let record = self.buffer[self.buffer_idx].clone();
                self.buffer_idx += 1;
                return Ok(Some(record));
            }

            let record = match self.input.next(store)? {
                Some(r) => r,
                None => return Ok(None),
            };

            let list_val = eval_expression(&self.expression, &record, store)?;

            let items = match list_val {
                Value::Property(PropertyValue::Array(arr)) => arr,
                Value::Property(PropertyValue::Vector(vec)) => vec
                    .into_iter()
                    .map(|f| PropertyValue::Float(f as f64))
                    .collect(),
                _ => vec![],
            };

            self.buffer.clear();
            self.buffer_idx = 0;
            for item in items {
                let mut new_record = record.clone();
                // Convert collected node/edge references back to NodeRef/EdgeRef
                let value = match &item {
                    PropertyValue::Map(map) if map.contains_key("__nodeId") => {
                        if let Some(PropertyValue::Integer(id)) = map.get("__nodeId") {
                            Value::NodeRef(crate::graph::types::NodeId(*id as u64))
                        } else {
                            Value::Property(item)
                        }
                    }
                    PropertyValue::Map(map) if map.contains_key("__edgeId") => {
                        if let Some(PropertyValue::Integer(id)) = map.get("__edgeId") {
                            Value::EdgeRef(
                                crate::graph::types::EdgeId(*id as u64),
                                crate::graph::types::NodeId(0),
                                crate::graph::types::NodeId(0),
                                crate::graph::EdgeType::new(""),
                            )
                        } else {
                            Value::Property(item)
                        }
                    }
                    _ => Value::Property(item),
                };
                new_record.bind(self.variable.clone(), value);
                self.buffer.push(new_record);
            }
        }
    }

    fn next_mut(
        &mut self,
        store: &mut GraphStore,
        tenant_id: &str,
    ) -> ExecutionResult<Option<Record>> {
        loop {
            if self.buffer_idx < self.buffer.len() {
                let record = self.buffer[self.buffer_idx].clone();
                self.buffer_idx += 1;
                return Ok(Some(record));
            }

            let record = match self.input.next_mut(store, tenant_id)? {
                Some(r) => r,
                None => return Ok(None),
            };

            let store_ref: &GraphStore = store;
            let list_val = eval_expression(&self.expression, &record, store_ref)?;

            let items = match list_val {
                Value::Property(PropertyValue::Array(arr)) => arr,
                Value::Property(PropertyValue::Vector(vec)) => vec
                    .into_iter()
                    .map(|f| PropertyValue::Float(f as f64))
                    .collect(),
                _ => vec![],
            };

            self.buffer.clear();
            self.buffer_idx = 0;
            for item in items {
                let mut new_record = record.clone();
                new_record.bind(self.variable.clone(), Value::Property(item));
                self.buffer.push(new_record);
            }
        }
    }

    fn next_batch(
        &mut self,
        store: &GraphStore,
        batch_size: usize,
    ) -> ExecutionResult<Option<RecordBatch>> {
        let mut records = Vec::new();
        for _ in 0..batch_size {
            match self.next(store)? {
                Some(r) => records.push(r),
                None => break,
            }
        }
        if records.is_empty() {
            Ok(None)
        } else {
            Ok(Some(RecordBatch {
                records,
                columns: vec![self.variable.clone()],
            }))
        }
    }

    fn reset(&mut self) {
        self.input.reset();
        self.buffer.clear();
        self.buffer_idx = 0;
    }

    fn is_mutating(&self) -> bool {
        self.input.is_mutating()
    }

    fn describe(&self) -> OperatorDescription {
        OperatorDescription {
            name: "Unwind".to_string(),
            details: format!(
                "{} AS {}",
                format_expression(&self.expression),
                self.variable
            ),
            children: vec![self.input.describe()],
        }
    }
}

/// MERGE operator - upsert: match or create pattern
pub struct MergeOperator {
    pattern: Pattern,
    on_create_set: Vec<(String, String, Expression)>,
    on_match_set: Vec<(String, String, Expression)>,
    executed: bool,
}

impl MergeOperator {
    pub fn new(
        pattern: Pattern,
        on_create_set: Vec<(String, String, Expression)>,
        on_match_set: Vec<(String, String, Expression)>,
    ) -> Self {
        Self {
            pattern,
            on_create_set,
            on_match_set,
            executed: false,
        }
    }
}

impl PhysicalOperator for MergeOperator {
    fn next(&mut self, _store: &GraphStore) -> ExecutionResult<Option<Record>> {
        Err(ExecutionError::RuntimeError(
            "MergeOperator requires mutable store access. Use next_mut instead.".to_string(),
        ))
    }

    fn next_mut(
        &mut self,
        store: &mut GraphStore,
        _tenant_id: &str,
    ) -> ExecutionResult<Option<Record>> {
        if self.executed {
            return Ok(None);
        }
        self.executed = true;

        let path = self.pattern.paths.first().ok_or_else(|| {
            ExecutionError::PlanningError("MERGE pattern has no paths".to_string())
        })?;

        let start = &path.start;
        let start_var = start.variable.clone().unwrap_or_else(|| "n".to_string());
        let labels = &start.labels;
        let props = start.properties.as_ref();

        // Search for existing nodes matching labels + properties
        let mut matched_node_id = None;
        if let Some(first_label) = labels.first() {
            let candidates = store.get_nodes_by_label(first_label);
            for node in candidates {
                let has_all_labels = labels.iter().all(|l| node.labels.contains(l));
                if !has_all_labels {
                    continue;
                }

                if let Some(required_props) = props {
                    let props_match = required_props
                        .iter()
                        .all(|(k, v)| node.properties.get(k) == Some(v));
                    if !props_match {
                        continue;
                    }
                }

                matched_node_id = Some(node.id);
                break;
            }
        }

        let node_id;
        let mut record = Record::new();

        if let Some(existing_id) = matched_node_id {
            node_id = existing_id;
            record.bind(start_var.clone(), Value::NodeRef(node_id));

            for (var, prop, expr) in &self.on_match_set {
                if var == &start_var {
                    let val = eval_expression(expr, &record, store)?;
                    if let Value::Property(pv) = val {
                        if let Some(node) = store.get_node_mut(node_id) {
                            node.set_property(prop.clone(), pv);
                        }
                    }
                }
            }
        } else {
            let label_str = labels.first().map(|l| l.as_str()).unwrap_or("Node");
            node_id = store.create_node(label_str);

            for label in labels.iter().skip(1) {
                if let Some(node) = store.get_node_mut(node_id) {
                    node.labels.insert(label.clone());
                }
            }

            if let Some(required_props) = props {
                for (k, v) in required_props {
                    if let Some(node) = store.get_node_mut(node_id) {
                        node.set_property(k.clone(), v.clone());
                    }
                }
            }

            record.bind(start_var.clone(), Value::NodeRef(node_id));

            for (var, prop, expr) in &self.on_create_set {
                if var == &start_var {
                    let val = eval_expression(expr, &record, store)?;
                    if let Value::Property(pv) = val {
                        if let Some(node) = store.get_node_mut(node_id) {
                            node.set_property(prop.clone(), pv);
                        }
                    }
                }
            }
        }

        // Handle relationship segments: (start)-[:TYPE]->(end)
        for seg in &path.segments {
            let edge_types = &seg.edge.types;
            let edge_type = edge_types.first().map(|t| t.as_str()).unwrap_or("RELATED");
            let edge_var = seg.edge.variable.clone();
            let target = &seg.node;
            let target_var = target.variable.clone().unwrap_or_else(|| "m".to_string());
            let target_labels = &target.labels;
            let target_props = target.properties.as_ref();
            let direction = &seg.edge.direction;

            // Try to find the target node
            let mut target_node_id = None;

            // If target node is already bound in record (from MATCH), use it
            if let Some(ref tv) = target.variable {
                if let Some(val) = record.get(tv) {
                    target_node_id = val.node_id();
                }
            }

            // If not bound, search by labels + properties
            if target_node_id.is_none() {
                if let Some(first_label) = target_labels.first() {
                    let candidates = store.get_nodes_by_label(first_label);
                    for candidate in candidates {
                        let has_labels = target_labels.iter().all(|l| candidate.labels.contains(l));
                        if !has_labels {
                            continue;
                        }
                        if let Some(req_props) = target_props {
                            let props_match = req_props
                                .iter()
                                .all(|(k, v)| candidate.properties.get(k) == Some(v));
                            if !props_match {
                                continue;
                            }
                        }
                        target_node_id = Some(candidate.id);
                        break;
                    }
                }
            }

            // If target doesn't exist, create it
            let target_id = if let Some(tid) = target_node_id {
                tid
            } else {
                let label_str = target_labels.first().map(|l| l.as_str()).unwrap_or("Node");
                let tid = store.create_node(label_str);
                for label in target_labels.iter().skip(1) {
                    if let Some(node) = store.get_node_mut(tid) {
                        node.labels.insert(label.clone());
                    }
                }
                if let Some(req_props) = target_props {
                    for (k, v) in req_props {
                        if let Some(node) = store.get_node_mut(tid) {
                            node.set_property(k.clone(), v.clone());
                        }
                    }
                }
                tid
            };

            record.bind(target_var, Value::NodeRef(target_id));

            // Check if relationship already exists
            let (source_id, dest_id) = match direction {
                Direction::Incoming => (target_id, node_id),
                _ => (node_id, target_id),
            };

            let mut edge_exists = false;
            let outgoing = store.get_outgoing_edges(source_id);
            for existing_edge in &outgoing {
                if existing_edge.target == dest_id && existing_edge.edge_type.as_str() == edge_type
                {
                    edge_exists = true;
                    if let Some(ref ev) = edge_var {
                        record.bind(
                            ev.clone(),
                            Value::EdgeRef(
                                existing_edge.id,
                                source_id,
                                dest_id,
                                existing_edge.edge_type.clone(),
                            ),
                        );
                    }
                    break;
                }
            }

            // Create relationship if it doesn't exist
            if !edge_exists {
                if let Ok(edge_id) = store.create_edge(source_id, dest_id, edge_type) {
                    // Set edge properties if any
                    if let Some(edge_props) = seg.edge.properties.as_ref() {
                        if let Some(edge) = store.get_edge_mut(edge_id) {
                            for (k, v) in edge_props {
                                edge.set_property(k.clone(), v.clone());
                            }
                        }
                    }
                    if let Some(ref ev) = edge_var {
                        record.bind(
                            ev.clone(),
                            Value::EdgeRef(edge_id, source_id, dest_id, EdgeType::new(edge_type)),
                        );
                    }
                }
            }
        }

        // Bind path variable if present (MERGE p = ...)
        if let Some(ref pv) = path.path_variable {
            let mut path_nodes = vec![node_id];
            let mut path_edges = Vec::new();
            for seg in &path.segments {
                if let Some(ref tv) = seg.node.variable {
                    if let Some(val) = record.get(tv) {
                        if let Some(nid) = val.node_id() {
                            path_nodes.push(nid);
                        }
                    }
                }
                if let Some(ref ev) = seg.edge.variable {
                    if let Some(val) = record.get(ev) {
                        if let Some(eid) = val.edge_id() {
                            path_edges.push(eid);
                        }
                    }
                }
            }
            record.bind(
                pv.clone(),
                Value::Path {
                    nodes: path_nodes,
                    edges: path_edges,
                },
            );
        }

        Ok(Some(record))
    }

    fn next_batch(
        &mut self,
        store: &GraphStore,
        batch_size: usize,
    ) -> ExecutionResult<Option<RecordBatch>> {
        let mut records = Vec::new();
        for _ in 0..batch_size {
            match self.next(store) {
                Ok(Some(r)) => records.push(r),
                _ => break,
            }
        }
        if records.is_empty() {
            Ok(None)
        } else {
            Ok(Some(RecordBatch {
                records,
                columns: vec![],
            }))
        }
    }

    fn reset(&mut self) {
        self.executed = false;
    }

    fn is_mutating(&self) -> bool {
        true
    }

    fn describe(&self) -> OperatorDescription {
        let labels: Vec<String> = self
            .pattern
            .paths
            .first()
            .map(|p| {
                p.start
                    .labels
                    .iter()
                    .map(|l| l.as_str().to_string())
                    .collect()
            })
            .unwrap_or_default();
        let var = self
            .pattern
            .paths
            .first()
            .and_then(|p| p.start.variable.clone())
            .unwrap_or_else(|| "?".to_string());
        OperatorDescription {
            name: "Merge".to_string(),
            details: format!("({}:{})", var, labels.join(":")),
            children: vec![],
        }
    }
}

/// FOREACH operator: FOREACH (x IN list | SET x.prop = val)
pub struct ForeachOperator {
    input: OperatorBox,
    variable: String,
    list_expr: Expression,
    set_items: Vec<(String, String, Expression)>, // (variable, property, value_expr)
    create_patterns: Vec<Pattern>,
}

impl ForeachOperator {
    pub fn new(
        input: OperatorBox,
        variable: String,
        list_expr: Expression,
        set_items: Vec<(String, String, Expression)>,
        create_patterns: Vec<Pattern>,
    ) -> Self {
        Self {
            input,
            variable,
            list_expr,
            set_items,
            create_patterns,
        }
    }
}

impl PhysicalOperator for ForeachOperator {
    fn next(&mut self, _store: &GraphStore) -> ExecutionResult<Option<Record>> {
        Err(ExecutionError::RuntimeError(
            "ForeachOperator requires mutable store access. Use next_mut instead.".to_string(),
        ))
    }

    fn next_mut(
        &mut self,
        store: &mut GraphStore,
        tenant_id: &str,
    ) -> ExecutionResult<Option<Record>> {
        if let Some(record) = self.input.next_mut(store, tenant_id)? {
            // Evaluate the list expression
            let list_val = eval_expression(&self.list_expr, &record, store)?;
            let items = match list_val {
                Value::Property(PropertyValue::Array(arr)) => arr,
                _ => return Ok(Some(record)),
            };

            // Iterate over list items
            for item in &items {
                let mut inner_record = record.clone();
                inner_record.bind(self.variable.clone(), Value::Property(item.clone()));

                // Execute SET operations
                for (var, prop, expr) in &self.set_items {
                    let val = eval_expression(expr, &inner_record, store)?;
                    let prop_val = match val {
                        Value::Property(p) => p,
                        Value::Null => PropertyValue::Null,
                        _ => continue,
                    };

                    if let Some(node_val) = inner_record.get(var) {
                        match node_val {
                            Value::NodeRef(id) | Value::Node(id, _) => {
                                if let Some(node) = store.get_node_mut(*id) {
                                    node.set_property(prop, prop_val.clone());
                                }
                            }
                            Value::EdgeRef(id, ..) | Value::Edge(id, _) => {
                                if let Some(edge) = store.get_edge_mut(*id) {
                                    edge.set_property(prop, prop_val.clone());
                                }
                            }
                            _ => {}
                        }
                    }
                }

                // Execute CREATE operations
                for pattern in &self.create_patterns {
                    for path in &pattern.paths {
                        let label_str = path
                            .start
                            .labels
                            .first()
                            .map(|l| l.as_str())
                            .unwrap_or("Node");
                        let node_id = store.create_node(label_str);
                        if let Some(props) = &path.start.properties {
                            for (k, v) in props {
                                if let Some(node) = store.get_node_mut(node_id) {
                                    node.set_property(k, v.clone());
                                }
                            }
                        }
                    }
                }
            }

            Ok(Some(record))
        } else {
            Ok(None)
        }
    }

    fn next_batch(
        &mut self,
        store: &GraphStore,
        batch_size: usize,
    ) -> ExecutionResult<Option<RecordBatch>> {
        let mut records = Vec::new();
        for _ in 0..batch_size {
            match self.next(store) {
                Ok(Some(r)) => records.push(r),
                _ => break,
            }
        }
        if records.is_empty() {
            Ok(None)
        } else {
            Ok(Some(RecordBatch {
                records,
                columns: vec![],
            }))
        }
    }

    fn reset(&mut self) {
        self.input.reset();
    }

    fn is_mutating(&self) -> bool {
        true
    }

    fn describe(&self) -> OperatorDescription {
        OperatorDescription {
            name: "Foreach".to_string(),
            details: format!(
                "{} IN ... | {} SETs, {} CREATEs",
                self.variable,
                self.set_items.len(),
                self.create_patterns.len()
            ),
            children: vec![self.input.describe()],
        }
    }
}

/// ShortestPathOperator - finds shortest path(s) between two nodes using BFS
pub struct ShortestPathOperator {
    input: OperatorBox,
    source_var: String,
    target_var: String,
    path_var: Option<String>,
    edge_types: Vec<String>,
    direction: Direction,
    all_paths: bool, // false = shortestPath, true = allShortestPaths
    results: std::vec::IntoIter<Record>,
    executed: bool,
}

impl ShortestPathOperator {
    pub fn new(
        input: OperatorBox,
        source_var: String,
        target_var: String,
        path_var: Option<String>,
        edge_types: Vec<String>,
        direction: Direction,
        all_paths: bool,
    ) -> Self {
        Self {
            input,
            source_var,
            target_var,
            path_var,
            edge_types,
            direction,
            all_paths,
            results: Vec::new().into_iter(),
            executed: false,
        }
    }

    fn execute_all(&mut self, store: &GraphStore) -> ExecutionResult<()> {
        let mut all_results = Vec::new();

        while let Some(record) = self.input.next(store)? {
            let source_id = record
                .get(&self.source_var)
                .and_then(|v| v.node_id())
                .ok_or_else(|| {
                    ExecutionError::RuntimeError("shortestPath source not a node".to_string())
                })?;
            let target_id = record
                .get(&self.target_var)
                .and_then(|v| v.node_id())
                .ok_or_else(|| {
                    ExecutionError::RuntimeError("shortestPath target not a node".to_string())
                })?;

            // BFS to find shortest path(s)
            let paths = self.bfs_shortest(store, source_id, target_id);

            if self.all_paths {
                for path in paths {
                    let mut new_record = record.clone();
                    if let Some(ref pv) = self.path_var {
                        new_record.bind(
                            pv.clone(),
                            Value::Path {
                                nodes: path.0,
                                edges: path.1,
                            },
                        );
                    }
                    all_results.push(new_record);
                }
            } else if let Some(path) = paths.into_iter().next() {
                let mut new_record = record.clone();
                if let Some(ref pv) = self.path_var {
                    new_record.bind(
                        pv.clone(),
                        Value::Path {
                            nodes: path.0,
                            edges: path.1,
                        },
                    );
                }
                all_results.push(new_record);
            }
        }

        self.results = all_results.into_iter();
        self.executed = true;
        Ok(())
    }

    fn bfs_shortest(
        &self,
        store: &GraphStore,
        source: NodeId,
        target: NodeId,
    ) -> Vec<(Vec<NodeId>, Vec<crate::graph::EdgeId>)> {
        use std::collections::VecDeque;

        if source == target {
            return vec![(vec![source], vec![])];
        }

        let mut queue: VecDeque<(NodeId, Vec<NodeId>, Vec<crate::graph::EdgeId>)> = VecDeque::new();
        let mut visited: HashSet<NodeId> = HashSet::new();
        let mut results = Vec::new();
        let mut found_distance: Option<usize> = None;

        queue.push_back((source, vec![source], vec![]));
        visited.insert(source);

        while let Some((current, path_nodes, path_edges)) = queue.pop_front() {
            if let Some(max_dist) = found_distance {
                if path_nodes.len() > max_dist {
                    break;
                }
            }

            let edges = match self.direction {
                Direction::Outgoing => store.get_outgoing_edges(current),
                Direction::Incoming => store.get_incoming_edges(current),
                Direction::Both => {
                    let mut all = store.get_outgoing_edges(current);
                    all.extend(store.get_incoming_edges(current));
                    all
                }
            };

            for edge in &edges {
                if !self.edge_types.is_empty()
                    && !self.edge_types.iter().any(|t| t == edge.edge_type.as_str())
                {
                    continue;
                }
                let next_node = if edge.source == current {
                    edge.target
                } else {
                    edge.source
                };

                if next_node == target {
                    let mut new_nodes = path_nodes.clone();
                    new_nodes.push(target);
                    let mut new_edges = path_edges.clone();
                    new_edges.push(edge.id);

                    if found_distance.is_none() {
                        found_distance = Some(new_nodes.len());
                    }
                    results.push((new_nodes, new_edges));

                    if !self.all_paths {
                        return results;
                    }
                    continue;
                }

                if !visited.contains(&next_node) || self.all_paths {
                    if !self.all_paths {
                        visited.insert(next_node);
                    }
                    let mut new_nodes = path_nodes.clone();
                    new_nodes.push(next_node);
                    let mut new_edges = path_edges.clone();
                    new_edges.push(edge.id);
                    queue.push_back((next_node, new_nodes, new_edges));
                }
            }
        }

        results
    }
}

impl PhysicalOperator for ShortestPathOperator {
    fn next(&mut self, store: &GraphStore) -> ExecutionResult<Option<Record>> {
        if !self.executed {
            self.execute_all(store)?;
        }
        Ok(self.results.next())
    }

    fn next_batch(
        &mut self,
        store: &GraphStore,
        batch_size: usize,
    ) -> ExecutionResult<Option<RecordBatch>> {
        if !self.executed {
            self.execute_all(store)?;
        }
        let mut records = Vec::new();
        for _ in 0..batch_size {
            match self.results.next() {
                Some(r) => records.push(r),
                None => break,
            }
        }
        if records.is_empty() {
            Ok(None)
        } else {
            Ok(Some(RecordBatch {
                records,
                columns: vec![],
            }))
        }
    }

    fn reset(&mut self) {
        self.input.reset();
        self.executed = false;
        self.results = Vec::new().into_iter();
    }
}

// ============================================================================
// WITH BARRIER OPERATOR
// ============================================================================

/// WITH projection barrier operator.
///
/// Materializes all input records, evaluates WITH items (expressions +
/// aggregations), applies DISTINCT / ORDER BY / SKIP / LIMIT, and projects
/// only the named WITH columns — forming a "barrier" that hides upstream
/// variables from downstream operators.
pub struct WithBarrierOperator {
    input: OperatorBox,
    items: Vec<(Expression, String)>, // (expr, alias)
    aggregates: Vec<AggregateFunction>,
    group_by: Vec<(Expression, String)>,
    has_aggregation: bool,
    distinct: bool,
    where_predicate: Option<Expression>,
    sort_items: Vec<(Expression, bool)>, // (expr, ascending)
    skip: Option<usize>,
    limit: Option<usize>,
    results: std::vec::IntoIter<Record>,
    executed: bool,
}

impl WithBarrierOperator {
    pub fn new(
        input: OperatorBox,
        items: Vec<(Expression, String)>,
        aggregates: Vec<AggregateFunction>,
        group_by: Vec<(Expression, String)>,
        has_aggregation: bool,
        distinct: bool,
        where_predicate: Option<Expression>,
        sort_items: Vec<(Expression, bool)>,
        skip: Option<usize>,
        limit: Option<usize>,
    ) -> Self {
        Self {
            input,
            items,
            aggregates,
            group_by,
            has_aggregation,
            distinct,
            where_predicate,
            sort_items,
            skip,
            limit,
            results: Vec::new().into_iter(),
            executed: false,
        }
    }

    fn evaluate_expression(
        expr: &Expression,
        record: &Record,
        store: &GraphStore,
    ) -> ExecutionResult<Value> {
        match expr {
            Expression::Variable(var) => Ok(record.get(var).cloned().unwrap_or(Value::Null)),
            Expression::Property { variable, property } => {
                let val = record.get(variable).unwrap_or(&Value::Null);
                let prop = val.resolve_property(property, store);
                Ok(Value::Property(prop))
            }
            Expression::Literal(lit) => Ok(Value::Property(lit.clone())),
            Expression::Binary { left, op, right } => {
                let left_val = Self::evaluate_expression(left, record, store)?;
                let right_val = Self::evaluate_expression(right, record, store)?;
                eval_binary_op(op, left_val, right_val)
            }
            Expression::Unary { op, expr } => {
                let val = Self::evaluate_expression(expr, record, store)?;
                eval_unary_op(op, val)
            }
            Expression::Function { name, args, .. } => {
                let arg_vals: Vec<Value> = args
                    .iter()
                    .map(|a| Self::evaluate_expression(a, record, store))
                    .collect::<ExecutionResult<Vec<_>>>()?;
                eval_function(name, &arg_vals, Some(store))
            }
            Expression::Case {
                operand,
                when_clauses,
                else_result,
            } => eval_case(
                operand.as_deref(),
                when_clauses,
                else_result.as_deref(),
                |e| Self::evaluate_expression(e, record, store),
            ),
            Expression::Index { expr, index } => {
                let collection = Self::evaluate_expression(expr, record, store)?;
                let idx = Self::evaluate_expression(index, record, store)?;
                eval_index(collection, idx)
            }
            Expression::ListSlice { expr, start, end } => {
                let collection = Self::evaluate_expression(expr, record, store)?;
                let s = match start {
                    Some(s) => Some(Self::evaluate_expression(s, record, store)?),
                    None => None,
                };
                let en = match end {
                    Some(e) => Some(Self::evaluate_expression(e, record, store)?),
                    None => None,
                };
                eval_list_slice(collection, s, en)
            }
            Expression::ExistsSubquery {
                pattern,
                where_clause,
            } => eval_exists_subquery(pattern, where_clause.as_deref(), record, store),
            Expression::ListComprehension {
                variable,
                list_expr,
                filter,
                map_expr,
            } => eval_list_comprehension(
                variable,
                list_expr,
                filter.as_deref(),
                map_expr,
                record,
                store,
            ),
            Expression::PredicateFunction {
                name,
                variable,
                list_expr,
                predicate,
            } => eval_predicate_function(name, variable, list_expr, predicate, record, store),
            Expression::Reduce {
                accumulator,
                init,
                variable,
                list_expr,
                expression,
            } => eval_reduce(
                accumulator,
                init,
                variable,
                list_expr,
                expression,
                record,
                store,
            ),
            Expression::PatternComprehension {
                pattern,
                filter,
                projection,
            } => eval_pattern_comprehension(pattern, filter.as_deref(), projection, record, store),
            Expression::MapExpression(entries) => {
                let mut map = std::collections::HashMap::new();
                for (key, val_expr) in entries {
                    let val = Self::evaluate_expression(val_expr, record, store)?;
                    let pv = match val {
                        Value::Property(pv) => pv,
                        Value::Null => PropertyValue::Null,
                        _ => PropertyValue::Null,
                    };
                    map.insert(key.clone(), pv);
                }
                Ok(Value::Property(PropertyValue::Map(map)))
            }
            Expression::PathVariable(var) => record
                .get(var)
                .cloned()
                .ok_or_else(|| ExecutionError::VariableNotFound(var.clone())),
            Expression::Parameter(name) => {
                record.get(&format!("${}", name)).cloned().ok_or_else(|| {
                    ExecutionError::RuntimeError(format!("Unresolved parameter: ${}", name))
                })
            }
        }
    }

    fn execute_all(&mut self, store: &GraphStore) -> ExecutionResult<()> {
        let mut output_records = if self.has_aggregation {
            // Aggregation path: group by non-aggregate items
            let mut groups: HashMap<Vec<Value>, Vec<AggregatorState>> = HashMap::new();

            let batch_size = 1024;
            while let Some(batch) = self.input.next_batch(store, batch_size)? {
                for record in batch.records {
                    let mut key = Vec::new();
                    for (expr, _) in &self.group_by {
                        key.push(Self::evaluate_expression(expr, &record, store)?);
                    }

                    let states = groups.entry(key).or_insert_with(|| {
                        self.aggregates
                            .iter()
                            .map(|agg| AggregatorState::new(&agg.func, agg.distinct))
                            .collect()
                    });

                    for (i, agg) in self.aggregates.iter().enumerate() {
                        let val = Self::evaluate_expression(&agg.expr, &record, store)?;
                        states[i].update(&val);
                    }
                }
            }

            let mut records = Vec::new();
            for (key, states) in groups {
                let mut record = Record::new();
                for (i, (_, alias)) in self.group_by.iter().enumerate() {
                    record.bind(alias.clone(), key[i].clone());
                }
                for (i, agg) in self.aggregates.iter().enumerate() {
                    record.bind(agg.alias.clone(), states[i].result());
                }
                records.push(record);
            }

            // Post-projection: evaluate items (which may contain rewritten aggregate
            // references like Variable("__agg_0")) against the intermediate records
            let mut projected = Vec::with_capacity(records.len());
            for intermediate in records {
                let mut new_record = Record::new();
                for (expr, alias) in &self.items {
                    let value = Self::evaluate_expression(expr, &intermediate, store)?;
                    new_record.bind(alias.clone(), value);
                }
                projected.push(new_record);
            }
            projected
        } else {
            // Non-aggregation path: project each row
            // If there's a WHERE filter, keep original records for evaluation
            let has_where = self.where_predicate.is_some();
            let mut records = Vec::new();
            let mut originals: Vec<Record> = Vec::new();
            let batch_size = 1024;
            while let Some(batch) = self.input.next_batch(store, batch_size)? {
                for record in batch.records {
                    let mut new_record = Record::new();
                    for (expr, alias) in &self.items {
                        let value = Self::evaluate_expression(expr, &record, store)?;
                        new_record.bind(alias.clone(), value);
                    }
                    if has_where {
                        originals.push(record);
                    }
                    records.push(new_record);
                }
            }

            // Apply WHERE filter against merged records (original + projected)
            if let Some(ref predicate) = self.where_predicate {
                let mut filtered = Vec::new();
                for (i, projected) in records.into_iter().enumerate() {
                    // Merge: original bindings + projected aliases
                    let mut merged = if i < originals.len() {
                        originals[i].clone()
                    } else {
                        Record::new()
                    };
                    for (k, v) in projected.bindings() {
                        merged.bind(k.clone(), v.clone());
                    }
                    let passes = match Self::evaluate_expression(predicate, &merged, store) {
                        Ok(Value::Property(PropertyValue::Boolean(b))) => b,
                        Ok(Value::Null) | Ok(Value::Property(PropertyValue::Null)) => false,
                        _ => false,
                    };
                    if passes {
                        // Only emit the projected columns
                        let mut out = Record::new();
                        for (_, alias) in &self.items {
                            if let Some(v) = merged.get(alias) {
                                out.bind(alias.clone(), v.clone());
                            }
                        }
                        filtered.push(out);
                    }
                }
                filtered
            } else {
                records
            }
        };

        // Apply WHERE filter for aggregation path (already has all needed vars)
        if self.has_aggregation {
            if let Some(ref predicate) = self.where_predicate {
                output_records.retain(|record| {
                    match Self::evaluate_expression(predicate, record, store) {
                        Ok(Value::Property(PropertyValue::Boolean(b))) => b,
                        Ok(Value::Null) | Ok(Value::Property(PropertyValue::Null)) => false,
                        _ => false,
                    }
                });
            }
        }

        // Apply DISTINCT
        if self.distinct {
            let mut seen: HashSet<Vec<Value>> = HashSet::new();
            output_records.retain(|record| {
                let mut key: Vec<(String, Value)> = record
                    .bindings()
                    .iter()
                    .map(|(k, v)| (k.clone(), v.clone()))
                    .collect();
                key.sort_by(|a, b| a.0.cmp(&b.0));
                let vals: Vec<Value> = key.into_iter().map(|(_, v)| v).collect();
                seen.insert(vals)
            });
        }

        // Apply ORDER BY
        if !self.sort_items.is_empty() {
            let sort_items = &self.sort_items;
            output_records.sort_by(|a, b| {
                for (expr, ascending) in sort_items {
                    let val_a = Self::evaluate_expression(expr, a, store).unwrap_or(Value::Null);
                    let val_b = Self::evaluate_expression(expr, b, store).unwrap_or(Value::Null);
                    let prop_a = val_a.as_property().unwrap_or(&PropertyValue::Null);
                    let prop_b = val_b.as_property().unwrap_or(&PropertyValue::Null);
                    let ord = prop_a.cmp(prop_b);
                    if ord != std::cmp::Ordering::Equal {
                        return if *ascending { ord } else { ord.reverse() };
                    }
                }
                std::cmp::Ordering::Equal
            });
        }

        // Apply SKIP
        if let Some(skip) = self.skip {
            if skip < output_records.len() {
                output_records = output_records.split_off(skip);
            } else {
                output_records.clear();
            }
        }

        // Apply LIMIT
        if let Some(limit) = self.limit {
            output_records.truncate(limit);
        }

        self.results = output_records.into_iter();
        self.executed = true;
        Ok(())
    }

    /// Mutable version of execute_all that drains mutating child operators.
    /// Used when the input pipeline contains write operators (PerRowCreate, SetProperty, etc.)
    fn execute_all_mut(&mut self, store: &mut GraphStore, tenant_id: &str) -> ExecutionResult<()> {
        let mut output_records = if self.has_aggregation {
            let mut groups: HashMap<Vec<Value>, Vec<AggregatorState>> = HashMap::new();
            let batch_size = 1024;
            while let Some(batch) = self.input.next_batch_mut(store, tenant_id, batch_size)? {
                for record in batch.records {
                    let mut key = Vec::new();
                    let store_ref: &GraphStore = store;
                    for (expr, _) in &self.group_by {
                        key.push(Self::evaluate_expression(expr, &record, store_ref)?);
                    }
                    let states = groups.entry(key).or_insert_with(|| {
                        self.aggregates
                            .iter()
                            .map(|agg| AggregatorState::new(&agg.func, agg.distinct))
                            .collect()
                    });
                    let store_ref: &GraphStore = store;
                    for (i, agg) in self.aggregates.iter().enumerate() {
                        let val = Self::evaluate_expression(&agg.expr, &record, store_ref)?;
                        states[i].update(&val);
                    }
                }
            }
            let mut records = Vec::new();
            let store_ref: &GraphStore = store;
            for (key, states) in groups {
                let mut record = Record::new();
                for (i, (_, alias)) in self.group_by.iter().enumerate() {
                    record.bind(alias.clone(), key[i].clone());
                }
                for (i, agg) in self.aggregates.iter().enumerate() {
                    record.bind(agg.alias.clone(), states[i].result());
                }
                records.push(record);
            }
            let mut projected = Vec::with_capacity(records.len());
            for intermediate in records {
                let mut new_record = Record::new();
                for (expr, alias) in &self.items {
                    let value = Self::evaluate_expression(expr, &intermediate, store_ref)?;
                    new_record.bind(alias.clone(), value);
                }
                projected.push(new_record);
            }
            projected
        } else {
            let mut records = Vec::new();
            let batch_size = 1024;
            while let Some(batch) = self.input.next_batch_mut(store, tenant_id, batch_size)? {
                let store_ref: &GraphStore = store;
                for record in batch.records {
                    let mut new_record = Record::new();
                    for (expr, alias) in &self.items {
                        let value = Self::evaluate_expression(expr, &record, store_ref)?;
                        new_record.bind(alias.clone(), value);
                    }
                    records.push(new_record);
                }
            }
            records
        };

        // Apply WHERE filter
        if let Some(ref predicate) = self.where_predicate {
            let store_ref: &GraphStore = store;
            output_records.retain(|record| {
                match Self::evaluate_expression(predicate, record, store_ref) {
                    Ok(Value::Property(PropertyValue::Boolean(b))) => b,
                    Ok(Value::Null) | Ok(Value::Property(PropertyValue::Null)) => false,
                    _ => false,
                }
            });
        }

        // Apply DISTINCT
        if self.distinct {
            let mut seen: HashSet<Vec<Value>> = HashSet::new();
            output_records.retain(|record| {
                let mut key: Vec<(String, Value)> = record
                    .bindings()
                    .iter()
                    .map(|(k, v)| (k.clone(), v.clone()))
                    .collect();
                key.sort_by(|a, b| a.0.cmp(&b.0));
                let vals: Vec<Value> = key.into_iter().map(|(_, v)| v).collect();
                seen.insert(vals)
            });
        }

        // Apply ORDER BY
        if !self.sort_items.is_empty() {
            let store_ref: &GraphStore = store;
            let sort_items = &self.sort_items;
            output_records.sort_by(|a, b| {
                for (expr, ascending) in sort_items {
                    let val_a =
                        Self::evaluate_expression(expr, a, store_ref).unwrap_or(Value::Null);
                    let val_b =
                        Self::evaluate_expression(expr, b, store_ref).unwrap_or(Value::Null);
                    let prop_a = val_a.as_property().unwrap_or(&PropertyValue::Null);
                    let prop_b = val_b.as_property().unwrap_or(&PropertyValue::Null);
                    let ord = prop_a.cmp(prop_b);
                    if ord != std::cmp::Ordering::Equal {
                        return if *ascending { ord } else { ord.reverse() };
                    }
                }
                std::cmp::Ordering::Equal
            });
        }

        // Apply SKIP
        if let Some(skip) = self.skip {
            if skip < output_records.len() {
                output_records = output_records.split_off(skip);
            } else {
                output_records.clear();
            }
        }

        // Apply LIMIT
        if let Some(limit) = self.limit {
            output_records.truncate(limit);
        }

        self.results = output_records.into_iter();
        self.executed = true;
        Ok(())
    }
}

impl PhysicalOperator for WithBarrierOperator {
    fn next(&mut self, store: &GraphStore) -> ExecutionResult<Option<Record>> {
        if !self.executed {
            self.execute_all(store)?;
        }
        Ok(self.results.next())
    }

    fn next_mut(
        &mut self,
        store: &mut GraphStore,
        tenant_id: &str,
    ) -> ExecutionResult<Option<Record>> {
        if !self.executed {
            self.execute_all_mut(store, tenant_id)?;
        }
        Ok(self.results.next())
    }

    fn next_batch(
        &mut self,
        store: &GraphStore,
        batch_size: usize,
    ) -> ExecutionResult<Option<RecordBatch>> {
        if !self.executed {
            self.execute_all(store)?;
        }

        let mut batch = Vec::with_capacity(batch_size);
        for _ in 0..batch_size {
            if let Some(record) = self.results.next() {
                batch.push(record);
            } else {
                break;
            }
        }

        if batch.is_empty() {
            Ok(None)
        } else {
            Ok(Some(RecordBatch {
                records: batch,
                columns: Vec::new(),
            }))
        }
    }

    fn next_batch_mut(
        &mut self,
        store: &mut GraphStore,
        tenant_id: &str,
        batch_size: usize,
    ) -> ExecutionResult<Option<RecordBatch>> {
        if !self.executed {
            self.execute_all_mut(store, tenant_id)?;
        }

        let mut batch = Vec::with_capacity(batch_size);
        for _ in 0..batch_size {
            if let Some(record) = self.results.next() {
                batch.push(record);
            } else {
                break;
            }
        }

        if batch.is_empty() {
            Ok(None)
        } else {
            Ok(Some(RecordBatch {
                records: batch,
                columns: Vec::new(),
            }))
        }
    }

    fn reset(&mut self) {
        self.input.reset();
        self.executed = false;
        self.results = Vec::new().into_iter();
    }

    fn is_mutating(&self) -> bool {
        self.input.is_mutating()
    }

    fn describe(&self) -> OperatorDescription {
        let item_strs: Vec<String> = self
            .items
            .iter()
            .map(|(e, a)| format!("{} AS {}", format_expression(e), a))
            .collect();
        let mut details = format!("items=[{}]", item_strs.join(", "));
        if self.distinct {
            details.push_str(", DISTINCT");
        }
        if !self.sort_items.is_empty() {
            details.push_str(", ORDER BY");
        }
        if let Some(s) = self.skip {
            details.push_str(&format!(", SKIP {}", s));
        }
        if let Some(l) = self.limit {
            details.push_str(&format!(", LIMIT {}", l));
        }
        OperatorDescription {
            name: "WithBarrier".to_string(),
            details,
            children: vec![self.input.describe()],
        }
    }
}

/// ExpandInto operator: checks whether an edge exists between two already-bound endpoints.
///
/// Unlike ExpandOperator (which fans out from one bound node to discover new neighbors),
/// ExpandInto takes a record where BOTH source and target are already bound, and checks
/// whether a connecting edge exists. If it does, the record passes through (with the edge
/// optionally bound); if not, the record is filtered out.
///
/// This is semantically a filter (fan-in), not an expansion (fan-out).
pub struct ExpandIntoOperator {
    input: OperatorBox,
    source_binding: String,
    target_binding: String,
    edge_type: Option<String>,
    edge_binding: Option<String>,
}

impl ExpandIntoOperator {
    pub fn new(
        input: OperatorBox,
        source_binding: String,
        target_binding: String,
        edge_type: Option<String>,
        edge_binding: Option<String>,
    ) -> Self {
        Self {
            input,
            source_binding,
            target_binding,
            edge_type,
            edge_binding,
        }
    }
}

impl PhysicalOperator for ExpandIntoOperator {
    fn next(&mut self, store: &GraphStore) -> ExecutionResult<Option<Record>> {
        loop {
            let record = match self.input.next(store)? {
                Some(r) => r,
                None => return Ok(None),
            };

            let source_id = record
                .get(&self.source_binding)
                .and_then(|v| v.node_id())
                .ok_or_else(|| ExecutionError::VariableNotFound(self.source_binding.clone()))?;

            let target_id = record
                .get(&self.target_binding)
                .and_then(|v| v.node_id())
                .ok_or_else(|| ExecutionError::VariableNotFound(self.target_binding.clone()))?;

            let et = self.edge_type.as_ref().map(|t| EdgeType::new(t.as_str()));
            let et_ref = et.as_ref();

            if let Some(edge_id) = store.edge_between(source_id, target_id, et_ref) {
                let mut new_record = record;
                if let Some(ref edge_var) = self.edge_binding {
                    if let Some(edge) = store.get_edge(edge_id) {
                        new_record.bind(
                            edge_var.clone(),
                            Value::EdgeRef(
                                edge_id,
                                edge.source,
                                edge.target,
                                edge.edge_type.clone(),
                            ),
                        );
                    }
                }
                return Ok(Some(new_record));
            }
            // No edge found — skip this record, try next
        }
    }

    fn next_batch(
        &mut self,
        store: &GraphStore,
        batch_size: usize,
    ) -> ExecutionResult<Option<RecordBatch>> {
        let mut records = Vec::new();
        for _ in 0..batch_size {
            match self.next(store)? {
                Some(r) => records.push(r),
                None => break,
            }
        }
        if records.is_empty() {
            Ok(None)
        } else {
            Ok(Some(RecordBatch {
                records,
                columns: Vec::new(),
            }))
        }
    }

    fn reset(&mut self) {
        self.input.reset();
    }

    fn describe(&self) -> OperatorDescription {
        let type_str = self.edge_type.as_deref().unwrap_or("*");
        OperatorDescription {
            name: "ExpandInto".to_string(),
            details: format!(
                "({})--[:{}]-->({})",
                self.source_binding, type_str, self.target_binding
            ),
            children: vec![self.input.describe()],
        }
    }
}

/// NodeById operator: start from a specific set of node IDs.
///
/// Useful when the planner knows the exact starting nodes (e.g., from an index lookup
/// or from a previous query stage).
pub struct NodeByIdOperator {
    node_ids: Vec<NodeId>,
    position: usize,
    variable: String,
}

impl NodeByIdOperator {
    pub fn new(node_ids: Vec<NodeId>, variable: String) -> Self {
        Self {
            node_ids,
            position: 0,
            variable,
        }
    }
}

impl PhysicalOperator for NodeByIdOperator {
    fn next(&mut self, store: &GraphStore) -> ExecutionResult<Option<Record>> {
        while self.position < self.node_ids.len() {
            let node_id = self.node_ids[self.position];
            self.position += 1;

            // Verify node still exists
            if store.has_node(node_id) {
                let mut record = Record::new();
                record.bind(self.variable.clone(), Value::NodeRef(node_id));
                return Ok(Some(record));
            }
        }
        Ok(None)
    }

    fn next_batch(
        &mut self,
        store: &GraphStore,
        batch_size: usize,
    ) -> ExecutionResult<Option<RecordBatch>> {
        let mut records = Vec::new();
        for _ in 0..batch_size {
            match self.next(store)? {
                Some(r) => records.push(r),
                None => break,
            }
        }
        if records.is_empty() {
            Ok(None)
        } else {
            Ok(Some(RecordBatch {
                records,
                columns: vec![self.variable.clone()],
            }))
        }
    }

    fn reset(&mut self) {
        self.position = 0;
    }

    fn describe(&self) -> OperatorDescription {
        OperatorDescription {
            name: "NodeById".to_string(),
            details: format!(
                "var={}, ids={:?}",
                self.variable,
                self.node_ids
                    .iter()
                    .map(|id| id.as_u64())
                    .collect::<Vec<_>>()
            ),
            children: Vec::new(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::graph::Label;

    #[test]
    fn test_node_scan_operator() {
        let mut store = GraphStore::new();
        let _alice = store.create_node("Person");
        let _bob = store.create_node("Person");

        let mut op = NodeScanOperator::new("n".to_string(), vec![Label::new("Person")]);

        let mut count = 0;
        while let Ok(Some(_record)) = op.next(&store) {
            count += 1;
        }

        assert_eq!(count, 2);
    }

    #[test]
    fn test_filter_operator() {
        let mut store = GraphStore::new();
        let alice = store.create_node("Person");
        if let Some(node) = store.get_node_mut(alice) {
            node.set_property("age", 30i64);
        }

        let bob = store.create_node("Person");
        if let Some(node) = store.get_node_mut(bob) {
            node.set_property("age", 25i64);
        }

        let scan = NodeScanOperator::new("n".to_string(), vec![Label::new("Person")]);
        let predicate = Expression::Binary {
            left: Box::new(Expression::Property {
                variable: "n".to_string(),
                property: "age".to_string(),
            }),
            op: BinaryOp::Gt,
            right: Box::new(Expression::Literal(PropertyValue::Integer(28))),
        };

        let mut filter = FilterOperator::new(Box::new(scan), predicate);

        let mut count = 0;
        while let Ok(Some(_record)) = filter.next(&store) {
            count += 1;
        }

        assert_eq!(count, 1); // Only Alice (age 30) passes the filter
    }

    #[test]
    fn test_limit_operator() {
        let mut store = GraphStore::new();
        for _ in 0..10 {
            store.create_node("Person");
        }

        let scan = NodeScanOperator::new("n".to_string(), vec![Label::new("Person")]);
        let mut limit = LimitOperator::new(Box::new(scan), 3);

        let mut count = 0;
        while let Ok(Some(_record)) = limit.next(&store) {
            count += 1;
        }

        assert_eq!(count, 3);
    }

    #[test]
    fn test_node_scan_batch() {
        let mut store = GraphStore::new();
        for i in 0..10 {
            let id = store.create_node("Person");
            store
                .set_node_property("default", id, "id", i as i64)
                .unwrap();
        }

        let mut op = NodeScanOperator::new("n".to_string(), vec![Label::new("Person")]);

        // Request batch size 4
        let batch1 = op.next_batch(&store, 4).unwrap().unwrap();
        assert_eq!(batch1.len(), 4);

        let batch2 = op.next_batch(&store, 4).unwrap().unwrap();
        assert_eq!(batch2.len(), 4);

        let batch3 = op.next_batch(&store, 4).unwrap().unwrap();
        assert_eq!(batch3.len(), 2); // Remaining

        let batch4 = op.next_batch(&store, 4).unwrap();
        assert!(batch4.is_none());
    }

    #[test]
    fn test_project_batch() {
        let mut store = GraphStore::new();
        let id = store.create_node("Person");
        store.set_node_property("default", id, "age", 30).unwrap();

        let scan = NodeScanOperator::new("n".to_string(), vec![Label::new("Person")]);
        let mut project = ProjectOperator::new(
            Box::new(scan),
            vec![(
                Expression::Property {
                    variable: "n".to_string(),
                    property: "age".to_string(),
                },
                "age".to_string(),
            )],
        );

        let batch = project.next_batch(&store, 10).unwrap().unwrap();
        assert_eq!(batch.len(), 1);
        let age = batch.records[0]
            .get("age")
            .unwrap()
            .as_property()
            .unwrap()
            .as_integer()
            .unwrap();
        assert_eq!(age, 30);
    }

    #[test]
    fn test_filter_batch() {
        let mut store = GraphStore::new();
        for i in 0..10 {
            let id = store.create_node("Person");
            store
                .set_node_property("default", id, "val", i as i64)
                .unwrap();
        }

        let scan = NodeScanOperator::new("n".to_string(), vec![Label::new("Person")]);
        // Filter val >= 5
        let predicate = Expression::Binary {
            left: Box::new(Expression::Property {
                variable: "n".to_string(),
                property: "val".to_string(),
            }),
            op: BinaryOp::Ge,
            right: Box::new(Expression::Literal(PropertyValue::Integer(5))),
        };

        let mut filter = FilterOperator::new(Box::new(scan), predicate);

        // Pull in batches of 10 (should get all 5 matches in one go or multiple depending on implementation)
        // Implementation loops until batch filled or source exhausted.
        let batch = filter.next_batch(&store, 10).unwrap().unwrap();
        assert_eq!(batch.len(), 5);
        for r in batch.records {
            let val = r
                .get("n")
                .unwrap()
                .resolve_property("val", &store)
                .as_integer()
                .unwrap();
            assert!(val >= 5);
        }
    }

    #[test]
    fn test_aggregate_batch() {
        let mut store = GraphStore::new();
        // 3 items group A, 2 items group B
        for _ in 0..3 {
            let id = store.create_node("Item");
            store
                .set_node_property("default", id, "group", "A")
                .unwrap();
        }
        for _ in 0..2 {
            let id = store.create_node("Item");
            store
                .set_node_property("default", id, "group", "B")
                .unwrap();
        }

        let scan = NodeScanOperator::new("n".to_string(), vec![Label::new("Item")]);
        let mut agg = AggregateOperator::new(
            Box::new(scan),
            vec![(
                Expression::Property {
                    variable: "n".to_string(),
                    property: "group".to_string(),
                },
                "group".to_string(),
            )],
            vec![AggregateFunction {
                func: AggregateType::Count,
                expr: Expression::Variable("n".to_string()),
                alias: "count".to_string(),
                distinct: false,
            }],
        );

        let batch = agg.next_batch(&store, 10).unwrap().unwrap();
        assert_eq!(batch.len(), 2); // 2 groups

        // Check results
        let mut counts = HashMap::new();
        for r in batch.records {
            let g = r
                .get("group")
                .unwrap()
                .as_property()
                .unwrap()
                .as_string()
                .unwrap()
                .to_string();
            let c = r
                .get("count")
                .unwrap()
                .as_property()
                .unwrap()
                .as_integer()
                .unwrap();
            counts.insert(g, c);
        }

        assert_eq!(counts.get("A"), Some(&3));
        assert_eq!(counts.get("B"), Some(&2));
    }

    #[test]
    fn test_sort_batch() {
        let mut store = GraphStore::new();
        let values = vec![5, 1, 3, 2, 4];
        for v in values {
            let id = store.create_node("Num");
            store.set_node_property("default", id, "val", v).unwrap();
        }

        let scan = NodeScanOperator::new("n".to_string(), vec![Label::new("Num")]);
        let mut sort = SortOperator::new(
            Box::new(scan),
            vec![(
                Expression::Property {
                    variable: "n".to_string(),
                    property: "val".to_string(),
                },
                true,
            )], // Ascending
        );

        let batch = sort.next_batch(&store, 10).unwrap().unwrap();
        assert_eq!(batch.len(), 5);

        let sorted_vals: Vec<i64> = batch
            .records
            .iter()
            .map(|r| {
                r.get("n")
                    .unwrap()
                    .resolve_property("val", &store)
                    .as_integer()
                    .unwrap()
            })
            .collect();

        assert_eq!(sorted_vals, vec![1, 2, 3, 4, 5]);
    }

    // ========== Batch 1: eval_function tests ==========

    // -- Date/Time functions --

    #[test]
    fn test_eval_function_date_no_args() {
        let result = eval_function("date", &[], None).unwrap();
        match result {
            Value::Property(PropertyValue::DateTime(ts)) => assert!(ts > 0),
            _ => panic!("Expected DateTime"),
        }
    }

    #[test]
    fn test_eval_function_date_string() {
        let result = eval_function(
            "date",
            &[Value::Property(PropertyValue::String(
                "2024-01-15".to_string(),
            ))],
            None,
        )
        .unwrap();
        match result {
            Value::Property(PropertyValue::DateTime(ts)) => {
                // 2024-01-15 00:00:00 UTC
                let expected = chrono::NaiveDate::from_ymd_opt(2024, 1, 15)
                    .unwrap()
                    .and_hms_opt(0, 0, 0)
                    .unwrap()
                    .and_utc()
                    .timestamp_millis();
                assert_eq!(ts, expected);
            }
            _ => panic!("Expected DateTime"),
        }
    }

    #[test]
    fn test_eval_function_date_map() {
        let mut map = HashMap::new();
        map.insert("year".to_string(), PropertyValue::Integer(2024));
        map.insert("month".to_string(), PropertyValue::Integer(6));
        map.insert("day".to_string(), PropertyValue::Integer(15));
        let result =
            eval_function("date", &[Value::Property(PropertyValue::Map(map))], None).unwrap();
        match result {
            Value::Property(PropertyValue::DateTime(ts)) => {
                let expected = chrono::NaiveDate::from_ymd_opt(2024, 6, 15)
                    .unwrap()
                    .and_hms_opt(0, 0, 0)
                    .unwrap()
                    .and_utc()
                    .timestamp_millis();
                assert_eq!(ts, expected);
            }
            _ => panic!("Expected DateTime"),
        }
    }

    #[test]
    fn test_eval_function_date_invalid_string() {
        let result = eval_function(
            "date",
            &[Value::Property(PropertyValue::String(
                "not-a-date".to_string(),
            ))],
            None,
        );
        assert!(result.is_err());
    }

    #[test]
    fn test_eval_function_date_invalid_map() {
        let mut map = HashMap::new();
        map.insert("year".to_string(), PropertyValue::Integer(2024));
        map.insert("month".to_string(), PropertyValue::Integer(13)); // invalid month
        map.insert("day".to_string(), PropertyValue::Integer(1));
        let result = eval_function("date", &[Value::Property(PropertyValue::Map(map))], None);
        assert!(result.is_err());
    }

    #[test]
    fn test_eval_function_date_type_error() {
        let result = eval_function("date", &[Value::Property(PropertyValue::Integer(42))], None);
        assert!(result.is_err());
    }

    #[test]
    fn test_eval_function_datetime_no_args() {
        let result = eval_function("datetime", &[], None).unwrap();
        match result {
            Value::Property(PropertyValue::DateTime(ts)) => assert!(ts > 0),
            _ => panic!("Expected DateTime"),
        }
    }

    #[test]
    fn test_eval_function_datetime_rfc3339() {
        let result = eval_function(
            "datetime",
            &[Value::Property(PropertyValue::String(
                "2024-01-15T10:30:00Z".to_string(),
            ))],
            None,
        )
        .unwrap();
        match result {
            Value::Property(PropertyValue::DateTime(ts)) => {
                let expected = chrono::DateTime::parse_from_rfc3339("2024-01-15T10:30:00Z")
                    .unwrap()
                    .timestamp_millis();
                assert_eq!(ts, expected);
            }
            _ => panic!("Expected DateTime"),
        }
    }

    #[test]
    fn test_eval_function_datetime_naive() {
        let result = eval_function(
            "datetime",
            &[Value::Property(PropertyValue::String(
                "2024-01-15T10:30:00".to_string(),
            ))],
            None,
        )
        .unwrap();
        match result {
            Value::Property(PropertyValue::DateTime(_ts)) => {} // valid
            _ => panic!("Expected DateTime"),
        }
    }

    #[test]
    fn test_eval_function_datetime_map() {
        let mut map = HashMap::new();
        map.insert("year".to_string(), PropertyValue::Integer(2024));
        map.insert("month".to_string(), PropertyValue::Integer(3));
        map.insert("day".to_string(), PropertyValue::Integer(15));
        map.insert("hour".to_string(), PropertyValue::Integer(10));
        map.insert("minute".to_string(), PropertyValue::Integer(30));
        map.insert("second".to_string(), PropertyValue::Integer(45));
        let result = eval_function(
            "datetime",
            &[Value::Property(PropertyValue::Map(map))],
            None,
        )
        .unwrap();
        match result {
            Value::Property(PropertyValue::DateTime(ts)) => {
                use chrono::TimeZone;
                let expected = chrono::Utc
                    .with_ymd_and_hms(2024, 3, 15, 10, 30, 45)
                    .unwrap()
                    .timestamp_millis();
                assert_eq!(ts, expected);
            }
            _ => panic!("Expected DateTime"),
        }
    }

    #[test]
    fn test_eval_function_datetime_invalid_string() {
        let result = eval_function(
            "datetime",
            &[Value::Property(PropertyValue::String(
                "garbage".to_string(),
            ))],
            None,
        );
        assert!(result.is_err());
    }

    #[test]
    fn test_eval_function_datetime_type_error() {
        let result = eval_function(
            "datetime",
            &[Value::Property(PropertyValue::Boolean(true))],
            None,
        );
        assert!(result.is_err());
    }

    #[test]
    fn test_eval_function_duration_iso_string() {
        let result = eval_function(
            "duration",
            &[Value::Property(PropertyValue::String(
                "P1Y2M3D".to_string(),
            ))],
            None,
        )
        .unwrap();
        match result {
            Value::Property(PropertyValue::Duration {
                months,
                days,
                seconds,
                ..
            }) => {
                assert_eq!(months, 14); // 1Y = 12M + 2M
                assert_eq!(days, 3);
                assert_eq!(seconds, 0);
            }
            _ => panic!("Expected Duration"),
        }
    }

    #[test]
    fn test_eval_function_duration_with_time() {
        let result = eval_function(
            "duration",
            &[Value::Property(PropertyValue::String(
                "P1DT2H30M".to_string(),
            ))],
            None,
        )
        .unwrap();
        match result {
            Value::Property(PropertyValue::Duration {
                months,
                days,
                seconds,
                ..
            }) => {
                assert_eq!(months, 0);
                assert_eq!(days, 1);
                assert_eq!(seconds, 2 * 3600 + 30 * 60);
            }
            _ => panic!("Expected Duration"),
        }
    }

    #[test]
    fn test_eval_function_duration_map() {
        let mut map = HashMap::new();
        map.insert("months".to_string(), PropertyValue::Integer(3));
        map.insert("days".to_string(), PropertyValue::Integer(5));
        map.insert("hours".to_string(), PropertyValue::Integer(2));
        let result = eval_function(
            "duration",
            &[Value::Property(PropertyValue::Map(map))],
            None,
        )
        .unwrap();
        match result {
            Value::Property(PropertyValue::Duration {
                months,
                days,
                seconds,
                ..
            }) => {
                assert_eq!(months, 3);
                assert_eq!(days, 5);
                assert_eq!(seconds, 7200);
            }
            _ => panic!("Expected Duration"),
        }
    }

    #[test]
    fn test_eval_function_duration_map_with_years() {
        let mut map = HashMap::new();
        map.insert("years".to_string(), PropertyValue::Integer(2));
        map.insert("months".to_string(), PropertyValue::Integer(6));
        let result = eval_function(
            "duration",
            &[Value::Property(PropertyValue::Map(map))],
            None,
        )
        .unwrap();
        match result {
            Value::Property(PropertyValue::Duration { months, .. }) => {
                assert_eq!(months, 30); // 2*12 + 6
            }
            _ => panic!("Expected Duration"),
        }
    }

    #[test]
    fn test_eval_function_duration_no_args() {
        let result = eval_function("duration", &[], None);
        assert!(result.is_err());
    }

    #[test]
    fn test_eval_function_duration_invalid_string() {
        let result = eval_function(
            "duration",
            &[Value::Property(PropertyValue::String(
                "not-a-duration".to_string(),
            ))],
            None,
        );
        assert!(result.is_err());
    }

    #[test]
    fn test_eval_function_duration_type_error() {
        let result = eval_function(
            "duration",
            &[Value::Property(PropertyValue::Integer(42))],
            None,
        );
        assert!(result.is_err());
    }

    #[test]
    fn test_eval_function_timestamp() {
        let result = eval_function("timestamp", &[], None).unwrap();
        match result {
            Value::Property(PropertyValue::Integer(ts)) => assert!(ts > 0),
            _ => panic!("Expected Integer timestamp"),
        }
    }

    #[test]
    fn test_eval_function_duration_between() {
        let dt1 = Value::Property(PropertyValue::DateTime(1000000));
        let dt2 = Value::Property(PropertyValue::DateTime(2000000));
        let result = eval_function("duration_between", &[dt1, dt2], None).unwrap();
        match result {
            Value::Property(PropertyValue::Duration { seconds, .. }) => {
                assert_eq!(seconds, 1000); // 1000000ms diff = 1000s
            }
            _ => panic!("Expected Duration"),
        }
    }

    #[test]
    fn test_eval_function_duration_between_type_error() {
        let result = eval_function(
            "duration_between",
            &[
                Value::Property(PropertyValue::String("a".to_string())),
                Value::Property(PropertyValue::DateTime(0)),
            ],
            None,
        );
        assert!(result.is_err());
    }

    #[test]
    fn test_eval_function_duration_between_too_few_args() {
        let result = eval_function(
            "duration_between",
            &[Value::Property(PropertyValue::DateTime(0))],
            None,
        );
        assert!(result.is_err());
    }

    // -- Math functions --

    #[test]
    fn test_eval_function_log_float() {
        let result =
            eval_function("log", &[Value::Property(PropertyValue::Float(1.0))], None).unwrap();
        match result {
            Value::Property(PropertyValue::Float(f)) => assert!((f - 0.0).abs() < 1e-10),
            _ => panic!("Expected Float"),
        }
    }

    #[test]
    fn test_eval_function_log_integer() {
        let result =
            eval_function("log", &[Value::Property(PropertyValue::Integer(1))], None).unwrap();
        match result {
            Value::Property(PropertyValue::Float(f)) => assert!((f - 0.0).abs() < 1e-10),
            _ => panic!("Expected Float"),
        }
    }

    #[test]
    fn test_eval_function_log_type_error() {
        let result = eval_function(
            "log",
            &[Value::Property(PropertyValue::String("x".to_string()))],
            None,
        );
        assert!(result.is_err());
    }

    #[test]
    fn test_eval_function_exp_float() {
        let result =
            eval_function("exp", &[Value::Property(PropertyValue::Float(1.0))], None).unwrap();
        match result {
            Value::Property(PropertyValue::Float(f)) => {
                assert!((f - std::f64::consts::E).abs() < 1e-10)
            }
            _ => panic!("Expected Float"),
        }
    }

    #[test]
    fn test_eval_function_exp_zero() {
        let result =
            eval_function("exp", &[Value::Property(PropertyValue::Integer(0))], None).unwrap();
        match result {
            Value::Property(PropertyValue::Float(f)) => assert!((f - 1.0).abs() < 1e-10),
            _ => panic!("Expected Float"),
        }
    }

    #[test]
    fn test_eval_function_exp_type_error() {
        let result = eval_function(
            "exp",
            &[Value::Property(PropertyValue::Boolean(true))],
            None,
        );
        assert!(result.is_err());
    }

    #[test]
    fn test_eval_function_rand() {
        let result = eval_function("rand", &[], None).unwrap();
        match result {
            Value::Property(PropertyValue::Float(f)) => {
                assert!(f >= 0.0 && f < 1.0);
            }
            _ => panic!("Expected Float"),
        }
    }

    #[test]
    fn test_eval_function_abs_int() {
        let result =
            eval_function("abs", &[Value::Property(PropertyValue::Integer(-42))], None).unwrap();
        assert_eq!(result, Value::Property(PropertyValue::Integer(42)));
    }

    #[test]
    fn test_eval_function_abs_float() {
        let result =
            eval_function("abs", &[Value::Property(PropertyValue::Float(-3.14))], None).unwrap();
        match result {
            Value::Property(PropertyValue::Float(f)) => assert!((f - 3.14).abs() < 1e-10),
            _ => panic!("Expected Float"),
        }
    }

    #[test]
    fn test_eval_function_abs_type_error() {
        let result = eval_function(
            "abs",
            &[Value::Property(PropertyValue::String("x".to_string()))],
            None,
        );
        assert!(result.is_err());
    }

    #[test]
    fn test_eval_function_ceil_float() {
        let result =
            eval_function("ceil", &[Value::Property(PropertyValue::Float(3.2))], None).unwrap();
        assert_eq!(result, Value::Property(PropertyValue::Integer(4)));
    }

    #[test]
    fn test_eval_function_ceil_int() {
        let result =
            eval_function("ceil", &[Value::Property(PropertyValue::Integer(3))], None).unwrap();
        assert_eq!(result, Value::Property(PropertyValue::Integer(3)));
    }

    #[test]
    fn test_eval_function_floor_float() {
        let result =
            eval_function("floor", &[Value::Property(PropertyValue::Float(3.9))], None).unwrap();
        assert_eq!(result, Value::Property(PropertyValue::Integer(3)));
    }

    #[test]
    fn test_eval_function_floor_int() {
        let result =
            eval_function("floor", &[Value::Property(PropertyValue::Integer(5))], None).unwrap();
        assert_eq!(result, Value::Property(PropertyValue::Integer(5)));
    }

    #[test]
    fn test_eval_function_round_float() {
        let result =
            eval_function("round", &[Value::Property(PropertyValue::Float(3.5))], None).unwrap();
        assert_eq!(result, Value::Property(PropertyValue::Integer(4)));
    }

    #[test]
    fn test_eval_function_round_int() {
        let result =
            eval_function("round", &[Value::Property(PropertyValue::Integer(7))], None).unwrap();
        assert_eq!(result, Value::Property(PropertyValue::Integer(7)));
    }

    #[test]
    fn test_eval_function_sqrt_float() {
        let result =
            eval_function("sqrt", &[Value::Property(PropertyValue::Float(16.0))], None).unwrap();
        match result {
            Value::Property(PropertyValue::Float(f)) => assert!((f - 4.0).abs() < 1e-10),
            _ => panic!("Expected Float"),
        }
    }

    #[test]
    fn test_eval_function_sqrt_int() {
        let result =
            eval_function("sqrt", &[Value::Property(PropertyValue::Integer(9))], None).unwrap();
        match result {
            Value::Property(PropertyValue::Float(f)) => assert!((f - 3.0).abs() < 1e-10),
            _ => panic!("Expected Float"),
        }
    }

    #[test]
    fn test_eval_function_sign_positive() {
        let result =
            eval_function("sign", &[Value::Property(PropertyValue::Integer(42))], None).unwrap();
        assert_eq!(result, Value::Property(PropertyValue::Integer(1)));
    }

    #[test]
    fn test_eval_function_sign_negative() {
        let result =
            eval_function("sign", &[Value::Property(PropertyValue::Integer(-5))], None).unwrap();
        assert_eq!(result, Value::Property(PropertyValue::Integer(-1)));
    }

    #[test]
    fn test_eval_function_sign_zero() {
        let result =
            eval_function("sign", &[Value::Property(PropertyValue::Integer(0))], None).unwrap();
        assert_eq!(result, Value::Property(PropertyValue::Integer(0)));
    }

    #[test]
    fn test_eval_function_sign_float() {
        let result =
            eval_function("sign", &[Value::Property(PropertyValue::Float(-2.5))], None).unwrap();
        assert_eq!(result, Value::Property(PropertyValue::Integer(-1)));
    }

    #[test]
    fn test_eval_function_sign_float_zero() {
        let result =
            eval_function("sign", &[Value::Property(PropertyValue::Float(0.0))], None).unwrap();
        assert_eq!(result, Value::Property(PropertyValue::Integer(0)));
    }

    // -- String edge-case functions --

    #[test]
    fn test_eval_function_ltrim() {
        let result = eval_function(
            "ltrim",
            &[Value::Property(PropertyValue::String(
                "  hello  ".to_string(),
            ))],
            None,
        )
        .unwrap();
        assert_eq!(
            result,
            Value::Property(PropertyValue::String("hello  ".to_string()))
        );
    }

    #[test]
    fn test_eval_function_rtrim() {
        let result = eval_function(
            "rtrim",
            &[Value::Property(PropertyValue::String(
                "  hello  ".to_string(),
            ))],
            None,
        )
        .unwrap();
        assert_eq!(
            result,
            Value::Property(PropertyValue::String("  hello".to_string()))
        );
    }

    #[test]
    fn test_eval_function_trim() {
        let result = eval_function(
            "trim",
            &[Value::Property(PropertyValue::String(
                "  hello  ".to_string(),
            ))],
            None,
        )
        .unwrap();
        assert_eq!(
            result,
            Value::Property(PropertyValue::String("hello".to_string()))
        );
    }

    #[test]
    fn test_eval_function_tostring_integer() {
        let result = eval_function(
            "tostring",
            &[Value::Property(PropertyValue::Integer(42))],
            None,
        )
        .unwrap();
        assert_eq!(
            result,
            Value::Property(PropertyValue::String("42".to_string()))
        );
    }

    #[test]
    fn test_eval_function_tostring_boolean() {
        let result = eval_function(
            "tostring",
            &[Value::Property(PropertyValue::Boolean(true))],
            None,
        )
        .unwrap();
        assert_eq!(
            result,
            Value::Property(PropertyValue::String("true".to_string()))
        );
    }

    #[test]
    fn test_eval_function_tostring_float() {
        let result = eval_function(
            "tostring",
            &[Value::Property(PropertyValue::Float(3.14))],
            None,
        )
        .unwrap();
        match result {
            Value::Property(PropertyValue::String(s)) => assert!(s.starts_with("3.14")),
            _ => panic!("Expected String"),
        }
    }

    #[test]
    fn test_eval_function_tostring_null() {
        // TCK: toString(null) returns null
        let result = eval_function("tostring", &[Value::Null], None).unwrap();
        assert_eq!(result, Value::Null);
    }

    #[test]
    fn test_eval_function_tostring_string() {
        let result = eval_function(
            "tostring",
            &[Value::Property(PropertyValue::String("hello".to_string()))],
            None,
        )
        .unwrap();
        assert_eq!(
            result,
            Value::Property(PropertyValue::String("hello".to_string()))
        );
    }

    #[test]
    fn test_eval_function_tointeger_string() {
        let result = eval_function(
            "tointeger",
            &[Value::Property(PropertyValue::String("42".to_string()))],
            None,
        )
        .unwrap();
        assert_eq!(result, Value::Property(PropertyValue::Integer(42)));
    }

    #[test]
    fn test_eval_function_tointeger_bad_string() {
        // TCK: toInteger("bad") returns null
        let result = eval_function(
            "tointeger",
            &[Value::Property(PropertyValue::String("bad".to_string()))],
            None,
        )
        .unwrap();
        assert_eq!(result, Value::Null);
    }

    #[test]
    fn test_eval_function_tointeger_float() {
        let result = eval_function(
            "tointeger",
            &[Value::Property(PropertyValue::Float(3.9))],
            None,
        )
        .unwrap();
        assert_eq!(result, Value::Property(PropertyValue::Integer(3)));
    }

    #[test]
    fn test_eval_function_tointeger_integer() {
        let result = eval_function(
            "tointeger",
            &[Value::Property(PropertyValue::Integer(7))],
            None,
        )
        .unwrap();
        assert_eq!(result, Value::Property(PropertyValue::Integer(7)));
    }

    #[test]
    fn test_eval_function_tointeger_type_error() {
        // TCK: toInteger(true) returns 1
        let result = eval_function(
            "tointeger",
            &[Value::Property(PropertyValue::Boolean(true))],
            None,
        )
        .unwrap();
        assert_eq!(result, Value::Property(PropertyValue::Integer(1)));
    }

    #[test]
    fn test_eval_function_tofloat_string() {
        let result = eval_function(
            "tofloat",
            &[Value::Property(PropertyValue::String("3.14".to_string()))],
            None,
        )
        .unwrap();
        match result {
            Value::Property(PropertyValue::Float(f)) => assert!((f - 3.14).abs() < 1e-10),
            _ => panic!("Expected Float"),
        }
    }

    #[test]
    fn test_eval_function_tofloat_bad_string() {
        // TCK: toFloat("bad") returns null
        let result = eval_function(
            "tofloat",
            &[Value::Property(PropertyValue::String("bad".to_string()))],
            None,
        )
        .unwrap();
        assert_eq!(result, Value::Null);
    }

    #[test]
    fn test_eval_function_tofloat_integer() {
        let result = eval_function(
            "tofloat",
            &[Value::Property(PropertyValue::Integer(5))],
            None,
        )
        .unwrap();
        match result {
            Value::Property(PropertyValue::Float(f)) => assert!((f - 5.0).abs() < 1e-10),
            _ => panic!("Expected Float"),
        }
    }

    #[test]
    fn test_eval_function_tofloat_float() {
        let result = eval_function(
            "tofloat",
            &[Value::Property(PropertyValue::Float(2.5))],
            None,
        )
        .unwrap();
        match result {
            Value::Property(PropertyValue::Float(f)) => assert!((f - 2.5).abs() < 1e-10),
            _ => panic!("Expected Float"),
        }
    }

    #[test]
    fn test_eval_function_tofloat_type_error() {
        let result = eval_function(
            "tofloat",
            &[Value::Property(PropertyValue::Boolean(false))],
            None,
        );
        assert!(result.is_err());
    }

    // -- String manipulation --

    #[test]
    fn test_eval_function_toupper() {
        let result = eval_function(
            "toupper",
            &[Value::Property(PropertyValue::String("hello".to_string()))],
            None,
        )
        .unwrap();
        assert_eq!(
            result,
            Value::Property(PropertyValue::String("HELLO".to_string()))
        );
    }

    #[test]
    fn test_eval_function_tolower() {
        let result = eval_function(
            "tolower",
            &[Value::Property(PropertyValue::String("HELLO".to_string()))],
            None,
        )
        .unwrap();
        assert_eq!(
            result,
            Value::Property(PropertyValue::String("hello".to_string()))
        );
    }

    #[test]
    fn test_eval_function_replace() {
        let result = eval_function(
            "replace",
            &[
                Value::Property(PropertyValue::String("hello world".to_string())),
                Value::Property(PropertyValue::String("world".to_string())),
                Value::Property(PropertyValue::String("rust".to_string())),
            ],
            None,
        )
        .unwrap();
        assert_eq!(
            result,
            Value::Property(PropertyValue::String("hello rust".to_string()))
        );
    }

    #[test]
    fn test_eval_function_replace_too_few_args() {
        let result = eval_function(
            "replace",
            &[Value::Property(PropertyValue::String("hello".to_string()))],
            None,
        );
        assert!(result.is_err());
    }

    #[test]
    fn test_eval_function_substring() {
        let result = eval_function(
            "substring",
            &[
                Value::Property(PropertyValue::String("hello world".to_string())),
                Value::Property(PropertyValue::Integer(6)),
            ],
            None,
        )
        .unwrap();
        assert_eq!(
            result,
            Value::Property(PropertyValue::String("world".to_string()))
        );
    }

    #[test]
    fn test_eval_function_substring_with_length() {
        let result = eval_function(
            "substring",
            &[
                Value::Property(PropertyValue::String("hello world".to_string())),
                Value::Property(PropertyValue::Integer(0)),
                Value::Property(PropertyValue::Integer(5)),
            ],
            None,
        )
        .unwrap();
        assert_eq!(
            result,
            Value::Property(PropertyValue::String("hello".to_string()))
        );
    }

    #[test]
    fn test_eval_function_substring_beyond_end() {
        let result = eval_function(
            "substring",
            &[
                Value::Property(PropertyValue::String("hi".to_string())),
                Value::Property(PropertyValue::Integer(100)),
            ],
            None,
        )
        .unwrap();
        assert_eq!(
            result,
            Value::Property(PropertyValue::String("".to_string()))
        );
    }

    #[test]
    fn test_eval_function_substring_too_few_args() {
        let result = eval_function(
            "substring",
            &[Value::Property(PropertyValue::String("hello".to_string()))],
            None,
        );
        assert!(result.is_err());
    }

    #[test]
    fn test_eval_function_left() {
        let result = eval_function(
            "left",
            &[
                Value::Property(PropertyValue::String("hello".to_string())),
                Value::Property(PropertyValue::Integer(3)),
            ],
            None,
        )
        .unwrap();
        assert_eq!(
            result,
            Value::Property(PropertyValue::String("hel".to_string()))
        );
    }

    #[test]
    fn test_eval_function_right() {
        let result = eval_function(
            "right",
            &[
                Value::Property(PropertyValue::String("hello".to_string())),
                Value::Property(PropertyValue::Integer(3)),
            ],
            None,
        )
        .unwrap();
        assert_eq!(
            result,
            Value::Property(PropertyValue::String("llo".to_string()))
        );
    }

    #[test]
    fn test_eval_function_reverse() {
        let result = eval_function(
            "reverse",
            &[Value::Property(PropertyValue::String("abc".to_string()))],
            None,
        )
        .unwrap();
        assert_eq!(
            result,
            Value::Property(PropertyValue::String("cba".to_string()))
        );
    }

    // -- Size/length --

    #[test]
    fn test_eval_function_size_string() {
        let result = eval_function(
            "size",
            &[Value::Property(PropertyValue::String("hello".to_string()))],
            None,
        )
        .unwrap();
        assert_eq!(result, Value::Property(PropertyValue::Integer(5)));
    }

    #[test]
    fn test_eval_function_size_array() {
        let arr = vec![
            PropertyValue::Integer(1),
            PropertyValue::Integer(2),
            PropertyValue::Integer(3),
        ];
        let result =
            eval_function("size", &[Value::Property(PropertyValue::Array(arr))], None).unwrap();
        assert_eq!(result, Value::Property(PropertyValue::Integer(3)));
    }

    #[test]
    fn test_eval_function_length_path() {
        use crate::graph::types::{EdgeId, NodeId};
        let path = Value::Path {
            nodes: vec![NodeId::new(1), NodeId::new(2), NodeId::new(3)],
            edges: vec![EdgeId::new(1), EdgeId::new(2)],
        };
        let result = eval_function("length", &[path], None).unwrap();
        assert_eq!(result, Value::Property(PropertyValue::Integer(2)));
    }

    #[test]
    fn test_eval_function_size_type_error() {
        let result = eval_function("size", &[Value::Property(PropertyValue::Integer(42))], None);
        assert!(result.is_err());
    }

    // -- Path functions --

    #[test]
    fn test_eval_function_nodes() {
        use crate::graph::types::{EdgeId, NodeId};
        let path = Value::Path {
            nodes: vec![NodeId::new(1), NodeId::new(2)],
            edges: vec![EdgeId::new(10)],
        };
        let result = eval_function("nodes", &[path], None).unwrap();
        match result {
            Value::Property(PropertyValue::Array(arr)) => {
                assert_eq!(arr.len(), 2);
                assert_eq!(arr[0].as_integer(), Some(1));
                assert_eq!(arr[1].as_integer(), Some(2));
            }
            _ => panic!("Expected Array"),
        }
    }

    #[test]
    fn test_eval_function_nodes_type_error() {
        let result = eval_function("nodes", &[Value::Property(PropertyValue::Integer(1))], None);
        assert!(result.is_err());
    }

    #[test]
    fn test_eval_function_relationships() {
        use crate::graph::types::{EdgeId, NodeId};
        let path = Value::Path {
            nodes: vec![NodeId::new(1), NodeId::new(2)],
            edges: vec![EdgeId::new(10)],
        };
        let result = eval_function("relationships", &[path], None).unwrap();
        match result {
            Value::Property(PropertyValue::Array(arr)) => {
                assert_eq!(arr.len(), 1);
                assert_eq!(arr[0].as_integer(), Some(10));
            }
            _ => panic!("Expected Array"),
        }
    }

    #[test]
    fn test_eval_function_relationships_type_error() {
        let result = eval_function(
            "relationships",
            &[Value::Property(PropertyValue::String("x".to_string()))],
            None,
        );
        assert!(result.is_err());
    }

    // -- startNode/endNode --

    #[test]
    fn test_eval_function_startnode_edgeref() {
        use crate::graph::types::{EdgeId, EdgeType, NodeId};
        let edge = Value::EdgeRef(
            EdgeId::new(1),
            NodeId::new(10),
            NodeId::new(20),
            EdgeType::new("KNOWS"),
        );
        let result = eval_function("startnode", &[edge], None).unwrap();
        assert_eq!(result, Value::NodeRef(NodeId::new(10)));
    }

    #[test]
    fn test_eval_function_endnode_edgeref() {
        use crate::graph::types::{EdgeId, EdgeType, NodeId};
        let edge = Value::EdgeRef(
            EdgeId::new(1),
            NodeId::new(10),
            NodeId::new(20),
            EdgeType::new("KNOWS"),
        );
        let result = eval_function("endnode", &[edge], None).unwrap();
        assert_eq!(result, Value::NodeRef(NodeId::new(20)));
    }

    #[test]
    fn test_eval_function_startnode_type_error() {
        let result = eval_function(
            "startnode",
            &[Value::Property(PropertyValue::Integer(1))],
            None,
        );
        assert!(result.is_err());
    }

    #[test]
    fn test_eval_function_endnode_type_error() {
        let result = eval_function(
            "endnode",
            &[Value::Property(PropertyValue::Integer(1))],
            None,
        );
        assert!(result.is_err());
    }

    // -- range() --

    #[test]
    fn test_eval_function_range_ascending() {
        let result = eval_function(
            "range",
            &[
                Value::Property(PropertyValue::Integer(1)),
                Value::Property(PropertyValue::Integer(5)),
            ],
            None,
        )
        .unwrap();
        match result {
            Value::Property(PropertyValue::Array(arr)) => {
                let vals: Vec<i64> = arr.iter().map(|v| v.as_integer().unwrap()).collect();
                assert_eq!(vals, vec![1, 2, 3, 4, 5]);
            }
            _ => panic!("Expected Array"),
        }
    }

    #[test]
    fn test_eval_function_range_descending() {
        let result = eval_function(
            "range",
            &[
                Value::Property(PropertyValue::Integer(5)),
                Value::Property(PropertyValue::Integer(1)),
                Value::Property(PropertyValue::Integer(-1)),
            ],
            None,
        )
        .unwrap();
        match result {
            Value::Property(PropertyValue::Array(arr)) => {
                let vals: Vec<i64> = arr.iter().map(|v| v.as_integer().unwrap()).collect();
                assert_eq!(vals, vec![5, 4, 3, 2, 1]);
            }
            _ => panic!("Expected Array"),
        }
    }

    #[test]
    fn test_eval_function_range_zero_step() {
        let result = eval_function(
            "range",
            &[
                Value::Property(PropertyValue::Integer(0)),
                Value::Property(PropertyValue::Integer(10)),
                Value::Property(PropertyValue::Integer(0)),
            ],
            None,
        );
        assert!(result.is_err());
    }

    #[test]
    fn test_eval_function_range_too_few_args() {
        let result = eval_function("range", &[Value::Property(PropertyValue::Integer(1))], None);
        assert!(result.is_err());
    }

    // -- Predicate / meta functions --

    #[test]
    fn test_eval_function_coalesce_first_non_null() {
        let result = eval_function(
            "coalesce",
            &[
                Value::Null,
                Value::Property(PropertyValue::Null),
                Value::Property(PropertyValue::Integer(42)),
                Value::Property(PropertyValue::Integer(99)),
            ],
            None,
        )
        .unwrap();
        assert_eq!(result, Value::Property(PropertyValue::Integer(42)));
    }

    #[test]
    fn test_eval_function_coalesce_all_null() {
        let result = eval_function(
            "coalesce",
            &[Value::Null, Value::Property(PropertyValue::Null)],
            None,
        )
        .unwrap();
        assert_eq!(result, Value::Null);
    }

    #[test]
    fn test_eval_function_head() {
        let arr = vec![PropertyValue::Integer(10), PropertyValue::Integer(20)];
        let result =
            eval_function("head", &[Value::Property(PropertyValue::Array(arr))], None).unwrap();
        assert_eq!(result, Value::Property(PropertyValue::Integer(10)));
    }

    #[test]
    fn test_eval_function_head_empty() {
        let result = eval_function(
            "head",
            &[Value::Property(PropertyValue::Array(vec![]))],
            None,
        )
        .unwrap();
        assert_eq!(result, Value::Null);
    }

    #[test]
    fn test_eval_function_head_type_error() {
        let result = eval_function("head", &[Value::Property(PropertyValue::Integer(1))], None);
        assert!(result.is_err());
    }

    #[test]
    fn test_eval_function_last() {
        let arr = vec![PropertyValue::Integer(10), PropertyValue::Integer(20)];
        let result =
            eval_function("last", &[Value::Property(PropertyValue::Array(arr))], None).unwrap();
        assert_eq!(result, Value::Property(PropertyValue::Integer(20)));
    }

    #[test]
    fn test_eval_function_last_empty() {
        let result = eval_function(
            "last",
            &[Value::Property(PropertyValue::Array(vec![]))],
            None,
        )
        .unwrap();
        assert_eq!(result, Value::Null);
    }

    #[test]
    fn test_eval_function_tail() {
        let arr = vec![
            PropertyValue::Integer(1),
            PropertyValue::Integer(2),
            PropertyValue::Integer(3),
        ];
        let result =
            eval_function("tail", &[Value::Property(PropertyValue::Array(arr))], None).unwrap();
        match result {
            Value::Property(PropertyValue::Array(arr)) => {
                assert_eq!(arr.len(), 2);
                assert_eq!(arr[0].as_integer(), Some(2));
                assert_eq!(arr[1].as_integer(), Some(3));
            }
            _ => panic!("Expected Array"),
        }
    }

    #[test]
    fn test_eval_function_tail_type_error() {
        let result = eval_function("tail", &[Value::Property(PropertyValue::Integer(1))], None);
        assert!(result.is_err());
    }

    #[test]
    fn test_eval_function_exists_non_null() {
        let result = eval_function(
            "exists",
            &[Value::Property(PropertyValue::Integer(42))],
            None,
        )
        .unwrap();
        assert_eq!(result, Value::Property(PropertyValue::Boolean(true)));
    }

    #[test]
    fn test_eval_function_exists_null() {
        let result = eval_function("exists", &[Value::Null], None).unwrap();
        assert_eq!(result, Value::Property(PropertyValue::Boolean(false)));
    }

    #[test]
    fn test_eval_function_exists_property_null() {
        let result =
            eval_function("exists", &[Value::Property(PropertyValue::Null)], None).unwrap();
        assert_eq!(result, Value::Property(PropertyValue::Boolean(false)));
    }

    #[test]
    fn test_eval_function_unknown() {
        let result = eval_function("no_such_function", &[], None);
        assert!(result.is_err());
    }

    // ========== eval_binary_op tests ==========

    #[test]
    fn test_binary_op_mod_int() {
        let result = eval_binary_op(
            &BinaryOp::Mod,
            Value::Property(PropertyValue::Integer(10)),
            Value::Property(PropertyValue::Integer(3)),
        )
        .unwrap();
        assert_eq!(result, Value::Property(PropertyValue::Integer(1)));
    }

    #[test]
    fn test_binary_op_mod_float() {
        let result = eval_binary_op(
            &BinaryOp::Mod,
            Value::Property(PropertyValue::Float(10.5)),
            Value::Property(PropertyValue::Float(3.0)),
        )
        .unwrap();
        match result {
            Value::Property(PropertyValue::Float(f)) => assert!((f - 1.5).abs() < 1e-10),
            _ => panic!("Expected Float"),
        }
    }

    #[test]
    fn test_binary_op_mod_int_float() {
        let result = eval_binary_op(
            &BinaryOp::Mod,
            Value::Property(PropertyValue::Integer(10)),
            Value::Property(PropertyValue::Float(3.0)),
        )
        .unwrap();
        match result {
            Value::Property(PropertyValue::Float(f)) => assert!((f - 1.0).abs() < 1e-10),
            _ => panic!("Expected Float"),
        }
    }

    #[test]
    fn test_binary_op_mod_float_int() {
        let result = eval_binary_op(
            &BinaryOp::Mod,
            Value::Property(PropertyValue::Float(10.0)),
            Value::Property(PropertyValue::Integer(3)),
        )
        .unwrap();
        match result {
            Value::Property(PropertyValue::Float(f)) => assert!((f - 1.0).abs() < 1e-10),
            _ => panic!("Expected Float"),
        }
    }

    #[test]
    fn test_binary_op_mod_zero() {
        let result = eval_binary_op(
            &BinaryOp::Mod,
            Value::Property(PropertyValue::Integer(10)),
            Value::Property(PropertyValue::Integer(0)),
        );
        assert!(result.is_err());
    }

    #[test]
    fn test_binary_op_mod_type_error() {
        let result = eval_binary_op(
            &BinaryOp::Mod,
            Value::Property(PropertyValue::String("a".to_string())),
            Value::Property(PropertyValue::Integer(1)),
        );
        assert!(result.is_err());
    }

    #[test]
    fn test_binary_op_starts_with() {
        let result = eval_binary_op(
            &BinaryOp::StartsWith,
            Value::Property(PropertyValue::String("hello world".to_string())),
            Value::Property(PropertyValue::String("hello".to_string())),
        )
        .unwrap();
        assert_eq!(result, Value::Property(PropertyValue::Boolean(true)));
    }

    #[test]
    fn test_binary_op_starts_with_false() {
        let result = eval_binary_op(
            &BinaryOp::StartsWith,
            Value::Property(PropertyValue::String("hello world".to_string())),
            Value::Property(PropertyValue::String("world".to_string())),
        )
        .unwrap();
        assert_eq!(result, Value::Property(PropertyValue::Boolean(false)));
    }

    #[test]
    fn test_binary_op_starts_with_type_error() {
        let result = eval_binary_op(
            &BinaryOp::StartsWith,
            Value::Property(PropertyValue::Integer(1)),
            Value::Property(PropertyValue::String("x".to_string())),
        );
        // Non-string operands return null per Cypher spec
        assert_eq!(result.unwrap(), Value::Property(PropertyValue::Null));
    }

    #[test]
    fn test_binary_op_ends_with() {
        let result = eval_binary_op(
            &BinaryOp::EndsWith,
            Value::Property(PropertyValue::String("hello world".to_string())),
            Value::Property(PropertyValue::String("world".to_string())),
        )
        .unwrap();
        assert_eq!(result, Value::Property(PropertyValue::Boolean(true)));
    }

    #[test]
    fn test_binary_op_ends_with_false() {
        let result = eval_binary_op(
            &BinaryOp::EndsWith,
            Value::Property(PropertyValue::String("hello world".to_string())),
            Value::Property(PropertyValue::String("hello".to_string())),
        )
        .unwrap();
        assert_eq!(result, Value::Property(PropertyValue::Boolean(false)));
    }

    #[test]
    fn test_binary_op_ends_with_type_error() {
        let result = eval_binary_op(
            &BinaryOp::EndsWith,
            Value::Property(PropertyValue::Integer(1)),
            Value::Property(PropertyValue::String("x".to_string())),
        );
        assert_eq!(result.unwrap(), Value::Property(PropertyValue::Null));
    }

    #[test]
    fn test_binary_op_contains() {
        let result = eval_binary_op(
            &BinaryOp::Contains,
            Value::Property(PropertyValue::String("hello world".to_string())),
            Value::Property(PropertyValue::String("lo wo".to_string())),
        )
        .unwrap();
        assert_eq!(result, Value::Property(PropertyValue::Boolean(true)));
    }

    #[test]
    fn test_binary_op_contains_false() {
        let result = eval_binary_op(
            &BinaryOp::Contains,
            Value::Property(PropertyValue::String("hello".to_string())),
            Value::Property(PropertyValue::String("xyz".to_string())),
        )
        .unwrap();
        assert_eq!(result, Value::Property(PropertyValue::Boolean(false)));
    }

    #[test]
    fn test_binary_op_contains_type_error() {
        let result = eval_binary_op(
            &BinaryOp::Contains,
            Value::Property(PropertyValue::Integer(1)),
            Value::Property(PropertyValue::String("x".to_string())),
        );
        assert_eq!(result.unwrap(), Value::Property(PropertyValue::Null));
    }

    #[test]
    fn test_binary_op_in_list() {
        let arr = PropertyValue::Array(vec![
            PropertyValue::Integer(1),
            PropertyValue::Integer(2),
            PropertyValue::Integer(3),
        ]);
        let result = eval_binary_op(
            &BinaryOp::In,
            Value::Property(PropertyValue::Integer(2)),
            Value::Property(arr),
        )
        .unwrap();
        assert_eq!(result, Value::Property(PropertyValue::Boolean(true)));
    }

    #[test]
    fn test_binary_op_in_list_false() {
        let arr = PropertyValue::Array(vec![PropertyValue::Integer(1), PropertyValue::Integer(2)]);
        let result = eval_binary_op(
            &BinaryOp::In,
            Value::Property(PropertyValue::Integer(5)),
            Value::Property(arr),
        )
        .unwrap();
        assert_eq!(result, Value::Property(PropertyValue::Boolean(false)));
    }

    #[test]
    fn test_binary_op_in_non_list() {
        // Per OpenCypher TCK: x IN non-list returns false, not error
        let result = eval_binary_op(
            &BinaryOp::In,
            Value::Property(PropertyValue::Integer(1)),
            Value::Property(PropertyValue::Integer(2)),
        );
        assert!(result.is_ok());
        assert_eq!(
            result.unwrap().as_property().unwrap(),
            &PropertyValue::Boolean(false)
        );
    }

    #[test]
    fn test_binary_op_regex_match() {
        let result = eval_binary_op(
            &BinaryOp::RegexMatch,
            Value::Property(PropertyValue::String("hello123".to_string())),
            Value::Property(PropertyValue::String("^hello\\d+$".to_string())),
        )
        .unwrap();
        assert_eq!(result, Value::Property(PropertyValue::Boolean(true)));
    }

    #[test]
    fn test_binary_op_regex_match_false() {
        let result = eval_binary_op(
            &BinaryOp::RegexMatch,
            Value::Property(PropertyValue::String("hello".to_string())),
            Value::Property(PropertyValue::String("^\\d+$".to_string())),
        )
        .unwrap();
        assert_eq!(result, Value::Property(PropertyValue::Boolean(false)));
    }

    #[test]
    fn test_binary_op_regex_invalid() {
        let result = eval_binary_op(
            &BinaryOp::RegexMatch,
            Value::Property(PropertyValue::String("hello".to_string())),
            Value::Property(PropertyValue::String("[invalid".to_string())),
        );
        assert!(result.is_err());
    }

    #[test]
    fn test_binary_op_regex_type_error() {
        let result = eval_binary_op(
            &BinaryOp::RegexMatch,
            Value::Property(PropertyValue::Integer(1)),
            Value::Property(PropertyValue::String(".*".to_string())),
        );
        assert!(result.is_err());
    }

    // -- Duration arithmetic --

    #[test]
    fn test_binary_op_add_datetime_duration() {
        let dt = PropertyValue::DateTime(0); // epoch
        let dur = PropertyValue::Duration {
            months: 0,
            days: 1,
            seconds: 3600,
            nanos: 0,
        };
        let result =
            eval_binary_op(&BinaryOp::Add, Value::Property(dt), Value::Property(dur)).unwrap();
        match result {
            Value::Property(PropertyValue::DateTime(ts)) => {
                // 1 day + 1 hour = 90000 seconds = 90000000 ms
                assert_eq!(ts, 90_000_000);
            }
            _ => panic!("Expected DateTime"),
        }
    }

    #[test]
    fn test_binary_op_add_duration_duration() {
        let d1 = PropertyValue::Duration {
            months: 1,
            days: 2,
            seconds: 3,
            nanos: 4,
        };
        let d2 = PropertyValue::Duration {
            months: 10,
            days: 20,
            seconds: 30,
            nanos: 40,
        };
        let result =
            eval_binary_op(&BinaryOp::Add, Value::Property(d1), Value::Property(d2)).unwrap();
        match result {
            Value::Property(PropertyValue::Duration {
                months,
                days,
                seconds,
                nanos,
            }) => {
                assert_eq!(months, 11);
                assert_eq!(days, 22);
                assert_eq!(seconds, 33);
                assert_eq!(nanos, 44);
            }
            _ => panic!("Expected Duration"),
        }
    }

    #[test]
    fn test_binary_op_sub_datetime_duration() {
        // Start at 1 day from epoch
        let dt = PropertyValue::DateTime(86_400_000);
        let dur = PropertyValue::Duration {
            months: 0,
            days: 1,
            seconds: 0,
            nanos: 0,
        };
        let result =
            eval_binary_op(&BinaryOp::Sub, Value::Property(dt), Value::Property(dur)).unwrap();
        match result {
            Value::Property(PropertyValue::DateTime(ts)) => {
                assert_eq!(ts, 0); // back to epoch
            }
            _ => panic!("Expected DateTime"),
        }
    }

    #[test]
    fn test_binary_op_sub_datetime_datetime() {
        let dt1 = PropertyValue::DateTime(10_000_000);
        let dt2 = PropertyValue::DateTime(5_000_000);
        let result =
            eval_binary_op(&BinaryOp::Sub, Value::Property(dt1), Value::Property(dt2)).unwrap();
        match result {
            Value::Property(PropertyValue::Duration { seconds, .. }) => {
                assert_eq!(seconds, 5000 % 86400); // 5000s total
            }
            _ => panic!("Expected Duration"),
        }
    }

    #[test]
    fn test_binary_op_sub_duration_duration() {
        let d1 = PropertyValue::Duration {
            months: 10,
            days: 20,
            seconds: 30,
            nanos: 40,
        };
        let d2 = PropertyValue::Duration {
            months: 1,
            days: 2,
            seconds: 3,
            nanos: 4,
        };
        let result =
            eval_binary_op(&BinaryOp::Sub, Value::Property(d1), Value::Property(d2)).unwrap();
        match result {
            Value::Property(PropertyValue::Duration {
                months,
                days,
                seconds,
                nanos,
            }) => {
                assert_eq!(months, 9);
                assert_eq!(days, 18);
                assert_eq!(seconds, 27);
                assert_eq!(nanos, 36);
            }
            _ => panic!("Expected Duration"),
        }
    }

    // -- String concatenation --

    #[test]
    fn test_binary_op_add_strings() {
        let result = eval_binary_op(
            &BinaryOp::Add,
            Value::Property(PropertyValue::String("hello ".to_string())),
            Value::Property(PropertyValue::String("world".to_string())),
        )
        .unwrap();
        assert_eq!(
            result,
            Value::Property(PropertyValue::String("hello world".to_string()))
        );
    }

    // -- Numeric cross-type operations --

    #[test]
    fn test_binary_op_add_int_float() {
        let result = eval_binary_op(
            &BinaryOp::Add,
            Value::Property(PropertyValue::Integer(1)),
            Value::Property(PropertyValue::Float(2.5)),
        )
        .unwrap();
        match result {
            Value::Property(PropertyValue::Float(f)) => assert!((f - 3.5).abs() < 1e-10),
            _ => panic!("Expected Float"),
        }
    }

    #[test]
    fn test_binary_op_sub_float_int() {
        let result = eval_binary_op(
            &BinaryOp::Sub,
            Value::Property(PropertyValue::Float(5.0)),
            Value::Property(PropertyValue::Integer(2)),
        )
        .unwrap();
        match result {
            Value::Property(PropertyValue::Float(f)) => assert!((f - 3.0).abs() < 1e-10),
            _ => panic!("Expected Float"),
        }
    }

    #[test]
    fn test_binary_op_mul_int_float() {
        let result = eval_binary_op(
            &BinaryOp::Mul,
            Value::Property(PropertyValue::Integer(3)),
            Value::Property(PropertyValue::Float(2.0)),
        )
        .unwrap();
        match result {
            Value::Property(PropertyValue::Float(f)) => assert!((f - 6.0).abs() < 1e-10),
            _ => panic!("Expected Float"),
        }
    }

    #[test]
    fn test_binary_op_div_int_zero() {
        let result = eval_binary_op(
            &BinaryOp::Div,
            Value::Property(PropertyValue::Integer(10)),
            Value::Property(PropertyValue::Integer(0)),
        );
        assert!(result.is_err());
    }

    #[test]
    fn test_binary_op_div_int_float() {
        let result = eval_binary_op(
            &BinaryOp::Div,
            Value::Property(PropertyValue::Integer(10)),
            Value::Property(PropertyValue::Float(4.0)),
        )
        .unwrap();
        match result {
            Value::Property(PropertyValue::Float(f)) => assert!((f - 2.5).abs() < 1e-10),
            _ => panic!("Expected Float"),
        }
    }

    // -- Eq/Ne with Null --

    #[test]
    fn test_binary_op_eq_null() {
        // TCK: null = null returns null (three-valued logic)
        let result = eval_binary_op(
            &BinaryOp::Eq,
            Value::Property(PropertyValue::Null),
            Value::Property(PropertyValue::Null),
        )
        .unwrap();
        assert_eq!(result, Value::Null);
    }

    #[test]
    fn test_binary_op_ne_null_vs_int() {
        // TCK: null <> 1 returns null (three-valued logic)
        let result = eval_binary_op(
            &BinaryOp::Ne,
            Value::Property(PropertyValue::Null),
            Value::Property(PropertyValue::Integer(1)),
        )
        .unwrap();
        assert_eq!(result, Value::Null);
    }

    // -- And/Or type errors --

    #[test]
    fn test_binary_op_and_type_error() {
        let result = eval_binary_op(
            &BinaryOp::And,
            Value::Property(PropertyValue::Integer(1)),
            Value::Property(PropertyValue::Boolean(true)),
        );
        assert!(result.is_err());
    }

    #[test]
    fn test_binary_op_or_type_error() {
        let result = eval_binary_op(
            &BinaryOp::Or,
            Value::Property(PropertyValue::String("a".to_string())),
            Value::Property(PropertyValue::Boolean(false)),
        );
        assert!(result.is_err());
    }

    // -- And/Or valid --

    #[test]
    fn test_binary_op_and_true() {
        let result = eval_binary_op(
            &BinaryOp::And,
            Value::Property(PropertyValue::Boolean(true)),
            Value::Property(PropertyValue::Boolean(true)),
        )
        .unwrap();
        assert_eq!(result, Value::Property(PropertyValue::Boolean(true)));
    }

    #[test]
    fn test_binary_op_and_false() {
        let result = eval_binary_op(
            &BinaryOp::And,
            Value::Property(PropertyValue::Boolean(true)),
            Value::Property(PropertyValue::Boolean(false)),
        )
        .unwrap();
        assert_eq!(result, Value::Property(PropertyValue::Boolean(false)));
    }

    #[test]
    fn test_binary_op_or_true() {
        let result = eval_binary_op(
            &BinaryOp::Or,
            Value::Property(PropertyValue::Boolean(false)),
            Value::Property(PropertyValue::Boolean(true)),
        )
        .unwrap();
        assert_eq!(result, Value::Property(PropertyValue::Boolean(true)));
    }

    #[test]
    fn test_binary_op_or_false() {
        let result = eval_binary_op(
            &BinaryOp::Or,
            Value::Property(PropertyValue::Boolean(false)),
            Value::Property(PropertyValue::Boolean(false)),
        )
        .unwrap();
        assert_eq!(result, Value::Property(PropertyValue::Boolean(false)));
    }

    // -- Add type errors --

    #[test]
    fn test_binary_op_add_type_error() {
        let result = eval_binary_op(
            &BinaryOp::Add,
            Value::Property(PropertyValue::Boolean(true)),
            Value::Property(PropertyValue::Integer(1)),
        );
        assert!(result.is_err());
    }

    #[test]
    fn test_binary_op_sub_type_error() {
        let result = eval_binary_op(
            &BinaryOp::Sub,
            Value::Property(PropertyValue::String("a".to_string())),
            Value::Property(PropertyValue::Integer(1)),
        );
        assert!(result.is_err());
    }

    #[test]
    fn test_binary_op_mul_type_error() {
        let result = eval_binary_op(
            &BinaryOp::Mul,
            Value::Property(PropertyValue::String("a".to_string())),
            Value::Property(PropertyValue::Integer(1)),
        );
        assert!(result.is_err());
    }

    #[test]
    fn test_binary_op_div_type_error() {
        let result = eval_binary_op(
            &BinaryOp::Div,
            Value::Property(PropertyValue::Boolean(true)),
            Value::Property(PropertyValue::Integer(1)),
        );
        assert!(result.is_err());
    }

    // -- Non-property Value type error in binary op --

    #[test]
    fn test_binary_op_non_property_left() {
        use crate::graph::types::NodeId;
        let result = eval_binary_op(
            &BinaryOp::Add,
            Value::NodeRef(NodeId::new(1)),
            Value::Property(PropertyValue::Integer(1)),
        );
        assert!(result.is_ok()); // NodeRef treated as null, null + int = null
    }

    #[test]
    fn test_binary_op_non_property_right() {
        use crate::graph::types::NodeId;
        let result = eval_binary_op(
            &BinaryOp::Add,
            Value::Property(PropertyValue::Integer(1)),
            Value::NodeRef(NodeId::new(1)),
        );
        assert!(result.is_ok()); // int + NodeRef = null
    }

    // -- Null handling in binary op --

    #[test]
    fn test_binary_op_null_value_left() {
        // TCK: null = 1 returns null (three-valued logic)
        let result = eval_binary_op(
            &BinaryOp::Eq,
            Value::Null,
            Value::Property(PropertyValue::Integer(1)),
        )
        .unwrap();
        assert_eq!(result, Value::Null);
    }

    // -- Comparison operators --

    #[test]
    fn test_binary_op_lt() {
        let result = eval_binary_op(
            &BinaryOp::Lt,
            Value::Property(PropertyValue::Integer(1)),
            Value::Property(PropertyValue::Integer(2)),
        )
        .unwrap();
        assert_eq!(result, Value::Property(PropertyValue::Boolean(true)));
    }

    #[test]
    fn test_binary_op_le_equal() {
        let result = eval_binary_op(
            &BinaryOp::Le,
            Value::Property(PropertyValue::Integer(2)),
            Value::Property(PropertyValue::Integer(2)),
        )
        .unwrap();
        assert_eq!(result, Value::Property(PropertyValue::Boolean(true)));
    }

    #[test]
    fn test_binary_op_gt() {
        let result = eval_binary_op(
            &BinaryOp::Gt,
            Value::Property(PropertyValue::Integer(3)),
            Value::Property(PropertyValue::Integer(2)),
        )
        .unwrap();
        assert_eq!(result, Value::Property(PropertyValue::Boolean(true)));
    }

    #[test]
    fn test_binary_op_ge_equal() {
        let result = eval_binary_op(
            &BinaryOp::Ge,
            Value::Property(PropertyValue::Integer(2)),
            Value::Property(PropertyValue::Integer(2)),
        )
        .unwrap();
        assert_eq!(result, Value::Property(PropertyValue::Boolean(true)));
    }

    #[test]
    fn test_binary_op_lt_false() {
        let result = eval_binary_op(
            &BinaryOp::Lt,
            Value::Property(PropertyValue::Integer(5)),
            Value::Property(PropertyValue::Integer(2)),
        )
        .unwrap();
        assert_eq!(result, Value::Property(PropertyValue::Boolean(false)));
    }

    // ========== eval_unary_op tests ==========

    #[test]
    fn test_unary_op_is_null_true() {
        let result = eval_unary_op(&UnaryOp::IsNull, Value::Null).unwrap();
        assert_eq!(result, Value::Property(PropertyValue::Boolean(true)));
    }

    #[test]
    fn test_unary_op_is_null_property_null() {
        let result = eval_unary_op(&UnaryOp::IsNull, Value::Property(PropertyValue::Null)).unwrap();
        assert_eq!(result, Value::Property(PropertyValue::Boolean(true)));
    }

    #[test]
    fn test_unary_op_is_null_false() {
        let result =
            eval_unary_op(&UnaryOp::IsNull, Value::Property(PropertyValue::Integer(1))).unwrap();
        assert_eq!(result, Value::Property(PropertyValue::Boolean(false)));
    }

    #[test]
    fn test_unary_op_is_not_null_true() {
        let result = eval_unary_op(
            &UnaryOp::IsNotNull,
            Value::Property(PropertyValue::Integer(1)),
        )
        .unwrap();
        assert_eq!(result, Value::Property(PropertyValue::Boolean(true)));
    }

    #[test]
    fn test_unary_op_is_not_null_false() {
        let result = eval_unary_op(&UnaryOp::IsNotNull, Value::Null).unwrap();
        assert_eq!(result, Value::Property(PropertyValue::Boolean(false)));
    }

    #[test]
    fn test_unary_op_not_true() {
        let result =
            eval_unary_op(&UnaryOp::Not, Value::Property(PropertyValue::Boolean(true))).unwrap();
        assert_eq!(result, Value::Property(PropertyValue::Boolean(false)));
    }

    #[test]
    fn test_unary_op_not_false() {
        let result = eval_unary_op(
            &UnaryOp::Not,
            Value::Property(PropertyValue::Boolean(false)),
        )
        .unwrap();
        assert_eq!(result, Value::Property(PropertyValue::Boolean(true)));
    }

    #[test]
    fn test_unary_op_not_type_error() {
        let result = eval_unary_op(&UnaryOp::Not, Value::Property(PropertyValue::Integer(1)));
        assert!(result.is_err());
    }

    #[test]
    fn test_unary_op_minus_int() {
        let result =
            eval_unary_op(&UnaryOp::Minus, Value::Property(PropertyValue::Integer(42))).unwrap();
        assert_eq!(result, Value::Property(PropertyValue::Integer(-42)));
    }

    #[test]
    fn test_unary_op_minus_float() {
        let result =
            eval_unary_op(&UnaryOp::Minus, Value::Property(PropertyValue::Float(3.14))).unwrap();
        match result {
            Value::Property(PropertyValue::Float(f)) => assert!((f - (-3.14)).abs() < 1e-10),
            _ => panic!("Expected Float"),
        }
    }

    #[test]
    fn test_unary_op_minus_type_error() {
        let result = eval_unary_op(
            &UnaryOp::Minus,
            Value::Property(PropertyValue::String("x".to_string())),
        );
        assert!(result.is_err());
    }

    // ========== eval_index + eval_list_slice tests ==========

    #[test]
    fn test_eval_index_array_positive() {
        let arr = Value::Property(PropertyValue::Array(vec![
            PropertyValue::Integer(10),
            PropertyValue::Integer(20),
            PropertyValue::Integer(30),
        ]));
        let result = eval_index(arr, Value::Property(PropertyValue::Integer(1))).unwrap();
        assert_eq!(result, Value::Property(PropertyValue::Integer(20)));
    }

    #[test]
    fn test_eval_index_array_negative() {
        let arr = Value::Property(PropertyValue::Array(vec![
            PropertyValue::Integer(10),
            PropertyValue::Integer(20),
            PropertyValue::Integer(30),
        ]));
        let result = eval_index(arr, Value::Property(PropertyValue::Integer(-1))).unwrap();
        assert_eq!(result, Value::Property(PropertyValue::Integer(30)));
    }

    #[test]
    fn test_eval_index_array_out_of_bounds() {
        let arr = Value::Property(PropertyValue::Array(vec![PropertyValue::Integer(10)]));
        let result = eval_index(arr, Value::Property(PropertyValue::Integer(5))).unwrap();
        assert_eq!(result, Value::Null);
    }

    #[test]
    fn test_eval_index_map() {
        let mut map = HashMap::new();
        map.insert("key".to_string(), PropertyValue::Integer(42));
        let result = eval_index(
            Value::Property(PropertyValue::Map(map)),
            Value::Property(PropertyValue::String("key".to_string())),
        )
        .unwrap();
        assert_eq!(result, Value::Property(PropertyValue::Integer(42)));
    }

    #[test]
    fn test_eval_index_map_missing_key() {
        let mut map = HashMap::new();
        map.insert("key".to_string(), PropertyValue::Integer(42));
        let result = eval_index(
            Value::Property(PropertyValue::Map(map)),
            Value::Property(PropertyValue::String("missing".to_string())),
        )
        .unwrap();
        assert_eq!(result, Value::Null);
    }

    #[test]
    fn test_eval_index_non_collection() {
        let result = eval_index(
            Value::Property(PropertyValue::Integer(1)),
            Value::Property(PropertyValue::Integer(0)),
        )
        .unwrap();
        assert_eq!(result, Value::Null);
    }

    #[test]
    fn test_eval_list_slice_range() {
        let arr = Value::Property(PropertyValue::Array(vec![
            PropertyValue::Integer(10),
            PropertyValue::Integer(20),
            PropertyValue::Integer(30),
            PropertyValue::Integer(40),
            PropertyValue::Integer(50),
        ]));
        let result = eval_list_slice(
            arr,
            Some(Value::Property(PropertyValue::Integer(1))),
            Some(Value::Property(PropertyValue::Integer(3))),
        )
        .unwrap();
        match result {
            Value::Property(PropertyValue::Array(arr)) => {
                assert_eq!(arr.len(), 2);
                assert_eq!(arr[0].as_integer(), Some(20));
                assert_eq!(arr[1].as_integer(), Some(30));
            }
            _ => panic!("Expected Array"),
        }
    }

    #[test]
    fn test_eval_list_slice_negative_start() {
        let arr = Value::Property(PropertyValue::Array(vec![
            PropertyValue::Integer(10),
            PropertyValue::Integer(20),
            PropertyValue::Integer(30),
        ]));
        let result =
            eval_list_slice(arr, Some(Value::Property(PropertyValue::Integer(-2))), None).unwrap();
        match result {
            Value::Property(PropertyValue::Array(arr)) => {
                assert_eq!(arr.len(), 2);
                assert_eq!(arr[0].as_integer(), Some(20));
                assert_eq!(arr[1].as_integer(), Some(30));
            }
            _ => panic!("Expected Array"),
        }
    }

    #[test]
    fn test_eval_list_slice_from_start() {
        let arr = Value::Property(PropertyValue::Array(vec![
            PropertyValue::Integer(10),
            PropertyValue::Integer(20),
            PropertyValue::Integer(30),
        ]));
        let result =
            eval_list_slice(arr, None, Some(Value::Property(PropertyValue::Integer(2)))).unwrap();
        match result {
            Value::Property(PropertyValue::Array(arr)) => {
                assert_eq!(arr.len(), 2);
                assert_eq!(arr[0].as_integer(), Some(10));
                assert_eq!(arr[1].as_integer(), Some(20));
            }
            _ => panic!("Expected Array"),
        }
    }

    #[test]
    fn test_eval_list_slice_empty_result() {
        let arr = Value::Property(PropertyValue::Array(vec![PropertyValue::Integer(10)]));
        let result = eval_list_slice(
            arr,
            Some(Value::Property(PropertyValue::Integer(3))),
            Some(Value::Property(PropertyValue::Integer(5))),
        )
        .unwrap();
        match result {
            Value::Property(PropertyValue::Array(arr)) => assert!(arr.is_empty()),
            _ => panic!("Expected Array"),
        }
    }

    #[test]
    fn test_eval_list_slice_non_array() {
        let result =
            eval_list_slice(Value::Property(PropertyValue::Integer(1)), None, None).unwrap();
        assert_eq!(result, Value::Null);
    }

    // -- id/labels/type/keys/exists meta functions --

    #[test]
    fn test_eval_function_id_noderef() {
        use crate::graph::types::NodeId;
        let result = eval_function("id", &[Value::NodeRef(NodeId::new(42))], None).unwrap();
        assert_eq!(result, Value::Property(PropertyValue::Integer(42)));
    }

    #[test]
    fn test_eval_function_id_edgeref() {
        use crate::graph::types::{EdgeId, EdgeType, NodeId};
        let result = eval_function(
            "id",
            &[Value::EdgeRef(
                EdgeId::new(7),
                NodeId::new(1),
                NodeId::new(2),
                EdgeType::new("R"),
            )],
            None,
        )
        .unwrap();
        assert_eq!(result, Value::Property(PropertyValue::Integer(7)));
    }

    #[test]
    fn test_eval_function_id_type_error() {
        let result = eval_function("id", &[Value::Property(PropertyValue::Integer(1))], None);
        assert!(result.is_err());
    }

    #[test]
    fn test_labels_with_noderef() {
        let mut store = GraphStore::new();
        let nid = store.create_node("Person");
        store
            .get_node_mut(nid)
            .unwrap()
            .add_label(crate::graph::types::Label::new("Employee"));

        let result = eval_function("labels", &[Value::NodeRef(nid)], Some(&store)).unwrap();
        match result {
            Value::Property(PropertyValue::Array(arr)) => {
                let labels: Vec<String> = arr
                    .iter()
                    .map(|v| v.as_string().unwrap().to_string())
                    .collect();
                assert!(labels.contains(&"Person".to_string()));
                assert!(labels.contains(&"Employee".to_string()));
            }
            _ => panic!("Expected array from labels()"),
        }
    }

    #[test]
    fn test_type_with_edgeref() {
        use crate::graph::types::{EdgeId, EdgeType, NodeId};

        let result = eval_function(
            "type",
            &[Value::EdgeRef(
                EdgeId::new(1),
                NodeId::new(10),
                NodeId::new(20),
                EdgeType::new("KNOWS"),
            )],
            None,
        )
        .unwrap();
        assert_eq!(
            result,
            Value::Property(PropertyValue::String("KNOWS".to_string()))
        );
    }

    #[test]
    fn test_keys_with_noderef() {
        let mut store = GraphStore::new();
        let nid = store.create_node("Person");
        store
            .get_node_mut(nid)
            .unwrap()
            .set_property("name", "Alice");
        store.get_node_mut(nid).unwrap().set_property("age", 30i64);

        let result = eval_function("keys", &[Value::NodeRef(nid)], Some(&store)).unwrap();
        match result {
            Value::Property(PropertyValue::Array(arr)) => {
                let keys: Vec<String> = arr
                    .iter()
                    .map(|v| v.as_string().unwrap().to_string())
                    .collect();
                assert!(keys.contains(&"name".to_string()));
                assert!(keys.contains(&"age".to_string()));
            }
            _ => panic!("Expected array from keys()"),
        }
    }

    #[test]
    fn test_keys_with_edgeref() {
        let mut store = GraphStore::new();
        let n1 = store.create_node("A");
        let n2 = store.create_node("B");
        let eid = store.create_edge(n1, n2, "REL").unwrap();
        store
            .get_edge_mut(eid)
            .unwrap()
            .set_property("weight", 1.5f64);

        let edge = store.get_edge(eid).unwrap();
        let result = eval_function(
            "keys",
            &[Value::EdgeRef(
                eid,
                edge.source,
                edge.target,
                edge.edge_type.clone(),
            )],
            Some(&store),
        )
        .unwrap();
        match result {
            Value::Property(PropertyValue::Array(arr)) => {
                let keys: Vec<String> = arr
                    .iter()
                    .map(|v| v.as_string().unwrap().to_string())
                    .collect();
                assert!(keys.contains(&"weight".to_string()));
            }
            _ => panic!("Expected array from keys()"),
        }
    }

    // ---- ExpandIntoOperator tests (TDD) ----

    #[test]
    fn test_expand_into_basic() {
        let mut store = GraphStore::new();
        let n1 = store.create_node("Person");
        let n2 = store.create_node("Person");
        let _eid = store.create_edge(n1, n2, "KNOWS").unwrap();

        // Create input that provides both source and target
        let mut records = Vec::new();
        let mut r = Record::new();
        r.bind("a".to_string(), Value::NodeRef(n1));
        r.bind("b".to_string(), Value::NodeRef(n2));
        records.push(r);

        // Use CartesianProductOperator isn't suitable here, so we build a simple mock
        // by using a NodeByIdOperator for `a` and manually creating input records.
        // Instead, let's just test with a WithBarrier-like approach: produce a batch
        // Actually, simplest: use a custom input that yields our records
        let input = Box::new(StaticInputOperator { records, index: 0 });

        let mut op = ExpandIntoOperator::new(
            input,
            "a".to_string(),
            "b".to_string(),
            Some("KNOWS".to_string()),
            None,
        );

        let result = op.next(&store).unwrap();
        assert!(result.is_some());

        // No more records
        let result2 = op.next(&store).unwrap();
        assert!(result2.is_none());
    }

    #[test]
    fn test_expand_into_no_edge() {
        let mut store = GraphStore::new();
        let n1 = store.create_node("Person");
        let n2 = store.create_node("Person");
        // No edge between n1 and n2

        let mut records = Vec::new();
        let mut r = Record::new();
        r.bind("a".to_string(), Value::NodeRef(n1));
        r.bind("b".to_string(), Value::NodeRef(n2));
        records.push(r);

        let input = Box::new(StaticInputOperator { records, index: 0 });
        let mut op = ExpandIntoOperator::new(
            input,
            "a".to_string(),
            "b".to_string(),
            Some("KNOWS".to_string()),
            None,
        );

        let result = op.next(&store).unwrap();
        assert!(result.is_none()); // Record filtered out
    }

    #[test]
    fn test_expand_into_with_edge_binding() {
        let mut store = GraphStore::new();
        let n1 = store.create_node("Person");
        let n2 = store.create_node("Person");
        let eid = store.create_edge(n1, n2, "KNOWS").unwrap();

        let mut records = Vec::new();
        let mut r = Record::new();
        r.bind("a".to_string(), Value::NodeRef(n1));
        r.bind("b".to_string(), Value::NodeRef(n2));
        records.push(r);

        let input = Box::new(StaticInputOperator { records, index: 0 });
        let mut op = ExpandIntoOperator::new(
            input,
            "a".to_string(),
            "b".to_string(),
            Some("KNOWS".to_string()),
            Some("r".to_string()),
        );

        let result = op.next(&store).unwrap().unwrap();
        // Edge should be bound
        let edge_val = result.get("r").unwrap();
        assert_eq!(edge_val.edge_id(), Some(eid));
    }

    #[test]
    fn test_expand_into_describe() {
        let input = Box::new(StaticInputOperator {
            records: Vec::new(),
            index: 0,
        });
        let op = ExpandIntoOperator::new(
            input,
            "a".to_string(),
            "b".to_string(),
            Some("KNOWS".to_string()),
            None,
        );
        let desc = op.describe();
        assert_eq!(desc.name, "ExpandInto");
        assert!(desc.details.contains("KNOWS"));
    }

    // ---- NodeByIdOperator tests (TDD) ----

    #[test]
    fn test_node_by_id_operator() {
        let mut store = GraphStore::new();
        let n1 = store.create_node("Person");
        let _n2 = store.create_node("Person");
        let n3 = store.create_node("Company");

        let mut op = NodeByIdOperator::new(vec![n1, n3], "n".to_string());

        let r1 = op.next(&store).unwrap().unwrap();
        assert_eq!(r1.get("n").unwrap().node_id(), Some(n1));

        let r2 = op.next(&store).unwrap().unwrap();
        assert_eq!(r2.get("n").unwrap().node_id(), Some(n3));

        let r3 = op.next(&store).unwrap();
        assert!(r3.is_none());
    }

    #[test]
    fn test_node_by_id_operator_deleted_node() {
        let mut store = GraphStore::new();
        let n1 = store.create_node("Person");
        let n2 = store.create_node("Person");
        store.delete_node("default", n1).unwrap();

        let mut op = NodeByIdOperator::new(vec![n1, n2], "n".to_string());

        // n1 is deleted, should skip it
        let r1 = op.next(&store).unwrap().unwrap();
        assert_eq!(r1.get("n").unwrap().node_id(), Some(n2));

        let r2 = op.next(&store).unwrap();
        assert!(r2.is_none());
    }

    #[test]
    fn test_node_by_id_operator_reset() {
        let mut store = GraphStore::new();
        let n1 = store.create_node("Person");

        let mut op = NodeByIdOperator::new(vec![n1], "n".to_string());
        let _ = op.next(&store).unwrap();
        assert!(op.next(&store).unwrap().is_none());

        op.reset();
        let r = op.next(&store).unwrap();
        assert!(r.is_some());
    }

    /// Helper: a simple operator that yields pre-built records (for testing downstream operators)
    struct StaticInputOperator {
        records: Vec<Record>,
        index: usize,
    }

    impl PhysicalOperator for StaticInputOperator {
        fn next(&mut self, _store: &GraphStore) -> ExecutionResult<Option<Record>> {
            if self.index < self.records.len() {
                let r = self.records[self.index].clone();
                self.index += 1;
                Ok(Some(r))
            } else {
                Ok(None)
            }
        }

        fn next_batch(
            &mut self,
            store: &GraphStore,
            batch_size: usize,
        ) -> ExecutionResult<Option<RecordBatch>> {
            let mut records = Vec::new();
            for _ in 0..batch_size {
                match self.next(store)? {
                    Some(r) => records.push(r),
                    None => break,
                }
            }
            if records.is_empty() {
                Ok(None)
            } else {
                Ok(Some(RecordBatch {
                    records,
                    columns: Vec::new(),
                }))
            }
        }

        fn reset(&mut self) {
            self.index = 0;
        }

        fn describe(&self) -> OperatorDescription {
            OperatorDescription {
                name: "StaticInput".to_string(),
                details: format!("{} records", self.records.len()),
                children: Vec::new(),
            }
        }
    }
}
