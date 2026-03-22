//! # OpenCypher Parser: PEG Grammar → AST
//!
//! This module transforms a Cypher query string into an `ast::Query` tree using
//! pest (a PEG parser generator for Rust). The grammar is defined in `cypher.pest`,
//! which is aligned with the official OpenCypher 9 EBNF specification.
//!
//! ## Grammar Architecture
//!
//! The grammar uses the OpenCypher query structure:
//! - `cypher` (entry) → `top_statement` → `query` → `regular_query`
//! - `regular_query` = `single_query ~ union*`
//! - `single_query` = `multi_part_query | single_part_query`
//!
//! Expressions use a layered precedence hierarchy:
//! `or_expression` > `xor_expression` > `and_expression` > `not_expression` >
//! `comparison_expression` > `string_list_null_predicate_expression` >
//! `add_or_subtract_expression` > `multiply_divide_modulo_expression` >
//! `power_of_expression` > `unary_add_or_subtract_expression` >
//! `non_arithmetic_operator_expression` > `atom`
//!
//! ## Compound-Atomic Rules ($)
//!
//! Many rules use pest's compound-atomic mode (`${ ... }`), which disables
//! implicit whitespace insertion at the top level but allows it inside
//! non-atomic sub-rules. This enforces mandatory whitespace between keywords
//! (e.g., `MATCH pattern`, `WHERE expression`). When iterating compound-atomic
//! pairs, `sp` tokens appear as children and must be skipped.

use crate::graph::{EdgeType, Label, PropertyValue};
use crate::query::ast::*;
use pest::Parser;
use pest_derive::Parser;
use std::collections::HashMap;
use thiserror::Error;

#[derive(Parser)]
#[grammar = "query/cypher.pest"]
struct CypherParser;

/// Parser errors
#[derive(Error, Debug)]
#[allow(clippy::large_enum_variant)]
pub enum ParseError {
    /// Pest parsing error
    #[error("Parse error: {0}")]
    PestError(#[from] pest::error::Error<Rule>),

    /// Semantic error
    #[error("Semantic error: {0}")]
    SemanticError(String),

    /// Unsupported feature
    #[error("Unsupported feature: {0}")]
    UnsupportedFeature(String),
}

pub type ParseResult<T> = Result<T, ParseError>;

/// Parse an integer literal supporting decimal, hex (0x...), and octal (0o...) formats
#[allow(dead_code)]
fn parse_integer_literal(s: &str) -> i64 {
    parse_integer_literal_checked(s).unwrap_or(0)
}

fn parse_integer_literal_checked(s: &str) -> Result<i64, String> {
    let s = s.trim();
    let (negative, digits) = if let Some(rest) = s.strip_prefix('-') {
        (true, rest.trim())
    } else {
        (false, s)
    };
    let unsigned = if let Some(hex) = digits
        .strip_prefix("0x")
        .or_else(|| digits.strip_prefix("0X"))
    {
        u64::from_str_radix(hex, 16).map_err(|e| format!("Integer overflow: {}", e))?
    } else if let Some(oct) = digits
        .strip_prefix("0o")
        .or_else(|| digits.strip_prefix("0O"))
    {
        u64::from_str_radix(oct, 8).map_err(|e| format!("Integer overflow: {}", e))?
    } else {
        digits
            .parse::<u64>()
            .map_err(|e| format!("Integer overflow: {}", e))?
    };
    if negative {
        if unsigned == 0x8000000000000000 {
            Ok(i64::MIN)
        } else if unsigned > i64::MAX as u64 {
            Err("Integer overflow: number too large to fit in target type".to_string())
        } else {
            Ok(-(unsigned as i64))
        }
    } else if unsigned > i64::MAX as u64 {
        Err("Integer overflow: number too large to fit in target type".to_string())
    } else {
        Ok(unsigned as i64)
    }
}

/// Convert an Expression to a PropertyValue (for literal values in lists/maps/properties).
/// Non-literal expressions are stored as Null — the executor handles them dynamically.
fn expression_to_property_value(expr: &Expression) -> PropertyValue {
    match expr {
        Expression::Literal(pv) => pv.clone(),
        Expression::Unary {
            op: UnaryOp::Minus,
            expr: inner,
        } => match inner.as_ref() {
            Expression::Literal(PropertyValue::Integer(i)) => PropertyValue::Integer(-i),
            Expression::Literal(PropertyValue::Float(f)) => PropertyValue::Float(-f),
            _ => PropertyValue::Null,
        },
        _ => PropertyValue::Null,
    }
}

// ============================================================
// Public API
// ============================================================

/// Parse a Cypher query string into an AST
pub fn parse_query(input: &str) -> ParseResult<Query> {
    let pairs = CypherParser::parse(Rule::cypher, input)?;

    let mut query = Query::new();

    for pair in pairs {
        if pair.as_rule() == Rule::cypher {
            for inner in pair.into_inner() {
                match inner.as_rule() {
                    Rule::explain_prefix => {
                        for ep in inner.into_inner() {
                            match ep.as_rule() {
                                Rule::kw_explain => query.explain = true,
                                Rule::kw_profile => query.profile = true,
                                _ => {}
                            }
                        }
                    }
                    Rule::top_statement => {
                        parse_top_statement(inner, &mut query)?;
                    }
                    Rule::EOI => break,
                    _ => {}
                }
            }
        }
    }

    Ok(query)
}

// ============================================================
// Top-level statement dispatch
// ============================================================

fn parse_top_statement(pair: pest::iterators::Pair<Rule>, query: &mut Query) -> ParseResult<()> {
    for inner in pair.into_inner() {
        match inner.as_rule() {
            Rule::show_indexes_stmt => {
                query.show_indexes = true;
            }
            Rule::show_constraints_stmt => {
                query.show_constraints = true;
            }
            Rule::drop_index_stmt => {
                parse_drop_index_statement(inner, query)?;
            }
            Rule::create_constraint_stmt => {
                parse_create_constraint_statement(inner, query)?;
            }
            Rule::create_vector_index_stmt => {
                parse_create_vector_index_statement(inner, query)?;
            }
            Rule::create_index_stmt => {
                parse_create_index_statement(inner, query)?;
            }
            Rule::query => {
                parse_query_rule(inner, &mut *query)?;
            }
            _ => {}
        }
    }
    Ok(())
}

// ============================================================
// DDL statements (CREATE INDEX, DROP INDEX, SHOW, CONSTRAINT, VECTOR INDEX)
// ============================================================

fn parse_create_index_statement(
    pair: pest::iterators::Pair<Rule>,
    query: &mut Query,
) -> ParseResult<()> {
    let mut label = None;
    let mut properties: Vec<String> = Vec::new();

    for inner in pair.into_inner() {
        match inner.as_rule() {
            Rule::schema_name => {
                if label.is_none() {
                    label = Some(Label::new(inner.as_str()));
                }
            }
            Rule::property_key_name => properties.push(inner.as_str().to_string()),
            _ => {}
        }
    }

    let first_property = properties
        .first()
        .ok_or_else(|| ParseError::SemanticError("Missing property".to_string()))?
        .clone();
    let additional_properties = properties.into_iter().skip(1).collect();

    query.create_index_clause = Some(CreateIndexClause {
        label: label.ok_or_else(|| ParseError::SemanticError("Missing label".to_string()))?,
        property: first_property,
        additional_properties,
    });
    Ok(())
}

fn parse_drop_index_statement(
    pair: pest::iterators::Pair<Rule>,
    query: &mut Query,
) -> ParseResult<()> {
    let mut label = None;
    let mut property = None;

    for inner in pair.into_inner() {
        match inner.as_rule() {
            Rule::schema_name => {
                if label.is_none() {
                    label = Some(Label::new(inner.as_str()));
                }
            }
            Rule::property_key_name => property = Some(inner.as_str().to_string()),
            _ => {}
        }
    }

    query.drop_index_clause = Some(DropIndexClause {
        label: label.ok_or_else(|| ParseError::SemanticError("Missing label".to_string()))?,
        property: property
            .ok_or_else(|| ParseError::SemanticError("Missing property".to_string()))?,
    });
    Ok(())
}

fn parse_create_constraint_statement(
    pair: pest::iterators::Pair<Rule>,
    query: &mut Query,
) -> ParseResult<()> {
    let mut variable = None;
    let mut label = None;
    let mut property = None;

    for inner in pair.into_inner() {
        match inner.as_rule() {
            Rule::variable => {
                if variable.is_none() {
                    variable = Some(inner.as_str().to_string());
                }
            }
            Rule::schema_name => {
                if label.is_none() {
                    label = Some(Label::new(inner.as_str()));
                }
            }
            Rule::property_expression => {
                // Extract property from property_expression (atom ~ property_lookup+)
                for pe in inner.into_inner() {
                    if pe.as_rule() == Rule::property_lookup {
                        // property_lookup = { "." ~ property_key_name }
                        for plk in pe.into_inner() {
                            if plk.as_rule() == Rule::property_key_name {
                                property = Some(plk.as_str().to_string());
                            }
                        }
                    }
                }
            }
            _ => {}
        }
    }

    query.create_constraint_clause = Some(CreateConstraintClause {
        variable: variable
            .ok_or_else(|| ParseError::SemanticError("Missing variable".to_string()))?,
        label: label.ok_or_else(|| ParseError::SemanticError("Missing label".to_string()))?,
        property: property
            .ok_or_else(|| ParseError::SemanticError("Missing property".to_string()))?,
    });
    Ok(())
}

fn parse_create_vector_index_statement(
    pair: pest::iterators::Pair<Rule>,
    query: &mut Query,
) -> ParseResult<()> {
    let mut index_name = None;
    let mut label = None;
    let mut property_key = None;
    let mut dimensions = 1536;
    let mut similarity = "cosine".to_string();

    for inner in pair.into_inner() {
        match inner.as_rule() {
            Rule::symbolic_name => {
                if index_name.is_none() {
                    index_name = Some(inner.as_str().to_string());
                }
            }
            Rule::variable => {
                // The variable inside FOR (...) — skip it, we get label from schema_name
            }
            Rule::schema_name => {
                if label.is_none() {
                    label = Some(Label::new(inner.as_str()));
                }
            }
            Rule::property_expression => {
                // property_expression = atom ~ property_lookup+
                for pe in inner.into_inner() {
                    if pe.as_rule() == Rule::property_lookup {
                        for plk in pe.into_inner() {
                            if plk.as_rule() == Rule::property_key_name {
                                property_key = Some(plk.as_str().to_string());
                            }
                        }
                    }
                }
            }
            Rule::map_literal => {
                let options_map = parse_map_literal_to_props(inner.clone())?;
                if let Some(PropertyValue::Integer(d)) = options_map.get("dimensions") {
                    dimensions = *d as usize;
                }
                if let Some(PropertyValue::String(s)) = options_map.get("similarity") {
                    similarity = s.clone();
                }
            }
            _ => {}
        }
    }

    query.create_vector_index_clause = Some(CreateVectorIndexClause {
        index_name,
        label: label.ok_or_else(|| {
            ParseError::SemanticError("Missing label in CREATE VECTOR INDEX".to_string())
        })?,
        property_key: property_key.ok_or_else(|| {
            ParseError::SemanticError("Missing property key in CREATE VECTOR INDEX".to_string())
        })?,
        dimensions,
        similarity,
    });
    Ok(())
}

// ============================================================
// Query rule: standalone_call | regular_query
// ============================================================

fn parse_query_rule(pair: pest::iterators::Pair<Rule>, query: &mut Query) -> ParseResult<()> {
    for inner in pair.into_inner() {
        match inner.as_rule() {
            Rule::standalone_call => {
                parse_standalone_call(inner, query)?;
            }
            Rule::regular_query => {
                parse_regular_query(inner, query)?;
            }
            _ => {}
        }
    }
    Ok(())
}

// ============================================================
// regular_query = single_query ~ union*
// ============================================================

fn parse_regular_query(pair: pest::iterators::Pair<Rule>, query: &mut Query) -> ParseResult<()> {
    let mut first = true;
    for inner in pair.into_inner() {
        match inner.as_rule() {
            Rule::single_query => {
                if first {
                    parse_single_query(inner, query)?;
                    first = false;
                }
                // Subsequent single_query come from union processing below
            }
            Rule::union => {
                // union = ${ (kw_union ~ sp ~ kw_all ~ sp? ~ single_query) | (kw_union ~ sp? ~ single_query) }
                let text = inner.as_str().to_uppercase();
                let is_union_all = text.contains("ALL");
                let mut union_query = Query::new();
                for u_inner in inner.into_inner() {
                    if u_inner.as_rule() == Rule::single_query {
                        parse_single_query(u_inner, &mut union_query)?;
                    }
                }
                query.union_queries.push((union_query, is_union_all));
            }
            _ => {}
        }
    }
    Ok(())
}

// ============================================================
// single_query = multi_part_query | single_part_query
// ============================================================

fn parse_single_query(pair: pest::iterators::Pair<Rule>, query: &mut Query) -> ParseResult<()> {
    for inner in pair.into_inner() {
        match inner.as_rule() {
            Rule::multi_part_query => {
                parse_multi_part_query(inner, query)?;
            }
            Rule::single_part_query => {
                parse_single_part_query(inner, query)?;
            }
            _ => {}
        }
    }
    Ok(())
}

// ============================================================
// single_part_query = reading_clause* ~ updating_clause+ ~ return_clause?
//                   | reading_clause* ~ return_clause
// ============================================================

fn parse_single_part_query(
    pair: pest::iterators::Pair<Rule>,
    query: &mut Query,
) -> ParseResult<()> {
    for inner in pair.into_inner() {
        match inner.as_rule() {
            Rule::reading_clause => {
                parse_reading_clause(inner, query)?;
            }
            Rule::updating_clause => {
                parse_updating_clause(inner, query)?;
            }
            Rule::return_clause => {
                let (ret, order_by, skip, limit) = parse_return_clause(inner)?;
                query.return_clause = Some(ret);
                if order_by.is_some() {
                    query.order_by = order_by;
                }
                if skip.is_some() {
                    query.skip = skip;
                }
                if limit.is_some() {
                    query.limit = limit;
                }
            }
            _ => {}
        }
    }
    Ok(())
}

// ============================================================
// multi_part_query = (reading_clause* ~ updating_clause* ~ with_clause)+ ~ single_part_query
// ============================================================

fn parse_multi_part_query(pair: pest::iterators::Pair<Rule>, query: &mut Query) -> ParseResult<()> {
    // The multi_part_query has N WITH-segments followed by a single_part_query.
    // We collect clauses between WITH separators.
    // Collect all children and process them segment by segment.
    let children: Vec<pest::iterators::Pair<Rule>> = pair.into_inner().collect();

    // We need to process: reading*, updating*, with_clause repeated, then single_part_query
    // Each WITH separates a stage.
    let mut pending_match_clauses: Vec<MatchClause> = Vec::new();
    let mut pending_unwind: Option<UnwindClause> = None;
    let mut pending_where: Option<WhereClause> = None;

    for child in children {
        match child.as_rule() {
            Rule::reading_clause => {
                // Accumulate reading clauses for the current segment
                parse_reading_clause_into_pending(
                    child,
                    &mut pending_match_clauses,
                    &mut pending_unwind,
                    query,
                )?;
            }
            Rule::updating_clause => {
                parse_updating_clause(child, query)?;
            }
            Rule::with_clause => {
                let wc = parse_with_clause(child)?;

                if query.with_clause.is_some() {
                    // Previous WITH exists — save it as an extra stage
                    let prev_with = query.with_clause.take().unwrap();
                    let prev_unwind = query.unwind_clause.take();

                    // Post-WITH match clauses are the ones accumulated since the split
                    let split = query.with_split_index.unwrap_or(query.match_clauses.len());
                    let post_matches: Vec<_> = query.match_clauses.drain(split..).collect();
                    let post_where = query.post_with_where_clause.take();

                    query.extra_with_stages.push((
                        prev_with,
                        prev_unwind,
                        post_matches,
                        post_where,
                    ));
                }

                // Push any pending match clauses to the main query
                for mc in pending_match_clauses.drain(..) {
                    query.match_clauses.push(mc);
                }
                if let Some(uw) = pending_unwind.take() {
                    if let Some(prev) = query.unwind_clause.take() {
                        query.additional_unwinds.push(prev);
                    }
                    query.unwind_clause = Some(uw);
                }

                query.with_split_index = Some(query.match_clauses.len());
                query.with_clause = Some(wc);
            }
            Rule::single_part_query => {
                // Push any remaining pending match clauses
                for mc in pending_match_clauses.drain(..) {
                    query.match_clauses.push(mc);
                }
                if let Some(uw) = pending_unwind.take() {
                    if let Some(prev) = query.unwind_clause.take() {
                        query.additional_unwinds.push(prev);
                    }
                    query.unwind_clause = Some(uw);
                }
                parse_single_part_query(child, query)?;
            }
            _ => {}
        }
    }

    // Apply pending_where if any
    if let Some(pw) = pending_where.take() {
        if query.with_clause.is_some() {
            query.post_with_where_clause = Some(pw);
        } else {
            query.where_clause = Some(pw);
        }
    }

    Ok(())
}

fn parse_reading_clause_into_pending(
    pair: pest::iterators::Pair<Rule>,
    match_clauses: &mut Vec<MatchClause>,
    unwind: &mut Option<UnwindClause>,
    query: &mut Query,
) -> ParseResult<()> {
    for inner in pair.into_inner() {
        match inner.as_rule() {
            Rule::match_clause => {
                let (mc, wc) = parse_match_clause(inner)?;
                match_clauses.push(mc);
                if let Some(w) = wc {
                    // WHERE clause from MATCH goes to main query
                    if query.with_clause.is_some() {
                        query.post_with_where_clause = Some(w);
                    } else {
                        query.where_clause = Some(w);
                    }
                }
            }
            Rule::unwind => {
                *unwind = Some(parse_unwind_clause(inner)?);
            }
            Rule::in_query_call => {
                query.call_clause = Some(parse_in_query_call(inner)?);
            }
            _ => {}
        }
    }
    Ok(())
}

// ============================================================
// reading_clause = match_clause | unwind | in_query_call
// ============================================================

fn parse_reading_clause(pair: pest::iterators::Pair<Rule>, query: &mut Query) -> ParseResult<()> {
    for inner in pair.into_inner() {
        match inner.as_rule() {
            Rule::match_clause => {
                let (mc, wc) = parse_match_clause(inner)?;
                query.match_clauses.push(mc);
                if let Some(w) = wc {
                    if query.with_clause.is_some() || query.with_split_index.is_some() {
                        query.post_with_where_clause = Some(w);
                    } else {
                        query.where_clause = Some(w);
                    }
                }
            }
            Rule::unwind => {
                let uc = parse_unwind_clause(inner)?;
                if let Some(prev) = query.unwind_clause.take() {
                    query.additional_unwinds.push(prev);
                }
                query.unwind_clause = Some(uc);
            }
            Rule::in_query_call => {
                query.call_clause = Some(parse_in_query_call(inner)?);
            }
            _ => {}
        }
    }
    Ok(())
}

// ============================================================
// updating_clause = create | merge | delete_clause | set_clause | remove | foreach_clause | call_subquery
// ============================================================

fn parse_updating_clause(pair: pest::iterators::Pair<Rule>, query: &mut Query) -> ParseResult<()> {
    for inner in pair.into_inner() {
        match inner.as_rule() {
            Rule::create => {
                let pattern = parse_create_clause(inner)?;
                let clause = CreateClause { pattern };
                if query.create_clause.is_none() {
                    query.create_clause = Some(clause.clone());
                }
                query.create_clauses.push(clause);
            }
            Rule::merge => {
                query.merge_clause = Some(parse_merge_clause(inner)?);
            }
            Rule::delete_clause => {
                query.delete_clause = Some(parse_delete_clause(inner)?);
            }
            Rule::set_clause => {
                query.set_clauses.push(parse_set_clause(inner)?);
            }
            Rule::remove => {
                query.remove_clauses.push(parse_remove_clause(inner)?);
            }
            Rule::foreach_clause => {
                query.foreach_clause = Some(parse_foreach_clause(inner)?);
            }
            Rule::call_subquery => {
                parse_call_subquery(inner, query)?;
            }
            _ => {}
        }
    }
    Ok(())
}

// ============================================================
// MATCH clause
// match_clause = ${ (kw_optional ~ sp)? ~ kw_match ~ sp ~ pattern ~ (sp? ~ where_clause)? }
// ============================================================

fn parse_match_clause(
    pair: pest::iterators::Pair<Rule>,
) -> ParseResult<(MatchClause, Option<WhereClause>)> {
    let mut optional = false;
    let mut pattern = None;
    let mut where_clause = None;

    for inner in pair.into_inner() {
        match inner.as_rule() {
            Rule::kw_optional => {
                optional = true;
            }
            Rule::pattern => {
                pattern = Some(parse_pattern(inner)?);
            }
            Rule::where_clause => {
                where_clause = Some(parse_where_clause(inner)?);
            }
            _ => {} // kw_match
        }
    }

    let pat =
        pattern.ok_or_else(|| ParseError::SemanticError("MATCH missing pattern".to_string()))?;

    Ok((
        MatchClause {
            pattern: pat,
            optional,
        },
        where_clause,
    ))
}

// ============================================================
// CREATE clause
// create = ${ kw_create ~ sp ~ pattern }
// ============================================================

fn parse_create_clause(pair: pest::iterators::Pair<Rule>) -> ParseResult<Pattern> {
    for inner in pair.into_inner() {
        if inner.as_rule() == Rule::pattern {
            return parse_pattern(inner);
        }
    }
    Err(ParseError::SemanticError(
        "CREATE missing pattern".to_string(),
    ))
}

// ============================================================
// MERGE clause
// merge = ${ kw_merge ~ sp ~ pattern_part ~ merge_action* }
// ============================================================

fn parse_merge_clause(pair: pest::iterators::Pair<Rule>) -> ParseResult<MergeClause> {
    let mut pattern_part = None;
    let mut on_create_set = Vec::new();
    let mut on_match_set = Vec::new();

    for inner in pair.into_inner() {
        match inner.as_rule() {
            Rule::pattern_part => {
                pattern_part = Some(parse_pattern_part(inner)?);
            }
            Rule::merge_action => {
                parse_merge_action(inner, &mut on_create_set, &mut on_match_set)?;
            }
            _ => {} // kw_merge, sp
        }
    }

    let pp = pattern_part
        .ok_or_else(|| ParseError::SemanticError("MERGE missing pattern".to_string()))?;

    Ok(MergeClause {
        pattern: Pattern { paths: vec![pp] },
        on_create_set,
        on_match_set,
    })
}

fn parse_merge_action(
    pair: pest::iterators::Pair<Rule>,
    on_create_set: &mut Vec<SetItem>,
    on_match_set: &mut Vec<SetItem>,
) -> ParseResult<()> {
    // merge_action = { (kw_on ~ kw_match ~ set_clause) | (kw_on ~ kw_create ~ set_clause) }
    let text = pair.as_str().to_uppercase();
    let is_create = text.contains("CREATE");

    for inner in pair.into_inner() {
        if inner.as_rule() == Rule::set_clause {
            let sc = parse_set_clause(inner)?;
            if is_create {
                on_create_set.extend(sc.items);
            } else {
                on_match_set.extend(sc.items);
            }
        }
    }
    Ok(())
}

// ============================================================
// DELETE clause
// delete_clause = ${ (kw_detach ~ sp)? ~ kw_delete ~ sp ~ expression ~ ("," ~ expression)* }
// ============================================================

fn parse_delete_clause(pair: pest::iterators::Pair<Rule>) -> ParseResult<DeleteClause> {
    let mut detach = false;
    let mut expressions = Vec::new();

    for inner in pair.into_inner() {
        match inner.as_rule() {
            Rule::kw_detach => detach = true,
            Rule::expression => expressions.push(parse_expression(inner)?),
            _ => {} // kw_delete
        }
    }

    Ok(DeleteClause {
        expressions,
        detach,
    })
}

// ============================================================
// SET clause
// set_clause = { kw_set ~ set_item ~ ("," ~ set_item)* }
// set_item = { (property_expression ~ "=" ~ expression) |
//              (variable ~ "+=" ~ expression) |
//              (variable ~ "=" ~ expression) |
//              (variable ~ node_labels) }
// ============================================================

fn parse_set_clause(pair: pest::iterators::Pair<Rule>) -> ParseResult<SetClause> {
    let mut items = Vec::new();

    for inner in pair.into_inner() {
        if inner.as_rule() == Rule::set_item {
            items.push(parse_set_item(inner)?);
        }
    }

    Ok(SetClause { items })
}

fn parse_set_item(pair: pest::iterators::Pair<Rule>) -> ParseResult<SetItem> {
    let full_text = pair.as_str();
    let children: Vec<pest::iterators::Pair<Rule>> = pair.into_inner().collect();

    // Determine which form of set_item this is by looking at children
    // Form 1: property_expression ~ "=" ~ expression (SET n.name = "Alice")
    // Form 2: variable ~ "+=" ~ expression (SET n += {map})
    // Form 3: variable ~ "=" ~ expression (SET n = {map})
    // Form 4: variable ~ node_labels (SET n:Label)

    if children.is_empty() {
        return Err(ParseError::SemanticError("Empty SET item".to_string()));
    }

    // Check if first child is a property_expression
    if children[0].as_rule() == Rule::property_expression {
        // Form 1: property_expression = expression
        let (variable, property) = parse_property_expression_parts(&children[0])?;
        let mut value = Expression::Literal(PropertyValue::Null);
        for c in &children[1..] {
            if c.as_rule() == Rule::expression {
                value = parse_expression(c.clone())?;
            }
        }
        return Ok(SetItem {
            variable,
            property,
            value,
        });
    }

    // Check if first child is a variable
    if children[0].as_rule() == Rule::variable {
        let variable = children[0].as_str().to_string();

        // Check for node_labels (Form 4)
        for c in &children[1..] {
            if c.as_rule() == Rule::node_labels {
                let mut labels = Vec::new();
                for nl in c.clone().into_inner() {
                    if nl.as_rule() == Rule::node_label {
                        // node_label = { ":" ~ label_name } where label_name = _{ schema_name }
                        let label_text = nl.as_str();
                        let label_name = label_text.trim_start_matches(':').trim();
                        labels.push(PropertyValue::String(label_name.to_string()));
                    }
                }
                return Ok(SetItem {
                    variable,
                    property: "__labels__".to_string(),
                    value: Expression::Literal(PropertyValue::Array(labels)),
                });
            }
        }

        // Check for += (Form 2) or = (Form 3) by looking at full text
        let is_map_merge = full_text.contains("+=");

        let mut value = Expression::Literal(PropertyValue::Null);
        for c in &children[1..] {
            if c.as_rule() == Rule::expression {
                value = parse_expression(c.clone())?;
            }
        }

        if is_map_merge {
            return Ok(SetItem {
                variable,
                property: "__map_merge__".to_string(),
                value,
            });
        } else {
            return Ok(SetItem {
                variable,
                property: "__map_replace__".to_string(),
                value,
            });
        }
    }

    Err(ParseError::SemanticError(format!(
        "Unrecognized SET item: {}",
        full_text
    )))
}

/// Extract variable and property name from a property_expression pair.
/// property_expression = { atom ~ property_lookup+ }
fn parse_property_expression_parts(
    pair: &pest::iterators::Pair<Rule>,
) -> ParseResult<(String, String)> {
    let mut variable = String::new();
    let mut property = String::new();

    for inner in pair.clone().into_inner() {
        match inner.as_rule() {
            Rule::atom => {
                // The atom should be a variable
                variable = inner.as_str().to_string();
            }
            Rule::property_lookup => {
                // property_lookup = { "." ~ property_key_name }
                for plk in inner.into_inner() {
                    if plk.as_rule() == Rule::property_key_name {
                        property = plk.as_str().to_string();
                    }
                }
            }
            _ => {}
        }
    }

    Ok((variable, property))
}

// ============================================================
// REMOVE clause
// remove = ${ kw_remove ~ sp ~ remove_item ~ ("," ~ remove_item)* }
// remove_item = { (variable ~ node_labels) | property_expression }
// ============================================================

fn parse_remove_clause(pair: pest::iterators::Pair<Rule>) -> ParseResult<RemoveClause> {
    let mut items = Vec::new();

    for inner in pair.into_inner() {
        if inner.as_rule() == Rule::remove_item {
            let children: Vec<_> = inner.into_inner().collect();

            // Check if it's property_expression (REMOVE n.prop) or variable + node_labels (REMOVE n:Label)
            if children.len() == 1 && children[0].as_rule() == Rule::property_expression {
                let (variable, property) = parse_property_expression_parts(&children[0])?;
                items.push(RemoveItem::Property { variable, property });
            } else {
                // variable ~ node_labels
                let mut variable = String::new();
                for child in &children {
                    match child.as_rule() {
                        Rule::variable => variable = child.as_str().to_string(),
                        Rule::node_labels => {
                            for nl in child.clone().into_inner() {
                                if nl.as_rule() == Rule::node_label {
                                    let label_text = nl.as_str();
                                    let label_name = label_text.trim_start_matches(':').trim();
                                    items.push(RemoveItem::Label {
                                        variable: variable.clone(),
                                        label: Label::new(label_name),
                                    });
                                }
                            }
                        }
                        _ => {}
                    }
                }
            }
        }
    }

    Ok(RemoveClause { items })
}

// ============================================================
// UNWIND clause
// unwind = ${ kw_unwind ~ sp ~ expression ~ sp ~ kw_as ~ sp ~ variable }
// ============================================================

fn parse_unwind_clause(pair: pest::iterators::Pair<Rule>) -> ParseResult<UnwindClause> {
    let mut expression = None;
    let mut variable = None;

    for inner in pair.into_inner() {
        match inner.as_rule() {
            Rule::expression => expression = Some(parse_expression(inner)?),
            Rule::variable => variable = Some(inner.as_str().to_string()),
            _ => {} // kw_unwind, kw_as, sp
        }
    }

    Ok(UnwindClause {
        expression: expression
            .ok_or_else(|| ParseError::SemanticError("UNWIND missing expression".to_string()))?,
        variable: variable
            .ok_or_else(|| ParseError::SemanticError("UNWIND missing AS variable".to_string()))?,
    })
}

// ============================================================
// FOREACH clause
// foreach_clause = { kw_foreach ~ "(" ~ variable ~ kw_in ~ expression ~ "|" ~ foreach_body+ ~ ")" }
// foreach_body = _{ set_clause | remove | delete_clause | create }
// ============================================================

fn parse_foreach_clause(pair: pest::iterators::Pair<Rule>) -> ParseResult<ForeachClause> {
    let mut variable = None;
    let mut expression = None;
    let mut set_clauses = Vec::new();
    let mut create_clauses = Vec::new();

    for inner in pair.into_inner() {
        match inner.as_rule() {
            Rule::variable => {
                if variable.is_none() {
                    variable = Some(inner.as_str().to_string());
                }
            }
            Rule::expression => {
                if expression.is_none() {
                    expression = Some(parse_expression(inner)?);
                }
            }
            Rule::set_clause => set_clauses.push(parse_set_clause(inner)?),
            Rule::create => {
                let pattern = parse_create_clause(inner)?;
                create_clauses.push(CreateClause { pattern });
            }
            _ => {} // kw_foreach, kw_in, "|"
        }
    }

    Ok(ForeachClause {
        variable: variable
            .ok_or_else(|| ParseError::SemanticError("FOREACH missing variable".to_string()))?,
        expression: expression
            .ok_or_else(|| ParseError::SemanticError("FOREACH missing expression".to_string()))?,
        set_clauses,
        create_clauses,
    })
}

// ============================================================
// CALL subquery
// call_subquery = { kw_call ~ "{" ~ regular_query ~ "}" }
// ============================================================

fn parse_call_subquery(pair: pest::iterators::Pair<Rule>, query: &mut Query) -> ParseResult<()> {
    for inner in pair.into_inner() {
        if inner.as_rule() == Rule::regular_query {
            let mut sub_query = Query::new();
            parse_regular_query(inner, &mut sub_query)?;
            query.call_subquery = Some(Box::new(sub_query));
        }
    }
    Ok(())
}

// ============================================================
// Standalone CALL
// standalone_call = ${ kw_call ~ sp ~ (explicit_procedure_invocation | implicit_procedure_invocation) ~ standalone_call_yield? }
// ============================================================

fn parse_standalone_call(pair: pest::iterators::Pair<Rule>, query: &mut Query) -> ParseResult<()> {
    let mut procedure_name = String::new();
    let mut arguments = Vec::new();
    let mut yield_items = Vec::new();

    for inner in pair.into_inner() {
        match inner.as_rule() {
            Rule::explicit_procedure_invocation => {
                for pi in inner.into_inner() {
                    match pi.as_rule() {
                        Rule::procedure_name => {
                            procedure_name = pi.as_str().to_string();
                        }
                        Rule::expression => {
                            arguments.push(parse_expression(pi)?);
                        }
                        _ => {}
                    }
                }
            }
            Rule::implicit_procedure_invocation => {
                for pi in inner.into_inner() {
                    if pi.as_rule() == Rule::procedure_name {
                        procedure_name = pi.as_str().to_string();
                    }
                }
            }
            Rule::standalone_call_yield => {
                // standalone_call_yield = ${ kw_yield ~ sp ~ ("*" | yield_items) }
                for sy in inner.into_inner() {
                    if sy.as_rule() == Rule::yield_items {
                        yield_items = parse_yield_items(sy)?;
                    }
                }
            }
            _ => {} // kw_call, sp
        }
    }

    query.call_clause = Some(CallClause {
        procedure_name,
        arguments,
        yield_items,
    });
    Ok(())
}

// ============================================================
// In-query CALL
// in_query_call = ${ kw_call ~ sp ~ explicit_procedure_invocation ~ in_query_call_yield? }
// ============================================================

fn parse_in_query_call(pair: pest::iterators::Pair<Rule>) -> ParseResult<CallClause> {
    let mut procedure_name = String::new();
    let mut arguments = Vec::new();
    let mut yield_items = Vec::new();

    for inner in pair.into_inner() {
        match inner.as_rule() {
            Rule::explicit_procedure_invocation => {
                for pi in inner.into_inner() {
                    match pi.as_rule() {
                        Rule::procedure_name => {
                            procedure_name = pi.as_str().to_string();
                        }
                        Rule::expression => {
                            arguments.push(parse_expression(pi)?);
                        }
                        _ => {}
                    }
                }
            }
            Rule::in_query_call_yield => {
                // in_query_call_yield = ${ kw_yield ~ sp ~ yield_items }
                for iy in inner.into_inner() {
                    if iy.as_rule() == Rule::yield_items {
                        yield_items = parse_yield_items(iy)?;
                    }
                }
            }
            _ => {} // kw_call, sp
        }
    }

    Ok(CallClause {
        procedure_name,
        arguments,
        yield_items,
    })
}

fn parse_yield_items(pair: pest::iterators::Pair<Rule>) -> ParseResult<Vec<YieldItem>> {
    let mut items = Vec::new();
    for inner in pair.into_inner() {
        if inner.as_rule() == Rule::yield_item {
            items.push(parse_yield_item(inner)?);
        }
    }
    Ok(items)
}

fn parse_yield_item(pair: pest::iterators::Pair<Rule>) -> ParseResult<YieldItem> {
    // yield_item = { (procedure_result_field ~ kw_as)? ~ variable }
    let mut name = String::new();
    let mut alias = None;
    let mut has_result_field = false;

    for inner in pair.into_inner() {
        match inner.as_rule() {
            Rule::procedure_result_field => {
                name = inner.as_str().to_string();
                has_result_field = true;
            }
            Rule::variable => {
                if has_result_field {
                    alias = Some(inner.as_str().to_string());
                } else {
                    name = inner.as_str().to_string();
                }
            }
            _ => {} // kw_as
        }
    }

    Ok(YieldItem { name, alias })
}

// ============================================================
// RETURN clause
// return_clause = ${ kw_return ~ sp ~ projection_body }
// Returns (ReturnClause, Option<OrderByClause>, Option<skip>, Option<limit>)
// ============================================================

fn parse_return_clause(
    pair: pest::iterators::Pair<Rule>,
) -> ParseResult<(
    ReturnClause,
    Option<OrderByClause>,
    Option<usize>,
    Option<usize>,
)> {
    let mut distinct = false;
    let mut star = false;
    let mut items = Vec::new();
    let mut order_by = None;
    let mut skip = None;
    let mut limit = None;

    for inner in pair.into_inner() {
        if inner.as_rule() == Rule::projection_body {
            parse_projection_body(
                inner,
                &mut distinct,
                &mut star,
                &mut items,
                &mut order_by,
                &mut skip,
                &mut limit,
            )?;
        }
    }

    Ok((
        ReturnClause {
            items,
            distinct,
            star,
        },
        order_by,
        skip,
        limit,
    ))
}

// ============================================================
// WITH clause
// with_clause = ${ kw_with ~ sp ~ projection_body ~ (sp? ~ where_clause)? }
// ============================================================

fn parse_with_clause(pair: pest::iterators::Pair<Rule>) -> ParseResult<WithClause> {
    let mut distinct = false;
    let mut star = false;
    let mut items = Vec::new();
    let mut order_by = None;
    let mut skip = None;
    let mut limit = None;
    let mut where_clause = None;

    for inner in pair.into_inner() {
        match inner.as_rule() {
            Rule::projection_body => {
                parse_projection_body(
                    inner,
                    &mut distinct,
                    &mut star,
                    &mut items,
                    &mut order_by,
                    &mut skip,
                    &mut limit,
                )?;
            }
            Rule::where_clause => {
                where_clause = Some(parse_where_clause(inner)?);
            }
            _ => {} // kw_with, sp
        }
    }

    // If star is set, add no items (star is handled separately in the planner)
    Ok(WithClause {
        items,
        distinct,
        where_clause,
        order_by,
        skip,
        limit,
    })
}

// ============================================================
// Projection body
// projection_body = { (projection_body_distinct | projection_items) ~ order? ~ skip_clause? ~ limit_clause? }
// ============================================================

fn parse_projection_body(
    pair: pest::iterators::Pair<Rule>,
    distinct: &mut bool,
    star: &mut bool,
    items: &mut Vec<ReturnItem>,
    order_by: &mut Option<OrderByClause>,
    skip: &mut Option<usize>,
    limit: &mut Option<usize>,
) -> ParseResult<()> {
    for inner in pair.into_inner() {
        match inner.as_rule() {
            Rule::projection_body_distinct => {
                // projection_body_distinct = { kw_distinct ~ projection_items }
                *distinct = true;
                for d_inner in inner.into_inner() {
                    if d_inner.as_rule() == Rule::projection_items {
                        parse_projection_items(d_inner, star, items)?;
                    }
                }
            }
            Rule::projection_items => {
                parse_projection_items(inner, star, items)?;
            }
            Rule::order => {
                *order_by = Some(parse_order_clause(inner)?);
            }
            Rule::skip_clause => {
                // skip_clause = ${ kw_skip ~ sp ~ expression }
                for si in inner.into_inner() {
                    if si.as_rule() == Rule::expression {
                        // Try to evaluate as integer literal
                        let expr = parse_expression(si)?;
                        if let Expression::Literal(PropertyValue::Integer(n)) = &expr {
                            *skip = Some(*n as usize);
                        } else {
                            // Expression-based skip — try parsing as_str
                            *skip = None; // Unsupported for now
                        }
                    }
                }
            }
            Rule::limit_clause => {
                // limit_clause = ${ kw_limit ~ sp ~ expression }
                for li in inner.into_inner() {
                    if li.as_rule() == Rule::expression {
                        let expr = parse_expression(li)?;
                        if let Expression::Literal(PropertyValue::Integer(n)) = &expr {
                            *limit = Some(*n as usize);
                        } else {
                            *limit = None;
                        }
                    }
                }
            }
            _ => {}
        }
    }
    Ok(())
}

fn parse_projection_items(
    pair: pest::iterators::Pair<Rule>,
    star: &mut bool,
    items: &mut Vec<ReturnItem>,
) -> ParseResult<()> {
    // projection_items = { ("*" ~ ("," ~ projection_item)*) | (projection_item ~ ("," ~ projection_item)*) }
    let text = pair.as_str().trim();
    if text.starts_with('*') {
        *star = true;
    }

    for inner in pair.into_inner() {
        if inner.as_rule() == Rule::projection_item {
            items.push(parse_projection_item(inner)?);
        }
    }
    Ok(())
}

fn parse_projection_item(pair: pest::iterators::Pair<Rule>) -> ParseResult<ReturnItem> {
    // projection_item = { (expression ~ kw_as ~ variable) | expression }
    let mut expression = None;
    let mut alias = None;
    let mut has_as = false;

    for inner in pair.into_inner() {
        match inner.as_rule() {
            Rule::expression => {
                if expression.is_none() {
                    expression = Some(parse_expression(inner)?);
                }
            }
            Rule::kw_as => {
                has_as = true;
            }
            Rule::variable => {
                if has_as {
                    alias = Some(inner.as_str().to_string());
                }
            }
            _ => {}
        }
    }

    Ok(ReturnItem {
        expression: expression
            .ok_or_else(|| ParseError::SemanticError("Missing expression in RETURN".to_string()))?,
        alias,
    })
}

// ============================================================
// ORDER BY clause
// order = ${ kw_order ~ sp ~ kw_by ~ sp ~ sort_item ~ ("," ~ sort_item)* }
// ============================================================

fn parse_order_clause(pair: pest::iterators::Pair<Rule>) -> ParseResult<OrderByClause> {
    let mut items = Vec::new();

    for inner in pair.into_inner() {
        if inner.as_rule() == Rule::sort_item {
            items.push(parse_sort_item(inner)?);
        }
    }

    Ok(OrderByClause { items })
}

fn parse_sort_item(pair: pest::iterators::Pair<Rule>) -> ParseResult<OrderByItem> {
    let mut expression = None;
    let mut ascending = true;

    for inner in pair.into_inner() {
        match inner.as_rule() {
            Rule::expression => {
                expression = Some(parse_expression(inner)?);
            }
            Rule::sort_direction => {
                // sort_direction = { kw_ascending | kw_descending | kw_asc | kw_desc }
                let text = inner.as_str().to_uppercase();
                ascending = text.starts_with("ASC");
            }
            _ => {}
        }
    }

    Ok(OrderByItem {
        expression: expression.ok_or_else(|| {
            ParseError::SemanticError("Missing expression in ORDER BY".to_string())
        })?,
        ascending,
    })
}

// ============================================================
// WHERE clause
// where_clause = ${ kw_where ~ sp ~ expression }
// ============================================================

fn parse_where_clause(pair: pest::iterators::Pair<Rule>) -> ParseResult<WhereClause> {
    for inner in pair.into_inner() {
        if inner.as_rule() == Rule::expression {
            return Ok(WhereClause {
                predicate: parse_expression(inner)?,
            });
        }
    }
    Err(ParseError::SemanticError(
        "Invalid WHERE clause".to_string(),
    ))
}

// ============================================================
// Pattern
// pattern = { pattern_part ~ ("," ~ pattern_part)* }
// ============================================================

fn parse_pattern(pair: pest::iterators::Pair<Rule>) -> ParseResult<Pattern> {
    let mut paths = Vec::new();

    for inner in pair.into_inner() {
        if inner.as_rule() == Rule::pattern_part {
            paths.push(parse_pattern_part(inner)?);
        }
    }

    Ok(Pattern { paths })
}

// ============================================================
// Pattern Part
// pattern_part = { (variable ~ "=" ~ anonymous_pattern_part) | anonymous_pattern_part }
// anonymous_pattern_part = _{ pattern_element }
// ============================================================

fn parse_pattern_part(pair: pest::iterators::Pair<Rule>) -> ParseResult<PathPattern> {
    let mut path_variable = None;
    let mut path_pattern = None;

    for inner in pair.into_inner() {
        match inner.as_rule() {
            Rule::variable => {
                if path_variable.is_none() {
                    path_variable = Some(inner.as_str().to_string());
                }
            }
            Rule::pattern_element => {
                path_pattern = Some(parse_pattern_element(inner)?);
            }
            Rule::shortest_path_pattern => {
                path_pattern = Some(parse_shortest_path_pattern(inner)?);
            }
            _ => {}
        }
    }

    let mut pp = path_pattern.ok_or_else(|| {
        ParseError::SemanticError("Pattern part missing pattern element".to_string())
    })?;
    pp.path_variable = path_variable;
    Ok(pp)
}

fn parse_shortest_path_pattern(pair: pest::iterators::Pair<Rule>) -> ParseResult<PathPattern> {
    let text = pair.as_str();
    let path_type = if text.to_lowercase().starts_with("allshortestpaths") {
        PathType::AllShortest
    } else {
        PathType::Shortest
    };

    let mut pp = None;
    for inner in pair.into_inner() {
        if inner.as_rule() == Rule::pattern_element {
            pp = Some(parse_pattern_element(inner)?);
        }
    }

    let mut path = pp.ok_or_else(|| {
        ParseError::SemanticError("shortestPath() missing inner path".to_string())
    })?;
    path.path_type = path_type;
    Ok(path)
}

// ============================================================
// Pattern Element
// pattern_element = { (node_pattern ~ pattern_element_chain*) | ("(" ~ pattern_element ~ ")") }
// ============================================================

fn parse_pattern_element(pair: pest::iterators::Pair<Rule>) -> ParseResult<PathPattern> {
    let mut start = None;
    let mut segments = Vec::new();

    for inner in pair.into_inner() {
        match inner.as_rule() {
            Rule::node_pattern => {
                if start.is_none() {
                    start = Some(parse_node_pattern(inner)?);
                }
            }
            Rule::pattern_element_chain => {
                // pattern_element_chain = { relationship_pattern ~ node_pattern }
                let mut edge = None;
                let mut node = None;
                for chain_inner in inner.into_inner() {
                    match chain_inner.as_rule() {
                        Rule::relationship_pattern => {
                            edge = Some(parse_relationship_pattern(chain_inner)?);
                        }
                        Rule::node_pattern => {
                            node = Some(parse_node_pattern(chain_inner)?);
                        }
                        _ => {}
                    }
                }
                if let (Some(e), Some(n)) = (edge, node) {
                    segments.push(PathSegment { edge: e, node: n });
                }
            }
            Rule::pattern_element => {
                // Nested parenthesized pattern_element
                return parse_pattern_element(inner);
            }
            _ => {}
        }
    }

    Ok(PathPattern {
        path_variable: None,
        path_type: PathType::Normal,
        start: start.unwrap_or(NodePattern {
            variable: None,
            labels: vec![],
            properties: None,
        }),
        segments,
    })
}

// ============================================================
// Node Pattern
// node_pattern = { "(" ~ variable? ~ node_labels? ~ properties? ~ ")" }
// ============================================================

fn parse_node_pattern(pair: pest::iterators::Pair<Rule>) -> ParseResult<NodePattern> {
    let mut variable = None;
    let mut labels = Vec::new();
    let mut properties = None;

    for inner in pair.into_inner() {
        match inner.as_rule() {
            Rule::variable => {
                variable = Some(inner.as_str().to_string());
            }
            Rule::node_labels => {
                for nl in inner.into_inner() {
                    if nl.as_rule() == Rule::node_label {
                        // node_label = { ":" ~ label_name } where label_name is silent → schema_name
                        let label_text = nl.as_str();
                        let label_name = label_text.trim_start_matches(':').trim();
                        labels.push(Label::new(label_name));
                    }
                }
            }
            Rule::properties => {
                properties = Some(parse_properties(inner)?);
            }
            _ => {}
        }
    }

    Ok(NodePattern {
        variable,
        labels,
        properties,
    })
}

// ============================================================
// Relationship Pattern
// relationship_pattern = { 4 forms with arrows }
// ============================================================

fn parse_relationship_pattern(pair: pest::iterators::Pair<Rule>) -> ParseResult<EdgePattern> {
    // Determine direction from arrow heads
    let mut has_left_arrow = false;
    let mut has_right_arrow = false;
    let mut variable = None;
    let mut types = Vec::new();
    let mut length = None;
    let mut properties = None;

    for inner in pair.into_inner() {
        match inner.as_rule() {
            Rule::left_arrow_head => has_left_arrow = true,
            Rule::right_arrow_head => has_right_arrow = true,
            Rule::relationship_detail => {
                for detail in inner.into_inner() {
                    match detail.as_rule() {
                        Rule::variable => {
                            variable = Some(detail.as_str().to_string());
                        }
                        Rule::relationship_types => {
                            // relationship_types = { ":" ~ rel_type_name ~ ("|" ~ ":"? ~ rel_type_name)* }
                            // rel_type_name is silent → schema_name
                            // We need to extract the type names from the text
                            let types_text = detail.as_str();
                            // Remove leading ":"
                            let types_str = types_text.trim_start_matches(':');
                            for t in types_str.split('|') {
                                let t = t.trim().trim_start_matches(':').trim();
                                if !t.is_empty() {
                                    types.push(EdgeType::new(t));
                                }
                            }
                        }
                        Rule::range_literal => {
                            length = Some(parse_range_literal(detail)?);
                        }
                        Rule::properties => {
                            properties = Some(parse_properties(detail)?);
                        }
                        _ => {}
                    }
                }
            }
            Rule::dash => {} // Skip dashes
            _ => {}
        }
    }

    let direction = if has_left_arrow && has_right_arrow {
        Direction::Both
    } else if has_left_arrow {
        Direction::Incoming
    } else if has_right_arrow {
        Direction::Outgoing
    } else {
        Direction::Both
    };

    Ok(EdgePattern {
        variable,
        types,
        direction,
        length,
        properties,
    })
}

// ============================================================
// Range Literal (variable-length paths)
// range_literal = { "*" ~ integer_literal? ~ (".." ~ integer_literal?)? }
// ============================================================

fn parse_range_literal(pair: pest::iterators::Pair<Rule>) -> ParseResult<LengthPattern> {
    let text = pair.as_str();
    // Remove the leading *
    let range_str = text.trim_start_matches('*').trim();

    if range_str.is_empty() {
        // Just * — 1..unbounded
        return Ok(LengthPattern {
            min: Some(1),
            max: None,
        });
    }

    if range_str.contains("..") {
        let parts: Vec<&str> = range_str.split("..").collect();
        let min = if parts[0].trim().is_empty() {
            Some(1)
        } else {
            Some(parts[0].trim().parse().unwrap_or(1))
        };
        let max = if parts.len() > 1 && !parts[1].trim().is_empty() {
            Some(parts[1].trim().parse().unwrap_or(1))
        } else {
            None
        };
        Ok(LengthPattern { min, max })
    } else {
        // Exact number: *3
        let exact: usize = range_str.trim().parse().unwrap_or(1);
        Ok(LengthPattern {
            min: Some(exact),
            max: Some(exact),
        })
    }
}

// ============================================================
// Properties
// properties = { map_literal | parameter }
// ============================================================

fn parse_properties(
    pair: pest::iterators::Pair<Rule>,
) -> ParseResult<HashMap<String, PropertyValue>> {
    for inner in pair.into_inner() {
        match inner.as_rule() {
            Rule::map_literal => {
                return parse_map_literal_to_props(inner);
            }
            Rule::parameter => {
                // Property from parameter — return empty map for now
                return Ok(HashMap::new());
            }
            _ => {}
        }
    }
    Ok(HashMap::new())
}

/// Parse a map_literal into a HashMap<String, PropertyValue>
/// map_literal = { "{" ~ (property_key_name ~ ":" ~ expression ~ ("," ~ property_key_name ~ ":" ~ expression)*)? ~ "}" }
fn parse_map_literal_to_props(
    pair: pest::iterators::Pair<Rule>,
) -> ParseResult<HashMap<String, PropertyValue>> {
    let mut props = HashMap::new();

    let children: Vec<pest::iterators::Pair<Rule>> = pair.into_inner().collect();
    // Children alternate: property_key_name, expression, property_key_name, expression, ...
    let mut i = 0;
    while i < children.len() {
        if children[i].as_rule() == Rule::property_key_name {
            let key = children[i].as_str().to_string();
            if i + 1 < children.len() && children[i + 1].as_rule() == Rule::expression {
                let expr = parse_expression(children[i + 1].clone())?;
                let value = expression_to_property_value(&expr);
                props.insert(key, value);
                i += 2;
            } else {
                i += 1;
            }
        } else {
            i += 1;
        }
    }

    Ok(props)
}

// ============================================================
// Expression parsing
// ============================================================

fn parse_expression(pair: pest::iterators::Pair<Rule>) -> ParseResult<Expression> {
    // expression = { or_expression }
    for inner in pair.into_inner() {
        if inner.as_rule() == Rule::or_expression {
            return parse_or_expression(inner);
        }
    }
    // Shouldn't reach here — expression always wraps or_expression
    Err(ParseError::SemanticError("Empty expression".to_string()))
}

// or_expression = { xor_expression ~ (kw_or ~ xor_expression)* }
fn parse_or_expression(pair: pest::iterators::Pair<Rule>) -> ParseResult<Expression> {
    let children: Vec<pest::iterators::Pair<Rule>> = pair
        .into_inner()
        .filter(|p| p.as_rule() == Rule::xor_expression)
        .collect();

    if children.is_empty() {
        return Err(ParseError::SemanticError("Empty OR expression".to_string()));
    }

    let mut iter = children.into_iter();
    let mut result = parse_xor_expression(iter.next().unwrap())?;

    for child in iter {
        let right = parse_xor_expression(child)?;
        result = Expression::Binary {
            left: Box::new(result),
            op: BinaryOp::Or,
            right: Box::new(right),
        };
    }

    Ok(result)
}

// xor_expression = { and_expression ~ (kw_xor ~ and_expression)* }
fn parse_xor_expression(pair: pest::iterators::Pair<Rule>) -> ParseResult<Expression> {
    let children: Vec<pest::iterators::Pair<Rule>> = pair
        .into_inner()
        .filter(|p| p.as_rule() == Rule::and_expression)
        .collect();

    if children.is_empty() {
        return Err(ParseError::SemanticError(
            "Empty XOR expression".to_string(),
        ));
    }

    let mut iter = children.into_iter();
    let mut result = parse_and_expression(iter.next().unwrap())?;

    for child in iter {
        let right = parse_and_expression(child)?;
        result = Expression::Binary {
            left: Box::new(result),
            op: BinaryOp::Xor,
            right: Box::new(right),
        };
    }

    Ok(result)
}

// and_expression = { not_expression ~ (kw_and ~ not_expression)* }
fn parse_and_expression(pair: pest::iterators::Pair<Rule>) -> ParseResult<Expression> {
    let children: Vec<pest::iterators::Pair<Rule>> = pair
        .into_inner()
        .filter(|p| p.as_rule() == Rule::not_expression)
        .collect();

    if children.is_empty() {
        return Err(ParseError::SemanticError(
            "Empty AND expression".to_string(),
        ));
    }

    let mut iter = children.into_iter();
    let mut result = parse_not_expression(iter.next().unwrap())?;

    for child in iter {
        let right = parse_not_expression(child)?;
        result = Expression::Binary {
            left: Box::new(result),
            op: BinaryOp::And,
            right: Box::new(right),
        };
    }

    Ok(result)
}

// not_expression = { kw_not* ~ comparison_expression }
fn parse_not_expression(pair: pest::iterators::Pair<Rule>) -> ParseResult<Expression> {
    let mut not_count = 0;

    let mut comparison = None;
    for inner in pair.into_inner() {
        match inner.as_rule() {
            Rule::kw_not => not_count += 1,
            Rule::comparison_expression => {
                comparison = Some(parse_comparison_expression(inner)?);
            }
            _ => {}
        }
    }

    let mut result = comparison
        .ok_or_else(|| ParseError::SemanticError("NOT missing expression".to_string()))?;

    for _ in 0..not_count {
        result = Expression::Unary {
            op: UnaryOp::Not,
            expr: Box::new(result),
        };
    }

    Ok(result)
}

// comparison_expression = { string_list_null_predicate_expression ~ partial_comparison_expression* }
fn parse_comparison_expression(pair: pest::iterators::Pair<Rule>) -> ParseResult<Expression> {
    let mut children: Vec<pest::iterators::Pair<Rule>> = pair.into_inner().collect();

    if children.is_empty() {
        return Err(ParseError::SemanticError(
            "Empty comparison expression".to_string(),
        ));
    }

    let first = children.remove(0);
    let mut result = parse_string_list_null_predicate_expression(first)?;

    for child in children {
        if child.as_rule() == Rule::partial_comparison_expression {
            // partial_comparison_expression = { ("<=" | ">=" | "<>" | "=" | "<" | ">") ~ string_list_null_predicate_expression }
            let text = child.as_str().trim();
            let op = if text.starts_with("<=") {
                BinaryOp::Le
            } else if text.starts_with(">=") {
                BinaryOp::Ge
            } else if text.starts_with("<>") {
                BinaryOp::Ne
            } else if text.starts_with("=~") {
                BinaryOp::RegexMatch
            } else if text.starts_with('=') {
                BinaryOp::Eq
            } else if text.starts_with('<') {
                BinaryOp::Lt
            } else if text.starts_with('>') {
                BinaryOp::Gt
            } else {
                BinaryOp::Eq
            };

            for inner in child.into_inner() {
                if inner.as_rule() == Rule::string_list_null_predicate_expression {
                    let right = parse_string_list_null_predicate_expression(inner)?;

                    // Chained comparison rewriting: `1 < n.num < 3` → `1 < n.num AND n.num < 3`
                    let is_comparison = matches!(
                        op,
                        BinaryOp::Lt | BinaryOp::Le | BinaryOp::Gt | BinaryOp::Ge
                    );
                    if is_comparison {
                        if let Expression::Binary {
                            left: _,
                            op: ref inner_op,
                            right: ref inner_right,
                        } = result
                        {
                            if matches!(
                                inner_op,
                                BinaryOp::Lt | BinaryOp::Le | BinaryOp::Gt | BinaryOp::Ge
                            ) {
                                let middle = inner_right.clone();
                                result = Expression::Binary {
                                    left: Box::new(result),
                                    op: BinaryOp::And,
                                    right: Box::new(Expression::Binary {
                                        left: middle,
                                        op: op.clone(),
                                        right: Box::new(right),
                                    }),
                                };
                                continue;
                            }
                        }
                    }

                    result = Expression::Binary {
                        left: Box::new(result),
                        op: op.clone(),
                        right: Box::new(right),
                    };
                }
            }
        }
    }

    Ok(result)
}

// string_list_null_predicate_expression = { add_or_subtract_expression ~ (string_predicate | list_predicate | null_predicate)* }
fn parse_string_list_null_predicate_expression(
    pair: pest::iterators::Pair<Rule>,
) -> ParseResult<Expression> {
    let mut children: Vec<pest::iterators::Pair<Rule>> = pair.into_inner().collect();

    if children.is_empty() {
        return Err(ParseError::SemanticError(
            "Empty predicate expression".to_string(),
        ));
    }

    let first = children.remove(0);
    let mut result = parse_add_or_subtract_expression(first)?;

    for child in children {
        match child.as_rule() {
            Rule::string_predicate_expression => {
                // string_predicate = { ((kw_starts ~ kw_with) | (kw_ends ~ kw_with) | kw_contains) ~ add_or_subtract_expression }
                let text = child.as_str().to_uppercase();
                let op = if text.starts_with("STARTS") {
                    BinaryOp::StartsWith
                } else if text.starts_with("ENDS") {
                    BinaryOp::EndsWith
                } else if text.starts_with("CONTAINS") {
                    BinaryOp::Contains
                } else if text.contains("=~") {
                    BinaryOp::RegexMatch
                } else {
                    BinaryOp::Contains // fallback
                };

                for inner in child.into_inner() {
                    if inner.as_rule() == Rule::add_or_subtract_expression {
                        let right = parse_add_or_subtract_expression(inner)?;
                        result = Expression::Binary {
                            left: Box::new(result),
                            op,
                            right: Box::new(right),
                        };
                        break;
                    }
                }
            }
            Rule::list_predicate_expression => {
                // list_predicate = { kw_in ~ add_or_subtract_expression }
                for inner in child.into_inner() {
                    if inner.as_rule() == Rule::add_or_subtract_expression {
                        let right = parse_add_or_subtract_expression(inner)?;
                        result = Expression::Binary {
                            left: Box::new(result),
                            op: BinaryOp::In,
                            right: Box::new(right),
                        };
                        break;
                    }
                }
            }
            Rule::null_predicate_expression => {
                // null_predicate = ${ (kw_is ~ sp ~ kw_not ~ sp ~ kw_null) | (kw_is ~ sp ~ kw_null) }
                let text = child.as_str().to_uppercase();
                let op = if text.contains("NOT") {
                    UnaryOp::IsNotNull
                } else {
                    UnaryOp::IsNull
                };
                result = Expression::Unary {
                    op,
                    expr: Box::new(result),
                };
            }
            _ => {}
        }
    }

    Ok(result)
}

// add_or_subtract_expression = { multiply_divide_modulo_expression ~ (("+" | "-") ~ multiply_divide_modulo_expression)* }
fn parse_add_or_subtract_expression(pair: pest::iterators::Pair<Rule>) -> ParseResult<Expression> {
    let text = pair.as_str();
    let children: Vec<pest::iterators::Pair<Rule>> = pair.into_inner().collect();

    if children.is_empty() {
        return Err(ParseError::SemanticError(
            "Empty add/subtract expression".to_string(),
        ));
    }

    // The children are multiply_divide_modulo_expressions.
    // Operators (+/-) are not separate tokens — they're part of the text between children.
    // We need to extract operators from the text between matched children.
    if children.len() == 1 {
        return parse_multiply_divide_modulo_expression(children.into_iter().next().unwrap());
    }

    // Multiple children — extract operators from text
    let mut result = parse_multiply_divide_modulo_expression(children[0].clone())?;
    for i in 1..children.len() {
        // Find the operator between children[i-1] and children[i]
        let prev_end = children[i - 1].as_span().end();
        let curr_start = children[i].as_span().start();
        let between = &text
            [prev_end - children[0].as_span().start()..curr_start - children[0].as_span().start()];
        let op = if between.contains('+') {
            BinaryOp::Add
        } else {
            BinaryOp::Sub
        };
        let right = parse_multiply_divide_modulo_expression(children[i].clone())?;
        result = Expression::Binary {
            left: Box::new(result),
            op,
            right: Box::new(right),
        };
    }

    Ok(result)
}

// multiply_divide_modulo_expression = { power_of_expression ~ (("*" | "/" | "%") ~ power_of_expression)* }
fn parse_multiply_divide_modulo_expression(
    pair: pest::iterators::Pair<Rule>,
) -> ParseResult<Expression> {
    let text = pair.as_str();
    let children: Vec<pest::iterators::Pair<Rule>> = pair.into_inner().collect();

    if children.is_empty() {
        return Err(ParseError::SemanticError(
            "Empty multiply/divide expression".to_string(),
        ));
    }

    if children.len() == 1 {
        return parse_power_of_expression(children.into_iter().next().unwrap());
    }

    let mut result = parse_power_of_expression(children[0].clone())?;
    for i in 1..children.len() {
        let prev_end = children[i - 1].as_span().end();
        let curr_start = children[i].as_span().start();
        let between = &text
            [prev_end - children[0].as_span().start()..curr_start - children[0].as_span().start()];
        let op = if between.contains('%') {
            BinaryOp::Mod
        } else if between.contains('/') {
            BinaryOp::Div
        } else {
            BinaryOp::Mul
        };
        let right = parse_power_of_expression(children[i].clone())?;
        result = Expression::Binary {
            left: Box::new(result),
            op,
            right: Box::new(right),
        };
    }

    Ok(result)
}

// power_of_expression = { unary_add_or_subtract_expression ~ ("^" ~ unary_add_or_subtract_expression)* }
fn parse_power_of_expression(pair: pest::iterators::Pair<Rule>) -> ParseResult<Expression> {
    let children: Vec<pest::iterators::Pair<Rule>> = pair.into_inner().collect();

    if children.is_empty() {
        return Err(ParseError::SemanticError(
            "Empty power expression".to_string(),
        ));
    }

    if children.len() == 1 {
        return parse_unary_add_or_subtract_expression(children.into_iter().next().unwrap());
    }

    // Right-associative power: a ^ b ^ c = a ^ (b ^ c)
    let mut exprs: Vec<Expression> = Vec::new();
    for child in children {
        if child.as_rule() == Rule::unary_add_or_subtract_expression {
            exprs.push(parse_unary_add_or_subtract_expression(child)?);
        }
    }

    let mut result = exprs.pop().unwrap();
    while let Some(left) = exprs.pop() {
        result = Expression::Binary {
            left: Box::new(left),
            op: BinaryOp::Pow,
            right: Box::new(result),
        };
    }

    Ok(result)
}

// unary_add_or_subtract_expression = { (("+" | "-") ~ non_arithmetic_operator_expression) | non_arithmetic_operator_expression }
fn parse_unary_add_or_subtract_expression(
    pair: pest::iterators::Pair<Rule>,
) -> ParseResult<Expression> {
    let text = pair.as_str().trim();

    let children: Vec<pest::iterators::Pair<Rule>> = pair.into_inner().collect();

    if children.is_empty() {
        return Err(ParseError::SemanticError(
            "Empty unary expression".to_string(),
        ));
    }

    // The grammar: (("+" | "-") ~ non_arithmetic_operator_expression) | non_arithmetic_operator_expression
    // In both cases, there's exactly 1 child: non_arithmetic_operator_expression
    // The "+" / "-" tokens don't produce pairs. We detect unary ops via the text.
    let expr = parse_non_arithmetic_operator_expression(children.into_iter().next().unwrap())?;

    if text.starts_with('-') {
        // Optimize: directly negate literals
        match &expr {
            Expression::Literal(PropertyValue::Integer(i)) => {
                // Check for i64::MIN case: the positive form overflowed to 0
                if *i == 0 {
                    let digits = text.trim_start_matches('-').trim();
                    if digits == "9223372036854775808"
                        || digits.eq_ignore_ascii_case("0x8000000000000000")
                        || digits.eq_ignore_ascii_case("0o1000000000000000000000")
                    {
                        return Ok(Expression::Literal(PropertyValue::Integer(i64::MIN)));
                    }
                }
                if let Some(neg) = i.checked_neg() {
                    return Ok(Expression::Literal(PropertyValue::Integer(neg)));
                }
                return Ok(Expression::Literal(PropertyValue::Integer(i64::MIN)));
            }
            Expression::Literal(PropertyValue::Float(f)) => {
                return Ok(Expression::Literal(PropertyValue::Float(-f)));
            }
            _ => {}
        }
        return Ok(Expression::Unary {
            op: UnaryOp::Minus,
            expr: Box::new(expr),
        });
    }
    // Leading + is a no-op, or no unary op at all
    Ok(expr)
}

// non_arithmetic_operator_expression = { atom ~ (list_operator_expression | property_lookup)* ~ node_labels? }
fn parse_non_arithmetic_operator_expression(
    pair: pest::iterators::Pair<Rule>,
) -> ParseResult<Expression> {
    let mut atom_expr = None;
    let mut label_check = None;

    for inner in pair.into_inner() {
        match inner.as_rule() {
            Rule::atom => {
                if atom_expr.is_none() {
                    atom_expr = Some(parse_atom(inner)?);
                }
            }
            Rule::property_lookup => {
                // property_lookup = { "." ~ property_key_name }
                if let Some(ref base) = atom_expr {
                    let mut prop_name = String::new();
                    for plk in inner.into_inner() {
                        if plk.as_rule() == Rule::property_key_name {
                            prop_name = plk.as_str().to_string();
                        }
                    }
                    // Convert atom + property_lookup into Property access
                    if let Expression::Variable(var) = base {
                        atom_expr = Some(Expression::Property {
                            variable: var.clone(),
                            property: prop_name,
                        });
                    } else {
                        // For more complex property access, just use the string repr
                        atom_expr = Some(Expression::Property {
                            variable: base.to_string_repr(),
                            property: prop_name,
                        });
                    }
                }
            }
            Rule::list_operator_expression => {
                // list_operator_expression = { ("[" ~ expression? ~ ".." ~ expression? ~ "]") | ("[" ~ expression ~ "]") }
                if let Some(ref base) = atom_expr {
                    let list_text = inner.as_str();
                    if list_text.contains("..") {
                        // Slice
                        let mut start = None;
                        let mut end = None;
                        let exprs: Vec<pest::iterators::Pair<Rule>> = inner
                            .into_inner()
                            .filter(|p| p.as_rule() == Rule::expression)
                            .collect();

                        if list_text.starts_with("[..") {
                            // [..end]
                            if !exprs.is_empty() {
                                end = Some(Box::new(parse_expression(exprs[0].clone())?));
                            }
                        } else if list_text.ends_with("..]") {
                            // [start..]
                            if !exprs.is_empty() {
                                start = Some(Box::new(parse_expression(exprs[0].clone())?));
                            }
                        } else {
                            // [start..end]
                            if exprs.len() >= 2 {
                                start = Some(Box::new(parse_expression(exprs[0].clone())?));
                                end = Some(Box::new(parse_expression(exprs[1].clone())?));
                            } else if exprs.len() == 1 {
                                start = Some(Box::new(parse_expression(exprs[0].clone())?));
                            }
                        }

                        atom_expr = Some(Expression::ListSlice {
                            expr: Box::new(base.clone()),
                            start,
                            end,
                        });
                    } else {
                        // Index
                        for idx_inner in inner.into_inner() {
                            if idx_inner.as_rule() == Rule::expression {
                                let index = parse_expression(idx_inner)?;
                                atom_expr = Some(Expression::Index {
                                    expr: Box::new(base.clone()),
                                    index: Box::new(index),
                                });
                                break;
                            }
                        }
                    }
                }
            }
            Rule::node_labels => {
                // Label check in WHERE: n:Person
                if let Some(ref base) = atom_expr {
                    for nl in inner.into_inner() {
                        if nl.as_rule() == Rule::node_label {
                            let label_text = nl.as_str();
                            let label_name = label_text.trim_start_matches(':').trim();
                            label_check = Some(Expression::Function {
                                name: "$hasLabel".to_string(),
                                args: vec![
                                    base.clone(),
                                    Expression::Literal(PropertyValue::String(
                                        label_name.to_string(),
                                    )),
                                ],
                                distinct: false,
                            });
                        }
                    }
                }
            }
            _ => {}
        }
    }

    if let Some(lc) = label_check {
        return Ok(lc);
    }

    atom_expr
        .ok_or_else(|| ParseError::SemanticError("Empty non-arithmetic expression".to_string()))
}

// ============================================================
// Atoms
// ============================================================

fn parse_atom(pair: pest::iterators::Pair<Rule>) -> ParseResult<Expression> {
    for inner in pair.into_inner() {
        match inner.as_rule() {
            Rule::literal => return parse_literal(inner),
            Rule::parameter => {
                let name = inner.as_str()[1..].to_string();
                return Ok(Expression::Parameter(name));
            }
            Rule::case_expression => return parse_case_expression(inner),
            Rule::count_star => {
                return Ok(Expression::Function {
                    name: "count".to_string(),
                    args: vec![],
                    distinct: false,
                });
            }
            Rule::reduce_expression => return parse_reduce_expression(inner),
            Rule::list_comprehension => return parse_list_comprehension(inner),
            Rule::pattern_comprehension => return parse_pattern_comprehension(inner),
            Rule::quantifier => return parse_quantifier(inner),
            Rule::pattern_predicate => return parse_pattern_predicate(inner),
            Rule::parenthesized_expression => {
                for pe in inner.into_inner() {
                    if pe.as_rule() == Rule::expression {
                        return parse_expression(pe);
                    }
                }
            }
            Rule::function_invocation => return parse_function_invocation(inner),
            Rule::existential_subquery => return parse_existential_subquery(inner),
            Rule::variable => {
                return Ok(Expression::Variable(inner.as_str().to_string()));
            }
            _ => {}
        }
    }
    Err(ParseError::SemanticError(
        "Invalid atom expression".to_string(),
    ))
}

// ============================================================
// Literals
// ============================================================

fn parse_literal(pair: pest::iterators::Pair<Rule>) -> ParseResult<Expression> {
    for inner in pair.into_inner() {
        match inner.as_rule() {
            Rule::boolean_literal => {
                let val = inner.as_str().eq_ignore_ascii_case("true");
                return Ok(Expression::Literal(PropertyValue::Boolean(val)));
            }
            Rule::null_literal => {
                return Ok(Expression::Literal(PropertyValue::Null));
            }
            Rule::number_literal => {
                return parse_number_literal(inner);
            }
            Rule::string_literal => {
                return Ok(Expression::Literal(PropertyValue::String(
                    parse_string_literal(inner),
                )));
            }
            Rule::list_literal => {
                return parse_list_literal(inner);
            }
            Rule::map_literal => {
                return parse_map_literal_expr(inner);
            }
            _ => {}
        }
    }
    Ok(Expression::Literal(PropertyValue::Null))
}

fn parse_number_literal(pair: pest::iterators::Pair<Rule>) -> ParseResult<Expression> {
    for inner in pair.into_inner() {
        match inner.as_rule() {
            Rule::double_literal => {
                let val: f64 = inner.as_str().parse().unwrap_or(0.0);
                if val.is_infinite() {
                    return Err(ParseError::SemanticError(
                        "Floating point number is too large".to_string(),
                    ));
                }
                return Ok(Expression::Literal(PropertyValue::Float(val)));
            }
            Rule::integer_literal => match parse_integer_literal_checked(inner.as_str()) {
                Ok(val) => return Ok(Expression::Literal(PropertyValue::Integer(val))),
                Err(e) => return Err(ParseError::SemanticError(e)),
            },
            _ => {}
        }
    }
    Ok(Expression::Literal(PropertyValue::Integer(0)))
}

fn parse_string_literal(pair: pest::iterators::Pair<Rule>) -> String {
    let s = pair.as_str();
    // Remove outer quotes
    if s.len() < 2 {
        return String::new();
    }
    let inner = &s[1..s.len() - 1];
    // Process escape sequences
    unescape_string(inner)
}

fn unescape_string(s: &str) -> String {
    let mut result = String::with_capacity(s.len());
    let mut chars = s.chars();
    while let Some(c) = chars.next() {
        if c == '\\' {
            match chars.next() {
                Some('\\') => result.push('\\'),
                Some('\'') => result.push('\''),
                Some('"') => result.push('"'),
                Some('n') => result.push('\n'),
                Some('r') => result.push('\r'),
                Some('t') => result.push('\t'),
                Some('b') => result.push('\u{0008}'),
                Some('f') => result.push('\u{000C}'),
                Some('u') => {
                    let hex: String = chars.by_ref().take(4).collect();
                    if let Ok(code) = u32::from_str_radix(&hex, 16) {
                        if let Some(ch) = char::from_u32(code) {
                            result.push(ch);
                        }
                    }
                }
                Some('U') => {
                    let hex: String = chars.by_ref().take(8).collect();
                    if let Ok(code) = u32::from_str_radix(&hex, 16) {
                        if let Some(ch) = char::from_u32(code) {
                            result.push(ch);
                        }
                    }
                }
                Some(other) => {
                    result.push('\\');
                    result.push(other);
                }
                None => result.push('\\'),
            }
        } else {
            result.push(c);
        }
    }
    result
}

fn parse_list_literal(pair: pest::iterators::Pair<Rule>) -> ParseResult<Expression> {
    let mut items = Vec::new();
    let mut all_numeric = true;
    let mut float_vals = Vec::new();

    for inner in pair.into_inner() {
        if inner.as_rule() == Rule::expression {
            let expr = parse_expression(inner)?;
            let pv = expression_to_property_value(&expr);
            match &pv {
                PropertyValue::Float(f) => float_vals.push(*f as f32),
                PropertyValue::Integer(i) => float_vals.push(*i as f32),
                _ => all_numeric = false,
            }
            items.push(pv);
        }
    }

    // If all numeric, produce a Vector for use in properties
    if !float_vals.is_empty() && all_numeric {
        Ok(Expression::Literal(PropertyValue::Vector(float_vals)))
    } else {
        Ok(Expression::Literal(PropertyValue::Array(items)))
    }
}

fn parse_map_literal_expr(pair: pest::iterators::Pair<Rule>) -> ParseResult<Expression> {
    let props = parse_map_literal_to_props(pair)?;
    Ok(Expression::Literal(PropertyValue::Map(props)))
}

// ============================================================
// CASE expression
// case_expression = { kw_case ~ (expression ~ case_alternative+ | case_alternative+) ~ (kw_else ~ expression)? ~ kw_end }
// ============================================================

fn parse_case_expression(pair: pest::iterators::Pair<Rule>) -> ParseResult<Expression> {
    let mut operand = None;
    let mut when_clauses = Vec::new();
    let mut else_result = None;
    let mut saw_alternative = false;

    for inner in pair.into_inner() {
        match inner.as_rule() {
            Rule::expression => {
                if !saw_alternative && operand.is_none() {
                    // Expression before any WHEN → simple CASE operand
                    operand = Some(Box::new(parse_expression(inner)?));
                } else {
                    // Expression after ELSE
                    else_result = Some(Box::new(parse_expression(inner)?));
                }
            }
            Rule::case_alternative => {
                saw_alternative = true;
                // case_alternative = { kw_when ~ expression ~ kw_then ~ expression }
                let mut when_expr = None;
                let mut then_expr = None;
                for ca in inner.into_inner() {
                    if ca.as_rule() == Rule::expression {
                        if when_expr.is_none() {
                            when_expr = Some(parse_expression(ca)?);
                        } else {
                            then_expr = Some(parse_expression(ca)?);
                        }
                    }
                }
                if let (Some(w), Some(t)) = (when_expr, then_expr) {
                    when_clauses.push((w, t));
                }
            }
            _ => {} // kw_case, kw_else, kw_end
        }
    }

    Ok(Expression::Case {
        operand,
        when_clauses,
        else_result,
    })
}

// ============================================================
// Function invocation
// function_invocation = { function_name ~ "(" ~ function_distinct? ~ (expression ~ ("," ~ expression)*)? ~ ")" }
// ============================================================

fn parse_function_invocation(pair: pest::iterators::Pair<Rule>) -> ParseResult<Expression> {
    let mut name = String::new();
    let mut args = Vec::new();
    let mut distinct = false;

    for inner in pair.into_inner() {
        match inner.as_rule() {
            Rule::function_name => {
                name = inner.as_str().to_string();
            }
            Rule::function_distinct => {
                distinct = true;
            }
            Rule::expression => {
                args.push(parse_expression(inner)?);
            }
            _ => {}
        }
    }

    Ok(Expression::Function {
        name,
        args,
        distinct,
    })
}

/// Parse reduce(acc = init, x IN list | expr) from the dedicated grammar rule.
/// reduce_expression = { kw_reduce ~ "(" ~ variable ~ "=" ~ expression ~ "," ~ id_in_coll ~ "|" ~ expression ~ ")" }
/// id_in_coll = { variable ~ kw_in ~ expression }
fn parse_reduce_expression(pair: pest::iterators::Pair<Rule>) -> ParseResult<Expression> {
    let mut accumulator = None;
    let mut init_expr = None;
    let mut iter_variable = None;
    let mut list_expr = None;
    let mut body_expr = None;

    for inner in pair.into_inner() {
        match inner.as_rule() {
            Rule::variable => {
                if accumulator.is_none() {
                    accumulator = Some(inner.as_str().to_string());
                }
            }
            Rule::expression => {
                if init_expr.is_none() {
                    init_expr = Some(parse_expression(inner)?);
                } else {
                    body_expr = Some(parse_expression(inner)?);
                }
            }
            Rule::id_in_coll => {
                for ic in inner.into_inner() {
                    match ic.as_rule() {
                        Rule::variable => {
                            iter_variable = Some(ic.as_str().to_string());
                        }
                        Rule::expression => {
                            list_expr = Some(parse_expression(ic)?);
                        }
                        _ => {} // kw_in
                    }
                }
            }
            _ => {} // kw_reduce, "(", "=", ",", "|", ")"
        }
    }

    Ok(Expression::Reduce {
        accumulator: accumulator.ok_or_else(|| {
            ParseError::SemanticError(
                "reduce() requires (acc = init, x IN list | expr)".to_string(),
            )
        })?,
        init: Box::new(init_expr.ok_or_else(|| {
            ParseError::SemanticError(
                "reduce() requires (acc = init, x IN list | expr)".to_string(),
            )
        })?),
        variable: iter_variable.ok_or_else(|| {
            ParseError::SemanticError(
                "reduce() requires (acc = init, x IN list | expr)".to_string(),
            )
        })?,
        list_expr: Box::new(list_expr.ok_or_else(|| {
            ParseError::SemanticError(
                "reduce() requires (acc = init, x IN list | expr)".to_string(),
            )
        })?),
        expression: Box::new(body_expr.ok_or_else(|| {
            ParseError::SemanticError(
                "reduce() requires (acc = init, x IN list | expr)".to_string(),
            )
        })?),
    })
}

// ============================================================
// EXISTS subquery
// existential_subquery = { kw_exists ~ "{" ~ (regular_query | (pattern ~ where_clause?)) ~ "}" }
// ============================================================

fn parse_existential_subquery(pair: pest::iterators::Pair<Rule>) -> ParseResult<Expression> {
    let mut pattern = None;
    let mut where_clause = None;

    for inner in pair.into_inner() {
        match inner.as_rule() {
            Rule::match_clause => {
                // EXISTS { MATCH pattern WHERE ... }
                let (mc, wc) = parse_match_clause(inner)?;
                pattern = Some(mc.pattern);
                if let Some(w) = wc {
                    where_clause = Some(w);
                }
            }
            Rule::regular_query => {
                // EXISTS { regular_query } — extract match pattern and where from the query
                let mut sub_query = Query::new();
                parse_regular_query(inner, &mut sub_query)?;
                if !sub_query.match_clauses.is_empty() {
                    pattern = Some(sub_query.match_clauses[0].pattern.clone());
                }
                if let Some(wc) = sub_query.where_clause {
                    where_clause = Some(wc);
                }
            }
            Rule::pattern => {
                pattern = Some(parse_pattern(inner)?);
            }
            Rule::where_clause => {
                where_clause = Some(parse_where_clause(inner)?);
            }
            _ => {} // kw_exists, "{", "}"
        }
    }

    Ok(Expression::ExistsSubquery {
        pattern: pattern
            .ok_or_else(|| ParseError::SemanticError("EXISTS missing pattern".to_string()))?,
        where_clause: where_clause.map(Box::new),
    })
}

// ============================================================
// List comprehension
// list_comprehension = { "[" ~ filter_expression ~ ("|" ~ expression)? ~ "]" }
// filter_expression = { id_in_coll ~ where_clause? }
// id_in_coll = { variable ~ kw_in ~ expression }
// ============================================================

fn parse_list_comprehension(pair: pest::iterators::Pair<Rule>) -> ParseResult<Expression> {
    let mut variable = None;
    let mut list_expr = None;
    let mut filter = None;
    let mut map_expr = None;

    for inner in pair.into_inner() {
        match inner.as_rule() {
            Rule::filter_expression => {
                for fe in inner.into_inner() {
                    match fe.as_rule() {
                        Rule::id_in_coll => {
                            for ic in fe.into_inner() {
                                match ic.as_rule() {
                                    Rule::variable => {
                                        variable = Some(ic.as_str().to_string());
                                    }
                                    Rule::expression => {
                                        list_expr = Some(parse_expression(ic)?);
                                    }
                                    _ => {} // kw_in
                                }
                            }
                        }
                        Rule::where_clause => {
                            let wc = parse_where_clause(fe)?;
                            filter = Some(wc.predicate);
                        }
                        _ => {}
                    }
                }
            }
            Rule::expression => {
                map_expr = Some(parse_expression(inner)?);
            }
            _ => {} // "[", "|", "]"
        }
    }

    // If no map_expr, use the variable as the identity mapping
    let map =
        map_expr.unwrap_or_else(|| Expression::Variable(variable.clone().unwrap_or_default()));

    Ok(Expression::ListComprehension {
        variable: variable.ok_or_else(|| {
            ParseError::SemanticError("List comprehension missing variable".to_string())
        })?,
        list_expr: Box::new(list_expr.ok_or_else(|| {
            ParseError::SemanticError("List comprehension missing list expression".to_string())
        })?),
        filter: filter.map(Box::new),
        map_expr: Box::new(map),
    })
}

// ============================================================
// Pattern comprehension
// pattern_comprehension = { "[" ~ (variable ~ "=")? ~ relationships_pattern ~ where_clause? ~ "|" ~ expression ~ "]" }
// ============================================================

fn parse_pattern_comprehension(pair: pest::iterators::Pair<Rule>) -> ParseResult<Expression> {
    let mut pattern_path = None;
    let mut filter = None;
    let mut projection = None;

    for inner in pair.into_inner() {
        match inner.as_rule() {
            Rule::relationships_pattern => {
                // relationships_pattern = { node_pattern ~ pattern_element_chain+ }
                pattern_path = Some(parse_relationships_pattern(inner)?);
            }
            Rule::where_clause => {
                let wc = parse_where_clause(inner)?;
                filter = Some(wc.predicate);
            }
            Rule::expression => {
                projection = Some(parse_expression(inner)?);
            }
            _ => {} // variable, "=", "[", "|", "]"
        }
    }

    let path = pattern_path.ok_or_else(|| {
        ParseError::SemanticError("Pattern comprehension missing pattern".to_string())
    })?;

    Ok(Expression::PatternComprehension {
        pattern: Pattern { paths: vec![path] },
        filter: filter.map(Box::new),
        projection: Box::new(projection.ok_or_else(|| {
            ParseError::SemanticError("Pattern comprehension missing projection".to_string())
        })?),
    })
}

fn parse_relationships_pattern(pair: pest::iterators::Pair<Rule>) -> ParseResult<PathPattern> {
    let mut start = None;
    let mut segments = Vec::new();

    for inner in pair.into_inner() {
        match inner.as_rule() {
            Rule::node_pattern => {
                if start.is_none() {
                    start = Some(parse_node_pattern(inner)?);
                }
            }
            Rule::pattern_element_chain => {
                let mut edge = None;
                let mut node = None;
                for chain_inner in inner.into_inner() {
                    match chain_inner.as_rule() {
                        Rule::relationship_pattern => {
                            edge = Some(parse_relationship_pattern(chain_inner)?);
                        }
                        Rule::node_pattern => {
                            node = Some(parse_node_pattern(chain_inner)?);
                        }
                        _ => {}
                    }
                }
                if let (Some(e), Some(n)) = (edge, node) {
                    segments.push(PathSegment { edge: e, node: n });
                }
            }
            _ => {}
        }
    }

    Ok(PathPattern {
        path_variable: None,
        path_type: PathType::Normal,
        start: start.unwrap_or(NodePattern {
            variable: None,
            labels: vec![],
            properties: None,
        }),
        segments,
    })
}

// ============================================================
// Quantifier (predicate functions: all, any, none, single)
// quantifier = { (kw_all | kw_any | kw_none | kw_single) ~ "(" ~ filter_expression ~ ")" }
// ============================================================

fn parse_quantifier(pair: pest::iterators::Pair<Rule>) -> ParseResult<Expression> {
    let mut name = String::new();
    let mut variable = None;
    let mut list_expr = None;
    let mut predicate = None;

    for inner in pair.into_inner() {
        match inner.as_rule() {
            Rule::kw_all => name = "all".to_string(),
            Rule::kw_any => name = "any".to_string(),
            Rule::kw_none => name = "none".to_string(),
            Rule::kw_single => name = "single".to_string(),
            Rule::filter_expression => {
                for fe in inner.into_inner() {
                    match fe.as_rule() {
                        Rule::id_in_coll => {
                            for ic in fe.into_inner() {
                                match ic.as_rule() {
                                    Rule::variable => {
                                        variable = Some(ic.as_str().to_string());
                                    }
                                    Rule::expression => {
                                        list_expr = Some(parse_expression(ic)?);
                                    }
                                    _ => {} // kw_in
                                }
                            }
                        }
                        Rule::where_clause => {
                            let wc = parse_where_clause(fe)?;
                            predicate = Some(wc.predicate);
                        }
                        _ => {}
                    }
                }
            }
            _ => {}
        }
    }

    Ok(Expression::PredicateFunction {
        name,
        variable: variable.ok_or_else(|| {
            ParseError::SemanticError("Predicate function missing variable".to_string())
        })?,
        list_expr: Box::new(list_expr.ok_or_else(|| {
            ParseError::SemanticError("Predicate function missing list".to_string())
        })?),
        predicate: Box::new(predicate.unwrap_or(Expression::Literal(PropertyValue::Boolean(true)))),
    })
}

// ============================================================
// Pattern predicate
// pattern_predicate = { relationships_pattern }
// ============================================================

fn parse_pattern_predicate(pair: pest::iterators::Pair<Rule>) -> ParseResult<Expression> {
    let pattern_str = pair.as_str();
    let mut source_var = String::new();

    for inner in pair.into_inner() {
        if inner.as_rule() == Rule::relationships_pattern {
            for rp in inner.into_inner() {
                if rp.as_rule() == Rule::node_pattern {
                    for np in rp.into_inner() {
                        if np.as_rule() == Rule::variable && source_var.is_empty() {
                            source_var = np.as_str().to_string();
                        }
                    }
                    break;
                }
            }
        }
    }

    Ok(Expression::Function {
        name: "$patternPredicate".to_string(),
        args: vec![
            Expression::Literal(PropertyValue::String(source_var)),
            Expression::Literal(PropertyValue::String(pattern_str.to_string())),
        ],
        distinct: false,
    })
}

// ============================================================
// Helper: string representation for complex expressions used as property base
// ============================================================

trait ExprRepr {
    fn to_string_repr(&self) -> String;
}

impl ExprRepr for Expression {
    fn to_string_repr(&self) -> String {
        match self {
            Expression::Variable(v) => v.clone(),
            Expression::Property { variable, property } => format!("{}.{}", variable, property),
            _ => String::new(),
        }
    }
}

// ============================================================
// Tests
// ============================================================

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_simple_match() {
        let query = "MATCH (n:Person) RETURN n";
        let result = parse_query(query);
        assert!(result.is_ok(), "Failed: {:?}", result.err());

        let ast = result.unwrap();
        assert_eq!(ast.match_clauses.len(), 1);
        assert!(ast.return_clause.is_some());
    }

    #[test]
    fn test_parse_match_with_properties() {
        let query = r#"MATCH (n:Person {name: "Alice"}) RETURN n"#;
        let result = parse_query(query);
        assert!(result.is_ok(), "Failed: {:?}", result.err());
    }

    #[test]
    fn test_parse_match_with_edge() {
        let query = "MATCH (a:Person)-[:KNOWS]->(b:Person) RETURN a, b";
        let result = parse_query(query);
        assert!(result.is_ok(), "Failed: {:?}", result.err());

        let ast = result.unwrap();
        let path = &ast.match_clauses[0].pattern.paths[0];
        assert_eq!(path.segments.len(), 1);
    }

    #[test]
    fn test_parse_with_where() {
        let query = "MATCH (n:Person) WHERE n.age > 30 RETURN n";
        let result = parse_query(query);
        assert!(result.is_ok(), "Failed: {:?}", result.err());

        let ast = result.unwrap();
        assert!(ast.where_clause.is_some());
    }

    #[test]
    fn test_parse_with_limit() {
        let query = "MATCH (n:Person) RETURN n LIMIT 10";
        let result = parse_query(query);
        assert!(result.is_ok(), "Failed: {:?}", result.err());

        let ast = result.unwrap();
        assert_eq!(ast.limit, Some(10));
    }

    #[test]
    fn test_parse_create() {
        let query = r#"CREATE (n:Person {name: "Alice", age: 30})"#;
        let result = parse_query(query);
        assert!(result.is_ok(), "Failed: {:?}", result.err());

        let ast = result.unwrap();
        assert!(ast.create_clause.is_some());
        assert!(!ast.is_read_only());
    }

    #[test]
    fn test_parse_explain() {
        let query = "EXPLAIN MATCH (n:Person) RETURN n";
        let result = parse_query(query);
        assert!(result.is_ok(), "Failed: {:?}", result.err());
        assert!(result.unwrap().explain);
    }

    #[test]
    fn test_parse_is_null() {
        let query = "MATCH (n:Person) WHERE n.email IS NULL RETURN n";
        let result = parse_query(query);
        assert!(
            result.is_ok(),
            "Failed to parse IS NULL: {:?}",
            result.err()
        );

        let ast = result.unwrap();
        let predicate = &ast.where_clause.unwrap().predicate;
        match predicate {
            Expression::Unary { op, expr } => {
                assert_eq!(*op, UnaryOp::IsNull);
                assert!(
                    matches!(expr.as_ref(), Expression::Property { variable, property }
                    if variable == "n" && property == "email")
                );
            }
            other => panic!("Expected Unary(IsNull), got {:?}", other),
        }
    }

    #[test]
    fn test_parse_is_not_null() {
        let query = "MATCH (n:Person) WHERE n.name IS NOT NULL RETURN n";
        let result = parse_query(query);
        assert!(
            result.is_ok(),
            "Failed to parse IS NOT NULL: {:?}",
            result.err()
        );

        let ast = result.unwrap();
        let predicate = &ast.where_clause.unwrap().predicate;
        match predicate {
            Expression::Unary { op, expr } => {
                assert_eq!(*op, UnaryOp::IsNotNull);
                assert!(
                    matches!(expr.as_ref(), Expression::Property { variable, property }
                    if variable == "n" && property == "name")
                );
            }
            other => panic!("Expected Unary(IsNotNull), got {:?}", other),
        }
    }

    #[test]
    fn test_parse_not_expression() {
        let query = "MATCH (n:Person) WHERE NOT n.active RETURN n";
        let result = parse_query(query);
        assert!(result.is_ok(), "Failed to parse NOT: {:?}", result.err());

        let ast = result.unwrap();
        let predicate = &ast.where_clause.unwrap().predicate;
        match predicate {
            Expression::Unary { op, expr } => {
                assert_eq!(*op, UnaryOp::Not);
                assert!(
                    matches!(expr.as_ref(), Expression::Property { variable, property }
                    if variable == "n" && property == "active")
                );
            }
            other => panic!("Expected Unary(Not), got {:?}", other),
        }
    }

    #[test]
    fn test_parse_optional_match() {
        let query = "MATCH (n:Person) OPTIONAL MATCH (n)-[:KNOWS]->(m:Person) RETURN n, m";
        let result = parse_query(query);
        assert!(
            result.is_ok(),
            "Failed to parse OPTIONAL MATCH: {:?}",
            result.err()
        );
        let ast = result.unwrap();
        assert_eq!(ast.match_clauses.len(), 2);
        assert!(!ast.match_clauses[0].optional);
        assert!(ast.match_clauses[1].optional);
    }

    #[test]
    fn test_parse_with_clause() {
        let query = "MATCH (n:Person) WITH n.name AS name RETURN name";
        let result = parse_query(query);
        assert!(result.is_ok(), "Failed to parse WITH: {:?}", result.err());
        let ast = result.unwrap();
        assert!(ast.with_clause.is_some());
    }

    #[test]
    fn test_parse_skip() {
        let query = "MATCH (n:Person) RETURN n SKIP 5 LIMIT 10";
        let result = parse_query(query);
        assert!(result.is_ok(), "Failed to parse SKIP: {:?}", result.err());
        let ast = result.unwrap();
        assert_eq!(ast.skip, Some(5));
        assert_eq!(ast.limit, Some(10));
    }

    #[test]
    fn test_parse_delete() {
        let query = "MATCH (n:Person) DELETE n";
        let result = parse_query(query);
        assert!(result.is_ok(), "Failed to parse DELETE: {:?}", result.err());
        let ast = result.unwrap();
        assert!(ast.delete_clause.is_some());
        assert!(!ast.delete_clause.unwrap().detach);
    }

    #[test]
    fn test_parse_detach_delete() {
        let query = "MATCH (n:Person) DETACH DELETE n";
        let result = parse_query(query);
        assert!(
            result.is_ok(),
            "Failed to parse DETACH DELETE: {:?}",
            result.err()
        );
        let ast = result.unwrap();
        assert!(ast.delete_clause.as_ref().unwrap().detach);
    }

    #[test]
    fn test_parse_set() {
        let query = r#"MATCH (n:Person) SET n.name = "Bob" RETURN n"#;
        let result = parse_query(query);
        assert!(result.is_ok(), "Failed to parse SET: {:?}", result.err());
        let ast = result.unwrap();
        assert_eq!(ast.set_clauses.len(), 1);
        assert_eq!(ast.set_clauses[0].items[0].variable, "n");
        assert_eq!(ast.set_clauses[0].items[0].property, "name");
    }

    #[test]
    fn test_parse_remove() {
        let query = "MATCH (n:Person) REMOVE n.email RETURN n";
        let result = parse_query(query);
        assert!(result.is_ok(), "Failed to parse REMOVE: {:?}", result.err());
        let ast = result.unwrap();
        assert_eq!(ast.remove_clauses.len(), 1);
    }

    #[test]
    fn test_parse_in_operator() {
        let query = r#"MATCH (n:Person) WHERE n.name IN ["Alice", "Bob"] RETURN n"#;
        let result = parse_query(query);
        assert!(result.is_ok(), "Failed to parse IN: {:?}", result.err());
        let ast = result.unwrap();
        let pred = &ast.where_clause.unwrap().predicate;
        assert!(matches!(
            pred,
            Expression::Binary {
                op: BinaryOp::In,
                ..
            }
        ));
    }

    #[test]
    fn test_parse_arithmetic() {
        let query = "MATCH (n:Person) RETURN n.age + 1";
        let result = parse_query(query);
        assert!(
            result.is_ok(),
            "Failed to parse arithmetic: {:?}",
            result.err()
        );
    }

    #[test]
    fn test_parse_regex() {
        let query = r#"MATCH (n:Person) WHERE n.email =~ ".*@gmail.com" RETURN n"#;
        let result = parse_query(query);
        assert!(result.is_ok(), "Failed to parse regex: {:?}", result.err());
        let ast = result.unwrap();
        let pred = &ast.where_clause.unwrap().predicate;
        assert!(matches!(
            pred,
            Expression::Binary {
                op: BinaryOp::RegexMatch,
                ..
            }
        ));
    }

    #[test]
    fn test_parse_case_expression() {
        let query = r#"MATCH (n:Person) RETURN CASE WHEN n.age > 18 THEN "adult" ELSE "minor" END"#;
        let result = parse_query(query);
        assert!(result.is_ok(), "Failed to parse CASE: {:?}", result.err());
    }

    #[test]
    fn test_parse_collect() {
        let query = "MATCH (n:Person) RETURN collect(n.name)";
        let result = parse_query(query);
        assert!(
            result.is_ok(),
            "Failed to parse collect: {:?}",
            result.err()
        );
    }

    #[test]
    fn test_parse_string_functions() {
        let query = r#"MATCH (n:Person) RETURN toUpper(n.name), toLower(n.name), trim(n.name)"#;
        let result = parse_query(query);
        assert!(
            result.is_ok(),
            "Failed to parse string functions: {:?}",
            result.err()
        );
    }

    #[test]
    fn test_parse_unwind() {
        let query = "UNWIND [1, 2, 3] AS x RETURN x";
        let result = parse_query(query);
        assert!(result.is_ok(), "Failed to parse UNWIND: {:?}", result.err());
        let ast = result.unwrap();
        assert!(ast.unwind_clause.is_some());
        assert_eq!(ast.unwind_clause.unwrap().variable, "x");
    }

    #[test]
    fn test_parse_merge() {
        let query = r#"MERGE (n:Person {name: "Alice"}) ON CREATE SET n.created = "now" ON MATCH SET n.lastSeen = "now" RETURN n"#;
        let result = parse_query(query);
        assert!(result.is_ok(), "Failed to parse MERGE: {:?}", result.err());
        let ast = result.unwrap();
        assert!(ast.merge_clause.is_some());
        let merge = ast.merge_clause.unwrap();
        assert_eq!(merge.on_create_set.len(), 1);
        assert_eq!(merge.on_match_set.len(), 1);
    }

    #[test]
    fn test_parse_merge_simple() {
        let query = r#"MERGE (n:Person {name: "Alice"})"#;
        let result = parse_query(query);
        assert!(
            result.is_ok(),
            "Failed to parse simple MERGE: {:?}",
            result.err()
        );
        let ast = result.unwrap();
        assert!(ast.merge_clause.is_some());
    }

    #[test]
    fn test_parse_union() {
        let query = "MATCH (n:Person) RETURN n.name UNION MATCH (m:Animal) RETURN m.name";
        let result = parse_query(query);
        assert!(result.is_ok(), "Failed to parse UNION: {:?}", result.err());
        let ast = result.unwrap();
        assert_eq!(ast.union_queries.len(), 1);
        assert!(!ast.union_queries[0].1);
    }

    #[test]
    fn test_parse_union_all() {
        let query = "MATCH (n:Person) RETURN n.name UNION ALL MATCH (m:Person) RETURN m.name";
        let result = parse_query(query);
        assert!(
            result.is_ok(),
            "Failed to parse UNION ALL: {:?}",
            result.err()
        );
        let ast = result.unwrap();
        assert_eq!(ast.union_queries.len(), 1);
        assert!(ast.union_queries[0].1);
    }

    #[test]
    fn test_parse_list_index() {
        let query = "MATCH (n:Person) RETURN n.tags[0]";
        let result = parse_query(query);
        assert!(
            result.is_ok(),
            "Failed to parse list index: {:?}",
            result.err()
        );
        let ast = result.unwrap();
        let item = &ast.return_clause.unwrap().items[0];
        assert!(matches!(&item.expression, Expression::Index { .. }));
    }

    #[test]
    fn test_parse_list_slice() {
        let query = "MATCH (n:Person) RETURN n.tags[1..3]";
        let result = parse_query(query);
        assert!(
            result.is_ok(),
            "Failed to parse list slice [1..3]: {:?}",
            result.err()
        );
        let ast = result.unwrap();
        let item = &ast.return_clause.unwrap().items[0];
        assert!(
            matches!(&item.expression, Expression::ListSlice { .. }),
            "Expected ListSlice, got: {:?}",
            item.expression
        );
    }

    #[test]
    fn test_parse_exists_subquery() {
        let query = "MATCH (n:Person) WHERE EXISTS { MATCH (n)-[:KNOWS]->(:Person) } RETURN n";
        let result = parse_query(query);
        assert!(
            result.is_ok(),
            "Failed to parse EXISTS subquery: {:?}",
            result.err()
        );
        let ast = result.unwrap();
        let where_clause = ast.where_clause.unwrap();
        assert!(matches!(
            where_clause.predicate,
            Expression::ExistsSubquery { .. }
        ));
    }

    #[test]
    fn test_parse_exists_subquery_with_where() {
        let query = "MATCH (n:Person) WHERE EXISTS { MATCH (n)-[:KNOWS]->(m:Person) WHERE m.age > 30 } RETURN n";
        let result = parse_query(query);
        assert!(
            result.is_ok(),
            "Failed to parse EXISTS with WHERE: {:?}",
            result.err()
        );
        let ast = result.unwrap();
        if let Expression::ExistsSubquery {
            pattern,
            where_clause,
        } = &ast.where_clause.unwrap().predicate
        {
            assert!(!pattern.paths.is_empty());
            assert!(where_clause.is_some());
        } else {
            panic!("Expected ExistsSubquery");
        }
    }

    #[test]
    fn test_parse_list_comprehension() {
        let query = "MATCH (n:Person) RETURN [x IN n.tags WHERE x <> 'admin' | x]";
        let result = parse_query(query);
        assert!(
            result.is_ok(),
            "Failed to parse list comprehension: {:?}",
            result.err()
        );
        let ast = result.unwrap();
        let item = &ast.return_clause.unwrap().items[0];
        if let Expression::ListComprehension {
            variable, filter, ..
        } = &item.expression
        {
            assert_eq!(variable, "x");
            assert!(filter.is_some());
        } else {
            panic!("Expected ListComprehension, got {:?}", item.expression);
        }
    }

    #[test]
    fn test_parse_foreach() {
        let query = "MATCH (n:Person) FOREACH (tag IN n.tags | SET n.processed = TRUE)";
        let result = parse_query(query);
        assert!(
            result.is_ok(),
            "Failed to parse FOREACH: {:?}",
            result.err()
        );
        let ast = result.unwrap();
        assert!(ast.foreach_clause.is_some());
        let fc = ast.foreach_clause.unwrap();
        assert_eq!(fc.variable, "tag");
        assert!(!fc.set_clauses.is_empty());
    }

    #[test]
    fn test_parse_profile() {
        let query = "PROFILE MATCH (n:Person) RETURN n";
        let result = parse_query(query);
        assert!(
            result.is_ok(),
            "Failed to parse PROFILE: {:?}",
            result.err()
        );
        let ast = result.unwrap();
        assert!(ast.profile);
    }

    #[test]
    fn test_parse_parameterized_query() {
        let query = "MATCH (n:Person) WHERE n.name = $name RETURN n";
        let result = parse_query(query);
        assert!(
            result.is_ok(),
            "Failed to parse parameterized query: {:?}",
            result.err()
        );
        let ast = result.unwrap();
        let where_clause = ast.where_clause.unwrap();
        if let Expression::Binary { right, .. } = &where_clause.predicate {
            assert!(matches!(right.as_ref(), Expression::Parameter(_)));
        } else {
            panic!(
                "Expected Binary with Parameter, got {:?}",
                where_clause.predicate
            );
        }
    }

    #[test]
    fn test_parse_create_index() {
        let query = "CREATE INDEX ON :Person(name)";
        let result = parse_query(query);
        assert!(
            result.is_ok(),
            "Failed to parse CREATE INDEX: {:?}",
            result.err()
        );
        let ast = result.unwrap();
        let idx = ast.create_index_clause.unwrap();
        assert_eq!(idx.label, Label::new("Person"));
        assert_eq!(idx.property, "name");
    }

    #[test]
    fn test_parse_create_composite_index() {
        let query = "CREATE INDEX ON :Person(name, age)";
        let result = parse_query(query);
        assert!(
            result.is_ok(),
            "Failed to parse composite index: {:?}",
            result.err()
        );
        let ast = result.unwrap();
        let idx = ast.create_index_clause.unwrap();
        assert_eq!(idx.label, Label::new("Person"));
        assert_eq!(idx.property, "name");
        assert_eq!(idx.additional_properties, vec!["age".to_string()]);
    }

    #[test]
    fn test_parse_drop_index() {
        let query = "DROP INDEX ON :Person(name)";
        let result = parse_query(query);
        assert!(
            result.is_ok(),
            "Failed to parse DROP INDEX: {:?}",
            result.err()
        );
        let ast = result.unwrap();
        let di = ast.drop_index_clause.unwrap();
        assert_eq!(di.label, Label::new("Person"));
        assert_eq!(di.property, "name");
    }

    #[test]
    fn test_parse_show_indexes() {
        let query = "SHOW INDEXES";
        let result = parse_query(query);
        assert!(
            result.is_ok(),
            "Failed to parse SHOW INDEXES: {:?}",
            result.err()
        );
        let ast = result.unwrap();
        assert!(ast.show_indexes);
    }

    #[test]
    fn test_parse_show_constraints() {
        let query = "SHOW CONSTRAINTS";
        let result = parse_query(query);
        assert!(
            result.is_ok(),
            "Failed to parse SHOW CONSTRAINTS: {:?}",
            result.err()
        );
        let ast = result.unwrap();
        assert!(ast.show_constraints);
    }

    #[test]
    fn test_parse_create_constraint() {
        let query = "CREATE CONSTRAINT ON (n:Person) ASSERT n.email IS UNIQUE";
        let result = parse_query(query);
        assert!(
            result.is_ok(),
            "Failed to parse CREATE CONSTRAINT: {:?}",
            result.err()
        );
        let ast = result.unwrap();
        let cc = ast.create_constraint_clause.unwrap();
        assert_eq!(cc.label, Label::new("Person"));
        assert_eq!(cc.property, "email");
        assert_eq!(cc.variable, "n");
    }

    #[test]
    fn test_parse_create_vector_index() {
        let query = "CREATE VECTOR INDEX myIdx FOR (n:Document) ON (n.embedding) OPTIONS {dimensions: 384, similarity: 'cosine'}";
        let result = parse_query(query);
        assert!(
            result.is_ok(),
            "Failed to parse CREATE VECTOR INDEX: {:?}",
            result.err()
        );
        let ast = result.unwrap();
        let vi = ast.create_vector_index_clause.unwrap();
        assert_eq!(vi.label, Label::new("Document"));
        assert_eq!(vi.property_key, "embedding");
        assert_eq!(vi.dimensions, 384);
        assert_eq!(vi.similarity, "cosine");
    }

    #[test]
    fn test_parse_call_algorithm() {
        let query =
            "CALL algo.pageRank({maxIterations: 20, dampingFactor: 0.85}) YIELD node, score";
        let result = parse_query(query);
        assert!(
            result.is_ok(),
            "Failed to parse CALL algo: {:?}",
            result.err()
        );
        let ast = result.unwrap();
        let call = ast.call_clause.unwrap();
        assert!(call.procedure_name.starts_with("algo."));
    }

    #[test]
    fn test_parse_named_path() {
        let query = "MATCH p = (a:Person)-[:KNOWS]->(b:Person) RETURN p";
        let result = parse_query(query);
        assert!(
            result.is_ok(),
            "Failed to parse named path: {:?}",
            result.err()
        );
        let ast = result.unwrap();
        assert!(!ast.match_clauses.is_empty());
        let pp = &ast.match_clauses[0].pattern.paths[0];
        assert_eq!(pp.path_variable, Some("p".to_string()));
    }

    #[test]
    fn test_parse_collect_distinct() {
        let query = "MATCH (n:Person) RETURN collect(DISTINCT n.name) AS names";
        let result = parse_query(query);
        assert!(
            result.is_ok(),
            "Failed to parse collect(DISTINCT): {:?}",
            result.err()
        );
    }

    #[test]
    fn test_parse_multiple_match_clauses() {
        let query = "MATCH (a:Person) MATCH (b:Company) RETURN a, b";
        let result = parse_query(query);
        assert!(
            result.is_ok(),
            "Failed to parse multi-MATCH: {:?}",
            result.err()
        );
        let ast = result.unwrap();
        assert_eq!(ast.match_clauses.len(), 2);
    }

    #[test]
    fn test_parse_variable_length_edge() {
        let query = "MATCH (a:Person)-[:KNOWS*1..3]->(b:Person) RETURN b";
        let result = parse_query(query);
        assert!(
            result.is_ok(),
            "Failed to parse variable-length edge: {:?}",
            result.err()
        );
    }

    #[test]
    fn test_parse_bidirectional_edge() {
        let query = "MATCH (a:Person)-[:KNOWS]-(b:Person) RETURN b";
        let result = parse_query(query);
        assert!(
            result.is_ok(),
            "Failed to parse bidirectional edge: {:?}",
            result.err()
        );
    }

    #[test]
    fn test_parse_return_distinct() {
        let query = "MATCH (n:Person) RETURN DISTINCT n.name";
        let result = parse_query(query);
        assert!(
            result.is_ok(),
            "Failed to parse RETURN DISTINCT: {:?}",
            result.err()
        );
        let ast = result.unwrap();
        let ret = ast.return_clause.unwrap();
        assert!(ret.distinct);
    }

    #[test]
    fn test_parse_order_by_desc() {
        let query = "MATCH (n:Person) RETURN n.name ORDER BY n.age DESC";
        let result = parse_query(query);
        assert!(
            result.is_ok(),
            "Failed to parse ORDER BY DESC: {:?}",
            result.err()
        );
        let ast = result.unwrap();
        let ob = ast.order_by.unwrap();
        assert!(!ob.items.is_empty());
        assert!(!ob.items[0].ascending);
    }

    #[test]
    fn test_parse_count_star() {
        let query = "MATCH (n) RETURN count(*)";
        let result = parse_query(query);
        assert!(
            result.is_ok(),
            "Failed to parse count(*): {:?}",
            result.err()
        );
        let ast = result.unwrap();
        let items = &ast.return_clause.unwrap().items;
        assert_eq!(items.len(), 1);
        match &items[0].expression {
            Expression::Function {
                name,
                args,
                distinct,
            } => {
                assert_eq!(name, "count");
                assert!(args.is_empty());
                assert!(!distinct);
            }
            other => panic!("Expected Function, got: {:?}", other),
        }
    }

    #[test]
    fn test_parse_starts_with_operator() {
        let query = "MATCH (n:Person) WHERE n.name STARTS WITH 'A' RETURN n";
        let result = parse_query(query);
        assert!(
            result.is_ok(),
            "Failed to parse STARTS WITH: {:?}",
            result.err()
        );
        let ast = result.unwrap();
        let wc = ast.where_clause.unwrap();
        if let Expression::Binary { op, .. } = &wc.predicate {
            assert_eq!(*op, BinaryOp::StartsWith);
        } else {
            panic!("Expected Binary with StartsWith, got {:?}", wc.predicate);
        }
    }

    #[test]
    fn test_parse_ends_with_operator() {
        let query = "MATCH (n:Person) WHERE n.name ENDS WITH 'son' RETURN n";
        let result = parse_query(query);
        assert!(
            result.is_ok(),
            "Failed to parse ENDS WITH: {:?}",
            result.err()
        );
        let ast = result.unwrap();
        let wc = ast.where_clause.unwrap();
        if let Expression::Binary { op, .. } = &wc.predicate {
            assert_eq!(*op, BinaryOp::EndsWith);
        } else {
            panic!("Expected Binary with EndsWith, got {:?}", wc.predicate);
        }
    }

    #[test]
    fn test_parse_contains_operator() {
        let query = "MATCH (n:Person) WHERE n.name CONTAINS 'lic' RETURN n";
        let result = parse_query(query);
        assert!(
            result.is_ok(),
            "Failed to parse CONTAINS: {:?}",
            result.err()
        );
        let ast = result.unwrap();
        let wc = ast.where_clause.unwrap();
        if let Expression::Binary { op, .. } = &wc.predicate {
            assert_eq!(*op, BinaryOp::Contains);
        } else {
            panic!("Expected Binary with Contains, got {:?}", wc.predicate);
        }
    }

    #[test]
    fn test_parse_match_create_in_same_query() {
        let query = "MATCH (a:Person {name: 'Alice'}) CREATE (a)-[:KNOWS]->(:Person {name: 'Bob'})";
        let result = parse_query(query);
        assert!(
            result.is_ok(),
            "Failed to parse MATCH+CREATE: {:?}",
            result.err()
        );
        let ast = result.unwrap();
        assert!(!ast.match_clauses.is_empty());
        assert!(ast.create_clause.is_some());
    }

    #[test]
    fn test_parse_multiple_set_items() {
        let query = "MATCH (n:Person) SET n.name = 'Bob', n.age = 25 RETURN n";
        let result = parse_query(query);
        assert!(
            result.is_ok(),
            "Failed to parse multiple SET items: {:?}",
            result.err()
        );
        let ast = result.unwrap();
        assert!(!ast.set_clauses.is_empty());
        assert!(ast.set_clauses[0].items.len() >= 2);
    }

    #[test]
    fn test_parse_with_where_clause() {
        let query =
            "MATCH (n:Person) WITH n.city AS city, count(n) AS cnt WHERE cnt > 5 RETURN city";
        let result = parse_query(query);
        assert!(
            result.is_ok(),
            "Failed to parse WITH WHERE: {:?}",
            result.err()
        );
        let ast = result.unwrap();
        let wc = ast.with_clause.unwrap();
        assert!(wc.where_clause.is_some());
    }

    #[test]
    fn test_parse_with_distinct() {
        let query = "MATCH (n:Person) WITH DISTINCT n.city AS city RETURN city";
        let result = parse_query(query);
        assert!(
            result.is_ok(),
            "Failed to parse WITH DISTINCT: {:?}",
            result.err()
        );
        let ast = result.unwrap();
        let wc = ast.with_clause.unwrap();
        assert!(wc.distinct);
    }

    #[test]
    fn test_parse_long_chain_pattern() {
        let query = "MATCH (a:Person)-[:KNOWS]->(b:Person)-[:WORKS_AT]->(c:Company) RETURN a, b, c";
        let result = parse_query(query);
        assert!(
            result.is_ok(),
            "Failed to parse chain pattern: {:?}",
            result.err()
        );
        let ast = result.unwrap();
        let pp = &ast.match_clauses[0].pattern.paths[0];
        assert_eq!(pp.segments.len(), 2);
    }

    #[test]
    fn test_parse_create_node_with_properties() {
        let query = r#"CREATE (n:Person {name: "Alice", age: 30, active: true})"#;
        let result = parse_query(query);
        assert!(
            result.is_ok(),
            "Failed to parse CREATE with properties: {:?}",
            result.err()
        );
        let ast = result.unwrap();
        let create = ast.create_clause.unwrap();
        let props = create.pattern.paths[0].start.properties.as_ref().unwrap();
        assert_eq!(
            props.get("name"),
            Some(&PropertyValue::String("Alice".to_string()))
        );
        assert_eq!(props.get("age"), Some(&PropertyValue::Integer(30)));
        assert_eq!(props.get("active"), Some(&PropertyValue::Boolean(true)));
    }

    #[test]
    fn test_parse_with_order_by_skip_limit() {
        let query = "MATCH (n:Person) WITH n ORDER BY n.age SKIP 5 LIMIT 10 RETURN n.name";
        let result = parse_query(query);
        assert!(
            result.is_ok(),
            "Failed to parse WITH ORDER BY SKIP LIMIT: {:?}",
            result.err()
        );
        let ast = result.unwrap();
        let wc = ast.with_clause.unwrap();
        assert!(wc.order_by.is_some());
        assert_eq!(wc.skip, Some(5));
        assert_eq!(wc.limit, Some(10));
    }

    #[test]
    fn test_parse_merge_inline_after_match() {
        let query = "MATCH (a:Person {name: 'Alice'}) MERGE (a)-[:KNOWS]->(b:Person {name: 'Bob'})";
        let result = parse_query(query);
        assert!(
            result.is_ok(),
            "Failed to parse MERGE after MATCH: {:?}",
            result.err()
        );
        let ast = result.unwrap();
        assert!(!ast.match_clauses.is_empty());
        assert!(ast.merge_clause.is_some());
    }

    #[test]
    fn test_parse_vector_list_literal() {
        let query = "CREATE (n:Doc {embedding: [0.1, 0.2, 0.3, 0.4]})";
        let result = parse_query(query);
        assert!(
            result.is_ok(),
            "Failed to parse vector list: {:?}",
            result.err()
        );
        let ast = result.unwrap();
        let create = ast.create_clause.unwrap();
        let props = create.pattern.paths[0].start.properties.as_ref().unwrap();
        if let Some(PropertyValue::Vector(v)) = props.get("embedding") {
            assert_eq!(v.len(), 4);
        } else {
            panic!("Expected Vector property, got {:?}", props.get("embedding"));
        }
    }

    #[test]
    fn test_parse_error_malformed() {
        let query = "MATCHH (n) RETURN n";
        let result = parse_query(query);
        assert!(result.is_err(), "Expected parse error for malformed query");
    }

    #[test]
    fn test_parse_error_empty() {
        let query = "";
        let result = parse_query(query);
        assert!(result.is_err(), "Expected parse error for empty query");
    }

    #[test]
    fn test_parse_unary_minus_expr() {
        let query = "MATCH (n:Item) RETURN -n.val AS neg";
        let result = parse_query(query);
        assert!(result.is_ok(), "Failed: {:?}", result.err());
        let ast = result.unwrap();
        let items = &ast.return_clause.unwrap().items;
        assert_eq!(items[0].alias, Some("neg".to_string()));
        match &items[0].expression {
            Expression::Unary {
                op: UnaryOp::Minus, ..
            } => {}
            Expression::Literal(PropertyValue::Integer(i)) if *i < 0 => {}
            other => panic!("Expected Unary Minus, got {:?}", other),
        }
    }

    #[test]
    fn test_parse_pattern_comprehension() {
        let query = "MATCH (n:Person) RETURN [(n)-[:KNOWS]->(m) WHERE m.age > 20 | m.name]";
        let result = parse_query(query);
        assert!(
            result.is_ok(),
            "Failed to parse pattern comprehension: {:?}",
            result.err()
        );
        let ast = result.unwrap();
        let ret = ast.return_clause.unwrap();
        assert!(matches!(
            &ret.items[0].expression,
            Expression::PatternComprehension { .. }
        ));
    }

    #[test]
    fn test_parse_multiple_edge_types() {
        let query = "MATCH (a)-[:KNOWS|FOLLOWS]->(b) RETURN b";
        let result = parse_query(query);
        assert!(
            result.is_ok(),
            "Failed to parse multi-edge types: {:?}",
            result.err()
        );
        let ast = result.unwrap();
        let segments = &ast.match_clauses[0].pattern.paths[0].segments;
        assert!(segments[0].edge.types.len() >= 2);
    }

    #[test]
    fn test_parse_call_with_yield_alias() {
        let query = "CALL algo.bfs({startNode: 'n1'}) YIELD node AS vertex, depth AS level";
        let result = parse_query(query);
        assert!(
            result.is_ok(),
            "Failed to parse CALL with YIELD alias: {:?}",
            result.err()
        );
        let ast = result.unwrap();
        let call = ast.call_clause.unwrap();
        assert_eq!(call.yield_items.len(), 2);
        assert_eq!(call.yield_items[0].name, "node");
        assert_eq!(call.yield_items[0].alias, Some("vertex".to_string()));
    }

    #[test]
    fn test_parse_return_list_literal() {
        let query = "RETURN [1, 2, 3, 4, 5]";
        let result = parse_query(query);
        // Should succeed with the new grammar
        let _ = result;
    }

    #[test]
    fn test_parse_return_map_literal() {
        let query = "RETURN {name: 'Alice', age: 30}";
        let result = parse_query(query);
        let _ = result;
    }

    #[test]
    fn test_parse_order_by_multiple() {
        let query = "MATCH (n:Person) RETURN n.name, n.age ORDER BY n.age DESC, n.name ASC";
        let result = parse_query(query);
        assert!(
            result.is_ok(),
            "Failed to parse multi ORDER BY: {:?}",
            result.err()
        );
        let ast = result.unwrap();
        let ob = ast.order_by.unwrap();
        assert_eq!(ob.items.len(), 2);
        assert!(!ob.items[0].ascending);
        assert!(ob.items[1].ascending);
    }

    #[test]
    fn test_parse_comparison_operators_all() {
        let queries = vec![
            ("MATCH (n) WHERE n.x = 1 RETURN n", BinaryOp::Eq),
            ("MATCH (n) WHERE n.x < 1 RETURN n", BinaryOp::Lt),
            ("MATCH (n) WHERE n.x > 1 RETURN n", BinaryOp::Gt),
            ("MATCH (n) WHERE n.x <= 1 RETURN n", BinaryOp::Le),
            ("MATCH (n) WHERE n.x >= 1 RETURN n", BinaryOp::Ge),
        ];
        for (query, expected_op) in &queries {
            let result = parse_query(query);
            assert!(
                result.is_ok(),
                "Failed to parse {}: {:?}",
                query,
                result.err()
            );
            let wc = result.unwrap().where_clause.unwrap();
            if let Expression::Binary { op, .. } = &wc.predicate {
                assert_eq!(op, expected_op, "Wrong op for query: {}", query);
            }
        }
    }

    #[test]
    fn test_parse_merge_on_create_on_match() {
        let query = "MERGE (n:Person {name: 'Alice'}) ON CREATE SET n.created = true ON MATCH SET n.visits = 1";
        let result = parse_query(query);
        assert!(
            result.is_ok(),
            "Failed to parse MERGE ON CREATE/ON MATCH: {:?}",
            result.err()
        );
        let ast = result.unwrap();
        assert!(ast.merge_clause.is_some());
    }

    #[test]
    fn test_parse_predicate_function_all() {
        let query = "MATCH (n) WHERE all(x IN n.scores WHERE x > 0) RETURN n";
        let result = parse_query(query);
        assert!(result.is_ok(), "Failed to parse all(): {:?}", result.err());
    }

    #[test]
    fn test_parse_predicate_function_any() {
        let query = "MATCH (n) WHERE any(x IN n.scores WHERE x > 90) RETURN n";
        let result = parse_query(query);
        assert!(result.is_ok(), "Failed to parse any(): {:?}", result.err());
    }

    #[test]
    fn test_parse_predicate_function_none() {
        let query = "MATCH (n) WHERE none(x IN n.scores WHERE x < 0) RETURN n";
        let result = parse_query(query);
        assert!(result.is_ok(), "Failed to parse none(): {:?}", result.err());
    }

    #[test]
    fn test_parse_predicate_function_single() {
        let query = "MATCH (n) WHERE single(x IN n.scores WHERE x = 100) RETURN n";
        let result = parse_query(query);
        assert!(
            result.is_ok(),
            "Failed to parse single(): {:?}",
            result.err()
        );
    }

    #[test]
    fn test_parse_error_display() {
        let err = ParseError::SemanticError("test semantic error".to_string());
        let display = format!("{}", err);
        assert!(display.contains("Semantic error"));
        assert!(display.contains("test semantic error"));

        let err2 = ParseError::UnsupportedFeature("test feature".to_string());
        let display2 = format!("{}", err2);
        assert!(display2.contains("Unsupported feature"));
    }

    #[test]
    fn test_parse_count_star_with_alias() {
        let query = "MATCH (n:Person) RETURN count(*) AS total";
        let result = parse_query(query);
        assert!(result.is_ok(), "Failed: {:?}", result.err());
        let ast = result.unwrap();
        let items = &ast.return_clause.unwrap().items;
        assert_eq!(items[0].alias, Some("total".to_string()));
    }

    #[test]
    fn test_parse_with_aggregation() {
        let query =
            "MATCH (n:Person) WITH n.city AS city, count(n) AS cnt RETURN city ORDER BY cnt DESC";
        let result = parse_query(query);
        assert!(
            result.is_ok(),
            "Failed to parse WITH aggregation: {:?}",
            result.err()
        );
    }

    #[test]
    fn test_parse_or_expression() {
        let query = "MATCH (n) WHERE n.age > 30 OR n.name = 'Alice' RETURN n";
        let result = parse_query(query);
        assert!(
            result.is_ok(),
            "Failed to parse OR expression: {:?}",
            result.err()
        );
    }

    #[test]
    fn test_parse_nested_function_calls() {
        let query = "MATCH (n) RETURN toUpper(trim(n.name))";
        let result = parse_query(query);
        assert!(
            result.is_ok(),
            "Failed to parse nested functions: {:?}",
            result.err()
        );
    }

    #[test]
    fn test_parse_remove_label() {
        let query = "MATCH (n:Person) REMOVE n:Employee RETURN n";
        let result = parse_query(query);
        assert!(
            result.is_ok(),
            "Failed to parse REMOVE label: {:?}",
            result.err()
        );
        let ast = result.unwrap();
        assert!(!ast.remove_clauses.is_empty());
    }

    #[test]
    fn test_parse_incoming_edge() {
        let query = "MATCH (a:Person)<-[:FOLLOWS]-(b:Person) RETURN b";
        let result = parse_query(query);
        assert!(
            result.is_ok(),
            "Failed to parse incoming edge: {:?}",
            result.err()
        );
    }

    #[test]
    fn test_parse_not_equals_operators() {
        let query = "MATCH (n) WHERE n.x <> 5 RETURN n";
        let result = parse_query(query);
        assert!(result.is_ok(), "Failed to parse <>: {:?}", result.err());
        let wc = result.unwrap().where_clause.unwrap();
        if let Expression::Binary { op, .. } = &wc.predicate {
            assert_eq!(*op, BinaryOp::Ne);
        }
    }

    #[test]
    fn test_parse_return_alias() {
        let query = "MATCH (n:Person) RETURN n.name AS personName, count(n) AS total";
        let result = parse_query(query);
        assert!(
            result.is_ok(),
            "Failed to parse RETURN alias: {:?}",
            result.err()
        );
        let ast = result.unwrap();
        let items = &ast.return_clause.unwrap().items;
        assert_eq!(items.len(), 2);
        assert_eq!(items[0].alias, Some("personName".to_string()));
        assert_eq!(items[1].alias, Some("total".to_string()));
    }

    #[test]
    fn test_parse_node_with_optional_parts() {
        // Test all combinations of optional parts in node patterns
        let cases = vec![
            "CREATE (n:Person {name: 'Alice'})",
            "MATCH (n:Person {name: 'Alice'}) RETURN n",
            "MATCH (n {name: 'Alice'}) RETURN n",
            "MATCH (n:Person) RETURN n SKIP 5 LIMIT 10",
            "MATCH (a)-[:KNOWS {since: 2020}]->(b) RETURN b",
        ];
        for q in cases {
            let r = parse_query(q);
            assert!(r.is_ok(), "Failed to parse '{}': {:?}", q, r.err());
        }
    }
}
