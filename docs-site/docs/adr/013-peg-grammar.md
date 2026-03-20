---
sidebar_position: 13
title: "ADR-013: PEG Grammar Atomic Keywords"
---

# ADR-013: PEG Grammar with Atomic Keyword Rules

## Status
**Accepted**

## Date
2025-12-20

## Context

Graphmind uses the Pest PEG parser for OpenCypher parsing. Critical parsing bugs were caused by how Pest handles whitespace in non-atomic rules:

**Problem 1: Keyword Prefix Matching** -- `^"OR"` matches the "OR" prefix of "ORDER", causing `ORDER BY` to be parsed as `OR` + `DER BY`.

**Problem 2: Implicit Whitespace Consumption** -- Non-atomic rules insert implicit WHITESPACE matches, breaking word boundary lookaheads.

**Problem 3: Operator Ordering Ambiguity** -- `<` matching before `<>` gets a chance.

## Decision

**We will use atomic rules for keyword operators and enforce strict PEG ordering for ambiguous alternatives.**

### Atomic Keyword Rules

```pest
and_op = @{ ^"AND" ~ !(ASCII_ALPHANUMERIC | "_") }
or_op  = @{ ^"OR"  ~ !(ASCII_ALPHANUMERIC | "_") }
not_op = @{ ^"NOT" ~ !(ASCII_ALPHANUMERIC | "_") }
in_op  = @{ ^"IN"  ~ !(ASCII_ALPHANUMERIC | "_") }
```

### Reserved Word Protection

```pest
variable = @{ !reserved ~ (ASCII_ALPHA | "_") ~ (ASCII_ALPHANUMERIC | "_")* }
```

### PEG Ordering Rules

Alternatives ordered longest-match-first:
```pest
comparison_op = { "<>" | "<=" | ">=" | "<" | ">" | "=" }
```

## Consequences

### Positive

- Keywords correctly disambiguated from identifiers in all cases
- `ORDER BY` no longer parsed as `OR` + `DER BY`
- Grammar is self-documenting -- atomic rules make word boundary handling explicit

### Negative

- Atomic rules require explicit whitespace handling in compound keywords
- Adding new keywords requires updating the `reserved` rule

## Related Decisions

- [ADR-007](./007-volcano.md): Query execution depends on correct parsing
- [ADR-011](./011-cypher-crud.md): CRUD keywords follow the same atomic pattern

---

**Last Updated**: 2025-12-20
**Status**: Accepted and Implemented
