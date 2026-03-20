---
sidebar_position: 5
title: Functions Reference
description: All built-in functions available in Graphmind Cypher
---

# Functions Reference

Graphmind includes 50+ built-in functions. This page lists them all with examples.

## String Functions

| Function | Description | Example |
|----------|-------------|---------|
| `toUpper(s)` | Uppercase | `toUpper("hello")` -> `"HELLO"` |
| `toLower(s)` | Lowercase | `toLower("Hello")` -> `"hello"` |
| `trim(s)` | Remove leading/trailing whitespace | `trim("  hi  ")` -> `"hi"` |
| `ltrim(s)` | Remove leading whitespace | `ltrim("  hi  ")` -> `"hi  "` |
| `rtrim(s)` | Remove trailing whitespace | `rtrim("  hi  ")` -> `"  hi"` |
| `replace(s, from, to)` | Replace substring | `replace("hello", "l", "r")` -> `"herro"` |
| `substring(s, start, len)` | Extract substring | `substring("hello", 1, 3)` -> `"ell"` |
| `left(s, n)` | First n characters | `left("hello", 3)` -> `"hel"` |
| `right(s, n)` | Last n characters | `right("hello", 3)` -> `"llo"` |
| `reverse(s)` | Reverse a string | `reverse("hello")` -> `"olleh"` |
| `toString(v)` | Convert to string | `toString(42)` -> `"42"` |
| `size(s)` | String length | `size("hello")` -> `5` |

### String function examples

```cypher
MATCH (p:Person)
RETURN toUpper(p.name) AS upper_name,
       size(p.name) AS name_length
```

```cypher
MATCH (p:Person)
WHERE toLower(p.name) STARTS WITH "al"
RETURN p.name
```

## Numeric Functions

| Function | Description | Example |
|----------|-------------|---------|
| `abs(n)` | Absolute value | `abs(-5)` -> `5` |
| `ceil(n)` | Round up | `ceil(2.3)` -> `3.0` |
| `floor(n)` | Round down | `floor(2.7)` -> `2.0` |
| `round(n)` | Round to nearest | `round(2.5)` -> `3.0` |
| `sqrt(n)` | Square root | `sqrt(16)` -> `4.0` |
| `sign(n)` | Sign (-1, 0, 1) | `sign(-5)` -> `-1` |
| `rand()` | Random float between 0 and 1 | `rand()` -> `0.7231...` |
| `log(n)` | Natural logarithm | `log(2.718)` -> `~1.0` |
| `exp(n)` | Euler's number raised to power | `exp(1)` -> `2.718...` |
| `toInteger(v)` | Convert to integer | `toInteger("42")` -> `42` |
| `toFloat(v)` | Convert to float | `toFloat("3.14")` -> `3.14` |

### Numeric function examples

```cypher
MATCH (p:Person)
RETURN p.name, abs(p.age - 30) AS distance_from_30
ORDER BY distance_from_30
```

```cypher
MATCH (p:Person)
RETURN round(avg(p.age)) AS rounded_avg
```

## List Functions

| Function | Description | Example |
|----------|-------------|---------|
| `size(list)` | Number of elements | `size([1,2,3])` -> `3` |
| `head(list)` | First element | `head([1,2,3])` -> `1` |
| `last(list)` | Last element | `last([1,2,3])` -> `3` |
| `tail(list)` | All except first | `tail([1,2,3])` -> `[2,3]` |
| `keys(node)` | Property keys | `keys(n)` -> `["name","age"]` |
| `range(start, end)` | Integer range | `range(1, 5)` -> `[1,2,3,4,5]` |
| `length(path)` | Length of a path (number of relationships) | `length(p)` -> `3` |

### List function examples

```cypher
MATCH (p:Person)
RETURN p.name, keys(p) AS properties
```

```cypher
UNWIND range(1, 10) AS i
RETURN i, i * i AS square
```

## Node and Relationship Functions

| Function | Description | Example |
|----------|-------------|---------|
| `id(n)` | Internal node/edge ID | `id(n)` -> `42` |
| `labels(n)` | Node labels | `labels(n)` -> `["Person"]` |
| `type(r)` | Relationship type | `type(r)` -> `"KNOWS"` |
| `exists(expr)` | Check if property exists | `exists(n.email)` -> `true` |
| `coalesce(a, b, ...)` | First non-null value | `coalesce(n.nick, n.name)` -> `"Alice"` |
| `nodes(path)` | List of nodes in a path | `nodes(p)` -> `[node1, node2]` |
| `relationships(path)` | List of relationships in a path | `relationships(p)` -> `[rel1]` |

### Node function examples

```cypher
-- Get all labels and their counts
MATCH (n)
RETURN labels(n) AS label, count(n) AS count
ORDER BY count DESC
```

```cypher
-- Find nodes missing email
MATCH (p:Person)
WHERE NOT exists(p.email)
RETURN p.name
```

```cypher
-- Display name with fallback
MATCH (p:Person)
RETURN coalesce(p.nickname, p.name) AS display_name
```

## Aggregate Functions

| Function | Description |
|----------|-------------|
| `count(x)` | Count non-null values (or `count(*)` for all rows) |
| `sum(x)` | Sum of numeric values |
| `avg(x)` | Average of numeric values |
| `min(x)` | Minimum value |
| `max(x)` | Maximum value |
| `collect(x)` | Collect values into a list |

See [Aggregations](aggregations) for detailed examples and grouping behavior.

## Temporal Functions

| Function | Description | Example |
|----------|-------------|---------|
| `date()` | Current date | `date()` -> `2026-03-20` |
| `date(string)` | Parse date from string | `date("2024-01-15")` |
| `date(map)` | Construct date from components | `date({year: 2024, month: 1, day: 15})` |
| `datetime()` | Current date and time | `datetime()` -> `2026-03-20T14:30:00Z` |
| `datetime(string)` | Parse datetime from string | `datetime("2024-01-15T10:30:00")` |
| `datetime(map)` | Construct datetime from components | `datetime({year: 2024, month: 1, day: 15, hour: 10})` |
| `duration(map)` | Construct a duration | `duration({days: 14, hours: 3})` |
| `duration(string)` | Parse ISO 8601 duration | `duration("P14DT3H")` |
| `timestamp()` | Current time as epoch milliseconds | `timestamp()` -> `1710936000000` |

### Temporal function examples

```cypher
RETURN date() AS today, datetime() AS now, timestamp() AS epoch_ms
```

```cypher
RETURN date("2024-06-15") AS parsed_date,
       datetime({year: 2024, month: 6, day: 15, hour: 12}) AS constructed
```

```cypher
RETURN duration({days: 30}) AS one_month,
       duration("P1Y2M") AS iso_duration
```

## Predicate Functions

These functions test every element in a list against a condition.

| Function | Description |
|----------|-------------|
| `all(x IN list WHERE predicate)` | True if predicate holds for all elements |
| `any(x IN list WHERE predicate)` | True if predicate holds for at least one element |
| `none(x IN list WHERE predicate)` | True if predicate holds for no elements |
| `single(x IN list WHERE predicate)` | True if predicate holds for exactly one element |

### Predicate function examples

```cypher
WITH [1, 2, 3, 4, 5] AS numbers
RETURN all(x IN numbers WHERE x > 0) AS all_positive,
       any(x IN numbers WHERE x > 4) AS has_large,
       none(x IN numbers WHERE x < 0) AS none_negative,
       single(x IN numbers WHERE x = 3) AS exactly_one_three
```

## Unsupported Functions

These OpenCypher functions are not yet implemented:

- `split(s, delimiter)` -- split string into list
- `log10(n)`, `e()`, `pi()` -- base-10 logarithm, mathematical constants
- `sin`, `cos`, `tan`, `asin`, `acos`, `atan`, `atan2` -- trigonometric functions
- `degrees(n)`, `radians(n)` -- angle conversion
- `elementId(n)` -- element identifier (use `id(n)` instead)
- `properties(n)` -- return all properties as map (use `keys(n)` + property access)
- `startNode(r)`, `endNode(r)` -- start/end node of a relationship
- `point(map)`, `distance(p1, p2)` -- spatial functions
- `collect(DISTINCT x)` -- distinct aggregation
