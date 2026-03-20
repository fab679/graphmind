---
sidebar_position: 5
title: Functions Reference
description: All built-in functions available in Graphmind Cypher
---

# Functions Reference

Graphmind includes 30+ built-in functions. This page lists them all with examples.

## String Functions

| Function | Description | Example |
|----------|-------------|---------|
| `toUpper(s)` | Uppercase | `toUpper("hello")` -> `"HELLO"` |
| `toLower(s)` | Lowercase | `toLower("Hello")` -> `"hello"` |
| `trim(s)` | Remove leading/trailing whitespace | `trim("  hi  ")` -> `"hi"` |
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

## Unsupported Functions

These OpenCypher functions are not yet implemented:

- `split(s, delimiter)` -- split string into list
- `rand()` -- random float
- `log(n)`, `exp(n)` -- logarithm and exponent
- `nodes(path)`, `relationships(path)` -- extract from named paths
- `timestamp()` -- current time
- `collect(DISTINCT x)` -- distinct aggregation
