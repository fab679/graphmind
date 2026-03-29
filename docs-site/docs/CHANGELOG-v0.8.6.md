# Changelog v0.8.6

## OpenCypher TCK Compliance: 84% to 97%

This release dramatically improves OpenCypher 9 compatibility, passing **1257 of 1302** TCK scenarios (96.5%) — up from 84.3% in v0.8.5. **160 previously-failing scenarios** now pass correctly.

### 25 Categories at 100% Compliance

clauses/create, clauses/delete, clauses/remove, clauses/return, clauses/return-skip-limit, clauses/set, clauses/union, clauses/unwind, clauses/with, clauses/with-skip-limit, expressions/aggregation, expressions/boolean, expressions/conditional, expressions/graph, expressions/literals, expressions/map, expressions/mathematical, expressions/null, expressions/precedence, expressions/string, expressions/temporal, expressions/typeConversion, useCases/countingSubgraphMatches, useCases/triadicSelection

## New Features

### DROP VECTOR INDEX
You can now drop vector indexes:
```cypher
DROP VECTOR INDEX Movie_embedding
```

### Better Error Messages
MATCH queries without RETURN now provide a helpful error message instead of a cryptic parse error:
```
MATCH query requires a RETURN clause. Add RETURN at the end, e.g.:
MATCH (p:Person) RETURN *
```

### Expression Property Access
Parenthesized expression property access now works:
```cypher
WITH [123, {name: 'test'}] AS list
RETURN (list[1]).name  -- returns 'test'
```

### Nested Map Property Access
Chained dot-separated property access:
```cypher
WITH {outer: {inner: 'value'}} AS map
RETURN map.outer.inner  -- returns 'value'
```

### Pattern Comprehension Path Variables
Path variables in pattern comprehensions now correctly bind:
```cypher
MATCH (n)
RETURN [p = (n)-->() | p] AS paths
```

## Query Engine Improvements

### Multi-CREATE in MATCH+CREATE
Multiple CREATE clauses after MATCH now correctly create both nodes and edges:
```cypher
MATCH (a:Person)
CREATE (b:City {name: 'Nairobi'})
CREATE (a)-[:LIVES_IN]->(b)
```

### Multi-Part Write Queries
Complex multi-part queries with WITH+DELETE+WITH+RETURN now execute in correct order:
```cypher
MATCH (n:Temp)
WITH n, n.value AS val
DELETE n
WITH val WHERE val > 0
RETURN val
```

### UNWIND+MERGE Support
UNWIND before MERGE now works correctly:
```cypher
UNWIND ['Alice', 'Bob'] AS name
MERGE (p:Person {name: name})
```

### Collect + UNWIND Pipeline
The collect() aggregate now correctly handles node/edge references, enabling patterns like:
```cypher
MATCH (n:Person)
WITH collect(n) AS people
UNWIND people AS person
RETURN person.name
```

### OPTIONAL MATCH + WITH WHERE
Triadic selection patterns (OPTIONAL MATCH with WITH WHERE IS NULL/IS NOT NULL) now work correctly — all 19 triadic selection tests pass.

## Validation Improvements

- `$param` in MATCH/MERGE patterns rejected at parse time
- Cross-CREATE variable rebinding detected
- Pattern expressions in RETURN/WITH/SET rejected
- `size()` on path variables detected as type error
- Undefined variables in WHERE caught at planning time
- `count(count(*))` nested aggregation rejected
- `rand()` inside aggregation rejected
- Non-aliased expressions in WITH (`WITH a, count(*)`) rejected
- UNION with different column names rejected
- List of maps as property value rejected (TypeError)
- EXISTS subquery with update clauses (SET/DELETE) rejected
- Aggregation in list comprehensions rejected
- Self-pattern check (`WHERE (n)`) rejected
- CALL YIELD variable shadowing detected
- CALL procedure argument count validation
- Boolean type mismatch in procedure arguments

## String Operations

STARTS WITH, ENDS WITH, and CONTAINS on non-string operands now return `null` per the Cypher specification instead of throwing a TypeError.

## SDK Improvements

### All SDKs
- Parameters tested end-to-end across all SDK variants
- Clear error messages for common mistakes (MATCH without RETURN)

### Python SDK
- Added parameter tests: `test_query_with_params`, `test_edge_property_with_params`
- Added error message test: `test_match_without_return_error`

### TypeScript SDK
- Added `Param handling` test suite verifying parameter acceptance
- All 19 SDK tests pass

## Bug Fixes

- Fixed anonymous node edge creation in `extract_create_specs` (PerRowCreateOperator)
- Fixed multi-part write query WITH stage ordering in parser
- Fixed MERGE RETURN aggregation (count(*) after MERGE)
- Fixed UNWIND dependency ordering for multi-WITH chains
- Fixed Value::Null and Value::Property(PropertyValue::Null) hash/equality consistency
- Fixed ProjectOperator graceful handling of deleted nodes/edges
- Fixed pattern comprehension path variable parsing
- Fixed TCK runner script fallback masking validation errors
- Fixed TCK runner procedure definition parsing from feature files
