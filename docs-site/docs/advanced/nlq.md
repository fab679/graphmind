---
sidebar_position: 3
title: Natural Language Queries
description: Query the graph using natural language with LLM translation
---

# Natural Language Queries (NLQ)

Graphmind can translate natural language questions into Cypher queries using an LLM provider. This lets users who do not know Cypher explore the graph by asking questions in plain language.

## How It Works

1. You send a natural language question (e.g., "Who are Alice's friends?")
2. Graphmind injects the current graph schema (labels, edge types, properties) into the prompt
3. The LLM generates a Cypher query
4. Graphmind validates the query for safety (read-only by default)
5. The query is executed and results are returned

## Setting Up an LLM Provider

Set one of these environment variables before starting the server:

| Variable | Provider | Default Model |
|----------|----------|---------------|
| `OPENAI_API_KEY=sk-...` | OpenAI | `gpt-4o-mini` |
| `GEMINI_API_KEY=...` | Google Gemini | `gemini-2.0-flash` |
| `CLAUDE_CODE_NLQ=1` | Claude Code CLI | Uses local Claude Code |

Override the model with `OPENAI_MODEL` or `GEMINI_MODEL`:

```bash
OPENAI_API_KEY=sk-... OPENAI_MODEL=gpt-4o graphmind
```

With Docker:

```bash
docker run -d -p 6379:6379 -p 8080:8080 \
  -e OPENAI_API_KEY=sk-... \
  ghcr.io/fab679/graphmind:latest
```

## Using NLQ

### HTTP API

```bash
curl -X POST http://localhost:8080/api/nlq \
  -H 'Content-Type: application/json' \
  -d '{"query": "Who are Alice'\''s friends?", "graph": "default"}'
```

Response:

```json
{
  "cypher": "MATCH (a:Person {name:'Alice'})-[:KNOWS]-(b) RETURN a, b",
  "provider": "OpenAI",
  "model": "gpt-4o-mini",
  "columns": ["a", "b"],
  "records": [["Alice", "Bob"]],
  "nodes": [...],
  "edges": [...]
}
```

The response includes the generated Cypher so you can see exactly what was executed.

### Web Visualizer

Toggle NLQ mode in the editor toolbar. When enabled, the editor accepts natural language instead of Cypher:

```
What companies have more than 100 employees?
```

The generated Cypher appears below the input, and results are displayed in the table and on the graph canvas as usual.

### TypeScript SDK

```typescript
const result = await client.nlq("Who are Alice's friends?");
console.log(result.columns, result.records);
```

## Example Questions

Here are examples of natural language questions and the Cypher they produce (results depend on your data):

| Question | Generated Cypher |
|----------|-----------------|
| "How many people are in the graph?" | `MATCH (p:Person) RETURN count(p)` |
| "Who knows Bob?" | `MATCH (p)-[:KNOWS]->(b:Person {name:'Bob'}) RETURN p.name` |
| "What cities do people live in?" | `MATCH (p:Person)-[:LIVES_IN]->(c:City) RETURN DISTINCT c.name` |
| "Find the most connected person" | `MATCH (p:Person)-[:KNOWS]->(f) RETURN p.name, count(f) AS friends ORDER BY friends DESC LIMIT 1` |
| "Show me Alice's company" | `MATCH (a:Person {name:'Alice'})-[:WORKS_AT]->(c:Company) RETURN c.name` |

## Safety

The NLQ pipeline validates generated Cypher before execution:

- Only read-only queries are allowed by default (`MATCH`, `RETURN`, `CALL`)
- Write operations (`CREATE`, `DELETE`, `SET`, `MERGE`) are blocked
- Markdown code fences are automatically stripped from LLM output
- Invalid Cypher is rejected with an error message

## Tips for Better Results

- **Load demo data first** -- NLQ works best when the graph has a clear schema the LLM can use
- **Be specific** -- "Find people older than 30 who work at Acme Corp" produces better results than "find some people"
- **Use entity names** -- refer to actual labels and property values in your data (e.g., "Alice" not "the first user")
- **Schema awareness** -- the NLQ pipeline automatically sends the graph schema to the LLM, so it knows what labels, edge types, and properties exist

## Limitations

- NLQ requires an external LLM provider (not included in Graphmind)
- Complex queries with multiple aggregations or subqueries may not translate perfectly
- The LLM may generate syntactically valid Cypher that does not match your data's schema
- Each NLQ request incurs an LLM API call (latency and cost)
