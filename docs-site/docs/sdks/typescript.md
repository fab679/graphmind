---
sidebar_position: 4
title: TypeScript SDK
description: TypeScript/Node.js client for the Graphmind graph database
---

# TypeScript SDK

The TypeScript SDK (`graphmind-sdk`) provides a fully typed HTTP client for connecting to a running Graphmind server. It supports queries, schema introspection, NLQ, import, and sampling.

## Installation

```bash
# npm
npm install graphmind-sdk

# yarn
yarn add graphmind-sdk

# pnpm
pnpm add graphmind-sdk
```

## Quick Start

```typescript
import { GraphmindClient } from "graphmind-sdk";

const client = new GraphmindClient({ url: "http://localhost:8080" });

// Create data
await client.query('CREATE (a:Person {name: "Alice", age: 30})');
await client.query('CREATE (b:Person {name: "Bob", age: 25})');
await client.query(`
  MATCH (a:Person {name: "Alice"}), (b:Person {name: "Bob"})
  CREATE (a)-[:KNOWS {since: 2020}]->(b)
`);

// Query data
const result = await client.queryReadonly(
  "MATCH (p:Person) RETURN p.name, p.age ORDER BY p.age"
);
console.log(result.columns);  // ['p.name', 'p.age']
console.log(result.records);  // [['Alice', 30], ['Bob', 25]]
```

## Client Options

```typescript
const client = new GraphmindClient({
  url: "http://localhost:8080",   // Server URL (default: http://localhost:8080)
  graph: "my_graph",              // Default graph namespace (default: "default")
  token: "your-bearer-token",     // Optional auth token
});

// Factory method alternative
const client2 = GraphmindClient.connectHttp("http://localhost:8080");
```

## Authentication

If the server requires authentication, pass a bearer token:

```typescript
const client = new GraphmindClient({
  url: "http://localhost:8080",
  token: process.env.GRAPHMIND_TOKEN,
});
```

The token is sent as an `Authorization: Bearer <token>` header on every request.

## CRUD Operations

### CREATE

```typescript
// Nodes with properties
await client.query('CREATE (p:Person {name: "Carol", age: 28, active: true})');

// Edges with properties
await client.query(`
  MATCH (a:Person {name: "Alice"}), (c:Person {name: "Carol"})
  CREATE (a)-[:WORKS_WITH {project: "GraphDB", since: 2023}]->(c)
`);
```

### MATCH with WHERE

```typescript
const result = await client.queryReadonly(`
  MATCH (p:Person)
  WHERE p.age > 25 AND p.active = true
  RETURN p.name, p.age
  ORDER BY p.age DESC
  LIMIT 10
`);

for (const [name, age] of result.records) {
  console.log(`${name} is ${age} years old`);
}
```

### SET (update)

```typescript
await client.query(
  'MATCH (p:Person {name: "Alice"}) SET p.age = 31, p.title = "Engineer"'
);
```

### DELETE

```typescript
await client.query('MATCH (p:Person {name: "Bob"}) DELETE p');
```

### MERGE (upsert)

```typescript
await client.query('MERGE (p:Person {name: "Dave"}) SET p.age = 35');
```

## Aggregations

```typescript
const result = await client.queryReadonly(`
  MATCH (p:Person)
  RETURN count(p) AS total,
         avg(p.age) AS avg_age,
         min(p.age) AS youngest,
         max(p.age) AS oldest,
         collect(p.name) AS names
`);

const [total, avgAge, youngest, oldest, names] = result.records[0];
console.log(`${total} people, avg age ${avgAge}`);
```

### GROUP BY

```typescript
const result = await client.queryReadonly(`
  MATCH (p:Person)-[:WORKS_AT]->(c:Company)
  RETURN c.name, count(p) AS employees, avg(p.age) AS avg_age
  ORDER BY employees DESC
`);

for (const [company, count, avg] of result.records) {
  console.log(`${company}: ${count} employees, avg age ${avg}`);
}
```

## Traversals

```typescript
// Multi-hop
const fof = await client.queryReadonly(`
  MATCH (a:Person {name: "Alice"})-[:KNOWS]->(b)-[:KNOWS]->(c)
  WHERE a <> c
  RETURN DISTINCT c.name AS friend_of_friend
`);

// Variable-length paths
const reachable = await client.queryReadonly(`
  MATCH (a:Person {name: "Alice"})-[:KNOWS*1..3]->(b:Person)
  RETURN DISTINCT b.name
`);
```

## Schema Introspection

```typescript
const schema = await client.schema();
console.log("Node types:", schema.node_types);
console.log("Edge types:", schema.edge_types);
console.log("Indexes:", schema.indexes);
console.log("Statistics:", schema.statistics);
```

The `GraphSchema` object contains:

| Field | Type | Description |
|-------|------|-------------|
| `node_types` | `NodeType[]` | Label, count, properties |
| `edge_types` | `EdgeType[]` | Type, count, source/target labels |
| `indexes` | `IndexInfo[]` | Label, property, index type |
| `constraints` | `ConstraintInfo[]` | Label, property, constraint type |
| `statistics` | `object` | Total nodes, edges, avg degree |

## EXPLAIN and PROFILE

```typescript
// Show the query plan without executing
const plan = await client.explain("MATCH (p:Person)-[:KNOWS]->(f) RETURN f.name");
for (const row of plan.records) {
  console.log(row);
}

// Execute with profiling instrumentation
const profile = await client.profile("MATCH (p:Person)-[:KNOWS]->(f) RETURN f.name");
for (const row of profile.records) {
  console.log(row);
}
```

## Natural Language Queries

Translate natural language to Cypher (requires the server to have NLQ enabled with an LLM provider):

```typescript
const result = await client.nlq("Who are Alice's friends?");
console.log(result);  // QueryResult with the generated Cypher results
```

## Script Execution

Execute multi-statement Cypher scripts:

```typescript
const results = await client.executeScript(`
  CREATE (a:City {name: "Paris"});
  CREATE (b:City {name: "London"});
  MATCH (a:City {name: "Paris"}), (b:City {name: "London"})
  CREATE (a)-[:CONNECTED_TO {distance: 450}]->(b)
`);

console.log(`Executed ${results.length} statements`);
```

## Data Import

### CSV Import

```typescript
const csvData = `name,age,city
Alice,30,Paris
Bob,25,London`;

const result = await client.importCsv(csvData, "Person", {
  idColumn: "name",
  delimiter: ",",
});
console.log(`Imported ${result.nodes_created} nodes`);
```

### JSON Import

```typescript
const nodes = [
  { name: "Alice", age: 30, city: "Paris" },
  { name: "Bob", age: 25, city: "London" },
];

const result = await client.importJson("Person", nodes);
console.log(`Imported ${result.nodes_created} nodes`);
```

## Subgraph Sampling

Sample a subset of the graph for visualization:

```typescript
const sample = await client.sample({
  max_nodes: 200,
  labels: ["Person", "Company"],
  graph: "default",
});

console.log(`Sampled ${sample.sampled_nodes}/${sample.total_nodes} nodes`);
console.log(`Sampled ${sample.sampled_edges}/${sample.total_edges} edges`);
```

## Multi-tenancy

```typescript
// Use separate graph namespaces
await client.query('CREATE (n:User {name: "Acme User"})', "tenant_acme");
await client.query('CREATE (n:User {name: "Globex User"})', "tenant_globex");

// Queries are scoped
const acme = await client.queryReadonly("MATCH (n) RETURN count(n)", "tenant_acme");

// List and delete
const graphs = await client.listGraphs();
await client.deleteGraph("tenant_acme");
```

## Error Handling

The client throws standard `Error` objects on failure:

```typescript
try {
  await client.query("INVALID CYPHER");
} catch (error) {
  console.error("Query failed:", (error as Error).message);
}
```

For server health checks:

```typescript
try {
  const pong = await client.ping();
  console.log("Server is up:", pong);  // "PONG"
} catch {
  console.error("Server is down");
}
```

## Integration with Express.js

Build a graph-backed REST API:

```typescript
import express from "express";
import { GraphmindClient } from "graphmind-sdk";

const app = express();
const db = new GraphmindClient({ url: "http://localhost:8080" });

app.use(express.json());

app.get("/api/people", async (req, res) => {
  const result = await db.queryReadonly(
    "MATCH (p:Person) RETURN p.name, p.age ORDER BY p.name"
  );
  res.json(result.records.map(([name, age]) => ({ name, age })));
});

app.get("/api/people/:name/friends", async (req, res) => {
  const result = await db.queryReadonly(
    `MATCH (p:Person {name: "${req.params.name}"})-[:KNOWS]->(f) RETURN f.name`
  );
  res.json(result.records.map(([name]) => name));
});

app.listen(3000);
```

## Integration with Next.js

Use in server components or API routes:

```typescript
// app/api/graph/route.ts
import { GraphmindClient } from "graphmind-sdk";
import { NextResponse } from "next/server";

const db = new GraphmindClient({ url: process.env.GRAPHMIND_URL });

export async function GET() {
  const result = await db.queryReadonly(
    "MATCH (n) RETURN labels(n)[0] AS label, count(n) AS count"
  );
  return NextResponse.json(result.records);
}

export async function POST(request: Request) {
  const { cypher } = await request.json();
  const result = await db.query(cypher);
  return NextResponse.json(result);
}
```

## TypeScript Types Reference

The SDK exports the following interfaces:

| Type | Description |
|------|-------------|
| `ClientOptions` | Constructor options (url, graph, token) |
| `QueryResult` | Query response (columns, records, nodes, edges) |
| `SdkNode` | Node with id, labels, properties |
| `SdkEdge` | Edge with id, source, target, type, properties |
| `ServerStatus` | Health status, version, storage counts |
| `GraphSchema` | Node types, edge types, indexes, statistics |
| `NodeType` | Label descriptor with count and properties |
| `EdgeType` | Edge type descriptor with source/target labels |
| `SampleRequest` | Sampling options (max_nodes, labels, graph) |
| `SampleResult` | Sampled nodes and edges with totals |
| `CsvImportResult` | CSV import response |
| `JsonImportResult` | JSON import response |
| `ErrorResponse` | Error response from server |
