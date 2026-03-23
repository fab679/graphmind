import type {
  QueryResult,
  ServerStatus,
  ClientOptions,
  GraphSchema,
  SampleRequest,
  SampleResult,
  CsvImportResult,
  JsonImportResult,
} from "./types.js";
import { HttpTransport } from "./http-client.js";

const DEFAULT_URL = "http://localhost:8080";

/**
 * Client for the Graphmind Graph Database.
 *
 * @example
 * ```ts
 * const client = new GraphmindClient({ url: "http://localhost:8080" });
 *
 * // Create data
 * await client.query('CREATE (n:Person {name: "Alice"})');
 *
 * // Query data
 * const result = await client.queryReadonly("MATCH (n:Person) RETURN n.name");
 * console.log(result.records);
 *
 * // Schema introspection
 * const schema = await client.schema();
 * console.log(schema.node_types);
 *
 * // EXPLAIN / PROFILE
 * const plan = await client.explain("MATCH (n:Person) RETURN n");
 * const profile = await client.profile("MATCH (n:Person) RETURN n");
 * ```
 */
export class GraphmindClient {
  private http: HttpTransport;
  private defaultGraph: string;

  constructor(options?: ClientOptions) {
    const url = options?.url ?? DEFAULT_URL;
    this.defaultGraph = options?.graph ?? "default";
    const headers: Record<string, string> = {};
    if (options?.token) {
      headers["Authorization"] = `Bearer ${options.token}`;
    }
    this.http = new HttpTransport(url, headers);
  }

  /**
   * Connect to a Graphmind server via HTTP.
   * Factory method for a more readable API.
   */
  static connectHttp(url: string = DEFAULT_URL): GraphmindClient {
    return new GraphmindClient({ url });
  }

  /** Execute a read-write Cypher query */
  async query(cypher: string, graph: string = "default"): Promise<QueryResult> {
    return this.http.query(cypher, graph);
  }

  /** Execute a read-only Cypher query */
  async queryReadonly(cypher: string, graph: string = "default"): Promise<QueryResult> {
    return this.http.query(cypher, graph);
  }

  /**
   * Return the EXPLAIN plan for a Cypher query without executing it.
   * Returns the plan as text rows in the QueryResult records.
   */
  async explain(cypher: string, graph: string = "default"): Promise<QueryResult> {
    const prefixed = cypher.trimStart().toUpperCase().startsWith("EXPLAIN")
      ? cypher
      : `EXPLAIN ${cypher}`;
    return this.http.query(prefixed, graph);
  }

  /**
   * Execute a Cypher query with PROFILE instrumentation.
   * Returns plan text with actual row counts and timing per operator.
   */
  async profile(cypher: string, graph: string = "default"): Promise<QueryResult> {
    const prefixed = cypher.trimStart().toUpperCase().startsWith("PROFILE")
      ? cypher
      : `PROFILE ${cypher}`;
    return this.http.query(prefixed, graph);
  }

  /** Delete a graph (executes MATCH (n) DELETE n) */
  async deleteGraph(graph: string = "default"): Promise<void> {
    await this.http.query("MATCH (n) DELETE n", graph);
  }

  /** List graphs (fetches graph name from server status) */
  async listGraphs(): Promise<string[]> {
    const s = await this.http.status(this.defaultGraph);
    // OSS mode returns a single graph; use the status graph field if present
    const graph = (s as any).graph ?? "default";
    return [graph];
  }

  /** Get server status */
  async status(graph?: string): Promise<ServerStatus> {
    return this.http.status(graph || this.defaultGraph);
  }

  /** Get the database server version */
  async version(): Promise<string> {
    const s = await this.status();
    return s.version;
  }

  /** Get graph schema (node types, edge types, indexes, constraints, statistics) */
  async schema(graph?: string): Promise<GraphSchema> {
    return this.http.schema(graph || this.defaultGraph);
  }

  /**
   * Sample a subgraph for visualization.
   * Returns a proportionally sampled set of nodes and edges.
   * @param options - max_nodes (default 200), labels filter, graph/tenant name
   */
  async sample(options: SampleRequest = {}): Promise<SampleResult> {
    return this.http.post<SampleResult>("/api/sample", options);
  }

  /**
   * Import nodes from CSV content.
   * @param csvContent - Raw CSV string (first row = headers)
   * @param label - Node label to assign
   * @param options - Optional: idColumn, delimiter
   */
  async importCsv(
    csvContent: string,
    label: string,
    options?: { idColumn?: string; delimiter?: string },
  ): Promise<CsvImportResult> {
    return this.http.importCsv(csvContent, label, options);
  }

  /**
   * Import nodes from JSON objects.
   * @param label - Node label to assign
   * @param nodes - Array of objects, each becoming a node
   */
  async importJson(
    label: string,
    nodes: Record<string, unknown>[],
  ): Promise<JsonImportResult> {
    return this.http.importJson(label, nodes);
  }

  /**
   * Execute a multi-statement Cypher script.
   * Each semicolon-separated statement is executed in order.
   */
  async executeScript(
    script: string,
    graph: string = this.defaultGraph,
  ): Promise<QueryResult[]> {
    const statements = script
      .split(";")
      .map((s) => s.trim())
      .filter((s) => s.length > 0);
    const results: QueryResult[] = [];
    for (const stmt of statements) {
      results.push(await this.query(stmt, graph));
    }
    return results;
  }

  /**
   * Translate a natural-language question into Cypher and execute it.
   * Requires the server to have NLQ enabled.
   */
  async nlq(
    question: string,
    graph: string = this.defaultGraph,
  ): Promise<QueryResult> {
    return this.http.post<QueryResult>("/api/nlq", {
      query: question,
      graph,
    });
  }

  /** Ping the server */
  async ping(): Promise<string> {
    const s = await this.status();
    if (s.status === "healthy") {
      return "PONG";
    }
    throw new Error(`Server unhealthy: ${s.status}`);
  }
}
