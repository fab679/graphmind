import type {
  QueryResult,
  ServerStatus,
  ErrorResponse,
  GraphSchema,
  CsvImportResult,
  JsonImportResult,
} from "./types.js";

/**
 * HTTP transport for the Graphmind SDK.
 * Uses the native `fetch` API (works in Node.js 18+ and browsers).
 */
export class HttpTransport {
  private baseUrl: string;
  private extraHeaders: Record<string, string>;

  constructor(baseUrl: string, extraHeaders: Record<string, string> = {}) {
    this.baseUrl = baseUrl.replace(/\/+$/, "");
    this.extraHeaders = extraHeaders;
  }

  /** Execute a Cypher query via POST /api/query */
  async query(cypher: string, graph: string = "default"): Promise<QueryResult> {
    const response = await fetch(`${this.baseUrl}/api/query`, {
      method: "POST",
      headers: { "Content-Type": "application/json", ...this.extraHeaders },
      body: JSON.stringify({ query: cypher, graph }),
    });

    if (!response.ok) {
      const body = (await response.json().catch(() => ({
        error: `HTTP ${response.status}`,
      }))) as ErrorResponse;
      throw new Error(body.error || `HTTP ${response.status}`);
    }

    return (await response.json()) as QueryResult;
  }

  /** Get server status via GET /api/status */
  async status(graph?: string): Promise<ServerStatus> {
    const q = graph ? `?graph=${encodeURIComponent(graph)}` : "";
    const response = await fetch(`${this.baseUrl}/api/status${q}`, {
      headers: this.extraHeaders,
    });

    if (!response.ok) {
      throw new Error(`Status endpoint returned ${response.status}`);
    }

    return (await response.json()) as ServerStatus;
  }

  /** Get graph schema via GET /api/schema */
  async schema(graph?: string): Promise<GraphSchema> {
    const q = graph ? `?graph=${encodeURIComponent(graph)}` : "";
    const response = await fetch(`${this.baseUrl}/api/schema${q}`, {
      headers: this.extraHeaders,
    });

    if (!response.ok) {
      const body = (await response.json().catch(() => ({
        error: `HTTP ${response.status}`,
      }))) as ErrorResponse;
      throw new Error(body.error || `HTTP ${response.status}`);
    }

    return (await response.json()) as GraphSchema;
  }

  /** Generic POST request returning typed JSON response */
  async post<T>(path: string, body: unknown): Promise<T> {
    const response = await fetch(`${this.baseUrl}${path}`, {
      method: "POST",
      headers: { "Content-Type": "application/json", ...this.extraHeaders },
      body: JSON.stringify(body),
    });

    if (!response.ok) {
      const err = (await response.json().catch(() => ({
        error: `HTTP ${response.status}`,
      }))) as ErrorResponse;
      throw new Error(err.error || `HTTP ${response.status}`);
    }

    return (await response.json()) as T;
  }

  /** Import nodes from CSV via POST /api/import/csv (multipart) */
  async importCsv(
    csvContent: string,
    label: string,
    options?: { idColumn?: string; delimiter?: string },
  ): Promise<CsvImportResult> {
    const formData = new FormData();
    const blob = new Blob([csvContent], { type: "text/csv" });
    formData.append("file", blob, "import.csv");
    formData.append("label", label);
    if (options?.idColumn) formData.append("id_column", options.idColumn);
    if (options?.delimiter) formData.append("delimiter", options.delimiter);

    const response = await fetch(`${this.baseUrl}/api/import/csv`, {
      method: "POST",
      headers: this.extraHeaders,
      body: formData,
    });

    if (!response.ok) {
      const body = (await response.json().catch(() => ({
        error: `HTTP ${response.status}`,
      }))) as ErrorResponse;
      throw new Error(body.error || `HTTP ${response.status}`);
    }

    return (await response.json()) as CsvImportResult;
  }

  /** Import nodes from JSON via POST /api/import/json */
  async importJson(
    label: string,
    nodes: Record<string, unknown>[],
  ): Promise<JsonImportResult> {
    const response = await fetch(`${this.baseUrl}/api/import/json`, {
      method: "POST",
      headers: { "Content-Type": "application/json", ...this.extraHeaders },
      body: JSON.stringify({ label, nodes }),
    });

    if (!response.ok) {
      const body = (await response.json().catch(() => ({
        error: `HTTP ${response.status}`,
      }))) as ErrorResponse;
      throw new Error(body.error || `HTTP ${response.status}`);
    }

    return (await response.json()) as JsonImportResult;
  }
}
