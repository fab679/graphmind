import type {
  QueryResponse,
  SampleRequest,
  SampleResponse,
  SchemaResponse,
  StatusResponse,
} from "../types/api";

const API_BASE = import.meta.env.VITE_API_URL ?? "";
const AUTH_STORAGE_KEY = "graphmind-auth-token";

// --- Auth token management ---

let authToken: string | null = localStorage.getItem(AUTH_STORAGE_KEY);

export function getAuthToken(): string | null {
  return authToken;
}

export function setAuthToken(token: string | null) {
  authToken = token;
  if (token) {
    localStorage.setItem(AUTH_STORAGE_KEY, token);
  } else {
    localStorage.removeItem(AUTH_STORAGE_KEY);
  }
}

export function isAuthenticated(): boolean {
  return authToken !== null;
}

// --- Request helper ---

class ApiError extends Error {
  status: number;
  constructor(status: number, message: string) {
    super(message);
    this.name = "ApiError";
    this.status = status;
  }
}

async function request<T>(
  path: string,
  options: RequestInit = {},
): Promise<T> {
  const url = `${API_BASE}${path}`;
  const headers: Record<string, string> = {
    "Content-Type": "application/json",
    ...(options.headers as Record<string, string>),
  };

  // Add auth token if set
  if (authToken) {
    headers["Authorization"] = `Bearer ${authToken}`;
  }

  const response = await fetch(url, {
    ...options,
    headers,
  });

  if (!response.ok) {
    const body = await response.text().catch(() => "Unknown error");
    throw new ApiError(response.status, body);
  }

  return response.json() as Promise<T>;
}

// --- API functions ---

export async function executeQuery(
  query: string,
  graph?: string,
): Promise<QueryResponse> {
  return request<QueryResponse>("/api/query", {
    method: "POST",
    body: JSON.stringify({ query, graph }),
  });
}

export async function getStatus(): Promise<StatusResponse> {
  return request<StatusResponse>("/api/status");
}

export async function getSchema(): Promise<SchemaResponse> {
  return request<SchemaResponse>("/api/schema");
}

export async function sampleGraph(
  params: SampleRequest,
): Promise<SampleResponse> {
  return request<SampleResponse>("/api/graph/sample", {
    method: "POST",
    body: JSON.stringify(params),
  });
}

export interface ScriptResponse {
  status: string;
  executed: number;
  errors: string[];
  storage: { nodes: number; edges: number };
}

export async function executeScript(
  script: string,
  graph = "default",
): Promise<ScriptResponse> {
  return request<ScriptResponse>("/api/script", {
    method: "POST",
    body: JSON.stringify({ query: script, graph }),
  });
}

export interface NlqResponse {
  cypher: string | null;
  error?: string;
  provider?: string;
  model?: string;
  schema_summary?: string;
}

export async function translateNlq(
  question: string,
  graph = "default",
): Promise<NlqResponse> {
  return request<NlqResponse>("/api/nlq", {
    method: "POST",
    body: JSON.stringify({ query: question, graph }),
  });
}

export async function listGraphs(): Promise<string[]> {
  const result = await request<{ graphs: string[] }>("/api/graphs");
  return result.graphs;
}

export async function deleteGraph(name: string): Promise<void> {
  await request<unknown>(`/api/graphs/${encodeURIComponent(name)}`, {
    method: "DELETE",
  });
}

export { ApiError };
