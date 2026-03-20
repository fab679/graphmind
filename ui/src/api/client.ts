import type {
  QueryResponse,
  SampleRequest,
  SampleResponse,
  SchemaResponse,
  StatusResponse,
} from "../types/api";

const API_BASE = import.meta.env.VITE_API_URL ?? "";
const AUTH_STORAGE_KEY = "graphmind-auth-token";
const BASIC_AUTH_STORAGE_KEY = "graphmind-basic-auth";

// --- Auth token management ---

let authToken: string | null = localStorage.getItem(AUTH_STORAGE_KEY);
let basicAuthHeader: string | null = localStorage.getItem(BASIC_AUTH_STORAGE_KEY);

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

/** Set Basic auth credentials. Pass empty username to clear. */
export function setBasicAuth(username: string, password: string) {
  if (username) {
    basicAuthHeader = `Basic ${btoa(`${username}:${password}`)}`;
    localStorage.setItem(BASIC_AUTH_STORAGE_KEY, basicAuthHeader);
    // Clear any token auth when using basic auth
    authToken = null;
    localStorage.removeItem(AUTH_STORAGE_KEY);
  } else {
    basicAuthHeader = null;
    localStorage.removeItem(BASIC_AUTH_STORAGE_KEY);
  }
}

/** Clear all auth state */
export function clearAuth() {
  authToken = null;
  basicAuthHeader = null;
  localStorage.removeItem(AUTH_STORAGE_KEY);
  localStorage.removeItem(BASIC_AUTH_STORAGE_KEY);
}

export function isAuthenticated(): boolean {
  return authToken !== null || basicAuthHeader !== null;
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

  // Add auth header: prefer Basic auth, fall back to Bearer token
  if (basicAuthHeader) {
    headers["Authorization"] = basicAuthHeader;
  } else if (authToken) {
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

// --- Auth API functions ---

export interface LoginResponse {
  authenticated: boolean;
  role: string;
  username: string;
  auth_required?: boolean;
  error?: string;
}

export async function login(
  username: string,
  password: string,
): Promise<LoginResponse> {
  // Temporarily set basic auth for this request
  const prevBasic = basicAuthHeader;
  const prevToken = authToken;
  basicAuthHeader = null;
  authToken = null;

  try {
    const result = await request<LoginResponse>("/api/auth/login", {
      method: "POST",
      body: JSON.stringify({ username, password }),
    });
    // Restore on success — caller will set proper auth
    basicAuthHeader = prevBasic;
    authToken = prevToken;
    return result;
  } catch (err) {
    basicAuthHeader = prevBasic;
    authToken = prevToken;
    throw err;
  }
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
