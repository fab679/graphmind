export interface GraphNode {
  id: string;
  labels: string[];
  properties: Record<string, unknown>;
}

export interface GraphEdge {
  id: string;
  source: string;
  target: string;
  type: string;
  properties: Record<string, unknown>;
}

export interface QueryResponse {
  nodes: GraphNode[];
  edges: GraphEdge[];
  columns: string[];
  records: unknown[][];
  error?: string;
}

export interface StatusResponse {
  status: string;
  version: string;
  storage: { nodes: number; edges: number };
  cache: { hits: number; misses: number; size: number };
}

export interface SchemaNodeType {
  label: string;
  count: number;
  properties: Record<string, string>;
}

export interface SchemaEdgeType {
  type: string;
  count: number;
  source_labels: string[];
  target_labels: string[];
  properties: Record<string, string>;
}

export interface SchemaResponse {
  node_types: SchemaNodeType[];
  edge_types: SchemaEdgeType[];
  indexes: Array<{ label: string; property: string; type: string }>;
  constraints: Array<{ label: string; property: string; type: string }>;
  statistics: { total_nodes: number; total_edges: number; avg_out_degree: number };
}

export interface SampleRequest {
  max_nodes?: number;
  labels?: string[];
  graph?: string;
}

export interface SampleResponse {
  nodes: GraphNode[];
  edges: GraphEdge[];
  total_nodes: number;
  total_edges: number;
  sampled_nodes: number;
  sampled_edges: number;
}
