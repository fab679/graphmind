import { forwardRef, useCallback, useEffect, useImperativeHandle, useRef, useState } from "react";
import {
  forceCenter,
  forceCollide,
  forceLink,
  forceManyBody,
  forceSimulation,
} from "d3-force";
import { select } from "d3-selection";
import { zoom, zoomIdentity } from "d3-zoom";
import type { D3ZoomEvent } from "d3-zoom";
import type { Simulation, SimulationNodeDatum } from "d3-force";
import type { ZoomBehavior } from "d3-zoom";
import type { GraphEdge, GraphNode } from "@/types/api";
import { useGraphStore } from "@/stores/graphStore";
import { useQueryStore } from "@/stores/queryStore";
import { useGraphSettingsStore } from "@/stores/graphSettingsStore";
import { getCustomColorForLabel, getCustomEdgeColor, getNodeCaption } from "@/lib/colors";
import { executeQuery } from "@/api/client";
import { GraphToolbar } from "@/components/graph/GraphToolbar";
import { GraphStats } from "@/components/graph/GraphStats";
import { drawIconOnCanvas, getImageUrl, NODE_ICON_CATALOG } from "@/lib/icons";

// ---------------------------------------------------------------------------
// Image cache for node images
// ---------------------------------------------------------------------------

const imageCache = new Map<string, HTMLImageElement | null>();

function loadImage(url: string, onLoad?: () => void): HTMLImageElement | null {
  if (imageCache.has(url)) return imageCache.get(url)!;
  const img = new Image();
  img.crossOrigin = "anonymous";
  img.src = url;
  imageCache.set(url, null); // mark as loading
  img.onload = () => {
    imageCache.set(url, img);
    onLoad?.();
  };
  img.onerror = () => {
    imageCache.set(url, null); // mark as failed
  };
  return null;
}

// ---------------------------------------------------------------------------
// Internal types
// ---------------------------------------------------------------------------

interface SimNode extends SimulationNodeDatum {
  id: string;
  labels: string[];
  properties: Record<string, unknown>;
  radius: number;
}

interface SimLink {
  source: SimNode | string;
  target: SimNode | string;
  id: string;
  type: string;
  properties: Record<string, unknown>;
  /** Curvature offset for parallel edges (0 = straight). */
  curvature: number;
}

interface Transform {
  x: number;
  y: number;
  k: number;
}

interface ContextMenuState {
  x: number;
  y: number;
  nodeId: string | null;
  nodeLabels: string[];
  nodeProperties: Record<string, unknown>;
}

type LayoutType = "force" | "circular" | "hierarchical" | "grid";

interface HighlightedPath {
  nodeIds: Set<string>;
  edgeIds: Set<string>;
  hops: number;
}

// ---------------------------------------------------------------------------
// Props
// ---------------------------------------------------------------------------

export interface ForceGraphHandle {
  applyLayout: (layout: string) => void;
  zoomIn: () => void;
  zoomOut: () => void;
  fitToScreen: () => void;
  exportPNG: () => void;
  getCanvas: () => HTMLCanvasElement | null;
  setShortestPathMode: (active: boolean) => void;
}

interface ForceGraphProps {
  /** Optionally pass data directly; when omitted the component reads from graphStore. */
  nodes?: GraphNode[];
  edges?: GraphEdge[];
  onNodeDoubleClick?: (node: GraphNode) => void;
  /** Hide the toolbar (useful when embedded in fullscreen explorer which has its own) */
  hideToolbar?: boolean;
  /** Search query to highlight matching nodes on canvas */
  searchQuery?: string;
}

// ---------------------------------------------------------------------------
// Constants
// ---------------------------------------------------------------------------

const MIN_RADIUS = 6;
const MAX_RADIUS = 20;
const ARROW_SIZE = 8;
const LABEL_FONT = "11px Inter, system-ui, sans-serif";
const EDGE_FONT = "9px Inter, system-ui, sans-serif";
const HIT_TOLERANCE = 8;
const SELECTED_RING_WIDTH = 3;
const SELECTED_EDGE_WIDTH = 3;
const DEFAULT_EDGE_WIDTH = 1;
const PATH_RING_WIDTH = 3;
const PATH_EDGE_WIDTH = 3;
const PATH_COLOR = "#f59e0b"; // amber/orange for shortest path

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

function nodeRadius(degree: number): number {
  return Math.min(MAX_RADIUS, Math.max(MIN_RADIUS, 4 + Math.sqrt(degree) * 3));
}

/** Compute curvature offsets for parallel edges between the same pair. */
function assignCurvatures(links: SimLink[]): void {
  const pairCounts = new Map<string, number>();
  const pairIndex = new Map<string, number>();

  for (const link of links) {
    const srcId = typeof link.source === "string" ? link.source : link.source.id;
    const tgtId = typeof link.target === "string" ? link.target : link.target.id;
    const key = srcId < tgtId ? `${srcId}|${tgtId}` : `${tgtId}|${srcId}`;
    pairCounts.set(key, (pairCounts.get(key) ?? 0) + 1);
  }

  for (const link of links) {
    const srcId = typeof link.source === "string" ? link.source : link.source.id;
    const tgtId = typeof link.target === "string" ? link.target : link.target.id;
    const key = srcId < tgtId ? `${srcId}|${tgtId}` : `${tgtId}|${srcId}`;
    const total = pairCounts.get(key) ?? 1;
    if (total === 1) {
      link.curvature = 0;
    } else {
      const idx = pairIndex.get(key) ?? 0;
      pairIndex.set(key, idx + 1);
      link.curvature = (idx - (total - 1) / 2) * 30;
    }
  }
}

function sourceNode(link: SimLink): SimNode {
  return link.source as SimNode;
}

function targetNode(link: SimLink): SimNode {
  return link.target as SimNode;
}

/** Quadratic bezier control point for curved edges. */
function controlPoint(
  sx: number,
  sy: number,
  tx: number,
  ty: number,
  curvature: number,
): [number, number] {
  const mx = (sx + tx) / 2;
  const my = (sy + ty) / 2;
  const dx = tx - sx;
  const dy = ty - sy;
  const len = Math.sqrt(dx * dx + dy * dy) || 1;
  const nx = -dy / len;
  const ny = dx / len;
  return [mx + nx * curvature, my + ny * curvature];
}

/** Point on a quadratic bezier at parameter t. */
function bezierPoint(
  sx: number,
  sy: number,
  cx: number,
  cy: number,
  tx: number,
  ty: number,
  t: number,
): [number, number] {
  const u = 1 - t;
  return [
    u * u * sx + 2 * u * t * cx + t * t * tx,
    u * u * sy + 2 * u * t * cy + t * t * ty,
  ];
}

/** Distance from point (px,py) to a quadratic bezier, sampled. */
function distToBezier(
  px: number,
  py: number,
  sx: number,
  sy: number,
  cx: number,
  cy: number,
  tx: number,
  ty: number,
): number {
  let minDist = Infinity;
  for (let i = 0; i <= 20; i++) {
    const t = i / 20;
    const [bx, by] = bezierPoint(sx, sy, cx, cy, tx, ty, t);
    const d = Math.hypot(px - bx, py - by);
    if (d < minDist) minDist = d;
  }
  return minDist;
}

function isDarkMode(): boolean {
  return document.documentElement.classList.contains("dark");
}

function getLabelColor(): string {
  return isDarkMode() ? "#e2e8f0" : "#1e293b";
}

// ---------------------------------------------------------------------------
// Layout functions
// ---------------------------------------------------------------------------

function applyCircularLayout(nodes: SimNode[], width: number, height: number): void {
  const cx = width / 2;
  const cy = height / 2;
  const radius = Math.min(width, height) * 0.35;
  nodes.forEach((node, i) => {
    const angle = (2 * Math.PI * i) / nodes.length;
    node.x = cx + radius * Math.cos(angle);
    node.y = cy + radius * Math.sin(angle);
    node.fx = node.x;
    node.fy = node.y;
  });
}

function applyHierarchicalLayout(
  nodes: SimNode[],
  links: SimLink[],
  width: number,
  height: number,
): void {
  if (nodes.length === 0) return;

  // Build adjacency and count incoming edges
  const incomingCount = new Map<string, number>();
  const adjacency = new Map<string, string[]>();
  for (const n of nodes) {
    incomingCount.set(n.id, 0);
    adjacency.set(n.id, []);
  }
  for (const link of links) {
    const srcId = typeof link.source === "string" ? link.source : link.source.id;
    const tgtId = typeof link.target === "string" ? link.target : link.target.id;
    incomingCount.set(tgtId, (incomingCount.get(tgtId) ?? 0) + 1);
    const neighbors = adjacency.get(srcId);
    if (neighbors) neighbors.push(tgtId);
    // Also add reverse for undirected traversal
    const revNeighbors = adjacency.get(tgtId);
    if (revNeighbors) revNeighbors.push(srcId);
  }

  // BFS from nodes with fewest incoming edges to assign layers
  const layers = new Map<string, number>();
  const sorted = [...nodes].sort(
    (a, b) => (incomingCount.get(a.id) ?? 0) - (incomingCount.get(b.id) ?? 0),
  );

  // Start BFS from roots (fewest incoming)
  const queue: string[] = [];
  for (const n of sorted) {
    if (!layers.has(n.id) && (incomingCount.get(n.id) ?? 0) === 0) {
      layers.set(n.id, 0);
      queue.push(n.id);
    }
  }
  // If no roots found, seed with first node
  if (queue.length === 0 && nodes.length > 0) {
    layers.set(sorted[0].id, 0);
    queue.push(sorted[0].id);
  }

  while (queue.length > 0) {
    const current = queue.shift()!;
    const currentLayer = layers.get(current) ?? 0;
    const neighbors = adjacency.get(current) ?? [];
    for (const neighbor of neighbors) {
      if (!layers.has(neighbor)) {
        layers.set(neighbor, currentLayer + 1);
        queue.push(neighbor);
      }
    }
  }

  // Assign remaining unvisited nodes
  for (const n of nodes) {
    if (!layers.has(n.id)) {
      layers.set(n.id, 0);
    }
  }

  // Group by layer
  const layerGroups = new Map<number, SimNode[]>();
  for (const n of nodes) {
    const layer = layers.get(n.id) ?? 0;
    if (!layerGroups.has(layer)) layerGroups.set(layer, []);
    layerGroups.get(layer)!.push(n);
  }

  const layerKeys = [...layerGroups.keys()].sort((a, b) => a - b);
  const numLayers = layerKeys.length;
  const layerHeight = height / (numLayers + 1);

  for (let li = 0; li < layerKeys.length; li++) {
    const layerNodes = layerGroups.get(layerKeys[li])!;
    const layerWidth = width / (layerNodes.length + 1);
    layerNodes.forEach((node, ni) => {
      node.x = (ni + 1) * layerWidth;
      node.y = (li + 1) * layerHeight;
      node.fx = node.x;
      node.fy = node.y;
    });
  }
}

function applyGridLayout(nodes: SimNode[], width: number, height: number): void {
  const cols = Math.ceil(Math.sqrt(nodes.length));
  const rows = Math.ceil(nodes.length / cols);
  const cellW = width / (cols + 1);
  const cellH = height / (rows + 1);
  nodes.forEach((node, i) => {
    node.x = ((i % cols) + 1) * cellW;
    node.y = (Math.floor(i / cols) + 1) * cellH;
    node.fx = node.x;
    node.fy = node.y;
  });
}

function unpinAllNodes(nodes: SimNode[]): void {
  for (const node of nodes) {
    node.fx = null;
    node.fy = null;
  }
}

// ---------------------------------------------------------------------------
// Client-side BFS for shortest path fallback
// ---------------------------------------------------------------------------

function findShortestPathBFS(
  links: SimLink[],
  startId: string,
  endId: string,
): { nodeIds: string[]; edgeIds: string[] } | null {
  // Build adjacency from simLinks
  const adj = new Map<string, { neighbor: string; edgeId: string }[]>();

  for (const link of links) {
    const srcId = typeof link.source === "string" ? link.source : link.source.id;
    const tgtId = typeof link.target === "string" ? link.target : link.target.id;

    if (!adj.has(srcId)) adj.set(srcId, []);
    if (!adj.has(tgtId)) adj.set(tgtId, []);
    adj.get(srcId)!.push({ neighbor: tgtId, edgeId: link.id });
    adj.get(tgtId)!.push({ neighbor: srcId, edgeId: link.id });
  }

  if (!adj.has(startId) || !adj.has(endId)) return null;

  const visited = new Set<string>();
  const parent = new Map<string, { node: string; edgeId: string } | null>();
  const queue: string[] = [startId];
  visited.add(startId);
  parent.set(startId, null);

  while (queue.length > 0) {
    const current = queue.shift()!;
    if (current === endId) {
      // Reconstruct path
      const nodeIds: string[] = [];
      const edgeIds: string[] = [];
      let cursor: string | null = endId;
      while (cursor !== null) {
        nodeIds.unshift(cursor);
        const p = parent.get(cursor);
        if (p) {
          edgeIds.unshift(p.edgeId);
          cursor = p.node;
        } else {
          cursor = null;
        }
      }
      return { nodeIds, edgeIds };
    }

    for (const edge of adj.get(current) ?? []) {
      if (!visited.has(edge.neighbor)) {
        visited.add(edge.neighbor);
        parent.set(edge.neighbor, { node: current, edgeId: edge.edgeId });
        queue.push(edge.neighbor);
      }
    }
  }

  return null;
}

// ---------------------------------------------------------------------------
// Search matching helper
// ---------------------------------------------------------------------------

function nodeMatchesSearch(node: SimNode, query: string): boolean {
  const q = query.toLowerCase();
  // Check labels
  for (const label of node.labels) {
    if (label.toLowerCase().includes(q)) return true;
  }
  // Check properties
  for (const val of Object.values(node.properties)) {
    if (val != null && String(val).toLowerCase().includes(q)) return true;
  }
  return false;
}

// ---------------------------------------------------------------------------
// Component
// ---------------------------------------------------------------------------

export const ForceGraph = forwardRef<ForceGraphHandle, ForceGraphProps>(function ForceGraph({
  nodes: propNodes,
  edges: propEdges,
  onNodeDoubleClick,
  hideToolbar,
  searchQuery: searchQueryProp,
}, ref) {
  const storeNodes = useGraphStore((s) => s.nodes);
  const storeEdges = useGraphStore((s) => s.edges);
  const selectedNode = useGraphStore((s) => s.selectedNode);
  const selectedEdge = useGraphStore((s) => s.selectedEdge);
  const selectNode = useGraphStore((s) => s.selectNode);
  const selectEdge = useGraphStore((s) => s.selectEdge);
  const selectedNodesFromStore = useGraphStore((s) => s.selectedNodes);

  const highlightMode = useGraphSettingsStore((s) => s.highlightMode);
  // Subscribe to trigger re-renders when these change (used in draw via getState)
  useGraphSettingsStore((s) => s.labelColors);
  useGraphSettingsStore((s) => s.edgeColors);
  useGraphSettingsStore((s) => s.labelIcons);

  const nodes = propNodes ?? storeNodes;
  const edges = propEdges ?? storeEdges;

  const canvasRef = useRef<HTMLCanvasElement>(null);
  const containerRef = useRef<HTMLDivElement>(null);
  const simRef = useRef<Simulation<SimNode, SimLink> | null>(null);
  const simNodesRef = useRef<SimNode[]>([]);
  const simLinksRef = useRef<SimLink[]>([]);
  const transformRef = useRef<Transform>({ x: 0, y: 0, k: 1 });
  const zoomRef = useRef<ZoomBehavior<HTMLCanvasElement, unknown> | null>(null);
  const dragNodeRef = useRef<SimNode | null>(null);
  const rafRef = useRef<number>(0);
  const selectedNodeIdRef = useRef<string | null>(null);
  const selectedEdgeIdRef = useRef<string | null>(null);
  const selectedNodeIdsRef = useRef<Set<string>>(new Set());
  const highlightModeRef = useRef(highlightMode);
  const layoutRef = useRef<LayoutType>("force");
  const searchQueryRef = useRef("");

  // Fix #3: Cache getBoundingClientRect — store in a ref, update on resize
  const canvasRectRef = useRef<DOMRect | null>(null);

  // Fix #1: Memoize search matches — store in refs, compute in useEffect
  const searchMatchNodesRef = useRef<Set<string> | null>(null);
  const searchMatchEdgesRef = useRef<Set<string> | null>(null);

  // Fix #2: Memoize connected nodes — store in refs, compute in useEffect
  const connectedNodeIdsRef = useRef<Set<string> | null>(null);
  const connectedEdgeIdsRef = useRef<Set<string> | null>(null);

  // Context menu state
  const [contextMenu, setContextMenu] = useState<ContextMenuState | null>(null);

  // Shortest path state
  const [shortestPathMode, setShortestPathMode] = useState(false);
  const [pathStartNode, setPathStartNode] = useState<string | null>(null);
  const [highlightedPath, setHighlightedPath] = useState<HighlightedPath | null>(null);
  const [pathBanner, setPathBanner] = useState<string | null>(null);

  const shortestPathModeRef = useRef(false);
  const pathStartNodeRef = useRef<string | null>(null);
  const highlightedPathRef = useRef<HighlightedPath | null>(null);

  // Forward reference to draw (declared later but only called at runtime)
  const drawRef = useRef<() => void>(() => {});

  // Fix #10: Move ref syncs into useEffect instead of render body
  useEffect(() => {
    selectedNodeIdRef.current = selectedNode?.id ?? null;
  }, [selectedNode]);

  useEffect(() => {
    selectedEdgeIdRef.current = selectedEdge?.id ?? null;
  }, [selectedEdge]);

  useEffect(() => {
    selectedNodeIdsRef.current = new Set(selectedNodesFromStore.map((n) => n.id));
  }, [selectedNodesFromStore]);

  useEffect(() => {
    highlightModeRef.current = highlightMode;
  }, [highlightMode]);

  useEffect(() => {
    shortestPathModeRef.current = shortestPathMode;
  }, [shortestPathMode]);

  useEffect(() => {
    pathStartNodeRef.current = pathStartNode;
  }, [pathStartNode]);

  useEffect(() => {
    highlightedPathRef.current = highlightedPath;
  }, [highlightedPath]);

  // Fix #1: Memoize search matches — compute when searchQueryProp changes
  useEffect(() => {
    const q = searchQueryProp ?? "";
    searchQueryRef.current = q;

    if (q) {
      const simNodes = simNodesRef.current;
      const simLinks = simLinksRef.current;
      const matchNodes = new Set<string>();
      for (const node of simNodes) {
        if (nodeMatchesSearch(node, q)) {
          matchNodes.add(node.id);
        }
      }
      const matchEdges = new Set<string>();
      for (const link of simLinks) {
        const srcId = typeof link.source === "string" ? link.source : (link.source as SimNode).id;
        const tgtId = typeof link.target === "string" ? link.target : (link.target as SimNode).id;
        if (matchNodes.has(srcId) && matchNodes.has(tgtId)) {
          matchEdges.add(link.id);
        }
      }
      searchMatchNodesRef.current = matchNodes;
      searchMatchEdgesRef.current = matchEdges;
    } else {
      searchMatchNodesRef.current = null;
      searchMatchEdgesRef.current = null;
    }

    drawRef.current();
  }, [searchQueryProp]);

  // Fix #2: Memoize connected nodes — compute when selectedNode/highlightMode changes
  useEffect(() => {
    const selId = selectedNode?.id ?? null;
    if (highlightMode && selId) {
      const simLinks = simLinksRef.current;
      const connNodes = new Set<string>();
      const connEdges = new Set<string>();
      for (const link of simLinks) {
        const srcId = typeof link.source === "string" ? link.source : (link.source as SimNode).id;
        const tgtId = typeof link.target === "string" ? link.target : (link.target as SimNode).id;
        if (srcId === selId || tgtId === selId) {
          connNodes.add(srcId);
          connNodes.add(tgtId);
          connEdges.add(link.id);
        }
      }
      connectedNodeIdsRef.current = connNodes;
      connectedEdgeIdsRef.current = connEdges;
    } else {
      connectedNodeIdsRef.current = null;
      connectedEdgeIdsRef.current = null;
    }

    drawRef.current();
  }, [selectedNode, highlightMode]);

  // Close context menu on Escape or scroll
  useEffect(() => {
    if (!contextMenu) return;

    function handleKeyDown(e: KeyboardEvent) {
      if (e.key === "Escape") setContextMenu(null);
    }
    function handleScroll() {
      setContextMenu(null);
    }
    function handleClickOutside() {
      setContextMenu(null);
    }

    document.addEventListener("keydown", handleKeyDown);
    window.addEventListener("scroll", handleScroll, true);
    const timer = setTimeout(() => {
      document.addEventListener("click", handleClickOutside);
    }, 0);

    return () => {
      document.removeEventListener("keydown", handleKeyDown);
      window.removeEventListener("scroll", handleScroll, true);
      clearTimeout(timer);
      document.removeEventListener("click", handleClickOutside);
    };
  }, [contextMenu]);

  // Escape key to exit shortest path mode
  useEffect(() => {
    function handleKeyDown(e: KeyboardEvent) {
      if (e.key === "Escape" && shortestPathMode) {
        setShortestPathMode(false);
        setPathStartNode(null);
        setHighlightedPath(null);
        setPathBanner(null);
      }
    }
    document.addEventListener("keydown", handleKeyDown);
    return () => document.removeEventListener("keydown", handleKeyDown);
  }, [shortestPathMode]);

  // --------------------------------------------------
  // Layout application
  // --------------------------------------------------

  const getCanvasDimensions = useCallback((): [number, number] => {
    const canvas = canvasRef.current;
    if (!canvas) return [800, 600];
    const dpr = window.devicePixelRatio || 1;
    return [canvas.width / dpr, canvas.height / dpr];
  }, []);

  const applyLayout = useCallback(
    (layout: LayoutType) => {
      layoutRef.current = layout;
      const simNodes = simNodesRef.current;
      const simLinks = simLinksRef.current;
      const [w, h] = getCanvasDimensions();

      // Guard: if simulation hasn't built nodes yet, defer
      if (simNodes.length === 0 || w === 0 || h === 0) {
        console.warn("applyLayout deferred: no nodes or zero canvas", { nodes: simNodes.length, w, h });
        return;
      }

      if (layout === "force") {
        unpinAllNodes(simNodes);
        if (simRef.current) {
          simRef.current.alpha(0.8).restart();
        }
      } else if (layout === "circular") {
        applyCircularLayout(simNodes, w, h);
        if (simRef.current) {
          simRef.current.alpha(0).stop();
        }
      } else if (layout === "hierarchical") {
        applyHierarchicalLayout(simNodes, simLinks, w, h);
        if (simRef.current) {
          simRef.current.alpha(0).stop();
        }
      } else if (layout === "grid") {
        applyGridLayout(simNodes, w, h);
        if (simRef.current) {
          simRef.current.alpha(0).stop();
        }
      }

      drawRef.current();
    },
    [getCanvasDimensions],
  );

  // --------------------------------------------------
  // Shortest path logic
  // --------------------------------------------------

  const handleShortestPathClick = useCallback(
    async (node: SimNode) => {
      if (!pathStartNodeRef.current) {
        // First click: set start node
        setPathStartNode(node.id);
        setPathBanner("Click second node to find path");
        drawRef.current();
      } else {
        // Second click: find path
        const startId = pathStartNodeRef.current;
        const endId = node.id;

        if (startId === endId) {
          setPathBanner("Start and end are the same node");
          return;
        }

        setPathBanner("Finding path...");

        // Try API first
        let pathResult: { nodeIds: string[]; edgeIds: string[] } | null = null;

        try {
          const apiQuery = `MATCH p = shortestPath((a)-[*]-(b)) WHERE id(a) = ${startId} AND id(b) = ${endId} RETURN p`;
          const result = await executeQuery(apiQuery);
          if (!result.error && result.nodes.length > 0) {
            const pathNodeIds = result.nodes.map((n) => n.id);
            const pathEdgeIds = result.edges.map((e) => e.id);
            pathResult = { nodeIds: pathNodeIds, edgeIds: pathEdgeIds };
          }
        } catch {
          // API call failed, fall through to client-side BFS
        }

        // Fallback: client-side BFS
        if (!pathResult) {
          pathResult = findShortestPathBFS(simLinksRef.current, startId, endId);
        }

        if (pathResult) {
          const hp: HighlightedPath = {
            nodeIds: new Set(pathResult.nodeIds),
            edgeIds: new Set(pathResult.edgeIds),
            hops: pathResult.edgeIds.length,
          };
          setHighlightedPath(hp);
          setPathBanner(`Path found: ${hp.hops} hop${hp.hops !== 1 ? "s" : ""}`);
        } else {
          setHighlightedPath(null);
          setPathBanner("No path found between these nodes");
        }

        drawRef.current();
      }
    },
    [],
  );

  const clearShortestPath = useCallback(() => {
    setPathStartNode(null);
    setHighlightedPath(null);
    if (shortestPathModeRef.current) {
      setPathBanner("Click first node for shortest path");
    } else {
      setPathBanner(null);
    }
    drawRef.current();
  }, []);

  // --------------------------------------------------
  // Context menu actions
  // --------------------------------------------------

  const handleExpandNeighbors = useCallback(async (nodeId: string) => {
    setContextMenu(null);
    try {
      const query = `MATCH (n)-[r]-(m) WHERE id(n) = ${nodeId} RETURN n, r, m`;
      const result = await executeQuery(query);
      if (result.error) {
        console.error("Expand neighbors error:", result.error);
        return;
      }

      const state = useGraphStore.getState();
      const existingNodeIds = new Set(state.nodes.map((n) => n.id));
      const existingEdgeIds = new Set(state.edges.map((e) => e.id));

      const mergedNodes = [...state.nodes];
      for (const node of result.nodes) {
        if (!existingNodeIds.has(node.id)) {
          mergedNodes.push(node);
          existingNodeIds.add(node.id);
        }
      }

      const mergedEdges = [...state.edges];
      for (const edge of result.edges) {
        if (!existingEdgeIds.has(edge.id)) {
          mergedEdges.push(edge);
          existingEdgeIds.add(edge.id);
        }
      }

      useGraphStore.getState().setGraphData(mergedNodes, mergedEdges);
    } catch (err) {
      console.error("Expand neighbors failed:", err);
    }
  }, []);

  const handleViewAllRelationships = useCallback(async () => {
    setContextMenu(null);
    const state = useGraphStore.getState();
    if (state.nodes.length === 0) return;

    try {
      const canvasNodeIds = new Set(state.nodes.map((n) => n.id));
      const existingEdgeIds = new Set(state.edges.map((e) => e.id));
      const mergedEdges = [...state.edges];

      const result = await executeQuery("MATCH (n)-[r]->(m) RETURN n, r, m");
      if (result.error) {
        console.error("View all relationships error:", result.error);
        return;
      }

      for (const edge of result.edges) {
        if (
          canvasNodeIds.has(edge.source) &&
          canvasNodeIds.has(edge.target) &&
          !existingEdgeIds.has(edge.id)
        ) {
          mergedEdges.push(edge);
          existingEdgeIds.add(edge.id);
        }
      }

      useGraphStore.getState().setGraphData(state.nodes, mergedEdges);
    } catch (err) {
      console.error("View all relationships failed:", err);
    }
  }, []);

  // --------------------------------------------------
  // Fit-to-screen helper (shared by imperative handle, toolbar, and auto-fit)
  // --------------------------------------------------

  const fitToScreen = useCallback(() => {
    const canvas = canvasRef.current;
    if (!canvas) return;
    const simNodes = simNodesRef.current;
    const dpr = window.devicePixelRatio || 1;
    const cw = canvas.width / dpr;
    const ch = canvas.height / dpr;

    if (simNodes.length === 0 || cw === 0 || ch === 0) {
      if (zoomRef.current) {
        select<HTMLCanvasElement, unknown>(canvas).call(zoomRef.current.transform, zoomIdentity);
      }
      transformRef.current = { x: 0, y: 0, k: 1 };
      drawRef.current();
      return;
    }

    // Compute bounding box of all nodes
    let minX = Infinity, minY = Infinity, maxX = -Infinity, maxY = -Infinity;
    for (const n of simNodes) {
      if (n.x != null && n.y != null) {
        minX = Math.min(minX, n.x - n.radius);
        minY = Math.min(minY, n.y - n.radius);
        maxX = Math.max(maxX, n.x + n.radius);
        maxY = Math.max(maxY, n.y + n.radius);
      }
    }
    if (!isFinite(minX)) {
      transformRef.current = { x: 0, y: 0, k: 1 };
      drawRef.current();
      return;
    }

    const graphW = maxX - minX || 1;
    const graphH = maxY - minY || 1;
    const padding = 40;
    const scale = Math.min((cw - padding * 2) / graphW, (ch - padding * 2) / graphH, 2);
    const tx = (cw - graphW * scale) / 2 - minX * scale;
    const ty = (ch - graphH * scale) / 2 - minY * scale;

    // Update transform and sync with d3-zoom
    transformRef.current = { x: tx, y: ty, k: scale };
    if (zoomRef.current) {
      select<HTMLCanvasElement, unknown>(canvas).call(
        zoomRef.current.transform,
        zoomIdentity.translate(tx, ty).scale(scale),
      );
    }
    drawRef.current();
  }, []);

  // --------------------------------------------------
  // Draw
  // --------------------------------------------------

  const draw = useCallback(() => {
    const canvas = canvasRef.current;
    if (!canvas) return;
    const ctx = canvas.getContext("2d");
    if (!ctx) return;

    const width = canvas.width;
    const height = canvas.height;
    const dpr = window.devicePixelRatio || 1;
    const t = transformRef.current;
    const simNodes = simNodesRef.current;
    const simLinks = simLinksRef.current;
    const selNodeId = selectedNodeIdRef.current;
    const selEdgeId = selectedEdgeIdRef.current;
    const multiSelectedIds = selectedNodeIdsRef.current;
    const hlMode = highlightModeRef.current;
    const labelColor = getLabelColor();
    const currentPath = highlightedPathRef.current;
    const spStartNode = pathStartNodeRef.current;
    const spMode = shortestPathModeRef.current;

    // Fix #2: Read memoized connected nodes from refs (computed in useEffect)
    const connectedNodeIds = connectedNodeIdsRef.current;
    const connectedEdgeIds = connectedEdgeIdsRef.current;

    // Fix #1: Read memoized search matches from refs (computed in useEffect)
    const searchMatchNodes = searchMatchNodesRef.current;
    const searchMatchEdges = searchMatchEdgesRef.current;

    // Fix #4: Get store state once before draw loop
    const settingsState = useGraphSettingsStore.getState();

    ctx.clearRect(0, 0, width, height);
    ctx.save();
    ctx.scale(dpr, dpr);
    ctx.translate(t.x, t.y);
    ctx.scale(t.k, t.k);

    // --- Edges ---
    // Fix #9: Batch edge text rendering — set font once before edge loop
    ctx.font = EDGE_FONT;

    for (const link of simLinks) {
      const src = sourceNode(link);
      const tgt = targetNode(link);
      if (src.x == null || src.y == null || tgt.x == null || tgt.y == null) continue;

      const isSelected = link.id === selEdgeId;
      const isOnPath = currentPath?.edgeIds.has(link.id) ?? false;
      const lineWidth = isSelected
        ? SELECTED_EDGE_WIDTH
        : isOnPath
          ? PATH_EDGE_WIDTH
          : DEFAULT_EDGE_WIDTH;

      // Determine edge opacity
      let edgeAlpha = 1;
      if (currentPath) {
        // Shortest path active: dim non-path edges
        edgeAlpha = isOnPath || isSelected ? 1 : 0.08;
      } else if (searchMatchEdges) {
        // Search active: dim non-matching edges
        edgeAlpha = searchMatchEdges.has(link.id) ? 1 : 0.1;
      } else if (hlMode && selNodeId && connectedEdgeIds && !isSelected) {
        edgeAlpha = connectedEdgeIds.has(link.id) ? 1 : 0.1;
      }

      const edgeColor = isOnPath ? PATH_COLOR : getCustomEdgeColor(link.type);
      if (isSelected) {
        ctx.strokeStyle = "#60a5fa";
      } else {
        ctx.globalAlpha = edgeAlpha;
        ctx.strokeStyle = edgeColor;
      }
      ctx.lineWidth = lineWidth;
      ctx.beginPath();

      const sx = src.x;
      const sy = src.y;
      const tx = tgt.x;
      const ty = tgt.y;

      if (link.curvature === 0) {
        ctx.moveTo(sx, sy);
        ctx.lineTo(tx, ty);
        ctx.stroke();

        const dx = tx - sx;
        const dy = ty - sy;
        const len = Math.sqrt(dx * dx + dy * dy) || 1;
        const ux = dx / len;
        const uy = dy / len;
        const arrowX = tx - ux * tgt.radius;
        const arrowY = ty - uy * tgt.radius;
        drawArrow(ctx, arrowX, arrowY, ux, uy, isSelected, isOnPath ? PATH_COLOR : edgeColor);

        const mx = (sx + tx) / 2;
        const my = (sy + ty) / 2;
        drawEdgeLabel(ctx, link.type, mx, my, t.k);
      } else {
        const [cpx, cpy] = controlPoint(sx, sy, tx, ty, link.curvature);
        ctx.moveTo(sx, sy);
        ctx.quadraticCurveTo(cpx, cpy, tx, ty);
        ctx.stroke();

        const tangentX = 2 * (tx - cpx);
        const tangentY = 2 * (ty - cpy);
        const tangentLen = Math.sqrt(tangentX * tangentX + tangentY * tangentY) || 1;
        const ux = tangentX / tangentLen;
        const uy = tangentY / tangentLen;
        const arrowX = tx - ux * tgt.radius;
        const arrowY = ty - uy * tgt.radius;
        drawArrow(ctx, arrowX, arrowY, ux, uy, isSelected, isOnPath ? PATH_COLOR : edgeColor);

        const [mx, my] = bezierPoint(sx, sy, cpx, cpy, tx, ty, 0.5);
        drawEdgeLabel(ctx, link.type, mx, my, t.k);
      }

      ctx.globalAlpha = 1;
    }

    // --- Nodes ---
    for (const node of simNodes) {
      if (node.x == null || node.y == null) continue;

      const label = node.labels[0] ?? "Node";
      const color = getCustomColorForLabel(label);
      const isSelected = node.id === selNodeId;
      const isOnPath = currentPath?.nodeIds.has(node.id) ?? false;
      const isPathStart = spMode && spStartNode === node.id;
      const isSearchMatch = searchMatchNodes?.has(node.id) ?? false;

      // Determine node opacity
      let nodeAlpha = 1;
      if (currentPath) {
        nodeAlpha = isOnPath || isSelected ? 1 : 0.15;
      } else if (searchMatchNodes) {
        nodeAlpha = isSearchMatch ? 1 : 0.2;
      } else if (hlMode && selNodeId && connectedNodeIds && !isSelected) {
        nodeAlpha = connectedNodeIds.has(node.id) ? 1 : 0.2;
      }

      ctx.globalAlpha = nodeAlpha;

      // Path highlight ring
      if (isOnPath && !isSelected) {
        ctx.globalAlpha = 1;
        ctx.beginPath();
        ctx.arc(node.x, node.y, node.radius + PATH_RING_WIDTH, 0, Math.PI * 2);
        ctx.fillStyle = "rgba(245, 158, 11, 0.3)";
        ctx.fill();
        ctx.strokeStyle = PATH_COLOR;
        ctx.lineWidth = 2;
        ctx.stroke();
        ctx.globalAlpha = nodeAlpha;
      }

      // Path start node ring (green)
      if (isPathStart && !highlightedPathRef.current) {
        ctx.globalAlpha = 1;
        ctx.beginPath();
        ctx.arc(node.x, node.y, node.radius + PATH_RING_WIDTH, 0, Math.PI * 2);
        ctx.fillStyle = "rgba(34, 197, 94, 0.3)";
        ctx.fill();
        ctx.strokeStyle = "#22c55e";
        ctx.lineWidth = 2;
        ctx.stroke();
        ctx.globalAlpha = nodeAlpha;
      }

      // Search highlight ring (bright cyan)
      if (isSearchMatch && searchMatchNodes && !isSelected && !isOnPath) {
        ctx.globalAlpha = 1;
        ctx.beginPath();
        ctx.arc(node.x, node.y, node.radius + 3, 0, Math.PI * 2);
        ctx.fillStyle = "rgba(6, 182, 212, 0.25)";
        ctx.fill();
        ctx.strokeStyle = "#06b6d4";
        ctx.lineWidth = 2;
        ctx.stroke();
        ctx.globalAlpha = nodeAlpha;
      }

      // Selection ring (primary selected or multi-selected)
      const isMultiSelected = multiSelectedIds.size > 1 && multiSelectedIds.has(node.id);
      if (isSelected || isMultiSelected) {
        ctx.globalAlpha = 1;
        ctx.beginPath();
        ctx.arc(node.x, node.y, node.radius + SELECTED_RING_WIDTH, 0, Math.PI * 2);
        ctx.fillStyle = isSelected
          ? "rgba(96, 165, 250, 0.35)"
          : "rgba(96, 165, 250, 0.2)";
        ctx.fill();
        ctx.strokeStyle = "#60a5fa";
        ctx.lineWidth = isSelected ? 2 : 1.5;
        ctx.stroke();
      }

      // Node circle
      ctx.beginPath();
      ctx.arc(node.x, node.y, node.radius, 0, Math.PI * 2);
      ctx.fillStyle = color;
      ctx.fill();
      ctx.strokeStyle = "rgba(0,0,0,0.3)";
      ctx.lineWidth = 1;
      ctx.stroke();

      // --- Icon / image overlay ---
      // Fix #4: Use settingsState captured once above instead of per-node getState()
      const iconName = settingsState.labelIcons[label];
      const imagePropName = settingsState.imageProperty[label];

      // Try explicit image property first
      let drewImage = false;
      if (imagePropName) {
        const imgUrl = node.properties[imagePropName];
        if (typeof imgUrl === "string" && imgUrl.startsWith("http")) {
          const img = loadImage(imgUrl, () => drawRef.current());
          if (img) {
            ctx.save();
            ctx.beginPath();
            ctx.arc(node.x, node.y, node.radius - 1, 0, Math.PI * 2);
            ctx.clip();
            ctx.drawImage(img, node.x - node.radius, node.y - node.radius, node.radius * 2, node.radius * 2);
            ctx.restore();
            drewImage = true;
          }
        }
      }

      // If no image, try icon from catalog
      if (!drewImage && iconName) {
        const icon = NODE_ICON_CATALOG.find(i => i.name === iconName);
        if (icon && icon.path) {
          drawIconOnCanvas(ctx, icon.path, node.x, node.y, node.radius * 0.6, "rgba(255,255,255,0.9)");
        }
      }

      // Auto-detect image URL from well-known properties
      if (!drewImage && !iconName) {
        const autoImgUrl = getImageUrl(node.properties);
        if (autoImgUrl) {
          const img = loadImage(autoImgUrl, () => drawRef.current());
          if (img) {
            ctx.save();
            ctx.beginPath();
            ctx.arc(node.x, node.y, node.radius - 1, 0, Math.PI * 2);
            ctx.clip();
            ctx.drawImage(img, node.x - node.radius, node.y - node.radius, node.radius * 2, node.radius * 2);
            ctx.restore();
          }
        }
      }

      // Node label
      const displayName = getNodeCaption(label, node.properties);
      const maxLabelLen = 18;
      const truncated =
        displayName.length > maxLabelLen
          ? displayName.slice(0, maxLabelLen - 1) + "\u2026"
          : displayName;

      ctx.font = LABEL_FONT;
      ctx.textAlign = "center";
      ctx.textBaseline = "top";
      ctx.fillStyle = labelColor;
      ctx.fillText(truncated, node.x, node.y + node.radius + 3);

      ctx.globalAlpha = 1;
    }

    ctx.restore();

    // "No data" message
    if (simNodes.length === 0) {
      const noDataColor = isDarkMode() ? "#64748b" : "#94a3b8";
      ctx.font = "14px Inter, system-ui, sans-serif";
      ctx.textAlign = "center";
      ctx.textBaseline = "middle";
      ctx.fillStyle = noDataColor;
      ctx.fillText("No data to display", width / (2 * dpr), height / (2 * dpr));
    }
  }, []);

  // Assign draw to forward ref
  drawRef.current = draw;

  // Expose imperative methods via ref
  useImperativeHandle(ref, () => ({
    applyLayout: (layout: string) => applyLayout(layout as LayoutType),
    zoomIn: () => {
      if (zoomRef.current && canvasRef.current) {
        select<HTMLCanvasElement, unknown>(canvasRef.current).call(zoomRef.current.scaleBy, 1.3);
      }
    },
    zoomOut: () => {
      if (zoomRef.current && canvasRef.current) {
        select<HTMLCanvasElement, unknown>(canvasRef.current).call(zoomRef.current.scaleBy, 0.7);
      }
    },
    fitToScreen,
    exportPNG: () => {
      const canvas = canvasRef.current;
      if (!canvas) return;
      const exportCanvas = document.createElement("canvas");
      exportCanvas.width = canvas.width;
      exportCanvas.height = canvas.height;
      const ctx = exportCanvas.getContext("2d");
      if (!ctx) return;
      const dark = document.documentElement.classList.contains("dark");
      ctx.fillStyle = dark ? "#0a0f1a" : "#ffffff";
      ctx.fillRect(0, 0, exportCanvas.width, exportCanvas.height);
      ctx.drawImage(canvas, 0, 0);
      const a = document.createElement("a");
      a.href = exportCanvas.toDataURL("image/png");
      a.download = "graphmind-export.png";
      a.click();
    },
    getCanvas: () => canvasRef.current,
    setShortestPathMode: (active: boolean) => {
      setShortestPathMode(active);
      if (active) {
        setPathStartNode(null);
        setHighlightedPath(null);
        setPathBanner("Click first node for shortest path");
      } else {
        setPathStartNode(null);
        setHighlightedPath(null);
        setPathBanner(null);
      }
    },
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }), []);

  // --------------------------------------------------
  // Build / rebuild simulation when data changes
  // --------------------------------------------------

  useEffect(() => {
    const degreeMap = new Map<string, number>();
    for (const edge of edges) {
      degreeMap.set(edge.source, (degreeMap.get(edge.source) ?? 0) + 1);
      degreeMap.set(edge.target, (degreeMap.get(edge.target) ?? 0) + 1);
    }

    // Initialize nodes near center with small random offset so the simulation spreads them out
    const cx = (canvasRef.current?.clientWidth ?? 800) / 2;
    const cy = (canvasRef.current?.clientHeight ?? 600) / 2;
    const simNodes: SimNode[] = nodes.map((n) => ({
      id: n.id,
      labels: n.labels,
      properties: n.properties,
      radius: nodeRadius(degreeMap.get(n.id) ?? 0),
      x: cx + (Math.random() - 0.5) * 50,
      y: cy + (Math.random() - 0.5) * 50,
    }));

    const nodeIdSet = new Set(simNodes.map((n) => n.id));

    const simLinks: SimLink[] = edges
      .filter((e) => nodeIdSet.has(e.source) && nodeIdSet.has(e.target))
      .map((e) => ({
        source: e.source,
        target: e.target,
        id: e.id,
        type: e.type,
        properties: e.properties,
        curvature: 0,
      }));

    assignCurvatures(simLinks);

    simNodesRef.current = simNodes;
    simLinksRef.current = simLinks;

    // Stop previous simulation
    if (simRef.current) {
      simRef.current.stop();
    }

    // Fix #5: Adaptive force parameters based on node count
    const nodeCount = simNodes.length;
    const linkDistance = nodeCount > 200 ? 120 : nodeCount > 50 ? 100 : 80;
    const chargeStrength = nodeCount > 200 ? -100 : nodeCount > 50 ? -150 : -200;
    const distanceMax = nodeCount > 200 ? 300 : nodeCount > 50 ? 500 : Infinity;

    const sim = forceSimulation<SimNode>(simNodes)
      .force(
        "link",
        forceLink<SimNode, SimLink>(simLinks)
          .id((d) => d.id)
          .distance(linkDistance),
      )
      .force(
        "charge",
        forceManyBody()
          .strength(chargeStrength)
          .distanceMax(distanceMax),
      )
      .force(
        "center",
        forceCenter(
          (canvasRef.current?.clientWidth ?? 800) / 2,
          (canvasRef.current?.clientHeight ?? 600) / 2,
        ),
      )
      .force(
        "collide",
        forceCollide<SimNode>().radius((d) => d.radius + 4),
      )
      // Fix #5: Faster alphaDecay and higher velocityDecay for quicker settling
      .alphaDecay(0.03)
      .velocityDecay(0.4)
      .on("tick", () => {
        cancelAnimationFrame(rafRef.current);
        // Fix #8: Use drawRef.current() instead of draw directly
        rafRef.current = requestAnimationFrame(() => drawRef.current());
      });

    // Pre-compute layout: run simulation silently to completion before rendering
    // This prevents the "double render" where nodes animate from random positions
    sim.stop();

    if (layoutRef.current !== "force") {
      // For non-force layouts, apply directly
      simRef.current = sim;
      applyLayout(layoutRef.current);
    } else {
      // For force layout: tick the simulation to completion silently
      const iterations = Math.min(300, Math.ceil(Math.log(simNodes.length + 1) * 50));
      for (let i = 0; i < iterations; i++) {
        sim.tick();
      }
      simRef.current = sim;
    }

    // Now fit to screen with the settled positions
    fitToScreen();

    // Start the simulation for interactive dragging (low alpha so it's nearly settled)
    sim.alpha(0.1)
      .on("tick", () => {
        cancelAnimationFrame(rafRef.current);
        rafRef.current = requestAnimationFrame(() => drawRef.current());
      })
      .restart();

    return () => {
      sim.stop();
      cancelAnimationFrame(rafRef.current);
    };
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [nodes, edges]);

  // --------------------------------------------------
  // Canvas setup: zoom, resize, interaction
  // --------------------------------------------------

  useEffect(() => {
    const canvas = canvasRef.current;
    const container = containerRef.current;
    if (!canvas || !container) return;

    const dpr = window.devicePixelRatio || 1;

    // Fix #7: Track last dimensions to guard canvas resize
    let lastWidth = 0;
    let lastHeight = 0;

    function resize() {
      if (!canvas || !container) return;
      const rect = container.getBoundingClientRect();
      const newWidth = rect.width;
      const newHeight = rect.height;

      // Fix #7: Skip if dimensions unchanged
      if (newWidth === lastWidth && newHeight === lastHeight) return;
      lastWidth = newWidth;
      lastHeight = newHeight;

      canvas.width = newWidth * dpr;
      canvas.height = newHeight * dpr;
      canvas.style.width = `${newWidth}px`;
      canvas.style.height = `${newHeight}px`;

      // Fix #3: Update cached rect on resize
      canvasRectRef.current = rect;

      drawRef.current();
    }

    const observer = new ResizeObserver(resize);
    observer.observe(container);
    resize();

    // --- Zoom ---
    const zoomBehavior = zoom<HTMLCanvasElement, unknown>()
      .scaleExtent([0.1, 8])
      .filter((event: Event) => {
        if (event.type === "wheel") return true;
        if (event.type === "dblclick") return false;
        if (event instanceof MouseEvent && event.button === 0) {
          const [mx, my] = mouseToWorld(event);
          return !findNodeAt(mx, my);
        }
        return true;
      })
      .on("zoom", (event: D3ZoomEvent<HTMLCanvasElement, unknown>) => {
        transformRef.current = {
          x: event.transform.x,
          y: event.transform.y,
          k: event.transform.k,
        };
        drawRef.current();
      });

    zoomRef.current = zoomBehavior;
    const sel = select<HTMLCanvasElement, unknown>(canvas).call(zoomBehavior);

    // --- Mouse helpers ---
    // Fix #3: Use cached rect in mouse handlers
    function mouseToWorld(event: MouseEvent): [number, number] {
      if (!canvas) return [0, 0];
      const rect = canvasRectRef.current ?? canvas.getBoundingClientRect();
      const t = transformRef.current;
      const cx = event.clientX - rect.left;
      const cy = event.clientY - rect.top;
      return [(cx - t.x) / t.k, (cy - t.y) / t.k];
    }

    function findNodeAt(wx: number, wy: number): SimNode | null {
      const simNodes = simNodesRef.current;
      for (let i = simNodes.length - 1; i >= 0; i--) {
        const n = simNodes[i];
        if (n.x == null || n.y == null) continue;
        const dist = Math.hypot(wx - n.x, wy - n.y);
        if (dist <= n.radius + HIT_TOLERANCE) return n;
      }
      return null;
    }

    function findEdgeAt(wx: number, wy: number): SimLink | null {
      const simLinks = simLinksRef.current;
      let closest: SimLink | null = null;
      let closestDist = HIT_TOLERANCE;

      for (const link of simLinks) {
        const src = sourceNode(link);
        const tgt = targetNode(link);
        if (src.x == null || src.y == null || tgt.x == null || tgt.y == null) continue;

        let dist: number;
        if (link.curvature === 0) {
          dist = distToSegment(wx, wy, src.x, src.y, tgt.x, tgt.y);
        } else {
          const [cpx, cpy] = controlPoint(src.x, src.y, tgt.x, tgt.y, link.curvature);
          dist = distToBezier(wx, wy, src.x, src.y, cpx, cpy, tgt.x, tgt.y);
        }

        if (dist < closestDist) {
          closestDist = dist;
          closest = link;
        }
      }
      return closest;
    }

    function distToSegment(
      px: number,
      py: number,
      ax: number,
      ay: number,
      bx: number,
      by: number,
    ): number {
      const dx = bx - ax;
      const dy = by - ay;
      const lenSq = dx * dx + dy * dy;
      if (lenSq === 0) return Math.hypot(px - ax, py - ay);
      let t = ((px - ax) * dx + (py - ay) * dy) / lenSq;
      t = Math.max(0, Math.min(1, t));
      return Math.hypot(px - (ax + t * dx), py - (ay + t * dy));
    }

    // --- Drag & click ---
    let isDragging = false;
    let didDrag = false;

    function onMouseDown(event: MouseEvent) {
      if (event.button !== 0) return;
      const [wx, wy] = mouseToWorld(event);
      const node = findNodeAt(wx, wy);
      if (node) {
        isDragging = true;
        didDrag = false;
        dragNodeRef.current = node;
        node.fx = node.x;
        node.fy = node.y;
        simRef.current?.alphaTarget(0.3).restart();
      }
    }

    function onMouseMove(event: MouseEvent) {
      if (!isDragging || !dragNodeRef.current) {
        const [wx, wy] = mouseToWorld(event);
        const over = findNodeAt(wx, wy);
        if (canvas) canvas.style.cursor = over ? "grab" : "default";
        return;
      }
      didDrag = true;
      if (canvas) canvas.style.cursor = "grabbing";
      const [wx, wy] = mouseToWorld(event);
      dragNodeRef.current.fx = wx;
      dragNodeRef.current.fy = wy;
    }

    function onMouseUp() {
      if (isDragging && dragNodeRef.current) {
        // For non-force layouts, keep node pinned at new position
        if (layoutRef.current === "force") {
          dragNodeRef.current.fx = null;
          dragNodeRef.current.fy = null;
        }
        simRef.current?.alphaTarget(0);
        isDragging = false;
        dragNodeRef.current = null;
      }
    }

    function onClick(event: MouseEvent) {
      if (didDrag) {
        didDrag = false;
        return;
      }
      const [wx, wy] = mouseToWorld(event);
      const node = findNodeAt(wx, wy);

      // Handle shortest path mode
      if (shortestPathModeRef.current) {
        if (node) {
          // If there's already a highlighted path, clear it first
          if (highlightedPathRef.current) {
            clearShortestPath();
            return;
          }
          handleShortestPathClick(node);
          return;
        }
        // Clicked empty space: clear path if displayed
        if (highlightedPathRef.current) {
          clearShortestPath();
          return;
        }
      }

      if (node) {
        const graphNode = {
          id: node.id,
          labels: node.labels,
          properties: node.properties,
        };
        if (event.shiftKey) {
          const { selectedNodes: selNodes, addToSelection, removeFromSelection } = useGraphStore.getState();
          const isAlreadySelected = selNodes.some((n) => n.id === node.id);
          if (isAlreadySelected) {
            removeFromSelection(graphNode);
          } else {
            addToSelection(graphNode);
          }
        } else {
          selectNode(graphNode);
        }
        drawRef.current();
        return;
      }
      const edge = findEdgeAt(wx, wy);
      if (edge) {
        selectEdge({
          id: edge.id,
          source: sourceNode(edge).id,
          target: targetNode(edge).id,
          type: edge.type,
          properties: edge.properties,
        });
        drawRef.current();
        return;
      }
      // Clicked on empty space -- clear selection
      selectNode(null);
      drawRef.current();
    }

    function onDblClick(event: MouseEvent) {
      if (!onNodeDoubleClick) return;
      const [wx, wy] = mouseToWorld(event);
      const node = findNodeAt(wx, wy);
      if (node) {
        event.preventDefault();
        onNodeDoubleClick({
          id: node.id,
          labels: node.labels,
          properties: node.properties,
        });
      }
    }

    function onContextMenu(event: MouseEvent) {
      event.preventDefault();
      const [wx, wy] = mouseToWorld(event);
      const node = findNodeAt(wx, wy);
      setContextMenu({
        x: event.clientX,
        y: event.clientY,
        nodeId: node?.id ?? null,
        nodeLabels: node?.labels ?? [],
        nodeProperties: node?.properties ?? {},
      });
    }

    canvas.addEventListener("mousedown", onMouseDown);
    canvas.addEventListener("mousemove", onMouseMove);
    window.addEventListener("mouseup", onMouseUp);
    canvas.addEventListener("click", onClick);
    canvas.addEventListener("dblclick", onDblClick);
    canvas.addEventListener("contextmenu", onContextMenu);

    return () => {
      observer.disconnect();
      sel.on(".zoom", null);
      canvas.removeEventListener("mousedown", onMouseDown);
      canvas.removeEventListener("mousemove", onMouseMove);
      window.removeEventListener("mouseup", onMouseUp);
      canvas.removeEventListener("click", onClick);
      canvas.removeEventListener("dblclick", onDblClick);
      canvas.removeEventListener("contextmenu", onContextMenu);
    };
    // Fix #8: Removed `draw` from deps — uses drawRef.current() throughout
  }, [selectNode, selectEdge, onNodeDoubleClick, handleShortestPathClick, clearShortestPath]);

  // Re-draw when selection or highlight mode changes (without restarting simulation)
  useEffect(() => {
    drawRef.current();
  }, [selectedNode, selectedEdge, selectedNodesFromStore, highlightMode, highlightedPath]);

  return (
    <div ref={containerRef} style={{ width: "100%", height: "100%", position: "relative" }}>
      <canvas ref={canvasRef} style={{ display: "block" }} />

      {/* Shortest path banner */}
      {shortestPathMode && pathBanner && (
        <div className="absolute top-2 left-1/2 -translate-x-1/2 z-20 px-4 py-1.5 rounded-full text-sm font-medium shadow-md bg-amber-100 text-amber-900 dark:bg-amber-900/80 dark:text-amber-100 border border-amber-300 dark:border-amber-700">
          {pathBanner}
        </div>
      )}

      {/* Toolbar */}
      {!hideToolbar && (
        <GraphToolbar
          onLayoutChange={(layout) => {
            applyLayout(layout as LayoutType);
          }}
          onFitToScreen={fitToScreen}
          onZoomIn={() => {
            if (zoomRef.current && canvasRef.current) {
              select<HTMLCanvasElement, unknown>(canvasRef.current).call(
                zoomRef.current.scaleBy,
                1.3,
              );
            }
          }}
          onZoomOut={() => {
            if (zoomRef.current && canvasRef.current) {
              select<HTMLCanvasElement, unknown>(canvasRef.current).call(
                zoomRef.current.scaleBy,
                0.7,
              );
            }
          }}
          onExportPNG={() => {
            const cvs = canvasRef.current;
            if (!cvs) return;
            const exportCanvas = document.createElement("canvas");
            exportCanvas.width = cvs.width;
            exportCanvas.height = cvs.height;
            const ctx = exportCanvas.getContext("2d");
            if (!ctx) return;
            const dark = document.documentElement.classList.contains("dark");
            ctx.fillStyle = dark ? "#0a0f1a" : "#ffffff";
            ctx.fillRect(0, 0, exportCanvas.width, exportCanvas.height);
            ctx.drawImage(cvs, 0, 0);
            const a = document.createElement("a");
            a.href = exportCanvas.toDataURL("image/png");
            a.download = "graphmind-export.png";
            a.click();
          }}
          onExportCSV={() => {
            const { columns, records } = useQueryStore.getState();
            if (columns.length === 0) return;
            const header = columns.join(",");
            const rows = records.map((r) => r.map((v) => JSON.stringify(v ?? "")).join(","));
            const csv = [header, ...rows].join("\n");
            const blob = new Blob([csv], { type: "text/csv" });
            const a = document.createElement("a");
            a.href = URL.createObjectURL(blob);
            a.download = "graphmind-export.csv";
            a.click();
          }}
          onExportJSON={() => {
            const { nodes: n, edges: e } = useGraphStore.getState();
            const json = JSON.stringify({ nodes: n, edges: e }, null, 2);
            const blob = new Blob([json], { type: "application/json" });
            const a = document.createElement("a");
            a.href = URL.createObjectURL(blob);
            a.download = "graphmind-export.json";
            a.click();
          }}
          onShortestPathToggle={(active) => {
            setShortestPathMode(active);
            if (active) {
              setPathStartNode(null);
              setHighlightedPath(null);
              setPathBanner("Click first node for shortest path");
            } else {
              setPathStartNode(null);
              setHighlightedPath(null);
              setPathBanner(null);
            }
          }}
        />
      )}

      {/* Graph Stats */}
      <GraphStats />

      {/* Context menu */}
      {contextMenu && (
        <div
          className="fixed z-[60] min-w-[200px] bg-popover text-popover-foreground border rounded-md shadow-md py-1"
          style={{ left: contextMenu.x, top: contextMenu.y }}
        >
          {contextMenu.nodeId && (
            <>
              <div className="px-3 py-1 text-[10px] font-medium text-muted-foreground uppercase tracking-wider">
                Node: {contextMenu.nodeLabels[0] ?? ""}
              </div>
              <button
                type="button"
                className="w-full text-left px-3 py-1.5 text-sm hover:bg-accent hover:text-accent-foreground transition-colors"
                onClick={() => handleExpandNeighbors(contextMenu.nodeId!)}
              >
                Expand Neighbors
              </button>
              <div className="my-1 border-t border-border" />
            </>
          )}
          <button
            type="button"
            className="w-full text-left px-3 py-1.5 text-sm hover:bg-accent hover:text-accent-foreground transition-colors"
            onClick={() => handleViewAllRelationships()}
          >
            Load All Relationships
          </button>
        </div>
      )}
    </div>
  );
});

// ---------------------------------------------------------------------------
// Canvas drawing helpers (kept outside component to avoid closure allocations)
// ---------------------------------------------------------------------------

function drawArrow(
  ctx: CanvasRenderingContext2D,
  tipX: number,
  tipY: number,
  ux: number,
  uy: number,
  isSelected: boolean,
  edgeColor: string,
) {
  const size = ARROW_SIZE;
  ctx.beginPath();
  ctx.moveTo(tipX, tipY);
  ctx.lineTo(tipX - ux * size + uy * size * 0.4, tipY - uy * size - ux * size * 0.4);
  ctx.lineTo(tipX - ux * size - uy * size * 0.4, tipY - uy * size + ux * size * 0.4);
  ctx.closePath();
  ctx.fillStyle = isSelected ? "#60a5fa" : edgeColor;
  ctx.fill();
}

function drawEdgeLabel(
  ctx: CanvasRenderingContext2D,
  label: string,
  x: number,
  y: number,
  zoomScale: number,
) {
  if (zoomScale < 0.5) return;
  // Fix #9: Font is already set once before the edge loop; just set alignment
  ctx.textAlign = "center";
  ctx.textBaseline = "middle";
  ctx.fillStyle = "rgba(148, 163, 184, 0.8)";
  ctx.fillText(label, x, y - 6);
}

export default ForceGraph;
