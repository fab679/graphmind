import { create } from "zustand";
import type { GraphEdge, GraphNode } from "../types/api";

interface GraphState {
  nodes: GraphNode[];
  edges: GraphEdge[];
  selectedNode: GraphNode | null;
  selectedEdge: GraphEdge | null;
  selectedNodes: GraphNode[];

  setGraphData: (nodes: GraphNode[], edges: GraphEdge[]) => void;
  addGraphData: (nodes: GraphNode[], edges: GraphEdge[]) => void;
  selectNode: (node: GraphNode | null) => void;
  selectEdge: (edge: GraphEdge | null) => void;
  addToSelection: (node: GraphNode) => void;
  removeFromSelection: (node: GraphNode) => void;
  selectAll: () => void;
  clearSelection: () => void;
  clearGraph: () => void;
  removeNodeFromCanvas: (nodeId: string) => void;
  removeSelectedFromCanvas: () => void;
}

export const useGraphStore = create<GraphState>((set, get) => ({
  nodes: [],
  edges: [],
  selectedNode: null,
  selectedEdge: null,
  selectedNodes: [],

  setGraphData: (nodes, edges) =>
    set({ nodes, edges, selectedNode: null, selectedEdge: null, selectedNodes: [] }),

  addGraphData: (newNodes, newEdges) =>
    set((state) => {
      const existingNodeIds = new Set(state.nodes.map((n) => n.id));
      const existingEdgeIds = new Set(state.edges.map((e) => e.id));
      const mergedNodes = [...state.nodes];
      for (const node of newNodes) {
        if (!existingNodeIds.has(node.id)) {
          mergedNodes.push(node);
          existingNodeIds.add(node.id);
        }
      }
      const mergedEdges = [...state.edges];
      for (const edge of newEdges) {
        if (!existingEdgeIds.has(edge.id)) {
          mergedEdges.push(edge);
          existingEdgeIds.add(edge.id);
        }
      }
      return { nodes: mergedNodes, edges: mergedEdges };
    }),

  selectNode: (node) =>
    set({
      selectedNode: node,
      selectedEdge: null,
      selectedNodes: node ? [node] : [],
    }),

  selectEdge: (edge) =>
    set({ selectedEdge: edge, selectedNode: null, selectedNodes: [] }),

  addToSelection: (node) =>
    set((state) => {
      const alreadySelected = state.selectedNodes.some((n) => n.id === node.id);
      if (alreadySelected) return state;
      const newSelected = [...state.selectedNodes, node];
      return {
        selectedNodes: newSelected,
        selectedNode: node,
        selectedEdge: null,
      };
    }),

  removeFromSelection: (node) =>
    set((state) => {
      const newSelected = state.selectedNodes.filter((n) => n.id !== node.id);
      return {
        selectedNodes: newSelected,
        selectedNode: newSelected.length > 0 ? newSelected[newSelected.length - 1] : null,
        selectedEdge: null,
      };
    }),

  selectAll: () =>
    set((state) => ({
      selectedNodes: [...state.nodes],
      selectedNode: state.nodes.length > 0 ? state.nodes[0] : null,
      selectedEdge: null,
    })),

  clearSelection: () =>
    set({ selectedNode: null, selectedEdge: null, selectedNodes: [] }),

  clearGraph: () =>
    set({ nodes: [], edges: [], selectedNode: null, selectedEdge: null, selectedNodes: [] }),

  removeNodeFromCanvas: (nodeId) =>
    set((state) => {
      const nodes = state.nodes.filter((n) => n.id !== nodeId);
      const edges = state.edges.filter(
        (e) => e.source !== nodeId && e.target !== nodeId,
      );
      const selectedNodes = state.selectedNodes.filter((n) => n.id !== nodeId);
      const selectedNode =
        state.selectedNode?.id === nodeId
          ? (selectedNodes.length > 0 ? selectedNodes[selectedNodes.length - 1] : null)
          : state.selectedNode;
      return { nodes, edges, selectedNodes, selectedNode };
    }),

  removeSelectedFromCanvas: () => {
    const state = get();
    const selectedIds = new Set(state.selectedNodes.map((n) => n.id));
    if (selectedIds.size === 0) return;
    const nodes = state.nodes.filter((n) => !selectedIds.has(n.id));
    const edges = state.edges.filter(
      (e) => !selectedIds.has(e.source) && !selectedIds.has(e.target),
    );
    set({
      nodes,
      edges,
      selectedNode: null,
      selectedEdge: null,
      selectedNodes: [],
    });
  },
}));
