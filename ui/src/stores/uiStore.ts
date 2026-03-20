import { create } from "zustand";
import type { SchemaResponse } from "../types/api";

type ConnectionStatus = "connected" | "disconnected" | "checking";

interface UiState {
  connectionStatus: ConnectionStatus;
  serverVersion: string;
  nodeCount: number;
  edgeCount: number;
  bottomPanelOpen: boolean;
  rightPanelOpen: boolean;
  schema: SchemaResponse | null;

  setConnectionStatus: (status: ConnectionStatus) => void;
  setServerInfo: (version: string, nodes: number, edges: number) => void;
  setSchema: (schema: SchemaResponse | null) => void;
  toggleRightPanel: () => void;
  toggleBottomPanel: () => void;
  setBottomPanelOpen: (open: boolean) => void;
  activeGraph: string;
  availableGraphs: string[];
  setActiveGraph: (graph: string) => void;
  setAvailableGraphs: (graphs: string[]) => void;
}

export const useUiStore = create<UiState>((set) => ({
  connectionStatus: "checking",
  serverVersion: "",
  nodeCount: 0,
  edgeCount: 0,
  bottomPanelOpen: false,
  rightPanelOpen: true,
  schema: null,

  setConnectionStatus: (connectionStatus) => set({ connectionStatus }),

  setServerInfo: (serverVersion, nodeCount, edgeCount) =>
    set({ serverVersion, nodeCount, edgeCount }),

  setSchema: (schema) => set({ schema }),

  toggleRightPanel: () =>
    set((state) => ({ rightPanelOpen: !state.rightPanelOpen })),

  toggleBottomPanel: () =>
    set((state) => ({ bottomPanelOpen: !state.bottomPanelOpen })),

  setBottomPanelOpen: (bottomPanelOpen) => set({ bottomPanelOpen }),

  activeGraph: "default",
  availableGraphs: ["default"],
  setActiveGraph: (activeGraph) => set({ activeGraph }),
  setAvailableGraphs: (availableGraphs) => set({ availableGraphs }),
}));
