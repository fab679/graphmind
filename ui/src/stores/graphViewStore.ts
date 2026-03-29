import { create } from "zustand";

/** Transient graph view state — shared between QueryTab and FullscreenExplorer.
 *  NOT persisted (resets on page reload). */

export type LayoutId = "force" | "circular" | "grid" | "hierarchical";

export interface PathResult {
  nodeLabels: string[];
  distance: number;
}

interface GraphViewState {
  layout: LayoutId;
  layoutTick: number;
  fitTick: number;
  searchQuery: string;
  focusedLabels: string[];
  incremental: boolean;
  pathMode: boolean;
  pathSource: string | null;
  pathTarget: string | null;
  pathResult: PathResult | null;

  setLayout: (l: LayoutId) => void;
  triggerLayout: (l: LayoutId) => void;
  triggerFit: () => void;
  setSearchQuery: (q: string) => void;
  toggleFocusLabel: (label: string) => void;
  setIncremental: (v: boolean) => void;
  toggleIncremental: () => void;
  setPathMode: (v: boolean) => void;
  togglePathMode: () => void;
  setPathSource: (id: string | null) => void;
  setPathTarget: (id: string | null) => void;
  selectPathNode: (id: string) => void;
  setPathResult: (r: PathResult | null) => void;
  clearPath: () => void;
}

export const useGraphViewStore = create<GraphViewState>((set, get) => ({
  layout: "force",
  layoutTick: 0,
  fitTick: 0,
  searchQuery: "",
  focusedLabels: [],
  incremental: false,
  pathMode: false,
  pathSource: null,
  pathTarget: null,
  pathResult: null,

  setLayout: (l) => set({ layout: l }),
  triggerLayout: (l) => set((s) => ({ layout: l, layoutTick: s.layoutTick + 1 })),
  triggerFit: () => set((s) => ({ fitTick: s.fitTick + 1 })),
  setSearchQuery: (q) => set({ searchQuery: q }),
  toggleFocusLabel: (label) =>
    set((s) => ({
      focusedLabels: s.focusedLabels.includes(label)
        ? s.focusedLabels.filter((l) => l !== label)
        : [...s.focusedLabels, label],
    })),
  setIncremental: (v) => set({ incremental: v }),
  toggleIncremental: () => set((s) => ({ incremental: !s.incremental })),
  setPathMode: (v) => {
    if (!v) set({ pathMode: false, pathSource: null, pathTarget: null, pathResult: null });
    else set({ pathMode: true });
  },
  togglePathMode: () => {
    const s = get();
    if (s.pathMode) set({ pathMode: false, pathSource: null, pathTarget: null, pathResult: null });
    else set({ pathMode: true, pathSource: null, pathTarget: null, pathResult: null });
  },
  setPathSource: (id) => set({ pathSource: id }),
  setPathTarget: (id) => set({ pathTarget: id }),
  selectPathNode: (id) => {
    const s = get();
    if (!s.pathSource) set({ pathSource: id, pathTarget: null, pathResult: null });
    else if (!s.pathTarget && id !== s.pathSource) set({ pathTarget: id });
    else set({ pathSource: id, pathTarget: null, pathResult: null });
  },
  setPathResult: (r) => set({ pathResult: r }),
  clearPath: () => set({ pathSource: null, pathTarget: null, pathResult: null }),
}));
