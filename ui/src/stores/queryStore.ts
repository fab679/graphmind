import { create } from "zustand";
import { persist } from "zustand/middleware";
import { executeQuery as apiExecuteQuery, getSchema, getStatus } from "../api/client";
import { useGraphStore } from "./graphStore";
import { useUiStore } from "./uiStore";

export interface HistoryEntry {
  query: string;
  timestamp: number;
  duration: number;
  rowCount: number;
}

export interface SavedQuery {
  id: string;
  name: string;
  query: string;
  createdAt: number;
}

interface QueryState {
  currentQuery: string;
  isExecuting: boolean;
  columns: string[];
  records: unknown[][];
  error: string | null;
  history: HistoryEntry[];
  savedQueries: SavedQuery[];

  setQuery: (query: string) => void;
  executeQuery: (query?: string, graph?: string) => Promise<void>;
  clearResults: () => void;
  clearHistory: () => void;
  deleteHistoryEntry: (timestamp: number) => void;
  saveQuery: (name: string, query: string) => void;
  deleteSavedQuery: (id: string) => void;
}

const MAX_HISTORY = 50;

export const useQueryStore = create<QueryState>()(
  persist(
    (set, get) => ({
      currentQuery: "",
      isExecuting: false,
      columns: [],
      records: [],
      error: null,
      history: [],
      savedQueries: [],

      setQuery: (query) => set({ currentQuery: query }),

      executeQuery: async (query?: string, graph?: string) => {
        const q = query ?? get().currentQuery;
        if (!q.trim()) return;

        set({ isExecuting: true, error: null });
        const start = performance.now();

        try {
          const activeGraph = graph ?? useUiStore.getState().activeGraph;
          const response = await apiExecuteQuery(q, activeGraph);
          const duration = Math.round(performance.now() - start);

          if (response.error) {
            set({ error: response.error, isExecuting: false });
            return;
          }

          const { setGraphData } = useGraphStore.getState();
          setGraphData(response.nodes, response.edges);

          const entry: HistoryEntry = {
            query: q,
            timestamp: Date.now(),
            duration,
            rowCount: response.records.length,
          };

          set((state) => ({
            columns: response.columns,
            records: response.records,
            error: null,
            isExecuting: false,
            currentQuery: q,
            history: [entry, ...state.history].slice(0, MAX_HISTORY),
          }));

          // Auto-show bottom panel when results are table-only (no graph nodes)
          const hasGraphData = response.nodes.length > 0;
          const hasTableData = response.records.length > 0;
          if (hasTableData && !hasGraphData) {
            useUiStore.getState().setBottomPanelOpen(true);
          }

          // Refresh schema after write operations
          const upperQuery = q.toUpperCase();
          const isWrite = ['CREATE', 'DELETE', 'SET', 'MERGE', 'REMOVE', 'DETACH'].some(kw => upperQuery.includes(kw));
          if (isWrite) {
            const activeGraph = useUiStore.getState().activeGraph;
            getSchema(activeGraph).then(schema => {
              useUiStore.getState().setSchema(schema);
            }).catch(() => {});
            getStatus(activeGraph).then(status => {
              useUiStore.getState().setServerInfo(
                status.version || "",
                status.storage?.nodes ?? 0,
                status.storage?.edges ?? 0,
              );
            }).catch(() => {});
          }
        } catch (err) {
          const message =
            err instanceof Error ? err.message : "Query execution failed";
          set({ error: message, isExecuting: false });
        }
      },

      clearResults: () =>
        set({ columns: [], records: [], error: null }),

      clearHistory: () => set({ history: [] }),

      deleteHistoryEntry: (timestamp) =>
        set((state) => ({
          history: state.history.filter((h) => h.timestamp !== timestamp),
        })),

      saveQuery: (name, query) =>
        set((state) => ({
          savedQueries: [
            {
              id: `${Date.now()}-${Math.random().toString(36).slice(2, 9)}`,
              name,
              query,
              createdAt: Date.now(),
            },
            ...state.savedQueries,
          ],
        })),

      deleteSavedQuery: (id) =>
        set((state) => ({
          savedQueries: state.savedQueries.filter((q) => q.id !== id),
        })),
    }),
    {
      name: "graphmind-query-history",
      partialize: (state) => ({
        history: state.history,
        savedQueries: state.savedQueries,
      }),
    },
  ),
);
