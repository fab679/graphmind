import { create } from "zustand";
import { persist } from "zustand/middleware";

interface GraphSettings {
  // Custom colors: label/type -> hex color. If not set, falls back to default.
  labelColors: Record<string, string>;
  edgeColors: Record<string, string>;

  // Which property to show as caption on nodes, per label.
  // e.g. { "Person": "name", "City": "name", "Car": "make" }
  captionProperty: Record<string, string>;

  // Icon/avatar settings per label
  labelIcons: Record<string, string>;       // label -> icon name from catalog
  imageProperty: Record<string, string>;    // label -> property name to use as image URL

  // Highlight mode: when true and a node is selected, connected nodes are highlighted and rest dimmed
  highlightMode: boolean;

  // Actions
  setLabelColor: (label: string, color: string) => void;
  setEdgeColor: (edgeType: string, color: string) => void;
  resetLabelColor: (label: string) => void;
  resetEdgeColor: (edgeType: string) => void;
  setCaptionProperty: (label: string, property: string) => void;
  setLabelIcon: (label: string, iconName: string) => void;
  resetLabelIcon: (label: string) => void;
  setImageProperty: (label: string, propertyName: string) => void;
  resetImageProperty: (label: string) => void;
  toggleHighlightMode: () => void;
  resetAll: () => void;
}

export const useGraphSettingsStore = create<GraphSettings>()(
  persist(
    (set) => ({
      labelColors: {},
      edgeColors: {},
      captionProperty: {},
      labelIcons: {},
      imageProperty: {},
      highlightMode: false,

      setLabelColor: (label, color) =>
        set((state) => ({
          labelColors: { ...state.labelColors, [label]: color },
        })),

      setEdgeColor: (edgeType, color) =>
        set((state) => ({
          edgeColors: { ...state.edgeColors, [edgeType]: color },
        })),

      resetLabelColor: (label) =>
        set((state) => {
          const next = { ...state.labelColors };
          delete next[label];
          return { labelColors: next };
        }),

      resetEdgeColor: (edgeType) =>
        set((state) => {
          const next = { ...state.edgeColors };
          delete next[edgeType];
          return { edgeColors: next };
        }),

      setCaptionProperty: (label, property) =>
        set((state) => ({
          captionProperty: { ...state.captionProperty, [label]: property },
        })),

      setLabelIcon: (label, iconName) =>
        set((state) => ({
          labelIcons: { ...state.labelIcons, [label]: iconName },
        })),

      resetLabelIcon: (label) =>
        set((state) => {
          const next = { ...state.labelIcons };
          delete next[label];
          return { labelIcons: next };
        }),

      setImageProperty: (label, propertyName) =>
        set((state) => ({
          imageProperty: { ...state.imageProperty, [label]: propertyName },
        })),

      resetImageProperty: (label) =>
        set((state) => {
          const next = { ...state.imageProperty };
          delete next[label];
          return { imageProperty: next };
        }),

      toggleHighlightMode: () =>
        set((state) => ({ highlightMode: !state.highlightMode })),

      resetAll: () =>
        set({
          labelColors: {},
          edgeColors: {},
          captionProperty: {},
          labelIcons: {},
          imageProperty: {},
          highlightMode: false,
        }),
    }),
    {
      name: "graphmind-graph-settings",
      partialize: (state) => ({
        labelColors: state.labelColors,
        edgeColors: state.edgeColors,
        captionProperty: state.captionProperty,
        labelIcons: state.labelIcons,
        imageProperty: state.imageProperty,
        highlightMode: state.highlightMode,
      }),
    },
  ),
);
