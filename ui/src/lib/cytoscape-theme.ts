// Cytoscape canvas colours (can't use CSS vars on <canvas>)

export type ResolvedTheme = "light" | "dark";

export interface CyTheme {
  nodeBg: string;
  nodeBorder: string;
  nodeLabel: string;
  edgeColor: string;
  edgeLabelColor: string;
  edgeLabelBg: string;
  selectedBorder: string;
  dimmedOpacity: number;
}

export const CY_THEMES: Record<ResolvedTheme, CyTheme> = {
  light: {
    nodeBg: "#e2e8f0",
    nodeBorder: "#94a3b8",
    nodeLabel: "#1e293b",
    edgeColor: "#94a3b8",
    edgeLabelColor: "#475569",
    edgeLabelBg: "#f1f5f9",
    selectedBorder: "#0f172a",
    dimmedOpacity: 0.12,
  },
  dark: {
    nodeBg: "#1e293b",
    nodeBorder: "#334155",
    nodeLabel: "#cbd5e1",
    edgeColor: "#334155",
    edgeLabelColor: "#94a3b8",
    edgeLabelBg: "#0c1425",
    selectedBorder: "#f8fafc",
    dimmedOpacity: 0.08,
  },
};
