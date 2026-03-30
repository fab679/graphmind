import { useEffect, useRef, useCallback, useMemo } from "react";
import cytoscape from "cytoscape";
import fcose from "cytoscape-fcose";
import type { Core, LayoutOptions, NodeSingular, EdgeSingular } from "cytoscape";
import type { GraphNode, GraphEdge } from "@/types/api";
import { CY_THEMES, type ResolvedTheme, type CyTheme } from "@/lib/cytoscape-theme";
import { getCustomColorForLabel, getCustomEdgeColor, getNodeCaption } from "@/lib/colors";
import { NODE_ICON_CATALOG } from "@/lib/icons";
import { useGraphSettingsStore } from "@/stores/graphSettingsStore";
import { useGraphStore } from "@/stores/graphStore";
import { useGraphViewStore } from "@/stores/graphViewStore";

cytoscape.use(fcose);

function svgIconToDataUri(iconName: string, color: string): string {
  const icon = NODE_ICON_CATALOG.find((i) => i.name === iconName);
  if (!icon?.path) return "";
  return `data:image/svg+xml,${encodeURIComponent(`<svg xmlns="http://www.w3.org/2000/svg" width="48" height="48" viewBox="0 0 24 24"><path d="${icon.path}" fill="${color}"/></svg>`)}`;
}

// eslint-disable-next-line @typescript-eslint/no-explicit-any
function buildStylesheet(t: CyTheme, labels: string[], edgeTypes: string[], s: { labelColors: Record<string,string>; edgeColors: Record<string,string>; edgeDashed: Record<string,boolean> }): any[] {
  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  const ss: any[] = [
    { selector: "node", style: {
      width: 38, height: 38, shape: "ellipse",
      "background-color": t.nodeBg, "border-width": 2.5, "border-color": t.nodeBorder, "border-style": "solid",
      label: "data(displayLabel)", "font-family": '"Inter",sans-serif', "font-size": 10, color: t.nodeLabel,
      "text-valign": "bottom", "text-halign": "center", "text-margin-y": 8, "text-wrap": "wrap", "text-max-width": "90px",
      "text-outline-color": t.edgeLabelBg, "text-outline-width": 3,
      "text-background-color": t.edgeLabelBg, "text-background-opacity": 0.75, "text-background-padding": "2px", "text-background-shape": "roundrectangle",
      "min-zoomed-font-size": 8,
      "transition-property": "border-color, border-width, width, height, opacity", "transition-duration": "200ms",
    } as Record<string,unknown> },
    { selector: "node.has-bg", style: { "background-image": "data(_bgImage)", "background-fit": "contain", "background-clip": "node" } },
    { selector: "node.image-mode", style: { "background-fit": "cover" } },
  ];
  for (const label of labels) {
    const c = s.labelColors[label] || getCustomColorForLabel(label);
    ss.push({ selector: `node[nodeLabel="${label}"]`, style: { width: 40, height: 40, "background-color": c, "border-color": c, "border-width": 2.5 } });
  }
  ss.push({ selector: "edge", style: {
    width: 1.8, "line-color": t.edgeColor, "target-arrow-color": t.edgeColor, "target-arrow-shape": "triangle", "arrow-scale": 0.9,
    "curve-style": "bezier", "control-point-step-size": 40, "line-style": "solid", opacity: 0.8,
    label: "data(edgeType)", "font-family": '"Inter",sans-serif', "font-size": 8, color: t.edgeLabelColor,
    "text-opacity": 0.85, "text-rotation": "autorotate", "text-margin-y": -8,
    "text-background-color": t.edgeLabelBg, "text-background-opacity": 0.85, "text-background-padding": "2px", "min-zoomed-font-size": 6,
    "transition-property": "opacity, line-color, width", "transition-duration": "200ms",
  } as Record<string,unknown> });
  for (const et of edgeTypes) {
    const c = s.edgeColors[et] || getCustomEdgeColor(et);
    const d = s.edgeDashed[et] ?? false;
    ss.push({ selector: `edge[edgeType="${et}"]`, style: {
      "line-color": c, "target-arrow-color": c, width: 2, opacity: 0.85,
      "line-style": d ? "dashed" as const : "solid" as const, ...(d ? { "line-dash-pattern": [6,3] } : {}),
    } });
  }
  ss.push(
    { selector: "node:selected", style: { "border-width": 3, "border-color": t.selectedBorder, width: 48, height: 48 } },
    { selector: "edge:selected", style: { width: 3, opacity: 1 } },
    { selector: ".dimmed", style: { opacity: t.dimmedOpacity } },
    { selector: ".highlighted", style: { opacity: 1, "border-width": 3.5 } },
  );
  return ss;
}

const LAYOUTS: Record<string, LayoutOptions> = {
  force: { name: "fcose", animate: true, animationDuration: 800, randomize: true,
    nodeRepulsion: 45000, idealEdgeLength: 200, edgeElasticity: 0.1, gravity: 0.02, gravityRange: 3.8,
    numIter: 5000, quality: "default", nodeSeparation: 150, componentSpacing: 200, fit: true, padding: 80 } as LayoutOptions,
  circular: { name: "circle", animate: true, animationDuration: 600, fit: true, padding: 80, spacingFactor: 1.6 },
  grid: { name: "grid", animate: true, animationDuration: 600, fit: true, padding: 80, rows: 4, spacingFactor: 1.4 },
  hierarchical: { name: "breadthfirst", animate: true, animationDuration: 600, fit: true, padding: 80, directed: true, spacingFactor: 2.0 },
};

function layoutPadding(n: number) { return n <= 3 ? 200 : n <= 10 ? 140 : n <= 30 ? 100 : 80; }

function buildCyElements(nodes: GraphNode[], edges: GraphEdge[]) {
  const settings = useGraphSettingsStore.getState();
  const cyNodes = nodes.map((n) => {
    const nodeLabel = n.labels[0] ?? "Node";
    const displayLabel = getNodeCaption(nodeLabel, n.properties);
    const iconName = settings.labelIcons[nodeLabel];
    const imageProp = settings.imageProperty[nodeLabel];
    let bgImage = "";
    if (iconName) bgImage = svgIconToDataUri(iconName, "#ffffff");
    else if (imageProp) { const url = n.properties[imageProp]; if (typeof url === "string" && url) bgImage = url; }
    return { group: "nodes" as const, data: { ...n.properties, id: n.id, nodeLabel, displayLabel, _bgImage: bgImage } };
  });
  const cyEdges = edges.map((e) => ({
    group: "edges" as const, data: { ...e.properties, id: `e_${e.id}`, _originalId: e.id, source: e.source, target: e.target, edgeType: e.type },
  }));
  return [...cyNodes, ...cyEdges];
}

// ---- Props (only resolvedTheme + context menu callback) ----

interface CytoscapeGraphProps {
  resolvedTheme: ResolvedTheme;
  onContextMenu?: (x: number, y: number, nodeId: string | null) => void;
}

export function CytoscapeGraph({ resolvedTheme, onContextMenu }: CytoscapeGraphProps) {
  const containerRef = useRef<HTMLDivElement>(null);
  const cyRef = useRef<Core | null>(null);
  const ringRef = useRef<HTMLDivElement>(null);
  const selectedNodeRef = useRef<NodeSingular | null>(null);
  const ringColorRef = useRef("#7c6af7");

  // data stores
  const nodes = useGraphStore((s) => s.nodes);
  const edges = useGraphStore((s) => s.edges);
  const labelColors = useGraphSettingsStore((s) => s.labelColors);
  const edgeColors = useGraphSettingsStore((s) => s.edgeColors);
  const edgeDashed = useGraphSettingsStore((s) => s.edgeDashed);
  const labelIcons = useGraphSettingsStore((s) => s.labelIcons);
  const imageProperty = useGraphSettingsStore((s) => s.imageProperty);
  const captionProperty = useGraphSettingsStore((s) => s.captionProperty);

  // view store
  const layout = useGraphViewStore((s) => s.layout);
  const layoutTick = useGraphViewStore((s) => s.layoutTick);
  const fitTick = useGraphViewStore((s) => s.fitTick);
  const searchQuery = useGraphViewStore((s) => s.searchQuery);
  const focusedLabels = useGraphViewStore((s) => s.focusedLabels);
  const incremental = useGraphViewStore((s) => s.incremental);
  const pathMode = useGraphViewStore((s) => s.pathMode);
  const pathSource = useGraphViewStore((s) => s.pathSource);
  const pathTarget = useGraphViewStore((s) => s.pathTarget);

  const pathModeRef = useRef(pathMode); pathModeRef.current = pathMode;

  const cyTheme = CY_THEMES[resolvedTheme];
  const labels = useMemo(() => [...new Set(nodes.flatMap((n) => n.labels))], [nodes]);
  const edgeTypes = useMemo(() => [...new Set(edges.map((e) => e.type))], [edges]);
  const stylesheet = useMemo(
    () => buildStylesheet(cyTheme, labels, edgeTypes, { labelColors, edgeColors, edgeDashed: edgeDashed ?? {} }),
    [cyTheme, labels, edgeTypes, labelColors, edgeColors, edgeDashed],
  );

  const updateRing = useCallback(() => {
    const ring = ringRef.current; const node = selectedNodeRef.current;
    if (!ring || !node || node.removed()) { if (ring) ring.style.display = "none"; return; }
    const pos = node.renderedPosition(); const size = node.renderedOuterWidth() + 22; const c = ringColorRef.current;
    ring.style.left = `${pos.x}px`; ring.style.top = `${pos.y}px`; ring.style.width = `${size}px`; ring.style.height = `${size}px`;
    ring.style.background = `conic-gradient(from 0deg, transparent 0%, ${c}cc 12%, transparent 25%, transparent 50%, ${c}cc 62%, transparent 75%)`;
    ring.style.setProperty("-webkit-mask", "radial-gradient(circle, transparent 60%, black 63%)");
    ring.style.setProperty("mask", "radial-gradient(circle, transparent 60%, black 63%)");
    ring.style.display = "block";
  }, []);
  const clearRing = useCallback(() => { selectedNodeRef.current = null; if (ringRef.current) ringRef.current.style.display = "none"; }, []);

  // ==== INIT once ====
  useEffect(() => {
    if (!containerRef.current) return;
    const cy = cytoscape({
      container: containerRef.current, elements: [], style: [],
      layout: { name: "preset" },
      userZoomingEnabled: true, userPanningEnabled: true, boxSelectionEnabled: false,
      maxZoom: 1.8, minZoom: 0.05,
    });
    cyRef.current = cy;
    cy.on("render", updateRing);

    cy.on("tap", "node", (evt) => {
      const node = evt.target as NodeSingular;
      if (pathModeRef.current) {
        useGraphViewStore.getState().selectPathNode(node.id());
        return;
      }
      cy.elements().addClass("dimmed"); node.removeClass("dimmed").addClass("highlighted");
      node.connectedEdges().forEach((e: EdgeSingular) => { e.removeClass("dimmed").addClass("highlighted"); });
      selectedNodeRef.current = node; ringColorRef.current = getCustomColorForLabel(node.data("nodeLabel")); updateRing();
      const gNode = useGraphStore.getState().nodes.find((n) => n.id === node.id());
      if (gNode) useGraphStore.getState().selectNode(gNode);
    });
    cy.on("tap", "edge", (evt) => {
      if (pathModeRef.current) return; clearRing();
      const gEdge = useGraphStore.getState().edges.find((e) => e.id === (evt.target as EdgeSingular).data("_originalId"));
      if (gEdge) useGraphStore.getState().selectEdge(gEdge);
    });
    cy.on("tap", (evt) => {
      if (evt.target === cy) {
        if (pathModeRef.current) {
          // In path mode, clicking background resets source/target selection
          useGraphViewStore.getState().clearPath();
        } else {
          cy.elements().removeClass("dimmed highlighted"); clearRing();
          useGraphStore.getState().clearSelection();
        }
      }
    });
    cy.on("cxttap", "node", (evt) => {
      const node = evt.target as NodeSingular;
      const pos = evt.renderedPosition; if (pos && onContextMenu) {
        const rect = containerRef.current?.getBoundingClientRect();
        onContextMenu((rect?.left ?? 0) + pos.x, (rect?.top ?? 0) + pos.y, node.id());
      }
    });
    cy.on("cxttap", (evt) => {
      if (evt.target === cy && onContextMenu) {
        const pos = evt.renderedPosition; const rect = containerRef.current?.getBoundingClientRect();
        onContextMenu((rect?.left ?? 0) + (pos?.x ?? 0), (rect?.top ?? 0) + (pos?.y ?? 0), null);
      }
    });
    cy.on("mouseover", "node", () => { if (containerRef.current) containerRef.current.style.cursor = "pointer"; });
    cy.on("mouseout", "node", () => { if (containerRef.current) containerRef.current.style.cursor = "default"; });
    return () => { cy.destroy(); cyRef.current = null; clearRing(); };
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, []);

  // ==== UPDATE elements ====
  useEffect(() => {
    const cy = cyRef.current; if (!cy || !containerRef.current) return;
    const newEls = buildCyElements(nodes, edges);
    const newIds = new Set(newEls.map((el) => el.data.id));
    const toRemove = cy.elements().filter((ele) => !newIds.has(ele.id()));
    if (toRemove.length > 0) toRemove.remove();
    const existingIds = new Set(cy.elements().map((ele) => ele.id()));
    const toAdd = newEls.filter((el) => !existingIds.has(el.data.id));
    if (toAdd.length > 0) cy.add(toAdd);
    for (const el of newEls) {
      const cyEl = cy.getElementById(el.data.id);
      if (cyEl.length > 0 && el.group === "nodes") {
        const d = el.data as Record<string, unknown>;
        if (d.displayLabel !== undefined) cyEl.data("displayLabel", d.displayLabel);
        if (d._bgImage !== undefined) { cyEl.data("_bgImage", d._bgImage); if (d._bgImage) cyEl.addClass("has-bg"); else cyEl.removeClass("has-bg"); }
      }
    }
    if (toAdd.length > 0 || toRemove.length > 0) {
      const nc = cy.nodes().length; const ec = cy.edges().length; const pad = layoutPadding(nc);
      const isIncremental = useGraphViewStore.getState().incremental;
      if (isIncremental) {
        // Incremental update (expand/load): position new nodes near connected existing nodes
        const addedNodeIds = new Set(toAdd.filter((el) => el.group === "nodes").map((el) => el.data.id));
        for (const nid of addedNodeIds) {
          const newNode = cy.getElementById(nid);
          if (newNode.length === 0) continue;
          const neighbors = newNode.connectedEdges().connectedNodes().filter((n: NodeSingular) => !addedNodeIds.has(n.id()));
          if (neighbors.length > 0) {
            const neighbor = neighbors.first() as NodeSingular;
            const pos = neighbor.position();
            newNode.position({ x: pos.x + (Math.random() - 0.5) * 100, y: pos.y + (Math.random() - 0.5) * 100 });
          }
        }
        cy.layout({ ...LAYOUTS.force, padding: pad, randomize: false, animate: true } as LayoutOptions).run();
        useGraphViewStore.getState().setIncremental(false);
      } else if (ec === 0 && nc > 1) {
        // No edges: use grid layout so disconnected nodes spread apart
        cy.layout({ name: "grid", animate: true, animationDuration: 400, fit: true, padding: pad, rows: Math.ceil(Math.sqrt(nc)), spacingFactor: 1.5 } as LayoutOptions).run();
      } else {
        cy.layout({ ...LAYOUTS.force, padding: pad } as LayoutOptions).run();
      }
    }
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [nodes, edges, labelIcons, imageProperty, captionProperty]);

  // ==== stylesheet ====
  useEffect(() => {
    const cy = cyRef.current; if (!cy) return;
    const sn = selectedNodeRef.current;
    if (sn && !sn.removed()) ringColorRef.current = getCustomColorForLabel(sn.data("nodeLabel"));
    cy.style().fromJson(stylesheet).update();
    cy.batch(() => { cy.nodes().forEach((node) => { if (node.data("_bgImage")) node.addClass("has-bg"); }); });
    if (sn && !sn.removed()) updateRing();
  }, [stylesheet, updateRing]);

  // ==== layout (toolbar change) ====
  useEffect(() => {
    const cy = cyRef.current; if (!cy || !containerRef.current || cy.nodes().length === 0 || layoutTick === 0) return;
    const base = LAYOUTS[layout] ?? LAYOUTS.force;
    const nc = cy.nodes().length; const pad = layoutPadding(nc);
    const w = containerRef.current.clientWidth; const h = containerRef.current.clientHeight;
    if (base.name === "fcose") {
      const opts = { ...base, padding: pad, randomize: incremental ? false : true };
      if (!incremental) cy.nodes().positions({ x: w/2, y: h/2 });
      cy.layout(opts as LayoutOptions).run();
    } else {
      cy.nodes().positions({ x: w/2, y: h/2 });
      cy.layout({ ...base, padding: pad } as LayoutOptions).run();
    }
  }, [layout, layoutTick, incremental]);

  // ==== fit ====
  useEffect(() => {
    if (fitTick === 0) return; const cy = cyRef.current; if (!cy || cy.nodes().length === 0) return;
    cy.animate({ fit: { eles: cy.elements(), padding: 80 }, duration: 300 } as unknown as cytoscape.AnimateOptions);
  }, [fitTick]);

  // ==== search + focus filter ====
  useEffect(() => {
    const cy = cyRef.current; if (!cy || pathMode) return;
    cy.elements().removeClass("dimmed highlighted");
    const q = searchQuery.trim().toLowerCase(); const hasFilter = focusedLabels.length > 0;
    if (!q && !hasFilter) return;
    let visible = cy.nodes();
    if (hasFilter) { const ls = new Set(focusedLabels); visible = visible.filter((n: NodeSingular) => ls.has(n.data("nodeLabel"))); }
    if (q) visible = visible.filter((n: NodeSingular) => Object.values(n.data()).some((v) => typeof v === "string" && v.toLowerCase().includes(q)));
    if (visible.length === 0) { cy.elements().addClass("dimmed"); return; }
    if (visible.length === cy.nodes().length) return;
    cy.elements().addClass("dimmed"); visible.removeClass("dimmed").addClass("highlighted");
    cy.edges().forEach((e) => { if (visible.contains(e.source()) && visible.contains(e.target())) e.removeClass("dimmed").addClass("highlighted"); });
  }, [searchQuery, focusedLabels, pathMode]);

  // ==== shortest path ====
  useEffect(() => {
    const cy = cyRef.current; if (!cy) return;
    const setPathResult = useGraphViewStore.getState().setPathResult;
    if (!pathMode) { cy.elements().removeClass("dimmed highlighted"); setPathResult(null); return; }
    cy.elements().removeClass("dimmed highlighted");
    if (pathSource && !pathTarget) {
      const src = cy.getElementById(pathSource);
      if (src.length) { cy.elements().addClass("dimmed"); src.removeClass("dimmed").addClass("highlighted"); }
      setPathResult(null);
      return;
    }
    if (pathSource && pathTarget) {
      const src = cy.getElementById(pathSource); const tgt = cy.getElementById(pathTarget);
      if (src.length && tgt.length) {
        const dij = cy.elements().dijkstra({ root: src, weight: () => 1, directed: false });
        const path = dij.pathTo(tgt);
        if (path && path.length > 0) {
          cy.elements().addClass("dimmed"); path.removeClass("dimmed").addClass("highlighted");
          const nodeLabels: string[] = [];
          path.forEach((e) => { if (e.isNode()) nodeLabels.push(e.data("displayLabel") as string); });
          setPathResult({ nodeLabels, distance: dij.distanceTo(tgt) });
        } else {
          cy.elements().addClass("dimmed");
          src.removeClass("dimmed").addClass("highlighted");
          tgt.removeClass("dimmed").addClass("highlighted");
          setPathResult({ nodeLabels: [], distance: Infinity });
        }
      }
    }
  }, [pathMode, pathSource, pathTarget]);

  return (
    <div style={{ width: "100%", height: "100%", position: "relative", overflow: "hidden" }}>
      <div ref={containerRef} style={{ width: "100%", height: "100%" }} />
      <div ref={ringRef} style={{ display: "none", position: "absolute", borderRadius: "50%", pointerEvents: "none", zIndex: 10, animation: "focus-ring-spin 2.5s linear infinite" }} />
    </div>
  );
}

export default CytoscapeGraph;
