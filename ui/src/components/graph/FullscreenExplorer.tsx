import { useCallback, useEffect, useMemo, useRef, useState } from "react";
import { createPortal } from "react-dom";
import { useGraphStore } from "@/stores/graphStore";
import { useQueryStore } from "@/stores/queryStore";
import { useGraphViewStore } from "@/stores/graphViewStore";
import { getNodeCaption } from "@/lib/colors";
import { LegendPanel } from "@/components/graph/LegendPanel";
import { PropertyInspector } from "@/components/inspector/PropertyInspector";
import { executeQuery } from "@/api/client";

interface FullscreenExplorerProps {
  open: boolean;
  onClose: () => void;
}

function triggerDownload(url: string, filename: string) {
  const a = document.createElement("a"); a.href = url; a.download = filename;
  document.body.appendChild(a); a.click(); document.body.removeChild(a);
}

function isDark() { return document.documentElement.classList.contains("dark"); }

function IconButton({ icon, tip, active = false, color, onClick }: {
  icon: string; tip: string; active?: boolean; color?: string; onClick: () => void;
}) {
  const ac = color ?? "var(--th-accent)";
  return (
    <button onClick={onClick} title={tip} style={{
      width: 28, height: 28, borderRadius: 5,
      border: active ? `1px solid ${ac}` : "1px solid transparent",
      background: active ? `color-mix(in srgb, ${ac} 15%, transparent)` : "transparent",
      color: active ? ac : "var(--th-text-muted)",
      cursor: "pointer", fontSize: 13, display: "flex", alignItems: "center", justifyContent: "center",
      padding: 0, transition: "all 0.12s",
    }}>{icon}</button>
  );
}

function exportPNG() {
  const c = document.querySelector("canvas") as HTMLCanvasElement | null; if (!c) return;
  const e = document.createElement("canvas"); e.width = c.width; e.height = c.height;
  const ctx = e.getContext("2d"); if (!ctx) return;
  ctx.fillStyle = isDark() ? "#020810" : "#f8fafc"; ctx.fillRect(0, 0, e.width, e.height); ctx.drawImage(c, 0, 0);
  triggerDownload(e.toDataURL("image/png"), "graphmind-export.png");
}
function exportCSV() {
  const { columns, records } = useQueryStore.getState(); if (!columns.length) return;
  const csv = [columns.join(","), ...records.map((r) => r.map((c) => { const s = String(c ?? ""); return s.includes(",") || s.includes('"') ? `"${s.replace(/"/g, '""')}"` : s; }).join(","))].join("\n");
  triggerDownload(URL.createObjectURL(new Blob([csv], { type: "text/csv" })), "graphmind-export.csv");
}
function exportJSON() {
  const { nodes, edges } = useGraphStore.getState();
  triggerDownload(URL.createObjectURL(new Blob([JSON.stringify({ nodes, edges }, null, 2)], { type: "application/json" })), "graphmind-export.json");
}

export function FullscreenExplorer({ open, onClose }: FullscreenExplorerProps) {
  const nodes = useGraphStore((s) => s.nodes);
  const selectedNode = useGraphStore((s) => s.selectedNode);
  const selectedEdge = useGraphStore((s) => s.selectedEdge);

  // Read/write shared view store
  const layout = useGraphViewStore((s) => s.layout);
  const triggerLayout = useGraphViewStore((s) => s.triggerLayout);
  const triggerFit = useGraphViewStore((s) => s.triggerFit);
  const searchQuery = useGraphViewStore((s) => s.searchQuery);
  const setSearchQuery = useGraphViewStore((s) => s.setSearchQuery);
  const focusedLabels = useGraphViewStore((s) => s.focusedLabels);
  const toggleFocusLabel = useGraphViewStore((s) => s.toggleFocusLabel);
  const incremental = useGraphViewStore((s) => s.incremental);
  const toggleIncremental = useGraphViewStore((s) => s.toggleIncremental);
  const pathMode = useGraphViewStore((s) => s.pathMode);
  const pathSource = useGraphViewStore((s) => s.pathSource);
  const pathTarget = useGraphViewStore((s) => s.pathTarget);
  const pathResult = useGraphViewStore((s) => s.pathResult);
  const togglePathMode = useGraphViewStore((s) => s.togglePathMode);

  const [showLegend, setShowLegend] = useState(true);
  const [exportOpen, setExportOpen] = useState(false);
  const exportRef = useRef<HTMLDivElement>(null);
  const [contextMenu, setContextMenu] = useState<{ x: number; y: number; nodeId: string | null } | null>(null);

  // Debounced search for display
  const [localSearch, setLocalSearch] = useState("");
  useEffect(() => { const t = setTimeout(() => setSearchQuery(localSearch), 300); return () => clearTimeout(t); }, [localSearch, setSearchQuery]);
  // Sync local on open
  useEffect(() => { if (open) setLocalSearch(searchQuery); }, [open, searchQuery]);

  // Close on Escape
  useEffect(() => {
    if (!open) return;
    const h = (e: KeyboardEvent) => { if (e.key === "Escape") { if (contextMenu) setContextMenu(null); else onClose(); } };
    document.addEventListener("keydown", h); return () => document.removeEventListener("keydown", h);
  }, [open, onClose, contextMenu]);

  // Close dropdowns
  useEffect(() => {
    if (!exportOpen && !contextMenu) return;
    const h = () => { setExportOpen(false); setContextMenu(null); };
    document.addEventListener("click", h, true); return () => document.removeEventListener("click", h, true);
  }, [exportOpen, contextMenu]);

  const searchMatchCount = useMemo(() => {
    const q = localSearch.trim().toLowerCase(); if (!q) return null;
    return nodes.filter((n) => Object.values(n.properties).some((v) => typeof v === "string" && v.toLowerCase().includes(q))).length;
  }, [localSearch, nodes]);

  const handleExpandNeighbors = useCallback(async (nodeId: string) => {
    setContextMenu(null);
    try {
      const r = await executeQuery(`MATCH (n)-[r]-(m) WHERE id(n) = ${nodeId} RETURN n, r, m`);
      if (r.error) return;
      useGraphViewStore.getState().setIncremental(true);
      useGraphStore.getState().addGraphData(r.nodes, r.edges);
    } catch {}
  }, []);

  const handleViewAllRelationships = useCallback(async () => {
    setContextMenu(null);
    const state = useGraphStore.getState(); if (state.nodes.length === 0) return;
    try {
      const r = await executeQuery("MATCH (n)-[r]->(m) RETURN n, r, m"); if (r.error) return;
      const ids = new Set(state.nodes.map((n) => n.id));
      useGraphViewStore.getState().setIncremental(true);
      useGraphStore.getState().addGraphData([], r.edges.filter((e) => ids.has(e.source) && ids.has(e.target)));
    } catch {}
  }, []);

  const nodeLabel = useCallback((id: string | null) => {
    if (!id) return ""; const f = nodes.find((n) => n.id === id); if (!f) return id;
    return getNodeCaption(f.labels[0] ?? "Node", f.properties);
  }, [nodes]);

  if (!open) return null;

  return createPortal(
    <div className="fixed inset-0 z-[52] flex flex-col" style={{ pointerEvents: "none", overflow: "hidden" }}>

      {/* Search (top-left) */}
      <div style={{ position: "absolute", top: 14, left: 14, zIndex: 20, display: "flex", alignItems: "center", gap: 6, pointerEvents: "auto" }}>
        <div style={{ position: "relative", display: "flex", alignItems: "center" }}>
          <span style={{ position: "absolute", left: 10, fontSize: 12, color: "var(--th-text-dim)", pointerEvents: "none" }}>&#x2315;</span>
          <input type="text" placeholder="Search nodes..." value={localSearch} onChange={(e) => setLocalSearch(e.target.value)}
            style={{ width: 200, padding: "7px 10px 7px 28px", fontSize: 11, fontFamily: '"Inter",sans-serif', color: "var(--th-text)",
              background: "var(--th-overlay-blur)", backdropFilter: "blur(8px)", WebkitBackdropFilter: "blur(8px)",
              border: "1px solid var(--th-border-subtle)", borderRadius: 6, outline: "none" }} />
          {localSearch && <button onClick={() => setLocalSearch("")} style={{ position: "absolute", right: 8, background: "none", border: "none", color: "var(--th-text-dim)", cursor: "pointer", fontSize: 12, padding: "0 2px", lineHeight: 1 }}>&times;</button>}
        </div>
        {localSearch && searchMatchCount !== null && (
          <span style={{ fontSize: 9, color: searchMatchCount > 0 ? "var(--th-text-muted)" : "#ef4444", background: "var(--th-overlay-blur)", padding: "4px 8px", borderRadius: 4, border: "1px solid var(--th-border-subtle)" }}>
            {searchMatchCount > 0 ? `${searchMatchCount} found` : "No matches"}
          </span>
        )}
      </div>

      {/* Path mode status */}
      {pathMode && (
        <div style={{ position: "absolute", top: 14, left: "50%", transform: "translateX(-50%)", zIndex: 20, display: "flex", alignItems: "center", gap: 10, padding: "8px 16px", pointerEvents: "auto",
          background: "var(--th-overlay)", backdropFilter: "blur(12px)", border: "1px solid #10b98144", borderRadius: 8, fontSize: 10.5, color: "var(--th-text)", boxShadow: "0 4px 20px rgba(0,0,0,0.15)" }}>
          <span style={{ color: "#10b981", fontSize: 14 }}>&#x2B95;</span>
          {!pathSource && <span style={{ color: "var(--th-text-muted)" }}>Click the <strong style={{ color: "#10b981" }}>source</strong> node</span>}
          {pathSource && !pathTarget && <span style={{ color: "var(--th-text-muted)" }}><strong style={{ color: "var(--th-text)" }}>{nodeLabel(pathSource)}</strong> &rarr; Click the <strong style={{ color: "#10b981" }}>target</strong></span>}
          {pathSource && pathTarget && pathResult && (
            pathResult.distance === Infinity
              ? <span style={{ color: "#ef4444" }}>No path between <strong>{nodeLabel(pathSource)}</strong> and <strong>{nodeLabel(pathTarget)}</strong></span>
              : <span>
                  <span style={{ color: "#10b981" }}>{pathResult.nodeLabels.join(" → ")}</span>
                  <span style={{ color: "var(--th-text-faint)", marginLeft: 8 }}>{pathResult.distance} hop{pathResult.distance !== 1 ? "s" : ""}</span>
                </span>
          )}
          {pathSource && pathTarget && !pathResult && <span style={{ color: "var(--th-text-muted)" }}>Computing...</span>}
          <button onClick={togglePathMode} style={{ background: "none", border: "none", color: "var(--th-text-dim)", cursor: "pointer", fontSize: 13, lineHeight: 1, padding: "0 2px", marginLeft: 4 }}>&times;</button>
        </div>
      )}

      {/* Toolbar (top-right) */}
      <div style={{ position: "absolute", top: 12, right: 12, zIndex: 20, display: "flex", alignItems: "center", gap: 4, padding: "4px 6px", borderRadius: 8, pointerEvents: "auto",
        border: "1px solid var(--th-border-subtle)", background: "var(--th-overlay)", backdropFilter: "blur(12px)" }}>
        {([
          { id: "force" as const, icon: "◎", tip: "Force" },
          { id: "hierarchical" as const, icon: "⊥", tip: "Hierarchy" },
          { id: "circular" as const, icon: "○", tip: "Circle" },
          { id: "grid" as const, icon: "⊞", tip: "Grid" },
        ] as const).map((l) => (
          <IconButton key={l.id} icon={l.icon} tip={l.tip} active={layout === l.id} onClick={() => triggerLayout(l.id)} />
        ))}
        <span style={{ width: 1, height: 18, background: "var(--th-border-subtle)", margin: "0 2px" }} />
        <IconButton icon="⇢" tip="Shortest Path" active={pathMode} color="#10b981" onClick={togglePathMode} />
        <IconButton icon="↻" tip="Incremental layout" active={incremental} color="#f59e0b" onClick={toggleIncremental} />
        <IconButton icon="⊡" tip="Fit to screen" onClick={triggerFit} />
        <span style={{ width: 1, height: 18, background: "var(--th-border-subtle)", margin: "0 2px" }} />
        <IconButton icon="☰" tip="Legend" active={showLegend} onClick={() => setShowLegend((v) => !v)} />
        <div ref={exportRef} style={{ position: "relative" }}>
          <IconButton icon="⤓" tip="Export" onClick={() => setExportOpen((v) => !v)} />
          {exportOpen && (
            <div style={{ position: "absolute", right: 0, top: "100%", marginTop: 4, minWidth: 120, borderRadius: 6, border: "1px solid var(--th-border-subtle)",
              background: "var(--th-overlay)", backdropFilter: "blur(12px)", boxShadow: "0 4px 20px rgba(0,0,0,0.15)", padding: "4px 0", zIndex: 30 }}>
              {[{ label: "Export PNG", fn: exportPNG }, { label: "Export CSV", fn: exportCSV }, { label: "Export JSON", fn: exportJSON }].map((item) => (
                <button key={item.label} onClick={() => { item.fn(); setExportOpen(false); }}
                  style={{ display: "block", width: "100%", textAlign: "left", padding: "6px 12px", fontSize: 11, color: "var(--th-text)", background: "transparent", border: "none", cursor: "pointer" }}
                  onMouseEnter={(e) => (e.currentTarget.style.background = "var(--th-bg-elevated)")} onMouseLeave={(e) => (e.currentTarget.style.background = "transparent")}>
                  {item.label}
                </button>
              ))}
            </div>
          )}
        </div>
        <span style={{ width: 1, height: 18, background: "var(--th-border-subtle)", margin: "0 2px" }} />
        <IconButton icon="⊙" tip="Exit fullscreen (Esc)" onClick={onClose} />
      </div>

      {/* Hint */}
      {!selectedNode && !selectedEdge && !localSearch && !pathMode && (
        <div style={{ position: "absolute", bottom: 20, left: "50%", transform: "translateX(-50%)", fontSize: 10, color: "var(--th-text-faint)",
          pointerEvents: "none", letterSpacing: "0.06em", textAlign: "center", lineHeight: 1.8, zIndex: 5 }}>
          Scroll to zoom · Drag to pan · Click a node to inspect · Right-click for actions
        </div>
      )}

      {/* Legend */}
      {showLegend && <div style={{ pointerEvents: "auto" }}><LegendPanel focusedLabels={focusedLabels} onToggleFocus={toggleFocusLabel} onClose={() => setShowLegend(false)} /></div>}

      {/* Inspector */}
      {(selectedNode || selectedEdge) && (
        <div style={{ position: "absolute", top: 56, right: 14, zIndex: 20, width: 280, maxHeight: "70vh", overflowY: "auto", pointerEvents: "auto",
          background: "var(--th-overlay)", backdropFilter: "blur(12px)", border: "1px solid var(--th-border-subtle)", borderRadius: 10, boxShadow: "0 8px 32px rgba(0,0,0,0.18)" }}>
          <button onClick={() => useGraphStore.getState().clearSelection()} style={{ position: "absolute", right: 8, top: 8, background: "none", border: "none", color: "var(--th-text-dim)", cursor: "pointer", fontSize: 14, lineHeight: 1, zIndex: 10 }}>&times;</button>
          <PropertyInspector />
        </div>
      )}

      {/* Context menu */}
      {contextMenu && (
        <div style={{ position: "fixed", left: contextMenu.x, top: contextMenu.y, zIndex: 60, minWidth: 200, pointerEvents: "auto", background: "var(--th-overlay)", backdropFilter: "blur(12px)",
          border: "1px solid var(--th-border-subtle)", borderRadius: 8, boxShadow: "0 8px 32px rgba(0,0,0,0.18)", padding: "4px 0" }} onClick={(e) => e.stopPropagation()}>
          {contextMenu.nodeId && (
            <>
              <div style={{ padding: "6px 12px", fontSize: 9, color: "var(--th-text-dim)", textTransform: "uppercase", letterSpacing: "0.1em" }}>Node actions</div>
              <button onClick={() => handleExpandNeighbors(contextMenu.nodeId!)}
                style={{ display: "block", width: "100%", textAlign: "left", padding: "8px 12px", fontSize: 11, color: "var(--th-text)", background: "transparent", border: "none", cursor: "pointer" }}
                onMouseEnter={(e) => (e.currentTarget.style.background = "var(--th-bg-elevated)")} onMouseLeave={(e) => (e.currentTarget.style.background = "transparent")}>
                Expand Neighbors
              </button>
              <div style={{ margin: "2px 8px", borderTop: "1px solid var(--th-border-subtle)" }} />
            </>
          )}
          <button onClick={handleViewAllRelationships}
            style={{ display: "block", width: "100%", textAlign: "left", padding: "8px 12px", fontSize: 11, color: "var(--th-text)", background: "transparent", border: "none", cursor: "pointer" }}
            onMouseEnter={(e) => (e.currentTarget.style.background = "var(--th-bg-elevated)")} onMouseLeave={(e) => (e.currentTarget.style.background = "transparent")}>
            Load All Relationships
          </button>
        </div>
      )}
    </div>,
    document.body,
  );
}

export default FullscreenExplorer;
