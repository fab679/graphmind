import { useCallback, useEffect, useMemo, useRef, useState } from "react";
import {
  ChevronDown,
  ChevronRight,
  Download,
  Highlighter,
  LayoutGrid,
  Maximize,
  Route,
  Search,
  X,
  ZoomIn,
  ZoomOut,
} from "lucide-react";
import { useGraphStore } from "@/stores/graphStore";
import { useGraphSettingsStore } from "@/stores/graphSettingsStore";
import { useQueryStore } from "@/stores/queryStore";
import { getCustomColorForLabel, getCustomEdgeColor } from "@/lib/colors";
import { NODE_ICON_CATALOG } from "@/lib/icons";
import { ForceGraph } from "@/components/graph/ForceGraph";
import type { ForceGraphHandle } from "@/components/graph/ForceGraph";
import { PropertyInspector } from "@/components/inspector/PropertyInspector";

// ---------------------------------------------------------------------------
// Types
// ---------------------------------------------------------------------------

interface FullscreenExplorerProps {
  open: boolean;
  onClose: () => void;
}

// ---------------------------------------------------------------------------
// Preset color palette (matches SchemaBrowser)
// ---------------------------------------------------------------------------

const PRESET_COLORS = [
  "#6366f1",
  "#3b82f6",
  "#10b981",
  "#f59e0b",
  "#ef4444",
  "#8b5cf6",
  "#ec4899",
  "#14b8a6",
  "#f97316",
  "#06b6d4",
  "#84cc16",
  "#64748b",
];

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

function isDark(): boolean {
  return document.documentElement.classList.contains("dark");
}

function glassStyle(): React.CSSProperties {
  return isDark()
    ? {
        background: "rgba(20, 20, 30, 0.8)",
        backdropFilter: "blur(12px)",
        border: "1px solid rgba(255, 255, 255, 0.1)",
      }
    : {
        background: "rgba(255, 255, 255, 0.8)",
        backdropFilter: "blur(12px)",
        border: "1px solid rgba(0, 0, 0, 0.1)",
      };
}

function triggerDownload(url: string, filename: string) {
  const link = document.createElement("a");
  link.href = url;
  link.download = filename;
  document.body.appendChild(link);
  link.click();
  document.body.removeChild(link);
}

function exportPNG() {
  const canvas = document.querySelector(
    "[data-fullscreen-explorer] canvas",
  ) as HTMLCanvasElement | null;
  if (!canvas) return;
  const exportCanvas = document.createElement("canvas");
  exportCanvas.width = canvas.width;
  exportCanvas.height = canvas.height;
  const ctx = exportCanvas.getContext("2d");
  if (!ctx) return;
  ctx.fillStyle = isDark() ? "#0a0f1a" : "#ffffff";
  ctx.fillRect(0, 0, exportCanvas.width, exportCanvas.height);
  ctx.drawImage(canvas, 0, 0);
  triggerDownload(exportCanvas.toDataURL("image/png"), "graphmind-export.png");
}

function exportCSV() {
  const { columns, records } = useQueryStore.getState();
  if (columns.length === 0) return;

  const header = columns.join(",");
  const rows = records.map((row) =>
    row
      .map((cell) => {
        const str = String(cell ?? "");
        return str.includes(",") || str.includes('"')
          ? `"${str.replace(/"/g, '""')}"`
          : str;
      })
      .join(","),
  );
  const csv = [header, ...rows].join("\n");
  const blob = new Blob([csv], { type: "text/csv;charset=utf-8;" });
  triggerDownload(URL.createObjectURL(blob), "graphmind-export.csv");
}

function exportJSON() {
  const { nodes, edges } = useGraphStore.getState();
  const json = JSON.stringify({ nodes, edges }, null, 2);
  const blob = new Blob([json], { type: "application/json" });
  triggerDownload(URL.createObjectURL(blob), "graphmind-export.json");
}

// ---------------------------------------------------------------------------
// InlineColorPicker
// ---------------------------------------------------------------------------

function InlineColorPicker({
  currentColor,
  onSelect,
}: {
  currentColor: string;
  onSelect: (color: string) => void;
}) {
  return (
    <div className="flex flex-wrap gap-1 p-1.5">
      {PRESET_COLORS.map((color) => (
        <button
          key={color}
          type="button"
          className={`h-4 w-4 rounded-sm transition-transform hover:scale-110 ${
            currentColor === color
              ? "ring-2 ring-white/70 ring-offset-1 ring-offset-transparent"
              : ""
          }`}
          style={{ backgroundColor: color }}
          onClick={() => onSelect(color)}
          title={color}
        />
      ))}
    </div>
  );
}

// ---------------------------------------------------------------------------
// Floating Legend Panel
// ---------------------------------------------------------------------------

function FloatingLegend() {
  const nodes = useGraphStore((s) => s.nodes);
  const edges = useGraphStore((s) => s.edges);
  const setLabelColor = useGraphSettingsStore((s) => s.setLabelColor);
  const setEdgeColor = useGraphSettingsStore((s) => s.setEdgeColor);
  const labelIcons = useGraphSettingsStore((s) => s.labelIcons);
  const setLabelIcon = useGraphSettingsStore((s) => s.setLabelIcon);
  const resetLabelIcon = useGraphSettingsStore((s) => s.resetLabelIcon);

  const [collapsed, setCollapsed] = useState(false);
  const [pickerTarget, setPickerTarget] = useState<{
    kind: "label" | "edge" | "icon";
    name: string;
  } | null>(null);

  const labelCounts = useMemo(() => {
    const map = new Map<string, number>();
    for (const node of nodes) {
      const label = node.labels[0] ?? "Node";
      map.set(label, (map.get(label) ?? 0) + 1);
    }
    return Array.from(map.entries()).sort((a, b) => b[1] - a[1]);
  }, [nodes]);

  const edgeTypeCounts = useMemo(() => {
    const map = new Map<string, number>();
    for (const edge of edges) {
      map.set(edge.type, (map.get(edge.type) ?? 0) + 1);
    }
    return Array.from(map.entries()).sort((a, b) => b[1] - a[1]);
  }, [edges]);

  if (nodes.length === 0 && edges.length === 0) return null;

  return (
    <div
      className="rounded-xl shadow-lg w-full"
      style={glassStyle()}
    >
      {/* Toggle header */}
      <button
        type="button"
        onClick={() => setCollapsed((prev) => !prev)}
        className="flex items-center gap-1.5 w-full px-3 py-2 text-xs font-semibold text-foreground/80 hover:text-foreground transition-colors"
      >
        {collapsed ? (
          <ChevronRight className="h-3 w-3" />
        ) : (
          <ChevronDown className="h-3 w-3" />
        )}
        Legend
      </button>

      {!collapsed && (
        <div className="max-h-[60vh] overflow-y-auto px-3 pb-3">
          {/* Node labels */}
          {labelCounts.length > 0 && (
            <>
              <div className="text-[10px] font-semibold text-muted-foreground uppercase tracking-wider mb-1">
                Node Labels
              </div>
              {labelCounts.map(([label, count]) => {
                const color = getCustomColorForLabel(label);
                const iconName = labelIcons[label];
                const icon = iconName ? NODE_ICON_CATALOG.find((i) => i.name === iconName) : null;
                return (
                  <div key={label} className="rounded-md hover:bg-accent/20 transition-colors">
                    <div className="flex items-center gap-1.5 px-1.5 py-1">
                      {/* Color swatch */}
                      <button
                        type="button"
                        className="h-3.5 w-3.5 rounded-full flex-shrink-0 ring-1 ring-black/10 hover:ring-2 hover:ring-primary/50 transition-all"
                        style={{ backgroundColor: color }}
                        onClick={() =>
                          setPickerTarget(
                            pickerTarget?.kind === "label" && pickerTarget.name === label
                              ? null
                              : { kind: "label", name: label },
                          )
                        }
                        title="Change color"
                      />
                      {/* Icon button */}
                      <button
                        type="button"
                        className="h-5 w-5 flex-shrink-0 flex items-center justify-center rounded hover:bg-accent/40 transition-colors"
                        onClick={() =>
                          setPickerTarget(
                            pickerTarget?.kind === "icon" && pickerTarget.name === label
                              ? null
                              : { kind: "icon", name: label },
                          )
                        }
                        title="Change icon"
                      >
                        {icon && icon.path ? (
                          <svg viewBox="0 0 24 24" className="h-3.5 w-3.5" style={{ color }}>
                            <path d={icon.path} fill="currentColor" />
                          </svg>
                        ) : (
                          <div className="h-2.5 w-2.5 rounded-full" style={{ backgroundColor: color, opacity: 0.5 }} />
                        )}
                      </button>
                      <span className="flex-1 text-xs font-medium text-foreground/90 truncate">
                        {label}
                      </span>
                      <span className="text-[10px] font-mono text-muted-foreground tabular-nums bg-muted/50 px-1.5 py-0.5 rounded">
                        {count}
                      </span>
                    </div>
                    {/* Color picker */}
                    {pickerTarget?.kind === "label" && pickerTarget.name === label && (
                      <div className="mt-1">
                        <InlineColorPicker
                          currentColor={color}
                          onSelect={(c) => {
                            setLabelColor(label, c);
                            setPickerTarget(null);
                          }}
                        />
                      </div>
                    )}
                    {/* Icon picker */}
                    {pickerTarget?.kind === "icon" && pickerTarget.name === label && (
                      <div className="mt-1.5 p-2 rounded-lg border border-border/50 bg-card/80 max-h-[300px] overflow-y-auto">
                        {(() => {
                          const categories = [...new Set(NODE_ICON_CATALOG.filter(ic => ic.path).map(ic => ic.category))];
                          return categories.map((cat) => (
                            <div key={cat} className="mb-2 last:mb-0">
                              <div className="text-[9px] font-semibold text-muted-foreground uppercase tracking-wider mb-0.5">{cat}</div>
                              <div className="grid grid-cols-7 gap-0.5">
                                {NODE_ICON_CATALOG.filter(ic => ic.category === cat && ic.path).map((ic) => (
                                  <button
                                    key={ic.name}
                                    type="button"
                                    className={`h-6 w-6 flex items-center justify-center rounded transition-colors ${
                                      iconName === ic.name
                                        ? "bg-primary text-primary-foreground ring-1 ring-primary"
                                        : "hover:bg-accent text-foreground/60 hover:text-foreground"
                                    }`}
                                    onClick={() => { setLabelIcon(label, ic.name); setPickerTarget(null); }}
                                    title={ic.name}
                                  >
                                    <svg viewBox="0 0 24 24" className="h-3.5 w-3.5"><path d={ic.path} fill="currentColor" /></svg>
                                  </button>
                                ))}
                              </div>
                            </div>
                          ));
                        })()}
                        <div className="border-t border-border/30 mt-1.5 pt-1.5">
                          <button
                            type="button"
                            className="w-full text-center text-[10px] text-muted-foreground hover:text-foreground transition-colors py-0.5"
                            onClick={() => { resetLabelIcon(label); setPickerTarget(null); }}
                          >
                            Reset to default circle
                          </button>
                        </div>
                      </div>
                    )}
                  </div>
                );
              })}
            </>
          )}

          {/* Edge types */}
          {edgeTypeCounts.length > 0 && (
            <>
              <div className="text-[10px] font-semibold text-muted-foreground uppercase tracking-wider mt-3 mb-1">
                Edge Types
              </div>
              {edgeTypeCounts.map(([edgeType, count]) => {
                const color = getCustomEdgeColor(edgeType);
                return (
                  <div key={edgeType} className="py-0.5">
                    <div className="flex items-center gap-2">
                      <button
                        type="button"
                        className="h-3 w-3 rounded-full flex-shrink-0 hover:ring-2 hover:ring-white/40 transition"
                        style={{ backgroundColor: color }}
                        onClick={() =>
                          setPickerTarget(
                            pickerTarget?.kind === "edge" &&
                              pickerTarget.name === edgeType
                              ? null
                              : { kind: "edge", name: edgeType },
                          )
                        }
                        title="Change color"
                      />
                      <span className="flex-1 text-xs text-foreground/80 truncate">
                        {edgeType}
                      </span>
                      <span className="text-[10px] text-muted-foreground tabular-nums">
                        {count}
                      </span>
                    </div>
                    {pickerTarget?.kind === "edge" &&
                      pickerTarget.name === edgeType && (
                        <div className="mt-1">
                          <InlineColorPicker
                            currentColor={color}
                            onSelect={(c) => {
                              setEdgeColor(edgeType, c);
                              setPickerTarget(null);
                            }}
                          />
                        </div>
                      )}
                  </div>
                );
              })}
            </>
          )}
        </div>
      )}
    </div>
  );
}

// ---------------------------------------------------------------------------
// Floating Search Bar
// ---------------------------------------------------------------------------

function FloatingSearch({
  searchText,
  onSearchChange,
  matchCount,
  totalCount,
}: {
  searchText: string;
  onSearchChange: (value: string) => void;
  matchCount: number | null;
  totalCount: number;
}) {
  return (
    <div
      className="rounded-xl shadow-lg flex items-center gap-2 px-3 py-2 w-full"
      style={glassStyle()}
    >
      <Search className="h-4 w-4 text-muted-foreground flex-shrink-0" />
      <input
        type="text"
        value={searchText}
        onChange={(e) => onSearchChange(e.target.value)}
        placeholder="Search nodes..."
        className="flex-1 bg-transparent text-sm text-foreground placeholder:text-muted-foreground focus:outline-none min-w-0"
      />
      {matchCount !== null && (
        <span className="text-[10px] text-muted-foreground whitespace-nowrap tabular-nums">
          {matchCount}/{totalCount}
        </span>
      )}
      {searchText && (
        <button
          type="button"
          onClick={() => onSearchChange("")}
          className="text-muted-foreground hover:text-foreground transition-colors"
          title="Clear search"
        >
          <X className="h-3.5 w-3.5" />
        </button>
      )}
    </div>
  );
}

// ---------------------------------------------------------------------------
// Minimap
// ---------------------------------------------------------------------------

function Minimap() {
  const nodes = useGraphStore((s) => s.nodes);
  const canvasRef = useRef<HTMLCanvasElement>(null);

  useEffect(() => {
    const minimapCanvas = canvasRef.current;
    if (!minimapCanvas) return;
    const ctx = minimapCanvas.getContext("2d");
    if (!ctx) return;

    let animId = 0;
    let stopped = false;

    function snapshot() {
      if (stopped || !ctx || !minimapCanvas) return;

      // Find the main ForceGraph canvas in the DOM
      const container = minimapCanvas.closest("[data-fullscreen-explorer]");
      const mainCanvas = container?.querySelector(
        'canvas:not([data-minimap])',
      ) as HTMLCanvasElement | null;

      ctx.clearRect(0, 0, 150, 100);

      if (mainCanvas && mainCanvas.width > 0 && mainCanvas.height > 0) {
        // Scale the main canvas into the minimap
        ctx.drawImage(mainCanvas, 0, 0, 150, 100);

        // Draw viewport rectangle
        ctx.strokeStyle = isDark()
          ? "rgba(255,255,255,0.6)"
          : "rgba(0,0,0,0.5)";
        ctx.lineWidth = 1;
        ctx.strokeRect(20, 15, 110, 70);
      }

      animId = window.setTimeout(() => {
        if (!stopped) snapshot();
      }, 500);
    }

    snapshot();

    return () => {
      stopped = true;
      clearTimeout(animId);
    };
  }, [nodes]);

  return (
    <div
      className="absolute bottom-3 right-3 rounded-xl shadow-lg overflow-hidden"
      style={glassStyle()}
    >
      <canvas
        ref={canvasRef}
        data-minimap
        width={150}
        height={100}
        className="block"
        style={{ width: 150, height: 100 }}
      />
    </div>
  );
}

// ---------------------------------------------------------------------------
// Toolbar button
// ---------------------------------------------------------------------------

function ToolbarButton({
  title,
  active = false,
  onClick,
  children,
}: {
  title: string;
  active?: boolean;
  onClick: () => void;
  children: React.ReactNode;
}) {
  return (
    <button
      type="button"
      onClick={onClick}
      title={title}
      className={`rounded-md p-1.5 transition-colors ${
        active
          ? "bg-primary text-primary-foreground"
          : "text-foreground/70 hover:text-foreground hover:bg-foreground/10"
      }`}
    >
      {children}
    </button>
  );
}

// ---------------------------------------------------------------------------
// FullscreenExplorer
// ---------------------------------------------------------------------------

export function FullscreenExplorer({ open, onClose }: FullscreenExplorerProps) {
  const nodes = useGraphStore((s) => s.nodes);
  const selectedNode = useGraphStore((s) => s.selectedNode);
  const selectedEdge = useGraphStore((s) => s.selectedEdge);

  const highlightMode = useGraphSettingsStore((s) => s.highlightMode);
  const toggleHighlightMode = useGraphSettingsStore(
    (s) => s.toggleHighlightMode,
  );

  const [searchText, setSearchText] = useState("");
  const [exportOpen, setExportOpen] = useState(false);

  const exportRef = useRef<HTMLDivElement>(null);
  const layoutRef = useRef<HTMLDivElement>(null);
  const graphRef = useRef<ForceGraphHandle>(null);
  const [layoutOpen, setLayoutOpen] = useState(false);
  const [currentLayout, setCurrentLayout] = useState("force");
  const [spMode, setSpMode] = useState(false);

  // Compute search match count
  const matchCount = useMemo(() => {
    if (!searchText.trim()) return null;
    const lower = searchText.toLowerCase();
    let count = 0;
    for (const node of nodes) {
      const values = Object.values(node.properties);
      const matches = values.some((v) =>
        String(v ?? "")
          .toLowerCase()
          .includes(lower),
      );
      if (matches) count++;
    }
    return count;
  }, [searchText, nodes]);

  // Close on Escape
  useEffect(() => {
    if (!open) return;
    function handleKey(e: KeyboardEvent) {
      if (e.key === "Escape") onClose();
    }
    document.addEventListener("keydown", handleKey);
    return () => document.removeEventListener("keydown", handleKey);
  }, [open, onClose]);

  // Auto-fit graph when fullscreen opens (after canvas has time to resize)
  useEffect(() => {
    if (!open) return;
    const timer = setTimeout(() => {
      graphRef.current?.fitToScreen();
    }, 200);
    return () => clearTimeout(timer);
  }, [open]);

  // Close export dropdown on outside click
  useEffect(() => {
    if (!exportOpen && !layoutOpen) return;
    function handleClick(e: MouseEvent) {
      if (exportOpen && exportRef.current && !exportRef.current.contains(e.target as Node)) {
        setExportOpen(false);
      }
      if (layoutOpen && layoutRef.current && !layoutRef.current.contains(e.target as Node)) {
        setLayoutOpen(false);
      }
    }
    document.addEventListener("click", handleClick, true);
    return () => document.removeEventListener("click", handleClick, true);
  }, [exportOpen, layoutOpen]);

  const handleSearchChange = useCallback((value: string) => {
    setSearchText(value);
  }, []);

  if (!open) return null;

  return (
    <div
      className="fixed inset-0 z-50 bg-background flex flex-col"
      data-fullscreen-explorer
    >
      {/* Top bar */}
      <div className="flex items-center justify-between px-3 py-2 border-b border-border/50 bg-background/80 backdrop-blur-sm z-20">
        {/* Left: Close */}
        <button
          type="button"
          onClick={onClose}
          title="Close fullscreen (Esc)"
          className="rounded-md p-1.5 text-foreground/70 hover:text-foreground hover:bg-foreground/10 transition-colors"
        >
          <X className="h-5 w-5" />
        </button>

        {/* Right: Controls */}
        <div className="flex items-center gap-0.5">
          {/* Layout selector */}
          <div ref={layoutRef} className="relative">
            <ToolbarButton
              title={`Layout: ${currentLayout}`}
              onClick={() => { setLayoutOpen((p) => !p); setExportOpen(false); }}
            >
              <LayoutGrid className="h-4 w-4" />
            </ToolbarButton>
            {layoutOpen && (
              <div className="absolute right-0 top-full mt-1 min-w-[130px] rounded-md border bg-popover text-popover-foreground shadow-md py-1 z-10">
                {["force", "circular", "hierarchical", "grid"].map((layout) => (
                  <button
                    key={layout}
                    type="button"
                    className={`w-full text-left px-3 py-1.5 text-sm transition-colors ${
                      currentLayout === layout ? "bg-accent text-accent-foreground" : "hover:bg-accent/50"
                    }`}
                    onClick={() => { setCurrentLayout(layout); setLayoutOpen(false); graphRef.current?.applyLayout(layout); }}
                  >
                    {layout.charAt(0).toUpperCase() + layout.slice(1)}
                  </button>
                ))}
              </div>
            )}
          </div>

          <div className="w-px h-5 bg-border mx-0.5" />

          <ToolbarButton
            title="Toggle highlight mode"
            active={highlightMode}
            onClick={toggleHighlightMode}
          >
            <Highlighter className="h-4 w-4" />
          </ToolbarButton>

          <ToolbarButton
            title="Shortest path mode"
            active={spMode}
            onClick={() => {
              const next = !spMode;
              setSpMode(next);
              graphRef.current?.setShortestPathMode(next);
            }}
          >
            <Route className="h-4 w-4" />
          </ToolbarButton>

          <div className="w-px h-5 bg-border mx-0.5" />

          <ToolbarButton title="Zoom in" onClick={() => graphRef.current?.zoomIn()}>
            <ZoomIn className="h-4 w-4" />
          </ToolbarButton>

          <ToolbarButton title="Zoom out" onClick={() => graphRef.current?.zoomOut()}>
            <ZoomOut className="h-4 w-4" />
          </ToolbarButton>

          <ToolbarButton title="Fit to screen" onClick={() => graphRef.current?.fitToScreen()}>
            <Maximize className="h-4 w-4" />
          </ToolbarButton>

          <div className="w-px h-5 bg-border mx-0.5" />

          {/* Export dropdown */}
          <div ref={exportRef} className="relative">
            <ToolbarButton
              title="Export"
              onClick={() => { setExportOpen((p) => !p); setLayoutOpen(false); }}
            >
              <Download className="h-4 w-4" />
            </ToolbarButton>

            {exportOpen && (
              <div className="absolute right-0 top-full mt-1 min-w-[120px] rounded-md border bg-popover text-popover-foreground shadow-md py-1 z-10">
                <button
                  type="button"
                  className="w-full text-left px-3 py-1.5 text-sm hover:bg-accent hover:text-accent-foreground transition-colors"
                  onClick={() => { exportPNG(); setExportOpen(false); }}
                >
                  Export PNG
                </button>
                <button
                  type="button"
                  className="w-full text-left px-3 py-1.5 text-sm hover:bg-accent hover:text-accent-foreground transition-colors"
                  onClick={() => { exportCSV(); setExportOpen(false); }}
                >
                  Export CSV
                </button>
                <button
                  type="button"
                  className="w-full text-left px-3 py-1.5 text-sm hover:bg-accent hover:text-accent-foreground transition-colors"
                  onClick={() => { exportJSON(); setExportOpen(false); }}
                >
                  Export JSON
                </button>
              </div>
            )}
          </div>
        </div>
      </div>

      {/* Canvas area (fills remaining space) */}
      <div className="flex-1 min-h-0 relative">
        <ForceGraph ref={graphRef} searchQuery={searchText} hideToolbar />

        {/* Left column: search + legend */}
        <div className="absolute top-3 left-3 z-10 flex flex-col gap-2 w-[220px]">
          <FloatingSearch
            searchText={searchText}
            onSearchChange={handleSearchChange}
            matchCount={matchCount}
            totalCount={nodes.length}
          />
          <FloatingLegend />
        </div>

        {/* Floating inspector (right side, appears when node/edge selected) */}
        {(selectedNode || selectedEdge) && (
          <div
            className="absolute top-3 right-3 z-10 w-[280px] max-h-[70vh] overflow-y-auto rounded-xl shadow-lg"
            style={glassStyle()}
          >
            <PropertyInspector />
          </div>
        )}

        {/* Minimap */}
        <Minimap />
      </div>
    </div>
  );
}

export default FullscreenExplorer;
