import { useCallback, useRef, useState } from "react";
import {
  Download,
  Highlighter,
  LayoutGrid,
  Maximize,
  Maximize2,
  Route,
  ZoomIn,
  ZoomOut,
} from "lucide-react";
import { useGraphSettingsStore } from "@/stores/graphSettingsStore";

// ---------------------------------------------------------------------------
// Types
// ---------------------------------------------------------------------------

interface GraphToolbarProps {
  onLayoutChange: (layout: string) => void;
  onFullscreen: () => void;
  onFitToScreen: () => void;
  onZoomIn: () => void;
  onZoomOut: () => void;
  onExportPNG: () => void;
  onExportCSV: () => void;
  onExportJSON: () => void;
  onShortestPathToggle?: (active: boolean) => void;
}

// ---------------------------------------------------------------------------
// Constants
// ---------------------------------------------------------------------------

const LAYOUT_OPTIONS: { value: string; label: string }[] = [
  { value: "force", label: "Force" },
  { value: "circular", label: "Circular" },
  { value: "hierarchical", label: "Hierarchical" },
  { value: "grid", label: "Grid" },
];

// ---------------------------------------------------------------------------
// ToolbarButton
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
      className={`rounded-md border p-1.5 shadow-sm transition-colors ${
        active
          ? "bg-primary text-primary-foreground border-primary"
          : "bg-popover text-popover-foreground border-border hover:bg-accent"
      }`}
    >
      {children}
    </button>
  );
}

// ---------------------------------------------------------------------------
// Component
// ---------------------------------------------------------------------------

export function GraphToolbar({
  onLayoutChange,
  onFullscreen,
  onFitToScreen,
  onZoomIn,
  onZoomOut,
  onExportPNG,
  onExportCSV,
  onExportJSON,
  onShortestPathToggle,
}: GraphToolbarProps) {
  const highlightMode = useGraphSettingsStore((s) => s.highlightMode);
  const toggleHighlightMode = useGraphSettingsStore((s) => s.toggleHighlightMode);

  const [layoutOpen, setLayoutOpen] = useState(false);
  const [exportOpen, setExportOpen] = useState(false);
  const [shortestPathActive, setShortestPathActive] = useState(false);
  const [currentLayout, setCurrentLayout] = useState("force");
  const layoutRef = useRef<HTMLDivElement>(null);
  const exportRef = useRef<HTMLDivElement>(null);

  const handleLayoutSelect = useCallback(
    (layout: string) => {
      setCurrentLayout(layout);
      onLayoutChange(layout);
      setLayoutOpen(false);
    },
    [onLayoutChange],
  );

  const handleShortestPathToggle = useCallback(() => {
    setShortestPathActive((prev) => {
      const next = !prev;
      onShortestPathToggle?.(next);
      return next;
    });
  }, [onShortestPathToggle]);

  return (
    <div className="absolute top-2 right-2 flex items-center gap-1 z-10">
      {/* Layout selector */}
      <div ref={layoutRef} className="relative">
        <ToolbarButton
          title={`Layout: ${currentLayout}`}
          onClick={() => {
            setLayoutOpen((prev) => !prev);
            setExportOpen(false);
          }}
        >
          <LayoutGrid className="h-4 w-4" />
        </ToolbarButton>

        {layoutOpen && (
          <div className="absolute right-0 top-full mt-1 min-w-[140px] rounded-md border bg-popover text-popover-foreground shadow-md py-1">
            {LAYOUT_OPTIONS.map((opt) => (
              <button
                key={opt.value}
                type="button"
                className={`w-full text-left px-3 py-1.5 text-sm transition-colors ${
                  currentLayout === opt.value
                    ? "bg-accent text-accent-foreground"
                    : "hover:bg-accent hover:text-accent-foreground"
                }`}
                onClick={() => handleLayoutSelect(opt.value)}
              >
                {opt.label}
              </button>
            ))}
          </div>
        )}
      </div>

      {/* Highlight mode */}
      <ToolbarButton
        title="Toggle highlight mode"
        active={highlightMode}
        onClick={toggleHighlightMode}
      >
        <Highlighter className="h-4 w-4" />
      </ToolbarButton>

      {/* Shortest path */}
      <ToolbarButton
        title="Shortest path mode"
        active={shortestPathActive}
        onClick={handleShortestPathToggle}
      >
        <Route className="h-4 w-4" />
      </ToolbarButton>

      {/* Fullscreen */}
      <ToolbarButton title="Fullscreen" onClick={onFullscreen}>
        <Maximize2 className="h-4 w-4" />
      </ToolbarButton>

      {/* Export dropdown */}
      <div ref={exportRef} className="relative">
        <ToolbarButton
          title="Export"
          onClick={() => {
            setExportOpen((prev) => !prev);
            setLayoutOpen(false);
          }}
        >
          <Download className="h-4 w-4" />
        </ToolbarButton>

        {exportOpen && (
          <div className="absolute right-0 top-full mt-1 min-w-[120px] rounded-md border bg-popover text-popover-foreground shadow-md py-1">
            <button
              type="button"
              className="w-full text-left px-3 py-1.5 text-sm hover:bg-accent hover:text-accent-foreground transition-colors"
              onClick={() => {
                onExportPNG();
                setExportOpen(false);
              }}
            >
              PNG
            </button>
            <button
              type="button"
              className="w-full text-left px-3 py-1.5 text-sm hover:bg-accent hover:text-accent-foreground transition-colors"
              onClick={() => {
                onExportCSV();
                setExportOpen(false);
              }}
            >
              CSV
            </button>
            <button
              type="button"
              className="w-full text-left px-3 py-1.5 text-sm hover:bg-accent hover:text-accent-foreground transition-colors"
              onClick={() => {
                onExportJSON();
                setExportOpen(false);
              }}
            >
              JSON
            </button>
          </div>
        )}
      </div>

      {/* Fit to screen */}
      <ToolbarButton title="Fit to screen" onClick={onFitToScreen}>
        <Maximize className="h-4 w-4" />
      </ToolbarButton>

      {/* Zoom */}
      <ToolbarButton title="Zoom in" onClick={onZoomIn}>
        <ZoomIn className="h-4 w-4" />
      </ToolbarButton>
      <ToolbarButton title="Zoom out" onClick={onZoomOut}>
        <ZoomOut className="h-4 w-4" />
      </ToolbarButton>
    </div>
  );
}

export default GraphToolbar;
