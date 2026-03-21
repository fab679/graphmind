import { useState } from "react";
import { ChevronDown, Compass, Search, X } from "lucide-react";
import { ForceGraph } from "@/components/graph/ForceGraph";
import { GraphStats } from "@/components/graph/GraphStats";
import { PropertyInspector } from "@/components/inspector/PropertyInspector";
import { IconPicker } from "@/components/ui/icon-picker";
import { useGraphStore } from "@/stores/graphStore";
import { useGraphSettingsStore } from "@/stores/graphSettingsStore";
import { getCustomColorForLabel, getCustomEdgeColor } from "@/lib/colors";
import { cn } from "@/lib/utils";

function FloatingLegend() {
  const nodes = useGraphStore((s) => s.nodes);
  const [open, setOpen] = useState(true);
  const { labelColors, edgeColors, labelIcons, imageProperty, setLabelColor, setEdgeColor, setLabelIcon, resetLabelIcon, setImageProperty, resetImageProperty } = useGraphSettingsStore();

  // Get unique labels from nodes currently on canvas
  const labels = [...new Set(nodes.flatMap((n) => n.labels || []))];
  const edgeTypes = [...new Set(useGraphStore.getState().edges.map((e) => e.type))];

  if (labels.length === 0) return null;

  return (
    <div className="absolute left-4 bottom-14 z-10 w-52 rounded-lg border bg-card/95 backdrop-blur-sm shadow-lg">
      <button
        onClick={() => setOpen(!open)}
        className="flex w-full items-center justify-between px-3 py-2 text-xs font-medium"
      >
        Legend
        <ChevronDown className={cn("h-3 w-3 transition-transform", !open && "-rotate-90")} />
      </button>
      {open && (
        <div className="border-t px-3 py-2 space-y-1 max-h-[300px] overflow-auto">
          {labels.map((label) => {
            const nodeProps = nodes.find((n) => n.labels?.includes(label))?.properties;
            const propNames = nodeProps ? Object.keys(nodeProps) : [];
            return (
              <div key={label} className="flex items-center gap-1.5">
                <input
                  type="color"
                  value={labelColors[label] || getCustomColorForLabel(label)}
                  onChange={(e) => setLabelColor(label, e.target.value)}
                  className="h-4 w-4 rounded cursor-pointer border-0"
                />
                <IconPicker
                  currentIcon={labelIcons[label] || null}
                  currentImageProp={imageProperty?.[label] || null}
                  label={label}
                  properties={propNames}
                  onSelectIcon={(name) => setLabelIcon(label, name)}
                  onResetIcon={() => resetLabelIcon(label)}
                  onSelectImageProp={(prop) => setImageProperty(label, prop)}
                  onResetImageProp={() => resetImageProperty(label)}
                />
                <span className="text-[11px] flex-1 truncate">{label}</span>
                <span className="text-[10px] text-muted-foreground">
                  {nodes.filter((n) => n.labels?.includes(label)).length}
                </span>
              </div>
            );
          })}
          {edgeTypes.length > 0 && (
            <>
              <div className="text-[10px] text-muted-foreground mt-2 mb-1">Edges</div>
              {edgeTypes.map((type) => (
                <div key={type} className="flex items-center gap-2">
                  <input
                    type="color"
                    value={edgeColors[type] || getCustomEdgeColor(type)}
                    onChange={(e) => setEdgeColor(type, e.target.value)}
                    className="h-4 w-4 rounded cursor-pointer border-0"
                  />
                  <span className="text-[11px] flex-1 truncate">{type}</span>
                </div>
              ))}
            </>
          )}
        </div>
      )}
    </div>
  );
}

export function ExploreTab() {
  const nodes = useGraphStore((s) => s.nodes);
  const selectedNode = useGraphStore((s) => s.selectedNode);
  const selectedEdge = useGraphStore((s) => s.selectedEdge);
  const [searchQuery, setSearchQuery] = useState("");

  return (
    <div className="relative h-full">
      {nodes.length === 0 ? (
        <div className="flex h-full items-center justify-center text-muted-foreground">
          <div className="text-center">
            <Compass className="mx-auto mb-3 h-10 w-10 opacity-20" />
            <p className="text-sm">No graph data to explore</p>
            <p className="text-xs mt-1 opacity-60">
              Run a query in the Query tab first
            </p>
          </div>
        </div>
      ) : (
        <>
          <ForceGraph searchQuery={searchQuery} />

          {/* Floating search bar */}
          <div className="absolute left-4 top-4 z-10">
            <div className="flex items-center gap-2 rounded-lg border bg-card/90 backdrop-blur-sm px-3 py-1.5 shadow-sm">
              <Search className="h-3.5 w-3.5 text-muted-foreground" />
              <input
                type="text"
                value={searchQuery}
                onChange={(e) => setSearchQuery(e.target.value)}
                placeholder="Search nodes..."
                className="w-48 bg-transparent text-xs outline-none placeholder:text-muted-foreground"
              />
              {searchQuery && (
                <button
                  onClick={() => setSearchQuery("")}
                  className="text-muted-foreground hover:text-foreground"
                >
                  <X className="h-3 w-3" />
                </button>
              )}
            </div>
          </div>

          {/* Floating legend (bottom-left, above stats) */}
          <FloatingLegend />

          {/* Graph stats (bottom-left) */}
          <GraphStats />

          {/* Property inspector (floating right, on node OR edge selection) */}
          {(selectedNode || selectedEdge) && (
            <div className="absolute right-2 top-14 z-[5] w-72 max-h-[calc(100%-72px)] overflow-auto rounded-lg border bg-card shadow-lg">
              <button
                onClick={() => {
                  useGraphStore.getState().selectNode(null);
                  useGraphStore.getState().selectEdge(null);
                }}
                className="absolute right-2 top-2 text-muted-foreground hover:text-foreground z-10"
              >
                <X className="h-3.5 w-3.5" />
              </button>
              <PropertyInspector />
            </div>
          )}
        </>
      )}
    </div>
  );
}
