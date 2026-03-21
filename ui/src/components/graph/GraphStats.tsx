import { useMemo, useState } from "react";
import { BarChart3, ChevronDown } from "lucide-react";
import { useGraphStore } from "@/stores/graphStore";
import { getCustomColorForLabel } from "@/lib/colors";

// ---------------------------------------------------------------------------
// Types
// ---------------------------------------------------------------------------

interface NodeDegree {
  name: string;
  degree: number;
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

const GLASS_CLASS = "backdrop-blur-xl border bg-card/80 dark:bg-card/80 border-border shadow-lg";

// ---------------------------------------------------------------------------
// Component
// ---------------------------------------------------------------------------

export function GraphStats() {
  const [expanded, setExpanded] = useState(false);
  const nodes = useGraphStore((s) => s.nodes);
  const edges = useGraphStore((s) => s.edges);

  const stats = useMemo(() => {
    // Label counts
    const labelCounts = new Map<string, number>();
    for (const node of nodes) {
      for (const label of node.labels) {
        labelCounts.set(label, (labelCounts.get(label) ?? 0) + 1);
      }
    }

    // Edge type counts
    const typeCounts = new Map<string, number>();
    for (const edge of edges) {
      typeCounts.set(edge.type, (typeCounts.get(edge.type) ?? 0) + 1);
    }

    // Degree map
    const degreeMap = new Map<string, number>();
    for (const edge of edges) {
      degreeMap.set(edge.source, (degreeMap.get(edge.source) ?? 0) + 1);
      degreeMap.set(edge.target, (degreeMap.get(edge.target) ?? 0) + 1);
    }

    // Average degree
    const avgDegree =
      nodes.length > 0 ? (edges.length * 2) / nodes.length : 0;

    // Isolated nodes (degree 0)
    const isolatedCount = nodes.filter((n) => !degreeMap.has(n.id)).length;

    // Most connected node
    let mostConnected: NodeDegree | null = null;
    for (const [nodeId, degree] of degreeMap) {
      if (!mostConnected || degree > mostConnected.degree) {
        const node = nodes.find((n) => n.id === nodeId);
        const name =
          (node?.properties?.name as string) ??
          (node?.properties?.title as string) ??
          node?.labels[0] ??
          nodeId;
        mostConnected = { name, degree };
      }
    }

    const labelBreakdown = [...labelCounts.entries()]
      .sort((a, b) => b[1] - a[1])
      .slice(0, 12);
    const typeBreakdown = [...typeCounts.entries()]
      .sort((a, b) => b[1] - a[1])
      .slice(0, 12);

    return {
      nodeCount: nodes.length,
      edgeCount: edges.length,
      labelBreakdown,
      typeBreakdown,
      avgDegree,
      isolatedCount,
      mostConnected,
    };
  }, [nodes, edges]);

  if (nodes.length === 0 && edges.length === 0) return null;

  return (
    <div className="absolute bottom-3 left-3 z-10">
      {/* Toggle button */}
      <button
        type="button"
        onClick={() => setExpanded((prev) => !prev)}
        className={`flex items-center gap-1.5 rounded-xl px-3 py-1.5 text-xs font-medium text-foreground/80 hover:text-foreground transition-colors ${GLASS_CLASS}`}
        title="Toggle graph statistics"
      >
        <BarChart3 className="h-3.5 w-3.5" />
        Stats
        <ChevronDown
          className={`h-3 w-3 transition-transform ${expanded ? "rotate-180" : ""}`}
        />
      </button>

      {/* Expanded panel */}
      <div
        className="overflow-hidden transition-all duration-200 ease-in-out"
        style={{
          maxHeight: expanded ? "400px" : "0px",
          opacity: expanded ? 1 : 0,
        }}
      >
        <div
          className={`mt-1.5 rounded-xl max-h-[360px] overflow-y-auto ${GLASS_CLASS}`}
        >
          {/* Header */}
          <div className="flex items-center justify-between px-3 pt-2.5 pb-1.5">
            <span className="text-xs font-semibold text-foreground/90">
              Graph Statistics
            </span>
            <div className="flex items-center gap-1.5">
              <span className="rounded-full bg-primary/15 px-2 py-0.5 text-[10px] font-medium text-primary tabular-nums">
                {stats.nodeCount} nodes
              </span>
              <span className="rounded-full bg-muted px-2 py-0.5 text-[10px] font-medium text-muted-foreground tabular-nums">
                {stats.edgeCount} edges
              </span>
            </div>
          </div>

          {/* Two-column breakdown */}
          <div className="grid grid-cols-2 gap-3 px-3 pb-2">
            {/* Node labels */}
            <div>
              <div className="text-[10px] font-semibold text-muted-foreground uppercase tracking-wider mb-1">
                Node Labels
              </div>
              <div className="space-y-0.5">
                {stats.labelBreakdown.map(([label, count]) => {
                  const color = getCustomColorForLabel(label);
                  return (
                    <div
                      key={label}
                      className="flex items-center gap-1.5 text-[10px]"
                    >
                      <span
                        className="h-2 w-2 rounded-full flex-shrink-0"
                        style={{ backgroundColor: color }}
                      />
                      <span className="flex-1 text-foreground/80 truncate">
                        {label}
                      </span>
                      <span className="text-muted-foreground tabular-nums">
                        ({count})
                      </span>
                    </div>
                  );
                })}
                {stats.labelBreakdown.length === 0 && (
                  <span className="text-[10px] text-muted-foreground">
                    None
                  </span>
                )}
              </div>
            </div>

            {/* Edge types */}
            <div>
              <div className="text-[10px] font-semibold text-muted-foreground uppercase tracking-wider mb-1">
                Edge Types
              </div>
              <div className="space-y-0.5">
                {stats.typeBreakdown.map(([type, count]) => (
                  <div
                    key={type}
                    className="flex items-center gap-1.5 text-[10px]"
                  >
                    <span className="flex-1 text-foreground/80 truncate">
                      {type}
                    </span>
                    <span className="text-muted-foreground tabular-nums">
                      ({count})
                    </span>
                  </div>
                ))}
                {stats.typeBreakdown.length === 0 && (
                  <span className="text-[10px] text-muted-foreground">
                    None
                  </span>
                )}
              </div>
            </div>
          </div>

          {/* Bottom metrics */}
          <div className="flex items-center justify-between gap-3 border-t border-foreground/5 px-3 py-2 text-[10px]">
            <div className="flex items-center gap-1">
              <span className="text-muted-foreground">Avg Degree:</span>
              <span className="font-medium text-foreground/90 tabular-nums">
                {stats.avgDegree.toFixed(2)}
              </span>
            </div>
            {stats.mostConnected && (
              <div className="flex items-center gap-1 min-w-0">
                <span className="text-muted-foreground flex-shrink-0">
                  Top:
                </span>
                <span className="font-medium text-foreground/90 truncate max-w-[6rem]">
                  {stats.mostConnected.name}
                </span>
                <span className="text-muted-foreground tabular-nums flex-shrink-0">
                  ({stats.mostConnected.degree})
                </span>
              </div>
            )}
            <div className="flex items-center gap-1">
              <span className="text-muted-foreground">Isolated:</span>
              <span className="font-medium text-foreground/90 tabular-nums">
                {stats.isolatedCount}
              </span>
            </div>
          </div>
        </div>
      </div>
    </div>
  );
}
