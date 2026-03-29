import { useState, useMemo, type CSSProperties } from "react";
import { useGraphStore } from "@/stores/graphStore";
import { useGraphSettingsStore } from "@/stores/graphSettingsStore";
import { getCustomColorForLabel, getCustomEdgeColor } from "@/lib/colors";
import { NODE_ICON_CATALOG } from "@/lib/icons";

// ---- Shared styles (graph-viz glassmorphism) ----

const panel: CSSProperties = {
  position: "absolute",
  bottom: 16,
  left: 16,
  width: 280,
  background: "var(--th-overlay)",
  backdropFilter: "blur(12px)",
  WebkitBackdropFilter: "blur(12px)",
  border: "1px solid var(--th-border-subtle)",
  borderRadius: 10,
  boxShadow: "0 8px 32px rgba(0,0,0,0.18)",
  zIndex: 20,
  fontFamily: '"Inter", sans-serif',
  overflow: "hidden",
  transition: "background-color 0.2s, border-color 0.2s",
};

const sectionLabel: CSSProperties = {
  fontSize: 8,
  color: "var(--th-text-dim)",
  textTransform: "uppercase",
  letterSpacing: "0.12em",
  padding: "10px 14px 4px",
  fontFamily: '"Inter", sans-serif',
};

const row: CSSProperties = {
  display: "flex",
  alignItems: "center",
  gap: 8,
  padding: "6px 14px",
};

const colorInput: CSSProperties = { width: 22, height: 22, flexShrink: 0 };

const labelText: CSSProperties = {
  fontSize: 10.5,
  color: "var(--th-text-2)",
  fontFamily: '"Inter", sans-serif',
  flex: 1,
};

const microBtn = (active: boolean, expanded: boolean): CSSProperties => ({
  padding: "3px 7px",
  fontSize: 8.5,
  fontFamily: '"Inter", sans-serif',
  cursor: "pointer",
  border: "none",
  background: active
    ? "var(--th-bg-elevated)"
    : expanded
      ? "var(--th-bg-input)"
      : "transparent",
  color: active
    ? "var(--th-text)"
    : expanded
      ? "var(--th-text-2)"
      : "var(--th-text-dim)",
  transition: "all 0.12s",
});

// ---- Node type row ----

function NodeLabelRow({
  label,
  count,
  focused,
  onToggleFocus,
}: {
  label: string;
  count: number;
  focused: boolean;
  onToggleFocus: () => void;
}) {
  const {
    labelColors, labelIcons, imageProperty,
    setLabelColor, setLabelIcon, resetLabelIcon,
    setImageProperty, resetImageProperty,
  } = useGraphSettingsStore();

  const color = labelColors[label] || getCustomColorForLabel(label);
  const iconName = labelIcons[label];
  const currentImageProp = imageProperty[label];

  const [expandedPicker, setExpandedPicker] = useState<"icon" | "image" | null>(null);

  // Discover string properties
  const nodes = useGraphStore((s) => s.nodes);
  const stringProps = useMemo(() => {
    const props = new Set<string>();
    nodes
      .filter((n) => n.labels.includes(label))
      .forEach((n) => {
        Object.entries(n.properties).forEach(([k, v]) => {
          if (typeof v === "string" && !["id"].includes(k)) props.add(k);
        });
      });
    return Array.from(props).sort();
  }, [nodes, label]);

  const displayMode = iconName ? "icon" : currentImageProp ? "image" : "color";

  const handleModeClick = (mode: "color" | "icon" | "image") => {
    if (mode === "color") {
      resetLabelIcon(label);
      resetImageProperty(label);
      setExpandedPicker(null);
    } else if (mode === "icon") {
      setExpandedPicker((p) => (p === "icon" ? null : "icon"));
    } else if (mode === "image") {
      setExpandedPicker((p) => (p === "image" ? null : "image"));
    }
  };

  return (
    <div>
      <div style={row}>
        <input
          type="color"
          value={color}
          onChange={(e) => setLabelColor(label, e.target.value)}
          style={colorInput}
          title={`${label} color`}
        />
        <span
          style={{
            width: 10,
            height: 10,
            borderRadius: "50%",
            background: color,
            border: `2px solid ${color}55`,
            flexShrink: 0,
          }}
        />
        {/* Focus toggle */}
        <button
          onClick={onToggleFocus}
          title={focused ? "Remove focus" : "Focus on this type"}
          style={{
            width: 20,
            height: 20,
            borderRadius: 4,
            border: focused
              ? `1px solid ${color}66`
              : "1px solid var(--th-border-subtle)",
            background: focused ? `${color}22` : "transparent",
            color: focused ? color : "var(--th-text-dim)",
            cursor: "pointer",
            display: "flex",
            alignItems: "center",
            justifyContent: "center",
            flexShrink: 0,
            padding: 0,
            fontSize: 11,
            lineHeight: 1,
            transition: "all 0.15s",
          }}
        >
          {focused ? "\u25C9" : "\u25CB"}
        </button>
        <span style={labelText}>{label}</span>
        <span style={{ fontSize: 9, color: "var(--th-text-faint)", fontFamily: "monospace" }}>
          {count}
        </span>

        {/* Mode buttons */}
        <div
          style={{
            display: "flex",
            borderRadius: 4,
            overflow: "hidden",
            border: "1px solid var(--th-border-subtle)",
          }}
        >
          <button
            onClick={() => handleModeClick("color")}
            style={{
              ...microBtn(displayMode === "color", false),
              borderRadius: "3px 0 0 3px",
            }}
          >
            Color
          </button>
          <button
            onClick={() => handleModeClick("icon")}
            style={{
              ...microBtn(displayMode === "icon", expandedPicker === "icon"),
              borderLeft: "1px solid var(--th-border-subtle)",
            }}
          >
            Icon
          </button>
          <button
            onClick={() => handleModeClick("image")}
            style={{
              ...microBtn(displayMode === "image", expandedPicker === "image"),
              borderLeft: "1px solid var(--th-border-subtle)",
              borderRadius: "0 3px 3px 0",
            }}
          >
            Image
          </button>
        </div>
      </div>

      {/* Icon picker - uses UI's SVG icon catalog with categories */}
      {expandedPicker === "icon" && (
        <div style={{ padding: "4px 14px 8px" }}>
          <div
            style={{
              background: "var(--th-bg-input)",
              border: "1px solid var(--th-border-subtle)",
              borderRadius: 6,
              padding: 6,
              maxHeight: 200,
              overflowY: "auto",
            }}
          >
            {(() => {
              const categories = [...new Set(NODE_ICON_CATALOG.filter((ic) => ic.path).map((ic) => ic.category))];
              return categories.map((cat) => (
                <div key={cat} style={{ marginBottom: 6 }}>
                  <div
                    style={{
                      fontSize: 7.5,
                      color: "var(--th-text-dim)",
                      fontFamily: '"Inter", sans-serif',
                      textTransform: "uppercase",
                      letterSpacing: "0.1em",
                      marginBottom: 3,
                    }}
                  >
                    {cat}
                  </div>
                  <div style={{ display: "flex", flexWrap: "wrap", gap: 2 }}>
                    {NODE_ICON_CATALOG.filter((ic) => ic.category === cat && ic.path).map((ic) => (
                      <button
                        key={ic.name}
                        onClick={() => {
                          setLabelIcon(label, ic.name);
                          setExpandedPicker(null);
                        }}
                        title={ic.name}
                        style={{
                          width: 24,
                          height: 24,
                          display: "flex",
                          alignItems: "center",
                          justifyContent: "center",
                          borderRadius: 3,
                          cursor: "pointer",
                          background:
                            iconName === ic.name
                              ? "var(--th-bg-elevated)"
                              : "transparent",
                          border:
                            iconName === ic.name
                              ? "1px solid var(--th-border-subtle)"
                              : "1px solid transparent",
                        }}
                      >
                        <svg viewBox="0 0 24 24" style={{ width: 14, height: 14, color }}>
                          <path d={ic.path} fill="currentColor" />
                        </svg>
                      </button>
                    ))}
                  </div>
                </div>
              ));
            })()}
            <div style={{ borderTop: "1px solid var(--th-border-subtle)", marginTop: 4, paddingTop: 4 }}>
              <button
                onClick={() => {
                  resetLabelIcon(label);
                  setExpandedPicker(null);
                }}
                style={{
                  width: "100%",
                  textAlign: "center",
                  fontSize: 9,
                  color: "var(--th-text-dim)",
                  background: "none",
                  border: "none",
                  cursor: "pointer",
                  padding: "2px 0",
                }}
              >
                Reset to default circle
              </button>
            </div>
          </div>
        </div>
      )}

      {/* Image property dropdown */}
      {expandedPicker === "image" && (
        <div style={{ padding: "4px 14px 8px" }}>
          <div
            style={{
              fontSize: 8.5,
              color: "var(--th-text-muted)",
              fontFamily: '"Inter", sans-serif',
              marginBottom: 4,
            }}
          >
            Select a property containing the image URL:
          </div>
          <div style={{ display: "flex", flexDirection: "column", gap: 2 }}>
            {stringProps.map((p) => (
              <button
                key={p}
                onClick={() => {
                  setImageProperty(label, p);
                  setExpandedPicker(null);
                }}
                style={{
                  display: "flex",
                  alignItems: "center",
                  gap: 6,
                  padding: "5px 8px",
                  fontSize: 10,
                  fontFamily: '"Inter", sans-serif',
                  color:
                    currentImageProp === p
                      ? "var(--th-text)"
                      : "var(--th-text-muted)",
                  background:
                    currentImageProp === p
                      ? "var(--th-bg-elevated)"
                      : "var(--th-bg-input)",
                  border: "1px solid var(--th-border-subtle)",
                  borderRadius: 4,
                  cursor: "pointer",
                  textAlign: "left",
                }}
              >
                {p}
              </button>
            ))}
            {stringProps.length === 0 && (
              <span style={{ fontSize: 9, color: "#ef4444" }}>
                No string properties found
              </span>
            )}
          </div>
        </div>
      )}
    </div>
  );
}

// ---- Props ----

interface LegendPanelProps {
  focusedLabels: string[];
  onToggleFocus: (label: string) => void;
  onClose: () => void;
}

// ---- Component ----

export function LegendPanel({
  focusedLabels,
  onToggleFocus,
  onClose,
}: LegendPanelProps) {
  const nodes = useGraphStore((s) => s.nodes);
  const edges = useGraphStore((s) => s.edges);
  const { edgeColors, edgeDashed, setEdgeColor, toggleEdgeDashed } =
    useGraphSettingsStore();

  // Compute label counts
  const labelCounts = useMemo(() => {
    const map = new Map<string, number>();
    for (const node of nodes) {
      const label = node.labels[0] ?? "Node";
      map.set(label, (map.get(label) ?? 0) + 1);
    }
    return Array.from(map.entries()).sort((a, b) => b[1] - a[1]);
  }, [nodes]);

  // Compute edge type counts
  const edgeTypeCounts = useMemo(() => {
    const map = new Map<string, number>();
    for (const edge of edges) {
      map.set(edge.type, (map.get(edge.type) ?? 0) + 1);
    }
    return Array.from(map.entries()).sort((a, b) => b[1] - a[1]);
  }, [edges]);

  if (nodes.length === 0) return null;

  return (
    <div style={panel}>
      {/* Header */}
      <div
        style={{
          display: "flex",
          alignItems: "center",
          justifyContent: "space-between",
          padding: "10px 14px",
          borderBottom: "1px solid var(--th-border-subtle)",
        }}
      >
        <span
          style={{
            fontSize: 9.5,
            color: "var(--th-text-muted)",
            textTransform: "uppercase",
            letterSpacing: "0.1em",
            fontFamily: '"Inter", sans-serif',
          }}
        >
          Legend &amp; Customization
        </span>
        <button
          onClick={onClose}
          title="Close"
          style={{
            background: "none",
            border: "none",
            color: "var(--th-text-dim)",
            cursor: "pointer",
            fontSize: 14,
            lineHeight: 1,
            padding: "2px 4px",
            borderRadius: 4,
          }}
        >
          &times;
        </button>
      </div>

      <div style={{ maxHeight: 420, overflowY: "auto", paddingBottom: 8 }}>
        {/* Node labels */}
        <div style={sectionLabel}>Nodes</div>
        {labelCounts.map(([label, count]) => (
          <NodeLabelRow
            key={label}
            label={label}
            count={count}
            focused={focusedLabels.includes(label)}
            onToggleFocus={() => onToggleFocus(label)}
          />
        ))}

        {/* Edge types */}
        {edgeTypeCounts.length > 0 && (
          <>
            <div style={{ ...sectionLabel, marginTop: 4 }}>Relationships</div>
            {edgeTypeCounts.map(([edgeType, count]) => {
              const color = edgeColors[edgeType] || getCustomEdgeColor(edgeType);
              const dashed = edgeDashed[edgeType] ?? false;
              return (
                <div key={edgeType} style={row}>
                  <input
                    type="color"
                    value={color}
                    onChange={(e) => setEdgeColor(edgeType, e.target.value)}
                    style={colorInput}
                    title={`${edgeType} color`}
                  />
                  <svg width="24" height="10" style={{ flexShrink: 0 }}>
                    <line
                      x1="0"
                      y1="5"
                      x2="24"
                      y2="5"
                      stroke={color}
                      strokeWidth={1.8}
                      strokeDasharray={dashed ? "5,3" : "none"}
                    />
                  </svg>
                  <span style={labelText}>{edgeType}</span>
                  <span style={{ fontSize: 9, color: "var(--th-text-faint)", fontFamily: "monospace" }}>
                    {count}
                  </span>
                  <button
                    onClick={() => toggleEdgeDashed(edgeType)}
                    title={dashed ? "Switch to solid" : "Switch to dashed"}
                    style={{
                      width: 28,
                      height: 18,
                      borderRadius: 4,
                      border: `1px solid ${dashed ? "var(--th-text-dim)" : "var(--th-border-subtle)"}`,
                      background: dashed ? "var(--th-bg-elevated)" : "transparent",
                      cursor: "pointer",
                      display: "flex",
                      alignItems: "center",
                      justifyContent: "center",
                      flexShrink: 0,
                      padding: 0,
                    }}
                  >
                    <svg width="16" height="4">
                      <line
                        x1="0"
                        y1="2"
                        x2="16"
                        y2="2"
                        stroke={dashed ? "var(--th-text-faint)" : "var(--th-text-dim)"}
                        strokeWidth="1.5"
                        strokeDasharray="3,2"
                      />
                    </svg>
                  </button>
                </div>
              );
            })}
          </>
        )}
      </div>
    </div>
  );
}

export default LegendPanel;
