import { useMemo } from "react";
import { useGraphStore } from "@/stores/graphStore";
import { useGraphSettingsStore } from "@/stores/graphSettingsStore";
import {
  getCustomColorForLabel,
  getCustomEdgeColor,
  getNodeCaption,
} from "@/lib/colors";
import type { CSSProperties } from "react";

// ---- Shared styles (graph-viz inspired) ----

const sectionTitle: CSSProperties = {
  fontSize: 8.5,
  color: "var(--th-text-dim, #64748b)",
  fontFamily: '"Inter", sans-serif',
  textTransform: "uppercase",
  letterSpacing: "0.1em",
  marginBottom: 8,
};

const propRow: CSSProperties = {
  display: "flex",
  justifyContent: "space-between",
  alignItems: "center",
  padding: "5px 0",
  borderBottom: "1px solid var(--th-border, #e2e8f0)",
  gap: 8,
  minHeight: 26,
};

const propKey: CSSProperties = {
  fontSize: 10,
  color: "var(--th-text-muted, #64748b)",
  fontFamily: '"Inter", sans-serif',
  flexShrink: 0,
};

const propVal: CSSProperties = {
  fontSize: 10,
  color: "var(--th-text-2, #1e293b)",
  fontFamily: '"Inter", sans-serif',
  textAlign: "right",
  overflow: "hidden",
  textOverflow: "ellipsis",
  whiteSpace: "nowrap",
  minWidth: 0,
  cursor: "default",
};

// ---- Type Badge ----

function TypeBadge({ type, color }: { type: string; color: string }) {
  return (
    <span
      style={{
        display: "inline-block",
        fontSize: 9,
        fontFamily: '"Inter", sans-serif',
        color,
        border: `1px solid ${color}44`,
        background: `${color}12`,
        borderRadius: 4,
        padding: "2px 8px",
        letterSpacing: "0.05em",
      }}
    >
      {type}
    </span>
  );
}

// ---- Component ----

export function PropertyInspector() {
  const selectedNode = useGraphStore((s) => s.selectedNode);
  const selectedEdge = useGraphStore((s) => s.selectedEdge);
  const nodes = useGraphStore((s) => s.nodes);
  const edges = useGraphStore((s) => s.edges);
  const { captionProperty, setCaptionProperty } = useGraphSettingsStore();

  // Connected edges for selected node
  const connectedEdges = useMemo(() => {
    if (!selectedNode) return [];
    return edges.filter(
      (e) => e.source === selectedNode.id || e.target === selectedNode.id,
    );
  }, [selectedNode, edges]);

  const degree = connectedEdges.length;

  if (!selectedNode && !selectedEdge) return null;

  // ---- Edge selected ----

  if (selectedEdge) {
    const edgeColor = getCustomEdgeColor(selectedEdge.type);
    const sourceNode = nodes.find((n) => n.id === selectedEdge.source);
    const targetNode = nodes.find((n) => n.id === selectedEdge.target);
    const sourceName = sourceNode
      ? getNodeCaption(sourceNode.labels?.[0] ?? "", sourceNode.properties)
      : String(selectedEdge.source);
    const targetName = targetNode
      ? getNodeCaption(targetNode.labels?.[0] ?? "", targetNode.properties)
      : String(selectedEdge.target);

    return (
      <div style={{ padding: "16px 20px" }}>
        <div style={{ marginBottom: 14 }}>
          <TypeBadge type={selectedEdge.type} color={edgeColor} />
        </div>
        <div style={{ ...sectionTitle, marginBottom: 10 }}>Connection</div>
        <div
          style={{
            background: "var(--th-bg-input, #e2e8f0)",
            borderRadius: 6,
            padding: "12px 14px",
            border: "1px solid var(--th-border, #e2e8f0)",
            display: "flex",
            flexDirection: "column",
            gap: 6,
          }}
        >
          <div style={{ fontSize: 11, color: "var(--th-text, #0f172a)", fontFamily: '"Inter", sans-serif' }}>
            {sourceName}
          </div>
          <div style={{ fontSize: 9, color: edgeColor, fontFamily: '"Inter", sans-serif' }}>
            &darr; {selectedEdge.type}
          </div>
          <div style={{ fontSize: 11, color: "var(--th-text, #0f172a)", fontFamily: '"Inter", sans-serif' }}>
            {targetName}
          </div>
        </div>
        {/* Edge properties */}
        {selectedEdge.properties && Object.keys(selectedEdge.properties).length > 0 && (
          <div style={{ marginTop: 14 }}>
            <div style={sectionTitle}>Properties</div>
            <div style={{ display: "flex", flexDirection: "column" }}>
              {Object.entries(selectedEdge.properties).map(([key, val]) => {
                const str = String(val);
                return (
                  <div key={key} style={propRow} title={str.length > 25 ? str : undefined}>
                    <span style={propKey}>{key}</span>
                    <span style={propVal}>{str}</span>
                  </div>
                );
              })}
            </div>
          </div>
        )}
      </div>
    );
  }

  // ---- Node selected ----

  const node = selectedNode!;
  const nodeLabel = node.labels[0] ?? "Node";
  const typeMeta = {
    color: getCustomColorForLabel(nodeLabel),
    label: nodeLabel,
  };
  const currentCaption = captionProperty[nodeLabel] ?? "";
  const displayLabel = currentCaption
    ? String(node.properties[currentCaption] ?? getNodeCaption(nodeLabel, node.properties))
    : getNodeCaption(nodeLabel, node.properties);
  const dataEntries = Object.entries(node.properties);

  return (
    <div style={{ padding: "16px 20px", display: "flex", flexDirection: "column", gap: 14 }}>
      {/* Header */}
      <div style={{ display: "flex", alignItems: "center", gap: 10 }}>
        <span
          style={{
            width: 10,
            height: 10,
            borderRadius: "50%",
            background: typeMeta.color,
            border: `2px solid ${typeMeta.color}55`,
            flexShrink: 0,
          }}
        />
        <div>
          <div
            style={{
              fontSize: 13,
              color: "var(--th-text, #0f172a)",
              fontFamily: '"Inter", sans-serif',
              fontWeight: 600,
              lineHeight: 1.3,
            }}
          >
            {displayLabel}
          </div>
          <TypeBadge type={typeMeta.label} color={typeMeta.color} />
        </div>
      </div>

      {/* Caption property (applies to all nodes of same type) */}
      <div>
        <div style={sectionTitle}>
          Caption property
          <span
            style={{
              color: "var(--th-text-faint, #94a3b8)",
              fontStyle: "italic",
              textTransform: "none",
              letterSpacing: 0,
            }}
          >
            {" "}&mdash; all {typeMeta.label}s
          </span>
        </div>
        <select
          value={currentCaption}
          onChange={(e) => {
            const val = e.target.value;
            if (val) {
              setCaptionProperty(nodeLabel, val);
            } else {
              // Reset to auto-detect
              setCaptionProperty(nodeLabel, "");
            }
          }}
          style={{
            width: "100%",
            padding: "6px 10px",
            fontSize: 11,
            fontFamily: '"Inter", sans-serif',
            color: "var(--th-text, #0f172a)",
            background: "var(--th-bg-input, #e2e8f0)",
            border: "1px solid var(--th-border-subtle, #cbd5e1)",
            borderRadius: 5,
            outline: "none",
            cursor: "pointer",
          }}
        >
          <option value="">(auto-detect)</option>
          {Object.keys(node.properties).map((k) => (
            <option key={k} value={k}>
              {k}
            </option>
          ))}
        </select>
      </div>

      {/* Connected edges */}
      {connectedEdges.length > 0 && (
        <div>
          <div style={sectionTitle}>Connected ({connectedEdges.length})</div>
          <div style={{ display: "flex", flexDirection: "column", gap: 3 }}>
            {connectedEdges.slice(0, 20).map((e) => {
              const edgeColor = getCustomEdgeColor(e.type);
              const srcNode = nodes.find((n) => n.id === e.source);
              const tgtNode = nodes.find((n) => n.id === e.target);
              const srcName = srcNode
                ? getNodeCaption(srcNode.labels[0] ?? "", srcNode.properties)
                : e.source;
              const tgtName = tgtNode
                ? getNodeCaption(tgtNode.labels[0] ?? "", tgtNode.properties)
                : e.target;

              return (
                <div
                  key={e.id}
                  style={{
                    display: "flex",
                    alignItems: "center",
                    gap: 4,
                    fontSize: 9.5,
                    fontFamily: '"Inter", sans-serif',
                    color: "var(--th-text-muted, #64748b)",
                    padding: "3px 0",
                  }}
                >
                  <span style={{ color: "var(--th-text, #0f172a)" }}>{srcName}</span>
                  <span style={{ color: edgeColor, fontSize: 8 }}>&rarr;</span>
                  <TypeBadge type={e.type} color={edgeColor} />
                  <span style={{ color: edgeColor, fontSize: 8 }}>&rarr;</span>
                  <span style={{ color: "var(--th-text, #0f172a)" }}>{tgtName}</span>
                </div>
              );
            })}
            {connectedEdges.length > 20 && (
              <span style={{ fontSize: 9, color: "var(--th-text-faint, #94a3b8)" }}>
                +{connectedEdges.length - 20} more
              </span>
            )}
          </div>
        </div>
      )}

      {/* Properties */}
      {dataEntries.length > 0 && (
        <div style={{ minHeight: 0 }}>
          <div style={sectionTitle}>Properties</div>
          <div style={{ display: "flex", flexDirection: "column" }}>
            {dataEntries.map(([key, val]) => {
              const str = val === null || val === undefined ? "null" : String(val);
              const isLong = str.length > 25;
              return (
                <div key={key} style={propRow} title={isLong ? str : undefined}>
                  <span style={propKey}>{key}</span>
                  <span style={propVal}>{str}</span>
                </div>
              );
            })}
          </div>
        </div>
      )}

      {/* Stats */}
      <div>
        <div style={sectionTitle}>Stats</div>
        <div style={propRow}>
          <span style={propKey}>degree</span>
          <span style={propVal}>{degree}</span>
        </div>
        <div style={propRow}>
          <span style={propKey}>id</span>
          <span style={{ ...propVal, fontFamily: "monospace" }}>{node.id}</span>
        </div>
      </div>
    </div>
  );
}
