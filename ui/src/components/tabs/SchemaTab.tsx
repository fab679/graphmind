import { useState, useEffect, useCallback, useRef } from "react";
import {
  ArrowRight,
  Database,
  Key,
  RefreshCw,
  RotateCcw,
  Search,
  Shield,
  Tag,
} from "lucide-react";
import { useUiStore } from "@/stores/uiStore";
import { useGraphSettingsStore } from "@/stores/graphSettingsStore";
import { getSchema } from "@/api/client";
import { getColorForLabel } from "@/lib/colors";
import { IconPicker } from "@/components/ui/icon-picker";
import { cn } from "@/lib/utils";
import type { SchemaNodeType, SchemaEdgeType } from "@/types/api";

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

const DEFAULT_EDGE_COLOR = "#94a3b8";

function ColorPickerInline({
  currentColor,
  onSelect,
  onReset,
  onClose,
}: {
  currentColor: string;
  onSelect: (color: string) => void;
  onReset: () => void;
  onClose: () => void;
}) {
  const ref = useRef<HTMLDivElement>(null);

  useEffect(() => {
    function handleClickOutside(e: MouseEvent) {
      if (ref.current && !ref.current.contains(e.target as Node)) {
        onClose();
      }
    }
    document.addEventListener("mousedown", handleClickOutside);
    return () => document.removeEventListener("mousedown", handleClickOutside);
  }, [onClose]);

  return (
    <div
      ref={ref}
      className="absolute right-0 top-8 z-50 w-48 rounded-md border border-border bg-popover p-2 shadow-lg overflow-hidden"
      onClick={(e) => e.stopPropagation()}
    >
      <div className="grid grid-cols-6 gap-1">
        {PRESET_COLORS.map((color) => (
          <button
            key={color}
            className={cn(
              "h-5 w-5 rounded-sm border transition-transform hover:scale-110",
              currentColor === color
                ? "border-foreground ring-1 ring-foreground"
                : "border-transparent",
            )}
            style={{ backgroundColor: color }}
            onClick={() => onSelect(color)}
            title={color}
          />
        ))}
      </div>
      <div className="mt-2 flex items-center gap-1.5">
        <label className="text-[10px] text-muted-foreground">Custom</label>
        <input
          type="color"
          value={currentColor}
          onChange={(e) => onSelect(e.target.value)}
          className="h-6 w-8 cursor-pointer rounded border-0 bg-transparent p-0"
        />
        <button
          onClick={onReset}
          className="ml-auto flex items-center gap-0.5 rounded px-1.5 py-0.5 text-[10px] text-muted-foreground transition-colors hover:bg-accent hover:text-foreground"
          title="Reset to default"
        >
          <RotateCcw className="h-2.5 w-2.5" />
          Reset
        </button>
      </div>
    </div>
  );
}

function NodeDetailPanel({ nodeType }: { nodeType: SchemaNodeType }) {
  const [colorPickerOpen, setColorPickerOpen] = useState(false);
  const properties = Object.entries(nodeType.properties);

  const customColor = useGraphSettingsStore(
    (s) => s.labelColors[nodeType.label],
  );
  const setLabelColor = useGraphSettingsStore((s) => s.setLabelColor);
  const resetLabelColor = useGraphSettingsStore((s) => s.resetLabelColor);
  const labelIcon = useGraphSettingsStore(
    (s) => s.labelIcons[nodeType.label] ?? "",
  );
  const setLabelIcon = useGraphSettingsStore((s) => s.setLabelIcon);
  const resetLabelIcon = useGraphSettingsStore((s) => s.resetLabelIcon);
  const imageProp = useGraphSettingsStore(
    (s) => s.imageProperty?.[nodeType.label] ?? "",
  );
  const setImageProperty = useGraphSettingsStore((s) => s.setImageProperty);
  const resetImageProperty = useGraphSettingsStore((s) => s.resetImageProperty);
  const captionProp = useGraphSettingsStore(
    (s) => s.captionProperty[nodeType.label] ?? "",
  );
  const setCaptionProperty = useGraphSettingsStore(
    (s) => s.setCaptionProperty,
  );

  const effectiveColor = customColor ?? getColorForLabel(nodeType.label);
  const handleCloseColorPicker = useCallback(
    () => setColorPickerOpen(false),
    [],
  );

  return (
    <div className="space-y-6">
      <div className="flex items-center gap-3">
        <div
          className="h-8 w-8 rounded-full shrink-0"
          style={{ backgroundColor: effectiveColor }}
        />
        <div>
          <h3 className="text-sm font-semibold">{nodeType.label}</h3>
          <p className="text-xs text-muted-foreground">
            {nodeType.count.toLocaleString()} node
            {nodeType.count !== 1 ? "s" : ""}
          </p>
        </div>
      </div>

      <section>
        <h4 className="text-xs font-semibold text-muted-foreground uppercase tracking-wider mb-3">
          Appearance
        </h4>
        <div className="space-y-3">
          <div className="flex items-center justify-between">
            <span className="text-xs text-foreground">Color</span>
            <div className="relative">
              <button
                className="flex items-center gap-2 rounded border border-border px-2 py-1"
                onClick={() => setColorPickerOpen((v) => !v)}
              >
                <div
                  className="h-4 w-4 rounded-sm"
                  style={{ backgroundColor: effectiveColor }}
                />
                <span className="text-[10px] font-mono text-muted-foreground">
                  {effectiveColor}
                </span>
              </button>
              {colorPickerOpen && (
                <ColorPickerInline
                  currentColor={effectiveColor}
                  onSelect={(color) => setLabelColor(nodeType.label, color)}
                  onReset={() => {
                    resetLabelColor(nodeType.label);
                    setColorPickerOpen(false);
                  }}
                  onClose={handleCloseColorPicker}
                />
              )}
            </div>
          </div>

          <div className="flex items-center justify-between">
            <span className="text-xs text-foreground">Icon</span>
            <IconPicker
              currentIcon={labelIcon || null}
              currentImageProp={imageProp || null}
              label={nodeType.label}
              properties={Array.isArray(nodeType.properties) ? nodeType.properties.map((p: { name: string }) => p.name) : Object.keys(nodeType.properties ?? {})}
              onSelectIcon={(name) => setLabelIcon(nodeType.label, name)}
              onResetIcon={() => resetLabelIcon(nodeType.label)}
              onSelectImageProp={(prop) => setImageProperty(nodeType.label, prop)}
              onResetImageProp={() => resetImageProperty(nodeType.label)}
            />
          </div>

          <div className="flex items-center justify-between">
            <span className="text-xs text-foreground">Caption</span>
            <select
              value={captionProp}
              onChange={(e) =>
                setCaptionProperty(nodeType.label, e.target.value)
              }
              className="h-7 rounded border border-border bg-input text-xs px-2"
            >
              <option value="">Auto</option>
              {properties.map(([name]) => (
                <option key={name} value={name}>
                  {name}
                </option>
              ))}
            </select>
          </div>
        </div>
      </section>

      <section>
        <h4 className="text-xs font-semibold text-muted-foreground uppercase tracking-wider mb-3">
          Properties ({properties.length})
        </h4>
        {properties.length === 0 ? (
          <p className="text-xs text-muted-foreground italic">
            No properties found
          </p>
        ) : (
          <div className="rounded-lg border border-border">
            {properties.map(([name, type]) => (
              <div
                key={name}
                className="flex items-center justify-between px-3 py-2 text-xs border-b border-border/50 last:border-0"
              >
                <span className="font-medium text-foreground">{name}</span>
                <span className="font-mono text-muted-foreground">{type}</span>
              </div>
            ))}
          </div>
        )}
      </section>
    </div>
  );
}

function EdgeDetailPanel({ edgeType }: { edgeType: SchemaEdgeType }) {
  const [colorPickerOpen, setColorPickerOpen] = useState(false);
  const properties = Object.entries(edgeType.properties);

  const customColor = useGraphSettingsStore(
    (s) => s.edgeColors[edgeType.type],
  );
  const setEdgeColor = useGraphSettingsStore((s) => s.setEdgeColor);
  const resetEdgeColor = useGraphSettingsStore((s) => s.resetEdgeColor);

  const effectiveColor = customColor ?? DEFAULT_EDGE_COLOR;
  const handleCloseColorPicker = useCallback(
    () => setColorPickerOpen(false),
    [],
  );

  return (
    <div className="space-y-6">
      <div className="flex items-center gap-3">
        <ArrowRight
          className="h-6 w-6 shrink-0"
          style={{ color: effectiveColor }}
        />
        <div>
          <h3 className="text-sm font-semibold">{edgeType.type}</h3>
          <p className="text-xs text-muted-foreground">
            {edgeType.count.toLocaleString()} edge
            {edgeType.count !== 1 ? "s" : ""}
          </p>
        </div>
      </div>

      <section>
        <h4 className="text-xs font-semibold text-muted-foreground uppercase tracking-wider mb-3">
          Direction
        </h4>
        <div className="flex items-center gap-2 text-xs">
          <span className="rounded bg-muted px-2 py-1 font-medium">
            {edgeType.source_labels.join(", ") || "Any"}
          </span>
          <ArrowRight className="h-3.5 w-3.5 text-muted-foreground" />
          <span className="rounded bg-muted px-2 py-1 font-medium">
            {edgeType.target_labels.join(", ") || "Any"}
          </span>
        </div>
      </section>

      <section>
        <h4 className="text-xs font-semibold text-muted-foreground uppercase tracking-wider mb-3">
          Appearance
        </h4>
        <div className="flex items-center justify-between">
          <span className="text-xs text-foreground">Color</span>
          <div className="relative">
            <button
              className="flex items-center gap-2 rounded border border-border px-2 py-1"
              onClick={() => setColorPickerOpen((v) => !v)}
            >
              <div
                className="h-4 w-4 rounded-sm"
                style={{ backgroundColor: effectiveColor }}
              />
              <span className="text-[10px] font-mono text-muted-foreground">
                {effectiveColor}
              </span>
            </button>
            {colorPickerOpen && (
              <ColorPickerInline
                currentColor={effectiveColor}
                onSelect={(color) => setEdgeColor(edgeType.type, color)}
                onReset={() => {
                  resetEdgeColor(edgeType.type);
                  setColorPickerOpen(false);
                }}
                onClose={handleCloseColorPicker}
              />
            )}
          </div>
        </div>
      </section>

      <section>
        <h4 className="text-xs font-semibold text-muted-foreground uppercase tracking-wider mb-3">
          Properties ({properties.length})
        </h4>
        {properties.length === 0 ? (
          <p className="text-xs text-muted-foreground italic">
            No properties found
          </p>
        ) : (
          <div className="rounded-lg border border-border">
            {properties.map(([name, type]) => (
              <div
                key={name}
                className="flex items-center justify-between px-3 py-2 text-xs border-b border-border/50 last:border-0"
              >
                <span className="font-medium text-foreground">{name}</span>
                <span className="font-mono text-muted-foreground">{type}</span>
              </div>
            ))}
          </div>
        )}
      </section>
    </div>
  );
}

export function SchemaTab() {
  const schema = useUiStore((s) => s.schema);
  const activeGraph = useUiStore((s) => s.activeGraph);
  const setSchema = useUiStore((s) => s.setSchema);
  const [search, setSearch] = useState("");
  const [selectedType, setSelectedType] = useState<string | null>(null);

  const handleRefresh = useCallback(async () => {
    try {
      const s = await getSchema(activeGraph);
      setSchema(s);
    } catch {
      // silently ignore refresh errors
    }
  }, [activeGraph, setSchema]);

  useEffect(() => {
    handleRefresh();
  }, [handleRefresh]);

  const filteredNodeTypes =
    schema?.node_types?.filter(
      (nt) =>
        !search || nt.label.toLowerCase().includes(search.toLowerCase()),
    ) ?? [];

  const filteredEdgeTypes =
    schema?.edge_types?.filter(
      (et) =>
        !search || et.type.toLowerCase().includes(search.toLowerCase()),
    ) ?? [];

  const selectedNodeType =
    selectedType?.startsWith("node:")
      ? schema?.node_types?.find(
          (nt) => nt.label === selectedType.slice(5),
        ) ?? null
      : null;

  const selectedEdgeType =
    selectedType?.startsWith("edge:")
      ? schema?.edge_types?.find(
          (et) => et.type === selectedType.slice(5),
        ) ?? null
      : null;

  return (
    <div className="flex h-full flex-col">
      <div className="flex items-center justify-between border-b border-border px-4 py-3">
        <div>
          <h2 className="text-sm font-semibold">Schema Browser</h2>
          <p className="text-xs text-muted-foreground">
            Graph: {activeGraph}
          </p>
        </div>
        <div className="flex items-center gap-2">
          <div className="relative">
            <Search className="absolute left-2 top-1/2 -translate-y-1/2 h-3 w-3 text-muted-foreground" />
            <input
              value={search}
              onChange={(e) => setSearch(e.target.value)}
              placeholder="Filter..."
              className="rounded border border-border bg-input pl-7 pr-2 py-1 text-xs w-40"
            />
          </div>
          <button
            onClick={handleRefresh}
            className="rounded p-1.5 hover:bg-accent"
            title="Refresh schema"
          >
            <RefreshCw className="h-3.5 w-3.5 text-muted-foreground" />
          </button>
        </div>
      </div>

      <div className="flex flex-1 min-h-0 overflow-hidden">
        <div className="w-72 shrink-0 border-r border-border overflow-auto p-3 space-y-4">
          <div>
            <h3 className="text-xs font-semibold text-muted-foreground uppercase tracking-wider mb-2 flex items-center gap-1.5">
              <Tag className="h-3 w-3" />
              Node Labels
            </h3>
            <div className="space-y-0.5">
              {filteredNodeTypes.map((nt) => (
                <button
                  key={nt.label}
                  onClick={() => setSelectedType(`node:${nt.label}`)}
                  className={cn(
                    "flex w-full items-center gap-2 rounded px-2 py-1.5 text-xs transition-colors",
                    selectedType === `node:${nt.label}`
                      ? "bg-accent"
                      : "hover:bg-accent/50",
                  )}
                >
                  <div
                    className="h-3 w-3 rounded-full shrink-0"
                    style={{
                      backgroundColor: getColorForLabel(nt.label),
                    }}
                  />
                  <span className="flex-1 text-left truncate">
                    {nt.label}
                  </span>
                  <span className="text-muted-foreground tabular-nums">
                    {nt.count.toLocaleString()}
                  </span>
                </button>
              ))}
              {filteredNodeTypes.length === 0 && (
                <p className="text-xs text-muted-foreground italic px-2 py-1">
                  No matching labels
                </p>
              )}
            </div>
          </div>

          <div>
            <h3 className="text-xs font-semibold text-muted-foreground uppercase tracking-wider mb-2 flex items-center gap-1.5">
              <ArrowRight className="h-3 w-3" />
              Edge Types
            </h3>
            <div className="space-y-0.5">
              {filteredEdgeTypes.map((et) => (
                <button
                  key={et.type}
                  onClick={() => setSelectedType(`edge:${et.type}`)}
                  className={cn(
                    "flex w-full items-center gap-2 rounded px-2 py-1.5 text-xs transition-colors",
                    selectedType === `edge:${et.type}`
                      ? "bg-accent"
                      : "hover:bg-accent/50",
                  )}
                >
                  <ArrowRight className="h-3 w-3 text-muted-foreground shrink-0" />
                  <span className="flex-1 text-left truncate">
                    {et.type}
                  </span>
                  <span className="text-muted-foreground tabular-nums">
                    {et.count.toLocaleString()}
                  </span>
                </button>
              ))}
              {filteredEdgeTypes.length === 0 && (
                <p className="text-xs text-muted-foreground italic px-2 py-1">
                  No matching types
                </p>
              )}
            </div>
          </div>

          {schema?.indexes && schema.indexes.length > 0 && (
            <div>
              <h3 className="text-xs font-semibold text-muted-foreground uppercase tracking-wider mb-2 flex items-center gap-1.5">
                <Key className="h-3 w-3" />
                Indexes
              </h3>
              <div className="space-y-0.5">
                {schema.indexes.map((idx, i) => (
                  <div
                    key={`${idx.label}-${idx.property}-${i}`}
                    className="flex items-center gap-2 rounded px-2 py-1.5 text-xs text-muted-foreground"
                  >
                    <span className="font-medium text-foreground">
                      {idx.label}
                    </span>
                    <span>.</span>
                    <span>{idx.property}</span>
                    <span className="ml-auto font-mono text-[10px]">
                      {idx.type}
                    </span>
                  </div>
                ))}
              </div>
            </div>
          )}

          {schema?.constraints && schema.constraints.length > 0 && (
            <div>
              <h3 className="text-xs font-semibold text-muted-foreground uppercase tracking-wider mb-2 flex items-center gap-1.5">
                <Shield className="h-3 w-3" />
                Constraints
              </h3>
              <div className="space-y-0.5">
                {schema.constraints.map((c, i) => (
                  <div
                    key={`${c.label}-${c.property}-${i}`}
                    className="flex items-center gap-2 rounded px-2 py-1.5 text-xs text-muted-foreground"
                  >
                    <span className="font-medium text-foreground">
                      {c.label}
                    </span>
                    <span>.</span>
                    <span>{c.property}</span>
                    <span className="ml-auto font-mono text-[10px]">
                      {c.type}
                    </span>
                  </div>
                ))}
              </div>
            </div>
          )}

          <div className="border-t border-border pt-3">
            <h3 className="text-xs font-semibold text-muted-foreground uppercase tracking-wider mb-2 flex items-center gap-1.5">
              <Database className="h-3 w-3" />
              Statistics
            </h3>
            <div className="grid grid-cols-2 gap-2 text-xs">
              <div className="rounded bg-muted p-2 text-center">
                <div className="text-lg font-bold text-foreground">
                  {schema?.statistics?.total_nodes?.toLocaleString() ?? 0}
                </div>
                <div className="text-muted-foreground">Nodes</div>
              </div>
              <div className="rounded bg-muted p-2 text-center">
                <div className="text-lg font-bold text-foreground">
                  {schema?.statistics?.total_edges?.toLocaleString() ?? 0}
                </div>
                <div className="text-muted-foreground">Edges</div>
              </div>
            </div>
            {schema?.statistics?.avg_out_degree != null && (
              <div className="mt-2 flex items-center justify-between rounded bg-muted px-2 py-1.5 text-xs">
                <span className="text-muted-foreground">Avg Out-Degree</span>
                <span className="font-mono tabular-nums text-foreground">
                  {schema.statistics.avg_out_degree.toFixed(2)}
                </span>
              </div>
            )}
          </div>
        </div>

        <div className="flex-1 min-w-0 overflow-auto p-6">
          {selectedNodeType ? (
            <NodeDetailPanel nodeType={selectedNodeType} />
          ) : selectedEdgeType ? (
            <EdgeDetailPanel edgeType={selectedEdgeType} />
          ) : (
            <div className="flex h-full flex-col items-center justify-center gap-3 text-muted-foreground">
              <Database className="h-10 w-10 opacity-30" />
              <div className="text-center">
                <p className="text-sm font-medium">
                  Select a label or type
                </p>
                <p className="text-xs mt-1">
                  Click an item on the left to view details and customize
                  appearance
                </p>
              </div>
            </div>
          )}
        </div>
      </div>
    </div>
  );
}
