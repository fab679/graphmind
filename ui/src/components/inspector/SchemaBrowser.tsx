import { useCallback, useEffect, useRef, useState } from "react";
import {
  ChevronDown,
  ChevronRight,
  Database,
  ArrowRight,
  Loader2,
  RotateCcw,
} from "lucide-react";
import { Badge } from "@/components/ui/badge";
import { IconPicker } from "@/components/ui/icon-picker";
import { useUiStore } from "@/stores/uiStore";
import { useGraphSettingsStore } from "@/stores/graphSettingsStore";
import { getColorForLabel } from "@/lib/colors";
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

function ColorPicker({
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
      className="absolute left-6 top-6 z-50 rounded-md border border-border bg-popover p-2 shadow-lg"
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
          className="h-5 w-8 cursor-pointer rounded border-0 bg-transparent p-0"
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

function NodeTypeItem({ nodeType }: { nodeType: SchemaNodeType }) {
  const [expanded, setExpanded] = useState(false);
  const [pickerOpen, setPickerOpen] = useState(false);
  const properties = Object.entries(nodeType.properties);

  const customColor = useGraphSettingsStore(
    (s) => s.labelColors[nodeType.label],
  );
  const setLabelColor = useGraphSettingsStore((s) => s.setLabelColor);
  const resetLabelColor = useGraphSettingsStore((s) => s.resetLabelColor);
  const captionProp = useGraphSettingsStore(
    (s) => s.captionProperty[nodeType.label],
  );
  const setCaptionProperty = useGraphSettingsStore(
    (s) => s.setCaptionProperty,
  );
  const labelIcon = useGraphSettingsStore(
    (s) => s.labelIcons[nodeType.label] ?? null,
  );
  const setLabelIcon = useGraphSettingsStore((s) => s.setLabelIcon);
  const resetLabelIcon = useGraphSettingsStore((s) => s.resetLabelIcon);
  const imageProp = useGraphSettingsStore(
    (s) => s.imageProperty[nodeType.label] ?? null,
  );
  const setImageProperty = useGraphSettingsStore((s) => s.setImageProperty);
  const resetImageProperty = useGraphSettingsStore((s) => s.resetImageProperty);

  const effectiveColor = customColor ?? getColorForLabel(nodeType.label);
  const propertyNames = properties.map(([name]) => name);

  const handleClosePicker = useCallback(() => setPickerOpen(false), []);

  return (
    <div className="border-b border-border/50 last:border-b-0">
      <button
        className="flex w-full items-center gap-2 px-3 py-1.5 text-left transition-colors hover:bg-accent/30"
        onClick={() => setExpanded(!expanded)}
      >
        {properties.length > 0 ? (
          expanded ? (
            <ChevronDown className="h-3 w-3 shrink-0 text-muted-foreground" />
          ) : (
            <ChevronRight className="h-3 w-3 shrink-0 text-muted-foreground" />
          )
        ) : (
          <span className="w-3" />
        )}
        <span className="relative">
          <span
            className="block h-2.5 w-2.5 shrink-0 cursor-pointer rounded-full ring-offset-1 transition-shadow hover:ring-2 hover:ring-foreground/30"
            style={{ backgroundColor: effectiveColor }}
            onClick={(e) => {
              e.stopPropagation();
              setPickerOpen((v) => !v);
            }}
            title="Change color"
          />
          {pickerOpen && (
            <ColorPicker
              currentColor={effectiveColor}
              onSelect={(color) => setLabelColor(nodeType.label, color)}
              onReset={() => {
                resetLabelColor(nodeType.label);
                setPickerOpen(false);
              }}
              onClose={handleClosePicker}
            />
          )}
        </span>
        <IconPicker
          currentIcon={labelIcon}
          currentImageProp={imageProp}
          label={nodeType.label}
          properties={propertyNames}
          onSelectIcon={(name) => setLabelIcon(nodeType.label, name)}
          onResetIcon={() => resetLabelIcon(nodeType.label)}
          onSelectImageProp={(prop) => setImageProperty(nodeType.label, prop)}
          onResetImageProp={() => resetImageProperty(nodeType.label)}
        />
        <span className="flex-1 truncate text-xs font-medium text-foreground">
          {nodeType.label}
        </span>
        <Badge variant="secondary" className="text-[10px]">
          {nodeType.count.toLocaleString()}
        </Badge>
      </button>

      {expanded && (
        <div className="border-t border-border/30 bg-muted/20 px-3 py-1.5">
          {/* Caption property selector */}
          {properties.length > 0 && (
            <div className="mb-1.5 flex items-center gap-2 text-[11px]">
              <span className="text-muted-foreground">Caption</span>
              <select
                className="flex-1 rounded border border-border bg-background px-1.5 py-0.5 text-[11px] text-foreground outline-none focus:ring-1 focus:ring-ring"
                value={captionProp ?? ""}
                onChange={(e) => {
                  const val = e.target.value;
                  if (val === "") {
                    // "Auto" — remove custom setting by setting to empty then
                    // We use setCaptionProperty with "" to signal auto, but
                    // ideally the store would have a reset. We'll set "" which
                    // downstream treats as unset.
                    setCaptionProperty(nodeType.label, "");
                  } else {
                    setCaptionProperty(nodeType.label, val);
                  }
                }}
              >
                <option value="">Auto</option>
                {properties.map(([name]) => (
                  <option key={name} value={name}>
                    {name}
                  </option>
                ))}
              </select>
            </div>
          )}

          {/* Property list */}
          {properties.map(([name, type]) => (
            <div
              key={name}
              className={cn(
                "flex items-center justify-between py-0.5 text-[11px]",
                captionProp === name && "font-semibold",
              )}
            >
              <span className="text-foreground">{name}</span>
              <span className="font-mono text-muted-foreground">{type}</span>
            </div>
          ))}
        </div>
      )}
    </div>
  );
}

function EdgeTypeItem({ edgeType }: { edgeType: SchemaEdgeType }) {
  const [expanded, setExpanded] = useState(false);
  const [pickerOpen, setPickerOpen] = useState(false);
  const properties = Object.entries(edgeType.properties);

  const customColor = useGraphSettingsStore(
    (s) => s.edgeColors[edgeType.type],
  );
  const setEdgeColor = useGraphSettingsStore((s) => s.setEdgeColor);
  const resetEdgeColor = useGraphSettingsStore((s) => s.resetEdgeColor);

  const effectiveColor = customColor ?? DEFAULT_EDGE_COLOR;

  const handleClosePicker = useCallback(() => setPickerOpen(false), []);

  return (
    <div className="border-b border-border/50 last:border-b-0">
      <button
        className="flex w-full items-center gap-2 px-3 py-1.5 text-left transition-colors hover:bg-accent/30"
        onClick={() => setExpanded(!expanded)}
      >
        {properties.length > 0 ? (
          expanded ? (
            <ChevronDown className="h-3 w-3 shrink-0 text-muted-foreground" />
          ) : (
            <ChevronRight className="h-3 w-3 shrink-0 text-muted-foreground" />
          )
        ) : (
          <span className="w-3" />
        )}
        <span className="relative">
          <span
            className="block h-2.5 w-2.5 shrink-0 cursor-pointer rounded-full ring-offset-1 transition-shadow hover:ring-2 hover:ring-foreground/30"
            style={{ backgroundColor: effectiveColor }}
            onClick={(e) => {
              e.stopPropagation();
              setPickerOpen((v) => !v);
            }}
            title="Change color"
          />
          {pickerOpen && (
            <ColorPicker
              currentColor={effectiveColor}
              onSelect={(color) => setEdgeColor(edgeType.type, color)}
              onReset={() => {
                resetEdgeColor(edgeType.type);
                setPickerOpen(false);
              }}
              onClose={handleClosePicker}
            />
          )}
        </span>
        <span className="flex-1 truncate text-xs font-medium text-foreground">
          {edgeType.type}
        </span>
        <Badge variant="secondary" className="text-[10px]">
          {edgeType.count.toLocaleString()}
        </Badge>
      </button>

      {expanded && (
        <div className="border-t border-border/30 bg-muted/20 px-3 py-1.5">
          <div className="mb-1 flex items-center gap-1 text-[10px] text-muted-foreground">
            <span>{edgeType.source_labels.join(", ")}</span>
            <ArrowRight className="h-2.5 w-2.5" />
            <span>{edgeType.target_labels.join(", ")}</span>
          </div>
          {properties.length > 0 &&
            properties.map(([name, type]) => (
              <div
                key={name}
                className="flex items-center justify-between py-0.5 text-[11px]"
              >
                <span className="text-foreground">{name}</span>
                <span className="font-mono text-muted-foreground">{type}</span>
              </div>
            ))}
        </div>
      )}
    </div>
  );
}

function CollapsibleSection({
  title,
  count,
  children,
  defaultOpen = true,
}: {
  title: string;
  count: number;
  children: React.ReactNode;
  defaultOpen?: boolean;
}) {
  const [open, setOpen] = useState(defaultOpen);

  return (
    <div>
      <button
        className="flex w-full items-center gap-2 px-3 py-2 text-left transition-colors hover:bg-accent/20"
        onClick={() => setOpen(!open)}
      >
        {open ? (
          <ChevronDown className="h-3.5 w-3.5 text-muted-foreground" />
        ) : (
          <ChevronRight className="h-3.5 w-3.5 text-muted-foreground" />
        )}
        <span className="text-xs font-semibold uppercase tracking-wider text-muted-foreground">
          {title}
        </span>
        <Badge variant="outline" className="ml-auto text-[10px]">
          {count}
        </Badge>
      </button>
      {open && <div>{children}</div>}
    </div>
  );
}

export function SchemaBrowser() {
  const schema = useUiStore((s) => s.schema);

  if (!schema) {
    return (
      <div className="flex h-full flex-col items-center justify-center gap-2">
        <Loader2 className={cn("h-5 w-5 animate-spin text-muted-foreground")} />
        <p className="text-xs text-muted-foreground">Loading schema...</p>
      </div>
    );
  }

  return (
    <div className="flex h-full flex-col overflow-y-auto">
      <CollapsibleSection title="Node Labels" count={schema.node_types.length}>
        {schema.node_types.map((nodeType) => (
          <NodeTypeItem key={nodeType.label} nodeType={nodeType} />
        ))}
      </CollapsibleSection>

      <div className="border-t border-border" />

      <CollapsibleSection title="Edge Types" count={schema.edge_types.length}>
        {schema.edge_types.map((edgeType) => (
          <EdgeTypeItem key={edgeType.type} edgeType={edgeType} />
        ))}
      </CollapsibleSection>

      <div className="mt-auto border-t border-border">
        <div className="flex items-center gap-2 px-3 py-2">
          <Database className="h-3.5 w-3.5 text-muted-foreground" />
          <span className="text-xs font-semibold uppercase tracking-wider text-muted-foreground">
            Statistics
          </span>
        </div>
        <div className="space-y-1 px-3 pb-3 text-xs">
          <div className="flex justify-between">
            <span className="text-muted-foreground">Total Nodes</span>
            <span className="font-mono tabular-nums text-foreground">
              {schema.statistics.total_nodes.toLocaleString()}
            </span>
          </div>
          <div className="flex justify-between">
            <span className="text-muted-foreground">Total Edges</span>
            <span className="font-mono tabular-nums text-foreground">
              {schema.statistics.total_edges.toLocaleString()}
            </span>
          </div>
          <div className="flex justify-between">
            <span className="text-muted-foreground">Avg Out-Degree</span>
            <span className="font-mono tabular-nums text-foreground">
              {schema.statistics.avg_out_degree.toFixed(2)}
            </span>
          </div>
        </div>
      </div>
    </div>
  );
}
