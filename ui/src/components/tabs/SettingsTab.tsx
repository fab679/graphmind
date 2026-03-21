import { useGraphSettingsStore } from "@/stores/graphSettingsStore";
import { useUiStore } from "@/stores/uiStore";
import { getColorForLabel } from "@/lib/colors";
import { IconPicker } from "@/components/ui/icon-picker";
import { RotateCcw } from "lucide-react";

export function SettingsTab() {
  const schema = useUiStore((s) => s.schema);
  const labelColors = useGraphSettingsStore((s) => s.labelColors);
  const edgeColors = useGraphSettingsStore((s) => s.edgeColors);
  const labelIcons = useGraphSettingsStore((s) => s.labelIcons);
  const captionProperty = useGraphSettingsStore((s) => s.captionProperty);
  const setLabelColor = useGraphSettingsStore((s) => s.setLabelColor);
  const setEdgeColor = useGraphSettingsStore((s) => s.setEdgeColor);
  const setLabelIcon = useGraphSettingsStore((s) => s.setLabelIcon);
  const resetLabelIcon = useGraphSettingsStore((s) => s.resetLabelIcon);
  const imageProperty = useGraphSettingsStore((s) => s.imageProperty);
  const setImageProperty = useGraphSettingsStore((s) => s.setImageProperty);
  const resetImageProperty = useGraphSettingsStore((s) => s.resetImageProperty);
  const setCaptionProperty = useGraphSettingsStore((s) => s.setCaptionProperty);
  const resetAll = useGraphSettingsStore((s) => s.resetAll);

  return (
    <div className="flex h-full flex-col">
      <div className="flex items-center justify-between border-b border-border px-4 py-3">
        <div>
          <h2 className="text-sm font-semibold">Settings</h2>
          <p className="text-xs text-muted-foreground">
            Customize appearance and preferences
          </p>
        </div>
        <button
          onClick={resetAll}
          className="flex items-center gap-1 rounded px-2 py-1 text-xs text-destructive hover:bg-destructive/10"
        >
          <RotateCcw className="h-3 w-3" /> Reset All
        </button>
      </div>

      <div className="flex-1 overflow-auto p-4 max-w-3xl space-y-6">
        {/* Node Appearance */}
        <section>
          <h3 className="text-sm font-semibold mb-3">Node Labels</h3>
          {schema?.node_types && schema.node_types.length > 0 ? (
            <div className="rounded-lg border border-border">
              <div className="grid grid-cols-[1fr_80px_80px_120px] gap-2 p-2 text-[10px] font-medium text-muted-foreground uppercase tracking-wider border-b border-border">
                <span>Label</span>
                <span>Color</span>
                <span>Icon</span>
                <span>Caption</span>
              </div>
              {schema.node_types.map((nt) => (
                <div
                  key={nt.label}
                  className="grid grid-cols-[1fr_80px_80px_120px] items-center gap-2 p-2 border-b border-border/50 last:border-0"
                >
                  <span className="text-xs font-medium">{nt.label}</span>
                  <input
                    type="color"
                    value={
                      labelColors[nt.label] || getColorForLabel(nt.label)
                    }
                    onChange={(e) => setLabelColor(nt.label, e.target.value)}
                    className="h-6 w-8 rounded cursor-pointer"
                  />
                  <IconPicker
                    currentIcon={labelIcons[nt.label] || null}
                    currentImageProp={imageProperty[nt.label] || null}
                    label={nt.label}
                    properties={nt.properties ? Object.keys(nt.properties) : []}
                    onSelectIcon={(name) => setLabelIcon(nt.label, name)}
                    onResetIcon={() => resetLabelIcon(nt.label)}
                    onSelectImageProp={(prop) => setImageProperty(nt.label, prop)}
                    onResetImageProp={() => resetImageProperty(nt.label)}
                  />
                  <select
                    value={captionProperty[nt.label] || ""}
                    onChange={(e) =>
                      setCaptionProperty(nt.label, e.target.value)
                    }
                    className="h-6 rounded border border-border bg-input text-[10px] px-1"
                  >
                    <option value="">Auto</option>
                    {Object.keys(nt.properties).map((p) => (
                      <option key={p} value={p}>
                        {p}
                      </option>
                    ))}
                  </select>
                </div>
              ))}
            </div>
          ) : (
            <p className="text-xs text-muted-foreground italic">
              No node labels in the current schema. Load data to configure
              appearance.
            </p>
          )}
        </section>

        {/* Edge Appearance */}
        <section>
          <h3 className="text-sm font-semibold mb-3">Edge Types</h3>
          {schema?.edge_types && schema.edge_types.length > 0 ? (
            <div className="rounded-lg border border-border">
              <div className="grid grid-cols-[1fr_80px] gap-2 p-2 text-[10px] font-medium text-muted-foreground uppercase tracking-wider border-b border-border">
                <span>Type</span>
                <span>Color</span>
              </div>
              {schema.edge_types.map((et) => (
                <div
                  key={et.type}
                  className="grid grid-cols-[1fr_80px] items-center gap-2 p-2 border-b border-border/50 last:border-0"
                >
                  <span className="text-xs font-medium">{et.type}</span>
                  <input
                    type="color"
                    value={edgeColors[et.type] || "#64748b"}
                    onChange={(e) => setEdgeColor(et.type, e.target.value)}
                    className="h-6 w-8 rounded cursor-pointer"
                  />
                </div>
              ))}
            </div>
          ) : (
            <p className="text-xs text-muted-foreground italic">
              No edge types in the current schema. Load data to configure
              appearance.
            </p>
          )}
        </section>
      </div>
    </div>
  );
}
