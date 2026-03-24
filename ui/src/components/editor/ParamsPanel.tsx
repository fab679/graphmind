import { useState, useCallback, useEffect } from "react";
import { Plus, Trash2, Code2, Table2 } from "lucide-react";
import { cn } from "@/lib/utils";

interface ParamEntry {
  key: string;
  value: string;
  type: "string" | "number" | "boolean" | "null";
}

function parseJsonToEntries(json: string): ParamEntry[] {
  if (!json.trim()) return [];
  try {
    const obj = JSON.parse(json);
    if (typeof obj !== "object" || obj === null || Array.isArray(obj)) return [];
    return Object.entries(obj).map(([key, value]) => {
      if (typeof value === "string") return { key, value, type: "string" as const };
      if (typeof value === "number") return { key, value: String(value), type: "number" as const };
      if (typeof value === "boolean") return { key, value: String(value), type: "boolean" as const };
      if (value === null) return { key, value: "null", type: "null" as const };
      return { key, value: JSON.stringify(value), type: "string" as const };
    });
  } catch {
    return [];
  }
}

function entriesToJson(entries: ParamEntry[]): string {
  if (entries.length === 0) return "";
  const obj: Record<string, unknown> = {};
  for (const e of entries) {
    if (!e.key.trim()) continue;
    switch (e.type) {
      case "number": {
        const n = Number(e.value);
        obj[e.key] = isNaN(n) ? e.value : n;
        break;
      }
      case "boolean":
        obj[e.key] = e.value === "true";
        break;
      case "null":
        obj[e.key] = null;
        break;
      default:
        obj[e.key] = e.value;
    }
  }
  return JSON.stringify(obj, null, 2);
}

function detectType(value: string): ParamEntry["type"] {
  if (value === "null") return "null";
  if (value === "true" || value === "false") return "boolean";
  if (/^-?\d+(\.\d+)?$/.test(value)) return "number";
  return "string";
}

export function ParamsPanel({
  value,
  onChange,
}: {
  value: string;
  onChange: (v: string) => void;
}) {
  const [mode, setMode] = useState<"form" | "json">("form");
  const [entries, setEntries] = useState<ParamEntry[]>(() =>
    parseJsonToEntries(value)
  );
  const [jsonText, setJsonText] = useState(value);
  const [jsonError, setJsonError] = useState<string | null>(null);

  // Sync entries → parent
  const updateFromEntries = useCallback(
    (newEntries: ParamEntry[]) => {
      setEntries(newEntries);
      const json = entriesToJson(newEntries);
      setJsonText(json);
      onChange(json);
    },
    [onChange]
  );

  // Sync json text → parent
  const updateFromJson = useCallback(
    (text: string) => {
      setJsonText(text);
      if (!text.trim()) {
        setJsonError(null);
        setEntries([]);
        onChange("");
        return;
      }
      try {
        JSON.parse(text);
        setJsonError(null);
        setEntries(parseJsonToEntries(text));
        onChange(text);
      } catch (e) {
        setJsonError(e instanceof Error ? e.message : "Invalid JSON");
      }
    },
    [onChange]
  );

  // When switching modes, sync
  useEffect(() => {
    if (mode === "json") {
      setJsonText(entriesToJson(entries));
    } else {
      const parsed = parseJsonToEntries(jsonText);
      if (parsed.length > 0 || !jsonText.trim()) {
        setEntries(parsed);
      }
    }
  // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [mode]);

  const addEntry = () => {
    updateFromEntries([...entries, { key: "", value: "", type: "string" }]);
  };

  const removeEntry = (idx: number) => {
    updateFromEntries(entries.filter((_, i) => i !== idx));
  };

  const updateEntry = (idx: number, field: keyof ParamEntry, val: string) => {
    const updated = [...entries];
    if (field === "type") {
      updated[idx] = { ...updated[idx], type: val as ParamEntry["type"] };
    } else {
      updated[idx] = { ...updated[idx], [field]: val };
      // Auto-detect type when value changes
      if (field === "value") {
        updated[idx].type = detectType(val);
      }
    }
    updateFromEntries(updated);
  };

  const hasParams = entries.some((e) => e.key.trim());

  return (
    <div className="shrink-0 border-b border-border bg-muted/20">
      {/* Header */}
      <div className="flex items-center gap-2 px-3 py-1.5 border-b border-border/50">
        <span className="text-[10px] font-semibold uppercase tracking-wider text-muted-foreground">
          Parameters
        </span>
        <span className="text-[10px] text-muted-foreground">
          Use <code className="rounded bg-muted px-1 font-mono">$name</code> in query
        </span>
        <div className="flex-1" />
        {/* Mode toggle */}
        <div className="flex items-center gap-0.5 rounded-md border border-border p-0.5">
          <button
            onClick={() => setMode("form")}
            className={cn(
              "rounded p-0.5 text-[10px]",
              mode === "form"
                ? "bg-accent text-foreground"
                : "text-muted-foreground"
            )}
            title="Form view"
          >
            <Table2 className="h-3 w-3" />
          </button>
          <button
            onClick={() => setMode("json")}
            className={cn(
              "rounded p-0.5 text-[10px]",
              mode === "json"
                ? "bg-accent text-foreground"
                : "text-muted-foreground"
            )}
            title="JSON view"
          >
            <Code2 className="h-3 w-3" />
          </button>
        </div>
      </div>

      {/* Form mode */}
      {mode === "form" && (
        <div className="px-3 py-2 space-y-1.5">
          {entries.length === 0 && (
            <div className="text-[11px] text-muted-foreground/60 italic py-1">
              No parameters. Click + to add one.
            </div>
          )}
          {entries.map((entry, idx) => (
            <div key={idx} className="flex items-center gap-1.5">
              {/* Key */}
              <span className="text-xs text-muted-foreground font-mono shrink-0">$</span>
              <input
                value={entry.key}
                onChange={(e) => updateEntry(idx, "key", e.target.value)}
                placeholder="name"
                spellCheck={false}
                className="w-28 shrink-0 rounded border border-border bg-background px-2 py-1 font-mono text-xs text-foreground placeholder:text-muted-foreground/40 focus:outline-none focus:ring-1 focus:ring-primary/50"
              />
              <span className="text-xs text-muted-foreground">=</span>
              {/* Value */}
              {entry.type === "boolean" ? (
                <select
                  value={entry.value}
                  onChange={(e) => updateEntry(idx, "value", e.target.value)}
                  className="flex-1 min-w-0 rounded border border-border bg-background px-2 py-1 font-mono text-xs text-foreground focus:outline-none focus:ring-1 focus:ring-primary/50"
                >
                  <option value="true">true</option>
                  <option value="false">false</option>
                </select>
              ) : (
                <input
                  value={entry.value}
                  onChange={(e) => updateEntry(idx, "value", e.target.value)}
                  placeholder={entry.type === "number" ? "42" : "value"}
                  spellCheck={false}
                  className="flex-1 min-w-0 rounded border border-border bg-background px-2 py-1 font-mono text-xs text-foreground placeholder:text-muted-foreground/40 focus:outline-none focus:ring-1 focus:ring-primary/50"
                />
              )}
              {/* Type badge */}
              <select
                value={entry.type}
                onChange={(e) => updateEntry(idx, "type", e.target.value)}
                className="w-20 shrink-0 rounded border border-border bg-background px-1 py-1 text-[10px] text-muted-foreground focus:outline-none"
              >
                <option value="string">String</option>
                <option value="number">Number</option>
                <option value="boolean">Boolean</option>
                <option value="null">Null</option>
              </select>
              {/* Remove */}
              <button
                onClick={() => removeEntry(idx)}
                className="shrink-0 rounded p-1 text-muted-foreground/50 hover:bg-destructive/10 hover:text-destructive transition-colors"
              >
                <Trash2 className="h-3 w-3" />
              </button>
            </div>
          ))}
          <button
            onClick={addEntry}
            className="flex items-center gap-1 rounded-md px-2 py-1 text-[11px] text-muted-foreground transition-colors hover:bg-accent hover:text-foreground"
          >
            <Plus className="h-3 w-3" />
            Add parameter
          </button>
          {hasParams && (
            <div className="mt-1 text-[10px] text-muted-foreground/50 italic">
              Tip: Use parameters for text with special characters (quotes, semicolons)
            </div>
          )}
        </div>
      )}

      {/* JSON mode */}
      {mode === "json" && (
        <div className="px-3 py-2">
          <textarea
            value={jsonText}
            onChange={(e) => updateFromJson(e.target.value)}
            placeholder={'{\n  "name": "Alice",\n  "age": 30\n}'}
            spellCheck={false}
            className={cn(
              "w-full rounded-md border bg-background px-3 py-2 font-mono text-xs text-foreground placeholder:text-muted-foreground/40 focus:outline-none focus:ring-1",
              jsonError
                ? "border-destructive/50 focus:ring-destructive/50"
                : "border-border focus:ring-primary/50"
            )}
            rows={4}
          />
          {jsonError && (
            <div className="mt-1 text-[10px] text-destructive">{jsonError}</div>
          )}
        </div>
      )}
    </div>
  );
}
