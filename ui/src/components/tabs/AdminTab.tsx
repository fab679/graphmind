import { useState, useEffect, useCallback, useRef } from "react";
import {
  Activity,
  Database,
  Download,
  Loader2,
  Plus,
  RefreshCw,
  Trash2,
  Upload,
} from "lucide-react";
import { useUiStore } from "@/stores/uiStore";
import {
  listGraphs,
  deleteGraph,
  getStatus,
  executeScript,
} from "@/api/client";
import type { StatusResponse } from "@/types/api";
import { cn } from "@/lib/utils";

function GraphsSection() {
  const activeGraph = useUiStore((s) => s.activeGraph);
  const setActiveGraph = useUiStore((s) => s.setActiveGraph);
  const availableGraphs = useUiStore((s) => s.availableGraphs);
  const setAvailableGraphs = useUiStore((s) => s.setAvailableGraphs);
  const [loading, setLoading] = useState(false);
  const [newGraphName, setNewGraphName] = useState("");
  const [showCreate, setShowCreate] = useState(false);
  const [error, setError] = useState<string | null>(null);

  const refresh = useCallback(async () => {
    setLoading(true);
    setError(null);
    try {
      const graphs = await listGraphs();
      setAvailableGraphs(graphs);
    } catch (err) {
      setError(err instanceof Error ? err.message : "Failed to list graphs");
    } finally {
      setLoading(false);
    }
  }, [setAvailableGraphs]);

  useEffect(() => {
    refresh();
  }, [refresh]);

  const handleCreate = async () => {
    const name = newGraphName.trim();
    if (!name) return;
    setError(null);
    try {
      // Creating a graph by executing a no-op query against it
      await executeScript("// init", name);
      setNewGraphName("");
      setShowCreate(false);
      await refresh();
      setActiveGraph(name);
    } catch (err) {
      setError(
        err instanceof Error ? err.message : "Failed to create graph",
      );
    }
  };

  const handleDelete = async (name: string) => {
    if (name === "default") return;
    setError(null);
    try {
      await deleteGraph(name);
      if (activeGraph === name) {
        setActiveGraph("default");
      }
      await refresh();
    } catch (err) {
      setError(
        err instanceof Error ? err.message : "Failed to delete graph",
      );
    }
  };

  return (
    <div className="space-y-4">
      <div className="flex items-center justify-between">
        <h3 className="text-sm font-semibold">
          Graphs ({availableGraphs.length})
        </h3>
        <div className="flex items-center gap-2">
          <button
            onClick={() => setShowCreate((v) => !v)}
            className="flex items-center gap-1 rounded bg-primary px-2.5 py-1.5 text-xs font-medium text-primary-foreground hover:bg-primary/90"
          >
            <Plus className="h-3 w-3" />
            New Graph
          </button>
          <button
            onClick={refresh}
            className="rounded p-1.5 hover:bg-accent"
            title="Refresh"
          >
            <RefreshCw
              className={cn(
                "h-3.5 w-3.5 text-muted-foreground",
                loading && "animate-spin",
              )}
            />
          </button>
        </div>
      </div>

      {error && (
        <div className="rounded border border-destructive/50 bg-destructive/10 px-3 py-2 text-xs text-destructive">
          {error}
        </div>
      )}

      {showCreate && (
        <div className="flex items-center gap-2 rounded border border-border bg-muted/50 p-3">
          <input
            value={newGraphName}
            onChange={(e) => setNewGraphName(e.target.value)}
            placeholder="Graph name..."
            className="flex-1 rounded border border-border bg-input px-2 py-1 text-xs"
            onKeyDown={(e) => {
              if (e.key === "Enter") handleCreate();
            }}
            autoFocus
          />
          <button
            onClick={handleCreate}
            disabled={!newGraphName.trim()}
            className="rounded bg-primary px-3 py-1 text-xs font-medium text-primary-foreground hover:bg-primary/90 disabled:opacity-50"
          >
            Create
          </button>
          <button
            onClick={() => setShowCreate(false)}
            className="rounded px-2 py-1 text-xs text-muted-foreground hover:bg-accent"
          >
            Cancel
          </button>
        </div>
      )}

      <div className="rounded-lg border border-border">
        {availableGraphs.map((name) => (
          <div
            key={name}
            className={cn(
              "flex items-center gap-3 px-3 py-2.5 text-xs border-b border-border/50 last:border-0",
              activeGraph === name && "bg-accent/50",
            )}
          >
            <Database className="h-4 w-4 text-muted-foreground shrink-0" />
            <span className="flex-1 font-medium text-foreground">{name}</span>
            {activeGraph === name && (
              <span className="rounded bg-primary/10 px-1.5 py-0.5 text-[10px] font-medium text-primary">
                Active
              </span>
            )}
            {activeGraph !== name && (
              <button
                onClick={() => setActiveGraph(name)}
                className="rounded px-2 py-0.5 text-[10px] text-muted-foreground hover:bg-accent hover:text-foreground"
              >
                Switch
              </button>
            )}
            {name !== "default" && (
              <button
                onClick={() => handleDelete(name)}
                className="rounded p-1 text-muted-foreground hover:bg-destructive/10 hover:text-destructive"
                title="Delete graph"
              >
                <Trash2 className="h-3 w-3" />
              </button>
            )}
          </div>
        ))}
        {availableGraphs.length === 0 && (
          <div className="p-4 text-center text-xs text-muted-foreground">
            No graphs found
          </div>
        )}
      </div>
    </div>
  );
}

function ServerSection() {
  const activeGraph = useUiStore((s) => s.activeGraph);
  const [status, setStatus] = useState<StatusResponse | null>(null);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);

  const refresh = useCallback(async () => {
    setLoading(true);
    setError(null);
    try {
      const s = await getStatus(activeGraph);
      setStatus(s);
    } catch (err) {
      setError(
        err instanceof Error ? err.message : "Failed to get server status",
      );
    } finally {
      setLoading(false);
    }
  }, [activeGraph]);

  useEffect(() => {
    refresh();
  }, [refresh]);

  const cacheHitRate =
    status?.cache &&
    status.cache.hits + status.cache.misses > 0
      ? (
          (status.cache.hits / (status.cache.hits + status.cache.misses)) *
          100
        ).toFixed(1)
      : "0.0";

  return (
    <div className="space-y-4">
      <div className="flex items-center justify-between">
        <h3 className="text-sm font-semibold">Server Status</h3>
        <button
          onClick={refresh}
          className="rounded p-1.5 hover:bg-accent"
          title="Refresh"
        >
          <RefreshCw
            className={cn(
              "h-3.5 w-3.5 text-muted-foreground",
              loading && "animate-spin",
            )}
          />
        </button>
      </div>

      {error && (
        <div className="rounded border border-destructive/50 bg-destructive/10 px-3 py-2 text-xs text-destructive">
          {error}
        </div>
      )}

      {loading && !status ? (
        <div className="flex items-center justify-center py-8">
          <Loader2 className="h-5 w-5 animate-spin text-muted-foreground" />
        </div>
      ) : status ? (
        <div className="space-y-4">
          {/* Server info */}
          <div className="rounded-lg border border-border">
            <div className="border-b border-border/50 px-3 py-2">
              <h4 className="text-xs font-semibold text-muted-foreground uppercase tracking-wider">
                Server Info
              </h4>
            </div>
            <div className="divide-y divide-border/50">
              <div className="flex items-center justify-between px-3 py-2 text-xs">
                <span className="text-muted-foreground">Status</span>
                <span
                  className={cn(
                    "rounded px-1.5 py-0.5 font-medium",
                    status.status === "ok"
                      ? "bg-green-500/10 text-green-600"
                      : "bg-red-500/10 text-red-600",
                  )}
                >
                  {status.status}
                </span>
              </div>
              <div className="flex items-center justify-between px-3 py-2 text-xs">
                <span className="text-muted-foreground">Version</span>
                <span className="font-mono text-foreground">
                  {status.version}
                </span>
              </div>
            </div>
          </div>

          {/* Storage */}
          <div className="rounded-lg border border-border">
            <div className="border-b border-border/50 px-3 py-2">
              <h4 className="text-xs font-semibold text-muted-foreground uppercase tracking-wider">
                Storage
              </h4>
            </div>
            <div className="grid grid-cols-2 gap-3 p-3">
              <div className="rounded bg-muted p-3 text-center">
                <div className="text-xl font-bold text-foreground">
                  {status.storage.nodes.toLocaleString()}
                </div>
                <div className="text-[10px] text-muted-foreground mt-1">
                  Nodes
                </div>
              </div>
              <div className="rounded bg-muted p-3 text-center">
                <div className="text-xl font-bold text-foreground">
                  {status.storage.edges.toLocaleString()}
                </div>
                <div className="text-[10px] text-muted-foreground mt-1">
                  Edges
                </div>
              </div>
            </div>
          </div>

          {/* Cache */}
          <div className="rounded-lg border border-border">
            <div className="border-b border-border/50 px-3 py-2">
              <h4 className="text-xs font-semibold text-muted-foreground uppercase tracking-wider">
                Cache
              </h4>
            </div>
            <div className="divide-y divide-border/50">
              <div className="flex items-center justify-between px-3 py-2 text-xs">
                <span className="text-muted-foreground">Hit Rate</span>
                <span className="font-mono text-foreground">
                  {cacheHitRate}%
                </span>
              </div>
              <div className="flex items-center justify-between px-3 py-2 text-xs">
                <span className="text-muted-foreground">Hits</span>
                <span className="font-mono tabular-nums text-foreground">
                  {status.cache.hits.toLocaleString()}
                </span>
              </div>
              <div className="flex items-center justify-between px-3 py-2 text-xs">
                <span className="text-muted-foreground">Misses</span>
                <span className="font-mono tabular-nums text-foreground">
                  {status.cache.misses.toLocaleString()}
                </span>
              </div>
              <div className="flex items-center justify-between px-3 py-2 text-xs">
                <span className="text-muted-foreground">Size</span>
                <span className="font-mono tabular-nums text-foreground">
                  {status.cache.size.toLocaleString()}
                </span>
              </div>
            </div>
          </div>
        </div>
      ) : null}
    </div>
  );
}

function ImportExportSection() {
  const activeGraph = useUiStore((s) => s.activeGraph);
  const [message, setMessage] = useState<{
    type: "success" | "error";
    text: string;
  } | null>(null);
  const [loading, setLoading] = useState(false);
  const csvInputRef = useRef<HTMLInputElement>(null);
  const jsonInputRef = useRef<HTMLInputElement>(null);
  const snapInputRef = useRef<HTMLInputElement>(null);

  const handleCsvImport = async (e: React.ChangeEvent<HTMLInputElement>) => {
    const file = e.target.files?.[0];
    if (!file) return;
    setLoading(true);
    setMessage(null);
    try {
      const text = await file.text();
      const resp = await fetch("/api/import/csv", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ data: text, graph: activeGraph }),
      });
      if (!resp.ok) throw new Error(await resp.text());
      const result = await resp.json();
      setMessage({
        type: "success",
        text: `Imported ${result.imported ?? "unknown"} records from CSV`,
      });
    } catch (err) {
      setMessage({
        type: "error",
        text: err instanceof Error ? err.message : "CSV import failed",
      });
    } finally {
      setLoading(false);
      if (csvInputRef.current) csvInputRef.current.value = "";
    }
  };

  const handleJsonImport = async (e: React.ChangeEvent<HTMLInputElement>) => {
    const file = e.target.files?.[0];
    if (!file) return;
    setLoading(true);
    setMessage(null);
    try {
      const text = await file.text();
      const resp = await fetch("/api/import/json", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ data: text, graph: activeGraph }),
      });
      if (!resp.ok) throw new Error(await resp.text());
      const result = await resp.json();
      setMessage({
        type: "success",
        text: `Imported ${result.imported ?? "unknown"} records from JSON`,
      });
    } catch (err) {
      setMessage({
        type: "error",
        text: err instanceof Error ? err.message : "JSON import failed",
      });
    } finally {
      setLoading(false);
      if (jsonInputRef.current) jsonInputRef.current.value = "";
    }
  };

  const handleSnapshotExport = async () => {
    setLoading(true);
    setMessage(null);
    try {
      const resp = await fetch("/api/snapshot/export", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ graph: activeGraph }),
      });
      if (!resp.ok) throw new Error(await resp.text());
      const blob = await resp.blob();
      const url = URL.createObjectURL(blob);
      const a = document.createElement("a");
      a.href = url;
      a.download = `${activeGraph}-snapshot.sgsnap`;
      a.click();
      URL.revokeObjectURL(url);
      setMessage({ type: "success", text: "Snapshot exported successfully" });
    } catch (err) {
      setMessage({
        type: "error",
        text: err instanceof Error ? err.message : "Export failed",
      });
    } finally {
      setLoading(false);
    }
  };

  const handleSnapshotImport = async (
    e: React.ChangeEvent<HTMLInputElement>,
  ) => {
    const file = e.target.files?.[0];
    if (!file) return;
    setLoading(true);
    setMessage(null);
    try {
      const formData = new FormData();
      formData.append("file", file);
      formData.append("graph", activeGraph);
      const resp = await fetch("/api/snapshot/import", {
        method: "POST",
        body: formData,
      });
      if (!resp.ok) throw new Error(await resp.text());
      setMessage({
        type: "success",
        text: "Snapshot imported successfully",
      });
    } catch (err) {
      setMessage({
        type: "error",
        text: err instanceof Error ? err.message : "Snapshot import failed",
      });
    } finally {
      setLoading(false);
      if (snapInputRef.current) snapInputRef.current.value = "";
    }
  };

  return (
    <div className="space-y-4">
      <h3 className="text-sm font-semibold">Import / Export</h3>
      <p className="text-xs text-muted-foreground">
        Target graph: <span className="font-medium text-foreground">{activeGraph}</span>
      </p>

      {message && (
        <div
          className={cn(
            "rounded border px-3 py-2 text-xs",
            message.type === "success"
              ? "border-green-500/50 bg-green-500/10 text-green-600"
              : "border-destructive/50 bg-destructive/10 text-destructive",
          )}
        >
          {message.text}
        </div>
      )}

      {/* Import section */}
      <div className="rounded-lg border border-border">
        <div className="border-b border-border/50 px-3 py-2">
          <h4 className="text-xs font-semibold text-muted-foreground uppercase tracking-wider">
            Import
          </h4>
        </div>
        <div className="divide-y divide-border/50">
          <div className="flex items-center justify-between px-3 py-3">
            <div>
              <div className="text-xs font-medium text-foreground">
                CSV File
              </div>
              <div className="text-[10px] text-muted-foreground mt-0.5">
                Import nodes from a CSV file
              </div>
            </div>
            <div>
              <input
                ref={csvInputRef}
                type="file"
                accept=".csv"
                onChange={handleCsvImport}
                className="hidden"
              />
              <button
                onClick={() => csvInputRef.current?.click()}
                disabled={loading}
                className="flex items-center gap-1.5 rounded border border-border bg-background px-3 py-1.5 text-xs hover:bg-accent disabled:opacity-50"
              >
                <Upload className="h-3 w-3" />
                Choose CSV
              </button>
            </div>
          </div>
          <div className="flex items-center justify-between px-3 py-3">
            <div>
              <div className="text-xs font-medium text-foreground">
                JSON File
              </div>
              <div className="text-[10px] text-muted-foreground mt-0.5">
                Import nodes from a JSON file
              </div>
            </div>
            <div>
              <input
                ref={jsonInputRef}
                type="file"
                accept=".json"
                onChange={handleJsonImport}
                className="hidden"
              />
              <button
                onClick={() => jsonInputRef.current?.click()}
                disabled={loading}
                className="flex items-center gap-1.5 rounded border border-border bg-background px-3 py-1.5 text-xs hover:bg-accent disabled:opacity-50"
              >
                <Upload className="h-3 w-3" />
                Choose JSON
              </button>
            </div>
          </div>
          <div className="flex items-center justify-between px-3 py-3">
            <div>
              <div className="text-xs font-medium text-foreground">
                Snapshot (.sgsnap)
              </div>
              <div className="text-[10px] text-muted-foreground mt-0.5">
                Restore from a portable snapshot file
              </div>
            </div>
            <div>
              <input
                ref={snapInputRef}
                type="file"
                accept=".sgsnap"
                onChange={handleSnapshotImport}
                className="hidden"
              />
              <button
                onClick={() => snapInputRef.current?.click()}
                disabled={loading}
                className="flex items-center gap-1.5 rounded border border-border bg-background px-3 py-1.5 text-xs hover:bg-accent disabled:opacity-50"
              >
                <Upload className="h-3 w-3" />
                Import Snapshot
              </button>
            </div>
          </div>
        </div>
      </div>

      {/* Export section */}
      <div className="rounded-lg border border-border">
        <div className="border-b border-border/50 px-3 py-2">
          <h4 className="text-xs font-semibold text-muted-foreground uppercase tracking-wider">
            Export
          </h4>
        </div>
        <div className="flex items-center justify-between px-3 py-3">
          <div>
            <div className="text-xs font-medium text-foreground">
              Snapshot (.sgsnap)
            </div>
            <div className="text-[10px] text-muted-foreground mt-0.5">
              Export the current graph as a portable snapshot
            </div>
          </div>
          <button
            onClick={handleSnapshotExport}
            disabled={loading}
            className="flex items-center gap-1.5 rounded border border-border bg-background px-3 py-1.5 text-xs hover:bg-accent disabled:opacity-50"
          >
            <Download className="h-3 w-3" />
            Export Snapshot
          </button>
        </div>
      </div>

      {loading && (
        <div className="flex items-center justify-center py-2">
          <Loader2 className="h-4 w-4 animate-spin text-muted-foreground" />
        </div>
      )}
    </div>
  );
}

export function AdminTab() {
  const [activeSection, setActiveSection] = useState<
    "graphs" | "server" | "import"
  >("graphs");

  return (
    <div className="flex h-full flex-col">
      <div className="border-b border-border px-4 py-3">
        <h2 className="text-sm font-semibold">Database Administration</h2>
        <p className="text-xs text-muted-foreground">
          Manage graphs, server status, and data import/export
        </p>
      </div>

      <div className="flex border-b border-border px-4">
        {(
          [
            { id: "graphs", label: "Graphs", icon: Database },
            { id: "server", label: "Server", icon: Activity },
            { id: "import", label: "Import/Export", icon: Download },
          ] as const
        ).map(({ id, label, icon: Icon }) => (
          <button
            key={id}
            onClick={() => setActiveSection(id)}
            className={cn(
              "flex items-center gap-1.5 px-3 py-2 text-xs border-b-2 transition-colors -mb-px",
              activeSection === id
                ? "border-primary text-primary"
                : "border-transparent text-muted-foreground hover:text-foreground",
            )}
          >
            <Icon className="h-3.5 w-3.5" /> {label}
          </button>
        ))}
      </div>

      <div className="flex-1 overflow-auto p-4">
        {activeSection === "graphs" && <GraphsSection />}
        {activeSection === "server" && <ServerSection />}
        {activeSection === "import" && <ImportExportSection />}
      </div>
    </div>
  );
}
