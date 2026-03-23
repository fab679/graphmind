import { useState } from "react";
import {
  Play,
  Upload,
  History,
  Bookmark,
  Check,
  AlertCircle,
  Terminal,
  X,
} from "lucide-react";
import { CypherEditor } from "@/components/editor/CypherEditor";
import { ForceGraph } from "@/components/graph/ForceGraph";
import { ResultsTable } from "@/components/results/ResultsTable";
import { PropertyInspector } from "@/components/inspector/PropertyInspector";
import { SavedQueries } from "@/components/editor/SavedQueries";
import { useQueryStore } from "@/stores/queryStore";
import { useGraphStore } from "@/stores/graphStore";
import { useUiStore } from "@/stores/uiStore";
import { executeScript } from "@/api/client";
import type { ScriptResponse } from "@/api/client";
import { cn } from "@/lib/utils";

interface ScriptResult {
  success: boolean;
  executed: number;
  errors: string[];
}

export function QueryTab() {
  const currentQuery = useQueryStore((s) => s.currentQuery);
  const setQuery = useQueryStore((s) => s.setQuery);
  const executeQuery = useQueryStore((s) => s.executeQuery);
  const isExecuting = useQueryStore((s) => s.isExecuting);
  const error = useQueryStore((s) => s.error);
  const columns = useQueryStore((s) => s.columns);
  const records = useQueryStore((s) => s.records);
  const writeStats = useQueryStore((s) => s.writeStats);
  const history = useQueryStore((s) => s.history);

  const nodes = useGraphStore((s) => s.nodes);
  const selectedNode = useGraphStore((s) => s.selectedNode);
  const selectedEdge = useGraphStore((s) => s.selectedEdge);

  const [editorHeight, setEditorHeight] = useState(200);
  const [showHistory, setShowHistory] = useState(false);
  const [showSaved, setShowSaved] = useState(false);
  const [forceView, setForceView] = useState<'auto' | 'graph' | 'table'>('auto');
  const [scriptResult, setScriptResult] = useState<ScriptResult | null>(null);
  const [lastExecutedQuery, setLastExecutedQuery] = useState("");

  // Determine result type
  const hasGraphResult = nodes.length > 0;
  const hasTableResult = columns.length > 0 && records.length > 0;
  const isWriteQuery = /\b(CREATE|DELETE|SET|MERGE|REMOVE|DETACH)\b/.test(
    lastExecutedQuery.toUpperCase(),
  );
  const hasError = !!error;
  const hasNoResult =
    !hasGraphResult && !hasTableResult && !hasError && !isExecuting && !lastExecutedQuery;

  // Write result: show success message (only after actual execution)
  const writeSuccess = isWriteQuery && !hasError && records.length === 0 && columns.length === 0 && !isExecuting && nodes.length === 0 && lastExecutedQuery.length > 0;

  const handleRun = () => {
    setLastExecutedQuery(currentQuery);
    setScriptResult(null);
    executeQuery(currentQuery);
  };

  const handleUpload = () => {
    const input = document.createElement("input");
    input.type = "file";
    input.accept = ".cypher,.cql,.txt";
    input.onchange = async (e) => {
      const file = (e.target as HTMLInputElement).files?.[0];
      if (file) {
        const text = await file.text();
        const graph = useUiStore.getState().activeGraph;
        const result: ScriptResponse = await executeScript(text, graph);
        if (result.errors && result.errors.length > 0) {
          setScriptResult({ success: false, executed: result.executed, errors: result.errors });
        } else {
          setScriptResult({ success: true, executed: result.executed, errors: [] });
        }
        // Refresh schema and stats (don't load nodes into canvas)
        try {
          const { getSchema, getStatus } = await import("@/api/client");
          const graph = useUiStore.getState().activeGraph;
          const schema = await getSchema(graph);
          useUiStore.getState().setSchema(schema);
          const status = await getStatus(graph);
          useUiStore.getState().setServerInfo(status.version, status.storage.nodes, status.storage.edges);
        } catch { /* ignore refresh errors */ }
      }
    };
    input.click();
  };

  return (
    <div className="flex h-full flex-col">
      {/* Editor section */}
      <div
        style={{ height: editorHeight, minHeight: 120 }}
        className="shrink-0 border-b border-border"
      >
        <CypherEditor
          value={currentQuery}
          onChange={setQuery}
          onExecute={handleRun}
        />
      </div>

      {/* Resize handle */}
      <div
        className="h-1 cursor-row-resize bg-border/50 transition-colors hover:bg-primary/30"
        onMouseDown={(e) => {
          const startY = e.clientY;
          const startH = editorHeight;
          const onMove = (ev: MouseEvent) =>
            setEditorHeight(
              Math.max(80, Math.min(500, startH + ev.clientY - startY)),
            );
          const onUp = () => {
            document.removeEventListener("mousemove", onMove);
            document.removeEventListener("mouseup", onUp);
          };
          document.addEventListener("mousemove", onMove);
          document.addEventListener("mouseup", onUp);
        }}
      />

      {/* Toolbar */}
      <div className="flex items-center gap-2 border-b border-border px-3 py-1.5">
        <button
          onClick={handleRun}
          disabled={isExecuting || !currentQuery.trim()}
          className="flex items-center gap-1.5 rounded-md bg-primary px-3 py-1 text-xs font-medium text-primary-foreground transition-colors hover:bg-primary/90 disabled:opacity-50"
        >
          <Play className="h-3 w-3" />
          {isExecuting ? "Running..." : "Run"}
        </button>

        <button
          onClick={handleUpload}
          className="flex items-center gap-1 rounded-md px-2 py-1 text-xs text-muted-foreground transition-colors hover:bg-accent hover:text-foreground"
          title="Upload .cypher file"
        >
          <Upload className="h-3 w-3" />
          Upload
        </button>

        {history.length > 0 && !isExecuting && (records.length > 0 || columns.length > 0 || nodes.length > 0) && (
          <span className="text-xs text-muted-foreground">
            {history[0].rowCount} rows &bull; {history[0].duration < 1000 ? `${history[0].duration}ms` : `${(history[0].duration / 1000).toFixed(1)}s`}
          </span>
        )}

        {(hasGraphResult || hasTableResult) && !isExecuting && (
          <div className="flex items-center gap-0.5 rounded-md border border-border p-0.5">
            <button
              onClick={() => setForceView('auto')}
              className={cn("rounded px-2 py-0.5 text-[10px]", forceView === 'auto' ? "bg-accent text-foreground" : "text-muted-foreground")}
            >
              Auto
            </button>
            <button
              onClick={() => setForceView('graph')}
              className={cn("rounded px-2 py-0.5 text-[10px]", forceView === 'graph' ? "bg-accent text-foreground" : "text-muted-foreground")}
            >
              Graph
            </button>
            <button
              onClick={() => setForceView('table')}
              className={cn("rounded px-2 py-0.5 text-[10px]", forceView === 'table' ? "bg-accent text-foreground" : "text-muted-foreground")}
            >
              Table
            </button>
          </div>
        )}

        <div className="flex-1" />

        <button
          onClick={() => {
            setShowHistory(!showHistory);
            setShowSaved(false);
          }}
          className={cn(
            "flex items-center gap-1 rounded-md px-2 py-1 text-xs transition-colors",
            showHistory
              ? "bg-accent text-foreground"
              : "text-muted-foreground hover:bg-accent hover:text-foreground",
          )}
        >
          <History className="h-3 w-3" />
          History ({history.length})
        </button>

        <button
          onClick={() => {
            setShowSaved(!showSaved);
            setShowHistory(false);
          }}
          className={cn(
            "flex items-center gap-1 rounded-md px-2 py-1 text-xs transition-colors",
            showSaved
              ? "bg-accent text-foreground"
              : "text-muted-foreground hover:bg-accent hover:text-foreground",
          )}
        >
          <Bookmark className="h-3 w-3" />
          Saved
        </button>
      </div>

      {/* Results area */}
      <div className="flex min-h-0 flex-1 overflow-hidden">
        {/* Main result */}
        <div className="relative min-w-0 flex-1">
          {isExecuting && (
            <div className="flex h-full items-center justify-center">
              <div className="h-6 w-6 animate-spin rounded-full border-2 border-primary border-t-transparent" />
            </div>
          )}

          {hasError && !isExecuting && (
            <div className="flex h-full items-center justify-center p-8">
              <div className="max-w-lg rounded-lg border border-destructive/30 bg-destructive/5 p-6">
                <div className="flex items-start gap-3">
                  <AlertCircle className="mt-0.5 h-5 w-5 shrink-0 text-destructive" />
                  <div className="min-w-0">
                    <h3 className="mb-2 font-semibold text-destructive">Query Error</h3>
                    {(() => {
                      // Parse error message for better display
                      let msg = error || "";
                      try { const parsed = JSON.parse(msg); msg = parsed.error || msg; } catch { /* not JSON */ }

                      // Extract error type and details
                      const parseMatch = msg.match(/^(Parse error|Type error|Runtime error|Semantic error|Planning error|Variable not found|Constraint violation):\s*(.*)/s);
                      if (parseMatch) {
                        const [, errorType, details] = parseMatch;
                        return (
                          <div className="space-y-2">
                            <span className="inline-block rounded bg-destructive/10 px-2 py-0.5 text-xs font-medium text-destructive">{errorType}</span>
                            <pre className="whitespace-pre-wrap break-words rounded bg-background/50 p-3 font-mono text-xs text-foreground/80">{details.trim()}</pre>
                          </div>
                        );
                      }
                      return <pre className="whitespace-pre-wrap break-words rounded bg-background/50 p-3 font-mono text-xs text-foreground/80">{msg}</pre>;
                    })()}
                  </div>
                </div>
              </div>
            </div>
          )}

          {writeSuccess && !isExecuting && (
            <div className="flex h-full items-center justify-center p-8">
              <div className="max-w-sm rounded-lg border border-emerald-500/30 bg-emerald-500/5 p-6">
                <div className="flex items-start gap-3">
                  <Check className="mt-0.5 h-5 w-5 shrink-0 text-emerald-500" />
                  <div>
                    <h3 className="mb-2 font-semibold text-foreground">Query Executed Successfully</h3>
                {writeStats ? (
                  <div className="space-y-1.5 text-sm">
                    {writeStats.nodes_created > 0 && (
                      <div className="flex items-center gap-2">
                        <span className="inline-block h-2 w-2 rounded-full bg-emerald-500" />
                        <span className="text-foreground">Created <strong>{writeStats.nodes_created}</strong> node{writeStats.nodes_created !== 1 ? 's' : ''}</span>
                      </div>
                    )}
                    {writeStats.edges_created > 0 && (
                      <div className="flex items-center gap-2">
                        <span className="inline-block h-2 w-2 rounded-full bg-emerald-500" />
                        <span className="text-foreground">Created <strong>{writeStats.edges_created}</strong> relationship{writeStats.edges_created !== 1 ? 's' : ''}</span>
                      </div>
                    )}
                    {writeStats.nodes_deleted > 0 && (
                      <div className="flex items-center gap-2">
                        <span className="inline-block h-2 w-2 rounded-full bg-red-400" />
                        <span className="text-foreground">Deleted <strong>{writeStats.nodes_deleted}</strong> node{writeStats.nodes_deleted !== 1 ? 's' : ''}</span>
                      </div>
                    )}
                    {writeStats.edges_deleted > 0 && (
                      <div className="flex items-center gap-2">
                        <span className="inline-block h-2 w-2 rounded-full bg-red-400" />
                        <span className="text-foreground">Deleted <strong>{writeStats.edges_deleted}</strong> relationship{writeStats.edges_deleted !== 1 ? 's' : ''}</span>
                      </div>
                    )}
                    {writeStats.nodes_created === 0 && writeStats.edges_created === 0 && writeStats.nodes_deleted === 0 && writeStats.edges_deleted === 0 && (
                      <p className="text-muted-foreground">No changes made</p>
                    )}
                    <div className="mt-2 rounded bg-background/50 px-3 py-1.5 text-xs text-muted-foreground">
                      Database: <strong>{writeStats.total_nodes}</strong> nodes, <strong>{writeStats.total_edges}</strong> relationships
                    </div>
                  </div>
                ) : (
                  <p className="text-sm text-muted-foreground">
                    {useUiStore.getState().nodeCount} nodes,{" "}
                    {useUiStore.getState().edgeCount} edges in database
                  </p>
                )}
                  </div>
                </div>
              </div>
            </div>
          )}

          {(forceView === 'graph' || (forceView === 'auto' && hasGraphResult)) &&
            !writeSuccess &&
            !hasError &&
            !isExecuting &&
            !(forceView === 'auto' && !hasGraphResult) && <ForceGraph />}

          {(forceView === 'table' || (forceView === 'auto' && hasTableResult && !hasGraphResult)) &&
            !writeSuccess &&
            !hasError &&
            !isExecuting &&
            hasTableResult && (
              <div className="h-full overflow-auto p-2">
                <ResultsTable />
              </div>
            )}

          {scriptResult && !isExecuting && (
            <div className="absolute left-3 top-3 z-10 max-w-sm">
              <div
                className={cn(
                  "rounded-lg border p-4 shadow-md",
                  scriptResult.success
                    ? "border-emerald-500/30 bg-emerald-500/10"
                    : "border-destructive/30 bg-destructive/10",
                )}
              >
                <div className="mb-1 flex items-center justify-between gap-3">
                  <div className="flex items-center gap-2">
                    {scriptResult.success ? (
                      <Check className="h-4 w-4 text-emerald-500" />
                    ) : (
                      <AlertCircle className="h-4 w-4 text-destructive" />
                    )}
                    <span className="text-sm font-medium text-foreground">
                      {scriptResult.success
                        ? `Script executed: ${scriptResult.executed} statements`
                        : `Script partially failed: ${scriptResult.executed} succeeded`}
                    </span>
                  </div>
                  <button
                    onClick={() => setScriptResult(null)}
                    className="text-muted-foreground hover:text-foreground"
                  >
                    <X className="h-3.5 w-3.5" />
                  </button>
                </div>
                {scriptResult.errors.length > 0 && (
                  <ul className="mt-2 space-y-1">
                    {scriptResult.errors.map((err, i) => (
                      <li key={i} className="font-mono text-xs text-destructive/80">
                        {err}
                      </li>
                    ))}
                  </ul>
                )}
              </div>
            </div>
          )}

          {hasNoResult && !writeSuccess && (
            <div className="flex h-full items-center justify-center text-muted-foreground">
              <div className="text-center">
                <Terminal className="mx-auto mb-3 h-10 w-10 opacity-20" />
                <p className="text-sm">Run a query to see results</p>
                <p className="mt-1 text-xs opacity-60">
                  Ctrl+Enter to execute
                </p>
              </div>
            </div>
          )}

          {/* Floating property inspector when node or edge selected */}
          {(selectedNode || selectedEdge) && hasGraphResult && !isExecuting && (
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
        </div>

        {/* History/Saved sidebar (collapsible) */}
        {(showHistory || showSaved) && (
          <div className="w-64 shrink-0 overflow-auto border-l border-border bg-card">
            {showHistory && <HistoryPanel />}
            {showSaved && <SavedQueriesPanel />}
          </div>
        )}
      </div>
    </div>
  );
}

// History panel
function HistoryPanel() {
  const history = useQueryStore((s) => s.history);
  const setQuery = useQueryStore((s) => s.setQuery);
  const executeQuery = useQueryStore((s) => s.executeQuery);
  const clearHistory = useQueryStore((s) => s.clearHistory);

  return (
    <div className="p-2">
      <div className="mb-2 flex items-center justify-between">
        <span className="text-xs font-medium text-muted-foreground">
          History ({history.length})
        </span>
        {history.length > 0 && (
          <button
            onClick={clearHistory}
            className="text-[10px] text-muted-foreground hover:text-destructive"
          >
            Clear
          </button>
        )}
      </div>
      {history.length === 0 && (
        <p className="text-xs text-muted-foreground">No history yet</p>
      )}
      {history.map((entry, i) => (
        <div
          key={i}
          className="group mb-1 cursor-pointer rounded px-2 py-1.5 hover:bg-accent"
          onClick={() => setQuery(entry.query)}
        >
          <p className="truncate font-mono text-[11px] text-foreground">
            {entry.query}
          </p>
          <div className="mt-0.5 flex items-center gap-2">
            <span className="text-[10px] text-muted-foreground">
              {entry.rowCount} rows
            </span>
            <span className="text-[10px] text-muted-foreground">
              {entry.duration}ms
            </span>
            <button
              className="ml-auto text-[10px] text-primary opacity-0 group-hover:opacity-100"
              onClick={(e) => {
                e.stopPropagation();
                executeQuery(entry.query);
              }}
            >
              Run
            </button>
          </div>
        </div>
      ))}
    </div>
  );
}

// Saved queries panel
function SavedQueriesPanel() {
  return (
    <div className="p-2">
      <SavedQueries />
    </div>
  );
}
