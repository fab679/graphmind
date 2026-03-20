import { useState, useRef, useCallback } from "react";
import { Play, AlertCircle, Clock, Upload, CheckCircle, SkipBack, SkipForward, Trash2 } from "lucide-react";
import { Tabs, TabsList, TabsTrigger, TabsContent } from "@/components/ui/tabs";
import { Button } from "@/components/ui/button";
import { Badge } from "@/components/ui/badge";
import { CypherEditor } from "@/components/editor/CypherEditor";
import { SavedQueries } from "@/components/editor/SavedQueries";
import { useQueryStore } from "@/stores/queryStore";
import type { HistoryEntry } from "@/stores/queryStore";
import { executeScript, translateNlq } from "@/api/client";
import { getSchema } from "@/api/client";
import { useUiStore } from "@/stores/uiStore";
import { cn } from "@/lib/utils";

function formatRelativeTime(timestamp: number): string {
  const diff = Date.now() - timestamp;
  const seconds = Math.floor(diff / 1000);

  if (seconds < 60) return `${seconds}s ago`;
  const minutes = Math.floor(seconds / 60);
  if (minutes < 60) return `${minutes}m ago`;
  const hours = Math.floor(minutes / 60);
  if (hours < 24) return `${hours}h ago`;
  const days = Math.floor(hours / 24);
  return `${days}d ago`;
}

function HistoryItem({ entry }: { entry: HistoryEntry }) {
  const setQuery = useQueryStore((s) => s.setQuery);
  const executeQuery = useQueryStore((s) => s.executeQuery);
  const deleteHistoryEntry = useQueryStore((s) => s.deleteHistoryEntry);

  return (
    <div className="group relative w-full rounded px-2 py-1.5 transition-colors hover:bg-accent">
      <button
        className="w-full text-left"
        onClick={() => setQuery(entry.query)}
      >
        <p className="truncate font-mono text-xs text-foreground pr-14">
          {entry.query}
        </p>
        <div className="mt-0.5 flex items-center gap-2 text-[10px] text-muted-foreground">
          <span className="flex items-center gap-0.5">
            <Clock className="h-2.5 w-2.5" />
            {formatRelativeTime(entry.timestamp)}
          </span>
          <span>{entry.rowCount} rows</span>
          <span>{entry.duration}ms</span>
        </div>
      </button>

      {/* Action buttons - visible on hover */}
      <div className="absolute right-1.5 top-1/2 -translate-y-1/2 flex items-center gap-0.5 opacity-0 group-hover:opacity-100 transition-opacity">
        <button
          className="rounded p-1 hover:bg-primary/20 text-primary"
          onClick={(e) => { e.stopPropagation(); executeQuery(entry.query); }}
          title="Re-run this query"
        >
          <Play className="h-3 w-3" />
        </button>
        <button
          className="rounded p-1 hover:bg-destructive/20 text-muted-foreground hover:text-destructive"
          onClick={(e) => { e.stopPropagation(); deleteHistoryEntry(entry.timestamp); }}
          title="Delete from history"
        >
          <Trash2 className="h-3 w-3" />
        </button>
      </div>
    </div>
  );
}

function QueryTimeline() {
  const history = useQueryStore((s) => s.history);
  const executeQuery = useQueryStore((s) => s.executeQuery);
  const [currentIndex, setCurrentIndex] = useState(0);

  if (history.length < 2) return null;

  // history is newest-first; slider left = oldest, right = newest
  const sliderValue = history.length - 1 - currentIndex;

  return (
    <div className="border-t border-border p-2">
      <div className="flex items-center gap-2 text-xs text-muted-foreground mb-1">
        <Clock className="h-3 w-3" />
        <span>Query Timeline</span>
        <span className="ml-auto tabular-nums">{currentIndex + 1} / {history.length}</span>
      </div>
      <div className="flex items-center gap-1">
        <button
          className="rounded p-0.5 text-muted-foreground hover:text-foreground disabled:opacity-30"
          onClick={() => {
            const i = Math.min(currentIndex + 1, history.length - 1);
            setCurrentIndex(i);
            executeQuery(history[i].query);
          }}
          disabled={currentIndex >= history.length - 1}
        >
          <SkipBack className="h-3.5 w-3.5" />
        </button>
        <input
          type="range"
          min={0}
          max={history.length - 1}
          value={sliderValue}
          onChange={(e) => {
            const i = history.length - 1 - Number(e.target.value);
            setCurrentIndex(i);
            executeQuery(history[i].query);
          }}
          className="flex-1"
        />
        <button
          className="rounded p-0.5 text-muted-foreground hover:text-foreground disabled:opacity-30"
          onClick={() => {
            const i = Math.max(currentIndex - 1, 0);
            setCurrentIndex(i);
            executeQuery(history[i].query);
          }}
          disabled={currentIndex <= 0}
        >
          <SkipForward className="h-3.5 w-3.5" />
        </button>
      </div>
      <p className="mt-1 truncate font-mono text-[10px] text-muted-foreground">
        {history[currentIndex]?.query}
      </p>
    </div>
  );
}

function ScriptLoader() {
  const fileRef = useRef<HTMLInputElement>(null);
  const [status, setStatus] = useState<{ type: "idle" | "loading" | "success" | "error"; message?: string }>({ type: "idle" });

  const handleFile = async (file: File) => {
    setStatus({ type: "loading", message: `Running ${file.name}...` });
    try {
      const text = await file.text();
      const graph = useUiStore.getState().activeGraph;
      const result = await executeScript(text, graph);
      if (result.errors.length > 0) {
        setStatus({ type: "error", message: `${result.executed} OK, ${result.errors.length} failed: ${result.errors[0]}` });
      } else {
        setStatus({ type: "success", message: `${result.executed} statements executed (${result.storage.nodes} nodes, ${result.storage.edges} edges)` });
      }
      // Refresh schema
      getSchema().then(schema => useUiStore.getState().setSchema(schema)).catch(() => {});
    } catch (e) {
      setStatus({ type: "error", message: e instanceof Error ? e.message : "Script failed" });
    }
    // Reset file input
    if (fileRef.current) fileRef.current.value = "";
    // Clear status after 5s
    setTimeout(() => setStatus({ type: "idle" }), 5000);
  };

  return (
    <>
      <input
        ref={fileRef}
        type="file"
        accept=".cypher,.cql,.txt"
        className="hidden"
        onChange={(e) => {
          const file = e.target.files?.[0];
          if (file) handleFile(file);
        }}
      />
      <Button
        size="sm"
        variant="outline"
        disabled={status.type === "loading"}
        onClick={() => fileRef.current?.click()}
        title="Load .cypher script file"
      >
        {status.type === "loading" ? (
          <span className="h-3.5 w-3.5 animate-spin rounded-full border-2 border-current border-t-transparent" />
        ) : status.type === "success" ? (
          <CheckCircle className="h-3.5 w-3.5 text-emerald-500" />
        ) : (
          <Upload className="h-3.5 w-3.5" />
        )}
      </Button>
      {status.message && (
        <div className={cn(
          "mt-1 rounded px-2 py-1 text-[10px]",
          status.type === "success" ? "bg-emerald-500/10 text-emerald-500" :
          status.type === "error" ? "bg-destructive/10 text-destructive" :
          "bg-muted text-muted-foreground"
        )}>
          {status.message}
        </div>
      )}
    </>
  );
}

/**
 * Simple NLQ-to-Cypher translator for common patterns.
 * For complex queries, users need to configure the backend NLQ pipeline with an LLM API key.
 */
function nlqToCypher(text: string): string | null {
  const t = text.trim().toLowerCase();

  // "show all X" / "get all X" / "find all X" / "list X"
  const allMatch = t.match(/^(?:show|get|find|list|display)\s+(?:all\s+)?(\w+)s?$/i);
  if (allMatch) {
    const label = allMatch[1].charAt(0).toUpperCase() + allMatch[1].slice(1);
    return `MATCH (n:${label}) RETURN n`;
  }

  // "who are X friends" / "X's friends" / "friends of X"
  const friendsMatch = t.match(/(?:who are|show|find|get)\s+(\w+(?:\s+\w+)?)'?s?\s+friends/i)
    || t.match(/friends\s+of\s+(\w+(?:\s+\w+)?)/i);
  if (friendsMatch) {
    const name = friendsMatch[1].split(/\s+/).map((w: string) => w.charAt(0).toUpperCase() + w.slice(1)).join(" ");
    return `MATCH (a:Person {name: '${name}'})-[:FRIENDS_WITH|KNOWS]-(b) RETURN a, b`;
  }

  // "how many X" / "count X"
  const countMatch = t.match(/^(?:how many|count)\s+(\w+)s?$/i);
  if (countMatch) {
    const label = countMatch[1].charAt(0).toUpperCase() + countMatch[1].slice(1);
    return `MATCH (n:${label}) RETURN count(n) AS count`;
  }

  // "who lives in X" / "people in X"
  const livesMatch = t.match(/(?:who\s+lives?\s+in|people\s+in|residents\s+of)\s+(.+)/i);
  if (livesMatch) {
    const city = livesMatch[1].split(/\s+/).map((w: string) => w.charAt(0).toUpperCase() + w.slice(1)).join(" ");
    return `MATCH (p:Person)-[:LIVES_IN]->(c:City {name: '${city}'}) RETURN p.name, c.name`;
  }

  // "who works at X"
  const worksMatch = t.match(/who\s+works?\s+at\s+(.+)/i);
  if (worksMatch) {
    const company = worksMatch[1].split(/\s+/).map((w: string) => w.charAt(0).toUpperCase() + w.slice(1)).join(" ");
    return `MATCH (p:Person)-[:WORKS_AT]->(c:Company {name: '${company}'}) RETURN p.name, c.name`;
  }

  // "show everything" / "show graph" / "show all"
  if (/^(?:show|display|get)\s+(?:everything|graph|all)$/i.test(t)) {
    return "MATCH (n) RETURN n";
  }

  // "shortest path from X to Y" / "path between X and Y"
  const pathMatch = t.match(/(?:shortest\s+)?path\s+(?:from|between)\s+(\w+(?:\s+\w+)?)\s+(?:to|and)\s+(\w+(?:\s+\w+)?)/i);
  if (pathMatch) {
    const from = pathMatch[1].split(/\s+/).map((w: string) => w.charAt(0).toUpperCase() + w.slice(1)).join(" ");
    const to = pathMatch[2].split(/\s+/).map((w: string) => w.charAt(0).toUpperCase() + w.slice(1)).join(" ");
    return `MATCH (a {name: '${from}'}), (b {name: '${to}'}), p = shortestPath((a)-[*]-(b)) RETURN p`;
  }

  return null;
}

export function LeftPanel() {
  const currentQuery = useQueryStore((s) => s.currentQuery);
  const setQuery = useQueryStore((s) => s.setQuery);
  const executeQuery = useQueryStore((s) => s.executeQuery);
  const isExecuting = useQueryStore((s) => s.isExecuting);
  const error = useQueryStore((s) => s.error);
  const history = useQueryStore((s) => s.history);
  const clearHistory = useQueryStore((s) => s.clearHistory);

  const [nlqMode, setNlqMode] = useState(false);

  const handleExecute = useCallback(async () => {
    if (nlqMode) {
      // Try backend NLQ first
      useQueryStore.setState({ isExecuting: true, error: null });
      try {
        const graph = useUiStore.getState().activeGraph;
        const nlqResult = await translateNlq(currentQuery, graph);
        if (nlqResult.error) {
          // Backend NLQ not available — fall back to client-side
          const cypher = nlqToCypher(currentQuery);
          if (cypher) {
            setQuery(cypher);
            executeQuery(cypher);
          } else {
            useQueryStore.setState({
              isExecuting: false,
              error: nlqResult.error + "\n\nClient-side fallback also failed. Try patterns like:\n- \"Show all persons\"\n- \"Who are Alice's friends\"\n- \"Who lives in San Francisco\"",
            });
          }
          return;
        }
        if (nlqResult.cypher) {
          // Show the generated Cypher in the editor and execute it
          setQuery(nlqResult.cypher);
          executeQuery(nlqResult.cypher);
        }
      } catch {
        // API call failed — fall back to client-side
        const cypher = nlqToCypher(currentQuery);
        if (cypher) {
          setQuery(cypher);
          executeQuery(cypher);
        } else {
          useQueryStore.setState({
            isExecuting: false,
            error: "NLQ backend unavailable. Try patterns like:\n- \"Show all persons\"\n- \"Who are Alice's friends\"",
          });
        }
      }
    } else {
      executeQuery();
    }
  }, [nlqMode, currentQuery, setQuery, executeQuery]);

  return (
    <div className="flex h-full flex-col bg-background">
      <Tabs defaultValue="editor" className="flex h-full flex-col">
        <TabsList>
          <TabsTrigger value="editor">Editor</TabsTrigger>
          <TabsTrigger value="history">History</TabsTrigger>
          <TabsTrigger value="saved">Saved</TabsTrigger>
        </TabsList>

        <TabsContent value="editor" className="flex flex-1 flex-col gap-2 p-2">
          {/* NLQ toggle */}
          <div className="flex items-center justify-between">
            <button
              className={cn(
                "flex items-center gap-1.5 rounded-full border px-2.5 py-0.5 text-[10px] font-medium transition-colors",
                nlqMode
                  ? "border-primary bg-primary/10 text-primary"
                  : "border-border text-muted-foreground hover:text-foreground"
              )}
              onClick={() => setNlqMode(!nlqMode)}
            >
              <span
                className={cn(
                  "inline-block h-2 w-2 rounded-full transition-colors",
                  nlqMode ? "bg-primary" : "bg-muted-foreground/40"
                )}
              />
              NLQ
            </button>
            {nlqMode && (
              <Badge variant="secondary" className="text-[10px]">
                NLQ Mode
              </Badge>
            )}
          </div>

          <div className="flex-1 min-h-0">
            <CypherEditor
              value={currentQuery}
              onChange={setQuery}
              onExecute={() => handleExecute()}
              placeholder={
                nlqMode
                  ? "Ask in plain English..."
                  : "Enter a Cypher query... (Ctrl+Enter to run)"
              }
            />
          </div>

          <div className="flex gap-1.5">
            <Button
              size="sm"
              className="flex-1"
              disabled={isExecuting || !currentQuery.trim()}
              onClick={() => handleExecute()}
            >
              <Play className="h-3.5 w-3.5" />
              {isExecuting ? "Running..." : nlqMode ? "Ask" : "Run Query"}
            </Button>

            <ScriptLoader />
          </div>

          {error && (
            <div
              className={cn(
                "flex items-start gap-2 rounded border border-destructive/30 bg-destructive/10 px-3 py-2 text-xs text-destructive"
              )}
            >
              <AlertCircle className="mt-0.5 h-3.5 w-3.5 shrink-0" />
              <span className="break-all">{error}</span>
            </div>
          )}
        </TabsContent>

        <TabsContent value="history" className="flex flex-1 flex-col p-2">
          {history.length === 0 ? (
            <p className="py-8 text-center text-xs text-muted-foreground">
              No query history yet
            </p>
          ) : (
            <>
              <div className="flex items-center justify-between mb-2">
                <span className="text-[10px] text-muted-foreground">{history.length} queries</span>
                <button
                  className="flex items-center gap-1 text-[10px] text-muted-foreground hover:text-destructive transition-colors"
                  onClick={() => clearHistory()}
                >
                  <Trash2 className="h-3 w-3" />
                  Clear
                </button>
              </div>
              <div className="flex flex-1 flex-col gap-0.5 overflow-y-auto">
                {history.map((entry, idx) => (
                  <HistoryItem key={`${entry.timestamp}-${idx}`} entry={entry} />
                ))}
              </div>
              <QueryTimeline />
            </>
          )}
        </TabsContent>

        <TabsContent value="saved" className="flex-1 overflow-y-auto p-2">
          <SavedQueries />
        </TabsContent>
      </Tabs>
    </div>
  );
}
