import { useState } from "react";
import { Database, Plus, Trash2, ChevronDown } from "lucide-react";
import { useUiStore } from "@/stores/uiStore";
import { useGraphStore } from "@/stores/graphStore";
import { listGraphs, deleteGraph, getSchema } from "@/api/client";
import { cn } from "@/lib/utils";

export function GraphSelector() {
  const activeGraph = useUiStore((s) => s.activeGraph);
  const availableGraphs = useUiStore((s) => s.availableGraphs);
  const setActiveGraph = useUiStore((s) => s.setActiveGraph);
  const setAvailableGraphs = useUiStore((s) => s.setAvailableGraphs);

  const [open, setOpen] = useState(false);
  const [newGraphName, setNewGraphName] = useState("");

  const handleRefresh = async () => {
    try {
      const graphs = await listGraphs();
      setAvailableGraphs(graphs);
    } catch {
      /* ignore */
    }
  };

  const handleSelect = async (graph: string) => {
    setActiveGraph(graph);
    setOpen(false);

    // Clear canvas for new tenant
    useGraphStore.getState().setGraphData([], []);
    useGraphStore.getState().selectNode(null);

    // Refresh schema for new tenant
    try {
      const schema = await getSchema();
      useUiStore.getState().setSchema(schema);
    } catch {
      /* ignore */
    }
  };

  const handleCreate = () => {
    if (newGraphName.trim() && !availableGraphs.includes(newGraphName.trim())) {
      const name = newGraphName.trim();
      setAvailableGraphs([...availableGraphs, name]);
      setActiveGraph(name);
      setNewGraphName("");
      setOpen(false);
    }
  };

  const handleDelete = async (name: string) => {
    if (name === "default") return;
    try {
      await deleteGraph(name);
      setAvailableGraphs(availableGraphs.filter((g) => g !== name));
      if (activeGraph === name) setActiveGraph("default");
    } catch {
      /* ignore */
    }
  };

  return (
    <div className="relative">
      <button
        onClick={() => {
          setOpen(!open);
          if (!open) handleRefresh();
        }}
        className="flex items-center gap-1.5 rounded-md border border-border bg-muted px-2 py-1 text-xs hover:bg-accent transition-colors"
        title="Switch graph/tenant"
      >
        <Database className="h-3 w-3 text-primary" />
        <span className="max-w-[80px] truncate font-medium">
          {activeGraph}
        </span>
        <ChevronDown className="h-3 w-3 text-muted-foreground" />
      </button>

      {open && (
        <div className="absolute left-0 top-full mt-1 z-50 w-56 rounded-lg border bg-popover shadow-lg">
          <div className="p-2">
            <div className="text-[10px] font-semibold text-muted-foreground uppercase tracking-wider mb-1 px-1">
              Graphs
            </div>
            <div className="max-h-[200px] overflow-y-auto">
              {availableGraphs.map((graph) => (
                <div
                  key={graph}
                  className={cn(
                    "group flex items-center justify-between rounded px-2 py-1 text-xs cursor-pointer",
                    graph === activeGraph
                      ? "bg-primary/10 text-primary"
                      : "hover:bg-accent",
                  )}
                >
                  <button
                    className="flex-1 text-left"
                    onClick={() => handleSelect(graph)}
                  >
                    {graph}
                  </button>
                  {graph !== "default" && (
                    <button
                      className="ml-1 p-0.5 text-muted-foreground hover:text-destructive opacity-0 group-hover:opacity-100"
                      onClick={(e) => {
                        e.stopPropagation();
                        handleDelete(graph);
                      }}
                      title="Delete graph"
                    >
                      <Trash2 className="h-3 w-3" />
                    </button>
                  )}
                </div>
              ))}
            </div>

            <div className="border-t border-border mt-1 pt-1">
              <div className="flex gap-1">
                <input
                  type="text"
                  value={newGraphName}
                  onChange={(e) => setNewGraphName(e.target.value)}
                  onKeyDown={(e) => e.key === "Enter" && handleCreate()}
                  placeholder="New graph name..."
                  className="flex-1 rounded border bg-input px-2 py-1 text-xs placeholder:text-muted-foreground focus:outline-none focus:ring-1 focus:ring-ring"
                />
                <button
                  onClick={handleCreate}
                  disabled={!newGraphName.trim()}
                  className="rounded bg-primary p-1 text-primary-foreground disabled:opacity-50"
                  title="Create new graph"
                >
                  <Plus className="h-3 w-3" />
                </button>
              </div>
            </div>
          </div>
        </div>
      )}
    </div>
  );
}
