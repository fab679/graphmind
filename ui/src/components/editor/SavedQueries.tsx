import { useState } from "react";
import { Trash2, Save } from "lucide-react";
import { Button } from "@/components/ui/button";
import { useQueryStore } from "@/stores/queryStore";
import { cn } from "@/lib/utils";

function formatDate(timestamp: number): string {
  const date = new Date(timestamp);
  return date.toLocaleDateString(undefined, {
    month: "short",
    day: "numeric",
    year: date.getFullYear() !== new Date().getFullYear() ? "numeric" : undefined,
  });
}

export function SavedQueries() {
  const savedQueries = useQueryStore((s) => s.savedQueries);
  const saveQuery = useQueryStore((s) => s.saveQuery);
  const deleteSavedQuery = useQueryStore((s) => s.deleteSavedQuery);
  const currentQuery = useQueryStore((s) => s.currentQuery);
  const setQuery = useQueryStore((s) => s.setQuery);

  const [saveName, setSaveName] = useState("");
  const [showSaveInput, setShowSaveInput] = useState(false);
  const [confirmDeleteId, setConfirmDeleteId] = useState<string | null>(null);

  const handleSave = () => {
    const name = saveName.trim();
    if (!name || !currentQuery.trim()) return;
    saveQuery(name, currentQuery);
    setSaveName("");
    setShowSaveInput(false);
  };

  return (
    <div className="flex flex-col gap-2">
      {/* Save current query */}
      {showSaveInput ? (
        <div className="flex gap-1">
          <input
            type="text"
            className="flex-1 rounded border border-border bg-background px-2 py-1 text-xs text-foreground placeholder:text-muted-foreground focus:outline-none focus:ring-1 focus:ring-ring"
            placeholder="Query name..."
            value={saveName}
            onChange={(e) => setSaveName(e.target.value)}
            onKeyDown={(e) => {
              if (e.key === "Enter") handleSave();
              if (e.key === "Escape") {
                setShowSaveInput(false);
                setSaveName("");
              }
            }}
            autoFocus
          />
          <Button
            size="sm"
            variant="default"
            disabled={!saveName.trim() || !currentQuery.trim()}
            onClick={handleSave}
          >
            Save
          </Button>
          <Button
            size="sm"
            variant="ghost"
            onClick={() => {
              setShowSaveInput(false);
              setSaveName("");
            }}
          >
            Cancel
          </Button>
        </div>
      ) : (
        <Button
          size="sm"
          variant="outline"
          className="w-full"
          disabled={!currentQuery.trim()}
          onClick={() => setShowSaveInput(true)}
        >
          <Save className="h-3.5 w-3.5" />
          Save Current Query
        </Button>
      )}

      {/* Saved queries list */}
      {savedQueries.length === 0 ? (
        <p className="py-8 text-center text-xs text-muted-foreground">
          No saved queries yet
        </p>
      ) : (
        <div className="flex flex-col gap-0.5 overflow-y-auto">
          {savedQueries.map((sq) => (
            <div
              key={sq.id}
              className={cn(
                "group flex items-start gap-1 rounded px-2 py-1.5 transition-colors hover:bg-accent",
                "cursor-pointer"
              )}
              onClick={() => setQuery(sq.query)}
            >
              <div className="min-w-0 flex-1">
                <p className="truncate text-xs font-semibold text-foreground">
                  {sq.name}
                </p>
                <p className="mt-0.5 truncate font-mono text-[10px] text-muted-foreground">
                  {sq.query}
                </p>
                <p className="mt-0.5 text-[10px] text-muted-foreground/60">
                  {formatDate(sq.createdAt)}
                </p>
              </div>
              {confirmDeleteId === sq.id ? (
                <div className="flex shrink-0 gap-0.5">
                  <Button
                    size="sm"
                    variant="destructive"
                    className="h-5 px-1.5 text-[10px]"
                    onClick={(e) => {
                      e.stopPropagation();
                      deleteSavedQuery(sq.id);
                      setConfirmDeleteId(null);
                    }}
                  >
                    Delete
                  </Button>
                  <Button
                    size="sm"
                    variant="ghost"
                    className="h-5 px-1.5 text-[10px]"
                    onClick={(e) => {
                      e.stopPropagation();
                      setConfirmDeleteId(null);
                    }}
                  >
                    Cancel
                  </Button>
                </div>
              ) : (
                <button
                  className="hidden shrink-0 rounded p-0.5 text-muted-foreground hover:text-destructive group-hover:block"
                  onClick={(e) => {
                    e.stopPropagation();
                    setConfirmDeleteId(sq.id);
                  }}
                  title="Delete saved query"
                >
                  <Trash2 className="h-3.5 w-3.5" />
                </button>
              )}
            </div>
          ))}
        </div>
      )}
    </div>
  );
}
