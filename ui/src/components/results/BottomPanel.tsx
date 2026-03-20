import { ResultsTable } from "@/components/results/ResultsTable";
import { useQueryStore } from "@/stores/queryStore";

function QueryStats() {
  const columns = useQueryStore((s) => s.columns);
  const records = useQueryStore((s) => s.records);
  const history = useQueryStore((s) => s.history);

  const lastExecution = history[0];

  if (columns.length === 0) return null;

  return (
    <div className="flex items-center gap-3 border-t border-border px-3 py-1 text-[10px] text-muted-foreground">
      <span>{records.length} rows</span>
      {lastExecution && <span>{lastExecution.duration}ms</span>}
      <span className="truncate">
        Columns: {columns.join(", ")}
      </span>
    </div>
  );
}

export function BottomPanel() {
  return (
    <div className="flex h-full flex-col bg-background">
      <div className="flex-1 min-h-0">
        <ResultsTable />
      </div>
      <QueryStats />
    </div>
  );
}
