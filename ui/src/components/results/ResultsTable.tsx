import { useMemo } from "react";
import {
  useReactTable,
  getCoreRowModel,
  flexRender,
  createColumnHelper,
} from "@tanstack/react-table";
import { Badge } from "@/components/ui/badge";
import { useQueryStore } from "@/stores/queryStore";
import { cn } from "@/lib/utils";

function CellValue({ value }: { value: unknown }) {
  if (value === null || value === undefined) {
    return <span className="italic text-muted-foreground/50">null</span>;
  }

  if (typeof value === "boolean") {
    return (
      <Badge variant={value ? "default" : "secondary"}>
        {String(value)}
      </Badge>
    );
  }

  if (typeof value === "number") {
    return <span className="font-mono tabular-nums">{value.toLocaleString()}</span>;
  }

  if (typeof value === "string") {
    return <span className="break-all">{value}</span>;
  }

  if (typeof value === "object") {
    const obj = value as Record<string, unknown>;

    // Node-like objects with id and labels
    if ("id" in obj && "labels" in obj && Array.isArray(obj.labels)) {
      return (
        <span className="inline-flex items-center gap-1">
          {(obj.labels as string[]).map((label) => (
            <Badge key={label} variant="default">
              {label}
            </Badge>
          ))}
          <span className="font-mono text-muted-foreground">
            #{String(obj.id)}
          </span>
        </span>
      );
    }

    // Fallback: JSON
    return (
      <span className="font-mono text-xs text-muted-foreground">
        {JSON.stringify(value)}
      </span>
    );
  }

  return <span>{String(value)}</span>;
}

const columnHelper = createColumnHelper<unknown[]>();

export function ResultsTable() {
  const columns = useQueryStore((s) => s.columns);
  const records = useQueryStore((s) => s.records);

  const tableColumns = useMemo(
    () =>
      columns.map((col, idx) =>
        columnHelper.display({
          id: col,
          header: col,
          cell: (info) => <CellValue value={info.row.original[idx]} />,
        })
      ),
    [columns]
  );

  const table = useReactTable({
    data: records,
    columns: tableColumns,
    getCoreRowModel: getCoreRowModel(),
  });

  if (columns.length === 0) {
    return (
      <div className="flex h-full items-center justify-center text-sm text-muted-foreground">
        No results
      </div>
    );
  }

  return (
    <div className="flex h-full flex-col">
      <div className="flex-1 overflow-auto">
        <table className="w-full border-collapse text-sm">
          <thead className="sticky top-0 z-10">
            {table.getHeaderGroups().map((headerGroup) => (
              <tr key={headerGroup.id} className="border-b border-border bg-muted/50">
                {headerGroup.headers.map((header) => (
                  <th
                    key={header.id}
                    className="px-3 py-1.5 text-left text-xs font-semibold text-muted-foreground"
                  >
                    {flexRender(header.column.columnDef.header, header.getContext())}
                  </th>
                ))}
              </tr>
            ))}
          </thead>
          <tbody>
            {table.getRowModel().rows.map((row) => (
              <tr
                key={row.id}
                className={cn(
                  "border-b border-border/50 transition-colors hover:bg-accent/30"
                )}
              >
                {row.getVisibleCells().map((cell) => (
                  <td key={cell.id} className="px-3 py-1.5 text-xs">
                    {flexRender(cell.column.columnDef.cell, cell.getContext())}
                  </td>
                ))}
              </tr>
            ))}
          </tbody>
        </table>
      </div>

      <div className="shrink-0 border-t border-border px-3 py-1 text-[10px] text-muted-foreground">
        {records.length} row{records.length !== 1 ? "s" : ""}
      </div>
    </div>
  );
}
