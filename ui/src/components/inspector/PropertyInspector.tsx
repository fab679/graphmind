import { useState } from "react";
import { ChevronDown, ChevronRight, Circle, ArrowRight } from "lucide-react";
import { Badge } from "@/components/ui/badge";
import { useGraphStore } from "@/stores/graphStore";
import { getCustomColorForLabel } from "@/lib/colors";
import { cn } from "@/lib/utils";

function ExpandableJson({ data }: { data: unknown }) {
  const [expanded, setExpanded] = useState(false);
  const json = JSON.stringify(data, null, 2);

  return (
    <div>
      <button
        className="flex items-center gap-1 text-xs text-primary hover:text-primary/80"
        onClick={() => setExpanded(!expanded)}
      >
        {expanded ? (
          <ChevronDown className="h-3 w-3" />
        ) : (
          <ChevronRight className="h-3 w-3" />
        )}
        {Array.isArray(data)
          ? `[Array ${(data as unknown[]).length} items]`
          : "{Object}"}
      </button>
      {expanded && (
        <pre className="mt-1 overflow-x-auto rounded bg-muted/50 px-2 py-1 font-mono text-[10px] text-muted-foreground">
          {json}
        </pre>
      )}
    </div>
  );
}

function PropertyValue({ value }: { value: unknown }) {
  if (value === null || value === undefined) {
    return <span className="italic text-muted-foreground/50">null</span>;
  }

  if (typeof value === "boolean") {
    return <Badge variant={value ? "default" : "secondary"}>{String(value)}</Badge>;
  }

  if (typeof value === "number") {
    return <span className="font-mono tabular-nums">{value.toLocaleString()}</span>;
  }

  if (typeof value === "string") {
    return <span className="break-all">{value}</span>;
  }

  if (Array.isArray(value)) {
    return <ExpandableJson data={value} />;
  }

  if (typeof value === "object") {
    return <ExpandableJson data={value} />;
  }

  return <span>{String(value)}</span>;
}

function PropertyList({ properties }: { properties: Record<string, unknown> }) {
  const entries = Object.entries(properties);

  if (entries.length === 0) {
    return (
      <p className="px-3 py-2 text-xs text-muted-foreground">No properties</p>
    );
  }

  return (
    <dl className="divide-y divide-border/50">
      {entries.map(([key, value]) => (
        <div key={key} className="flex items-start gap-2 px-3 py-1.5">
          <dt className="shrink-0 text-xs font-medium text-muted-foreground">
            {key}
          </dt>
          <dd className="min-w-0 text-xs text-foreground">
            <PropertyValue value={value} />
          </dd>
        </div>
      ))}
    </dl>
  );
}

export function PropertyInspector() {
  const selectedNode = useGraphStore((s) => s.selectedNode);
  const selectedEdge = useGraphStore((s) => s.selectedEdge);

  if (!selectedNode && !selectedEdge) {
    return (
      <div className="flex h-full flex-col items-center justify-center gap-2 px-4 text-center">
        <Circle className="h-8 w-8 text-muted-foreground/30" />
        <p className="text-sm text-muted-foreground">
          Click a node or edge to inspect
        </p>
      </div>
    );
  }

  if (selectedNode) {
    return (
      <div className="flex flex-col gap-3 overflow-y-auto p-3">
        <div>
          <h3 className="mb-1.5 text-xs font-semibold uppercase tracking-wider text-muted-foreground">
            Node
          </h3>
          <div className="flex flex-wrap gap-1">
            {selectedNode.labels.map((label) => (
              <Badge
                key={label}
                className={cn("border-none text-white")}
                style={{ backgroundColor: getCustomColorForLabel(label) }}
              >
                {label}
              </Badge>
            ))}
          </div>
        </div>

        <div>
          <span className="text-[10px] text-muted-foreground">ID</span>
          <p className="font-mono text-xs text-foreground">{selectedNode.id}</p>
        </div>

        <div>
          <h3 className="mb-1 text-xs font-semibold uppercase tracking-wider text-muted-foreground">
            Properties
          </h3>
          <div className="rounded border border-border">
            <PropertyList properties={selectedNode.properties} />
          </div>
        </div>
      </div>
    );
  }

  if (selectedEdge) {
    return (
      <div className="flex flex-col gap-3 overflow-y-auto p-3">
        <div>
          <h3 className="mb-1.5 text-xs font-semibold uppercase tracking-wider text-muted-foreground">
            Edge
          </h3>
          <Badge variant="secondary">{selectedEdge.type}</Badge>
        </div>

        <div className="flex items-center gap-2 text-xs text-muted-foreground">
          <span className="font-mono">{selectedEdge.source}</span>
          <ArrowRight className="h-3 w-3" />
          <span className="font-mono">{selectedEdge.target}</span>
        </div>

        <div>
          <h3 className="mb-1 text-xs font-semibold uppercase tracking-wider text-muted-foreground">
            Properties
          </h3>
          <div className="rounded border border-border">
            <PropertyList properties={selectedEdge.properties} />
          </div>
        </div>
      </div>
    );
  }

  return null;
}
