import { useMemo, useState } from "react";
import {
  ChevronDown,
  ChevronRight,
  Copy,
  Check,
  Database,
  Filter,
  GitBranch,
  Layers,
  Search,
  ArrowRightLeft,
  Combine,
  SortAsc,
  Hash,
  Workflow,
  Unplug,
  Pencil,
  Trash2,
  Plus,
  Merge,
  Repeat,
  BarChart3,
  Rows3,
  Timer,
} from "lucide-react";
import { cn } from "@/lib/utils";

interface PlanNode {
  name: string;
  details: string;
  children: PlanNode[];
}

interface ProfileInfo {
  rows: number;
  executionTime: string;
}

interface StatsEntry {
  label: string;
  value: string;
}

// Operator visual styling: color + icon by category
const OPERATOR_STYLES: Record<
  string,
  { bg: string; text: string; border: string; icon: React.ComponentType<{ className?: string }> }
> = {
  // Scan
  NodeScan:      { bg: "bg-blue-500/10",    text: "text-blue-600 dark:text-blue-400",    border: "border-blue-500/30",    icon: Search },
  IndexScan:     { bg: "bg-blue-500/10",    text: "text-blue-600 dark:text-blue-400",    border: "border-blue-500/30",    icon: Database },
  VectorSearch:  { bg: "bg-blue-500/10",    text: "text-blue-600 dark:text-blue-400",    border: "border-blue-500/30",    icon: Search },
  SingleRow:     { bg: "bg-slate-500/8",    text: "text-slate-500 dark:text-slate-400",  border: "border-slate-500/20",   icon: Database },
  // Filter/Project
  Filter:        { bg: "bg-amber-500/10",   text: "text-amber-600 dark:text-amber-400",  border: "border-amber-500/30",   icon: Filter },
  Project:       { bg: "bg-violet-500/10",  text: "text-violet-600 dark:text-violet-400", border: "border-violet-500/30", icon: Layers },
  Projection:    { bg: "bg-violet-500/10",  text: "text-violet-600 dark:text-violet-400", border: "border-violet-500/30", icon: Layers },
  // Traversal
  Expand:        { bg: "bg-emerald-500/10", text: "text-emerald-600 dark:text-emerald-400", border: "border-emerald-500/30", icon: GitBranch },
  ExpandInto:    { bg: "bg-emerald-500/10", text: "text-emerald-600 dark:text-emerald-400", border: "border-emerald-500/30", icon: GitBranch },
  VarLengthExpand: { bg: "bg-emerald-500/10", text: "text-emerald-600 dark:text-emerald-400", border: "border-emerald-500/30", icon: GitBranch },
  ShortestPath:  { bg: "bg-emerald-500/10", text: "text-emerald-600 dark:text-emerald-400", border: "border-emerald-500/30", icon: Workflow },
  // Join (BRANCHING operators)
  Join:              { bg: "bg-cyan-500/10",    text: "text-cyan-600 dark:text-cyan-400",    border: "border-cyan-500/30",    icon: Combine },
  LeftOuterJoin:     { bg: "bg-cyan-500/10",    text: "text-cyan-600 dark:text-cyan-400",    border: "border-cyan-500/30",    icon: Combine },
  CartesianProduct:  { bg: "bg-cyan-500/10",    text: "text-cyan-600 dark:text-cyan-400",    border: "border-cyan-500/30",    icon: ArrowRightLeft },
  // Aggregation/Sort
  Aggregate:     { bg: "bg-orange-500/10",  text: "text-orange-600 dark:text-orange-400", border: "border-orange-500/30", icon: BarChart3 },
  Sort:          { bg: "bg-orange-500/10",  text: "text-orange-600 dark:text-orange-400", border: "border-orange-500/30", icon: SortAsc },
  Limit:         { bg: "bg-orange-500/10",  text: "text-orange-600 dark:text-orange-400", border: "border-orange-500/30", icon: Hash },
  Skip:          { bg: "bg-orange-500/10",  text: "text-orange-600 dark:text-orange-400", border: "border-orange-500/30", icon: Hash },
  // Barrier
  WithBarrier:   { bg: "bg-indigo-500/10",  text: "text-indigo-600 dark:text-indigo-400", border: "border-indigo-500/30", icon: Unplug },
  Unwind:        { bg: "bg-indigo-500/10",  text: "text-indigo-600 dark:text-indigo-400", border: "border-indigo-500/30", icon: Repeat },
  // Write - Create
  CreateNode:          { bg: "bg-green-500/10",   text: "text-green-600 dark:text-green-400",   border: "border-green-500/30",   icon: Plus },
  CreateNodesAndEdges: { bg: "bg-green-500/10",   text: "text-green-600 dark:text-green-400",   border: "border-green-500/30",   icon: Plus },
  PerRowCreate:        { bg: "bg-green-500/10",   text: "text-green-600 dark:text-green-400",   border: "border-green-500/30",   icon: Plus },
  CreateEdge:          { bg: "bg-green-500/10",   text: "text-green-600 dark:text-green-400",   border: "border-green-500/30",   icon: Plus },
  MatchCreateEdge:     { bg: "bg-green-500/10",   text: "text-green-600 dark:text-green-400",   border: "border-green-500/30",   icon: Plus },
  // Write - Modify
  SetProperty:       { bg: "bg-yellow-500/10",  text: "text-yellow-600 dark:text-yellow-400", border: "border-yellow-500/30", icon: Pencil },
  Delete:            { bg: "bg-red-500/10",      text: "text-red-600 dark:text-red-400",       border: "border-red-500/30",     icon: Trash2 },
  DetachDelete:      { bg: "bg-red-500/10",      text: "text-red-600 dark:text-red-400",       border: "border-red-500/30",     icon: Trash2 },
  RemoveProperty:    { bg: "bg-red-500/10",      text: "text-red-600 dark:text-red-400",       border: "border-red-500/30",     icon: Trash2 },
  // Write - Merge
  Merge:           { bg: "bg-teal-500/10",    text: "text-teal-600 dark:text-teal-400",     border: "border-teal-500/30",    icon: Merge },
  PerRowMerge:     { bg: "bg-teal-500/10",    text: "text-teal-600 dark:text-teal-400",     border: "border-teal-500/30",    icon: Merge },
  // Misc
  Foreach:     { bg: "bg-pink-500/10",    text: "text-pink-600 dark:text-pink-400",     border: "border-pink-500/30",    icon: Repeat },
  Algorithm:   { bg: "bg-pink-500/10",    text: "text-pink-600 dark:text-pink-400",     border: "border-pink-500/30",    icon: Workflow },
};

const DEFAULT_STYLE = {
  bg: "bg-muted/50",
  text: "text-muted-foreground",
  border: "border-border",
  icon: Layers,
};

function getStyle(name: string) {
  if (OPERATOR_STYLES[name]) return OPERATOR_STYLES[name];
  for (const [key, style] of Object.entries(OPERATOR_STYLES)) {
    if (name.startsWith(key)) return style;
  }
  return DEFAULT_STYLE;
}

// --- Parsing ---

function parsePlanText(text: string): {
  tree: PlanNode | null;
  profile: ProfileInfo | null;
  stats: StatsEntry[];
} {
  const lines = text.split("\n");

  // Find section boundaries
  const profileIdx = lines.findIndex((l) => l.includes("--- Profile ---"));
  const statsIdx = lines.findIndex((l) => l.includes("--- Statistics ---"));

  const planEnd = profileIdx >= 0 ? profileIdx : statsIdx >= 0 ? statsIdx : lines.length;
  const planLines = lines.slice(0, planEnd);

  // Parse profile
  let profile: ProfileInfo | null = null;
  if (profileIdx >= 0) {
    const profileEnd = statsIdx >= 0 ? statsIdx : lines.length;
    for (const line of lines.slice(profileIdx, profileEnd)) {
      const m = line.match(/Rows:\s*(\d+),\s*Execution time:\s*([\d.]+)ms/);
      if (m) {
        profile = { rows: parseInt(m[1]), executionTime: m[2] };
      }
    }
  }

  // Parse stats
  const stats: StatsEntry[] = [];
  if (statsIdx >= 0) {
    for (const line of lines.slice(statsIdx + 1)) {
      const trimmed = line.trim();
      if (!trimmed || trimmed.startsWith("Graph Statistics")) continue;
      const m = trimmed.match(/^(.+?):\s+(.+)$/);
      if (m) stats.push({ label: m[1].trim(), value: m[2].trim() });
    }
  }

  // Parse operator tree
  const tree = parseOperatorTree(planLines);

  return { tree, profile, stats };
}

function parseOperatorTree(lines: string[]): PlanNode | null {
  // Each line: optional indent + optional "+- " prefix + "OperatorName" + optional " (details)"
  const entries: { depth: number; name: string; details: string }[] = [];

  for (const line of lines) {
    const trimmed = line.trimEnd();
    if (!trimmed) continue;

    // Calculate depth from leading whitespace and +- markers
    let depth = 0;
    let pos = 0;
    while (pos < trimmed.length && trimmed[pos] === " ") {
      pos++;
    }
    if (trimmed.substring(pos).startsWith("+- ")) {
      depth = Math.floor(pos / 3) + 1;
      pos += 3;
    }

    const content = trimmed.substring(pos);
    const opMatch = content.match(/^(\S+?)(?:\s+\((.+)\))?$/);
    if (!opMatch) continue;

    entries.push({
      depth,
      name: opMatch[1],
      details: opMatch[2] || "",
    });
  }

  if (entries.length === 0) return null;

  // Build tree using a stack
  const root: PlanNode = {
    name: entries[0].name,
    details: entries[0].details,
    children: [],
  };

  // Stack holds (node, depth) pairs
  const stack: { node: PlanNode; depth: number }[] = [{ node: root, depth: 0 }];

  for (let i = 1; i < entries.length; i++) {
    const { depth, name, details } = entries[i];
    const child: PlanNode = { name, details, children: [] };

    // Find parent: pop stack until we find a node at depth-1
    while (stack.length > 0 && stack[stack.length - 1].depth >= depth) {
      stack.pop();
    }

    if (stack.length > 0) {
      stack[stack.length - 1].node.children.push(child);
    }

    stack.push({ node: child, depth });
  }

  return root;
}

// Branching operators with 2+ children
const BRANCHING_OPS = new Set([
  "CartesianProduct",
  "Join",
  "LeftOuterJoin",
]);

// --- Rendering ---

function OperatorNode({
  node,
  isRoot = false,
  parentIsBranching = false,
  branchLabel,
}: {
  node: PlanNode;
  isRoot?: boolean;
  parentIsBranching?: boolean;
  branchLabel?: string;
}) {
  const [expanded, setExpanded] = useState(true);
  const style = getStyle(node.name);
  const Icon = style.icon;
  const hasChildren = node.children.length > 0;
  const isBranching = BRANCHING_OPS.has(node.name) && node.children.length >= 2;

  return (
    <div className={cn("relative", !isRoot && !parentIsBranching && "ml-6 mt-0.5")}>
      {/* Vertical + horizontal connector line for non-branching children */}
      {!isRoot && !parentIsBranching && (
        <div className="absolute -left-6 top-0 h-4 w-6 border-b-2 border-l-2 border-border/40 rounded-bl-lg" />
      )}

      {/* Branch label (Left/Right) */}
      {branchLabel && (
        <div className="mb-1 ml-1 text-[10px] font-medium uppercase tracking-wider text-muted-foreground/60">
          {branchLabel}
        </div>
      )}

      {/* Operator card */}
      <div
        className={cn(
          "group rounded-lg border transition-all",
          style.bg,
          style.border,
          "hover:shadow-sm",
        )}
      >
        <div
          className={cn(
            "flex items-center gap-2 px-3 py-1.5",
            hasChildren && "cursor-pointer",
          )}
          onClick={() => hasChildren && setExpanded(!expanded)}
        >
          {hasChildren ? (
            expanded ? (
              <ChevronDown className="h-3.5 w-3.5 shrink-0 text-muted-foreground/60" />
            ) : (
              <ChevronRight className="h-3.5 w-3.5 shrink-0 text-muted-foreground/60" />
            )
          ) : (
            <span className="w-3.5 shrink-0" />
          )}

          <Icon className={cn("h-4 w-4 shrink-0", style.text)} />

          <span className={cn("text-xs font-bold", style.text)}>
            {node.name}
          </span>

          {node.details && (
            <span className="min-w-0 truncate font-mono text-[11px] text-muted-foreground">
              {node.details}
            </span>
          )}

          {isBranching && (
            <span className="ml-auto shrink-0 rounded-full bg-cyan-500/15 px-1.5 py-0.5 text-[9px] font-semibold text-cyan-600 dark:text-cyan-400">
              {node.children.length} branches
            </span>
          )}
        </div>
      </div>

      {/* Children */}
      {hasChildren && expanded && (
        isBranching ? (
          // Branching layout: side-by-side columns
          <div className="mt-2 ml-2">
            <div className="flex gap-3">
              {node.children.map((child, i) => (
                <div
                  key={`${child.name}-${i}`}
                  className="min-w-0 flex-1 rounded-lg border border-dashed border-border/40 p-2"
                >
                  <OperatorNode
                    node={child}
                    parentIsBranching
                    branchLabel={
                      node.children.length === 2
                        ? i === 0
                          ? "left"
                          : "right"
                        : `branch ${i + 1}`
                    }
                  />
                </div>
              ))}
            </div>
          </div>
        ) : (
          // Linear layout: stacked children
          <div className="relative ml-3">
            {node.children.map((child, i) => (
              <OperatorNode
                key={`${child.name}-${i}`}
                node={child}
              />
            ))}
          </div>
        )
      )}
    </div>
  );
}

function formatTime(ms: string): string {
  const val = parseFloat(ms);
  if (val < 0.001) return `${(val * 1_000_000).toFixed(0)} ns`;
  if (val < 1) return `${(val * 1_000).toFixed(1)} \u00B5s`;
  if (val < 1000) return `${val.toFixed(1)} ms`;
  return `${(val / 1000).toFixed(2)} s`;
}

function countOps(node: PlanNode): number {
  return 1 + node.children.reduce((s, c) => s + countOps(c), 0);
}

export function ExplainPlan({ planText }: { planText: string }) {
  const { tree, profile, stats } = useMemo(() => parsePlanText(planText), [planText]);
  const [copied, setCopied] = useState(false);

  const handleCopy = async () => {
    await navigator.clipboard.writeText(planText);
    setCopied(true);
    setTimeout(() => setCopied(false), 2000);
  };

  if (!tree) {
    return (
      <div className="p-4 font-mono text-xs whitespace-pre-wrap text-muted-foreground">
        {planText}
      </div>
    );
  }

  const isProfile = profile !== null;

  return (
    <div className="flex h-full flex-col">
      {/* Header */}
      <div className="flex items-center gap-2 border-b border-border px-4 py-2">
        <Workflow className="h-4 w-4 text-primary" />
        <span className="text-sm font-semibold text-foreground">
          {isProfile ? "Profile" : "Execution Plan"}
        </span>
        <span className="rounded-full bg-muted px-2 py-0.5 text-[10px] text-muted-foreground">
          {countOps(tree)} operators
        </span>

        {/* Profile stats */}
        {profile && (
          <>
            <span className="mx-1 text-border">|</span>
            <Rows3 className="h-3.5 w-3.5 text-muted-foreground" />
            <span className="text-xs text-muted-foreground">
              <strong className="text-foreground">{profile.rows.toLocaleString()}</strong> rows
            </span>
            <Timer className="h-3.5 w-3.5 text-muted-foreground" />
            <span className="text-xs text-muted-foreground">
              <strong className="text-foreground">{formatTime(profile.executionTime)}</strong>
            </span>
          </>
        )}

        <div className="flex-1" />
        <button
          onClick={handleCopy}
          className="flex items-center gap-1 rounded-md px-2 py-1 text-xs text-muted-foreground transition-colors hover:bg-accent hover:text-foreground"
          title="Copy plan text"
        >
          {copied ? (
            <>
              <Check className="h-3 w-3 text-emerald-500" />
              <span className="text-emerald-500">Copied</span>
            </>
          ) : (
            <>
              <Copy className="h-3 w-3" />
              Copy
            </>
          )}
        </button>
      </div>

      {/* Plan tree */}
      <div className="flex-1 overflow-auto p-4">
        <OperatorNode node={tree} isRoot />
      </div>

      {/* Statistics footer */}
      {stats.length > 0 && (
        <div className="shrink-0 border-t border-border bg-muted/30 px-4 py-2">
          <div className="mb-1 flex items-center gap-1.5 text-[10px] font-semibold uppercase tracking-wider text-muted-foreground">
            <BarChart3 className="h-3 w-3" />
            Graph Statistics
          </div>
          <div className="grid grid-cols-2 gap-x-6 gap-y-0.5 sm:grid-cols-3 lg:grid-cols-4">
            {stats.map((s) => (
              <div key={s.label} className="flex items-baseline gap-1.5 text-[11px]">
                <span className="text-muted-foreground">{s.label}:</span>
                <span className="font-mono font-medium text-foreground">{s.value}</span>
              </div>
            ))}
          </div>
        </div>
      )}
    </div>
  );
}
