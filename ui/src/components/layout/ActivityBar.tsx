import { useState, useRef, useEffect } from "react";
import {
  Terminal,
  Compass,
  MessageSquare,
  Database,
  Shield,
  Settings,
  Sun,
  Moon,
} from "lucide-react";
import { useUiStore } from "@/stores/uiStore";
import { useTheme } from "@/components/theme-provider";
import { cn } from "@/lib/utils";

const tabs = [
  { id: "query" as const, icon: Terminal, label: "Query Editor" },
  { id: "explore" as const, icon: Compass, label: "Explore Graph" },
  { id: "nlq" as const, icon: MessageSquare, label: "Natural Language" },
  { id: "schema" as const, icon: Database, label: "Schema Browser" },
  { id: "admin" as const, icon: Shield, label: "Administration" },
  { id: "settings" as const, icon: Settings, label: "Settings" },
];

export function ActivityBar() {
  const activeTab = useUiStore((s) => s.activeTab);
  const setActiveTab = useUiStore((s) => s.setActiveTab);

  return (
    <div className="flex h-full w-[52px] flex-col items-center border-r border-border bg-card py-2">
      {/* Logo */}
      <Tooltip text="Graphmind" side="right">
        <div className="mb-4 flex h-10 w-10 items-center justify-center">
          <img src="/favicon.svg" alt="Graphmind" className="h-7 w-7" />
        </div>
      </Tooltip>

      {/* Tab icons */}
      <div className="flex flex-1 flex-col gap-1">
        {tabs.map(({ id, icon: Icon, label }) => (
          <Tooltip key={id} text={label} side="right">
            <button
              onClick={() => setActiveTab(id)}
              className={cn(
                "flex h-10 w-10 items-center justify-center rounded-lg transition-colors",
                activeTab === id
                  ? "bg-primary/15 text-primary"
                  : "text-muted-foreground hover:bg-accent hover:text-foreground"
              )}
            >
              <Icon className="h-5 w-5" />
            </button>
          </Tooltip>
        ))}
      </div>

      {/* Bottom: separator + theme toggle */}
      <div className="mb-1 h-px w-6 bg-border" />
      <ThemeToggle />
    </div>
  );
}

/** Simple tooltip that appears on hover */
function Tooltip({
  children,
  text,
  side = "right",
}: {
  children: React.ReactNode;
  text: string;
  side?: "right" | "bottom";
}) {
  const [show, setShow] = useState(false);
  const timeoutRef = useRef<ReturnType<typeof setTimeout> | null>(null);

  const onEnter = () => {
    timeoutRef.current = setTimeout(() => setShow(true), 400);
  };
  const onLeave = () => {
    if (timeoutRef.current) clearTimeout(timeoutRef.current);
    setShow(false);
  };

  useEffect(() => {
    return () => {
      if (timeoutRef.current) clearTimeout(timeoutRef.current);
    };
  }, []);

  return (
    <div className="relative" onMouseEnter={onEnter} onMouseLeave={onLeave}>
      {children}
      {show && (
        <div
          className={cn(
            "pointer-events-none absolute z-50 whitespace-nowrap rounded-md bg-popover px-2.5 py-1 text-xs font-medium text-popover-foreground shadow-md border border-border",
            side === "right" ? "left-full ml-2 top-1/2 -translate-y-1/2" : "top-full mt-2 left-1/2 -translate-x-1/2"
          )}
        >
          {text}
        </div>
      )}
    </div>
  );
}

function ThemeToggle() {
  const { theme, setTheme } = useTheme();
  const isDark = theme === "dark" || (theme === "system" && window.matchMedia("(prefers-color-scheme: dark)").matches);

  const toggle = () => {
    setTheme(isDark ? "light" : "dark");
  };

  return (
    <Tooltip text={isDark ? "Light mode" : "Dark mode"} side="right">
      <button
        onClick={toggle}
        className="flex h-10 w-10 items-center justify-center rounded-lg text-muted-foreground hover:bg-accent hover:text-foreground transition-colors"
      >
        {isDark ? <Sun className="h-4 w-4" /> : <Moon className="h-4 w-4" />}
      </button>
    </Tooltip>
  );
}
