import { useState } from "react";
import { Lock, LockOpen, X } from "lucide-react";
import { useUiStore } from "@/stores/uiStore";
import { GraphSelector } from "@/components/ui/graph-selector";
import { getAuthToken, setAuthToken } from "@/api/client";
import { cn } from "@/lib/utils";

function AuthButton() {
  const [showDialog, setShowDialog] = useState(false);
  const [tokenInput, setTokenInput] = useState("");
  const currentToken = getAuthToken();
  const [hasToken, setHasToken] = useState(!!currentToken);

  const handleSave = () => {
    if (tokenInput.trim()) {
      setAuthToken(tokenInput.trim());
      setHasToken(true);
    }
    setTokenInput("");
    setShowDialog(false);
  };

  const handleClear = () => {
    setAuthToken(null);
    setHasToken(false);
    setShowDialog(false);
  };

  return (
    <div className="relative">
      <button
        onClick={() => setShowDialog(!showDialog)}
        className={cn(
          "rounded-md p-1.5 transition-colors",
          hasToken
            ? "text-emerald-500 hover:bg-emerald-500/10"
            : "text-muted-foreground hover:text-foreground hover:bg-muted"
        )}
        title={hasToken ? "Authenticated (click to manage)" : "Set auth token"}
      >
        {hasToken ? (
          <Lock className="h-3.5 w-3.5" />
        ) : (
          <LockOpen className="h-3.5 w-3.5" />
        )}
      </button>

      {showDialog && (
        <div className="absolute right-0 top-full mt-2 z-50 w-72 rounded-lg border bg-popover p-3 shadow-lg">
          <div className="flex items-center justify-between mb-2">
            <span className="text-xs font-medium text-foreground">
              Auth Token
            </span>
            <button
              onClick={() => setShowDialog(false)}
              className="text-muted-foreground hover:text-foreground"
            >
              <X className="h-3.5 w-3.5" />
            </button>
          </div>

          {hasToken ? (
            <div className="space-y-2">
              <div className="flex items-center gap-2 text-xs text-emerald-500">
                <Lock className="h-3 w-3" />
                <span>Token is set</span>
              </div>
              <p className="text-[10px] text-muted-foreground">
                All API requests include the auth token. Clear to disable.
              </p>
              <button
                onClick={handleClear}
                className="w-full rounded-md border border-destructive/30 bg-destructive/10 px-3 py-1.5 text-xs text-destructive hover:bg-destructive/20 transition-colors"
              >
                Clear Token
              </button>
            </div>
          ) : (
            <div className="space-y-2">
              <p className="text-[10px] text-muted-foreground">
                If the server requires authentication, enter the token here.
              </p>
              <input
                type="password"
                value={tokenInput}
                onChange={(e) => setTokenInput(e.target.value)}
                onKeyDown={(e) => e.key === "Enter" && handleSave()}
                placeholder="Enter auth token..."
                className="w-full rounded-md border bg-input px-2 py-1.5 text-xs text-foreground placeholder:text-muted-foreground focus:outline-none focus:ring-1 focus:ring-ring"
                autoFocus
              />
              <button
                onClick={handleSave}
                disabled={!tokenInput.trim()}
                className="w-full rounded-md bg-primary px-3 py-1.5 text-xs text-primary-foreground hover:bg-primary/90 disabled:opacity-50 transition-colors"
              >
                Save Token
              </button>
            </div>
          )}
        </div>
      )}
    </div>
  );
}

export function Navbar() {
  const connectionStatus = useUiStore((s) => s.connectionStatus);
  const serverVersion = useUiStore((s) => s.serverVersion);
  const nodeCount = useUiStore((s) => s.nodeCount);
  const edgeCount = useUiStore((s) => s.edgeCount);

  const isConnected = connectionStatus === "connected";

  return (
    <header className="flex h-10 shrink-0 items-center justify-between border-b border-border bg-background px-4">
      <div className="flex items-center gap-3">
        <GraphSelector />
      </div>

      <div className="flex items-center gap-3">
        <div className="flex items-center gap-2">
          <div className="flex items-center gap-1.5">
            <div
              className={cn(
                "h-2 w-2 rounded-full",
                isConnected
                  ? "bg-emerald-500 shadow-[0_0_6px_rgba(16,185,129,0.6)] animate-pulse"
                  : "bg-red-500 shadow-[0_0_6px_rgba(239,68,68,0.6)]"
              )}
            />
            <span className="text-xs text-muted-foreground">
              {isConnected ? "Connected" : "Disconnected"}
            </span>
          </div>

          {isConnected && (
            <>
              <span className="text-xs text-muted-foreground/60">|</span>
              <span className="text-xs text-muted-foreground">
                {nodeCount.toLocaleString()} nodes
              </span>
              <span className="text-xs text-muted-foreground/60">/</span>
              <span className="text-xs text-muted-foreground">
                {edgeCount.toLocaleString()} edges
              </span>
            </>
          )}
        </div>

        {serverVersion && (
          <span className="text-xs text-muted-foreground/70">
            v{serverVersion}
          </span>
        )}

        <AuthButton />
      </div>
    </header>
  );
}
