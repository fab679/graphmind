import { useEffect, useRef } from "react";

interface KeyboardShortcutsHelpProps {
  open: boolean;
  onClose: () => void;
}

const shortcuts = [
  { keys: ["F5", "Ctrl+Enter"], description: "Run query" },
  { keys: ["Escape"], description: "Clear selection / Close fullscreen" },
  { keys: ["Delete"], description: "Remove selected node from canvas" },
  { keys: ["Ctrl+A"], description: "Select all nodes" },
  { keys: ["Ctrl+Shift+F"], description: "Toggle fullscreen" },
  { keys: ["Ctrl+Shift+H"], description: "Toggle highlight mode" },
  { keys: ["Ctrl+E"], description: "Export as PNG" },
  { keys: ["?"], description: "Show keyboard shortcuts" },
];

function Kbd({ children }: { children: React.ReactNode }) {
  return (
    <kbd className="inline-flex items-center justify-center rounded border border-gray-600 bg-gray-700 px-2 py-0.5 font-mono text-xs text-gray-200 shadow-sm min-w-[1.75rem]">
      {children}
    </kbd>
  );
}

export function KeyboardShortcutsHelp({ open, onClose }: KeyboardShortcutsHelpProps) {
  const panelRef = useRef<HTMLDivElement>(null);

  useEffect(() => {
    if (!open) return;

    function handleKeyDown(e: KeyboardEvent) {
      if (e.key === "Escape") {
        e.preventDefault();
        e.stopPropagation();
        onClose();
      }
    }

    document.addEventListener("keydown", handleKeyDown, true);
    return () => document.removeEventListener("keydown", handleKeyDown, true);
  }, [open, onClose]);

  if (!open) return null;

  return (
    <div
      className="fixed inset-0 z-50 flex items-center justify-center bg-black/50"
      onClick={(e) => {
        if (e.target === e.currentTarget) onClose();
      }}
    >
      <div
        ref={panelRef}
        className="w-full max-w-md rounded-lg border border-gray-700 bg-gray-800 p-6 shadow-xl"
      >
        <div className="mb-4 flex items-center justify-between">
          <h2 className="text-lg font-semibold text-gray-100">Keyboard Shortcuts</h2>
          <button
            onClick={onClose}
            className="rounded p-1 text-gray-400 hover:bg-gray-700 hover:text-gray-200"
            aria-label="Close"
          >
            <svg className="h-5 w-5" fill="none" viewBox="0 0 24 24" stroke="currentColor" strokeWidth={2}>
              <path strokeLinecap="round" strokeLinejoin="round" d="M6 18L18 6M6 6l12 12" />
            </svg>
          </button>
        </div>

        <div className="space-y-2">
          {shortcuts.map((s) => (
            <div
              key={s.description}
              className="flex items-center justify-between rounded px-2 py-1.5 hover:bg-gray-700/50"
            >
              <span className="text-sm text-gray-300">{s.description}</span>
              <span className="flex items-center gap-1.5">
                {s.keys.map((k, i) => (
                  <span key={k} className="flex items-center gap-1">
                    {i > 0 && <span className="text-xs text-gray-500">or</span>}
                    <Kbd>{k}</Kbd>
                  </span>
                ))}
              </span>
            </div>
          ))}
        </div>

        <p className="mt-4 text-center text-xs text-gray-500">
          Press <Kbd>Esc</Kbd> to close
        </p>
      </div>
    </div>
  );
}
