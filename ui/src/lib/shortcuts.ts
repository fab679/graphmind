import { useEffect } from "react";

export interface Shortcut {
  key: string; // e.g. "F5", "Escape", "Delete", "ctrl+a", "ctrl+shift+e"
  description: string;
  action: () => void;
}

interface ParsedShortcut {
  ctrl: boolean;
  shift: boolean;
  alt: boolean;
  meta: boolean;
  key: string;
}

function parseShortcut(shortcut: string): ParsedShortcut {
  const parts = shortcut.toLowerCase().split("+");
  const key = parts.pop() ?? "";
  return {
    ctrl: parts.includes("ctrl"),
    shift: parts.includes("shift"),
    alt: parts.includes("alt"),
    meta: parts.includes("meta"),
    key,
  };
}

function matchesShortcut(event: KeyboardEvent, shortcut: string): boolean {
  const parsed = parseShortcut(shortcut);

  if (parsed.ctrl !== (event.ctrlKey || event.metaKey)) return false;
  if (parsed.shift !== event.shiftKey) return false;
  if (parsed.alt !== event.altKey) return false;

  const eventKey = event.key.toLowerCase();

  // Handle special key names
  if (parsed.key === "enter" && eventKey === "enter") return true;
  if (parsed.key === "escape" && eventKey === "escape") return true;
  if (parsed.key === "delete" && (eventKey === "delete" || eventKey === "backspace")) return true;
  if (parsed.key === "?" && (eventKey === "?" || (event.shiftKey && eventKey === "/"))) return true;

  // Function keys
  if (parsed.key.startsWith("f") && parsed.key.length <= 3) {
    return eventKey === parsed.key;
  }

  return eventKey === parsed.key;
}

function isEditableTarget(event: KeyboardEvent): boolean {
  const target = event.target as HTMLElement | null;
  if (!target) return false;

  const tagName = target.tagName.toLowerCase();
  if (tagName === "input" || tagName === "textarea" || tagName === "select") return true;
  if (target.isContentEditable) return true;

  // Check for CodeMirror editor
  if (target.closest(".cm-editor") || target.closest(".cm-content")) return true;

  return false;
}

export function useKeyboardShortcuts(shortcuts: Shortcut[]): void {
  useEffect(() => {
    function handleKeyDown(event: KeyboardEvent) {
      if (isEditableTarget(event)) return;

      for (const shortcut of shortcuts) {
        if (matchesShortcut(event, shortcut.key)) {
          event.preventDefault();
          event.stopPropagation();
          shortcut.action();
          return;
        }
      }
    }

    document.addEventListener("keydown", handleKeyDown);
    return () => document.removeEventListener("keydown", handleKeyDown);
  }, [shortcuts]);
}
