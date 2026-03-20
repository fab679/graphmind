import { useCallback, useEffect, useRef } from "react";
import { EditorState } from "@codemirror/state";
import { EditorView, keymap, placeholder as cmPlaceholder, lineNumbers } from "@codemirror/view";
import { defaultKeymap, history, historyKeymap } from "@codemirror/commands";
import { searchKeymap, highlightSelectionMatches } from "@codemirror/search";
import { StreamLanguage, syntaxHighlighting, HighlightStyle } from "@codemirror/language";
import { oneDark } from "@codemirror/theme-one-dark";
import { tags } from "@lezer/highlight";
import { autocompletion, closeBrackets } from "@codemirror/autocomplete";
import type { CompletionContext, CompletionResult } from "@codemirror/autocomplete";
import type { StringStream } from "@codemirror/language";
import { CYPHER_KEYWORDS, CYPHER_FUNCTIONS, CYPHER_PROCEDURES } from "@/lib/cypher";
import { useUiStore } from "@/stores/uiStore";
import { useTheme } from "@/components/theme-provider";

interface CypherEditorProps {
  value: string;
  onChange: (value: string) => void;
  onExecute: (query: string) => void;
  placeholder?: string;
}

// --- Keyword / function sets for tokenizer ---
const KEYWORD_SET = new Set(
  CYPHER_KEYWORDS.flatMap((kw) => {
    const words = kw.split(/\s+/);
    return words.length > 1 ? [kw, ...words] : [kw];
  }).map((w) => w.toUpperCase())
);

const CYPHER_KEYWORDS_SET = new Set(CYPHER_KEYWORDS.map((kw) => kw.toUpperCase()));

const FUNCTION_SET = new Set(CYPHER_FUNCTIONS.map((f) => f.toLowerCase()));

// --- StreamLanguage tokenizer for Cypher ---
interface CypherState {
  inString: false | "'" | '"';
}

const cypherTokenizer = {
  startState(): CypherState {
    return { inString: false };
  },
  token(stream: StringStream, _state: CypherState): string | null {
    // Skip whitespace
    if (stream.eatSpace()) return null;

    const peek = stream.peek();

    // Line comments
    if (peek === "/" && stream.match("//")) {
      stream.skipToEnd();
      return "lineComment";
    }

    // Block comments
    if (peek === "/" && stream.match("/*")) {
      while (!stream.eol()) {
        if (stream.match("*/")) break;
        stream.next();
      }
      return "blockComment";
    }

    // Strings
    if (peek === "'" || peek === '"') {
      const quote = stream.next()!;
      while (!stream.eol()) {
        const ch = stream.next();
        if (ch === "\\") { stream.next(); continue; }
        if (ch === quote) break;
      }
      return "string";
    }

    // Numbers
    if (stream.match(/^-?\d+(\.\d+)?([eE][+-]?\d+)?/)) {
      return "number";
    }

    // Operators and punctuation
    if (stream.match(/^[<>=!]+/) || stream.match(/^[+\-*/%^]/) || stream.match(/^[(){}[\],;.:]/)) {
      return "operator";
    }

    // Identifiers and keywords
    if (stream.match(/^[a-zA-Z_]\w*/)) {
      const word = stream.current();
      const upper = word.toUpperCase();
      if (KEYWORD_SET.has(upper)) return "keyword";
      if (FUNCTION_SET.has(word.toLowerCase())) return "function";
      return "variableName";
    }

    // Backtick-quoted identifiers
    if (peek === "`") {
      stream.next();
      while (!stream.eol()) {
        if (stream.next() === "`") break;
      }
      return "variableName";
    }

    // Parameter ($param)
    if (peek === "$") {
      stream.next();
      stream.match(/^\w+/);
      return "variableName";
    }

    stream.next();
    return null;
  },
};

const cypherLanguage = StreamLanguage.define(cypherTokenizer);

// --- Dark highlight style ---
const darkHighlightStyle = HighlightStyle.define([
  { tag: tags.keyword, color: "#7cafc2", fontWeight: "bold" },
  { tag: tags.string, color: "#a1b56c" },
  { tag: tags.number, color: "#dc9656" },
  { tag: tags.lineComment, color: "#747369", fontStyle: "italic" },
  { tag: tags.blockComment, color: "#747369", fontStyle: "italic" },
  { tag: tags.function(tags.variableName), color: "#86c1b9" },
  { tag: tags.operator, color: "#d8d8d8" },
  { tag: tags.variableName, color: "#e8e8e8" },
]);

// --- Light highlight style ---
const lightHighlightStyle = HighlightStyle.define([
  { tag: tags.keyword, color: "#1a6fb5", fontWeight: "bold" },
  { tag: tags.string, color: "#50802b" },
  { tag: tags.number, color: "#b35e14" },
  { tag: tags.lineComment, color: "#8e908c", fontStyle: "italic" },
  { tag: tags.blockComment, color: "#8e908c", fontStyle: "italic" },
  { tag: tags.function(tags.variableName), color: "#0b7a75" },
  { tag: tags.operator, color: "#4d4d4c" },
  { tag: tags.variableName, color: "#333333" },
]);

// --- Dark editor theme ---
const darkEditorTheme = EditorView.theme(
  {
    "&": { fontSize: "14px", height: "100%" },
    "&.cm-focused": { outline: "none" },
    ".cm-scroller": {
      fontFamily: "'JetBrains Mono', 'Fira Code', 'Cascadia Code', monospace",
      overflow: "auto",
    },
    ".cm-gutters": {
      backgroundColor: "#1a1a2e",
      borderRight: "1px solid #2a2a3e",
      color: "#4a4a5e",
    },
    ".cm-activeLineGutter": { backgroundColor: "#1e1e32" },
    ".cm-activeLine": { backgroundColor: "rgba(255, 255, 255, 0.03)" },
    ".cm-tooltip.cm-tooltip-autocomplete": {
      backgroundColor: "#1a1a2e",
      border: "1px solid #2a2a3e",
    },
    ".cm-tooltip-autocomplete ul li": { color: "#e8e8e8" },
    ".cm-tooltip-autocomplete ul li[aria-selected]": {
      backgroundColor: "#2a2a3e",
      color: "#ffffff",
    },
    ".cm-placeholder": { color: "#4a4a5e", fontStyle: "italic" },
  },
  { dark: true }
);

// --- Light editor theme ---
const lightEditorTheme = EditorView.theme(
  {
    "&": { fontSize: "14px", height: "100%", backgroundColor: "#ffffff" },
    "&.cm-focused": { outline: "none" },
    ".cm-scroller": {
      fontFamily: "'JetBrains Mono', 'Fira Code', 'Cascadia Code', monospace",
      overflow: "auto",
    },
    ".cm-gutters": {
      backgroundColor: "#f5f5f5",
      borderRight: "1px solid #e0e0e0",
      color: "#999999",
    },
    ".cm-activeLineGutter": { backgroundColor: "#eaeaea" },
    ".cm-activeLine": { backgroundColor: "rgba(0, 0, 0, 0.04)" },
    ".cm-tooltip.cm-tooltip-autocomplete": {
      backgroundColor: "#ffffff",
      border: "1px solid #e0e0e0",
      boxShadow: "0 2px 8px rgba(0,0,0,0.12)",
    },
    ".cm-tooltip-autocomplete ul li": { color: "#333333" },
    ".cm-tooltip-autocomplete ul li[aria-selected]": {
      backgroundColor: "#e8f0fe",
      color: "#1a1a1a",
    },
    ".cm-placeholder": { color: "#aaaaaa", fontStyle: "italic" },
    ".cm-content": { caretColor: "#333333" },
    ".cm-cursor": { borderLeftColor: "#333333" },
    ".cm-selectionBackground": { backgroundColor: "#d7e4f5 !important" },
  },
  { dark: false }
);

// --- Autocomplete provider ---
function createCypherCompletion(getSchema: () => ReturnType<typeof useUiStore.getState>["schema"]) {
  return function cypherCompletions(context: CompletionContext): CompletionResult | null {
    const word = context.matchBefore(/[\w.]+/);
    if (!word) return null;
    if (word.from === word.to && !context.explicit) return null;

    const options: Array<{ label: string; type: string; detail?: string; boost?: number }> = [];

    for (const kw of CYPHER_KEYWORDS) {
      options.push({ label: kw, type: "keyword", boost: 2 });
    }
    for (const fn of CYPHER_FUNCTIONS) {
      options.push({ label: fn + "()", type: "function", detail: "function", boost: 1 });
    }
    for (const proc of CYPHER_PROCEDURES) {
      options.push({ label: proc, type: "function", detail: "procedure", boost: 0 });
    }

    // Extract variables from current query text
    const queryText = context.state.doc.toString();
    const varPattern = /\((\w+)\s*:|[\[]\s*(\w+)\s*:|AS\s+(\w+)/gi;
    const variables = new Set<string>();
    let varMatch;
    while ((varMatch = varPattern.exec(queryText)) !== null) {
      const varName = varMatch[1] || varMatch[2] || varMatch[3];
      if (varName && !CYPHER_KEYWORDS_SET.has(varName.toUpperCase())) {
        variables.add(varName);
      }
    }
    for (const v of variables) {
      options.push({ label: v, type: "variable", boost: 4 });
    }

    // Schema-based completions
    const schema = getSchema();
    if (schema) {
      for (const nodeType of schema.node_types) {
        options.push({
          label: nodeType.label,
          type: "type",
          detail: `label (${nodeType.count})`,
          boost: 3,
        });
        for (const propName of Object.keys(nodeType.properties)) {
          options.push({
            label: propName,
            type: "property",
            detail: `${nodeType.label}.${propName}`,
            boost: 1,
          });
        }
      }
      for (const edgeType of schema.edge_types) {
        options.push({
          label: edgeType.type,
          type: "type",
          detail: `rel type (${edgeType.count})`,
          boost: 3,
        });
      }
    }

    return { from: word.from, options, validFor: /^[\w.]*$/ };
  };
}

export function CypherEditor({ value, onChange, onExecute, placeholder }: CypherEditorProps) {
  const containerRef = useRef<HTMLDivElement>(null);
  const viewRef = useRef<EditorView | null>(null);
  const onChangeRef = useRef(onChange);
  const onExecuteRef = useRef(onExecute);
  const { theme } = useTheme();

  onChangeRef.current = onChange;
  onExecuteRef.current = onExecute;

  const getSchema = useCallback(() => useUiStore.getState().schema, []);

  // Determine effective dark/light
  const isDark =
    theme === "dark" ||
    (theme === "system" && window.matchMedia("(prefers-color-scheme: dark)").matches);

  // Recreate editor when theme changes
  useEffect(() => {
    if (!containerRef.current) return;

    const executeKeymap = keymap.of([
      {
        key: "Mod-Enter",
        run: (view) => {
          onExecuteRef.current(view.state.doc.toString());
          return true;
        },
      },
    ]);

    const updateListener = EditorView.updateListener.of((update) => {
      if (update.docChanged) {
        onChangeRef.current(update.state.doc.toString());
      }
    });

    const extensions = [
      lineNumbers(),
      history(),
      closeBrackets(),
      highlightSelectionMatches(),
      cypherLanguage,
      syntaxHighlighting(isDark ? darkHighlightStyle : lightHighlightStyle),
      ...(isDark ? [oneDark, darkEditorTheme] : [lightEditorTheme]),
      executeKeymap,
      keymap.of([...defaultKeymap, ...historyKeymap, ...searchKeymap]),
      autocompletion({
        override: [createCypherCompletion(getSchema)],
        activateOnTyping: true,
        icons: true,
      }),
      placeholder ? cmPlaceholder(placeholder) : [],
      updateListener,
      EditorView.lineWrapping,
    ];

    const state = EditorState.create({
      doc: viewRef.current?.state.doc.toString() ?? value,
      extensions,
    });

    // Destroy previous editor if exists
    viewRef.current?.destroy();

    const view = new EditorView({
      state,
      parent: containerRef.current,
    });

    viewRef.current = view;

    return () => {
      view.destroy();
      viewRef.current = null;
    };
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [isDark]);

  // Sync external value changes into the editor
  useEffect(() => {
    const view = viewRef.current;
    if (!view) return;

    const currentDoc = view.state.doc.toString();
    if (currentDoc !== value) {
      view.dispatch({
        changes: { from: 0, to: currentDoc.length, insert: value },
      });
    }
  }, [value]);

  return (
    <div
      ref={containerRef}
      className="h-full w-full overflow-hidden rounded border border-border bg-card"
    />
  );
}
