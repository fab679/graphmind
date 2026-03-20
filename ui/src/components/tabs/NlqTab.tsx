import { useState, useRef, useEffect } from "react";
import { Send, Play, Copy, MessageSquare, Bot, User } from "lucide-react";
import { translateNlq } from "@/api/client";
import { useQueryStore } from "@/stores/queryStore";
import { useUiStore } from "@/stores/uiStore";
import { cn } from "@/lib/utils";

interface Message {
  id: string;
  role: "user" | "assistant";
  text: string;
  cypher?: string;
  result?: { rowCount: number; error?: string };
  timestamp: number;
}

export function NlqTab() {
  const [messages, setMessages] = useState<Message[]>([]);
  const [input, setInput] = useState("");
  const [loading, setLoading] = useState(false);
  const scrollRef = useRef<HTMLDivElement>(null);
  const executeQuery = useQueryStore((s) => s.executeQuery);

  useEffect(() => {
    scrollRef.current?.scrollTo({
      top: scrollRef.current.scrollHeight,
      behavior: "smooth",
    });
  }, [messages]);

  const handleSend = async () => {
    if (!input.trim() || loading) return;
    const userMsg: Message = {
      id: Date.now().toString(),
      role: "user",
      text: input,
      timestamp: Date.now(),
    };
    setMessages((prev) => [...prev, userMsg]);
    setInput("");
    setLoading(true);

    try {
      const graph = useUiStore.getState().activeGraph;
      const nlqResult = await translateNlq(input, graph);

      const assistantMsg: Message = {
        id: (Date.now() + 1).toString(),
        role: "assistant",
        text: nlqResult.cypher
          ? "Here's the generated Cypher query:"
          : "Could not generate a query for that question.",
        cypher: nlqResult.cypher ?? undefined,
        timestamp: Date.now(),
      };
      setMessages((prev) => [...prev, assistantMsg]);
    } catch (err) {
      const errMsg: Message = {
        id: (Date.now() + 1).toString(),
        role: "assistant",
        text: `Error: ${err instanceof Error ? err.message : "Failed to translate"}`,
        timestamp: Date.now(),
      };
      setMessages((prev) => [...prev, errMsg]);
    }
    setLoading(false);
  };

  const handleRunCypher = async (cypher: string, msgId: string) => {
    try {
      await executeQuery(cypher);
      setMessages((prev) =>
        prev.map((m) =>
          m.id === msgId ? { ...m, result: { rowCount: -1 } } : m,
        ),
      );
      // Switch to query tab to see results
      useUiStore.getState().setActiveTab("query");
    } catch (err) {
      setMessages((prev) =>
        prev.map((m) =>
          m.id === msgId
            ? { ...m, result: { rowCount: 0, error: String(err) } }
            : m,
        ),
      );
    }
  };

  const handleCopy = (text: string) => {
    navigator.clipboard.writeText(text);
  };

  return (
    <div className="flex h-full flex-col">
      {/* Header */}
      <div className="border-b border-border px-4 py-3">
        <h2 className="text-sm font-semibold">Natural Language Queries</h2>
        <p className="text-xs text-muted-foreground">
          Ask questions about your graph in plain English
        </p>
      </div>

      {/* Messages */}
      <div ref={scrollRef} className="flex-1 overflow-auto p-4 space-y-4">
        {messages.length === 0 && (
          <div className="flex h-full items-center justify-center">
            <div className="text-center max-w-sm">
              <MessageSquare className="mx-auto mb-4 h-12 w-12 text-muted-foreground/20" />
              <h3 className="text-sm font-medium mb-2">
                Ask anything about your graph
              </h3>
              <div className="space-y-2 text-xs text-muted-foreground">
                <p>&quot;Who are Alice&apos;s friends?&quot;</p>
                <p>&quot;How many people live in San Francisco?&quot;</p>
                <p>
                  &quot;Show me the shortest path between Alice and Eve&quot;
                </p>
                <p>&quot;What companies have more than 3 employees?&quot;</p>
              </div>
              <p className="mt-4 text-[10px] text-muted-foreground/60">
                Requires an LLM provider (OpenAI, Gemini, or Ollama)
              </p>
            </div>
          </div>
        )}

        {messages.map((msg) => (
          <div
            key={msg.id}
            className={cn(
              "flex gap-3",
              msg.role === "user" ? "justify-end" : "justify-start",
            )}
          >
            {msg.role === "assistant" && (
              <div className="flex h-7 w-7 shrink-0 items-center justify-center rounded-full bg-primary/10">
                <Bot className="h-4 w-4 text-primary" />
              </div>
            )}
            <div
              className={cn(
                "max-w-[75%] rounded-lg px-3 py-2 text-sm",
                msg.role === "user"
                  ? "bg-primary text-primary-foreground"
                  : "bg-muted",
              )}
            >
              <p>{msg.text}</p>

              {msg.cypher && (
                <div className="mt-2 rounded bg-background/80 p-2">
                  <pre className="text-xs font-mono whitespace-pre-wrap text-foreground">
                    {msg.cypher}
                  </pre>
                  <div className="mt-2 flex gap-2">
                    <button
                      onClick={() => handleRunCypher(msg.cypher!, msg.id)}
                      className="flex items-center gap-1 rounded bg-primary px-2 py-0.5 text-[10px] font-medium text-primary-foreground hover:bg-primary/90"
                    >
                      <Play className="h-2.5 w-2.5" /> Run
                    </button>
                    <button
                      onClick={() => handleCopy(msg.cypher!)}
                      className="flex items-center gap-1 rounded border px-2 py-0.5 text-[10px] text-muted-foreground hover:text-foreground"
                    >
                      <Copy className="h-2.5 w-2.5" /> Copy
                    </button>
                  </div>
                </div>
              )}

              {msg.result && !msg.result.error && (
                <p className="mt-1 text-xs text-emerald-500">
                  Query executed — see Query tab
                </p>
              )}
              {msg.result?.error && (
                <p className="mt-1 text-xs text-destructive">
                  {msg.result.error}
                </p>
              )}
            </div>
            {msg.role === "user" && (
              <div className="flex h-7 w-7 shrink-0 items-center justify-center rounded-full bg-muted">
                <User className="h-4 w-4 text-muted-foreground" />
              </div>
            )}
          </div>
        ))}

        {loading && (
          <div className="flex gap-3">
            <div className="flex h-7 w-7 shrink-0 items-center justify-center rounded-full bg-primary/10">
              <Bot className="h-4 w-4 text-primary" />
            </div>
            <div className="rounded-lg bg-muted px-3 py-2">
              <div className="flex gap-1">
                <div
                  className="h-2 w-2 rounded-full bg-muted-foreground/40 animate-bounce"
                  style={{ animationDelay: "0ms" }}
                />
                <div
                  className="h-2 w-2 rounded-full bg-muted-foreground/40 animate-bounce"
                  style={{ animationDelay: "150ms" }}
                />
                <div
                  className="h-2 w-2 rounded-full bg-muted-foreground/40 animate-bounce"
                  style={{ animationDelay: "300ms" }}
                />
              </div>
            </div>
          </div>
        )}
      </div>

      {/* Input */}
      <div className="border-t border-border p-3">
        <form
          onSubmit={(e) => {
            e.preventDefault();
            handleSend();
          }}
          className="flex gap-2"
        >
          <input
            type="text"
            value={input}
            onChange={(e) => setInput(e.target.value)}
            placeholder="Ask a question about your graph..."
            className="flex-1 rounded-lg border bg-input px-3 py-2 text-sm outline-none focus:ring-1 focus:ring-ring placeholder:text-muted-foreground"
            disabled={loading}
          />
          <button
            type="submit"
            disabled={!input.trim() || loading}
            className="rounded-lg bg-primary px-3 py-2 text-primary-foreground hover:bg-primary/90 disabled:opacity-50 transition-colors"
          >
            <Send className="h-4 w-4" />
          </button>
        </form>
      </div>
    </div>
  );
}
