import { useState } from "react";

interface LoginScreenProps {
  onLogin: (username: string, password: string) => Promise<boolean>;
  onSkip?: () => void;
}

export function LoginScreen({ onLogin, onSkip }: LoginScreenProps) {
  const [username, setUsername] = useState("");
  const [password, setPassword] = useState("");
  const [error, setError] = useState("");
  const [loading, setLoading] = useState(false);

  const handleSubmit = async (e: React.FormEvent) => {
    e.preventDefault();
    setLoading(true);
    setError("");
    const success = await onLogin(username, password);
    if (!success) setError("Invalid credentials");
    setLoading(false);
  };

  return (
    <div className="flex min-h-screen items-center justify-center bg-background">
      <div className="w-full max-w-sm rounded-lg border bg-card p-6 shadow-lg">
        <div className="mb-6 text-center">
          <div className="mx-auto mb-3 flex h-12 w-12 items-center justify-center rounded-xl bg-primary text-xl font-bold text-primary-foreground">
            G
          </div>
          <h1 className="text-xl font-bold">Graphmind</h1>
          <p className="text-sm text-muted-foreground">
            Connect to your graph database
          </p>
        </div>

        <form onSubmit={handleSubmit} className="space-y-3">
          <div>
            <label className="text-xs font-medium text-muted-foreground">
              Username
            </label>
            <input
              type="text"
              value={username}
              onChange={(e) => setUsername(e.target.value)}
              placeholder="Enter username"
              className="w-full rounded-md border bg-input px-3 py-2 text-sm"
              autoFocus
            />
          </div>
          <div>
            <label className="text-xs font-medium text-muted-foreground">
              Password
            </label>
            <input
              type="password"
              value={password}
              onChange={(e) => setPassword(e.target.value)}
              placeholder="Enter password"
              className="w-full rounded-md border bg-input px-3 py-2 text-sm"
            />
          </div>

          {error && <p className="text-xs text-destructive">{error}</p>}

          <button
            type="submit"
            disabled={loading}
            className="w-full rounded-md bg-primary px-4 py-2 text-sm font-medium text-primary-foreground hover:bg-primary/90 disabled:opacity-50"
          >
            {loading ? "Connecting..." : "Connect"}
          </button>

          {onSkip && (
            <button
              type="button"
              onClick={onSkip}
              className="w-full text-xs text-muted-foreground hover:text-foreground"
            >
              Skip — connect without auth
            </button>
          )}
        </form>
      </div>
    </div>
  );
}
