import type { FormEvent } from "react";
import { useState } from "react";
import { Navigate } from "react-router-dom";
import { useAuth } from "@/lib/auth/authContext";

export function LoginPage() {
  const { session, busy, login, ready } = useAuth();
  const [email, setEmail] = useState("");
  const [password, setPassword] = useState("");
  const [error, setError] = useState<string | null>(null);

  if (!ready) {
    return <section className="auth-screen"><div className="auth-card"><p>Checking hive access...</p></div></section>;
  }

  if (session?.authenticated) {
    return <Navigate to="/chat" replace />;
  }

  async function handleSubmit(event: FormEvent<HTMLFormElement>) {
    event.preventDefault();
    setError(null);
    try {
      await login(email, password);
    } catch (submitError) {
      setError(submitError instanceof Error ? submitError.message : "Hive access failed");
    }
  }

  return (
    <section className="auth-screen">
      <div className="auth-card auth-card--hero">
        <div className="eyebrow">Hive Signal</div>
        <h1>Sign in before entering the chat hive.</h1>
        <p className="lede">
          The public product is now login-first. After access is granted, users only see the chat experience, not the operator or tenant tooling.
        </p>
        <div className="auth-badges">
          <span className="pill">Server-side cookie session</span>
          <span className="pill">API-only browser boundary</span>
          <span className="pill">Admin-provisioned access</span>
        </div>
      </div>

      <div className="auth-card auth-card--form">
        <form className="auth-form" onSubmit={handleSubmit}>
          <label>
            Email
            <input type="email" value={email} onChange={(event) => setEmail(event.target.value)} placeholder="you@example.com" />
          </label>
          <label>
            Password
            <input
              type="password"
              value={password}
              onChange={(event) => setPassword(event.target.value)}
              placeholder="Enter your password"
            />
          </label>
          {error ? <div className="notice notice--warn">{error}</div> : null}
          <button className="button button--primary auth-submit" type="submit" disabled={busy}>
            {busy ? "Checking access..." : "Enter hive"}
          </button>
        </form>

        <div className="auth-footer">
          <p className="caption">Accounts are provisioned server-side. Public self-signup is disabled.</p>
          <p className="caption">Operator tools remain internal and are intentionally removed from the public shell.</p>
        </div>
      </div>
    </section>
  );
}
