import { createContext, useContext, useEffect, useState, startTransition, type ReactNode } from "react";
import { loadAuthSession, login as loginRequest, logout as logoutRequest, type AuthSessionResponse } from "@/lib/api/auth";

interface AuthContextValue {
  ready: boolean;
  busy: boolean;
  session: AuthSessionResponse | null;
  refresh: () => Promise<void>;
  login: (email: string, password: string) => Promise<void>;
  logout: () => Promise<void>;
}

const AuthContext = createContext<AuthContextValue | null>(null);

interface AuthProviderProps {
  children: ReactNode;
}

export function AuthProvider({ children }: AuthProviderProps) {
  const [ready, setReady] = useState(false);
  const [busy, setBusy] = useState(false);
  const [session, setSession] = useState<AuthSessionResponse | null>(null);

  async function refresh() {
    const payload = await loadAuthSession();
    startTransition(() => {
      setSession(payload);
    });
  }

  useEffect(() => {
    let active = true;

    async function bootstrap() {
      try {
        const payload = await loadAuthSession();
        if (!active) {
          return;
        }
        setSession(payload);
      } finally {
        if (active) {
          setReady(true);
        }
      }
    }

    void bootstrap();
    return () => {
      active = false;
    };
  }, []);

  async function login(email: string, password: string) {
    setBusy(true);
    try {
      const payload = await loginRequest({ email, password });
      setSession(payload);
    } finally {
      setBusy(false);
    }
  }

  async function logout() {
    setBusy(true);
    try {
      await logoutRequest();
      setSession({ authenticated: false, user: null });
    } finally {
      setBusy(false);
    }
  }

  return (
    <AuthContext.Provider value={{ ready, busy, session, refresh, login, logout }}>
      {children}
    </AuthContext.Provider>
  );
}

export function useAuth() {
  const context = useContext(AuthContext);
  if (!context) {
    throw new Error("useAuth must be used inside AuthProvider");
  }
  return context;
}
