import type { ReactElement } from "react";
import { BrowserRouter, Navigate, Route, Routes } from "react-router-dom";
import { LoginPage } from "@/features/auth/LoginPage";
import { AdminPage } from "@/features/admin/AdminPage";
import { PublicChatPage } from "@/features/chat/PublicChatPage";
import { useAuth } from "@/lib/auth/authContext";

function RequireAuth({ children }: { children: ReactElement }) {
  const { ready, session } = useAuth();
  if (!ready) {
    return <section className="auth-screen"><div className="auth-card"><p>Loading hive access...</p></div></section>;
  }
  if (!session?.authenticated) {
    return <Navigate to="/" replace />;
  }
  return children;
}

export function AppRouter() {
  return (
    <BrowserRouter basename="/app">
      <Routes>
        <Route path="/" element={<LoginPage />} />
        <Route
          path="/chat"
          element={
            <RequireAuth>
              <PublicChatPage />
            </RequireAuth>
          }
        />
        <Route path="/control" element={<AdminPage />} />
        <Route path="*" element={<Navigate to="/" replace />} />
      </Routes>
    </BrowserRouter>
  );
}
