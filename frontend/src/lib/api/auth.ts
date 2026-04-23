import { requestJson } from "@/lib/api/http";
import type { SessionAuthUser } from "@/lib/contracts";

export type AuthUser = SessionAuthUser;

export interface AuthSessionResponse {
  authenticated: boolean;
  auth_session_id?: string;
  user: AuthUser | null;
}

export interface LoginRequest {
  email: string;
  password: string;
}

export function loadAuthSession() {
  return requestJson<AuthSessionResponse>("/auth/session");
}

export function login(payload: LoginRequest) {
  return requestJson<AuthSessionResponse>("/auth/login", {
    method: "POST",
    body: payload,
  });
}

export function logout() {
  return requestJson<{ ok: boolean; authenticated: boolean }>("/auth/logout", {
    method: "POST",
  });
}
