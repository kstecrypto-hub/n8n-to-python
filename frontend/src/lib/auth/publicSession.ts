import type { PublicProfileSummary } from "@/lib/contracts";

export interface PublicSessionSnapshot {
  session_id?: string | null;
  active?: boolean;
  title?: string | null;
  status?: string | null;
  workspace_kind?: "general" | "hive" | string | null;
  updated_at?: string | null;
  profile_id?: string | null;
}

export interface PublicProfileSnapshot {
  profile_id?: string | null;
  active?: boolean;
  profile?: {
    display_name?: string | null;
    summary_json?: PublicProfileSummary | null;
  } | null;
}

export const PUBLIC_TENANT_ID = "shared";

export function summarizePublicSession(session: PublicSessionSnapshot | null): string {
  if (!session?.active || !session.session_id) {
    return "Fresh hive cell";
  }
  return session.title?.trim() || session.session_id;
}

export function summarizePublicProfile(profile: PublicProfileSnapshot | null): string {
  const displayName = profile?.profile?.display_name?.trim();
  if (displayName) {
    return displayName;
  }
  return profile?.profile_id ? `Profile ${profile.profile_id.slice(0, 8)}` : "Profile pending";
}
