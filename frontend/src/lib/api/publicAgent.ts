import type { PublicProfileSnapshot, PublicSessionSnapshot } from "@/lib/auth/publicSession";
import { requestJson } from "@/lib/api/http";
import type { SessionMemorySummary } from "@/lib/contracts";

export interface AgentCitation {
  citation_kind?: "chunk" | "asset";
  chunk_id?: string;
  asset_id?: string;
  document_id?: string;
  page_start?: number | null;
  page_end?: number | null;
  page_number?: number | null;
  section_title?: string | null;
  asset_type?: string | null;
  label?: string | null;
  quote?: string;
  image_url?: string;
}

export interface AgentMessage {
  message_id?: string;
  role?: "user" | "assistant" | "system";
  content?: string;
  citations?: AgentCitation[];
  metadata_json?: Record<string, unknown>;
  created_at?: string;
}

export interface PublicAgentResponse {
  session_id?: string | null;
  profile_id?: string | null;
  query_run_id?: string | null;
  answer?: string;
  confidence?: number;
  abstained?: boolean;
  abstain_reason?: string | null;
  citations?: AgentCitation[];
  supporting_entities?: string[];
  supporting_assertions?: string[];
  review_status?: string | null;
  review_reason?: string | null;
  query_mode?: "auto" | "general" | "sensor";
  system_prompt_variant?: "general" | "sensor" | string | null;
  retrieval_mode?: string | null;
  messages?: AgentMessage[];
  [key: string]: unknown;
}

export interface PublicChatRequest {
  question: string;
  document_ids?: string[] | null;
  top_k?: number | null;
  query_mode?: "auto" | "general" | "sensor" | null;
  workspace_kind?: "general" | "hive" | null;
}

export interface PublicFeedbackRequest {
  feedback: "like" | "dislike";
  notes?: string | null;
}

export interface PublicAgentSessionSummary {
  session_id: string;
  title?: string | null;
  status?: string | null;
  workspace_kind?: "general" | "hive" | string | null;
  created_at?: string | null;
  updated_at?: string | null;
  message_count?: number;
  query_count?: number;
  last_message_content?: string | null;
  last_message_role?: "user" | "assistant" | "system" | string | null;
  last_message_at?: string | null;
}

export interface PublicAgentSessionMemory {
  session_id?: string;
  summary_json?: SessionMemorySummary | null;
  summary_text?: string | null;
  updated_at?: string | null;
}

export interface PublicAgentSessionDetail {
  session?: PublicAgentSessionSummary | null;
  memory?: PublicAgentSessionMemory | null;
  messages?: AgentMessage[];
}

export function loadPublicSession() {
  return requestJson<PublicSessionSnapshot>("/agent/session");
}

export function loadPublicProfile() {
  return requestJson<PublicProfileSnapshot>("/agent/profile");
}

export function resetPublicSession() {
  return requestJson<{ ok: boolean; session_id: null }>("/agent/session/reset", { method: "POST" });
}

export function sendPublicChat(payload: PublicChatRequest) {
  return requestJson<PublicAgentResponse>("/agent/chat", {
    method: "POST",
    body: payload,
  });
}

export function listPublicSessions(workspaceKind: "general" | "hive" = "general", limit = 24, offset = 0) {
  const params = new URLSearchParams({
    workspace_kind: workspaceKind,
    limit: String(limit),
    offset: String(offset),
  });
  return requestJson<{ items: PublicAgentSessionSummary[]; total: number }>(`/agent/sessions?${params.toString()}`);
}

export function activatePublicSession(sessionId: string, limit = 200) {
  return requestJson<PublicAgentSessionDetail>(`/agent/sessions/${encodeURIComponent(sessionId)}/activate?limit=${limit}`, {
    method: "POST",
  });
}

export function sendPublicFeedback(queryRunId: string, payload: PublicFeedbackRequest) {
  return requestJson<{ query_run_id: string; feedback: string; decision: string; pattern?: unknown }>(
    `/agent/runs/${encodeURIComponent(queryRunId)}/feedback`,
    {
      method: "POST",
      body: payload,
    },
  );
}

export function publicAssetImageUrl(assetId: string) {
  return `/agent/assets/${encodeURIComponent(assetId)}/image`;
}
