export interface PagedResponse<T> {
  items: T[];
  total: number;
  limit: number;
  offset: number;
}

export interface AuthUserCore {
  user_id: string;
  email: string;
  display_name?: string;
  tenant_id: string;
  role: string;
  permissions?: string[];
}

export interface SessionAuthUser extends AuthUserCore {}

export interface AdminAuthUser extends AuthUserCore {
  status: string;
  created_at?: string;
  updated_at?: string;
  last_login_at?: string | null;
  active_sessions?: number;
}

export type RolePermissionPresets = Record<string, string[]>;

export interface PublicProfileSummary {
  display_name?: string | null;
  user_background?: string | null;
  beekeeping_context?: string | null;
  experience_level?: string | null;
  answer_preferences?: string[] | null;
  recurring_topics?: string[] | null;
  persistent_constraints?: string[] | null;
}

export interface SessionMemoryConstraint {
  constraint?: string;
  kind?: string;
  source?: string;
}

export interface SessionMemoryFact {
  fact?: string;
  fact_type?: string;
  source_type?: string;
  confidence?: number;
  review_policy?: string;
  chunk_ids?: string[];
  asset_ids?: string[];
  assertion_ids?: string[];
  evidence_ids?: string[];
}

export interface SessionMemoryThread {
  thread?: string;
  source?: string;
  source_query?: string;
  question_type?: string;
  expiry_policy?: string;
}

export interface SessionMemoryPreference {
  preference?: string;
  source?: string;
}

export interface SessionMemorySummary {
  session_goal?: string;
  active_constraints?: SessionMemoryConstraint[] | null;
  stable_facts?: SessionMemoryFact[] | null;
  open_threads?: SessionMemoryThread[] | null;
  resolved_threads?: SessionMemoryThread[] | null;
  user_preferences?: SessionMemoryPreference[] | null;
  topic_keywords?: string[] | null;
  preferred_document_ids?: string[] | null;
  scope_signature?: string | null;
  last_query?: string | null;
}
