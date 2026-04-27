import { buildQuery, requestJson, type RequestJsonOptions } from "@/lib/api/http";
import { loadAdminToken } from "@/lib/auth/adminToken";
import type { AdminAuthUser, PagedResponse, RolePermissionPresets } from "@/lib/contracts";

export interface AdminOverview {
  [key: string]: unknown;
}

export type AdminPagedResponse<T> = PagedResponse<T>;

export interface AdminMetrics {
  [key: string]: unknown;
}

export interface AdminRoute {
  path: string;
  methods: string[];
}

export interface AdminConfigResponse {
  tenant_id?: string;
  config?: Record<string, unknown>;
  defaults?: Record<string, unknown>;
  stored_override?: Record<string, unknown>;
  has_api_key_override?: boolean;
  effective_api_key_source?: string;
  updated_at?: string | null;
  updated_by?: string | null;
}

export interface SystemConfigPayload {
  group: string;
  groups: Record<string, string[]>;
  editable_config: Record<string, unknown>;
  effective_config: Record<string, unknown>;
  secret_keys: string[];
  provider_key_sources?: Record<string, unknown>;
  collection_defaults?: Record<string, unknown>;
  env_path?: string;
  restart_required?: boolean;
  note?: string;
}

export interface DbRelationSummary {
  schema_name: string;
  relation_name: string;
  relation_type: string;
  estimated_rows: number;
  has_primary_key: boolean;
}

export interface DbColumn {
  column_name: string;
  data_type: string;
  udt_name: string;
  is_nullable: string;
  column_default?: string | null;
  ordinal_position: number;
}

export interface DbRelationDetail {
  database?: string;
  schema_name: string;
  relation_name: string;
  relation_type: string;
  columns: DbColumn[];
  primary_key: string[];
  redacted_columns?: string[];
  rows: Record<string, unknown>[];
  total: number;
  limit: number;
  offset: number;
  order_by?: string[];
}

export interface DbRowMutationRequest {
  database?: string;
  schema_name?: string;
  relation_name: string;
  key?: Record<string, unknown> | null;
  values?: Record<string, unknown> | null;
}

export interface DbRowMutationResponse {
  database?: string;
  schema_name: string;
  relation_name: string;
  row?: Record<string, unknown>;
  deleted?: boolean;
}

export interface AdminSqlResponse {
  database?: string;
  statement_type: string;
  columns: string[];
  rows: Record<string, unknown>[];
  row_count: number;
  truncated: boolean;
}

export type AuthUser = AdminAuthUser;

export interface AuthUserDetailResponse {
  user: AuthUser;
  sessions: AuthSessionRecord[];
}

export interface AuthSessionRecord {
  auth_session_id: string;
  user_id: string;
  tenant_id: string;
  created_at: string;
  last_seen_at: string;
  expires_at: string;
  revoked_at?: string | null;
  email: string;
  display_name?: string;
  active: boolean;
}

export interface AuthUsersResponse extends AdminPagedResponse<AuthUser> {
  available_roles: string[];
  available_statuses: string[];
  available_permissions: string[];
  role_permission_presets: RolePermissionPresets;
}

export interface AuthUserCreateRequest {
  email: string;
  password: string;
  display_name?: string | null;
  tenant_id?: string;
  role?: string;
  status?: string;
  permissions?: string[] | null;
}

export interface AuthUserUpdateRequest {
  email?: string | null;
  password?: string | null;
  display_name?: string | null;
  tenant_id?: string | null;
  role?: string | null;
  status?: string | null;
  permissions?: string[] | null;
}

export interface AdminDocumentRecord extends Record<string, unknown> {
  document_id: string;
  filename?: string;
  status?: string;
}

export interface AdminChunkRecord extends Record<string, unknown> {
  chunk_id: string;
  document_id?: string;
  validation_status?: string;
  text?: string;
  metadata_json?: Record<string, unknown>;
}

export interface AdminDocumentBundle extends Record<string, unknown> {
  document_id?: string;
  filename?: string;
  status?: string;
  counts?: Record<string, unknown>;
  sources?: Record<string, unknown>[];
  pages?: Record<string, unknown>[];
  page_assets?: Record<string, unknown>[];
  chunk_asset_links?: Record<string, unknown>[];
  chunks?: AdminChunkRecord[];
  chunk_metadata?: Record<string, unknown>[];
  kg_assertions?: Record<string, unknown>[];
  kg_entities?: Record<string, unknown>[];
  kg_evidence?: Record<string, unknown>[];
  kg_raw?: Record<string, unknown>[];
  chroma_records?: Record<string, unknown>[];
  asset_chroma_records?: Record<string, unknown>[];
}

export interface AdminChunkDetail extends Record<string, unknown> {
  chunk?: AdminChunkRecord;
  document?: AdminDocumentRecord;
  metadata?: Record<string, unknown>;
  linked_assets?: Record<string, unknown>[];
  kg_assertions?: Record<string, unknown>[];
  neighbor_chunks?: AdminChunkRecord[];
  chroma_record?: Record<string, unknown> | null;
  chroma_error?: string | null;
}

export interface AdminChunkDecisionResponse extends Record<string, unknown> {
  chunk_id?: string;
  action?: string;
}

export interface AdminAutoReviewResponse extends Record<string, unknown> {
  processed_chunks?: number;
  accepted?: number;
  rejected?: number;
  quarantined?: number;
}

export interface AdminKgEntityRecord extends Record<string, unknown> {
  entity_id: string;
  canonical_name?: string;
  entity_type?: string;
}

export interface AdminKgEntityDetail extends Record<string, unknown> {
  entity?: AdminKgEntityRecord;
  assertions?: Record<string, unknown>[];
  evidence?: Record<string, unknown>[];
  chunks?: Record<string, unknown>[];
  documents?: Record<string, unknown>[];
}

export interface AdminChromaCollectionRecord extends Record<string, unknown> {
  name: string;
  count: number;
  metadata?: Record<string, unknown>;
  is_default_chunk?: boolean;
  is_default_asset?: boolean;
}

export interface AdminChromaRecordsResponse extends AdminPagedResponse<Record<string, unknown>> {
  error?: string | null;
}

export interface AdminOntologyPayload extends Record<string, unknown> {
  path?: string;
  content?: string;
  updated_at?: string | null;
  stats?: Record<string, unknown>;
  parse_error?: string | null;
}

export interface AdminIngestProgressSnapshot extends Record<string, unknown> {
  progress?: Record<string, unknown> | null;
  phase?: Record<string, unknown>;
  runner_active?: boolean;
  runner_pid?: number | null;
  latest_asset_write?: Record<string, unknown> | null;
  active_documents?: Record<string, unknown>[];
  recent_document_stages?: Record<string, unknown>[];
}

export interface AdminStageRunRecord extends Record<string, unknown> {
  stage_run_id?: string;
  stage_name?: string;
  status?: string;
  document_id?: string;
}

export interface AdminReviewRunRecord extends Record<string, unknown> {
  review_run_id?: string;
  decision?: string;
  document_id?: string;
}

export interface AdminRunReviewResponse extends Record<string, unknown> {
  review_id?: string;
  decision?: string;
}

export interface AdminMemoryClearResponse extends Record<string, unknown> {
  session_id?: string;
  profile_id?: string;
  cleared_sections?: string[];
  available_sections?: string[];
  memory?: Record<string, unknown> | null;
  profile?: Record<string, unknown> | null;
}

export interface AdminEditorPayload extends Record<string, unknown> {
  record_type?: string;
  record_id?: string;
  secondary_id?: string | null;
  payload?: Record<string, unknown> | null;
}

export interface AdminUploadIngestPayload {
  file: File;
  tenant_id?: string;
  document_class?: string;
  parser_version?: string;
  source_type?: "pdf" | "text";
  filename?: string;
  page_start?: number | null;
  page_end?: number | null;
}

function requestAdminJson<T>(path: string, options: RequestJsonOptions = {}) {
  const token = loadAdminToken();
  const headers = new Headers(options.headers);
  if (token) {
    headers.set("X-Admin-Token", token);
  }
  return requestJson<T>(path, {
    ...options,
    headers,
  });
}

export function loadAdminOverview() {
  return requestAdminJson<AdminOverview>("/admin/api/overview");
}

export function loadAdminMetrics() {
  return requestAdminJson<AdminMetrics>("/admin/api/agent/metrics");
}

export function loadAdminRoutes() {
  return requestAdminJson<AdminRoute[]>("/admin/api/system/routes");
}

export function loadAdminDocuments(limit = 8, offset = 0) {
  return requestAdminJson<AdminPagedResponse<AdminDocumentRecord>>(`/admin/api/documents${buildQuery({ limit, offset })}`);
}

export function loadAdminSessions(params: { status?: string; limit?: number; offset?: number } = {}) {
  return requestAdminJson<AdminPagedResponse<Record<string, unknown>>>(`/admin/api/agent/sessions${buildQuery(params)}`);
}

export function loadAdminRuns(params: { session_id?: string; status?: string; abstained?: boolean; review_status?: string; limit?: number; offset?: number } = {}) {
  return requestAdminJson<AdminPagedResponse<Record<string, unknown>>>(`/admin/api/agent/runs${buildQuery(params)}`);
}

export function loadAdminProfiles(params: { tenant_id?: string; status?: string; limit?: number; offset?: number } = {}) {
  return requestAdminJson<AdminPagedResponse<Record<string, unknown>>>(`/admin/api/agent/profiles${buildQuery(params)}`);
}

export function loadAdminConfig(tenantId = "shared") {
  return requestAdminJson<AdminConfigResponse>(`/admin/api/agent/config${buildQuery({ tenant_id: tenantId })}`);
}

export function saveAdminRuntimeConfig(tenantId: string, config: Record<string, unknown>, clearApiKeyOverride = false) {
  return requestAdminJson<AdminConfigResponse>("/admin/api/agent/config", {
    method: "PUT",
    body: {
      tenant_id: tenantId,
      config,
      clear_api_key_override: clearApiKeyOverride,
      updated_by: "frontend-admin",
    },
  });
}

export function resetAdminRuntimeConfig(tenantId: string) {
  return requestAdminJson<AdminConfigResponse>(`/admin/api/agent/config${buildQuery({ tenant_id: tenantId })}`, {
    method: "DELETE",
  });
}

export function loadSystemConfig(group: string) {
  return requestAdminJson<SystemConfigPayload>(`/admin/api/system/config${buildQuery({ group })}`);
}

export function saveSystemConfig(group: string, config: Record<string, unknown>) {
  return requestAdminJson<SystemConfigPayload>("/admin/api/system/config", {
    method: "PUT",
    body: {
      group,
      config,
      updated_by: "frontend-admin",
    },
  });
}

export function resetSystemConfig(group: string) {
  return requestAdminJson<SystemConfigPayload>(`/admin/api/system/config${buildQuery({ group })}`, {
    method: "DELETE",
  });
}

export function loadDbRelations(search = "", schemaName?: string, database = "app") {
  return requestAdminJson<{ items: DbRelationSummary[]; total: number; schema_name: string; database?: string }>(`/admin/api/db/relations${buildQuery({ search, schema_name: schemaName, database })}`);
}

export function loadDbRelationDetail(relationName: string, schemaName = "public", database = "app", limit = 50, offset = 0) {
  return requestAdminJson<DbRelationDetail>(`/admin/api/db/relations/${encodeURIComponent(relationName)}${buildQuery({ schema_name: schemaName, database, limit, offset })}`);
}

export function insertDbRow(payload: DbRowMutationRequest) {
  return requestAdminJson<DbRowMutationResponse>("/admin/api/db/rows", {
    method: "POST",
    body: payload,
  });
}

export function updateDbRow(payload: DbRowMutationRequest) {
  return requestAdminJson<DbRowMutationResponse>("/admin/api/db/rows", {
    method: "PUT",
    body: payload,
  });
}

export function deleteDbRow(payload: DbRowMutationRequest) {
  return requestAdminJson<DbRowMutationResponse>("/admin/api/db/rows", {
    method: "DELETE",
    body: payload,
  });
}

export function executeAdminSql(sql: string, database = "app") {
  return requestAdminJson<AdminSqlResponse>("/admin/api/db/sql", {
    method: "POST",
    body: { sql, database },
  });
}

export function loadAuthUsers(params: { search?: string; tenant_id?: string; role?: string; status?: string; limit?: number; offset?: number } = {}) {
  return requestAdminJson<AuthUsersResponse>(`/admin/api/auth/users${buildQuery(params)}`);
}

export function loadAuthUserDetail(userId: string) {
  return requestAdminJson<AuthUserDetailResponse>(`/admin/api/auth/users/${encodeURIComponent(userId)}`);
}

export function createAuthUser(payload: AuthUserCreateRequest) {
  return requestAdminJson<{ user: AuthUser }>("/admin/api/auth/users", {
    method: "POST",
    body: payload,
  });
}

export function updateAuthUser(userId: string, payload: AuthUserUpdateRequest) {
  return requestAdminJson<{ user: AuthUser }>(`/admin/api/auth/users/${encodeURIComponent(userId)}`, {
    method: "PUT",
    body: payload,
  });
}

export function revokeAuthUserSessions(userId: string) {
  return requestAdminJson<{ user_id: string; revoked_sessions: number }>(`/admin/api/auth/users/${encodeURIComponent(userId)}/revoke-sessions`, {
    method: "POST",
  });
}

export function deleteAuthUser(userId: string) {
  return requestAdminJson<{ user_id: string; deleted: boolean }>(`/admin/api/auth/users/${encodeURIComponent(userId)}`, {
    method: "DELETE",
  });
}

export function loadAdminDocumentBundle(documentId: string, limit = 250) {
  return requestAdminJson<AdminDocumentBundle>(`/admin/api/documents/${encodeURIComponent(documentId)}/bundle${buildQuery({ limit })}`);
}

export function runAdminDocumentAction(
  documentId: string,
  action: "rebuild" | "revalidate" | "reindex" | "reprocess-kg" | "delete",
  options: { rerunKg?: boolean; batchSize?: number } = {},
) {
  const path = `/admin/api/documents/${encodeURIComponent(documentId)}/${action}`;
  if (action === "revalidate") {
    return requestAdminJson<Record<string, unknown>>(path, {
      method: "POST",
      body: { rerun_kg: options.rerunKg ?? true },
    });
  }
  if (action === "reprocess-kg") {
    return requestAdminJson<Record<string, unknown>>(path, {
      method: "POST",
      body: { batch_size: options.batchSize ?? 250 },
    });
  }
  return requestAdminJson<Record<string, unknown>>(path, { method: "POST" });
}

export function loadAdminChunks(params: { document_id?: string; status?: string; limit?: number; offset?: number } = {}) {
  return requestAdminJson<AdminPagedResponse<AdminChunkRecord>>(`/admin/api/chunks${buildQuery(params)}`);
}

export function loadAdminChunkDetail(chunkId: string) {
  return requestAdminJson<AdminChunkDetail>(`/admin/api/chunks/${encodeURIComponent(chunkId)}`);
}

export function decideAdminChunk(chunkId: string, action: "accept" | "reject" | "auto") {
  return requestAdminJson<AdminChunkDecisionResponse>(`/admin/api/chunks/${encodeURIComponent(chunkId)}/decision`, {
    method: "POST",
    body: { action },
  });
}

export function autoReviewAdminChunks(documentId?: string, batchSize = 250) {
  return requestAdminJson<AdminAutoReviewResponse>("/admin/api/chunks/review/auto", {
    method: "POST",
    body: { document_id: documentId || null, batch_size: batchSize },
  });
}

export function loadAdminChunkMetadata(params: { document_id?: string; status?: string; limit?: number; offset?: number } = {}) {
  return requestAdminJson<AdminPagedResponse<Record<string, unknown>>>(`/admin/api/metadata/chunks${buildQuery(params)}`);
}

export function loadAdminKgEntities(params: { document_id?: string; search?: string; entity_type?: string; limit?: number; offset?: number } = {}) {
  return requestAdminJson<AdminPagedResponse<AdminKgEntityRecord>>(`/admin/api/kg/entities${buildQuery(params)}`);
}

export function loadAdminKgEntityDetail(entityId: string) {
  return requestAdminJson<AdminKgEntityDetail>(`/admin/api/kg/entities/${encodeURIComponent(entityId)}`);
}

export function loadAdminKgAssertions(
  params: { document_id?: string; entity_id?: string; predicate?: string; status?: string; chunk_id?: string; limit?: number; offset?: number } = {},
) {
  return requestAdminJson<AdminPagedResponse<Record<string, unknown>>>(`/admin/api/kg/assertions${buildQuery(params)}`);
}

export function loadAdminKgRaw(params: { document_id?: string; chunk_id?: string; status?: string; limit?: number; offset?: number } = {}) {
  return requestAdminJson<AdminPagedResponse<Record<string, unknown>>>(`/admin/api/kg/raw${buildQuery(params)}`);
}

export function loadAdminChromaCollections() {
  return requestAdminJson<AdminChromaCollectionRecord[]>("/admin/api/chroma/collections");
}

export function loadAdminChromaRecords(params: { collection_name?: string; document_id?: string; limit?: number; offset?: number } = {}) {
  return requestAdminJson<AdminChromaRecordsResponse>(`/admin/api/chroma/records${buildQuery(params)}`);
}

export function loadAdminChromaParity(documentId?: string) {
  return requestAdminJson<Record<string, unknown>>(`/admin/api/chroma/parity${buildQuery({ document_id: documentId })}`);
}

export function loadAdminProcesses(limit = 12) {
  return requestAdminJson<Record<string, unknown>>(`/admin/api/system/processes${buildQuery({ limit })}`);
}

export function loadAdminIngestProgress() {
  return requestAdminJson<AdminIngestProgressSnapshot>("/admin/api/system/ingest-progress");
}

export function startAdminIngest() {
  return requestAdminJson<Record<string, unknown>>("/admin/api/system/reingest/start", { method: "POST" });
}

export function resumeAdminIngest() {
  return requestAdminJson<Record<string, unknown>>("/admin/api/system/reingest/resume", { method: "POST" });
}

export function stopAdminIngest() {
  return requestAdminJson<Record<string, unknown>>("/admin/api/system/reingest/stop", { method: "POST" });
}

export function loadAdminStageRuns(params: { document_id?: string; status?: string; limit?: number; offset?: number } = {}) {
  return requestAdminJson<AdminPagedResponse<AdminStageRunRecord>>(`/admin/api/activity/stages${buildQuery(params)}`);
}

export function loadAdminReviewRuns(params: { document_id?: string; decision?: string; limit?: number; offset?: number } = {}) {
  return requestAdminJson<AdminPagedResponse<AdminReviewRunRecord>>(`/admin/api/activity/reviews${buildQuery(params)}`);
}

export function loadAdminSessionDetail(sessionId: string) {
  return requestAdminJson<Record<string, unknown>>(`/admin/api/agent/sessions/${encodeURIComponent(sessionId)}`);
}

export function clearAdminSessionMemory(sessionId: string, sections: string[]) {
  return requestAdminJson<AdminMemoryClearResponse>(`/admin/api/agent/sessions/${encodeURIComponent(sessionId)}/memory/clear`, {
    method: "POST",
    body: { sections },
  });
}

export function loadAdminProfileDetail(profileId: string) {
  return requestAdminJson<Record<string, unknown>>(`/admin/api/agent/profiles/${encodeURIComponent(profileId)}`);
}

export function clearAdminProfileMemory(profileId: string, sections: string[]) {
  return requestAdminJson<AdminMemoryClearResponse>(`/admin/api/agent/profiles/${encodeURIComponent(profileId)}/memory/clear`, {
    method: "POST",
    body: { sections },
  });
}

export function loadAdminRunDetail(queryRunId: string) {
  return requestAdminJson<Record<string, unknown>>(`/admin/api/agent/runs/${encodeURIComponent(queryRunId)}`);
}

export function loadAdminAgentReviews(params: { decision?: string; limit?: number; offset?: number } = {}) {
  return requestAdminJson<AdminPagedResponse<Record<string, unknown>>>(`/admin/api/agent/reviews${buildQuery(params)}`);
}

export function loadAdminPatterns(params: { tenant_id?: string; search?: string; limit?: number; offset?: number } = {}) {
  return requestAdminJson<AdminPagedResponse<Record<string, unknown>>>(`/admin/api/agent/patterns${buildQuery(params)}`);
}

export function reviewAdminRun(queryRunId: string, decision: string, notes = "") {
  return requestAdminJson<AdminRunReviewResponse>(`/admin/api/agent/runs/${encodeURIComponent(queryRunId)}/review`, {
    method: "POST",
    body: { decision, notes },
  });
}

export function replayAdminRun(queryRunId: string) {
  return requestAdminJson<Record<string, unknown>>(`/admin/api/agent/runs/${encodeURIComponent(queryRunId)}/replay`, {
    method: "POST",
  });
}

export function loadAdminOntology() {
  return requestAdminJson<AdminOntologyPayload>("/admin/api/ontology");
}

export function saveAdminOntology(content: string) {
  return requestAdminJson<AdminOntologyPayload>("/admin/api/ontology", {
    method: "PUT",
    body: { content },
  });
}

export function runAdminRetrievalEvaluation(payload: { queries_file: string; output: string; tenant_id?: string; top_k?: number }) {
  return requestAdminJson<Record<string, unknown>>("/admin/api/retrieval/evaluate", {
    method: "POST",
    body: payload,
  });
}

export function loadAdminRetrievalEvaluation(path: string) {
  return requestAdminJson<Record<string, unknown>>(`/admin/api/retrieval/evaluation${buildQuery({ path })}`);
}

export function runAdminAgentEvaluation(payload: { queries_file: string; output: string; tenant_id?: string; top_k?: number }) {
  return requestAdminJson<Record<string, unknown>>("/admin/api/agent/evaluate", {
    method: "POST",
    body: payload,
  });
}

export function loadAdminAgentEvaluation(path: string) {
  return requestAdminJson<Record<string, unknown>>(`/admin/api/agent/evaluation${buildQuery({ path })}`);
}

export function resetAdminPipeline() {
  return requestAdminJson<Record<string, unknown>>("/admin/api/reset", { method: "POST" });
}

export function loadAdminEditorRecord(recordType: string, recordId: string, secondaryId?: string) {
  return requestAdminJson<Record<string, unknown>>("/admin/api/editor/load", {
    method: "POST",
    body: {
      record_type: recordType,
      record_id: recordId,
      secondary_id: secondaryId || null,
    },
  });
}

export function saveAdminEditorRecord(payload: {
  record_type: string;
  record_id: string;
  secondary_id?: string;
  payload: Record<string, unknown>;
  sync_index?: boolean;
}) {
  return requestAdminJson<Record<string, unknown>>("/admin/api/editor/save", {
    method: "PUT",
    body: {
      ...payload,
      secondary_id: payload.secondary_id || null,
      sync_index: payload.sync_index ?? false,
    },
  });
}

export function deleteAdminEditorRecord(payload: {
  record_type: string;
  record_id: string;
  secondary_id?: string;
  sync_index?: boolean;
}) {
  return requestAdminJson<Record<string, unknown>>("/admin/api/editor/delete", {
    method: "POST",
    body: {
      ...payload,
      secondary_id: payload.secondary_id || null,
      sync_index: payload.sync_index ?? false,
    },
  });
}

export function resyncAdminEditorRecord(recordType: string, recordId: string) {
  return requestAdminJson<Record<string, unknown>>("/admin/api/editor/resync", {
    method: "POST",
    body: {
      record_type: recordType,
      record_id: recordId,
    },
  });
}

export function ingestAdminText(payload: {
  tenant_id?: string;
  source_type?: string;
  filename: string;
  raw_text: string;
  document_class?: string;
  parser_version?: string;
}) {
  return requestAdminJson<Record<string, unknown>>("/ingest/text", {
    method: "POST",
    body: payload,
  });
}

export function ingestAdminPdf(payload: {
  tenant_id?: string;
  path: string;
  filename?: string;
  document_class?: string;
  parser_version?: string;
  page_start?: number | null;
  page_end?: number | null;
}) {
  return requestAdminJson<Record<string, unknown>>("/ingest/pdf", {
    method: "POST",
    body: payload,
  });
}

export function uploadAndIngestAdminFile(payload: AdminUploadIngestPayload) {
  const form = new FormData();
  form.set("file", payload.file);
  form.set("tenant_id", payload.tenant_id || "shared");
  form.set("document_class", payload.document_class || "book");
  form.set("parser_version", payload.parser_version || "v1");
  if (payload.source_type) {
    form.set("source_type", payload.source_type);
  }
  if (payload.filename) {
    form.set("filename", payload.filename);
  }
  if (payload.page_start !== null && payload.page_start !== undefined) {
    form.set("page_start", String(payload.page_start));
  }
  if (payload.page_end !== null && payload.page_end !== undefined) {
    form.set("page_end", String(payload.page_end));
  }
  return requestAdminJson<Record<string, unknown>>("/admin/api/uploads/ingest", {
    method: "POST",
    body: form,
  });
}
