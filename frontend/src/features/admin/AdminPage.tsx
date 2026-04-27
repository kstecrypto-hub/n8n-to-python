import { startTransition, useDeferredValue, useEffect, useState } from "react";
import {
  createAuthUser,
  deleteAuthUser,
  deleteDbRow,
  executeAdminSql,
  insertDbRow,
  loadAdminConfig,
  loadAdminDocuments,
  loadAdminMetrics,
  loadAdminOverview,
  loadAdminRoutes,
  loadAuthUserDetail,
  loadAuthUsers,
  loadDbRelationDetail,
  loadDbRelations,
  loadSystemConfig,
  resetAdminRuntimeConfig,
  resetSystemConfig,
  revokeAuthUserSessions,
  saveAdminRuntimeConfig,
  saveSystemConfig,
  updateAuthUser,
  updateDbRow,
  type AdminConfigResponse,
  type AdminDocumentRecord,
  type AdminOverview,
  type AdminRoute,
  type AdminSqlResponse,
  type AuthSessionRecord,
  type AuthUser,
  type AuthUsersResponse,
  type DbRelationDetail,
  type DbRelationSummary,
  type SystemConfigPayload,
} from "@/lib/api/admin";
import { AdminExtendedSections, type AdminExtendedSection } from "@/features/admin/AdminExtendedSections";
import { useAuth } from "@/lib/auth/authContext";
import { clearAdminToken, loadAdminToken, saveAdminToken } from "@/lib/auth/adminToken";
import type { RolePermissionPresets } from "@/lib/contracts";

type AdminSection = "overview" | "corpus" | "chunks" | "kg" | "chroma" | "agent" | "operations" | "tables" | "accounts" | "limits" | "runtime" | "sql";
type AdminDatabase = "app" | "identity";

interface DashboardSnapshot {
  overview: AdminOverview;
  metrics: Record<string, unknown>;
  routes: AdminRoute[];
  documents: AdminDocumentRecord[];
}

interface AccountFormState {
  email: string;
  password: string;
  display_name: string;
  tenant_id: string;
  role: string;
  status: string;
  permissions: string[];
}

const sectionLabels: Array<{ id: AdminSection; label: string; detail: string }> = [
  { id: "overview", label: "Overview", detail: "Platform health, routes, and recent data" },
  { id: "corpus", label: "Corpus", detail: "Documents, bundle inspection, rebuilds, and replay operations" },
  { id: "chunks", label: "Chunks", detail: "Review queue, manual chunk decisions, and chunk metadata" },
  { id: "kg", label: "Knowledge graph", detail: "Entities, assertions, raw extractions, and graph triage" },
  { id: "chroma", label: "Chroma", detail: "Collections, records, and vector parity checks" },
  { id: "agent", label: "Agent", detail: "Sessions, runs, reviews, patterns, and replay tooling" },
  { id: "operations", label: "Operations", detail: "Ingest runner, ontology, evals, and pipeline controls" },
  { id: "tables", label: "Tables", detail: "Supabase-style Postgres table browser and row editor" },
  { id: "accounts", label: "Accounts", detail: "Create, update, disable, and delete login accounts" },
  { id: "limits", label: "Rate limits", detail: "Tune login and public agent request throttles" },
  { id: "runtime", label: "Runtime", detail: "Agent overrides and startup config groups" },
  { id: "sql", label: "SQL", detail: "Run single-statement Postgres commands for advanced changes" },
];

const fallbackRoleOptions = [
  "guest",
  "member",
  "review_analyst",
  "knowledge_curator",
  "tenant_admin",
  "platform_owner",
];

const fallbackStatusOptions = ["active", "disabled"];
const adminDatabaseOptions: Array<{ id: AdminDatabase; label: string; detail: string }> = [
  { id: "app", label: "Application DB", detail: "Corpus, runtime, KG, and agent state" },
  { id: "identity", label: "Identity DB", detail: "Users, sessions, roles, and permissions" },
];

function defaultSchemaForDatabase(database: AdminDatabase): string {
  return database === "identity" ? "auth" : "public";
}

function hasPermission(permissions: Set<string>, permission: string): boolean {
  return permissions.has(permission);
}

function canAccessAdminConsole(permissions: Set<string>, availablePermissions: string[]): boolean {
  return availablePermissions
    .filter((permission) => !permission.startsWith("chat."))
    .some((permission) => permissions.has(permission));
}

function canAccessSection(section: AdminSection, permissions: Set<string>, availablePermissions: string[]): boolean {
  switch (section) {
    case "overview":
      return canAccessAdminConsole(permissions, availablePermissions);
    case "corpus":
    case "chunks":
    case "chroma":
      return (
        permissions.has("documents.read") ||
        permissions.has("documents.write") ||
        permissions.has("kg.read") ||
        permissions.has("kg.write") ||
        permissions.has("agent.review")
      );
    case "kg":
    case "agent":
    case "operations":
      return (
        permissions.has("kg.read") ||
        permissions.has("kg.write") ||
        permissions.has("documents.write") ||
        permissions.has("agent.review") ||
        permissions.has("runtime.write")
      );
    case "tables":
      return (
        permissions.has("db.rows.write") ||
        permissions.has("db.sql.write") ||
        permissions.has("accounts.read") ||
        permissions.has("accounts.write")
      );
    case "accounts":
      return permissions.has("accounts.read") || permissions.has("accounts.write");
    case "limits":
      return permissions.has("rate_limits.write") || permissions.has("runtime.write");
    case "runtime":
      return permissions.has("runtime.read") || permissions.has("runtime.write") || permissions.has("rate_limits.write");
    case "sql":
      return permissions.has("db.sql.write");
    default:
      return false;
  }
}

function canReadDatabase(permissions: Set<string>, database: AdminDatabase): boolean {
  if (database === "identity") {
    return permissions.has("accounts.read") || permissions.has("accounts.write") || permissions.has("db.sql.write");
  }
  return permissions.has("db.rows.write") || permissions.has("db.sql.write");
}

function canWriteDatabase(permissions: Set<string>, database: AdminDatabase): boolean {
  if (database === "identity") {
    return permissions.has("accounts.write") || permissions.has("db.sql.write");
  }
  return permissions.has("db.rows.write") || permissions.has("db.sql.write");
}

function formatValue(value: unknown): string {
  if (value === null || value === undefined || value === "") {
    return "n/a";
  }
  if (typeof value === "boolean") {
    return value ? "yes" : "no";
  }
  if (typeof value === "number") {
    return Number.isInteger(value) ? String(value) : value.toFixed(2);
  }
  if (Array.isArray(value)) {
    return value.length ? value.map((item) => formatValue(item)).join(", ") : "[]";
  }
  if (typeof value === "object") {
    return JSON.stringify(value);
  }
  return String(value);
}

function prettyJson(value: unknown): string {
  return JSON.stringify(value ?? {}, null, 2);
}

function parseJsonObject(label: string, value: string): Record<string, unknown> {
  try {
    const parsed = JSON.parse(value || "{}") as unknown;
    if (!parsed || typeof parsed !== "object" || Array.isArray(parsed)) {
      throw new Error(`${label} must be a JSON object.`);
    }
    return parsed as Record<string, unknown>;
  } catch (error) {
    throw new Error(error instanceof Error ? error.message : `${label} is not valid JSON.`);
  }
}

function shortId(value: unknown): string {
  const text = String(value ?? "").trim();
  if (!text) {
    return "n/a";
  }
  return text.length > 18 ? `${text.slice(0, 8)}...${text.slice(-4)}` : text;
}

function buildPrimaryKey(detail: DbRelationDetail | null, row: Record<string, unknown>): Record<string, unknown> | null {
  const primaryKey = detail?.primary_key ?? [];
  if (!primaryKey.length) {
    return null;
  }
  const key: Record<string, unknown> = {};
  for (const columnName of primaryKey) {
    key[columnName] = row[columnName];
  }
  return key;
}

function editableRowDraft(detail: DbRelationDetail | null, row: Record<string, unknown>): string {
  const blocked = new Set(detail?.redacted_columns ?? []);
  const payload: Record<string, unknown> = {};
  for (const [key, value] of Object.entries(row)) {
    if (blocked.has(key)) {
      continue;
    }
    payload[key] = value;
  }
  return prettyJson(payload);
}

function createEmptyAccountForm(rolePermissionPresets: RolePermissionPresets = {}): AccountFormState {
  return {
    email: "",
    password: "",
    display_name: "",
    tenant_id: "shared",
    role: "member",
    status: "active",
    permissions: [...(rolePermissionPresets.member || [])],
  };
}

function createSqlStarter(): string {
  return [
    "-- Single statement only.",
    "-- Example:",
    "-- SELECT table_name FROM information_schema.tables WHERE table_schema = 'public' ORDER BY table_name",
    "",
  ].join("\n");
}

function togglePermissionValue(current: string[], permission: string): string[] {
  return current.includes(permission)
    ? current.filter((value) => value !== permission)
    : [...current, permission].sort((left, right) => left.localeCompare(right));
}

export function AdminPage() {
  const { ready, session } = useAuth();
  const [section, setSection] = useState<AdminSection>("overview");
  const [tokenInput, setTokenInput] = useState(() => loadAdminToken());
  const [tenantId, setTenantId] = useState("shared");
  const [busy, setBusy] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [notice, setNotice] = useState<string | null>(null);

  const [dashboard, setDashboard] = useState<DashboardSnapshot | null>(null);

  const [relationSearch, setRelationSearch] = useState("");
  const deferredRelationSearch = useDeferredValue(relationSearch);
  const [selectedDatabase, setSelectedDatabase] = useState<AdminDatabase>("app");
  const [relations, setRelations] = useState<DbRelationSummary[]>([]);
  const [selectedRelationName, setSelectedRelationName] = useState<string | null>(null);
  const [selectedRelationSchemaName, setSelectedRelationSchemaName] = useState<string>("public");
  const [relationDetail, setRelationDetail] = useState<DbRelationDetail | null>(null);
  const [selectedRowKey, setSelectedRowKey] = useState<Record<string, unknown> | null>(null);
  const [selectedRowDraft, setSelectedRowDraft] = useState("{}");
  const [newRowDraft, setNewRowDraft] = useState("{}");
  const [tableBusy, setTableBusy] = useState(false);

  const [accountSearch, setAccountSearch] = useState("");
  const deferredAccountSearch = useDeferredValue(accountSearch);
  const [accounts, setAccounts] = useState<AuthUsersResponse | null>(null);
  const [selectedUserId, setSelectedUserId] = useState<string | null>(null);
  const [selectedUser, setSelectedUser] = useState<AuthUser | null>(null);
  const [selectedUserSessions, setSelectedUserSessions] = useState<AuthSessionRecord[]>([]);
  const [accountBusy, setAccountBusy] = useState(false);
  const [createAccountForm, setCreateAccountForm] = useState<AccountFormState>(() => createEmptyAccountForm());
  const [editAccountForm, setEditAccountForm] = useState<AccountFormState>(() => createEmptyAccountForm());

  const [rateLimitPayload, setRateLimitPayload] = useState<SystemConfigPayload | null>(null);
  const [rateLimitDraft, setRateLimitDraft] = useState<Record<string, unknown>>({});

  const [runtimeConfig, setRuntimeConfig] = useState<AdminConfigResponse | null>(null);
  const [runtimeDraft, setRuntimeDraft] = useState("{}");
  const [systemGroup, setSystemGroup] = useState("platform");
  const [systemConfigPayload, setSystemConfigPayload] = useState<SystemConfigPayload | null>(null);
  const [systemConfigDraft, setSystemConfigDraft] = useState("{}");
  const [sqlDraft, setSqlDraft] = useState(() => createSqlStarter());
  const [sqlResult, setSqlResult] = useState<AdminSqlResponse | null>(null);
  const [sqlBusy, setSqlBusy] = useState(false);

  function resolveReadableDatabase(database: AdminDatabase): AdminDatabase {
    const permissions = new Set((session?.user?.permissions ?? []).map((permission) => String(permission).trim().toLowerCase()));
    if (tokenInput.trim() || canReadDatabase(permissions, database)) {
      return database;
    }
    return adminDatabaseOptions.find((item) => canReadDatabase(permissions, item.id))?.id ?? database;
  }

  async function loadDashboard() {
    const [overview, metrics, routes, documents] = await Promise.all([
      loadAdminOverview(),
      loadAdminMetrics(),
      loadAdminRoutes(),
      loadAdminDocuments(8, 0),
    ]);
    startTransition(() => {
      setDashboard({
        overview,
        metrics,
        routes,
        documents: documents.items,
      });
    });
  }

  async function loadTableCatalog() {
    const activeDatabase = resolveReadableDatabase(selectedDatabase);
    if (activeDatabase !== selectedDatabase) {
      setSelectedDatabase(activeDatabase);
    }
    const payload = await loadDbRelations(deferredRelationSearch, undefined, activeDatabase);
    startTransition(() => {
      setRelations(payload.items);
      if (!selectedRelationName && payload.items[0]) {
        setSelectedRelationName(payload.items[0].relation_name);
        setSelectedRelationSchemaName(payload.items[0].schema_name || defaultSchemaForDatabase(activeDatabase));
      } else if (!payload.items.length) {
        setRelationDetail(null);
        setSelectedRelationSchemaName(defaultSchemaForDatabase(activeDatabase));
      }
    });
  }

  async function loadTableDetail(relationName: string, schemaName: string, database: AdminDatabase) {
    setTableBusy(true);
    try {
      const payload = await loadDbRelationDetail(relationName, schemaName, database, 50, 0);
      startTransition(() => {
        setRelationDetail(payload);
        setSelectedRelationName(relationName);
        setSelectedRelationSchemaName(schemaName);
        setSelectedRowKey(null);
        setSelectedRowDraft("{}");
      });
    } finally {
      setTableBusy(false);
    }
  }

  async function loadAccountsCatalog() {
    const payload = await loadAuthUsers({ search: deferredAccountSearch, limit: 100, offset: 0 });
    startTransition(() => {
      setAccounts(payload);
      if (!selectedUserId && payload.items[0]) {
        setSelectedUserId(payload.items[0].user_id);
      }
    });
  }

  async function loadAccountDetail(userId: string) {
    setAccountBusy(true);
    try {
      const payload = await loadAuthUserDetail(userId);
      startTransition(() => {
        setSelectedUser(payload.user);
        setSelectedUserSessions(payload.sessions);
        setSelectedUserId(userId);
        setEditAccountForm({
          email: payload.user.email,
          password: "",
          display_name: payload.user.display_name || "",
          tenant_id: payload.user.tenant_id || "shared",
          role: payload.user.role || "member",
          status: payload.user.status || "active",
          permissions: [...(payload.user.permissions || availableRolePresets[payload.user.role || "member"] || [])],
        });
      });
    } finally {
      setAccountBusy(false);
    }
  }

  async function loadRateLimits() {
    const payload = await loadSystemConfig("rate_limits");
    startTransition(() => {
      setRateLimitPayload(payload);
      setRateLimitDraft({ ...payload.editable_config });
    });
  }

  async function loadRuntime() {
    const [agentPayload, systemPayload] = await Promise.all([
      loadAdminConfig(tenantId),
      loadSystemConfig(systemGroup),
    ]);
    startTransition(() => {
      setRuntimeConfig(agentPayload);
      setRuntimeDraft(prettyJson(agentPayload.stored_override ?? agentPayload.config ?? {}));
      setSystemConfigPayload(systemPayload);
      setSystemConfigDraft(prettyJson(systemPayload.editable_config ?? {}));
    });
  }

  async function bootstrap(sectionId: AdminSection = section) {
    const token = tokenInput.trim();
    if (token) {
      saveAdminToken(token);
    }
    setBusy(true);
    setError(null);
    try {
      if (sectionId === "overview") {
        await loadDashboard();
      }
      if (sectionId === "tables") {
        await loadTableCatalog();
      }
      if (sectionId === "accounts") {
        await loadAccountsCatalog();
      }
      if (sectionId === "limits") {
        await loadRateLimits();
      }
      if (sectionId === "runtime") {
        await loadRuntime();
      }
    } catch (requestError) {
      setError(requestError instanceof Error ? requestError.message : "Failed to load the admin console");
    } finally {
      setBusy(false);
    }
  }

  useEffect(() => {
    if (ready && (tokenInput.trim() || session?.authenticated)) {
      const permissions = new Set((session?.user?.permissions ?? []).map((permission) => String(permission).trim().toLowerCase()));
      const initialSection =
        tokenInput.trim() || !session?.authenticated
          ? "overview"
          : sectionLabels.find((item) => canAccessSection(item.id, permissions, availablePermissions))?.id ?? "overview";
      void bootstrap(initialSection);
    }
  }, [ready, session?.authenticated, session?.user?.permissions, tokenInput]);

  useEffect(() => {
    if (section === "overview" && (tokenInput.trim() || session?.authenticated)) {
      void loadDashboard().catch((requestError) => {
        setError(requestError instanceof Error ? requestError.message : "Failed to load dashboard");
      });
    }
  }, [section, tokenInput, session?.authenticated]);

  useEffect(() => {
    if (section === "tables" && (tokenInput.trim() || session?.authenticated)) {
      void loadTableCatalog().catch((requestError) => {
        setError(requestError instanceof Error ? requestError.message : "Failed to load tables");
      });
    }
  }, [section, deferredRelationSearch, selectedDatabase, tokenInput, session?.authenticated]);

  useEffect(() => {
    if (section === "accounts" && (tokenInput.trim() || session?.authenticated)) {
      void loadAccountsCatalog().catch((requestError) => {
        setError(requestError instanceof Error ? requestError.message : "Failed to load accounts");
      });
    }
  }, [section, deferredAccountSearch, tokenInput, session?.authenticated]);

  useEffect(() => {
    if (section === "limits" && (tokenInput.trim() || session?.authenticated)) {
      void loadRateLimits().catch((requestError) => {
        setError(requestError instanceof Error ? requestError.message : "Failed to load rate limits");
      });
    }
  }, [section, tokenInput, session?.authenticated]);

  useEffect(() => {
    if (section === "runtime" && (tokenInput.trim() || session?.authenticated)) {
      void loadRuntime().catch((requestError) => {
        setError(requestError instanceof Error ? requestError.message : "Failed to load runtime settings");
      });
    }
  }, [section, tokenInput, tenantId, systemGroup, session?.authenticated]);

  useEffect(() => {
    if (section === "tables" && (tokenInput.trim() || session?.authenticated) && selectedRelationName) {
      void loadTableDetail(selectedRelationName, selectedRelationSchemaName, selectedDatabase).catch((requestError) => {
        setError(requestError instanceof Error ? requestError.message : "Failed to load relation detail");
      });
    }
  }, [section, selectedDatabase, selectedRelationName, selectedRelationSchemaName, tokenInput, session?.authenticated]);

  useEffect(() => {
    startTransition(() => {
      setSelectedRelationName(null);
      setSelectedRelationSchemaName(defaultSchemaForDatabase(selectedDatabase));
      setRelationDetail(null);
      setSelectedRowKey(null);
      setSelectedRowDraft("{}");
      setNewRowDraft("{}");
    });
  }, [selectedDatabase]);

  useEffect(() => {
    const usingSessionToken = Boolean(tokenInput.trim());
    if (usingSessionToken) {
      return;
    }
    const permissions = new Set((session?.user?.permissions ?? []).map((permission) => String(permission).trim().toLowerCase()));
    if (canReadDatabase(permissions, selectedDatabase)) {
      return;
    }
    const fallbackDatabase = adminDatabaseOptions.find((item) => canReadDatabase(permissions, item.id));
    if (fallbackDatabase) {
      setSelectedDatabase(fallbackDatabase.id);
    }
  }, [tokenInput, selectedDatabase, session?.user?.permissions]);

  useEffect(() => {
    if (section === "accounts" && (tokenInput.trim() || session?.authenticated) && selectedUserId) {
      void loadAccountDetail(selectedUserId).catch((requestError) => {
        setError(requestError instanceof Error ? requestError.message : "Failed to load account detail");
      });
    }
  }, [section, selectedUserId, tokenInput, session?.authenticated]);

  async function handleInsertRow() {
    if (!selectedRelationName) {
      return;
    }
    setTableBusy(true);
    setNotice(null);
    setError(null);
    try {
      const values = parseJsonObject("New row payload", newRowDraft);
      await insertDbRow({ database: selectedDatabase, schema_name: selectedRelationSchemaName, relation_name: selectedRelationName, values });
      await loadTableDetail(selectedRelationName, selectedRelationSchemaName, selectedDatabase);
      setNewRowDraft("{}");
      setNotice(`Inserted a row into ${selectedRelationSchemaName}.${selectedRelationName}.`);
    } catch (mutationError) {
      setError(mutationError instanceof Error ? mutationError.message : "Failed to insert row");
    } finally {
      setTableBusy(false);
    }
  }

  async function handleSaveRow() {
    if (!selectedRelationName || !selectedRowKey) {
      return;
    }
    setTableBusy(true);
    setNotice(null);
    setError(null);
    try {
      const values = parseJsonObject("Row payload", selectedRowDraft);
      await updateDbRow({ database: selectedDatabase, schema_name: selectedRelationSchemaName, relation_name: selectedRelationName, key: selectedRowKey, values });
      await loadTableDetail(selectedRelationName, selectedRelationSchemaName, selectedDatabase);
      setNotice(`Updated a row in ${selectedRelationSchemaName}.${selectedRelationName}.`);
    } catch (mutationError) {
      setError(mutationError instanceof Error ? mutationError.message : "Failed to update row");
    } finally {
      setTableBusy(false);
    }
  }

  async function handleDeleteRow() {
    if (!selectedRelationName || !selectedRowKey) {
      return;
    }
    if (!window.confirm(`Delete the selected row from ${selectedRelationSchemaName}.${selectedRelationName}?`)) {
      return;
    }
    setTableBusy(true);
    setNotice(null);
    setError(null);
    try {
      await deleteDbRow({ database: selectedDatabase, schema_name: selectedRelationSchemaName, relation_name: selectedRelationName, key: selectedRowKey });
      await loadTableDetail(selectedRelationName, selectedRelationSchemaName, selectedDatabase);
      setSelectedRowKey(null);
      setSelectedRowDraft("{}");
      setNotice(`Deleted a row from ${selectedRelationSchemaName}.${selectedRelationName}.`);
    } catch (mutationError) {
      setError(mutationError instanceof Error ? mutationError.message : "Failed to delete row");
    } finally {
      setTableBusy(false);
    }
  }

  async function handleCreateAccount() {
    setAccountBusy(true);
    setNotice(null);
    setError(null);
    try {
      await createAuthUser({
        email: createAccountForm.email,
        password: createAccountForm.password,
        display_name: createAccountForm.display_name || null,
        tenant_id: createAccountForm.tenant_id || "shared",
        role: createAccountForm.role,
        status: createAccountForm.status,
        permissions: createAccountForm.permissions,
      });
      await loadAccountsCatalog();
      setCreateAccountForm(createEmptyAccountForm(availableRolePresets));
      setNotice("Created a new login account.");
    } catch (requestError) {
      setError(requestError instanceof Error ? requestError.message : "Failed to create account");
    } finally {
      setAccountBusy(false);
    }
  }

  async function handleSaveAccount() {
    if (!selectedUserId) {
      return;
    }
    setAccountBusy(true);
    setNotice(null);
    setError(null);
    try {
      await updateAuthUser(selectedUserId, {
        email: editAccountForm.email,
        password: editAccountForm.password || null,
        display_name: editAccountForm.display_name,
        tenant_id: editAccountForm.tenant_id,
        role: editAccountForm.role,
        status: editAccountForm.status,
        permissions: editAccountForm.permissions,
      });
      await loadAccountsCatalog();
      await loadAccountDetail(selectedUserId);
      setNotice("Saved account changes.");
    } catch (requestError) {
      setError(requestError instanceof Error ? requestError.message : "Failed to save account");
    } finally {
      setAccountBusy(false);
    }
  }

  async function handleDeleteAccount() {
    if (!selectedUserId) {
      return;
    }
    if (!window.confirm("Delete this account and its login sessions?")) {
      return;
    }
    setAccountBusy(true);
    setNotice(null);
    setError(null);
    try {
      await deleteAuthUser(selectedUserId);
      await loadAccountsCatalog();
      setSelectedUserId(null);
      setSelectedUser(null);
      setSelectedUserSessions([]);
      setEditAccountForm(createEmptyAccountForm(availableRolePresets));
      setNotice("Deleted the account.");
    } catch (requestError) {
      setError(requestError instanceof Error ? requestError.message : "Failed to delete account");
    } finally {
      setAccountBusy(false);
    }
  }

  async function handleRevokeSessions() {
    if (!selectedUserId) {
      return;
    }
    setAccountBusy(true);
    setNotice(null);
    setError(null);
    try {
      await revokeAuthUserSessions(selectedUserId);
      await loadAccountDetail(selectedUserId);
      await loadAccountsCatalog();
      setNotice("Revoked active sessions for the selected account.");
    } catch (requestError) {
      setError(requestError instanceof Error ? requestError.message : "Failed to revoke sessions");
    } finally {
      setAccountBusy(false);
    }
  }

  async function handleSaveRateLimits() {
    setBusy(true);
    setNotice(null);
    setError(null);
    try {
      const payload = await saveSystemConfig("rate_limits", rateLimitDraft);
      setRateLimitPayload(payload);
      setRateLimitDraft({ ...payload.editable_config });
      setNotice("Saved rate-limit settings to the startup config.");
    } catch (requestError) {
      setError(requestError instanceof Error ? requestError.message : "Failed to save rate limits");
    } finally {
      setBusy(false);
    }
  }

  async function handleResetRateLimits() {
    setBusy(true);
    setNotice(null);
    setError(null);
    try {
      const payload = await resetSystemConfig("rate_limits");
      setRateLimitPayload(payload);
      setRateLimitDraft({ ...payload.editable_config });
      setNotice("Reset rate-limit settings to defaults.");
    } catch (requestError) {
      setError(requestError instanceof Error ? requestError.message : "Failed to reset rate limits");
    } finally {
      setBusy(false);
    }
  }

  async function handleSaveRuntimeConfig() {
    setBusy(true);
    setNotice(null);
    setError(null);
    try {
      const config = parseJsonObject("Agent runtime config", runtimeDraft);
      const payload = await saveAdminRuntimeConfig(tenantId || "shared", config);
      setRuntimeConfig(payload);
      setRuntimeDraft(prettyJson(payload.stored_override ?? payload.config ?? {}));
      setNotice("Saved tenant runtime config.");
    } catch (requestError) {
      setError(requestError instanceof Error ? requestError.message : "Failed to save runtime config");
    } finally {
      setBusy(false);
    }
  }

  async function handleResetRuntimeConfig() {
    setBusy(true);
    setNotice(null);
    setError(null);
    try {
      const payload = await resetAdminRuntimeConfig(tenantId || "shared");
      setRuntimeConfig(payload);
      setRuntimeDraft(prettyJson(payload.stored_override ?? payload.config ?? {}));
      setNotice("Reset tenant runtime config.");
    } catch (requestError) {
      setError(requestError instanceof Error ? requestError.message : "Failed to reset runtime config");
    } finally {
      setBusy(false);
    }
  }

  async function handleSaveSystemGroup() {
    setBusy(true);
    setNotice(null);
    setError(null);
    try {
      const config = parseJsonObject("System config", systemConfigDraft);
      const payload = await saveSystemConfig(systemGroup, config);
      setSystemConfigPayload(payload);
      setSystemConfigDraft(prettyJson(payload.editable_config ?? {}));
      setNotice(`Saved ${systemGroup} startup config.`);
    } catch (requestError) {
      setError(requestError instanceof Error ? requestError.message : "Failed to save system config");
    } finally {
      setBusy(false);
    }
  }

  async function handleResetSystemGroup() {
    setBusy(true);
    setNotice(null);
    setError(null);
    try {
      const payload = await resetSystemConfig(systemGroup);
      setSystemConfigPayload(payload);
      setSystemConfigDraft(prettyJson(payload.editable_config ?? {}));
      setNotice(`Reset ${systemGroup} startup config.`);
    } catch (requestError) {
      setError(requestError instanceof Error ? requestError.message : "Failed to reset system config");
    } finally {
      setBusy(false);
    }
  }

  async function handleRunSql() {
    setSqlBusy(true);
    setNotice(null);
    setError(null);
    try {
      const payload = await executeAdminSql(sqlDraft, selectedDatabase);
      setSqlResult(payload);
      setNotice(`Executed ${payload.statement_type.toUpperCase()} successfully.`);
      if (section === "tables" && selectedRelationName) {
        void loadTableDetail(selectedRelationName, selectedRelationSchemaName, selectedDatabase).catch(() => undefined);
      }
    } catch (requestError) {
      setError(requestError instanceof Error ? requestError.message : "Failed to execute SQL");
    } finally {
      setSqlBusy(false);
    }
  }

  const activeSection = sectionLabels.find((item) => item.id === section);
  const overviewEntries = Object.entries(dashboard?.overview ?? {});
  const metricEntries = Object.entries(dashboard?.metrics ?? {});
  const availableSystemGroups = Object.keys(systemConfigPayload?.groups ?? rateLimitPayload?.groups ?? {});
  const sessionPermissionSet = new Set((session?.user?.permissions ?? []).map((permission) => String(permission).trim().toLowerCase()));
  const availableRolePresets = accounts?.role_permission_presets ?? {};
  const availablePermissions = accounts?.available_permissions ?? Array.from(sessionPermissionSet);
  const availableRoles = accounts?.available_roles ?? fallbackRoleOptions;
  const availableStatuses = accounts?.available_statuses ?? fallbackStatusOptions;
  const usingToken = Boolean(tokenInput.trim());
  const hasConsoleAccess = usingToken || canAccessAdminConsole(sessionPermissionSet, availablePermissions);
  const visibleSections = sectionLabels.filter((item) => usingToken || canAccessSection(item.id, sessionPermissionSet, availablePermissions));
  const visibleSectionIds = visibleSections.map((item) => item.id).join("|");
  const firstVisibleSectionId = visibleSections[0]?.id;
  const selectedDatabaseMeta = adminDatabaseOptions.find((item) => item.id === selectedDatabase) ?? adminDatabaseOptions[0]!;
  const defaultSchemaName = defaultSchemaForDatabase(selectedDatabase);
  const canReadSelectedDatabase = usingToken || canReadDatabase(sessionPermissionSet, selectedDatabase);
  const canManageSelectedDatabase = usingToken || canWriteDatabase(sessionPermissionSet, selectedDatabase);
  const canManageAccounts = usingToken || hasPermission(sessionPermissionSet, "accounts.write");
  const canManageRates = usingToken || hasPermission(sessionPermissionSet, "rate_limits.write") || hasPermission(sessionPermissionSet, "runtime.write");
  const canManageRuntime = usingToken || hasPermission(sessionPermissionSet, "runtime.write");
  const canRunSql = usingToken || hasPermission(sessionPermissionSet, "db.sql.write");
  const selectedPrimaryKey = relationDetail?.primary_key ?? [];
  const selectedRow =
    relationDetail?.rows.find((row) =>
      selectedPrimaryKey.every((columnName) => row[columnName] === selectedRowKey?.[columnName]),
    ) ?? null;

  useEffect(() => {
    if (!firstVisibleSectionId) {
      return;
    }
    if (!visibleSections.some((item) => item.id === section)) {
      setSection(firstVisibleSectionId);
    }
  }, [section, visibleSectionIds, firstVisibleSectionId]);

  if (!ready) {
    return <section className="auth-screen"><div className="auth-card"><p>Loading operator access...</p></div></section>;
  }

  if (!hasConsoleAccess) {
    return (
      <section className="auth-screen">
        <div className="auth-card auth-card--form">
          <div className="eyebrow">Operator Access</div>
          <h1>Admin access is restricted.</h1>
          <p className="caption">
            Sign in with a role that has internal permissions, or use the break-glass admin token for direct control-plane access.
          </p>
          <label>
            Admin token (optional)
            <input
              type="password"
              value={tokenInput}
              onChange={(event) => setTokenInput(event.target.value)}
              placeholder="Optional break-glass token"
            />
          </label>
          {error ? <div className="notice notice--warn">{error}</div> : null}
          <div className="button-row">
            <button className="button button--primary" type="button" onClick={() => void bootstrap()}>
              Load with token
            </button>
          </div>
        </div>
      </section>
    );
  }

  return (
    <section className="admin-shell">
      <aside className="admin-sidebar">
        <div className="admin-brand">
          <div className="eyebrow">Operator frontend</div>
          <h1>Hive Control</h1>
          <p>Supabase-style admin console for application data, identity data, startup configuration, and rate limiting.</p>
        </div>

        <div className="admin-sidebar__panel">
          <label>
            Admin token (optional)
            <input
              type="password"
              value={tokenInput}
              onChange={(event) => setTokenInput(event.target.value)}
              placeholder="Optional break-glass token"
            />
          </label>
          <label>
            Tenant scope
            <input value={tenantId} onChange={(event) => setTenantId(event.target.value)} placeholder="shared" />
          </label>
          <div className="button-row">
            <button className="button button--primary" type="button" onClick={() => void bootstrap()} disabled={busy}>
              {busy ? "Loading..." : "Load console"}
            </button>
            <button
              className="button button--ghost"
              type="button"
              onClick={() => {
                clearAdminToken();
                setTokenInput("");
                setNotice(null);
                setError(null);
              }}
            >
              Clear token
            </button>
          </div>
        </div>

        <nav className="admin-nav" aria-label="Admin sections">
          {visibleSections.map((item) => (
            <button
              key={item.id}
              className={`admin-nav__item ${section === item.id ? "admin-nav__item--active" : ""}`}
              type="button"
              onClick={() => setSection(item.id)}
            >
              <strong>{item.label}</strong>
              <span>{item.detail}</span>
            </button>
          ))}
        </nav>

        <div className="admin-sidebar__panel admin-sidebar__panel--muted">
          <div className="mini-card__label">Storage split</div>
          <strong>App Postgres + Identity Postgres + Chroma</strong>
          <p className="caption">
            Application records live in the app database, login and permission data live in a dedicated identity database, and Chroma remains a derived vector index.
          </p>
          {session?.user ? <p className="caption">Signed in as {session.user.role}</p> : null}
        </div>
      </aside>

      <section className="admin-main">
        <header className="admin-topbar">
          <div>
            <div className="eyebrow">Internal console</div>
            <h2>{activeSection?.label}</h2>
            <p className="caption">{activeSection?.detail}</p>
          </div>
          <div className="admin-topbar__meta">
            <span className="pill">tenant {tenantId || "shared"}</span>
            {(section === "tables" || section === "sql") ? <span className="pill">{selectedDatabaseMeta.label}</span> : null}
            <span className="pill">{busy || tableBusy || accountBusy || sqlBusy ? "busy" : "idle"}</span>
            <a className="text-link" href="/admin/legacy" target="_blank" rel="noreferrer">
              Open legacy console
            </a>
          </div>
        </header>

        {error ? <div className="notice notice--warn">{error}</div> : null}
        {notice ? <div className="notice admin-notice">{notice}</div> : null}

        {section === "overview" ? (
          <div className="admin-section-grid">
            <article className="admin-panel">
              <div className="admin-panel__header">
                <div>
                  <div className="mini-card__label">Corpus + agent health</div>
                  <h3>Overview metrics</h3>
                </div>
              </div>
              <div className="admin-metric-grid">
                {overviewEntries.length ? (
                  overviewEntries.map(([key, value]) => (
                    <div key={key} className="admin-metric-card">
                      <span>{key.replace(/_/g, " ")}</span>
                      <strong>{formatValue(value)}</strong>
                    </div>
                  ))
                ) : (
                  <div className="empty-state">Load the console to see persisted platform metrics.</div>
                )}
              </div>
            </article>

            <article className="admin-panel">
              <div className="admin-panel__header">
                <div>
                  <div className="mini-card__label">Model runtime</div>
                  <h3>Agent quality signals</h3>
                </div>
              </div>
              <div className="admin-metric-grid admin-metric-grid--compact">
                {metricEntries.length ? (
                  metricEntries.map(([key, value]) => (
                    <div key={key} className="admin-metric-card admin-metric-card--soft">
                      <span>{key.replace(/_/g, " ")}</span>
                      <strong>{formatValue(value)}</strong>
                    </div>
                  ))
                ) : (
                  <div className="empty-state">No agent metrics available yet.</div>
                )}
              </div>
            </article>

            <article className="admin-panel admin-panel--wide">
              <div className="admin-panel__header">
                <div>
                  <div className="mini-card__label">Recent content</div>
                  <h3>Documents</h3>
                </div>
                <span className="pill">{dashboard?.documents.length ?? 0} shown</span>
              </div>
              <div className="table-wrap admin-table-wrap">
                <table className="surface-table">
                  <thead>
                    <tr>
                      <th>Document</th>
                      <th>Tenant</th>
                      <th>Status</th>
                      <th>Chunks</th>
                      <th>Created</th>
                    </tr>
                  </thead>
                  <tbody>
                    {dashboard?.documents.length ? (
                      dashboard.documents.map((row, index) => (
                        <tr key={String(row.document_id ?? index)}>
                          <td>{formatValue(row.filename)}</td>
                          <td>{formatValue(row.tenant_id)}</td>
                          <td>{formatValue(row.status)}</td>
                          <td>{formatValue(row.total_chunks)}</td>
                          <td>{formatValue(row.created_at)}</td>
                        </tr>
                      ))
                    ) : (
                      <tr>
                        <td className="empty-cell" colSpan={5}>
                          No documents loaded.
                        </td>
                      </tr>
                    )}
                  </tbody>
                </table>
              </div>
            </article>

            <article className="admin-panel admin-panel--wide">
              <div className="admin-panel__header">
                <div>
                  <div className="mini-card__label">Control-plane map</div>
                  <h3>Routes</h3>
                </div>
                <span className="pill">{dashboard?.routes.length ?? 0} routes</span>
              </div>
              <div className="admin-route-list">
                {dashboard?.routes.length ? (
                  dashboard.routes.map((route) => (
                    <div key={`${route.path}:${route.methods.join(",")}`} className="route-card admin-route-card">
                      <div className="admin-route-card__title">{route.path}</div>
                      <div className="admin-route-card__meta">
                        {route.methods.map((method) => (
                          <span key={method} className="pill">
                            {method}
                          </span>
                        ))}
                      </div>
                    </div>
                  ))
                ) : (
                  <div className="empty-state">No routes loaded yet.</div>
                )}
              </div>
            </article>
          </div>
        ) : null}

        {(["corpus", "chunks", "kg", "chroma", "agent", "operations"] as AdminExtendedSection[]).includes(section as AdminExtendedSection) ? (
          <AdminExtendedSections
            section={section as AdminExtendedSection}
            usingToken={usingToken}
            permissions={sessionPermissionSet}
            tenantId={tenantId}
          />
        ) : null}

        {section === "tables" ? (
          <div className="admin-section-grid admin-section-grid--tables">
            <article className="admin-panel admin-panel--sidebar">
              <div className="admin-panel__header">
                <div>
                  <div className="mini-card__label">Catalog</div>
                  <h3>Relations</h3>
                </div>
                <span className="pill">{relations.length}</span>
              </div>
              <label>
                Database
                <select value={selectedDatabase} onChange={(event) => setSelectedDatabase(event.target.value as AdminDatabase)}>
                  {adminDatabaseOptions.map((database) => (
                    <option key={database.id} value={database.id} disabled={!usingToken && !canReadDatabase(sessionPermissionSet, database.id)}>
                      {database.label}
                    </option>
                  ))}
                </select>
              </label>
              <p className="caption">{selectedDatabaseMeta.detail}</p>
              <label>
                Search relation
                <input
                  value={relationSearch}
                  onChange={(event) => setRelationSearch(event.target.value)}
                  placeholder={selectedDatabase === "identity" ? "auth_users, auth_sessions" : "documents, agent_query_runs, kg_*"}
                />
              </label>
              <div className="admin-list">
                {!canReadSelectedDatabase ? (
                  <div className="empty-state">This session cannot browse the selected database.</div>
                ) : relations.length ? (
                  relations.map((relation) => (
                    <button
                      key={`${relation.schema_name}.${relation.relation_name}`}
                      className={`admin-list-button ${selectedRelationName === relation.relation_name && selectedRelationSchemaName === relation.schema_name ? "admin-list-button--active" : ""}`}
                      type="button"
                      onClick={() => {
                        setSelectedRelationName(relation.relation_name);
                        setSelectedRelationSchemaName(relation.schema_name || defaultSchemaName);
                      }}
                    >
                      <strong>{relation.schema_name}.{relation.relation_name}</strong>
                      <span>{relation.relation_type}</span>
                      <span>{relation.estimated_rows} rows</span>
                    </button>
                  ))
                ) : (
                  <div className="empty-state">No relations matched the current search.</div>
                )}
              </div>
            </article>

            <article className="admin-panel admin-panel--wide">
              <div className="admin-panel__header">
                <div>
                  <div className="mini-card__label">Table editor</div>
                  <h3>{relationDetail ? `${relationDetail.schema_name}.${relationDetail.relation_name}` : "Select a relation"}</h3>
                </div>
                {relationDetail ? (
                  <div className="admin-inline-pills">
                    <span className="pill">{selectedDatabaseMeta.label}</span>
                    <span className="pill">{relationDetail.schema_name}</span>
                    <span className="pill">{relationDetail.relation_type}</span>
                    <span className="pill">{relationDetail.total} total rows</span>
                    <span className="pill">pk {relationDetail.primary_key.join(", ") || "none"}</span>
                  </div>
                ) : null}
              </div>

              {relationDetail?.redacted_columns?.length ? (
                <div className="notice admin-notice">
                  Redacted or sensitive columns are blocked from the generic editor: {relationDetail.redacted_columns.join(", ")}.
                </div>
              ) : null}

              <div className="admin-subgrid">
                <div className="admin-subpanel">
                  <div className="mini-card__label">Insert row</div>
                  <textarea
                    rows={8}
                    value={newRowDraft}
                    onChange={(event) => setNewRowDraft(event.target.value)}
                    placeholder='{"tenant_id":"shared","status":"active"}'
                  />
                  <div className="button-row">
                    <button
                      className="button button--primary"
                      type="button"
                      onClick={() => void handleInsertRow()}
                      disabled={!relationDetail || tableBusy || !canManageSelectedDatabase}
                    >
                      {tableBusy ? "Saving..." : "Insert row"}
                    </button>
                    <button className="button button--ghost" type="button" onClick={() => setNewRowDraft("{}")}>
                      Clear
                    </button>
                  </div>
                </div>

                <div className="admin-subpanel">
                  <div className="mini-card__label">Selected row</div>
                  {selectedRowKey ? (
                    <>
                      <div className="admin-inline-pills">
                        {Object.entries(selectedRowKey).map(([key, value]) => (
                          <span key={key} className="pill">
                            {key}: {shortId(value)}
                          </span>
                        ))}
                      </div>
                      {selectedRow ? <p className="caption">Loaded from the current page snapshot for inline editing.</p> : null}
                      <textarea rows={10} value={selectedRowDraft} onChange={(event) => setSelectedRowDraft(event.target.value)} />
                      <div className="button-row">
                        <button className="button button--primary" type="button" onClick={() => void handleSaveRow()} disabled={tableBusy || !canManageSelectedDatabase}>
                          Save row
                        </button>
                        <button className="button" type="button" onClick={() => void handleDeleteRow()} disabled={tableBusy || !canManageSelectedDatabase}>
                          Delete row
                        </button>
                      </div>
                    </>
                  ) : (
                    <div className="empty-state">Select a row from the table preview to edit it.</div>
                  )}
                </div>
              </div>

              <div className="table-wrap admin-table-wrap">
                <table className="surface-table surface-table--dense">
                  <thead>
                    <tr>
                      <th>Pick</th>
                      {relationDetail?.columns.map((column) => (
                        <th key={column.column_name}>{column.column_name}</th>
                      ))}
                    </tr>
                  </thead>
                  <tbody>
                    {relationDetail?.rows.length ? (
                      relationDetail.rows.map((row, index) => {
                        const key = buildPrimaryKey(relationDetail, row);
                        const isActive =
                          key !== null &&
                          Object.entries(key).every(([columnName, value]) => selectedRowKey?.[columnName] === value);
                        return (
                          <tr key={JSON.stringify(key) || String(index)} className={isActive ? "admin-table-row--active" : ""}>
                            <td>
                              <button
                                className="button button--ghost admin-row-picker"
                                type="button"
                                onClick={() => {
                                  if (!key) {
                                    setError("This relation has no primary key. Use the SQL editor for advanced mutations.");
                                    return;
                                  }
                                  setSelectedRowKey(key);
                                  setSelectedRowDraft(editableRowDraft(relationDetail, row));
                                }}
                              >
                                Open
                              </button>
                            </td>
                            {relationDetail.columns.map((column) => (
                              <td key={`${JSON.stringify(key) || index}:${column.column_name}`}>
                                <span className="admin-cell">{formatValue(row[column.column_name])}</span>
                              </td>
                            ))}
                          </tr>
                        );
                      })
                    ) : (
                      <tr>
                        <td className="empty-cell" colSpan={(relationDetail?.columns.length ?? 0) + 1}>
                          No rows found for this relation.
                        </td>
                      </tr>
                    )}
                  </tbody>
                </table>
              </div>
            </article>
          </div>
        ) : null}

        {section === "accounts" ? (
          <div className="admin-section-grid admin-section-grid--accounts">
            <article className="admin-panel admin-panel--sidebar">
              <div className="admin-panel__header">
                <div>
                  <div className="mini-card__label">Identity store</div>
                  <h3>Accounts</h3>
                </div>
                <span className="pill">{accounts?.total ?? 0}</span>
              </div>
              <label>
                Search accounts
                <input
                  value={accountSearch}
                  onChange={(event) => setAccountSearch(event.target.value)}
                  placeholder="email or display name"
                />
              </label>
              <div className="admin-list">
                {accounts?.items.length ? (
                  accounts.items.map((account) => (
                    <button
                      key={account.user_id}
                      className={`admin-list-button ${selectedUserId === account.user_id ? "admin-list-button--active" : ""}`}
                      type="button"
                      onClick={() => setSelectedUserId(account.user_id)}
                    >
                      <strong>{account.display_name || account.email}</strong>
                      <span>{account.email}</span>
                      <span>
                        {account.role} / {account.status}
                      </span>
                    </button>
                  ))
                ) : (
                  <div className="empty-state">No accounts found in the current auth store.</div>
                )}
              </div>
            </article>

            <article className="admin-panel">
              <div className="admin-panel__header">
                <div>
                  <div className="mini-card__label">Provisioning</div>
                  <h3>Create account</h3>
                </div>
                <span className="pill">Dedicated identity Postgres</span>
              </div>
              <div className="admin-form-grid">
                <label>
                  Email
                  <input
                    value={createAccountForm.email}
                    onChange={(event) => setCreateAccountForm((current) => ({ ...current, email: event.target.value }))}
                    placeholder="operator@example.com"
                  />
                </label>
                <label>
                  Display name
                  <input
                    value={createAccountForm.display_name}
                    onChange={(event) => setCreateAccountForm((current) => ({ ...current, display_name: event.target.value }))}
                    placeholder="Operations lead"
                  />
                </label>
                <label>
                  Password
                  <input
                    type="password"
                    value={createAccountForm.password}
                    onChange={(event) => setCreateAccountForm((current) => ({ ...current, password: event.target.value }))}
                    placeholder="Set an initial password"
                  />
                </label>
                <label>
                  Tenant ID
                  <input
                    value={createAccountForm.tenant_id}
                    onChange={(event) => setCreateAccountForm((current) => ({ ...current, tenant_id: event.target.value }))}
                    placeholder="shared"
                  />
                </label>
                <label>
                  Role
                  <select
                    value={createAccountForm.role}
                    onChange={(event) =>
                      setCreateAccountForm((current) => ({
                        ...current,
                        role: event.target.value,
                        permissions: [...(availableRolePresets[event.target.value] || [])],
                      }))
                    }
                  >
                    {availableRoles.map((role) => (
                      <option key={role} value={role}>
                        {role}
                      </option>
                    ))}
                  </select>
                </label>
                <label>
                  Status
                  <select
                    value={createAccountForm.status}
                    onChange={(event) => setCreateAccountForm((current) => ({ ...current, status: event.target.value }))}
                  >
                    {availableStatuses.map((status) => (
                      <option key={status} value={status}>
                        {status}
                      </option>
                    ))}
                  </select>
                </label>
                <div className="admin-permission-panel">
                  <div className="mini-card__label">Permissions</div>
                  <div className="admin-permission-grid">
                    {availablePermissions.map((permission) => (
                      <label key={permission} className="admin-checkbox">
                        <input
                          type="checkbox"
                          checked={createAccountForm.permissions.includes(permission)}
                          onChange={() =>
                            setCreateAccountForm((current) => ({
                              ...current,
                              permissions: togglePermissionValue(current.permissions, permission),
                            }))
                          }
                        />
                        <span>{permission}</span>
                      </label>
                    ))}
                  </div>
                </div>
              </div>
              <div className="button-row">
                <button className="button button--primary" type="button" onClick={() => void handleCreateAccount()} disabled={accountBusy || !canManageAccounts}>
                  {accountBusy ? "Saving..." : "Create account"}
                </button>
                <button className="button button--ghost" type="button" onClick={() => setCreateAccountForm(createEmptyAccountForm(availableRolePresets))}>
                  Reset
                </button>
              </div>
            </article>

            <article className="admin-panel">
              <div className="admin-panel__header">
                <div>
                  <div className="mini-card__label">Selected account</div>
                  <h3>{selectedUser?.display_name || selectedUser?.email || "Choose an account"}</h3>
                </div>
                {selectedUser ? (
                  <div className="admin-inline-pills">
                    <span className="pill">{selectedUser.role}</span>
                    <span className="pill">{selectedUser.status}</span>
                    <span className="pill">{selectedUser.active_sessions ?? 0} active sessions</span>
                  </div>
                ) : null}
              </div>

              {selectedUser ? (
                <>
                  <div className="admin-form-grid">
                    <label>
                      Email
                      <input
                        value={editAccountForm.email}
                        onChange={(event) => setEditAccountForm((current) => ({ ...current, email: event.target.value }))}
                      />
                    </label>
                    <label>
                      Display name
                      <input
                        value={editAccountForm.display_name}
                        onChange={(event) => setEditAccountForm((current) => ({ ...current, display_name: event.target.value }))}
                      />
                    </label>
                    <label>
                      New password
                      <input
                        type="password"
                        value={editAccountForm.password}
                        onChange={(event) => setEditAccountForm((current) => ({ ...current, password: event.target.value }))}
                        placeholder="Leave blank to keep current password"
                      />
                    </label>
                    <label>
                      Tenant ID
                      <input
                        value={editAccountForm.tenant_id}
                        onChange={(event) => setEditAccountForm((current) => ({ ...current, tenant_id: event.target.value }))}
                      />
                    </label>
                    <label>
                      Role
                      <select
                        value={editAccountForm.role}
                        onChange={(event) =>
                          setEditAccountForm((current) => ({
                            ...current,
                            role: event.target.value,
                            permissions: [...(availableRolePresets[event.target.value] || [])],
                          }))
                        }
                      >
                        {availableRoles.map((role) => (
                          <option key={role} value={role}>
                            {role}
                          </option>
                        ))}
                      </select>
                    </label>
                    <label>
                      Status
                      <select
                        value={editAccountForm.status}
                        onChange={(event) => setEditAccountForm((current) => ({ ...current, status: event.target.value }))}
                      >
                        {availableStatuses.map((status) => (
                          <option key={status} value={status}>
                            {status}
                          </option>
                        ))}
                      </select>
                    </label>
                    <div className="admin-permission-panel">
                      <div className="mini-card__label">Permissions</div>
                      <div className="admin-permission-grid">
                        {availablePermissions.map((permission) => (
                          <label key={permission} className="admin-checkbox">
                            <input
                              type="checkbox"
                              checked={editAccountForm.permissions.includes(permission)}
                              onChange={() =>
                                setEditAccountForm((current) => ({
                                  ...current,
                                  permissions: togglePermissionValue(current.permissions, permission),
                                }))
                              }
                            />
                            <span>{permission}</span>
                          </label>
                        ))}
                      </div>
                    </div>
                  </div>
                  <div className="button-row">
                    <button className="button button--primary" type="button" onClick={() => void handleSaveAccount()} disabled={accountBusy || !canManageAccounts}>
                      Save account
                    </button>
                    <button className="button" type="button" onClick={() => void handleRevokeSessions()} disabled={accountBusy || !canManageAccounts}>
                      Revoke sessions
                    </button>
                    <button className="button button--ghost" type="button" onClick={() => void handleDeleteAccount()} disabled={accountBusy || !canManageAccounts}>
                      Delete account
                    </button>
                  </div>

                  <div className="admin-subpanel">
                    <div className="mini-card__label">Sessions</div>
                    <div className="table-wrap admin-table-wrap">
                      <table className="surface-table surface-table--dense">
                        <thead>
                          <tr>
                            <th>Session</th>
                            <th>Active</th>
                            <th>Created</th>
                            <th>Last seen</th>
                            <th>Expires</th>
                          </tr>
                        </thead>
                        <tbody>
                          {selectedUserSessions.length ? (
                            selectedUserSessions.map((sessionRecord) => (
                              <tr key={String(sessionRecord.auth_session_id)}>
                                <td>{shortId(sessionRecord.auth_session_id)}</td>
                                <td>{formatValue(sessionRecord.active)}</td>
                                <td>{formatValue(sessionRecord.created_at)}</td>
                                <td>{formatValue(sessionRecord.last_seen_at)}</td>
                                <td>{formatValue(sessionRecord.expires_at)}</td>
                              </tr>
                            ))
                          ) : (
                            <tr>
                              <td className="empty-cell" colSpan={5}>
                                No sessions recorded for this account.
                              </td>
                            </tr>
                          )}
                        </tbody>
                      </table>
                    </div>
                  </div>
                </>
              ) : (
                <div className="empty-state">Select an account to update its role, tenant, password, and active sessions.</div>
              )}
            </article>
          </div>
        ) : null}

        {section === "limits" ? (
          <div className="admin-section-grid">
            <article className="admin-panel">
              <div className="admin-panel__header">
                <div>
                  <div className="mini-card__label">Abuse protection</div>
                  <h3>Rate limits</h3>
                </div>
              </div>
              <div className="admin-metric-grid admin-metric-grid--compact">
                {Object.entries(rateLimitDraft).map(([key, value]) => (
                  <div key={key} className="admin-metric-card admin-metric-card--soft">
                    <span>{key.replace(/_/g, " ")}</span>
                    <strong>{formatValue(value)}</strong>
                  </div>
                ))}
              </div>
              <div className="admin-form-grid">
                {Object.entries(rateLimitDraft).map(([key, value]) => (
                  <label key={key}>
                    {key.replace(/_/g, " ")}
                    <input
                      type="number"
                      value={String(value ?? "")}
                      onChange={(event) =>
                        setRateLimitDraft((current) => ({
                          ...current,
                          [key]: Number(event.target.value),
                        }))
                      }
                    />
                  </label>
                ))}
              </div>
              <div className="button-row">
                <button className="button button--primary" type="button" onClick={() => void handleSaveRateLimits()} disabled={busy || !canManageRates}>
                  Save rate limits
                </button>
                <button className="button button--ghost" type="button" onClick={() => void handleResetRateLimits()} disabled={busy || !canManageRates}>
                  Reset defaults
                </button>
              </div>
            </article>

            <article className="admin-panel">
              <div className="admin-panel__header">
                <div>
                  <div className="mini-card__label">Runtime note</div>
                  <h3>How these limits apply</h3>
                </div>
              </div>
              <dl className="fact-list">
                <div>
                  <dt>Login endpoint</dt>
                  <dd>/auth/login is throttled per client identity to slow brute-force attempts.</dd>
                </div>
                <div>
                  <dt>Public chat and query</dt>
                  <dd>/agent/chat and /agent/query are throttled per authenticated public user session.</dd>
                </div>
                <div>
                  <dt>Persistence</dt>
                  <dd>{rateLimitPayload?.note || "These values are written to the startup config and take effect after restart."}</dd>
                </div>
                <div>
                  <dt>Env file</dt>
                  <dd>{rateLimitPayload?.env_path || "Not available"}</dd>
                </div>
              </dl>
            </article>
          </div>
        ) : null}

        {section === "runtime" ? (
          <div className="admin-section-grid">
            <article className="admin-panel admin-panel--wide">
              <div className="admin-panel__header">
                <div>
                  <div className="mini-card__label">Tenant runtime</div>
                  <h3>Agent override JSON</h3>
                </div>
                <div className="admin-inline-pills">
                  <span className="pill">tenant {runtimeConfig?.tenant_id || tenantId || "shared"}</span>
                  <span className="pill">api key {runtimeConfig?.effective_api_key_source || "default"}</span>
                </div>
              </div>
              <div className="admin-metadata-grid">
                <div className="admin-metric-card admin-metric-card--soft">
                  <span>Updated by</span>
                  <strong>{runtimeConfig?.updated_by || "n/a"}</strong>
                </div>
                <div className="admin-metric-card admin-metric-card--soft">
                  <span>Updated at</span>
                  <strong>{runtimeConfig?.updated_at || "n/a"}</strong>
                </div>
                <div className="admin-metric-card admin-metric-card--soft">
                  <span>API key override</span>
                  <strong>{runtimeConfig?.has_api_key_override ? "present" : "not set"}</strong>
                </div>
              </div>
              <textarea rows={18} value={runtimeDraft} onChange={(event) => setRuntimeDraft(event.target.value)} />
              <div className="button-row">
                <button className="button button--primary" type="button" onClick={() => void handleSaveRuntimeConfig()} disabled={busy || !canManageRuntime}>
                  Save tenant config
                </button>
                <button className="button button--ghost" type="button" onClick={() => void handleResetRuntimeConfig()} disabled={busy || !canManageRuntime}>
                  Reset tenant config
                </button>
              </div>
            </article>

            <article className="admin-panel">
              <div className="admin-panel__header">
                <div>
                  <div className="mini-card__label">Startup config</div>
                  <h3>System group editor</h3>
                </div>
              </div>
              <label>
                Config group
                <select value={systemGroup} onChange={(event) => setSystemGroup(event.target.value)}>
                  {availableSystemGroups.map((group) => (
                    <option key={group} value={group}>
                      {group}
                    </option>
                  ))}
                </select>
              </label>
              <p className="caption">{systemConfigPayload?.note}</p>
              <p className="caption">Env file: {systemConfigPayload?.env_path || "Not available"}</p>
              <textarea rows={18} value={systemConfigDraft} onChange={(event) => setSystemConfigDraft(event.target.value)} />
              <div className="button-row">
                <button className="button button--primary" type="button" onClick={() => void handleSaveSystemGroup()} disabled={busy || !canManageRuntime}>
                  Save group
                </button>
                <button className="button button--ghost" type="button" onClick={() => void handleResetSystemGroup()} disabled={busy || !canManageRuntime}>
                  Reset group
                </button>
              </div>
            </article>
          </div>
        ) : null}

        {section === "sql" ? (
          <div className="admin-section-grid">
            <article className="admin-panel admin-panel--wide">
              <div className="admin-panel__header">
                <div>
                  <div className="mini-card__label">Advanced Postgres access</div>
                  <h3>SQL editor</h3>
                </div>
                <div className="admin-inline-pills">
                  <span className="pill">{selectedDatabaseMeta.label}</span>
                  <span className="pill">single statement only</span>
                  <span className="pill">max 250 rows returned</span>
                </div>
              </div>
              <label>
                Database
                <select value={selectedDatabase} onChange={(event) => setSelectedDatabase(event.target.value as AdminDatabase)}>
                  {adminDatabaseOptions.map((database) => (
                    <option key={database.id} value={database.id} disabled={!usingToken && !canReadDatabase(sessionPermissionSet, database.id)}>
                      {database.label}
                    </option>
                  ))}
                </select>
              </label>
              <div className="button-row">
                <button
                  className="button button--ghost"
                  type="button"
                  onClick={() =>
                    setSqlDraft(
                      `SELECT table_name FROM information_schema.tables WHERE table_schema = '${selectedRelationSchemaName || defaultSchemaName}' ORDER BY table_name`,
                    )
                  }
                >
                  List tables
                </button>
                <button
                  className="button button--ghost"
                  type="button"
                  onClick={() =>
                    setSqlDraft(
                      selectedRelationName
                        ? `SELECT column_name, data_type, is_nullable FROM information_schema.columns WHERE table_schema = '${selectedRelationSchemaName || defaultSchemaName}' AND table_name = '${selectedRelationName}' ORDER BY ordinal_position`
                        : `SELECT column_name, data_type, is_nullable FROM information_schema.columns WHERE table_schema = '${selectedRelationSchemaName || defaultSchemaName}' ORDER BY table_name, ordinal_position`,
                    )
                  }
                >
                  Inspect columns
                </button>
                <button
                  className="button button--ghost"
                  type="button"
                  onClick={() =>
                    setSqlDraft(
                      `CREATE TABLE IF NOT EXISTS ${selectedRelationSchemaName || defaultSchemaName}.sample_admin_table (\n  id bigint GENERATED BY DEFAULT AS IDENTITY PRIMARY KEY,\n  tenant_id text NOT NULL DEFAULT 'shared',\n  created_at timestamptz NOT NULL DEFAULT now()\n)`,
                    )
                  }
                >
                  Create table template
                </button>
              </div>
              <textarea rows={18} value={sqlDraft} onChange={(event) => setSqlDraft(event.target.value)} />
              <div className="button-row">
                <button className="button button--primary" type="button" onClick={() => void handleRunSql()} disabled={sqlBusy || !canRunSql}>
                  {sqlBusy ? "Running..." : "Run SQL"}
                </button>
                <button className="button button--ghost" type="button" onClick={() => setSqlDraft(createSqlStarter())}>
                  Reset editor
                </button>
              </div>
            </article>

            <article className="admin-panel">
              <div className="admin-panel__header">
                <div>
                  <div className="mini-card__label">Result</div>
                  <h3>{sqlResult ? sqlResult.statement_type.toUpperCase() : "Waiting for execution"}</h3>
                </div>
                {sqlResult ? <span className="pill">{sqlResult.row_count} rows</span> : null}
              </div>
              {sqlResult ? (
                <>
                  <div className="admin-inline-pills">
                    {sqlResult.truncated ? <span className="pill">truncated</span> : null}
                    <span className="pill">{sqlResult.columns.length ? "query result" : "mutation result"}</span>
                  </div>
                  {sqlResult.columns.length ? (
                    <div className="table-wrap admin-table-wrap">
                      <table className="surface-table surface-table--dense">
                        <thead>
                          <tr>
                            {sqlResult.columns.map((column) => (
                              <th key={column}>{column}</th>
                            ))}
                          </tr>
                        </thead>
                        <tbody>
                          {sqlResult.rows.map((row, index) => (
                            <tr key={index}>
                              {sqlResult.columns.map((column) => (
                                <td key={`${index}:${column}`}>{formatValue(row[column])}</td>
                              ))}
                            </tr>
                          ))}
                        </tbody>
                      </table>
                    </div>
                  ) : (
                    <div className="empty-state">Statement executed successfully. {sqlResult.row_count} rows were affected.</div>
                  )}
                </>
              ) : (
                <div className="empty-state">Run a query here for schema changes, migrations, or deeper inspection.</div>
              )}
            </article>
          </div>
        ) : null}
      </section>
    </section>
  );
}
