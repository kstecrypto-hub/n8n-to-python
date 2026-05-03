import {
  startTransition,
  useDeferredValue,
  useEffect,
  useState,
  type Dispatch,
  type SetStateAction,
} from "react";
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
  type AdminSqlResponse,
  type AuthSessionRecord,
  type AuthUser,
  type AuthUsersResponse,
  type DbRelationDetail,
  type DbRelationSummary,
  type SystemConfigPayload,
} from "@/lib/api/admin";
import { useAuth } from "@/lib/auth/authContext";
import {
  clearAdminToken,
  loadAdminToken,
  saveAdminToken,
} from "@/lib/auth/adminToken";
import {
  createEmptyAccountForm,
  createSqlStarter,
  editableRowDraft,
  parseJsonObject,
  prettyJson,
} from "@/features/admin/adminDisplay";
import {
  adminDatabaseOptions,
  extendedAdminSections,
  fallbackRoleOptions,
  fallbackStatusOptions,
  sectionLabels,
  type AccountFormState,
  type AdminDatabase,
  type AdminDatabaseOption,
  type AdminDatabaseSelectOption,
  type AdminExtendedSection,
  type AdminSection,
  type AdminSectionDefinition,
  type DashboardSnapshot,
} from "@/features/admin/adminModels";
import {
  canAccessAdminConsole,
  canAccessSection,
  canReadDatabase,
  canWriteDatabase,
  defaultSchemaForDatabase,
  hasPermission,
} from "@/features/admin/adminPermissions";

type StateSetter<T> = Dispatch<SetStateAction<T>>;

export interface AdminOverviewState {
  dashboard: DashboardSnapshot | null;
  overviewEntries: Array<[string, unknown]>;
  metricEntries: Array<[string, unknown]>;
}

export interface AdminTablesState {
  databaseOptions: AdminDatabaseSelectOption[];
  selectedDatabase: AdminDatabase;
  setSelectedDatabase: StateSetter<AdminDatabase>;
  selectedDatabaseMeta: AdminDatabaseOption;
  defaultSchemaName: string;
  relationSearch: string;
  setRelationSearch: StateSetter<string>;
  relations: DbRelationSummary[];
  selectedRelationName: string | null;
  selectedRelationSchemaName: string;
  relationDetail: DbRelationDetail | null;
  selectedRowKey: Record<string, unknown> | null;
  selectedRow: Record<string, unknown> | null;
  selectedRowDraft: string;
  setSelectedRowDraft: StateSetter<string>;
  newRowDraft: string;
  setNewRowDraft: StateSetter<string>;
  tableBusy: boolean;
  canReadSelectedDatabase: boolean;
  canManageSelectedDatabase: boolean;
  selectRelation: (relationName: string, schemaName: string) => void;
  selectRow: (row: Record<string, unknown>) => void;
  insertRow: () => Promise<void>;
  saveRow: () => Promise<void>;
  deleteRow: () => Promise<void>;
}

export interface AdminAccountsState {
  accountSearch: string;
  setAccountSearch: StateSetter<string>;
  accounts: AuthUsersResponse | null;
  selectedUserId: string | null;
  setSelectedUserId: StateSetter<string | null>;
  selectedUser: AuthUser | null;
  selectedUserSessions: AuthSessionRecord[];
  accountBusy: boolean;
  createAccountForm: AccountFormState;
  setCreateAccountForm: StateSetter<AccountFormState>;
  editAccountForm: AccountFormState;
  setEditAccountForm: StateSetter<AccountFormState>;
  availablePermissions: string[];
  availableRoles: string[];
  availableStatuses: string[];
  availableRolePresets: Record<string, string[]>;
  canManageAccounts: boolean;
  createAccount: () => Promise<void>;
  saveAccount: () => Promise<void>;
  deleteAccount: () => Promise<void>;
  revokeSessions: () => Promise<void>;
  resetCreateAccountForm: () => void;
}

export interface AdminRateLimitsState {
  rateLimitPayload: SystemConfigPayload | null;
  rateLimitDraft: Record<string, unknown>;
  setRateLimitDraft: StateSetter<Record<string, unknown>>;
  busy: boolean;
  canManageRates: boolean;
  saveRateLimits: () => Promise<void>;
  resetRateLimits: () => Promise<void>;
}

export interface AdminRuntimeState {
  runtimeConfig: AdminConfigResponse | null;
  runtimeDraft: string;
  setRuntimeDraft: StateSetter<string>;
  systemGroup: string;
  setSystemGroup: StateSetter<string>;
  systemConfigPayload: SystemConfigPayload | null;
  systemConfigDraft: string;
  setSystemConfigDraft: StateSetter<string>;
  availableSystemGroups: string[];
  busy: boolean;
  canManageRuntime: boolean;
  saveRuntimeConfig: () => Promise<void>;
  resetRuntimeConfig: () => Promise<void>;
  saveSystemGroup: () => Promise<void>;
  resetSystemGroup: () => Promise<void>;
}

export interface AdminSqlState {
  databaseOptions: AdminDatabaseSelectOption[];
  selectedDatabase: AdminDatabase;
  setSelectedDatabase: StateSetter<AdminDatabase>;
  selectedDatabaseMeta: AdminDatabaseOption;
  selectedRelationName: string | null;
  selectedRelationSchemaName: string;
  defaultSchemaName: string;
  sqlDraft: string;
  setSqlDraft: StateSetter<string>;
  sqlResult: AdminSqlResponse | null;
  sqlBusy: boolean;
  canRunSql: boolean;
  runSql: () => Promise<void>;
  resetSqlDraft: () => void;
}

export interface AdminConsoleState {
  ready: boolean;
  section: AdminSection;
  setSection: StateSetter<AdminSection>;
  tokenInput: string;
  setTokenInput: StateSetter<string>;
  tenantId: string;
  setTenantId: StateSetter<string>;
  busy: boolean;
  error: string | null;
  notice: string | null;
  usingToken: boolean;
  hasConsoleAccess: boolean;
  activeSection: AdminSectionDefinition | undefined;
  visibleSections: AdminSectionDefinition[];
  sessionPermissionSet: Set<string>;
  sessionUserRole: string | null;
  selectedDatabaseMeta: AdminDatabaseOption;
  activityLabel: "busy" | "idle";
  bootstrap: (sectionId?: AdminSection) => Promise<void>;
  clearToken: () => void;
  isExtendedSection: (section: AdminSection) => section is AdminExtendedSection;
  overview: AdminOverviewState;
  tables: AdminTablesState;
  accounts: AdminAccountsState;
  limits: AdminRateLimitsState;
  runtime: AdminRuntimeState;
  sql: AdminSqlState;
}

export function useAdminConsoleState(): AdminConsoleState {
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
  const [selectedRelationName, setSelectedRelationName] = useState<string | null>(
    null,
  );
  const [selectedRelationSchemaName, setSelectedRelationSchemaName] =
    useState<string>("public");
  const [relationDetail, setRelationDetail] = useState<DbRelationDetail | null>(
    null,
  );
  const [selectedRowKey, setSelectedRowKey] = useState<Record<string, unknown> | null>(
    null,
  );
  const [selectedRowDraft, setSelectedRowDraft] = useState("{}");
  const [newRowDraft, setNewRowDraft] = useState("{}");
  const [tableBusy, setTableBusy] = useState(false);

  const [accountSearch, setAccountSearch] = useState("");
  const deferredAccountSearch = useDeferredValue(accountSearch);
  const [accounts, setAccounts] = useState<AuthUsersResponse | null>(null);
  const [selectedUserId, setSelectedUserId] = useState<string | null>(null);
  const [selectedUser, setSelectedUser] = useState<AuthUser | null>(null);
  const [selectedUserSessions, setSelectedUserSessions] = useState<
    AuthSessionRecord[]
  >([]);
  const [accountBusy, setAccountBusy] = useState(false);
  const [createAccountForm, setCreateAccountForm] = useState<AccountFormState>(() =>
    createEmptyAccountForm(),
  );
  const [editAccountForm, setEditAccountForm] = useState<AccountFormState>(() =>
    createEmptyAccountForm(),
  );

  const [rateLimitPayload, setRateLimitPayload] =
    useState<SystemConfigPayload | null>(null);
  const [rateLimitDraft, setRateLimitDraft] = useState<Record<string, unknown>>(
    {},
  );

  const [runtimeConfig, setRuntimeConfig] =
    useState<AdminConfigResponse | null>(null);
  const [runtimeDraft, setRuntimeDraft] = useState("{}");
  const [systemGroup, setSystemGroup] = useState("platform");
  const [systemConfigPayload, setSystemConfigPayload] =
    useState<SystemConfigPayload | null>(null);
  const [systemConfigDraft, setSystemConfigDraft] = useState("{}");
  const [sqlDraft, setSqlDraft] = useState(() => createSqlStarter());
  const [sqlResult, setSqlResult] = useState<AdminSqlResponse | null>(null);
  const [sqlBusy, setSqlBusy] = useState(false);

  const sessionPermissionSet = new Set(
    (session?.user?.permissions ?? []).map((permission) =>
      String(permission).trim().toLowerCase(),
    ),
  );
  const availableRolePresets = accounts?.role_permission_presets ?? {};
  const availablePermissions =
    accounts?.available_permissions ?? Array.from(sessionPermissionSet);
  const availableRoles = accounts?.available_roles ?? fallbackRoleOptions;
  const availableStatuses = accounts?.available_statuses ?? fallbackStatusOptions;
  const usingToken = Boolean(tokenInput.trim());
  const hasConsoleAccess =
    usingToken || canAccessAdminConsole(sessionPermissionSet, availablePermissions);
  const visibleSections = sectionLabels.filter(
    (item) =>
      usingToken ||
      canAccessSection(item.id, sessionPermissionSet, availablePermissions),
  );
  const visibleSectionIds = visibleSections.map((item) => item.id).join("|");
  const firstVisibleSectionId = visibleSections[0]?.id;
  const databaseOptions: AdminDatabaseSelectOption[] = adminDatabaseOptions.map(
    (database) => ({
      ...database,
      disabled: !usingToken && !canReadDatabase(sessionPermissionSet, database.id),
    }),
  );
  const selectedDatabaseMeta =
    adminDatabaseOptions.find((item) => item.id === selectedDatabase) ??
    adminDatabaseOptions[0]!;
  const defaultSchemaName = defaultSchemaForDatabase(selectedDatabase);
  const canReadSelectedDatabase =
    usingToken || canReadDatabase(sessionPermissionSet, selectedDatabase);
  const canManageSelectedDatabase =
    usingToken || canWriteDatabase(sessionPermissionSet, selectedDatabase);
  const canManageAccounts =
    usingToken || hasPermission(sessionPermissionSet, "accounts.write");
  const canManageRates =
    usingToken ||
    hasPermission(sessionPermissionSet, "rate_limits.write") ||
    hasPermission(sessionPermissionSet, "runtime.write");
  const canManageRuntime =
    usingToken || hasPermission(sessionPermissionSet, "runtime.write");
  const canRunSql = usingToken || hasPermission(sessionPermissionSet, "db.sql.write");
  const activeSection = sectionLabels.find((item) => item.id === section);
  const overviewEntries = Object.entries(dashboard?.overview ?? {});
  const metricEntries = Object.entries(dashboard?.metrics ?? {});
  const availableSystemGroups = Object.keys(
    systemConfigPayload?.groups ?? rateLimitPayload?.groups ?? {},
  );
  const selectedPrimaryKey = relationDetail?.primary_key ?? [];
  const selectedRow =
    relationDetail?.rows.find((row) =>
      selectedPrimaryKey.every(
        (columnName) => row[columnName] === selectedRowKey?.[columnName],
      ),
    ) ?? null;

  function resolveReadableDatabase(database: AdminDatabase): AdminDatabase {
    if (tokenInput.trim() || canReadDatabase(sessionPermissionSet, database)) {
      return database;
    }
    return (
      adminDatabaseOptions.find((item) =>
        canReadDatabase(sessionPermissionSet, item.id),
      )?.id ?? database
    );
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
    const payload = await loadDbRelations(
      deferredRelationSearch,
      undefined,
      activeDatabase,
    );
    startTransition(() => {
      setRelations(payload.items);
      if (!selectedRelationName && payload.items[0]) {
        setSelectedRelationName(payload.items[0].relation_name);
        setSelectedRelationSchemaName(
          payload.items[0].schema_name || defaultSchemaForDatabase(activeDatabase),
        );
      } else if (!payload.items.length) {
        setRelationDetail(null);
        setSelectedRelationSchemaName(defaultSchemaForDatabase(activeDatabase));
      }
    });
  }

  async function loadTableDetail(
    relationName: string,
    schemaName: string,
    database: AdminDatabase,
  ) {
    setTableBusy(true);
    try {
      const payload = await loadDbRelationDetail(
        relationName,
        schemaName,
        database,
        50,
        0,
      );
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
    const payload = await loadAuthUsers({
      search: deferredAccountSearch,
      limit: 100,
      offset: 0,
    });
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
          permissions: [
            ...(payload.user.permissions ||
              availableRolePresets[payload.user.role || "member"] ||
              []),
          ],
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
      setError(
        requestError instanceof Error
          ? requestError.message
          : "Failed to load the admin console",
      );
    } finally {
      setBusy(false);
    }
  }

  function clearToken() {
    clearAdminToken();
    setTokenInput("");
    setNotice(null);
    setError(null);
  }

  useEffect(() => {
    if (ready && (tokenInput.trim() || session?.authenticated)) {
      const initialSection =
        tokenInput.trim() || !session?.authenticated
          ? "overview"
          : sectionLabels.find((item) =>
              canAccessSection(item.id, sessionPermissionSet, availablePermissions),
            )?.id ?? "overview";
      void bootstrap(initialSection);
    }
  }, [ready, session?.authenticated, session?.user?.permissions, tokenInput]);

  useEffect(() => {
    if (section === "overview" && (tokenInput.trim() || session?.authenticated)) {
      void loadDashboard().catch((requestError) => {
        setError(
          requestError instanceof Error
            ? requestError.message
            : "Failed to load dashboard",
        );
      });
    }
  }, [section, tokenInput, session?.authenticated]);

  useEffect(() => {
    if (section === "tables" && (tokenInput.trim() || session?.authenticated)) {
      void loadTableCatalog().catch((requestError) => {
        setError(
          requestError instanceof Error
            ? requestError.message
            : "Failed to load tables",
        );
      });
    }
  }, [
    section,
    deferredRelationSearch,
    selectedDatabase,
    tokenInput,
    session?.authenticated,
  ]);

  useEffect(() => {
    if (section === "accounts" && (tokenInput.trim() || session?.authenticated)) {
      void loadAccountsCatalog().catch((requestError) => {
        setError(
          requestError instanceof Error
            ? requestError.message
            : "Failed to load accounts",
        );
      });
    }
  }, [section, deferredAccountSearch, tokenInput, session?.authenticated]);

  useEffect(() => {
    if (section === "limits" && (tokenInput.trim() || session?.authenticated)) {
      void loadRateLimits().catch((requestError) => {
        setError(
          requestError instanceof Error
            ? requestError.message
            : "Failed to load rate limits",
        );
      });
    }
  }, [section, tokenInput, session?.authenticated]);

  useEffect(() => {
    if (section === "runtime" && (tokenInput.trim() || session?.authenticated)) {
      void loadRuntime().catch((requestError) => {
        setError(
          requestError instanceof Error
            ? requestError.message
            : "Failed to load runtime settings",
        );
      });
    }
  }, [section, tokenInput, tenantId, systemGroup, session?.authenticated]);

  useEffect(() => {
    if (
      section === "tables" &&
      (tokenInput.trim() || session?.authenticated) &&
      selectedRelationName
    ) {
      void loadTableDetail(
        selectedRelationName,
        selectedRelationSchemaName,
        selectedDatabase,
      ).catch((requestError) => {
        setError(
          requestError instanceof Error
            ? requestError.message
            : "Failed to load relation detail",
        );
      });
    }
  }, [
    section,
    selectedDatabase,
    selectedRelationName,
    selectedRelationSchemaName,
    tokenInput,
    session?.authenticated,
  ]);

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
    if (usingToken) {
      return;
    }
    if (canReadDatabase(sessionPermissionSet, selectedDatabase)) {
      return;
    }
    const fallbackDatabase = adminDatabaseOptions.find((item) =>
      canReadDatabase(sessionPermissionSet, item.id),
    );
    if (fallbackDatabase) {
      setSelectedDatabase(fallbackDatabase.id);
    }
  }, [tokenInput, selectedDatabase, session?.user?.permissions]);

  useEffect(() => {
    if (
      section === "accounts" &&
      (tokenInput.trim() || session?.authenticated) &&
      selectedUserId
    ) {
      void loadAccountDetail(selectedUserId).catch((requestError) => {
        setError(
          requestError instanceof Error
            ? requestError.message
            : "Failed to load account detail",
        );
      });
    }
  }, [section, selectedUserId, tokenInput, session?.authenticated]);

  useEffect(() => {
    if (!firstVisibleSectionId) {
      return;
    }
    if (!visibleSections.some((item) => item.id === section)) {
      setSection(firstVisibleSectionId);
    }
  }, [section, visibleSectionIds, firstVisibleSectionId]);

  function selectRelation(relationName: string, schemaName: string) {
    setSelectedRelationName(relationName);
    setSelectedRelationSchemaName(schemaName);
  }

  function selectRow(row: Record<string, unknown>) {
    const primaryKey = relationDetail?.primary_key ?? [];
    if (!primaryKey.length) {
      setError("This relation has no primary key. Use the SQL editor for advanced mutations.");
      return;
    }
    const key: Record<string, unknown> = {};
    for (const columnName of primaryKey) {
      key[columnName] = row[columnName];
    }
    setSelectedRowKey(key);
    setSelectedRowDraft(editableRowDraft(relationDetail, row));
  }

  async function insertRow() {
    if (!selectedRelationName) {
      return;
    }
    setTableBusy(true);
    setNotice(null);
    setError(null);
    try {
      const values = parseJsonObject("New row payload", newRowDraft);
      await insertDbRow({
        database: selectedDatabase,
        schema_name: selectedRelationSchemaName,
        relation_name: selectedRelationName,
        values,
      });
      await loadTableDetail(
        selectedRelationName,
        selectedRelationSchemaName,
        selectedDatabase,
      );
      setNewRowDraft("{}");
      setNotice(`Inserted a row into ${selectedRelationSchemaName}.${selectedRelationName}.`);
    } catch (mutationError) {
      setError(
        mutationError instanceof Error
          ? mutationError.message
          : "Failed to insert row",
      );
    } finally {
      setTableBusy(false);
    }
  }

  async function saveRow() {
    if (!selectedRelationName || !selectedRowKey) {
      return;
    }
    setTableBusy(true);
    setNotice(null);
    setError(null);
    try {
      const values = parseJsonObject("Row payload", selectedRowDraft);
      await updateDbRow({
        database: selectedDatabase,
        schema_name: selectedRelationSchemaName,
        relation_name: selectedRelationName,
        key: selectedRowKey,
        values,
      });
      await loadTableDetail(
        selectedRelationName,
        selectedRelationSchemaName,
        selectedDatabase,
      );
      setNotice(`Updated a row in ${selectedRelationSchemaName}.${selectedRelationName}.`);
    } catch (mutationError) {
      setError(
        mutationError instanceof Error
          ? mutationError.message
          : "Failed to update row",
      );
    } finally {
      setTableBusy(false);
    }
  }

  async function deleteRow() {
    if (!selectedRelationName || !selectedRowKey) {
      return;
    }
    if (
      !window.confirm(
        `Delete the selected row from ${selectedRelationSchemaName}.${selectedRelationName}?`,
      )
    ) {
      return;
    }
    setTableBusy(true);
    setNotice(null);
    setError(null);
    try {
      await deleteDbRow({
        database: selectedDatabase,
        schema_name: selectedRelationSchemaName,
        relation_name: selectedRelationName,
        key: selectedRowKey,
      });
      await loadTableDetail(
        selectedRelationName,
        selectedRelationSchemaName,
        selectedDatabase,
      );
      setSelectedRowKey(null);
      setSelectedRowDraft("{}");
      setNotice(`Deleted a row from ${selectedRelationSchemaName}.${selectedRelationName}.`);
    } catch (mutationError) {
      setError(
        mutationError instanceof Error
          ? mutationError.message
          : "Failed to delete row",
      );
    } finally {
      setTableBusy(false);
    }
  }

  async function createAccount() {
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
      setError(
        requestError instanceof Error
          ? requestError.message
          : "Failed to create account",
      );
    } finally {
      setAccountBusy(false);
    }
  }

  async function saveAccount() {
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
      setError(
        requestError instanceof Error
          ? requestError.message
          : "Failed to save account",
      );
    } finally {
      setAccountBusy(false);
    }
  }

  async function deleteAccount() {
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
      setError(
        requestError instanceof Error
          ? requestError.message
          : "Failed to delete account",
      );
    } finally {
      setAccountBusy(false);
    }
  }

  async function revokeSessions() {
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
      setError(
        requestError instanceof Error
          ? requestError.message
          : "Failed to revoke sessions",
      );
    } finally {
      setAccountBusy(false);
    }
  }

  function resetCreateAccountForm() {
    setCreateAccountForm(createEmptyAccountForm(availableRolePresets));
  }

  async function saveRateLimits() {
    setBusy(true);
    setNotice(null);
    setError(null);
    try {
      const payload = await saveSystemConfig("rate_limits", rateLimitDraft);
      setRateLimitPayload(payload);
      setRateLimitDraft({ ...payload.editable_config });
      setNotice("Saved rate-limit settings to the startup config.");
    } catch (requestError) {
      setError(
        requestError instanceof Error
          ? requestError.message
          : "Failed to save rate limits",
      );
    } finally {
      setBusy(false);
    }
  }

  async function resetRateLimits() {
    setBusy(true);
    setNotice(null);
    setError(null);
    try {
      const payload = await resetSystemConfig("rate_limits");
      setRateLimitPayload(payload);
      setRateLimitDraft({ ...payload.editable_config });
      setNotice("Reset rate-limit settings to defaults.");
    } catch (requestError) {
      setError(
        requestError instanceof Error
          ? requestError.message
          : "Failed to reset rate limits",
      );
    } finally {
      setBusy(false);
    }
  }

  async function saveRuntimeConfig() {
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
      setError(
        requestError instanceof Error
          ? requestError.message
          : "Failed to save runtime config",
      );
    } finally {
      setBusy(false);
    }
  }

  async function resetRuntimeConfig() {
    setBusy(true);
    setNotice(null);
    setError(null);
    try {
      const payload = await resetAdminRuntimeConfig(tenantId || "shared");
      setRuntimeConfig(payload);
      setRuntimeDraft(prettyJson(payload.stored_override ?? payload.config ?? {}));
      setNotice("Reset tenant runtime config.");
    } catch (requestError) {
      setError(
        requestError instanceof Error
          ? requestError.message
          : "Failed to reset runtime config",
      );
    } finally {
      setBusy(false);
    }
  }

  async function saveSystemGroup() {
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
      setError(
        requestError instanceof Error
          ? requestError.message
          : "Failed to save system config",
      );
    } finally {
      setBusy(false);
    }
  }

  async function resetSystemGroup() {
    setBusy(true);
    setNotice(null);
    setError(null);
    try {
      const payload = await resetSystemConfig(systemGroup);
      setSystemConfigPayload(payload);
      setSystemConfigDraft(prettyJson(payload.editable_config ?? {}));
      setNotice(`Reset ${systemGroup} startup config.`);
    } catch (requestError) {
      setError(
        requestError instanceof Error
          ? requestError.message
          : "Failed to reset system config",
      );
    } finally {
      setBusy(false);
    }
  }

  async function runSql() {
    setSqlBusy(true);
    setNotice(null);
    setError(null);
    try {
      const payload = await executeAdminSql(sqlDraft, selectedDatabase);
      setSqlResult(payload);
      setNotice(`Executed ${payload.statement_type.toUpperCase()} successfully.`);
      if (section === "tables" && selectedRelationName) {
        void loadTableDetail(
          selectedRelationName,
          selectedRelationSchemaName,
          selectedDatabase,
        ).catch(() => undefined);
      }
    } catch (requestError) {
      setError(
        requestError instanceof Error
          ? requestError.message
          : "Failed to execute SQL",
      );
    } finally {
      setSqlBusy(false);
    }
  }

  function resetSqlDraft() {
    setSqlDraft(createSqlStarter());
  }

  function isExtendedSection(value: AdminSection): value is AdminExtendedSection {
    return extendedAdminSections.includes(value as AdminExtendedSection);
  }

  return {
    ready,
    section,
    setSection,
    tokenInput,
    setTokenInput,
    tenantId,
    setTenantId,
    busy,
    error,
    notice,
    usingToken,
    hasConsoleAccess,
    activeSection,
    visibleSections,
    sessionPermissionSet,
    sessionUserRole: session?.user?.role ?? null,
    selectedDatabaseMeta,
    activityLabel: busy || tableBusy || accountBusy || sqlBusy ? "busy" : "idle",
    bootstrap,
    clearToken,
    isExtendedSection,
    overview: {
      dashboard,
      overviewEntries,
      metricEntries,
    },
    tables: {
      databaseOptions,
      selectedDatabase,
      setSelectedDatabase,
      selectedDatabaseMeta,
      defaultSchemaName,
      relationSearch,
      setRelationSearch,
      relations,
      selectedRelationName,
      selectedRelationSchemaName,
      relationDetail,
      selectedRowKey,
      selectedRow,
      selectedRowDraft,
      setSelectedRowDraft,
      newRowDraft,
      setNewRowDraft,
      tableBusy,
      canReadSelectedDatabase,
      canManageSelectedDatabase,
      selectRelation,
      selectRow,
      insertRow,
      saveRow,
      deleteRow,
    },
    accounts: {
      accountSearch,
      setAccountSearch,
      accounts,
      selectedUserId,
      setSelectedUserId,
      selectedUser,
      selectedUserSessions,
      accountBusy,
      createAccountForm,
      setCreateAccountForm,
      editAccountForm,
      setEditAccountForm,
      availablePermissions,
      availableRoles,
      availableStatuses,
      availableRolePresets,
      canManageAccounts,
      createAccount,
      saveAccount,
      deleteAccount,
      revokeSessions,
      resetCreateAccountForm,
    },
    limits: {
      rateLimitPayload,
      rateLimitDraft,
      setRateLimitDraft,
      busy,
      canManageRates,
      saveRateLimits,
      resetRateLimits,
    },
    runtime: {
      runtimeConfig,
      runtimeDraft,
      setRuntimeDraft,
      systemGroup,
      setSystemGroup,
      systemConfigPayload,
      systemConfigDraft,
      setSystemConfigDraft,
      availableSystemGroups,
      busy,
      canManageRuntime,
      saveRuntimeConfig,
      resetRuntimeConfig,
      saveSystemGroup,
      resetSystemGroup,
    },
    sql: {
      databaseOptions,
      selectedDatabase,
      setSelectedDatabase,
      selectedDatabaseMeta,
      selectedRelationName,
      selectedRelationSchemaName,
      defaultSchemaName,
      sqlDraft,
      setSqlDraft,
      sqlResult,
      sqlBusy,
      canRunSql,
      runSql,
      resetSqlDraft,
    },
  };
}
