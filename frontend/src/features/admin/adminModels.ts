import type {
  AdminDocumentRecord,
  AdminOverview,
  AdminRoute,
} from "@/lib/api/admin";

export type AdminSection =
  | "overview"
  | "corpus"
  | "chunks"
  | "kg"
  | "chroma"
  | "agent"
  | "operations"
  | "tables"
  | "accounts"
  | "limits"
  | "runtime"
  | "sql";

export type AdminExtendedSection = Extract<
  AdminSection,
  "corpus" | "chunks" | "kg" | "chroma" | "agent" | "operations"
>;

export type AdminDatabase = "app" | "identity";

export interface DashboardSnapshot {
  overview: AdminOverview;
  metrics: Record<string, unknown>;
  routes: AdminRoute[];
  documents: AdminDocumentRecord[];
}

export interface AccountFormState {
  email: string;
  password: string;
  display_name: string;
  tenant_id: string;
  role: string;
  status: string;
  permissions: string[];
}

export interface AdminSectionDefinition {
  id: AdminSection;
  label: string;
  detail: string;
}

export interface AdminDatabaseOption {
  id: AdminDatabase;
  label: string;
  detail: string;
}

export interface AdminDatabaseSelectOption extends AdminDatabaseOption {
  disabled: boolean;
}

export const sectionLabels: AdminSectionDefinition[] = [
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

export const extendedAdminSections: AdminExtendedSection[] = [
  "corpus",
  "chunks",
  "kg",
  "chroma",
  "agent",
  "operations",
];

export const fallbackRoleOptions = [
  "guest",
  "member",
  "review_analyst",
  "knowledge_curator",
  "tenant_admin",
  "platform_owner",
];

export const fallbackStatusOptions = ["active", "disabled"];

export const adminDatabaseOptions: AdminDatabaseOption[] = [
  { id: "app", label: "Application DB", detail: "Corpus, runtime, KG, and agent state" },
  { id: "identity", label: "Identity DB", detail: "Users, sessions, roles, and permissions" },
];
