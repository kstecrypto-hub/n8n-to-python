import type { DbRelationDetail } from "@/lib/api/admin";
import type { RolePermissionPresets } from "@/lib/contracts";
import type { AccountFormState } from "@/features/admin/adminModels";

export function formatValue(value: unknown): string {
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

export function prettyJson(value: unknown): string {
  return JSON.stringify(value ?? {}, null, 2);
}

export function parseJsonObject(
  label: string,
  value: string,
): Record<string, unknown> {
  try {
    const parsed = JSON.parse(value || "{}") as unknown;
    if (!parsed || typeof parsed !== "object" || Array.isArray(parsed)) {
      throw new Error(`${label} must be a JSON object.`);
    }
    return parsed as Record<string, unknown>;
  } catch (error) {
    throw new Error(
      error instanceof Error ? error.message : `${label} is not valid JSON.`,
    );
  }
}

export function shortId(value: unknown): string {
  const text = String(value ?? "").trim();
  if (!text) {
    return "n/a";
  }
  return text.length > 18 ? `${text.slice(0, 8)}...${text.slice(-4)}` : text;
}

export function buildPrimaryKey(
  detail: DbRelationDetail | null,
  row: Record<string, unknown>,
): Record<string, unknown> | null {
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

export function editableRowDraft(
  detail: DbRelationDetail | null,
  row: Record<string, unknown>,
): string {
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

export function createEmptyAccountForm(
  rolePermissionPresets: RolePermissionPresets = {},
): AccountFormState {
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

export function createSqlStarter(): string {
  return [
    "-- Single statement only.",
    "-- Example:",
    "-- SELECT table_name FROM information_schema.tables WHERE table_schema = 'public' ORDER BY table_name",
    "",
  ].join("\n");
}

export function togglePermissionValue(
  current: string[],
  permission: string,
): string[] {
  return current.includes(permission)
    ? current.filter((value) => value !== permission)
    : [...current, permission].sort((left, right) =>
        left.localeCompare(right),
      );
}
