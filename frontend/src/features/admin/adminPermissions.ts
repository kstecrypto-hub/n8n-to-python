import type { AdminDatabase, AdminSection } from "@/features/admin/adminModels";

export function defaultSchemaForDatabase(database: AdminDatabase): string {
  return database === "identity" ? "auth" : "public";
}

export function hasPermission(permissions: Set<string>, permission: string): boolean {
  return permissions.has(permission);
}

export function canAccessAdminConsole(
  permissions: Set<string>,
  availablePermissions: string[],
): boolean {
  return availablePermissions
    .filter((permission) => !permission.startsWith("chat."))
    .some((permission) => permissions.has(permission));
}

export function canAccessSection(
  section: AdminSection,
  permissions: Set<string>,
  availablePermissions: string[],
): boolean {
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
      return (
        permissions.has("runtime.read") ||
        permissions.has("runtime.write") ||
        permissions.has("rate_limits.write")
      );
    case "sql":
      return permissions.has("db.sql.write");
    default:
      return false;
  }
}

export function canReadDatabase(
  permissions: Set<string>,
  database: AdminDatabase,
): boolean {
  if (database === "identity") {
    return (
      permissions.has("accounts.read") ||
      permissions.has("accounts.write") ||
      permissions.has("db.sql.write")
    );
  }
  return permissions.has("db.rows.write") || permissions.has("db.sql.write");
}

export function canWriteDatabase(
  permissions: Set<string>,
  database: AdminDatabase,
): boolean {
  if (database === "identity") {
    return permissions.has("accounts.write") || permissions.has("db.sql.write");
  }
  return permissions.has("db.rows.write") || permissions.has("db.sql.write");
}
