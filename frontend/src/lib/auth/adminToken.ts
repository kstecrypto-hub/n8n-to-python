const STORAGE_KEY = "hive-admin-token";

export function loadAdminToken(): string {
  return sessionStorage.getItem(STORAGE_KEY) ?? "";
}

export function saveAdminToken(token: string): void {
  const trimmed = token.trim();
  if (!trimmed) {
    sessionStorage.removeItem(STORAGE_KEY);
    return;
  }
  sessionStorage.setItem(STORAGE_KEY, trimmed);
}

export function clearAdminToken(): void {
  sessionStorage.removeItem(STORAGE_KEY);
}
