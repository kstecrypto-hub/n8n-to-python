import { AdminExtendedSections } from "@/features/admin/AdminExtendedSections";
import { useAdminConsoleState } from "@/features/admin/hooks/useAdminConsoleState";
import { AdminAccountsSection } from "@/features/admin/sections/AdminAccountsSection";
import { AdminOverviewSection } from "@/features/admin/sections/AdminOverviewSection";
import { AdminRateLimitsSection } from "@/features/admin/sections/AdminRateLimitsSection";
import { AdminRuntimeSection } from "@/features/admin/sections/AdminRuntimeSection";
import { AdminSqlSection } from "@/features/admin/sections/AdminSqlSection";
import { AdminTablesSection } from "@/features/admin/sections/AdminTablesSection";

export function AdminPage() {
  const admin = useAdminConsoleState();

  if (!admin.ready) {
    return (
      <section className="auth-screen">
        <div className="auth-card">
          <p>Loading operator access...</p>
        </div>
      </section>
    );
  }

  if (!admin.hasConsoleAccess) {
    return (
      <section className="auth-screen">
        <div className="auth-card auth-card--form">
          <div className="eyebrow">Operator Access</div>
          <h1>Admin access is restricted.</h1>
          <p className="caption">
            Sign in with a role that has internal permissions, or use the
            break-glass admin token for direct control-plane access.
          </p>
          <label>
            Admin token (optional)
            <input
              type="password"
              value={admin.tokenInput}
              onChange={(event) => admin.setTokenInput(event.target.value)}
              placeholder="Optional break-glass token"
            />
          </label>
          {admin.error ? <div className="notice notice--warn">{admin.error}</div> : null}
          <div className="button-row">
            <button
              className="button button--primary"
              type="button"
              onClick={() => void admin.bootstrap()}
            >
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
          <p>
            Supabase-style admin console for application data, identity data,
            startup configuration, and rate limiting.
          </p>
        </div>

        <div className="admin-sidebar__panel">
          <label>
            Admin token (optional)
            <input
              type="password"
              value={admin.tokenInput}
              onChange={(event) => admin.setTokenInput(event.target.value)}
              placeholder="Optional break-glass token"
            />
          </label>
          <label>
            Tenant scope
            <input
              value={admin.tenantId}
              onChange={(event) => admin.setTenantId(event.target.value)}
              placeholder="shared"
            />
          </label>
          <div className="button-row">
            <button
              className="button button--primary"
              type="button"
              onClick={() => void admin.bootstrap()}
              disabled={admin.busy}
            >
              {admin.busy ? "Loading..." : "Load console"}
            </button>
            <button
              className="button button--ghost"
              type="button"
              onClick={admin.clearToken}
            >
              Clear token
            </button>
          </div>
        </div>

        <nav className="admin-nav" aria-label="Admin sections">
          {admin.visibleSections.map((item) => (
            <button
              key={item.id}
              className={`admin-nav__item ${
                admin.section === item.id ? "admin-nav__item--active" : ""
              }`}
              type="button"
              onClick={() => admin.setSection(item.id)}
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
            Application records live in the app database, login and permission
            data live in a dedicated identity database, and Chroma remains a
            derived vector index.
          </p>
          {admin.sessionUserRole ? (
            <p className="caption">Signed in as {admin.sessionUserRole}</p>
          ) : null}
        </div>
      </aside>

      <section className="admin-main">
        <header className="admin-topbar">
          <div>
            <div className="eyebrow">Internal console</div>
            <h2>{admin.activeSection?.label}</h2>
            <p className="caption">{admin.activeSection?.detail}</p>
          </div>
          <div className="admin-topbar__meta">
            <span className="pill">tenant {admin.tenantId || "shared"}</span>
            {admin.section === "tables" || admin.section === "sql" ? (
              <span className="pill">{admin.selectedDatabaseMeta.label}</span>
            ) : null}
            <span className="pill">{admin.activityLabel}</span>
            <a className="text-link" href="/admin/legacy" target="_blank" rel="noreferrer">
              Open legacy console
            </a>
          </div>
        </header>

        {admin.error ? <div className="notice notice--warn">{admin.error}</div> : null}
        {admin.notice ? <div className="notice admin-notice">{admin.notice}</div> : null}

        {admin.section === "overview" ? (
          <AdminOverviewSection overview={admin.overview} />
        ) : null}

        {admin.isExtendedSection(admin.section) ? (
          <AdminExtendedSections
            section={admin.section}
            usingToken={admin.usingToken}
            permissions={admin.sessionPermissionSet}
            tenantId={admin.tenantId}
          />
        ) : null}

        {admin.section === "tables" ? (
          <AdminTablesSection tables={admin.tables} />
        ) : null}

        {admin.section === "accounts" ? (
          <AdminAccountsSection accounts={admin.accounts} />
        ) : null}

        {admin.section === "limits" ? (
          <AdminRateLimitsSection limits={admin.limits} />
        ) : null}

        {admin.section === "runtime" ? (
          <AdminRuntimeSection runtime={admin.runtime} tenantId={admin.tenantId} />
        ) : null}

        {admin.section === "sql" ? <AdminSqlSection sql={admin.sql} /> : null}
      </section>
    </section>
  );
}
