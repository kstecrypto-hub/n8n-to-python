import { formatValue } from "@/features/admin/adminDisplay";
import type { AdminOverviewState } from "@/features/admin/hooks/useAdminConsoleState";

interface AdminOverviewSectionProps {
  overview: AdminOverviewState;
}

export function AdminOverviewSection({
  overview,
}: AdminOverviewSectionProps) {
  const { dashboard, overviewEntries, metricEntries } = overview;

  return (
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
            <div className="empty-state">
              Load the console to see persisted platform metrics.
            </div>
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
              <div
                key={`${route.path}:${route.methods.join(",")}`}
                className="route-card admin-route-card"
              >
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
  );
}
