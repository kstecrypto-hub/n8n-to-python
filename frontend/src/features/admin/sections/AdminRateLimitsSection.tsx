import { formatValue } from "@/features/admin/adminDisplay";
import type { AdminRateLimitsState } from "@/features/admin/hooks/useAdminConsoleState";

interface AdminRateLimitsSectionProps {
  limits: AdminRateLimitsState;
}

export function AdminRateLimitsSection({
  limits,
}: AdminRateLimitsSectionProps) {
  return (
    <div className="admin-section-grid">
      <article className="admin-panel">
        <div className="admin-panel__header">
          <div>
            <div className="mini-card__label">Abuse protection</div>
            <h3>Rate limits</h3>
          </div>
        </div>
        <div className="admin-metric-grid admin-metric-grid--compact">
          {Object.entries(limits.rateLimitDraft).map(([key, value]) => (
            <div key={key} className="admin-metric-card admin-metric-card--soft">
              <span>{key.replace(/_/g, " ")}</span>
              <strong>{formatValue(value)}</strong>
            </div>
          ))}
        </div>
        <div className="admin-form-grid">
          {Object.entries(limits.rateLimitDraft).map(([key, value]) => (
            <label key={key}>
              {key.replace(/_/g, " ")}
              <input
                type="number"
                value={String(value ?? "")}
                onChange={(event) =>
                  limits.setRateLimitDraft((current) => ({
                    ...current,
                    [key]: Number(event.target.value),
                  }))
                }
              />
            </label>
          ))}
        </div>
        <div className="button-row">
          <button
            className="button button--primary"
            type="button"
            onClick={() => void limits.saveRateLimits()}
            disabled={limits.busy || !limits.canManageRates}
          >
            Save rate limits
          </button>
          <button
            className="button button--ghost"
            type="button"
            onClick={() => void limits.resetRateLimits()}
            disabled={limits.busy || !limits.canManageRates}
          >
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
            <dd>
              /auth/login is throttled per client identity to slow brute-force
              attempts.
            </dd>
          </div>
          <div>
            <dt>Public chat and query</dt>
            <dd>
              /agent/chat and /agent/query are throttled per authenticated public
              user session.
            </dd>
          </div>
          <div>
            <dt>Persistence</dt>
            <dd>
              {limits.rateLimitPayload?.note ||
                "These values are written to the startup config and take effect after restart."}
            </dd>
          </div>
          <div>
            <dt>Env file</dt>
            <dd>{limits.rateLimitPayload?.env_path || "Not available"}</dd>
          </div>
        </dl>
      </article>
    </div>
  );
}
