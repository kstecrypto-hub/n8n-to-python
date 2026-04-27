import type { AdminRuntimeState } from "@/features/admin/hooks/useAdminConsoleState";

interface AdminRuntimeSectionProps {
  runtime: AdminRuntimeState;
  tenantId: string;
}

export function AdminRuntimeSection({
  runtime,
  tenantId,
}: AdminRuntimeSectionProps) {
  return (
    <div className="admin-section-grid">
      <article className="admin-panel admin-panel--wide">
        <div className="admin-panel__header">
          <div>
            <div className="mini-card__label">Tenant runtime</div>
            <h3>Agent override JSON</h3>
          </div>
          <div className="admin-inline-pills">
            <span className="pill">
              tenant {runtime.runtimeConfig?.tenant_id || tenantId || "shared"}
            </span>
            <span className="pill">
              api key {runtime.runtimeConfig?.effective_api_key_source || "default"}
            </span>
          </div>
        </div>
        <div className="admin-metadata-grid">
          <div className="admin-metric-card admin-metric-card--soft">
            <span>Updated by</span>
            <strong>{runtime.runtimeConfig?.updated_by || "n/a"}</strong>
          </div>
          <div className="admin-metric-card admin-metric-card--soft">
            <span>Updated at</span>
            <strong>{runtime.runtimeConfig?.updated_at || "n/a"}</strong>
          </div>
          <div className="admin-metric-card admin-metric-card--soft">
            <span>API key override</span>
            <strong>
              {runtime.runtimeConfig?.has_api_key_override ? "present" : "not set"}
            </strong>
          </div>
        </div>
        <textarea
          rows={18}
          value={runtime.runtimeDraft}
          onChange={(event) => runtime.setRuntimeDraft(event.target.value)}
        />
        <div className="button-row">
          <button
            className="button button--primary"
            type="button"
            onClick={() => void runtime.saveRuntimeConfig()}
            disabled={runtime.busy || !runtime.canManageRuntime}
          >
            Save tenant config
          </button>
          <button
            className="button button--ghost"
            type="button"
            onClick={() => void runtime.resetRuntimeConfig()}
            disabled={runtime.busy || !runtime.canManageRuntime}
          >
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
          <select
            value={runtime.systemGroup}
            onChange={(event) => runtime.setSystemGroup(event.target.value)}
          >
            {runtime.availableSystemGroups.map((group) => (
              <option key={group} value={group}>
                {group}
              </option>
            ))}
          </select>
        </label>
        <p className="caption">{runtime.systemConfigPayload?.note}</p>
        <p className="caption">
          Env file: {runtime.systemConfigPayload?.env_path || "Not available"}
        </p>
        <textarea
          rows={18}
          value={runtime.systemConfigDraft}
          onChange={(event) => runtime.setSystemConfigDraft(event.target.value)}
        />
        <div className="button-row">
          <button
            className="button button--primary"
            type="button"
            onClick={() => void runtime.saveSystemGroup()}
            disabled={runtime.busy || !runtime.canManageRuntime}
          >
            Save group
          </button>
          <button
            className="button button--ghost"
            type="button"
            onClick={() => void runtime.resetSystemGroup()}
            disabled={runtime.busy || !runtime.canManageRuntime}
          >
            Reset group
          </button>
        </div>
      </article>
    </div>
  );
}
