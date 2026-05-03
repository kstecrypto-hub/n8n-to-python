import type { Dispatch, SetStateAction } from "react";
import { formatValue, shortId, togglePermissionValue } from "@/features/admin/adminDisplay";
import type { AccountFormState } from "@/features/admin/adminModels";
import type { AdminAccountsState } from "@/features/admin/hooks/useAdminConsoleState";

interface AccountFormFieldsProps {
  form: AccountFormState;
  setForm: Dispatch<SetStateAction<AccountFormState>>;
  availablePermissions: string[];
  availableRoles: string[];
  availableStatuses: string[];
  availableRolePresets: Record<string, string[]>;
  passwordLabel: string;
  passwordPlaceholder: string;
}

function AccountFormFields({
  form,
  setForm,
  availablePermissions,
  availableRoles,
  availableStatuses,
  availableRolePresets,
  passwordLabel,
  passwordPlaceholder,
}: AccountFormFieldsProps) {
  return (
    <div className="admin-form-grid">
      <label>
        Email
        <input
          value={form.email}
          onChange={(event) =>
            setForm((current) => ({ ...current, email: event.target.value }))
          }
          placeholder="operator@example.com"
        />
      </label>
      <label>
        Display name
        <input
          value={form.display_name}
          onChange={(event) =>
            setForm((current) => ({
              ...current,
              display_name: event.target.value,
            }))
          }
          placeholder="Operations lead"
        />
      </label>
      <label>
        {passwordLabel}
        <input
          type="password"
          value={form.password}
          onChange={(event) =>
            setForm((current) => ({ ...current, password: event.target.value }))
          }
          placeholder={passwordPlaceholder}
        />
      </label>
      <label>
        Tenant ID
        <input
          value={form.tenant_id}
          onChange={(event) =>
            setForm((current) => ({ ...current, tenant_id: event.target.value }))
          }
          placeholder="shared"
        />
      </label>
      <label>
        Role
        <select
          value={form.role}
          onChange={(event) =>
            setForm((current) => ({
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
          value={form.status}
          onChange={(event) =>
            setForm((current) => ({ ...current, status: event.target.value }))
          }
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
                checked={form.permissions.includes(permission)}
                onChange={() =>
                  setForm((current) => ({
                    ...current,
                    permissions: togglePermissionValue(
                      current.permissions,
                      permission,
                    ),
                  }))
                }
              />
              <span>{permission}</span>
            </label>
          ))}
        </div>
      </div>
    </div>
  );
}

interface AdminAccountsSectionProps {
  accounts: AdminAccountsState;
}

export function AdminAccountsSection({
  accounts,
}: AdminAccountsSectionProps) {
  return (
    <div className="admin-section-grid admin-section-grid--accounts">
      <article className="admin-panel admin-panel--sidebar">
        <div className="admin-panel__header">
          <div>
            <div className="mini-card__label">Identity store</div>
            <h3>Accounts</h3>
          </div>
          <span className="pill">{accounts.accounts?.total ?? 0}</span>
        </div>
        <label>
          Search accounts
          <input
            value={accounts.accountSearch}
            onChange={(event) => accounts.setAccountSearch(event.target.value)}
            placeholder="email or display name"
          />
        </label>
        <div className="admin-list">
          {accounts.accounts?.items.length ? (
            accounts.accounts.items.map((account) => (
              <button
                key={account.user_id}
                className={`admin-list-button ${
                  accounts.selectedUserId === account.user_id
                    ? "admin-list-button--active"
                    : ""
                }`}
                type="button"
                onClick={() => accounts.setSelectedUserId(account.user_id)}
              >
                <strong>{account.display_name || account.email}</strong>
                <span>{account.email}</span>
                <span>
                  {account.role} / {account.status}
                </span>
              </button>
            ))
          ) : (
            <div className="empty-state">
              No accounts found in the current auth store.
            </div>
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
        <AccountFormFields
          form={accounts.createAccountForm}
          setForm={accounts.setCreateAccountForm}
          availablePermissions={accounts.availablePermissions}
          availableRoles={accounts.availableRoles}
          availableStatuses={accounts.availableStatuses}
          availableRolePresets={accounts.availableRolePresets}
          passwordLabel="Password"
          passwordPlaceholder="Set an initial password"
        />
        <div className="button-row">
          <button
            className="button button--primary"
            type="button"
            onClick={() => void accounts.createAccount()}
            disabled={accounts.accountBusy || !accounts.canManageAccounts}
          >
            {accounts.accountBusy ? "Saving..." : "Create account"}
          </button>
          <button
            className="button button--ghost"
            type="button"
            onClick={accounts.resetCreateAccountForm}
          >
            Reset
          </button>
        </div>
      </article>

      <article className="admin-panel">
        <div className="admin-panel__header">
          <div>
            <div className="mini-card__label">Selected account</div>
            <h3>
              {accounts.selectedUser?.display_name ||
                accounts.selectedUser?.email ||
                "Choose an account"}
            </h3>
          </div>
          {accounts.selectedUser ? (
            <div className="admin-inline-pills">
              <span className="pill">{accounts.selectedUser.role}</span>
              <span className="pill">{accounts.selectedUser.status}</span>
              <span className="pill">
                {accounts.selectedUser.active_sessions ?? 0} active sessions
              </span>
            </div>
          ) : null}
        </div>

        {accounts.selectedUser ? (
          <>
            <AccountFormFields
              form={accounts.editAccountForm}
              setForm={accounts.setEditAccountForm}
              availablePermissions={accounts.availablePermissions}
              availableRoles={accounts.availableRoles}
              availableStatuses={accounts.availableStatuses}
              availableRolePresets={accounts.availableRolePresets}
              passwordLabel="New password"
              passwordPlaceholder="Leave blank to keep current password"
            />
            <div className="button-row">
              <button
                className="button button--primary"
                type="button"
                onClick={() => void accounts.saveAccount()}
                disabled={accounts.accountBusy || !accounts.canManageAccounts}
              >
                Save account
              </button>
              <button
                className="button"
                type="button"
                onClick={() => void accounts.revokeSessions()}
                disabled={accounts.accountBusy || !accounts.canManageAccounts}
              >
                Revoke sessions
              </button>
              <button
                className="button button--ghost"
                type="button"
                onClick={() => void accounts.deleteAccount()}
                disabled={accounts.accountBusy || !accounts.canManageAccounts}
              >
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
                    {accounts.selectedUserSessions.length ? (
                      accounts.selectedUserSessions.map((sessionRecord) => (
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
          <div className="empty-state">
            Select an account to update its role, tenant, password, and active
            sessions.
          </div>
        )}
      </article>
    </div>
  );
}
