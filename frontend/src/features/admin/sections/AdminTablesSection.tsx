import { buildPrimaryKey, formatValue, shortId } from "@/features/admin/adminDisplay";
import type { AdminDatabase } from "@/features/admin/adminModels";
import type { AdminTablesState } from "@/features/admin/hooks/useAdminConsoleState";

interface AdminTablesSectionProps {
  tables: AdminTablesState;
}

export function AdminTablesSection({ tables }: AdminTablesSectionProps) {
  const relationDetail = tables.relationDetail;

  return (
    <div className="admin-section-grid admin-section-grid--tables">
      <article className="admin-panel admin-panel--sidebar">
        <div className="admin-panel__header">
          <div>
            <div className="mini-card__label">Catalog</div>
            <h3>Relations</h3>
          </div>
          <span className="pill">{tables.relations.length}</span>
        </div>
        <label>
          Database
          <select
            value={tables.selectedDatabase}
            onChange={(event) =>
              tables.setSelectedDatabase(event.target.value as AdminDatabase)
            }
          >
            {tables.databaseOptions.map((database) => (
              <option
                key={database.id}
                value={database.id}
                disabled={database.disabled}
              >
                {database.label}
              </option>
            ))}
          </select>
        </label>
        <p className="caption">{tables.selectedDatabaseMeta.detail}</p>
        <label>
          Search relation
          <input
            value={tables.relationSearch}
            onChange={(event) => tables.setRelationSearch(event.target.value)}
            placeholder={
              tables.selectedDatabase === "identity"
                ? "auth_users, auth_sessions"
                : "documents, agent_query_runs, kg_*"
            }
          />
        </label>
        <div className="admin-list">
          {!tables.canReadSelectedDatabase ? (
            <div className="empty-state">
              This session cannot browse the selected database.
            </div>
          ) : tables.relations.length ? (
            tables.relations.map((relation) => (
              <button
                key={`${relation.schema_name}.${relation.relation_name}`}
                className={`admin-list-button ${
                  tables.selectedRelationName === relation.relation_name &&
                  tables.selectedRelationSchemaName === relation.schema_name
                    ? "admin-list-button--active"
                    : ""
                }`}
                type="button"
                onClick={() =>
                  tables.selectRelation(
                    relation.relation_name,
                    relation.schema_name || tables.defaultSchemaName,
                  )
                }
              >
                <strong>
                  {relation.schema_name}.{relation.relation_name}
                </strong>
                <span>{relation.relation_type}</span>
                <span>{relation.estimated_rows} rows</span>
              </button>
            ))
          ) : (
            <div className="empty-state">
              No relations matched the current search.
            </div>
          )}
        </div>
      </article>

      <article className="admin-panel admin-panel--wide">
        <div className="admin-panel__header">
          <div>
            <div className="mini-card__label">Table editor</div>
            <h3>
              {tables.relationDetail
                ? `${tables.relationDetail.schema_name}.${tables.relationDetail.relation_name}`
                : "Select a relation"}
            </h3>
          </div>
          {tables.relationDetail ? (
            <div className="admin-inline-pills">
              <span className="pill">{tables.selectedDatabaseMeta.label}</span>
              <span className="pill">{tables.relationDetail.schema_name}</span>
              <span className="pill">{tables.relationDetail.relation_type}</span>
              <span className="pill">{tables.relationDetail.total} total rows</span>
              <span className="pill">
                pk {tables.relationDetail.primary_key.join(", ") || "none"}
              </span>
            </div>
          ) : null}
        </div>

        {tables.relationDetail?.redacted_columns?.length ? (
          <div className="notice admin-notice">
            Redacted or sensitive columns are blocked from the generic editor:{" "}
            {tables.relationDetail.redacted_columns.join(", ")}.
          </div>
        ) : null}

        <div className="admin-subgrid">
          <div className="admin-subpanel">
            <div className="mini-card__label">Insert row</div>
            <textarea
              rows={8}
              value={tables.newRowDraft}
              onChange={(event) => tables.setNewRowDraft(event.target.value)}
              placeholder='{"tenant_id":"shared","status":"active"}'
            />
            <div className="button-row">
              <button
                className="button button--primary"
                type="button"
                onClick={() => void tables.insertRow()}
                disabled={
                  !tables.relationDetail ||
                  tables.tableBusy ||
                  !tables.canManageSelectedDatabase
                }
              >
                {tables.tableBusy ? "Saving..." : "Insert row"}
              </button>
              <button
                className="button button--ghost"
                type="button"
                onClick={() => tables.setNewRowDraft("{}")}
              >
                Clear
              </button>
            </div>
          </div>

          <div className="admin-subpanel">
            <div className="mini-card__label">Selected row</div>
            {tables.selectedRowKey ? (
              <>
                <div className="admin-inline-pills">
                  {Object.entries(tables.selectedRowKey).map(([key, value]) => (
                    <span key={key} className="pill">
                      {key}: {shortId(value)}
                    </span>
                  ))}
                </div>
                {tables.selectedRow ? (
                  <p className="caption">
                    Loaded from the current page snapshot for inline editing.
                  </p>
                ) : null}
                <textarea
                  rows={10}
                  value={tables.selectedRowDraft}
                  onChange={(event) =>
                    tables.setSelectedRowDraft(event.target.value)
                  }
                />
                <div className="button-row">
                  <button
                    className="button button--primary"
                    type="button"
                    onClick={() => void tables.saveRow()}
                    disabled={tables.tableBusy || !tables.canManageSelectedDatabase}
                  >
                    Save row
                  </button>
                  <button
                    className="button"
                    type="button"
                    onClick={() => void tables.deleteRow()}
                    disabled={tables.tableBusy || !tables.canManageSelectedDatabase}
                  >
                    Delete row
                  </button>
                </div>
              </>
            ) : (
              <div className="empty-state">
                Select a row from the table preview to edit it.
              </div>
            )}
          </div>
        </div>

        <div className="table-wrap admin-table-wrap">
          <table className="surface-table surface-table--dense">
            <thead>
              <tr>
                <th>Pick</th>
                {tables.relationDetail?.columns.map((column) => (
                  <th key={column.column_name}>{column.column_name}</th>
                ))}
              </tr>
            </thead>
            <tbody>
              {relationDetail?.rows.length ? (
                relationDetail.rows.map((row, index) => {
                  const key = buildPrimaryKey(relationDetail, row);
                  const isActive =
                    key !== null &&
                    Object.entries(key).every(
                      ([columnName, value]) =>
                        tables.selectedRowKey?.[columnName] === value,
                    );
                  return (
                    <tr
                      key={JSON.stringify(key) || String(index)}
                      className={isActive ? "admin-table-row--active" : ""}
                    >
                      <td>
                        <button
                          className="button button--ghost admin-row-picker"
                          type="button"
                          onClick={() => tables.selectRow(row)}
                        >
                          Open
                        </button>
                      </td>
                      {relationDetail.columns.map((column) => (
                        <td
                          key={`${JSON.stringify(key) || index}:${column.column_name}`}
                        >
                          <span className="admin-cell">
                            {formatValue(row[column.column_name])}
                          </span>
                        </td>
                      ))}
                    </tr>
                  );
                })
              ) : (
                <tr>
                  <td
                    className="empty-cell"
                    colSpan={(relationDetail?.columns.length ?? 0) + 1}
                  >
                    No rows found for this relation.
                  </td>
                </tr>
              )}
            </tbody>
          </table>
        </div>
      </article>
    </div>
  );
}
