import { useEffect, useState } from "react";
import {
  loadAdminKgAssertions,
  loadAdminKgEntities,
  loadAdminKgEntityDetail,
  loadAdminKgRaw,
  type AdminKgEntityDetail,
  type AdminKgEntityRecord,
} from "@/lib/api/admin";
import { pretty, type AdminExtendedSectionProps } from "./AdminExtendedSectionSupport";

export function AdminKgSection(_props: AdminExtendedSectionProps) {
  const [entitySearch, setEntitySearch] = useState("");
  const [entities, setEntities] = useState<AdminKgEntityRecord[]>([]);
  const [entityId, setEntityId] = useState("");
  const [entityDetail, setEntityDetail] = useState<AdminKgEntityDetail | null>(null);
  const [assertions, setAssertions] = useState<Record<string, unknown>[]>([]);
  const [rawRows, setRawRows] = useState<Record<string, unknown>[]>([]);

  useEffect(() => {
    Promise.all([
      loadAdminKgEntities({ search: entitySearch || undefined, limit: 100, offset: 0 }),
      loadAdminKgAssertions({ limit: 100, offset: 0 }),
      loadAdminKgRaw({ limit: 100, offset: 0 }),
    ]).then(([entityRows, assertionRows, raw]) => {
      setEntities(entityRows.items);
      setAssertions(assertionRows.items);
      setRawRows(raw.items);
      setEntityId((current) => current || String(entityRows.items[0]?.entity_id ?? ""));
    });
  }, [entitySearch]);

  useEffect(() => {
    if (entityId) loadAdminKgEntityDetail(entityId).then(setEntityDetail);
  }, [entityId]);

  return (
    <div className="admin-panel">
      <div className="button-row">
        <input value={entitySearch} onChange={(event) => setEntitySearch(event.target.value)} placeholder="search entities" />
      </div>
      <div className="admin-section-grid admin-section-grid--tables">
        <div className="admin-panel admin-panel--sidebar">
          <div className="admin-list">
            {entities.map((entity) => (
              <button
                key={entity.entity_id}
                type="button"
                className={`admin-list-button ${entityId === String(entity.entity_id) ? "admin-list-button--active" : ""}`}
                onClick={() => setEntityId(String(entity.entity_id))}
              >
                <strong>{String(entity.canonical_name ?? entity.entity_id)}</strong>
                <span>{String(entity.entity_type ?? "n/a")}</span>
              </button>
            ))}
          </div>
        </div>
        <div className="admin-panel">
          <pre className="admin-json-block">{pretty({ entity: entityDetail, assertions: assertions.slice(0, 50), raw: rawRows.slice(0, 50) })}</pre>
        </div>
      </div>
    </div>
  );
}
