import { useEffect, useState } from "react";
import {
  loadAdminChromaCollections,
  loadAdminChromaParity,
  loadAdminChromaRecords,
  type AdminChromaCollectionRecord,
} from "@/lib/api/admin";
import { pretty, type AdminExtendedSectionProps } from "./AdminExtendedSectionSupport";

export function AdminChromaSection(_props: AdminExtendedSectionProps) {
  const [collections, setCollections] = useState<AdminChromaCollectionRecord[]>([]);
  const [collectionName, setCollectionName] = useState("");
  const [records, setRecords] = useState<Record<string, unknown>[]>([]);
  const [parityDocId, setParityDocId] = useState("");
  const [parity, setParity] = useState<Record<string, unknown> | null>(null);

  useEffect(() => {
    loadAdminChromaCollections().then((rows) => {
      setCollections(rows);
      setCollectionName((current) => current || rows[0]?.name || "");
    });
  }, []);

  useEffect(() => {
    if (collectionName) loadAdminChromaRecords({ collection_name: collectionName, limit: 100, offset: 0 }).then((response) => setRecords(response.items));
  }, [collectionName]);

  return (
    <div className="admin-panel">
      <div className="button-row">
        <input value={parityDocId} onChange={(event) => setParityDocId(event.target.value)} placeholder="parity document id" />
        <button type="button" onClick={() => void loadAdminChromaParity(parityDocId || undefined).then(setParity)}>
          Run parity
        </button>
      </div>
      <div className="admin-section-grid admin-section-grid--tables">
        <div className="admin-panel admin-panel--sidebar">
          <div className="admin-list">
            {collections.map((collection) => (
              <button
                key={collection.name}
                type="button"
                className={`admin-list-button ${collectionName === collection.name ? "admin-list-button--active" : ""}`}
                onClick={() => setCollectionName(collection.name)}
              >
                <strong>{collection.name}</strong>
                <span>{collection.count} records</span>
              </button>
            ))}
          </div>
        </div>
        <div className="admin-panel">
          <pre className="admin-json-block">{pretty({ collection: collections.find((collection) => collection.name === collectionName), records: records.slice(0, 100), parity })}</pre>
        </div>
      </div>
    </div>
  );
}
