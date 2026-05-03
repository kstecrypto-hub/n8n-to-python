import { useEffect, useState } from "react";
import {
  loadAdminDocumentBundle,
  loadAdminDocuments,
  runAdminDocumentAction,
  type AdminDocumentBundle,
  type AdminDocumentRecord,
} from "@/lib/api/admin";
import { can, pretty, type AdminExtendedSectionProps } from "./AdminExtendedSectionSupport";

export function AdminCorpusSection(props: AdminExtendedSectionProps) {
  const [status, setStatus] = useState("");
  const [docs, setDocs] = useState<AdminDocumentRecord[]>([]);
  const [docId, setDocId] = useState("");
  const [bundle, setBundle] = useState<AdminDocumentBundle | null>(null);

  useEffect(() => {
    loadAdminDocuments(50, 0).then((r) => {
      setDocs(r.items);
      setDocId((current) => current || String(r.items[0]?.document_id ?? ""));
    });
  }, []);

  useEffect(() => {
    if (docId) loadAdminDocumentBundle(docId, 250).then(setBundle);
  }, [docId]);

  async function runAction(action: "rebuild" | "revalidate" | "reindex" | "reprocess-kg" | "delete") {
    await runAdminDocumentAction(docId, action, { rerunKg: true, batchSize: 250 });
    setBundle(await loadAdminDocumentBundle(docId, 250));
    setStatus(`${action} completed`);
  }

  return (
    <div className="admin-panel">
      <div className="button-row">
        {(["rebuild", "revalidate", "reindex", "reprocess-kg", "delete"] as const).map((action) => (
          <button key={action} type="button" disabled={!docId || !can(props, "documents.write")} onClick={() => void runAction(action)}>
            {action}
          </button>
        ))}
      </div>
      <div className="admin-section-grid admin-section-grid--tables">
        <div className="admin-panel admin-panel--sidebar">
          <div className="admin-list">
            {docs.map((doc) => (
              <button
                key={String(doc.document_id)}
                type="button"
                className={`admin-list-button ${docId === String(doc.document_id) ? "admin-list-button--active" : ""}`}
                onClick={() => setDocId(String(doc.document_id))}
              >
                <strong>{String(doc.filename ?? doc.document_id)}</strong>
                <span>{String(doc.status ?? "n/a")}</span>
              </button>
            ))}
          </div>
        </div>
        <div className="admin-panel">
          <div className="empty-state">{status || "Corpus controls"}</div>
          <pre className="admin-json-block">{pretty(bundle)}</pre>
        </div>
      </div>
    </div>
  );
}
