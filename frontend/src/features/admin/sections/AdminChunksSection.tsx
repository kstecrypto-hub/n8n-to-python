import { useEffect, useState } from "react";
import {
  autoReviewAdminChunks,
  decideAdminChunk,
  loadAdminChunkDetail,
  loadAdminChunkMetadata,
  loadAdminChunks,
  type AdminChunkDetail,
  type AdminChunkRecord,
} from "@/lib/api/admin";
import { can, pretty, sid, type AdminExtendedSectionProps } from "./AdminExtendedSectionSupport";

export function AdminChunksSection(props: AdminExtendedSectionProps) {
  const [status, setStatus] = useState("");
  const [chunkDocId, setChunkDocId] = useState("");
  const [chunkStatus, setChunkStatus] = useState("");
  const [chunks, setChunks] = useState<AdminChunkRecord[]>([]);
  const [chunkId, setChunkId] = useState("");
  const [chunkDetail, setChunkDetail] = useState<AdminChunkDetail | null>(null);
  const [chunkMetadata, setChunkMetadata] = useState<Record<string, unknown>[]>([]);

  useEffect(() => {
    Promise.all([
      loadAdminChunks({ document_id: chunkDocId || undefined, status: chunkStatus || undefined, limit: 100, offset: 0 }),
      loadAdminChunkMetadata({ document_id: chunkDocId || undefined, status: chunkStatus || undefined, limit: 100, offset: 0 }),
    ]).then(([chunkRows, metadataRows]) => {
      setChunks(chunkRows.items);
      setChunkMetadata(metadataRows.items);
      setChunkId((current) => current || String(chunkRows.items[0]?.chunk_id ?? ""));
    });
  }, [chunkDocId, chunkStatus]);

  useEffect(() => {
    if (chunkId) loadAdminChunkDetail(chunkId).then(setChunkDetail);
  }, [chunkId]);

  async function decide(action: "accept" | "reject" | "auto") {
    await decideAdminChunk(chunkId, action);
    setChunkDetail(await loadAdminChunkDetail(chunkId));
    setStatus(`${action} completed`);
  }

  return (
    <div className="admin-panel">
      <div className="button-row">
        <input value={chunkDocId} onChange={(event) => setChunkDocId(event.target.value)} placeholder="document id" />
        <input value={chunkStatus} onChange={(event) => setChunkStatus(event.target.value)} placeholder="status" />
        <button
          type="button"
          disabled={!can(props, "documents.write")}
          onClick={() => void autoReviewAdminChunks(chunkDocId || undefined, 250).then(() => setStatus("auto review completed"))}
        >
          Auto review
        </button>
        {(["accept", "reject", "auto"] as const).map((action) => (
          <button key={action} type="button" disabled={!chunkId || !can(props, "documents.write")} onClick={() => void decide(action)}>
            {action}
          </button>
        ))}
      </div>
      <div className="admin-section-grid admin-section-grid--tables">
        <div className="admin-panel admin-panel--sidebar">
          <div className="admin-list">
            {chunks.map((chunk) => (
              <button
                key={chunk.chunk_id}
                type="button"
                className={`admin-list-button ${chunkId === String(chunk.chunk_id) ? "admin-list-button--active" : ""}`}
                onClick={() => setChunkId(String(chunk.chunk_id))}
              >
                <strong>{sid(chunk.chunk_id)}</strong>
                <span>{String(chunk.validation_status ?? "n/a")}</span>
              </button>
            ))}
          </div>
        </div>
        <div className="admin-panel">
          <div className="empty-state">{status || "Chunk review"}</div>
          <pre className="admin-json-block">{pretty({ detail: chunkDetail, metadata: chunkMetadata.slice(0, 25) })}</pre>
        </div>
      </div>
    </div>
  );
}
