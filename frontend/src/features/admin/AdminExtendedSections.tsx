import { useEffect, useState } from "react";
import {
  autoReviewAdminChunks,
  clearAdminProfileMemory,
  clearAdminSessionMemory,
  decideAdminChunk,
  deleteAdminEditorRecord,
  ingestAdminPdf,
  ingestAdminText,
  loadAdminAgentEvaluation,
  loadAdminAgentReviews,
  loadAdminChunkDetail,
  loadAdminChunkMetadata,
  loadAdminChunks,
  loadAdminChromaCollections,
  loadAdminChromaParity,
  loadAdminChromaRecords,
  loadAdminDocumentBundle,
  loadAdminDocuments,
  loadAdminEditorRecord,
  loadAdminIngestProgress,
  loadAdminKgAssertions,
  loadAdminKgEntities,
  loadAdminKgEntityDetail,
  loadAdminKgRaw,
  loadAdminOntology,
  loadAdminPatterns,
  loadAdminProcesses,
  loadAdminProfileDetail,
  loadAdminProfiles,
  loadAdminRetrievalEvaluation,
  loadAdminReviewRuns,
  loadAdminRunDetail,
  loadAdminRuns,
  loadAdminSessionDetail,
  loadAdminSessions,
  loadAdminStageRuns,
  replayAdminRun,
  resetAdminPipeline,
  resumeAdminIngest,
  reviewAdminRun,
  runAdminAgentEvaluation,
  runAdminDocumentAction,
  runAdminRetrievalEvaluation,
  saveAdminEditorRecord,
  saveAdminOntology,
  startAdminIngest,
  stopAdminIngest,
  type AdminChunkDetail,
  type AdminChunkRecord,
  type AdminChromaCollectionRecord,
  type AdminDocumentRecord,
  type AdminDocumentBundle,
  type AdminKgEntityDetail,
  type AdminKgEntityRecord,
  type AdminIngestProgressSnapshot,
  uploadAndIngestAdminFile,
  resyncAdminEditorRecord,
} from "@/lib/api/admin";

export type AdminExtendedSection = "corpus" | "chunks" | "kg" | "chroma" | "agent" | "operations";

interface AdminExtendedSectionsProps {
  section: AdminExtendedSection;
  usingToken: boolean;
  permissions: Set<string>;
  tenantId: string;
}

function pretty(value: unknown) {
  return JSON.stringify(value ?? {}, null, 2);
}

function sid(value: unknown) {
  const text = String(value ?? "").trim();
  return text.length > 18 ? `${text.slice(0, 8)}...${text.slice(-4)}` : text || "n/a";
}

function can(props: AdminExtendedSectionsProps, permission: string) {
  return props.usingToken || props.permissions.has(permission);
}

function asRecord(value: unknown): Record<string, unknown> | null {
  return value && typeof value === "object" && !Array.isArray(value) ? (value as Record<string, unknown>) : null;
}

function asArray(value: unknown): unknown[] {
  return Array.isArray(value) ? value : [];
}

function memoryCount(value: unknown): number {
  if (Array.isArray(value)) return value.length;
  return String(value ?? "").trim() ? 1 : 0;
}

function memoryPreview(value: unknown): string {
  if (Array.isArray(value)) {
    const first = value[0];
    if (!first) return "empty";
    if (typeof first === "string") return first;
    const record = asRecord(first);
    if (!record) return String(first);
    return String(
      record.fact ??
        record.thread ??
        record.preference ??
        record.constraint ??
        record.topic ??
        record.goal ??
        JSON.stringify(record),
    );
  }
  return String(value ?? "").trim() || "empty";
}

function renderMemoryItems(value: unknown, kind: "facts" | "threads" | "generic") {
  const items = asArray(value).slice(0, 6);
  if (!items.length) return <div className="muted">No items.</div>;
  return (
    <ul className="admin-list admin-list--compact">
      {items.map((item, index) => {
        const record = asRecord(item);
        const label =
          String(
            record?.fact ??
              record?.thread ??
              record?.preference ??
              record?.constraint ??
              record?.topic ??
              record?.goal ??
              item,
          ) || "item";
        const meta: string[] = [];
        if (kind === "facts") {
          const source = String(record?.source_type ?? "");
          const confidence = record?.confidence;
          const reviewPolicy = String(record?.review_policy ?? "");
          if (source) meta.push(source);
          if (typeof confidence === "number") meta.push(`confidence ${confidence.toFixed(2)}`);
          if (reviewPolicy) meta.push(reviewPolicy);
        }
        if (kind === "threads") {
          const source = String(record?.source ?? "");
          const expiry = String(record?.expiry_policy ?? "");
          const questionType = String(record?.question_type ?? "");
          if (source) meta.push(source);
          if (questionType) meta.push(questionType);
          if (expiry) meta.push(expiry);
        }
        if (kind === "generic") {
          const source = String(record?.source ?? "");
          if (source) meta.push(source);
        }
        return (
          <li key={`${label}-${index}`} className="admin-list-item">
            <strong>{label}</strong>
            {meta.length ? <div className="muted">{meta.join(" | ")}</div> : null}
          </li>
        );
      })}
    </ul>
  );
}

export function AdminExtendedSections(props: AdminExtendedSectionsProps) {
  const [status, setStatus] = useState("");

  const [docs, setDocs] = useState<AdminDocumentRecord[]>([]);
  const [docId, setDocId] = useState("");
  const [bundle, setBundle] = useState<AdminDocumentBundle | null>(null);

  const [chunkDocId, setChunkDocId] = useState("");
  const [chunkStatus, setChunkStatus] = useState("");
  const [chunks, setChunks] = useState<AdminChunkRecord[]>([]);
  const [chunkId, setChunkId] = useState("");
  const [chunkDetail, setChunkDetail] = useState<AdminChunkDetail | null>(null);
  const [chunkMetadata, setChunkMetadata] = useState<Record<string, unknown>[]>([]);

  const [entitySearch, setEntitySearch] = useState("");
  const [entities, setEntities] = useState<AdminKgEntityRecord[]>([]);
  const [entityId, setEntityId] = useState("");
  const [entityDetail, setEntityDetail] = useState<AdminKgEntityDetail | null>(null);
  const [assertions, setAssertions] = useState<Record<string, unknown>[]>([]);
  const [rawRows, setRawRows] = useState<Record<string, unknown>[]>([]);

  const [collections, setCollections] = useState<AdminChromaCollectionRecord[]>([]);
  const [collectionName, setCollectionName] = useState("");
  const [records, setRecords] = useState<Record<string, unknown>[]>([]);
  const [parityDocId, setParityDocId] = useState("");
  const [parity, setParity] = useState<Record<string, unknown> | null>(null);

  const [sessions, setSessions] = useState<Record<string, unknown>[]>([]);
  const [sessionId, setSessionId] = useState("");
  const [sessionDetail, setSessionDetail] = useState<Record<string, unknown> | null>(null);
  const [runs, setRuns] = useState<Record<string, unknown>[]>([]);
  const [runId, setRunId] = useState("");
  const [runDetail, setRunDetail] = useState<Record<string, unknown> | null>(null);
  const [profiles, setProfiles] = useState<Record<string, unknown>[]>([]);
  const [profileId, setProfileId] = useState("");
  const [profileDetail, setProfileDetail] = useState<Record<string, unknown> | null>(null);
  const [reviews, setReviews] = useState<Record<string, unknown>[]>([]);
  const [patterns, setPatterns] = useState<Record<string, unknown>[]>([]);
  const [reviewDecision, setReviewDecision] = useState("approved");
  const [reviewNotes, setReviewNotes] = useState("");

  const [ingest, setIngest] = useState<AdminIngestProgressSnapshot | null>(null);
  const [processes, setProcesses] = useState<Record<string, unknown> | null>(null);
  const [stages, setStages] = useState<Record<string, unknown>[]>([]);
  const [reviewRuns, setReviewRuns] = useState<Record<string, unknown>[]>([]);
  const [ontology, setOntology] = useState("");
  const [ontologyMeta, setOntologyMeta] = useState<Record<string, unknown> | null>(null);
  const [retrievalQueriesFile, setRetrievalQueriesFile] = useState("data/evals/retrieval-queries.json");
  const [retrievalOutput, setRetrievalOutput] = useState("data/evals/retrieval-output.json");
  const [retrievalEval, setRetrievalEval] = useState<Record<string, unknown> | null>(null);
  const [agentQueriesFile, setAgentQueriesFile] = useState("data/evals/agent-queries.json");
  const [agentOutput, setAgentOutput] = useState("data/evals/agent-output.json");
  const [agentEval, setAgentEval] = useState<Record<string, unknown> | null>(null);
  const [editorType, setEditorType] = useState("chunk");
  const [editorId, setEditorId] = useState("");
  const [editorSecondaryId, setEditorSecondaryId] = useState("");
  const [editorPayload, setEditorPayload] = useState("{}");
  const [editorSyncIndex, setEditorSyncIndex] = useState(false);
  const [editorResult, setEditorResult] = useState<Record<string, unknown> | null>(null);
  const [textFilename, setTextFilename] = useState("manual-note.txt");
  const [textBody, setTextBody] = useState("");
  const [pdfPath, setPdfPath] = useState("");
  const [uploadFile, setUploadFile] = useState<File | null>(null);
  const [ingestResult, setIngestResult] = useState<Record<string, unknown> | null>(null);

  useEffect(() => {
    if (props.section === "corpus") {
      loadAdminDocuments(50, 0).then((r) => {
        setDocs(r.items);
        setDocId((current) => current || String(r.items[0]?.document_id ?? ""));
      });
    }
    if (props.section === "chunks") {
      Promise.all([
        loadAdminChunks({ document_id: chunkDocId || undefined, status: chunkStatus || undefined, limit: 100, offset: 0 }),
        loadAdminChunkMetadata({ document_id: chunkDocId || undefined, status: chunkStatus || undefined, limit: 100, offset: 0 }),
      ]).then(([chunkRows, metadataRows]) => {
        setChunks(chunkRows.items);
        setChunkMetadata(metadataRows.items);
        setChunkId((current) => current || String(chunkRows.items[0]?.chunk_id ?? ""));
      });
    }
    if (props.section === "kg") {
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
    }
    if (props.section === "chroma") {
      loadAdminChromaCollections().then((r) => {
        setCollections(r);
        setCollectionName((current) => current || r[0]?.name || "");
      });
    }
    if (props.section === "agent") {
      Promise.all([
        loadAdminSessions({ limit: 50, offset: 0 }),
        loadAdminRuns({ limit: 50, offset: 0 }),
        loadAdminProfiles({ tenant_id: props.tenantId || "shared", limit: 50, offset: 0 }),
        loadAdminAgentReviews({ limit: 50, offset: 0 }),
        loadAdminPatterns({ tenant_id: props.tenantId || "shared", limit: 50, offset: 0 }),
      ]).then(([sessionRows, runRows, profileRows, reviewRows, patternRows]) => {
        setSessions(sessionRows.items);
        setRuns(runRows.items);
        setProfiles(profileRows.items);
        setReviews(reviewRows.items);
        setPatterns(patternRows.items);
        setSessionId((current) => current || String(sessionRows.items[0]?.session_id ?? sessionRows.items[0]?.agent_session_id ?? ""));
        setRunId((current) => current || String(runRows.items[0]?.query_run_id ?? ""));
        setProfileId((current) => current || String(profileRows.items[0]?.profile_id ?? ""));
      });
    }
    if (props.section === "operations") {
      Promise.all([
        loadAdminIngestProgress(),
        loadAdminProcesses(20),
        loadAdminStageRuns({ limit: 50, offset: 0 }),
        loadAdminReviewRuns({ limit: 50, offset: 0 }),
        loadAdminOntology(),
      ]).then(([ingestSnapshot, processSnapshot, stageRows, reviewRows, ontologyPayload]) => {
        setIngest(ingestSnapshot);
        setProcesses(processSnapshot);
        setStages(stageRows.items);
        setReviewRuns(reviewRows.items);
        setOntology(String(ontologyPayload.content ?? ""));
        setOntologyMeta(ontologyPayload);
      });
    }
  }, [props.section, props.tenantId, chunkDocId, chunkStatus, entitySearch]);

  useEffect(() => {
    if (props.section === "corpus" && docId) loadAdminDocumentBundle(docId, 250).then(setBundle);
  }, [props.section, docId]);

  useEffect(() => {
    if (props.section === "chunks" && chunkId) loadAdminChunkDetail(chunkId).then(setChunkDetail);
  }, [props.section, chunkId]);

  useEffect(() => {
    if (props.section === "kg" && entityId) loadAdminKgEntityDetail(entityId).then(setEntityDetail);
  }, [props.section, entityId]);

  useEffect(() => {
    if (props.section === "chroma" && collectionName) loadAdminChromaRecords({ collection_name: collectionName, limit: 100, offset: 0 }).then((r) => setRecords(r.items));
  }, [props.section, collectionName]);

  useEffect(() => {
    if (props.section === "agent" && sessionId) loadAdminSessionDetail(sessionId).then(setSessionDetail);
  }, [props.section, sessionId]);

  useEffect(() => {
    if (props.section === "agent" && runId) loadAdminRunDetail(runId).then(setRunDetail);
  }, [props.section, runId]);

  useEffect(() => {
    if (props.section === "agent" && profileId) loadAdminProfileDetail(profileId).then(setProfileDetail);
  }, [props.section, profileId]);

  async function refreshOperations() {
    const [ingestSnapshot, processSnapshot, stageRows, reviewRows] = await Promise.all([
      loadAdminIngestProgress(),
      loadAdminProcesses(20),
      loadAdminStageRuns({ limit: 50, offset: 0 }),
      loadAdminReviewRuns({ limit: 50, offset: 0 }),
    ]);
    setIngest(ingestSnapshot);
    setProcesses(processSnapshot);
    setStages(stageRows.items);
    setReviewRuns(reviewRows.items);
  }

  async function refreshAgentWorkbench() {
    const [nextSessionDetail, nextRunDetail, nextProfileDetail, nextReviews, nextPatterns] = await Promise.all([
      sessionId ? loadAdminSessionDetail(sessionId) : Promise.resolve(null),
      runId ? loadAdminRunDetail(runId) : Promise.resolve(null),
      profileId ? loadAdminProfileDetail(profileId) : Promise.resolve(null),
      loadAdminAgentReviews({ limit: 50, offset: 0 }),
      loadAdminPatterns({ tenant_id: props.tenantId || "shared", limit: 50, offset: 0 }),
    ]);
    setSessionDetail(nextSessionDetail);
    setRunDetail(nextRunDetail);
    setProfileDetail(nextProfileDetail);
    setReviews(nextReviews.items);
    setPatterns(nextPatterns.items);
  }

  async function clearSessionMemorySection(section: string) {
    if (!sessionId) return;
    await clearAdminSessionMemory(sessionId, [section]);
    await refreshAgentWorkbench();
    setStatus(`session memory cleared: ${section}`);
  }

  async function clearProfileMemorySection(section: string) {
    if (!profileId) return;
    await clearAdminProfileMemory(profileId, [section]);
    await refreshAgentWorkbench();
    setStatus(`profile memory cleared: ${section}`);
  }

  const sessionMemory = asRecord(asRecord(sessionDetail)?.memory);
  const sessionSummary = asRecord(sessionMemory?.summary_json);
  const profileSummary = asRecord(asRecord(profileDetail)?.summary_json);

  if (props.section === "corpus") return <div className="admin-panel"><div className="button-row">{(["rebuild","revalidate","reindex","reprocess-kg","delete"] as const).map((action) => <button key={action} type="button" disabled={!docId || !can(props,"documents.write")} onClick={() => void runAdminDocumentAction(docId, action, { rerunKg: true, batchSize: 250 }).then(() => loadAdminDocumentBundle(docId,250).then(setBundle)).then(() => setStatus(`${action} completed`))}>{action}</button>)}</div><div className="admin-section-grid admin-section-grid--tables"><div className="admin-panel admin-panel--sidebar"><div className="admin-list">{docs.map((d) => <button key={String(d.document_id)} type="button" className={`admin-list-button ${docId===String(d.document_id)?"admin-list-button--active":""}`} onClick={() => setDocId(String(d.document_id))}><strong>{String(d.filename ?? d.document_id)}</strong><span>{String(d.status ?? "n/a")}</span></button>)}</div></div><div className="admin-panel"><div className="empty-state">{status || "Corpus controls"}</div><pre className="admin-json-block">{pretty(bundle)}</pre></div></div></div>;

  if (props.section === "chunks") return <div className="admin-panel"><div className="button-row"><input value={chunkDocId} onChange={(e) => setChunkDocId(e.target.value)} placeholder="document id" /><input value={chunkStatus} onChange={(e) => setChunkStatus(e.target.value)} placeholder="status" /><button type="button" disabled={!can(props,"documents.write")} onClick={() => void autoReviewAdminChunks(chunkDocId || undefined, 250).then(() => setStatus("auto review completed"))}>Auto review</button>{(["accept","reject","auto"] as const).map((action) => <button key={action} type="button" disabled={!chunkId || !can(props,"documents.write")} onClick={() => void decideAdminChunk(chunkId, action).then(() => loadAdminChunkDetail(chunkId).then(setChunkDetail)).then(() => setStatus(`${action} completed`))}>{action}</button>)}</div><div className="admin-section-grid admin-section-grid--tables"><div className="admin-panel admin-panel--sidebar"><div className="admin-list">{chunks.map((c) => <button key={c.chunk_id} type="button" className={`admin-list-button ${chunkId===String(c.chunk_id)?"admin-list-button--active":""}`} onClick={() => setChunkId(String(c.chunk_id))}><strong>{sid(c.chunk_id)}</strong><span>{String(c.validation_status ?? "n/a")}</span></button>)}</div></div><div className="admin-panel"><div className="empty-state">{status || "Chunk review"}</div><pre className="admin-json-block">{pretty({ detail: chunkDetail, metadata: chunkMetadata.slice(0, 25) })}</pre></div></div></div>;

  if (props.section === "kg") return <div className="admin-panel"><div className="button-row"><input value={entitySearch} onChange={(e) => setEntitySearch(e.target.value)} placeholder="search entities" /></div><div className="admin-section-grid admin-section-grid--tables"><div className="admin-panel admin-panel--sidebar"><div className="admin-list">{entities.map((e) => <button key={e.entity_id} type="button" className={`admin-list-button ${entityId===String(e.entity_id)?"admin-list-button--active":""}`} onClick={() => setEntityId(String(e.entity_id))}><strong>{String(e.canonical_name ?? e.entity_id)}</strong><span>{String(e.entity_type ?? "n/a")}</span></button>)}</div></div><div className="admin-panel"><pre className="admin-json-block">{pretty({ entity: entityDetail, assertions: assertions.slice(0, 50), raw: rawRows.slice(0, 50) })}</pre></div></div></div>;

  if (props.section === "chroma") return <div className="admin-panel"><div className="button-row"><input value={parityDocId} onChange={(e) => setParityDocId(e.target.value)} placeholder="parity document id" /><button type="button" onClick={() => void loadAdminChromaParity(parityDocId || undefined).then(setParity)}>Run parity</button></div><div className="admin-section-grid admin-section-grid--tables"><div className="admin-panel admin-panel--sidebar"><div className="admin-list">{collections.map((c) => <button key={c.name} type="button" className={`admin-list-button ${collectionName===c.name?"admin-list-button--active":""}`} onClick={() => setCollectionName(c.name)}><strong>{c.name}</strong><span>{c.count} records</span></button>)}</div></div><div className="admin-panel"><pre className="admin-json-block">{pretty({ collection: collections.find((c) => c.name === collectionName), records: records.slice(0, 100), parity })}</pre></div></div></div>;

  if (props.section === "agent") {
    const sessionSections = [
      { key: "facts", label: "Facts", value: sessionSummary?.stable_facts, kind: "facts" as const },
      { key: "open_threads", label: "Open threads", value: sessionSummary?.open_threads, kind: "threads" as const },
      { key: "resolved_threads", label: "Resolved threads", value: sessionSummary?.resolved_threads, kind: "threads" as const },
      { key: "preferences", label: "Preferences", value: sessionSummary?.user_preferences, kind: "generic" as const },
      { key: "constraints", label: "Constraints", value: sessionSummary?.active_constraints, kind: "generic" as const },
      {
        key: "scope",
        label: "Scope",
        value: {
          topic_keywords: sessionSummary?.topic_keywords,
          preferred_document_ids: sessionSummary?.preferred_document_ids,
          last_query: sessionSummary?.last_query,
        },
        kind: "generic" as const,
      },
      { key: "goal", label: "Goal", value: sessionSummary?.session_goal, kind: "generic" as const },
    ];
    const profileSections = [
      { key: "background", label: "Background", value: profileSummary?.user_background, kind: "generic" as const },
      { key: "beekeeping_context", label: "Beekeeping context", value: profileSummary?.beekeeping_context, kind: "generic" as const },
      { key: "experience_level", label: "Experience level", value: profileSummary?.experience_level, kind: "generic" as const },
      { key: "communication_style", label: "Communication style", value: profileSummary?.communication_style, kind: "generic" as const },
      { key: "preferences", label: "Answer preferences", value: profileSummary?.answer_preferences, kind: "generic" as const },
      { key: "topics", label: "Recurring topics", value: profileSummary?.recurring_topics, kind: "generic" as const },
      { key: "learning_goals", label: "Learning goals", value: profileSummary?.learning_goals, kind: "generic" as const },
      { key: "constraints", label: "Persistent constraints", value: profileSummary?.persistent_constraints, kind: "generic" as const },
    ];

    return (
      <div className="admin-section-grid">
        <div className="admin-panel">
          <div className="button-row">
            <select value={reviewDecision} onChange={(e) => setReviewDecision(e.target.value)}>
              <option value="approved">approved</option>
              <option value="needs_revision">needs_revision</option>
              <option value="rejected">rejected</option>
            </select>
            <input value={reviewNotes} onChange={(e) => setReviewNotes(e.target.value)} placeholder="review notes" />
            <button type="button" disabled={!runId || !can(props, "agent.review")} onClick={() => void reviewAdminRun(runId, reviewDecision, reviewNotes).then(() => setStatus(`review saved: ${reviewDecision}`))}>Review run</button>
            <button type="button" disabled={!runId || !can(props, "agent.review")} onClick={() => void replayAdminRun(runId).then(() => setStatus("run replayed"))}>Replay run</button>
            <button type="button" disabled={!can(props, "agent.review")} onClick={() => void refreshAgentWorkbench().then(() => setStatus("agent workbench refreshed"))}>Refresh</button>
          </div>
          <div className="empty-state">{status || "Agent workbench"}</div>
          <pre className="admin-json-block">{pretty({ run: runDetail, reviews: reviews.slice(0, 10), patterns: patterns.slice(0, 10) })}</pre>
        </div>

        <div className="admin-panel admin-panel--sidebar">
          <div className="mini-card__label">Sessions</div>
          <div className="admin-list">
            {sessions.map((s) => {
              const id = String(s.session_id ?? s.agent_session_id ?? "");
              return (
                <button key={id} type="button" className={`admin-list-button ${sessionId === id ? "admin-list-button--active" : ""}`} onClick={() => setSessionId(id)}>
                  <strong>{sid(id)}</strong>
                  <span>{String(s.status ?? "n/a")}</span>
                </button>
              );
            })}
          </div>
        </div>

        <div className="admin-panel admin-panel--sidebar">
          <div className="mini-card__label">Runs</div>
          <div className="admin-list">
            {runs.map((r) => (
              <button key={String(r.query_run_id)} type="button" className={`admin-list-button ${runId === String(r.query_run_id) ? "admin-list-button--active" : ""}`} onClick={() => setRunId(String(r.query_run_id))}>
                <strong>{sid(r.query_run_id)}</strong>
                <span>{String(r.status ?? "n/a")}</span>
              </button>
            ))}
          </div>
        </div>

        <div className="admin-panel admin-panel--sidebar">
          <div className="mini-card__label">Profiles</div>
          <div className="admin-list">
            {profiles.map((p) => (
              <button key={String(p.profile_id)} type="button" className={`admin-list-button ${profileId === String(p.profile_id) ? "admin-list-button--active" : ""}`} onClick={() => setProfileId(String(p.profile_id))}>
                <strong>{sid(p.profile_id)}</strong>
                <span>{String(p.status ?? "n/a")}</span>
              </button>
            ))}
          </div>
        </div>

        <div className="admin-panel">
          <div className="mini-card__label">Session memory</div>
          <div className="muted">
            provider={String(sessionMemory?.source_provider ?? "") || "n/a"} | model={String(sessionMemory?.source_model ?? "") || "n/a"} | prompt={String(sessionMemory?.prompt_version ?? "") || "n/a"}
          </div>
          <div className="button-row">
            {["facts", "open_threads", "resolved_threads", "preferences", "constraints", "scope", "goal", "all"].map((section) => (
              <button key={section} type="button" disabled={!sessionId || !can(props, "agent.review")} onClick={() => void clearSessionMemorySection(section)}>
                Clear {section.replace("_", " ")}
              </button>
            ))}
          </div>
          <div className="chunk" style={{ marginTop: 8 }}>{String(sessionMemory?.summary_text ?? "").trim() || "No session memory summary."}</div>
          <div className="admin-section-grid" style={{ marginTop: 12 }}>
            {sessionSections.map((section) => (
              <div key={section.key} className="admin-panel">
                <strong>{section.label}</strong>
                <div className="muted">count {memoryCount(section.value)} | preview {memoryPreview(section.value)}</div>
                {section.key === "facts" && renderMemoryItems(section.value, "facts")}
                {section.key === "open_threads" || section.key === "resolved_threads"
                  ? renderMemoryItems(section.value, "threads")
                  : Array.isArray(section.value)
                    ? renderMemoryItems(section.value, "generic")
                    : <pre className="admin-json-block">{pretty(section.value)}</pre>}
              </div>
            ))}
          </div>
        </div>

        <div className="admin-panel">
          <div className="mini-card__label">Profile memory</div>
          <div className="muted">
            provider={String(asRecord(profileDetail)?.source_provider ?? "") || "n/a"} | model={String(asRecord(profileDetail)?.source_model ?? "") || "n/a"} | prompt={String(asRecord(profileDetail)?.prompt_version ?? "") || "n/a"}
          </div>
          <div className="button-row">
            {["background", "beekeeping_context", "experience_level", "communication_style", "preferences", "topics", "learning_goals", "constraints", "all"].map((section) => (
              <button key={section} type="button" disabled={!profileId || !can(props, "agent.review")} onClick={() => void clearProfileMemorySection(section)}>
                Clear {section.replace("_", " ")}
              </button>
            ))}
          </div>
          <div className="chunk" style={{ marginTop: 8 }}>{String(asRecord(profileDetail)?.summary_text ?? "").trim() || "No profile memory summary."}</div>
          <div className="admin-section-grid" style={{ marginTop: 12 }}>
            {profileSections.map((section) => (
              <div key={section.key} className="admin-panel">
                <strong>{section.label}</strong>
                <div className="muted">count {memoryCount(section.value)} | preview {memoryPreview(section.value)}</div>
                {Array.isArray(section.value) ? renderMemoryItems(section.value, "generic") : <pre className="admin-json-block">{pretty(section.value)}</pre>}
              </div>
            ))}
          </div>
        </div>

        <div className="admin-panel">
          <div className="mini-card__label">Raw detail</div>
          <pre className="admin-json-block">{pretty({ session: sessionDetail, profile: profileDetail })}</pre>
        </div>
      </div>
    );
  }

  return <div className="admin-section-grid"><div className="admin-panel"><div className="button-row"><button type="button" disabled={!can(props,"runtime.write")} onClick={() => void startAdminIngest().then(refreshOperations).then(() => setStatus("ingest started"))}>Start full ingest</button><button type="button" disabled={!can(props,"runtime.write")} onClick={() => void resumeAdminIngest().then(refreshOperations).then(() => setStatus("ingest resumed"))}>Resume ingest</button><button type="button" disabled={!can(props,"runtime.write")} onClick={() => void stopAdminIngest().then(refreshOperations).then(() => setStatus("ingest stopped"))}>Stop ingest</button><button type="button" disabled={!can(props,"runtime.write")} onClick={() => void resetAdminPipeline().then(refreshOperations).then(() => setStatus("pipeline reset"))}>Reset pipeline</button></div><div className="empty-state">{status || "Operations"}</div><pre className="admin-json-block">{pretty({ ingest, processes, stages: stages.slice(0, 25), review_runs: reviewRuns.slice(0, 25) })}</pre></div><div className="admin-panel"><label>Ontology<textarea rows={12} value={ontology} onChange={(e) => setOntology(e.target.value)} /></label><div className="button-row"><button type="button" disabled={!can(props,"kg.write")} onClick={() => void saveAdminOntology(ontology).then((r) => { setOntologyMeta(r); setStatus("ontology saved"); })}>Save ontology</button></div><pre className="admin-json-block">{pretty(ontologyMeta)}</pre></div><div className="admin-panel"><div className="button-row"><input value={retrievalQueriesFile} onChange={(e) => setRetrievalQueriesFile(e.target.value)} placeholder="retrieval queries file" /><input value={retrievalOutput} onChange={(e) => setRetrievalOutput(e.target.value)} placeholder="retrieval output" /><button type="button" disabled={!can(props,"agent.review")} onClick={() => void runAdminRetrievalEvaluation({ queries_file: retrievalQueriesFile, output: retrievalOutput, tenant_id: props.tenantId || "shared", top_k: 8 }).then(() => loadAdminRetrievalEvaluation(retrievalOutput).then(setRetrievalEval).catch(() => null))}>Run retrieval eval</button></div><div className="button-row"><input value={agentQueriesFile} onChange={(e) => setAgentQueriesFile(e.target.value)} placeholder="agent queries file" /><input value={agentOutput} onChange={(e) => setAgentOutput(e.target.value)} placeholder="agent output" /><button type="button" disabled={!can(props,"agent.review")} onClick={() => void runAdminAgentEvaluation({ queries_file: agentQueriesFile, output: agentOutput, tenant_id: props.tenantId || "shared", top_k: 8 }).then(() => loadAdminAgentEvaluation(agentOutput).then(setAgentEval).catch(() => null))}>Run agent eval</button></div><pre className="admin-json-block">{pretty({ retrieval_eval: retrievalEval, agent_eval: agentEval })}</pre></div><div className="admin-panel"><div className="button-row"><input value={editorType} onChange={(e) => setEditorType(e.target.value)} placeholder="record type" /><input value={editorId} onChange={(e) => setEditorId(e.target.value)} placeholder="record id" /><input value={editorSecondaryId} onChange={(e) => setEditorSecondaryId(e.target.value)} placeholder="secondary id" /></div><label className="checkbox-row"><input type="checkbox" checked={editorSyncIndex} onChange={(e) => setEditorSyncIndex(e.target.checked)} />Sync index</label><textarea rows={12} value={editorPayload} onChange={(e) => setEditorPayload(e.target.value)} /><div className="button-row"><button type="button" disabled={!can(props,"documents.write")} onClick={() => void loadAdminEditorRecord(editorType, editorId, editorSecondaryId || undefined).then((r) => { setEditorResult(r); setEditorPayload(pretty((r as Record<string, unknown>).record ?? (r as Record<string, unknown>).payload ?? r)); })}>Load</button><button type="button" disabled={!can(props,"documents.write")} onClick={() => void saveAdminEditorRecord({ record_type: editorType, record_id: editorId, secondary_id: editorSecondaryId || undefined, payload: JSON.parse(editorPayload || "{}") as Record<string, unknown>, sync_index: editorSyncIndex }).then(setEditorResult)}>Save</button><button type="button" disabled={!can(props,"documents.write")} onClick={() => void deleteAdminEditorRecord({ record_type: editorType, record_id: editorId, secondary_id: editorSecondaryId || undefined, sync_index: editorSyncIndex }).then(setEditorResult)}>Delete</button><button type="button" disabled={!can(props,"documents.write")} onClick={() => void resyncAdminEditorRecord(editorType, editorId).then(setEditorResult)}>Resync</button></div><pre className="admin-json-block">{pretty(editorResult)}</pre></div><div className="admin-panel"><input value={textFilename} onChange={(e) => setTextFilename(e.target.value)} placeholder="text filename" /><textarea rows={8} value={textBody} onChange={(e) => setTextBody(e.target.value)} placeholder="text body" /><div className="button-row"><button type="button" disabled={!can(props,"documents.write") || !textBody.trim()} onClick={() => void ingestAdminText({ tenant_id: props.tenantId || "shared", filename: textFilename, raw_text: textBody, document_class: "note", parser_version: "v1", source_type: "text" }).then(setIngestResult)}>Ingest text</button><input value={pdfPath} onChange={(e) => setPdfPath(e.target.value)} placeholder="pdf path" /><button type="button" disabled={!can(props,"documents.write") || !pdfPath.trim()} onClick={() => void ingestAdminPdf({ tenant_id: props.tenantId || "shared", path: pdfPath, filename: pdfPath.split(/[\\/]/).pop(), document_class: "book", parser_version: "v1" }).then(setIngestResult)}>Ingest PDF</button><input type="file" onChange={(e) => setUploadFile(e.target.files?.[0] ?? null)} /><button type="button" disabled={!can(props,"documents.write") || !uploadFile} onClick={() => uploadFile ? void uploadAndIngestAdminFile({ file: uploadFile, tenant_id: props.tenantId || "shared", document_class: uploadFile.name.toLowerCase().endsWith(".pdf") ? "book" : "note", parser_version: "v1", source_type: uploadFile.name.toLowerCase().endsWith(".pdf") ? "pdf" : "text", filename: uploadFile.name }).then(setIngestResult) : undefined}>Upload ingest</button></div><pre className="admin-json-block">{pretty(ingestResult)}</pre></div></div>;
}
