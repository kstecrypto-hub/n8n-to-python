import { useEffect, useState } from "react";
import {
  deleteAdminEditorRecord,
  ingestAdminPdf,
  ingestAdminText,
  loadAdminAgentEvaluation,
  loadAdminEditorRecord,
  loadAdminIngestProgress,
  loadAdminOntology,
  loadAdminProcesses,
  loadAdminRetrievalEvaluation,
  loadAdminReviewRuns,
  loadAdminStageRuns,
  resyncAdminEditorRecord,
  resetAdminPipeline,
  runAdminAgentEvaluation,
  runAdminRetrievalEvaluation,
  saveAdminEditorRecord,
  saveAdminOntology,
  startAdminIngest,
  stopAdminIngest,
  resumeAdminIngest,
  uploadAndIngestAdminFile,
  type AdminIngestProgressSnapshot,
} from "@/lib/api/admin";
import { can, pretty, type AdminExtendedSectionProps } from "./AdminExtendedSectionSupport";

export function AdminOperationsSection(props: AdminExtendedSectionProps) {
  const [status, setStatus] = useState("");
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
  }, []);

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

  return (
    <div className="admin-section-grid">
      <div className="admin-panel">
        <div className="button-row">
          <button type="button" disabled={!can(props, "runtime.write")} onClick={() => void startAdminIngest().then(refreshOperations).then(() => setStatus("ingest started"))}>
            Start full ingest
          </button>
          <button type="button" disabled={!can(props, "runtime.write")} onClick={() => void resumeAdminIngest().then(refreshOperations).then(() => setStatus("ingest resumed"))}>
            Resume ingest
          </button>
          <button type="button" disabled={!can(props, "runtime.write")} onClick={() => void stopAdminIngest().then(refreshOperations).then(() => setStatus("ingest stopped"))}>
            Stop ingest
          </button>
          <button type="button" disabled={!can(props, "runtime.write")} onClick={() => void resetAdminPipeline().then(refreshOperations).then(() => setStatus("pipeline reset"))}>
            Reset pipeline
          </button>
        </div>
        <div className="empty-state">{status || "Operations"}</div>
        <pre className="admin-json-block">{pretty({ ingest, processes, stages: stages.slice(0, 25), review_runs: reviewRuns.slice(0, 25) })}</pre>
      </div>

      <div className="admin-panel">
        <label>
          Ontology
          <textarea rows={12} value={ontology} onChange={(event) => setOntology(event.target.value)} />
        </label>
        <div className="button-row">
          <button type="button" disabled={!can(props, "kg.write")} onClick={() => void saveAdminOntology(ontology).then((response) => { setOntologyMeta(response); setStatus("ontology saved"); })}>
            Save ontology
          </button>
        </div>
        <pre className="admin-json-block">{pretty(ontologyMeta)}</pre>
      </div>

      <div className="admin-panel">
        <div className="button-row">
          <input value={retrievalQueriesFile} onChange={(event) => setRetrievalQueriesFile(event.target.value)} placeholder="retrieval queries file" />
          <input value={retrievalOutput} onChange={(event) => setRetrievalOutput(event.target.value)} placeholder="retrieval output" />
          <button
            type="button"
            disabled={!can(props, "agent.review")}
            onClick={() => void runAdminRetrievalEvaluation({ queries_file: retrievalQueriesFile, output: retrievalOutput, tenant_id: props.tenantId || "shared", top_k: 8 }).then(() => loadAdminRetrievalEvaluation(retrievalOutput).then(setRetrievalEval).catch(() => null))}
          >
            Run retrieval eval
          </button>
        </div>
        <div className="button-row">
          <input value={agentQueriesFile} onChange={(event) => setAgentQueriesFile(event.target.value)} placeholder="agent queries file" />
          <input value={agentOutput} onChange={(event) => setAgentOutput(event.target.value)} placeholder="agent output" />
          <button
            type="button"
            disabled={!can(props, "agent.review")}
            onClick={() => void runAdminAgentEvaluation({ queries_file: agentQueriesFile, output: agentOutput, tenant_id: props.tenantId || "shared", top_k: 8 }).then(() => loadAdminAgentEvaluation(agentOutput).then(setAgentEval).catch(() => null))}
          >
            Run agent eval
          </button>
        </div>
        <pre className="admin-json-block">{pretty({ retrieval_eval: retrievalEval, agent_eval: agentEval })}</pre>
      </div>

      <div className="admin-panel">
        <div className="button-row">
          <input value={editorType} onChange={(event) => setEditorType(event.target.value)} placeholder="record type" />
          <input value={editorId} onChange={(event) => setEditorId(event.target.value)} placeholder="record id" />
          <input value={editorSecondaryId} onChange={(event) => setEditorSecondaryId(event.target.value)} placeholder="secondary id" />
        </div>
        <label className="checkbox-row">
          <input type="checkbox" checked={editorSyncIndex} onChange={(event) => setEditorSyncIndex(event.target.checked)} />
          Sync index
        </label>
        <textarea rows={12} value={editorPayload} onChange={(event) => setEditorPayload(event.target.value)} />
        <div className="button-row">
          <button type="button" disabled={!can(props, "documents.write")} onClick={() => void loadAdminEditorRecord(editorType, editorId, editorSecondaryId || undefined).then((response) => { setEditorResult(response); setEditorPayload(pretty((response as Record<string, unknown>).record ?? (response as Record<string, unknown>).payload ?? response)); })}>
            Load
          </button>
          <button type="button" disabled={!can(props, "documents.write")} onClick={() => void saveAdminEditorRecord({ record_type: editorType, record_id: editorId, secondary_id: editorSecondaryId || undefined, payload: JSON.parse(editorPayload || "{}") as Record<string, unknown>, sync_index: editorSyncIndex }).then(setEditorResult)}>
            Save
          </button>
          <button type="button" disabled={!can(props, "documents.write")} onClick={() => void deleteAdminEditorRecord({ record_type: editorType, record_id: editorId, secondary_id: editorSecondaryId || undefined, sync_index: editorSyncIndex }).then(setEditorResult)}>
            Delete
          </button>
          <button type="button" disabled={!can(props, "documents.write")} onClick={() => void resyncAdminEditorRecord(editorType, editorId).then(setEditorResult)}>
            Resync
          </button>
        </div>
        <pre className="admin-json-block">{pretty(editorResult)}</pre>
      </div>

      <div className="admin-panel">
        <input value={textFilename} onChange={(event) => setTextFilename(event.target.value)} placeholder="text filename" />
        <textarea rows={8} value={textBody} onChange={(event) => setTextBody(event.target.value)} placeholder="text body" />
        <div className="button-row">
          <button type="button" disabled={!can(props, "documents.write") || !textBody.trim()} onClick={() => void ingestAdminText({ tenant_id: props.tenantId || "shared", filename: textFilename, raw_text: textBody, document_class: "note", parser_version: "v1", source_type: "text" }).then(setIngestResult)}>
            Ingest text
          </button>
          <input value={pdfPath} onChange={(event) => setPdfPath(event.target.value)} placeholder="pdf path" />
          <button type="button" disabled={!can(props, "documents.write") || !pdfPath.trim()} onClick={() => void ingestAdminPdf({ tenant_id: props.tenantId || "shared", path: pdfPath, filename: pdfPath.split(/[\\/]/).pop(), document_class: "book", parser_version: "v1" }).then(setIngestResult)}>
            Ingest PDF
          </button>
          <input type="file" onChange={(event) => setUploadFile(event.target.files?.[0] ?? null)} />
          <button
            type="button"
            disabled={!can(props, "documents.write") || !uploadFile}
            onClick={() => uploadFile ? void uploadAndIngestAdminFile({ file: uploadFile, tenant_id: props.tenantId || "shared", document_class: uploadFile.name.toLowerCase().endsWith(".pdf") ? "book" : "note", parser_version: "v1", source_type: uploadFile.name.toLowerCase().endsWith(".pdf") ? "pdf" : "text", filename: uploadFile.name }).then(setIngestResult) : undefined}
          >
            Upload ingest
          </button>
        </div>
        <pre className="admin-json-block">{pretty(ingestResult)}</pre>
      </div>
    </div>
  );
}
