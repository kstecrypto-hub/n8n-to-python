from __future__ import annotations


ADMIN_HTML = """
<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8" />
  <meta name="viewport" content="width=device-width, initial-scale=1" />
  <title>Bee Ingestion Admin</title>
  <style>
    :root {
      --bg: #101418;
      --panel: #161d24;
      --panel-2: #1d262f;
      --ink: #e8edf2;
      --muted: #9aa7b5;
      --line: #2d3945;
      --accent: #df8b49;
      --accent-2: #69b08e;
      --danger: #e06a5f;
      --warn: #d7ae52;
      --ok: #63c088;
      --shadow: 0 20px 44px rgba(0, 0, 0, 0.34);
    }

    * { box-sizing: border-box; }
    body {
      margin: 0;
      font-family: Georgia, "Times New Roman", serif;
      color: var(--ink);
      background:
        radial-gradient(circle at top left, rgba(223, 139, 73, 0.18), transparent 28%),
        radial-gradient(circle at bottom right, rgba(105, 176, 142, 0.14), transparent 30%),
        var(--bg);
    }

    .app {
      display: grid;
      grid-template-columns: 260px 1fr;
      min-height: 100vh;
    }

    .sidebar {
      padding: 28px 18px;
      border-right: 1px solid var(--line);
      background: rgba(16, 20, 24, 0.92);
      backdrop-filter: blur(10px);
    }

    .brand {
      margin-bottom: 26px;
      padding: 14px;
      border: 1px solid var(--line);
      background: linear-gradient(140deg, #192129, #12181e);
      border-radius: 18px;
      box-shadow: var(--shadow);
    }

    .brand h1 {
      margin: 0 0 6px;
      font-size: 24px;
      line-height: 1.1;
    }

    .brand p {
      margin: 0;
      color: var(--muted);
      font-size: 14px;
    }

    .nav button {
      width: 100%;
      margin-bottom: 10px;
      padding: 12px 14px;
      border-radius: 14px;
      border: 1px solid var(--line);
      background: var(--panel);
      color: var(--ink);
      text-align: left;
      font-size: 15px;
      cursor: pointer;
      transition: transform .12s ease, border-color .12s ease, background .12s ease;
    }

    .nav button:hover,
    .nav button.active {
      transform: translateX(4px);
      border-color: rgba(163, 93, 45, 0.55);
      background: linear-gradient(135deg, rgba(223, 139, 73, 0.18), rgba(105, 176, 142, 0.14));
      color: var(--ink);
    }

    .main {
      padding: 28px;
    }

    .toolbar {
      display: flex;
      gap: 12px;
      align-items: center;
      justify-content: space-between;
      margin-bottom: 18px;
    }

    .toolbar .pill {
      display: inline-flex;
      align-items: center;
      gap: 8px;
      padding: 8px 12px;
      border-radius: 999px;
      border: 1px solid var(--line);
      background: rgba(22, 29, 36, 0.88);
      color: var(--muted);
      font-size: 14px;
    }

    .grid {
      display: grid;
      gap: 16px;
    }

    .cards {
      grid-template-columns: repeat(auto-fit, minmax(180px, 1fr));
      margin-bottom: 18px;
    }

    .card,
    .panel {
      border: 1px solid var(--line);
      border-radius: 18px;
      background: var(--panel);
      box-shadow: var(--shadow);
    }

    .card {
      padding: 16px;
    }

    .card h3 {
      margin: 0 0 8px;
      font-size: 14px;
      color: var(--muted);
      font-weight: normal;
      text-transform: uppercase;
      letter-spacing: .06em;
    }

    .card strong {
      font-size: 28px;
      line-height: 1;
    }

    .panel {
      overflow: hidden;
    }

    .panel-header {
      padding: 14px 16px;
      border-bottom: 1px solid var(--line);
      background: linear-gradient(180deg, rgba(29, 38, 47, 0.92), rgba(22, 29, 36, 0.75));
      font-weight: bold;
    }

    .panel-body {
      padding: 12px;
      overflow: auto;
    }

    table {
      width: 100%;
      border-collapse: collapse;
      font-size: 14px;
    }

    th, td {
      padding: 10px 12px;
      border-bottom: 1px solid var(--line);
      vertical-align: top;
      text-align: left;
    }

    th {
      color: var(--muted);
      font-weight: normal;
      text-transform: uppercase;
      letter-spacing: .05em;
      font-size: 12px;
    }

    tr:hover td {
      background: rgba(45, 57, 69, 0.35);
    }

    .status {
      display: inline-block;
      padding: 4px 8px;
      border-radius: 999px;
      font-size: 12px;
      border: 1px solid currentColor;
      white-space: nowrap;
    }

    .status.accepted { color: var(--ok); }
    .status.review { color: var(--warn); }
    .status.rejected { color: var(--danger); }
    .status.validated { color: var(--ok); }
    .status.skipped { color: var(--muted); }
    .status.completed { color: var(--ok); }
    .status.raw { color: var(--accent); }
    .status.quarantined { color: var(--danger); }

    .mono {
      font-family: "Cascadia Code", Consolas, monospace;
      font-size: 12px;
      word-break: break-word;
    }

    .chunk {
      white-space: pre-wrap;
      line-height: 1.5;
      max-width: 100%;
    }

    .split {
      display: grid;
      grid-template-columns: minmax(0, 1.1fr) minmax(320px, 0.9fr);
      gap: 16px;
    }

    .meta-list {
      display: grid;
      gap: 10px;
    }

    .meta-item {
      padding: 12px;
      border: 1px solid var(--line);
      border-radius: 14px;
      background: var(--panel-2);
    }

    .detail-stack {
      display: grid;
      gap: 14px;
    }

    .detail-section {
      border: 1px solid var(--line);
      border-radius: 14px;
      background: var(--panel-2);
      overflow: hidden;
    }

    .detail-section h4 {
      margin: 0;
      padding: 10px 12px;
      border-bottom: 1px solid var(--line);
      background: rgba(22, 29, 36, 0.7);
      font-size: 14px;
    }

    .detail-section .section-body {
      padding: 10px 12px;
      overflow: auto;
    }

    .summary-grid {
      display: grid;
      grid-template-columns: repeat(auto-fit, minmax(150px, 1fr));
      gap: 10px;
    }

    .summary-chip {
      border: 1px solid var(--line);
      border-radius: 12px;
      background: #11171d;
      padding: 10px;
    }

    .summary-chip strong {
      display: block;
      font-size: 18px;
    }

    .doc-link {
      color: var(--accent-2);
      cursor: pointer;
      text-decoration: underline;
    }

    .linked-row {
      cursor: pointer;
    }

    .focused-row td {
      background: rgba(223, 139, 73, 0.16) !important;
    }

    .muted { color: var(--muted); }

    .filters {
      display: flex;
      gap: 10px;
      margin-bottom: 12px;
      flex-wrap: wrap;
    }

    input, select, textarea {
      padding: 9px 12px;
      border-radius: 12px;
      border: 1px solid var(--line);
      background: #11171d;
      color: var(--ink);
      min-width: 180px;
    }
    textarea {
      min-height: 140px;
      resize: vertical;
      font-family: "Cascadia Code", Consolas, monospace;
    }

    .hidden { display: none; }

    .actions {
      display: flex;
      gap: 8px;
      flex-wrap: wrap;
      min-width: 140px;
    }

    .action-btn {
      padding: 6px 10px;
      border-radius: 10px;
      border: 1px solid var(--line);
      background: #11171d;
      color: var(--ink);
      cursor: pointer;
      font-size: 12px;
      transition: background .12s ease, border-color .12s ease, transform .12s ease;
    }

    .action-btn:hover:not(:disabled) {
      background: #1a232c;
      border-color: rgba(223, 139, 73, 0.4);
      transform: translateY(-1px);
    }

    .action-btn:disabled {
      opacity: 0.6;
      cursor: not-allowed;
    }

    .action-btn.accept {
      color: var(--ok);
      border-color: rgba(47, 106, 68, 0.35);
    }

    .action-btn.reject {
      color: var(--danger);
      border-color: rgba(141, 60, 47, 0.35);
    }

    @media (max-width: 1080px) {
      .app { grid-template-columns: 1fr; }
      .sidebar { border-right: 0; border-bottom: 1px solid var(--line); }
      .split { grid-template-columns: 1fr; }
    }
  </style>
</head>
<body>
  <div class="app">
    <aside class="sidebar">
      <div class="brand">
        <h1>Bee Admin</h1>
        <p>Postgres + Chroma dashboard for the ingestion pipeline.</p>
        <div style="margin-top: 12px;">
          <input id="admin-token" type="password" placeholder="Admin token" style="width: 100%;" />
          <div class="small-note" style="margin-top: 6px;">When the control plane is locked, admin actions use this token.</div>
        </div>
      </div>
      <div class="nav">
        <button class="active" data-view="overview">Overview</button>
        <button data-view="documents">Documents</button>
        <button data-view="chunks">Chunks</button>
        <button data-view="metadata">Metadata</button>
        <button data-view="kg">Knowledge Graph</button>
        <button data-view="chroma">Chroma</button>
        <button data-view="database">Database</button>
        <button data-view="agent">Agent</button>
        <button data-view="operations">Operations</button>
      </div>
    </aside>
    <main class="main">
      <div class="toolbar">
        <div class="pill">Local admin UI for ingestion inspection</div>
        <div class="pill" id="live-status">Live refresh off</div>
        <div class="pill" id="timestamp">Loading…</div>
      </div>

      <section id="view-overview" class="view">
        <div class="grid cards" id="overview-cards"></div>
        <div class="panel">
          <div class="panel-header">Recent Documents</div>
          <div class="panel-body">
            <table id="overview-documents"></table>
          </div>
        </div>
      </section>

      <section id="view-documents" class="view hidden">
        <div class="split">
          <div class="panel">
            <div class="panel-header">Documents</div>
            <div class="panel-body">
              <table id="documents-table"></table>
              <div id="documents-pager" class="filters" style="margin-top: 12px;"></div>
            </div>
          </div>
          <div class="panel">
            <div class="panel-header">Document Detail</div>
            <div class="panel-body" id="document-detail">
              <p class="muted">Select a document to inspect jobs and stages.</p>
            </div>
          </div>
        </div>
      </section>

      <section id="view-chunks" class="view hidden">
        <div class="filters">
          <input id="chunk-document-id" placeholder="Document ID filter" />
          <select id="chunk-status">
            <option value="">All statuses</option>
            <option value="accepted">Accepted</option>
            <option value="review">Review</option>
            <option value="rejected">Rejected</option>
          </select>
          <input id="chunk-text-filter" placeholder="Chunk text / section filter" />
          <input id="review-batch-size" type="number" min="1" max="500" value="50" style="min-width: 120px;" />
          <button id="chunk-auto-review">Run Auto Review</button>
          <button id="chunk-refresh">Refresh</button>
        </div>
        <div class="split">
          <div class="panel">
            <div class="panel-header">Chunks</div>
            <div class="panel-body">
              <table id="chunks-table"></table>
              <div id="chunks-pager" class="filters" style="margin-top: 12px;"></div>
            </div>
          </div>
          <div class="panel">
            <div class="panel-header">Chunk Detail</div>
            <div class="panel-body" id="chunk-detail">
              <p class="muted">Select a chunk to inspect metadata, KG, neighbors, and Chroma.</p>
            </div>
          </div>
        </div>
      </section>

      <section id="view-metadata" class="view hidden">
        <div class="filters">
          <input id="metadata-document-id" placeholder="Document ID filter" />
          <select id="metadata-status">
            <option value="">All statuses</option>
            <option value="accepted">Accepted</option>
            <option value="review">Review</option>
            <option value="rejected">Rejected</option>
          </select>
          <button id="metadata-refresh">Refresh</button>
        </div>
        <div class="panel">
          <div class="panel-header">Postgres Chunk Metadata View</div>
          <div class="panel-body">
            <table id="metadata-table"></table>
            <div id="metadata-pager" class="filters" style="margin-top: 12px;"></div>
          </div>
        </div>
      </section>

      <section id="view-kg" class="view hidden">
        <div class="filters">
          <input id="kg-document-id" placeholder="Document ID filter" />
          <input id="kg-entity-filter" placeholder="Entity / predicate filter" />
          <input id="kg-entity-type" placeholder="Entity type filter" />
          <input id="kg-predicate-filter" placeholder="Predicate filter" />
          <select id="kg-status">
            <option value="">All KG statuses</option>
            <option value="validated">Validated</option>
            <option value="review">Review</option>
            <option value="quarantined">Quarantined</option>
          </select>
          <button id="kg-refresh">Refresh</button>
        </div>
        <div class="split">
          <div class="panel">
            <div class="panel-header">KG Entities</div>
            <div class="panel-body">
              <table id="kg-entities-table"></table>
              <div id="kg-entities-pager" class="filters" style="margin-top: 12px;"></div>
            </div>
          </div>
          <div class="panel">
            <div class="panel-header">KG Assertions</div>
            <div class="panel-body">
              <table id="kg-assertions-table"></table>
              <div id="kg-assertions-pager" class="filters" style="margin-top: 12px;"></div>
            </div>
          </div>
        </div>
        <div class="panel" style="margin-top: 16px;">
          <div class="panel-header">KG Entity Detail</div>
          <div class="panel-body" id="kg-entity-detail">
            <p class="muted">Select a KG entity to inspect its assertions, evidence, chunks, and source documents.</p>
          </div>
        </div>
        <div class="panel" style="margin-top: 16px;">
          <div class="panel-header">KG Raw Extractions</div>
          <div class="panel-body">
            <table id="kg-raw-table"></table>
            <div id="kg-raw-pager" class="filters" style="margin-top: 12px;"></div>
          </div>
        </div>
      </section>

      <section id="view-chroma" class="view hidden">
        <div class="filters">
          <input id="chroma-document-id" placeholder="Document ID filter" />
          <select id="chroma-collection">
            <option value="">Loading collections...</option>
          </select>
          <button id="chroma-refresh">Refresh</button>
        </div>
        <div class="panel" style="margin-bottom: 16px;">
          <div class="panel-header">Vector Parity</div>
          <div class="panel-body" id="chroma-parity"></div>
        </div>
        <div class="split">
          <div class="panel">
            <div class="panel-header">Chroma Collections</div>
            <div class="panel-body">
              <table id="chroma-table"></table>
            </div>
          </div>
          <div class="panel">
            <div class="panel-header">Chroma Records</div>
            <div class="panel-body">
              <table id="chroma-records-table"></table>
              <div id="chroma-records-pager" class="filters" style="margin-top: 12px;"></div>
            </div>
          </div>
        </div>
      </section>

      <section id="view-database" class="view hidden">
        <div class="split">
          <div class="panel">
            <div class="panel-header">Database Browser</div>
            <div class="panel-body">
              <div class="detail-stack">
                <div class="detail-section">
                  <h4>Relations</h4>
                  <div class="section-body">
                    <div class="filters">
                      <input id="db-relation-search" placeholder="Filter tables and views" />
                      <button class="action-btn" id="db-relations-refresh">Refresh</button>
                    </div>
                    <table id="db-relations-table"></table>
                  </div>
                </div>
              </div>
            </div>
          </div>
          <div class="panel">
            <div class="panel-header">Relation Detail</div>
            <div class="panel-body">
              <div id="db-relation-detail" class="detail-stack">
                <div class="meta-item">
                  <div><strong>No relation selected</strong></div>
                  <div class="muted" style="margin-top: 8px;">Select a table or view to load its schema and rows.</div>
                </div>
              </div>
              <div id="db-rows-pager" class="filters" style="margin-top: 12px;"></div>
            </div>
          </div>
        </div>
      </section>

      <section id="view-agent" class="view hidden">
        <div class="split">
          <div class="panel">
            <div class="panel-header">Agent Query</div>
            <div class="panel-body">
              <div class="detail-stack">
                <div class="detail-section">
                  <h4>Run Query</h4>
                  <div class="section-body">
                    <div class="filters">
                      <input id="agent-session-id" placeholder="Existing session id (optional)" style="min-width: 320px;" />
                      <input id="agent-session-token" placeholder="Session token (required for reuse)" style="min-width: 320px;" />
                      <input id="agent-tenant-id" placeholder="Tenant" value="shared" />
                      <input id="agent-document-ids" placeholder="Document ids (comma separated, optional)" style="min-width: 320px;" />
                      <input id="agent-top-k" type="number" min="1" max="12" value="8" style="min-width: 120px;" />
                    </div>
                    <div class="filters">
                      <textarea id="agent-question" placeholder="Ask the agent a corpus-grounded question" style="min-width: 100%; width: 100%; min-height: 120px;"></textarea>
                    </div>
                    <div class="actions">
                      <button class="action-btn accept" id="agent-run-query">Run Agent Query</button>
                    </div>
                  </div>
                </div>
                <div class="detail-section">
                  <h4>Answer Runtime Config</h4>
                  <div class="section-body">
                    <div class="filters">
                      <input id="agent-config-tenant" placeholder="Tenant" value="shared" style="min-width: 180px;" />
                      <input id="agent-config-updated" placeholder="Updated metadata" readonly style="min-width: 320px;" />
                    </div>
                    <div class="filters">
                      <textarea id="agent-config-json" placeholder='{"model":"gpt-5-mini"}' style="min-width: 100%; width: 100%; min-height: 260px;"></textarea>
                    </div>
                    <div class="actions">
                      <button class="action-btn" id="agent-config-load">Load Config</button>
                      <button class="action-btn accept" id="agent-config-save">Save Config</button>
                      <button class="action-btn reject" id="agent-config-reset">Reset To Defaults</button>
                    </div>
                  </div>
                </div>
                <div class="detail-section">
                  <h4>System Runtime Env</h4>
                  <div class="section-body">
                    <div class="filters">
                      <select id="system-config-group" style="min-width: 220px;">
                        <option value="platform">Platform</option>
                        <option value="ingestion">Ingestion</option>
                        <option value="embedding">Embedding</option>
                        <option value="vision">Vision</option>
                        <option value="review">Review</option>
                        <option value="kg">KG</option>
                        <option value="agent_defaults">Agent Defaults</option>
                      </select>
                      <input id="system-config-meta" placeholder="Env metadata" readonly style="min-width: 420px;" />
                    </div>
                    <div class="filters">
                      <textarea id="system-config-json" placeholder='{"VISION_MODEL":"gpt-5-mini"}' style="min-width: 100%; width: 100%; min-height: 220px;"></textarea>
                    </div>
                    <div class="filters">
                      <textarea id="system-config-effective" placeholder="Effective running values" readonly style="min-width: 100%; width: 100%; min-height: 180px;"></textarea>
                    </div>
                    <div class="filters">
                      <textarea id="system-config-key-sources" placeholder="Effective provider key sources" readonly style="min-width: 100%; width: 100%; min-height: 160px;"></textarea>
                    </div>
                    <div class="actions">
                      <button class="action-btn" id="system-config-load">Load Group</button>
                      <button class="action-btn accept" id="system-config-save">Save Group</button>
                      <button class="action-btn reject" id="system-config-reset">Reset Group</button>
                    </div>
                  </div>
                </div>
                <div class="detail-section">
                  <h4>Ontology</h4>
                  <div class="section-body">
                    <div class="filters">
                      <input id="ontology-path" placeholder="Ontology path" readonly style="min-width: 320px;" />
                      <input id="ontology-meta" placeholder="Ontology metadata" readonly style="min-width: 420px;" />
                    </div>
                    <div class="filters">
                      <textarea id="ontology-content" placeholder="@prefix ..." style="min-width: 100%; width: 100%; min-height: 260px;"></textarea>
                    </div>
                    <div class="actions">
                      <button class="action-btn" id="ontology-load">Load Ontology</button>
                      <button class="action-btn accept" id="ontology-save">Save Ontology</button>
                    </div>
                  </div>
                </div>
                <div class="detail-section">
                  <h4>Last Agent Result</h4>
                  <div class="section-body" id="agent-result">
                    <p class="muted">Run a query to see the grounded answer, citations, and trace id.</p>
                  </div>
                </div>
              </div>
            </div>
          </div>
          <div class="panel">
            <div class="panel-header">Agent Monitoring</div>
            <div class="panel-body">
              <div class="detail-stack">
                <div class="detail-section">
                  <h4>Agent Metrics</h4>
                  <div class="section-body">
                    <div id="agent-metrics-cards" class="cards"></div>
                  </div>
                </div>
                <div class="detail-section">
                  <h4>Profiles</h4>
                  <div class="section-body">
                    <table id="agent-profiles-table"></table>
                    <div id="agent-profiles-pager" class="filters" style="margin-top: 12px;"></div>
                  </div>
                </div>
                <div class="detail-section">
                  <h4>Sessions</h4>
                  <div class="section-body">
                    <div class="filters">
                      <select id="agent-session-status">
                        <option value="">All statuses</option>
                        <option value="active">Active</option>
                      </select>
                    </div>
                    <table id="agent-sessions-table"></table>
                    <div id="agent-sessions-pager" class="filters" style="margin-top: 12px;"></div>
                  </div>
                </div>
                <div class="detail-section">
                  <h4>Selected Session</h4>
                  <div class="section-body" id="agent-session-detail">
                    <p class="muted">Select a session to inspect the transcript and recent runs.</p>
                  </div>
                </div>
                <div class="detail-section">
                  <h4>Query Runs</h4>
                  <div class="section-body">
                    <div class="filters">
                      <select id="agent-run-status">
                        <option value="">All statuses</option>
                        <option value="completed">Completed</option>
                        <option value="failed">Failed</option>
                      </select>
                      <select id="agent-run-abstained">
                        <option value="">All answers</option>
                        <option value="false">Answered</option>
                        <option value="true">Abstained</option>
                      </select>
                      <select id="agent-run-review-status">
                        <option value="">All review states</option>
                        <option value="needs_review">Needs review</option>
                        <option value="unreviewed">Unreviewed</option>
                        <option value="approved">Approved</option>
                        <option value="rejected">Rejected</option>
                      </select>
                    </div>
                    <table id="agent-runs-table"></table>
                    <div id="agent-runs-pager" class="filters" style="margin-top: 12px;"></div>
                  </div>
                </div>
                <div class="detail-section">
                  <h4>Selected Run Detail</h4>
                  <div class="section-body" id="agent-run-detail">
                    <p class="muted">Select a query run to inspect its answer trace and sources.</p>
                  </div>
                </div>
                <div class="detail-section">
                  <h4>Answer Reviews</h4>
                  <div class="section-body">
                    <div class="filters">
                      <select id="agent-review-decision">
                        <option value="">All review decisions</option>
                        <option value="approved">Approved</option>
                        <option value="needs_review">Needs review</option>
                        <option value="rejected">Rejected</option>
                      </select>
                    </div>
                    <table id="agent-reviews-table"></table>
                    <div id="agent-reviews-pager" class="filters" style="margin-top: 12px;"></div>
                  </div>
                </div>
                <div class="detail-section">
                  <h4>Query Patterns</h4>
                  <div class="section-body">
                    <div class="filters">
                      <input id="agent-pattern-search" placeholder="Pattern keyword filter" style="min-width: 220px;" />
                    </div>
                    <table id="agent-patterns-table"></table>
                    <div id="agent-patterns-pager" class="filters" style="margin-top: 12px;"></div>
                  </div>
                </div>
              </div>
            </div>
          </div>
        </div>
      </section>

      <section id="view-operations" class="view hidden">
        <div class="split">
          <div class="panel">
            <div class="panel-header">Pipeline Operations</div>
            <div class="panel-body">
              <div class="detail-stack">
                <div class="detail-section">
                  <h4>Global Controls</h4>
                  <div class="section-body">
                    <div class="filters">
                      <input id="ops-eval-tenant" placeholder="Eval tenant id" value="retrieval-eval-20260324" />
                      <input id="ops-eval-top-k" type="number" min="1" max="20" value="5" style="min-width: 120px;" />
                      <input id="ops-eval-queries" placeholder="Eval queries file" value="data/evaluation/retrieval_small_queries.json" style="min-width: 320px;" />
                      <input id="ops-eval-output" placeholder="Eval output path" value="data/evaluation/latest-admin-eval.json" />
                    </div>
                    <div class="filters">
                      <input id="ops-agent-eval-tenant" placeholder="Agent eval tenant id" value="retrieval-eval-20260324" />
                      <input id="ops-agent-eval-top-k" type="number" min="1" max="20" value="5" style="min-width: 120px;" />
                      <input id="ops-agent-eval-queries" placeholder="Agent eval queries file" value="data/evaluation/agent_small_queries.json" style="min-width: 320px;" />
                      <input id="ops-agent-eval-output" placeholder="Agent eval output path" value="data/evaluation/latest-agent-eval.json" />
                    </div>
                    <div class="actions">
                      <button class="action-btn" id="ops-run-eval">Run Retrieval Eval</button>
                      <button class="action-btn" id="ops-run-agent-eval">Run Agent Eval</button>
                      <button class="action-btn reject" id="ops-reset">Reset Pipeline Data</button>
                    </div>
                  </div>
                </div>
                <div class="detail-section">
                  <h4>Retrieval Eval</h4>
                  <div class="section-body" id="ops-eval-summary">
                    <p class="muted">Run a retrieval evaluation to inspect pass/fail, selected chunks, and selected assets.</p>
                  </div>
                </div>
                <div class="detail-section">
                  <h4>Agent Eval</h4>
                  <div class="section-body" id="ops-agent-eval-summary">
                    <p class="muted">Run an agent evaluation to inspect router choice, grounding, citations, and abstention quality.</p>
                  </div>
                </div>
                <div class="detail-section">
                  <h4>Live Monitor</h4>
                  <div class="section-body">
                    <div class="filters">
                      <label class="pill" style="cursor: pointer;">
                        <input id="ops-auto-refresh-enabled" type="checkbox" checked style="min-width: auto; margin-right: 8px;" />
                        Auto refresh
                      </label>
                      <input id="ops-auto-refresh-seconds" type="number" min="3" max="120" value="10" style="min-width: 140px;" />
                      <button class="action-btn" id="ops-refresh-now">Refresh Now</button>
                      <button class="action-btn accept" id="ops-start-reingest">Start Full Ingest</button>
                      <button class="action-btn" id="ops-resume-reingest">Resume Ingest</button>
                      <button class="action-btn reject" id="ops-stop-reingest">Stop Full Ingest</button>
                    </div>
                    <div id="ops-live-summary">
                      <p class="muted">Loading live process state…</p>
                    </div>
                  </div>
                </div>
                <div class="detail-section">
                  <h4>Ingest PDF</h4>
                  <div class="section-body">
                    <div class="filters">
                      <input id="ops-pdf-path" placeholder="E:\\n8n to python\\file.pdf" style="min-width: 360px;" />
                      <input id="ops-pdf-filename" placeholder="Optional filename override" />
                      <input id="ops-pdf-tenant" placeholder="Tenant" value="shared" />
                      <input id="ops-pdf-class" placeholder="Document class" value="book" />
                      <input id="ops-pdf-start" type="number" min="1" placeholder="Page start" style="min-width: 120px;" />
                      <input id="ops-pdf-end" type="number" min="1" placeholder="Page end" style="min-width: 120px;" />
                    </div>
                    <div class="actions">
                      <button class="action-btn accept" id="ops-ingest-pdf">Ingest PDF</button>
                    </div>
                  </div>
                </div>
                <div class="detail-section">
                  <h4>Upload And Ingest File</h4>
                  <div class="section-body">
                    <div class="filters">
                      <input id="ops-upload-file" type="file" accept=".pdf,.txt,text/plain,application/pdf" style="min-width: 320px;" />
                      <select id="ops-upload-type" style="min-width: 140px;">
                        <option value="">Auto detect</option>
                        <option value="pdf">PDF</option>
                        <option value="text">Text</option>
                      </select>
                      <input id="ops-upload-filename" placeholder="Optional filename override" />
                      <input id="ops-upload-tenant" placeholder="Tenant" value="shared" />
                      <input id="ops-upload-class" placeholder="Document class" value="book" />
                    </div>
                    <div class="filters">
                      <input id="ops-upload-start" type="number" min="1" placeholder="PDF page start" style="min-width: 140px;" />
                      <input id="ops-upload-end" type="number" min="1" placeholder="PDF page end" style="min-width: 140px;" />
                    </div>
                    <div class="actions">
                      <button class="action-btn accept" id="ops-upload-ingest">Upload And Ingest</button>
                    </div>
                  </div>
                </div>
                <div class="detail-section">
                  <h4>API Console</h4>
                  <div class="section-body">
                    <div class="filters">
                      <select id="ops-api-route" style="min-width: 320px;">
                        <option value="">Select API route</option>
                      </select>
                      <select id="ops-api-method" style="min-width: 120px;">
                        <option value="GET">GET</option>
                        <option value="POST">POST</option>
                      </select>
                      <input id="ops-api-path" placeholder="/admin/api/overview" style="min-width: 360px;" />
                      <button class="action-btn" id="ops-api-run">Run Request</button>
                    </div>
                    <div class="filters">
                      <textarea id="ops-api-body" placeholder='{"document_id":"..."}' style="min-width: 100%; width: 100%; min-height: 120px;"></textarea>
                    </div>
                    <div id="ops-api-response">
                      <p class="muted">Run an API request to inspect the raw response.</p>
                    </div>
                  </div>
                </div>
                <div class="detail-section">
                  <h4>Direct Record Editor</h4>
                  <div class="section-body">
                    <div class="filters">
                      <select id="ops-editor-type" style="min-width: 180px;">
                        <option value="document">Document</option>
                        <option value="source">Source</option>
                        <option value="page">Page</option>
                        <option value="chunk">Chunk</option>
                        <option value="asset">Asset</option>
                        <option value="asset_link">Chunk Asset Link</option>
                        <option value="kg_entity">KG Entity</option>
                        <option value="kg_assertion">KG Assertion</option>
                        <option value="kg_raw">KG Raw Extraction</option>
                        <option value="agent_session">Agent Session</option>
                        <option value="agent_profile">Agent Profile</option>
                        <option value="agent_session_memory">Agent Session Memory</option>
                        <option value="agent_pattern">Agent Query Pattern</option>
                      </select>
                      <input id="ops-editor-id" placeholder="Primary id" style="min-width: 280px;" />
                      <input id="ops-editor-secondary-id" placeholder="Secondary id (page number or tenant)" style="min-width: 220px;" />
                      <button class="action-btn" id="ops-editor-load">Load</button>
                      <button class="action-btn accept" id="ops-editor-save">Save</button>
                      <button class="action-btn" id="ops-editor-resync">Resync Index</button>
                      <button class="action-btn reject" id="ops-editor-delete">Delete</button>
                    </div>
                    <div class="small-note" id="ops-editor-hint" style="margin-bottom: 8px;">
                      Load a persisted record, edit its JSON, then save it back. Chunk, asset, and document records can also resync vectors.
                    </div>
                    <textarea id="ops-editor-json" placeholder='{"field":"value"}' style="min-width: 100%; width: 100%; min-height: 220px;"></textarea>
                    <div id="ops-editor-result" style="margin-top: 12px;">
                      <p class="muted">Select or load a record to edit its stored fields directly.</p>
                    </div>
                  </div>
                </div>
                <div class="detail-section">
                  <h4>Ingest Text</h4>
                  <div class="section-body">
                    <div class="filters">
                      <input id="ops-text-filename" placeholder="Filename" />
                      <input id="ops-text-tenant" placeholder="Tenant" value="shared" />
                      <input id="ops-text-class" placeholder="Document class" value="note" />
                    </div>
                    <div class="filters">
                      <textarea id="ops-text-body" placeholder="Paste text to ingest" style="min-width: 100%; width: 100%;"></textarea>
                    </div>
                    <div class="actions">
                      <button class="action-btn accept" id="ops-ingest-text">Ingest Text</button>
                    </div>
                  </div>
                </div>
                <div class="detail-section">
                  <h4>Operation Result</h4>
                  <div class="section-body" id="ops-result">
                    <p class="muted">Run an operation to see the latest result here.</p>
                  </div>
                </div>
              </div>
            </div>
          </div>
          <div class="panel">
            <div class="panel-header">Activity</div>
            <div class="panel-body">
              <div class="detail-stack">
                <div class="detail-section">
                  <h4>Stage Runs</h4>
                  <div class="section-body">
                    <div class="filters">
                      <input id="ops-stage-document-id" placeholder="Document ID filter" />
                      <select id="ops-stage-status">
                        <option value="">All statuses</option>
                        <option value="completed">Completed</option>
                        <option value="review">Review</option>
                        <option value="failed">Failed</option>
                      </select>
                      <button class="action-btn" id="ops-stage-refresh">Refresh</button>
                    </div>
                    <table id="ops-stage-table"></table>
                    <div id="ops-stage-pager" class="filters" style="margin-top: 12px;"></div>
                  </div>
                </div>
                <div class="detail-section">
                  <h4>Review Runs</h4>
                  <div class="section-body">
                    <div class="filters">
                      <input id="ops-review-document-id" placeholder="Document ID filter" />
                      <select id="ops-review-decision">
                        <option value="">All decisions</option>
                        <option value="accept">Accept</option>
                        <option value="reject">Reject</option>
                        <option value="review">Review</option>
                      </select>
                      <button class="action-btn" id="ops-review-refresh">Refresh</button>
                    </div>
                    <table id="ops-review-table"></table>
                    <div id="ops-review-pager" class="filters" style="margin-top: 12px;"></div>
                  </div>
                </div>
              </div>
            </div>
          </div>
        </div>
      </section>
    </main>
  </div>

  <script>
    const state = {
      documents: [],
      activeDocumentId: null,
      selectedChunkId: null,
      selectedAssetId: null,
      selectedKgEntityId: null,
      selectedAgentSessionId: null,
      selectedAgentRunId: null,
      selectedDatabaseRelation: null,
      selectedDatabaseRowIndex: null,
      editor: {
        recordType: "document",
        recordId: "",
        secondaryId: ""
      },
      routes: [],
      chromaDefaults: {
        chunk: "",
        asset: ""
      },
      autoRefresh: {
        enabled: true,
        intervalMs: 10000,
        timer: null,
        inFlight: false,
        lastCompletedAt: null
      },
      paging: {
        documents: { limit: 25, offset: 0 },
        chunks: { limit: 80, offset: 0 },
        metadata: { limit: 80, offset: 0 },
        kgEntities: { limit: 80, offset: 0 },
        kgAssertions: { limit: 80, offset: 0 },
        kgRaw: { limit: 60, offset: 0 },
        chroma: { limit: 50, offset: 0 },
        stageRuns: { limit: 50, offset: 0 },
        reviewRuns: { limit: 50, offset: 0 },
        agentSessions: { limit: 40, offset: 0 },
        agentRuns: { limit: 40, offset: 0 },
        agentReviews: { limit: 30, offset: 0 },
        agentProfiles: { limit: 30, offset: 0 },
        agentPatterns: { limit: 25, offset: 0 },
        databaseRows: { limit: 50, offset: 0 }
      }
    };

    function statusBadge(value) {
      if (!value) return "";
      const cls = String(value).toLowerCase().replace(/[^a-z0-9_-]/g, "");
      return `<span class="status ${cls}">${escapeHtml(value)}</span>`;
    }

    function escapeHtml(value) {
      return String(value ?? "")
        .replaceAll("&", "&amp;")
        .replaceAll("<", "&lt;")
        .replaceAll(">", "&gt;");
    }

    function getAdminToken() {
      const token = (sessionStorage.getItem("bee-admin-token") || localStorage.getItem("bee-admin-token") || "").trim();
      if (token && !sessionStorage.getItem("bee-admin-token")) {
        sessionStorage.setItem("bee-admin-token", token);
        localStorage.removeItem("bee-admin-token");
      }
      return token;
    }

    function shortText(value, maxChars = 320) {
      const text = String(value ?? "");
      if (text.length <= maxChars) return text;
      return `${text.slice(0, maxChars)}…`;
    }

    function debounce(fn, wait = 350) {
      let timer = null;
      return (...args) => {
        window.clearTimeout(timer);
        timer = window.setTimeout(() => fn(...args), wait);
      };
    }

    function currentView() {
      return document.querySelector(".nav button.active")?.dataset.view || "overview";
    }

    function countNote(displayed, total) {
      if ((total ?? 0) > displayed) {
        return `<span class="muted">Showing ${displayed} of ${total}</span>`;
      }
      return `<span class="muted">${total ?? displayed}</span>`;
    }

    function structuredMetadataSummary(metadata) {
      if (!metadata) return "";
      const summary = [
        metadata.document_id ? `doc=${metadata.document_id}` : null,
        metadata.chunk_id ? `chunk_id=${metadata.chunk_id}` : null,
        metadata.chunk_index != null ? `chunk=${metadata.chunk_index}` : null,
        metadata.page_start || metadata.page_end ? `pages=${metadata.page_start || ""}-${metadata.page_end || ""}` : null,
        metadata.page_number != null ? `page=${metadata.page_number}` : null,
        metadata.asset_index != null ? `asset=${metadata.asset_index}` : null,
        metadata.asset_type ? `asset_type=${metadata.asset_type}` : null,
        metadata.chunk_role ? `role=${metadata.chunk_role}` : null,
        metadata.section_title ? `section=${metadata.section_title}` : null,
        metadata.label ? `label=${metadata.label}` : null,
        metadata.ontology_classes ? `ontology=${metadata.ontology_classes}` : null,
        metadata.validation_status ? `status=${metadata.validation_status}` : null,
        metadata.important_terms ? `terms=${metadata.important_terms}` : null
      ].filter(Boolean);
      if (summary.length) return summary.join(" | ");
      return Object.entries(metadata)
        .filter(([_, value]) => value !== null && value !== undefined && String(value).trim() !== "")
        .slice(0, 6)
        .map(([key, value]) => `${key}=${typeof value === "object" ? JSON.stringify(value) : String(value)}`)
        .join(" | ");
    }

    function resetKgPaging() {
      state.paging.kgEntities.offset = 0;
      state.paging.kgAssertions.offset = 0;
      state.paging.kgRaw.offset = 0;
    }

    function renderPager(containerId, pageState, total, onChange) {
      const root = document.getElementById(containerId);
      if (!root) return;
      const limit = pageState.limit;
      const offset = pageState.offset;
      const shownFrom = total === 0 ? 0 : offset + 1;
      const shownTo = Math.min(offset + limit, total);
      root.innerHTML = `
        <span class="muted">Showing ${shownFrom}-${shownTo} of ${total}</span>
        <button class="action-btn" ${offset <= 0 ? "disabled" : ""} data-page-action="prev">Previous</button>
        <button class="action-btn" ${(offset + limit) >= total ? "disabled" : ""} data-page-action="next">Next</button>
      `;
      root.querySelectorAll("[data-page-action]").forEach(button => {
        button.addEventListener("click", async () => {
          if (button.dataset.pageAction === "prev") {
            pageState.offset = Math.max(0, pageState.offset - pageState.limit);
          } else {
            pageState.offset = pageState.offset + pageState.limit;
          }
          await onChange();
        });
      });
    }

    async function loadJson(path) {
      const response = await fetch(path, { headers: adminHeaders() });
      if (!response.ok) {
        const body = await response.json().catch(() => ({}));
        const detail = body.detail || `Failed to load ${path}`;
        const error = new Error(detail);
        error.status = response.status;
        throw error;
      }
      return response.json();
    }

    function adminHeaders(extra = {}) {
      const token = getAdminToken();
      return token ? { ...extra, "X-Admin-Token": token } : extra;
    }

    async function postJson(path, payload) {
      const response = await fetch(path, {
        method: "POST",
        headers: adminHeaders({ "Content-Type": "application/json" }),
        body: JSON.stringify(payload)
      });
      if (!response.ok) {
        const body = await response.json().catch(() => ({ detail: "Request failed" }));
        const error = new Error(body.detail || `Failed to post ${path}`);
        error.status = response.status;
        throw error;
      }
      return response.json();
    }

    async function postForm(path, formData) {
      const response = await fetch(path, {
        method: "POST",
        headers: adminHeaders(),
        body: formData
      });
      if (!response.ok) {
        const body = await response.json().catch(() => ({ detail: "Request failed" }));
        const error = new Error(body.detail || `Failed to post ${path}`);
        error.status = response.status;
        throw error;
      }
      return response.json();
    }

    async function putJson(path, payload) {
      const response = await fetch(path, {
        method: "PUT",
        headers: adminHeaders({ "Content-Type": "application/json" }),
        body: JSON.stringify(payload)
      });
      if (!response.ok) {
        const body = await response.json().catch(() => ({ detail: "Request failed" }));
        const error = new Error(body.detail || `Failed to put ${path}`);
        error.status = response.status;
        throw error;
      }
      return response.json();
    }

    async function deleteJson(path) {
      const response = await fetch(path, { method: "DELETE", headers: adminHeaders() });
      if (!response.ok) {
        const body = await response.json().catch(() => ({ detail: "Request failed" }));
        const error = new Error(body.detail || `Failed to delete ${path}`);
        error.status = response.status;
        throw error;
      }
      return response.json();
    }

    function setTimestamp() {
      document.getElementById("timestamp").textContent = new Date().toLocaleString();
    }

    function setLiveStatus(message, status = "idle") {
      const root = document.getElementById("live-status");
      if (!root) return;
      const color = status === "error" ? "#8d3c2f" : status === "running" ? "#a35d2d" : "#315c45";
      root.style.borderColor = color;
      root.style.color = color;
      root.textContent = message;
    }

    function setOperationResult(value, isError = false) {
      const root = document.getElementById("ops-result");
      if (!root) return;
      root.innerHTML = `
        <div class="meta-item">
          <div><strong>${isError ? "Operation failed" : "Operation result"}</strong></div>
          <div class="mono" style="margin-top: 8px;">${escapeHtml(typeof value === "string" ? value : JSON.stringify(value, null, 2))}</div>
        </div>
      `;
    }

    function showAdminAuthNotice(message, isError = true) {
      const text = message || "Admin token required. Enter the Admin token from .env in the sidebar to unlock admin data and actions.";
      const views = [
        "overview-cards",
        "overview-documents",
        "documents-table",
        "document-detail",
        "chunks-table",
        "chunk-detail",
        "metadata-table",
        "kg-entities-table",
        "kg-assertions-table",
        "kg-entity-detail",
        "kg-raw-table",
        "chroma-table",
        "chroma-records-table",
        "chroma-parity",
        "db-relations-table",
        "db-relation-detail",
        "agent-session-detail",
        "agent-run-detail",
        "ops-result",
      ];
      for (const id of views) {
        const root = document.getElementById(id);
        if (!root) continue;
        root.innerHTML = `
          <div class="meta-item">
            <div><strong>${isError ? "Admin token required" : "Admin notice"}</strong></div>
            <div class="muted" style="margin-top: 8px;">${escapeHtml(text)}</div>
          </div>
        `;
      }
      setLiveStatus("Admin token required", "error");
    }

    function editorSupportsDelete(recordType) {
      return ["asset", "asset_link", "kg_entity", "kg_assertion", "kg_raw", "agent_session", "agent_profile", "agent_pattern"].includes(recordType);
    }

    function editorSupportsResync(recordType) {
      return ["document", "chunk", "asset"].includes(recordType);
    }

    function updateEditorUiState() {
      const recordType = document.getElementById("ops-editor-type")?.value || "document";
      const deleteButton = document.getElementById("ops-editor-delete");
      const resyncButton = document.getElementById("ops-editor-resync");
      const secondaryInput = document.getElementById("ops-editor-secondary-id");
      const hint = document.getElementById("ops-editor-hint");
      if (deleteButton) deleteButton.disabled = !editorSupportsDelete(recordType);
      if (resyncButton) resyncButton.disabled = !editorSupportsResync(recordType);
      if (secondaryInput) {
        secondaryInput.placeholder = recordType === "page"
          ? "Page number"
          : recordType === "agent_session_memory"
            ? "Secondary id not used"
          : recordType === "agent_profile"
            ? "Secondary id not used"
          : recordType === "agent_pattern"
            ? "Tenant id (default shared)"
            : "Secondary id (optional)";
      }
      if (hint) {
        hint.textContent = recordType === "page"
          ? "Pages use document_id as the primary id and page_number as the secondary id."
          : recordType === "agent_session_memory"
            ? "Agent session memory uses session_id as the primary id."
          : recordType === "agent_profile"
            ? "Agent profiles use profile_id as the primary id."
          : recordType === "agent_pattern"
            ? "Agent query patterns use pattern_signature as the primary id and tenant_id as the secondary id."
            : "Load a persisted record, edit its JSON, then save it back. Chunk, asset, and document records can also resync vectors.";
      }
    }

    function setEditorSelection(recordType, recordId, secondaryId = "", options = {}) {
      const { autoLoad = false } = options;
      state.editor.recordType = recordType || "document";
      state.editor.recordId = recordId || "";
      state.editor.secondaryId = secondaryId || "";
      const typeInput = document.getElementById("ops-editor-type");
      const idInput = document.getElementById("ops-editor-id");
      const secondaryInput = document.getElementById("ops-editor-secondary-id");
      if (typeInput) typeInput.value = state.editor.recordType;
      if (idInput) idInput.value = state.editor.recordId;
      if (secondaryInput) secondaryInput.value = state.editor.secondaryId;
      updateEditorUiState();
      if (autoLoad && recordId) {
        loadEditorRecord().catch(error => setOperationResult(error.message, true));
      }
    }

    function renderEditorResult(payload, isError = false) {
      const root = document.getElementById("ops-editor-result");
      if (!root) return;
      root.innerHTML = `
        <div class="meta-item">
          <div><strong>${isError ? "Editor request failed" : "Editor result"}</strong></div>
          <div class="mono" style="margin-top: 8px;">${escapeHtml(typeof payload === "string" ? payload : JSON.stringify(payload, null, 2))}</div>
        </div>
      `;
    }

    async function loadEditorRecord() {
      const recordType = document.getElementById("ops-editor-type").value || "document";
      const recordId = document.getElementById("ops-editor-id").value.trim();
      const secondaryId = document.getElementById("ops-editor-secondary-id").value.trim();
      if (!recordId) throw new Error("Primary id is required");
      const result = await postJson("/admin/api/editor/load", {
        record_type: recordType,
        record_id: recordId,
        secondary_id: secondaryId || null
      });
      document.getElementById("ops-editor-json").value = JSON.stringify(result.record || {}, null, 2);
      state.editor = { recordType, recordId, secondaryId };
      renderEditorResult(result);
      return result;
    }

    async function saveEditorRecord() {
      const recordType = document.getElementById("ops-editor-type").value || "document";
      const recordId = document.getElementById("ops-editor-id").value.trim();
      const secondaryId = document.getElementById("ops-editor-secondary-id").value.trim();
      if (!recordId) throw new Error("Primary id is required");
      const payload = JSON.parse(document.getElementById("ops-editor-json").value || "{}");
      const syncIndex = editorSupportsResync(recordType) && window.confirm("Save changes and resync the related index if supported?");
      const result = await putJson("/admin/api/editor/save", {
        record_type: recordType,
        record_id: recordId,
        secondary_id: secondaryId || null,
        payload,
        sync_index: syncIndex,
        updated_by: "admin-ui"
      });
      document.getElementById("ops-editor-json").value = JSON.stringify(result.record || {}, null, 2);
      renderEditorResult(result);
      setOperationResult(result);
      await reloadAllViews();
      return result;
    }

    async function deleteEditorRecord() {
      const recordType = document.getElementById("ops-editor-type").value || "document";
      const recordId = document.getElementById("ops-editor-id").value.trim();
      const secondaryId = document.getElementById("ops-editor-secondary-id").value.trim();
      if (!recordId) throw new Error("Primary id is required");
      if (!editorSupportsDelete(recordType)) throw new Error(`Delete is not supported for ${recordType}`);
      if (!window.confirm(`Delete ${recordType} ${recordId}?`)) return null;
      const result = await postJson("/admin/api/editor/delete", {
        record_type: recordType,
        record_id: recordId,
        secondary_id: secondaryId || null,
        sync_index: recordType === "asset"
      });
      renderEditorResult(result);
      setOperationResult(result);
      document.getElementById("ops-editor-json").value = "";
      await reloadAllViews();
      return result;
    }

    async function resyncEditorRecord() {
      const recordType = document.getElementById("ops-editor-type").value || "document";
      const recordId = document.getElementById("ops-editor-id").value.trim();
      if (!recordId) throw new Error("Primary id is required");
      if (!editorSupportsResync(recordType)) throw new Error(`Resync is not supported for ${recordType}`);
      const result = await postJson("/admin/api/editor/resync", {
        record_type: recordType,
        record_id: recordId
      });
      renderEditorResult(result);
      setOperationResult(result);
      await reloadAllViews();
      return result;
    }

    function summarizeFailures(results) {
      return results
        .filter(result => result.status === "rejected")
        .map(result => result.reason)
        .filter(Boolean);
    }

    async function reloadAllViews() {
      const failures = summarizeFailures(await Promise.allSettled([
        loadOverview(),
        loadChunks(),
        loadMetadata(),
        loadKg(),
        loadChroma(),
        ...(currentView() === "database" ? [loadDatabase()] : []),
        loadAgent(),
        loadOperations()
      ]));
      const authFailure = failures.find(error => error?.status === 401 || error?.status === 503);
      if (authFailure) {
        throw authFailure;
      }
      if (state.activeDocumentId) {
        try {
          await loadDocumentDetail(state.activeDocumentId);
        } catch (_) {}
      }
      if (state.selectedChunkId) {
        try {
          await loadChunkDetail(state.selectedChunkId);
        } catch (_) {}
      }
      if (state.selectedKgEntityId) {
        try {
          await loadKgEntityDetail(state.selectedKgEntityId);
        } catch (_) {}
      }
      if (state.selectedAgentRunId) {
        try {
          await loadAgentRunDetail(state.selectedAgentRunId);
        } catch (_) {}
      }
      state.autoRefresh.lastCompletedAt = new Date().toLocaleTimeString();
      if (failures.length) {
        setLiveStatus(
          `Live refresh partial · ${failures.length} view${failures.length === 1 ? "" : "s"} failed · last ${state.autoRefresh.lastCompletedAt}`,
          "error"
        );
      } else {
        setLiveStatus(
          state.autoRefresh.enabled
            ? `Live refresh on · last ${state.autoRefresh.lastCompletedAt}`
            : `Live refresh paused · last ${state.autoRefresh.lastCompletedAt}`,
          "idle"
        );
      }
    }

    async function runDocumentAction(documentId, action, payload = null) {
      let path = null;
      if (action === "rebuild") path = `/admin/api/documents/${documentId}/rebuild`;
      if (action === "revalidate") path = `/admin/api/documents/${documentId}/revalidate`;
      if (action === "reindex") path = `/admin/api/documents/${documentId}/reindex`;
      if (action === "reprocess-kg") path = `/admin/api/documents/${documentId}/reprocess-kg`;
      if (action === "delete") path = `/admin/api/documents/${documentId}/delete`;
      if (action === "auto-review") path = `/admin/api/chunks/review/auto`;
      if (!path) throw new Error(`Unsupported document action: ${action}`);
      const requestBody = action === "auto-review"
        ? { document_id: documentId, batch_size: 100 }
        : (payload || {});
      const result = await postJson(path, requestBody);
      if (action === "delete") {
        state.activeDocumentId = null;
      }
      setOperationResult(result);
      await reloadAllViews();
      return result;
    }

    async function loadRouteCatalog() {
      const routes = await loadJson("/admin/api/system/routes");
      state.routes = routes;
      const select = document.getElementById("ops-api-route");
      if (!select) return;
      const currentValue = select.value;
      select.innerHTML = `
        <option value="">Select API route</option>
        ${routes.map(route => `<option value="${escapeHtml(route.path)}">${escapeHtml(route.path)} [${escapeHtml(route.methods.join(", "))}]</option>`).join("")}
      `;
      if (currentValue && routes.some(route => route.path === currentValue)) {
        select.value = currentValue;
      }
    }

    async function loadProcessMonitor() {
      const [payload, ingestProgress] = await Promise.all([
        loadJson("/admin/api/system/processes?limit=12"),
        loadJson("/admin/api/system/ingest-progress")
      ]);
      const root = document.getElementById("ops-live-summary");
      if (!root) return;
      const active = payload.active_documents || [];
      const recentStages = payload.recent_stage_runs || [];
      const recentReviews = payload.recent_review_runs || [];
      const progress = ingestProgress.progress || null;
      const latestAssetWrite = ingestProgress.latest_asset_write || null;
      const phase = ingestProgress.phase || null;
      const phaseMetrics = phase?.metrics || progress?.phase_metrics || {};
      const currentOrdinal = progress?.current_file ? Number(progress.completed_files ?? 0) + 1 : null;
      const lastCompletedRun = progress?.runs && progress.runs.length ? progress.runs[progress.runs.length - 1] : null;
      const metricLines = [];
      if (phaseMetrics.page_current || phaseMetrics.page_total) metricLines.push(`page ${phaseMetrics.page_current || 0}/${phaseMetrics.page_total || 0}`);
      if (phaseMetrics.pages_prepared || phaseMetrics.page_total) metricLines.push(`pages prepared ${phaseMetrics.pages_prepared || 0}/${phaseMetrics.page_total || 0}`);
      if (phaseMetrics.assets_extracted || phaseMetrics.asset_total) metricLines.push(`assets extracted ${phaseMetrics.assets_extracted || 0}${phaseMetrics.asset_total ? `/${phaseMetrics.asset_total}` : ""}`);
      if (phaseMetrics.kg_completed || phaseMetrics.kg_total) metricLines.push(`KG ${phaseMetrics.kg_completed || 0}/${phaseMetrics.kg_total || 0}`);
      if (phaseMetrics.kg_failures) metricLines.push(`KG failures ${phaseMetrics.kg_failures}`);
      if (phaseMetrics.embedding_completed || phaseMetrics.embedding_total) metricLines.push(`embeddings ${phaseMetrics.embedding_completed || 0}/${phaseMetrics.embedding_total || 0}`);
      if (phaseMetrics.accepted || phaseMetrics.review || phaseMetrics.rejected) metricLines.push(`chunks accepted/review/rejected ${phaseMetrics.accepted || 0}/${phaseMetrics.review || 0}/${phaseMetrics.rejected || 0}`);
      if (phaseMetrics.vision_pages_used || phaseMetrics.vision_assets_used) metricLines.push(`vision pages/assets ${phaseMetrics.vision_pages_used || 0}/${phaseMetrics.vision_assets_used || 0}`);
      if (phaseMetrics.page_render_failures || phaseMetrics.asset_render_failures) metricLines.push(`render failures pages/assets ${phaseMetrics.page_render_failures || 0}/${phaseMetrics.asset_render_failures || 0}`);
      root.innerHTML = `
        <div class="summary-grid">
          <div class="summary-chip"><span class="muted">Documents</span><strong>${payload.overview?.documents ?? 0}</strong></div>
          <div class="summary-chip"><span class="muted">Accepted</span><strong>${payload.overview?.accepted_chunks ?? 0}</strong></div>
          <div class="summary-chip"><span class="muted">Review</span><strong>${payload.overview?.review_chunks ?? 0}</strong></div>
          <div class="summary-chip"><span class="muted">Active Docs</span><strong>${active.length}</strong></div>
          <div class="summary-chip"><span class="muted">Runner</span><strong>${ingestProgress.runner_active ? "active" : "idle"}</strong></div>
        </div>
        <div class="meta-item" style="margin-top: 12px;">
          <div><strong>Runner progress</strong></div>
          <div style="margin-top: 8px;">
            <div class="mono">runner active Â· ${escapeHtml(String(!!ingestProgress.runner_active))}</div>
            <div class="mono">runner pid Â· ${escapeHtml(String(ingestProgress.runner_pid || "n/a"))}</div>
            ${progress
              ? `
                <div class="mono">phase · ${escapeHtml(phase?.label || "unknown")}</div>
                <div class="mono">phase detail · ${escapeHtml(phase?.detail || "n/a")}</div>
                <div class="mono">document status · ${escapeHtml(phase?.document_status || "n/a")}</div>
                <div class="mono">latest stage · ${escapeHtml(phase?.latest_stage_name || "n/a")}</div>
                <div class="mono">status · ${escapeHtml(progress.status || "unknown")}</div>
                <div class="mono">ingesting now · ${escapeHtml(currentOrdinal ? `${currentOrdinal}/${progress.total_files ?? "?"} - ${progress.current_file || "n/a"}` : (progress.current_file || "n/a"))}</div>
                <div class="mono">last completed · ${escapeHtml(lastCompletedRun?.filename || "n/a")}</div>
                <div class="mono">completed files · ${escapeHtml(String(progress.completed_files ?? 0))} / ${escapeHtml(String(progress.total_files ?? 0))}</div>
                <div class="mono">last progress · ${escapeHtml(progress.last_progress_at || "n/a")}</div>
                <div class="mono">heartbeat · ${escapeHtml(progress.last_heartbeat_at || "n/a")}</div>
                ${metricLines.map(line => `<div class="mono">progress · ${escapeHtml(line)}</div>`).join("")}
                <div class="mono">phase metrics · ${escapeHtml(JSON.stringify(phaseMetrics))}</div>
                <div class="mono">asset files on disk · ${escapeHtml(String(ingestProgress.asset_file_count ?? 0))}</div>
                <div class="mono">latest asset write · ${escapeHtml(latestAssetWrite?.modified_at || "n/a")}</div>
                <div class="mono">latest asset path · ${escapeHtml(latestAssetWrite?.path || "n/a")}</div>
              `
              : `<div class="muted">No runner progress file found.</div>`}
          </div>
        </div>
        <div class="meta-item" style="margin-top: 12px;">
          <div><strong>Active documents</strong></div>
          <div style="margin-top: 8px;">
            ${active.length
              ? active.map(item => `<div class="mono">${escapeHtml(item.document_id)} · ${escapeHtml(item.filename)} · ${escapeHtml(item.status)}</div>`).join("")
              : `<div class="muted">No active documents.</div>`}
          </div>
        </div>
        <div class="meta-item" style="margin-top: 12px;">
          <div><strong>Most recent stage activity</strong></div>
          <div style="margin-top: 8px;">
            ${recentStages.slice(0, 6).map(item => `<div class="mono">${escapeHtml(item.stage_name)} · ${escapeHtml(item.document_id)} · ${escapeHtml(item.status)} · ${escapeHtml(item.finished_at || item.started_at || "")}</div>`).join("") || `<div class="muted">No stage activity yet.</div>`}
          </div>
        </div>
        <div class="meta-item" style="margin-top: 12px;">
          <div><strong>Most recent review activity</strong></div>
          <div style="margin-top: 8px;">
            ${recentReviews.slice(0, 6).map(item => `<div class="mono">${escapeHtml(item.decision)} · ${escapeHtml(item.chunk_id)} · ${escapeHtml(item.created_at || "")}</div>`).join("") || `<div class="muted">No review activity yet.</div>`}
          </div>
        </div>
      `;
    }

    async function runApiConsoleRequest() {
      const method = document.getElementById("ops-api-method").value;
      const path = document.getElementById("ops-api-path").value.trim();
      const bodyText = document.getElementById("ops-api-body").value.trim();
      if (!path) {
        throw new Error("Provide an API path first.");
      }
      const response = await fetch(path, {
        method,
        headers: bodyText && method !== "GET" ? adminHeaders({ "Content-Type": "application/json" }) : adminHeaders(),
        body: bodyText && method !== "GET" ? JSON.stringify(JSON.parse(bodyText)) : undefined
      });
      const payload = await response.text();
      let parsed = payload;
      try {
        parsed = JSON.parse(payload);
      } catch (_) {}
      const root = document.getElementById("ops-api-response");
      root.innerHTML = `
        <div class="meta-item">
          <div><strong>${escapeHtml(method)} ${escapeHtml(path)}</strong></div>
          <div class="muted" style="margin-top: 8px;">HTTP ${response.status}</div>
          <div class="mono" style="margin-top: 8px;">${escapeHtml(typeof parsed === "string" ? parsed : JSON.stringify(parsed, null, 2))}</div>
        </div>
      `;
      setOperationResult({ method, path, status: response.status, response: parsed }, !response.ok);
    }

    function bindReactiveFilter(inputId, onChange, pageState = null, eventName = "input") {
      const node = document.getElementById(inputId);
      if (!node) return;
      const handler = eventName === "input" ? debounce(async () => {
        if (pageState) pageState.offset = 0;
        await onChange();
      }) : async () => {
        if (pageState) pageState.offset = 0;
        await onChange();
      };
      node.addEventListener(eventName, handler);
      node.addEventListener("keydown", async (event) => {
        if (event.key === "Enter") {
          event.preventDefault();
          if (pageState) pageState.offset = 0;
          await onChange();
        }
      });
    }

    async function autoRefreshTick() {
      if (!state.autoRefresh.enabled || state.autoRefresh.inFlight || document.hidden) return;
      state.autoRefresh.inFlight = true;
      setLiveStatus("Live refresh running", "running");
      try {
        await reloadAllViews();
        await loadProcessMonitor();
      } catch (error) {
        setLiveStatus(`Live refresh error: ${error.message}`, "error");
      } finally {
        state.autoRefresh.inFlight = false;
      }
    }

    function restartAutoRefresh() {
      if (state.autoRefresh.timer) {
        window.clearInterval(state.autoRefresh.timer);
        state.autoRefresh.timer = null;
      }
      if (!state.autoRefresh.enabled) {
        setLiveStatus(
          state.autoRefresh.lastCompletedAt
            ? `Live refresh paused · last ${state.autoRefresh.lastCompletedAt}`
            : "Live refresh paused",
          "idle"
        );
        return;
      }
      setLiveStatus(
        state.autoRefresh.lastCompletedAt
          ? `Live refresh on · last ${state.autoRefresh.lastCompletedAt}`
          : "Live refresh on",
        "idle"
      );
      state.autoRefresh.timer = window.setInterval(autoRefreshTick, state.autoRefresh.intervalMs);
    }

    async function loadOverview() {
      const docsPage = state.paging.documents;
      const [overview, documents] = await Promise.all([
        loadJson("/admin/api/overview"),
        loadJson(`/admin/api/documents?limit=${docsPage.limit}&offset=${docsPage.offset}`)
      ]);

      state.documents = documents.items;
      const cards = document.getElementById("overview-cards");
      const labels = {
        documents: "Documents",
        jobs: "Jobs",
        chunks: "Chunks",
        accepted_chunks: "Accepted",
        review_chunks: "Review",
        rejected_chunks: "Rejected",
        kg_entities: "KG Entities",
        kg_assertions: "KG Assertions",
        agent_sessions: "Agent Sessions",
        agent_query_runs: "Agent Queries",
        agent_abstentions: "Agent Abstentions"
      };
      cards.innerHTML = Object.entries(labels).map(([key, label]) => `
        <div class="card">
          <h3>${label}</h3>
          <strong>${overview[key] ?? 0}</strong>
        </div>
      `).join("");

      renderDocumentsTable("overview-documents", documents.items.slice(0, 8), true);
      renderDocumentsTable("documents-table", documents.items, true);
      renderPager("documents-pager", docsPage, documents.total, loadOverview);
    }

    function renderDocumentsTable(id, rows, clickable) {
      const table = document.getElementById(id);
      table.innerHTML = `
        <thead>
          <tr>
            <th>Filename</th>
            <th>Class</th>
            <th>Chunks</th>
            <th>Accepted</th>
            <th>Review</th>
            <th>Rejected</th>
            <th>Document ID</th>
          </tr>
        </thead>
        <tbody>
          ${rows.map(row => `
            <tr ${clickable ? `data-document-id="${row.document_id}"` : ""}>
              <td>${escapeHtml(row.filename)}</td>
              <td>${escapeHtml(row.document_class)}</td>
              <td>${row.total_chunks}</td>
              <td>${row.accepted_chunks}</td>
              <td>${row.review_chunks}</td>
              <td>${row.rejected_chunks}</td>
              <td class="mono">${escapeHtml(row.document_id)}</td>
            </tr>
          `).join("")}
        </tbody>
      `;
      if (clickable) {
        table.querySelectorAll("tbody tr").forEach(row => {
          row.style.cursor = "pointer";
          row.addEventListener("click", async () => {
            switchView("documents");
            await loadDocumentDetail(row.dataset.documentId);
          });
        });
      }
    }

    function syncDocumentFilters(documentId) {
      document.getElementById("metadata-document-id").value = documentId;
      document.getElementById("kg-document-id").value = documentId;
      document.getElementById("chroma-document-id").value = documentId;
    }

    function attachDetailLinks(root) {
      root.querySelectorAll("[data-scroll-target]").forEach(node => {
        node.addEventListener("click", () => {
          const target = root.querySelector(`[data-row-id="${node.dataset.scrollTarget}"]`);
          if (!target) return;
          root.querySelectorAll(".focused-row").forEach(row => row.classList.remove("focused-row"));
          target.classList.add("focused-row");
          target.scrollIntoView({ behavior: "smooth", block: "center" });
        });
      });
    }

    async function loadDocumentDetail(documentId) {
      state.activeDocumentId = documentId;
      syncDocumentFilters(documentId);
      const root = document.getElementById("document-detail");
      root.innerHTML = `<p class="muted">Loading document bundle…</p>`;
      let detail;
      try {
        detail = await loadJson(`/admin/api/documents/${documentId}/bundle`);
      } catch (error) {
        root.innerHTML = `<div class="meta-item"><strong>Document detail failed to load</strong><div class="muted" style="margin-top:8px;">${escapeHtml(error.message)}</div></div>`;
        return;
      }

      Promise.allSettled([loadChunks(), loadMetadata(), loadKg(), loadChroma()]);
      const jobs = detail.jobs.map(job => `
        <div class="meta-item">
          <div><strong>${escapeHtml(job.job_id)}</strong></div>
          <div class="muted">${statusBadge(job.status)} parser=${escapeHtml(job.parser_version)} chunker=${escapeHtml(job.chunker_version)}</div>
          <div class="muted">embedding=${escapeHtml(job.embedding_version)} kg=${escapeHtml(job.kg_version)}</div>
        </div>
      `).join("") || "<p class='muted'>No jobs found.</p>";
      const stages = detail.stages.map(stage => `
        <div class="meta-item">
          <div><strong>${escapeHtml(stage.stage_name)}</strong> ${statusBadge(stage.status)}</div>
          <div class="muted mono">${escapeHtml(JSON.stringify(stage.metrics_json || {}))}</div>
          ${stage.error_message ? `<div class="mono">${escapeHtml(stage.error_message)}</div>` : ""}
        </div>
      `).join("") || "<p class='muted'>No stages found.</p>";

      const counts = {
        sources: detail.counts?.sources ?? detail.sources.length,
        pages: detail.counts?.pages ?? detail.pages.length,
        pageAssets: detail.counts?.page_assets ?? detail.page_assets.length,
        assetLinks: detail.counts?.chunk_asset_links ?? detail.chunk_asset_links.length,
        chunks: detail.counts?.chunks ?? detail.chunks.length,
        metadata: detail.counts?.metadata ?? detail.chunk_metadata.length,
        kgEntities: detail.counts?.kg_entities ?? detail.kg_entities.length,
        kgAssertions: detail.counts?.kg_assertions ?? detail.kg_assertions.length,
        kgEvidence: detail.counts?.kg_evidence ?? detail.kg_evidence.length,
        kgRaw: detail.counts?.kg_raw ?? detail.kg_raw.length,
        vectors: detail.counts?.vectors ?? detail.chroma_records.length,
        assetVectors: detail.counts?.asset_vectors ?? detail.asset_chroma_records.length
      };
      const parity = detail.counts?.parity || null;

      const sourceCards = detail.sources.map(source => `
        <div class="meta-item" data-row-id="source:${source.source_id}">
          <div><strong>${escapeHtml(source.source_id)}</strong></div>
          <div class="muted mono">created=${escapeHtml(source.created_at)}</div>
          <div class="muted mono">${escapeHtml(JSON.stringify(source.extraction_metrics_json || {}))}</div>
          <div class="actions" style="margin-top: 8px;">
            <button class="action-btn" data-editor-type="source" data-editor-id="${source.source_id}">Edit Source</button>
          </div>
          <details style="margin-top: 10px;">
            <summary>Raw Text</summary>
            <div class="chunk" style="margin-top: 8px;">${escapeHtml(source.raw_text || "")}</div>
          </details>
          <details style="margin-top: 10px;">
            <summary>Normalized Text</summary>
            <div class="chunk" style="margin-top: 8px;">${escapeHtml(source.normalized_text || "")}</div>
          </details>
        </div>
      `).join("") || "<p class='muted'>No source rows found.</p>";

      const pageRows = detail.pages.map(page => `
        <tr>
          <td>${page.page_number}</td>
          <td class="mono">${escapeHtml(shortText(page.extracted_text || "", 220))}</td>
          <td class="mono">${escapeHtml(shortText(page.ocr_text || "", 220))}</td>
          <td class="mono">${escapeHtml((page.metadata_json?.important_terms || []).join(", "))}</td>
          <td>${page.page_image_path ? `<a class="doc-link" href="${escapeHtml(`/admin/api/assets/${encodeURIComponent(detail.page_assets.find(item => item.page_number === page.page_number && item.asset_type === "page_image")?.asset_id || "")}/image`)}" target="_blank" rel="noreferrer">open image</a>` : ""}</td>
          <td><button class="action-btn" data-editor-type="page" data-editor-id="${detail.document.document_id}" data-editor-secondary-id="${page.page_number}">Edit</button></td>
        </tr>
      `).join("") || `<tr><td colspan="6" class="muted">No page rows found.</td></tr>`;

      const assetRows = detail.page_assets.map(asset => `
        <tr data-asset-id="${asset.asset_id}">
          <td class="mono">${escapeHtml(asset.asset_id)}</td>
          <td>${asset.page_number}</td>
          <td>${escapeHtml(asset.asset_type || "")}</td>
          <td>${escapeHtml(asset.metadata_json?.label || "")}</td>
          <td class="chunk">${escapeHtml(shortText(asset.description_text || asset.ocr_text || asset.search_text || "", 220))}</td>
          <td><a class="doc-link" href="/admin/api/assets/${encodeURIComponent(asset.asset_id)}/image" target="_blank" rel="noreferrer">image</a></td>
          <td><button class="action-btn" data-editor-type="asset" data-editor-id="${asset.asset_id}">Edit</button></td>
        </tr>
      `).join("") || `<tr><td colspan="7" class="muted">No page assets found.</td></tr>`;

      const assetLinkRows = detail.chunk_asset_links.map(link => `
        <tr>
          <td><span class="doc-link mono" data-scroll-target="chunk:${link.chunk_id}">${escapeHtml(link.chunk_id)}</span></td>
          <td class="mono">${escapeHtml(link.asset_id)}</td>
          <td>${escapeHtml(link.link_type || "")}</td>
          <td>${link.confidence ?? ""}</td>
          <td><button class="action-btn" data-editor-type="asset_link" data-editor-id="${link.link_id}">Edit</button></td>
        </tr>
      `).join("") || `<tr><td colspan="5" class="muted">No chunk-asset links found.</td></tr>`;

      const chunkRows = detail.chunks.map(chunk => `
        <tr data-row-id="chunk:${chunk.chunk_id}">
          <td class="mono">${escapeHtml(chunk.chunk_id)}</td>
          <td>${statusBadge(chunk.validation_status)}</td>
          <td>${chunk.page_start ?? ""}-${chunk.page_end ?? ""}</td>
          <td>${escapeHtml(chunk.metadata_json?.chunk_role || "")}</td>
          <td>${escapeHtml(chunk.metadata_json?.section_title || "")}</td>
          <td class="chunk">${escapeHtml(chunk.text)}</td>
        </tr>
      `).join("") || `<tr><td colspan="6" class="muted">No chunks found.</td></tr>`;

      const metadataRows = detail.chunk_metadata.map(row => `
        <tr data-row-id="chunk:${row.chunk_id}">
          <td class="mono">${escapeHtml(row.chunk_id)}</td>
          <td>${statusBadge(row.validation_status)}</td>
          <td>${escapeHtml(row.chunk_role || "")}</td>
          <td class="mono">${escapeHtml(JSON.stringify(row.ontology_classes || []))}</td>
          <td class="mono">${escapeHtml(JSON.stringify(row.metadata_json || {}))}</td>
        </tr>
      `).join("") || `<tr><td colspan="5" class="muted">No metadata rows found.</td></tr>`;

      const kgEntityRows = detail.kg_entities.map(entity => `
        <tr>
          <td class="mono">${escapeHtml(entity.entity_id)}</td>
          <td>${escapeHtml(entity.canonical_name)}</td>
          <td>${escapeHtml(entity.entity_type)}</td>
          <td class="mono">${escapeHtml(entity.source || "")}</td>
        </tr>
      `).join("") || `<tr><td colspan="4" class="muted">No KG entities found.</td></tr>`;

      const kgAssertionRows = detail.kg_assertions.map(assertion => `
        <tr data-row-id="assertion:${assertion.assertion_id}">
          <td class="mono">${escapeHtml(assertion.assertion_id)}</td>
          <td>${statusBadge(assertion.status)}</td>
          <td><span class="doc-link mono" data-scroll-target="chunk:${assertion.chunk_id}">${escapeHtml(assertion.chunk_id)}</span></td>
          <td class="mono">${escapeHtml(assertion.subject_entity_id)}</td>
          <td>${escapeHtml(assertion.predicate)}</td>
          <td class="mono">${escapeHtml(assertion.object_entity_id || assertion.object_literal || "")}</td>
          <td><button class="action-btn" data-editor-type="kg_assertion" data-editor-id="${assertion.assertion_id}">Edit</button></td>
        </tr>
      `).join("") || `<tr><td colspan="7" class="muted">No KG assertions found.</td></tr>`;

      const kgEvidenceRows = detail.kg_evidence.map(item => `
        <tr>
          <td class="mono">${escapeHtml(item.evidence_id)}</td>
          <td><span class="doc-link mono" data-scroll-target="assertion:${item.assertion_id}">${escapeHtml(item.assertion_id)}</span></td>
          <td><span class="doc-link mono" data-scroll-target="chunk:${item.chunk_id}">${escapeHtml(item.chunk_id)}</span></td>
          <td class="chunk">${escapeHtml(shortText(item.excerpt || "", 420))}</td>
          <td class="mono">${item.start_offset ?? ""}-${item.end_offset ?? ""}</td>
        </tr>
      `).join("") || `<tr><td colspan="5" class="muted">No KG evidence found.</td></tr>`;

      const kgRawRows = detail.kg_raw.map(item => `
        <tr>
          <td class="mono">${escapeHtml(item.extraction_id)}</td>
          <td>${statusBadge(item.status)}</td>
          <td><span class="doc-link mono" data-scroll-target="chunk:${item.chunk_id}">${escapeHtml(item.chunk_id)}</span></td>
          <td class="mono">${escapeHtml(JSON.stringify(item.payload?.errors || []))}</td>
          <td><button class="action-btn" data-editor-type="kg_raw" data-editor-id="${item.extraction_id}">Edit</button></td>
        </tr>
      `).join("") || `<tr><td colspan="5" class="muted">No KG raw records found.</td></tr>`;

      const chromaRows = detail.chroma_records.map(item => `
        <tr>
          <td class="mono">${escapeHtml(item.id)}</td>
          <td><span class="doc-link mono" data-scroll-target="chunk:${item.metadata?.chunk_id || ""}">${escapeHtml(item.metadata?.chunk_id || "")}</span></td>
          <td class="chunk">${escapeHtml(shortText(item.document || "", 280))}</td>
          <td class="mono">${escapeHtml(JSON.stringify(item.metadata || {}))}</td>
        </tr>
      `).join("") || `<tr><td colspan="4" class="muted">No Chroma vectors found.</td></tr>`;

      const assetChromaRows = detail.asset_chroma_records.map(item => `
        <tr>
          <td class="mono">${escapeHtml(item.id)}</td>
          <td>${escapeHtml(item.metadata?.page_number || "")}</td>
          <td>${escapeHtml(item.metadata?.asset_type || "")}</td>
          <td class="chunk">${escapeHtml(shortText(item.document || "", 220))}</td>
          <td class="mono">${escapeHtml(JSON.stringify(item.metadata || {}))}</td>
        </tr>
      `).join("") || `<tr><td colspan="5" class="muted">No asset vectors found.</td></tr>`;

      root.innerHTML = `
        <div class="detail-stack">
          <div class="detail-section">
            <h4>Document Summary</h4>
            <div class="section-body">
              <div class="meta-item">
                <div><strong>${escapeHtml(detail.document.filename)}</strong></div>
                <div class="muted">class=${escapeHtml(detail.document.document_class)} tenant=${escapeHtml(detail.document.tenant_id)} source=${escapeHtml(detail.document.source_type)} ${statusBadge(detail.document.status)}</div>
                <div class="mono">${escapeHtml(detail.document.document_id)}</div>
                <div class="muted mono">content_hash=${escapeHtml(detail.document.content_hash || "")}</div>
              </div>
              <div class="actions" style="margin-top: 12px;">
                <button class="action-btn" data-doc-action="rebuild" data-document-id="${detail.document.document_id}">Rebuild</button>
                <button class="action-btn" data-doc-action="revalidate" data-document-id="${detail.document.document_id}" data-rerun-kg="true">Revalidate + KG</button>
                <button class="action-btn" data-doc-action="revalidate" data-document-id="${detail.document.document_id}" data-rerun-kg="false">Revalidate Only</button>
                <button class="action-btn" data-doc-action="reindex" data-document-id="${detail.document.document_id}">Reindex</button>
                <button class="action-btn" data-doc-action="reprocess-kg" data-document-id="${detail.document.document_id}">Replay KG</button>
                <button class="action-btn" data-doc-action="auto-review" data-document-id="${detail.document.document_id}">Auto Review</button>
                <button class="action-btn reject" data-doc-action="delete" data-document-id="${detail.document.document_id}">Delete</button>
              </div>
              <div class="summary-grid" style="margin-top: 12px;">
                <div class="summary-chip"><span class="muted">Sources</span><strong>${counts.sources}</strong></div>
                <div class="summary-chip"><span class="muted">Pages</span><strong>${counts.pages}</strong></div>
                <div class="summary-chip"><span class="muted">Page Assets</span><strong>${counts.pageAssets}</strong></div>
                <div class="summary-chip"><span class="muted">Asset Links</span><strong>${counts.assetLinks}</strong></div>
                <div class="summary-chip"><span class="muted">Chunks</span><strong>${counts.chunks}</strong></div>
                <div class="summary-chip"><span class="muted">Metadata</span><strong>${counts.metadata}</strong></div>
                <div class="summary-chip"><span class="muted">KG Entities</span><strong>${counts.kgEntities}</strong></div>
                <div class="summary-chip"><span class="muted">KG Assertions</span><strong>${counts.kgAssertions}</strong></div>
                <div class="summary-chip"><span class="muted">KG Evidence</span><strong>${counts.kgEvidence}</strong></div>
                <div class="summary-chip"><span class="muted">KG Raw</span><strong>${counts.kgRaw}</strong></div>
                <div class="summary-chip"><span class="muted">Vectors</span><strong>${counts.vectors}</strong></div>
                <div class="summary-chip"><span class="muted">Asset Vectors</span><strong>${counts.assetVectors}</strong></div>
              </div>
              ${parity ? `
              <div class="summary-grid" style="margin-top: 12px;">
                <div class="summary-chip"><span class="muted">Missing Vectors</span><strong>${parity.missing_vectors_total}</strong></div>
                <div class="summary-chip"><span class="muted">Extra Vectors</span><strong>${parity.extra_vectors_total}</strong></div>
              </div>
              ` : ""}
            </div>
          </div>
          <div class="detail-section">
            <h4>Document Drilldown Filter</h4>
            <div class="section-body">
              <input id="document-detail-search" placeholder="Filter this document view across chunks, metadata, KG, and vectors" style="width: 100%; min-width: 0;" />
            </div>
          </div>
          <div class="detail-section">
            <h4>Source Records</h4>
            <div class="section-body meta-list">${sourceCards}</div>
          </div>
          <div class="detail-section">
            <h4>Document Pages</h4>
            <div class="section-body">
              <div style="margin-bottom: 8px;">${countNote(detail.pages.length, counts.pages)}</div>
              <table>
                <thead><tr><th>Page</th><th>Extracted Text</th><th>LLM OCR</th><th>Important Terms</th><th>Image</th><th>Edit</th></tr></thead>
                <tbody>${pageRows}</tbody>
              </table>
            </div>
          </div>
          <div class="detail-section">
            <h4>Page Assets</h4>
            <div class="section-body">
              <div style="margin-bottom: 8px;">${countNote(detail.page_assets.length, counts.pageAssets)}</div>
              <table>
                <thead><tr><th>Asset</th><th>Page</th><th>Type</th><th>Label</th><th>Description / OCR</th><th>Image</th><th>Edit</th></tr></thead>
                <tbody>${assetRows}</tbody>
              </table>
            </div>
          </div>
          <div class="detail-section">
            <h4>Chunk Asset Links</h4>
            <div class="section-body">
              <div style="margin-bottom: 8px;">${countNote(detail.chunk_asset_links.length, counts.assetLinks)}</div>
              <table>
                <thead><tr><th>Chunk</th><th>Asset</th><th>Link Type</th><th>Confidence</th><th>Edit</th></tr></thead>
                <tbody>${assetLinkRows}</tbody>
              </table>
            </div>
          </div>
          <div class="detail-section">
            <h4>Jobs</h4>
            <div class="section-body meta-list">${jobs}</div>
          </div>
          <div class="detail-section">
            <h4>Stages</h4>
            <div class="section-body meta-list">${stages}</div>
          </div>
          <div class="detail-section">
            <h4>Chunks</h4>
            <div class="section-body">
              <div style="margin-bottom: 8px;">${countNote(detail.chunks.length, counts.chunks)}</div>
              <table>
                <thead><tr><th>Chunk</th><th>Status</th><th>Pages</th><th>Role</th><th>Section</th><th>Text</th></tr></thead>
                <tbody>${chunkRows}</tbody>
              </table>
            </div>
          </div>
          <div class="detail-section">
            <h4>Chunk Metadata</h4>
            <div class="section-body">
              <div style="margin-bottom: 8px;">${countNote(detail.chunk_metadata.length, counts.metadata)}</div>
              <table>
                <thead><tr><th>Chunk</th><th>Status</th><th>Role</th><th>Ontology</th><th>Metadata</th></tr></thead>
                <tbody>${metadataRows}</tbody>
              </table>
            </div>
          </div>
          <div class="detail-section">
            <h4>KG Entities</h4>
            <div class="section-body">
              <div style="margin-bottom: 8px;">${countNote(detail.kg_entities.length, counts.kgEntities)}</div>
              <table>
                <thead><tr><th>Entity</th><th>Name</th><th>Type</th><th>Source</th></tr></thead>
                <tbody>${kgEntityRows}</tbody>
              </table>
            </div>
          </div>
          <div class="detail-section">
            <h4>KG Assertions</h4>
            <div class="section-body">
              <div style="margin-bottom: 8px;">${countNote(detail.kg_assertions.length, counts.kgAssertions)}</div>
              <table>
                <thead><tr><th>Assertion</th><th>Status</th><th>Chunk</th><th>Subject</th><th>Predicate</th><th>Object</th><th>Edit</th></tr></thead>
                <tbody>${kgAssertionRows}</tbody>
              </table>
            </div>
          </div>
          <div class="detail-section">
            <h4>KG Evidence</h4>
            <div class="section-body">
              <div style="margin-bottom: 8px;">${countNote(detail.kg_evidence.length, counts.kgEvidence)}</div>
              <table>
                <thead><tr><th>Evidence</th><th>Assertion</th><th>Chunk</th><th>Excerpt</th><th>Offsets</th></tr></thead>
                <tbody>${kgEvidenceRows}</tbody>
              </table>
            </div>
          </div>
          <div class="detail-section">
            <h4>KG Raw Extractions</h4>
            <div class="section-body">
              <div style="margin-bottom: 8px;">${countNote(detail.kg_raw.length, counts.kgRaw)}</div>
              <table>
                <thead><tr><th>Extraction</th><th>Status</th><th>Chunk</th><th>Errors</th><th>Edit</th></tr></thead>
                <tbody>${kgRawRows}</tbody>
              </table>
            </div>
          </div>
          <div class="detail-section">
            <h4>Chroma Records</h4>
            <div class="section-body">
              <div style="margin-bottom: 8px;">${countNote(detail.chroma_records.length, counts.vectors)}</div>
              ${detail.chroma_error ? `<div class="meta-item" style="margin-bottom: 10px;"><strong>Chroma unavailable</strong><div class="muted" style="margin-top:8px;">${escapeHtml(detail.chroma_error)}</div></div>` : ""}
              <table>
                <thead><tr><th>Record</th><th>Chunk</th><th>Text</th><th>Metadata</th></tr></thead>
                <tbody>${chromaRows}</tbody>
              </table>
            </div>
          </div>
          <div class="detail-section">
            <h4>Asset Vectors</h4>
            <div class="section-body">
              <div style="margin-bottom: 8px;">${countNote(detail.asset_chroma_records.length, counts.assetVectors)}</div>
              ${detail.asset_chroma_error ? `<div class="meta-item" style="margin-bottom: 10px;"><strong>Asset vector store unavailable</strong><div class="muted" style="margin-top:8px;">${escapeHtml(detail.asset_chroma_error)}</div></div>` : ""}
              <table>
                <thead><tr><th>Record</th><th>Page</th><th>Type</th><th>Text</th><th>Metadata</th></tr></thead>
                <tbody>${assetChromaRows}</tbody>
              </table>
            </div>
          </div>
        </div>
      `;
      attachDetailLinks(root);
      const searchInput = root.querySelector("#document-detail-search");
      if (searchInput) {
        searchInput.addEventListener("input", () => {
          const needle = searchInput.value.trim().toLowerCase();
          root.querySelectorAll("tbody tr, .meta-list .meta-item").forEach(node => {
            if (!needle) {
              node.classList.remove("hidden");
              return;
            }
            node.classList.toggle("hidden", !node.textContent.toLowerCase().includes(needle));
          });
        });
      }
      root.querySelectorAll("[data-doc-action]").forEach(button => {
        button.addEventListener("click", async () => {
          const action = button.dataset.docAction;
          const targetDocumentId = button.dataset.documentId;
          const confirmMessage = action === "delete"
            ? `Delete document ${targetDocumentId}? This removes chunks, vectors, and KG rows.`
            : `Run ${action} for document ${targetDocumentId}?`;
          if (!window.confirm(confirmMessage)) return;
          button.disabled = true;
          try {
            let payload = {};
            if (action === "revalidate") {
              payload = { rerun_kg: button.dataset.rerunKg !== "false" };
            } else if (action === "reprocess-kg") {
              payload = { batch_size: 100 };
            }
            await runDocumentAction(targetDocumentId, action, payload);
            if (action !== "delete") {
              await loadDocumentDetail(targetDocumentId);
            } else {
              document.getElementById("document-detail").innerHTML = `<p class="muted">Select a document to inspect jobs and stages.</p>`;
            }
          } catch (error) {
            setOperationResult(error.message, true);
            alert(error.message);
          } finally {
            button.disabled = false;
          }
        });
      });
      root.querySelectorAll("[data-editor-type][data-editor-id]").forEach(button => {
        button.addEventListener("click", async (event) => {
          event.preventDefault();
          event.stopPropagation();
          setEditorSelection(
            button.dataset.editorType,
            button.dataset.editorId,
            button.dataset.editorSecondaryId || "",
            { autoLoad: true }
          );
          switchView("operations");
        });
      });
    }

    async function loadChunkDetail(chunkId) {
      state.selectedChunkId = chunkId;
      const root = document.getElementById("chunk-detail");
      root.innerHTML = `<p class="muted">Loading chunk detail…</p>`;
      try {
        const detail = await loadJson(`/admin/api/chunks/${encodeURIComponent(chunkId)}`);
        const metadata = detail.metadata || {};
        const chunk = detail.chunk || {};
        const neighbors = (detail.neighbors || []).map(item => `
          <tr>
            <td class="mono">${escapeHtml(item.chunk_id)}</td>
            <td>${statusBadge(item.validation_status)}</td>
            <td>${item.page_start ?? ""}-${item.page_end ?? ""}</td>
            <td class="chunk">${escapeHtml(shortText(item.text || "", 220))}</td>
          </tr>
        `).join("") || `<tr><td colspan="4" class="muted">No adjacent chunks.</td></tr>`;
        const assertions = (detail.assertions || []).map(item => `
          <tr>
            <td class="mono">${escapeHtml(item.assertion_id)}</td>
            <td>${escapeHtml(item.predicate)}</td>
            <td class="mono">${escapeHtml(item.subject_entity_id || "")}</td>
            <td class="mono">${escapeHtml(item.object_entity_id || item.object_literal || "")}</td>
          </tr>
        `).join("") || `<tr><td colspan="4" class="muted">No KG assertions for this chunk.</td></tr>`;
        const evidence = (detail.evidence || []).map(item => `
          <tr>
            <td class="mono">${escapeHtml(item.evidence_id)}</td>
            <td class="mono">${escapeHtml(item.assertion_id)}</td>
            <td class="chunk">${escapeHtml(shortText(item.excerpt || "", 220))}</td>
          </tr>
        `).join("") || `<tr><td colspan="3" class="muted">No KG evidence for this chunk.</td></tr>`;
        const raw = (detail.raw_extractions || []).map(item => `
          <tr>
            <td class="mono">${escapeHtml(item.extraction_id)}</td>
            <td>${statusBadge(item.status)}</td>
            <td class="mono">${escapeHtml(JSON.stringify(item.payload?.errors || []))}</td>
            <td><button class="action-btn" data-editor-type="kg_raw" data-editor-id="${item.extraction_id}">Edit</button></td>
          </tr>
        `).join("") || `<tr><td colspan="4" class="muted">No raw KG extraction rows.</td></tr>`;
        const linkedAssets = (detail.linked_assets || []).map(item => `
          <tr>
            <td class="mono">${escapeHtml(item.asset_id)}</td>
            <td>${item.page_number ?? ""}</td>
            <td>${escapeHtml(item.asset_type || "")}</td>
            <td>${escapeHtml(item.link_type || "")}</td>
            <td class="chunk">${escapeHtml(shortText(item.description_text || item.ocr_text || item.search_text || "", 220))}</td>
            <td><a class="doc-link" href="/admin/api/assets/${encodeURIComponent(item.asset_id)}/image" target="_blank" rel="noreferrer">image</a></td>
            <td><button class="action-btn" data-editor-type="asset" data-editor-id="${item.asset_id}">Edit</button></td>
          </tr>
        `).join("") || `<tr><td colspan="7" class="muted">No linked assets for this chunk.</td></tr>`;

        root.innerHTML = `
          <div class="detail-stack">
            <div class="meta-item">
              <div><strong>${escapeHtml(chunk.chunk_id || chunkId)}</strong></div>
              <div class="muted">${statusBadge(chunk.validation_status)} pages=${chunk.page_start ?? ""}-${chunk.page_end ?? ""} index=${chunk.chunk_index ?? ""}</div>
              <div class="muted mono">doc=${escapeHtml(chunk.document_id || "")}</div>
              <div class="muted mono">prev=${escapeHtml(chunk.prev_chunk_id || "")} next=${escapeHtml(chunk.next_chunk_id || "")}</div>
              <div class="actions" style="margin-top: 8px;">
                <button class="action-btn" data-editor-type="chunk" data-editor-id="${chunk.chunk_id || chunkId}">Edit Chunk</button>
                <button class="action-btn" data-editor-type="document" data-editor-id="${chunk.document_id || ""}">Edit Document</button>
              </div>
            </div>
            <div class="detail-section">
              <h4>Chunk Text</h4>
              <div class="section-body"><div class="chunk">${escapeHtml(chunk.text || "")}</div></div>
            </div>
            <div class="detail-section">
              <h4>Metadata</h4>
              <div class="section-body">
                <div class="meta-item">
                  <div class="muted mono">${escapeHtml(JSON.stringify(metadata.metadata_json || chunk.metadata_json || {}, null, 2))}</div>
                </div>
                <div class="summary-grid" style="margin-top: 10px;">
                  <div class="summary-chip"><span class="muted">Role</span><strong>${escapeHtml(metadata.chunk_role || "")}</strong></div>
                  <div class="summary-chip"><span class="muted">Section</span><strong>${escapeHtml(metadata.section_title || "")}</strong></div>
                  <div class="summary-chip"><span class="muted">Quality</span><strong>${metadata.quality_score ?? ""}</strong></div>
                  <div class="summary-chip"><span class="muted">Reasons</span><strong>${escapeHtml((metadata.reasons || []).join(", "))}</strong></div>
                </div>
              </div>
            </div>
            <div class="detail-section">
              <h4>Neighbors</h4>
              <div class="section-body"><table><thead><tr><th>Chunk</th><th>Status</th><th>Pages</th><th>Text</th></tr></thead><tbody>${neighbors}</tbody></table></div>
            </div>
            <div class="detail-section">
              <h4>Linked Assets</h4>
              <div class="section-body"><table><thead><tr><th>Asset</th><th>Page</th><th>Type</th><th>Link</th><th>Description / OCR</th><th>Image</th><th>Edit</th></tr></thead><tbody>${linkedAssets}</tbody></table></div>
            </div>
            <div class="detail-section">
              <h4>KG Assertions</h4>
              <div class="section-body"><table><thead><tr><th>Assertion</th><th>Predicate</th><th>Subject</th><th>Object</th></tr></thead><tbody>${assertions}</tbody></table></div>
            </div>
            <div class="detail-section">
              <h4>KG Evidence</h4>
              <div class="section-body"><table><thead><tr><th>Evidence</th><th>Assertion</th><th>Excerpt</th></tr></thead><tbody>${evidence}</tbody></table></div>
            </div>
            <div class="detail-section">
              <h4>KG Raw Extractions</h4>
              <div class="section-body"><table><thead><tr><th>Extraction</th><th>Status</th><th>Errors</th><th>Edit</th></tr></thead><tbody>${raw}</tbody></table></div>
            </div>
            <div class="detail-section">
              <h4>Chroma Record</h4>
              <div class="section-body">
                ${detail.chroma_error ? `<div class="meta-item"><strong>Chroma unavailable</strong><div class="muted" style="margin-top:8px;">${escapeHtml(detail.chroma_error)}</div></div>` : ""}
                ${detail.chroma_record ? `
                  <div class="meta-item">
                    <div class="mono">${escapeHtml(detail.chroma_record.id)}</div>
                    <div class="muted" style="margin: 8px 0;">${escapeHtml(structuredMetadataSummary(detail.chroma_record.metadata || {}))}</div>
                    <div class="chunk">${escapeHtml(detail.chroma_record.document || "")}</div>
                    <details style="margin-top: 10px;">
                      <summary>Raw metadata</summary>
                      <div class="mono" style="margin-top: 8px;">${escapeHtml(JSON.stringify(detail.chroma_record.metadata || {}, null, 2))}</div>
                    </details>
                  </div>
                ` : `<p class="muted">No Chroma record for this chunk.</p>`}
              </div>
            </div>
          </div>
        `;
        root.querySelectorAll("[data-editor-type][data-editor-id]").forEach(button => {
          button.addEventListener("click", async (event) => {
            event.preventDefault();
            event.stopPropagation();
            setEditorSelection(
              button.dataset.editorType,
              button.dataset.editorId,
              button.dataset.editorSecondaryId || "",
              { autoLoad: true }
            );
            switchView("operations");
          });
        });
      } catch (error) {
        root.innerHTML = `<div class="meta-item"><strong>Chunk detail failed to load</strong><div class="muted" style="margin-top:8px;">${escapeHtml(error.message)}</div></div>`;
      }
    }

    async function loadChunks() {
      const page = state.paging.chunks;
      const documentId = document.getElementById("chunk-document-id").value.trim();
      const status = document.getElementById("chunk-status").value;
      const textFilter = document.getElementById("chunk-text-filter").value.trim().toLowerCase();
      const params = new URLSearchParams();
      if (documentId) params.set("document_id", documentId);
      if (status) params.set("status", status);
      params.set("limit", String(page.limit));
      params.set("offset", String(page.offset));
      const response = await loadJson(`/admin/api/chunks?${params.toString()}`);
      const filteredChunks = response.items.filter(chunk => {
        if (!textFilter) return true;
        return [
          chunk.text || "",
          chunk.metadata_json?.section_title || "",
          chunk.metadata_json?.chunk_role || "",
          chunk.chunk_id || "",
        ].join(" ").toLowerCase().includes(textFilter);
        });
      const table = document.getElementById("chunks-table");
      table.innerHTML = `
        <thead>
          <tr>
            <th>Chunk</th>
            <th>Status</th>
            <th>Pages</th>
            <th>Role</th>
            <th>Section</th>
            <th>Actions</th>
            <th>Text</th>
          </tr>
        </thead>
        <tbody>
          ${filteredChunks.map(chunk => `
            <tr data-chunk-id="${chunk.chunk_id}" class="${state.selectedChunkId === chunk.chunk_id ? "focused-row" : ""}">
              <td class="mono">${escapeHtml(chunk.chunk_id)}</td>
              <td>${statusBadge(chunk.validation_status)}</td>
              <td>${chunk.page_start ?? ""}-${chunk.page_end ?? ""}</td>
              <td>${escapeHtml(chunk.metadata_json?.chunk_role || "")}</td>
              <td>${escapeHtml(chunk.metadata_json?.section_title || "")}</td>
              <td>
                ${chunk.validation_status === "review" ? `
                  <div class="actions">
                    <button class="action-btn accept" data-action="accept" data-chunk-id="${chunk.chunk_id}">Accept</button>
                    <button class="action-btn reject" data-action="reject" data-chunk-id="${chunk.chunk_id}">Reject</button>
                  </div>
                ` : `<span class="muted">-</span>`}
              </td>
              <td class="chunk">${escapeHtml(chunk.text)}</td>
            </tr>
          `).join("")}
        </tbody>
      `;
      table.querySelectorAll("tbody tr[data-chunk-id]").forEach(row => {
        row.addEventListener("click", async (event) => {
          if (event.target.closest(".action-btn")) return;
          await loadChunkDetail(row.dataset.chunkId);
        });
      });
      table.querySelectorAll(".action-btn").forEach(button => {
        button.addEventListener("click", async () => {
          button.disabled = true;
          try {
            await postJson(`/admin/api/chunks/${button.dataset.chunkId}/decision`, {
              action: button.dataset.action
            });
            await Promise.all([loadOverview(), loadChunks(), loadMetadata(), loadKg(), loadChroma()]);
            await loadChunkDetail(button.dataset.chunkId);
          } catch (error) {
            await Promise.allSettled([loadOverview(), loadChunks(), loadKg(), loadMetadata(), loadChroma()]);
            alert(error.message);
          } finally {
            button.disabled = false;
          }
        });
      });

      if (state.selectedChunkId && filteredChunks.some(chunk => chunk.chunk_id === state.selectedChunkId)) {
        await loadChunkDetail(state.selectedChunkId);
      } else if (!state.selectedChunkId && filteredChunks.length) {
        await loadChunkDetail(filteredChunks[0].chunk_id);
      }
      renderPager("chunks-pager", page, response.total, loadChunks);
    }

    async function runAutoReview() {
      const documentId = document.getElementById("chunk-document-id").value.trim();
      const batchSize = Number(document.getElementById("review-batch-size").value || "50");
      const scope = documentId ? `document ${documentId}` : "all review chunks";
      if (!window.confirm(`Run the LLM reviewer for ${scope}?`)) {
        return;
      }
      const result = await postJson("/admin/api/chunks/review/auto", {
        document_id: documentId || null,
        batch_size: Math.max(1, Math.min(batchSize || 50, 500))
      });
      await Promise.all([loadOverview(), loadChunks(), loadMetadata(), loadKg(), loadChroma()]);
      alert(`Auto review complete. Processed=${result.processed_chunks} Accepted=${result.accepted} Rejected=${result.rejected} StillReview=${result.review}`);
    }

    async function loadKgEntityDetail(entityId) {
      state.selectedKgEntityId = entityId;
      const root = document.getElementById("kg-entity-detail");
      root.innerHTML = `<p class="muted">Loading KG entity detail…</p>`;
      try {
        const detail = await loadJson(`/admin/api/kg/entities/${encodeURIComponent(entityId)}`);
        const entity = detail.entity || {};
        const assertions = (detail.assertions || []).map(item => `
          <tr>
            <td class="mono">${escapeHtml(item.assertion_id)}</td>
            <td>${escapeHtml(item.predicate)}</td>
            <td class="mono">${escapeHtml(item.subject_entity_id || "")}</td>
            <td class="mono">${escapeHtml(item.object_entity_id || item.object_literal || "")}</td>
            <td>${item.confidence ?? ""}</td>
            <td><button class="action-btn" data-editor-type="kg_assertion" data-editor-id="${item.assertion_id}">Edit</button></td>
          </tr>
        `).join("") || `<tr><td colspan="6" class="muted">No linked assertions.</td></tr>`;
        const evidence = (detail.evidence || []).map(item => `
          <tr>
            <td class="mono">${escapeHtml(item.evidence_id)}</td>
            <td class="mono">${escapeHtml(item.assertion_id)}</td>
            <td class="chunk">${escapeHtml(shortText(item.excerpt || "", 240))}</td>
          </tr>
        `).join("") || `<tr><td colspan="3" class="muted">No linked evidence.</td></tr>`;
        const chunks = (detail.chunks || []).map(item => `
          <tr data-linked-chunk-id="${item.chunk_id}">
            <td class="mono">${escapeHtml(item.chunk_id)}</td>
            <td>${statusBadge(item.validation_status)}</td>
            <td>${item.page_start ?? ""}-${item.page_end ?? ""}</td>
            <td class="chunk">${escapeHtml(shortText(item.text || "", 220))}</td>
          </tr>
        `).join("") || `<tr><td colspan="4" class="muted">No linked chunks.</td></tr>`;
        const documents = (detail.documents || []).map(item => `
          <tr data-linked-document-id="${item.document_id}">
            <td>${escapeHtml(item.filename)}</td>
            <td>${escapeHtml(item.document_class || "")}</td>
            <td>${statusBadge(item.status)}</td>
            <td class="mono">${escapeHtml(item.document_id)}</td>
          </tr>
        `).join("") || `<tr><td colspan="4" class="muted">No linked documents.</td></tr>`;

        root.innerHTML = `
          <div class="detail-stack">
            <div class="meta-item">
              <div><strong>${escapeHtml(entity.canonical_name || entityId)}</strong></div>
              <div class="muted">type=${escapeHtml(entity.entity_type || "")} source=${escapeHtml(entity.source || "")}</div>
              <div class="mono">${escapeHtml(entity.entity_id || entityId)}</div>
              <div class="actions" style="margin-top: 8px;">
                <button class="action-btn" data-editor-type="kg_entity" data-editor-id="${entity.entity_id || entityId}">Edit Entity</button>
              </div>
            </div>
            <div class="detail-section">
              <h4>Assertions</h4>
              <div class="section-body"><table><thead><tr><th>Assertion</th><th>Predicate</th><th>Subject</th><th>Object</th><th>Confidence</th><th>Edit</th></tr></thead><tbody>${assertions}</tbody></table></div>
            </div>
            <div class="detail-section">
              <h4>Evidence</h4>
              <div class="section-body"><table><thead><tr><th>Evidence</th><th>Assertion</th><th>Excerpt</th></tr></thead><tbody>${evidence}</tbody></table></div>
            </div>
            <div class="detail-section">
              <h4>Source Chunks</h4>
              <div class="section-body"><table><thead><tr><th>Chunk</th><th>Status</th><th>Pages</th><th>Text</th></tr></thead><tbody>${chunks}</tbody></table></div>
            </div>
            <div class="detail-section">
              <h4>Documents</h4>
              <div class="section-body"><table><thead><tr><th>Filename</th><th>Class</th><th>Status</th><th>Document ID</th></tr></thead><tbody>${documents}</tbody></table></div>
            </div>
          </div>
        `;
        root.querySelectorAll("[data-linked-document-id]").forEach(row => {
          row.addEventListener("click", async () => {
            switchView("documents");
            await loadDocumentDetail(row.dataset.linkedDocumentId);
          });
        });
        root.querySelectorAll("[data-linked-chunk-id]").forEach(row => {
          row.addEventListener("click", async () => {
            switchView("chunks");
            document.getElementById("chunk-document-id").value = state.activeDocumentId || "";
            await loadChunks();
            await loadChunkDetail(row.dataset.linkedChunkId);
          });
        });
        root.querySelectorAll("[data-editor-type][data-editor-id]").forEach(button => {
          button.addEventListener("click", async (event) => {
            event.preventDefault();
            event.stopPropagation();
            setEditorSelection(
              button.dataset.editorType,
              button.dataset.editorId,
              button.dataset.editorSecondaryId || "",
              { autoLoad: true }
            );
            switchView("operations");
          });
        });
      } catch (error) {
        root.innerHTML = `<div class="meta-item"><strong>KG entity detail failed to load</strong><div class="muted" style="margin-top:8px;">${escapeHtml(error.message)}</div></div>`;
      }
    }

    async function loadKg() {
      const entityPage = state.paging.kgEntities;
      const assertionPage = state.paging.kgAssertions;
      const rawPage = state.paging.kgRaw;
      const documentId = document.getElementById("kg-document-id").value.trim();
      const status = document.getElementById("kg-status").value;
      const entityFilter = document.getElementById("kg-entity-filter").value.trim().toLowerCase();
      const entityType = document.getElementById("kg-entity-type").value.trim();
      const predicateFilter = document.getElementById("kg-predicate-filter").value.trim();
      const entityParams = new URLSearchParams();
      if (documentId) entityParams.set("document_id", documentId);
      if (entityFilter) entityParams.set("search", entityFilter);
      if (entityType) entityParams.set("entity_type", entityType);
      entityParams.set("limit", String(entityPage.limit));
      entityParams.set("offset", String(entityPage.offset));
      const assertionParams = new URLSearchParams();
      if (documentId) assertionParams.set("document_id", documentId);
      if (entityFilter && !predicateFilter) assertionParams.set("entity_id", entityFilter);
      if (predicateFilter) assertionParams.set("predicate", predicateFilter);
      assertionParams.set("limit", String(assertionPage.limit));
      assertionParams.set("offset", String(assertionPage.offset));
      const rawParams = new URLSearchParams();
      if (documentId) rawParams.set("document_id", documentId);
      if (status) rawParams.set("status", status);
      rawParams.set("limit", String(rawPage.limit));
      rawParams.set("offset", String(rawPage.offset));

      const [entities, assertions, rawExtractions] = await Promise.all([
        loadJson(`/admin/api/kg/entities?${entityParams.toString()}`),
        loadJson(`/admin/api/kg/assertions?${assertionParams.toString()}`),
        loadJson(`/admin/api/kg/raw?${rawParams.toString()}`)
      ]);
      const filteredAssertions = assertions.items.filter(assertion => {
        if (!entityFilter) return true;
        return [
          assertion.assertion_id || "",
          assertion.subject_entity_id || "",
          assertion.predicate || "",
          assertion.object_entity_id || "",
          assertion.object_literal || "",
        ].join(" ").toLowerCase().includes(entityFilter);
      });

      document.getElementById("kg-entities-table").innerHTML = `
        <thead>
          <tr><th>Entity ID</th><th>Name</th><th>Type</th><th>Assertions</th><th>Documents</th><th>Source</th></tr>
        </thead>
        <tbody>
          ${entities.items.map(entity => `
            <tr data-entity-id="${entity.entity_id}" class="${state.selectedKgEntityId === entity.entity_id ? "focused-row" : ""}">
              <td class="mono">${escapeHtml(entity.entity_id)}</td>
              <td>${escapeHtml(entity.canonical_name)}</td>
              <td>${escapeHtml(entity.entity_type)}</td>
              <td>${entity.assertion_count ?? 0}</td>
              <td>${entity.document_count ?? 0}</td>
              <td class="mono">${escapeHtml(entity.source || "")}</td>
            </tr>
          `).join("")}
        </tbody>
      `;
      document.getElementById("kg-entities-table").querySelectorAll("tbody tr[data-entity-id]").forEach(row => {
        row.addEventListener("click", async () => {
          await loadKgEntityDetail(row.dataset.entityId);
        });
      });

      document.getElementById("kg-assertions-table").innerHTML = `
        <thead>
          <tr><th>Assertion ID</th><th>Subject</th><th>Predicate</th><th>Object</th><th>Confidence</th><th>Edit</th></tr>
        </thead>
        <tbody>
          ${filteredAssertions.map(assertion => `
            <tr>
              <td class="mono">${escapeHtml(assertion.assertion_id)}</td>
              <td class="mono">${escapeHtml(assertion.subject_entity_id)}</td>
              <td>${escapeHtml(assertion.predicate)}</td>
              <td class="mono">${escapeHtml(assertion.object_entity_id || assertion.object_literal || "")}</td>
              <td>${assertion.confidence}</td>
              <td><button class="action-btn" data-editor-type="kg_assertion" data-editor-id="${assertion.assertion_id}">Edit</button></td>
            </tr>
          `).join("")}
        </tbody>
      `;

      document.getElementById("kg-raw-table").innerHTML = `
        <thead>
          <tr><th>Extraction</th><th>Status</th><th>Chunk</th><th>Errors</th><th>Payload</th><th>Edit</th></tr>
        </thead>
        <tbody>
          ${rawExtractions.items.map(item => `
            <tr>
              <td class="mono">${escapeHtml(item.extraction_id)}</td>
              <td>${statusBadge(item.status)}</td>
              <td class="mono">${escapeHtml(item.chunk_id)}</td>
              <td class="mono">${escapeHtml(JSON.stringify(item.payload?.errors || []))}</td>
              <td class="mono">${escapeHtml(JSON.stringify(item.payload || {}))}</td>
              <td><button class="action-btn" data-editor-type="kg_raw" data-editor-id="${item.extraction_id}">Edit</button></td>
            </tr>
          `).join("")}
        </tbody>
      `;
      document.querySelectorAll("#kg-assertions-table [data-editor-type][data-editor-id], #kg-raw-table [data-editor-type][data-editor-id]").forEach(button => {
        button.addEventListener("click", async event => {
          event.preventDefault();
          event.stopPropagation();
          setEditorSelection(
            button.dataset.editorType,
            button.dataset.editorId,
            button.dataset.editorSecondaryId || "",
            { autoLoad: true }
          );
          switchView("operations");
        });
      });

      renderPager("kg-entities-pager", entityPage, entities.total, loadKg);
      renderPager("kg-assertions-pager", assertionPage, assertions.total, loadKg);
      renderPager("kg-raw-pager", rawPage, rawExtractions.total, loadKg);

      if (state.selectedKgEntityId && entities.items.some(entity => entity.entity_id === state.selectedKgEntityId)) {
        await loadKgEntityDetail(state.selectedKgEntityId);
      } else if (!state.selectedKgEntityId && entities.items.length) {
        await loadKgEntityDetail(entities.items[0].entity_id);
      }
    }

    async function loadMetadata() {
      const page = state.paging.metadata;
      const documentId = document.getElementById("metadata-document-id").value.trim();
      const status = document.getElementById("metadata-status").value;
      const params = new URLSearchParams();
      if (documentId) params.set("document_id", documentId);
      if (status) params.set("status", status);
      params.set("limit", String(page.limit));
      params.set("offset", String(page.offset));
      const response = await loadJson(`/admin/api/metadata/chunks?${params.toString()}`);
      document.getElementById("metadata-table").innerHTML = `
        <thead>
          <tr>
            <th>Chunk</th>
            <th>Status</th>
            <th>Role</th>
            <th>Pages</th>
            <th>Section</th>
            <th>Ontology</th>
            <th>Metadata JSON</th>
          </tr>
        </thead>
        <tbody>
          ${response.items.map(row => `
            <tr>
              <td class="mono">${escapeHtml(row.chunk_id)}</td>
              <td>${statusBadge(row.validation_status)}</td>
              <td>${escapeHtml(row.chunk_role || "")}</td>
              <td>${row.page_start ?? ""}-${row.page_end ?? ""}</td>
              <td>${escapeHtml(row.section_title || "")}</td>
              <td class="mono">${escapeHtml(JSON.stringify(row.ontology_classes || []))}</td>
              <td class="mono">${escapeHtml(JSON.stringify(row.metadata_json || {}))}</td>
            </tr>
          `).join("")}
        </tbody>
      `;
      renderPager("metadata-pager", page, response.total, loadMetadata);
    }

    async function loadChroma() {
      const page = state.paging.chroma;
      const documentId = document.getElementById("chroma-document-id").value.trim();
      const select = document.getElementById("chroma-collection");
      const collections = await loadJson("/admin/api/chroma/collections");
      const chunkDefault = collections.find(item => item.is_default_chunk)?.name || collections[0]?.name || "";
      const assetDefault = collections.find(item => item.is_default_asset)?.name || "";
      state.chromaDefaults.chunk = chunkDefault;
      state.chromaDefaults.asset = assetDefault;
      const currentValue = select.value;
      select.innerHTML = collections.length
        ? collections.map(item => {
            const suffix = item.is_default_chunk ? " (chunk vectors)" : item.is_default_asset ? " (asset vectors)" : "";
            return `<option value="${escapeHtml(item.name)}">${escapeHtml(item.name + suffix)}</option>`;
          }).join("")
        : `<option value="">No collections</option>`;
      if (collections.some(item => item.name === currentValue)) {
        select.value = currentValue;
      } else if (chunkDefault) {
        select.value = chunkDefault;
      }
      const collectionName = select.value;
      const params = new URLSearchParams();
      if (documentId) params.set("document_id", documentId);
      if (collectionName) params.set("collection_name", collectionName);
      params.set("limit", String(page.limit));
      params.set("offset", String(page.offset));
      const [records, parity] = await Promise.all([
        loadJson(`/admin/api/chroma/records?${params.toString()}`),
        loadJson(`/admin/api/chroma/parity${documentId ? `?document_id=${encodeURIComponent(documentId)}` : ""}`)
      ]);

      document.getElementById("chroma-table").innerHTML = `
        <thead>
          <tr><th>Collection</th><th>Count</th><th>Collection metadata</th></tr>
        </thead>
        <tbody>
          ${collections.map(item => `
            <tr>
              <td>${escapeHtml(item.name)}</td>
              <td>${item.count}</td>
              <td class="mono">${escapeHtml(JSON.stringify(item.metadata || {}))}</td>
            </tr>
          `).join("")}
        </tbody>
      `;

      document.getElementById("chroma-parity").innerHTML = `
        ${parity.error ? `<div class="meta-item"><strong>Parity check error</strong><div class="muted" style="margin-top:8px;">${escapeHtml(parity.error)}</div></div>` : ""}
        ${collectionName !== state.chromaDefaults.chunk ? `<div class="meta-item" style="margin-bottom: 12px;"><strong>Parity note</strong><div class="muted" style="margin-top:8px;">Parity compares accepted chunk rows against the configured chunk-vector collection only.</div></div>` : ""}
        <div class="summary-grid">
          <div class="summary-chip"><span class="muted">Accepted Chunks</span><strong>${parity.accepted_chunks}</strong></div>
          <div class="summary-chip"><span class="muted">Vectors</span><strong>${parity.vectors}</strong></div>
          <div class="summary-chip"><span class="muted">Missing Vectors</span><strong>${parity.missing_vectors_total}</strong></div>
          <div class="summary-chip"><span class="muted">Extra Vectors</span><strong>${parity.extra_vectors_total}</strong></div>
        </div>
        <div class="meta-item" style="margin-top: 12px;">
          <div><strong>Missing vector chunk IDs</strong></div>
          <div class="mono" style="margin-top: 8px;">${escapeHtml(JSON.stringify(parity.missing_vectors || []))}</div>
        </div>
        <div class="meta-item" style="margin-top: 12px;">
          <div><strong>Extra vector IDs</strong></div>
          <div class="mono" style="margin-top: 8px;">${escapeHtml(JSON.stringify(parity.extra_vectors || []))}</div>
        </div>
      `;

      document.getElementById("chroma-records-table").innerHTML = `
        <thead>
          <tr><th>Record ID</th><th>Source</th><th>Pages</th><th>Type</th><th>Section</th><th>Summary</th><th>Text</th></tr>
        </thead>
        <tbody>
          ${records.error ? `<tr><td colspan="7" class="muted">Chroma error: ${escapeHtml(records.error)}</td></tr>` : ""}
          ${records.items.map(item => `
            <tr>
              <td class="mono">${escapeHtml(item.id)}</td>
              <td class="mono">${escapeHtml(item.metadata?.chunk_id || item.metadata?.asset_id || item.id || "")}</td>
              <td>${escapeHtml(item.metadata?.page_number ? String(item.metadata?.page_number) : `${item.metadata?.page_start || ""}-${item.metadata?.page_end || ""}`)}</td>
              <td>${escapeHtml(item.metadata?.asset_type || item.metadata?.chunk_role || "")}</td>
              <td>${escapeHtml(item.metadata?.section_title || item.metadata?.section || "")}</td>
              <td class="mono">${escapeHtml(structuredMetadataSummary(item.metadata || {}) || JSON.stringify(item.metadata || {}))}</td>
              <td class="chunk">${escapeHtml(shortText(item.document || "", 260))}</td>
            </tr>
          `).join("")}
          ${!records.error && !(records.items || []).length ? `<tr><td colspan="7" class="muted">No Chroma records found for the current filter.</td></tr>` : ""}
        </tbody>
      `;
      renderPager("chroma-records-pager", page, records.total, loadChroma);
    }

    async function loadDatabase() {
      const search = document.getElementById("db-relation-search")?.value.trim() || "";
      const params = new URLSearchParams();
      if (search) params.set("search", search);
      const relations = await loadJson(`/admin/api/db/relations?${params.toString()}`);
      const relationRows = relations.items || [];
      const relationsTable = document.getElementById("db-relations-table");
      relationsTable.innerHTML = `
        <thead>
          <tr><th>Name</th><th>Type</th><th>Rows</th><th>PK</th></tr>
        </thead>
        <tbody>
          ${relationRows.map(item => `
            <tr data-db-relation="${escapeHtml(item.relation_name || "")}" class="${state.selectedDatabaseRelation === item.relation_name ? "focused-row" : ""}">
              <td class="mono">${escapeHtml(item.relation_name || "")}</td>
              <td>${escapeHtml(item.relation_type || "")}</td>
              <td>${escapeHtml(item.estimated_rows ?? 0)}</td>
              <td>${item.has_primary_key ? "Yes" : "No"}</td>
            </tr>
          `).join("") || `<tr><td colspan="4" class="muted">No relations found.</td></tr>`}
        </tbody>
      `;
      relationsTable.querySelectorAll("tbody tr[data-db-relation]").forEach(row => {
        row.addEventListener("click", async () => {
          state.selectedDatabaseRelation = row.dataset.dbRelation;
          state.selectedDatabaseRowIndex = null;
          state.paging.databaseRows.offset = 0;
          await loadDatabase();
        });
      });

      if (!state.selectedDatabaseRelation && relationRows.length) {
        state.selectedDatabaseRelation = relationRows[0].relation_name;
      } else if (
        state.selectedDatabaseRelation &&
        !relationRows.some(item => item.relation_name === state.selectedDatabaseRelation)
      ) {
        state.selectedDatabaseRelation = relationRows[0]?.relation_name || null;
        state.selectedDatabaseRowIndex = null;
        state.paging.databaseRows.offset = 0;
      }

      const detailRoot = document.getElementById("db-relation-detail");
      const pagerRoot = document.getElementById("db-rows-pager");
      if (!state.selectedDatabaseRelation) {
        detailRoot.innerHTML = `
          <div class="meta-item">
            <div><strong>No relation selected</strong></div>
            <div class="muted" style="margin-top: 8px;">Select a table or view to load its schema and rows.</div>
          </div>
        `;
        pagerRoot.innerHTML = "";
        return;
      }

      const page = state.paging.databaseRows;
      const relation = await loadJson(
        `/admin/api/db/relations/${encodeURIComponent(state.selectedDatabaseRelation)}?limit=${page.limit}&offset=${page.offset}`
      );
      const columns = relation.columns || [];
      const rows = relation.rows || [];
      const columnNames = columns.map(item => item.column_name);
      const selectedIndex = rows.length ? Math.min(state.selectedDatabaseRowIndex ?? 0, rows.length - 1) : null;
      state.selectedDatabaseRowIndex = selectedIndex;
      const selectedRow = selectedIndex == null ? null : rows[selectedIndex];

      detailRoot.innerHTML = `
        <div class="meta-item">
          <div><strong>${escapeHtml(relation.relation_name || "")}</strong></div>
          <div class="mono" style="margin-top: 8px;">schema=${escapeHtml(relation.schema_name || "public")} | type=${escapeHtml(relation.relation_type || "")} | total_rows=${escapeHtml(relation.total ?? 0)}</div>
          <div class="mono" style="margin-top: 8px;">primary_key=${escapeHtml(JSON.stringify(relation.primary_key || []))} | order_by=${escapeHtml(JSON.stringify(relation.order_by || []))}</div>
        </div>
        <div class="detail-section">
          <h4>Columns</h4>
          <div class="section-body">
            <table>
              <thead><tr><th>Name</th><th>Type</th><th>Nullable</th><th>Default</th></tr></thead>
              <tbody>
                ${columns.map(item => `
                  <tr>
                    <td class="mono">${escapeHtml(item.column_name || "")}</td>
                    <td>${escapeHtml(item.data_type || item.udt_name || "")}</td>
                    <td>${escapeHtml(item.is_nullable || "")}</td>
                    <td class="mono">${escapeHtml(item.column_default || "")}</td>
                  </tr>
                `).join("") || `<tr><td colspan="4" class="muted">No columns found.</td></tr>`}
              </tbody>
            </table>
          </div>
        </div>
        <div class="detail-section">
          <h4>Rows</h4>
          <div class="section-body">
            <table id="db-rows-table">
              <thead>
                <tr>${columnNames.map(name => `<th>${escapeHtml(name)}</th>`).join("")}</tr>
              </thead>
              <tbody>
                ${rows.map((row, index) => `
                  <tr data-db-row-index="${index}" class="${selectedIndex === index ? "focused-row" : ""}">
                    ${columnNames.map(name => {
                      const value = row[name];
                      const text = typeof value === "string" ? value : JSON.stringify(value);
                      const cls = value && typeof value === "object" ? "mono" : "chunk";
                      return `<td class="${cls}">${escapeHtml(shortText(text, 180))}</td>`;
                    }).join("")}
                  </tr>
                `).join("") || `<tr><td colspan="${Math.max(1, columnNames.length)}" class="muted">No rows in this relation.</td></tr>`}
              </tbody>
            </table>
          </div>
        </div>
        <div class="detail-section">
          <h4>Selected Row JSON</h4>
          <div class="section-body">
            ${selectedRow ? `<pre class="mono" style="white-space: pre-wrap;">${escapeHtml(JSON.stringify(selectedRow, null, 2))}</pre>` : `<p class="muted">Select a row to inspect its full JSON.</p>`}
          </div>
        </div>
      `;
      detailRoot.querySelectorAll("#db-rows-table tbody tr[data-db-row-index]").forEach(row => {
        row.addEventListener("click", async () => {
          state.selectedDatabaseRowIndex = Number(row.dataset.dbRowIndex || "0");
          await loadDatabase();
        });
      });
      renderPager("db-rows-pager", page, relation.total || 0, loadDatabase);
    }

    async function loadAgent() {
      const sessionPage = state.paging.agentSessions;
      const runPage = state.paging.agentRuns;
      const reviewPage = state.paging.agentReviews;
      const profilePage = state.paging.agentProfiles;
      const sessionStatus = document.getElementById("agent-session-status")?.value || "";
      const runStatus = document.getElementById("agent-run-status")?.value || "";
      const abstainedValue = document.getElementById("agent-run-abstained")?.value || "";
      const runReviewStatus = document.getElementById("agent-run-review-status")?.value || "";
      const reviewDecision = document.getElementById("agent-review-decision")?.value || "";
      const patternSearch = document.getElementById("agent-pattern-search")?.value.trim() || "";
      const profileTenant = document.getElementById("agent-config-tenant")?.value.trim() || "shared";

      const sessionParams = new URLSearchParams();
      if (sessionStatus) sessionParams.set("status", sessionStatus);
      sessionParams.set("limit", String(sessionPage.limit));
      sessionParams.set("offset", String(sessionPage.offset));

      const runParams = new URLSearchParams();
      if (state.selectedAgentSessionId) runParams.set("session_id", state.selectedAgentSessionId);
      if (runStatus) runParams.set("status", runStatus);
      if (abstainedValue) runParams.set("abstained", abstainedValue);
      if (runReviewStatus) runParams.set("review_status", runReviewStatus);
      runParams.set("limit", String(runPage.limit));
      runParams.set("offset", String(runPage.offset));

      const reviewParams = new URLSearchParams();
      if (reviewDecision) reviewParams.set("decision", reviewDecision);
      reviewParams.set("limit", String(reviewPage.limit));
      reviewParams.set("offset", String(reviewPage.offset));

      const profileParams = new URLSearchParams();
      profileParams.set("tenant_id", profileTenant);
      profileParams.set("limit", String(profilePage.limit));
      profileParams.set("offset", String(profilePage.offset));

      const agentConfigTenant = document.getElementById("agent-config-tenant")?.value.trim() || "shared";
      const systemConfigGroup = document.getElementById("system-config-group")?.value || "platform";
      const patternPage = state.paging.agentPatterns;
      const patternParams = new URLSearchParams();
      patternParams.set("tenant_id", agentConfigTenant);
      if (patternSearch) patternParams.set("search", patternSearch);
      patternParams.set("limit", String(patternPage.limit));
      patternParams.set("offset", String(patternPage.offset));

      const [sessions, runs, reviews, profiles, patterns, agentMetrics, agentConfig, systemConfig, ontologyPayload] = await Promise.all([
        loadJson(`/admin/api/agent/sessions?${sessionParams.toString()}`),
        loadJson(`/admin/api/agent/runs?${runParams.toString()}`),
        loadJson(`/admin/api/agent/reviews?${reviewParams.toString()}`),
        loadJson(`/admin/api/agent/profiles?${profileParams.toString()}`),
        loadJson(`/admin/api/agent/patterns?${patternParams.toString()}`),
        loadJson("/admin/api/agent/metrics"),
        loadJson(`/admin/api/agent/config?tenant_id=${encodeURIComponent(agentConfigTenant)}`),
        loadJson(`/admin/api/system/config?group=${encodeURIComponent(systemConfigGroup)}`),
        loadJson("/admin/api/ontology"),
      ]);

      document.getElementById("agent-metrics-cards").innerHTML = `
        <div class="card"><h3>Total Runs</h3><strong>${escapeHtml(agentMetrics.total_runs ?? 0)}</strong></div>
        <div class="card"><h3>Needs Review</h3><strong>${escapeHtml(agentMetrics.needs_review ?? 0)}</strong></div>
        <div class="card"><h3>Abstentions</h3><strong>${escapeHtml(agentMetrics.abstentions ?? 0)}</strong></div>
        <div class="card"><h3>Rejected</h3><strong>${escapeHtml(agentMetrics.rejected ?? 0)}</strong></div>
        <div class="card"><h3>Avg Confidence</h3><strong>${escapeHtml(Number(agentMetrics.avg_confidence || 0).toFixed(3))}</strong></div>
        <div class="card"><h3>Avg Latency ms</h3><strong>${escapeHtml(Number(agentMetrics.avg_latency_ms || 0).toFixed(2))}</strong></div>
        <div class="card"><h3>Max Latency ms</h3><strong>${escapeHtml(Number(agentMetrics.max_latency_ms || 0).toFixed(2))}</strong></div>
      `;

      const sessionsTable = document.getElementById("agent-sessions-table");
      sessionsTable.innerHTML = `
        <thead>
          <tr><th>Session</th><th>Title</th><th>Status</th><th>Lease</th><th>Messages</th><th>Queries</th><th>Updated</th></tr>
        </thead>
        <tbody>
          ${sessions.items.map(item => `
            <tr data-agent-session-id="${item.session_id}">
              <td class="mono">${escapeHtml(item.session_id)}</td>
              <td>${escapeHtml(item.title || "")}</td>
              <td>${statusBadge(item.status)}</td>
              <td class="mono">${escapeHtml(item.claimed_by || "")}${item.lease_expires_at ? ` | until ${escapeHtml(item.lease_expires_at)}` : ""}</td>
              <td>${item.message_count}</td>
              <td>${item.query_count}</td>
              <td class="mono">${escapeHtml(item.updated_at || "")}</td>
            </tr>
          `).join("") || `<tr><td colspan="7" class="muted">No agent sessions found.</td></tr>`}
        </tbody>
      `;
      sessionsTable.querySelectorAll("tbody tr[data-agent-session-id]").forEach(row => {
        row.addEventListener("click", async () => {
          state.selectedAgentSessionId = row.dataset.agentSessionId;
          setEditorSelection("agent_session", row.dataset.agentSessionId);
          state.paging.agentRuns.offset = 0;
          await loadAgent();
        });
      });

      const runsTable = document.getElementById("agent-runs-table");
      runsTable.innerHTML = `
        <thead>
          <tr><th>Created</th><th>Session</th><th>Question</th><th>Status</th><th>Confidence</th><th>Abstained</th><th>Review</th></tr>
        </thead>
        <tbody>
          ${runs.items.map(item => `
            <tr data-agent-run-id="${item.query_run_id}">
              <td class="mono">${escapeHtml(item.created_at || "")}</td>
              <td class="mono">${escapeHtml(item.session_id || "")}</td>
              <td class="chunk">${escapeHtml(shortText(item.question || "", 180))}</td>
              <td>${statusBadge(item.status)}</td>
              <td>${escapeHtml(item.confidence ?? "")}</td>
              <td>${item.abstained ? "Yes" : "No"}</td>
              <td>${statusBadge(item.review_status || "")}</td>
            </tr>
          `).join("") || `<tr><td colspan="7" class="muted">No agent runs found.</td></tr>`}
        </tbody>
      `;
      runsTable.querySelectorAll("tbody tr[data-agent-run-id]").forEach(row => {
        row.addEventListener("click", async () => {
          state.selectedAgentRunId = row.dataset.agentRunId;
          await loadAgentRunDetail(state.selectedAgentRunId);
        });
      });

      renderPager("agent-sessions-pager", sessionPage, sessions.total, loadAgent);
      renderPager("agent-runs-pager", runPage, runs.total, loadAgent);

      document.getElementById("agent-reviews-table").innerHTML = `
        <thead>
          <tr><th>Created</th><th>Decision</th><th>Question</th><th>Confidence</th><th>Abstained</th><th>Reviewer</th></tr>
        </thead>
        <tbody>
          ${reviews.items.map(item => `
            <tr data-agent-review-run-id="${item.query_run_id}">
              <td class="mono">${escapeHtml(item.created_at || "")}</td>
              <td>${statusBadge(item.decision)}</td>
              <td class="chunk">${escapeHtml(shortText(item.question || "", 180))}</td>
              <td>${escapeHtml(item.confidence ?? "")}</td>
              <td>${item.abstained ? "Yes" : "No"}</td>
              <td>${escapeHtml(item.reviewer || "")}</td>
            </tr>
          `).join("") || `<tr><td colspan="6" class="muted">No agent reviews found.</td></tr>`}
        </tbody>
      `;
      document.getElementById("agent-reviews-table").querySelectorAll("tbody tr[data-agent-review-run-id]").forEach(row => {
        row.addEventListener("click", async () => {
          state.selectedAgentRunId = row.dataset.agentReviewRunId;
          await loadAgentRunDetail(state.selectedAgentRunId);
        });
      });
      renderPager("agent-reviews-pager", reviewPage, reviews.total, loadAgent);
      document.getElementById("agent-profiles-table").innerHTML = `
        <thead>
          <tr><th>Profile</th><th>Display Name</th><th>Experience</th><th>Topics</th><th>Sessions</th><th>Updated</th><th>Edit</th></tr>
        </thead>
        <tbody>
          ${profiles.items.map(item => `
            <tr data-agent-profile-id="${item.profile_id}">
              <td class="mono">${escapeHtml(item.profile_id || "")}</td>
              <td>${escapeHtml(item.display_name || "")}</td>
              <td>${escapeHtml(item.summary_json?.experience_level || "")}</td>
              <td class="chunk">${escapeHtml((item.summary_json?.recurring_topics || []).join(" | "))}</td>
              <td>${escapeHtml(item.session_count ?? 0)}</td>
              <td class="mono">${escapeHtml(item.updated_at || "")}</td>
              <td><button class="action-btn" data-editor-type="agent_profile" data-editor-id="${escapeHtml(item.profile_id || "")}">Edit</button></td>
            </tr>
          `).join("") || `<tr><td colspan="7" class="muted">No agent profiles found.</td></tr>`}
        </tbody>
      `;
      document.getElementById("agent-profiles-table").querySelectorAll("tbody tr[data-agent-profile-id]").forEach(row => {
        row.addEventListener("click", async event => {
          if (event.target.closest(".action-btn")) return;
          setEditorSelection("agent_profile", row.dataset.agentProfileId, "", { autoLoad: true });
          switchView("operations");
        });
      });
      document.getElementById("agent-profiles-table").querySelectorAll("[data-editor-type][data-editor-id]").forEach(button => {
        button.addEventListener("click", async event => {
          event.preventDefault();
          event.stopPropagation();
          setEditorSelection(
            button.dataset.editorType,
            button.dataset.editorId,
            button.dataset.editorSecondaryId || "",
            { autoLoad: true }
          );
          switchView("operations");
        });
      });
      renderPager("agent-profiles-pager", profilePage, profiles.total, loadAgent);
      document.getElementById("agent-patterns-table").innerHTML = `
        <thead>
          <tr><th>Signature</th><th>Keywords</th><th>Accepted</th><th>Rejected</th><th>Review</th><th>Feedback Events</th><th>Router Cache</th><th>Example Query</th><th>Edit</th></tr>
        </thead>
        <tbody>
          ${patterns.items.map(item => `
            <tr data-pattern-signature="${escapeHtml(item.pattern_signature || "")}">
              <td class="mono">${escapeHtml(item.pattern_signature || "")}</td>
              <td class="chunk">${escapeHtml(JSON.stringify(item.keywords_json || []))}</td>
              <td>${escapeHtml(item.approved_count ?? 0)}</td>
              <td>${escapeHtml(item.rejected_count ?? 0)}</td>
              <td>${escapeHtml(item.needs_review_count ?? 0)}</td>
              <td>${escapeHtml(item.total_feedback_count ?? 0)}</td>
              <td class="chunk">${escapeHtml(`hits=${item.router_cache_hits ?? 0} | model=${item.router_model || ""} | cached_at=${item.router_cached_at || ""}`)}</td>
              <td class="chunk">${escapeHtml(shortText(item.example_query || "", 140))}</td>
              <td><button class="action-btn" data-editor-type="agent_pattern" data-editor-id="${escapeHtml(item.pattern_signature || "")}" data-editor-secondary-id="${escapeHtml(agentConfigTenant)}">Edit</button></td>
            </tr>
          `).join("") || `<tr><td colspan="9" class="muted">No query-pattern feedback yet.</td></tr>`}
        </tbody>
      `;
      document.getElementById("agent-patterns-table").querySelectorAll("[data-editor-type][data-editor-id]").forEach(button => {
        button.addEventListener("click", async event => {
          event.preventDefault();
          event.stopPropagation();
          setEditorSelection(
            button.dataset.editorType,
            button.dataset.editorId,
            button.dataset.editorSecondaryId || "",
            { autoLoad: true }
          );
          switchView("operations");
        });
      });
      renderPager("agent-patterns-pager", patternPage, patterns.total, loadAgent);
      document.getElementById("agent-config-json").value = JSON.stringify(agentConfig.config || {}, null, 2);
      document.getElementById("agent-config-updated").value = agentConfig.updated_at
        ? `${agentConfig.updated_at} by ${agentConfig.updated_by || "unknown"}${agentConfig.effective_api_key_source ? ` | key source: ${agentConfig.effective_api_key_source}` : ""}${agentConfig.has_api_key_override ? " | api key override set" : ""}`
        : (agentConfig.has_api_key_override ? `defaults only | key source: ${agentConfig.effective_api_key_source || "none"} | api key override set` : `defaults only${agentConfig.effective_api_key_source ? ` | key source: ${agentConfig.effective_api_key_source}` : ""}`);
      document.getElementById("system-config-json").value = JSON.stringify(systemConfig.editable_config || {}, null, 2);
      document.getElementById("system-config-effective").value = JSON.stringify(systemConfig.effective_config || {}, null, 2);
      document.getElementById("system-config-key-sources").value = JSON.stringify({
        provider_key_sources: systemConfig.provider_key_sources || {},
        collection_defaults: systemConfig.collection_defaults || {}
      }, null, 2);
      const secretNote = (systemConfig.secret_keys || []).length
        ? ` | redacted secrets: ${(systemConfig.secret_keys || []).join(", ")}`
        : "";
      document.getElementById("system-config-meta").value = `${systemConfig.env_path || ""} | ${systemConfig.note || ""}${secretNote}`;
      document.getElementById("ontology-path").value = ontologyPayload.path || "";
      document.getElementById("ontology-content").value = ontologyPayload.content || "";
      const ontologyMeta = [];
      if (ontologyPayload.updated_at) ontologyMeta.push(`updated ${ontologyPayload.updated_at}`);
      if (ontologyPayload.stats) ontologyMeta.push(`classes=${ontologyPayload.stats.classes} predicates=${ontologyPayload.stats.predicates}`);
      if (ontologyPayload.parse_error) ontologyMeta.push(`parse error: ${ontologyPayload.parse_error}`);
      document.getElementById("ontology-meta").value = ontologyMeta.join(" | ");

      if (state.selectedAgentSessionId) {
        try {
          await loadAgentSessionDetail(state.selectedAgentSessionId);
        } catch (_) {}
      }

      if (state.selectedAgentRunId) {
        try {
          await loadAgentRunDetail(state.selectedAgentRunId);
        } catch (_) {}
      }
    }

    async function loadAgentRunDetail(queryRunId) {
      const detail = await loadJson(`/admin/api/agent/runs/${queryRunId}`);
      const run = detail.query_run || {};
      const pattern = detail.pattern || {};
      const sources = detail.sources || [];
      const reviews = detail.reviews || [];
      const selectedSources = sources.filter(item => item.selected);
      const responsePayload = run.final_response_payload || {};
      const routing = run.prompt_payload?.request_scope?.routing || responsePayload.routing || {};
      const citations = responsePayload.citations || [];
      const supportingEntities = responsePayload.supporting_entities || [];
      const supportingAssertions = responsePayload.supporting_assertions || [];
      const sourceRows = selectedSources.map(item => {
        const label = item.source_kind === "chunk"
          ? `Chunk ${item.chunk_id || item.source_id || ""}`
          : item.source_kind === "entity"
            ? `Entity ${item.entity_id || item.source_id || ""}`
            : `Assertion ${item.assertion_id || item.source_id || ""}`;
        return `
          <tr data-agent-source-kind="${escapeHtml(item.source_kind || "")}" data-agent-chunk-id="${escapeHtml(item.chunk_id || "")}" data-agent-entity-id="${escapeHtml(item.entity_id || "")}">
            <td>${escapeHtml(item.source_kind || "")}</td>
            <td class="mono">${escapeHtml(item.source_id || "")}</td>
            <td>${item.rank ?? ""}</td>
            <td>${escapeHtml(item.score ?? "")}</td>
            <td>${item.selected ? "Yes" : "No"}</td>
            <td class="chunk">${escapeHtml(label)}</td>
          </tr>
        `;
      }).join("") || `<tr><td colspan="6" class="muted">No selected sources.</td></tr>`;
      document.getElementById("agent-run-detail").innerHTML = `
        <div class="detail-stack">
          <div class="meta-item">
            <div><strong>Answer Trace</strong></div>
            <div class="mono" style="margin-top: 8px;">run=${escapeHtml(run.query_run_id || "")} | session=${escapeHtml(run.session_id || "")} | type=${escapeHtml(run.question_type || "")} | mode=${escapeHtml(run.retrieval_mode || "")}</div>
            <div style="margin-top: 8px;">${statusBadge(run.status)}</div>
            <div class="mono" style="margin-top: 8px;">provider=${escapeHtml(run.provider || "")} | model=${escapeHtml(run.model || "")} | prompt=${escapeHtml(run.prompt_version || "")}</div>
            <div class="mono" style="margin-top: 8px;">router=${escapeHtml(routing.source || "")} | route_type=${escapeHtml(routing.question_type || "")} | route_top_k=${escapeHtml(routing.top_k ?? "")} | route_conf=${escapeHtml(routing.confidence ?? "")}</div>
            <div class="mono" style="margin-top: 8px;">review=${escapeHtml(run.review_status || "")} | reviewed_by=${escapeHtml(run.reviewed_by || "")}</div>
            <div class="mono" style="margin-top: 8px;">pattern=${escapeHtml(run.query_signature || "")}</div>
            <div class="chunk" style="margin-top: 8px;"><strong>Question:</strong> ${escapeHtml(run.question || "")}</div>
            <div class="chunk" style="margin-top: 8px;"><strong>Answer:</strong> ${escapeHtml(run.answer || "")}</div>
            <div class="mono" style="margin-top: 8px;">confidence=${escapeHtml(run.confidence ?? "")} | abstained=${escapeHtml(run.abstained)} | latency_ms=${escapeHtml(run.metrics_json?.latency_ms ?? "")} | fallback=${escapeHtml(run.metrics_json?.fallback_used ?? false)}</div>
            <div class="mono" style="margin-top: 8px;">grounding=${escapeHtml(responsePayload?.grounding_check?.method || "")} | grounding_passed=${escapeHtml(responsePayload?.grounding_check?.passed ?? false)} | supported_ratio=${escapeHtml(responsePayload?.grounding_check?.supported_ratio ?? "")}</div>
            <div class="chunk" style="margin-top: 8px;"><strong>Abstain reason:</strong> ${escapeHtml(run.abstain_reason || "")}</div>
            <div class="chunk" style="margin-top: 8px;"><strong>Review reason:</strong> ${escapeHtml(run.review_reason || "")}</div>
            <div class="mono" style="margin-top: 8px;">${escapeHtml(JSON.stringify(run.metrics_json || {}, null, 2))}</div>
            <div class="filters" style="margin-top: 10px;">
              <input id="agent-run-review-notes" placeholder="Review notes" style="min-width: 280px;" />
              <button class="action-btn" id="agent-run-replay">Replay</button>
              <button class="action-btn accept" id="agent-run-accept">Accept</button>
              <button class="action-btn" id="agent-run-needs-review">Keep Review</button>
              <button class="action-btn reject" id="agent-run-reject">Reject</button>
            </div>
          </div>
          <div class="meta-item">
            <div><strong>Query Pattern History</strong></div>
            <div class="mono" style="margin-top: 8px;">signature=${escapeHtml(pattern.pattern_signature || run.query_signature || "")}</div>
            <div class="mono" style="margin-top: 8px;">keywords=${escapeHtml(JSON.stringify(pattern.keywords_json || run.query_keywords || []))}</div>
            <div class="mono" style="margin-top: 8px;">accepted=${escapeHtml(pattern.approved_count ?? 0)} | rejected=${escapeHtml(pattern.rejected_count ?? 0)} | review=${escapeHtml(pattern.needs_review_count ?? 0)} | feedback_events=${escapeHtml(pattern.total_feedback_count ?? 0)}</div>
            <div class="chunk" style="margin-top: 8px;"><strong>Example query:</strong> ${escapeHtml(pattern.example_query || run.question || "")}</div>
          </div>
          <div class="meta-item">
            <div><strong>Citations</strong></div>
            <table style="margin-top: 8px;">
              <thead><tr><th>Chunk</th><th>Pages</th><th>Section</th><th>Quote</th></tr></thead>
              <tbody>
                ${citations.map(item => `
                  <tr data-agent-citation-chunk-id="${escapeHtml(item.chunk_id || "")}">
                    <td class="mono">${escapeHtml(item.chunk_id || "")}</td>
                    <td>${item.page_start ?? ""}-${item.page_end ?? ""}</td>
                    <td>${escapeHtml(item.section_title || "")}</td>
                    <td class="chunk">${escapeHtml(shortText(item.quote || "", 260))}</td>
                  </tr>
                `).join("") || `<tr><td colspan="4" class="muted">No citations.</td></tr>`}
              </tbody>
            </table>
          </div>
          <div class="meta-item">
            <div><strong>Selected Sources</strong></div>
            <table style="margin-top: 8px;">
              <thead><tr><th>Kind</th><th>Source ID</th><th>Rank</th><th>Score</th><th>Selected</th><th>Target</th></tr></thead>
              <tbody>${sourceRows}</tbody>
            </table>
          </div>
          <div class="meta-item">
            <div><strong>Supporting Graph</strong></div>
            <div class="mono" style="margin-top: 8px;">entities=${escapeHtml(JSON.stringify(supportingEntities, null, 2))}</div>
            <div class="mono" style="margin-top: 8px;">assertions=${escapeHtml(JSON.stringify(supportingAssertions, null, 2))}</div>
          </div>
          <div class="meta-item">
            <details>
              <summary>Prompt Payload</summary>
              <div class="mono" style="margin-top: 8px;">${escapeHtml(JSON.stringify(run.prompt_payload || {}, null, 2))}</div>
            </details>
            <details style="margin-top: 10px;">
              <summary>Raw Model Response</summary>
              <div class="mono" style="margin-top: 8px;">${escapeHtml(JSON.stringify(run.raw_response_payload || {}, null, 2))}</div>
            </details>
            <details style="margin-top: 10px;">
              <summary>Final Response Payload</summary>
              <div class="mono" style="margin-top: 8px;">${escapeHtml(JSON.stringify(responsePayload || {}, null, 2))}</div>
            </details>
          </div>
          <div class="meta-item">
            <div><strong>Review History</strong></div>
            <div class="mono" style="margin-top: 8px;">${escapeHtml(JSON.stringify(reviews, null, 2))}</div>
          </div>
        </div>
      `;
      document.getElementById("agent-run-accept").addEventListener("click", async () => {
        await submitAgentRunReview(queryRunId, "approved");
      });
      document.getElementById("agent-run-needs-review").addEventListener("click", async () => {
        await submitAgentRunReview(queryRunId, "needs_review");
      });
      document.getElementById("agent-run-reject").addEventListener("click", async () => {
        await submitAgentRunReview(queryRunId, "rejected");
      });
      document.getElementById("agent-run-replay").addEventListener("click", async () => {
        await replayAgentRun(queryRunId);
      });
      document.getElementById("agent-run-detail").querySelectorAll("[data-agent-citation-chunk-id]").forEach(row => {
        row.addEventListener("click", async () => {
          switchView("chunks");
          await loadChunks();
          await loadChunkDetail(row.dataset.agentCitationChunkId);
        });
      });
      document.getElementById("agent-run-detail").querySelectorAll("[data-agent-source-kind]").forEach(row => {
        row.addEventListener("click", async () => {
          if (row.dataset.agentChunkId) {
            switchView("chunks");
            await loadChunks();
            await loadChunkDetail(row.dataset.agentChunkId);
            return;
          }
          if (row.dataset.agentEntityId) {
            switchView("kg");
            await loadKg();
            await loadKgEntityDetail(row.dataset.agentEntityId);
          }
        });
      });
    }

    async function loadAgentSessionDetail(sessionId) {
      const detail = await loadJson(`/admin/api/agent/sessions/${sessionId}`);
      const session = detail.session || {};
      const profile = detail.profile || null;
      const memory = detail.memory || null;
      const messages = detail.messages || [];
      const runs = detail.query_runs || [];
      document.getElementById("agent-session-detail").innerHTML = `
        <div class="detail-stack">
          <div class="meta-item">
            <div><strong>${escapeHtml(session.title || sessionId)}</strong></div>
            <div class="mono" style="margin-top: 8px;">session=${escapeHtml(session.session_id || "")} | tenant=${escapeHtml(session.tenant_id || "")} | status=${escapeHtml(session.status || "")}</div>
            <div class="mono" style="margin-top: 8px;">claimed_by=${escapeHtml(session.claimed_by || "")} | lease_expires_at=${escapeHtml(session.lease_expires_at || "")}</div>
          </div>
          <div class="detail-section">
            <h4>Linked Profile</h4>
            <div class="section-body">
              ${profile ? `
                <div class="meta-item">
                  <div><strong>${escapeHtml(profile.display_name || profile.profile_id || "")}</strong></div>
                  <div class="mono" style="margin-top: 8px;">profile=${escapeHtml(profile.profile_id || "")} | status=${escapeHtml(profile.status || "")}</div>
                  <div class="chunk" style="margin-top: 8px;">${escapeHtml(profile.summary_text || "")}</div>
                  <pre class="mono" style="margin-top: 8px; white-space: pre-wrap;">${escapeHtml(JSON.stringify(profile.summary_json || {}, null, 2))}</pre>
                  <div style="margin-top: 10px;"><button class="action-btn" data-editor-type="agent_profile" data-editor-id="${escapeHtml(profile.profile_id || "")}">Edit</button></div>
                </div>
              ` : `<p class="muted">No linked profile.</p>`}
            </div>
          </div>
          <div class="detail-section">
            <h4>Session Memory</h4>
            <div class="section-body">
              ${memory ? `
                <div class="meta-item">
                  <div class="mono">provider=${escapeHtml(memory.source_provider || "")} | model=${escapeHtml(memory.source_model || "")} | prompt_version=${escapeHtml(memory.prompt_version || "")}</div>
                  <div class="chunk" style="margin-top: 8px;">${escapeHtml(memory.summary_text || "")}</div>
                  <pre class="mono" style="margin-top: 8px; white-space: pre-wrap;">${escapeHtml(JSON.stringify(memory.summary_json || {}, null, 2))}</pre>
                  <div style="margin-top: 10px;"><button class="action-btn" data-editor-type="agent_session_memory" data-editor-id="${escapeHtml(sessionId)}">Edit</button></div>
                </div>
              ` : `<p class="muted">No compacted session memory has been written yet.</p>`}
            </div>
          </div>
          <div class="detail-section">
            <h4>Transcript</h4>
            <div class="section-body">
              ${messages.map(item => `
                <div class="meta-item" style="margin-bottom: 8px;">
                  <div><strong>${escapeHtml(item.role || "")}</strong></div>
                  <div class="chunk" style="margin-top: 8px;">${escapeHtml(item.content || "")}</div>
                </div>
              `).join("") || `<p class="muted">No session messages.</p>`}
            </div>
          </div>
          <div class="detail-section">
            <h4>Recent Runs</h4>
            <div class="section-body">
              <table>
                <thead><tr><th>Run</th><th>Question</th><th>Status</th><th>Review</th></tr></thead>
                <tbody>
                  ${runs.map(item => `
                    <tr data-session-run-id="${item.query_run_id}">
                      <td class="mono">${escapeHtml(item.query_run_id || "")}</td>
                      <td class="chunk">${escapeHtml(shortText(item.question || "", 140))}</td>
                      <td>${statusBadge(item.status)}</td>
                      <td>${statusBadge(item.review_status || "")}</td>
                    </tr>
                  `).join("") || `<tr><td colspan="4" class="muted">No runs for this session.</td></tr>`}
                </tbody>
              </table>
            </div>
          </div>
        </div>
      `;
      document.getElementById("agent-session-detail").querySelectorAll("[data-session-run-id]").forEach(row => {
        row.addEventListener("click", async () => {
          state.selectedAgentRunId = row.dataset.sessionRunId;
          await loadAgentRunDetail(state.selectedAgentRunId);
        });
      });
      document.getElementById("agent-session-detail").querySelectorAll("[data-editor-type][data-editor-id]").forEach(button => {
        button.addEventListener("click", async event => {
          event.preventDefault();
          event.stopPropagation();
          setEditorSelection(
            button.dataset.editorType,
            button.dataset.editorId,
            button.dataset.editorSecondaryId || "",
            { autoLoad: true }
          );
          switchView("operations");
        });
      });
    }

    async function submitAgentRunReview(queryRunId, decision) {
      const notes = document.getElementById("agent-run-review-notes")?.value.trim() || "";
      const result = await postJson(`/admin/api/agent/runs/${queryRunId}/review`, {
        decision,
        notes,
        reviewer: "admin-ui"
      });
      setOperationResult(result);
      await loadAgent();
      await loadAgentRunDetail(queryRunId);
    }

    async function replayAgentRun(queryRunId) {
      const result = await postJson(`/admin/api/agent/runs/${queryRunId}/replay`, {
        reuse_session: false,
      });
      setOperationResult(result);
      state.selectedAgentSessionId = result.session_id || null;
      state.selectedAgentRunId = result.query_run_id || null;
      await loadAgent();
      if (result.query_run_id) {
        await loadAgentRunDetail(result.query_run_id);
      }
    }

    async function runAgentQuery() {
      const question = document.getElementById("agent-question").value.trim();
      if (!question) {
        throw new Error("Enter a question first.");
      }
      const sessionId = document.getElementById("agent-session-id").value.trim() || null;
      const sessionToken = document.getElementById("agent-session-token").value.trim() || null;
      const tenantId = document.getElementById("agent-tenant-id").value.trim() || "shared";
      const documentIds = document.getElementById("agent-document-ids").value
        .split(",")
        .map(item => item.trim())
        .filter(Boolean);
      const topK = Number(document.getElementById("agent-top-k").value || "8");

      const result = await postJson("/agent/chat", {
        question,
        session_id: sessionId,
        session_token: sessionToken,
        tenant_id: tenantId,
        document_ids: documentIds.length ? documentIds : null,
        top_k: topK,
      });

      document.getElementById("agent-session-id").value = result.session_id || "";
      document.getElementById("agent-session-token").value = result.session_token || "";
      state.selectedAgentSessionId = result.session_id || null;
      state.selectedAgentRunId = result.query_run_id || null;
      document.getElementById("agent-result").innerHTML = `
        <div class="detail-stack">
          <div class="meta-item">
            <div><strong>Grounded Answer</strong></div>
            <div class="chunk" style="margin-top: 8px;">${escapeHtml(result.answer || "")}</div>
            <div class="mono" style="margin-top: 8px;">confidence=${escapeHtml(result.confidence ?? "")} | abstained=${escapeHtml(result.abstained)} | review=${escapeHtml(result.review_status || "")} | latency_ms=${escapeHtml(result.latency_ms ?? "")} | fallback=${escapeHtml(result.fallback_used ?? false)} | run=${escapeHtml(result.query_run_id || "")}</div>
            <div class="chunk" style="margin-top: 8px;"><strong>Abstain reason:</strong> ${escapeHtml(result.abstain_reason || "")}</div>
            <div class="chunk" style="margin-top: 8px;"><strong>Review reason:</strong> ${escapeHtml(result.review_reason || "")}</div>
            <div class="filters" style="margin-top: 10px;">
              <input id="agent-result-review-notes" placeholder="Review notes" style="min-width: 240px;" />
              <button class="action-btn accept" id="agent-result-accept">Accept</button>
              <button class="action-btn reject" id="agent-result-reject">Reject</button>
            </div>
          </div>
          <div class="meta-item">
            <div><strong>Citations</strong></div>
            <table style="margin-top: 8px;">
              <thead><tr><th>Chunk</th><th>Pages</th><th>Section</th><th>Excerpt</th></tr></thead>
              <tbody>
                ${(result.citations || []).map(item => `
                  <tr>
                    <td class="mono">${escapeHtml(item.chunk_id || "")}</td>
                    <td>${item.page_start ?? ""}-${item.page_end ?? ""}</td>
                    <td>${escapeHtml(item.section_title || "")}</td>
                    <td class="chunk">${escapeHtml(shortText(item.quote || "", 260))}</td>
                  </tr>
                `).join("") || `<tr><td colspan="4" class="muted">No citations.</td></tr>`}
              </tbody>
            </table>
          </div>
          <div class="meta-item">
            <div><strong>Supporting graph ids</strong></div>
            <div class="mono" style="margin-top: 8px;">entities=${escapeHtml(JSON.stringify(result.supporting_entities || []))} | assertions=${escapeHtml(JSON.stringify(result.supporting_assertions || []))}</div>
          </div>
        </div>
      `;
      document.getElementById("agent-result-accept").addEventListener("click", async () => {
        const notes = document.getElementById("agent-result-review-notes").value.trim() || "";
        const reviewResult = await postJson(`/admin/api/agent/runs/${result.query_run_id}/review`, {
          decision: "approved",
          notes,
          reviewer: "admin-ui"
        });
        setOperationResult(reviewResult);
        await loadAgent();
        await loadAgentRunDetail(result.query_run_id);
      });
      document.getElementById("agent-result-reject").addEventListener("click", async () => {
        const notes = document.getElementById("agent-result-review-notes").value.trim() || "";
        const reviewResult = await postJson(`/admin/api/agent/runs/${result.query_run_id}/review`, {
          decision: "rejected",
          notes,
          reviewer: "admin-ui"
        });
        setOperationResult(reviewResult);
        await loadAgent();
        await loadAgentRunDetail(result.query_run_id);
      });
      setOperationResult(result);
      await loadAgent();
    }

    async function loadOperations() {
      const stagePage = state.paging.stageRuns;
      const reviewPage = state.paging.reviewRuns;
      const stageDocumentId = document.getElementById("ops-stage-document-id")?.value.trim() || "";
      const stageStatus = document.getElementById("ops-stage-status")?.value || "";
      const reviewDocumentId = document.getElementById("ops-review-document-id")?.value.trim() || "";
      const reviewDecision = document.getElementById("ops-review-decision")?.value || "";
      const evalOutputPath = document.getElementById("ops-eval-output")?.value.trim() || "data/evaluation/latest-admin-eval.json";
      const agentEvalOutputPath = document.getElementById("ops-agent-eval-output")?.value.trim() || "data/evaluation/latest-agent-eval.json";

      const stageParams = new URLSearchParams();
      if (stageDocumentId) stageParams.set("document_id", stageDocumentId);
      if (stageStatus) stageParams.set("status", stageStatus);
      stageParams.set("limit", String(stagePage.limit));
      stageParams.set("offset", String(stagePage.offset));

      const reviewParams = new URLSearchParams();
      if (reviewDocumentId) reviewParams.set("document_id", reviewDocumentId);
      if (reviewDecision) reviewParams.set("decision", reviewDecision);
      reviewParams.set("limit", String(reviewPage.limit));
      reviewParams.set("offset", String(reviewPage.offset));

      const evalParams = new URLSearchParams();
      evalParams.set("path", evalOutputPath);
      const agentEvalParams = new URLSearchParams();
      agentEvalParams.set("path", agentEvalOutputPath);

      const [stages, reviews, evalPayload, agentEvalPayload] = await Promise.all([
        loadJson(`/admin/api/activity/stages?${stageParams.toString()}`),
        loadJson(`/admin/api/activity/reviews?${reviewParams.toString()}`),
        loadJson(`/admin/api/retrieval/evaluation?${evalParams.toString()}`).catch(() => null),
        loadJson(`/admin/api/agent/evaluation?${agentEvalParams.toString()}`).catch(() => null)
      ]);

      document.getElementById("ops-stage-table").innerHTML = `
        <thead>
          <tr><th>Finished</th><th>Stage</th><th>Status</th><th>Document</th><th>Job</th><th>Metrics</th><th>Error</th></tr>
        </thead>
        <tbody>
          ${stages.items.map(item => `
            <tr>
              <td class="mono">${escapeHtml(item.finished_at || item.started_at || "")}</td>
              <td>${escapeHtml(item.stage_name)}</td>
              <td>${statusBadge(item.status)}</td>
              <td class="mono">${escapeHtml(item.document_id)}</td>
              <td class="mono">${escapeHtml(item.job_id)}</td>
              <td class="mono">${escapeHtml(JSON.stringify(item.metrics_json || {}))}</td>
              <td class="mono">${escapeHtml(item.error_message || "")}</td>
            </tr>
          `).join("") || `<tr><td colspan="7" class="muted">No stage runs found.</td></tr>`}
        </tbody>
      `;

      document.getElementById("ops-review-table").innerHTML = `
        <thead>
          <tr><th>Created</th><th>Decision</th><th>Chunk</th><th>Document</th><th>Model</th><th>Role</th><th>Reason</th></tr>
        </thead>
        <tbody>
          ${reviews.items.map(item => `
            <tr>
              <td class="mono">${escapeHtml(item.created_at || "")}</td>
              <td>${statusBadge(item.decision)}</td>
              <td class="mono">${escapeHtml(item.chunk_id)}</td>
              <td class="mono">${escapeHtml(item.document_id)}</td>
              <td class="mono">${escapeHtml(item.model || "")}</td>
              <td>${escapeHtml(item.detected_role || "")}</td>
              <td class="chunk">${escapeHtml(shortText(item.reason || "", 220))}</td>
            </tr>
          `).join("") || `<tr><td colspan="7" class="muted">No review runs found.</td></tr>`}
        </tbody>
      `;

      document.getElementById("ops-eval-summary").innerHTML = evalPayload ? `
        <div class="detail-stack">
          <div class="meta-item">
            <div><strong>Summary</strong></div>
            <div class="mono" style="margin-top: 8px;">queries=${escapeHtml(evalPayload.summary?.queries ?? 0)} | passed=${escapeHtml(evalPayload.summary?.passed ?? 0)} | failed=${escapeHtml(evalPayload.summary?.failed ?? 0)} | pass_rate=${escapeHtml(evalPayload.summary?.pass_rate ?? 0)} | asset_queries=${escapeHtml(evalPayload.summary?.asset_queries ?? 0)} | asset_queries_passed=${escapeHtml(evalPayload.summary?.asset_queries_passed ?? 0)}</div>
            <div class="mono" style="margin-top: 8px;">queries_file=${escapeHtml(evalPayload.queries_file || "")}</div>
          </div>
          <table>
            <thead>
              <tr><th>Query</th><th>Status</th><th>Mode</th><th>Chunks</th><th>Assets</th><th>Docs</th><th>Terms</th></tr>
            </thead>
            <tbody>
              ${(evalPayload.results || []).map(item => `
                <tr>
                  <td class="chunk">${escapeHtml(shortText(item.query || "", 160))}</td>
                  <td>${statusBadge(item.passed ? "passed" : "failed")}</td>
                  <td>${escapeHtml(item.retrieval_mode || "")}</td>
                  <td>${escapeHtml(item.selected_chunk_count ?? 0)}</td>
                  <td>${escapeHtml(item.selected_asset_count ?? 0)}</td>
                  <td class="chunk">${escapeHtml((item.selected_document_filenames || []).join(", "))}</td>
                  <td class="chunk">${escapeHtml(`hits=${(item.term_hits || []).join(", ")} | coverage=${item.term_coverage ?? 0}`)}</td>
                </tr>
              `).join("") || `<tr><td colspan="7" class="muted">No retrieval evaluation results found.</td></tr>`}
            </tbody>
          </table>
        </div>
      ` : `<p class="muted">No retrieval evaluation result file found for ${escapeHtml(evalOutputPath)}.</p>`;

      document.getElementById("ops-agent-eval-summary").innerHTML = agentEvalPayload ? `
        <div class="detail-stack">
          <div class="meta-item">
            <div><strong>Summary</strong></div>
            <div class="mono" style="margin-top: 8px;">total=${escapeHtml(agentEvalPayload.total ?? 0)} | passed=${escapeHtml(agentEvalPayload.passed ?? 0)} | pass_rate=${escapeHtml(agentEvalPayload.pass_rate ?? 0)} | abstention_cases=${escapeHtml(agentEvalPayload.abstention_cases ?? 0)} | abstention_cases_passed=${escapeHtml(agentEvalPayload.abstention_cases_passed ?? 0)}</div>
            <div class="mono" style="margin-top: 8px;">queries_file=${escapeHtml(agentEvalPayload.queries_file || "")}</div>
          </div>
          <table>
            <thead>
              <tr><th>Query</th><th>Status</th><th>Type</th><th>Router</th><th>Grounding</th><th>Citations</th><th>Docs</th></tr>
            </thead>
            <tbody>
              ${(agentEvalPayload.rows || []).map(item => `
                <tr>
                  <td class="chunk">${escapeHtml(shortText(item.query || "", 160))}</td>
                  <td>${statusBadge(item.passed ? "passed" : "failed")}</td>
                  <td>${escapeHtml(item.question_type || "")}</td>
                  <td class="chunk">${escapeHtml(`ok=${item.router_ok} | source=${item.routing?.source || ""}`)}</td>
                  <td class="chunk">${escapeHtml(`ok=${item.grounding_check?.passed ?? false} | method=${item.grounding_check?.method || ""}`)}</td>
                  <td class="chunk">${escapeHtml((item.citation_kinds || []).join(", "))}</td>
                  <td class="chunk">${escapeHtml((item.citation_document_filenames || []).join(", "))}</td>
                </tr>
              `).join("") || `<tr><td colspan="7" class="muted">No agent evaluation results found.</td></tr>`}
            </tbody>
          </table>
        </div>
      ` : `<p class="muted">No agent evaluation result file found for ${escapeHtml(agentEvalOutputPath)}.</p>`;

      renderPager("ops-stage-pager", stagePage, stages.total, loadOperations);
      renderPager("ops-review-pager", reviewPage, reviews.total, loadOperations);
      await loadProcessMonitor();
    }

    async function switchView(view) {
      document.querySelectorAll(".view").forEach(node => node.classList.add("hidden"));
      document.getElementById(`view-${view}`).classList.remove("hidden");
      document.querySelectorAll(".nav button").forEach(btn => btn.classList.toggle("active", btn.dataset.view === view));
      if (view === "database") {
        await loadDatabase();
      }
    }

    async function boot() {
      const adminTokenInput = document.getElementById("admin-token");
      if (adminTokenInput) {
        adminTokenInput.value = getAdminToken();
        adminTokenInput.addEventListener("input", () => {
          const nextValue = adminTokenInput.value || "";
          sessionStorage.setItem("bee-admin-token", nextValue);
          localStorage.removeItem("bee-admin-token");
        });
      }
      setTimestamp();
      try {
        await loadOverview();
        await loadChunks();
        await loadMetadata();
        await loadKg();
        await loadChroma();
        await loadAgent();
        await loadOperations();
        await loadProcessMonitor();
        await loadRouteCatalog();
        const first = state.documents[0];
        if (first) {
          await loadDocumentDetail(first.document_id);
        }
      } catch (error) {
        if (error?.status === 401 || error?.status === 503) {
          showAdminAuthNotice(error.message);
        } else {
          throw error;
        }
      }

      document.querySelectorAll(".nav button").forEach(button => {
        button.addEventListener("click", async () => {
          await switchView(button.dataset.view);
        });
      });
      document.getElementById("chunk-auto-review").addEventListener("click", runAutoReview);
      document.getElementById("chunk-refresh").addEventListener("click", loadChunks);
      document.getElementById("metadata-refresh").addEventListener("click", loadMetadata);
      document.getElementById("kg-refresh").addEventListener("click", loadKg);
      document.getElementById("chroma-refresh").addEventListener("click", loadChroma);
      document.getElementById("db-relations-refresh").addEventListener("click", loadDatabase);
      document.getElementById("agent-run-query").addEventListener("click", async () => {
        try {
          await runAgentQuery();
        } catch (error) {
          setOperationResult(error.message, true);
          alert(error.message);
        }
      });
      document.getElementById("agent-config-load").addEventListener("click", async () => {
        try {
          await loadAgent();
        } catch (error) {
          setOperationResult(error.message, true);
          alert(error.message);
        }
      });
      document.getElementById("agent-config-save").addEventListener("click", async () => {
        try {
          const tenantId = document.getElementById("agent-config-tenant").value.trim() || "shared";
          const config = JSON.parse(document.getElementById("agent-config-json").value || "{}");
          const result = await putJson("/admin/api/agent/config", {
            tenant_id: tenantId,
            config,
            updated_by: "admin-ui",
          });
          setOperationResult(result);
          await loadAgent();
        } catch (error) {
          setOperationResult(error.message, true);
          alert(error.message);
        }
      });
      document.getElementById("agent-config-reset").addEventListener("click", async () => {
        try {
          const tenantId = document.getElementById("agent-config-tenant").value.trim() || "shared";
          const result = await deleteJson(`/admin/api/agent/config?tenant_id=${encodeURIComponent(tenantId)}`);
          setOperationResult(result);
          await loadAgent();
        } catch (error) {
          setOperationResult(error.message, true);
          alert(error.message);
        }
      });
      document.getElementById("system-config-load").addEventListener("click", async () => {
        try {
          await loadAgent();
        } catch (error) {
          setOperationResult(error.message, true);
          alert(error.message);
        }
      });
      document.getElementById("system-config-save").addEventListener("click", async () => {
        try {
          const group = document.getElementById("system-config-group").value || "platform";
          const config = JSON.parse(document.getElementById("system-config-json").value || "{}");
          const result = await putJson("/admin/api/system/config", {
            group,
            config,
            updated_by: "admin-ui",
          });
          setOperationResult(result);
          await loadAgent();
        } catch (error) {
          setOperationResult(error.message, true);
          alert(error.message);
        }
      });
      document.getElementById("system-config-reset").addEventListener("click", async () => {
        try {
          const group = document.getElementById("system-config-group").value || "platform";
          if (!window.confirm(`Reset the ${group} startup config group to defaults? This removes that group's keys from .env.`)) return;
          const result = await deleteJson(`/admin/api/system/config?group=${encodeURIComponent(group)}`);
          setOperationResult(result);
          await loadAgent();
        } catch (error) {
          setOperationResult(error.message, true);
          alert(error.message);
        }
      });
      document.getElementById("ontology-load").addEventListener("click", loadAgent);
      document.getElementById("ontology-save").addEventListener("click", async () => {
        try {
          const result = await putJson("/admin/api/ontology", {
            content: document.getElementById("ontology-content").value || "",
            updated_by: "admin-ui",
          });
          setOperationResult(result);
          await loadAgent();
        } catch (error) {
          setOperationResult(error.message, true);
          alert(error.message);
        }
      });
      document.getElementById("ops-stage-refresh").addEventListener("click", loadOperations);
      document.getElementById("ops-review-refresh").addEventListener("click", loadOperations);
      document.getElementById("ops-refresh-now").addEventListener("click", autoRefreshTick);
      document.getElementById("ops-auto-refresh-enabled").addEventListener("change", event => {
        state.autoRefresh.enabled = event.target.checked;
        restartAutoRefresh();
      });
      document.getElementById("ops-auto-refresh-seconds").addEventListener("change", event => {
        const nextSeconds = Math.max(3, Math.min(Number(event.target.value || "10"), 120));
        event.target.value = String(nextSeconds);
        state.autoRefresh.intervalMs = nextSeconds * 1000;
        restartAutoRefresh();
      });
      document.getElementById("ops-api-route").addEventListener("change", event => {
        const selected = state.routes.find(route => route.path === event.target.value);
        if (!selected) return;
        document.getElementById("ops-api-path").value = selected.path;
        document.getElementById("ops-api-method").value = selected.methods.includes("GET") ? "GET" : selected.methods[0];
      });
      document.getElementById("ops-api-run").addEventListener("click", async () => {
        try {
          await runApiConsoleRequest();
        } catch (error) {
          setOperationResult(error.message, true);
          document.getElementById("ops-api-response").innerHTML = `
            <div class="meta-item">
              <div><strong>API request failed</strong></div>
              <div class="mono" style="margin-top: 8px;">${escapeHtml(error.message)}</div>
            </div>
          `;
        }
      });
      document.getElementById("ops-editor-type").addEventListener("change", () => {
        updateEditorUiState();
      });
      document.getElementById("ops-editor-load").addEventListener("click", async () => {
        try {
          await loadEditorRecord();
        } catch (error) {
          renderEditorResult(error.message, true);
          setOperationResult(error.message, true);
        }
      });
      document.getElementById("ops-editor-save").addEventListener("click", async () => {
        try {
          await saveEditorRecord();
        } catch (error) {
          renderEditorResult(error.message, true);
          setOperationResult(error.message, true);
        }
      });
      document.getElementById("ops-editor-delete").addEventListener("click", async () => {
        try {
          await deleteEditorRecord();
        } catch (error) {
          renderEditorResult(error.message, true);
          setOperationResult(error.message, true);
        }
      });
      document.getElementById("ops-editor-resync").addEventListener("click", async () => {
        try {
          await resyncEditorRecord();
        } catch (error) {
          renderEditorResult(error.message, true);
          setOperationResult(error.message, true);
        }
      });

      bindReactiveFilter("chunk-document-id", loadChunks, state.paging.chunks);
      bindReactiveFilter("chunk-text-filter", loadChunks, state.paging.chunks);
      bindReactiveFilter("chunk-status", loadChunks, state.paging.chunks, "change");
      bindReactiveFilter("metadata-document-id", loadMetadata, state.paging.metadata);
      bindReactiveFilter("metadata-status", loadMetadata, state.paging.metadata, "change");
      bindReactiveFilter("kg-document-id", async () => { resetKgPaging(); await loadKg(); });
      bindReactiveFilter("kg-entity-filter", async () => { resetKgPaging(); await loadKg(); });
      bindReactiveFilter("kg-entity-type", async () => { resetKgPaging(); await loadKg(); });
      bindReactiveFilter("kg-predicate-filter", async () => { resetKgPaging(); await loadKg(); });
      bindReactiveFilter("kg-status", async () => { resetKgPaging(); await loadKg(); }, null, "change");
      bindReactiveFilter("chroma-document-id", loadChroma, state.paging.chroma);
      bindReactiveFilter("chroma-collection", loadChroma, state.paging.chroma, "change");
      bindReactiveFilter("db-relation-search", async () => {
        state.selectedDatabaseRelation = null;
        state.selectedDatabaseRowIndex = null;
        state.paging.databaseRows.offset = 0;
        await loadDatabase();
      });
      bindReactiveFilter("agent-session-status", loadAgent, state.paging.agentSessions, "change");
      bindReactiveFilter("agent-run-status", loadAgent, state.paging.agentRuns, "change");
      bindReactiveFilter("agent-run-abstained", loadAgent, state.paging.agentRuns, "change");
      bindReactiveFilter("agent-run-review-status", loadAgent, state.paging.agentRuns, "change");
      bindReactiveFilter("agent-review-decision", loadAgent, state.paging.agentReviews, "change");
      bindReactiveFilter("agent-pattern-search", loadAgent, state.paging.agentPatterns);
      bindReactiveFilter("system-config-group", loadAgent, null, "change");
      bindReactiveFilter("ops-stage-document-id", loadOperations, state.paging.stageRuns);
      bindReactiveFilter("ops-stage-status", loadOperations, state.paging.stageRuns, "change");
      bindReactiveFilter("ops-review-document-id", loadOperations, state.paging.reviewRuns);
      bindReactiveFilter("ops-review-decision", loadOperations, state.paging.reviewRuns, "change");
      document.getElementById("ops-reset").addEventListener("click", async () => {
        if (!window.confirm("Reset the entire pipeline data set? This deletes documents, chunks, vectors, and KG rows.")) return;
        try {
          const result = await postJson("/admin/api/reset", {});
          setOperationResult(result);
          state.activeDocumentId = null;
          state.selectedChunkId = null;
          state.selectedKgEntityId = null;
          await reloadAllViews();
          document.getElementById("document-detail").innerHTML = `<p class="muted">Select a document to inspect jobs and stages.</p>`;
          document.getElementById("chunk-detail").innerHTML = `<p class="muted">Select a chunk to inspect metadata, KG, neighbors, and Chroma.</p>`;
          document.getElementById("kg-entity-detail").innerHTML = `<p class="muted">Select a KG entity to inspect its assertions, evidence, chunks, and source documents.</p>`;
        } catch (error) {
          setOperationResult(error.message, true);
          alert(error.message);
        }
      });
      document.getElementById("ops-run-eval").addEventListener("click", async () => {
        try {
          const result = await postJson("/admin/api/retrieval/evaluate", {
            tenant_id: document.getElementById("ops-eval-tenant").value.trim() || null,
            top_k: Number(document.getElementById("ops-eval-top-k").value || "5"),
            queries_file: document.getElementById("ops-eval-queries").value.trim() || "data/evaluation/retrieval_small_queries.json",
            output: document.getElementById("ops-eval-output").value.trim() || "data/evaluation/latest-admin-eval.json"
          });
          setOperationResult(result);
          await loadOperations();
        } catch (error) {
          setOperationResult(error.message, true);
          alert(error.message);
        }
      });
      document.getElementById("ops-run-agent-eval").addEventListener("click", async () => {
        try {
          const result = await postJson("/admin/api/agent/evaluate", {
            tenant_id: document.getElementById("ops-agent-eval-tenant").value.trim() || null,
            top_k: Number(document.getElementById("ops-agent-eval-top-k").value || "5"),
            queries_file: document.getElementById("ops-agent-eval-queries").value.trim() || "data/evaluation/agent_small_queries.json",
            output: document.getElementById("ops-agent-eval-output").value.trim() || "data/evaluation/latest-agent-eval.json"
          });
          setOperationResult(result);
          await loadOperations();
        } catch (error) {
          setOperationResult(error.message, true);
          alert(error.message);
        }
      });
      document.getElementById("ops-start-reingest").addEventListener("click", async () => {
        try {
          const result = await postJson("/admin/api/system/reingest/start", {});
          setOperationResult(result);
          await loadOperations();
        } catch (error) {
          setOperationResult(error.message, true);
          alert(error.message);
        }
      });
      document.getElementById("ops-resume-reingest").addEventListener("click", async () => {
        try {
          const result = await postJson("/admin/api/system/reingest/resume", {});
          setOperationResult(result);
          await loadOperations();
        } catch (error) {
          setOperationResult(error.message, true);
          alert(error.message);
        }
      });
      document.getElementById("ops-stop-reingest").addEventListener("click", async () => {
        try {
          const result = await postJson("/admin/api/system/reingest/stop", {});
          setOperationResult(result);
          await loadOperations();
        } catch (error) {
          setOperationResult(error.message, true);
          alert(error.message);
        }
      });
      document.getElementById("ops-ingest-pdf").addEventListener("click", async () => {
        try {
          const result = await postJson("/ingest/pdf", {
            tenant_id: document.getElementById("ops-pdf-tenant").value.trim() || "shared",
            path: document.getElementById("ops-pdf-path").value.trim(),
            filename: document.getElementById("ops-pdf-filename").value.trim() || null,
            document_class: document.getElementById("ops-pdf-class").value.trim() || "book",
            page_start: document.getElementById("ops-pdf-start").value ? Number(document.getElementById("ops-pdf-start").value) : null,
            page_end: document.getElementById("ops-pdf-end").value ? Number(document.getElementById("ops-pdf-end").value) : null
          });
          setOperationResult(result);
          await reloadAllViews();
        } catch (error) {
          setOperationResult(error.message, true);
          alert(error.message);
        }
      });
      document.getElementById("ops-upload-ingest").addEventListener("click", async () => {
        try {
          const input = document.getElementById("ops-upload-file");
          const file = input.files && input.files[0];
          if (!file) {
            throw new Error("Choose a file to upload first.");
          }
          const form = new FormData();
          form.append("file", file);
          const sourceType = document.getElementById("ops-upload-type").value;
          const filename = document.getElementById("ops-upload-filename").value.trim();
          const tenantId = document.getElementById("ops-upload-tenant").value.trim();
          const documentClass = document.getElementById("ops-upload-class").value.trim();
          const pageStart = document.getElementById("ops-upload-start").value;
          const pageEnd = document.getElementById("ops-upload-end").value;
          if (sourceType) form.append("source_type", sourceType);
          if (filename) form.append("filename", filename);
          form.append("tenant_id", tenantId || "shared");
          form.append("document_class", documentClass || "book");
          if (pageStart) form.append("page_start", String(Number(pageStart)));
          if (pageEnd) form.append("page_end", String(Number(pageEnd)));
          const result = await postForm("/admin/api/uploads/ingest", form);
          setOperationResult(result);
          input.value = "";
          await reloadAllViews();
        } catch (error) {
          setOperationResult(error.message, true);
          alert(error.message);
        }
      });
      document.getElementById("ops-ingest-text").addEventListener("click", async () => {
        try {
          const result = await postJson("/ingest/text", {
            tenant_id: document.getElementById("ops-text-tenant").value.trim() || "shared",
            source_type: "text",
            filename: document.getElementById("ops-text-filename").value.trim() || `manual-${Date.now()}.txt`,
            raw_text: document.getElementById("ops-text-body").value,
            document_class: document.getElementById("ops-text-class").value.trim() || "note"
          });
          setOperationResult(result);
          await reloadAllViews();
        } catch (error) {
          setOperationResult(error.message, true);
          alert(error.message);
        }
      });
      updateEditorUiState();
      restartAutoRefresh();
    }

    boot().catch(error => {
      console.error(error);
      const banner = document.createElement("div");
      banner.style.padding = "16px";
      banner.style.color = "#8d3c2f";
      banner.textContent = String(error.message || "Admin UI failed to boot");
      document.body.prepend(banner);
    });
  </script>
</body>
</html>
"""
