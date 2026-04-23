from __future__ import annotations


AGENT_UI_HTML = """
<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8" />
  <meta name="viewport" content="width=device-width, initial-scale=1" />
  <title>Bee Agent</title>
  <style>
    :root {
      --bg: #0b1015;
      --panel: #121922;
      --panel-2: #17212c;
      --line: #293545;
      --ink: #edf2f7;
      --muted: #8da0b5;
      --accent: #72d0a3;
      --accent-2: #e49a59;
      --danger: #e26d6d;
      --shadow: 0 16px 36px rgba(0, 0, 0, 0.34);
    }

    * { box-sizing: border-box; }
    body {
      margin: 0;
      color: var(--ink);
      font-family: Georgia, "Times New Roman", serif;
      background:
        radial-gradient(circle at top left, rgba(114, 208, 163, 0.08), transparent 24%),
        radial-gradient(circle at bottom right, rgba(228, 154, 89, 0.08), transparent 22%),
        var(--bg);
    }

    .app {
      max-width: 1180px;
      margin: 0 auto;
      min-height: 100vh;
      display: grid;
      grid-template-columns: 320px 1fr;
      gap: 22px;
      padding: 24px;
    }

    .panel {
      background: linear-gradient(180deg, rgba(23, 33, 44, 0.96), rgba(17, 24, 32, 0.98));
      border: 1px solid var(--line);
      border-radius: 20px;
      box-shadow: var(--shadow);
    }

    .sidebar {
      padding: 20px;
      display: flex;
      flex-direction: column;
      gap: 18px;
    }

    .brand h1 {
      margin: 0;
      font-size: 34px;
      line-height: 1;
    }

    .brand p {
      margin: 10px 0 0;
      color: var(--muted);
      font-size: 15px;
      line-height: 1.45;
    }

    .section {
      display: flex;
      flex-direction: column;
      gap: 10px;
    }

    .section h2 {
      margin: 0;
      font-size: 14px;
      letter-spacing: 0.08em;
      text-transform: uppercase;
      color: var(--muted);
    }

    label {
      display: flex;
      flex-direction: column;
      gap: 6px;
      font-size: 13px;
      color: var(--muted);
    }

    input, textarea, select {
      width: 100%;
      border: 1px solid var(--line);
      border-radius: 12px;
      background: rgba(9, 14, 19, 0.9);
      color: var(--ink);
      padding: 12px 14px;
      font: inherit;
      outline: none;
    }

    input:focus, textarea:focus, select:focus {
      border-color: var(--accent);
      box-shadow: 0 0 0 3px rgba(114, 208, 163, 0.12);
    }

    button {
      border: 1px solid var(--line);
      background: var(--panel-2);
      color: var(--ink);
      border-radius: 12px;
      padding: 10px 14px;
      font: inherit;
      cursor: pointer;
      transition: border-color 120ms ease, background 120ms ease, transform 120ms ease;
    }

    button:hover {
      background: #1b2834;
      border-color: var(--accent-2);
    }

    button.primary {
      background: linear-gradient(135deg, rgba(114, 208, 163, 0.18), rgba(114, 208, 163, 0.08));
      border-color: rgba(114, 208, 163, 0.45);
    }

    button.danger {
      border-color: rgba(226, 109, 109, 0.45);
      color: #ffd7d7;
    }

    .meta-grid {
      display: grid;
      grid-template-columns: 1fr 1fr;
      gap: 10px;
    }

    .stat {
      padding: 12px;
      border: 1px solid var(--line);
      border-radius: 14px;
      background: rgba(9, 14, 19, 0.6);
    }

    .stat .label {
      color: var(--muted);
      font-size: 12px;
      text-transform: uppercase;
      letter-spacing: 0.06em;
    }

    .stat .value {
      margin-top: 6px;
      font-size: 18px;
    }

    .chat {
      display: grid;
      grid-template-rows: auto 1fr auto;
      min-height: calc(100vh - 48px);
    }

    .chat-header {
      padding: 20px 22px 18px;
      border-bottom: 1px solid var(--line);
      display: flex;
      justify-content: space-between;
      align-items: center;
      gap: 14px;
    }

    .chat-header h2 {
      margin: 0;
      font-size: 22px;
    }

    .chat-header p {
      margin: 6px 0 0;
      color: var(--muted);
      font-size: 14px;
    }

    .status {
      color: var(--muted);
      font-size: 13px;
      text-align: right;
    }

    .messages {
      padding: 22px;
      overflow: auto;
      display: flex;
      flex-direction: column;
      gap: 16px;
    }

    .message {
      max-width: 86%;
      padding: 15px 16px;
      border-radius: 18px;
      border: 1px solid var(--line);
      white-space: pre-wrap;
      line-height: 1.55;
    }

    .message.user {
      align-self: flex-end;
      background: rgba(114, 208, 163, 0.09);
      border-color: rgba(114, 208, 163, 0.28);
    }

    .message.assistant {
      align-self: flex-start;
      background: rgba(255, 255, 255, 0.03);
    }

    .message.system {
      align-self: center;
      background: rgba(228, 154, 89, 0.08);
      border-color: rgba(228, 154, 89, 0.22);
      color: var(--muted);
    }

    .message header {
      display: flex;
      justify-content: space-between;
      align-items: baseline;
      gap: 12px;
      margin-bottom: 10px;
      font-size: 12px;
      color: var(--muted);
      text-transform: uppercase;
      letter-spacing: 0.06em;
    }

    .citations {
      margin-top: 14px;
      display: flex;
      flex-direction: column;
      gap: 10px;
    }

    .feedback-row {
      margin-top: 14px;
      display: flex;
      gap: 10px;
      flex-wrap: wrap;
      align-items: center;
    }

    .citation {
      border: 1px solid var(--line);
      border-radius: 12px;
      padding: 10px 12px;
      background: rgba(9, 14, 19, 0.52);
    }

    .citation strong {
      color: var(--accent);
    }

    .citation small {
      display: block;
      color: var(--muted);
      margin-bottom: 6px;
    }

    .citation img {
      display: block;
      max-width: 220px;
      max-height: 180px;
      margin-top: 10px;
      border-radius: 10px;
      border: 1px solid var(--line);
    }

    .composer {
      border-top: 1px solid var(--line);
      padding: 18px 22px 22px;
      display: flex;
      flex-direction: column;
      gap: 12px;
    }

    .composer-row {
      display: grid;
      grid-template-columns: 1fr auto;
      gap: 12px;
      align-items: end;
    }

    .composer-actions {
      display: flex;
      gap: 10px;
      justify-content: space-between;
      align-items: center;
    }

    .tiny {
      font-size: 12px;
      color: var(--muted);
    }

    .hidden {
      display: none !important;
    }

    @media (max-width: 980px) {
      .app {
        grid-template-columns: 1fr;
      }
      .chat {
        min-height: auto;
      }
      .message {
        max-width: 100%;
      }
    }
  </style>
</head>
<body>
  <div class="app">
    <aside class="panel sidebar">
      <div class="brand">
        <h1>Bee Agent</h1>
        <p>Read-only question answering over the indexed beekeeping corpus with citations.</p>
      </div>

      <section class="section">
        <h2>Session</h2>
        <div class="meta-grid">
          <div class="stat">
            <div class="label">Session</div>
            <div class="value" id="session-id">new</div>
          </div>
          <div class="stat">
            <div class="label">Tenant</div>
            <div class="value" id="tenant-label">shared</div>
          </div>
        </div>
        <label>
          Tenant (public app is shared-tenant only)
          <input id="tenant-id" value="shared" readonly />
        </label>
        <label>
          Document IDs
          <input id="document-ids" placeholder="optional comma-separated ids" />
        </label>
        <label>
          Top K Override
          <input id="top-k" type="number" min="1" max="24" placeholder="auto" />
        </label>
        <label>
          Query Mode
          <select id="query-mode">
            <option value="auto" selected>Auto</option>
            <option value="general">General Corpus</option>
            <option value="sensor">Sensor Data</option>
          </select>
        </label>
        <div class="composer-actions">
          <button id="new-session" type="button">New Session</button>
          <button id="clear-chat" type="button" class="danger">Clear Chat View</button>
        </div>
      </section>

      <section class="section">
        <h2>Profile</h2>
        <div class="meta-grid">
          <div class="stat">
            <div class="label">Profile</div>
            <div class="value" id="profile-id">new</div>
          </div>
          <div class="stat">
            <div class="label">Experience</div>
            <div class="value" id="profile-experience-label">-</div>
          </div>
        </div>
        <label>
          Display Name
          <input id="profile-display-name" placeholder="optional" />
        </label>
        <label>
          Background / Context
          <textarea id="profile-background" rows="3" placeholder="Apiary size, climate, goals, operating constraints..."></textarea>
        </label>
        <label>
          Answer Preferences
          <textarea id="profile-preferences" rows="2" placeholder="Concise, step-by-step, technical depth, etc."></textarea>
        </label>
        <div class="composer-actions">
          <button id="save-profile" type="button">Save Profile</button>
        </div>
      </section>

      <section class="section">
        <h2>Run State</h2>
        <div class="meta-grid">
          <div class="stat">
            <div class="label">Last Status</div>
            <div class="value" id="run-status">idle</div>
          </div>
          <div class="stat">
            <div class="label">Confidence</div>
            <div class="value" id="run-confidence">-</div>
          </div>
          <div class="stat">
            <div class="label">Abstained</div>
            <div class="value" id="run-abstained">-</div>
          </div>
          <div class="stat">
            <div class="label">Top K</div>
            <div class="value" id="run-top-k">auto</div>
          </div>
          <div class="stat">
            <div class="label">Mode</div>
            <div class="value" id="run-mode">auto</div>
          </div>
          <div class="stat">
            <div class="label">Last Run</div>
            <div class="value" id="run-id">-</div>
          </div>
        </div>
        <div class="tiny" id="status-note">Ready.</div>
      </section>
    </aside>

    <main class="panel chat">
      <header class="chat-header">
        <div>
          <h2>Grounded Chat</h2>
          <p>Answers are generated from indexed chunks and KG support. Unsupported answers should abstain.</p>
        </div>
        <div class="status" id="live-status">Disconnected</div>
      </header>

      <section class="messages" id="messages"></section>

      <section class="composer">
        <label>
          Question
          <textarea id="question" rows="4" placeholder="Ask a question about the ingested corpus..."></textarea>
        </label>
        <div class="composer-row">
          <div class="tiny">The agent uses the current session unless you start a new one.</div>
          <button id="send" type="button" class="primary">Send</button>
        </div>
      </section>
    </main>
  </div>

  <script>
    const state = {
      sessionId: "",
      profileId: "",
      busy: false,
    };

    const el = {
      messages: document.getElementById("messages"),
      question: document.getElementById("question"),
      send: document.getElementById("send"),
      newSession: document.getElementById("new-session"),
      clearChat: document.getElementById("clear-chat"),
      sessionId: document.getElementById("session-id"),
      profileId: document.getElementById("profile-id"),
      profileExperienceLabel: document.getElementById("profile-experience-label"),
      profileDisplayName: document.getElementById("profile-display-name"),
      profileBackground: document.getElementById("profile-background"),
      profilePreferences: document.getElementById("profile-preferences"),
      saveProfile: document.getElementById("save-profile"),
      tenantId: document.getElementById("tenant-id"),
      tenantLabel: document.getElementById("tenant-label"),
      documentIds: document.getElementById("document-ids"),
      topK: document.getElementById("top-k"),
      queryMode: document.getElementById("query-mode"),
      runStatus: document.getElementById("run-status"),
      runConfidence: document.getElementById("run-confidence"),
      runAbstained: document.getElementById("run-abstained"),
      runTopK: document.getElementById("run-top-k"),
      runMode: document.getElementById("run-mode"),
      runId: document.getElementById("run-id"),
      statusNote: document.getElementById("status-note"),
      liveStatus: document.getElementById("live-status"),
    };

    function renderSession() {
      el.sessionId.textContent = state.sessionId || "new";
      el.profileId.textContent = state.profileId || "new";
      el.tenantLabel.textContent = el.tenantId.value || "shared";
    }

    function setBusy(busy, note) {
      state.busy = busy;
      el.send.disabled = busy;
      el.liveStatus.textContent = busy ? "Running" : "Connected";
      if (note) el.statusNote.textContent = note;
    }

    async function postJson(path, payload) {
      const response = await fetch(path, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify(payload),
      });
      const body = await response.json();
      if (!response.ok) {
        throw new Error(body.detail || "Request failed");
      }
      return body;
    }

    function addMessage(role, content, meta = {}) {
      const node = document.createElement("article");
      node.className = "message " + role;

      const header = document.createElement("header");
      const label = document.createElement("span");
      label.textContent = role === "user" ? "You" : role === "assistant" ? "Agent" : "System";
      const extra = document.createElement("span");
      extra.textContent = meta.extra || "";
      header.appendChild(label);
      header.appendChild(extra);
      node.appendChild(header);

      const body = document.createElement("div");
      body.textContent = content;
      node.appendChild(body);

      if (role === "assistant" && Array.isArray(meta.citations) && meta.citations.length) {
        const citations = document.createElement("div");
        citations.className = "citations";
        meta.citations.forEach((item) => {
          const citation = document.createElement("div");
          citation.className = "citation";
          const title = document.createElement("small");
          const isAsset = item.citation_kind === "asset" || !!item.asset_id;
          const pages = item.page_start ? " pages " + item.page_start + "-" + item.page_end : "";
          const assetPage = item.page_number ? " page " + item.page_number : "";
          title.textContent = isAsset
            ? ((item.label || item.asset_type || item.asset_id || "image") + assetPage)
            : ((item.section_title || item.chunk_id || "source") + pages);
          const quote = document.createElement("div");
          const chunkLabel = document.createElement("strong");
          chunkLabel.textContent = isAsset ? (item.asset_id || "asset") : (item.chunk_id || "chunk");
          const quoteBody = document.createElement("div");
          quoteBody.textContent = item.quote || "";
          quote.appendChild(chunkLabel);
          quote.appendChild(document.createElement("br"));
          quote.appendChild(quoteBody);
          citation.appendChild(title);
          citation.appendChild(quote);
          if (isAsset && item.image_url) {
            const image = document.createElement("img");
            image.src = item.image_url;
            image.alt = item.label || item.asset_type || item.asset_id || "asset image";
            citation.appendChild(image);
          }
          citations.appendChild(citation);
        });
        node.appendChild(citations);
      }

      if (role === "assistant" && meta.queryRunId) {
        const feedbackRow = document.createElement("div");
        feedbackRow.className = "feedback-row";
        const likeBtn = document.createElement("button");
        likeBtn.type = "button";
        likeBtn.className = "primary";
        likeBtn.textContent = "Like";
        const dislikeBtn = document.createElement("button");
        dislikeBtn.type = "button";
        dislikeBtn.className = "danger";
        dislikeBtn.textContent = "Dislike";
        const feedbackNote = document.createElement("span");
        feedbackNote.className = "tiny";
        if (meta.pattern) {
          feedbackNote.textContent =
            `Admin review history for similar queries: approved ${meta.pattern.approved_count || 0} | ` +
            `rejected ${meta.pattern.rejected_count || 0}`;
        } else {
          feedbackNote.textContent = "Mark this answer if it was useful. This feedback is logged, not used as an automatic correctness label.";
        }
        const handleFeedback = async (feedback) => {
          likeBtn.disabled = true;
          dislikeBtn.disabled = true;
          feedbackNote.textContent = "Saving feedback...";
          try {
            const result = await postJson(`/agent/runs/${meta.queryRunId}/feedback`, { feedback });
            const pattern = result.pattern || {};
            feedbackNote.textContent =
              `${feedback === "like" ? "Liked" : "Disliked"} | ` +
              `Admin review history: approved ${pattern.approved_count || 0} | ` +
              `rejected ${pattern.rejected_count || 0}`;
          } catch (error) {
            feedbackNote.textContent = String(error.message || error);
            likeBtn.disabled = false;
            dislikeBtn.disabled = false;
          }
        };
        likeBtn.addEventListener("click", () => handleFeedback("like"));
        dislikeBtn.addEventListener("click", () => handleFeedback("dislike"));
        feedbackRow.appendChild(likeBtn);
        feedbackRow.appendChild(dislikeBtn);
        feedbackRow.appendChild(feedbackNote);
        node.appendChild(feedbackRow);
      }

      el.messages.appendChild(node);
      el.messages.scrollTop = el.messages.scrollHeight;
    }

    function clearMessages() {
      el.messages.innerHTML = "";
    }

    async function syncSession() {
      try {
        const response = await fetch("/agent/session");
        const payload = await response.json();
        state.sessionId = payload.session_id || "";
      } catch (error) {
        state.sessionId = "";
      }
      renderSession();
    }

    async function syncProfile() {
      try {
        const response = await fetch("/agent/profile");
        const payload = await response.json();
        state.profileId = payload.profile_id || "";
        const profile = payload.profile || {};
        const summary = profile.summary_json || {};
        el.profileDisplayName.value = profile.display_name || "";
        el.profileBackground.value = [summary.user_background || "", summary.beekeeping_context || ""].filter(Boolean).join("\n");
        el.profilePreferences.value = (summary.answer_preferences || []).join(", ");
        el.profileExperienceLabel.textContent = summary.experience_level || "-";
      } catch (error) {
        state.profileId = "";
      }
      renderSession();
    }

    async function saveProfile() {
      try {
        const backgroundLines = (el.profileBackground.value || "").split(/\r?\n/).map(item => item.trim()).filter(Boolean);
        const preferences = (el.profilePreferences.value || "").split(",").map(item => item.trim()).filter(Boolean);
        const payload = {
          display_name: el.profileDisplayName.value.trim() || null,
          user_background: backgroundLines[0] || null,
          beekeeping_context: backgroundLines.slice(1).join(" | ") || null,
          answer_preferences: preferences,
          experience_level: el.profileExperienceLabel.textContent && el.profileExperienceLabel.textContent !== "-" ? el.profileExperienceLabel.textContent : null,
        };
        const response = await fetch("/agent/profile", {
          method: "PUT",
          headers: { "Content-Type": "application/json" },
          body: JSON.stringify(payload),
        });
        const body = await response.json();
        if (!response.ok) {
          throw new Error(body.detail || "Profile update failed");
        }
        const profile = body.profile || {};
        const summary = profile.summary_json || {};
        state.profileId = body.profile_id || state.profileId;
        el.profileExperienceLabel.textContent = summary.experience_level || "-";
        renderSession();
        addMessage("system", "Profile updated for this browser.");
      } catch (error) {
        addMessage("system", String(error.message || error));
      }
    }

    async function resetSession(reason) {
      state.sessionId = "";
      try {
        await fetch("/agent/session/reset", { method: "POST" });
      } catch (error) {
        // Client-side state is still cleared even if the reset request fails.
      }
      renderSession();
      if (reason) {
        addMessage("system", reason);
      }
    }

    async function sendQuestion() {
      if (state.busy) return;
      const question = el.question.value.trim();
      if (!question) return;

      const tenantId = (el.tenantId.value || "shared").trim() || "shared";
      const rawTopK = String(el.topK.value || "").trim();
      const topK = rawTopK ? Number(rawTopK) : null;
      const queryMode = String(el.queryMode.value || "auto").trim().toLowerCase() || "auto";
      const documentIds = (el.documentIds.value || "")
        .split(",")
        .map((item) => item.trim())
        .filter(Boolean);

      addMessage("user", question);
      el.question.value = "";
      renderSession();
      setBusy(true, "Running agent query...");

      try {
        const response = await fetch("/agent/chat", {
          method: "POST",
          headers: { "Content-Type": "application/json" },
          body: JSON.stringify({
            question,
            tenant_id: tenantId,
            document_ids: documentIds.length ? documentIds : null,
            top_k: Number.isFinite(topK) ? topK : null,
            query_mode: queryMode,
          }),
        });

        const payload = await response.json();
        if (!response.ok) {
          throw new Error(payload.detail || "Agent request failed");
        }

        state.sessionId = payload.session_id || state.sessionId;
        state.profileId = payload.profile_id || state.profileId;
        renderSession();

        el.runStatus.textContent = payload.review_status || "completed";
        el.runConfidence.textContent = typeof payload.confidence === "number" ? payload.confidence.toFixed(2) : "-";
        el.runAbstained.textContent = payload.abstained ? "yes" : "no";
        el.runTopK.textContent = payload.top_k ? `${payload.top_k} (${payload.top_k_source || "auto"})` : "auto";
        el.runMode.textContent = `${payload.query_mode || queryMode} / ${payload.system_prompt_variant || "general"}`;
        el.runId.textContent = payload.query_run_id || "-";
        const routing = payload.routing || {};
        const routingSource = routing.source || "unknown-router";
        const routingConfidence = routing.confidence != null ? ` @ ${routing.confidence}` : "";
        el.statusNote.textContent = payload.abstain_reason || `${payload.question_type || "unknown"} · ${payload.retrieval_mode || "unknown"} · ${routingSource}${routingConfidence}`;

        addMessage("assistant", payload.answer || "", {
          extra: payload.abstained ? "abstained" : "answered",
          citations: payload.citations || [],
          queryRunId: payload.query_run_id || "",
        });
      } catch (error) {
        const message = String(error.message || error);
        if (message.includes("Session token") || message.includes("Session not found") || message.includes("Session scope changed")) {
          await resetSession("The previous session is no longer valid, so a fresh session was started.");
        }
        addMessage("system", String(error.message || error));
        el.runStatus.textContent = "error";
        el.runTopK.textContent = "error";
        el.runMode.textContent = "error";
        el.statusNote.textContent = message;
      } finally {
        setBusy(false);
      }
    }

    el.send.addEventListener("click", sendQuestion);
    el.question.addEventListener("keydown", (event) => {
      if (event.key === "Enter" && (event.ctrlKey || event.metaKey)) {
        sendQuestion();
      }
    });

    el.newSession.addEventListener("click", async () => {
      await resetSession("Started a new session. The next question will create a fresh server session.");
    });
    el.saveProfile.addEventListener("click", saveProfile);

    el.clearChat.addEventListener("click", async () => {
      clearMessages();
      await resetSession("Cleared the local chat view and reset the server session to avoid hidden context carry-over.");
    });

    el.tenantId.addEventListener("change", async () => {
      await resetSession("Tenant changed. The previous session was cleared so retrieval scope stays consistent.");
    });
    el.documentIds.addEventListener("change", async () => {
      await resetSession("Document filter changed. The previous session was cleared so retrieval scope stays consistent.");
    });
    el.queryMode.addEventListener("change", async () => {
      await resetSession("Query mode changed. The previous session was cleared so hidden context does not mix general and sensor runs.");
    });

    Promise.all([syncSession(), syncProfile()]).finally(() => {
      renderSession();
      addMessage("system", "Agent UI ready. Public chat runs against the shared tenant unless a trusted internal route is used.");
      el.liveStatus.textContent = "Connected";
    });
  </script>
</body>
</html>
"""
