import { useEffect, useState } from "react";
import {
  clearAdminProfileMemory,
  clearAdminSessionMemory,
  loadAdminAgentReviews,
  loadAdminPatterns,
  loadAdminProfileDetail,
  loadAdminProfiles,
  loadAdminRunDetail,
  loadAdminRuns,
  loadAdminSessionDetail,
  loadAdminSessions,
  replayAdminRun,
  reviewAdminRun,
} from "@/lib/api/admin";
import {
  asRecord,
  can,
  memoryCount,
  memoryPreview,
  pretty,
  renderMemoryItems,
  sid,
  type AdminExtendedSectionProps,
} from "./AdminExtendedSectionSupport";

export function AdminAgentSection(props: AdminExtendedSectionProps) {
  const [status, setStatus] = useState("");
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

  useEffect(() => {
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
  }, [props.tenantId]);

  useEffect(() => {
    if (sessionId) loadAdminSessionDetail(sessionId).then(setSessionDetail);
  }, [sessionId]);

  useEffect(() => {
    if (runId) loadAdminRunDetail(runId).then(setRunDetail);
  }, [runId]);

  useEffect(() => {
    if (profileId) loadAdminProfileDetail(profileId).then(setProfileDetail);
  }, [profileId]);

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
          <select value={reviewDecision} onChange={(event) => setReviewDecision(event.target.value)}>
            <option value="approved">approved</option>
            <option value="needs_revision">needs_revision</option>
            <option value="rejected">rejected</option>
          </select>
          <input value={reviewNotes} onChange={(event) => setReviewNotes(event.target.value)} placeholder="review notes" />
          <button type="button" disabled={!runId || !can(props, "agent.review")} onClick={() => void reviewAdminRun(runId, reviewDecision, reviewNotes).then(() => setStatus(`review saved: ${reviewDecision}`))}>
            Review run
          </button>
          <button type="button" disabled={!runId || !can(props, "agent.review")} onClick={() => void replayAdminRun(runId).then(() => setStatus("run replayed"))}>
            Replay run
          </button>
          <button type="button" disabled={!can(props, "agent.review")} onClick={() => void refreshAgentWorkbench().then(() => setStatus("agent workbench refreshed"))}>
            Refresh
          </button>
        </div>
        <div className="empty-state">{status || "Agent workbench"}</div>
        <pre className="admin-json-block">{pretty({ run: runDetail, reviews: reviews.slice(0, 10), patterns: patterns.slice(0, 10) })}</pre>
      </div>

      <div className="admin-panel admin-panel--sidebar">
        <div className="mini-card__label">Sessions</div>
        <div className="admin-list">
          {sessions.map((session) => {
            const id = String(session.session_id ?? session.agent_session_id ?? "");
            return (
              <button key={id} type="button" className={`admin-list-button ${sessionId === id ? "admin-list-button--active" : ""}`} onClick={() => setSessionId(id)}>
                <strong>{sid(id)}</strong>
                <span>{String(session.status ?? "n/a")}</span>
              </button>
            );
          })}
        </div>
      </div>

      <div className="admin-panel admin-panel--sidebar">
        <div className="mini-card__label">Runs</div>
        <div className="admin-list">
          {runs.map((run) => (
            <button key={String(run.query_run_id)} type="button" className={`admin-list-button ${runId === String(run.query_run_id) ? "admin-list-button--active" : ""}`} onClick={() => setRunId(String(run.query_run_id))}>
              <strong>{sid(run.query_run_id)}</strong>
              <span>{String(run.status ?? "n/a")}</span>
            </button>
          ))}
        </div>
      </div>

      <div className="admin-panel admin-panel--sidebar">
        <div className="mini-card__label">Profiles</div>
        <div className="admin-list">
          {profiles.map((profile) => (
            <button key={String(profile.profile_id)} type="button" className={`admin-list-button ${profileId === String(profile.profile_id) ? "admin-list-button--active" : ""}`} onClick={() => setProfileId(String(profile.profile_id))}>
              <strong>{sid(profile.profile_id)}</strong>
              <span>{String(profile.status ?? "n/a")}</span>
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
        <div className="chunk" style={{ marginTop: 8 }}>
          {String(sessionMemory?.summary_text ?? "").trim() || "No session memory summary."}
        </div>
        <div className="admin-section-grid" style={{ marginTop: 12 }}>
          {sessionSections.map((section) => (
            <div key={section.key} className="admin-panel">
              <strong>{section.label}</strong>
              <div className="muted">count {memoryCount(section.value)} | preview {memoryPreview(section.value)}</div>
              {section.key === "facts" && renderMemoryItems(section.value, "facts")}
              {section.key === "open_threads" || section.key === "resolved_threads" ? (
                renderMemoryItems(section.value, "threads")
              ) : Array.isArray(section.value) ? (
                renderMemoryItems(section.value, "generic")
              ) : (
                <pre className="admin-json-block">{pretty(section.value)}</pre>
              )}
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
        <div className="chunk" style={{ marginTop: 8 }}>
          {String(asRecord(profileDetail)?.summary_text ?? "").trim() || "No profile memory summary."}
        </div>
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
