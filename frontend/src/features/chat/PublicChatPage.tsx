import { startTransition, useEffect, useMemo, useRef, useState, type KeyboardEvent } from "react";
import { useAuth } from "@/lib/auth/authContext";
import {
  activatePublicSession,
  listPublicSessions,
  publicAssetImageUrl,
  loadPublicProfile,
  loadPublicSession,
  resetPublicSession,
  sendPublicChat,
  sendPublicFeedback,
  type AgentCitation,
  type AgentMessage,
  type PublicAgentSessionDetail,
  type PublicAgentSessionMemory,
  type PublicAgentSessionSummary,
} from "@/lib/api/publicAgent";
import { loadHiveWorkspaceDataset, type HiveWorkspaceDataset, type SensorReadingRecord, type UserPlaceRecord, type UserHiveRecord, type UserSensorRecord } from "@/lib/api/hiveWorkspace";
import { PUBLIC_TENANT_ID, summarizePublicProfile, summarizePublicSession, type PublicProfileSnapshot, type PublicSessionSnapshot } from "@/lib/auth/publicSession";

type HiveStatus = "Healthy" | "High activity" | "Needs inspection";
type QueryMode = "auto" | "general" | "sensor";
type WorkspaceCategory = "hive" | "general";

interface Hive {
  id: string;
  name: string;
  status: HiveStatus;
  summary: string;
  reading: {
    temperature: string;
    humidity: string;
    weight: string;
    acoustics: string;
    activity: string;
    lastUpdated: string;
  };
}

interface Apiary {
  id: string;
  name: string;
  place: string;
  notes: string;
  hives: Hive[];
}

interface HiveReport {
  id: string;
  apiaryId: string;
  hiveId: string;
  title: string;
  summary: string;
  riskLabel: string;
  confidence: number;
  generatedAt: string;
  findings: string[];
  actions: string[];
}

interface ChatEntry {
  id: string;
  role: "user" | "assistant" | "system";
  content: string;
  citations?: AgentCitation[];
  queryRunId?: string | null;
  feedback?: "like" | "dislike" | null;
}

const emptyHive: Hive = {
  id: "",
  name: "No hive connected",
  status: "Needs inspection",
  summary: "The workspace is now reading from the live database. Add or sync hive records to populate this view.",
  reading: {
    temperature: "--",
    humidity: "--",
    weight: "--",
    acoustics: "--",
    activity: "No live data",
    lastUpdated: "Awaiting readings",
  },
};

const emptyApiary: Apiary = {
  id: "",
  name: "No apiary connected",
  place: "Awaiting live data",
  notes: "The UI is wired to Postgres now. Create or sync user places, hives, and sensors to fill this workspace.",
  hives: [emptyHive],
};

function createId(prefix: string) {
  return `${prefix}-${Math.random().toString(36).slice(2, 10)}`;
}

function statusClassName(status: HiveStatus) {
  if (status === "Healthy") return "hive-status hive-status--healthy";
  if (status === "High activity") return "hive-status hive-status--active";
  return "hive-status hive-status--alert";
}

function normalizeWorkspaceError(error: unknown) {
  const message = error instanceof Error ? error.message : String(error);
  if (message.toLowerCase().includes("sensor.read")) {
    return "This account can chat, but it is not allowed to read hive telemetry.";
  }
  return message;
}

function formatObservedAt(value: string | null | undefined) {
  if (!value) {
    return "Awaiting readings";
  }
  const parsed = new Date(value);
  if (Number.isNaN(parsed.getTime())) {
    return value;
  }
  return parsed.toLocaleString();
}

function formatSessionTime(value: string | null | undefined) {
  if (!value) {
    return "No activity yet";
  }
  const parsed = new Date(value);
  if (Number.isNaN(parsed.getTime())) {
    return value;
  }
  return parsed.toLocaleString([], { month: "short", day: "numeric", hour: "2-digit", minute: "2-digit" });
}

function sessionPreview(session: PublicAgentSessionSummary) {
  const content = String(session.last_message_content || "").trim();
  if (!content) {
    return "No messages yet.";
  }
  return content.length > 120 ? `${content.slice(0, 117)}...` : content;
}

function normalizeStoredMessages(messages: AgentMessage[] | undefined): ChatEntry[] {
  return (messages || [])
    .map((message) => {
      const metadata = (message.metadata_json || {}) as Record<string, unknown>;
      const storedCitations = Array.isArray(metadata.citations_payload)
        ? (metadata.citations_payload as AgentCitation[]).filter((item) => Boolean(item))
        : [];
      return {
        id: String(message.message_id || createId("msg")),
        role: (message.role as ChatEntry["role"]) || "assistant",
        content: String(message.content || "").trim(),
        citations: storedCitations.length ? storedCitations : undefined,
        queryRunId: typeof metadata.query_run_id === "string" ? metadata.query_run_id : null,
      } satisfies ChatEntry;
    })
    .filter((message) => message.content);
}

function summarizeSessionMemory(memory: PublicAgentSessionMemory | null) {
  const summary = memory?.summary_json || {};
  const sessionGoal = typeof summary.session_goal === "string" ? summary.session_goal.trim() : "";
  const topics = Array.isArray(summary.topic_keywords) ? summary.topic_keywords.map((item) => String(item).trim()).filter(Boolean) : [];
  const openThreads = Array.isArray(summary.open_threads)
    ? summary.open_threads.map((item) => String(item?.thread || "").trim()).filter(Boolean)
    : [];
  return {
    sessionGoal,
    topics,
    openThreads,
    summaryText: String(memory?.summary_text || "").trim(),
  };
}

function formatMetricValue(record: SensorReadingRecord | undefined, fallbackUnit: string) {
  if (!record) {
    return "--";
  }
  if (record.numeric_value !== null && record.numeric_value !== undefined && Number.isFinite(record.numeric_value)) {
    const precision = Math.abs(record.numeric_value) >= 100 ? 0 : 1;
    return `${record.numeric_value.toFixed(precision)} ${record.unit || fallbackUnit}`.trim();
  }
  if (record.text_value) {
    return record.text_value;
  }
  return "--";
}

function metricValue(record: SensorReadingRecord | undefined) {
  return record?.numeric_value ?? null;
}

function pickLatestMetric(readings: SensorReadingRecord[], names: string[]) {
  const nameSet = new Set(names.map((item) => item.trim().toLowerCase()));
  return readings.find((reading) => nameSet.has(String(reading.metric_name || "").trim().toLowerCase()));
}

function deriveHiveStatus(readings: SensorReadingRecord[]) {
  const temperature = metricValue(pickLatestMetric(readings, ["temperature", "temp"]));
  const humidity = metricValue(pickLatestMetric(readings, ["humidity"]));
  const weight = metricValue(pickLatestMetric(readings, ["weight", "mass"]));
  const acoustics = metricValue(pickLatestMetric(readings, ["acoustics", "acoustic_frequency", "sound", "sound_level"]));
  if (temperature === null && humidity === null && weight === null && acoustics === null) {
    return "Needs inspection";
  }
  if ((temperature !== null && (temperature < 33 || temperature > 36.5)) || (humidity !== null && humidity >= 64)) {
    return "Needs inspection";
  }
  if ((temperature !== null && temperature >= 35) || (humidity !== null && humidity >= 60) || (acoustics !== null && acoustics >= 190)) {
    return "High activity";
  }
  return "Healthy";
}

function deriveActivityLabel(status: HiveStatus, readings: SensorReadingRecord[]) {
  if (!readings.length) {
    return "No live data";
  }
  if (status === "High activity") {
    return "Elevated";
  }
  if (status === "Needs inspection") {
    return "Check needed";
  }
  return "Normal";
}

function placeLabel(place: UserPlaceRecord) {
  const meta = place.metadata_json || {};
  const yard = typeof meta.yard === "string" ? meta.yard.trim() : "";
  if (yard) {
    return `${yard} yard`;
  }
  return place.place_name;
}

function placeNotes(place: UserPlaceRecord) {
  const meta = place.metadata_json || {};
  const parts = Object.entries(meta)
    .slice(0, 2)
    .map(([key, value]) => `${key}: ${String(value)}`)
    .filter((item) => item.trim());
  if (parts.length) {
    return parts.join(" | ");
  }
  return `Live apiary record synced from ${place.external_place_id || "the database"}.`;
}

function deriveHiveSummary(hive: UserHiveRecord, status: HiveStatus, readings: SensorReadingRecord[]) {
  const temperature = formatMetricValue(pickLatestMetric(readings, ["temperature", "temp"]), "C");
  const humidity = formatMetricValue(pickLatestMetric(readings, ["humidity"]), "%");
  if (!readings.length) {
    return `${hive.hive_name} has no linked live readings yet. Check sensor assignment for this hive.`;
  }
  if (status === "High activity") {
    return `${hive.hive_name} is trending warm and active. Latest telemetry shows ${temperature} and ${humidity}.`;
  }
  if (status === "Needs inspection") {
    return `${hive.hive_name} is outside the safer operating band. Latest telemetry shows ${temperature} and ${humidity}.`;
  }
  return `${hive.hive_name} is currently stable with live readings around ${temperature} and ${humidity}.`;
}

function deriveHiveReading(readings: SensorReadingRecord[], status: HiveStatus): Hive["reading"] {
  const observedAt = readings.find((reading) => reading.observed_at)?.observed_at;
  return {
    temperature: formatMetricValue(pickLatestMetric(readings, ["temperature", "temp"]), "C"),
    humidity: formatMetricValue(pickLatestMetric(readings, ["humidity"]), "%"),
    weight: formatMetricValue(pickLatestMetric(readings, ["weight", "mass"]), "kg"),
    acoustics: formatMetricValue(pickLatestMetric(readings, ["acoustics", "acoustic_frequency", "sound", "sound_level"]), "Hz"),
    activity: deriveActivityLabel(status, readings),
    lastUpdated: formatObservedAt(observedAt),
  };
}

function deriveApiaries(dataset: HiveWorkspaceDataset): Apiary[] {
  const readingsBySensorId = dataset.readingsBySensorId || {};
  const sensorsByHiveId = new Map<string, UserSensorRecord[]>();
  for (const sensor of dataset.sensors) {
    const hiveId = String(sensor.hive_id || "").trim();
    if (!hiveId) {
      continue;
    }
    const bucket = sensorsByHiveId.get(hiveId) || [];
    bucket.push(sensor);
    sensorsByHiveId.set(hiveId, bucket);
  }
  const hivesByPlace = new Map<string, UserHiveRecord[]>();
  for (const hive of dataset.hives) {
    const placeId = String(hive.place_id || "").trim();
    if (!placeId) {
      continue;
    }
    const bucket = hivesByPlace.get(placeId) || [];
    bucket.push(hive);
    hivesByPlace.set(placeId, bucket);
  }

  return dataset.places
    .map((place) => {
      const rows = (hivesByPlace.get(place.place_id) || []).map((hive) => {
        const readings = (sensorsByHiveId.get(hive.hive_id) || []).flatMap((sensor) => readingsBySensorId[sensor.sensor_id] || []);
        const status = deriveHiveStatus(readings);
        return {
          id: hive.hive_id,
          name: hive.hive_name,
          status,
          summary: deriveHiveSummary(hive, status, readings),
          reading: deriveHiveReading(readings, status),
        } satisfies Hive;
      });
      return {
        id: place.place_id,
        name: place.place_name,
        place: placeLabel(place),
        notes: placeNotes(place),
        hives: rows.length ? rows : [emptyHive],
      } satisfies Apiary;
    })
    .filter((place) => place.id);
}

function buildReport(apiary: Apiary, hive: Hive): HiveReport {
  const generatedAt = new Date().toLocaleString();
  if (hive.status === "High activity") {
    return {
      id: createId("report"),
      apiaryId: apiary.id,
      hiveId: hive.id,
      title: `${hive.name} swarm watch briefing`,
      summary: `${hive.name} is pushing above baseline across temperature, weight, and acoustics. The colony looks productive, but the activity pattern now justifies swarm-prevention checks before the next nectar surge.`,
      riskLabel: "Swarm watch",
      confidence: 0.84,
      generatedAt,
      findings: [
        `Weight is sitting at ${hive.reading.weight}, which implies a strong intake window.`,
        `Acoustic readings at ${hive.reading.acoustics} are above the calmer hives in the same apiary.`,
        `Current activity is ${hive.reading.activity.toLowerCase()}, so congestion risk is rising faster than cooling capacity.`,
      ],
      actions: ["Inspect brood space and queen cell pressure.", "Prepare super addition within 48 hours.", "Cross-check entrance traffic with visual inspection before intervention."],
    };
  }
  if (hive.status === "Needs inspection") {
    return {
      id: createId("report"),
      apiaryId: apiary.id,
      hiveId: hive.id,
      title: `${hive.name} inspection priority briefing`,
      summary: `${hive.name} is showing a softer movement profile with elevated humidity. The model is treating this as an inspection-first hive rather than a production-first hive.`,
      riskLabel: "Inspection priority",
      confidence: 0.79,
      generatedAt,
      findings: [
        `Humidity is ${hive.reading.humidity}, above the preferred band for this yard.`,
        `Acoustics at ${hive.reading.acoustics} and activity marked ${hive.reading.activity.toLowerCase()} point to a quieter colony state.`,
        `Temperature is ${hive.reading.temperature}, so the main anomaly is moisture and movement, not heat stress.`,
      ],
      actions: ["Check ventilation and moisture sources.", "Inspect brood consistency and feed reserves.", "Validate sensor placement before changing management strategy."],
    };
  }
  return {
    id: createId("report"),
    apiaryId: apiary.id,
    hiveId: hive.id,
    title: `${hive.name} stable colony briefing`,
    summary: `${hive.name} is within its expected operating band. The report focuses on maintaining momentum, preserving space, and monitoring for the next shift rather than immediate intervention.`,
    riskLabel: "Stable",
    confidence: 0.88,
    generatedAt,
    findings: [
      `Temperature is ${hive.reading.temperature} and humidity is ${hive.reading.humidity}, both in the normal range for this site.`,
      `Weight at ${hive.reading.weight} suggests ongoing forage success without abrupt jumps.`,
      `Activity is ${hive.reading.activity.toLowerCase()} with a steady acoustic profile at ${hive.reading.acoustics}.`,
    ],
    actions: ["Keep the current management plan in place.", "Monitor brood expansion before adding more space.", "Use the next report cycle to confirm trend stability."],
  };
}

function seedMessages(report: HiveReport): ChatEntry[] {
  return [
    { id: createId("msg"), role: "system", content: `Report ${report.title} is active. Follow-up questions are now scoped to this briefing.` },
    {
      id: createId("msg"),
      role: "assistant",
      content: `${report.summary}\n\nKey findings:\n- ${report.findings.join("\n- ")}\n\nRecommended actions:\n- ${report.actions.join("\n- ")}`,
    },
  ];
}

function normalizeChatError(error: unknown) {
  const message = error instanceof Error ? error.message : String(error);
  if (message.toLowerCase().includes("quota") || message.includes("429")) {
    return "The live model is reachable, but the current embedding quota is exhausted. Report generation still works locally; live follow-up answers need a working provider key.";
  }
  return message;
}

function reportScopedQuestion(report: HiveReport, apiary: Apiary, hive: Hive, question: string) {
  return [
    `Active tenant: ${PUBLIC_TENANT_ID}`,
    `Apiary: ${apiary.name} (${apiary.place})`,
    `Hive: ${hive.name}`,
    `Report title: ${report.title}`,
    `Report summary: ${report.summary}`,
    `Key findings: ${report.findings.join(" | ")}`,
    `Recommended actions: ${report.actions.join(" | ")}`,
    `User question: ${question}`,
  ].join("\n");
}

export function PublicChatPage() {
  const { session: authSession, logout } = useAuth();
  const [apiaries, setApiaries] = useState<Apiary[]>([]);
  const [workspaceBusy, setWorkspaceBusy] = useState(true);
  const [workspaceError, setWorkspaceError] = useState<string | null>(null);
  const [workspaceCategory, setWorkspaceCategory] = useState<WorkspaceCategory>("hive");
  const [selectedApiaryId, setSelectedApiaryId] = useState("");
  const [selectedHiveId, setSelectedHiveId] = useState("");
  const [queryMode, setQueryMode] = useState<QueryMode>("auto");
  const [activeReport, setActiveReport] = useState<HiveReport | null>(null);
  const [reportHistory, setReportHistory] = useState<HiveReport[]>([]);
  const [messages, setMessages] = useState<ChatEntry[]>([]);
  const [prompt, setPrompt] = useState("");
  const [busy, setBusy] = useState(false);
  const [feedbackBusy, setFeedbackBusy] = useState<Record<string, boolean>>({});
  const [apiaryOpen, setApiaryOpen] = useState(false);
  const [hiveOpen, setHiveOpen] = useState(false);
  const [historyOpen, setHistoryOpen] = useState(false);
  const [modeOpen, setModeOpen] = useState(false);
  const [placeOpen, setPlaceOpen] = useState(true);
  const [operatorOpen, setOperatorOpen] = useState(false);
  const [sessionSnapshot, setSessionSnapshot] = useState<PublicSessionSnapshot | null>(null);
  const [profileSnapshot, setProfileSnapshot] = useState<PublicProfileSnapshot | null>(null);
  const [generalSessions, setGeneralSessions] = useState<PublicAgentSessionSummary[]>([]);
  const [generalSessionsBusy, setGeneralSessionsBusy] = useState(false);
  const [generalSessionsError, setGeneralSessionsError] = useState<string | null>(null);
  const [generalSessionMemory, setGeneralSessionMemory] = useState<PublicAgentSessionMemory | null>(null);
  const [activeGeneralSessionId, setActiveGeneralSessionId] = useState<string | null>(null);
  const [draftGeneralSessionId, setDraftGeneralSessionId] = useState<string | null>(null);
  const [generalDetailBusy, setGeneralDetailBusy] = useState(false);
  const generalViewEpochRef = useRef(0);

  const isGeneralAssistant = workspaceCategory === "general";
  const selectedApiary = useMemo<Apiary>(() => apiaries.find((item) => item.id === selectedApiaryId) ?? apiaries[0] ?? emptyApiary, [apiaries, selectedApiaryId]);
  const selectedHive = useMemo<Hive>(() => selectedApiary.hives.find((item) => item.id === selectedHiveId) ?? selectedApiary.hives[0] ?? emptyHive, [selectedApiary, selectedHiveId]);
  const canUseSensorMode = useMemo(
    () => (authSession?.user?.permissions ?? []).some((permission) => String(permission).trim().toLowerCase() === "sensor.read"),
    [authSession?.user?.permissions],
  );
  const availableQueryModes = useMemo<QueryMode[]>(
    () => (isGeneralAssistant || !canUseSensorMode ? ["auto", "general"] : ["auto", "general", "sensor"]),
    [isGeneralAssistant, canUseSensorMode],
  );

  useEffect(() => {
    let alive = true;
    async function bootstrap() {
      try {
        const [sessionPayload, profilePayload] = await Promise.all([loadPublicSession(), loadPublicProfile()]);
        if (!alive) return;
        startTransition(() => {
          setSessionSnapshot(sessionPayload);
          setProfileSnapshot(profilePayload);
        });
      } catch {
        if (!alive) return;
      }
    }
    void bootstrap();
    return () => {
      alive = false;
    };
  }, []);

  useEffect(() => {
    let alive = true;
    async function bootstrapWorkspace() {
      setWorkspaceBusy(true);
      setWorkspaceError(null);
      try {
        const dataset = await loadHiveWorkspaceDataset();
        if (!alive) {
          return;
        }
        const nextApiaries = deriveApiaries(dataset);
        startTransition(() => {
          setApiaries(nextApiaries);
        });
      } catch (error) {
        if (!alive) {
          return;
        }
        setApiaries([]);
        setWorkspaceError(normalizeWorkspaceError(error));
      } finally {
        if (alive) {
          setWorkspaceBusy(false);
        }
      }
    }
    void bootstrapWorkspace();
    return () => {
      alive = false;
    };
  }, []);

  useEffect(() => {
    if (!apiaries.length) {
      if (selectedApiaryId) {
        setSelectedApiaryId("");
      }
      return;
    }
    if (!apiaries.some((item) => item.id === selectedApiaryId)) {
      setSelectedApiaryId(apiaries[0]!.id);
    }
  }, [apiaries, selectedApiaryId]);

  useEffect(() => {
    if (!selectedApiary.hives.length) {
      if (selectedHiveId) {
        setSelectedHiveId("");
      }
      return;
    }
    if (selectedApiary.hives.some((item) => item.id === selectedHiveId)) return;
    setSelectedHiveId(selectedApiary.hives[0]!.id);
  }, [selectedApiary, selectedHiveId]);

  useEffect(() => {
    if (!availableQueryModes.includes(queryMode)) {
      setQueryMode(isGeneralAssistant ? "general" : "auto");
    }
  }, [availableQueryModes, isGeneralAssistant, queryMode]);

  async function refreshSnapshots() {
    try {
      const [sessionPayload, profilePayload] = await Promise.all([loadPublicSession(), loadPublicProfile()]);
      startTransition(() => {
        setSessionSnapshot(sessionPayload);
        setProfileSnapshot(profilePayload);
      });
    } catch {
      return;
    }
  }

  async function refreshGeneralSessions() {
    setGeneralSessionsBusy(true);
    setGeneralSessionsError(null);
    try {
      const payload = await listPublicSessions("general");
      startTransition(() => {
        setGeneralSessions(payload.items || []);
      });
    } catch (error) {
      setGeneralSessionsError(normalizeChatError(error));
    } finally {
      setGeneralSessionsBusy(false);
    }
  }

  async function loadGeneralSessionDetail(sessionId: string) {
    const requestEpoch = generalViewEpochRef.current;
    setGeneralDetailBusy(true);
    setGeneralSessionsError(null);
    try {
      const payload: PublicAgentSessionDetail = await activatePublicSession(sessionId);
      if (requestEpoch !== generalViewEpochRef.current) {
        return;
      }
      startTransition(() => {
        setActiveGeneralSessionId(sessionId);
        setMessages(normalizeStoredMessages(payload.messages));
        setGeneralSessionMemory(payload.memory || null);
        setSessionSnapshot((current) => ({
          ...(current || {}),
          session_id: payload.session?.session_id || sessionId,
          active: true,
          title: payload.session?.title || current?.title || null,
          status: payload.session?.status || current?.status || null,
          workspace_kind: payload.session?.workspace_kind || "general",
        }));
      });
    } catch (error) {
      setGeneralSessionsError(normalizeChatError(error));
    } finally {
      setGeneralDetailBusy(false);
    }
  }

  useEffect(() => {
    if (!isGeneralAssistant) {
      return;
    }
    void refreshGeneralSessions();
  }, [isGeneralAssistant]);

  useEffect(() => {
    if (!isGeneralAssistant) {
      return;
    }
    const sessionId = String(sessionSnapshot?.session_id || "").trim();
    const sessionIsGeneral = String(sessionSnapshot?.workspace_kind || "").trim().toLowerCase() === "general";
    if (!sessionSnapshot?.active || !sessionId || !sessionIsGeneral) {
      return;
    }
    if (activeGeneralSessionId === sessionId) {
      return;
    }
    void loadGeneralSessionDetail(sessionId);
  }, [activeGeneralSessionId, isGeneralAssistant, sessionSnapshot?.active, sessionSnapshot?.session_id, sessionSnapshot?.workspace_kind]);

  async function clearWorkspace() {
    generalViewEpochRef.current += 1;
    setActiveReport(null);
    setMessages([]);
    setActiveGeneralSessionId(null);
    setDraftGeneralSessionId(isGeneralAssistant ? createId("draft") : null);
    setGeneralSessionMemory(null);
    setPrompt("");
    setSessionSnapshot((current) => ({
      ...(current || {}),
      session_id: null,
      active: false,
      title: null,
      status: null,
      workspace_kind: isGeneralAssistant ? "general" : current?.workspace_kind || null,
    }));
    await resetPublicSession().catch(() => undefined);
    await refreshSnapshots();
    if (isGeneralAssistant) {
      await refreshGeneralSessions();
    }
  }

  async function handleWorkspaceCategorySelect(nextCategory: WorkspaceCategory) {
    if (nextCategory === workspaceCategory) {
      return;
    }
    generalViewEpochRef.current += 1;
    setWorkspaceCategory(nextCategory);
    setMessages([]);
    setActiveGeneralSessionId(null);
    setDraftGeneralSessionId(null);
    setGeneralSessionMemory(null);
    setPrompt("");
    setBusy(false);
    await resetPublicSession().catch(() => undefined);
    await refreshSnapshots();
    if (nextCategory === "general") {
      await refreshGeneralSessions();
    }
  }

  async function handleApiarySelect(nextApiaryId: string) {
    generalViewEpochRef.current += 1;
    const nextApiary = apiaries.find((item) => item.id === nextApiaryId) ?? apiaries[0] ?? emptyApiary;
    setSelectedApiaryId(nextApiary.id);
    setSelectedHiveId(nextApiary.hives[0]?.id ?? "");
    setActiveReport(null);
    setMessages([]);
    setDraftGeneralSessionId(null);
    setPrompt("");
    await resetPublicSession().catch(() => undefined);
    await refreshSnapshots();
  }

  async function handleHiveSelect(nextHiveId: string) {
    generalViewEpochRef.current += 1;
    setSelectedHiveId(nextHiveId);
    setActiveReport(null);
    setMessages([]);
    setDraftGeneralSessionId(null);
    setPrompt("");
    await resetPublicSession().catch(() => undefined);
    await refreshSnapshots();
  }

  async function handleGenerateReport() {
    if (!selectedApiary.id || !selectedHive.id) {
      setMessages([{ id: createId("msg"), role: "assistant", content: "No live hive is available yet. Add or sync hive data before generating a report." }]);
      return;
    }
    generalViewEpochRef.current += 1;
    const nextReport = buildReport(selectedApiary, selectedHive);
    setActiveReport(nextReport);
    setMessages(seedMessages(nextReport));
    setDraftGeneralSessionId(null);
    setReportHistory((current) => [nextReport, ...current.filter((item) => item.hiveId !== nextReport.hiveId)].slice(0, 8));
    setPrompt("");
    await resetPublicSession().catch(() => undefined);
    await refreshSnapshots();
  }

  async function handleSend() {
    const question = prompt.trim();
    if (!question || busy) return;
    const requestEpoch = generalViewEpochRef.current;
    const effectiveQueryMode = isGeneralAssistant && queryMode === "sensor" ? "general" : queryMode;
    const userEntry: ChatEntry = { id: createId("msg"), role: "user", content: question };
    setMessages((current) => [...current, userEntry]);
    setPrompt("");
    if (isGeneralAssistant) {
      setBusy(true);
      try {
        const response = await sendPublicChat({ question, query_mode: effectiveQueryMode, workspace_kind: "general" });
        const answer = response.answer?.trim() || "The model returned without an answer body.";
        if (requestEpoch !== generalViewEpochRef.current) {
          await refreshGeneralSessions();
          await refreshSnapshots();
          return;
        }
        startTransition(() => {
          setMessages((current) => [...current, { id: createId("msg"), role: "assistant", content: answer, citations: response.citations, queryRunId: response.query_run_id ?? null }]);
          setActiveGeneralSessionId(String(response.session_id || "").trim() || null);
          setDraftGeneralSessionId(null);
          setGeneralSessionMemory((response.session_memory as PublicAgentSessionMemory | null | undefined) || null);
        });
        await refreshSnapshots();
        await refreshGeneralSessions();
      } catch (error) {
        if (requestEpoch !== generalViewEpochRef.current) {
          await refreshGeneralSessions();
          await refreshSnapshots();
          return;
        }
        setMessages((current) => [...current, { id: createId("msg"), role: "assistant", content: normalizeChatError(error) }]);
      } finally {
        if (requestEpoch === generalViewEpochRef.current) {
          setBusy(false);
        }
      }
      return;
    }
    if (!selectedHive.id) {
      setMessages((current) => [...current, { id: createId("msg"), role: "assistant", content: "No live hive is available for discussion yet." }]);
      return;
    }
    if (!activeReport) {
      setMessages((current) => [...current, { id: createId("msg"), role: "assistant", content: "Generate a hive report first. The follow-up discussion is designed to explain a concrete report, not answer in a vacuum." }]);
      return;
    }
    setBusy(true);
    try {
      const response = await sendPublicChat({ question: reportScopedQuestion(activeReport, selectedApiary, selectedHive, question), query_mode: effectiveQueryMode, workspace_kind: "hive" });
      const answer = response.answer?.trim() || "The model returned without an answer body.";
      setMessages((current) => [...current, { id: createId("msg"), role: "assistant", content: answer, citations: response.citations, queryRunId: response.query_run_id ?? null }]);
      await refreshSnapshots();
    } catch (error) {
      setMessages((current) => [...current, { id: createId("msg"), role: "assistant", content: normalizeChatError(error) }]);
    } finally {
      setBusy(false);
    }
  }

  async function handleFeedback(message: ChatEntry, feedback: "like" | "dislike") {
    if (!message.queryRunId || feedbackBusy[message.id]) return;
    setFeedbackBusy((current) => ({ ...current, [message.id]: true }));
    try {
      await sendPublicFeedback(message.queryRunId, { feedback });
      setMessages((current) => current.map((entry) => (entry.id === message.id ? { ...entry, feedback } : entry)));
    } finally {
      setFeedbackBusy((current) => ({ ...current, [message.id]: false }));
    }
  }

  async function handleGeneralSessionSelect(sessionId: string) {
    if (!sessionId || generalDetailBusy || busy) {
      return;
    }
    setDraftGeneralSessionId(null);
    await loadGeneralSessionDetail(sessionId);
    await refreshSnapshots();
  }

  function handlePromptKeyDown(event: KeyboardEvent<HTMLTextAreaElement>) {
    if (event.key !== "Enter" || event.shiftKey) {
      return;
    }
    event.preventDefault();
    if (!prompt.trim() || busy) {
      return;
    }
    void handleSend();
  }

  function openReport(report: HiveReport) {
    setSelectedApiaryId(report.apiaryId);
    setSelectedHiveId(report.hiveId);
    setActiveReport(report);
    setMessages(seedMessages(report));
    setPrompt("");
  }

  const profileSummary = summarizePublicProfile(profileSnapshot);
  const sessionSummary = summarizePublicSession(sessionSnapshot);
  const sessionMemorySummary = summarizeSessionMemory(generalSessionMemory);
  const mainEyebrow = isGeneralAssistant ? "General assistant" : "Hive briefing workspace";
  const mainTitle = isGeneralAssistant ? "Ask the system directly" : selectedHive.name;
  const mainCaption = isGeneralAssistant ? "This conversation is not tied to a hive or a generated report." : `${selectedApiary.name} in ${selectedApiary.place}`;
  const titlebarLabel = isGeneralAssistant ? "Conversation scope" : "Current report";
  const titlebarTitle = isGeneralAssistant ? "General system conversation" : activeReport?.title ?? "No report generated yet";
  const titlebarPill = isGeneralAssistant ? (queryMode === "auto" ? "Auto retrieval" : "General retrieval") : activeReport?.riskLabel ?? "Pending";
  const conversationTitle = isGeneralAssistant ? "Ask the assistant" : "Ask about the report";
  const conversationEmptyState = isGeneralAssistant
    ? "Start a general conversation. This path does not require hive selection or a generated report."
    : "Generate a report to start the discussion. The assistant will explain that report rather than operate as a generic chat bot.";
  const composerLabel = isGeneralAssistant ? "General question" : "Report-scoped question";
  const composerPlaceholder = isGeneralAssistant
    ? "Ask a general beekeeping, knowledge-base, or system question without selecting a hive."
    : "Ask why the report highlighted a risk, what evidence supported a finding, or what should happen next on site.";
  const composerHint = isGeneralAssistant
    ? "Live answers use the actual agent API. This category does not require a report or hive selection."
    : "Live answers use the actual agent API. Report generation itself is local UI logic for now.";

  return (
    <section className="chat-app chat-app--report">
      <aside className="chat-sidebar report-sidebar">
        <div className="chat-sidebar__brand">
          <div className="report-sidebar__brand-row">
            <span className="brand-mark__glyph">BEE</span>
            <div><strong>Hive Mind</strong><small>{isGeneralAssistant ? "General assistant workspace" : "Report-first field workspace"}</small></div>
          </div>
        </div>
        <div className="panel-card report-sidebar__shell">
          <details className="report-sidebar__accordion report-sidebar__group" open>
            <summary className="report-sidebar__summary">
              <div><div className="eyebrow">Category</div><h2>Conversation path</h2></div>
              <span className="pill">{isGeneralAssistant ? "General" : "Hive"}</span>
            </summary>
            <div className="report-sidebar__section">
              <div className="report-chip-row">
                <button type="button" className={`mode-chip ${workspaceCategory === "hive" ? "mode-chip--active" : ""}`} onClick={() => void handleWorkspaceCategorySelect("hive")}>
                  <strong>Hive workspace</strong>
                  <span>Generate hive briefings and ask follow-up questions tied to a selected colony.</span>
                </button>
                <button type="button" className={`mode-chip ${workspaceCategory === "general" ? "mode-chip--active" : ""}`} onClick={() => void handleWorkspaceCategorySelect("general")}>
                  <strong>General assistant</strong>
                  <span>Speak with the system directly without selecting a hive or generating a report first.</span>
                </button>
              </div>
            </div>
          </details>

          <div className="report-sidebar__context">
            <div className="report-sidebar__context-item">
              <span className="mini-card__label">{isGeneralAssistant ? "Category" : "Apiary"}</span>
              <strong>{isGeneralAssistant ? "General assistant" : workspaceBusy ? "Loading..." : selectedApiary.name}</strong>
            </div>
            <div className="report-sidebar__context-item">
              <span className="mini-card__label">{isGeneralAssistant ? "Mode" : "Hive"}</span>
              <strong>{isGeneralAssistant ? queryMode : workspaceBusy ? "Loading..." : selectedHive.name}</strong>
            </div>
            <div className="report-sidebar__context-item">
              <span className="mini-card__label">{isGeneralAssistant ? "Profile" : "Status"}</span>
              <strong>{isGeneralAssistant ? profileSummary : workspaceBusy ? "Syncing" : selectedHive.status}</strong>
            </div>
            <div className="report-sidebar__context-item">
              <span className="mini-card__label">{isGeneralAssistant ? "Session" : "Last sync"}</span>
              <strong>{isGeneralAssistant ? sessionSummary : workspaceBusy ? "Loading..." : selectedHive.reading.lastUpdated}</strong>
            </div>
          </div>

          {!isGeneralAssistant ? (
          <>
          <details className="report-sidebar__accordion report-sidebar__group" open={apiaryOpen} onToggle={(event) => setApiaryOpen(event.currentTarget.open)}>
            <summary className="report-sidebar__summary">
              <div><div className="eyebrow">Apiary</div><h2>Choose a place</h2></div>
              <span className="pill">{selectedApiary.place}</span>
            </summary>
            <label><span>Apiary and placement</span><select value={selectedApiaryId} disabled={workspaceBusy || !apiaries.length} onChange={(event) => { void handleApiarySelect(event.target.value); }}>{apiaries.length ? apiaries.map((apiary) => <option key={apiary.id} value={apiary.id}>{apiary.name} - {apiary.place}</option>) : <option value="">{workspaceBusy ? "Loading apiaries..." : "No apiaries available"}</option>}</select></label>
            <div className="report-sidebar__section"><span className="mini-card__label">Field note</span><strong>{selectedApiary.name}</strong><p className="caption">{selectedApiary.notes}</p></div>
          </details>

          <details className="report-sidebar__accordion report-sidebar__group" open={hiveOpen} onToggle={(event) => setHiveOpen(event.currentTarget.open)}>
            <summary className="report-sidebar__summary">
              <div><div className="eyebrow">Hives</div><h2>Select colony</h2></div>
              <span className={`pill ${statusClassName(selectedHive.status)}`}>{selectedHive.status}</span>
            </summary>
            <label><span>Hive and colony</span><select value={selectedHiveId} disabled={workspaceBusy || !selectedApiary.id || !selectedApiary.hives.filter((hive) => hive.id).length} onChange={(event) => { void handleHiveSelect(event.target.value); }}>{selectedApiary.hives.filter((hive) => hive.id).length ? selectedApiary.hives.filter((hive) => hive.id).map((hive) => <option key={hive.id} value={hive.id}>{hive.name} - {hive.status}</option>) : <option value="">{workspaceBusy ? "Loading hives..." : "No hives available"}</option>}</select></label>
            <div className="report-sidebar__section"><div className="hive-selector__top"><strong>{selectedHive.name}</strong><span className={statusClassName(selectedHive.status)}>{selectedHive.status}</span></div><p className="caption">{selectedHive.summary}</p><div className="hive-selector__stats"><span className="metric-pill">{selectedHive.reading.temperature}</span><span className="metric-pill">{selectedHive.reading.weight}</span><span className="metric-pill">{selectedHive.reading.acoustics}</span><span className="metric-pill">{selectedHive.reading.activity}</span></div></div>
          </details>

          <details className="report-sidebar__accordion report-sidebar__group" open={historyOpen} onToggle={(event) => setHistoryOpen(event.currentTarget.open)}>
            <summary className="report-sidebar__summary">
              <div><div className="eyebrow">History</div><h2>Recent reports</h2></div>
              <span className="pill">{reportHistory.length}</span>
            </summary>
            <div className="report-sidebar__section">
              <div className="report-history-list">{reportHistory.length ? reportHistory.map((report) => <button key={report.id} type="button" className={`report-history-item ${activeReport?.id === report.id ? "report-history-item--active" : ""}`} onClick={() => openReport(report)}><strong>{report.title}</strong><span>{report.generatedAt}</span></button>) : <div className="empty-state"><p>No briefings yet. Generate the first report for the selected hive.</p></div>}</div>
            </div>
          </details>
          </>
          ) : (
            <div className="report-sidebar__section">
              <div className="general-session-header">
                <span className="mini-card__label">General chats</span>
                <button type="button" className="button button--ghost general-session-header__button" disabled={busy} onClick={() => void clearWorkspace()}>
                  New chat
                </button>
              </div>
              <p className="caption">Only the conversation you open is loaded into the stage. Other chats stay as lightweight history rows.</p>
              {generalSessionsError ? <p className="caption">{generalSessionsError}</p> : null}
              {workspaceError ? <p className="caption">{workspaceError}</p> : null}
              <div className="general-session-list">
                {draftGeneralSessionId ? (
                  <button
                    type="button"
                    className={`general-session-item ${!activeGeneralSessionId ? "general-session-item--active" : ""}`}
                    onClick={() => {
                      setActiveGeneralSessionId(null);
                      setMessages([]);
                      setGeneralSessionMemory(null);
                    }}
                  >
                    <div className="general-session-item__title">
                      <strong>New chat</strong>
                      <span>Draft</span>
                    </div>
                    <p>Nothing sent yet.</p>
                  </button>
                ) : null}
                {generalSessionsBusy ? (
                  <div className="empty-state"><p>Loading recent chats...</p></div>
                ) : generalSessions.length ? (
                  generalSessions.map((session) => {
                    const isActive = activeGeneralSessionId === session.session_id;
                    return (
                      <button
                        key={session.session_id}
                        type="button"
                        className={`general-session-item ${isActive ? "general-session-item--active" : ""}`}
                        onClick={() => void handleGeneralSessionSelect(session.session_id)}
                      >
                        <div className="general-session-item__title">
                          <strong>{session.title?.trim() || "Untitled chat"}</strong>
                          <span>{formatSessionTime(session.updated_at || session.last_message_at || session.created_at)}</span>
                        </div>
                        <p>{sessionPreview(session)}</p>
                      </button>
                    );
                  })
                ) : (
                  <div className="empty-state"><p>No general chats yet. Start one and it will appear here.</p></div>
                )}
              </div>
            </div>
          )}

          <details className="report-sidebar__accordion report-sidebar__group" open={modeOpen} onToggle={(event) => setModeOpen(event.currentTarget.open)}>
            <summary className="report-sidebar__summary">
              <div><div className="eyebrow">Discussion mode</div><h2>Answer style</h2></div>
              <span className="pill">{queryMode}</span>
            </summary>
            <div className="report-sidebar__section">
              <div className="report-chip-row">{availableQueryModes.map((mode) => <button key={mode} type="button" className={`mode-chip ${queryMode === mode ? "mode-chip--active" : ""}`} onClick={() => setQueryMode(mode)}><strong>{mode}</strong><span>{mode === "auto" ? "Let the backend pick the best retrieval mode." : mode === "sensor" ? "Bias discussion toward telemetry and report evidence." : "Keep answers general and explanatory."}</span></button>)}</div>
            </div>
          </details>

          <div className="chat-sidebar__footer chat-sidebar__footer--inside"><button type="button" className="button button--ghost" disabled={busy} onClick={() => void clearWorkspace()}>{isGeneralAssistant ? "New conversation" : "Fresh briefing"}</button><button type="button" className="button" onClick={() => void logout()}>Sign out</button></div>
        </div>
      </aside>

      <section className="report-stage">
        <div className="panel-card report-summary-card">
          {!isGeneralAssistant && workspaceError ? <div className="notice"><p>{workspaceError}</p></div> : null}
          {!isGeneralAssistant && !workspaceError && workspaceBusy ? <div className="notice"><p>Loading live apiary, hive, sensor, and reading data from Postgres.</p></div> : null}
          <div className="panel-card__header report-summary-card__header">
            <div>
              <div className="eyebrow">{mainEyebrow}</div>
              <h2>{mainTitle}</h2>
              <p className="caption">{mainCaption}</p>
            </div>
            <div className="report-summary-card__actions">
              <span className="pill">Tenant {PUBLIC_TENANT_ID}</span>
              {!isGeneralAssistant ? <button type="button" className="button button--primary" onClick={() => void handleGenerateReport()}>Generate report</button> : null}
              <button type="button" className="button button--ghost" disabled={busy} onClick={() => void clearWorkspace()}>Reset</button>
            </div>
          </div>
          <div className="report-summary-card__titlebar">
            <div>
              <div className="eyebrow">{titlebarLabel}</div>
              <strong>{titlebarTitle}</strong>
            </div>
            <span className={`pill ${!isGeneralAssistant && activeReport ? statusClassName(selectedHive.status) : ""}`.trim()}>{titlebarPill}</span>
          </div>
          {isGeneralAssistant ? (
            <div className="report-briefing-bubble">
              <div className="report-briefing-section">
                <span className="mini-card__label">How this category works</span>
                <p>Ask general beekeeping, knowledge retrieval, or system questions directly. This path does not require a selected hive or a generated report.</p>
                <div className="report-meta-row"><span className="pill">{queryMode === "auto" ? "Adaptive retrieval" : "General retrieval"}</span><span className="pill">Live agent API</span></div>
              </div>
              {selectedApiary.id && selectedHive.id ? (
                <div className="report-briefing-section">
                  <span className="mini-card__label">Optional live workspace</span>
                  <p>{selectedHive.name} in {selectedApiary.name} is still available if you want to switch back to hive-scoped briefings later.</p>
                </div>
              ) : null}
            </div>
          ) : activeReport ? <div className="report-briefing-bubble"><div className="report-briefing-section"><span className="mini-card__label">Executive summary</span><p>{activeReport.summary}</p><div className="report-meta-row"><span className="pill">Confidence {Math.round(activeReport.confidence * 100)}%</span><span className="pill">{activeReport.generatedAt}</span></div></div><div className="report-briefing-section"><span className="mini-card__label">Key findings</span><ul className="bullet-list">{activeReport.findings.map((item) => <li key={item}>{item}</li>)}</ul></div><div className="report-briefing-section"><span className="mini-card__label">Recommended actions</span><ul className="bullet-list">{activeReport.actions.map((item) => <li key={item}>{item}</li>)}</ul></div></div> : <div className="empty-state"><p>The report panel stays empty until you generate a briefing for the selected hive. That keeps follow-up chat anchored to a real report instead of a generic prompt.</p></div>}
        </div>

        <div className="panel-card report-chat-card">
          <div className="panel-card__header"><div><div className="eyebrow">Discussion</div><h2>{conversationTitle}</h2></div><span className="pill">{busy ? "Consulting model" : sessionSummary}</span></div>
          <div className="conversation report-conversation">{messages.length ? messages.map((message) => <article key={message.id} className={`bubble ${message.role === "user" ? "bubble--user" : message.role === "system" ? "bubble--system" : "bubble--assistant"}`}><div className="bubble__meta"><span>{message.role === "user" ? authSession?.user?.display_name || "You" : message.role === "system" ? "System" : "Hive Mind"}</span>{message.queryRunId ? <span className="mono-text">{message.queryRunId.slice(0, 8)}</span> : null}</div><p className="report-bubble__text">{message.content}</p>{message.citations?.length ? <div className="citation-list">{message.citations.map((citation, index) => <div key={`${message.id}-${index}`} className="citation-card"><div className="citation-card__title"><strong>{citation.label || citation.section_title || "Source evidence"}</strong></div>{citation.quote ? <p>{citation.quote}</p> : null}<div className="citation-card__meta"><span>{citation.document_id || citation.asset_type || citation.citation_kind || "retrieval"}</span>{citation.asset_id ? <a className="text-link" href={publicAssetImageUrl(citation.asset_id)} target="_blank" rel="noreferrer">Open asset</a> : null}</div></div>)}</div> : null}{message.role === "assistant" && message.queryRunId ? <div className="feedback-row"><button type="button" className="button button--ghost" disabled={feedbackBusy[message.id]} onClick={() => void handleFeedback(message, "like")}>{message.feedback === "like" ? "Liked" : "Like"}</button><button type="button" className="button button--ghost" disabled={feedbackBusy[message.id]} onClick={() => void handleFeedback(message, "dislike")}>{message.feedback === "dislike" ? "Disliked" : "Dislike"}</button></div> : null}</article>) : <div className="empty-state"><p>{conversationEmptyState}</p></div>}</div>
          <div className="composer composer--chat"><label><span>{composerLabel}</span><textarea rows={4} value={prompt} placeholder={composerPlaceholder} onChange={(event) => setPrompt(event.target.value)} onKeyDown={handlePromptKeyDown} /></label><div className="composer__actions"><span className="caption">{composerHint}</span><button type="button" className="button button--primary" disabled={busy} onClick={() => void handleSend()}>{busy ? "Sending..." : "Send to agent"}</button></div></div>
        </div>
      </section>

      <aside className="report-rail">
        {isGeneralAssistant ? (
          <div className="panel-card report-rail__panel">
            <div className="panel-card__header"><div><div className="eyebrow">General assistant</div><h2>Conversation context</h2></div><span className="pill">{generalDetailBusy ? "Loading chat" : queryMode}</span></div>
            <p className="caption">This category is not pinned to a hive report. Use it for general beekeeping or system questions.</p>
            <dl className="fact-list"><div><dt>Tenant</dt><dd>{PUBLIC_TENANT_ID}</dd></div><div><dt>Session</dt><dd>{sessionSummary}</dd></div><div><dt>Profile</dt><dd>{profileSummary}</dd></div><div><dt>Optional live hive</dt><dd>{selectedHive.id ? `${selectedHive.name} in ${selectedApiary.name}` : "None selected"}</dd></div></dl>
            <div className="report-sidebar__section report-sidebar__section--flush">
              <span className="mini-card__label">Session memory</span>
              {sessionMemorySummary.sessionGoal ? <p><strong>Goal:</strong> {sessionMemorySummary.sessionGoal}</p> : null}
              {sessionMemorySummary.topics.length ? <p><strong>Topics:</strong> {sessionMemorySummary.topics.join(", ")}</p> : null}
              {sessionMemorySummary.openThreads.length ? <p><strong>Open threads:</strong> {sessionMemorySummary.openThreads.join(" | ")}</p> : null}
              {!sessionMemorySummary.sessionGoal && !sessionMemorySummary.topics.length && !sessionMemorySummary.openThreads.length ? (
                <p className="caption">{sessionMemorySummary.summaryText || "This chat has not built a reusable session memory yet."}</p>
              ) : null}
            </div>
          </div>
        ) : (
          <div className="panel-card report-rail__panel">
            <div className="panel-card__header"><div><div className="eyebrow">Selected hive</div><h2>{selectedHive.name}</h2></div><span className={statusClassName(selectedHive.status)}>{selectedHive.status}</span></div>
            <p className="caption">{selectedHive.summary}</p>
            <div className="reading-grid"><div className="reading-card"><span className="mini-card__label">Temperature</span><strong>{selectedHive.reading.temperature}</strong></div><div className="reading-card"><span className="mini-card__label">Humidity</span><strong>{selectedHive.reading.humidity}</strong></div><div className="reading-card"><span className="mini-card__label">Weight</span><strong>{selectedHive.reading.weight}</strong></div><div className="reading-card"><span className="mini-card__label">Acoustics</span><strong>{selectedHive.reading.acoustics}</strong></div></div>
          </div>
        )}
        <div className="panel-card report-rail__panel report-rail__panel--stack">
          <details className="report-fold" open={placeOpen} onToggle={(event) => setPlaceOpen(event.currentTarget.open)}>
            <summary>{isGeneralAssistant ? "Optional live workspace" : "Place context"}</summary>
            <dl className="fact-list"><div><dt>Apiary</dt><dd>{selectedApiary.name}</dd></div><div><dt>Placement</dt><dd>{selectedApiary.place}</dd></div><div><dt>Activity</dt><dd>{selectedHive.reading.activity}</dd></div><div><dt>Last sync</dt><dd>{selectedHive.reading.lastUpdated}</dd></div></dl>
          </details>
          <details className="report-fold" open={operatorOpen} onToggle={(event) => setOperatorOpen(event.currentTarget.open)}>
            <summary>Operator facts</summary>
            <dl className="fact-list"><div><dt>Signed in as</dt><dd>{authSession?.user?.display_name || authSession?.user?.email || "Authenticated user"}</dd></div><div><dt>Public profile</dt><dd>{profileSummary}</dd></div><div><dt>Role</dt><dd>{authSession?.user?.role || "member"}</dd></div><div><dt>Permissions</dt><dd>{authSession?.user?.permissions?.join(", ") || "chat.use"}</dd></div></dl>
          </details>
        </div>
      </aside>
    </section>
  );
}
