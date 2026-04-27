import type {
  HiveWorkspaceDataset,
  SensorReadingRecord,
  UserHiveRecord,
  UserPlaceRecord,
  UserSensorRecord,
} from "@/lib/api/hiveWorkspace";

export type HiveStatus = "Healthy" | "High activity" | "Needs inspection";

export interface Hive {
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

export interface Apiary {
  id: string;
  name: string;
  place: string;
  notes: string;
  hives: Hive[];
}

export interface HiveReport {
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

export interface ReportSeedMessage {
  id: string;
  role: "assistant" | "system";
  content: string;
}

function createId(prefix: string) {
  return `${prefix}-${Math.random().toString(36).slice(2, 10)}`;
}

export const emptyHive: Hive = {
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

export const emptyApiary: Apiary = {
  id: "",
  name: "No apiary connected",
  place: "Awaiting live data",
  notes: "The UI is wired to Postgres now. Create or sync user places, hives, and sensors to fill this workspace.",
  hives: [emptyHive],
};

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

function deriveHiveStatus(readings: SensorReadingRecord[]): HiveStatus {
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

export function deriveApiaries(dataset: HiveWorkspaceDataset): Apiary[] {
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

export function buildReport(apiary: Apiary, hive: Hive): HiveReport {
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

export function seedMessages(report: HiveReport): ReportSeedMessage[] {
  return [
    { id: createId("msg"), role: "system", content: `Report ${report.title} is active. Follow-up questions are now scoped to this briefing.` },
    {
      id: createId("msg"),
      role: "assistant",
      content: `${report.summary}\n\nKey findings:\n- ${report.findings.join("\n- ")}\n\nRecommended actions:\n- ${report.actions.join("\n- ")}`,
    },
  ];
}

export function reportScopedQuestion(
  tenantId: string,
  report: HiveReport,
  apiary: Apiary,
  hive: Hive,
  question: string,
) {
  return [
    `Active tenant: ${tenantId}`,
    `Apiary: ${apiary.name} (${apiary.place})`,
    `Hive: ${hive.name}`,
    `Report title: ${report.title}`,
    `Report summary: ${report.summary}`,
    `Key findings: ${report.findings.join(" | ")}`,
    `Recommended actions: ${report.actions.join(" | ")}`,
    `User question: ${question}`,
  ].join("\n");
}
