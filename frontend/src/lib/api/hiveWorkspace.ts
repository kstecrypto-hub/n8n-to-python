import { buildQuery, requestJson } from "@/lib/api/http";
import type { PagedResponse } from "@/lib/contracts";

export interface UserPlaceRecord {
  place_id: string;
  external_place_id?: string | null;
  place_name: string;
  status?: string | null;
  metadata_json?: Record<string, unknown> | null;
  updated_at?: string | null;
  created_at?: string | null;
}

export interface UserHiveRecord {
  hive_id: string;
  external_hive_id?: string | null;
  hive_name: string;
  place_id?: string | null;
  place_name?: string | null;
  resolved_place_name?: string | null;
  status?: string | null;
  metadata_json?: Record<string, unknown> | null;
  updated_at?: string | null;
  created_at?: string | null;
}

export interface UserSensorRecord {
  sensor_id: string;
  external_sensor_id?: string | null;
  sensor_name: string;
  sensor_type?: string | null;
  place_id?: string | null;
  hive_id?: string | null;
  hive_name?: string | null;
  place_name?: string | null;
  location_label?: string | null;
  status?: string | null;
  metadata_json?: Record<string, unknown> | null;
  updated_at?: string | null;
  created_at?: string | null;
}

export interface SensorReadingRecord {
  reading_id?: string;
  sensor_id: string;
  observed_at: string;
  metric_name: string;
  unit?: string | null;
  numeric_value?: number | null;
  text_value?: string | null;
  quality_score?: number | null;
  metadata_json?: Record<string, unknown> | null;
  created_at?: string | null;
}

export interface HiveWorkspaceDataset {
  places: UserPlaceRecord[];
  hives: UserHiveRecord[];
  sensors: UserSensorRecord[];
  readingsBySensorId: Record<string, SensorReadingRecord[]>;
}

function listPlaces() {
  return requestJson<PagedResponse<UserPlaceRecord>>(`/places${buildQuery({ limit: 100, offset: 0 })}`);
}

function listHives() {
  return requestJson<PagedResponse<UserHiveRecord>>(`/hives${buildQuery({ limit: 200, offset: 0 })}`);
}

function listSensors() {
  return requestJson<PagedResponse<UserSensorRecord>>(`/sensors${buildQuery({ limit: 200, offset: 0 })}`);
}

function listSensorReadings(sensorId: string, limit = 24) {
  return requestJson<PagedResponse<SensorReadingRecord>>(
    `/sensors/${encodeURIComponent(sensorId)}/readings${buildQuery({ limit, offset: 0 })}`,
  );
}

export async function loadHiveWorkspaceDataset(): Promise<HiveWorkspaceDataset> {
  const [placesResponse, hivesResponse, sensorsResponse] = await Promise.all([listPlaces(), listHives(), listSensors()]);
  const activeSensors = sensorsResponse.items.filter((sensor) => String(sensor.status || "active").toLowerCase() !== "archived");
  const readingPairs = await Promise.all(
    activeSensors.map(async (sensor) => {
      const readingsResponse = await listSensorReadings(sensor.sensor_id, 24);
      return [sensor.sensor_id, readingsResponse.items] as const;
    }),
  );
  return {
    places: placesResponse.items,
    hives: hivesResponse.items,
    sensors: activeSensors,
    readingsBySensorId: Object.fromEntries(readingPairs),
  };
}
