"""Workspace persistence for places, hives, sensors, and sensor readings.

This bounded storage module owns hive/workspace state. It intentionally does
not own agent runtime persistence, admin inspection, or ingestion state.
"""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
import re
from typing import Any

import psycopg
from psycopg.rows import dict_row
from psycopg.types.json import Jsonb

from src.bee_ingestion.settings import settings


def upsert_user_place(
    repo: Any,
    *,
    tenant_id: str,
    auth_user_id: str,
    external_place_id: str,
    place_name: str,
    status: str = "active",
    metadata_json: dict[str, Any] | None = None,
) -> dict:
    normalized_tenant_id = repo._sanitize_text(tenant_id or "").strip() or "shared"
    normalized_auth_user_id = repo._sanitize_text(auth_user_id or "").strip()
    normalized_external_place_id = repo._sanitize_text(external_place_id or "").strip()[:160]
    normalized_place_name = repo._sanitize_text(place_name or "").strip()[:160]
    normalized_status = repo._normalize_sensor_status(status)
    if not normalized_auth_user_id:
        raise ValueError("auth_user_id is required")
    if not normalized_external_place_id:
        raise ValueError("external_place_id is required")
    if not normalized_place_name:
        raise ValueError("place_name is required")
    with psycopg.connect(repo.dsn, row_factory=dict_row) as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO user_places (
                  tenant_id,
                  auth_user_id,
                  external_place_id,
                  place_name,
                  status,
                  metadata_json
                )
                VALUES (%s, %s, %s, %s, %s, %s)
                ON CONFLICT (tenant_id, auth_user_id, external_place_id) DO UPDATE SET
                  place_name = EXCLUDED.place_name,
                  status = EXCLUDED.status,
                  metadata_json = EXCLUDED.metadata_json,
                  updated_at = now()
                RETURNING *
                """,
                (
                    normalized_tenant_id,
                    normalized_auth_user_id,
                    normalized_external_place_id,
                    normalized_place_name,
                    normalized_status,
                    Jsonb(repo._sanitize_json_value(metadata_json or {})),
                ),
            )
            row = cur.fetchone()
        conn.commit()
    return dict(row) if row else {}


def list_user_places(
    repo: Any,
    *,
    tenant_id: str,
    auth_user_id: str,
    status: str | None = None,
    limit: int = 100,
    offset: int = 0,
) -> list[dict]:
    clauses = ["tenant_id = %s", "auth_user_id = %s"]
    params: list[object] = [tenant_id, auth_user_id]
    if status:
        clauses.append("status = %s")
        params.append(repo._normalize_sensor_status(status))
    with psycopg.connect(repo.dsn, row_factory=dict_row) as conn:
        with conn.cursor() as cur:
            cur.execute(
                f"""
                SELECT *
                FROM user_places
                WHERE {' AND '.join(clauses)}
                ORDER BY updated_at DESC, created_at DESC
                LIMIT %s OFFSET %s
                """,
                tuple(params + [limit, offset]),
            )
            return [dict(row) for row in cur.fetchall()]


def get_user_place(repo: Any, place_id: str, *, tenant_id: str, auth_user_id: str) -> dict | None:
    with psycopg.connect(repo.dsn, row_factory=dict_row) as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT *
                FROM user_places
                WHERE place_id = %s
                  AND tenant_id = %s
                  AND auth_user_id = %s
                """,
                (place_id, tenant_id, auth_user_id),
            )
            row = cur.fetchone()
            return dict(row) if row else None


def count_user_places(repo: Any, *, tenant_id: str, auth_user_id: str, status: str | None = None) -> int:
    clauses = ["tenant_id = %s", "auth_user_id = %s"]
    params: list[object] = [tenant_id, auth_user_id]
    if status:
        clauses.append("status = %s")
        params.append(repo._normalize_sensor_status(status))
    return repo._fetch_scalar(
        f"SELECT COUNT(*) AS value FROM user_places WHERE {' AND '.join(clauses)}",
        tuple(params),
    )


def upsert_user_hive(
    repo: Any,
    *,
    tenant_id: str,
    auth_user_id: str,
    external_hive_id: str,
    hive_name: str,
    place_id: str | None = None,
    status: str = "active",
    metadata_json: dict[str, Any] | None = None,
) -> dict:
    normalized_tenant_id = repo._sanitize_text(tenant_id or "").strip() or "shared"
    normalized_auth_user_id = repo._sanitize_text(auth_user_id or "").strip()
    normalized_external_hive_id = repo._sanitize_text(external_hive_id or "").strip()[:160]
    normalized_hive_name = repo._sanitize_text(hive_name or "").strip()[:160]
    normalized_status = repo._normalize_sensor_status(status)
    if not normalized_auth_user_id:
        raise ValueError("auth_user_id is required")
    if not normalized_external_hive_id:
        raise ValueError("external_hive_id is required")
    if not normalized_hive_name:
        raise ValueError("hive_name is required")
    place_row = None
    if place_id:
        place_row = get_user_place(
            repo,
            place_id,
            tenant_id=normalized_tenant_id,
            auth_user_id=normalized_auth_user_id,
        )
        if place_row is None:
            raise ValueError("Place not found")
    with psycopg.connect(repo.dsn, row_factory=dict_row) as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO user_hives (
                  tenant_id,
                  auth_user_id,
                  external_hive_id,
                  hive_name,
                  place_id,
                  status,
                  metadata_json
                )
                VALUES (%s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (tenant_id, auth_user_id, external_hive_id) DO UPDATE SET
                  hive_name = EXCLUDED.hive_name,
                  place_id = EXCLUDED.place_id,
                  status = EXCLUDED.status,
                  metadata_json = EXCLUDED.metadata_json,
                  updated_at = now()
                RETURNING *
                """,
                (
                    normalized_tenant_id,
                    normalized_auth_user_id,
                    normalized_external_hive_id,
                    normalized_hive_name,
                    str((place_row or {}).get("place_id") or "") or None,
                    normalized_status,
                    Jsonb(repo._sanitize_json_value(metadata_json or {})),
                ),
            )
            row = cur.fetchone()
        conn.commit()
    return dict(row) if row else {}


def list_user_hives(
    repo: Any,
    *,
    tenant_id: str,
    auth_user_id: str,
    status: str | None = None,
    place_id: str | None = None,
    limit: int = 100,
    offset: int = 0,
) -> list[dict]:
    clauses = ["h.tenant_id = %s", "h.auth_user_id = %s"]
    params: list[object] = [tenant_id, auth_user_id]
    if status:
        clauses.append("h.status = %s")
        params.append(repo._normalize_sensor_status(status))
    if place_id:
        clauses.append("h.place_id = %s")
        params.append(place_id)
    with psycopg.connect(repo.dsn, row_factory=dict_row) as conn:
        with conn.cursor() as cur:
            cur.execute(
                f"""
                SELECT
                  h.*,
                  COALESCE(p.place_name, NULL) AS resolved_place_name,
                  p.place_name,
                  p.external_place_id
                FROM user_hives h
                LEFT JOIN user_places p
                  ON p.place_id = h.place_id
                 AND p.tenant_id = h.tenant_id
                 AND p.auth_user_id = h.auth_user_id
                WHERE {' AND '.join(clauses)}
                ORDER BY h.updated_at DESC, h.created_at DESC
                LIMIT %s OFFSET %s
                """,
                tuple(params + [limit, offset]),
            )
            return [dict(row) for row in cur.fetchall()]


def get_user_hive(repo: Any, hive_id: str, *, tenant_id: str, auth_user_id: str) -> dict | None:
    with psycopg.connect(repo.dsn, row_factory=dict_row) as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT
                  h.*,
                  COALESCE(p.place_name, NULL) AS resolved_place_name,
                  p.place_name,
                  p.external_place_id
                FROM user_hives h
                LEFT JOIN user_places p
                  ON p.place_id = h.place_id
                 AND p.tenant_id = h.tenant_id
                 AND p.auth_user_id = h.auth_user_id
                WHERE h.hive_id = %s
                  AND h.tenant_id = %s
                  AND h.auth_user_id = %s
                """,
                (hive_id, tenant_id, auth_user_id),
            )
            row = cur.fetchone()
            return dict(row) if row else None


def count_user_hives(
    repo: Any,
    *,
    tenant_id: str,
    auth_user_id: str,
    status: str | None = None,
    place_id: str | None = None,
) -> int:
    clauses = ["tenant_id = %s", "auth_user_id = %s"]
    params: list[object] = [tenant_id, auth_user_id]
    if status:
        clauses.append("status = %s")
        params.append(repo._normalize_sensor_status(status))
    if place_id:
        clauses.append("place_id = %s")
        params.append(place_id)
    return repo._fetch_scalar(
        f"SELECT COUNT(*) AS value FROM user_hives WHERE {' AND '.join(clauses)}",
        tuple(params),
    )


def upsert_user_sensor(
    repo: Any,
    *,
    tenant_id: str,
    auth_user_id: str,
    external_sensor_id: str,
    sensor_name: str,
    sensor_type: str = "environment",
    place_id: str | None = None,
    hive_id: str | None = None,
    hive_name: str | None = None,
    location_label: str | None = None,
    status: str = "active",
    metadata_json: dict[str, Any] | None = None,
) -> dict:
    normalized_tenant_id = repo._sanitize_text(tenant_id or "").strip() or "shared"
    normalized_auth_user_id = repo._sanitize_text(auth_user_id or "").strip()
    normalized_external_sensor_id = repo._sanitize_text(external_sensor_id or "").strip()[:160]
    normalized_sensor_name = repo._sanitize_text(sensor_name or "").strip()[:160]
    normalized_sensor_type = repo._sanitize_text(sensor_type or "environment").strip().lower()[:64] or "environment"
    normalized_status = repo._normalize_sensor_status(status)
    place_row = None
    hive_row = None
    if not normalized_auth_user_id:
        raise ValueError("auth_user_id is required")
    if not normalized_external_sensor_id:
        raise ValueError("external_sensor_id is required")
    if not normalized_sensor_name:
        raise ValueError("sensor_name is required")
    if place_id:
        place_row = get_user_place(
            repo,
            place_id,
            tenant_id=normalized_tenant_id,
            auth_user_id=normalized_auth_user_id,
        )
        if place_row is None:
            raise ValueError("Place not found")
    if hive_id:
        hive_row = get_user_hive(
            repo,
            hive_id,
            tenant_id=normalized_tenant_id,
            auth_user_id=normalized_auth_user_id,
        )
        if hive_row is None:
            raise ValueError("Hive not found")
        hive_place_id = str(hive_row.get("place_id") or "").strip() or None
        if place_row and hive_place_id and hive_place_id != str(place_row.get("place_id") or ""):
            raise ValueError("Hive does not belong to the selected place")
        if place_row is None and hive_place_id:
            place_row = get_user_place(
                repo,
                hive_place_id,
                tenant_id=normalized_tenant_id,
                auth_user_id=normalized_auth_user_id,
            )
        elif place_row and not hive_place_id:
            raise ValueError("Hive must be assigned to the selected place before sensor registration")
    effective_hive_name = repo._sanitize_text(str((hive_row or {}).get("hive_name") or hive_name or "")).strip()[:160] or None
    effective_location_label = repo._sanitize_text(str((place_row or {}).get("place_name") or location_label or "")).strip()[:160] or None
    with psycopg.connect(repo.dsn, row_factory=dict_row) as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO user_sensors (
                  tenant_id,
                  auth_user_id,
                  external_sensor_id,
                  sensor_name,
                  sensor_type,
                  place_id,
                  hive_id,
                  hive_name,
                  location_label,
                  status,
                  metadata_json
                )
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (tenant_id, auth_user_id, external_sensor_id) DO UPDATE SET
                  sensor_name = EXCLUDED.sensor_name,
                  sensor_type = EXCLUDED.sensor_type,
                  place_id = EXCLUDED.place_id,
                  hive_id = EXCLUDED.hive_id,
                  hive_name = EXCLUDED.hive_name,
                  location_label = EXCLUDED.location_label,
                  status = EXCLUDED.status,
                  metadata_json = EXCLUDED.metadata_json,
                  updated_at = now()
                RETURNING *
                """,
                (
                    normalized_tenant_id,
                    normalized_auth_user_id,
                    normalized_external_sensor_id,
                    normalized_sensor_name,
                    normalized_sensor_type,
                    str((place_row or {}).get("place_id") or "") or None,
                    str((hive_row or {}).get("hive_id") or "") or None,
                    effective_hive_name,
                    effective_location_label,
                    normalized_status,
                    Jsonb(repo._sanitize_json_value(metadata_json or {})),
                ),
            )
            row = cur.fetchone()
        conn.commit()
    return dict(row) if row else {}


def list_user_sensors(
    repo: Any,
    *,
    tenant_id: str,
    auth_user_id: str,
    status: str | None = None,
    limit: int = 100,
    offset: int = 0,
) -> list[dict]:
    clauses = ["tenant_id = %s", "auth_user_id = %s"]
    params: list[object] = [tenant_id, auth_user_id]
    if status:
        clauses.append("status = %s")
        params.append(repo._normalize_sensor_status(status))
    with psycopg.connect(repo.dsn, row_factory=dict_row) as conn:
        with conn.cursor() as cur:
            cur.execute(
                f"""
                SELECT
                  s.*,
                  COALESCE(p.place_name, s.location_label) AS resolved_place_name,
                  p.place_name,
                  p.external_place_id,
                  COALESCE(h.hive_name, s.hive_name) AS resolved_hive_name,
                  h.hive_name AS linked_hive_name,
                  h.external_hive_id
                FROM user_sensors s
                LEFT JOIN user_places p
                  ON p.place_id = s.place_id
                 AND p.tenant_id = s.tenant_id
                 AND p.auth_user_id = s.auth_user_id
                LEFT JOIN user_hives h
                  ON h.hive_id = s.hive_id
                 AND h.tenant_id = s.tenant_id
                 AND h.auth_user_id = s.auth_user_id
                WHERE {' AND '.join(f's.{clause}' for clause in clauses)}
                ORDER BY s.updated_at DESC, s.created_at DESC
                LIMIT %s OFFSET %s
                """,
                tuple(params + [limit, offset]),
            )
            rows = [dict(row) for row in cur.fetchall()]
    return [_normalize_sensor_row(row) for row in rows]


def get_user_sensor(repo: Any, sensor_id: str, *, tenant_id: str, auth_user_id: str) -> dict | None:
    with psycopg.connect(repo.dsn, row_factory=dict_row) as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT
                  s.*,
                  COALESCE(p.place_name, s.location_label) AS resolved_place_name,
                  p.place_name,
                  p.external_place_id,
                  COALESCE(h.hive_name, s.hive_name) AS resolved_hive_name,
                  h.hive_name AS linked_hive_name,
                  h.external_hive_id
                FROM user_sensors s
                LEFT JOIN user_places p
                  ON p.place_id = s.place_id
                 AND p.tenant_id = s.tenant_id
                 AND p.auth_user_id = s.auth_user_id
                LEFT JOIN user_hives h
                  ON h.hive_id = s.hive_id
                 AND h.tenant_id = s.tenant_id
                 AND h.auth_user_id = s.auth_user_id
                WHERE s.sensor_id = %s
                  AND s.tenant_id = %s
                  AND s.auth_user_id = %s
                """,
                (sensor_id, tenant_id, auth_user_id),
            )
            row = cur.fetchone()
    if not row:
        return None
    return _normalize_sensor_row(dict(row))


def count_user_sensors(repo: Any, *, tenant_id: str, auth_user_id: str, status: str | None = None) -> int:
    clauses = ["tenant_id = %s", "auth_user_id = %s"]
    params: list[object] = [tenant_id, auth_user_id]
    if status:
        clauses.append("status = %s")
        params.append(repo._normalize_sensor_status(status))
    return repo._fetch_scalar(
        f"SELECT COUNT(*) AS value FROM user_sensors WHERE {' AND '.join(clauses)}",
        tuple(params),
    )


def save_sensor_readings(
    repo: Any,
    *,
    sensor_id: str,
    tenant_id: str,
    auth_user_id: str,
    readings: list[dict[str, Any]],
) -> list[dict]:
    if not readings:
        return []
    if len(readings) > settings.sensor_ingest_max_batch:
        raise ValueError(f"At most {settings.sensor_ingest_max_batch} readings are allowed per request")
    sensor_row = get_user_sensor(repo, sensor_id, tenant_id=tenant_id, auth_user_id=auth_user_id)
    if sensor_row is None:
        raise ValueError("Sensor not found")
    inserted: list[dict] = []
    with psycopg.connect(repo.dsn, row_factory=dict_row) as conn:
        with conn.cursor() as cur:
            for reading in readings:
                observed_at = repo._coerce_sensor_observed_at(reading.get("observed_at"))
                metric_name = repo._sanitize_text(str(reading.get("metric_name") or "")).strip().lower()[:80]
                unit = repo._sanitize_text(str(reading.get("unit") or "")).strip()[:32] or None
                text_value = repo._sanitize_text(str(reading.get("text_value") or "")).strip()[:4000] or None
                numeric_value = reading.get("numeric_value")
                quality_score = reading.get("quality_score")
                if not metric_name:
                    raise ValueError("metric_name is required for each reading")
                if numeric_value in ("", None) and text_value is None:
                    raise ValueError("Each reading requires numeric_value or text_value")
                normalized_numeric_value = None if numeric_value in ("", None) else float(numeric_value)
                reading_hash = repo._sensor_reading_hash(
                    observed_at=observed_at,
                    metric_name=metric_name,
                    unit=unit,
                    numeric_value=normalized_numeric_value,
                    text_value=text_value,
                )
                cur.execute(
                    """
                    INSERT INTO sensor_readings (
                      sensor_id,
                      tenant_id,
                      auth_user_id,
                      reading_hash,
                      observed_at,
                      metric_name,
                      unit,
                      numeric_value,
                      text_value,
                      quality_score,
                      metadata_json
                    )
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT (sensor_id, reading_hash) DO UPDATE SET
                      quality_score = COALESCE(EXCLUDED.quality_score, sensor_readings.quality_score),
                      metadata_json = CASE
                        WHEN EXCLUDED.metadata_json = '{}'::jsonb THEN sensor_readings.metadata_json
                        ELSE EXCLUDED.metadata_json
                      END
                    RETURNING *
                    """,
                    (
                        sensor_id,
                        tenant_id,
                        auth_user_id,
                        reading_hash,
                        observed_at,
                        metric_name,
                        unit,
                        normalized_numeric_value,
                        text_value,
                        None if quality_score in ("", None) else float(quality_score),
                        Jsonb(repo._sanitize_json_value(reading.get("metadata_json") or {})),
                    ),
                )
                row = cur.fetchone()
                if row:
                    inserted.append(dict(row))
        conn.commit()
    return inserted


def list_sensor_readings(
    repo: Any,
    *,
    tenant_id: str,
    auth_user_id: str,
    sensor_id: str | None = None,
    metric_name: str | None = None,
    start_at: datetime | None = None,
    end_at: datetime | None = None,
    limit: int = 200,
    offset: int = 0,
) -> list[dict]:
    clauses = ["tenant_id = %s", "auth_user_id = %s"]
    params: list[object] = [tenant_id, auth_user_id]
    if sensor_id:
        clauses.append("sensor_id = %s")
        params.append(sensor_id)
    if metric_name:
        clauses.append("metric_name = %s")
        params.append(repo._sanitize_text(metric_name).strip().lower())
    if start_at is not None:
        clauses.append("observed_at >= %s")
        params.append(start_at)
    if end_at is not None:
        clauses.append("observed_at <= %s")
        params.append(end_at)
    with psycopg.connect(repo.dsn, row_factory=dict_row) as conn:
        with conn.cursor() as cur:
            cur.execute(
                f"""
                SELECT *
                FROM sensor_readings
                WHERE {' AND '.join(clauses)}
                ORDER BY observed_at DESC, created_at DESC
                LIMIT %s OFFSET %s
                """,
                tuple(params + [limit, offset]),
            )
            return [dict(row) for row in cur.fetchall()]


def build_user_sensor_context(
    repo: Any,
    *,
    tenant_id: str,
    auth_user_id: str,
    normalized_query: str,
    max_rows: int,
    hours: int,
    points_per_metric: int,
) -> list[dict]:
    sensors = list_user_sensors(
        repo,
        tenant_id=tenant_id,
        auth_user_id=auth_user_id,
        status="active",
        limit=200,
        offset=0,
    )
    if not sensors:
        return []
    sensor_by_id = {str(row["sensor_id"]): row for row in sensors}
    start_at = datetime.now(timezone.utc) - timedelta(hours=max(1, hours))
    with psycopg.connect(repo.dsn, row_factory=dict_row) as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                WITH ranked AS (
                  SELECT
                    r.*,
                    ROW_NUMBER() OVER (
                      PARTITION BY r.sensor_id, r.metric_name
                      ORDER BY r.observed_at DESC, r.created_at DESC
                    ) AS rn
                  FROM sensor_readings r
                  JOIN user_sensors s
                    ON s.sensor_id = r.sensor_id
                   AND s.tenant_id = r.tenant_id
                   AND s.auth_user_id = r.auth_user_id
                  WHERE r.tenant_id = %s
                    AND r.auth_user_id = %s
                    AND r.observed_at >= %s
                    AND s.status = 'active'
                )
                SELECT *
                FROM ranked
                WHERE rn <= %s
                ORDER BY observed_at DESC, created_at DESC
                """,
                (
                    tenant_id,
                    auth_user_id,
                    start_at,
                    max(1, points_per_metric),
                ),
            )
            readings = [dict(row) for row in cur.fetchall()]
    grouped: dict[tuple[str, str], list[dict[str, Any]]] = {}
    for row in readings:
        sensor_id = str(row.get("sensor_id") or "")
        metric_name = str(row.get("metric_name") or "")
        if sensor_id not in sensor_by_id or not metric_name:
            continue
        grouped.setdefault((sensor_id, metric_name), []).append(row)
    query_terms = {
        token
        for token in re.findall(r"[a-z0-9%:/\.-]{2,}", repo._sanitize_text(normalized_query).lower())
        if token
    }
    rows: list[dict[str, Any]] = []
    for (sensor_id, metric_name), metric_rows in grouped.items():
        sensor = sensor_by_id[sensor_id]
        resolved_place_name = str(sensor.get("resolved_place_name") or sensor.get("place_name") or sensor.get("location_label") or "").strip()
        resolved_hive_name = str(sensor.get("resolved_hive_name") or sensor.get("linked_hive_name") or sensor.get("hive_name") or "").strip()
        metric_rows.sort(key=lambda item: item.get("observed_at") or datetime.min.replace(tzinfo=timezone.utc), reverse=True)
        latest = metric_rows[0]
        numeric_values = [float(row["numeric_value"]) for row in metric_rows if row.get("numeric_value") is not None]
        recent_points = []
        for row in metric_rows[: max(1, points_per_metric)]:
            point_value = row.get("numeric_value")
            if point_value is None:
                point_value = row.get("text_value")
            recent_points.append(
                {
                    "reading_id": str(row.get("reading_id") or ""),
                    "observed_at": row.get("observed_at"),
                    "value": point_value,
                }
            )
        summary_parts = [
            f"sensor {sensor.get('sensor_name')}",
            f"type {sensor.get('sensor_type')}",
            f"metric {metric_name}",
        ]
        if resolved_place_name:
            summary_parts.append(f"place {resolved_place_name}")
        if resolved_hive_name:
            summary_parts.append(f"hive {resolved_hive_name}")
        if latest.get("numeric_value") is not None:
            summary_parts.append(f"latest {latest.get('numeric_value')} {latest.get('unit') or ''}".strip())
        elif latest.get("text_value"):
            summary_parts.append(f"latest {latest.get('text_value')}")
        metric_query_text = " ".join(
            part
            for part in [
                str(sensor.get("sensor_name") or ""),
                str(sensor.get("sensor_type") or ""),
                metric_name,
                resolved_place_name,
                resolved_hive_name,
                str(sensor.get("location_label") or ""),
            ]
            if part
        ).lower()
        relevance_score = 0.0
        if not query_terms:
            relevance_score = 1.0
        else:
            for token in query_terms:
                if token in metric_query_text:
                    relevance_score += 1.0
            if metric_name in query_terms:
                relevance_score += 1.0
        if relevance_score <= 0:
            continue
        rows.append(
            {
                "sensor_row_id": f"{sensor_id}:{metric_name}",
                "sensor_id": sensor_id,
                "sensor_name": sensor.get("sensor_name"),
                "sensor_type": sensor.get("sensor_type"),
                "metric_name": metric_name,
                "place_name": resolved_place_name,
                "hive_name": resolved_hive_name,
                "latest_observed_at": latest.get("observed_at"),
                "latest_numeric_value": latest.get("numeric_value"),
                "latest_text_value": latest.get("text_value"),
                "unit": latest.get("unit"),
                "point_count": len(metric_rows),
                "min_value": min(numeric_values) if numeric_values else None,
                "max_value": max(numeric_values) if numeric_values else None,
                "avg_value": round(sum(numeric_values) / len(numeric_values), 4) if numeric_values else None,
                "recent_points": recent_points,
                "summary_text": "; ".join(summary_parts),
                "_relevance_score": relevance_score,
            }
        )
    rows.sort(
        key=lambda item: (
            float(item.get("_relevance_score") or 0.0),
            item.get("latest_observed_at") or datetime.min.replace(tzinfo=timezone.utc),
        ),
        reverse=True,
    )
    return rows[: max(1, max_rows)]


def _normalize_sensor_row(row: dict[str, Any]) -> dict[str, Any]:
    data = dict(row)
    resolved_place_name = str(data.get("resolved_place_name") or data.get("place_name") or data.get("location_label") or "").strip()
    resolved_hive_name = str(data.get("resolved_hive_name") or data.get("linked_hive_name") or data.get("hive_name") or "").strip()
    data["place_name"] = resolved_place_name
    data["location_label"] = resolved_place_name
    data["hive_name"] = resolved_hive_name
    return data
