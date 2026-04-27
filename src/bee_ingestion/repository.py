"""Postgres persistence layer for ingestion, KG, admin, and agent state.

The repository is the source-of-truth layer. Chroma is treated as a derived index,
while everything operationally important is persisted here first.
"""

from __future__ import annotations

from contextlib import contextmanager
from datetime import UTC, datetime, timedelta, timezone
import json
import re
from hashlib import sha256
from typing import Any
from uuid import uuid4

import psycopg
from psycopg.errors import UniqueViolation
from psycopg.rows import dict_row
from psycopg import sql
from psycopg.types.json import Jsonb
try:
    from cryptography.fernet import Fernet, InvalidToken
except ImportError:  # pragma: no cover - optional until dependency install
    Fernet = None
    InvalidToken = Exception

from src.bee_ingestion.models import (
    Chunk,
    ChunkAssetLink,
    ChunkValidation,
    DocumentPage,
    KGExtractionResult,
    PageAsset,
    ParsedBlock,
    SensorReading,
    SourceDocument,
    UserSensor,
)
from src.bee_ingestion.pipeline import is_terminal_job_status, validate_job_transition, validate_stage_run
from src.bee_ingestion.settings import settings

ALLOWED_AGENT_REVIEW_DECISIONS = {"approved", "rejected", "needs_review"}
_TRACE_MAX_STRING_CHARS = 4000
_TRACE_MAX_LIST_ITEMS = 48
_TRACE_MAX_DICT_KEYS = 64
_ADMIN_REDACTED_COLUMNS: dict[str, set[str]] = {
    "agent_answer_reviews": {"payload"},
    "agent_profiles": {"profile_token_hash"},
    "agent_query_runs": {"prompt_payload", "raw_response_payload", "final_response_payload"},
    "agent_query_sources": {"payload"},
    "auth_sessions": {"session_token_hash"},
    "auth_users": {"password_hash", "password_salt", "password_iterations"},
    "agent_runtime_secrets": {"api_key_override"},
    "agent_sessions": {"session_token_hash"},
    "document_pages": {"page_image_path"},
    "document_sources": {"raw_text", "normalized_text"},
    "page_assets": {"asset_path"},
}
_ADMIN_MUTATION_BLOCKED_COLUMNS: dict[str, set[str]] = {
    relation_name: set(columns)
    for relation_name, columns in _ADMIN_REDACTED_COLUMNS.items()
}
_SENSOR_KEY_RE = re.compile(r"^[a-z0-9][a-z0-9._:-]{1,127}$")
_METRIC_NAME_RE = re.compile(r"^[a-z0-9][a-z0-9._:-]{1,127}$")


class Repository:
    def __init__(self, dsn: str | None = None) -> None:
        self.dsn = dsn or settings.postgres_dsn
        self._ensure_agent_schema_compatibility()

    def _ensure_agent_schema_compatibility(self) -> None:
        with psycopg.connect(self.dsn) as conn:
            with conn.cursor() as cur:
                cur.execute("ALTER TABLE IF EXISTS agent_profiles ADD COLUMN IF NOT EXISTS profile_token_issued_at timestamptz")
                cur.execute("ALTER TABLE IF EXISTS agent_sessions ADD COLUMN IF NOT EXISTS auth_user_id text")
                cur.execute("ALTER TABLE IF EXISTS agent_sessions ADD COLUMN IF NOT EXISTS workspace_kind text NOT NULL DEFAULT 'general'")
                cur.execute("ALTER TABLE IF EXISTS agent_sessions ADD COLUMN IF NOT EXISTS session_token_issued_at timestamptz")
                cur.execute("ALTER TABLE IF EXISTS sensor_readings ADD COLUMN IF NOT EXISTS reading_hash text NOT NULL DEFAULT ''")
                cur.execute(
                    """
                    CREATE TABLE IF NOT EXISTS corpus_snapshots (
                      snapshot_id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
                      tenant_id text NOT NULL DEFAULT 'shared',
                      snapshot_kind text NOT NULL,
                      summary text NOT NULL DEFAULT '',
                      document_id uuid,
                      job_id uuid,
                      metrics_json jsonb NOT NULL DEFAULT '{}'::jsonb,
                      metadata_json jsonb NOT NULL DEFAULT '{}'::jsonb,
                      created_at timestamptz NOT NULL DEFAULT now()
                    )
                    """
                )
                cur.execute(
                    """
                    CREATE TABLE IF NOT EXISTS document_synopses (
                      document_id uuid PRIMARY KEY REFERENCES documents(document_id) ON DELETE CASCADE,
                      tenant_id text NOT NULL,
                      title text NOT NULL DEFAULT '',
                      synopsis_text text NOT NULL DEFAULT '',
                      accepted_chunk_count integer NOT NULL DEFAULT 0,
                      section_count integer NOT NULL DEFAULT 0,
                      source_stage text NOT NULL DEFAULT 'chunks_validated',
                      synopsis_version text NOT NULL DEFAULT 'extractive-v1',
                      metadata_json jsonb NOT NULL DEFAULT '{}'::jsonb,
                      created_at timestamptz NOT NULL DEFAULT now(),
                      updated_at timestamptz NOT NULL DEFAULT now()
                    )
                    """
                )
                cur.execute(
                    """
                    CREATE TABLE IF NOT EXISTS document_section_synopses (
                      section_id text PRIMARY KEY,
                      document_id uuid NOT NULL REFERENCES documents(document_id) ON DELETE CASCADE,
                      tenant_id text NOT NULL,
                      parent_section_id text REFERENCES document_section_synopses(section_id) ON DELETE CASCADE,
                      section_path text[] NOT NULL DEFAULT '{}',
                      section_level integer NOT NULL DEFAULT 0,
                      section_title text NOT NULL DEFAULT '',
                      page_start integer,
                      page_end integer,
                      char_start integer,
                      char_end integer,
                      first_chunk_id text REFERENCES document_chunks(chunk_id) ON DELETE SET NULL,
                      last_chunk_id text REFERENCES document_chunks(chunk_id) ON DELETE SET NULL,
                      accepted_chunk_count integer NOT NULL DEFAULT 0,
                      total_chunk_count integer NOT NULL DEFAULT 0,
                      synopsis_text text NOT NULL DEFAULT '',
                      synopsis_version text NOT NULL DEFAULT 'extractive-v1',
                      metadata_json jsonb NOT NULL DEFAULT '{}'::jsonb,
                      created_at timestamptz NOT NULL DEFAULT now(),
                      updated_at timestamptz NOT NULL DEFAULT now(),
                      UNIQUE (document_id, section_path)
                    )
                    """
                )
                cur.execute("ALTER TABLE IF EXISTS documents ADD COLUMN IF NOT EXISTS latest_corpus_snapshot_id uuid")
                cur.execute("ALTER TABLE IF EXISTS ingestion_jobs ADD COLUMN IF NOT EXISTS completed_corpus_snapshot_id uuid")
                cur.execute("ALTER TABLE IF EXISTS agent_query_runs ADD COLUMN IF NOT EXISTS corpus_snapshot_id uuid")
                cur.execute("ALTER TABLE IF EXISTS agent_query_sources ADD COLUMN IF NOT EXISTS corpus_snapshot_id uuid")
                cur.execute("ALTER TABLE IF EXISTS agent_messages ADD COLUMN IF NOT EXISTS profile_id uuid")
                cur.execute("ALTER TABLE IF EXISTS agent_messages ADD COLUMN IF NOT EXISTS auth_user_id text")
                cur.execute("ALTER TABLE IF EXISTS agent_query_runs ADD COLUMN IF NOT EXISTS profile_id uuid")
                cur.execute("ALTER TABLE IF EXISTS agent_query_runs ADD COLUMN IF NOT EXISTS auth_user_id text")
                cur.execute("ALTER TABLE IF EXISTS agent_session_memories ADD COLUMN IF NOT EXISTS profile_id uuid")
                cur.execute("ALTER TABLE IF EXISTS agent_session_memories ADD COLUMN IF NOT EXISTS auth_user_id text")
                cur.execute(
                    """
                    CREATE TABLE IF NOT EXISTS agent_query_embeddings (
                      tenant_id text NOT NULL,
                      query_hash text NOT NULL,
                      normalized_query text NOT NULL,
                      cache_identity text NOT NULL,
                      embedding_json jsonb NOT NULL,
                      embedding_dimensions integer NOT NULL DEFAULT 0,
                      cache_hits integer NOT NULL DEFAULT 0,
                      cached_at timestamptz NOT NULL DEFAULT now(),
                      created_at timestamptz NOT NULL DEFAULT now(),
                      updated_at timestamptz NOT NULL DEFAULT now(),
                      PRIMARY KEY (tenant_id, query_hash, cache_identity)
                    )
                    """
                )
                cur.execute("CREATE INDEX IF NOT EXISTS idx_agent_sessions_owner ON agent_sessions(tenant_id, auth_user_id, updated_at DESC)")
                cur.execute("CREATE INDEX IF NOT EXISTS idx_agent_sessions_owner_kind ON agent_sessions(tenant_id, auth_user_id, workspace_kind, updated_at DESC)")
                cur.execute("CREATE INDEX IF NOT EXISTS idx_agent_messages_session_created_desc ON agent_messages(session_id, created_at DESC)")
                cur.execute("CREATE INDEX IF NOT EXISTS idx_agent_messages_owner_created_desc ON agent_messages(auth_user_id, created_at DESC)")
                cur.execute("CREATE INDEX IF NOT EXISTS idx_agent_query_runs_owner_created_desc ON agent_query_runs(auth_user_id, created_at DESC)")
                cur.execute("CREATE INDEX IF NOT EXISTS idx_agent_session_memories_owner_updated_desc ON agent_session_memories(auth_user_id, updated_at DESC)")
                cur.execute(
                    """
                    UPDATE agent_messages m
                    SET profile_id = s.profile_id,
                        auth_user_id = s.auth_user_id
                    FROM agent_sessions s
                    WHERE s.session_id = m.session_id
                      AND (m.profile_id IS DISTINCT FROM s.profile_id OR m.auth_user_id IS DISTINCT FROM s.auth_user_id)
                    """
                )
                cur.execute(
                    """
                    UPDATE agent_query_runs q
                    SET profile_id = s.profile_id,
                        auth_user_id = s.auth_user_id
                    FROM agent_sessions s
                    WHERE s.session_id = q.session_id
                      AND (q.profile_id IS DISTINCT FROM s.profile_id OR q.auth_user_id IS DISTINCT FROM s.auth_user_id)
                    """
                )
                cur.execute(
                    """
                    UPDATE agent_session_memories m
                    SET profile_id = s.profile_id,
                        auth_user_id = s.auth_user_id
                    FROM agent_sessions s
                    WHERE s.session_id = m.session_id
                      AND (m.profile_id IS DISTINCT FROM s.profile_id OR m.auth_user_id IS DISTINCT FROM s.auth_user_id)
                    """
                )
                cur.execute("CREATE UNIQUE INDEX IF NOT EXISTS idx_sensor_readings_sensor_hash ON sensor_readings(sensor_id, reading_hash)")
                cur.execute(
                    """
                    CREATE INDEX IF NOT EXISTS page_assets_tenant_document_page_asset_idx
                    ON page_assets (tenant_id, document_id, page_number, asset_index)
                    """
                )
                cur.execute(
                    """
                    CREATE INDEX IF NOT EXISTS document_chunks_tenant_document_chunk_idx
                    ON document_chunks (tenant_id, document_id, chunk_index)
                    """
                )
                cur.execute(
                    """
                    CREATE INDEX IF NOT EXISTS document_chunks_status_tenant_document_idx
                    ON document_chunks (status, tenant_id, document_id, chunk_index)
                    """
                )
                cur.execute(
                    """
                    CREATE INDEX IF NOT EXISTS chunk_validations_status_chunk_idx
                    ON chunk_validations (status, chunk_id)
                    """
                )
                cur.execute(
                    """
                    CREATE INDEX IF NOT EXISTS kg_assertions_chunk_status_confidence_idx
                    ON kg_assertions (chunk_id, status, confidence DESC, created_at DESC)
                    """
                )
                cur.execute(
                    """
                    CREATE INDEX IF NOT EXISTS kg_assertions_subject_status_confidence_idx
                    ON kg_assertions (subject_entity_id, status, confidence DESC, created_at DESC)
                    """
                )
                cur.execute(
                    """
                    CREATE INDEX IF NOT EXISTS kg_assertions_object_status_confidence_idx
                    ON kg_assertions (object_entity_id, status, confidence DESC, created_at DESC)
                    """
                )
                cur.execute(
                    """
                    CREATE INDEX IF NOT EXISTS kg_assertions_document_status_confidence_idx
                    ON kg_assertions (document_id, status, confidence DESC, created_at DESC)
                    """
                )
                cur.execute(
                    """
                    CREATE INDEX IF NOT EXISTS kg_assertion_evidence_assertion_created_idx
                    ON kg_assertion_evidence (assertion_id, created_at DESC)
                    """
                )
                cur.execute("SELECT EXISTS (SELECT 1 FROM pg_extension WHERE extname = 'pg_trgm')")
                has_pg_trgm = bool(cur.fetchone()[0])
                if has_pg_trgm:
                    cur.execute(
                        """
                        CREATE INDEX IF NOT EXISTS kg_entities_canonical_name_trgm_idx
                        ON kg_entities USING gin (canonical_name gin_trgm_ops)
                        """
                    )
                    cur.execute(
                        """
                        CREATE INDEX IF NOT EXISTS kg_entities_type_trgm_idx
                        ON kg_entities USING gin (entity_type gin_trgm_ops)
                        """
                    )
                    cur.execute(
                        """
                        CREATE INDEX IF NOT EXISTS kg_assertions_object_literal_trgm_idx
                        ON kg_assertions USING gin ((COALESCE(object_literal, '')) gin_trgm_ops)
                        """
                    )
                    cur.execute(
                        """
                        CREATE INDEX IF NOT EXISTS kg_assertion_evidence_excerpt_trgm_idx
                        ON kg_assertion_evidence USING gin (excerpt gin_trgm_ops)
                        """
                    )
                    cur.execute(
                        """
                        CREATE INDEX IF NOT EXISTS document_synopses_title_trgm_idx
                        ON document_synopses USING gin ((COALESCE(title, '')) gin_trgm_ops)
                        """
                    )
                    cur.execute(
                        """
                        CREATE INDEX IF NOT EXISTS document_synopses_text_trgm_idx
                        ON document_synopses USING gin (synopsis_text gin_trgm_ops)
                        """
                    )
                    cur.execute(
                        """
                        CREATE INDEX IF NOT EXISTS document_section_synopses_title_trgm_idx
                        ON document_section_synopses USING gin ((COALESCE(section_title, '')) gin_trgm_ops)
                        """
                    )
                    cur.execute(
                        """
                        CREATE INDEX IF NOT EXISTS document_section_synopses_text_trgm_idx
                        ON document_section_synopses USING gin (synopsis_text gin_trgm_ops)
                        """
                    )
                    cur.execute(
                        """
                        CREATE INDEX IF NOT EXISTS page_assets_search_text_trgm_idx
                        ON page_assets USING gin (search_text gin_trgm_ops)
                        """
                    )
                    cur.execute(
                        """
                        CREATE INDEX IF NOT EXISTS page_assets_label_trgm_idx
                        ON page_assets USING gin ((COALESCE(metadata_json->>'label', '')) gin_trgm_ops)
                        """
                    )
                    cur.execute(
                        """
                        CREATE INDEX IF NOT EXISTS page_assets_asset_type_trgm_idx
                        ON page_assets USING gin ((COALESCE(asset_type, '')) gin_trgm_ops)
                        """
                    )
                    cur.execute(
                        """
                        CREATE INDEX IF NOT EXISTS document_chunks_text_trgm_idx
                        ON document_chunks USING gin (text gin_trgm_ops)
                        """
                    )
                    cur.execute(
                        """
                        CREATE INDEX IF NOT EXISTS document_chunks_title_trgm_idx
                        ON document_chunks USING gin ((COALESCE(metadata_json->>'title', '')) gin_trgm_ops)
                        """
                    )
                    cur.execute(
                        """
                        CREATE INDEX IF NOT EXISTS document_chunks_section_title_trgm_idx
                        ON document_chunks USING gin ((COALESCE(metadata_json->>'section_title', '')) gin_trgm_ops)
                        """
                    )
                    cur.execute(
                        """
                        CREATE INDEX IF NOT EXISTS document_chunks_section_heading_trgm_idx
                        ON document_chunks USING gin ((COALESCE(metadata_json->>'section_heading', '')) gin_trgm_ops)
                        """
                    )
                cur.execute(
                    """
                    CREATE INDEX IF NOT EXISTS document_synopses_tenant_updated_idx
                    ON document_synopses (tenant_id, updated_at DESC)
                    """
                )
                cur.execute(
                    """
                    CREATE INDEX IF NOT EXISTS document_section_synopses_document_parent_idx
                    ON document_section_synopses (document_id, parent_section_id)
                    """
                )
                cur.execute(
                    """
                    CREATE INDEX IF NOT EXISTS document_section_synopses_document_level_idx
                    ON document_section_synopses (document_id, section_level)
                    """
                )
                cur.execute(
                    """
                    CREATE INDEX IF NOT EXISTS idx_corpus_snapshots_tenant_created
                    ON corpus_snapshots(tenant_id, created_at DESC)
                    """
                )
                cur.execute(
                    """
                    CREATE INDEX IF NOT EXISTS idx_corpus_snapshots_document_created
                    ON corpus_snapshots(document_id, created_at DESC)
                    """
                )
                cur.execute(
                    """
                    CREATE INDEX IF NOT EXISTS idx_agent_query_embeddings_lookup
                    ON agent_query_embeddings(tenant_id, query_hash, cached_at DESC)
                    """
                )
            conn.commit()

    @staticmethod
    def _advisory_lock_key(*parts: str) -> int:
        digest = sha256("|".join(str(part) for part in parts).encode("utf-8")).digest()[:8]
        value = int.from_bytes(digest, byteorder="big", signed=False)
        if value >= (1 << 63):
            value -= (1 << 64)
        return value

    @contextmanager
    def advisory_lock(self, *parts: str):
        key = self._advisory_lock_key(*parts)
        conn = psycopg.connect(self.dsn)
        try:
            with conn.cursor() as cur:
                cur.execute("SELECT pg_advisory_lock(%s)", (key,))
            yield
        finally:
            try:
                with conn.cursor() as cur:
                    cur.execute("SELECT pg_advisory_unlock(%s)", (key,))
            finally:
                conn.close()

    @staticmethod
    def _sanitize_text(value: str) -> str:
        # Strip NUL bytes at the persistence edge because Postgres rejects them in text/json payloads.
        return value.replace("\x00", "") if value else ""

    @classmethod
    def _normalize_sensor_key(cls, value: str) -> str:
        normalized = cls._sanitize_text(str(value or "")).strip().lower()
        if not _SENSOR_KEY_RE.fullmatch(normalized):
            raise ValueError("sensor_key must be 2-128 chars and use lowercase letters, digits, dot, underscore, colon, or dash")
        return normalized

    @classmethod
    def _normalize_metric_name(cls, value: str) -> str:
        normalized = cls._sanitize_text(str(value or "")).strip().lower()
        if not _METRIC_NAME_RE.fullmatch(normalized):
            raise ValueError("metric_name must be 2-128 chars and use lowercase letters, digits, dot, underscore, colon, or dash")
        return normalized

    @classmethod
    def _normalize_sensor_status(cls, value: str | None) -> str:
        normalized = cls._sanitize_text(str(value or "active")).strip().lower()
        if normalized not in {"active", "disabled", "archived"}:
            raise ValueError("Unsupported sensor status")
        return normalized

    @classmethod
    def _coerce_sensor_observed_at(cls, value: Any) -> datetime:
        if isinstance(value, datetime):
            observed_at = value
        else:
            raw_value = cls._sanitize_text(str(value or "")).strip()
            if not raw_value:
                raise ValueError("observed_at is required for each reading")
            try:
                observed_at = datetime.fromisoformat(raw_value.replace("Z", "+00:00"))
            except ValueError as exc:
                raise ValueError("observed_at must be a valid ISO-8601 timestamp") from exc
        if observed_at.tzinfo is None:
            return observed_at.replace(tzinfo=timezone.utc)
        return observed_at.astimezone(timezone.utc)

    @classmethod
    def _sensor_reading_hash(
        cls,
        *,
        observed_at: datetime,
        metric_name: str,
        unit: str | None,
        numeric_value: float | None,
        text_value: str | None,
    ) -> str:
        payload = {
            "observed_at": observed_at.astimezone(timezone.utc).isoformat(),
            "metric_name": metric_name,
            "unit": unit or "",
            "numeric_value": None if numeric_value is None else round(float(numeric_value), 8),
            "text_value": text_value or "",
        }
        return sha256(json.dumps(payload, ensure_ascii=False, sort_keys=True).encode("utf-8")).hexdigest()

    @classmethod
    def _sanitize_json_value(cls, value):
        if isinstance(value, str):
            return cls._sanitize_text(value)
        if isinstance(value, list):
            return [cls._sanitize_json_value(item) for item in value]
        if isinstance(value, tuple):
            return [cls._sanitize_json_value(item) for item in value]
        if isinstance(value, dict):
            return {cls._sanitize_json_value(key): cls._sanitize_json_value(item) for key, item in value.items()}
        if value is None or isinstance(value, (int, float, bool)):
            return value
        if hasattr(value, "isoformat"):
            try:
                return value.isoformat()
            except Exception:
                return str(value)
        if not isinstance(value, (str, list, tuple, dict)):
            return str(value)
        return value

    @staticmethod
    def _runtime_secret_cipher():
        key = str(settings.runtime_secret_encryption_key or "").strip()
        if not key:
            raise RuntimeError("RUNTIME_SECRET_ENCRYPTION_KEY is required to persist runtime secrets")
        if Fernet is None:
            raise RuntimeError("cryptography is required for runtime secret encryption")
        return Fernet(key.encode("utf-8"))

    @classmethod
    def _encrypt_runtime_secret(cls, value: str) -> str:
        cleaned = cls._sanitize_text(value or "").strip()
        if not cleaned:
            return ""
        token = cls._runtime_secret_cipher().encrypt(cleaned.encode("utf-8")).decode("utf-8")
        return f"enc:v1:{token}"

    @classmethod
    def _decrypt_runtime_secret(cls, value: str | None) -> str:
        encrypted = cls._sanitize_text(value or "").strip()
        if not encrypted:
            return ""
        if encrypted.startswith("enc:v1:"):
            token = encrypted[len("enc:v1:") :]
            try:
                return cls._runtime_secret_cipher().decrypt(token.encode("utf-8")).decode("utf-8")
            except InvalidToken as exc:  # pragma: no cover - configuration/runtime failure
                raise RuntimeError("Unable to decrypt runtime secret") from exc
        return encrypted

    def find_existing_document(self, source: SourceDocument) -> str | None:
        source_path = self._sanitize_text(str(source.metadata.get("source_path") or source.metadata.get("uploaded_path") or "")).strip()
        with psycopg.connect(self.dsn, row_factory=dict_row) as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT d.document_id
                    FROM documents d
                    LEFT JOIN LATERAL (
                      SELECT metadata_json
                      FROM document_sources ds
                      WHERE ds.document_id = d.document_id
                      ORDER BY ds.created_at DESC
                      LIMIT 1
                    ) ds ON TRUE
                    WHERE d.tenant_id = %s
                      AND d.source_type = %s
                      AND d.content_hash = %s
                      AND (
                        d.filename = %s
                        OR (%s <> '' AND COALESCE(ds.metadata_json->>'source_path', ds.metadata_json->>'uploaded_path', '') = %s)
                      )
                    ORDER BY d.updated_at DESC, d.created_at DESC
                    LIMIT 1
                    """,
                    (
                        source.tenant_id,
                        source.source_type,
                        source.content_hash,
                        source.filename,
                        source_path,
                        source_path,
                    ),
                )
                row = cur.fetchone()
                return str(row["document_id"]) if row else None

    @classmethod
    def _redact_sensitive_json_value(cls, value: Any):
        if isinstance(value, dict):
            redacted: dict[str, Any] = {}
            items = list(value.items())
            for index, (key, item) in enumerate(items):
                if index >= _TRACE_MAX_DICT_KEYS:
                    redacted["_truncated_keys"] = len(items) - _TRACE_MAX_DICT_KEYS
                    break
                key_str = cls._sanitize_text(str(key))
                key_lower = key_str.lower()
                if any(
                    marker in key_lower
                    for marker in ("api_key", "authorization", "session_token", "profile_token", "x-admin-token", "x-agent-token", "password", "secret", "bearer", "token")
                ):
                    redacted[key_str] = "[redacted]" if item not in (None, "") else ""
                else:
                    redacted[key_str] = cls._redact_sensitive_json_value(item)
            return redacted
        if isinstance(value, list):
            items = [cls._redact_sensitive_json_value(item) for item in value[:_TRACE_MAX_LIST_ITEMS]]
            if len(value) > _TRACE_MAX_LIST_ITEMS:
                items.append({"_truncated_items": len(value) - _TRACE_MAX_LIST_ITEMS})
            return items
        if isinstance(value, tuple):
            items = [cls._redact_sensitive_json_value(item) for item in value[:_TRACE_MAX_LIST_ITEMS]]
            if len(value) > _TRACE_MAX_LIST_ITEMS:
                items.append({"_truncated_items": len(value) - _TRACE_MAX_LIST_ITEMS})
            return items
        sanitized = cls._sanitize_json_value(value)
        if isinstance(sanitized, str) and len(sanitized) > _TRACE_MAX_STRING_CHARS:
            return sanitized[:_TRACE_MAX_STRING_CHARS] + f"... [truncated {len(sanitized) - _TRACE_MAX_STRING_CHARS} chars]"
        return sanitized

    @classmethod
    def _redact_admin_relation_row(cls, relation_name: str, row: dict[str, Any]) -> dict[str, Any]:
        redacted_columns = _ADMIN_REDACTED_COLUMNS.get(relation_name, set())
        payload: dict[str, Any] = {}
        for key, value in row.items():
            if key in redacted_columns:
                payload[key] = "[redacted]" if value not in (None, "", {}, []) else value
            elif key.endswith("_path"):
                payload[key] = "[redacted-path]" if value not in (None, "") else value
            elif key.endswith("_payload") or key == "payload":
                payload[key] = cls._redact_sensitive_json_value(value)
            else:
                payload[key] = cls._sanitize_json_value(value)
        return payload

    def _build_allowed_patch(
        self,
        patch: dict | None,
        *,
        allowed_fields: dict[str, str],
        json_fields: set[str] | None = None,
        array_fields: set[str] | None = None,
        text_fields: set[str] | None = None,
    ) -> tuple[list[str], list[object]]:
        if not isinstance(patch, dict) or not patch:
            raise ValueError("Patch payload must be a non-empty object")
        json_fields = json_fields or set()
        array_fields = array_fields or set()
        text_fields = text_fields or set()
        assignments: list[str] = []
        params: list[object] = []
        for key, value in patch.items():
            if key not in allowed_fields:
                raise ValueError(f"Field '{key}' is not editable")
            column = allowed_fields[key]
            if key in json_fields:
                value = json.dumps(self._sanitize_json_value(value or {}))
            elif key in array_fields:
                value = [self._sanitize_text(str(item)) for item in list(value or [])]
            elif key in text_fields and value is not None:
                value = self._sanitize_text(str(value))
            assignments.append(f"{column} = %s")
            params.append(value)
        return assignments, params

    def _fetch_scalar(self, sql: str, params: tuple[object, ...] = ()) -> int:
        with psycopg.connect(self.dsn, row_factory=dict_row) as conn:
            with conn.cursor() as cur:
                cur.execute(sql, params)
                row = cur.fetchone()
                return int(row["value"]) if row else 0

    @staticmethod
    def _admin_relation_schemas(schema_name: str | None = None) -> list[str]:
        if schema_name:
            return [str(schema_name).strip()]
        schemas = ["public", str(settings.auth_postgres_schema or "auth").strip()]
        unique: list[str] = []
        for item in schemas:
            if item and item not in unique:
                unique.append(item)
        return unique

    def list_admin_relations(self, *, schema_name: str | None = None, search: str | None = None) -> list[dict]:
        clauses = ["n.nspname = ANY(%s)", "c.relkind IN ('r', 'v', 'm')"]
        params: list[object] = [self._admin_relation_schemas(schema_name)]
        if search:
            clauses.append("c.relname ILIKE %s")
            params.append(f"%{self._sanitize_text(search.strip())}%")
        where_clause = " AND ".join(clauses)
        with psycopg.connect(self.dsn, row_factory=dict_row) as conn:
            with conn.cursor() as cur:
                cur.execute(
                    f"""
                    SELECT
                      n.nspname AS schema_name,
                      c.relname AS relation_name,
                      CASE c.relkind
                        WHEN 'r' THEN 'table'
                        WHEN 'v' THEN 'view'
                        WHEN 'm' THEN 'materialized_view'
                        ELSE c.relkind::text
                      END AS relation_type,
                      COALESCE(s.n_live_tup::bigint, c.reltuples::bigint, 0) AS estimated_rows,
                      EXISTS (
                        SELECT 1
                        FROM pg_index idx
                        WHERE idx.indrelid = c.oid
                          AND idx.indisprimary
                      ) AS has_primary_key
                    FROM pg_class c
                    JOIN pg_namespace n ON n.oid = c.relnamespace
                    LEFT JOIN pg_stat_user_tables s ON s.relid = c.oid
                    WHERE {where_clause}
                    ORDER BY n.nspname, c.relname
                    """,
                    tuple(params),
                )
                return [dict(row) for row in cur.fetchall()]

    def get_admin_relation_schema(self, relation_name: str, *, schema_name: str = "public") -> dict | None:
        relation_name = self._sanitize_text(relation_name.strip())
        with psycopg.connect(self.dsn, row_factory=dict_row) as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT
                      c.relname AS relation_name,
                      CASE c.relkind
                        WHEN 'r' THEN 'table'
                        WHEN 'v' THEN 'view'
                        WHEN 'm' THEN 'materialized_view'
                        ELSE c.relkind::text
                      END AS relation_type
                    FROM pg_class c
                    JOIN pg_namespace n ON n.oid = c.relnamespace
                    WHERE n.nspname = %s
                      AND c.relname = %s
                      AND c.relkind IN ('r', 'v', 'm')
                    """,
                    (schema_name, relation_name),
                )
                relation = cur.fetchone()
                if not relation:
                    return None
                cur.execute(
                    """
                    SELECT
                      column_name,
                      data_type,
                      udt_name,
                      is_nullable,
                      column_default,
                      ordinal_position
                    FROM information_schema.columns
                    WHERE table_schema = %s
                      AND table_name = %s
                    ORDER BY ordinal_position
                    """,
                    (schema_name, relation_name),
                )
                columns = [dict(row) for row in cur.fetchall()]
                cur.execute(
                    """
                    SELECT a.attname AS column_name
                    FROM pg_index i
                    JOIN pg_class c ON c.oid = i.indrelid
                    JOIN pg_namespace n ON n.oid = c.relnamespace
                    JOIN pg_attribute a ON a.attrelid = c.oid AND a.attnum = ANY(i.indkey)
                    WHERE n.nspname = %s
                      AND c.relname = %s
                      AND i.indisprimary
                    ORDER BY array_position(i.indkey, a.attnum)
                    """,
                    (schema_name, relation_name),
                )
                primary_key = [str(row["column_name"]) for row in cur.fetchall()]
        return {
            "schema_name": schema_name,
            "relation_name": relation_name,
            "relation_type": relation["relation_type"],
            "columns": columns,
            "primary_key": primary_key,
        }

    def count_admin_relation_rows(self, relation_name: str, *, schema_name: str = "public") -> int:
        schema = self.get_admin_relation_schema(relation_name, schema_name=schema_name)
        if not schema:
            raise ValueError("Relation not found")
        query = sql.SQL("SELECT COUNT(*) AS value FROM {}.{}").format(
            sql.Identifier(schema_name),
            sql.Identifier(relation_name),
        )
        with psycopg.connect(self.dsn, row_factory=dict_row) as conn:
            with conn.cursor() as cur:
                cur.execute(query)
                row = cur.fetchone()
                return int(row["value"]) if row else 0

    def list_admin_relation_rows(
        self,
        relation_name: str,
        *,
        schema_name: str = "public",
        limit: int = 50,
        offset: int = 0,
    ) -> dict:
        schema = self.get_admin_relation_schema(relation_name, schema_name=schema_name)
        if not schema:
            raise ValueError("Relation not found")
        columns = [str(item["column_name"]) for item in schema["columns"]]
        if not columns:
            return {
                "schema_name": schema_name,
                "relation_name": relation_name,
                "relation_type": schema["relation_type"],
                "columns": [],
                "primary_key": schema["primary_key"],
                "rows": [],
                "total": 0,
                "limit": limit,
                "offset": offset,
            }
        order_columns = [item for item in schema["primary_key"] if item in columns] or [columns[0]]
        count_query = sql.SQL("SELECT COUNT(*) AS value FROM {}.{}").format(
            sql.Identifier(schema_name),
            sql.Identifier(relation_name),
        )
        select_query = sql.SQL("SELECT * FROM {}.{} ORDER BY {} LIMIT %s OFFSET %s").format(
            sql.Identifier(schema_name),
            sql.Identifier(relation_name),
            sql.SQL(", ").join(sql.Identifier(item) for item in order_columns),
        )
        with psycopg.connect(self.dsn, row_factory=dict_row) as conn:
            with conn.cursor() as cur:
                cur.execute(count_query)
                count_row = cur.fetchone()
                total = int(count_row["value"]) if count_row else 0
                cur.execute(select_query, (limit, offset))
                rows = [self._redact_admin_relation_row(relation_name, dict(row)) for row in cur.fetchall()]
        return {
            "schema_name": schema_name,
            "relation_name": relation_name,
            "relation_type": schema["relation_type"],
            "columns": schema["columns"],
            "primary_key": schema["primary_key"],
            "redacted_columns": sorted(_ADMIN_REDACTED_COLUMNS.get(relation_name, set())),
            "rows": rows,
            "total": total,
            "limit": limit,
            "offset": offset,
            "order_by": order_columns,
        }

    def _require_admin_table_schema(self, relation_name: str, *, schema_name: str = "public") -> dict:
        relation_schema = self.get_admin_relation_schema(relation_name, schema_name=schema_name)
        if not relation_schema:
            raise ValueError("Relation not found")
        if str(relation_schema.get("relation_type") or "") != "table":
            raise ValueError("Only base tables are writable through the relation editor")
        return relation_schema

    @staticmethod
    def _admin_column_map(relation_schema: dict) -> dict[str, dict[str, Any]]:
        return {str(column["column_name"]): dict(column) for column in relation_schema.get("columns") or []}

    def _coerce_admin_relation_value(self, relation_name: str, column: dict[str, Any], value: Any):
        column_name = str(column.get("column_name") or "")
        if column_name in _ADMIN_MUTATION_BLOCKED_COLUMNS.get(relation_name, set()):
            raise ValueError(f"Field '{column_name}' must be managed through a dedicated admin surface")
        if value is None:
            return None
        data_type = str(column.get("data_type") or "").lower()
        if data_type in {"json", "jsonb"}:
            return Jsonb(self._sanitize_json_value(value))
        if data_type == "array":
            if not isinstance(value, list):
                raise ValueError(f"Field '{column_name}' expects an array value")
            return [self._sanitize_json_value(item) for item in value]
        if isinstance(value, str):
            return self._sanitize_text(value)
        if isinstance(value, dict):
            return Jsonb(self._sanitize_json_value(value))
        if isinstance(value, list):
            return [self._sanitize_json_value(item) for item in value]
        return value

    def _normalize_admin_relation_values(self, relation_name: str, relation_schema: dict, values: dict[str, Any]) -> list[tuple[str, Any]]:
        if not isinstance(values, dict) or not values:
            raise ValueError("Row values must be a non-empty object")
        column_map = self._admin_column_map(relation_schema)
        normalized_items: list[tuple[str, Any]] = []
        for key, value in values.items():
            column_name = str(key or "").strip()
            if column_name not in column_map:
                raise ValueError(f"Unknown column '{column_name}' for relation '{relation_name}'")
            normalized_items.append((column_name, self._coerce_admin_relation_value(relation_name, column_map[column_name], value)))
        return normalized_items

    def _build_admin_relation_key(self, relation_name: str, relation_schema: dict, key: dict[str, Any]) -> tuple[list[sql.Composed], list[Any]]:
        primary_key = [str(item) for item in relation_schema.get("primary_key") or []]
        if not primary_key:
            raise ValueError("The selected table has no primary key and cannot be mutated through the table editor")
        if not isinstance(key, dict) or not key:
            raise ValueError("Row key must be an object with the table primary key values")
        column_map = self._admin_column_map(relation_schema)
        clauses: list[sql.Composed] = []
        params: list[Any] = []
        for column_name in primary_key:
            if column_name not in key:
                raise ValueError(f"Missing primary key column '{column_name}'")
            clauses.append(sql.SQL("{} = %s").format(sql.Identifier(column_name)))
            params.append(self._coerce_admin_relation_value(relation_name, column_map[column_name], key[column_name]))
        return clauses, params

    def _fetch_admin_relation_row_by_key(self, relation_name: str, relation_schema: dict, key: dict[str, Any], *, schema_name: str = "public") -> dict[str, Any] | None:
        clauses, params = self._build_admin_relation_key(relation_name, relation_schema, key)
        query = sql.SQL("SELECT * FROM {}.{} WHERE {}").format(
            sql.Identifier(schema_name),
            sql.Identifier(relation_name),
            sql.SQL(" AND ").join(clauses),
        )
        with psycopg.connect(self.dsn, row_factory=dict_row) as conn:
            with conn.cursor() as cur:
                cur.execute(query, tuple(params))
                row = cur.fetchone()
        return self._redact_admin_relation_row(relation_name, dict(row)) if row else None

    def insert_admin_relation_row(self, relation_name: str, values: dict[str, Any], *, schema_name: str = "public") -> dict[str, Any]:
        relation_schema = self._require_admin_table_schema(relation_name, schema_name=schema_name)
        normalized_items = self._normalize_admin_relation_values(relation_name, relation_schema, values)
        columns = [name for name, _ in normalized_items]
        params = [value for _, value in normalized_items]
        query = sql.SQL("INSERT INTO {}.{} ({}) VALUES ({}) RETURNING *").format(
            sql.Identifier(schema_name),
            sql.Identifier(relation_name),
            sql.SQL(", ").join(sql.Identifier(name) for name in columns),
            sql.SQL(", ").join(sql.SQL("%s") for _ in columns),
        )
        with psycopg.connect(self.dsn, row_factory=dict_row) as conn:
            with conn.cursor() as cur:
                cur.execute(query, tuple(params))
                row = cur.fetchone()
                conn.commit()
        if row is None:
            raise ValueError("Row insert failed")
        return self._redact_admin_relation_row(relation_name, dict(row))

    def update_admin_relation_row(
        self,
        relation_name: str,
        key: dict[str, Any],
        values: dict[str, Any],
        *,
        schema_name: str = "public",
    ) -> dict[str, Any] | None:
        relation_schema = self._require_admin_table_schema(relation_name, schema_name=schema_name)
        primary_key = {str(item) for item in relation_schema.get("primary_key") or []}
        normalized_items = self._normalize_admin_relation_values(relation_name, relation_schema, values)
        if any(column_name in primary_key for column_name, _ in normalized_items):
            raise ValueError("Primary key columns cannot be edited through the table editor")
        set_clauses = [sql.SQL("{} = %s").format(sql.Identifier(column_name)) for column_name, _ in normalized_items]
        set_params = [value for _, value in normalized_items]
        where_clauses, where_params = self._build_admin_relation_key(relation_name, relation_schema, key)
        query = sql.SQL("UPDATE {}.{} SET {} WHERE {} RETURNING *").format(
            sql.Identifier(schema_name),
            sql.Identifier(relation_name),
            sql.SQL(", ").join(set_clauses),
            sql.SQL(" AND ").join(where_clauses),
        )
        with psycopg.connect(self.dsn, row_factory=dict_row) as conn:
            with conn.cursor() as cur:
                cur.execute(query, tuple(set_params + where_params))
                row = cur.fetchone()
                conn.commit()
        return self._redact_admin_relation_row(relation_name, dict(row)) if row else None

    def delete_admin_relation_row(self, relation_name: str, key: dict[str, Any], *, schema_name: str = "public") -> int:
        relation_schema = self._require_admin_table_schema(relation_name, schema_name=schema_name)
        where_clauses, where_params = self._build_admin_relation_key(relation_name, relation_schema, key)
        query = sql.SQL("DELETE FROM {}.{} WHERE {}").format(
            sql.Identifier(schema_name),
            sql.Identifier(relation_name),
            sql.SQL(" AND ").join(where_clauses),
        )
        with psycopg.connect(self.dsn) as conn:
            with conn.cursor() as cur:
                cur.execute(query, tuple(where_params))
                deleted = int(cur.rowcount or 0)
                conn.commit()
        return deleted

    def execute_admin_sql(self, statement: str, *, row_limit: int = 250) -> dict[str, Any]:
        cleaned = self._sanitize_text(str(statement or "")).strip()
        if not cleaned:
            raise ValueError("SQL statement is required")
        normalized = cleaned.rstrip(";").strip()
        if not normalized:
            raise ValueError("SQL statement is required")
        if ";" in normalized:
            raise ValueError("Only one SQL statement can be executed at a time")
        statement_type = normalized.split(None, 1)[0].lower()
        with psycopg.connect(self.dsn, row_factory=dict_row) as conn:
            with conn.cursor() as cur:
                try:
                    cur.execute(normalized)
                except Exception as exc:
                    raise ValueError(str(exc)) from exc
                if cur.description:
                    rows = [self._redact_sensitive_json_value(dict(row)) for row in cur.fetchmany(row_limit + 1)]
                    columns = [str(column.name) for column in cur.description]
                    truncated = len(rows) > row_limit
                    return {
                        "statement_type": statement_type,
                        "columns": columns,
                        "rows": rows[:row_limit],
                        "row_count": len(rows[:row_limit]),
                        "truncated": truncated,
                    }
                affected_rows = int(cur.rowcount or 0) if cur.rowcount != -1 else 0
                conn.commit()
                return {
                    "statement_type": statement_type,
                    "columns": [],
                    "rows": [],
                    "row_count": affected_rows,
                    "truncated": False,
                }

    @staticmethod
    def _token_hash(value: str) -> str:
        return sha256((value or "").encode("utf-8")).hexdigest()

    @staticmethod
    def _query_pattern_keywords(value: str) -> list[str]:
        tokens = [item for item in re.split(r"[^a-z0-9]+", (value or "").lower()) if len(item) >= 3]
        seen: set[str] = set()
        keywords: list[str] = []
        for token in tokens:
            if token in seen:
                continue
            seen.add(token)
            keywords.append(token)
            if len(keywords) >= 12:
                break
        return keywords

    @classmethod
    def _build_query_pattern(cls, value: str) -> tuple[str, list[str]]:
        keywords = cls._query_pattern_keywords(value)
        if not keywords:
            return ("empty-query", [])
        return ("|".join(keywords), keywords)

    @classmethod
    def build_query_pattern(cls, value: str) -> tuple[str, list[str]]:
        return cls._build_query_pattern(value)

    @staticmethod
    def _query_hash(value: str) -> str:
        return sha256((value or "").strip().lower().encode("utf-8")).hexdigest()

    def register_document(self, source: SourceDocument) -> tuple[str, str]:
        document_id = str(uuid4())
        source_id = str(uuid4())
        # A document row stores stable document identity, while document_sources keeps
        # the raw/normalized source payload and extraction metadata for replay.
        with psycopg.connect(self.dsn) as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO documents (
                      document_id, tenant_id, source_type, filename, content_hash,
                      parser_version, ocr_engine, ocr_model, document_class, status
                    ) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,'registered')
                    """,
                    (
                        document_id,
                        source.tenant_id,
                        source.source_type,
                        source.filename,
                        source.content_hash,
                        source.parser_version,
                        source.ocr_engine,
                        source.ocr_model,
                        source.document_class,
                    ),
                )
                cur.execute(
                    """
                    INSERT INTO document_sources (source_id, document_id, raw_text, normalized_text, extraction_metrics_json, metadata_json)
                    VALUES (%s, %s, %s, %s, %s, %s)
                    """,
                    (
                        source_id,
                        document_id,
                        self._sanitize_text(source.raw_text),
                        self._sanitize_text(source.normalized_text or source.raw_text),
                        json.dumps(self._sanitize_json_value(source.extraction_metrics or {})),
                        json.dumps(self._sanitize_json_value(source.metadata or {})),
                    ),
                )
            conn.commit()
        return document_id, source_id

    def create_job(
        self,
        document_id: str,
        extractor_version: str,
        normalizer_version: str,
        parser_version: str,
        chunker_version: str,
        validator_version: str,
        embedding_version: str,
        kg_version: str,
    ) -> str:
        job_id = str(uuid4())
        # Jobs snapshot the pipeline versions that produced one ingestion run so later
        # replays can be reasoned about without guessing which code path was used.
        insert_params = (
            job_id,
            document_id,
            extractor_version,
            normalizer_version,
            parser_version,
            chunker_version,
            validator_version,
            embedding_version,
            kg_version,
        )
        try:
            with psycopg.connect(self.dsn) as conn:
                with conn.cursor() as cur:
                    cur.execute(
                        """
                        INSERT INTO ingestion_jobs (
                          job_id, document_id, status, extractor_version, normalizer_version,
                          parser_version, chunker_version, validator_version, embedding_version, kg_version
                        ) VALUES (%s,%s,'registered',%s,%s,%s,%s,%s,%s,%s)
                        """,
                        insert_params,
                    )
                conn.commit()
        except UniqueViolation as exc:
            if self._finalize_stale_active_job(document_id):
                job_id = str(uuid4())
                insert_params = (
                    job_id,
                    document_id,
                    extractor_version,
                    normalizer_version,
                    parser_version,
                    chunker_version,
                    validator_version,
                    embedding_version,
                    kg_version,
                )
                with psycopg.connect(self.dsn) as conn:
                    with conn.cursor() as cur:
                        cur.execute(
                            """
                            INSERT INTO ingestion_jobs (
                              job_id, document_id, status, extractor_version, normalizer_version,
                              parser_version, chunker_version, validator_version, embedding_version, kg_version
                            ) VALUES (%s,%s,'registered',%s,%s,%s,%s,%s,%s,%s)
                            """,
                            insert_params,
                        )
                    conn.commit()
            else:
                raise ValueError("Active ingestion job already exists for this document") from exc
        return job_id

    def create_corpus_snapshot(
        self,
        tenant_id: str,
        snapshot_kind: str,
        *,
        document_id: str | None = None,
        job_id: str | None = None,
        summary: str | None = None,
        metrics: dict | None = None,
        metadata: dict | None = None,
    ) -> str:
        snapshot_id = str(uuid4())
        with psycopg.connect(self.dsn) as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO corpus_snapshots (
                      snapshot_id,
                      tenant_id,
                      snapshot_kind,
                      summary,
                      document_id,
                      job_id,
                      metrics_json,
                      metadata_json
                    )
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                    """,
                    (
                        snapshot_id,
                        tenant_id,
                        self._sanitize_text(snapshot_kind or "unknown"),
                        self._sanitize_text(summary or ""),
                        document_id,
                        job_id,
                        json.dumps(self._sanitize_json_value(metrics or {})),
                        json.dumps(self._sanitize_json_value(metadata or {})),
                    ),
                )
                if document_id:
                    cur.execute(
                        """
                        UPDATE documents
                        SET latest_corpus_snapshot_id = %s,
                            updated_at = now()
                        WHERE document_id = %s
                        """,
                        (snapshot_id, document_id),
                    )
                if job_id:
                    cur.execute(
                        """
                        UPDATE ingestion_jobs
                        SET completed_corpus_snapshot_id = %s,
                            updated_at = now()
                        WHERE job_id = %s
                        """,
                        (snapshot_id, job_id),
                    )
            conn.commit()
        return snapshot_id

    def get_latest_corpus_snapshot(self, tenant_id: str = "shared") -> dict | None:
        with psycopg.connect(self.dsn, row_factory=dict_row) as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT
                      snapshot_id,
                      tenant_id,
                      snapshot_kind,
                      summary,
                      document_id,
                      job_id,
                      metrics_json,
                      metadata_json,
                      created_at
                    FROM corpus_snapshots
                    WHERE tenant_id = %s
                    ORDER BY created_at DESC
                    LIMIT 1
                    """,
                    (tenant_id,),
                )
                row = cur.fetchone()
                return dict(row) if row is not None else None

    def get_latest_corpus_snapshot_id(self, tenant_id: str = "shared") -> str | None:
        row = self.get_latest_corpus_snapshot(tenant_id)
        if row is None:
            return None
        snapshot_id = str(row.get("snapshot_id") or "").strip()
        return snapshot_id or None

    def _finalize_stale_active_job(self, document_id: str) -> bool:
        with psycopg.connect(self.dsn) as conn:
            with conn.cursor(row_factory=dict_row) as cur:
                cur.execute(
                    """
                    SELECT job_id, status, claimed_by, lease_expires_at, updated_at
                    FROM ingestion_jobs
                    WHERE document_id = %s
                      AND status NOT IN ('completed', 'review', 'failed', 'quarantined')
                    ORDER BY updated_at DESC
                    LIMIT 1
                    FOR UPDATE
                    """,
                    (document_id,),
                )
                row = cur.fetchone()
                if row is None:
                    conn.rollback()
                    return False
                status = str(row.get("status") or "").strip().lower()
                lease_expires_at = row.get("lease_expires_at")
                claimed_by = row.get("claimed_by")
                updated_at = row.get("updated_at")
                cur.execute("SELECT now()")
                current_time = cur.fetchone()["now"]
                has_live_claim = bool(claimed_by and lease_expires_at is not None and lease_expires_at > current_time)
                recently_updated = bool(updated_at and (current_time - updated_at).total_seconds() < settings.job_lease_seconds)
                if has_live_claim or (not claimed_by and recently_updated):
                    conn.rollback()
                    return False
                cur.execute(
                    """
                    UPDATE ingestion_jobs
                    SET status = 'failed',
                        claimed_by = NULL,
                        claimed_at = NULL,
                        lease_expires_at = NULL,
                        updated_at = now()
                    WHERE job_id = %s
                    """,
                    (row["job_id"],),
                )
                cur.execute(
                    """
                    UPDATE documents
                    SET status = %s,
                        updated_at = now()
                    WHERE document_id = %s
                    """,
                    ('failed' if status != 'indexed' else 'completed', document_id),
                )
            conn.commit()
        return True

    def claim_job(self, job_id: str, worker_id: str, lease_seconds: int, preserve_status: bool = False) -> bool:
        # Claims are row-level leases. Only one worker may actively process a job until
        # the lease is released or expires.
        with psycopg.connect(self.dsn) as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT claimed_by, lease_expires_at
                    FROM ingestion_jobs
                    WHERE job_id = %s
                    FOR UPDATE
                    """,
                    (job_id,),
                )
                row = cur.fetchone()
                if row is None:
                    raise ValueError("Job not found")

                claimed_by, lease_expires_at = row
                if claimed_by and claimed_by != worker_id and lease_expires_at is not None:
                    cur.execute("SELECT now()")
                    current_time = cur.fetchone()[0]
                    if lease_expires_at > current_time:
                        conn.rollback()
                        return False

                if preserve_status:
                    cur.execute(
                        """
                        UPDATE ingestion_jobs
                        SET claimed_by = %s,
                            claimed_at = now(),
                            lease_expires_at = now() + (%s * interval '1 second'),
                            updated_at = now()
                        WHERE job_id = %s
                        """,
                        (worker_id, lease_seconds, job_id),
                    )
                else:
                    cur.execute(
                        """
                        UPDATE ingestion_jobs
                        SET status = 'processing',
                            claimed_by = %s,
                            claimed_at = now(),
                            lease_expires_at = now() + (%s * interval '1 second'),
                            updated_at = now()
                        WHERE job_id = %s
                        """,
                        (worker_id, lease_seconds, job_id),
                    )
                    cur.execute(
                        """
                        UPDATE documents
                        SET status = 'processing',
                            updated_at = now()
                        WHERE document_id = (SELECT document_id FROM ingestion_jobs WHERE job_id = %s)
                        """,
                        (job_id,),
                    )
            conn.commit()
        return True

    def release_job(self, job_id: str, worker_id: str) -> None:
        with psycopg.connect(self.dsn) as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    UPDATE ingestion_jobs
                    SET claimed_by = NULL,
                        claimed_at = NULL,
                        lease_expires_at = NULL,
                        updated_at = now()
                    WHERE job_id = %s AND claimed_by = %s
                    """,
                    (job_id, worker_id),
                )
            conn.commit()

    def renew_job_lease(self, job_id: str, worker_id: str, lease_seconds: int) -> bool:
        with psycopg.connect(self.dsn) as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    UPDATE ingestion_jobs
                    SET lease_expires_at = now() + (%s * interval '1 second'),
                        updated_at = now()
                    WHERE job_id = %s
                      AND claimed_by = %s
                      AND status NOT IN ('completed', 'review', 'failed', 'quarantined')
                    RETURNING job_id
                    """,
                    (lease_seconds, job_id, worker_id),
                )
                row = cur.fetchone()
            conn.commit()
        return row is not None

    def get_job(self, job_id: str) -> dict | None:
        with psycopg.connect(self.dsn, row_factory=dict_row) as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT * FROM ingestion_jobs WHERE job_id = %s", (job_id,))
                row = cur.fetchone()
                return dict(row) if row else None

    def get_latest_job_for_document(self, document_id: str) -> dict | None:
        with psycopg.connect(self.dsn, row_factory=dict_row) as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT *
                    FROM ingestion_jobs
                    WHERE document_id = %s
                    ORDER BY created_at DESC
                    LIMIT 1
                    """,
                    (document_id,),
                )
                row = cur.fetchone()
                return dict(row) if row else None

    def get_running_stage_run(self, job_id: str) -> dict | None:
        with psycopg.connect(self.dsn, row_factory=dict_row) as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT
                      stage_run_id,
                      job_id,
                      document_id,
                      stage_name,
                      status,
                      attempt,
                      worker_version,
                      input_version,
                      metrics_json,
                      error_message,
                      started_at,
                      finished_at
                    FROM ingestion_stage_runs
                    WHERE job_id = %s
                      AND status = 'running'
                    ORDER BY started_at DESC
                    LIMIT 1
                    """,
                    (job_id,),
                )
                row = cur.fetchone()
                return dict(row) if row else None

    def delete_document(self, document_id: str) -> int:
        with psycopg.connect(self.dsn) as conn:
            with conn.cursor() as cur:
                cur.execute("DELETE FROM documents WHERE document_id = %s", (document_id,))
                deleted = cur.rowcount
            conn.commit()
        return deleted

    def reset_pipeline_data(self) -> None:
        with psycopg.connect(self.dsn) as conn:
            with conn.cursor() as cur:
                cur.execute("TRUNCATE TABLE documents, kg_entities RESTART IDENTITY CASCADE")
            conn.commit()

    def record_stage(
        self,
        job_id: str,
        document_id: str,
        stage_name: str,
        job_status: str,
        stage_outcome: str,
        metrics: dict | None = None,
        error_message: str | None = None,
        worker_version: str | None = None,
        input_version: str | None = None,
        started_at: datetime | None = None,
        finished_at: datetime | None = None,
    ) -> None:
        validate_stage_run(stage_name, stage_outcome)
        started_at = started_at or datetime.now(UTC)
        finished_at = finished_at or started_at
        # Stage writes update both the append-only run history and the mutable document
        # / job status so the admin console can show current state plus full history.
        with psycopg.connect(self.dsn) as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT status
                    FROM ingestion_jobs
                    WHERE job_id = %s
                    FOR UPDATE
                    """,
                    (job_id,),
                )
                row = cur.fetchone()
                if row is None:
                    raise ValueError("Job not found")

                current_status = row[0]
                validate_job_transition(current_status, job_status)

                cur.execute(
                    """
                    SELECT COALESCE(MAX(attempt), 0) + 1
                    FROM ingestion_stage_runs
                    WHERE job_id = %s AND stage_name = %s
                    """,
                    (job_id, stage_name),
                )
                attempt = cur.fetchone()[0]

                cur.execute(
                    """
                    INSERT INTO ingestion_stage_runs (
                      job_id, document_id, stage_name, status, attempt, worker_version, input_version,
                      metrics_json, error_message, started_at, finished_at
                    ) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
                    """,
                    (
                        job_id,
                        document_id,
                        stage_name,
                        stage_outcome,
                        attempt,
                        worker_version,
                        input_version,
                        json.dumps(self._sanitize_json_value(metrics or {})),
                        error_message,
                        started_at,
                        finished_at,
                    ),
                )
                cur.execute(
                    """
                    UPDATE ingestion_jobs
                    SET status = %s,
                        updated_at = now()
                    WHERE job_id = %s
                    """,
                    (job_status, job_id),
                )
                cur.execute(
                    """
                    UPDATE documents
                    SET status = %s,
                        updated_at = now()
                    WHERE document_id = %s
                    """,
                    (job_status, document_id),
                )
                if is_terminal_job_status(job_status):
                    cur.execute(
                        """
                        UPDATE ingestion_jobs
                        SET claimed_by = NULL,
                            claimed_at = NULL,
                            lease_expires_at = NULL,
                            updated_at = now()
                        WHERE job_id = %s
                        """,
                        (job_id,),
                    )
            conn.commit()

    def start_stage_run(
        self,
        job_id: str,
        document_id: str,
        stage_name: str,
        job_status: str,
        metrics: dict | None = None,
        worker_version: str | None = None,
        input_version: str | None = None,
        started_at: datetime | None = None,
    ) -> str:
        validate_stage_run(stage_name, "running")
        started_at = started_at or datetime.now(UTC)
        with psycopg.connect(self.dsn, row_factory=dict_row) as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT status
                    FROM ingestion_jobs
                    WHERE job_id = %s
                    FOR UPDATE
                    """,
                    (job_id,),
                )
                row = cur.fetchone()
                if row is None:
                    raise ValueError("Job not found")
                current_status = row["status"]
                validate_job_transition(current_status, job_status)

                cur.execute(
                    """
                    SELECT COALESCE(MAX(attempt), 0) + 1 AS attempt
                    FROM ingestion_stage_runs
                    WHERE job_id = %s AND stage_name = %s
                    """,
                    (job_id, stage_name),
                )
                attempt = int(cur.fetchone()["attempt"])

                cur.execute(
                    """
                    INSERT INTO ingestion_stage_runs (
                      job_id, document_id, stage_name, status, attempt, worker_version, input_version,
                      metrics_json, error_message, started_at, finished_at
                    ) VALUES (%s,%s,%s,'running',%s,%s,%s,%s,%s,%s,NULL)
                    RETURNING stage_run_id
                    """,
                    (
                        job_id,
                        document_id,
                        stage_name,
                        attempt,
                        worker_version,
                        input_version,
                        json.dumps(self._sanitize_json_value(metrics or {})),
                        None,
                        started_at,
                    ),
                )
                stage_run_id = str(cur.fetchone()["stage_run_id"])

                cur.execute(
                    """
                    UPDATE ingestion_jobs
                    SET status = %s,
                        updated_at = now()
                    WHERE job_id = %s
                    """,
                    (job_status, job_id),
                )
                cur.execute(
                    """
                    UPDATE documents
                    SET status = %s,
                        updated_at = now()
                    WHERE document_id = %s
                    """,
                    (job_status, document_id),
                )
            conn.commit()
        return stage_run_id

    def finish_stage_run(
        self,
        stage_run_id: str,
        job_id: str,
        document_id: str,
        stage_outcome: str,
        *,
        job_status: str | None = None,
        metrics: dict | None = None,
        error_message: str | None = None,
        finished_at: datetime | None = None,
    ) -> None:
        finished_at = finished_at or datetime.now(UTC)
        with psycopg.connect(self.dsn, row_factory=dict_row) as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT stage_name
                    FROM ingestion_stage_runs
                    WHERE stage_run_id = %s
                    FOR UPDATE
                    """,
                    (stage_run_id,),
                )
                stage_row = cur.fetchone()
                if stage_row is None:
                    raise ValueError("Stage run not found")
                stage_name = str(stage_row["stage_name"])
                validate_stage_run(stage_name, stage_outcome)

                cur.execute(
                    """
                    SELECT status
                    FROM ingestion_jobs
                    WHERE job_id = %s
                    FOR UPDATE
                    """,
                    (job_id,),
                )
                job_row = cur.fetchone()
                if job_row is None:
                    raise ValueError("Job not found")
                current_status = str(job_row["status"])
                if job_status and job_status != current_status:
                    validate_job_transition(current_status, job_status)

                cur.execute(
                    """
                    UPDATE ingestion_stage_runs
                    SET status = %s,
                        metrics_json = %s,
                        error_message = %s,
                        finished_at = %s
                    WHERE stage_run_id = %s
                    """,
                    (
                        stage_outcome,
                        json.dumps(self._sanitize_json_value(metrics or {})),
                        error_message,
                        finished_at,
                        stage_run_id,
                    ),
                )

                if job_status:
                    cur.execute(
                        """
                        UPDATE ingestion_jobs
                        SET status = %s,
                            updated_at = now()
                        WHERE job_id = %s
                        """,
                        (job_status, job_id),
                    )
                    cur.execute(
                        """
                        UPDATE documents
                        SET status = %s,
                            updated_at = now()
                        WHERE document_id = %s
                        """,
                        (job_status, document_id),
                    )
                    if is_terminal_job_status(job_status):
                        cur.execute(
                            """
                            UPDATE ingestion_jobs
                            SET claimed_by = NULL,
                                claimed_at = NULL,
                                lease_expires_at = NULL,
                                updated_at = now()
                            WHERE job_id = %s
                            """,
                            (job_id,),
                        )
            conn.commit()

    @staticmethod
    def _kg_payload(
        result: KGExtractionResult,
        errors: list[str],
        raw_payload: dict | None = None,
        provider: str | None = None,
        model: str | None = None,
        prompt_version: str | None = None,
    ) -> dict:
        return {
            "provider": provider,
            "model": model,
            "prompt_version": prompt_version,
            "result": {
                "source_id": result.source_id,
                "segment_id": result.segment_id,
                "mentions": result.mentions,
                "candidate_entities": result.candidate_entities,
                "candidate_relations": result.candidate_relations,
                "evidence": result.evidence,
            },
            "errors": errors,
            "raw_payload": raw_payload or {},
        }

    def _write_kg_result(
        self,
        cur,
        *,
        document_id: str,
        chunk_id: str,
        result: KGExtractionResult,
        status: str,
        errors: list[str],
        raw_payload: dict | None = None,
        provider: str | None = None,
        model: str | None = None,
        prompt_version: str | None = None,
    ) -> None:
        payload = self._sanitize_json_value(
            self._kg_payload(
                result=result,
                errors=errors,
                raw_payload=raw_payload,
                provider=provider,
                model=model,
                prompt_version=prompt_version,
            )
        )
        cur.execute("DELETE FROM kg_raw_extractions WHERE chunk_id = %s", (chunk_id,))
        cur.execute(
            """
            INSERT INTO kg_raw_extractions (chunk_id, document_id, payload, status)
            VALUES (%s, %s, %s, %s)
            """,
            (
                chunk_id,
                document_id,
                json.dumps(payload),
                status,
            ),
        )

        cur.execute("DELETE FROM kg_assertions WHERE chunk_id = %s", (chunk_id,))

        if status != "validated":
            return

        for entity in result.candidate_entities:
            sanitized_type = self._sanitize_text(entity["proposed_type"])
            sanitized_name = self._sanitize_text(entity["canonical_name"])
            canonical_key = self._sanitize_text(entity.get("canonical_key") or "").strip()
            if not canonical_key:
                canonical_key = f"{sanitized_type.lower()}_{sanitized_name.lower().replace(' ', '_')}"
            entity_id = canonical_key
            cur.execute(
                """
                INSERT INTO kg_entities (entity_id, canonical_name, entity_type, source)
                VALUES (%s, %s, %s, %s)
                ON CONFLICT (entity_id) DO UPDATE SET
                  canonical_name = EXCLUDED.canonical_name,
                  entity_type = EXCLUDED.entity_type,
                  updated_at = now()
                """,
                (
                    entity_id,
                    sanitized_name,
                    sanitized_type,
                    document_id,
                ),
            )

        entity_lookup = {
            entity["candidate_id"]: (
                self._sanitize_text(entity.get("canonical_key") or "")
                or f"{self._sanitize_text(entity['proposed_type']).lower()}_{self._sanitize_text(entity['canonical_name']).lower().replace(' ', '_')}"
            )
            for entity in result.candidate_entities
        }

        for relation in result.candidate_relations:
            assertion_id = f"{chunk_id}:{relation['relation_id']}"
            object_candidate_id = relation.get("object_candidate_id")
            object_entity_id = entity_lookup.get(object_candidate_id) if object_candidate_id else None
            cur.execute(
                """
                INSERT INTO kg_assertions (
                  assertion_id, document_id, chunk_id, subject_entity_id, predicate,
                  object_entity_id, object_literal, confidence, qualifiers, status
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, 'accepted')
                ON CONFLICT (assertion_id) DO UPDATE SET
                  subject_entity_id = EXCLUDED.subject_entity_id,
                  predicate = EXCLUDED.predicate,
                  object_entity_id = EXCLUDED.object_entity_id,
                  object_literal = EXCLUDED.object_literal,
                  confidence = EXCLUDED.confidence,
                  qualifiers = EXCLUDED.qualifiers,
                  status = EXCLUDED.status
                """,
                (
                    assertion_id,
                    document_id,
                    chunk_id,
                    entity_lookup[relation["subject_candidate_id"]],
                    self._sanitize_text(relation["predicate_text"]),
                    object_entity_id,
                    self._sanitize_text(relation.get("object_literal") or "") or None,
                    relation["confidence"],
                    json.dumps(self._sanitize_json_value(relation["qualifiers"])),
                ),
            )
            for evidence in result.evidence:
                if relation["relation_id"] in evidence["supports"]:
                    evidence_id = f"{assertion_id}:{evidence['evidence_id']}"
                    cur.execute(
                        """
                        INSERT INTO kg_assertion_evidence (evidence_id, assertion_id, excerpt, start_offset, end_offset)
                        VALUES (%s, %s, %s, %s, %s)
                        ON CONFLICT (evidence_id) DO UPDATE SET
                          excerpt = EXCLUDED.excerpt,
                          start_offset = EXCLUDED.start_offset,
                          end_offset = EXCLUDED.end_offset
                        """,
                        (
                            evidence_id,
                            assertion_id,
                            self._sanitize_text(evidence["excerpt"]),
                            evidence.get("start"),
                            evidence.get("end"),
                        ),
                    )

    def save_blocks(self, blocks: list[ParsedBlock]) -> None:
        with psycopg.connect(self.dsn) as conn:
            with conn.cursor() as cur:
                for block in blocks:
                    cur.execute(
                        """
                        INSERT INTO parsed_blocks (
                          block_id, document_id, page, section_path, block_type, char_start, char_end, text
                        ) VALUES (%s,%s,%s,%s,%s,%s,%s,%s)
                        ON CONFLICT (block_id) DO UPDATE SET
                          section_path=EXCLUDED.section_path,
                          block_type=EXCLUDED.block_type,
                          char_start=EXCLUDED.char_start,
                          char_end=EXCLUDED.char_end,
                          text=EXCLUDED.text
                        """,
                        (
                            block.block_id,
                            block.document_id,
                            block.page,
                            block.section_path,
                            block.block_type,
                            block.char_start,
                            block.char_end,
                            self._sanitize_text(block.text),
                        ),
                    )
            conn.commit()

    def save_document_pages(self, pages: list[DocumentPage]) -> None:
        if not pages:
            return
        with psycopg.connect(self.dsn) as conn:
            with conn.cursor() as cur:
                for page in pages:
                    cur.execute(
                        """
                        INSERT INTO document_pages (
                          document_id, page_number, extracted_text, ocr_text, merged_text, page_image_path, metadata_json
                        ) VALUES (%s,%s,%s,%s,%s,%s,%s)
                        ON CONFLICT (document_id, page_number) DO UPDATE SET
                          extracted_text = EXCLUDED.extracted_text,
                          ocr_text = EXCLUDED.ocr_text,
                          merged_text = EXCLUDED.merged_text,
                          page_image_path = EXCLUDED.page_image_path,
                          metadata_json = EXCLUDED.metadata_json,
                          updated_at = now()
                        """,
                        (
                            page.document_id,
                            page.page_number,
                            self._sanitize_text(page.extracted_text),
                            self._sanitize_text(page.ocr_text),
                            self._sanitize_text(page.merged_text),
                            self._sanitize_text(page.page_image_path or ""),
                            json.dumps(self._sanitize_json_value(page.metadata or {})),
                        ),
                    )
            conn.commit()

    def save_page_assets(self, assets: list[PageAsset]) -> None:
        if not assets:
            return
        with psycopg.connect(self.dsn) as conn:
            with conn.cursor() as cur:
                for asset in assets:
                    cur.execute(
                        """
                        INSERT INTO page_assets (
                          asset_id, document_id, tenant_id, page_number, asset_index, asset_type,
                          bbox_json, asset_path, content_hash, ocr_text, description_text, search_text, metadata_json
                        ) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
                        ON CONFLICT (asset_id) DO UPDATE SET
                          asset_type = EXCLUDED.asset_type,
                          bbox_json = EXCLUDED.bbox_json,
                          asset_path = EXCLUDED.asset_path,
                          content_hash = EXCLUDED.content_hash,
                          ocr_text = EXCLUDED.ocr_text,
                          description_text = EXCLUDED.description_text,
                          search_text = EXCLUDED.search_text,
                          metadata_json = EXCLUDED.metadata_json,
                          updated_at = now()
                        """,
                        (
                            asset.asset_id,
                            asset.document_id,
                            asset.tenant_id,
                            asset.page_number,
                            asset.asset_index,
                            asset.asset_type,
                            json.dumps(self._sanitize_json_value(asset.bbox or [])) if asset.bbox else None,
                            self._sanitize_text(asset.asset_path),
                            asset.content_hash,
                            self._sanitize_text(asset.ocr_text),
                            self._sanitize_text(asset.description_text),
                            self._sanitize_text(asset.search_text),
                            json.dumps(self._sanitize_json_value(asset.metadata or {})),
                        ),
                    )
            conn.commit()

    def save_chunk_asset_links(self, links: list[ChunkAssetLink]) -> None:
        if not links:
            return
        with psycopg.connect(self.dsn) as conn:
            with conn.cursor() as cur:
                for link in links:
                    cur.execute(
                        """
                        INSERT INTO chunk_asset_links (chunk_id, asset_id, link_type, confidence, metadata_json)
                        VALUES (%s,%s,%s,%s,%s)
                        ON CONFLICT (chunk_id, asset_id, link_type) DO UPDATE SET
                          confidence = EXCLUDED.confidence,
                          metadata_json = EXCLUDED.metadata_json
                        """,
                        (
                            link.chunk_id,
                            link.asset_id,
                            link.link_type,
                            link.confidence,
                            json.dumps(self._sanitize_json_value(link.metadata or {})),
                        ),
                    )
            conn.commit()

    def save_chunks(self, chunks: list[Chunk]) -> None:
        with psycopg.connect(self.dsn) as conn:
            with conn.cursor() as cur:
                for chunk in chunks:
                    # Chunk rows are upserted because rebuild/replay operations can
                    # recompute metadata for a stable deterministic chunk id.
                    cur.execute(
                        """
                        INSERT INTO document_chunks (
                          chunk_id, document_id, tenant_id, chunk_index, page_start, page_end,
                          section_path, prev_chunk_id, next_chunk_id, char_start, char_end,
                          content_type, text, parser_version, chunker_version, content_hash, metadata_json, status
                        ) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,'pending')
                        ON CONFLICT (chunk_id) DO UPDATE SET
                          prev_chunk_id=EXCLUDED.prev_chunk_id,
                          next_chunk_id=EXCLUDED.next_chunk_id,
                          text=EXCLUDED.text,
                          metadata_json=EXCLUDED.metadata_json,
                          updated_at=now()
                        """,
                        (
                            chunk.chunk_id,
                            chunk.document_id,
                            chunk.tenant_id,
                            chunk.chunk_index,
                            chunk.page_start,
                            chunk.page_end,
                            chunk.section_path,
                            chunk.prev_chunk_id,
                            chunk.next_chunk_id,
                            chunk.char_start,
                            chunk.char_end,
                            chunk.content_type,
                            self._sanitize_text(chunk.text),
                            chunk.parser_version,
                            chunk.chunker_version,
                            chunk.content_hash,
                            json.dumps(self._sanitize_json_value(chunk.metadata)),
                        ),
                    )
            conn.commit()

    def save_validations(self, validations: list[ChunkValidation]) -> None:
        with psycopg.connect(self.dsn) as conn:
            with conn.cursor() as cur:
                for validation in validations:
                    # Validation is mirrored into both the validation table and the
                    # chunk row status so read paths do not need to join everywhere.
                    cur.execute(
                        """
                        INSERT INTO chunk_validations (chunk_id, status, quality_score, reasons)
                        VALUES (%s,%s,%s,%s)
                        ON CONFLICT (chunk_id) DO UPDATE SET
                          status=EXCLUDED.status,
                          quality_score=EXCLUDED.quality_score,
                          reasons=EXCLUDED.reasons
                        """,
                        (
                            validation.chunk_id,
                            validation.status,
                            validation.quality_score,
                            json.dumps(self._sanitize_json_value(validation.reasons)),
                        ),
                    )
                    cur.execute(
                        "UPDATE document_chunks SET status=%s, updated_at=now() WHERE chunk_id=%s",
                        (validation.status, validation.chunk_id),
                    )
            conn.commit()

    def save_kg_result(
        self,
        document_id: str,
        chunk_id: str,
        result: KGExtractionResult,
        status: str,
        errors: list[str],
        raw_payload: dict | None = None,
        provider: str | None = None,
        model: str | None = None,
        prompt_version: str | None = None,
    ) -> None:
        # KG persistence is replace-by-chunk. One chunk owns one raw extraction record
        # and zero-or-more accepted assertions derived from that record.
        with psycopg.connect(self.dsn) as conn:
            with conn.cursor() as cur:
                self._write_kg_result(
                    cur,
                    document_id=document_id,
                    chunk_id=chunk_id,
                    result=result,
                    status=status,
                    errors=errors,
                    raw_payload=raw_payload,
                    provider=provider,
                    model=model,
                    prompt_version=prompt_version,
                )
            conn.commit()

    def replace_document_kg_results(self, document_id: str, kg_results: list[dict]) -> None:
        with psycopg.connect(self.dsn) as conn:
            with conn.cursor() as cur:
                cur.execute("DELETE FROM kg_assertions WHERE document_id = %s", (document_id,))
                cur.execute("DELETE FROM kg_raw_extractions WHERE document_id = %s", (document_id,))
                for item in kg_results:
                    result_obj = item.get("_result_obj")
                    if not isinstance(result_obj, KGExtractionResult):
                        raise ValueError("replace_document_kg_results requires in-memory KG result objects")
                    self._write_kg_result(
                        cur,
                        document_id=document_id,
                        chunk_id=str(item["chunk_id"]),
                        result=result_obj,
                        status=str(item["status"]),
                        errors=list(item.get("errors") or []),
                        raw_payload=dict(item.get("_raw_payload") or {}),
                        provider=item.get("provider"),
                        model=item.get("model"),
                        prompt_version=item.get("prompt_version"),
                    )
            conn.commit()
        self.prune_orphan_kg_entities()

    def apply_revalidation_state(
        self,
        document_id: str,
        links: list[ChunkAssetLink],
        chunk_updates: list[dict[str, object]],
        *,
        kg_results: list[dict] | None = None,
        remove_chunk_kg_ids: list[str] | None = None,
    ) -> None:
        with psycopg.connect(self.dsn) as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    DELETE FROM chunk_asset_links
                    WHERE chunk_id IN (
                      SELECT chunk_id
                      FROM document_chunks
                      WHERE document_id = %s
                    )
                    """,
                    (document_id,),
                )
                for link in links:
                    cur.execute(
                        """
                        INSERT INTO chunk_asset_links (chunk_id, asset_id, link_type, confidence, metadata_json)
                        VALUES (%s,%s,%s,%s,%s)
                        ON CONFLICT (chunk_id, asset_id, link_type) DO UPDATE SET
                          confidence = EXCLUDED.confidence,
                          metadata_json = EXCLUDED.metadata_json
                        """,
                        (
                            link.chunk_id,
                            link.asset_id,
                            link.link_type,
                            link.confidence,
                            json.dumps(self._sanitize_json_value(link.metadata or {})),
                        ),
                    )
                for item in chunk_updates:
                    chunk_id = str(item["chunk_id"])
                    status = str(item["status"])
                    quality_score = float(item["quality_score"])
                    reasons = list(item.get("reasons") or [])
                    metadata = dict(item.get("metadata") or {})
                    cur.execute(
                        """
                        INSERT INTO chunk_validations (chunk_id, status, quality_score, reasons)
                        VALUES (%s, %s, %s, %s)
                        ON CONFLICT (chunk_id) DO UPDATE SET
                          status = EXCLUDED.status,
                          quality_score = EXCLUDED.quality_score,
                          reasons = EXCLUDED.reasons
                        """,
                        (chunk_id, status, quality_score, json.dumps(self._sanitize_json_value(reasons))),
                    )
                    cur.execute(
                        """
                        UPDATE document_chunks
                        SET status = %s,
                            metadata_json = %s,
                            updated_at = now()
                        WHERE chunk_id = %s
                        """,
                        (status, json.dumps(self._sanitize_json_value(metadata)), chunk_id),
                    )
                if kg_results is not None:
                    cur.execute("DELETE FROM kg_assertions WHERE document_id = %s", (document_id,))
                    cur.execute("DELETE FROM kg_raw_extractions WHERE document_id = %s", (document_id,))
                    for item in kg_results:
                        result_obj = item.get("_result_obj")
                        if not isinstance(result_obj, KGExtractionResult):
                            raise ValueError("apply_revalidation_state requires in-memory KG result objects")
                        self._write_kg_result(
                            cur,
                            document_id=document_id,
                            chunk_id=str(item["chunk_id"]),
                            result=result_obj,
                            status=str(item["status"]),
                            errors=list(item.get("errors") or []),
                            raw_payload=dict(item.get("_raw_payload") or {}),
                            provider=item.get("provider"),
                            model=item.get("model"),
                            prompt_version=item.get("prompt_version"),
                        )
                elif remove_chunk_kg_ids:
                    for chunk_id in remove_chunk_kg_ids:
                        cur.execute("DELETE FROM kg_raw_extractions WHERE chunk_id = %s", (chunk_id,))
                        cur.execute("DELETE FROM kg_assertions WHERE chunk_id = %s", (chunk_id,))
            conn.commit()
        if kg_results is not None or remove_chunk_kg_ids:
            self.prune_orphan_kg_entities()

    def get_dashboard_overview(self) -> dict:
        query_map = {
            "documents": "SELECT COUNT(*) AS value FROM documents",
            "jobs": "SELECT COUNT(*) AS value FROM ingestion_jobs",
            "pages": "SELECT COUNT(*) AS value FROM document_pages",
            "page_assets": "SELECT COUNT(*) AS value FROM page_assets",
            "chunks": "SELECT COUNT(*) AS value FROM document_chunks",
            "accepted_chunks": "SELECT COUNT(*) AS value FROM chunk_validations WHERE status = 'accepted'",
            "review_chunks": "SELECT COUNT(*) AS value FROM chunk_validations WHERE status = 'review'",
            "rejected_chunks": "SELECT COUNT(*) AS value FROM chunk_validations WHERE status = 'rejected'",
            "kg_entities": "SELECT COUNT(*) AS value FROM kg_entities",
            "kg_assertions": "SELECT COUNT(*) AS value FROM kg_assertions",
            "kg_evidence": "SELECT COUNT(*) AS value FROM kg_assertion_evidence",
            "kg_raw_extractions": "SELECT COUNT(*) AS value FROM kg_raw_extractions",
            "kg_validated_extractions": "SELECT COUNT(*) AS value FROM kg_raw_extractions WHERE status = 'validated'",
            "kg_review_extractions": "SELECT COUNT(*) AS value FROM kg_raw_extractions WHERE status = 'review'",
            "kg_skipped_extractions": "SELECT COUNT(*) AS value FROM kg_raw_extractions WHERE status = 'skipped'",
            "kg_quarantined_extractions": "SELECT COUNT(*) AS value FROM kg_raw_extractions WHERE status = 'quarantined'",
            "corpus_snapshots": "SELECT COUNT(*) AS value FROM corpus_snapshots",
            "agent_sessions": "SELECT COUNT(*) AS value FROM agent_sessions",
            "agent_query_runs": "SELECT COUNT(*) AS value FROM agent_query_runs",
            "agent_abstentions": "SELECT COUNT(*) AS value FROM agent_query_runs WHERE abstained = true",
            "agent_review_queue": "SELECT COUNT(*) AS value FROM agent_query_runs WHERE review_status = 'needs_review'",
        }
        overview: dict[str, int] = {}
        with psycopg.connect(self.dsn, row_factory=dict_row) as conn:
            with conn.cursor() as cur:
                for key, sql in query_map.items():
                    cur.execute(sql)
                    overview[key] = cur.fetchone()["value"]
        return overview

    def count_documents(self) -> int:
        with psycopg.connect(self.dsn, row_factory=dict_row) as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT COUNT(*) AS value FROM documents")
                return cur.fetchone()["value"]

    def list_documents(
        self,
        limit: int | None = None,
        offset: int = 0,
        tenant_id: str | None = None,
        status: str | None = None,
        document_class: str | None = None,
        source_type: str | None = None,
    ) -> list[dict]:
        with psycopg.connect(self.dsn, row_factory=dict_row) as conn:
            with conn.cursor() as cur:
                sql = """
                    SELECT
                      d.document_id,
                      d.filename,
                      d.document_class,
                      d.tenant_id,
                      d.source_type,
                      d.status,
                      d.created_at,
                      COALESCE(ch.total_chunks, 0) AS total_chunks,
                      COALESCE(ch.accepted_chunks, 0) AS accepted_chunks,
                      COALESCE(ch.review_chunks, 0) AS review_chunks,
                      COALESCE(ch.rejected_chunks, 0) AS rejected_chunks
                    FROM documents d
                    LEFT JOIN (
                      SELECT
                        dc.document_id,
                        COUNT(*) AS total_chunks,
                        COUNT(*) FILTER (WHERE cv.status = 'accepted') AS accepted_chunks,
                        COUNT(*) FILTER (WHERE cv.status = 'review') AS review_chunks,
                        COUNT(*) FILTER (WHERE cv.status = 'rejected') AS rejected_chunks
                      FROM document_chunks dc
                      LEFT JOIN chunk_validations cv ON cv.chunk_id = dc.chunk_id
                      GROUP BY dc.document_id
                    ) ch ON ch.document_id = d.document_id
                    """
                clauses: list[str] = []
                params_list: list[object] = []
                if tenant_id:
                    clauses.append("d.tenant_id = %s")
                    params_list.append(tenant_id)
                if status:
                    clauses.append("d.status = %s")
                    params_list.append(status)
                if document_class:
                    clauses.append("d.document_class = %s")
                    params_list.append(document_class)
                if source_type:
                    clauses.append("d.source_type = %s")
                    params_list.append(source_type)
                if clauses:
                    sql += "\nWHERE " + " AND ".join(clauses)
                sql += "\nORDER BY d.created_at DESC"
                params: tuple[object, ...] = tuple(params_list)
                if limit is not None:
                    sql += "\nLIMIT %s OFFSET %s"
                    params = tuple(params_list + [limit, offset])
                cur.execute(sql, params)
                return [dict(row) for row in cur.fetchall()]

    def get_document_detail(self, document_id: str) -> dict | None:
        with psycopg.connect(self.dsn, row_factory=dict_row) as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT document_id, filename, document_class, tenant_id, source_type, status, content_hash, created_at, updated_at
                    FROM documents
                    WHERE document_id = %s
                    """,
                    (document_id,),
                )
                document = cur.fetchone()
                if document is None:
                    return None

                cur.execute(
                    """
                    SELECT job_id, status, parser_version, chunker_version, embedding_version, kg_version, created_at, updated_at
                    FROM ingestion_jobs
                    WHERE document_id = %s
                    ORDER BY created_at DESC
                    """,
                    (document_id,),
                )
                jobs = [dict(row) for row in cur.fetchall()]

                cur.execute(
                    """
                    SELECT stage_run_id, stage_name, status, metrics_json, error_message, started_at, finished_at
                    FROM ingestion_stage_runs
                    WHERE document_id = %s
                    ORDER BY
                      CASE WHEN status = 'running' THEN 0 ELSE 1 END,
                      COALESCE(finished_at, started_at) DESC,
                      started_at DESC
                    LIMIT 25
                    """,
                    (document_id,),
                )
                stages = [dict(row) for row in cur.fetchall()]

        return {
            "document": dict(document),
            "jobs": jobs,
            "stages": stages,
            "document_synopsis": self.get_document_synopsis(document_id),
            "section_synopses": self.list_document_section_synopses(document_id),
        }

    def replace_document_synopsis(
        self,
        *,
        document_id: str,
        tenant_id: str,
        title: str,
        synopsis_text: str,
        accepted_chunk_count: int,
        section_count: int,
        source_stage: str,
        synopsis_version: str,
        metadata_json: dict[str, Any] | None = None,
    ) -> None:
        with psycopg.connect(self.dsn, row_factory=dict_row) as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO document_synopses (
                      document_id,
                      tenant_id,
                      title,
                      synopsis_text,
                      accepted_chunk_count,
                      section_count,
                      source_stage,
                      synopsis_version,
                      metadata_json
                    )
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT (document_id) DO UPDATE
                    SET tenant_id = EXCLUDED.tenant_id,
                        title = EXCLUDED.title,
                        synopsis_text = EXCLUDED.synopsis_text,
                        accepted_chunk_count = EXCLUDED.accepted_chunk_count,
                        section_count = EXCLUDED.section_count,
                        source_stage = EXCLUDED.source_stage,
                        synopsis_version = EXCLUDED.synopsis_version,
                        metadata_json = EXCLUDED.metadata_json,
                        updated_at = now()
                    """,
                    (
                        document_id,
                        tenant_id,
                        self._sanitize_text(title),
                        self._sanitize_text(synopsis_text),
                        int(accepted_chunk_count),
                        int(section_count),
                        self._sanitize_text(source_stage),
                        self._sanitize_text(synopsis_version),
                        Json(self._sanitize_json_value(metadata_json or {})),
                    ),
                )
            conn.commit()

    def replace_section_synopses(self, document_id: str, synopses: list[dict[str, Any]]) -> None:
        with psycopg.connect(self.dsn, row_factory=dict_row) as conn:
            with conn.cursor() as cur:
                cur.execute("DELETE FROM document_section_synopses WHERE document_id = %s", (document_id,))
                ordered_synopses = sorted(
                    synopses,
                    key=lambda item: (
                        int(item.get("section_level") or 0),
                        int(item.get("page_start") or 0),
                        int(item.get("char_start") or 0),
                        str(item.get("section_id") or ""),
                    ),
                )
                for synopsis in ordered_synopses:
                    cur.execute(
                        """
                        INSERT INTO document_section_synopses (
                          section_id,
                          document_id,
                          tenant_id,
                          parent_section_id,
                          section_path,
                          section_level,
                          section_title,
                          page_start,
                          page_end,
                          char_start,
                          char_end,
                          first_chunk_id,
                          last_chunk_id,
                          accepted_chunk_count,
                          total_chunk_count,
                          synopsis_text,
                          synopsis_version,
                          metadata_json
                        )
                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                        """,
                        (
                            self._sanitize_text(str(synopsis.get("section_id") or "")),
                            document_id,
                            self._sanitize_text(str(synopsis.get("tenant_id") or "")),
                            self._sanitize_text(str(synopsis.get("parent_section_id") or "")) or None,
                            [self._sanitize_text(str(item)) for item in list(synopsis.get("section_path") or [])],
                            int(synopsis.get("section_level") or 0),
                            self._sanitize_text(str(synopsis.get("section_title") or "")),
                            synopsis.get("page_start"),
                            synopsis.get("page_end"),
                            synopsis.get("char_start"),
                            synopsis.get("char_end"),
                            self._sanitize_text(str(synopsis.get("first_chunk_id") or "")) or None,
                            self._sanitize_text(str(synopsis.get("last_chunk_id") or "")) or None,
                            int(synopsis.get("accepted_chunk_count") or 0),
                            int(synopsis.get("total_chunk_count") or 0),
                            self._sanitize_text(str(synopsis.get("synopsis_text") or "")),
                            self._sanitize_text(str(synopsis.get("synopsis_version") or "")),
                            Json(self._sanitize_json_value(dict(synopsis.get("metadata_json") or {}))),
                        ),
                    )
            conn.commit()

    def get_document_synopsis(self, document_id: str) -> dict | None:
        with psycopg.connect(self.dsn, row_factory=dict_row) as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT
                      document_id,
                      tenant_id,
                      title,
                      synopsis_text,
                      accepted_chunk_count,
                      section_count,
                      source_stage,
                      synopsis_version,
                      metadata_json,
                      created_at,
                      updated_at
                    FROM document_synopses
                    WHERE document_id = %s
                    """,
                    (document_id,),
                )
                row = cur.fetchone()
        return dict(row) if row else None

    def list_document_section_synopses(self, document_id: str) -> list[dict]:
        with psycopg.connect(self.dsn, row_factory=dict_row) as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT
                      section_id,
                      document_id,
                      tenant_id,
                      parent_section_id,
                      section_path,
                      section_level,
                      section_title,
                      page_start,
                      page_end,
                      char_start,
                      char_end,
                      first_chunk_id,
                      last_chunk_id,
                      accepted_chunk_count,
                      total_chunk_count,
                      synopsis_text,
                      synopsis_version,
                      metadata_json,
                      created_at,
                      updated_at
                    FROM document_section_synopses
                    WHERE document_id = %s
                    ORDER BY section_level, page_start NULLS LAST, char_start NULLS LAST, section_path
                    """,
                    (document_id,),
                )
                return [dict(row) for row in cur.fetchall()]

    def list_document_synopses_by_ids(self, document_ids: list[str]) -> list[dict]:
        if not document_ids:
            return []
        with psycopg.connect(self.dsn, row_factory=dict_row) as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT
                      document_id,
                      tenant_id,
                      title,
                      synopsis_text,
                      accepted_chunk_count,
                      section_count,
                      source_stage,
                      synopsis_version,
                      metadata_json,
                      created_at,
                      updated_at
                    FROM document_synopses
                    WHERE document_id = ANY(%s)
                    """,
                    (document_ids,),
                )
                return [dict(row) for row in cur.fetchall()]

    def list_section_synopses_for_chunk_ids(self, chunk_ids: list[str], limit: int = 24) -> list[dict]:
        if not chunk_ids:
            return []
        with psycopg.connect(self.dsn, row_factory=dict_row) as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT DISTINCT ON (dss.section_id)
                      dss.section_id,
                      dss.document_id,
                      dss.tenant_id,
                      dss.parent_section_id,
                      dss.section_path,
                      dss.section_level,
                      dss.section_title,
                      dss.page_start,
                      dss.page_end,
                      dss.char_start,
                      dss.char_end,
                      dss.first_chunk_id,
                      dss.last_chunk_id,
                      dss.accepted_chunk_count,
                      dss.total_chunk_count,
                      dss.synopsis_text,
                      dss.synopsis_version,
                      dss.metadata_json,
                      dss.created_at,
                      dss.updated_at
                    FROM document_section_synopses dss
                    JOIN document_chunks dc
                      ON dc.document_id = dss.document_id
                     AND dc.section_path = dss.section_path
                    WHERE dc.chunk_id = ANY(%s)
                    ORDER BY dss.section_id, dss.section_level, dss.page_start NULLS LAST, dss.char_start NULLS LAST
                    LIMIT %s
                    """,
                    (chunk_ids, limit),
                )
                return [dict(row) for row in cur.fetchall()]

    def search_document_synopses_lexical(
        self,
        query_text: str,
        tenant_id: str,
        document_ids: list[str] | None = None,
        limit: int = 6,
    ) -> list[dict]:
        tokens = [item for item in re.split(r"[^a-z0-9]+", query_text.lower()) if len(item) >= 3]
        if not tokens:
            return []
        match_clauses: list[str] = []
        score_sql: list[str] = []
        score_params: list[object] = []
        match_params: list[object] = []
        for token in tokens[:8]:
            needle = f"%{token}%"
            prefix = f"{token}%"
            match_clauses.append(
                "("
                "ds.synopsis_text ILIKE %s OR "
                "COALESCE(ds.title, '') ILIKE %s"
                ")"
            )
            match_params.extend([needle, needle])
            score_sql.extend(
                [
                    "MAX(CASE WHEN COALESCE(ds.title, '') ILIKE %s THEN 24 ELSE 0 END)",
                    "MAX(CASE WHEN ds.synopsis_text ILIKE %s THEN 9 ELSE 0 END)",
                ]
            )
            score_params.extend([prefix, needle])
        clauses = [
            "(" + " OR ".join(match_clauses) + ")",
            "ds.tenant_id = %s",
        ]
        params: list[object] = list(score_params) + list(match_params) + [tenant_id]
        if document_ids:
            clauses.append("ds.document_id = ANY(%s)")
            params.append(document_ids)
        relevance_score = " + ".join(score_sql) if score_sql else "0"
        where_clause = "WHERE " + " AND ".join(clauses)
        with psycopg.connect(self.dsn, row_factory=dict_row) as conn:
            with conn.cursor() as cur:
                cur.execute(
                    f"""
                    SELECT
                      ds.document_id,
                      ds.tenant_id,
                      ds.title,
                      ds.synopsis_text,
                      ds.accepted_chunk_count,
                      ds.section_count,
                      ({relevance_score}) AS lexical_score
                    FROM document_synopses ds
                    {where_clause}
                    GROUP BY ds.document_id, ds.tenant_id, ds.title, ds.synopsis_text, ds.accepted_chunk_count, ds.section_count
                    ORDER BY lexical_score DESC, ds.accepted_chunk_count DESC, ds.document_id
                    LIMIT %s
                    """,
                    tuple(params + [limit]),
                )
                return [dict(row) for row in cur.fetchall()]

    def search_section_synopses_lexical(
        self,
        query_text: str,
        tenant_id: str,
        document_ids: list[str] | None = None,
        limit: int = 12,
    ) -> list[dict]:
        tokens = [item for item in re.split(r"[^a-z0-9]+", query_text.lower()) if len(item) >= 3]
        if not tokens:
            return []
        match_clauses: list[str] = []
        score_sql: list[str] = []
        score_params: list[object] = []
        match_params: list[object] = []
        for token in tokens[:8]:
            needle = f"%{token}%"
            prefix = f"{token}%"
            match_clauses.append(
                "("
                "dss.synopsis_text ILIKE %s OR "
                "COALESCE(dss.section_title, '') ILIKE %s"
                ")"
            )
            match_params.extend([needle, needle])
            score_sql.extend(
                [
                    "MAX(CASE WHEN COALESCE(dss.section_title, '') ILIKE %s THEN 24 ELSE 0 END)",
                    "MAX(CASE WHEN dss.synopsis_text ILIKE %s THEN 9 ELSE 0 END)",
                ]
            )
            score_params.extend([prefix, needle])
        clauses = [
            "(" + " OR ".join(match_clauses) + ")",
            "dss.tenant_id = %s",
        ]
        params: list[object] = list(score_params) + list(match_params) + [tenant_id]
        if document_ids:
            clauses.append("dss.document_id = ANY(%s)")
            params.append(document_ids)
        relevance_score = " + ".join(score_sql) if score_sql else "0"
        where_clause = "WHERE " + " AND ".join(clauses)
        with psycopg.connect(self.dsn, row_factory=dict_row) as conn:
            with conn.cursor() as cur:
                cur.execute(
                    f"""
                    SELECT
                      dss.section_id,
                      dss.document_id,
                      dss.tenant_id,
                      dss.section_path,
                      dss.section_title,
                      dss.synopsis_text,
                      dss.accepted_chunk_count,
                      dss.section_level,
                      ({relevance_score}) AS lexical_score
                    FROM document_section_synopses dss
                    {where_clause}
                    GROUP BY
                      dss.section_id,
                      dss.document_id,
                      dss.tenant_id,
                      dss.section_path,
                      dss.section_title,
                      dss.synopsis_text,
                      dss.accepted_chunk_count,
                      dss.section_level
                    ORDER BY lexical_score DESC, dss.accepted_chunk_count DESC, dss.section_level ASC, dss.section_id
                    LIMIT %s
                    """,
                    tuple(params + [limit]),
                )
                return [dict(row) for row in cur.fetchall()]

    def get_document_record(self, document_id: str) -> dict | None:
        with psycopg.connect(self.dsn, row_factory=dict_row) as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT
                      document_id,
                      tenant_id,
                      source_type,
                      filename,
                      content_hash,
                      parser_version,
                      ocr_engine,
                      ocr_model,
                      document_class,
                      status,
                      created_at,
                      updated_at
                    FROM documents
                    WHERE document_id = %s
                    """,
                    (document_id,),
                )
                row = cur.fetchone()
                return dict(row) if row else None

    def update_document_record(self, document_id: str, patch: dict) -> dict | None:
        assignments, params = self._build_allowed_patch(
            patch,
            allowed_fields={
                "tenant_id": "tenant_id",
                "source_type": "source_type",
                "filename": "filename",
                "content_hash": "content_hash",
                "parser_version": "parser_version",
                "ocr_engine": "ocr_engine",
                "ocr_model": "ocr_model",
                "document_class": "document_class",
                "status": "status",
            },
            text_fields={
                "tenant_id",
                "source_type",
                "filename",
                "content_hash",
                "parser_version",
                "ocr_engine",
                "ocr_model",
                "document_class",
                "status",
            },
        )
        with psycopg.connect(self.dsn, row_factory=dict_row) as conn:
            with conn.cursor() as cur:
                cur.execute(
                    f"""
                    UPDATE documents
                    SET {", ".join(assignments)}, updated_at = now()
                    WHERE document_id = %s
                    RETURNING
                      document_id,
                      tenant_id,
                      source_type,
                      filename,
                      content_hash,
                      parser_version,
                      ocr_engine,
                      ocr_model,
                      document_class,
                      status,
                      created_at,
                      updated_at
                    """,
                    tuple(params + [document_id]),
                )
                row = cur.fetchone()
            conn.commit()
        return dict(row) if row else None

    def count_stage_runs(self, document_id: str | None = None, status: str | None = None) -> int:
        clauses: list[str] = []
        params: list[object] = []
        if document_id:
            clauses.append("document_id = %s")
            params.append(document_id)
        if status:
            clauses.append("status = %s")
            params.append(status)
        where_clause = f"WHERE {' AND '.join(clauses)}" if clauses else ""
        with psycopg.connect(self.dsn, row_factory=dict_row) as conn:
            with conn.cursor() as cur:
                cur.execute(f"SELECT COUNT(*) AS value FROM ingestion_stage_runs {where_clause}", tuple(params))
                return cur.fetchone()["value"]

    def list_stage_runs(
        self,
        document_id: str | None = None,
        status: str | None = None,
        limit: int = 100,
        offset: int = 0,
    ) -> list[dict]:
        clauses: list[str] = []
        params: list[object] = []
        if document_id:
            clauses.append("document_id = %s")
            params.append(document_id)
        if status:
            clauses.append("status = %s")
            params.append(status)
        where_clause = f"WHERE {' AND '.join(clauses)}" if clauses else ""
        params.extend([limit, offset])
        with psycopg.connect(self.dsn, row_factory=dict_row) as conn:
            with conn.cursor() as cur:
                cur.execute(
                    f"""
                    SELECT
                      stage_run_id,
                      job_id,
                      document_id,
                      stage_name,
                      status,
                      attempt,
                      worker_version,
                      input_version,
                      error_code,
                      error_message,
                      metrics_json,
                      started_at,
                      finished_at
                    FROM ingestion_stage_runs
                    {where_clause}
                    ORDER BY
                      CASE WHEN status = 'running' THEN 0 ELSE 1 END,
                      COALESCE(finished_at, started_at) DESC,
                      started_at DESC
                    LIMIT %s OFFSET %s
                    """,
                    tuple(params),
                )
                return [dict(row) for row in cur.fetchall()]

    def list_document_sources(self, document_id: str, limit: int = 25) -> list[dict]:
        with psycopg.connect(self.dsn, row_factory=dict_row) as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT
                      source_id,
                      document_id,
                      raw_text,
                      normalized_text,
                      extraction_metrics_json,
                      metadata_json,
                      created_at
                    FROM document_sources
                    WHERE document_id = %s
                    ORDER BY created_at DESC
                    LIMIT %s
                    """,
                    (document_id, limit),
                )
                return [dict(row) for row in cur.fetchall()]

    def get_document_source(self, source_id: str) -> dict | None:
        with psycopg.connect(self.dsn, row_factory=dict_row) as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT
                      source_id,
                      document_id,
                      raw_text,
                      normalized_text,
                      extraction_metrics_json,
                      metadata_json,
                      created_at
                    FROM document_sources
                    WHERE source_id = %s
                    """,
                    (source_id,),
                )
                row = cur.fetchone()
                return dict(row) if row else None

    def update_document_source(self, source_id: str, patch: dict) -> dict | None:
        assignments, params = self._build_allowed_patch(
            patch,
            allowed_fields={
                "raw_text": "raw_text",
                "normalized_text": "normalized_text",
                "extraction_metrics_json": "extraction_metrics_json",
                "metadata_json": "metadata_json",
            },
            json_fields={"extraction_metrics_json", "metadata_json"},
            text_fields={"raw_text", "normalized_text"},
        )
        with psycopg.connect(self.dsn, row_factory=dict_row) as conn:
            with conn.cursor() as cur:
                cur.execute(
                    f"""
                    UPDATE document_sources
                    SET {", ".join(assignments)}
                    WHERE source_id = %s
                    RETURNING
                      source_id,
                      document_id,
                      raw_text,
                      normalized_text,
                      extraction_metrics_json,
                      metadata_json,
                      created_at
                    """,
                    tuple(params + [source_id]),
                )
                row = cur.fetchone()
            conn.commit()
        return dict(row) if row else None

    def list_document_pages(self, document_id: str, limit: int = 250) -> list[dict]:
        with psycopg.connect(self.dsn, row_factory=dict_row) as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT
                      document_id,
                      page_number,
                      extracted_text,
                      ocr_text,
                      merged_text,
                      page_image_path,
                      metadata_json,
                      created_at,
                      updated_at
                    FROM document_pages
                    WHERE document_id = %s
                    ORDER BY page_number
                    LIMIT %s
                    """,
                    (document_id, limit),
                )
                return [dict(row) for row in cur.fetchall()]

    def get_document_page(self, document_id: str, page_number: int) -> dict | None:
        with psycopg.connect(self.dsn, row_factory=dict_row) as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT
                      document_id,
                      page_number,
                      extracted_text,
                      ocr_text,
                      merged_text,
                      page_image_path,
                      metadata_json,
                      created_at,
                      updated_at
                    FROM document_pages
                    WHERE document_id = %s AND page_number = %s
                    """,
                    (document_id, page_number),
                )
                row = cur.fetchone()
                return dict(row) if row else None

    def update_document_page(self, document_id: str, page_number: int, patch: dict) -> dict | None:
        assignments, params = self._build_allowed_patch(
            patch,
            allowed_fields={
                "extracted_text": "extracted_text",
                "ocr_text": "ocr_text",
                "merged_text": "merged_text",
                "page_image_path": "page_image_path",
                "metadata_json": "metadata_json",
            },
            json_fields={"metadata_json"},
            text_fields={"extracted_text", "ocr_text", "merged_text", "page_image_path"},
        )
        with psycopg.connect(self.dsn, row_factory=dict_row) as conn:
            with conn.cursor() as cur:
                cur.execute(
                    f"""
                    UPDATE document_pages
                    SET {", ".join(assignments)}, updated_at = now()
                    WHERE document_id = %s AND page_number = %s
                    RETURNING
                      document_id,
                      page_number,
                      extracted_text,
                      ocr_text,
                      merged_text,
                      page_image_path,
                      metadata_json,
                      created_at,
                      updated_at
                    """,
                    tuple(params + [document_id, page_number]),
                )
                row = cur.fetchone()
            conn.commit()
        return dict(row) if row else None

    def list_page_assets(
        self,
        document_id: str | None = None,
        chunk_id: str | None = None,
        limit: int = 250,
        offset: int = 0,
    ) -> list[dict]:
        clauses: list[str] = []
        params: list[object] = []
        joins = ""
        if document_id:
            clauses.append("pa.document_id = %s")
            params.append(document_id)
        if chunk_id:
            joins += " JOIN chunk_asset_links cal ON cal.asset_id = pa.asset_id"
            clauses.append("cal.chunk_id = %s")
            params.append(chunk_id)
        where_clause = f"WHERE {' AND '.join(clauses)}" if clauses else ""
        with psycopg.connect(self.dsn, row_factory=dict_row) as conn:
            with conn.cursor() as cur:
                cur.execute(
                    f"""
                    SELECT
                      pa.asset_id,
                      pa.document_id,
                      pa.tenant_id,
                      pa.page_number,
                      pa.asset_index,
                      pa.asset_type,
                      pa.bbox_json,
                      pa.asset_path,
                      pa.content_hash,
                      pa.ocr_text,
                      pa.description_text,
                      pa.search_text,
                      pa.metadata_json,
                      pa.created_at,
                      pa.updated_at
                    FROM page_assets pa
                    {joins}
                    {where_clause}
                    ORDER BY pa.page_number, pa.asset_index
                    LIMIT %s OFFSET %s
                    """,
                    tuple(params + [limit, offset]),
                )
                return [dict(row) for row in cur.fetchall()]

    def list_chunk_asset_links(self, document_id: str | None = None, chunk_id: str | None = None, limit: int = 500) -> list[dict]:
        clauses: list[str] = []
        params: list[object] = []
        if document_id:
            clauses.append("dc.document_id = %s")
            params.append(document_id)
        if chunk_id:
            clauses.append("cal.chunk_id = %s")
            params.append(chunk_id)
        where_clause = f"WHERE {' AND '.join(clauses)}" if clauses else ""
        with psycopg.connect(self.dsn, row_factory=dict_row) as conn:
            with conn.cursor() as cur:
                cur.execute(
                    f"""
                    SELECT
                      cal.link_id,
                      cal.chunk_id,
                      cal.asset_id,
                      cal.link_type,
                      cal.confidence,
                      cal.metadata_json,
                      pa.document_id,
                      pa.page_number,
                      pa.asset_type,
                      pa.asset_path,
                      pa.description_text,
                      pa.ocr_text,
                      pa.metadata_json AS asset_metadata_json,
                      cal.created_at
                    FROM chunk_asset_links cal
                    JOIN document_chunks dc ON dc.chunk_id = cal.chunk_id
                    JOIN page_assets pa ON pa.asset_id = cal.asset_id
                    {where_clause}
                    ORDER BY pa.page_number, pa.asset_index, cal.created_at
                    LIMIT %s
                    """,
                    tuple(params + [limit]),
                )
                return [dict(row) for row in cur.fetchall()]

    def delete_document_chunk_asset_links(self, document_id: str) -> None:
        with psycopg.connect(self.dsn) as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    DELETE FROM chunk_asset_links
                    WHERE chunk_id IN (
                      SELECT chunk_id
                      FROM document_chunks
                      WHERE document_id = %s
                    )
                    """,
                    (document_id,),
                )
            conn.commit()

    def get_latest_document_source(self, document_id: str) -> dict | None:
        with psycopg.connect(self.dsn, row_factory=dict_row) as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT
                      d.document_id,
                      d.tenant_id,
                      d.source_type,
                      d.filename,
                      d.content_hash,
                      d.document_class,
                      d.parser_version,
                      d.ocr_engine,
                      d.ocr_model,
                      ds.source_id,
                      ds.raw_text,
                      ds.normalized_text,
                      ds.extraction_metrics_json,
                      ds.metadata_json,
                      ds.created_at
                    FROM documents d
                    JOIN document_sources ds ON ds.document_id = d.document_id
                    WHERE d.document_id = %s
                    ORDER BY ds.created_at DESC
                    LIMIT 1
                    """,
                    (document_id,),
                )
                row = cur.fetchone()
                return dict(row) if row else None

    def get_agent_runtime_secret(self, tenant_id: str = "shared", include_value: bool = False) -> dict | None:
        with psycopg.connect(self.dsn, row_factory=dict_row) as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT tenant_id, api_key_override, updated_by, created_at, updated_at
                    FROM agent_runtime_secrets
                    WHERE tenant_id = %s
                    """,
                    (tenant_id,),
                )
                row = cur.fetchone()
                if not row:
                    return None
                payload = dict(row)
                payload["has_api_key_override"] = bool(row.get("api_key_override"))
                if include_value:
                    payload["api_key_override"] = self._decrypt_runtime_secret(str(row.get("api_key_override") or ""))
                else:
                    payload.pop("api_key_override", None)
                return payload

    def replace_document_source(self, document_id: str, source: SourceDocument) -> str:
        source_id = str(uuid4())
        with psycopg.connect(self.dsn) as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    UPDATE documents
                    SET tenant_id = %s,
                        source_type = %s,
                        filename = %s,
                        content_hash = %s,
                        document_class = %s,
                        parser_version = %s,
                        ocr_engine = %s,
                        ocr_model = %s,
                        status = 'registered',
                        updated_at = now()
                    WHERE document_id = %s
                    """,
                    (
                        source.tenant_id,
                        source.source_type,
                        source.filename,
                        source.content_hash,
                        source.document_class,
                        source.parser_version,
                        source.ocr_engine,
                        source.ocr_model,
                        document_id,
                    ),
                )
                if cur.rowcount == 0:
                    raise ValueError("Document not found")

                cur.execute(
                    """
                    INSERT INTO document_sources (source_id, document_id, raw_text, normalized_text, extraction_metrics_json, metadata_json)
                    VALUES (%s, %s, %s, %s, %s, %s)
                    """,
                    (
                        source_id,
                        document_id,
                        self._sanitize_text(source.raw_text),
                        self._sanitize_text(source.normalized_text or source.raw_text),
                        json.dumps(self._sanitize_json_value(source.extraction_metrics or {})),
                        json.dumps(self._sanitize_json_value(source.metadata or {})),
                    ),
                )
            conn.commit()
        return source_id

    def reset_document_pipeline_state(self, document_id: str, status: str = "registered") -> None:
        with psycopg.connect(self.dsn) as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    DELETE FROM chunk_asset_links
                    WHERE chunk_id IN (SELECT chunk_id FROM document_chunks WHERE document_id = %s)
                    """,
                    (document_id,),
                )
                cur.execute(
                    """
                    DELETE FROM chunk_review_runs
                    WHERE chunk_id IN (SELECT chunk_id FROM document_chunks WHERE document_id = %s)
                    """,
                    (document_id,),
                )
                cur.execute(
                    """
                    DELETE FROM chunk_validations
                    WHERE chunk_id IN (SELECT chunk_id FROM document_chunks WHERE document_id = %s)
                    """,
                    (document_id,),
                )
                cur.execute("DELETE FROM kg_assertions WHERE document_id = %s", (document_id,))
                cur.execute("DELETE FROM kg_raw_extractions WHERE document_id = %s", (document_id,))
                cur.execute("DELETE FROM page_assets WHERE document_id = %s", (document_id,))
                cur.execute("DELETE FROM document_pages WHERE document_id = %s", (document_id,))
                cur.execute("DELETE FROM parsed_blocks WHERE document_id = %s", (document_id,))
                cur.execute("DELETE FROM document_chunks WHERE document_id = %s", (document_id,))
                cur.execute(
                    """
                    UPDATE documents
                    SET status = %s,
                        updated_at = now()
                    WHERE document_id = %s
                    """,
                    (status, document_id),
                )
            conn.commit()
        self.prune_orphan_kg_entities()

    def delete_document_kg(self, document_id: str) -> None:
        with psycopg.connect(self.dsn) as conn:
            with conn.cursor() as cur:
                cur.execute("DELETE FROM kg_assertions WHERE document_id = %s", (document_id,))
                cur.execute("DELETE FROM kg_raw_extractions WHERE document_id = %s", (document_id,))
            conn.commit()
        self.prune_orphan_kg_entities()

    def delete_document(self, document_id: str) -> int:
        with psycopg.connect(self.dsn) as conn:
            with conn.cursor() as cur:
                cur.execute("DELETE FROM documents WHERE document_id = %s", (document_id,))
                deleted = cur.rowcount
            conn.commit()
        self.prune_orphan_kg_entities()
        return deleted

    def clear_ingestion_data(self) -> None:
        with psycopg.connect(self.dsn) as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    TRUNCATE TABLE
                      kg_assertion_evidence,
                      kg_assertions,
                      kg_raw_extractions,
                      kg_entities,
                      chunk_asset_links,
                      chunk_review_runs,
                      chunk_validations,
                      document_chunks,
                      parsed_blocks,
                      page_assets,
                      document_pages,
                      document_sources,
                      ingestion_stage_runs,
                      ingestion_jobs,
                      documents,
                      agent_answer_reviews,
                      agent_query_sources,
                      agent_query_runs,
                      agent_messages,
                      agent_session_memories,
                      agent_sessions,
                      agent_query_patterns,
                      agent_profiles
                    RESTART IDENTITY CASCADE
                    """
                )
            conn.commit()

    def get_document_related_counts(self, document_id: str) -> dict[str, int]:
        query_map = {
            "sources": "SELECT COUNT(*) AS value FROM document_sources WHERE document_id = %s",
            "pages": "SELECT COUNT(*) AS value FROM document_pages WHERE document_id = %s",
            "page_assets": "SELECT COUNT(*) AS value FROM page_assets WHERE document_id = %s",
            "chunk_asset_links": """
                SELECT COUNT(*) AS value
                FROM chunk_asset_links cal
                JOIN document_chunks dc ON dc.chunk_id = cal.chunk_id
                WHERE dc.document_id = %s
            """,
            "chunks": "SELECT COUNT(*) AS value FROM document_chunks WHERE document_id = %s",
            "metadata": "SELECT COUNT(*) AS value FROM chunk_metadata_view WHERE document_id = %s",
            "accepted_chunks": """
                SELECT COUNT(*) AS value
                FROM document_chunks dc
                LEFT JOIN chunk_validations cv ON cv.chunk_id = dc.chunk_id
                WHERE dc.document_id = %s AND COALESCE(cv.status, dc.status) = 'accepted'
            """,
            "review_chunks": """
                SELECT COUNT(*) AS value
                FROM document_chunks dc
                LEFT JOIN chunk_validations cv ON cv.chunk_id = dc.chunk_id
                WHERE dc.document_id = %s AND COALESCE(cv.status, dc.status) = 'review'
            """,
            "rejected_chunks": """
                SELECT COUNT(*) AS value
                FROM document_chunks dc
                LEFT JOIN chunk_validations cv ON cv.chunk_id = dc.chunk_id
                WHERE dc.document_id = %s AND COALESCE(cv.status, dc.status) = 'rejected'
            """,
            "kg_entities": """
                SELECT COUNT(DISTINCT e.entity_id) AS value
                FROM kg_entities e
                JOIN kg_assertions a
                  ON a.subject_entity_id = e.entity_id
                  OR a.object_entity_id = e.entity_id
                WHERE a.document_id = %s
            """,
            "kg_assertions": "SELECT COUNT(*) AS value FROM kg_assertions WHERE document_id = %s",
            "kg_evidence": """
                SELECT COUNT(*) AS value
                FROM kg_assertion_evidence e
                JOIN kg_assertions a ON a.assertion_id = e.assertion_id
                WHERE a.document_id = %s
            """,
            "kg_raw": "SELECT COUNT(*) AS value FROM kg_raw_extractions WHERE document_id = %s",
            "kg_validated": "SELECT COUNT(*) AS value FROM kg_raw_extractions WHERE document_id = %s AND status = 'validated'",
            "kg_review": "SELECT COUNT(*) AS value FROM kg_raw_extractions WHERE document_id = %s AND status = 'review'",
            "kg_skipped": "SELECT COUNT(*) AS value FROM kg_raw_extractions WHERE document_id = %s AND status = 'skipped'",
            "kg_quarantined": "SELECT COUNT(*) AS value FROM kg_raw_extractions WHERE document_id = %s AND status = 'quarantined'",
        }
        counts: dict[str, int] = {}
        with psycopg.connect(self.dsn, row_factory=dict_row) as conn:
            with conn.cursor() as cur:
                for key, sql in query_map.items():
                    cur.execute(sql, (document_id,))
                    counts[key] = cur.fetchone()["value"]
        return counts

    def count_documents(
        self,
        tenant_id: str | None = None,
        status: str | None = None,
        document_class: str | None = None,
        source_type: str | None = None,
    ) -> int:
        clauses: list[str] = []
        params: list[object] = []
        if tenant_id:
            clauses.append("tenant_id = %s")
            params.append(tenant_id)
        if status:
            clauses.append("status = %s")
            params.append(status)
        if document_class:
            clauses.append("document_class = %s")
            params.append(document_class)
        if source_type:
            clauses.append("source_type = %s")
            params.append(source_type)
        where_clause = f"WHERE {' AND '.join(clauses)}" if clauses else ""
        return self._fetch_scalar(f"SELECT COUNT(*) AS value FROM documents {where_clause}", tuple(params))

    def count_chunks(
        self,
        document_id: str | None = None,
        status: str | None = None,
        chunk_role: str | None = None,
        section_query: str | None = None,
    ) -> int:
        clauses: list[str] = []
        params: list[object] = []
        if document_id:
            clauses.append("dc.document_id = %s")
            params.append(document_id)
        if status:
            clauses.append("COALESCE(cv.status, dc.status) = %s")
            params.append(status)
        if chunk_role:
            clauses.append("COALESCE(dc.metadata_json->>'chunk_role', '') = %s")
            params.append(chunk_role)
        if section_query:
            clauses.append("COALESCE(dc.metadata_json->>'section_title', '') ILIKE %s")
            params.append(f"%{section_query}%")
        where_clause = f"WHERE {' AND '.join(clauses)}" if clauses else ""
        return self._fetch_scalar(
            f"""
            SELECT COUNT(*) AS value
            FROM document_chunks dc
            LEFT JOIN chunk_validations cv ON cv.chunk_id = dc.chunk_id
            {where_clause}
            """,
            tuple(params),
        )

    def list_chunk_records(
        self,
        document_id: str | None = None,
        status: str | None = None,
        limit: int = 500,
        offset: int = 0,
    ) -> list[dict]:
        clauses: list[str] = []
        params: list[object] = []
        if document_id:
            clauses.append("dc.document_id = %s")
            params.append(document_id)
        if status:
            clauses.append("COALESCE(cv.status, dc.status) = %s")
            params.append(status)
        where_clause = f"WHERE {' AND '.join(clauses)}" if clauses else ""
        params.extend([limit, offset])
        with psycopg.connect(self.dsn, row_factory=dict_row) as conn:
            with conn.cursor() as cur:
                cur.execute(
                    f"""
                    SELECT
                      dc.chunk_id,
                      dc.document_id,
                      dc.tenant_id,
                      dc.chunk_index,
                      dc.page_start,
                      dc.page_end,
                      dc.section_path,
                      dc.prev_chunk_id,
                      dc.next_chunk_id,
                      dc.char_start,
                      dc.char_end,
                      dc.content_type,
                      dc.text,
                      dc.parser_version,
                      dc.chunker_version,
                      dc.metadata_json,
                      COALESCE(cv.status, dc.status) AS validation_status,
                      cv.quality_score,
                      cv.reasons
                    FROM document_chunks dc
                    LEFT JOIN chunk_validations cv ON cv.chunk_id = dc.chunk_id
                    {where_clause}
                    ORDER BY dc.chunk_index
                    LIMIT %s OFFSET %s
                    """,
                    tuple(params),
                )
                return [dict(row) for row in cur.fetchall()]

    def count_chunk_metadata(
        self,
        document_id: str | None = None,
        status: str | None = None,
        chunk_role: str | None = None,
        section_query: str | None = None,
    ) -> int:
        clauses: list[str] = []
        params: list[object] = []
        if document_id:
            clauses.append("document_id = %s")
            params.append(document_id)
        if status:
            clauses.append("validation_status = %s")
            params.append(status)
        if chunk_role:
            clauses.append("chunk_role = %s")
            params.append(chunk_role)
        if section_query:
            clauses.append("COALESCE(section_title, '') ILIKE %s")
            params.append(f"%{section_query}%")
        where_clause = f"WHERE {' AND '.join(clauses)}" if clauses else ""
        return self._fetch_scalar(f"SELECT COUNT(*) AS value FROM chunk_metadata_view {where_clause}", tuple(params))

    def count_kg_entities(
        self,
        document_id: str | None = None,
        search: str | None = None,
        entity_type: str | None = None,
    ) -> int:
        clauses: list[str] = []
        params: list[object] = []
        if document_id:
            clauses.append("a.document_id = %s")
            params.append(document_id)
        if entity_type:
            clauses.append("e.entity_type = %s")
            params.append(entity_type)
        if search:
            clauses.append("(e.entity_id ILIKE %s OR e.canonical_name ILIKE %s OR e.entity_type ILIKE %s)")
            needle = f"%{search}%"
            params.extend([needle, needle, needle])
        where_clause = f"WHERE {' AND '.join(clauses)}" if clauses else ""
        return self._fetch_scalar(
            f"""
            SELECT COUNT(*) AS value FROM (
              SELECT e.entity_id
              FROM kg_entities e
              LEFT JOIN kg_assertions a
                ON a.subject_entity_id = e.entity_id
                OR a.object_entity_id = e.entity_id
              {where_clause}
              GROUP BY e.entity_id
            ) entity_rows
            """,
            tuple(params),
        )

    def count_kg_assertions(
        self,
        document_id: str | None = None,
        entity_id: str | None = None,
        predicate: str | None = None,
        status: str | None = None,
        chunk_id: str | None = None,
    ) -> int:
        clauses: list[str] = []
        params: list[object] = []
        if document_id:
            clauses.append("document_id = %s")
            params.append(document_id)
        if entity_id:
            clauses.append("(subject_entity_id = %s OR object_entity_id = %s)")
            params.extend([entity_id, entity_id])
        if predicate:
            clauses.append("predicate ILIKE %s")
            params.append(f"%{predicate}%")
        if status:
            clauses.append("status = %s")
            params.append(status)
        if chunk_id:
            clauses.append("chunk_id = %s")
            params.append(chunk_id)
        where_clause = f"WHERE {' AND '.join(clauses)}" if clauses else ""
        return self._fetch_scalar(f"SELECT COUNT(*) AS value FROM kg_assertions {where_clause}", tuple(params))

    def count_kg_raw_extractions(
        self,
        document_id: str | None = None,
        chunk_id: str | None = None,
        status: str | None = None,
    ) -> int:
        clauses: list[str] = []
        params: list[object] = []
        if document_id:
            clauses.append("document_id = %s")
            params.append(document_id)
        if chunk_id:
            clauses.append("chunk_id = %s")
            params.append(chunk_id)
        if status:
            clauses.append("status = %s")
            params.append(status)
        where_clause = f"WHERE {' AND '.join(clauses)}" if clauses else ""
        return self._fetch_scalar(f"SELECT COUNT(*) AS value FROM kg_raw_extractions {where_clause}", tuple(params))

    def count_chunks(self, document_id: str | None = None, status: str | None = None) -> int:
        clauses: list[str] = []
        params: list[object] = []
        if document_id:
            clauses.append("dc.document_id = %s")
            params.append(document_id)
        if status:
            clauses.append("COALESCE(cv.status, dc.status) = %s")
            params.append(status)
        where_clause = f"WHERE {' AND '.join(clauses)}" if clauses else ""
        with psycopg.connect(self.dsn, row_factory=dict_row) as conn:
            with conn.cursor() as cur:
                cur.execute(
                    f"""
                    SELECT COUNT(*) AS value
                    FROM document_chunks dc
                    LEFT JOIN chunk_validations cv ON cv.chunk_id = dc.chunk_id
                    {where_clause}
                    """,
                    tuple(params),
                )
                return cur.fetchone()["value"]

    def list_chunks(
        self,
        document_id: str | None = None,
        status: str | None = None,
        limit: int = 100,
        offset: int = 0,
        chunk_role: str | None = None,
        section_query: str | None = None,
    ) -> list[dict]:
        clauses: list[str] = []
        params: list[object] = []
        if document_id:
            clauses.append("dc.document_id = %s")
            params.append(document_id)
        if status:
            clauses.append("COALESCE(cv.status, dc.status) = %s")
            params.append(status)
        if chunk_role:
            clauses.append("COALESCE(dc.metadata_json->>'chunk_role', '') = %s")
            params.append(chunk_role)
        if section_query:
            clauses.append("COALESCE(dc.metadata_json->>'section_title', '') ILIKE %s")
            params.append(f"%{section_query}%")
        where_clause = f"WHERE {' AND '.join(clauses)}" if clauses else ""
        params.extend([limit, offset])

        with psycopg.connect(self.dsn, row_factory=dict_row) as conn:
            with conn.cursor() as cur:
                cur.execute(
                    f"""
                    SELECT
                      dc.chunk_id,
                      dc.document_id,
                      dc.chunk_index,
                      dc.page_start,
                      dc.page_end,
                      dc.section_path,
                      dc.prev_chunk_id,
                      dc.next_chunk_id,
                      dc.metadata_json,
                      COALESCE(cv.status, dc.status) AS validation_status,
                      cv.quality_score,
                      cv.reasons,
                      dc.text
                    FROM document_chunks dc
                    LEFT JOIN chunk_validations cv ON cv.chunk_id = dc.chunk_id
                    {where_clause}
                    ORDER BY dc.document_id, dc.chunk_index
                    LIMIT %s OFFSET %s
                    """,
                    tuple(params),
                )
                return [dict(row) for row in cur.fetchall()]

    def list_chunk_records(
        self,
        document_id: str | None = None,
        status: str | None = None,
        limit: int = 200,
        offset: int = 0,
    ) -> list[dict]:
        clauses: list[str] = []
        params: list[object] = []
        if document_id:
            clauses.append("dc.document_id = %s")
            params.append(document_id)
        if status:
            clauses.append("COALESCE(cv.status, dc.status) = %s")
            params.append(status)
        params.extend([limit, offset])
        where_clause = f"WHERE {' AND '.join(clauses)}" if clauses else ""

        with psycopg.connect(self.dsn, row_factory=dict_row) as conn:
            with conn.cursor() as cur:
                cur.execute(
                    f"""
                    SELECT
                      dc.chunk_id,
                      dc.document_id,
                      dc.tenant_id,
                      dc.chunk_index,
                      dc.page_start,
                      dc.page_end,
                      dc.section_path,
                      dc.prev_chunk_id,
                      dc.next_chunk_id,
                      dc.char_start,
                      dc.char_end,
                      dc.content_type,
                      dc.text,
                      dc.parser_version,
                      dc.chunker_version,
                      dc.metadata_json,
                      COALESCE(cv.status, dc.status) AS validation_status,
                      cv.quality_score,
                      cv.reasons
                    FROM document_chunks dc
                    LEFT JOIN chunk_validations cv ON cv.chunk_id = dc.chunk_id
                    {where_clause}
                    ORDER BY dc.document_id, dc.chunk_index
                    LIMIT %s OFFSET %s
                    """,
                    tuple(params),
                )
                return [dict(row) for row in cur.fetchall()]

    def list_chunk_records_for_kg(
        self,
        document_id: str | None = None,
        limit: int = 200,
        offset: int = 0,
    ) -> list[dict]:
        clauses = ["COALESCE(cv.status, dc.status) = 'accepted'"]
        params: list[object] = []
        if document_id:
            clauses.append("dc.document_id = %s")
            params.append(document_id)
        params.extend([limit, offset])
        where_clause = f"WHERE {' AND '.join(clauses)}"

        with psycopg.connect(self.dsn, row_factory=dict_row) as conn:
            with conn.cursor() as cur:
                cur.execute(
                    f"""
                    SELECT
                      dc.chunk_id,
                      dc.document_id,
                      dc.tenant_id,
                      dc.chunk_index,
                      dc.page_start,
                      dc.page_end,
                      dc.section_path,
                      dc.prev_chunk_id,
                      dc.next_chunk_id,
                      dc.char_start,
                      dc.char_end,
                      dc.content_type,
                      dc.text,
                      dc.parser_version,
                      dc.chunker_version,
                      dc.metadata_json,
                      COALESCE(cv.status, dc.status) AS validation_status,
                      cv.quality_score,
                      cv.reasons
                    FROM document_chunks dc
                    LEFT JOIN chunk_validations cv ON cv.chunk_id = dc.chunk_id
                    {where_clause}
                    ORDER BY dc.document_id, dc.chunk_index
                    LIMIT %s OFFSET %s
                    """,
                    tuple(params),
                )
                return [dict(row) for row in cur.fetchall()]

    def list_pending_kg_chunk_records(
        self,
        document_id: str,
        limit: int = 200,
        offset: int = 0,
    ) -> list[dict]:
        with psycopg.connect(self.dsn, row_factory=dict_row) as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT
                      dc.chunk_id,
                      dc.document_id,
                      dc.tenant_id,
                      dc.chunk_index,
                      dc.page_start,
                      dc.page_end,
                      dc.section_path,
                      dc.prev_chunk_id,
                      dc.next_chunk_id,
                      dc.char_start,
                      dc.char_end,
                      dc.content_type,
                      dc.text,
                      dc.parser_version,
                      dc.chunker_version,
                      dc.metadata_json,
                      COALESCE(cv.status, dc.status) AS validation_status,
                      cv.quality_score,
                      cv.reasons
                    FROM document_chunks dc
                    LEFT JOIN chunk_validations cv ON cv.chunk_id = dc.chunk_id
                    LEFT JOIN kg_raw_extractions kg ON kg.chunk_id = dc.chunk_id
                    WHERE dc.document_id = %s
                      AND COALESCE(cv.status, dc.status) = 'accepted'
                      AND kg.chunk_id IS NULL
                    ORDER BY dc.chunk_index
                    LIMIT %s OFFSET %s
                    """,
                    (document_id, limit, offset),
                )
                return [dict(row) for row in cur.fetchall()]

    def list_review_chunk_records(
        self,
        document_id: str | None = None,
        limit: int = 200,
        offset: int = 0,
    ) -> list[dict]:
        clauses = ["COALESCE(cv.status, dc.status) = 'review'"]
        params: list[object] = []
        if document_id:
            clauses.append("dc.document_id = %s")
            params.append(document_id)
        params.extend([limit, offset])
        where_clause = f"WHERE {' AND '.join(clauses)}"

        with psycopg.connect(self.dsn, row_factory=dict_row) as conn:
            with conn.cursor() as cur:
                cur.execute(
                    f"""
                    SELECT
                      dc.chunk_id,
                      dc.document_id,
                      dc.tenant_id,
                      dc.chunk_index,
                      dc.page_start,
                      dc.page_end,
                      dc.section_path,
                      dc.prev_chunk_id,
                      dc.next_chunk_id,
                      dc.char_start,
                      dc.char_end,
                      dc.content_type,
                      dc.text,
                      dc.parser_version,
                      dc.chunker_version,
                      dc.metadata_json,
                      COALESCE(cv.status, dc.status) AS validation_status,
                      cv.quality_score,
                      cv.reasons
                    FROM document_chunks dc
                    LEFT JOIN chunk_validations cv ON cv.chunk_id = dc.chunk_id
                    {where_clause}
                    ORDER BY dc.document_id, dc.chunk_index
                    LIMIT %s OFFSET %s
                    """,
                    tuple(params),
                )
                return [dict(row) for row in cur.fetchall()]

    def count_kg_entities(self, document_id: str | None = None, search: str | None = None, entity_type: str | None = None) -> int:
        clauses: list[str] = []
        params: list[object] = []
        if document_id:
            clauses.append("a.document_id = %s")
            params.append(document_id)
        if entity_type:
            clauses.append("e.entity_type = %s")
            params.append(entity_type)
        if search:
            clauses.append("(e.entity_id ILIKE %s OR e.canonical_name ILIKE %s OR e.entity_type ILIKE %s)")
            needle = f"%{search}%"
            params.extend([needle, needle, needle])
        where_clause = f"WHERE {' AND '.join(clauses)}" if clauses else ""
        with psycopg.connect(self.dsn, row_factory=dict_row) as conn:
            with conn.cursor() as cur:
                cur.execute(
                    f"""
                    SELECT COUNT(*) AS value
                    FROM (
                      SELECT e.entity_id
                      FROM kg_entities e
                      LEFT JOIN kg_assertions a
                        ON a.subject_entity_id = e.entity_id
                        OR a.object_entity_id = e.entity_id
                      {where_clause}
                      GROUP BY e.entity_id
                    ) entity_counts
                    """,
                    tuple(params),
                )
                return cur.fetchone()["value"]

    def list_kg_entities(
        self,
        document_id: str | None = None,
        search: str | None = None,
        entity_type: str | None = None,
        limit: int = 100,
        offset: int = 0,
    ) -> list[dict]:
        with psycopg.connect(self.dsn, row_factory=dict_row) as conn:
            with conn.cursor() as cur:
                clauses: list[str] = []
                params: list[object] = []
                if document_id:
                    clauses.append("a.document_id = %s")
                    params.append(document_id)
                if entity_type:
                    clauses.append("e.entity_type = %s")
                    params.append(entity_type)
                if search:
                    clauses.append("(e.entity_id ILIKE %s OR e.canonical_name ILIKE %s OR e.entity_type ILIKE %s)")
                    needle = f"%{search}%"
                    params.extend([needle, needle, needle])
                where_clause = f"WHERE {' AND '.join(clauses)}" if clauses else ""
                cur.execute(
                    f"""
                        SELECT
                          e.entity_id,
                          e.canonical_name,
                          e.entity_type,
                          e.source,
                          e.created_at,
                          e.updated_at,
                          COUNT(DISTINCT a.assertion_id) AS assertion_count,
                          COUNT(DISTINCT a.document_id) AS document_count
                        FROM kg_entities e
                        LEFT JOIN kg_assertions a
                          ON a.subject_entity_id = e.entity_id
                          OR a.object_entity_id = e.entity_id
                        {where_clause}
                        GROUP BY e.entity_id, e.canonical_name, e.entity_type, e.source, e.created_at, e.updated_at
                        ORDER BY assertion_count DESC, updated_at DESC
                        LIMIT %s OFFSET %s
                        """,
                    tuple(params + [limit, offset]),
                )
                return [dict(row) for row in cur.fetchall()]

    def get_kg_entity_detail(self, entity_id: str) -> dict | None:
        with psycopg.connect(self.dsn, row_factory=dict_row) as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT entity_id, canonical_name, entity_type, source, created_at, updated_at
                    FROM kg_entities
                    WHERE entity_id = %s
                    """,
                    (entity_id,),
                )
                entity = cur.fetchone()
                if entity is None:
                    return None

                cur.execute(
                    """
                    SELECT
                      assertion_id,
                      document_id,
                      chunk_id,
                      subject_entity_id,
                      predicate,
                      object_entity_id,
                      object_literal,
                      confidence,
                      qualifiers,
                      status,
                      created_at
                    FROM kg_assertions
                    WHERE subject_entity_id = %s OR object_entity_id = %s
                    ORDER BY created_at DESC
                    LIMIT 200
                    """,
                    (entity_id, entity_id),
                )
                assertions = [dict(row) for row in cur.fetchall()]

                assertion_ids = [row["assertion_id"] for row in assertions]
                chunks = []
                evidence = []
                documents = []
                if assertion_ids:
                    cur.execute(
                        """
                        SELECT evidence_id, assertion_id, excerpt, start_offset, end_offset, created_at
                        FROM kg_assertion_evidence
                        WHERE assertion_id = ANY(%s)
                        ORDER BY created_at DESC
                        LIMIT 200
                        """,
                        (assertion_ids,),
                    )
                    evidence = [dict(row) for row in cur.fetchall()]

                    cur.execute(
                        """
                        SELECT DISTINCT
                          dc.chunk_id,
                          dc.document_id,
                          dc.chunk_index,
                          dc.page_start,
                          dc.page_end,
                          dc.text,
                          dc.metadata_json,
                          COALESCE(cv.status, dc.status) AS validation_status
                        FROM document_chunks dc
                        LEFT JOIN chunk_validations cv ON cv.chunk_id = dc.chunk_id
                        JOIN kg_assertions a ON a.chunk_id = dc.chunk_id
                        WHERE a.assertion_id = ANY(%s)
                        ORDER BY dc.document_id, dc.chunk_index
                        LIMIT 200
                        """,
                        (assertion_ids,),
                    )
                    chunks = [dict(row) for row in cur.fetchall()]

                    cur.execute(
                        """
                        SELECT DISTINCT d.document_id, d.filename, d.document_class, d.tenant_id, d.status
                        FROM documents d
                        JOIN kg_assertions a ON a.document_id = d.document_id
                        WHERE a.assertion_id = ANY(%s)
                        ORDER BY d.filename
                        LIMIT 50
                        """,
                        (assertion_ids,),
                    )
                    documents = [dict(row) for row in cur.fetchall()]

        return {
            "entity": dict(entity),
            "assertions": assertions,
            "evidence": evidence,
            "chunks": chunks,
            "documents": documents,
        }

    def get_kg_entity_record(self, entity_id: str) -> dict | None:
        with psycopg.connect(self.dsn, row_factory=dict_row) as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT entity_id, canonical_name, entity_type, source, created_at, updated_at
                    FROM kg_entities
                    WHERE entity_id = %s
                    """,
                    (entity_id,),
                )
                row = cur.fetchone()
                return dict(row) if row else None

    def update_kg_entity(self, entity_id: str, patch: dict) -> dict | None:
        assignments, params = self._build_allowed_patch(
            patch,
            allowed_fields={
                "canonical_name": "canonical_name",
                "entity_type": "entity_type",
                "source": "source",
            },
            text_fields={"canonical_name", "entity_type", "source"},
        )
        with psycopg.connect(self.dsn, row_factory=dict_row) as conn:
            with conn.cursor() as cur:
                cur.execute(
                    f"""
                    UPDATE kg_entities
                    SET {", ".join(assignments)}, updated_at = now()
                    WHERE entity_id = %s
                    RETURNING entity_id, canonical_name, entity_type, source, created_at, updated_at
                    """,
                    tuple(params + [entity_id]),
                )
                row = cur.fetchone()
            conn.commit()
        return dict(row) if row else None

    def delete_kg_entity(self, entity_id: str) -> int:
        with psycopg.connect(self.dsn) as conn:
            with conn.cursor() as cur:
                cur.execute("DELETE FROM kg_entities WHERE entity_id = %s", (entity_id,))
                deleted = cur.rowcount
            conn.commit()
        return deleted

    def count_kg_assertions(
        self,
        document_id: str | None = None,
        entity_id: str | None = None,
        predicate: str | None = None,
        status: str | None = None,
        chunk_id: str | None = None,
    ) -> int:
        clauses: list[str] = []
        params: list[object] = []
        if document_id:
            clauses.append("document_id = %s")
            params.append(document_id)
        if entity_id:
            clauses.append("(subject_entity_id = %s OR object_entity_id = %s)")
            params.extend([entity_id, entity_id])
        if predicate:
            clauses.append("predicate ILIKE %s")
            params.append(f"%{predicate}%")
        if status:
            clauses.append("status = %s")
            params.append(status)
        if chunk_id:
            clauses.append("chunk_id = %s")
            params.append(chunk_id)
        where_clause = f"WHERE {' AND '.join(clauses)}" if clauses else ""
        with psycopg.connect(self.dsn, row_factory=dict_row) as conn:
            with conn.cursor() as cur:
                cur.execute(f"SELECT COUNT(*) AS value FROM kg_assertions {where_clause}", tuple(params))
                return cur.fetchone()["value"]

    def list_kg_assertions(
        self,
        document_id: str | None = None,
        entity_id: str | None = None,
        predicate: str | None = None,
        status: str | None = None,
        chunk_id: str | None = None,
        limit: int = 100,
        offset: int = 0,
    ) -> list[dict]:
        with psycopg.connect(self.dsn, row_factory=dict_row) as conn:
            with conn.cursor() as cur:
                clauses: list[str] = []
                params: list[object] = []
                if document_id:
                    clauses.append("document_id = %s")
                    params.append(document_id)
                if entity_id:
                    clauses.append("(subject_entity_id = %s OR object_entity_id = %s)")
                    params.extend([entity_id, entity_id])
                if predicate:
                    clauses.append("predicate ILIKE %s")
                    params.append(f"%{predicate}%")
                if status:
                    clauses.append("status = %s")
                    params.append(status)
                if chunk_id:
                    clauses.append("chunk_id = %s")
                    params.append(chunk_id)
                where_clause = f"WHERE {' AND '.join(clauses)}" if clauses else ""
                cur.execute(
                    f"""
                    SELECT assertion_id, document_id, chunk_id, subject_entity_id, predicate, object_entity_id, object_literal, confidence, qualifiers, status, created_at
                    FROM kg_assertions
                    {where_clause}
                    ORDER BY created_at DESC
                    LIMIT %s OFFSET %s
                    """,
                    tuple(params + [limit, offset]),
                )
                return [dict(row) for row in cur.fetchall()]

    def get_kg_assertion(self, assertion_id: str) -> dict | None:
        with psycopg.connect(self.dsn, row_factory=dict_row) as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT
                      assertion_id,
                      document_id,
                      chunk_id,
                      subject_entity_id,
                      predicate,
                      object_entity_id,
                      object_literal,
                      confidence,
                      qualifiers,
                      status,
                      created_at
                    FROM kg_assertions
                    WHERE assertion_id = %s
                    """,
                    (assertion_id,),
                )
                row = cur.fetchone()
                return dict(row) if row else None

    def update_kg_assertion(self, assertion_id: str, patch: dict) -> dict | None:
        assignments, params = self._build_allowed_patch(
            patch,
            allowed_fields={
                "document_id": "document_id",
                "chunk_id": "chunk_id",
                "subject_entity_id": "subject_entity_id",
                "predicate": "predicate",
                "object_entity_id": "object_entity_id",
                "object_literal": "object_literal",
                "confidence": "confidence",
                "qualifiers": "qualifiers",
                "status": "status",
            },
            json_fields={"qualifiers"},
            text_fields={
                "document_id",
                "chunk_id",
                "subject_entity_id",
                "predicate",
                "object_entity_id",
                "object_literal",
                "status",
            },
        )
        with psycopg.connect(self.dsn, row_factory=dict_row) as conn:
            with conn.cursor() as cur:
                cur.execute(
                    f"""
                    UPDATE kg_assertions
                    SET {", ".join(assignments)}
                    WHERE assertion_id = %s
                    RETURNING
                      assertion_id,
                      document_id,
                      chunk_id,
                      subject_entity_id,
                      predicate,
                      object_entity_id,
                      object_literal,
                      confidence,
                      qualifiers,
                      status,
                      created_at
                    """,
                    tuple(params + [assertion_id]),
                )
                row = cur.fetchone()
            conn.commit()
        return dict(row) if row else None

    def delete_kg_assertion(self, assertion_id: str) -> int:
        with psycopg.connect(self.dsn) as conn:
            with conn.cursor() as cur:
                cur.execute("DELETE FROM kg_assertions WHERE assertion_id = %s", (assertion_id,))
                deleted = cur.rowcount
            conn.commit()
        return deleted

    def list_kg_evidence(self, document_id: str | None = None, limit: int = 100) -> list[dict]:
        with psycopg.connect(self.dsn, row_factory=dict_row) as conn:
            with conn.cursor() as cur:
                if document_id:
                    cur.execute(
                        """
                        SELECT
                          e.evidence_id,
                          e.assertion_id,
                          a.document_id,
                          a.chunk_id,
                          a.subject_entity_id,
                          a.predicate,
                          a.object_entity_id,
                          a.object_literal,
                          e.excerpt,
                          e.start_offset,
                          e.end_offset,
                          e.created_at
                        FROM kg_assertion_evidence e
                        JOIN kg_assertions a ON a.assertion_id = e.assertion_id
                        WHERE a.document_id = %s
                        ORDER BY e.created_at DESC
                        LIMIT %s
                        """,
                        (document_id, limit),
                    )
                else:
                    cur.execute(
                        """
                        SELECT
                          e.evidence_id,
                          e.assertion_id,
                          a.document_id,
                          a.chunk_id,
                          a.subject_entity_id,
                          a.predicate,
                          a.object_entity_id,
                          a.object_literal,
                          e.excerpt,
                          e.start_offset,
                          e.end_offset,
                          e.created_at
                        FROM kg_assertion_evidence e
                        JOIN kg_assertions a ON a.assertion_id = e.assertion_id
                        ORDER BY e.created_at DESC
                        LIMIT %s
                        """,
                        (limit,),
                    )
                return [dict(row) for row in cur.fetchall()]

    def count_kg_raw_extractions(
        self,
        document_id: str | None = None,
        chunk_id: str | None = None,
        status: str | None = None,
    ) -> int:
        clauses: list[str] = []
        params: list[object] = []
        if document_id:
            clauses.append("document_id = %s")
            params.append(document_id)
        if chunk_id:
            clauses.append("chunk_id = %s")
            params.append(chunk_id)
        if status:
            clauses.append("status = %s")
            params.append(status)
        where_clause = f"WHERE {' AND '.join(clauses)}" if clauses else ""
        with psycopg.connect(self.dsn, row_factory=dict_row) as conn:
            with conn.cursor() as cur:
                cur.execute(f"SELECT COUNT(*) AS value FROM kg_raw_extractions {where_clause}", tuple(params))
                return cur.fetchone()["value"]

    def list_kg_raw_extractions(
        self,
        document_id: str | None = None,
        chunk_id: str | None = None,
        status: str | None = None,
        limit: int = 100,
        offset: int = 0,
    ) -> list[dict]:
        clauses: list[str] = []
        params: list[object] = []
        if document_id:
            clauses.append("document_id = %s")
            params.append(document_id)
        if chunk_id:
            clauses.append("chunk_id = %s")
            params.append(chunk_id)
        if status:
            clauses.append("status = %s")
            params.append(status)
        where_clause = f"WHERE {' AND '.join(clauses)}" if clauses else ""
        params.extend([limit, offset])

        with psycopg.connect(self.dsn, row_factory=dict_row) as conn:
            with conn.cursor() as cur:
                cur.execute(
                    f"""
                    SELECT extraction_id, chunk_id, document_id, status, payload, created_at
                    FROM kg_raw_extractions
                    {where_clause}
                    ORDER BY created_at DESC
                    LIMIT %s OFFSET %s
                    """,
                    tuple(params),
                )
                return [dict(row) for row in cur.fetchall()]

    def get_kg_raw_extraction(self, extraction_id: str) -> dict | None:
        with psycopg.connect(self.dsn, row_factory=dict_row) as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT extraction_id, chunk_id, document_id, payload, status, created_at
                    FROM kg_raw_extractions
                    WHERE extraction_id = %s
                    """,
                    (extraction_id,),
                )
                row = cur.fetchone()
                return dict(row) if row else None

    def update_kg_raw_extraction(self, extraction_id: str, patch: dict) -> dict | None:
        assignments, params = self._build_allowed_patch(
            patch,
            allowed_fields={"payload": "payload", "status": "status"},
            json_fields={"payload"},
            text_fields={"status"},
        )
        with psycopg.connect(self.dsn, row_factory=dict_row) as conn:
            with conn.cursor() as cur:
                cur.execute(
                    f"""
                    UPDATE kg_raw_extractions
                    SET {", ".join(assignments)}
                    WHERE extraction_id = %s
                    RETURNING extraction_id, chunk_id, document_id, payload, status, created_at
                    """,
                    tuple(params + [extraction_id]),
                )
                row = cur.fetchone()
            conn.commit()
        return dict(row) if row else None

    def delete_kg_raw_extraction(self, extraction_id: str) -> int:
        with psycopg.connect(self.dsn) as conn:
            with conn.cursor() as cur:
                cur.execute("DELETE FROM kg_raw_extractions WHERE extraction_id = %s", (extraction_id,))
                deleted = cur.rowcount
            conn.commit()
        return deleted

    def save_chunk_review_run(
        self,
        document_id: str,
        chunk_id: str,
        provider: str,
        model: str,
        prompt_version: str,
        decision: str,
        confidence: float,
        detected_role: str,
        reason: str,
        payload: dict,
    ) -> None:
        # Review runs are append-only audit records so automatic decisions remain
        # inspectable even after the chunk's current status changes.
        with psycopg.connect(self.dsn) as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO chunk_review_runs (
                      chunk_id, document_id, provider, model, prompt_version,
                      decision, confidence, detected_role, reason, payload
                    )
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    """,
                    (
                        chunk_id,
                        document_id,
                        provider,
                        model,
                        prompt_version,
                        decision,
                        confidence,
                        detected_role,
                        self._sanitize_text(reason),
                        json.dumps(self._sanitize_json_value(payload)),
                    ),
                )
            conn.commit()

    def count_chunk_review_runs(self, document_id: str | None = None, decision: str | None = None) -> int:
        clauses: list[str] = []
        params: list[object] = []
        if document_id:
            clauses.append("document_id = %s")
            params.append(document_id)
        if decision:
            clauses.append("decision = %s")
            params.append(decision)
        where_clause = f"WHERE {' AND '.join(clauses)}" if clauses else ""
        with psycopg.connect(self.dsn, row_factory=dict_row) as conn:
            with conn.cursor() as cur:
                cur.execute(f"SELECT COUNT(*) AS value FROM chunk_review_runs {where_clause}", tuple(params))
                return cur.fetchone()["value"]

    def list_chunk_review_runs(
        self,
        document_id: str | None = None,
        decision: str | None = None,
        limit: int = 100,
        offset: int = 0,
    ) -> list[dict]:
        clauses: list[str] = []
        params: list[object] = []
        if document_id:
            clauses.append("document_id = %s")
            params.append(document_id)
        if decision:
            clauses.append("decision = %s")
            params.append(decision)
        where_clause = f"WHERE {' AND '.join(clauses)}" if clauses else ""
        params.extend([limit, offset])
        with psycopg.connect(self.dsn, row_factory=dict_row) as conn:
            with conn.cursor() as cur:
                cur.execute(
                    f"""
                    SELECT
                      review_run_id,
                      chunk_id,
                      document_id,
                      provider,
                      model,
                      prompt_version,
                      decision,
                      confidence,
                      detected_role,
                      reason,
                      payload,
                      created_at
                    FROM chunk_review_runs
                    {where_clause}
                    ORDER BY created_at DESC
                    LIMIT %s OFFSET %s
                    """,
                    tuple(params),
                )
                return [dict(row) for row in cur.fetchall()]

    def delete_chunk_kg(self, chunk_id: str) -> None:
        with psycopg.connect(self.dsn) as conn:
            with conn.cursor() as cur:
                cur.execute("DELETE FROM kg_raw_extractions WHERE chunk_id = %s", (chunk_id,))
                cur.execute("DELETE FROM kg_assertions WHERE chunk_id = %s", (chunk_id,))
            conn.commit()
        self.prune_orphan_kg_entities()

    def count_chunk_metadata(self, document_id: str | None = None, status: str | None = None) -> int:
        clauses: list[str] = []
        params: list[object] = []
        if document_id:
            clauses.append("document_id = %s")
            params.append(document_id)
        if status:
            clauses.append("validation_status = %s")
            params.append(status)
        where_clause = f"WHERE {' AND '.join(clauses)}" if clauses else ""
        with psycopg.connect(self.dsn, row_factory=dict_row) as conn:
            with conn.cursor() as cur:
                cur.execute(f"SELECT COUNT(*) AS value FROM chunk_metadata_view {where_clause}", tuple(params))
                return cur.fetchone()["value"]

    def list_chunk_metadata(
        self,
        document_id: str | None = None,
        status: str | None = None,
        limit: int = 100,
        offset: int = 0,
        chunk_role: str | None = None,
        section_query: str | None = None,
    ) -> list[dict]:
        clauses: list[str] = []
        params: list[object] = []
        if document_id:
            clauses.append("document_id = %s")
            params.append(document_id)
        if status:
            clauses.append("validation_status = %s")
            params.append(status)
        if chunk_role:
            clauses.append("chunk_role = %s")
            params.append(chunk_role)
        if section_query:
            clauses.append("COALESCE(section_title, '') ILIKE %s")
            params.append(f"%{section_query}%")
        where_clause = f"WHERE {' AND '.join(clauses)}" if clauses else ""
        params.extend([limit, offset])

        with psycopg.connect(self.dsn, row_factory=dict_row) as conn:
            with conn.cursor() as cur:
                cur.execute(
                    f"""
                    SELECT
                      chunk_id,
                      document_id,
                      filename,
                      document_class,
                      tenant_id,
                      chunk_index,
                      page_start,
                      page_end,
                      prev_chunk_id,
                      next_chunk_id,
                      parser_version,
                      chunker_version,
                      validation_status,
                      quality_score,
                      reasons,
                      chunk_role,
                      section_title,
                      metadata_document_class,
                      ontology_classes,
                      metadata_json
                    FROM chunk_metadata_view
                    {where_clause}
                    ORDER BY document_id, chunk_index
                    LIMIT %s OFFSET %s
                    """,
                    tuple(params),
                )
                return [dict(row) for row in cur.fetchall()]

    def get_chunk_record(self, chunk_id: str) -> dict | None:
        with psycopg.connect(self.dsn, row_factory=dict_row) as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT
                      dc.chunk_id,
                      dc.document_id,
                      dc.tenant_id,
                      dc.chunk_index,
                      dc.page_start,
                      dc.page_end,
                      dc.section_path,
                      dc.prev_chunk_id,
                      dc.next_chunk_id,
                      dc.char_start,
                      dc.char_end,
                      dc.content_type,
                      dc.text,
                      dc.parser_version,
                      dc.chunker_version,
                      dc.metadata_json,
                      COALESCE(cv.status, dc.status) AS validation_status,
                      cv.quality_score,
                      cv.reasons
                    FROM document_chunks dc
                    LEFT JOIN chunk_validations cv ON cv.chunk_id = dc.chunk_id
                    WHERE dc.chunk_id = %s
                    """,
                    (chunk_id,),
                )
                row = cur.fetchone()
                return dict(row) if row else None

    def list_document_chunk_records(self, document_id: str | None = None, status: str | None = None) -> list[dict]:
        clauses: list[str] = []
        params: list[object] = []
        if document_id:
            clauses.append("dc.document_id = %s")
            params.append(document_id)
        if status:
            clauses.append("COALESCE(cv.status, dc.status) = %s")
            params.append(status)
        where_clause = f"WHERE {' AND '.join(clauses)}" if clauses else ""

        with psycopg.connect(self.dsn, row_factory=dict_row) as conn:
            with conn.cursor() as cur:
                cur.execute(
                    f"""
                    SELECT
                      dc.chunk_id,
                      dc.document_id,
                      dc.tenant_id,
                      dc.chunk_index,
                      dc.page_start,
                      dc.page_end,
                      dc.section_path,
                      dc.prev_chunk_id,
                      dc.next_chunk_id,
                      dc.char_start,
                      dc.char_end,
                      dc.content_type,
                      dc.text,
                      dc.parser_version,
                      dc.chunker_version,
                      dc.metadata_json,
                      COALESCE(cv.status, dc.status) AS validation_status,
                      cv.quality_score,
                      cv.reasons
                    FROM document_chunks dc
                    LEFT JOIN chunk_validations cv ON cv.chunk_id = dc.chunk_id
                    {where_clause}
                    ORDER BY dc.document_id, dc.chunk_index
                    """,
                    tuple(params),
                )
                return [dict(row) for row in cur.fetchall()]

    def get_chunk_detail(self, chunk_id: str) -> dict | None:
        chunk = self.get_chunk_record(chunk_id)
        if chunk is None:
            return None

        with psycopg.connect(self.dsn, row_factory=dict_row) as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT
                      chunk_id,
                      document_id,
                      filename,
                      document_class,
                      tenant_id,
                      chunk_index,
                      page_start,
                      page_end,
                      prev_chunk_id,
                      next_chunk_id,
                      parser_version,
                      chunker_version,
                      validation_status,
                      quality_score,
                      reasons,
                      chunk_role,
                      section_title,
                      metadata_document_class,
                      ontology_classes,
                      metadata_json
                    FROM chunk_metadata_view
                    WHERE chunk_id = %s
                    """,
                    (chunk_id,),
                )
                metadata_row = cur.fetchone()

                cur.execute(
                    """
                    SELECT
                      assertion_id,
                      document_id,
                      chunk_id,
                      subject_entity_id,
                      predicate,
                      object_entity_id,
                      object_literal,
                      confidence,
                      qualifiers,
                      status,
                      created_at
                    FROM kg_assertions
                    WHERE chunk_id = %s
                    ORDER BY created_at DESC
                    LIMIT 200
                    """,
                    (chunk_id,),
                )
                assertions = [dict(row) for row in cur.fetchall()]

                assertion_ids = [row["assertion_id"] for row in assertions]

                cur.execute(
                    """
                    SELECT extraction_id, chunk_id, document_id, status, payload, created_at
                    FROM kg_raw_extractions
                    WHERE chunk_id = %s
                    ORDER BY created_at DESC
                    LIMIT 50
                    """,
                    (chunk_id,),
                )
                raw = [dict(row) for row in cur.fetchall()]

                evidence = []
                if assertion_ids:
                    cur.execute(
                        """
                        SELECT evidence_id, assertion_id, excerpt, start_offset, end_offset, created_at
                        FROM kg_assertion_evidence
                        WHERE assertion_id = ANY(%s)
                        ORDER BY created_at DESC
                        LIMIT 200
                        """,
                        (assertion_ids,),
                    )
                    evidence = [dict(row) for row in cur.fetchall()]

                neighbor_ids = [value for value in [chunk.get("prev_chunk_id"), chunk.get("next_chunk_id")] if value]
                neighbors = []
                if neighbor_ids:
                    cur.execute(
                        """
                        SELECT
                          dc.chunk_id,
                          dc.chunk_index,
                          dc.page_start,
                          dc.page_end,
                          COALESCE(cv.status, dc.status) AS validation_status,
                          dc.text
                        FROM document_chunks dc
                        LEFT JOIN chunk_validations cv ON cv.chunk_id = dc.chunk_id
                        WHERE dc.chunk_id = ANY(%s)
                        ORDER BY dc.chunk_index
                        """,
                        (neighbor_ids,),
                    )
                    neighbors = [dict(row) for row in cur.fetchall()]

                cur.execute(
                    """
                    SELECT
                      cal.link_id,
                      cal.chunk_id,
                      cal.asset_id,
                      cal.link_type,
                      cal.confidence,
                      cal.metadata_json,
                      pa.document_id,
                      pa.page_number,
                      pa.asset_index,
                      pa.asset_type,
                      pa.asset_path,
                      pa.ocr_text,
                      pa.description_text,
                      pa.search_text,
                      pa.bbox_json,
                      pa.metadata_json AS asset_metadata_json
                    FROM chunk_asset_links cal
                    JOIN page_assets pa ON pa.asset_id = cal.asset_id
                    WHERE cal.chunk_id = %s
                    ORDER BY pa.page_number, pa.asset_index
                    LIMIT 200
                    """,
                    (chunk_id,),
                )
                linked_assets = [dict(row) for row in cur.fetchall()]

        return {
            "chunk": chunk,
            "metadata": dict(metadata_row) if metadata_row else None,
            "assertions": assertions,
            "raw_extractions": raw,
            "evidence": evidence,
            "neighbors": neighbors,
            "linked_assets": linked_assets,
        }

    def get_page_asset_detail(self, asset_id: str) -> dict | None:
        with psycopg.connect(self.dsn, row_factory=dict_row) as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT
                      pa.asset_id,
                      pa.document_id,
                      pa.tenant_id,
                      pa.page_number,
                      pa.asset_index,
                      pa.asset_type,
                      pa.bbox_json,
                      pa.asset_path,
                      pa.content_hash,
                      pa.ocr_text,
                      pa.description_text,
                      pa.search_text,
                      pa.metadata_json,
                      d.filename,
                      d.document_class,
                      d.status
                    FROM page_assets pa
                    JOIN documents d ON d.document_id = pa.document_id
                    WHERE pa.asset_id = %s
                    """,
                    (asset_id,),
                )
                asset = cur.fetchone()
                if asset is None:
                    return None
                cur.execute(
                    """
                    SELECT
                      cal.link_id,
                      cal.chunk_id,
                      cal.link_type,
                      cal.confidence,
                      cal.metadata_json,
                      dc.chunk_index,
                      dc.page_start,
                      dc.page_end,
                      COALESCE(cv.status, dc.status) AS validation_status,
                      dc.text
                    FROM chunk_asset_links cal
                    JOIN document_chunks dc ON dc.chunk_id = cal.chunk_id
                    LEFT JOIN chunk_validations cv ON cv.chunk_id = dc.chunk_id
                    WHERE cal.asset_id = %s
                    ORDER BY dc.chunk_index
                    LIMIT 200
                    """,
                    (asset_id,),
                )
                links = [dict(row) for row in cur.fetchall()]
        return {"asset": dict(asset), "links": links}

    def get_chunk_asset_link(self, link_id: str) -> dict | None:
        with psycopg.connect(self.dsn, row_factory=dict_row) as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT
                      cal.link_id,
                      cal.chunk_id,
                      cal.asset_id,
                      cal.link_type,
                      cal.confidence,
                      cal.metadata_json,
                      cal.created_at,
                      dc.document_id,
                      pa.page_number,
                      pa.asset_type
                    FROM chunk_asset_links cal
                    JOIN document_chunks dc ON dc.chunk_id = cal.chunk_id
                    JOIN page_assets pa ON pa.asset_id = cal.asset_id
                    WHERE cal.link_id = %s
                    """,
                    (link_id,),
                )
                row = cur.fetchone()
                return dict(row) if row else None

    def update_page_asset(self, asset_id: str, patch: dict) -> dict | None:
        assignments, params = self._build_allowed_patch(
            patch,
            allowed_fields={
                "page_number": "page_number",
                "asset_index": "asset_index",
                "asset_type": "asset_type",
                "bbox_json": "bbox_json",
                "asset_path": "asset_path",
                "ocr_text": "ocr_text",
                "description_text": "description_text",
                "search_text": "search_text",
                "metadata_json": "metadata_json",
            },
            json_fields={"bbox_json", "metadata_json"},
            text_fields={"asset_type", "asset_path", "ocr_text", "description_text", "search_text"},
        )
        with psycopg.connect(self.dsn, row_factory=dict_row) as conn:
            with conn.cursor() as cur:
                cur.execute(
                    f"""
                    UPDATE page_assets
                    SET {", ".join(assignments)}, updated_at = now()
                    WHERE asset_id = %s
                    RETURNING
                      asset_id,
                      document_id,
                      tenant_id,
                      page_number,
                      asset_index,
                      asset_type,
                      bbox_json,
                      asset_path,
                      content_hash,
                      ocr_text,
                      description_text,
                      search_text,
                      metadata_json,
                      created_at,
                      updated_at
                    """,
                    tuple(params + [asset_id]),
                )
                row = cur.fetchone()
            conn.commit()
        return dict(row) if row else None

    def delete_page_asset(self, asset_id: str) -> int:
        with psycopg.connect(self.dsn) as conn:
            with conn.cursor() as cur:
                cur.execute("DELETE FROM page_assets WHERE asset_id = %s", (asset_id,))
                deleted = cur.rowcount
            conn.commit()
        return deleted

    def update_chunk_asset_link(self, link_id: str, patch: dict) -> dict | None:
        assignments, params = self._build_allowed_patch(
            patch,
            allowed_fields={
                "chunk_id": "chunk_id",
                "asset_id": "asset_id",
                "link_type": "link_type",
                "confidence": "confidence",
                "metadata_json": "metadata_json",
            },
            json_fields={"metadata_json"},
            text_fields={"chunk_id", "asset_id", "link_type"},
        )
        with psycopg.connect(self.dsn, row_factory=dict_row) as conn:
            with conn.cursor() as cur:
                cur.execute(
                    f"""
                    UPDATE chunk_asset_links
                    SET {", ".join(assignments)}
                    WHERE link_id = %s
                    RETURNING
                      link_id,
                      chunk_id,
                      asset_id,
                      link_type,
                      confidence,
                      metadata_json,
                      created_at
                    """,
                    tuple(params + [link_id]),
                )
                row = cur.fetchone()
            conn.commit()
        return dict(row) if row else None

    def delete_chunk_asset_link(self, link_id: str) -> int:
        with psycopg.connect(self.dsn) as conn:
            with conn.cursor() as cur:
                cur.execute("DELETE FROM chunk_asset_links WHERE link_id = %s", (link_id,))
                deleted = cur.rowcount
            conn.commit()
        return deleted

    def update_chunk_validation(self, chunk_id: str, status: str, quality_score: float, reasons: list[str]) -> None:
        with psycopg.connect(self.dsn) as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO chunk_validations (chunk_id, status, quality_score, reasons)
                    VALUES (%s, %s, %s, %s)
                    ON CONFLICT (chunk_id) DO UPDATE SET
                      status = EXCLUDED.status,
                      quality_score = EXCLUDED.quality_score,
                      reasons = EXCLUDED.reasons
                    """,
                    (chunk_id, status, quality_score, json.dumps(self._sanitize_json_value(reasons))),
                )
                cur.execute(
                    "UPDATE document_chunks SET status = %s, updated_at = now() WHERE chunk_id = %s",
                    (status, chunk_id),
                    )
            conn.commit()

    def update_chunk_metadata(self, chunk_id: str, metadata: dict) -> None:
        with psycopg.connect(self.dsn) as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    UPDATE document_chunks
                    SET metadata_json = %s, updated_at = now()
                    WHERE chunk_id = %s
                    """,
                    (json.dumps(self._sanitize_json_value(metadata)), chunk_id),
                )
            conn.commit()

    def update_chunk_record_admin(self, chunk_id: str, patch: dict) -> dict | None:
        chunk_fields = {
            "page_start": "page_start",
            "page_end": "page_end",
            "section_path": "section_path",
            "prev_chunk_id": "prev_chunk_id",
            "next_chunk_id": "next_chunk_id",
            "char_start": "char_start",
            "char_end": "char_end",
            "content_type": "content_type",
            "text": "text",
            "metadata_json": "metadata_json",
            "validation_status": "status",
        }
        validation_fields = {"validation_status", "quality_score", "reasons"}
        chunk_patch = {key: value for key, value in patch.items() if key in chunk_fields}
        current_record = self.get_chunk_record(chunk_id)
        if current_record is None:
            return None
        validation_status = patch.get("validation_status", current_record.get("validation_status"))
        quality_score = patch.get("quality_score", current_record.get("quality_score"))
        reasons = patch.get("reasons", current_record.get("reasons"))
        with psycopg.connect(self.dsn, row_factory=dict_row) as conn:
            with conn.cursor() as cur:
                if chunk_patch:
                    assignments, params = self._build_allowed_patch(
                        chunk_patch,
                        allowed_fields=chunk_fields,
                        json_fields={"metadata_json"},
                        array_fields={"section_path"},
                        text_fields={"prev_chunk_id", "next_chunk_id", "content_type", "text", "validation_status"},
                    )
                    cur.execute(
                        f"""
                        UPDATE document_chunks
                        SET {", ".join(assignments)}, updated_at = now()
                        WHERE chunk_id = %s
                        """,
                        tuple(params + [chunk_id]),
                    )
                if validation_fields & set(patch.keys()):
                    cur.execute(
                        """
                        INSERT INTO chunk_validations (chunk_id, status, quality_score, reasons)
                        VALUES (%s, %s, %s, %s)
                        ON CONFLICT (chunk_id) DO UPDATE SET
                          status = EXCLUDED.status,
                          quality_score = EXCLUDED.quality_score,
                          reasons = EXCLUDED.reasons
                        """,
                        (
                            chunk_id,
                            self._sanitize_text(str(validation_status or "review")),
                            float(quality_score if quality_score is not None else 0.0),
                            json.dumps(self._sanitize_json_value(list(reasons or []))),
                        ),
                    )
            conn.commit()
        return self.get_chunk_record(chunk_id)

    def delete_kg_for_chunk(self, chunk_id: str) -> None:
        with psycopg.connect(self.dsn) as conn:
            with conn.cursor() as cur:
                cur.execute("DELETE FROM kg_assertions WHERE chunk_id = %s", (chunk_id,))
                cur.execute("DELETE FROM kg_raw_extractions WHERE chunk_id = %s", (chunk_id,))
            conn.commit()

    def prune_orphan_kg_entities(self) -> int:
        # Entity cleanup is performed after KG deletion/replay so the graph does not
        # retain nodes that no longer participate in any accepted assertion.
        with psycopg.connect(self.dsn) as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    DELETE FROM kg_entities e
                    WHERE NOT EXISTS (
                      SELECT 1
                      FROM kg_assertions a
                      WHERE a.subject_entity_id = e.entity_id
                         OR a.object_entity_id = e.entity_id
                    )
                    """
                )
                deleted = cur.rowcount
            conn.commit()
        return deleted

    def create_agent_profile(
        self,
        tenant_id: str = "shared",
        display_name: str | None = None,
        auth_user_id: str | None = None,
    ) -> str:
        profile_id = str(uuid4())
        with psycopg.connect(self.dsn) as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO agent_profiles (profile_id, tenant_id, auth_user_id, display_name, status)
                    VALUES (%s, %s, %s, %s, 'active')
                    """,
                    (
                        profile_id,
                        tenant_id,
                        self._sanitize_text(auth_user_id or "") or None,
                        self._sanitize_text(display_name or ""),
                    ),
                )
            conn.commit()
        return profile_id

    def set_agent_profile_token(self, profile_id: str, token: str) -> None:
        with psycopg.connect(self.dsn) as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    UPDATE agent_profiles
                    SET profile_token_hash = %s,
                        profile_token_issued_at = now(),
                        updated_at = now()
                    WHERE profile_id = %s
                    """,
                    (self._token_hash(token), profile_id),
                )
            conn.commit()

    def verify_agent_profile_token(self, profile_id: str, token: str | None, tenant_id: str | None = None) -> bool:
        if not token:
            return False
        with psycopg.connect(self.dsn, row_factory=dict_row) as conn:
            with conn.cursor() as cur:
                clauses = ["profile_id = %s"]
                params: list[object] = [profile_id]
                if tenant_id is not None:
                    clauses.append("tenant_id = %s")
                    params.append(tenant_id)
                cur.execute(
                    f"""
                    SELECT tenant_id, profile_token_hash, profile_token_issued_at, updated_at
                    FROM agent_profiles
                    WHERE {' AND '.join(clauses)}
                    """,
                    tuple(params),
                )
                row = cur.fetchone()
                if not row:
                    return False
                stored = str(row.get("profile_token_hash") or "")
                if not stored:
                    return False
                issued_at = row.get("profile_token_issued_at") or row.get("updated_at")
                if isinstance(issued_at, datetime):
                    age_seconds = (datetime.now(timezone.utc) - issued_at.astimezone(timezone.utc)).total_seconds()
                    if age_seconds > settings.agent_profile_token_max_age_seconds:
                        return False
                return stored == self._token_hash(token)

    def get_agent_profile(self, profile_id: str, tenant_id: str | None = None) -> dict | None:
        with psycopg.connect(self.dsn, row_factory=dict_row) as conn:
            with conn.cursor() as cur:
                clauses = ["profile_id = %s"]
                params: list[object] = [profile_id]
                if tenant_id is not None:
                    clauses.append("tenant_id = %s")
                    params.append(tenant_id)
                cur.execute(
                    f"""
                    SELECT
                      profile_id,
                      tenant_id,
                      auth_user_id,
                      display_name,
                      status,
                      summary_json,
                      summary_text,
                      source_provider,
                      source_model,
                      prompt_version,
                      created_at,
                      updated_at
                    FROM agent_profiles
                    WHERE {' AND '.join(clauses)}
                    """,
                    tuple(params),
                )
                row = cur.fetchone()
                return dict(row) if row else None

    def get_agent_profile_by_auth_user(self, auth_user_id: str, tenant_id: str = "shared") -> dict | None:
        normalized_auth_user_id = self._sanitize_text(auth_user_id or "").strip()
        if not normalized_auth_user_id:
            return None
        with psycopg.connect(self.dsn, row_factory=dict_row) as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT
                      profile_id,
                      tenant_id,
                      auth_user_id,
                      display_name,
                      status,
                      summary_json,
                      summary_text,
                      source_provider,
                      source_model,
                      prompt_version,
                      created_at,
                      updated_at
                    FROM agent_profiles
                    WHERE tenant_id = %s
                      AND auth_user_id = %s
                    ORDER BY updated_at DESC, created_at DESC
                    LIMIT 1
                    """,
                    (tenant_id, normalized_auth_user_id),
                )
                row = cur.fetchone()
                return dict(row) if row else None

    def save_agent_profile(
        self,
        profile_id: str,
        summary_json: dict[str, Any],
        summary_text: str,
        source_provider: str,
        source_model: str,
        prompt_version: str,
        display_name: str | None = None,
    ) -> None:
        with psycopg.connect(self.dsn) as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    UPDATE agent_profiles
                    SET summary_json = %s,
                        summary_text = %s,
                        source_provider = %s,
                        source_model = %s,
                        prompt_version = %s,
                        display_name = COALESCE(NULLIF(%s, ''), display_name),
                        updated_at = now()
                    WHERE profile_id = %s
                    """,
                    (
                        json.dumps(self._sanitize_json_value(summary_json)),
                        self._sanitize_text(summary_text),
                        self._sanitize_text(source_provider),
                        self._sanitize_text(source_model),
                        self._sanitize_text(prompt_version),
                        self._sanitize_text(display_name or ""),
                        profile_id,
                    ),
                )
            conn.commit()

    def update_agent_profile_record(self, profile_id: str, patch: dict) -> dict | None:
        assignments, params = self._build_allowed_patch(
            patch,
            allowed_fields={
                "display_name": "display_name",
                "auth_user_id": "auth_user_id",
                "status": "status",
                "summary_json": "summary_json",
                "summary_text": "summary_text",
                "source_provider": "source_provider",
                "source_model": "source_model",
                "prompt_version": "prompt_version",
            },
            json_fields={"summary_json"},
            text_fields={"display_name", "auth_user_id", "status", "summary_text", "source_provider", "source_model", "prompt_version"},
        )
        with psycopg.connect(self.dsn, row_factory=dict_row) as conn:
            with conn.cursor() as cur:
                cur.execute(
                    f"""
                    UPDATE agent_profiles
                    SET {", ".join(assignments)}, updated_at = now()
                    WHERE profile_id = %s
                    RETURNING
                      profile_id,
                      tenant_id,
                      auth_user_id,
                      display_name,
                      status,
                      summary_json,
                      summary_text,
                      source_provider,
                      source_model,
                      prompt_version,
                      created_at,
                      updated_at
                    """,
                    tuple(params + [profile_id]),
                )
                row = cur.fetchone()
            conn.commit()
        return dict(row) if row else None

    def list_agent_profiles(self, tenant_id: str = "shared", status: str | None = None, limit: int = 100, offset: int = 0) -> list[dict]:
        clauses: list[str] = ["p.tenant_id = %s"]
        params: list[object] = [tenant_id]
        if status:
            clauses.append("p.status = %s")
            params.append(status)
        where_clause = "WHERE " + " AND ".join(clauses)
        with psycopg.connect(self.dsn, row_factory=dict_row) as conn:
            with conn.cursor() as cur:
                cur.execute(
                    f"""
                    SELECT
                      p.profile_id,
                      p.tenant_id,
                      p.auth_user_id,
                      p.display_name,
                      p.status,
                      p.summary_json,
                      p.summary_text,
                      p.source_provider,
                      p.source_model,
                      p.prompt_version,
                      p.created_at,
                      p.updated_at,
                      COUNT(DISTINCT s.session_id) AS session_count
                    FROM agent_profiles p
                    LEFT JOIN agent_sessions s ON s.profile_id = p.profile_id
                    {where_clause}
                    GROUP BY p.profile_id
                    ORDER BY p.updated_at DESC
                    LIMIT %s OFFSET %s
                    """,
                    tuple(params + [limit, offset]),
                )
                return [dict(row) for row in cur.fetchall()]

    def count_agent_profiles(self, tenant_id: str = "shared", status: str | None = None) -> int:
        clauses: list[str] = ["tenant_id = %s"]
        params: list[object] = [tenant_id]
        if status:
            clauses.append("status = %s")
            params.append(status)
        where_clause = "WHERE " + " AND ".join(clauses)
        return self._fetch_scalar(f"SELECT COUNT(*) AS value FROM agent_profiles {where_clause}", tuple(params))

    def delete_agent_profile(self, profile_id: str) -> int:
        with psycopg.connect(self.dsn) as conn:
            with conn.cursor() as cur:
                cur.execute("DELETE FROM agent_profiles WHERE profile_id = %s", (profile_id,))
                deleted = cur.rowcount
            conn.commit()
        return deleted

    def upsert_user_place(
        self,
        *,
        tenant_id: str,
        auth_user_id: str,
        external_place_id: str,
        place_name: str,
        status: str = "active",
        metadata_json: dict[str, Any] | None = None,
    ) -> dict:
        normalized_tenant_id = self._sanitize_text(tenant_id or "").strip() or "shared"
        normalized_auth_user_id = self._sanitize_text(auth_user_id or "").strip()
        normalized_external_place_id = self._sanitize_text(external_place_id or "").strip()[:160]
        normalized_place_name = self._sanitize_text(place_name or "").strip()[:160]
        normalized_status = self._normalize_sensor_status(status)
        if not normalized_auth_user_id:
            raise ValueError("auth_user_id is required")
        if not normalized_external_place_id:
            raise ValueError("external_place_id is required")
        if not normalized_place_name:
            raise ValueError("place_name is required")
        with psycopg.connect(self.dsn, row_factory=dict_row) as conn:
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
                        Jsonb(self._sanitize_json_value(metadata_json or {})),
                    ),
                )
                row = cur.fetchone()
            conn.commit()
        return dict(row) if row else {}

    def list_user_places(
        self,
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
            params.append(self._normalize_sensor_status(status))
        with psycopg.connect(self.dsn, row_factory=dict_row) as conn:
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

    def get_user_place(self, place_id: str, *, tenant_id: str, auth_user_id: str) -> dict | None:
        with psycopg.connect(self.dsn, row_factory=dict_row) as conn:
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

    def count_user_places(self, *, tenant_id: str, auth_user_id: str, status: str | None = None) -> int:
        clauses = ["tenant_id = %s", "auth_user_id = %s"]
        params: list[object] = [tenant_id, auth_user_id]
        if status:
            clauses.append("status = %s")
            params.append(self._normalize_sensor_status(status))
        return self._fetch_scalar(
            f"SELECT COUNT(*) AS value FROM user_places WHERE {' AND '.join(clauses)}",
            tuple(params),
        )

    def upsert_user_hive(
        self,
        *,
        tenant_id: str,
        auth_user_id: str,
        external_hive_id: str,
        hive_name: str,
        place_id: str | None = None,
        status: str = "active",
        metadata_json: dict[str, Any] | None = None,
    ) -> dict:
        normalized_tenant_id = self._sanitize_text(tenant_id or "").strip() or "shared"
        normalized_auth_user_id = self._sanitize_text(auth_user_id or "").strip()
        normalized_external_hive_id = self._sanitize_text(external_hive_id or "").strip()[:160]
        normalized_hive_name = self._sanitize_text(hive_name or "").strip()[:160]
        normalized_status = self._normalize_sensor_status(status)
        if not normalized_auth_user_id:
            raise ValueError("auth_user_id is required")
        if not normalized_external_hive_id:
            raise ValueError("external_hive_id is required")
        if not normalized_hive_name:
            raise ValueError("hive_name is required")
        place_row = None
        if place_id:
            place_row = self.get_user_place(place_id, tenant_id=normalized_tenant_id, auth_user_id=normalized_auth_user_id)
            if place_row is None:
                raise ValueError("Place not found")
        with psycopg.connect(self.dsn, row_factory=dict_row) as conn:
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
                        Jsonb(self._sanitize_json_value(metadata_json or {})),
                    ),
                )
                row = cur.fetchone()
            conn.commit()
        return dict(row) if row else {}

    def list_user_hives(
        self,
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
            params.append(self._normalize_sensor_status(status))
        if place_id:
            clauses.append("h.place_id = %s")
            params.append(place_id)
        with psycopg.connect(self.dsn, row_factory=dict_row) as conn:
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

    def get_user_hive(self, hive_id: str, *, tenant_id: str, auth_user_id: str) -> dict | None:
        with psycopg.connect(self.dsn, row_factory=dict_row) as conn:
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
        self,
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
            params.append(self._normalize_sensor_status(status))
        if place_id:
            clauses.append("place_id = %s")
            params.append(place_id)
        return self._fetch_scalar(
            f"SELECT COUNT(*) AS value FROM user_hives WHERE {' AND '.join(clauses)}",
            tuple(params),
        )

    def upsert_user_sensor(
        self,
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
        normalized_tenant_id = self._sanitize_text(tenant_id or "").strip() or "shared"
        normalized_auth_user_id = self._sanitize_text(auth_user_id or "").strip()
        normalized_external_sensor_id = self._sanitize_text(external_sensor_id or "").strip()[:160]
        normalized_sensor_name = self._sanitize_text(sensor_name or "").strip()[:160]
        normalized_sensor_type = self._sanitize_text(sensor_type or "environment").strip().lower()[:64] or "environment"
        normalized_status = self._normalize_sensor_status(status)
        place_row = None
        hive_row = None
        if not normalized_auth_user_id:
            raise ValueError("auth_user_id is required")
        if not normalized_external_sensor_id:
            raise ValueError("external_sensor_id is required")
        if not normalized_sensor_name:
            raise ValueError("sensor_name is required")
        if place_id:
            place_row = self.get_user_place(place_id, tenant_id=normalized_tenant_id, auth_user_id=normalized_auth_user_id)
            if place_row is None:
                raise ValueError("Place not found")
        if hive_id:
            hive_row = self.get_user_hive(hive_id, tenant_id=normalized_tenant_id, auth_user_id=normalized_auth_user_id)
            if hive_row is None:
                raise ValueError("Hive not found")
            hive_place_id = str(hive_row.get("place_id") or "").strip() or None
            if place_row and hive_place_id and hive_place_id != str(place_row.get("place_id") or ""):
                raise ValueError("Hive does not belong to the selected place")
            if place_row is None and hive_place_id:
                place_row = self.get_user_place(hive_place_id, tenant_id=normalized_tenant_id, auth_user_id=normalized_auth_user_id)
            elif place_row and not hive_place_id:
                raise ValueError("Hive must be assigned to the selected place before sensor registration")
        effective_hive_name = self._sanitize_text(str((hive_row or {}).get("hive_name") or hive_name or "")).strip()[:160] or None
        effective_location_label = self._sanitize_text(str((place_row or {}).get("place_name") or location_label or "")).strip()[:160] or None
        with psycopg.connect(self.dsn, row_factory=dict_row) as conn:
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
                        Jsonb(self._sanitize_json_value(metadata_json or {})),
                    ),
                )
                row = cur.fetchone()
            conn.commit()
        return dict(row) if row else {}

    def list_user_sensors(
        self,
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
            params.append(self._normalize_sensor_status(status))
        with psycopg.connect(self.dsn, row_factory=dict_row) as conn:
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
        for row in rows:
            resolved_place_name = str(row.get("resolved_place_name") or row.get("place_name") or row.get("location_label") or "").strip()
            resolved_hive_name = str(row.get("resolved_hive_name") or row.get("linked_hive_name") or row.get("hive_name") or "").strip()
            row["place_name"] = resolved_place_name
            row["location_label"] = resolved_place_name
            row["hive_name"] = resolved_hive_name
        return rows

    def get_user_sensor(self, sensor_id: str, *, tenant_id: str, auth_user_id: str) -> dict | None:
        with psycopg.connect(self.dsn, row_factory=dict_row) as conn:
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
        data = dict(row)
        resolved_place_name = str(data.get("resolved_place_name") or data.get("place_name") or data.get("location_label") or "").strip()
        resolved_hive_name = str(data.get("resolved_hive_name") or data.get("linked_hive_name") or data.get("hive_name") or "").strip()
        data["place_name"] = resolved_place_name
        data["location_label"] = resolved_place_name
        data["hive_name"] = resolved_hive_name
        return data

    def count_user_sensors(self, *, tenant_id: str, auth_user_id: str, status: str | None = None) -> int:
        clauses = ["tenant_id = %s", "auth_user_id = %s"]
        params: list[object] = [tenant_id, auth_user_id]
        if status:
            clauses.append("status = %s")
            params.append(self._normalize_sensor_status(status))
        return self._fetch_scalar(
            f"SELECT COUNT(*) AS value FROM user_sensors WHERE {' AND '.join(clauses)}",
            tuple(params),
        )

    def save_sensor_readings(
        self,
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
        sensor_row = self.get_user_sensor(sensor_id, tenant_id=tenant_id, auth_user_id=auth_user_id)
        if sensor_row is None:
            raise ValueError("Sensor not found")
        inserted: list[dict] = []
        with psycopg.connect(self.dsn, row_factory=dict_row) as conn:
            with conn.cursor() as cur:
                for reading in readings:
                    observed_at = self._coerce_sensor_observed_at(reading.get("observed_at"))
                    metric_name = self._sanitize_text(str(reading.get("metric_name") or "")).strip().lower()[:80]
                    unit = self._sanitize_text(str(reading.get("unit") or "")).strip()[:32] or None
                    text_value = self._sanitize_text(str(reading.get("text_value") or "")).strip()[:4000] or None
                    numeric_value = reading.get("numeric_value")
                    quality_score = reading.get("quality_score")
                    if not metric_name:
                        raise ValueError("metric_name is required for each reading")
                    if numeric_value in ("", None) and text_value is None:
                        raise ValueError("Each reading requires numeric_value or text_value")
                    normalized_numeric_value = None if numeric_value in ("", None) else float(numeric_value)
                    reading_hash = self._sensor_reading_hash(
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
                            Jsonb(self._sanitize_json_value(reading.get("metadata_json") or {})),
                        ),
                    )
                    row = cur.fetchone()
                    if row:
                        inserted.append(dict(row))
            conn.commit()
        return inserted

    def list_sensor_readings(
        self,
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
            params.append(self._sanitize_text(metric_name).strip().lower())
        if start_at is not None:
            clauses.append("observed_at >= %s")
            params.append(start_at)
        if end_at is not None:
            clauses.append("observed_at <= %s")
            params.append(end_at)
        with psycopg.connect(self.dsn, row_factory=dict_row) as conn:
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
        self,
        *,
        tenant_id: str,
        auth_user_id: str,
        normalized_query: str,
        max_rows: int,
        hours: int,
        points_per_metric: int,
    ) -> list[dict]:
        sensors = self.list_user_sensors(tenant_id=tenant_id, auth_user_id=auth_user_id, status="active", limit=200, offset=0)
        if not sensors:
            return []
        sensor_by_id = {str(row["sensor_id"]): row for row in sensors}
        start_at = datetime.now(timezone.utc) - timedelta(hours=max(1, hours))
        with psycopg.connect(self.dsn, row_factory=dict_row) as conn:
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
            for token in re.findall(r"[a-z0-9%:/\.-]{2,}", self._sanitize_text(normalized_query).lower())
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
            latest_value = latest.get("numeric_value")
            if latest_value is None:
                latest_value = latest.get("text_value")
            summary_parts.append(f"latest {latest_value}")
            if latest.get("unit"):
                summary_parts.append(str(latest.get("unit")))
            summary_parts.append(f"at {latest.get('observed_at')}")
            if numeric_values:
                summary_parts.append(
                    "window stats "
                    + json.dumps(
                        {
                            "count": len(numeric_values),
                            "min": round(min(numeric_values), 4),
                            "max": round(max(numeric_values), 4),
                            "avg": round(sum(numeric_values) / len(numeric_values), 4),
                            "delta": round(numeric_values[0] - numeric_values[-1], 4) if len(numeric_values) > 1 else 0.0,
                        },
                        ensure_ascii=False,
                    )
                )
            row_text = " | ".join(str(part) for part in summary_parts if str(part).strip())
            sensor_terms = {
                token
                for token in re.findall(
                    r"[a-z0-9%:/\.-]{2,}",
                    " ".join(
                        str(value or "")
                        for value in [
                            sensor.get("sensor_name"),
                            sensor.get("sensor_type"),
                            resolved_place_name,
                            sensor.get("external_place_id"),
                            resolved_hive_name,
                            sensor.get("external_hive_id"),
                            metric_name,
                            latest.get("unit"),
                        ]
                    ).lower(),
                )
            }
            overlap = len(query_terms & sensor_terms)
            freshness_bonus = 0.0
            observed_at = latest.get("observed_at")
            if isinstance(observed_at, datetime):
                age_hours = max(0.0, (datetime.now(timezone.utc) - observed_at.astimezone(timezone.utc)).total_seconds() / 3600.0)
                freshness_bonus = max(0.0, 1.5 - min(age_hours / 24.0, 1.5))
            score = overlap * 2.5 + freshness_bonus + (0.6 if metric_name in {"temperature", "humidity", "weight"} else 0.0)
            rows.append(
                {
                    "sensor_row_id": f"{sensor_id}:{metric_name}",
                    "sensor_id": sensor_id,
                    "reading_ids": [str(item.get("reading_id") or "") for item in metric_rows[: max(1, points_per_metric)]],
                    "tenant_id": tenant_id,
                    "auth_user_id": auth_user_id,
                    "sensor_name": str(sensor.get("sensor_name") or ""),
                    "sensor_type": str(sensor.get("sensor_type") or ""),
                    "place_id": str(sensor.get("place_id") or ""),
                    "place_name": resolved_place_name,
                    "external_place_id": str(sensor.get("external_place_id") or ""),
                    "hive_id": str(sensor.get("hive_id") or ""),
                    "external_hive_id": str(sensor.get("external_hive_id") or ""),
                    "hive_name": resolved_hive_name,
                    "location_label": resolved_place_name,
                    "metric_name": metric_name,
                    "unit": latest.get("unit"),
                    "latest_value": latest_value,
                    "latest_observed_at": latest.get("observed_at"),
                    "window_start_at": metric_rows[-1].get("observed_at"),
                    "window_end_at": metric_rows[0].get("observed_at"),
                    "sample_count": len(metric_rows),
                    "min_value": round(min(numeric_values), 4) if numeric_values else None,
                    "max_value": round(max(numeric_values), 4) if numeric_values else None,
                    "avg_value": round(sum(numeric_values) / len(numeric_values), 4) if numeric_values else None,
                    "delta_value": round(numeric_values[0] - numeric_values[-1], 4) if len(numeric_values) > 1 else None,
                    "recent_points": recent_points,
                    "summary_text": row_text,
                    "_relevance_score": round(score, 6),
                }
            )
        rows.sort(
            key=lambda item: (
                float(item.get("_relevance_score") or 0.0),
                str(item.get("latest_observed_at") or ""),
            ),
            reverse=True,
        )
        return rows[: max(0, max_rows)]

    def create_agent_session(
        self,
        tenant_id: str = "shared",
        title: str | None = None,
        profile_id: str | None = None,
        auth_user_id: str | None = None,
        workspace_kind: str = "general",
    ) -> str:
        # Sessions isolate chat history and concurrency for one tenant-scoped conversation.
        session_id = str(uuid4())
        normalized_auth_user_id = self._sanitize_text(auth_user_id or "").strip() or None
        normalized_workspace_kind = self._sanitize_text(workspace_kind or "general").strip().lower() or "general"
        with psycopg.connect(self.dsn) as conn:
            with conn.cursor() as cur:
                if profile_id:
                    cur.execute(
                        "SELECT auth_user_id FROM agent_profiles WHERE profile_id = %s AND tenant_id = %s",
                        (profile_id, tenant_id),
                    )
                    profile_row = cur.fetchone()
                    if profile_row is None:
                        raise ValueError("Profile tenant mismatch")
                    profile_auth_user_id = self._sanitize_text(str(profile_row[0] or "")).strip() or None
                    if normalized_auth_user_id and profile_auth_user_id and profile_auth_user_id != normalized_auth_user_id:
                        raise ValueError("Session owner does not match profile owner")
                    normalized_auth_user_id = normalized_auth_user_id or profile_auth_user_id
                cur.execute(
                    """
                    INSERT INTO agent_sessions (session_id, tenant_id, auth_user_id, profile_id, workspace_kind, title, status)
                    VALUES (%s, %s, %s, %s, %s, %s, 'active')
                    """,
                    (session_id, tenant_id, normalized_auth_user_id, profile_id, normalized_workspace_kind, self._sanitize_text(title or "")),
                )
            conn.commit()
        return session_id

    def set_agent_session_token(self, session_id: str, token: str) -> None:
        with psycopg.connect(self.dsn) as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    UPDATE agent_sessions
                    SET session_token_hash = %s,
                        session_token_issued_at = now(),
                        updated_at = now()
                    WHERE session_id = %s
                    """,
                    (self._token_hash(token), session_id),
                )
            conn.commit()

    def bind_agent_session_auth_user(self, session_id: str, auth_user_id: str) -> None:
        normalized_auth_user_id = self._sanitize_text(auth_user_id or "").strip()
        if not normalized_auth_user_id:
            raise ValueError("auth_user_id is required")
        with psycopg.connect(self.dsn) as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    UPDATE agent_sessions
                    SET auth_user_id = %s,
                        updated_at = now()
                    WHERE session_id = %s
                      AND (auth_user_id IS NULL OR auth_user_id = %s)
                    """,
                    (normalized_auth_user_id, session_id, normalized_auth_user_id),
                )
                if cur.rowcount == 0:
                    raise ValueError("Session owner mismatch")
            conn.commit()

    def verify_agent_session_token(
        self,
        session_id: str,
        token: str | None,
        tenant_id: str | None = None,
        auth_user_id: str | None = None,
    ) -> bool:
        if not token:
            return False
        normalized_auth_user_id = self._sanitize_text(auth_user_id or "").strip() or None
        with psycopg.connect(self.dsn, row_factory=dict_row) as conn:
            with conn.cursor() as cur:
                clauses = ["session_id = %s"]
                params: list[object] = [session_id]
                if tenant_id is not None:
                    clauses.append("tenant_id = %s")
                    params.append(tenant_id)
                cur.execute(
                    f"""
                    SELECT tenant_id, auth_user_id, session_token_hash, session_token_issued_at, updated_at
                    FROM agent_sessions
                    WHERE {' AND '.join(clauses)}
                    """,
                    tuple(params),
                )
                row = cur.fetchone()
                if not row:
                    return False
                stored = str(row.get("session_token_hash") or "")
                if not stored:
                    return False
                issued_at = row.get("session_token_issued_at") or row.get("updated_at")
                if isinstance(issued_at, datetime):
                    age_seconds = (datetime.now(timezone.utc) - issued_at.astimezone(timezone.utc)).total_seconds()
                    if age_seconds > settings.agent_session_token_max_age_seconds:
                        return False
                stored_auth_user_id = self._sanitize_text(str(row.get("auth_user_id") or "")).strip() or None
                if normalized_auth_user_id and stored_auth_user_id and stored_auth_user_id != normalized_auth_user_id:
                    return False
                return stored == self._token_hash(token)

    def claim_agent_session(self, session_id: str, worker_id: str, lease_seconds: int) -> bool:
        # Session leasing mirrors job leasing: it serializes turn writes per session.
        with psycopg.connect(self.dsn) as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT claimed_by, lease_expires_at
                    FROM agent_sessions
                    WHERE session_id = %s
                    FOR UPDATE
                    """,
                    (session_id,),
                )
                row = cur.fetchone()
                if row is None:
                    raise ValueError("Session not found")

                claimed_by, lease_expires_at = row
                if claimed_by and claimed_by != worker_id and lease_expires_at is not None:
                    cur.execute("SELECT now()")
                    current_time = cur.fetchone()[0]
                    if lease_expires_at > current_time:
                        conn.rollback()
                        return False

                cur.execute(
                    """
                    UPDATE agent_sessions
                    SET claimed_by = %s,
                        claimed_at = now(),
                        lease_expires_at = now() + (%s * interval '1 second'),
                        updated_at = now()
                    WHERE session_id = %s
                    """,
                    (worker_id, lease_seconds, session_id),
                )
            conn.commit()
        return True

    def release_agent_session(self, session_id: str, worker_id: str) -> None:
        with psycopg.connect(self.dsn) as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    UPDATE agent_sessions
                    SET claimed_by = NULL,
                        claimed_at = NULL,
                        lease_expires_at = NULL,
                        updated_at = now()
                    WHERE session_id = %s AND claimed_by = %s
                    """,
                    (session_id, worker_id),
                )
            conn.commit()

    def attach_agent_profile_to_session(self, session_id: str, profile_id: str) -> None:
        with psycopg.connect(self.dsn) as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    UPDATE agent_sessions
                    SET profile_id = %s,
                        auth_user_id = COALESCE(agent_sessions.auth_user_id, p.auth_user_id),
                        updated_at = now()
                    FROM agent_profiles p
                    WHERE session_id = %s
                      AND p.profile_id = %s
                      AND p.tenant_id = agent_sessions.tenant_id
                      AND (
                        agent_sessions.auth_user_id IS NULL
                        OR p.auth_user_id IS NULL
                        OR agent_sessions.auth_user_id = p.auth_user_id
                      )
                    """,
                    (profile_id, session_id, profile_id),
                )
                if cur.rowcount == 0:
                    raise ValueError("Profile tenant mismatch")
            conn.commit()

    def get_agent_session(self, session_id: str, tenant_id: str | None = None) -> dict | None:
        with psycopg.connect(self.dsn, row_factory=dict_row) as conn:
            with conn.cursor() as cur:
                clauses = ["session_id = %s"]
                params: list[object] = [session_id]
                if tenant_id is not None:
                    clauses.append("tenant_id = %s")
                    params.append(tenant_id)
                cur.execute(
                    f"""
                    SELECT
                      session_id,
                      tenant_id,
                      auth_user_id,
                      profile_id,
                      workspace_kind,
                      title,
                      status,
                      lease_expires_at AS leased_until,
                      created_at,
                      updated_at
                    FROM agent_sessions
                    WHERE {' AND '.join(clauses)}
                    """,
                    tuple(params),
                )
                row = cur.fetchone()
                return dict(row) if row else None

    def get_agent_session_memory(
        self,
        session_id: str,
        tenant_id: str | None = None,
        auth_user_id: str | None = None,
        profile_id: str | None = None,
    ) -> dict | None:
        with psycopg.connect(self.dsn, row_factory=dict_row) as conn:
            with conn.cursor() as cur:
                clauses = ["m.session_id = %s"]
                params: list[object] = [session_id]
                if tenant_id is not None:
                    clauses.append("s.tenant_id = %s")
                    params.append(tenant_id)
                if auth_user_id is not None:
                    clauses.append("s.auth_user_id = %s")
                    params.append(self._sanitize_text(auth_user_id))
                if profile_id is not None:
                    clauses.append("s.profile_id = %s")
                    params.append(profile_id)
                cur.execute(
                    f"""
                    SELECT
                      m.session_id,
                      m.summary_json,
                      m.summary_text,
                      m.source_provider,
                      m.source_model,
                      m.prompt_version,
                      m.created_at,
                      m.updated_at
                    FROM agent_session_memories m
                    JOIN agent_sessions s ON s.session_id = m.session_id
                    WHERE {' AND '.join(clauses)}
                    """,
                    tuple(params),
                )
                row = cur.fetchone()
                return dict(row) if row else None

    def save_agent_session_memory(
        self,
        session_id: str,
        summary_json: dict[str, Any],
        summary_text: str,
        source_provider: str,
        source_model: str,
        prompt_version: str,
    ) -> None:
        with psycopg.connect(self.dsn) as conn:
            with conn.cursor() as cur:
                cur.execute(
                    "SELECT profile_id, auth_user_id FROM agent_sessions WHERE session_id = %s",
                    (session_id,),
                )
                session_row = cur.fetchone()
                if session_row is None:
                    raise ValueError("Session not found")
                profile_id, auth_user_id = session_row
                cur.execute(
                    """
                    INSERT INTO agent_session_memories (
                      session_id, profile_id, auth_user_id, summary_json, summary_text, source_provider, source_model, prompt_version
                    )
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT (session_id) DO UPDATE SET
                      profile_id = EXCLUDED.profile_id,
                      auth_user_id = EXCLUDED.auth_user_id,
                      summary_json = EXCLUDED.summary_json,
                      summary_text = EXCLUDED.summary_text,
                      source_provider = EXCLUDED.source_provider,
                      source_model = EXCLUDED.source_model,
                      prompt_version = EXCLUDED.prompt_version,
                      updated_at = now()
                    """,
                    (
                        session_id,
                        profile_id,
                        self._sanitize_text(str(auth_user_id or "")) or None,
                        json.dumps(self._sanitize_json_value(summary_json)),
                        self._sanitize_text(summary_text),
                        self._sanitize_text(source_provider),
                        self._sanitize_text(source_model),
                        self._sanitize_text(prompt_version),
                    ),
                )
            conn.commit()

    def update_agent_session_memory_record(self, session_id: str, patch: dict) -> dict | None:
        assignments, params = self._build_allowed_patch(
            patch,
            allowed_fields={
                "summary_json": "summary_json",
                "summary_text": "summary_text",
                "source_provider": "source_provider",
                "source_model": "source_model",
                "prompt_version": "prompt_version",
            },
            json_fields={"summary_json"},
            text_fields={"summary_text", "source_provider", "source_model", "prompt_version"},
        )
        with psycopg.connect(self.dsn, row_factory=dict_row) as conn:
            with conn.cursor() as cur:
                cur.execute(
                    f"""
                    UPDATE agent_session_memories
                    SET {", ".join(assignments)}, updated_at = now()
                    WHERE session_id = %s
                    RETURNING session_id, summary_json, summary_text, source_provider, source_model, prompt_version, created_at, updated_at
                    """,
                    tuple(params + [session_id]),
                )
                row = cur.fetchone()
            conn.commit()
        return dict(row) if row else None

    def update_agent_session(self, session_id: str, title: str | None = None, status: str | None = None) -> None:
        assignments: list[str] = ["updated_at = now()"]
        params: list[object] = []
        if title is not None:
            assignments.append("title = %s")
            params.append(self._sanitize_text(title))
        if status is not None:
            assignments.append("status = %s")
            params.append(status)
        params.append(session_id)
        with psycopg.connect(self.dsn) as conn:
            with conn.cursor() as cur:
                cur.execute(
                    f"""
                    UPDATE agent_sessions
                    SET {", ".join(assignments)}
                    WHERE session_id = %s
                    """,
                    tuple(params),
                )
            conn.commit()

    def update_agent_session_record(self, session_id: str, patch: dict) -> dict | None:
        assignments, params = self._build_allowed_patch(
            patch,
            allowed_fields={"title": "title", "status": "status", "profile_id": "profile_id", "auth_user_id": "auth_user_id"},
            text_fields={"title", "status", "profile_id", "auth_user_id"},
        )
        with psycopg.connect(self.dsn, row_factory=dict_row) as conn:
            with conn.cursor() as cur:
                cur.execute(
                    f"""
                    UPDATE agent_sessions
                    SET {", ".join(assignments)}, updated_at = now()
                    WHERE session_id = %s
                    RETURNING *
                    """,
                    tuple(params + [session_id]),
                )
                row = cur.fetchone()
            conn.commit()
        return dict(row) if row else None

    def delete_agent_session(self, session_id: str) -> int:
        with psycopg.connect(self.dsn) as conn:
            with conn.cursor() as cur:
                cur.execute("DELETE FROM agent_sessions WHERE session_id = %s", (session_id,))
                deleted = cur.rowcount
            conn.commit()
        return deleted

    def delete_sensor_data_for_auth_user(self, auth_user_id: str, tenant_id: str = "shared") -> dict[str, int]:
        normalized_auth_user_id = self._sanitize_text(auth_user_id or "").strip()
        if not normalized_auth_user_id:
            return {"places": 0, "hives": 0, "sensors": 0, "readings": 0}
        with psycopg.connect(self.dsn) as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    WITH deleted_readings AS (
                      DELETE FROM sensor_readings
                      WHERE tenant_id = %s AND auth_user_id = %s
                      RETURNING 1
                    ),
                    deleted_sensors AS (
                      DELETE FROM user_sensors
                      WHERE tenant_id = %s AND auth_user_id = %s
                      RETURNING 1
                    ),
                    deleted_hives AS (
                      DELETE FROM user_hives
                      WHERE tenant_id = %s AND auth_user_id = %s
                      RETURNING 1
                    ),
                    deleted_places AS (
                      DELETE FROM user_places
                      WHERE tenant_id = %s AND auth_user_id = %s
                      RETURNING 1
                    )
                    SELECT
                      (SELECT COUNT(*) FROM deleted_places) AS places,
                      (SELECT COUNT(*) FROM deleted_hives) AS hives,
                      (SELECT COUNT(*) FROM deleted_sensors) AS sensors,
                      (SELECT COUNT(*) FROM deleted_readings) AS readings
                    """,
                    (
                        tenant_id,
                        normalized_auth_user_id,
                        tenant_id,
                        normalized_auth_user_id,
                        tenant_id,
                        normalized_auth_user_id,
                        tenant_id,
                        normalized_auth_user_id,
                    ),
                )
                row = cur.fetchone()
            conn.commit()
        return {
            "places": int((row or {}).get("places") or 0),
            "hives": int((row or {}).get("hives") or 0),
            "sensors": int((row or {}).get("sensors") or 0),
            "readings": int((row or {}).get("readings") or 0),
        }

    def get_agent_runtime_config(self, tenant_id: str = "shared") -> dict | None:
        with psycopg.connect(self.dsn, row_factory=dict_row) as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT tenant_id, settings_json, updated_by, created_at, updated_at
                    FROM agent_runtime_configs
                    WHERE tenant_id = %s
                    """,
                    (tenant_id,),
                )
                row = cur.fetchone()
                return dict(row) if row else None

    def save_agent_runtime_config(self, tenant_id: str, settings_json: dict, updated_by: str = "admin") -> None:
        with psycopg.connect(self.dsn) as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO agent_runtime_configs (tenant_id, settings_json, updated_by)
                    VALUES (%s, %s, %s)
                    ON CONFLICT (tenant_id) DO UPDATE SET
                      settings_json = EXCLUDED.settings_json,
                      updated_by = EXCLUDED.updated_by,
                      updated_at = now()
                    """,
                    (
                        tenant_id,
                        json.dumps(self._sanitize_json_value(settings_json)),
                        self._sanitize_text(updated_by),
                    ),
                )
            conn.commit()

    def save_agent_runtime_secret(self, tenant_id: str, api_key_override: str, updated_by: str = "admin") -> None:
        encrypted_value = self._encrypt_runtime_secret(api_key_override)
        with psycopg.connect(self.dsn) as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO agent_runtime_secrets (tenant_id, api_key_override, updated_by)
                    VALUES (%s, %s, %s)
                    ON CONFLICT (tenant_id) DO UPDATE SET
                      api_key_override = EXCLUDED.api_key_override,
                      updated_by = EXCLUDED.updated_by,
                      updated_at = now()
                    """,
                    (
                        tenant_id,
                        encrypted_value,
                        self._sanitize_text(updated_by),
                    ),
                )
            conn.commit()

    def delete_agent_runtime_config(self, tenant_id: str = "shared") -> int:
        with psycopg.connect(self.dsn) as conn:
            with conn.cursor() as cur:
                cur.execute("DELETE FROM agent_runtime_configs WHERE tenant_id = %s", (tenant_id,))
                deleted = cur.rowcount
            conn.commit()
        return deleted

    def delete_agent_runtime_secret(self, tenant_id: str = "shared") -> int:
        with psycopg.connect(self.dsn) as conn:
            with conn.cursor() as cur:
                cur.execute("DELETE FROM agent_runtime_secrets WHERE tenant_id = %s", (tenant_id,))
                deleted = cur.rowcount
            conn.commit()
        return deleted

    def save_agent_message(self, session_id: str, role: str, content: str, metadata: dict | None = None) -> str:
        message_id = str(uuid4())
        with psycopg.connect(self.dsn) as conn:
            with conn.cursor() as cur:
                cur.execute(
                    "SELECT profile_id, auth_user_id FROM agent_sessions WHERE session_id = %s",
                    (session_id,),
                )
                session_row = cur.fetchone()
                if session_row is None:
                    raise ValueError("Session not found")
                profile_id, auth_user_id = session_row
                cur.execute(
                    """
                    INSERT INTO agent_messages (message_id, session_id, profile_id, auth_user_id, role, content, metadata_json)
                    VALUES (%s, %s, %s, %s, %s, %s, %s)
                    """,
                    (
                        message_id,
                        session_id,
                        profile_id,
                        self._sanitize_text(str(auth_user_id or "")) or None,
                        role,
                        self._sanitize_text(content),
                        json.dumps(self._sanitize_json_value(metadata or {})),
                    ),
                )
                cur.execute(
                    "UPDATE agent_sessions SET updated_at = now() WHERE session_id = %s",
                    (session_id,),
                )
            conn.commit()
        return message_id

    def list_agent_messages(
        self,
        session_id: str,
        limit: int = 20,
        tenant_id: str | None = None,
        auth_user_id: str | None = None,
        profile_id: str | None = None,
    ) -> list[dict]:
        with psycopg.connect(self.dsn, row_factory=dict_row) as conn:
            with conn.cursor() as cur:
                clauses = ["m.session_id = %s"]
                params: list[object] = [session_id]
                if tenant_id is not None:
                    clauses.append("s.tenant_id = %s")
                    params.append(tenant_id)
                if auth_user_id is not None:
                    clauses.append("s.auth_user_id = %s")
                    params.append(self._sanitize_text(auth_user_id))
                if profile_id is not None:
                    clauses.append("s.profile_id = %s")
                    params.append(profile_id)
                cur.execute(
                    f"""
                    SELECT *
                    FROM (
                      SELECT m.message_id, m.session_id, m.role, m.content, m.metadata_json, m.created_at
                      FROM agent_messages m
                      JOIN agent_sessions s ON s.session_id = m.session_id
                      WHERE {' AND '.join(clauses)}
                      ORDER BY m.created_at DESC
                      LIMIT %s
                    ) recent_messages
                    ORDER BY created_at ASC
                    """,
                    tuple(params + [limit]),
                )
                return [dict(row) for row in cur.fetchall()]

    def get_latest_agent_session_scope(
        self,
        session_id: str,
        tenant_id: str | None = None,
        auth_user_id: str | None = None,
        profile_id: str | None = None,
    ) -> dict | None:
        with psycopg.connect(self.dsn, row_factory=dict_row) as conn:
            with conn.cursor() as cur:
                clauses = ["q.session_id = %s"]
                params: list[object] = [session_id]
                if tenant_id is not None:
                    clauses.append("q.tenant_id = %s")
                    params.append(tenant_id)
                if auth_user_id is not None:
                    clauses.append("q.auth_user_id = %s")
                    params.append(self._sanitize_text(auth_user_id))
                if profile_id is not None:
                    clauses.append("q.profile_id = %s")
                    params.append(profile_id)
                cur.execute(
                    f"""
                    SELECT prompt_payload
                    FROM agent_query_runs q
                    WHERE {' AND '.join(clauses)}
                    ORDER BY created_at DESC
                    LIMIT 1
                    """,
                    tuple(params),
                )
                row = cur.fetchone()
                if row is None:
                    return None
                prompt_payload = dict(row.get("prompt_payload") or {})
                scope = prompt_payload.get("request_scope")
                return dict(scope) if isinstance(scope, dict) else None

    def save_agent_query_run(
        self,
        session_id: str | None,
        tenant_id: str,
        question: str,
        normalized_query: str,
        question_type: str,
        retrieval_mode: str,
        status: str,
        answer: str | None,
        confidence: float,
        abstained: bool,
        abstain_reason: str | None,
        provider: str,
        model: str,
        prompt_version: str,
        metrics: dict | None = None,
        error_message: str | None = None,
        prompt_payload: dict | None = None,
        raw_response_payload: dict | None = None,
        final_response_payload: dict | None = None,
        review_status: str = "unreviewed",
        review_reason: str | None = None,
        corpus_snapshot_id: str | None = None,
    ) -> str:
        query_run_id = str(uuid4())
        with psycopg.connect(self.dsn) as conn:
            with conn.cursor() as cur:
                profile_id = None
                auth_user_id = None
                if session_id:
                    cur.execute(
                        "SELECT profile_id, auth_user_id FROM agent_sessions WHERE session_id = %s",
                        (session_id,),
                    )
                    session_row = cur.fetchone()
                    if session_row is None:
                        raise ValueError("Session not found")
                    profile_id, auth_user_id = session_row
                cur.execute(
                    """
                    INSERT INTO agent_query_runs (
                      query_run_id, session_id, profile_id, auth_user_id, tenant_id, question, normalized_query, question_type,
                      retrieval_mode, status, answer, confidence, abstained, abstain_reason,
                      provider, model, prompt_version, metrics_json, error_message,
                      prompt_payload, raw_response_payload, final_response_payload,
                      review_status, review_reason, corpus_snapshot_id
                    )
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    """,
                    (
                        query_run_id,
                        session_id,
                        profile_id,
                        self._sanitize_text(str(auth_user_id or "")) or None,
                        tenant_id,
                        self._sanitize_text(question),
                        self._sanitize_text(normalized_query),
                        question_type,
                        retrieval_mode,
                        status,
                        self._sanitize_text(answer or ""),
                        confidence,
                        abstained,
                        self._sanitize_text(abstain_reason or ""),
                        provider,
                        model,
                        prompt_version,
                        json.dumps(self._redact_sensitive_json_value(metrics or {})),
                        self._sanitize_text(error_message or ""),
                        json.dumps(self._redact_sensitive_json_value(prompt_payload or {})),
                        json.dumps(self._redact_sensitive_json_value(raw_response_payload or {})),
                        json.dumps(self._redact_sensitive_json_value(final_response_payload or {})),
                        review_status,
                        self._sanitize_text(review_reason or ""),
                        corpus_snapshot_id,
                    ),
                )
                if session_id:
                    cur.execute(
                        "UPDATE agent_sessions SET updated_at = now() WHERE session_id = %s",
                        (session_id,),
                    )
            conn.commit()
        return query_run_id

    def save_agent_query_sources(self, query_run_id: str, sources: list[dict], corpus_snapshot_id: str | None = None) -> None:
        if not sources:
            return
        with psycopg.connect(self.dsn) as conn:
            with conn.cursor() as cur:
                for source in sources:
                    cur.execute(
                        """
                        INSERT INTO agent_query_sources (
                          query_run_id, source_kind, source_id, document_id, chunk_id, assertion_id, entity_id,
                          rank, score, selected, payload, corpus_snapshot_id
                        )
                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                        """,
                        (
                            query_run_id,
                            source.get("source_kind"),
                            source.get("source_id"),
                            source.get("document_id"),
                            source.get("chunk_id"),
                            source.get("assertion_id"),
                            source.get("entity_id"),
                            source.get("rank"),
                            source.get("score"),
                            bool(source.get("selected", False)),
                            json.dumps(self._redact_sensitive_json_value(source.get("payload", {}))),
                            corpus_snapshot_id,
                        ),
                    )
            conn.commit()

    def persist_agent_turn(
        self,
        session_id: str,
        tenant_id: str,
        user_message_id: str,
        question: str,
        normalized_query: str,
        question_type: str,
        retrieval_mode: str,
        status: str,
        answer: str | None,
        confidence: float,
        abstained: bool,
        abstain_reason: str | None,
        provider: str,
        model: str,
        prompt_version: str,
        metrics: dict | None,
        prompt_payload: dict | None,
        raw_response_payload: dict | None,
        final_response_payload: dict | None,
        review_status: str,
        review_reason: str | None,
        sources: list[dict],
        assistant_metadata: dict | None,
        corpus_snapshot_id: str | None = None,
    ) -> str:
        # Persist the user turn, assistant turn, and provenance links together so one query is auditable end to end.
        query_run_id = str(uuid4())
        assistant_message_id = str(uuid4())
        query_signature, query_keywords = self._build_query_pattern(normalized_query)
        # Persist the user message, run trace, source links, and assistant message in a
        # single transaction so replay and review always see a consistent turn record.
        with psycopg.connect(self.dsn) as conn:
            with conn.cursor() as cur:
                cur.execute(
                    "SELECT profile_id, auth_user_id FROM agent_sessions WHERE session_id = %s",
                    (session_id,),
                )
                session_row = cur.fetchone()
                if session_row is None:
                    raise ValueError("Session not found")
                profile_id, auth_user_id = session_row
                normalized_auth_user_id = self._sanitize_text(str(auth_user_id or "")) or None
                cur.execute(
                    """
                    INSERT INTO agent_messages (message_id, session_id, profile_id, auth_user_id, role, content, metadata_json)
                    VALUES (%s, %s, %s, %s, %s, %s, %s)
                    """,
                    (
                        user_message_id,
                        session_id,
                        profile_id,
                        normalized_auth_user_id,
                        "user",
                        self._sanitize_text(question),
                        json.dumps(
                            self._sanitize_json_value(
                                {
                                    "normalized_query": normalized_query,
                                    "question_type": question_type,
                                    "retrieval_mode": retrieval_mode,
                                }
                            )
                        ),
                    ),
                )
                cur.execute(
                    """
                    INSERT INTO agent_query_runs (
                      query_run_id, session_id, profile_id, auth_user_id, tenant_id, question, normalized_query, query_signature, query_keywords, question_type,
                      retrieval_mode, status, answer, confidence, abstained, abstain_reason,
                      provider, model, prompt_version, metrics_json, error_message,
                      prompt_payload, raw_response_payload, final_response_payload,
                      review_status, review_reason, corpus_snapshot_id
                    )
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    """,
                    (
                        query_run_id,
                        session_id,
                        profile_id,
                        normalized_auth_user_id,
                        tenant_id,
                        self._sanitize_text(question),
                        self._sanitize_text(normalized_query),
                        query_signature,
                        json.dumps(query_keywords),
                        question_type,
                        retrieval_mode,
                        status,
                        self._sanitize_text(answer or ""),
                        confidence,
                        abstained,
                        self._sanitize_text(abstain_reason or ""),
                        provider,
                        model,
                        prompt_version,
                        json.dumps(self._redact_sensitive_json_value(metrics or {})),
                        "",
                        json.dumps(self._redact_sensitive_json_value(prompt_payload or {})),
                        json.dumps(self._redact_sensitive_json_value(raw_response_payload or {})),
                        json.dumps(self._redact_sensitive_json_value(final_response_payload or {})),
                        review_status,
                        self._sanitize_text(review_reason or ""),
                        corpus_snapshot_id,
                    ),
                )
                for source in sources:
                    cur.execute(
                        """
                        INSERT INTO agent_query_sources (
                          query_run_id, source_kind, source_id, document_id, chunk_id, assertion_id, entity_id,
                          rank, score, selected, payload, corpus_snapshot_id
                        )
                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                        """,
                        (
                            query_run_id,
                            source.get("source_kind"),
                            source.get("source_id"),
                            source.get("document_id"),
                            source.get("chunk_id"),
                            source.get("assertion_id"),
                            source.get("entity_id"),
                            source.get("rank"),
                            source.get("score"),
                            bool(source.get("selected", False)),
                            json.dumps(self._redact_sensitive_json_value(source.get("payload", {}))),
                            corpus_snapshot_id,
                        ),
                    )
                cur.execute(
                    """
                    INSERT INTO agent_messages (message_id, session_id, profile_id, auth_user_id, role, content, metadata_json)
                    VALUES (%s, %s, %s, %s, %s, %s, %s)
                    """,
                    (
                        assistant_message_id,
                        session_id,
                        profile_id,
                        normalized_auth_user_id,
                        "assistant",
                        self._sanitize_text(answer or ""),
                        json.dumps(
                            self._redact_sensitive_json_value(
                                {
                                    **(assistant_metadata or {}),
                                    "query_run_id": query_run_id,
                                }
                            )
                        ),
                    ),
                )
                cur.execute(
                    "UPDATE agent_sessions SET updated_at = now() WHERE session_id = %s",
                    (session_id,),
                )
            conn.commit()
        return query_run_id

    def count_agent_sessions(
        self,
        status: str | None = None,
        tenant_id: str | None = None,
        auth_user_id: str | None = None,
        workspace_kind: str | None = None,
    ) -> int:
        clauses: list[str] = []
        params: list[object] = []
        if tenant_id:
            clauses.append("tenant_id = %s")
            params.append(tenant_id)
        if auth_user_id:
            clauses.append("auth_user_id = %s")
            params.append(auth_user_id)
        if workspace_kind:
            clauses.append("workspace_kind = %s")
            params.append(workspace_kind)
        if status:
            clauses.append("status = %s")
            params.append(status)
        where_clause = f"WHERE {' AND '.join(clauses)}" if clauses else ""
        return self._fetch_scalar(f"SELECT COUNT(*) AS value FROM agent_sessions {where_clause}", tuple(params))

    def list_agent_sessions(
        self,
        status: str | None = None,
        limit: int = 100,
        offset: int = 0,
        tenant_id: str | None = None,
        auth_user_id: str | None = None,
        workspace_kind: str | None = None,
    ) -> list[dict]:
        clauses: list[str] = []
        params: list[object] = []
        if tenant_id:
            clauses.append("s.tenant_id = %s")
            params.append(tenant_id)
        if auth_user_id:
            clauses.append("s.auth_user_id = %s")
            params.append(auth_user_id)
        if workspace_kind:
            clauses.append("s.workspace_kind = %s")
            params.append(workspace_kind)
        if status:
            clauses.append("s.status = %s")
            params.append(status)
        where_clause = f"WHERE {' AND '.join(clauses)}" if clauses else ""
        params.extend([limit, offset])
        with psycopg.connect(self.dsn, row_factory=dict_row) as conn:
            with conn.cursor() as cur:
                cur.execute(
                    f"""
                    SELECT
                      s.session_id,
                      s.tenant_id,
                      s.auth_user_id,
                      s.profile_id,
                      s.workspace_kind,
                      s.title,
                      s.status,
                      s.claimed_by,
                      s.claimed_at,
                      s.lease_expires_at,
                      s.created_at,
                      s.updated_at,
                      COALESCE(message_counts.message_count, 0) AS message_count,
                      COALESCE(query_counts.query_count, 0) AS query_count,
                      latest_message.content AS last_message_content,
                      latest_message.role AS last_message_role,
                      latest_message.created_at AS last_message_at
                    FROM agent_sessions s
                    LEFT JOIN LATERAL (
                      SELECT COUNT(*)::integer AS message_count
                      FROM agent_messages m
                      WHERE m.session_id = s.session_id
                    ) message_counts ON TRUE
                    LEFT JOIN LATERAL (
                      SELECT COUNT(*)::integer AS query_count
                      FROM agent_query_runs q
                      WHERE q.session_id = s.session_id
                    ) query_counts ON TRUE
                    LEFT JOIN LATERAL (
                      SELECT m.content, m.role, m.created_at
                      FROM agent_messages m
                      WHERE m.session_id = s.session_id
                      ORDER BY m.created_at DESC
                      LIMIT 1
                    ) latest_message ON TRUE
                    {where_clause}
                    ORDER BY s.updated_at DESC
                    LIMIT %s OFFSET %s
                    """,
                    tuple(params),
                )
                return [dict(row) for row in cur.fetchall()]

    def count_agent_query_runs(
        self,
        session_id: str | None = None,
        tenant_id: str | None = None,
        status: str | None = None,
        abstained: bool | None = None,
        review_status: str | None = None,
        auth_user_id: str | None = None,
        profile_id: str | None = None,
    ) -> int:
        clauses: list[str] = []
        params: list[object] = []
        if session_id:
            clauses.append("session_id = %s")
            params.append(session_id)
        if tenant_id:
            clauses.append("tenant_id = %s")
            params.append(tenant_id)
        if auth_user_id:
            clauses.append("auth_user_id = %s")
            params.append(self._sanitize_text(auth_user_id))
        if profile_id:
            clauses.append("profile_id = %s")
            params.append(profile_id)
        if status:
            clauses.append("status = %s")
            params.append(status)
        if abstained is not None:
            clauses.append("abstained = %s")
            params.append(abstained)
        if review_status:
            clauses.append("review_status = %s")
            params.append(review_status)
        where_clause = f"WHERE {' AND '.join(clauses)}" if clauses else ""
        return self._fetch_scalar(f"SELECT COUNT(*) AS value FROM agent_query_runs {where_clause}", tuple(params))

    def list_agent_query_runs(
        self,
        session_id: str | None = None,
        tenant_id: str | None = None,
        status: str | None = None,
        abstained: bool | None = None,
        review_status: str | None = None,
        auth_user_id: str | None = None,
        profile_id: str | None = None,
        limit: int = 100,
        offset: int = 0,
    ) -> list[dict]:
        clauses: list[str] = []
        params: list[object] = []
        if session_id:
            clauses.append("q.session_id = %s")
            params.append(session_id)
        if tenant_id:
            clauses.append("q.tenant_id = %s")
            params.append(tenant_id)
        if auth_user_id:
            clauses.append("q.auth_user_id = %s")
            params.append(self._sanitize_text(auth_user_id))
        if profile_id:
            clauses.append("q.profile_id = %s")
            params.append(profile_id)
        if status:
            clauses.append("q.status = %s")
            params.append(status)
        if abstained is not None:
            clauses.append("q.abstained = %s")
            params.append(abstained)
        if review_status:
            clauses.append("q.review_status = %s")
            params.append(review_status)
        where_clause = f"WHERE {' AND '.join(clauses)}" if clauses else ""
        params.extend([limit, offset])
        with psycopg.connect(self.dsn, row_factory=dict_row) as conn:
            with conn.cursor() as cur:
                cur.execute(
                    f"""
                    SELECT
                      q.query_run_id,
                      q.session_id,
                      q.tenant_id,
                      q.question,
                      q.normalized_query,
                      q.query_signature,
                      q.query_keywords,
                      q.question_type,
                      q.retrieval_mode,
                      q.status,
                      q.confidence,
                      q.abstained,
                      q.abstain_reason,
                      q.provider,
                      q.model,
                      q.prompt_version,
                      q.review_status,
                      q.review_reason,
                      q.reviewed_at,
                      q.reviewed_by,
                      q.corpus_snapshot_id,
                      q.metrics_json,
                      q.created_at,
                      s.title AS session_title
                    FROM agent_query_runs q
                    LEFT JOIN agent_sessions s ON s.session_id = q.session_id
                    {where_clause}
                    ORDER BY q.created_at DESC
                    LIMIT %s OFFSET %s
                    """,
                    tuple(params),
                )
                return [dict(row) for row in cur.fetchall()]

    def get_agent_query_detail(
        self,
        query_run_id: str,
        tenant_id: str | None = None,
        session_id: str | None = None,
        auth_user_id: str | None = None,
        profile_id: str | None = None,
    ) -> dict | None:
        with psycopg.connect(self.dsn, row_factory=dict_row) as conn:
            with conn.cursor() as cur:
                clauses = ["q.query_run_id = %s"]
                params: list[object] = [query_run_id]
                if tenant_id:
                    clauses.append("q.tenant_id = %s")
                    params.append(tenant_id)
                if session_id:
                    clauses.append("q.session_id = %s")
                    params.append(session_id)
                if auth_user_id:
                    clauses.append("q.auth_user_id = %s")
                    params.append(self._sanitize_text(auth_user_id))
                if profile_id:
                    clauses.append("q.profile_id = %s")
                    params.append(profile_id)
                cur.execute(
                    f"""
                    SELECT q.*, s.title AS session_title, s.status AS session_status, s.profile_id
                    FROM agent_query_runs q
                    LEFT JOIN agent_sessions s ON s.session_id = q.session_id
                    WHERE {' AND '.join(clauses)}
                    """,
                    tuple(params),
                )
                run = cur.fetchone()
                if run is None:
                    return None

                cur.execute(
                    """
                    SELECT
                      source_link_id,
                      query_run_id,
                      source_kind,
                      source_id,
                      document_id,
                      chunk_id,
                      assertion_id,
                      entity_id,
                      rank,
                      score,
                      selected,
                      payload,
                      corpus_snapshot_id,
                      created_at
                    FROM agent_query_sources
                    WHERE query_run_id = %s
                    ORDER BY selected DESC, rank NULLS LAST, created_at ASC
                    """,
                    (query_run_id,),
                )
                sources = [dict(row) for row in cur.fetchall()]

                cur.execute(
                    """
                    SELECT review_id, query_run_id, decision, reviewer, notes, payload, created_at
                    FROM agent_answer_reviews
                    WHERE query_run_id = %s
                    ORDER BY created_at DESC
                    """,
                    (query_run_id,),
                )
                reviews = [dict(row) for row in cur.fetchall()]

                pattern = None
                session_memory = None
                profile = None
                tenant_id = str(run.get("tenant_id") or "shared")
                query_signature = str(run.get("query_signature") or "")
                session_id = run.get("session_id")
                run_profile_id = str(run.get("profile_id") or "")
                if session_id:
                    memory_clauses = ["session_id = %s"]
                    memory_params: list[object] = [session_id]
                    if auth_user_id:
                        memory_clauses.append("auth_user_id = %s")
                        memory_params.append(self._sanitize_text(auth_user_id))
                    if profile_id:
                        memory_clauses.append("profile_id = %s")
                        memory_params.append(profile_id)
                    cur.execute(
                        f"""
                        SELECT session_id, summary_json, summary_text, source_provider, source_model, prompt_version, created_at, updated_at
                        FROM agent_session_memories
                        WHERE {' AND '.join(memory_clauses)}
                        """,
                        tuple(memory_params),
                    )
                    session_memory_row = cur.fetchone()
                    session_memory = dict(session_memory_row) if session_memory_row else None
                if run_profile_id:
                    cur.execute(
                        """
                        SELECT
                          profile_id,
                          tenant_id,
                          display_name,
                          status,
                          summary_json,
                          summary_text,
                          source_provider,
                          source_model,
                          prompt_version,
                          created_at,
                          updated_at
                        FROM agent_profiles
                        WHERE profile_id = %s
                        """,
                        (run_profile_id,),
                    )
                    profile_row = cur.fetchone()
                    profile = dict(profile_row) if profile_row else None
                if query_signature:
                    cur.execute(
                        """
                        SELECT
                          tenant_id,
                          pattern_signature,
                          keywords_json,
                          example_query,
                          approved_count,
                          rejected_count,
                          needs_review_count,
                          total_feedback_count,
                          router_cache_json,
                          router_cached_at,
                          router_cache_hits,
                          router_model,
                          last_query_run_id,
                          last_feedback_at,
                          last_feedback_by,
                          created_at,
                          updated_at
                        FROM agent_query_patterns
                        WHERE tenant_id = %s AND pattern_signature = %s
                        """,
                        (tenant_id, query_signature),
                    )
                    pattern_row = cur.fetchone()
                    pattern = dict(pattern_row) if pattern_row else None

        return {
            "query_run": self._redact_sensitive_json_value(dict(run)),
            "sources": [self._redact_sensitive_json_value(item) for item in sources],
            "reviews": [self._redact_sensitive_json_value(item) for item in reviews],
            "pattern": self._redact_sensitive_json_value(pattern) if pattern is not None else None,
            "session_memory": self._redact_sensitive_json_value(session_memory) if session_memory is not None else None,
            "profile": self._redact_sensitive_json_value(profile) if profile is not None else None,
        }

    def list_documents_by_ids(self, document_ids: list[str]) -> list[dict]:
        if not document_ids:
            return []
        with psycopg.connect(self.dsn, row_factory=dict_row) as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT document_id, tenant_id, filename, status, document_class
                    FROM documents
                    WHERE document_id::text = ANY(%s)
                    """,
                    (document_ids,),
                )
                return [dict(row) for row in cur.fetchall()]

    def search_kg_entities_for_query(
        self,
        query_text: str,
        tenant_id: str,
        document_ids: list[str] | None = None,
        limit: int = 6,
    ) -> list[dict]:
        tokens = [item for item in re.split(r"[^a-z0-9]+", query_text.lower()) if len(item) >= 3]
        if not tokens:
            return []
        clauses: list[str] = []
        score_sql: list[str] = []
        score_params: list[object] = []
        match_params: list[object] = []
        token_sql: list[str] = []
        for token in tokens[:8]:
            token_sql.append(
                "("
                "e.entity_id ILIKE %s OR "
                "e.canonical_name ILIKE %s OR "
                "e.entity_type ILIKE %s OR "
                "COALESCE(a.object_literal, '') ILIKE %s OR "
                "COALESCE(ev.excerpt, '') ILIKE %s"
                ")"
            )
            needle = f"%{token}%"
            prefix = f"{token}%"
            match_params.extend([needle, needle, needle, needle, needle])
            score_sql.extend(
                [
                    "MAX(CASE WHEN e.canonical_name ILIKE %s THEN 40 ELSE 0 END)",
                    "MAX(CASE WHEN e.canonical_name ILIKE %s THEN 24 ELSE 0 END)",
                    "MAX(CASE WHEN e.entity_type ILIKE %s THEN 10 ELSE 0 END)",
                    "MAX(CASE WHEN e.entity_id ILIKE %s THEN 6 ELSE 0 END)",
                    "MAX(CASE WHEN COALESCE(a.object_literal, '') ILIKE %s THEN 5 ELSE 0 END)",
                    "MAX(CASE WHEN COALESCE(ev.excerpt, '') ILIKE %s THEN 3 ELSE 0 END)",
                ]
            )
            score_params.extend([prefix, needle, needle, needle, needle, needle])
        clauses.append("(" + " OR ".join(token_sql) + ")")
        clauses.append("d.tenant_id = %s")
        params: list[object] = list(score_params) + list(match_params) + [tenant_id]
        if document_ids:
            clauses.append("a.document_id = ANY(%s)")
            params.append(document_ids)
        where_clause = f"WHERE {' AND '.join(clauses)}" if clauses else ""
        relevance_score = " + ".join(score_sql) if score_sql else "0"
        with psycopg.connect(self.dsn, row_factory=dict_row) as conn:
            with conn.cursor() as cur:
                cur.execute(
                    f"""
                    SELECT
                      e.entity_id,
                      e.canonical_name,
                      e.entity_type,
                      e.source,
                      COUNT(DISTINCT a.assertion_id) AS assertion_count,
                      COUNT(DISTINCT a.document_id) AS document_count,
                      ({relevance_score}) AS relevance_score
                    FROM kg_entities e
                    LEFT JOIN kg_assertions a
                      ON a.subject_entity_id = e.entity_id
                      OR a.object_entity_id = e.entity_id
                    LEFT JOIN kg_assertion_evidence ev
                      ON ev.assertion_id = a.assertion_id
                    LEFT JOIN documents d
                      ON d.document_id = a.document_id
                    {where_clause}
                    GROUP BY e.entity_id, e.canonical_name, e.entity_type, e.source
                    ORDER BY relevance_score DESC, assertion_count DESC, document_count DESC, e.canonical_name ASC
                    LIMIT %s
                    """,
                    tuple(params + [limit]),
                )
                return [dict(row) for row in cur.fetchall()]

    def save_agent_answer_review(
        self,
        query_run_id: str,
        decision: str,
        reviewer: str = "admin",
        notes: str | None = None,
        payload: dict | None = None,
        tenant_id: str | None = None,
        session_id: str | None = None,
    ) -> None:
        decision = self._sanitize_text(decision or "").strip().lower()
        reviewer = self._sanitize_text(reviewer or "admin").strip() or "admin"
        if decision not in ALLOWED_AGENT_REVIEW_DECISIONS:
            raise ValueError("Invalid review decision")
        is_quality_judgment = reviewer != "user-ui"
        with psycopg.connect(self.dsn, row_factory=dict_row) as conn:
            with conn.cursor() as cur:
                clauses = ["query_run_id = %s"]
                params: list[object] = [query_run_id]
                if tenant_id:
                    clauses.append("tenant_id = %s")
                    params.append(tenant_id)
                if session_id:
                    clauses.append("session_id = %s")
                    params.append(session_id)
                cur.execute(
                    f"""
                    SELECT tenant_id, question, normalized_query, query_signature, query_keywords
                    FROM agent_query_runs
                    WHERE {' AND '.join(clauses)}
                    """,
                    tuple(params),
                )
                run = cur.fetchone()
                if run is None:
                    raise ValueError("Agent query run not found")
                tenant_id = str(run["tenant_id"] or "shared")
                question = str(run["question"] or "")
                normalized_query = str(run["normalized_query"] or "")
                query_signature = str(run["query_signature"] or "")
                query_keywords = list(run["query_keywords"] or [])
                if not query_signature:
                    query_signature, query_keywords = self._build_query_pattern(normalized_query)
                    cur.execute(
                        """
                        UPDATE agent_query_runs
                        SET query_signature = %s,
                            query_keywords = %s
                        WHERE query_run_id = %s
                        """,
                        (query_signature, json.dumps(query_keywords), query_run_id),
                    )

                cur.execute(
                    """
                    SELECT decision
                    FROM agent_answer_reviews
                    WHERE query_run_id = %s
                      AND reviewer <> 'user-ui'
                    ORDER BY created_at DESC
                    LIMIT 1
                    """,
                    (query_run_id,),
                )
                previous_review = cur.fetchone()
                previous_decision = str(previous_review["decision"]) if previous_review else None

                cur.execute(
                    """
                    INSERT INTO agent_answer_reviews (query_run_id, decision, reviewer, notes, payload)
                    VALUES (%s, %s, %s, %s, %s)
                    """,
                    (
                        query_run_id,
                        decision,
                        reviewer,
                        self._sanitize_text(notes or ""),
                        json.dumps(self._redact_sensitive_json_value(payload or {})),
                    ),
                )
                if is_quality_judgment:
                    cur.execute(
                        """
                        UPDATE agent_query_runs
                        SET review_status = %s,
                            review_reason = %s,
                            reviewed_at = now(),
                            reviewed_by = %s
                        WHERE query_run_id = %s
                        """,
                        (decision, self._sanitize_text(notes or ""), reviewer, query_run_id),
                    )
                    self._apply_agent_query_pattern_feedback(
                        cur=cur,
                        tenant_id=tenant_id,
                        query_signature=query_signature,
                        query_keywords=query_keywords,
                        example_query=question,
                        previous_decision=previous_decision,
                        decision=decision,
                        reviewer=reviewer,
                        query_run_id=query_run_id,
                    )
            conn.commit()

    def _apply_agent_query_pattern_feedback(
        self,
        cur,
        tenant_id: str,
        query_signature: str,
        query_keywords: list[str],
        example_query: str,
        previous_decision: str | None,
        decision: str,
        reviewer: str,
        query_run_id: str,
    ) -> None:
        cur.execute(
            """
            INSERT INTO agent_query_patterns (
              tenant_id, pattern_signature, keywords_json, example_query,
              last_query_run_id, last_feedback_at, last_feedback_by
            )
            VALUES (%s, %s, %s, %s, %s, now(), %s)
            ON CONFLICT (tenant_id, pattern_signature) DO UPDATE SET
              keywords_json = EXCLUDED.keywords_json,
              example_query = COALESCE(agent_query_patterns.example_query, EXCLUDED.example_query),
              last_query_run_id = EXCLUDED.last_query_run_id,
              last_feedback_at = EXCLUDED.last_feedback_at,
              last_feedback_by = EXCLUDED.last_feedback_by,
              updated_at = now()
            """,
            (
                tenant_id,
                query_signature,
                json.dumps(query_keywords),
                self._sanitize_text(example_query),
                query_run_id,
                self._sanitize_text(reviewer),
            ),
        )
        column_map = {
            "approved": "approved_count",
            "rejected": "rejected_count",
            "needs_review": "needs_review_count",
        }
        if previous_decision in column_map and previous_decision != decision:
            column = column_map[previous_decision]
            cur.execute(
                f"""
                UPDATE agent_query_patterns
                SET {column} = GREATEST({column} - 1, 0),
                    updated_at = now()
                WHERE tenant_id = %s AND pattern_signature = %s
                """,
                (tenant_id, query_signature),
            )
        if decision in column_map and previous_decision != decision:
            column = column_map[decision]
            cur.execute(
                f"""
                UPDATE agent_query_patterns
                SET {column} = {column} + 1,
                    total_feedback_count = total_feedback_count + 1,
                    last_query_run_id = %s,
                    last_feedback_at = now(),
                    last_feedback_by = %s,
                    updated_at = now()
                WHERE tenant_id = %s AND pattern_signature = %s
                """,
                (query_run_id, self._sanitize_text(reviewer), tenant_id, query_signature),
            )
        elif previous_decision == decision:
            cur.execute(
                """
                UPDATE agent_query_patterns
                SET total_feedback_count = total_feedback_count + 1,
                    last_query_run_id = %s,
                    last_feedback_at = now(),
                    last_feedback_by = %s,
                    updated_at = now()
                WHERE tenant_id = %s AND pattern_signature = %s
                """,
                (query_run_id, self._sanitize_text(reviewer), tenant_id, query_signature),
            )

    def list_agent_answer_reviews(self, decision: str | None = None, limit: int = 100, offset: int = 0) -> list[dict]:
        clauses: list[str] = []
        params: list[object] = []
        if decision:
            clauses.append("ar.decision = %s")
            params.append(decision)
        where_clause = f"WHERE {' AND '.join(clauses)}" if clauses else ""
        with psycopg.connect(self.dsn, row_factory=dict_row) as conn:
            with conn.cursor() as cur:
                cur.execute(
                    f"""
                    SELECT
                      ar.review_id,
                      ar.query_run_id,
                      ar.decision,
                      ar.reviewer,
                      ar.notes,
                      ar.payload,
                      ar.created_at,
                      qr.question,
                      qr.query_signature,
                      qr.query_keywords,
                      qr.session_id,
                      qr.confidence,
                      qr.abstained
                    FROM agent_answer_reviews ar
                    JOIN agent_query_runs qr ON qr.query_run_id = ar.query_run_id
                    {where_clause}
                    ORDER BY ar.created_at DESC
                    LIMIT %s OFFSET %s
                    """,
                    tuple(params + [limit, offset]),
                )
                return [dict(row) for row in cur.fetchall()]

    def count_agent_answer_reviews(self, decision: str | None = None) -> int:
        clauses: list[str] = []
        params: list[object] = []
        if decision:
            clauses.append("decision = %s")
            params.append(decision)
        where_clause = f"WHERE {' AND '.join(clauses)}" if clauses else ""
        return self._fetch_scalar(f"SELECT COUNT(*) AS value FROM agent_answer_reviews {where_clause}", tuple(params))

    def list_agent_query_patterns(self, tenant_id: str = "shared", search: str | None = None, limit: int = 50, offset: int = 0) -> list[dict]:
        clauses: list[str] = ["tenant_id = %s"]
        params: list[object] = [tenant_id]
        if search:
            clauses.append("(pattern_signature ILIKE %s OR example_query ILIKE %s)")
            needle = f"%{search}%"
            params.extend([needle, needle])
        where_clause = "WHERE " + " AND ".join(clauses)
        with psycopg.connect(self.dsn, row_factory=dict_row) as conn:
            with conn.cursor() as cur:
                cur.execute(
                    f"""
                    SELECT
                      tenant_id,
                      pattern_signature,
                      keywords_json,
                      example_query,
                      approved_count,
                      rejected_count,
                      needs_review_count,
                      total_feedback_count,
                      router_cache_json,
                      router_cached_at,
                      router_cache_hits,
                      router_model,
                      last_query_run_id,
                      last_feedback_at,
                      last_feedback_by,
                      created_at,
                      updated_at
                    FROM agent_query_patterns
                    {where_clause}
                    ORDER BY approved_count DESC, total_feedback_count DESC, updated_at DESC
                    LIMIT %s OFFSET %s
                    """,
                    tuple(params + [limit, offset]),
                )
                return [dict(row) for row in cur.fetchall()]

    def count_agent_query_patterns(self, tenant_id: str = "shared", search: str | None = None) -> int:
        clauses: list[str] = ["tenant_id = %s"]
        params: list[object] = [tenant_id]
        if search:
            clauses.append("(pattern_signature ILIKE %s OR example_query ILIKE %s)")
            needle = f"%{search}%"
            params.extend([needle, needle])
        where_clause = "WHERE " + " AND ".join(clauses)
        return self._fetch_scalar(f"SELECT COUNT(*) AS value FROM agent_query_patterns {where_clause}", tuple(params))

    def get_agent_metrics(self) -> dict:
        with psycopg.connect(self.dsn, row_factory=dict_row) as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT
                      COUNT(*) AS total_runs,
                      COUNT(*) FILTER (WHERE abstained = true) AS abstentions,
                      COUNT(*) FILTER (WHERE review_status = 'needs_review') AS needs_review,
                      COUNT(*) FILTER (WHERE review_status = 'approved') AS approved,
                      COUNT(*) FILTER (WHERE review_status = 'rejected') AS rejected,
                      COUNT(*) FILTER (WHERE COALESCE(abstain_reason, '') = 'no_valid_citations') AS no_citation_answers,
                      COALESCE(AVG(confidence), 0) AS avg_confidence,
                      COALESCE(AVG(NULLIF(metrics_json->>'latency_ms', '')::numeric), 0) AS avg_latency_ms,
                      COALESCE(MAX(NULLIF(metrics_json->>'latency_ms', '')::numeric), 0) AS max_latency_ms
                    FROM agent_query_runs
                    """
                )
                row = dict(cur.fetchone() or {})
                return {
                    "total_runs": int(row.get("total_runs") or 0),
                    "abstentions": int(row.get("abstentions") or 0),
                    "needs_review": int(row.get("needs_review") or 0),
                    "approved": int(row.get("approved") or 0),
                    "rejected": int(row.get("rejected") or 0),
                    "no_citation_answers": int(row.get("no_citation_answers") or 0),
                    "avg_confidence": float(row.get("avg_confidence") or 0.0),
                    "avg_latency_ms": float(row.get("avg_latency_ms") or 0.0),
                    "max_latency_ms": float(row.get("max_latency_ms") or 0.0),
                }

    def get_agent_query_pattern(self, tenant_id: str, pattern_signature: str) -> dict | None:
        with psycopg.connect(self.dsn, row_factory=dict_row) as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT
                      tenant_id,
                      pattern_signature,
                      keywords_json,
                      example_query,
                      approved_count,
                      rejected_count,
                      needs_review_count,
                      total_feedback_count,
                      router_cache_json,
                      router_cached_at,
                      router_cache_hits,
                      router_model,
                      last_query_run_id,
                      last_feedback_at,
                      last_feedback_by,
                      created_at,
                      updated_at
                    FROM agent_query_patterns
                    WHERE tenant_id = %s AND pattern_signature = %s
                    """,
                    (tenant_id, pattern_signature),
                )
                row = cur.fetchone()
                return dict(row) if row else None

    def get_cached_query_embedding(
        self,
        tenant_id: str,
        normalized_query: str,
        cache_identity: str,
    ) -> dict | None:
        query_hash = self._query_hash(normalized_query)
        with psycopg.connect(self.dsn, row_factory=dict_row) as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT
                      tenant_id,
                      query_hash,
                      normalized_query,
                      cache_identity,
                      embedding_json,
                      embedding_dimensions,
                      cache_hits,
                      cached_at,
                      created_at,
                      updated_at
                    FROM agent_query_embeddings
                    WHERE tenant_id = %s AND query_hash = %s AND cache_identity = %s
                    """,
                    (tenant_id, query_hash, self._sanitize_text(cache_identity)),
                )
                row = cur.fetchone()
                return dict(row) if row else None

    def save_cached_query_embedding(
        self,
        tenant_id: str,
        normalized_query: str,
        cache_identity: str,
        embedding: list[float],
    ) -> None:
        query_hash = self._query_hash(normalized_query)
        vector = [float(value) for value in embedding]
        with psycopg.connect(self.dsn) as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO agent_query_embeddings (
                      tenant_id,
                      query_hash,
                      normalized_query,
                      cache_identity,
                      embedding_json,
                      embedding_dimensions,
                      cache_hits,
                      cached_at
                    )
                    VALUES (%s, %s, %s, %s, %s, %s, 0, now())
                    ON CONFLICT (tenant_id, query_hash, cache_identity) DO UPDATE SET
                      normalized_query = EXCLUDED.normalized_query,
                      embedding_json = EXCLUDED.embedding_json,
                      embedding_dimensions = EXCLUDED.embedding_dimensions,
                      cached_at = EXCLUDED.cached_at,
                      updated_at = now()
                    """,
                    (
                        tenant_id,
                        query_hash,
                        self._sanitize_text(normalized_query),
                        self._sanitize_text(cache_identity),
                        Jsonb(vector),
                        len(vector),
                    ),
                )
            conn.commit()

    def touch_cached_query_embedding_hit(
        self,
        tenant_id: str,
        normalized_query: str,
        cache_identity: str,
    ) -> None:
        query_hash = self._query_hash(normalized_query)
        with psycopg.connect(self.dsn) as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    UPDATE agent_query_embeddings
                    SET cache_hits = cache_hits + 1,
                        updated_at = now()
                    WHERE tenant_id = %s AND query_hash = %s AND cache_identity = %s
                    """,
                    (tenant_id, query_hash, self._sanitize_text(cache_identity)),
                )
            conn.commit()

    def save_agent_query_pattern_route(
        self,
        tenant_id: str,
        pattern_signature: str,
        query_keywords: list[str],
        example_query: str,
        route_payload: dict[str, Any],
        router_model: str,
    ) -> None:
        with psycopg.connect(self.dsn) as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO agent_query_patterns (
                      tenant_id,
                      pattern_signature,
                      keywords_json,
                      example_query,
                      router_cache_json,
                      router_cached_at,
                      router_model,
                      router_cache_hits
                    )
                    VALUES (%s, %s, %s, %s, %s, now(), %s, 0)
                    ON CONFLICT (tenant_id, pattern_signature) DO UPDATE SET
                      keywords_json = EXCLUDED.keywords_json,
                      example_query = COALESCE(agent_query_patterns.example_query, EXCLUDED.example_query),
                      router_cache_json = EXCLUDED.router_cache_json,
                      router_cached_at = EXCLUDED.router_cached_at,
                      router_model = EXCLUDED.router_model,
                      updated_at = now()
                    """,
                    (
                        tenant_id,
                        pattern_signature,
                        json.dumps(self._sanitize_json_value(query_keywords)),
                        self._sanitize_text(example_query),
                        json.dumps(self._sanitize_json_value(route_payload)),
                        self._sanitize_text(router_model),
                    ),
                )
            conn.commit()

    def touch_agent_query_pattern_route_hit(self, tenant_id: str, pattern_signature: str) -> None:
        with psycopg.connect(self.dsn) as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    UPDATE agent_query_patterns
                    SET router_cache_hits = router_cache_hits + 1,
                        updated_at = now()
                    WHERE tenant_id = %s AND pattern_signature = %s
                    """,
                    (tenant_id, pattern_signature),
                )
            conn.commit()

    def update_agent_query_pattern(self, tenant_id: str, pattern_signature: str, patch: dict) -> dict | None:
        assignments, params = self._build_allowed_patch(
            patch,
            allowed_fields={
                "keywords_json": "keywords_json",
                "example_query": "example_query",
                "approved_count": "approved_count",
                "rejected_count": "rejected_count",
                "needs_review_count": "needs_review_count",
                "total_feedback_count": "total_feedback_count",
                "last_query_run_id": "last_query_run_id",
                "last_feedback_by": "last_feedback_by",
            },
            json_fields={"keywords_json"},
            text_fields={"example_query", "last_query_run_id", "last_feedback_by"},
        )
        with psycopg.connect(self.dsn, row_factory=dict_row) as conn:
            with conn.cursor() as cur:
                cur.execute(
                    f"""
                    UPDATE agent_query_patterns
                    SET {", ".join(assignments)}, updated_at = now()
                    WHERE tenant_id = %s AND pattern_signature = %s
                    RETURNING
                      tenant_id,
                      pattern_signature,
                      keywords_json,
                      example_query,
                      approved_count,
                      rejected_count,
                      needs_review_count,
                      total_feedback_count,
                      last_query_run_id,
                      last_feedback_at,
                      last_feedback_by,
                      created_at,
                      updated_at
                    """,
                    tuple(params + [tenant_id, pattern_signature]),
                )
                row = cur.fetchone()
            conn.commit()
        return dict(row) if row else None

    def delete_agent_query_pattern(self, tenant_id: str, pattern_signature: str) -> int:
        with psycopg.connect(self.dsn) as conn:
            with conn.cursor() as cur:
                cur.execute(
                    "DELETE FROM agent_query_patterns WHERE tenant_id = %s AND pattern_signature = %s",
                    (tenant_id, pattern_signature),
                )
                deleted = cur.rowcount
            conn.commit()
        return deleted

    def list_chunk_records_by_ids(self, chunk_ids: list[str]) -> list[dict]:
        if not chunk_ids:
            return []
        with psycopg.connect(self.dsn, row_factory=dict_row) as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT
                      dc.chunk_id,
                      dc.document_id,
                      dc.tenant_id,
                      dc.chunk_index,
                      dc.page_start,
                      dc.page_end,
                      dc.section_path,
                      dc.prev_chunk_id,
                      dc.next_chunk_id,
                      dc.char_start,
                      dc.char_end,
                      dc.content_type,
                      dc.text,
                      dc.parser_version,
                      dc.chunker_version,
                      dc.metadata_json,
                      (
                        SELECT COUNT(*)
                        FROM kg_assertions ka
                        WHERE ka.chunk_id = dc.chunk_id
                      ) AS kg_assertion_count,
                      COALESCE(cv.status, dc.status) AS validation_status,
                      cv.quality_score,
                      cv.reasons
                    FROM document_chunks dc
                    LEFT JOIN chunk_validations cv ON cv.chunk_id = dc.chunk_id
                    WHERE dc.chunk_id = ANY(%s)
                    """,
                    (chunk_ids,),
                )
                rows = [dict(row) for row in cur.fetchall()]
        positions = {chunk_id: index for index, chunk_id in enumerate(chunk_ids)}
        rows.sort(key=lambda row: positions.get(row["chunk_id"], 10**9))
        return rows

    def list_chunk_records_for_asset_pages(self, assets: list[dict], limit: int = 120) -> list[dict]:
        if not assets:
            return []
        clauses: list[str] = []
        params: list[object] = []
        seen_pairs: set[tuple[str, int]] = set()
        for asset in assets:
            document_id = str(asset.get("document_id") or "")
            page_number = int(asset.get("page_number") or 0)
            if not document_id or page_number <= 0:
                continue
            key = (document_id, page_number)
            if key in seen_pairs:
                continue
            seen_pairs.add(key)
            clauses.append("(dc.document_id = %s AND COALESCE(dc.page_start, dc.page_end) <= %s AND COALESCE(dc.page_end, dc.page_start) >= %s)")
            params.extend([document_id, page_number, page_number])
        if not clauses:
            return []
        with psycopg.connect(self.dsn, row_factory=dict_row) as conn:
            with conn.cursor() as cur:
                cur.execute(
                    f"""
                    SELECT
                      dc.chunk_id,
                      dc.document_id,
                      dc.tenant_id,
                      dc.chunk_index,
                      dc.page_start,
                      dc.page_end,
                      dc.section_path,
                      dc.prev_chunk_id,
                      dc.next_chunk_id,
                      dc.char_start,
                      dc.char_end,
                      dc.content_type,
                      dc.text,
                      dc.parser_version,
                      dc.chunker_version,
                      dc.metadata_json,
                      (
                        SELECT COUNT(*)
                        FROM kg_assertions ka
                        WHERE ka.chunk_id = dc.chunk_id
                      ) AS kg_assertion_count,
                      COALESCE(cv.status, dc.status) AS validation_status,
                      cv.quality_score,
                      cv.reasons
                    FROM document_chunks dc
                    LEFT JOIN chunk_validations cv ON cv.chunk_id = dc.chunk_id
                    WHERE {" OR ".join(clauses)}
                    ORDER BY dc.document_id, dc.page_start, dc.chunk_index
                    LIMIT %s
                    """,
                    tuple(params + [limit]),
                )
                return [dict(row) for row in cur.fetchall()]

    def list_page_assets_by_ids(self, asset_ids: list[str]) -> list[dict]:
        if not asset_ids:
            return []
        with psycopg.connect(self.dsn, row_factory=dict_row) as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT
                      asset_id,
                      document_id,
                      tenant_id,
                      page_number,
                      asset_index,
                      asset_type,
                      bbox_json,
                      asset_path,
                      content_hash,
                      ocr_text,
                      description_text,
                      search_text,
                      metadata_json,
                      created_at,
                      updated_at
                    FROM page_assets
                    WHERE asset_id = ANY(%s)
                    """,
                    (asset_ids,),
                )
                rows = [dict(row) for row in cur.fetchall()]
        positions = {asset_id: index for index, asset_id in enumerate(asset_ids)}
        rows.sort(key=lambda row: positions.get(row["asset_id"], 10**9))
        return rows

    def list_page_assets_for_chunks(self, chunk_ids: list[str], limit: int = 200) -> list[dict]:
        if not chunk_ids:
            return []
        with psycopg.connect(self.dsn, row_factory=dict_row) as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT
                      pa.asset_id,
                      pa.document_id,
                      pa.tenant_id,
                      pa.page_number,
                      pa.asset_index,
                      pa.asset_type,
                      pa.bbox_json,
                      pa.asset_path,
                      pa.content_hash,
                      pa.ocr_text,
                      pa.description_text,
                      pa.search_text,
                      pa.metadata_json,
                      pa.created_at,
                      pa.updated_at
                    FROM page_assets pa
                    JOIN chunk_asset_links cal ON cal.asset_id = pa.asset_id
                    WHERE cal.chunk_id = ANY(%s)
                    ORDER BY pa.page_number, pa.asset_index
                    LIMIT %s
                    """,
                    (chunk_ids, limit),
                    )
                return [dict(row) for row in cur.fetchall()]

    def search_chunk_records_lexical(
        self,
        query_text: str,
        tenant_id: str,
        document_ids: list[str] | None = None,
        limit: int = 12,
    ) -> list[dict]:
        tokens = [item for item in re.split(r"[^a-z0-9]+", query_text.lower()) if len(item) >= 3]
        if not tokens:
            return []
        match_clauses: list[str] = []
        score_sql: list[str] = []
        score_params: list[object] = []
        match_params: list[object] = []
        for token in tokens[:8]:
            needle = f"%{token}%"
            prefix = f"{token}%"
            match_clauses.append(
                "("
                "dc.text ILIKE %s OR "
                "COALESCE(dc.metadata_json->>'section_title', '') ILIKE %s OR "
                "COALESCE(dc.metadata_json->>'title', '') ILIKE %s OR "
                "COALESCE(dc.metadata_json->>'section_heading', '') ILIKE %s"
                ")"
            )
            match_params.extend([needle, needle, needle, needle])
            score_sql.extend(
                [
                    "MAX(CASE WHEN COALESCE(dc.metadata_json->>'title', '') ILIKE %s THEN 28 ELSE 0 END)",
                    "MAX(CASE WHEN COALESCE(dc.metadata_json->>'section_title', '') ILIKE %s THEN 20 ELSE 0 END)",
                    "MAX(CASE WHEN COALESCE(dc.metadata_json->>'section_heading', '') ILIKE %s THEN 14 ELSE 0 END)",
                    "MAX(CASE WHEN dc.text ILIKE %s THEN 8 ELSE 0 END)",
                ]
            )
            score_params.extend([prefix, needle, needle, needle])
        clauses = [
            "(" + " OR ".join(match_clauses) + ")",
            "dc.tenant_id = %s",
            "COALESCE(cv.status, dc.status) = 'accepted'",
        ]
        params: list[object] = list(score_params) + list(match_params) + [tenant_id]
        if document_ids:
            clauses.append("dc.document_id = ANY(%s)")
            params.append(document_ids)
        relevance_score = " + ".join(score_sql) if score_sql else "0"
        where_clause = "WHERE " + " AND ".join(clauses)
        with psycopg.connect(self.dsn, row_factory=dict_row) as conn:
            with conn.cursor() as cur:
                cur.execute(
                    f"""
                    SELECT
                      dc.chunk_id,
                      dc.text AS document,
                      dc.document_id,
                      dc.metadata_json,
                      ({relevance_score}) AS lexical_score
                    FROM document_chunks dc
                    LEFT JOIN chunk_validations cv ON cv.chunk_id = dc.chunk_id
                    {where_clause}
                    GROUP BY dc.chunk_id, dc.text, dc.document_id, dc.metadata_json
                    ORDER BY lexical_score DESC, dc.chunk_index ASC
                    LIMIT %s
                    """,
                    tuple(params + [limit]),
                )
                rows = [dict(row) for row in cur.fetchall()]
        results: list[dict] = []
        for index, row in enumerate(rows, start=1):
            metadata = dict(row.get("metadata_json") or {})
            metadata.setdefault("document_id", str(row.get("document_id") or ""))
            results.append(
                {
                    "chunk_id": str(row["chunk_id"]),
                    "document": str(row.get("document") or ""),
                    "metadata": metadata,
                    "distance": None,
                    "rank": index,
                    "lexical_score": float(row.get("lexical_score") or 0.0),
                    "match_source": "lexical",
                }
            )
        return results

    def search_page_assets_lexical(
        self,
        query_text: str,
        tenant_id: str,
        document_ids: list[str] | None = None,
        limit: int = 8,
    ) -> list[dict]:
        tokens = [item for item in re.split(r"[^a-z0-9]+", query_text.lower()) if len(item) >= 3]
        if not tokens:
            return []
        match_clauses: list[str] = []
        score_sql: list[str] = []
        score_params: list[object] = []
        match_params: list[object] = []
        for token in tokens[:8]:
            needle = f"%{token}%"
            prefix = f"{token}%"
            match_clauses.append(
                "("
                "pa.search_text ILIKE %s OR "
                "COALESCE(pa.metadata_json->>'label', '') ILIKE %s OR "
                "COALESCE(pa.asset_type, '') ILIKE %s"
                ")"
            )
            match_params.extend([needle, needle, needle])
            score_sql.extend(
                [
                    "MAX(CASE WHEN COALESCE(pa.metadata_json->>'label', '') ILIKE %s THEN 24 ELSE 0 END)",
                    "MAX(CASE WHEN pa.search_text ILIKE %s THEN 10 ELSE 0 END)",
                    "MAX(CASE WHEN COALESCE(pa.asset_type, '') ILIKE %s THEN 6 ELSE 0 END)",
                ]
            )
            score_params.extend([prefix, needle, needle])
        clauses = [
            "(" + " OR ".join(match_clauses) + ")",
            "pa.tenant_id = %s",
        ]
        params: list[object] = list(score_params) + list(match_params) + [tenant_id]
        if document_ids:
            clauses.append("pa.document_id = ANY(%s)")
            params.append(document_ids)
        relevance_score = " + ".join(score_sql) if score_sql else "0"
        where_clause = "WHERE " + " AND ".join(clauses)
        with psycopg.connect(self.dsn, row_factory=dict_row) as conn:
            with conn.cursor() as cur:
                cur.execute(
                    f"""
                    SELECT
                      pa.asset_id,
                      pa.search_text AS document,
                      pa.document_id,
                      pa.page_number,
                      pa.asset_type,
                      pa.metadata_json,
                      ({relevance_score}) AS lexical_score
                    FROM page_assets pa
                    {where_clause}
                    GROUP BY pa.asset_id, pa.search_text, pa.document_id, pa.page_number, pa.asset_type, pa.metadata_json
                    ORDER BY lexical_score DESC, pa.page_number ASC, pa.asset_index ASC
                    LIMIT %s
                    """,
                    tuple(params + [limit]),
                )
                rows = [dict(row) for row in cur.fetchall()]
        results: list[dict] = []
        for index, row in enumerate(rows, start=1):
            metadata = dict(row.get("metadata_json") or {})
            metadata.setdefault("document_id", str(row.get("document_id") or ""))
            metadata.setdefault("page_number", row.get("page_number"))
            metadata.setdefault("asset_type", str(row.get("asset_type") or ""))
            results.append(
                {
                    "asset_id": str(row["asset_id"]),
                    "document": str(row.get("document") or ""),
                    "metadata": metadata,
                    "distance": None,
                    "rank": index,
                    "lexical_score": float(row.get("lexical_score") or 0.0),
                    "match_source": "lexical",
                }
            )
        return results

    def list_chunk_asset_links_for_chunks(self, chunk_ids: list[str], limit: int = 250) -> list[dict]:
        if not chunk_ids:
            return []
        with psycopg.connect(self.dsn, row_factory=dict_row) as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT
                      link_id,
                      chunk_id,
                      asset_id,
                      link_type,
                      confidence,
                      metadata_json,
                      created_at
                    FROM chunk_asset_links
                    WHERE chunk_id = ANY(%s)
                    ORDER BY created_at ASC
                    LIMIT %s
                    """,
                    (chunk_ids, limit),
                )
                return [dict(row) for row in cur.fetchall()]

    def list_kg_assertions_for_chunks(self, chunk_ids: list[str], limit: int = 200, per_chunk_limit: int | None = None) -> list[dict]:
        if not chunk_ids:
            return []
        with psycopg.connect(self.dsn, row_factory=dict_row) as conn:
            with conn.cursor() as cur:
                if per_chunk_limit and per_chunk_limit > 0:
                    cur.execute(
                        """
                        WITH ranked_assertions AS (
                          SELECT
                            assertion_id,
                            document_id,
                            chunk_id,
                            subject_entity_id,
                            predicate,
                            object_entity_id,
                            object_literal,
                            confidence,
                            qualifiers,
                            status,
                            created_at,
                            ROW_NUMBER() OVER (
                              PARTITION BY chunk_id
                              ORDER BY confidence DESC, created_at DESC
                            ) AS row_num
                          FROM kg_assertions
                          WHERE chunk_id = ANY(%s)
                        )
                        SELECT
                          assertion_id,
                          document_id,
                          chunk_id,
                          subject_entity_id,
                          predicate,
                          object_entity_id,
                          object_literal,
                          confidence,
                          qualifiers,
                          status,
                          created_at
                        FROM ranked_assertions
                        WHERE row_num <= %s
                        ORDER BY confidence DESC, created_at DESC
                        LIMIT %s
                        """,
                        (chunk_ids, per_chunk_limit, limit),
                    )
                else:
                    cur.execute(
                        """
                        SELECT
                          assertion_id,
                          document_id,
                          chunk_id,
                          subject_entity_id,
                          predicate,
                          object_entity_id,
                          object_literal,
                          confidence,
                          qualifiers,
                          status,
                          created_at
                        FROM kg_assertions
                        WHERE chunk_id = ANY(%s)
                        ORDER BY confidence DESC, created_at DESC
                        LIMIT %s
                        """,
                        (chunk_ids, limit),
                    )
                return [dict(row) for row in cur.fetchall()]

    def list_kg_neighbor_assertions_for_entities(
        self,
        entity_ids: list[str],
        tenant_id: str,
        *,
        document_ids: list[str] | None = None,
        exclude_assertion_ids: list[str] | None = None,
        limit: int = 16,
        per_entity_limit: int | None = None,
    ) -> list[dict]:
        normalized_entity_ids = [
            str(entity_id).strip()
            for entity_id in entity_ids
            if str(entity_id).strip()
        ]
        if not normalized_entity_ids:
            return []
        normalized_exclude_ids = [
            str(assertion_id).strip()
            for assertion_id in (exclude_assertion_ids or [])
            if str(assertion_id).strip()
        ]
        with psycopg.connect(self.dsn, row_factory=dict_row) as conn:
            with conn.cursor() as cur:
                clauses = [
                    "d.tenant_id = %s",
                    "ka.status = 'accepted'",
                ]
                params: list[object] = [normalized_entity_ids, tenant_id]
                if document_ids:
                    clauses.append("ka.document_id = ANY(%s)")
                    params.append(document_ids)
                if normalized_exclude_ids:
                    clauses.append("NOT (ka.assertion_id = ANY(%s))")
                    params.append(normalized_exclude_ids)
                where_clause = " AND ".join(clauses)
                if per_entity_limit and per_entity_limit > 0:
                    cur.execute(
                        f"""
                        WITH seed_entities AS (
                          SELECT UNNEST(%s::text[]) AS seed_entity_id
                        ),
                        grouped_neighbors AS (
                          SELECT
                            se.seed_entity_id,
                            ka.assertion_id,
                            ka.document_id,
                            ka.chunk_id,
                            ka.subject_entity_id,
                            ka.predicate,
                            ka.object_entity_id,
                            ka.object_literal,
                            ka.confidence,
                            ka.qualifiers,
                            ka.status,
                            ka.created_at,
                            CASE
                              WHEN ka.subject_entity_id = se.seed_entity_id THEN ka.object_entity_id
                              ELSE ka.subject_entity_id
                            END AS neighbor_entity_id,
                            COUNT(ev.evidence_id) AS evidence_count
                          FROM seed_entities se
                          JOIN kg_assertions ka
                            ON ka.subject_entity_id = se.seed_entity_id
                            OR ka.object_entity_id = se.seed_entity_id
                          JOIN documents d ON d.document_id = ka.document_id
                          LEFT JOIN kg_assertion_evidence ev ON ev.assertion_id = ka.assertion_id
                          WHERE {where_clause}
                          GROUP BY
                            se.seed_entity_id,
                            ka.assertion_id,
                            ka.document_id,
                            ka.chunk_id,
                            ka.subject_entity_id,
                            ka.predicate,
                            ka.object_entity_id,
                            ka.object_literal,
                            ka.confidence,
                            ka.qualifiers,
                            ka.status,
                            ka.created_at
                        ),
                        ranked_neighbors AS (
                          SELECT
                            seed_entity_id,
                            assertion_id,
                            document_id,
                            chunk_id,
                            subject_entity_id,
                            predicate,
                            object_entity_id,
                            object_literal,
                            confidence,
                            qualifiers,
                            status,
                            created_at,
                            neighbor_entity_id,
                            evidence_count,
                            ROW_NUMBER() OVER (
                              PARTITION BY seed_entity_id
                              ORDER BY confidence DESC, evidence_count DESC, created_at DESC
                            ) AS row_num
                          FROM grouped_neighbors
                        )
                        SELECT
                          seed_entity_id,
                          assertion_id,
                          document_id,
                          chunk_id,
                          subject_entity_id,
                          predicate,
                          object_entity_id,
                          object_literal,
                          confidence,
                          qualifiers,
                          status,
                          created_at,
                          neighbor_entity_id,
                          evidence_count
                        FROM ranked_neighbors
                        WHERE row_num <= %s
                        ORDER BY confidence DESC, evidence_count DESC, created_at DESC
                        LIMIT %s
                        """,
                        tuple(params + [per_entity_limit, limit]),
                    )
                else:
                    cur.execute(
                        f"""
                        WITH seed_entities AS (
                          SELECT UNNEST(%s::text[]) AS seed_entity_id
                        )
                        SELECT
                          se.seed_entity_id,
                          ka.assertion_id,
                          ka.document_id,
                          ka.chunk_id,
                          ka.subject_entity_id,
                          ka.predicate,
                          ka.object_entity_id,
                          ka.object_literal,
                          ka.confidence,
                          ka.qualifiers,
                          ka.status,
                          ka.created_at,
                          CASE
                            WHEN ka.subject_entity_id = se.seed_entity_id THEN ka.object_entity_id
                            ELSE ka.subject_entity_id
                          END AS neighbor_entity_id,
                          COUNT(ev.evidence_id) AS evidence_count
                        FROM seed_entities se
                        JOIN kg_assertions ka
                          ON ka.subject_entity_id = se.seed_entity_id
                          OR ka.object_entity_id = se.seed_entity_id
                        JOIN documents d ON d.document_id = ka.document_id
                        LEFT JOIN kg_assertion_evidence ev ON ev.assertion_id = ka.assertion_id
                        WHERE {where_clause}
                        GROUP BY
                          se.seed_entity_id,
                          ka.assertion_id,
                          ka.document_id,
                          ka.chunk_id,
                          ka.subject_entity_id,
                          ka.predicate,
                          ka.object_entity_id,
                          ka.object_literal,
                          ka.confidence,
                          ka.qualifiers,
                          ka.status,
                          ka.created_at
                        ORDER BY ka.confidence DESC, COUNT(ev.evidence_id) DESC, ka.created_at DESC
                        LIMIT %s
                        """,
                        tuple(params + [limit]),
                    )
                return [dict(row) for row in cur.fetchall()]

    def list_kg_evidence_for_assertions(self, assertion_ids: list[str], limit: int = 200, per_assertion_limit: int | None = None) -> list[dict]:
        if not assertion_ids:
            return []
        with psycopg.connect(self.dsn, row_factory=dict_row) as conn:
            with conn.cursor() as cur:
                if per_assertion_limit and per_assertion_limit > 0:
                    cur.execute(
                        """
                        WITH ranked_evidence AS (
                          SELECT
                            evidence_id,
                            assertion_id,
                            excerpt,
                            start_offset,
                            end_offset,
                            created_at,
                            ROW_NUMBER() OVER (
                              PARTITION BY assertion_id
                              ORDER BY created_at DESC
                            ) AS row_num
                          FROM kg_assertion_evidence
                          WHERE assertion_id = ANY(%s)
                        )
                        SELECT evidence_id, assertion_id, excerpt, start_offset, end_offset, created_at
                        FROM ranked_evidence
                        WHERE row_num <= %s
                        ORDER BY created_at DESC
                        LIMIT %s
                        """,
                        (assertion_ids, per_assertion_limit, limit),
                    )
                else:
                    cur.execute(
                        """
                        SELECT evidence_id, assertion_id, excerpt, start_offset, end_offset, created_at
                        FROM kg_assertion_evidence
                        WHERE assertion_id = ANY(%s)
                        ORDER BY created_at DESC
                        LIMIT %s
                        """,
                        (assertion_ids, limit),
                    )
                return [dict(row) for row in cur.fetchall()]

    def list_kg_entities_by_ids(self, entity_ids: list[str]) -> list[dict]:
        if not entity_ids:
            return []
        with psycopg.connect(self.dsn, row_factory=dict_row) as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT entity_id, canonical_name, entity_type, source, created_at, updated_at
                    FROM kg_entities
                    WHERE entity_id = ANY(%s)
                    """,
                    (entity_ids,),
                )
                rows = [dict(row) for row in cur.fetchall()]
        positions = {entity_id: index for index, entity_id in enumerate(entity_ids)}
        rows.sort(key=lambda row: positions.get(row["entity_id"], 10**9))
        return rows
