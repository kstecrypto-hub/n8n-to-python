from __future__ import annotations

import base64
from datetime import datetime, timedelta, timezone
import hashlib
import hmac
import json
import logging
from pathlib import Path
import re
import secrets
import sqlite3
from typing import Any
from uuid import uuid4

import psycopg
from psycopg import sql
from psycopg.errors import UniqueViolation
from psycopg.rows import dict_row
from psycopg.types.json import Jsonb

from src.bee_ingestion.settings import settings

logger = logging.getLogger(__name__)

_EMAIL_RE = re.compile(r"^[^@\s]+@[^@\s]+\.[^@\s]+$")
_PASSWORD_MAX_LENGTH = 256
_PBKDF2_ITERATIONS = 600_000
_SCHEMA_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]{0,62}$")
ALLOWED_AUTH_ROLES = {
    "guest",
    "member",
    "review_analyst",
    "knowledge_curator",
    "tenant_admin",
    "platform_owner",
}
ALLOWED_AUTH_STATUSES = {"active", "disabled"}
ALLOWED_AUTH_PERMISSIONS = {
    "chat.use",
    "chat.history.read",
    "sensor.read",
    "sensor.write",
    "documents.read",
    "documents.write",
    "kg.read",
    "kg.write",
    "agent.review",
    "runtime.read",
    "runtime.write",
    "accounts.read",
    "accounts.write",
    "rate_limits.write",
    "db.rows.write",
    "db.sql.write",
}
DEFAULT_ROLE_PERMISSIONS = {
    "guest": ["chat.use"],
    "member": ["chat.use", "chat.history.read", "sensor.read", "sensor.write"],
    "review_analyst": ["chat.use", "chat.history.read", "sensor.read", "documents.read", "kg.read", "agent.review"],
    "knowledge_curator": [
        "chat.use",
        "chat.history.read",
        "sensor.read",
        "sensor.write",
        "documents.read",
        "documents.write",
        "kg.read",
        "kg.write",
        "agent.review",
    ],
    "tenant_admin": [
        "chat.use",
        "chat.history.read",
        "sensor.read",
        "sensor.write",
        "documents.read",
        "documents.write",
        "kg.read",
        "kg.write",
        "agent.review",
        "runtime.read",
        "runtime.write",
        "accounts.read",
        "accounts.write",
        "rate_limits.write",
        "db.rows.write",
    ],
    "platform_owner": sorted(ALLOWED_AUTH_PERMISSIONS),
}


def _utcnow() -> datetime:
    return datetime.now(timezone.utc)


def _isoformat(value: datetime) -> str:
    return value.astimezone(timezone.utc).isoformat()


def _coerce_datetime(value: Any) -> datetime | None:
    if value is None or value == "":
        return None
    if isinstance(value, datetime):
        return value.astimezone(timezone.utc)
    return datetime.fromisoformat(str(value)).astimezone(timezone.utc)


def _stringify_datetime(value: Any) -> str:
    parsed = _coerce_datetime(value)
    return _isoformat(parsed) if parsed is not None else ""


def _normalize_email(email: str) -> str:
    normalized = str(email or "").strip().lower()
    if not _EMAIL_RE.match(normalized):
        raise ValueError("A valid email address is required")
    return normalized


def _normalize_display_name(display_name: str | None) -> str:
    return str(display_name or "").strip().replace("\x00", "")[:120]


def _normalize_tenant_id(tenant_id: str | None) -> str:
    normalized = str(tenant_id or "shared").strip().replace("\x00", "")[:120]
    return normalized or "shared"


def _normalize_role(role: str | None) -> str:
    normalized = str(role or "member").strip().lower()
    if normalized not in ALLOWED_AUTH_ROLES:
        raise ValueError(f"Unsupported auth role '{normalized}'")
    return normalized


def _normalize_status(status: str | None) -> str:
    normalized = str(status or "active").strip().lower()
    if normalized not in ALLOWED_AUTH_STATUSES:
        raise ValueError(f"Unsupported auth status '{normalized}'")
    return normalized


def _default_permissions_for_role(role: str | None) -> list[str]:
    normalized_role = _normalize_role(role)
    return list(DEFAULT_ROLE_PERMISSIONS.get(normalized_role, DEFAULT_ROLE_PERMISSIONS["member"]))


def _normalize_permissions(permissions: list[str] | None, *, role: str | None = None) -> list[str]:
    if permissions is None:
        return sorted(_default_permissions_for_role(role))
    normalized: list[str] = []
    seen: set[str] = set()
    for raw_value in permissions:
        value = str(raw_value or "").strip().lower()
        if not value:
            continue
        if value not in ALLOWED_AUTH_PERMISSIONS:
            raise ValueError(f"Unsupported auth permission '{value}'")
        if value in seen:
            continue
        seen.add(value)
        normalized.append(value)
    return sorted(normalized)


def _normalize_password(password: str) -> str:
    normalized = str(password or "")
    if len(normalized) < settings.auth_password_min_length:
        raise ValueError(f"Password must be at least {settings.auth_password_min_length} characters")
    if len(normalized) > _PASSWORD_MAX_LENGTH:
        raise ValueError("Password is too long")
    return normalized


def _hash_password(password: str, salt: bytes, *, iterations: int = _PBKDF2_ITERATIONS) -> str:
    digest = hashlib.pbkdf2_hmac("sha256", password.encode("utf-8"), salt, iterations)
    return base64.b64encode(digest).decode("ascii")


def _hash_session_token(token: str) -> str:
    return hashlib.sha256(token.encode("utf-8")).hexdigest()


def _normalize_schema_name(schema_name: str | None) -> str:
    normalized = str(schema_name or settings.auth_postgres_schema or "auth").strip().lower()
    if not _SCHEMA_RE.match(normalized):
        raise ValueError(f"Unsupported auth schema '{normalized}'")
    return normalized


def _schema_from_hint(path_hint: str | Path) -> str:
    digest = hashlib.sha256(str(Path(path_hint)).encode("utf-8")).hexdigest()[:16]
    return _normalize_schema_name(f"auth_{digest}")


class AuthStore:
    def __init__(self, path: str | Path | None = None, *, dsn: str | None = None, schema_name: str | None = None) -> None:
        resolved_dsn = str(dsn or settings.auth_postgres_dsn or "").strip()
        if not resolved_dsn:
            raise ValueError("AUTH_POSTGRES_DSN must be configured; auth storage does not fall back to the application database")
        self.dsn = resolved_dsn
        self.schema_name = _normalize_schema_name(schema_name) if schema_name is not None else (
            _schema_from_hint(path) if path is not None else _normalize_schema_name(settings.auth_postgres_schema)
        )
        self._init_db()

    def _connect(self) -> psycopg.Connection:
        conn = psycopg.connect(self.dsn, row_factory=dict_row)
        with conn.cursor() as cur:
            cur.execute(sql.SQL("SET search_path TO {}").format(sql.Identifier(self.schema_name)))
        return conn

    def _init_db(self) -> None:
        with psycopg.connect(self.dsn, row_factory=dict_row) as conn:
            with conn.cursor() as cur:
                cur.execute(sql.SQL("CREATE SCHEMA IF NOT EXISTS {}").format(sql.Identifier(self.schema_name)))
                cur.execute(sql.SQL("SET search_path TO {}").format(sql.Identifier(self.schema_name)))
                cur.execute(
                    """
                    CREATE TABLE IF NOT EXISTS auth_users (
                      user_id TEXT PRIMARY KEY,
                      email TEXT NOT NULL UNIQUE,
                      display_name TEXT,
                      tenant_id TEXT NOT NULL DEFAULT 'shared',
                      role TEXT NOT NULL DEFAULT 'member',
                      status TEXT NOT NULL DEFAULT 'active',
                      permissions_json JSONB NOT NULL DEFAULT '[]'::jsonb,
                      password_hash TEXT NOT NULL,
                      password_salt TEXT NOT NULL,
                      password_iterations INTEGER NOT NULL,
                      created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
                      updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
                      last_login_at TIMESTAMPTZ
                    )
                    """
                )
                cur.execute(
                    """
                    CREATE TABLE IF NOT EXISTS auth_sessions (
                      auth_session_id TEXT PRIMARY KEY,
                      user_id TEXT NOT NULL REFERENCES auth_users(user_id) ON DELETE CASCADE,
                      tenant_id TEXT NOT NULL DEFAULT 'shared',
                      session_token_hash TEXT NOT NULL,
                      created_at TIMESTAMPTZ NOT NULL,
                      last_seen_at TIMESTAMPTZ NOT NULL,
                      expires_at TIMESTAMPTZ NOT NULL,
                      revoked_at TIMESTAMPTZ
                    )
                    """
                )
                cur.execute("ALTER TABLE auth_users ADD COLUMN IF NOT EXISTS permissions_json JSONB NOT NULL DEFAULT '[]'::jsonb")
                cur.execute(
                    """
                    CREATE INDEX IF NOT EXISTS auth_users_tenant_idx
                      ON auth_users (tenant_id, role, status, created_at DESC)
                    """
                )
                cur.execute(
                    """
                    CREATE INDEX IF NOT EXISTS auth_sessions_user_idx
                      ON auth_sessions (user_id, expires_at)
                    """
                )
                cur.execute(
                    """
                    CREATE INDEX IF NOT EXISTS auth_sessions_active_idx
                      ON auth_sessions (tenant_id, revoked_at, expires_at)
                    """
                )
                cur.execute(
                    """
                    CREATE TABLE IF NOT EXISTS auth_audit_events (
                      event_id TEXT PRIMARY KEY,
                      user_id TEXT REFERENCES auth_users(user_id) ON DELETE SET NULL,
                      auth_session_id TEXT REFERENCES auth_sessions(auth_session_id) ON DELETE SET NULL,
                      tenant_id TEXT NOT NULL DEFAULT 'shared',
                      event_type TEXT NOT NULL,
                      outcome TEXT NOT NULL DEFAULT 'success',
                      email TEXT,
                      ip_address TEXT,
                      user_agent TEXT,
                      metadata_json JSONB NOT NULL DEFAULT '{}'::jsonb,
                      created_at TIMESTAMPTZ NOT NULL DEFAULT now()
                    )
                    """
                )
                cur.execute(
                    """
                    CREATE INDEX IF NOT EXISTS auth_audit_events_tenant_idx
                      ON auth_audit_events (tenant_id, event_type, created_at DESC)
                    """
                )
                cur.execute(
                    """
                    CREATE INDEX IF NOT EXISTS auth_audit_events_user_idx
                      ON auth_audit_events (user_id, created_at DESC)
                    """
                )

    def record_event(
        self,
        *,
        event_type: str,
        outcome: str = "success",
        tenant_id: str | None = None,
        user_id: str | None = None,
        auth_session_id: str | None = None,
        email: str | None = None,
        ip_address: str | None = None,
        user_agent: str | None = None,
        metadata: dict[str, Any] | None = None,
    ) -> str:
        normalized_event_type = str(event_type or "").strip().lower()[:80]
        if not normalized_event_type:
            raise ValueError("event_type is required")
        normalized_outcome = str(outcome or "success").strip().lower()[:40] or "success"
        event_id = str(uuid4())
        with self._connect() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO auth_audit_events (
                      event_id,
                      user_id,
                      auth_session_id,
                      tenant_id,
                      event_type,
                      outcome,
                      email,
                      ip_address,
                      user_agent,
                      metadata_json
                    )
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    """,
                    (
                        event_id,
                        str(user_id or "").strip() or None,
                        str(auth_session_id or "").strip() or None,
                        _normalize_tenant_id(tenant_id),
                        normalized_event_type,
                        normalized_outcome,
                        str(email or "").strip().lower()[:320] or None,
                        str(ip_address or "").strip()[:160] or None,
                        str(user_agent or "").strip()[:512] or None,
                        Jsonb(metadata or {}),
                    ),
                )
        return event_id

    @staticmethod
    def _decode_permissions(row: dict[str, Any]) -> list[str]:
        role = str(row.get("role") or "member")
        raw_value = row.get("permissions_json", [])
        if isinstance(raw_value, list):
            return _normalize_permissions([str(item) for item in raw_value], role=role)
        if isinstance(raw_value, tuple):
            return _normalize_permissions([str(item) for item in raw_value], role=role)
        if isinstance(raw_value, str):
            try:
                parsed = json.loads(raw_value or "[]")
                if isinstance(parsed, list):
                    return _normalize_permissions([str(item) for item in parsed], role=role)
                logger.warning("Unexpected auth permissions payload type for role '%s'; falling back to role defaults", role)
            except (TypeError, ValueError, json.JSONDecodeError):
                logger.warning("Failed to decode persisted auth permissions for role '%s'; falling back to role defaults", role)
                return _default_permissions_for_role(role)
        elif raw_value not in (None, ""):
            logger.warning(
                "Unsupported persisted auth permissions payload type '%s' for role '%s'; falling back to role defaults",
                type(raw_value).__name__,
                role,
            )
        return _default_permissions_for_role(role)

    def _row_to_user(self, row: dict[str, Any] | None) -> dict[str, Any] | None:
        if row is None:
            return None
        permissions = self._decode_permissions(row)
        return {
            "user_id": str(row.get("user_id") or ""),
            "email": str(row.get("email") or ""),
            "display_name": str(row.get("display_name") or ""),
            "tenant_id": str(row.get("tenant_id") or "shared"),
            "role": str(row.get("role") or "member"),
            "status": str(row.get("status") or "active"),
            "permissions": permissions,
            "created_at": _stringify_datetime(row.get("created_at")),
            "updated_at": _stringify_datetime(row.get("updated_at")),
            "last_login_at": _stringify_datetime(row.get("last_login_at")) if row.get("last_login_at") else None,
            "active_sessions": int(row.get("active_sessions") or 0),
        }

    def create_user(
        self,
        email: str,
        password: str,
        *,
        display_name: str | None = None,
        tenant_id: str = "shared",
        role: str = "member",
        status: str = "active",
        permissions: list[str] | None = None,
    ) -> dict[str, Any]:
        normalized_email = _normalize_email(email)
        normalized_password = _normalize_password(password)
        normalized_name = _normalize_display_name(display_name)
        normalized_tenant_id = _normalize_tenant_id(tenant_id)
        normalized_role = _normalize_role(role)
        normalized_status = _normalize_status(status)
        normalized_permissions = _normalize_permissions(permissions, role=normalized_role)
        salt = secrets.token_bytes(16)
        now = _utcnow()
        user_id = str(uuid4())
        password_hash = _hash_password(normalized_password, salt)
        try:
            with self._connect() as conn:
                with conn.cursor() as cur:
                    cur.execute(
                        """
                        INSERT INTO auth_users (
                          user_id, email, display_name, tenant_id, role, status, permissions_json,
                          password_hash, password_salt, password_iterations,
                          created_at, updated_at
                        )
                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                        """,
                        (
                            user_id,
                            normalized_email,
                            normalized_name,
                            normalized_tenant_id,
                            normalized_role,
                            normalized_status,
                            Jsonb(normalized_permissions),
                            password_hash,
                            base64.b64encode(salt).decode("ascii"),
                            _PBKDF2_ITERATIONS,
                            now,
                            now,
                        ),
                    )
        except UniqueViolation as exc:
            raise ValueError("An account with that email already exists") from exc
        return self.get_user(user_id) or {}

    def get_user(self, user_id: str) -> dict[str, Any] | None:
        with self._connect() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT
                      u.user_id,
                      u.email,
                      u.display_name,
                      u.tenant_id,
                      u.role,
                      u.status,
                      u.permissions_json,
                      u.created_at,
                      u.updated_at,
                      u.last_login_at,
                      (
                        SELECT COUNT(*)
                        FROM auth_sessions s
                        WHERE s.user_id = u.user_id
                          AND s.revoked_at IS NULL
                          AND s.expires_at > %s
                      ) AS active_sessions
                    FROM auth_users u
                    WHERE u.user_id = %s
                    """,
                    (_utcnow(), str(user_id)),
                )
                row = cur.fetchone()
        return self._row_to_user(row)

    def count_users(
        self,
        *,
        search: str | None = None,
        tenant_id: str | None = None,
        role: str | None = None,
        status: str | None = None,
    ) -> int:
        clauses = ["1 = 1"]
        params: list[Any] = []
        if search:
            needle = f"%{str(search).strip()}%"
            clauses.append("(email ILIKE %s OR display_name ILIKE %s)")
            params.extend([needle, needle])
        if tenant_id:
            clauses.append("tenant_id = %s")
            params.append(_normalize_tenant_id(tenant_id))
        if role:
            clauses.append("role = %s")
            params.append(_normalize_role(role))
        if status:
            clauses.append("status = %s")
            params.append(_normalize_status(status))
        query = f"SELECT COUNT(*) AS value FROM auth_users WHERE {' AND '.join(clauses)}"
        with self._connect() as conn:
            with conn.cursor() as cur:
                cur.execute(query, tuple(params))
                row = cur.fetchone()
        return int(row["value"] or 0) if row else 0

    def list_users(
        self,
        *,
        search: str | None = None,
        tenant_id: str | None = None,
        role: str | None = None,
        status: str | None = None,
        limit: int = 100,
        offset: int = 0,
    ) -> list[dict[str, Any]]:
        clauses = ["1 = 1"]
        params: list[Any] = [_utcnow()]
        if search:
            needle = f"%{str(search).strip()}%"
            clauses.append("(u.email ILIKE %s OR u.display_name ILIKE %s)")
            params.extend([needle, needle])
        if tenant_id:
            clauses.append("u.tenant_id = %s")
            params.append(_normalize_tenant_id(tenant_id))
        if role:
            clauses.append("u.role = %s")
            params.append(_normalize_role(role))
        if status:
            clauses.append("u.status = %s")
            params.append(_normalize_status(status))
        params.extend([int(limit), int(offset)])
        with self._connect() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    f"""
                    SELECT
                      u.user_id,
                      u.email,
                      u.display_name,
                      u.tenant_id,
                      u.role,
                      u.status,
                      u.permissions_json,
                      u.created_at,
                      u.updated_at,
                      u.last_login_at,
                      (
                        SELECT COUNT(*)
                        FROM auth_sessions s
                        WHERE s.user_id = u.user_id
                          AND s.revoked_at IS NULL
                          AND s.expires_at > %s
                      ) AS active_sessions
                    FROM auth_users u
                    WHERE {' AND '.join(clauses)}
                    ORDER BY u.created_at DESC, u.email ASC
                    LIMIT %s OFFSET %s
                    """,
                    tuple(params),
                )
                rows = cur.fetchall()
        return [self._row_to_user(row) or {} for row in rows]

    def authenticate_user(self, email: str, password: str) -> dict[str, Any] | None:
        normalized_email = _normalize_email(email)
        normalized_password = _normalize_password(password)
        with self._connect() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT *
                    FROM auth_users
                    WHERE email = %s AND status = 'active'
                    """,
                    (normalized_email,),
                )
                row = cur.fetchone()
                if row is None:
                    return None
                salt = base64.b64decode(str(row["password_salt"]).encode("ascii"))
                expected = str(row["password_hash"])
                actual = _hash_password(normalized_password, salt, iterations=int(row["password_iterations"] or _PBKDF2_ITERATIONS))
                if not hmac.compare_digest(expected, actual):
                    return None
                now = _utcnow()
                cur.execute(
                    "UPDATE auth_users SET last_login_at = %s, updated_at = %s WHERE user_id = %s",
                    (now, now, str(row["user_id"])),
                )
        return self.get_user(str(row["user_id"]))

    def list_sessions(self, *, user_id: str | None = None, limit: int = 50, offset: int = 0) -> list[dict[str, Any]]:
        clauses = ["1 = 1"]
        params: list[Any] = []
        if user_id:
            clauses.append("s.user_id = %s")
            params.append(str(user_id))
        params.extend([int(limit), int(offset)])
        with self._connect() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    f"""
                    SELECT
                      s.auth_session_id,
                      s.user_id,
                      s.tenant_id,
                      s.created_at,
                      s.last_seen_at,
                      s.expires_at,
                      s.revoked_at,
                      u.email,
                      u.display_name
                    FROM auth_sessions s
                    JOIN auth_users u ON u.user_id = s.user_id
                    WHERE {' AND '.join(clauses)}
                    ORDER BY s.created_at DESC
                    LIMIT %s OFFSET %s
                    """,
                    tuple(params),
                )
                rows = cur.fetchall()
        sessions: list[dict[str, Any]] = []
        now = _utcnow()
        for row in rows:
            expires_at = _coerce_datetime(row.get("expires_at"))
            revoked_at = _coerce_datetime(row.get("revoked_at"))
            sessions.append(
                {
                    "auth_session_id": str(row["auth_session_id"]),
                    "user_id": str(row["user_id"]),
                    "tenant_id": str(row.get("tenant_id") or "shared"),
                    "created_at": _stringify_datetime(row.get("created_at")),
                    "last_seen_at": _stringify_datetime(row.get("last_seen_at")),
                    "expires_at": _stringify_datetime(row.get("expires_at")),
                    "revoked_at": _stringify_datetime(row.get("revoked_at")) if revoked_at else None,
                    "email": str(row.get("email") or ""),
                    "display_name": str(row.get("display_name") or ""),
                    "active": revoked_at is None and expires_at is not None and expires_at > now,
                }
            )
        return sessions

    def update_user(
        self,
        user_id: str,
        *,
        email: str | None = None,
        password: str | None = None,
        display_name: str | None = None,
        tenant_id: str | None = None,
        role: str | None = None,
        status: str | None = None,
        permissions: list[str] | None = None,
    ) -> dict[str, Any] | None:
        updates: list[str] = []
        params: list[Any] = []
        current_user = self.get_user(str(user_id))
        effective_role = _normalize_role(role) if role is not None else str((current_user or {}).get("role") or "member")
        if email is not None:
            updates.append("email = %s")
            params.append(_normalize_email(email))
        if display_name is not None:
            updates.append("display_name = %s")
            params.append(_normalize_display_name(display_name))
        if tenant_id is not None:
            updates.append("tenant_id = %s")
            params.append(_normalize_tenant_id(tenant_id))
        if role is not None:
            updates.append("role = %s")
            params.append(effective_role)
        if status is not None:
            updates.append("status = %s")
            params.append(_normalize_status(status))
        if permissions is not None:
            updates.append("permissions_json = %s")
            params.append(Jsonb(_normalize_permissions(permissions, role=effective_role)))
        elif role is not None:
            updates.append("permissions_json = %s")
            params.append(Jsonb(_normalize_permissions(None, role=effective_role)))
        if password is not None and str(password).strip():
            normalized_password = _normalize_password(password)
            salt = secrets.token_bytes(16)
            updates.extend(["password_hash = %s", "password_salt = %s", "password_iterations = %s"])
            params.extend([
                _hash_password(normalized_password, salt),
                base64.b64encode(salt).decode("ascii"),
                _PBKDF2_ITERATIONS,
            ])
        if not updates:
            raise ValueError("No user fields to update")
        now = _utcnow()
        updates.append("updated_at = %s")
        params.append(now)
        params.append(str(user_id))
        try:
            with self._connect() as conn:
                with conn.cursor() as cur:
                    cur.execute(
                        f"UPDATE auth_users SET {', '.join(updates)} WHERE user_id = %s",
                        tuple(params),
                    )
                    if int(cur.rowcount or 0) == 0:
                        return None
                    if status is not None and _normalize_status(status) != "active":
                        cur.execute(
                            "UPDATE auth_sessions SET revoked_at = %s WHERE user_id = %s AND revoked_at IS NULL",
                            (now, str(user_id)),
                        )
        except UniqueViolation as exc:
            raise ValueError("An account with that email already exists") from exc
        return self.get_user(str(user_id))

    def revoke_user_sessions(self, user_id: str) -> int:
        normalized_id = str(user_id or "").strip()
        if not normalized_id:
            return 0
        with self._connect() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    "UPDATE auth_sessions SET revoked_at = %s WHERE user_id = %s AND revoked_at IS NULL",
                    (_utcnow(), normalized_id),
                )
                return int(cur.rowcount or 0)

    def delete_user(self, user_id: str) -> int:
        normalized_id = str(user_id or "").strip()
        if not normalized_id:
            return 0
        with self._connect() as conn:
            with conn.cursor() as cur:
                cur.execute("DELETE FROM auth_users WHERE user_id = %s", (normalized_id,))
                return int(cur.rowcount or 0)

    def create_session(self, user_id: str) -> dict[str, Any]:
        user = self.get_user(user_id)
        if user is None:
            raise ValueError("User not found")
        if str(user.get("status") or "") != "active":
            raise ValueError("User is not active")
        session_id = str(uuid4())
        session_token = secrets.token_urlsafe(32)
        now = _utcnow()
        expires_at = now + timedelta(seconds=settings.auth_session_max_age_seconds)
        with self._connect() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO auth_sessions (
                      auth_session_id, user_id, tenant_id, session_token_hash,
                      created_at, last_seen_at, expires_at
                    )
                    VALUES (%s, %s, %s, %s, %s, %s, %s)
                    """,
                    (
                        session_id,
                        str(user.get("user_id") or ""),
                        str(user.get("tenant_id") or "shared"),
                        _hash_session_token(session_token),
                        now,
                        now,
                        expires_at,
                    ),
                )
        return {
            "auth_session_id": session_id,
            "auth_session_token": session_token,
            "user": user,
            "authenticated": True,
            "refresh_cookie": True,
        }

    def verify_session(self, session_id: str | None, session_token: str | None) -> dict[str, Any] | None:
        normalized_id = str(session_id or "").strip()
        normalized_token = str(session_token or "").strip()
        if not normalized_id or not normalized_token:
            return None
        now = _utcnow()
        with self._connect() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT
                      s.auth_session_id,
                      s.user_id,
                      s.tenant_id,
                      s.session_token_hash,
                      s.created_at,
                      s.last_seen_at,
                      s.expires_at,
                      s.revoked_at,
                      u.email,
                      u.display_name,
                      u.role,
                      u.status,
                      u.permissions_json
                    FROM auth_sessions s
                    JOIN auth_users u ON u.user_id = s.user_id
                    WHERE s.auth_session_id = %s
                    """,
                    (normalized_id,),
                )
                row = cur.fetchone()
                if row is None or row.get("revoked_at") is not None:
                    return None
                if str(row.get("status") or "") != "active":
                    return None
                expires_at = _coerce_datetime(row.get("expires_at"))
                if expires_at is None or expires_at <= now:
                    return None
                if not hmac.compare_digest(str(row.get("session_token_hash") or ""), _hash_session_token(normalized_token)):
                    return None
                refresh_cookie = (expires_at - now).total_seconds() <= (settings.auth_session_max_age_seconds / 2)
                if refresh_cookie:
                    cur.execute(
                        "UPDATE auth_sessions SET last_seen_at = %s, expires_at = %s WHERE auth_session_id = %s",
                        (now, now + timedelta(seconds=settings.auth_session_max_age_seconds), normalized_id),
                    )
                else:
                    cur.execute(
                        "UPDATE auth_sessions SET last_seen_at = %s WHERE auth_session_id = %s",
                        (now, normalized_id),
                    )
        user = self._row_to_user(row) or {}
        return {
            "authenticated": True,
            "auth_session_id": normalized_id,
            "refresh_cookie": refresh_cookie,
            "user": {
                "user_id": str(row.get("user_id") or ""),
                "email": str(row.get("email") or ""),
                "display_name": str(row.get("display_name") or ""),
                "tenant_id": str(row.get("tenant_id") or "shared"),
                "role": str(row.get("role") or "member"),
                "permissions": list(user.get("permissions") or []),
            },
        }

    def revoke_session(self, session_id: str | None, session_token: str | None = None) -> int:
        normalized_id = str(session_id or "").strip()
        if not normalized_id:
            return 0
        with self._connect() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    "SELECT session_token_hash, revoked_at FROM auth_sessions WHERE auth_session_id = %s",
                    (normalized_id,),
                )
                row = cur.fetchone()
                if row is None or row.get("revoked_at") is not None:
                    return 0
                if session_token is not None:
                    normalized_token = str(session_token or "").strip()
                    if not normalized_token or not hmac.compare_digest(str(row.get("session_token_hash") or ""), _hash_session_token(normalized_token)):
                        return 0
                cur.execute(
                    "UPDATE auth_sessions SET revoked_at = %s WHERE auth_session_id = %s AND revoked_at IS NULL",
                    (_utcnow(), normalized_id),
                )
                return int(cur.rowcount or 0)

    @staticmethod
    def _normalize_import_permissions(raw_permissions: Any, *, role: str) -> list[str]:
        if isinstance(raw_permissions, list):
            return _normalize_permissions([str(item) for item in raw_permissions], role=role)
        if isinstance(raw_permissions, tuple):
            return _normalize_permissions([str(item) for item in raw_permissions], role=role)
        if isinstance(raw_permissions, str):
            try:
                parsed = json.loads(raw_permissions or "[]")
                if isinstance(parsed, list):
                    return _normalize_permissions([str(item) for item in parsed], role=role)
                logger.warning("Unexpected imported auth permissions payload type for role '%s'; falling back to role defaults", role)
            except (TypeError, ValueError, json.JSONDecodeError):
                logger.warning("Failed to decode imported auth permissions for role '%s'; falling back to role defaults", role)
                return _default_permissions_for_role(role)
        elif raw_permissions not in (None, ""):
            logger.warning(
                "Unsupported imported auth permissions payload type '%s' for role '%s'; falling back to role defaults",
                type(raw_permissions).__name__,
                role,
            )
        return _default_permissions_for_role(role)

    def _import_rows(self, user_rows: list[dict[str, Any]], session_rows: list[dict[str, Any]]) -> dict[str, int]:
        with self._connect() as conn:
            with conn.cursor() as cur:
                for row in user_rows:
                    role = _normalize_role(str(row.get("role") or "member"))
                    permissions = self._normalize_import_permissions(row.get("permissions_json"), role=role)
                    cur.execute(
                        """
                        INSERT INTO auth_users (
                          user_id, email, display_name, tenant_id, role, status, permissions_json,
                          password_hash, password_salt, password_iterations,
                          created_at, updated_at, last_login_at
                        )
                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                        ON CONFLICT (user_id) DO UPDATE SET
                          email = EXCLUDED.email,
                          display_name = EXCLUDED.display_name,
                          tenant_id = EXCLUDED.tenant_id,
                          role = EXCLUDED.role,
                          status = EXCLUDED.status,
                          permissions_json = EXCLUDED.permissions_json,
                          password_hash = EXCLUDED.password_hash,
                          password_salt = EXCLUDED.password_salt,
                          password_iterations = EXCLUDED.password_iterations,
                          created_at = EXCLUDED.created_at,
                          updated_at = EXCLUDED.updated_at,
                          last_login_at = EXCLUDED.last_login_at
                        """,
                        (
                            str(row.get("user_id") or ""),
                            _normalize_email(str(row.get("email") or "")),
                            _normalize_display_name(str(row.get("display_name") or "")),
                            _normalize_tenant_id(str(row.get("tenant_id") or "shared")),
                            role,
                            _normalize_status(str(row.get("status") or "active")),
                            Jsonb(permissions),
                            str(row.get("password_hash") or ""),
                            str(row.get("password_salt") or ""),
                            int(row.get("password_iterations") or _PBKDF2_ITERATIONS),
                            _coerce_datetime(row.get("created_at")) or _utcnow(),
                            _coerce_datetime(row.get("updated_at")) or _utcnow(),
                            _coerce_datetime(row.get("last_login_at")),
                        ),
                    )
                for row in session_rows:
                    cur.execute(
                        """
                        INSERT INTO auth_sessions (
                          auth_session_id, user_id, tenant_id, session_token_hash,
                          created_at, last_seen_at, expires_at, revoked_at
                        )
                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                        ON CONFLICT (auth_session_id) DO UPDATE SET
                          user_id = EXCLUDED.user_id,
                          tenant_id = EXCLUDED.tenant_id,
                          session_token_hash = EXCLUDED.session_token_hash,
                          created_at = EXCLUDED.created_at,
                          last_seen_at = EXCLUDED.last_seen_at,
                          expires_at = EXCLUDED.expires_at,
                          revoked_at = EXCLUDED.revoked_at
                        """,
                        (
                            str(row.get("auth_session_id") or ""),
                            str(row.get("user_id") or ""),
                            _normalize_tenant_id(str(row.get("tenant_id") or "shared")),
                            str(row.get("session_token_hash") or ""),
                            _coerce_datetime(row.get("created_at")) or _utcnow(),
                            _coerce_datetime(row.get("last_seen_at")) or _utcnow(),
                            _coerce_datetime(row.get("expires_at")) or _utcnow(),
                            _coerce_datetime(row.get("revoked_at")),
                        ),
                    )
        return {"users": len(user_rows), "sessions": len(session_rows)}

    def export_snapshot(self, *, include_sessions: bool = True) -> dict[str, list[dict[str, Any]]]:
        with self._connect() as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT * FROM auth_users ORDER BY created_at ASC")
                user_rows = [dict(row) for row in cur.fetchall()]
                session_rows: list[dict[str, Any]] = []
                if include_sessions:
                    cur.execute("SELECT * FROM auth_sessions ORDER BY created_at ASC")
                    session_rows = [dict(row) for row in cur.fetchall()]
        return {
            "users": user_rows,
            "sessions": session_rows,
        }

    def import_from_store(self, source_store: "AuthStore", *, include_sessions: bool = True) -> dict[str, int]:
        snapshot = source_store.export_snapshot(include_sessions=include_sessions)
        return self._import_rows(list(snapshot.get("users") or []), list(snapshot.get("sessions") or []))

    def import_from_sqlite(self, sqlite_path: str | Path, *, include_sessions: bool = True) -> dict[str, int]:
        source_path = Path(sqlite_path).resolve()
        if not source_path.exists():
            raise ValueError(f"SQLite source not found: {source_path}")
        source = sqlite3.connect(str(source_path))
        source.row_factory = sqlite3.Row
        try:
            user_rows = source.execute("SELECT * FROM auth_users ORDER BY created_at ASC").fetchall()
            session_rows = source.execute("SELECT * FROM auth_sessions ORDER BY created_at ASC").fetchall() if include_sessions else []
        finally:
            source.close()
        return self._import_rows([dict(row) for row in user_rows], [dict(row) for row in session_rows])
