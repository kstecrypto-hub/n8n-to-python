"""Agent session persistence."""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Any
from uuid import uuid4

import psycopg
from psycopg.rows import dict_row

from src.bee_ingestion.settings import settings


def create_agent_session(
    repo: Any,
    *,
    tenant_id: str = "shared",
    title: str | None = None,
    profile_id: str | None = None,
    auth_user_id: str | None = None,
    workspace_kind: str = "general",
) -> str:
    session_id = str(uuid4())
    normalized_auth_user_id = repo._sanitize_text(auth_user_id or "").strip() or None
    normalized_workspace_kind = repo._sanitize_text(workspace_kind or "general").strip().lower() or "general"
    with psycopg.connect(repo.dsn) as conn:
        with conn.cursor() as cur:
            if profile_id:
                cur.execute(
                    "SELECT auth_user_id FROM agent_profiles WHERE profile_id = %s AND tenant_id = %s",
                    (profile_id, tenant_id),
                )
                profile_row = cur.fetchone()
                if profile_row is None:
                    raise ValueError("Profile tenant mismatch")
                profile_auth_user_id = repo._sanitize_text(str(profile_row[0] or "")).strip() or None
                if normalized_auth_user_id and profile_auth_user_id and profile_auth_user_id != normalized_auth_user_id:
                    raise ValueError("Session owner does not match profile owner")
                normalized_auth_user_id = normalized_auth_user_id or profile_auth_user_id
            cur.execute(
                """
                INSERT INTO agent_sessions (session_id, tenant_id, auth_user_id, profile_id, workspace_kind, title, status)
                VALUES (%s, %s, %s, %s, %s, %s, 'active')
                """,
                (session_id, tenant_id, normalized_auth_user_id, profile_id, normalized_workspace_kind, repo._sanitize_text(title or "")),
            )
        conn.commit()
    return session_id


def set_agent_session_token(repo: Any, session_id: str, token: str) -> None:
    with psycopg.connect(repo.dsn) as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                UPDATE agent_sessions
                SET session_token_hash = %s,
                    session_token_issued_at = now(),
                    updated_at = now()
                WHERE session_id = %s
                """,
                (repo._token_hash(token), session_id),
        )
        conn.commit()


def bind_agent_session_auth_user(repo: Any, session_id: str, auth_user_id: str) -> None:
    normalized_auth_user_id = repo._sanitize_text(auth_user_id or "").strip()
    if not normalized_auth_user_id:
        raise ValueError("auth_user_id is required")
    with psycopg.connect(repo.dsn) as conn:
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
    repo: Any,
    session_id: str,
    token: str | None,
    *,
    tenant_id: str | None = None,
    auth_user_id: str | None = None,
) -> bool:
    if not token:
        return False
    normalized_auth_user_id = repo._sanitize_text(auth_user_id or "").strip() or None
    with psycopg.connect(repo.dsn, row_factory=dict_row) as conn:
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
            stored_auth_user_id = repo._sanitize_text(str(row.get("auth_user_id") or "")).strip() or None
            if normalized_auth_user_id and stored_auth_user_id and stored_auth_user_id != normalized_auth_user_id:
                return False
            return stored == repo._token_hash(token)


def claim_agent_session(repo: Any, session_id: str, worker_id: str, lease_seconds: int) -> bool:
    with psycopg.connect(repo.dsn) as conn:
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


def release_agent_session(repo: Any, session_id: str, worker_id: str) -> None:
    with psycopg.connect(repo.dsn) as conn:
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


def attach_agent_profile_to_session(repo: Any, session_id: str, profile_id: str) -> None:
    with psycopg.connect(repo.dsn) as conn:
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


def get_agent_session(repo: Any, session_id: str, tenant_id: str | None = None) -> dict | None:
    with psycopg.connect(repo.dsn, row_factory=dict_row) as conn:
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


def update_agent_session(repo: Any, session_id: str, *, title: str | None = None, status: str | None = None) -> None:
    assignments: list[str] = ["updated_at = now()"]
    params: list[object] = []
    if title is not None:
        assignments.append("title = %s")
        params.append(repo._sanitize_text(title))
    if status is not None:
        assignments.append("status = %s")
        params.append(status)
    params.append(session_id)
    with psycopg.connect(repo.dsn) as conn:
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


def count_agent_sessions(
    repo: Any,
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
    return repo._fetch_scalar(f"SELECT COUNT(*) AS value FROM agent_sessions {where_clause}", tuple(params))


def list_agent_sessions(
    repo: Any,
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
    with psycopg.connect(repo.dsn, row_factory=dict_row) as conn:
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


def update_agent_session_record(repo: Any, session_id: str, patch: dict) -> dict | None:
    assignments, params = repo._build_allowed_patch(
        patch,
        allowed_fields={"title": "title", "status": "status", "profile_id": "profile_id", "auth_user_id": "auth_user_id"},
        text_fields={"title", "status", "profile_id", "auth_user_id"},
    )
    with psycopg.connect(repo.dsn, row_factory=dict_row) as conn:
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


def delete_agent_session(repo: Any, session_id: str) -> int:
    with psycopg.connect(repo.dsn) as conn:
        with conn.cursor() as cur:
            cur.execute("DELETE FROM agent_sessions WHERE session_id = %s", (session_id,))
            deleted = cur.rowcount
        conn.commit()
    return deleted
