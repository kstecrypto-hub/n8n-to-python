"""Agent profile persistence."""

from __future__ import annotations

from datetime import datetime, timezone
import json
from typing import Any
from uuid import uuid4

import psycopg
from psycopg.rows import dict_row

from src.bee_ingestion.settings import settings


def create_agent_profile(
    repo: Any,
    *,
    tenant_id: str = "shared",
    display_name: str | None = None,
    auth_user_id: str | None = None,
) -> str:
    profile_id = str(uuid4())
    with psycopg.connect(repo.dsn) as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO agent_profiles (profile_id, tenant_id, auth_user_id, display_name, status)
                VALUES (%s, %s, %s, %s, 'active')
                """,
                (
                    profile_id,
                    tenant_id,
                    repo._sanitize_text(auth_user_id or "") or None,
                    repo._sanitize_text(display_name or ""),
                ),
            )
        conn.commit()
    return profile_id


def set_agent_profile_token(repo: Any, profile_id: str, token: str) -> None:
    with psycopg.connect(repo.dsn) as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                UPDATE agent_profiles
                SET profile_token_hash = %s,
                    profile_token_issued_at = now(),
                    updated_at = now()
                WHERE profile_id = %s
                """,
                (repo._token_hash(token), profile_id),
            )
        conn.commit()


def verify_agent_profile_token(repo: Any, profile_id: str, token: str | None, tenant_id: str | None = None) -> bool:
    if not token:
        return False
    with psycopg.connect(repo.dsn, row_factory=dict_row) as conn:
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
            return stored == repo._token_hash(token)


def get_agent_profile(repo: Any, profile_id: str, tenant_id: str | None = None) -> dict | None:
    with psycopg.connect(repo.dsn, row_factory=dict_row) as conn:
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


def get_agent_profile_by_auth_user(repo: Any, auth_user_id: str, tenant_id: str = "shared") -> dict | None:
    normalized_auth_user_id = repo._sanitize_text(auth_user_id or "").strip()
    if not normalized_auth_user_id:
        return None
    with psycopg.connect(repo.dsn, row_factory=dict_row) as conn:
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
    repo: Any,
    profile_id: str,
    summary_json: dict[str, Any],
    summary_text: str,
    source_provider: str,
    source_model: str,
    prompt_version: str,
    *,
    display_name: str | None = None,
) -> None:
    with psycopg.connect(repo.dsn) as conn:
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
                    json.dumps(repo._sanitize_json_value(summary_json)),
                    repo._sanitize_text(summary_text),
                    repo._sanitize_text(source_provider),
                    repo._sanitize_text(source_model),
                    repo._sanitize_text(prompt_version),
                    repo._sanitize_text(display_name or ""),
                    profile_id,
                ),
            )
        conn.commit()


def update_agent_profile_record(repo: Any, profile_id: str, patch: dict) -> dict | None:
    assignments, params = repo._build_allowed_patch(
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
    with psycopg.connect(repo.dsn, row_factory=dict_row) as conn:
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


def list_agent_profiles(repo: Any, tenant_id: str = "shared", status: str | None = None, limit: int = 100, offset: int = 0) -> list[dict]:
    clauses: list[str] = ["p.tenant_id = %s"]
    params: list[object] = [tenant_id]
    if status:
        clauses.append("p.status = %s")
        params.append(status)
    where_clause = "WHERE " + " AND ".join(clauses)
    with psycopg.connect(repo.dsn, row_factory=dict_row) as conn:
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


def count_agent_profiles(repo: Any, tenant_id: str = "shared", status: str | None = None) -> int:
    clauses: list[str] = ["tenant_id = %s"]
    params: list[object] = [tenant_id]
    if status:
        clauses.append("status = %s")
        params.append(status)
    where_clause = "WHERE " + " AND ".join(clauses)
    return repo._fetch_scalar(f"SELECT COUNT(*) AS value FROM agent_profiles {where_clause}", tuple(params))


def delete_agent_profile(repo: Any, profile_id: str) -> int:
    with psycopg.connect(repo.dsn) as conn:
        with conn.cursor() as cur:
            cur.execute("DELETE FROM agent_profiles WHERE profile_id = %s", (profile_id,))
            deleted = cur.rowcount
        conn.commit()
    return deleted
