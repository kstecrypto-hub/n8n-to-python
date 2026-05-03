"""Typed memory persistence for agent session memory."""

from __future__ import annotations

import json
from typing import Any

import psycopg
from psycopg.rows import dict_row


def get_agent_session_memory(
    repo: Any,
    session_id: str,
    *,
    tenant_id: str | None = None,
    auth_user_id: str | None = None,
    profile_id: str | None = None,
) -> dict | None:
    with psycopg.connect(repo.dsn, row_factory=dict_row) as conn:
        with conn.cursor() as cur:
            clauses = ["m.session_id = %s"]
            params: list[object] = [session_id]
            if tenant_id is not None:
                clauses.append("s.tenant_id = %s")
                params.append(tenant_id)
            if auth_user_id is not None:
                clauses.append("s.auth_user_id = %s")
                params.append(repo._sanitize_text(auth_user_id))
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
    repo: Any,
    session_id: str,
    summary_json: dict[str, Any],
    summary_text: str,
    source_provider: str,
    source_model: str,
    prompt_version: str,
) -> None:
    with psycopg.connect(repo.dsn) as conn:
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
                    repo._sanitize_text(str(auth_user_id or "")) or None,
                    json.dumps(repo._sanitize_json_value(summary_json)),
                    repo._sanitize_text(summary_text),
                    repo._sanitize_text(source_provider),
                    repo._sanitize_text(source_model),
                    repo._sanitize_text(prompt_version),
                ),
            )
        conn.commit()


def update_agent_session_memory_record(repo: Any, session_id: str, patch: dict) -> dict | None:
    assignments, params = repo._build_allowed_patch(
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
    with psycopg.connect(repo.dsn, row_factory=dict_row) as conn:
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
