"""Agent message persistence."""

from __future__ import annotations

import json
from typing import Any
from uuid import uuid4

import psycopg
from psycopg.rows import dict_row


def create_agent_message(
    repo: Any,
    *,
    session_id: str,
    role: str,
    content: str,
    metadata_json: dict | None = None,
) -> str:
    message_id = str(uuid4())
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
                INSERT INTO agent_messages (message_id, session_id, profile_id, auth_user_id, role, content, metadata_json)
                VALUES (%s, %s, %s, %s, %s, %s, %s)
                """,
                (
                    message_id,
                    session_id,
                    profile_id,
                    repo._sanitize_text(str(auth_user_id or "")) or None,
                    repo._sanitize_text(role),
                    repo._sanitize_text(content),
                    json.dumps(repo._sanitize_json_value(metadata_json or {})),
                ),
            )
        conn.commit()
    return message_id


def save_agent_message(
    repo: Any,
    session_id: str,
    role: str,
    content: str,
    metadata: dict | None = None,
) -> str:
    """Persist one transcript row and bump the parent session timestamp."""
    message_id = create_agent_message(
        repo,
        session_id=session_id,
        role=role,
        content=content,
        metadata_json=metadata,
    )
    with psycopg.connect(repo.dsn) as conn:
        with conn.cursor() as cur:
            cur.execute(
                "UPDATE agent_sessions SET updated_at = now() WHERE session_id = %s",
                (session_id,),
            )
        conn.commit()
    return message_id


def list_agent_messages(
    repo: Any,
    session_id: str,
    limit: int = 20,
    tenant_id: str | None = None,
    auth_user_id: str | None = None,
    profile_id: str | None = None,
) -> list[dict]:
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
