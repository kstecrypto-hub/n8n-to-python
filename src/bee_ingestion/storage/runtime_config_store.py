"""Runtime-config persistence for agent serving configuration and secrets.

This bounded storage module owns tenant-scoped runtime overrides and encrypted
runtime secrets. It does not own chat transcripts, profiles, documents, or
admin SQL inspection.
"""

from __future__ import annotations

import json
from typing import Any

import psycopg
from psycopg.rows import dict_row


def get_agent_runtime_secret(repo: Any, tenant_id: str = "shared", include_value: bool = False) -> dict | None:
    with psycopg.connect(repo.dsn, row_factory=dict_row) as conn:
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
                payload["api_key_override"] = repo._decrypt_runtime_secret(str(row.get("api_key_override") or ""))
            else:
                payload.pop("api_key_override", None)
            return payload


def get_agent_runtime_config(repo: Any, tenant_id: str = "shared") -> dict | None:
    with psycopg.connect(repo.dsn, row_factory=dict_row) as conn:
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


def save_agent_runtime_config(repo: Any, tenant_id: str, settings_json: dict, updated_by: str = "admin") -> None:
    with psycopg.connect(repo.dsn) as conn:
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
                    json.dumps(repo._sanitize_json_value(settings_json)),
                    repo._sanitize_text(updated_by),
                ),
            )
        conn.commit()


def save_agent_runtime_secret(repo: Any, tenant_id: str, api_key_override: str, updated_by: str = "admin") -> None:
    encrypted_value = repo._encrypt_runtime_secret(api_key_override)
    with psycopg.connect(repo.dsn) as conn:
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
                    repo._sanitize_text(updated_by),
                ),
            )
        conn.commit()


def delete_agent_runtime_config(repo: Any, tenant_id: str = "shared") -> int:
    with psycopg.connect(repo.dsn) as conn:
        with conn.cursor() as cur:
            cur.execute("DELETE FROM agent_runtime_configs WHERE tenant_id = %s", (tenant_id,))
            deleted = cur.rowcount
        conn.commit()
    return deleted


def delete_agent_runtime_secret(repo: Any, tenant_id: str = "shared") -> int:
    with psycopg.connect(repo.dsn) as conn:
        with conn.cursor() as cur:
            cur.execute("DELETE FROM agent_runtime_secrets WHERE tenant_id = %s", (tenant_id,))
            deleted = cur.rowcount
        conn.commit()
    return deleted
