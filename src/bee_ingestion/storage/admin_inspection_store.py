"""Admin inspection persistence.

This module owns read/write persistence for operator-facing relation inspection
and single-statement SQL execution. It does not own HTTP policy or prompt
runtime behavior.
"""

from __future__ import annotations

from typing import Any

import psycopg
from psycopg import sql
from psycopg.rows import dict_row


def list_admin_relations(repo: Any, *, schema_name: str | None = None, search: str | None = None) -> list[dict]:
    clauses = ["n.nspname = ANY(%s)", "c.relkind IN ('r', 'v', 'm')"]
    params: list[object] = [repo._admin_relation_schemas(schema_name)]
    if search:
        clauses.append("c.relname ILIKE %s")
        params.append(f"%{repo._sanitize_text(search.strip())}%")
    where_clause = " AND ".join(clauses)
    with psycopg.connect(repo.dsn, row_factory=dict_row) as conn:
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


def get_admin_relation_schema(repo: Any, relation_name: str, *, schema_name: str = "public") -> dict | None:
    relation_name = repo._sanitize_text(relation_name.strip())
    with psycopg.connect(repo.dsn, row_factory=dict_row) as conn:
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


def count_admin_relation_rows(repo: Any, relation_name: str, *, schema_name: str = "public") -> int:
    schema = get_admin_relation_schema(repo, relation_name, schema_name=schema_name)
    if not schema:
        raise ValueError("Relation not found")
    query = sql.SQL("SELECT COUNT(*) AS value FROM {}.{}").format(
        sql.Identifier(schema_name),
        sql.Identifier(relation_name),
    )
    with psycopg.connect(repo.dsn, row_factory=dict_row) as conn:
        with conn.cursor() as cur:
            cur.execute(query)
            row = cur.fetchone()
            return int(row["value"]) if row else 0


def list_admin_relation_rows(
    repo: Any,
    relation_name: str,
    *,
    schema_name: str = "public",
    limit: int = 50,
    offset: int = 0,
) -> dict:
    schema = repo.get_admin_relation_schema(relation_name, schema_name=schema_name)
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
    with psycopg.connect(repo.dsn, row_factory=dict_row) as conn:
        with conn.cursor() as cur:
            cur.execute(count_query)
            count_row = cur.fetchone()
            total = int(count_row["value"]) if count_row else 0
            cur.execute(select_query, (limit, offset))
            rows = [repo._redact_admin_relation_row(relation_name, dict(row)) for row in cur.fetchall()]
    return {
        "schema_name": schema_name,
        "relation_name": relation_name,
        "relation_type": schema["relation_type"],
        "columns": schema["columns"],
        "primary_key": schema["primary_key"],
        "redacted_columns": sorted(getattr(repo, "_ADMIN_REDACTED_COLUMNS", {}).get(relation_name, set())) if hasattr(repo, "_ADMIN_REDACTED_COLUMNS") else [],
        "rows": rows,
        "total": total,
        "limit": limit,
        "offset": offset,
        "order_by": order_columns,
    }


def insert_admin_relation_row(repo: Any, relation_name: str, values: dict[str, Any], *, schema_name: str = "public") -> dict[str, Any]:
    relation_schema = repo._require_admin_table_schema(relation_name, schema_name=schema_name)
    normalized_items = repo._normalize_admin_relation_values(relation_name, relation_schema, values)
    columns = [name for name, _ in normalized_items]
    params = [value for _, value in normalized_items]
    query = sql.SQL("INSERT INTO {}.{} ({}) VALUES ({}) RETURNING *").format(
        sql.Identifier(schema_name),
        sql.Identifier(relation_name),
        sql.SQL(", ").join(sql.Identifier(name) for name in columns),
        sql.SQL(", ").join(sql.SQL("%s") for _ in columns),
    )
    with psycopg.connect(repo.dsn, row_factory=dict_row) as conn:
        with conn.cursor() as cur:
            cur.execute(query, tuple(params))
            row = cur.fetchone()
            conn.commit()
    if row is None:
        raise ValueError("Row insert failed")
    return repo._redact_admin_relation_row(relation_name, dict(row))


def update_admin_relation_row(
    repo: Any,
    relation_name: str,
    key: dict[str, Any],
    values: dict[str, Any],
    *,
    schema_name: str = "public",
) -> dict[str, Any] | None:
    relation_schema = repo._require_admin_table_schema(relation_name, schema_name=schema_name)
    primary_key = {str(item) for item in relation_schema.get("primary_key") or []}
    normalized_items = repo._normalize_admin_relation_values(relation_name, relation_schema, values)
    if any(column_name in primary_key for column_name, _ in normalized_items):
        raise ValueError("Primary key columns cannot be edited through the table editor")
    set_clauses = [sql.SQL("{} = %s").format(sql.Identifier(column_name)) for column_name, _ in normalized_items]
    set_params = [value for _, value in normalized_items]
    where_clauses, where_params = repo._build_admin_relation_key(relation_name, relation_schema, key)
    query = sql.SQL("UPDATE {}.{} SET {} WHERE {} RETURNING *").format(
        sql.Identifier(schema_name),
        sql.Identifier(relation_name),
        sql.SQL(", ").join(set_clauses),
        sql.SQL(" AND ").join(where_clauses),
    )
    with psycopg.connect(repo.dsn, row_factory=dict_row) as conn:
        with conn.cursor() as cur:
            cur.execute(query, tuple(set_params + where_params))
            row = cur.fetchone()
            conn.commit()
    return repo._redact_admin_relation_row(relation_name, dict(row)) if row else None


def delete_admin_relation_row(repo: Any, relation_name: str, key: dict[str, Any], *, schema_name: str = "public") -> int:
    relation_schema = repo._require_admin_table_schema(relation_name, schema_name=schema_name)
    where_clauses, where_params = repo._build_admin_relation_key(relation_name, relation_schema, key)
    query = sql.SQL("DELETE FROM {}.{} WHERE {}").format(
        sql.Identifier(schema_name),
        sql.Identifier(relation_name),
        sql.SQL(" AND ").join(where_clauses),
    )
    with psycopg.connect(repo.dsn) as conn:
        with conn.cursor() as cur:
            cur.execute(query, tuple(where_params))
            deleted = int(cur.rowcount or 0)
            conn.commit()
    return deleted


def execute_admin_sql(repo: Any, statement: str, *, row_limit: int = 250) -> dict[str, Any]:
    cleaned = repo._sanitize_text(str(statement or "")).strip()
    if not cleaned:
        raise ValueError("SQL statement is required")
    normalized = cleaned.rstrip(";").strip()
    if not normalized:
        raise ValueError("SQL statement is required")
    if ";" in normalized:
        raise ValueError("Only one SQL statement can be executed at a time")
    statement_type = normalized.split(None, 1)[0].lower()
    with psycopg.connect(repo.dsn, row_factory=dict_row) as conn:
        with conn.cursor() as cur:
            try:
                cur.execute(normalized)
            except Exception as exc:
                raise ValueError(str(exc)) from exc
            if cur.description:
                rows = [repo._redact_sensitive_json_value(dict(row)) for row in cur.fetchmany(row_limit + 1)]
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
