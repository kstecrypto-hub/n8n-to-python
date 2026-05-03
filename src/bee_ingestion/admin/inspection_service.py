"""Admin inspection workflows for database inspection and SQL execution.

This module owns admin database access policy and inspection workflows. It does
not own HTTP route declarations, auth session cookies, or direct UI concerns.
"""

from __future__ import annotations

from typing import Any

from fastapi import HTTPException, Request

from src.bee_ingestion.http_api.request_auth import (
    has_valid_control_plane_token,
    resolve_authenticated_session,
    session_has_any_permission,
)
from src.bee_ingestion.storage import admin_inspection_store

ADMIN_DATABASE_KEYS = {"app", "identity"}


def normalize_database_key(value: str | None) -> str:
    normalized = str(value or "app").strip().lower()
    if normalized not in ADMIN_DATABASE_KEYS:
        raise HTTPException(status_code=400, detail=f"Unknown admin database '{normalized}'")
    return normalized


def repository_for_database(*, app_repository: Any, identity_repository: Any, database: str):
    normalized = normalize_database_key(database)
    return identity_repository if normalized == "identity" else app_repository


def enforce_database_scope(request: Request, database: str, *, mode: str) -> str:
    normalized = normalize_database_key(database)
    if has_valid_control_plane_token(request):
        return normalized
    auth_session = resolve_authenticated_session(request)
    if auth_session is None:
        raise HTTPException(status_code=401, detail="Operator login or admin token required")
    if mode == "sql":
        required = ["db.sql.write"]
    elif normalized == "identity" and mode == "read":
        required = ["accounts.read", "accounts.write", "db.sql.write"]
    elif normalized == "identity" and mode == "write":
        required = ["accounts.write", "db.sql.write"]
    elif mode in {"read", "write"}:
        required = ["db.rows.write", "db.sql.write"]
    else:
        raise HTTPException(status_code=400, detail=f"Unsupported admin database access mode '{mode}'")
    if not session_has_any_permission(auth_session, required):
        detail = (
            "Insufficient permissions for the identity database"
            if normalized == "identity"
            else "Insufficient permissions for the application database"
        )
        raise HTTPException(status_code=403, detail=detail)
    return normalized


def list_relations(*, request: Request, app_repository: Any, identity_repository: Any, search: str | None, schema_name: str | None, database: str) -> dict:
    normalized = enforce_database_scope(request, database, mode="read")
    repository = repository_for_database(
        app_repository=app_repository,
        identity_repository=identity_repository,
        database=normalized,
    )
    items = admin_inspection_store.list_admin_relations(repository, search=search, schema_name=schema_name)
    return {
        "items": items,
        "total": len(items),
        "database": normalized,
        "schema_name": schema_name or "all",
    }


def relation_detail(
    *,
    request: Request,
    app_repository: Any,
    identity_repository: Any,
    relation_name: str,
    database: str,
    schema_name: str,
    limit: int,
    offset: int,
) -> dict:
    normalized = enforce_database_scope(request, database, mode="read")
    repository = repository_for_database(
        app_repository=app_repository,
        identity_repository=identity_repository,
        database=normalized,
    )
    try:
        payload = admin_inspection_store.list_admin_relation_rows(
            repository,
            relation_name,
            schema_name=schema_name,
            limit=limit,
            offset=offset,
        )
    except ValueError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    payload["database"] = normalized
    return payload


def insert_row(
    *,
    request: Request,
    app_repository: Any,
    identity_repository: Any,
    relation_name: str,
    schema_name: str,
    values: dict[str, Any],
    database: str,
) -> dict:
    normalized = enforce_database_scope(request, database, mode="write")
    repository = repository_for_database(
        app_repository=app_repository,
        identity_repository=identity_repository,
        database=normalized,
    )
    try:
        row = admin_inspection_store.insert_admin_relation_row(
            repository,
            relation_name,
            dict(values or {}),
            schema_name=schema_name,
        )
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    return {
        "database": normalized,
        "schema_name": schema_name,
        "relation_name": relation_name,
        "row": row,
    }


def update_row(
    *,
    request: Request,
    app_repository: Any,
    identity_repository: Any,
    relation_name: str,
    schema_name: str,
    key: dict[str, Any],
    values: dict[str, Any],
    database: str,
) -> dict:
    normalized = enforce_database_scope(request, database, mode="write")
    repository = repository_for_database(
        app_repository=app_repository,
        identity_repository=identity_repository,
        database=normalized,
    )
    try:
        row = admin_inspection_store.update_admin_relation_row(
            repository,
            relation_name,
            dict(key or {}),
            dict(values or {}),
            schema_name=schema_name,
        )
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    if row is None:
        raise HTTPException(status_code=404, detail="Row not found")
    return {
        "database": normalized,
        "schema_name": schema_name,
        "relation_name": relation_name,
        "row": row,
    }


def delete_row(
    *,
    request: Request,
    app_repository: Any,
    identity_repository: Any,
    relation_name: str,
    schema_name: str,
    key: dict[str, Any],
    database: str,
) -> dict:
    normalized = enforce_database_scope(request, database, mode="write")
    repository = repository_for_database(
        app_repository=app_repository,
        identity_repository=identity_repository,
        database=normalized,
    )
    try:
        deleted = admin_inspection_store.delete_admin_relation_row(
            repository,
            relation_name,
            dict(key or {}),
            schema_name=schema_name,
        )
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    if not deleted:
        raise HTTPException(status_code=404, detail="Row not found")
    return {
        "database": normalized,
        "schema_name": schema_name,
        "relation_name": relation_name,
        "deleted": True,
    }


def execute_sql(
    *,
    request: Request,
    app_repository: Any,
    identity_repository: Any,
    statement: str,
    database: str,
) -> dict:
    normalized = enforce_database_scope(request, database, mode="sql")
    repository = repository_for_database(
        app_repository=app_repository,
        identity_repository=identity_repository,
        database=normalized,
    )
    try:
        payload = admin_inspection_store.execute_admin_sql(repository, statement)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    payload["database"] = normalized
    return payload
