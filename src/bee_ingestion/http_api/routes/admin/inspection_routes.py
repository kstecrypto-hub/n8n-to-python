"""HTTP routes for admin database inspection."""

from __future__ import annotations

from typing import Any

from fastapi import APIRouter, Query, Request
from pydantic import BaseModel

from src.bee_ingestion.admin import inspection_service
from src.bee_ingestion.http_api.dependencies import identity_repository, repository

router = APIRouter()


class AdminDbRowRequest(BaseModel):
    relation_name: str
    schema_name: str = "public"
    key: dict[str, Any] | None = None
    values: dict[str, Any] | None = None
    database: str = "app"


class AdminSqlRequest(BaseModel):
    sql: str
    database: str = "app"


@router.get("/admin/api/db/relations")
def admin_db_relations(request: Request, search: str | None = None, schema_name: str | None = None, database: str = Query(default="app")) -> dict:
    return inspection_service.list_relations(
        request=request,
        app_repository=repository,
        identity_repository=identity_repository,
        search=search,
        schema_name=schema_name,
        database=database,
    )


@router.get("/admin/api/db/relations/{relation_name}")
def admin_db_relation_detail(
    request: Request,
    relation_name: str,
    database: str = Query(default="app"),
    schema_name: str = Query(default="public"),
    limit: int = Query(default=50, ge=1, le=250),
    offset: int = Query(default=0, ge=0),
) -> dict:
    return inspection_service.relation_detail(
        request=request,
        app_repository=repository,
        identity_repository=identity_repository,
        relation_name=relation_name,
        database=database,
        schema_name=schema_name,
        limit=limit,
        offset=offset,
    )


@router.post("/admin/api/db/rows")
def admin_db_insert_row(http_request: Request, request: AdminDbRowRequest) -> dict:
    return inspection_service.insert_row(
        request=http_request,
        app_repository=repository,
        identity_repository=identity_repository,
        relation_name=request.relation_name,
        schema_name=request.schema_name,
        values=dict(request.values or {}),
        database=request.database,
    )


@router.put("/admin/api/db/rows")
def admin_db_update_row(http_request: Request, request: AdminDbRowRequest) -> dict:
    return inspection_service.update_row(
        request=http_request,
        app_repository=repository,
        identity_repository=identity_repository,
        relation_name=request.relation_name,
        schema_name=request.schema_name,
        key=dict(request.key or {}),
        values=dict(request.values or {}),
        database=request.database,
    )


@router.delete("/admin/api/db/rows")
def admin_db_delete_row(http_request: Request, request: AdminDbRowRequest) -> dict:
    return inspection_service.delete_row(
        request=http_request,
        app_repository=repository,
        identity_repository=identity_repository,
        relation_name=request.relation_name,
        schema_name=request.schema_name,
        key=dict(request.key or {}),
        database=request.database,
    )


@router.post("/admin/api/db/sql")
def admin_db_execute_sql(http_request: Request, request: AdminSqlRequest) -> dict:
    return inspection_service.execute_sql(
        request=http_request,
        app_repository=repository,
        identity_repository=identity_repository,
        statement=request.sql,
        database=request.database,
    )

