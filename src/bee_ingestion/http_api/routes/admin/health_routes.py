"""HTTP routes for admin health and observability summaries."""

from __future__ import annotations

from datetime import datetime, timezone

from fastapi import APIRouter, Query, Request

from src.bee_ingestion.admin import ingestion_admin_service, metrics_service
from src.bee_ingestion.http_api.dependencies import repository

router = APIRouter()


@router.get("/admin/api/overview")
def admin_overview() -> dict:
    return metrics_service.get_overview(repository=repository)


@router.get("/admin/api/system/processes")
def admin_system_processes(limit: int = Query(default=10, ge=1, le=100)) -> dict:
    return metrics_service.get_system_processes(repository=repository, limit=limit)


@router.get("/admin/api/system/ingest-progress")
def admin_system_ingest_progress() -> dict:
    return ingestion_admin_service.get_ingest_progress(repository=repository)


@router.get("/admin/api/system/routes")
def admin_system_routes(request: Request) -> list[dict]:
    return metrics_service.list_system_routes(request.app.routes)

