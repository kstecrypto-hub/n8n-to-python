"""Public workspace routes for places, hives, sensors, and sensor readings.

This module owns the workspace HTTP surface. It does not own auth/session
implementation, repository internals, or global app composition.
"""

from __future__ import annotations

from datetime import datetime
from typing import Any, Callable
from uuid import UUID

from fastapi import APIRouter, HTTPException, Query, Request
from pydantic import BaseModel, Field

from src.bee_ingestion.settings import settings


class UserSensorUpsertRequest(BaseModel):
    external_sensor_id: str
    sensor_name: str
    sensor_type: str = "environment"
    place_id: UUID | None = None
    hive_id: UUID | None = None
    hive_name: str | None = None
    location_label: str | None = None
    status: str = "active"
    metadata_json: dict[str, Any] | None = None


class UserPlaceUpsertRequest(BaseModel):
    external_place_id: str
    place_name: str
    status: str = "active"
    metadata_json: dict[str, Any] | None = None


class UserHiveUpsertRequest(BaseModel):
    external_hive_id: str
    hive_name: str
    place_id: UUID | None = None
    status: str = "active"
    metadata_json: dict[str, Any] | None = None


class SensorReadingItemRequest(BaseModel):
    observed_at: datetime
    metric_name: str
    unit: str | None = None
    numeric_value: float | None = None
    text_value: str | None = None
    quality_score: float | None = None
    metadata_json: dict[str, Any] | None = None


class SensorReadingsIngestRequest(BaseModel):
    readings: list[SensorReadingItemRequest] = Field(min_length=1, max_length=settings.sensor_ingest_max_batch)


def create_workspace_router(
    *,
    repository: Any,
    require_authenticated_sensor_user: Callable[..., dict[str, Any]],
    enforce_rate_limit: Callable[..., None],
) -> APIRouter:
    router = APIRouter()

    def _json_safe(value: Any) -> Any:
        if isinstance(value, dict):
            return {str(key): _json_safe(item) for key, item in value.items()}
        if isinstance(value, list):
            return [_json_safe(item) for item in value]
        if isinstance(value, tuple):
            return [_json_safe(item) for item in value]
        if isinstance(value, set):
            return [_json_safe(item) for item in sorted(value, key=lambda item: str(item))]
        if isinstance(value, UUID):
            return str(value)
        if isinstance(value, datetime):
            return value.isoformat()
        return value

    def _workspace_auth_context(request: Request, *, write: bool) -> tuple[dict[str, Any], str, str]:
        auth_session = require_authenticated_sensor_user(request, write=write)
        user = auth_session.get("user") or {}
        public_tenant = settings.agent_public_tenant_id or "shared"
        user_id = str(user.get("user_id") or "").strip()
        return auth_session, public_tenant, user_id

    def _sensor_read_limit(request: Request, *, user_id: str) -> None:
        enforce_rate_limit(
            request,
            bucket="sensor-read",
            limit=settings.public_agent_rate_limit_max_requests,
            window_seconds=settings.public_agent_rate_limit_window_seconds,
            subject=user_id,
        )

    def _sensor_write_limit(request: Request, *, user_id: str) -> None:
        enforce_rate_limit(
            request,
            bucket="sensor-write",
            limit=max(10, settings.public_agent_rate_limit_max_requests // 2),
            window_seconds=settings.public_agent_rate_limit_window_seconds,
            subject=user_id,
        )

    @router.get("/sensors")
    def list_current_user_sensors(
        request: Request,
        status: str | None = None,
        limit: int = Query(default=100, ge=1, le=500),
        offset: int = Query(default=0, ge=0),
    ) -> dict:
        _, public_tenant, user_id = _workspace_auth_context(request, write=False)
        _sensor_read_limit(request, user_id=user_id)
        try:
            return {
                "items": repository.list_user_sensors(
                    tenant_id=public_tenant,
                    auth_user_id=user_id,
                    status=status,
                    limit=limit,
                    offset=offset,
                ),
                "total": repository.count_user_sensors(
                    tenant_id=public_tenant,
                    auth_user_id=user_id,
                    status=status,
                ),
                "limit": limit,
                "offset": offset,
            }
        except ValueError as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc

    @router.get("/places")
    def list_current_user_places(
        request: Request,
        status: str | None = None,
        limit: int = Query(default=100, ge=1, le=500),
        offset: int = Query(default=0, ge=0),
    ) -> dict:
        _, public_tenant, user_id = _workspace_auth_context(request, write=False)
        _sensor_read_limit(request, user_id=user_id)
        try:
            return {
                "items": repository.list_user_places(
                    tenant_id=public_tenant,
                    auth_user_id=user_id,
                    status=status,
                    limit=limit,
                    offset=offset,
                ),
                "total": repository.count_user_places(
                    tenant_id=public_tenant,
                    auth_user_id=user_id,
                    status=status,
                ),
                "limit": limit,
                "offset": offset,
            }
        except ValueError as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc

    @router.post("/places")
    def upsert_current_user_place(request: Request, payload: UserPlaceUpsertRequest) -> dict:
        _, public_tenant, user_id = _workspace_auth_context(request, write=True)
        _sensor_write_limit(request, user_id=user_id)
        try:
            place = repository.upsert_user_place(
                tenant_id=public_tenant,
                auth_user_id=user_id,
                external_place_id=payload.external_place_id,
                place_name=payload.place_name,
                status=payload.status,
                metadata_json=payload.metadata_json,
            )
        except ValueError as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc
        return {"place": place}

    @router.get("/places/{place_id}")
    def current_user_place_detail(request: Request, place_id: UUID) -> dict:
        _, public_tenant, user_id = _workspace_auth_context(request, write=False)
        place = repository.get_user_place(str(place_id), tenant_id=public_tenant, auth_user_id=user_id)
        if place is None:
            raise HTTPException(status_code=404, detail="Place not found")
        return {"place": place}

    @router.get("/places/{place_id}/hives")
    def list_current_user_hives_for_place(
        request: Request,
        place_id: UUID,
        status: str | None = None,
        limit: int = Query(default=100, ge=1, le=500),
        offset: int = Query(default=0, ge=0),
    ) -> dict:
        _, public_tenant, user_id = _workspace_auth_context(request, write=False)
        if repository.get_user_place(str(place_id), tenant_id=public_tenant, auth_user_id=user_id) is None:
            raise HTTPException(status_code=404, detail="Place not found")
        try:
            return {
                "items": repository.list_user_hives(
                    tenant_id=public_tenant,
                    auth_user_id=user_id,
                    status=status,
                    place_id=str(place_id),
                    limit=limit,
                    offset=offset,
                ),
                "total": repository.count_user_hives(
                    tenant_id=public_tenant,
                    auth_user_id=user_id,
                    status=status,
                    place_id=str(place_id),
                ),
                "limit": limit,
                "offset": offset,
            }
        except ValueError as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc

    @router.get("/hives")
    def list_current_user_hives(
        request: Request,
        status: str | None = None,
        place_id: UUID | None = None,
        limit: int = Query(default=100, ge=1, le=500),
        offset: int = Query(default=0, ge=0),
    ) -> dict:
        _, public_tenant, user_id = _workspace_auth_context(request, write=False)
        _sensor_read_limit(request, user_id=user_id)
        if place_id and repository.get_user_place(str(place_id), tenant_id=public_tenant, auth_user_id=user_id) is None:
            raise HTTPException(status_code=404, detail="Place not found")
        try:
            return {
                "items": repository.list_user_hives(
                    tenant_id=public_tenant,
                    auth_user_id=user_id,
                    status=status,
                    place_id=str(place_id) if place_id else None,
                    limit=limit,
                    offset=offset,
                ),
                "total": repository.count_user_hives(
                    tenant_id=public_tenant,
                    auth_user_id=user_id,
                    status=status,
                    place_id=str(place_id) if place_id else None,
                ),
                "limit": limit,
                "offset": offset,
            }
        except ValueError as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc

    @router.post("/hives")
    def upsert_current_user_hive(request: Request, payload: UserHiveUpsertRequest) -> dict:
        _, public_tenant, user_id = _workspace_auth_context(request, write=True)
        _sensor_write_limit(request, user_id=user_id)
        try:
            hive = repository.upsert_user_hive(
                tenant_id=public_tenant,
                auth_user_id=user_id,
                external_hive_id=payload.external_hive_id,
                hive_name=payload.hive_name,
                place_id=str(payload.place_id) if payload.place_id else None,
                status=payload.status,
                metadata_json=payload.metadata_json,
            )
        except ValueError as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc
        return {"hive": hive}

    @router.get("/hives/{hive_id}")
    def current_user_hive_detail(request: Request, hive_id: UUID) -> dict:
        _, public_tenant, user_id = _workspace_auth_context(request, write=False)
        hive = repository.get_user_hive(str(hive_id), tenant_id=public_tenant, auth_user_id=user_id)
        if hive is None:
            raise HTTPException(status_code=404, detail="Hive not found")
        return {"hive": hive}

    @router.post("/sensors")
    def upsert_current_user_sensor(request: Request, payload: UserSensorUpsertRequest) -> dict:
        _, public_tenant, user_id = _workspace_auth_context(request, write=True)
        _sensor_write_limit(request, user_id=user_id)
        try:
            sensor = repository.upsert_user_sensor(
                tenant_id=public_tenant,
                auth_user_id=user_id,
                external_sensor_id=payload.external_sensor_id,
                sensor_name=payload.sensor_name,
                sensor_type=payload.sensor_type,
                place_id=str(payload.place_id) if payload.place_id else None,
                hive_id=str(payload.hive_id) if payload.hive_id else None,
                hive_name=payload.hive_name,
                location_label=payload.location_label,
                status=payload.status,
                metadata_json=payload.metadata_json,
            )
        except ValueError as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc
        return {"sensor": sensor}

    @router.get("/sensors/context")
    def current_user_sensor_context(
        request: Request,
        question: str = Query(..., min_length=1),
        limit: int = Query(default=8, ge=1, le=50),
        hours: int = Query(default=72, ge=1, le=24 * 30),
        points_per_metric: int = Query(default=6, ge=1, le=24),
    ) -> dict:
        _, public_tenant, user_id = _workspace_auth_context(request, write=False)
        _sensor_read_limit(request, user_id=user_id)
        rows = repository.build_user_sensor_context(
            tenant_id=public_tenant,
            auth_user_id=user_id,
            normalized_query=question,
            max_rows=limit,
            hours=hours,
            points_per_metric=points_per_metric,
        )
        return {"items": _json_safe(rows), "total": len(rows)}

    @router.get("/sensors/{sensor_id}")
    def current_user_sensor_detail(request: Request, sensor_id: UUID) -> dict:
        _, public_tenant, user_id = _workspace_auth_context(request, write=False)
        sensor = repository.get_user_sensor(str(sensor_id), tenant_id=public_tenant, auth_user_id=user_id)
        if sensor is None:
            raise HTTPException(status_code=404, detail="Sensor not found")
        return {"sensor": sensor}

    @router.get("/sensors/{sensor_id}/readings")
    def current_user_sensor_readings(
        request: Request,
        sensor_id: UUID,
        metric_name: str | None = None,
        start_at: datetime | None = None,
        end_at: datetime | None = None,
        limit: int = Query(default=200, ge=1, le=1000),
        offset: int = Query(default=0, ge=0),
    ) -> dict:
        _, public_tenant, user_id = _workspace_auth_context(request, write=False)
        if repository.get_user_sensor(str(sensor_id), tenant_id=public_tenant, auth_user_id=user_id) is None:
            raise HTTPException(status_code=404, detail="Sensor not found")
        readings = repository.list_sensor_readings(
            tenant_id=public_tenant,
            auth_user_id=user_id,
            sensor_id=str(sensor_id),
            metric_name=metric_name,
            start_at=start_at,
            end_at=end_at,
            limit=limit,
            offset=offset,
        )
        return {"items": _json_safe(readings), "limit": limit, "offset": offset}

    @router.post("/sensors/{sensor_id}/readings")
    def ingest_current_user_sensor_readings(
        request: Request,
        sensor_id: UUID,
        payload: SensorReadingsIngestRequest,
    ) -> dict:
        _, public_tenant, user_id = _workspace_auth_context(request, write=True)
        _sensor_write_limit(request, user_id=user_id)
        try:
            inserted = repository.save_sensor_readings(
                sensor_id=str(sensor_id),
                tenant_id=public_tenant,
                auth_user_id=user_id,
                readings=[item.model_dump() for item in payload.readings],
            )
        except ValueError as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc
        return {"items": _json_safe(inserted), "count": len(inserted)}

    return router
