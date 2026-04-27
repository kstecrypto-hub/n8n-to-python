"""Admin workflows for identity user management.

This module owns operator-facing user CRUD/session-revocation behavior. It does
not own route declarations or identity-store persistence internals.
"""

from __future__ import annotations

from typing import Any

from fastapi import HTTPException

from src.bee_ingestion.auth_store import (
    ALLOWED_AUTH_PERMISSIONS,
    ALLOWED_AUTH_ROLES,
    ALLOWED_AUTH_STATUSES,
    DEFAULT_ROLE_PERMISSIONS,
)
from src.bee_ingestion.settings import settings


def list_users(
    *,
    auth_store: Any,
    search: str | None,
    tenant_id: str | None,
    role: str | None,
    status: str | None,
    limit: int,
    offset: int,
) -> dict:
    try:
        items = auth_store.list_users(
            search=search,
            tenant_id=tenant_id,
            role=role,
            status=status,
            limit=limit,
            offset=offset,
        )
        total = auth_store.count_users(
            search=search,
            tenant_id=tenant_id,
            role=role,
            status=status,
        )
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    return {
        "items": items,
        "total": total,
        "limit": limit,
        "offset": offset,
        "available_roles": sorted(ALLOWED_AUTH_ROLES),
        "available_statuses": sorted(ALLOWED_AUTH_STATUSES),
        "available_permissions": sorted(ALLOWED_AUTH_PERMISSIONS),
        "role_permission_presets": DEFAULT_ROLE_PERMISSIONS,
    }


def get_user_detail(*, auth_store: Any, user_id: str) -> dict:
    user = auth_store.get_user(user_id)
    if user is None:
        raise HTTPException(status_code=404, detail="Auth user not found")
    return {
        "user": user,
        "sessions": auth_store.list_sessions(user_id=user_id, limit=50, offset=0),
    }


def create_user(
    *,
    auth_store: Any,
    email: str,
    password: str,
    display_name: str | None,
    tenant_id: str,
    role: str,
    status: str,
    permissions: list[str] | None,
) -> dict:
    try:
        user = auth_store.create_user(
            email,
            password,
            display_name=display_name,
            tenant_id=tenant_id,
            role=role,
            status=status,
            permissions=permissions,
        )
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    return {"user": user}


def update_user(
    *,
    auth_store: Any,
    repository: Any,
    user_id: str,
    email: str | None,
    password: str | None,
    display_name: str | None,
    tenant_id: str | None,
    role: str | None,
    status: str | None,
    permissions: list[str] | None,
) -> dict:
    prior_user = auth_store.get_user(user_id)
    try:
        user = auth_store.update_user(
            user_id,
            email=email,
            password=password,
            display_name=display_name,
            tenant_id=tenant_id,
            role=role,
            status=status,
            permissions=permissions,
        )
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    if user is None:
        raise HTTPException(status_code=404, detail="Auth user not found")
    if str(user.get("status") or "") != "active":
        cleanup_tenants: list[str] = []
        for candidate in (
            str((prior_user or {}).get("tenant_id") or "").strip(),
            str(user.get("tenant_id") or "").strip(),
            str(settings.agent_public_tenant_id or "shared").strip(),
        ):
            if candidate and candidate not in cleanup_tenants:
                cleanup_tenants.append(candidate)
        for cleanup_tenant in cleanup_tenants:
            repository.delete_sensor_data_for_auth_user(user_id, tenant_id=cleanup_tenant)
    return {"user": user}


def revoke_user_sessions(*, auth_store: Any, user_id: str) -> dict:
    revoked = auth_store.revoke_user_sessions(user_id)
    return {"user_id": user_id, "revoked_sessions": revoked}


def delete_user(*, auth_store: Any, repository: Any, user_id: str) -> dict:
    existing_user = auth_store.get_user(user_id)
    cleanup_tenant = str((existing_user or {}).get("tenant_id") or settings.agent_public_tenant_id or "shared")
    repository.delete_sensor_data_for_auth_user(user_id, tenant_id=cleanup_tenant)
    deleted = auth_store.delete_user(user_id)
    if not deleted:
        raise HTTPException(status_code=404, detail="Auth user not found")
    return {"user_id": user_id, "deleted": True}
