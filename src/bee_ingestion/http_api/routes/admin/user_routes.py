"""HTTP routes for admin user management."""

from __future__ import annotations

from typing import Literal

from fastapi import APIRouter, Query
from pydantic import BaseModel

from src.bee_ingestion.admin import user_admin_service
from src.bee_ingestion.http_api.dependencies import auth_store, repository

router = APIRouter()


class AdminAuthUserCreateRequest(BaseModel):
    email: str
    password: str
    display_name: str | None = None
    tenant_id: str = "shared"
    role: Literal["platform_owner", "tenant_admin", "review_analyst", "member"] = "member"
    status: Literal["active", "disabled"] = "active"
    permissions: list[str] | None = None


class AdminAuthUserUpdateRequest(BaseModel):
    email: str | None = None
    password: str | None = None
    display_name: str | None = None
    tenant_id: str | None = None
    role: Literal["platform_owner", "tenant_admin", "review_analyst", "member"] | None = None
    status: Literal["active", "disabled"] | None = None
    permissions: list[str] | None = None


@router.get("/admin/api/auth/users")
def admin_auth_users(
    search: str | None = None,
    tenant_id: str | None = None,
    role: str | None = None,
    status: str | None = None,
    limit: int = Query(default=100, ge=1, le=250),
    offset: int = Query(default=0, ge=0),
) -> dict:
    return user_admin_service.list_users(
        auth_store=auth_store,
        search=search,
        tenant_id=tenant_id,
        role=role,
        status=status,
        limit=limit,
        offset=offset,
    )


@router.get("/admin/api/auth/users/{user_id}")
def admin_auth_user_detail(user_id: str) -> dict:
    return user_admin_service.get_user_detail(auth_store=auth_store, user_id=user_id)


@router.post("/admin/api/auth/users")
def admin_auth_user_create(request: AdminAuthUserCreateRequest) -> dict:
    return user_admin_service.create_user(
        auth_store=auth_store,
        email=request.email,
        password=request.password,
        display_name=request.display_name,
        tenant_id=request.tenant_id,
        role=request.role,
        status=request.status,
        permissions=request.permissions,
    )


@router.put("/admin/api/auth/users/{user_id}")
def admin_auth_user_update(user_id: str, request: AdminAuthUserUpdateRequest) -> dict:
    return user_admin_service.update_user(
        auth_store=auth_store,
        repository=repository,
        user_id=user_id,
        email=request.email,
        password=request.password,
        display_name=request.display_name,
        tenant_id=request.tenant_id,
        role=request.role,
        status=request.status,
        permissions=request.permissions,
    )


@router.post("/admin/api/auth/users/{user_id}/revoke-sessions")
def admin_auth_user_revoke_sessions(user_id: str) -> dict:
    return user_admin_service.revoke_user_sessions(auth_store=auth_store, user_id=user_id)


@router.delete("/admin/api/auth/users/{user_id}")
def admin_auth_user_delete(user_id: str) -> dict:
    return user_admin_service.delete_user(auth_store=auth_store, repository=repository, user_id=user_id)
