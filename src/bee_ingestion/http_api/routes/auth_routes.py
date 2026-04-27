"""Public auth HTTP surface."""

from __future__ import annotations

from fastapi import APIRouter, HTTPException, Request
from fastapi.encoders import jsonable_encoder
from fastapi.responses import JSONResponse
from pydantic import BaseModel

from src.bee_ingestion.http_api.dependencies import auth_store
from src.bee_ingestion.http_api.request_auth import (
    apply_auth_session_cookies,
    clear_agent_profile_cookies,
    clear_agent_session_cookies,
    clear_auth_session_cookies,
    enforce_rate_limit,
    public_auth_payload,
    resolve_auth_session_credentials,
)
from src.bee_ingestion.settings import settings

router = APIRouter()


class AuthRegisterRequest(BaseModel):
    email: str
    password: str
    display_name: str | None = None


class AuthLoginRequest(BaseModel):
    email: str
    password: str


@router.get("/auth/session")
def auth_current_session(request: Request) -> JSONResponse:
    auth_session_id, auth_session_token = resolve_auth_session_credentials(request)
    auth_session = auth_store.verify_session(auth_session_id, auth_session_token)
    if auth_session is None:
        response = JSONResponse({"authenticated": False, "user": None})
        clear_auth_session_cookies(response)
        return response
    return JSONResponse(jsonable_encoder(public_auth_payload(auth_session)))


@router.post("/auth/register")
def auth_register(payload: AuthRegisterRequest, request: Request) -> JSONResponse:
    enforce_rate_limit(
        request,
        bucket="auth-register",
        limit=settings.auth_login_rate_limit_max_attempts,
        window_seconds=settings.auth_login_rate_limit_window_seconds,
    )
    if not settings.auth_public_registration_enabled:
        raise HTTPException(status_code=403, detail="Self-service registration is disabled")
    public_tenant = settings.agent_public_tenant_id or "shared"
    try:
        user = auth_store.create_user(
            payload.email,
            payload.password,
            display_name=payload.display_name,
            tenant_id=public_tenant,
        )
        result = auth_store.create_session(str(user.get("user_id") or ""))
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    response = JSONResponse(jsonable_encoder(public_auth_payload(result)))
    apply_auth_session_cookies(response, result)
    return response


@router.post("/auth/login")
def auth_login(payload: AuthLoginRequest, request: Request) -> JSONResponse:
    enforce_rate_limit(
        request,
        bucket="auth-login",
        limit=settings.auth_login_rate_limit_max_attempts,
        window_seconds=settings.auth_login_rate_limit_window_seconds,
    )
    try:
        user = auth_store.authenticate_user(payload.email, payload.password)
    except ValueError:
        user = None
    if user is None:
        raise HTTPException(status_code=401, detail="Invalid credentials")
    result = auth_store.create_session(str(user.get("user_id") or ""))
    response = JSONResponse(jsonable_encoder(public_auth_payload(result)))
    apply_auth_session_cookies(response, result)
    return response


@router.post("/auth/logout")
def auth_logout(request: Request) -> JSONResponse:
    auth_session_id, auth_session_token = resolve_auth_session_credentials(request)
    auth_store.revoke_session(auth_session_id, auth_session_token)
    response = JSONResponse({"ok": True, "authenticated": False})
    clear_auth_session_cookies(response)
    clear_agent_session_cookies(response)
    clear_agent_profile_cookies(response)
    return response
