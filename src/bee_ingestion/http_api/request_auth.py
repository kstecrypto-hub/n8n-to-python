"""HTTP auth/session boundary for the serving app.

This module owns browser/session-facing request auth behavior:
- cookie names and cookie application/clearing
- authenticated session resolution
- public chat auth and sensor-mode gating
- browser same-origin checks and request rate limiting

It does not own route handlers, repository business logic, or prompt/runtime logic.
"""

from __future__ import annotations

from typing import Any, Literal
from urllib.parse import urlparse

from fastapi import FastAPI, HTTPException, Request
from fastapi.responses import JSONResponse

from src.bee_ingestion.auth_store import ALLOWED_AUTH_PERMISSIONS
from src.bee_ingestion.http_api.dependencies import auth_store, rate_limiter
from src.bee_ingestion.settings import settings

AGENT_SESSION_ID_COOKIE = "bee_agent_session_id"
AGENT_SESSION_TOKEN_COOKIE = "bee_agent_session_token"
AGENT_PROFILE_ID_COOKIE = "bee_agent_profile_id"
AGENT_PROFILE_TOKEN_COOKIE = "bee_agent_profile_token"
AUTH_SESSION_ID_COOKIE = "bee_auth_session_id"
AUTH_SESSION_TOKEN_COOKIE = "bee_auth_session_token"
CONTROL_PLANE_READ_PERMISSIONS = sorted(
    permission for permission in ALLOWED_AUTH_PERMISSIONS if permission not in {"chat.use", "chat.history.read"}
)


def resolve_agent_session_credentials(request: Request, payload: Any) -> tuple[str | None, str | None]:
    return (
        getattr(payload, "session_id", None) or request.cookies.get(AGENT_SESSION_ID_COOKIE),
        getattr(payload, "session_token", None) or request.cookies.get(AGENT_SESSION_TOKEN_COOKIE),
    )


def resolve_agent_session_cookies(request: Request) -> tuple[str | None, str | None]:
    return (
        request.cookies.get(AGENT_SESSION_ID_COOKIE),
        request.cookies.get(AGENT_SESSION_TOKEN_COOKIE),
    )


def resolve_agent_profile_credentials(request: Request) -> tuple[str | None, str | None]:
    return (
        request.cookies.get(AGENT_PROFILE_ID_COOKIE),
        request.cookies.get(AGENT_PROFILE_TOKEN_COOKIE),
    )


def apply_agent_session_cookies(response: JSONResponse, result: dict[str, Any]) -> None:
    session_id = str(result.get("session_id") or "").strip()
    session_token = str(result.get("session_token") or "").strip()
    if session_id:
        response.set_cookie(
            key=AGENT_SESSION_ID_COOKIE,
            value=session_id,
            httponly=True,
            samesite="lax",
            secure=bool(settings.auth_cookie_secure),
            max_age=settings.agent_session_token_max_age_seconds,
            path="/",
        )
    if session_token:
        response.set_cookie(
            key=AGENT_SESSION_TOKEN_COOKIE,
            value=session_token,
            httponly=True,
            samesite="lax",
            secure=bool(settings.auth_cookie_secure),
            max_age=settings.agent_session_token_max_age_seconds,
            path="/",
        )


def apply_agent_profile_cookies(response: JSONResponse, result: dict[str, Any]) -> None:
    profile_id = str(result.get("profile_id") or "").strip()
    profile_token = str(result.get("profile_token") or "").strip()
    if profile_id:
        response.set_cookie(
            key=AGENT_PROFILE_ID_COOKIE,
            value=profile_id,
            httponly=True,
            samesite="lax",
            secure=bool(settings.auth_cookie_secure),
            max_age=settings.agent_profile_token_max_age_seconds,
            path="/",
        )
    if profile_token:
        response.set_cookie(
            key=AGENT_PROFILE_TOKEN_COOKIE,
            value=profile_token,
            httponly=True,
            samesite="lax",
            secure=bool(settings.auth_cookie_secure),
            max_age=settings.agent_profile_token_max_age_seconds,
            path="/",
        )


def clear_agent_session_cookies(response: JSONResponse) -> None:
    response.delete_cookie(AGENT_SESSION_ID_COOKIE, path="/")
    response.delete_cookie(AGENT_SESSION_TOKEN_COOKIE, path="/")


def clear_agent_profile_cookies(response: JSONResponse) -> None:
    response.delete_cookie(AGENT_PROFILE_ID_COOKIE, path="/")
    response.delete_cookie(AGENT_PROFILE_TOKEN_COOKIE, path="/")


def resolve_auth_session_credentials(request: Request) -> tuple[str | None, str | None]:
    return (
        request.cookies.get(AUTH_SESSION_ID_COOKIE),
        request.cookies.get(AUTH_SESSION_TOKEN_COOKIE),
    )


def apply_auth_session_cookies(response: JSONResponse, result: dict[str, Any]) -> None:
    session_id = str(result.get("auth_session_id") or "").strip()
    session_token = str(result.get("auth_session_token") or "").strip()
    if session_id:
        response.set_cookie(
            key=AUTH_SESSION_ID_COOKIE,
            value=session_id,
            httponly=True,
            samesite="lax",
            secure=bool(settings.auth_cookie_secure),
            max_age=settings.auth_session_max_age_seconds,
            path="/",
        )
    if session_token:
        response.set_cookie(
            key=AUTH_SESSION_TOKEN_COOKIE,
            value=session_token,
            httponly=True,
            samesite="lax",
            secure=bool(settings.auth_cookie_secure),
            max_age=settings.auth_session_max_age_seconds,
            path="/",
        )


def clear_auth_session_cookies(response: JSONResponse) -> None:
    response.delete_cookie(AUTH_SESSION_ID_COOKIE, path="/")
    response.delete_cookie(AUTH_SESSION_TOKEN_COOKIE, path="/")


def normalized_browser_origin(value: str | None) -> str | None:
    raw_value = str(value or "").strip()
    if not raw_value:
        return None
    parsed = urlparse(raw_value)
    scheme = parsed.scheme.strip().lower()
    hostname = (parsed.hostname or "").strip().lower()
    if scheme not in {"http", "https"} or not hostname:
        return None
    default_port = 443 if scheme == "https" else 80
    port = parsed.port
    port_part = f":{port}" if port not in {None, default_port} else ""
    return f"{scheme}://{hostname}{port_part}"


def allowed_browser_origins(request: Request) -> set[str]:
    allowed: set[str] = set()
    current_origin = normalized_browser_origin(str(request.base_url))
    if current_origin:
        allowed.add(current_origin)
    for raw_value in str(settings.browser_origin_allowlist or "").split(","):
        normalized = normalized_browser_origin(raw_value)
        if normalized:
            allowed.add(normalized)
    return allowed


def request_origin(request: Request) -> str | None:
    return normalized_browser_origin(request.headers.get("Origin")) or normalized_browser_origin(request.headers.get("Referer"))


def requires_same_origin(request: Request) -> bool:
    if request.method.upper() in {"GET", "HEAD", "OPTIONS"}:
        return False
    path = request.url.path
    return (
        path.startswith("/auth/")
        or path.startswith("/agent/")
        or path.startswith("/places")
        or path.startswith("/hives")
        or path.startswith("/sensors")
    )


def enforce_same_origin(request: Request) -> None:
    if not requires_same_origin(request):
        return
    current_request_origin = request_origin(request)
    if current_request_origin is None:
        return
    if current_request_origin not in allowed_browser_origins(request):
        raise HTTPException(status_code=403, detail="Cross-site browser request blocked")


def resolve_authenticated_session(request: Request) -> dict[str, Any] | None:
    cached = getattr(request.state, "auth_session", None)
    if cached is not None:
        return cached
    auth_session_id, auth_session_token = resolve_auth_session_credentials(request)
    auth_session = auth_store.verify_session(auth_session_id, auth_session_token)
    request.state.auth_session = auth_session
    if (
        auth_session is not None
        and bool(auth_session.get("refresh_cookie"))
        and auth_session_id
        and auth_session_token
    ):
        request.state.auth_session_cookie_refresh = {
            "auth_session_id": auth_session_id,
            "auth_session_token": auth_session_token,
        }
    return auth_session


def session_permissions(auth_session: dict[str, Any] | None) -> set[str]:
    user = (auth_session or {}).get("user") or {}
    return {
        str(item or "").strip().lower()
        for item in list(user.get("permissions") or [])
        if str(item or "").strip()
    }


def session_has_any_permission(
    auth_session: dict[str, Any] | None,
    permissions: list[str] | tuple[str, ...] | set[str],
) -> bool:
    allowed = session_permissions(auth_session)
    return any(str(permission or "").strip().lower() in allowed for permission in permissions)


def has_valid_control_plane_token(request: Request) -> bool:
    expected = (settings.admin_api_token or "").strip()
    provided = (request.headers.get("X-Admin-Token") or "").strip()
    return bool(expected and provided == expected)


def control_plane_permissions_for_request(request: Request) -> list[str]:
    path = request.url.path
    method = request.method.upper()
    if path.startswith("/ingest/") or path.startswith("/admin/api/uploads/ingest"):
        return ["documents.write"]
    if path == "/admin/api/reset":
        return ["db.sql.write"]
    if path.startswith("/admin/api/db/sql"):
        return ["db.sql.write"]
    if path.startswith("/admin/api/db/"):
        return ["db.rows.write", "accounts.read", "accounts.write", "db.sql.write"] if method == "GET" else ["db.rows.write", "accounts.write", "db.sql.write"]
    if path.startswith("/admin/api/auth/users"):
        return ["accounts.read", "accounts.write"] if method == "GET" else ["accounts.write"]
    if path.startswith("/admin/api/agent/config") or path.startswith("/admin/api/system/config"):
        return ["runtime.read", "runtime.write", "rate_limits.write"] if method == "GET" else ["runtime.write", "rate_limits.write"]
    if path == "/admin/api/overview" or path.startswith("/admin/api/system/"):
        return CONTROL_PLANE_READ_PERMISSIONS
    if path.startswith("/admin/api/ontology"):
        return ["kg.read", "kg.write"] if method == "GET" else ["kg.write"]
    if path.startswith("/admin/api/kg/"):
        return ["kg.read", "kg.write"] if method == "GET" else ["kg.write"]
    if path.startswith("/admin/api/agent/") or path.startswith("/admin/api/retrieval/"):
        return ["agent.review"]
    if (
        path.startswith("/admin/api/documents")
        or path.startswith("/admin/api/chunks")
        or path.startswith("/admin/api/chroma")
        or path.startswith("/admin/api/metadata")
    ):
        return ["documents.read", "documents.write"] if method == "GET" else ["documents.write"]
    return CONTROL_PLANE_READ_PERMISSIONS


def require_authenticated_public_user(request: Request) -> dict[str, Any]:
    auth_session = resolve_authenticated_session(request)
    public_tenant = settings.agent_public_tenant_id or "shared"
    if auth_session is None:
        raise HTTPException(status_code=401, detail="Login required")
    user = auth_session.get("user") or {}
    permissions = {
        str(item or "").strip().lower()
        for item in list(user.get("permissions") or [])
        if str(item or "").strip()
    }
    if "chat.use" not in permissions:
        raise HTTPException(status_code=403, detail="Account is not allowed to use chat")
    if str(user.get("tenant_id") or public_tenant) != public_tenant:
        raise HTTPException(status_code=403, detail="Account is not allowed in the public tenant")
    return auth_session


def require_authenticated_sensor_user(request: Request, *, write: bool) -> dict[str, Any]:
    auth_session = require_authenticated_public_user(request)
    required_permission = "sensor.write" if write else "sensor.read"
    if required_permission not in session_permissions(auth_session):
        raise HTTPException(status_code=403, detail=f"Account is not allowed to {required_permission}")
    return auth_session


def resolve_public_query_mode(
    auth_session: dict[str, Any],
    requested_query_mode: Literal["auto", "general", "sensor"] | None,
) -> Literal["auto", "general", "sensor"] | None:
    if "sensor.read" in session_permissions(auth_session):
        return requested_query_mode
    if requested_query_mode == "sensor":
        raise HTTPException(status_code=403, detail="Account is not allowed to use sensor mode")
    return "general"


def public_auth_payload(result: dict[str, Any] | None) -> dict[str, Any]:
    if not result:
        return {"authenticated": False, "user": None}
    return {
        "authenticated": bool(result.get("authenticated")),
        "user": result.get("user"),
    }


def request_client_identity(request: Request) -> str:
    forwarded = str(request.headers.get("x-forwarded-for") or "").strip()
    if forwarded:
        return forwarded.split(",")[0].strip()
    client = getattr(request, "client", None)
    host = getattr(client, "host", None)
    return str(host or "unknown")


def enforce_rate_limit(
    request: Request,
    *,
    bucket: str,
    limit: int,
    window_seconds: int,
    subject: str | None = None,
) -> None:
    identity_parts = [bucket, request_client_identity(request)]
    if subject:
        identity_parts.append(str(subject).strip())
    retry_after = rate_limiter.check("|".join(identity_parts), limit=limit, window_seconds=window_seconds)
    if retry_after is not None:
        raise HTTPException(status_code=429, detail=f"Rate limit exceeded. Retry in {retry_after} seconds.")


def register_control_plane_middleware(app: FastAPI) -> None:
    @app.middleware("http")
    async def protect_control_plane(request: Request, call_next):
        try:
            enforce_same_origin(request)
            if request.url.path.startswith("/admin/api/") or request.url.path.startswith("/ingest/"):
                if has_valid_control_plane_token(request):
                    request.state.control_plane_token_authenticated = True
                    response = await call_next(request)
                else:
                    auth_session = resolve_authenticated_session(request)
                    if auth_session is None:
                        return JSONResponse(status_code=401, content={"detail": "Operator login or admin token required"})
                    required_permissions = control_plane_permissions_for_request(request)
                    if not session_has_any_permission(auth_session, required_permissions):
                        return JSONResponse(status_code=403, content={"detail": "Insufficient permissions for this operation"})
                    response = await call_next(request)
            else:
                response = await call_next(request)
        except HTTPException as exc:
            return JSONResponse(status_code=exc.status_code, content={"detail": exc.detail})
        refresh_payload = getattr(request.state, "auth_session_cookie_refresh", None)
        if refresh_payload and not bool(getattr(request.state, "suppress_auth_cookie_refresh", False)):
            apply_auth_session_cookies(response, refresh_payload)
        return response
