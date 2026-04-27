"""Public agent/chat HTTP surface."""

from __future__ import annotations

from typing import Any, Literal
from uuid import uuid4

from fastapi import APIRouter, HTTPException, Query, Request
from fastapi.encoders import jsonable_encoder
from fastapi.responses import JSONResponse
from pydantic import BaseModel

from src.bee_ingestion.agent import AgentQueryError
from src.bee_ingestion.http_api.dependencies import agent_service, repository
from src.bee_ingestion.http_api.request_auth import (
    apply_agent_profile_cookies,
    apply_agent_session_cookies,
    clear_agent_profile_cookies,
    clear_agent_session_cookies,
    enforce_rate_limit,
    require_authenticated_public_user,
    require_authenticated_sensor_user,
    resolve_agent_profile_credentials,
    resolve_agent_session_cookies,
    resolve_agent_session_credentials,
    resolve_public_query_mode,
)
from src.bee_ingestion.settings import settings

router = APIRouter()


class AgentQueryRequest(BaseModel):
    question: str
    session_id: str | None = None
    session_token: str | None = None
    tenant_id: str = "shared"
    document_ids: list[str] | None = None
    top_k: int | None = None
    query_mode: Literal["auto", "general", "sensor"] | None = None
    workspace_kind: Literal["general", "hive"] | None = None


class AgentProfileUpdateRequest(BaseModel):
    display_name: str | None = None
    user_background: str | None = None
    beekeeping_context: str | None = None
    experience_level: str | None = None
    answer_preferences: list[str] | None = None
    recurring_topics: list[str] | None = None
    persistent_constraints: list[str] | None = None


class AgentFeedbackRequest(BaseModel):
    feedback: str
    notes: str | None = None


def _public_tenant() -> str:
    return settings.agent_public_tenant_id or "shared"


def _response(payload: dict[str, Any], *, status_code: int = 200) -> JSONResponse:
    return JSONResponse(jsonable_encoder(payload), status_code=status_code)


@router.post("/agent/query")
def agent_query(request: Request, payload: AgentQueryRequest) -> JSONResponse:
    try:
        auth_session = (
            require_authenticated_sensor_user(request, write=False)
            if payload.query_mode == "sensor"
            else require_authenticated_public_user(request)
        )
        user = auth_session.get("user") or {}
        enforce_rate_limit(
            request,
            bucket="public-agent",
            limit=settings.public_agent_rate_limit_max_requests,
            window_seconds=settings.public_agent_rate_limit_window_seconds,
            subject=str(user.get("user_id") or ""),
        )
        public_tenant = _public_tenant()
        if str(payload.tenant_id or public_tenant).strip() != public_tenant:
            raise HTTPException(status_code=400, detail="Public agent tenant_id is fixed")
        session_id, session_token = resolve_agent_session_credentials(request, payload)
        profile_id, profile_token = resolve_agent_profile_credentials(request)
        result = agent_service.query(
            question=payload.question,
            session_id=session_id,
            session_token=session_token,
            profile_id=profile_id,
            profile_token=profile_token,
            auth_user_id=str(user.get("user_id") or "").strip() or None,
            tenant_id=public_tenant,
            document_ids=payload.document_ids,
            top_k=payload.top_k,
            query_mode=resolve_public_query_mode(auth_session, payload.query_mode),
            workspace_kind=payload.workspace_kind,
            trusted_tenant=False,
        )
        response = _response(result)
        apply_agent_session_cookies(response, result)
        apply_agent_profile_cookies(response, result)
        return response
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    except AgentQueryError as exc:
        raise HTTPException(status_code=502, detail=str(exc)) from exc


@router.post("/agent/chat")
def agent_chat(request: Request, payload: AgentQueryRequest) -> JSONResponse:
    try:
        auth_session = (
            require_authenticated_sensor_user(request, write=False)
            if payload.query_mode == "sensor"
            else require_authenticated_public_user(request)
        )
        user = auth_session.get("user") or {}
        enforce_rate_limit(
            request,
            bucket="public-agent",
            limit=settings.public_agent_rate_limit_max_requests,
            window_seconds=settings.public_agent_rate_limit_window_seconds,
            subject=str(user.get("user_id") or ""),
        )
        public_tenant = _public_tenant()
        if str(payload.tenant_id or public_tenant).strip() != public_tenant:
            raise HTTPException(status_code=400, detail="Public agent tenant_id is fixed")
        session_id, session_token = resolve_agent_session_credentials(request, payload)
        profile_id, profile_token = resolve_agent_profile_credentials(request)
        result = agent_service.chat(
            question=payload.question,
            session_id=session_id,
            session_token=session_token,
            profile_id=profile_id,
            profile_token=profile_token,
            auth_user_id=str(user.get("user_id") or "").strip() or None,
            tenant_id=public_tenant,
            document_ids=payload.document_ids,
            top_k=payload.top_k,
            query_mode=resolve_public_query_mode(auth_session, payload.query_mode),
            workspace_kind=payload.workspace_kind,
            trusted_tenant=False,
        )
        response = _response(result)
        apply_agent_session_cookies(response, result)
        apply_agent_profile_cookies(response, result)
        return response
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    except AgentQueryError as exc:
        raise HTTPException(status_code=502, detail=str(exc)) from exc


@router.post("/agent/runs/{query_run_id}/feedback")
def agent_run_feedback(query_run_id: str, request: Request, payload: AgentFeedbackRequest) -> dict:
    auth_session = require_authenticated_sensor_user(request, write=False)
    user = auth_session.get("user") or {}
    public_tenant = _public_tenant()
    session_id, session_token = resolve_agent_session_cookies(request)
    user_id = str(user.get("user_id") or "").strip()
    if not session_id or not repository.verify_agent_session_token(
        session_id,
        session_token,
        tenant_id=public_tenant,
        auth_user_id=user_id,
    ):
        raise HTTPException(status_code=400, detail="Active session is required for feedback")
    detail = repository.get_agent_query_detail(
        query_run_id,
        tenant_id=public_tenant,
        session_id=session_id,
        auth_user_id=user_id,
    )
    if detail is None:
        raise HTTPException(status_code=404, detail="Agent query run not found")
    feedback = (payload.feedback or "").strip().lower()
    if feedback not in {"like", "dislike"}:
        raise HTTPException(status_code=400, detail="Feedback must be like or dislike")
    decision = "approved" if feedback == "like" else "rejected"
    try:
        repository.save_agent_answer_review(
            query_run_id=query_run_id,
            decision=decision,
            reviewer="user-ui",
            notes=payload.notes,
            payload={"manual": True, "source": "user-ui", "feedback": feedback},
            tenant_id=public_tenant,
            session_id=session_id,
        )
    except ValueError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    detail = repository.get_agent_query_detail(
        query_run_id,
        tenant_id=public_tenant,
        session_id=session_id,
        auth_user_id=user_id,
    )
    return {
        "query_run_id": query_run_id,
        "feedback": feedback,
        "decision": decision,
        "pattern": detail.get("pattern") if detail else None,
    }


@router.get("/agent/session")
def agent_current_session(request: Request) -> JSONResponse:
    auth_session = require_authenticated_public_user(request)
    user = auth_session.get("user") or {}
    public_tenant = _public_tenant()
    session_id, session_token = resolve_agent_session_cookies(request)
    profile_id, _ = resolve_agent_profile_credentials(request)
    if not session_id:
        return _response({"session_id": None, "active": False, "profile_id": profile_id})
    session = repository.get_agent_session(session_id, tenant_id=public_tenant)
    if session is None or not repository.verify_agent_session_token(
        session_id,
        session_token,
        tenant_id=public_tenant,
        auth_user_id=str(user.get("user_id") or "").strip(),
    ):
        response = _response({"session_id": None, "active": False, "profile_id": profile_id}, status_code=400)
        clear_agent_session_cookies(response)
        return response
    return _response(
        {
            "session_id": session_id,
            "active": True,
            "title": session.get("title"),
            "status": session.get("status"),
            "workspace_kind": session.get("workspace_kind") or "general",
            "updated_at": session.get("updated_at"),
            "profile_id": session.get("profile_id") or profile_id,
        }
    )


@router.get("/agent/sessions")
def agent_list_sessions(
    request: Request,
    workspace_kind: Literal["general", "hive"] | None = Query(default="general"),
    limit: int = Query(default=24, ge=1, le=100),
    offset: int = Query(default=0, ge=0),
) -> JSONResponse:
    auth_session = require_authenticated_public_user(request)
    public_tenant = _public_tenant()
    auth_user_id = str(((auth_session.get("user") or {}).get("user_id")) or "").strip()
    items = repository.list_agent_sessions(
        tenant_id=public_tenant,
        auth_user_id=auth_user_id,
        workspace_kind=workspace_kind or None,
        limit=limit,
        offset=offset,
    )
    total = repository.count_agent_sessions(
        tenant_id=public_tenant,
        auth_user_id=auth_user_id,
        workspace_kind=workspace_kind or None,
    )
    return _response({"items": items, "total": total})


@router.post("/agent/sessions/{session_id}/activate")
def agent_activate_session(
    request: Request,
    session_id: str,
    limit: int = Query(default=200, ge=1, le=400),
) -> JSONResponse:
    auth_session = require_authenticated_public_user(request)
    public_tenant = _public_tenant()
    auth_user_id = str(((auth_session.get("user") or {}).get("user_id")) or "").strip()
    session = repository.get_agent_session(session_id, tenant_id=public_tenant)
    if session is None or str(session.get("auth_user_id") or "").strip() != auth_user_id:
        raise HTTPException(status_code=404, detail="Session not found")
    session_token = str(uuid4())
    repository.set_agent_session_token(session_id, session_token)
    response = _response(
        {
            "session": session,
            "memory": repository.get_agent_session_memory(session_id, tenant_id=public_tenant, auth_user_id=auth_user_id),
            "messages": repository.list_agent_messages(session_id, limit=limit, tenant_id=public_tenant, auth_user_id=auth_user_id),
        }
    )
    apply_agent_session_cookies(response, {"session_id": session_id, "session_token": session_token})
    return response


@router.post("/agent/session/reset")
def agent_reset_session(request: Request) -> JSONResponse:
    require_authenticated_public_user(request)
    response = _response({"ok": True, "session_id": None})
    clear_agent_session_cookies(response)
    return response


@router.get("/agent/profile")
def agent_current_profile(request: Request) -> JSONResponse:
    auth_session = require_authenticated_public_user(request)
    public_tenant = _public_tenant()
    auth_user_id = str(((auth_session.get("user") or {}).get("user_id")) or "").strip()
    profile_id, profile_token = resolve_agent_profile_credentials(request)
    profile = repository.get_agent_profile(profile_id, tenant_id=public_tenant) if profile_id else None
    if profile is None and auth_user_id:
        profile = repository.get_agent_profile_by_auth_user(auth_user_id, tenant_id=public_tenant)
    if profile is None:
        display_name = str((auth_session.get("user") or {}).get("display_name") or "").strip() or None
        created_profile_id = repository.create_agent_profile(
            tenant_id=public_tenant,
            display_name=display_name,
            auth_user_id=auth_user_id or None,
        )
        created_profile_token = str(uuid4())
        repository.set_agent_profile_token(created_profile_id, created_profile_token)
        profile = repository.get_agent_profile(created_profile_id, tenant_id=public_tenant)
        response = _response(
            {
                "profile_id": created_profile_id,
                "active": profile is not None,
                "profile": profile,
            }
        )
        apply_agent_profile_cookies(
            response,
            {
                "profile_id": created_profile_id,
                "profile_token": created_profile_token,
            },
        )
        return response
    profile_id_value = str(profile.get("profile_id") or "")
    effective_profile_token = profile_token or ""
    if not repository.verify_agent_profile_token(profile_id_value, effective_profile_token, tenant_id=public_tenant):
        if auth_user_id and str(profile.get("auth_user_id") or "").strip() == auth_user_id:
            effective_profile_token = str(uuid4())
            repository.set_agent_profile_token(profile_id_value, effective_profile_token)
        else:
            response = _response({"profile_id": None, "active": False, "profile": None}, status_code=400)
            clear_agent_profile_cookies(response)
            return response
    response = _response({"profile_id": profile_id_value, "active": True, "profile": profile})
    apply_agent_profile_cookies(response, {"profile_id": profile_id_value, "profile_token": effective_profile_token})
    return response


@router.put("/agent/profile")
def agent_update_profile(request: Request, payload: AgentProfileUpdateRequest) -> JSONResponse:
    auth_session = require_authenticated_public_user(request)
    public_tenant = _public_tenant()
    auth_user_id = str(((auth_session.get("user") or {}).get("user_id")) or "").strip()
    profile_id, profile_token = resolve_agent_profile_credentials(request)
    profile = repository.get_agent_profile(profile_id, tenant_id=public_tenant) if profile_id else None
    if profile is None and auth_user_id:
        profile = repository.get_agent_profile_by_auth_user(auth_user_id, tenant_id=public_tenant)
    if profile is None:
        raise HTTPException(status_code=400, detail="Profile token is required")
    profile_id_value = str(profile.get("profile_id") or "")
    effective_profile_token = profile_token or ""
    if not repository.verify_agent_profile_token(profile_id_value, effective_profile_token, tenant_id=public_tenant):
        if auth_user_id and str(profile.get("auth_user_id") or "").strip() == auth_user_id:
            effective_profile_token = str(uuid4())
            repository.set_agent_profile_token(profile_id_value, effective_profile_token)
        else:
            raise HTTPException(status_code=400, detail="Profile token is required")
    summary_json = dict(profile.get("summary_json") or {})
    summary_json.update(
        {
            "user_background": str(payload.user_background or summary_json.get("user_background") or "").strip()[:220],
            "beekeeping_context": str(payload.beekeeping_context or summary_json.get("beekeeping_context") or "").strip()[:220],
            "experience_level": str(payload.experience_level or summary_json.get("experience_level") or "").strip()[:80],
            "answer_preferences": [str(item).strip()[:160] for item in (payload.answer_preferences or summary_json.get("answer_preferences") or []) if str(item).strip()][:8],
            "recurring_topics": [str(item).strip()[:180] for item in (payload.recurring_topics or summary_json.get("recurring_topics") or []) if str(item).strip()][:8],
            "persistent_constraints": [str(item).strip()[:220] for item in (payload.persistent_constraints or summary_json.get("persistent_constraints") or []) if str(item).strip()][:8],
            "last_query": str(summary_json.get("last_query") or "").strip()[:220],
        }
    )
    summary_text = "\n".join(
        part
        for part in [
            f"background: {summary_json['user_background']}".strip() if summary_json.get("user_background") else "",
            f"context: {summary_json['beekeeping_context']}".strip() if summary_json.get("beekeeping_context") else "",
            f"experience: {summary_json['experience_level']}".strip() if summary_json.get("experience_level") else "",
            "preferences: " + "; ".join(summary_json.get("answer_preferences") or []) if summary_json.get("answer_preferences") else "",
            "topics: " + " | ".join(summary_json.get("recurring_topics") or []) if summary_json.get("recurring_topics") else "",
            "constraints: " + "; ".join(summary_json.get("persistent_constraints") or []) if summary_json.get("persistent_constraints") else "",
        ]
        if part
    )
    repository.save_agent_profile(
        profile_id=profile_id_value,
        summary_json=summary_json,
        summary_text=summary_text,
        source_provider=str(profile.get("source_provider") or "user-ui"),
        source_model=str(profile.get("source_model") or "user-ui"),
        prompt_version=str(profile.get("prompt_version") or "manual"),
        display_name=payload.display_name,
    )
    updated = repository.get_agent_profile(profile_id_value, tenant_id=public_tenant)
    response = _response({"profile_id": profile_id_value, "active": updated is not None, "profile": updated})
    apply_agent_profile_cookies(response, {"profile_id": profile_id_value, "profile_token": effective_profile_token})
    return response
