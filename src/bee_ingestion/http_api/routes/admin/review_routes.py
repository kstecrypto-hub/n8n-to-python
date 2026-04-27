"""HTTP routes for admin review and agent oversight."""

from __future__ import annotations

from typing import Literal

from fastapi import APIRouter, Query
from pydantic import BaseModel

from src.bee_ingestion.admin import review_service
from src.bee_ingestion.http_api.dependencies import agent_service, repository, service

router = APIRouter()


class ChunkDecisionRequest(BaseModel):
    action: Literal["accept", "reject"]


class AgentReviewRequest(BaseModel):
    decision: Literal["approved", "rejected"]
    reviewer: str = "admin"
    notes: str | None = None


class AgentReplayRequest(BaseModel):
    reuse_session: bool = True
    top_k: int | None = None
    query_mode: str | None = None


class AdminMemoryClearRequest(BaseModel):
    sections: list[str]


@router.get("/admin/api/activity/stages")
def admin_stage_runs(
    document_id: str | None = None,
    status: str | None = None,
    limit: int = Query(default=100, ge=1, le=250),
    offset: int = Query(default=0, ge=0),
) -> dict:
    return review_service.list_stage_runs(repository=repository, document_id=document_id, status=status, limit=limit, offset=offset)


@router.get("/admin/api/activity/reviews")
def admin_review_runs(
    document_id: str | None = None,
    decision: str | None = None,
    limit: int = Query(default=100, ge=1, le=250),
    offset: int = Query(default=0, ge=0),
) -> dict:
    return review_service.list_review_runs(repository=repository, document_id=document_id, decision=decision, limit=limit, offset=offset)


@router.get("/admin/api/agent/sessions")
def admin_agent_sessions(
    status: str | None = None,
    limit: int = Query(default=100, ge=1, le=250),
    offset: int = Query(default=0, ge=0),
) -> dict:
    return review_service.list_agent_sessions(repository=repository, status=status, limit=limit, offset=offset)


@router.get("/admin/api/agent/sessions/{session_id}")
def admin_agent_session_detail(session_id: str) -> dict:
    return review_service.get_agent_session_detail(repository=repository, session_id=session_id)


@router.post("/admin/api/agent/sessions/{session_id}/memory/clear")
def admin_clear_agent_session_memory(session_id: str, request: AdminMemoryClearRequest) -> dict:
    return review_service.clear_session_memory(repository=repository, session_id=session_id, sections=request.sections)


@router.get("/admin/api/agent/profiles")
def admin_agent_profiles(
    tenant_id: str = "shared",
    status: str | None = None,
    limit: int = Query(default=100, ge=1, le=250),
    offset: int = Query(default=0, ge=0),
) -> dict:
    return review_service.list_agent_profiles(repository=repository, tenant_id=tenant_id, status=status, limit=limit, offset=offset)


@router.get("/admin/api/agent/profiles/{profile_id}")
def admin_agent_profile_detail(profile_id: str) -> dict:
    return review_service.get_agent_profile_detail(repository=repository, profile_id=profile_id)


@router.post("/admin/api/agent/profiles/{profile_id}/memory/clear")
def admin_clear_agent_profile_memory(profile_id: str, request: AdminMemoryClearRequest) -> dict:
    return review_service.clear_profile_memory(repository=repository, profile_id=profile_id, sections=request.sections)


@router.get("/admin/api/agent/runs")
def admin_agent_runs(
    session_id: str | None = None,
    status: str | None = None,
    abstained: bool | None = None,
    review_status: str | None = None,
    limit: int = Query(default=100, ge=1, le=250),
    offset: int = Query(default=0, ge=0),
) -> dict:
    return review_service.list_agent_runs(
        repository=repository,
        session_id=session_id,
        status=status,
        abstained=abstained,
        review_status=review_status,
        limit=limit,
        offset=offset,
    )


@router.get("/admin/api/agent/runs/{query_run_id}")
def admin_agent_run_detail(query_run_id: str) -> dict:
    return review_service.get_agent_run_detail(repository=repository, query_run_id=query_run_id)


@router.get("/admin/api/agent/reviews")
def admin_agent_reviews(
    decision: str | None = None,
    limit: int = Query(default=100, ge=1, le=250),
    offset: int = Query(default=0, ge=0),
) -> dict:
    return review_service.list_agent_reviews(repository=repository, decision=decision, limit=limit, offset=offset)


@router.get("/admin/api/agent/patterns")
def admin_agent_patterns(
    tenant_id: str = "shared",
    search: str | None = None,
    limit: int = Query(default=50, ge=1, le=250),
    offset: int = Query(default=0, ge=0),
) -> dict:
    return review_service.list_agent_patterns(repository=repository, tenant_id=tenant_id, search=search, limit=limit, offset=offset)


@router.post("/admin/api/agent/runs/{query_run_id}/review")
def admin_agent_run_review(query_run_id: str, request: AgentReviewRequest) -> dict:
    return review_service.review_agent_run(
        repository=repository,
        query_run_id=query_run_id,
        decision=request.decision,
        reviewer=request.reviewer,
        notes=request.notes,
    )


@router.post("/admin/api/agent/runs/{query_run_id}/replay")
def admin_agent_run_replay(query_run_id: str, request: AgentReplayRequest) -> dict:
    return review_service.replay_agent_run(
        repository=repository,
        agent_service=agent_service,
        query_run_id=query_run_id,
        reuse_session=request.reuse_session,
        top_k=request.top_k,
        query_mode=request.query_mode,
    )


@router.post("/admin/api/chunks/{chunk_id}/decision")
def admin_chunk_decision(chunk_id: str, request: ChunkDecisionRequest) -> dict:
    try:
        return service.review_chunk_decision(chunk_id, request.action)
    except ValueError as exc:
        from fastapi import HTTPException
        raise HTTPException(status_code=400, detail=str(exc)) from exc

