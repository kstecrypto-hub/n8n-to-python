"""Admin review and agent oversight workflows.

This module owns operator-facing review history, agent memory resets, agent run
review/replay, and related oversight reads. It does not own HTTP routes or
prompt construction.
"""

from __future__ import annotations

from typing import Any

from fastapi import HTTPException

from src.bee_ingestion.agent import AgentQueryError
from src.bee_ingestion.repository import ALLOWED_AGENT_REVIEW_DECISIONS
from src.bee_ingestion.settings import settings
from src.bee_ingestion.storage import (
    agent_feedback_store,
    agent_message_store,
    agent_profile_store,
    agent_session_store,
    agent_trace_store,
    memory_store,
)

SESSION_MEMORY_CLEAR_RULES: dict[str, dict[str, Any]] = {
    "facts": {"stable_facts": []},
    "open_threads": {"open_threads": []},
    "resolved_threads": {"resolved_threads": []},
    "preferences": {"user_preferences": []},
    "constraints": {"active_constraints": []},
    "scope": {
        "topic_keywords": [],
        "preferred_document_ids": [],
        "scope_signature": "",
        "last_query": "",
    },
    "goal": {"session_goal": ""},
}
PROFILE_MEMORY_CLEAR_RULES: dict[str, dict[str, Any]] = {
    "background": {"user_background": ""},
    "beekeeping_context": {"beekeeping_context": ""},
    "experience_level": {"experience_level": ""},
    "communication_style": {"communication_style": ""},
    "preferences": {"answer_preferences": []},
    "topics": {"recurring_topics": []},
    "learning_goals": {"learning_goals": []},
    "constraints": {"persistent_constraints": []},
}


def _normalize_memory_clear_sections(requested: list[str] | None, allowed_rules: dict[str, dict[str, Any]]) -> list[str]:
    normalized = [str(item or "").strip().lower() for item in (requested or []) if str(item or "").strip()]
    if not normalized:
        raise HTTPException(status_code=400, detail="At least one memory section is required")
    if "all" in normalized:
        return list(allowed_rules.keys())
    unknown = sorted({item for item in normalized if item not in allowed_rules})
    if unknown:
        raise HTTPException(status_code=400, detail=f"Unsupported memory sections: {', '.join(unknown)}")
    deduped: list[str] = []
    for item in normalized:
        if item not in deduped:
            deduped.append(item)
    return deduped


def _apply_memory_clear_rules(summary_json: dict[str, Any] | None, *, sections: list[str], allowed_rules: dict[str, dict[str, Any]]) -> dict[str, Any]:
    payload = dict(summary_json or {})
    for section in sections:
        payload.update(allowed_rules.get(section, {}))
    return payload


def _coerce_and_refresh_session_memory(summary_json: dict[str, Any]) -> dict[str, Any]:
    from src.bee_ingestion.agent import _coerce_memory_summary, _refresh_memory_summary_text

    return _refresh_memory_summary_text(
        _coerce_memory_summary(
            dict(summary_json or {}),
            max_facts=int(settings.agent_memory_max_facts or 6),
            max_open_threads=int(settings.agent_memory_max_open_threads or 6),
            max_resolved_threads=int(settings.agent_memory_max_resolved_threads or 6),
            max_preferences=int(settings.agent_memory_max_preferences or 6),
            max_topics=int(settings.agent_memory_max_topics or 8),
        )
    )


def _coerce_and_refresh_profile_summary(summary_json: dict[str, Any]) -> dict[str, Any]:
    from src.bee_ingestion.agent import _coerce_profile_summary, _refresh_profile_summary_text

    return _refresh_profile_summary_text(_coerce_profile_summary(dict(summary_json or {})))


def list_stage_runs(*, repository: Any, document_id: str | None, status: str | None, limit: int, offset: int) -> dict:
    return {
        "items": repository.list_stage_runs(document_id=document_id, status=status, limit=limit, offset=offset),
        "total": repository.count_stage_runs(document_id=document_id, status=status),
        "limit": limit,
        "offset": offset,
    }


def list_review_runs(*, repository: Any, document_id: str | None, decision: str | None, limit: int, offset: int) -> dict:
    return {
        "items": repository.list_chunk_review_runs(document_id=document_id, decision=decision, limit=limit, offset=offset),
        "total": repository.count_chunk_review_runs(document_id=document_id, decision=decision),
        "limit": limit,
        "offset": offset,
    }


def list_agent_sessions(*, repository: Any, status: str | None, limit: int, offset: int) -> dict:
    return {
        "items": agent_session_store.list_agent_sessions(repository, status=status, limit=limit, offset=offset),
        "total": agent_session_store.count_agent_sessions(repository, status=status),
        "limit": limit,
        "offset": offset,
    }


def get_agent_session_detail(*, repository: Any, session_id: str) -> dict:
    session = agent_session_store.get_agent_session(repository, session_id)
    if session is None:
        raise HTTPException(status_code=404, detail="Agent session not found")
    return {
        "session": session,
        "profile": agent_profile_store.get_agent_profile(repository, str(session.get("profile_id") or "")) if session.get("profile_id") else None,
        "memory": memory_store.get_agent_session_memory(repository, session_id),
        "messages": agent_message_store.list_agent_messages(repository, session_id, limit=100),
        "query_runs": agent_trace_store.list_agent_query_runs(repository, session_id=session_id, limit=100, offset=0),
    }


def clear_session_memory(*, repository: Any, session_id: str, sections: list[str] | None) -> dict:
    memory_row = memory_store.get_agent_session_memory(repository, session_id)
    if memory_row is None:
        raise HTTPException(status_code=404, detail="Agent session memory not found")
    cleared_sections = _normalize_memory_clear_sections(sections, SESSION_MEMORY_CLEAR_RULES)
    summary_json = _apply_memory_clear_rules(
        dict(memory_row.get("summary_json") or {}),
        sections=cleared_sections,
        allowed_rules=SESSION_MEMORY_CLEAR_RULES,
    )
    refreshed_summary = _coerce_and_refresh_session_memory(summary_json)
    updated = memory_store.update_agent_session_memory_record(
        repository,
        session_id,
        {
            "summary_json": refreshed_summary,
            "summary_text": str(refreshed_summary.get("summary_text") or ""),
        },
    )
    return {
        "session_id": session_id,
        "cleared_sections": cleared_sections,
        "available_sections": list(SESSION_MEMORY_CLEAR_RULES.keys()),
        "memory": updated,
    }


def list_agent_profiles(*, repository: Any, tenant_id: str, status: str | None, limit: int, offset: int) -> dict:
    return {
        "items": agent_profile_store.list_agent_profiles(repository, tenant_id=tenant_id, status=status, limit=limit, offset=offset),
        "total": agent_profile_store.count_agent_profiles(repository, tenant_id=tenant_id, status=status),
        "limit": limit,
        "offset": offset,
    }


def get_agent_profile_detail(*, repository: Any, profile_id: str) -> dict:
    profile = agent_profile_store.get_agent_profile(repository, profile_id)
    if profile is None:
        raise HTTPException(status_code=404, detail="Agent profile not found")
    return profile


def clear_profile_memory(*, repository: Any, profile_id: str, sections: list[str] | None) -> dict:
    profile = agent_profile_store.get_agent_profile(repository, profile_id)
    if profile is None:
        raise HTTPException(status_code=404, detail="Agent profile not found")
    cleared_sections = _normalize_memory_clear_sections(sections, PROFILE_MEMORY_CLEAR_RULES)
    summary_json = _apply_memory_clear_rules(
        dict(profile.get("summary_json") or {}),
        sections=cleared_sections,
        allowed_rules=PROFILE_MEMORY_CLEAR_RULES,
    )
    refreshed_summary = _coerce_and_refresh_profile_summary(summary_json)
    updated = agent_profile_store.update_agent_profile_record(
        repository,
        profile_id,
        {
            "summary_json": refreshed_summary,
            "summary_text": str(refreshed_summary.get("summary_text") or ""),
        },
    )
    return {
        "profile_id": profile_id,
        "cleared_sections": cleared_sections,
        "available_sections": list(PROFILE_MEMORY_CLEAR_RULES.keys()),
        "profile": updated,
    }


def list_agent_runs(*, repository: Any, session_id: str | None, status: str | None, abstained: bool | None, review_status: str | None, limit: int, offset: int) -> dict:
    return {
        "items": agent_trace_store.list_agent_query_runs(
            repository,
            session_id=session_id,
            status=status,
            abstained=abstained,
            review_status=review_status,
            limit=limit,
            offset=offset,
        ),
        "total": agent_trace_store.count_agent_query_runs(
            repository,
            session_id=session_id,
            status=status,
            abstained=abstained,
            review_status=review_status,
        ),
        "limit": limit,
        "offset": offset,
    }


def get_agent_run_detail(*, repository: Any, query_run_id: str) -> dict:
    detail = agent_trace_store.get_agent_query_detail(repository, query_run_id)
    if detail is None:
        raise HTTPException(status_code=404, detail="Agent query run not found")
    return detail


def list_agent_reviews(*, repository: Any, decision: str | None, limit: int, offset: int) -> dict:
    return {
        "items": agent_feedback_store.list_agent_answer_reviews(repository, decision=decision, limit=limit, offset=offset),
        "total": agent_feedback_store.count_agent_answer_reviews(repository, decision=decision),
        "limit": limit,
        "offset": offset,
    }


def list_agent_patterns(*, repository: Any, tenant_id: str, search: str | None, limit: int, offset: int) -> dict:
    return {
        "items": agent_trace_store.list_agent_query_patterns(repository, tenant_id=tenant_id, search=search, limit=limit, offset=offset),
        "total": agent_trace_store.count_agent_query_patterns(repository, tenant_id=tenant_id, search=search),
        "limit": limit,
        "offset": offset,
    }


def review_agent_run(*, repository: Any, query_run_id: str, decision: str, reviewer: str, notes: str | None) -> dict:
    detail = agent_trace_store.get_agent_query_detail(repository, query_run_id)
    if detail is None:
        raise HTTPException(status_code=404, detail="Agent query run not found")
    if decision not in ALLOWED_AGENT_REVIEW_DECISIONS:
        raise HTTPException(status_code=400, detail="Invalid review decision")
    try:
        agent_feedback_store.save_agent_answer_review(
            repository,
            query_run_id=query_run_id,
            decision=decision,
            reviewer=reviewer,
            notes=notes,
            payload={"manual": True, "source": "admin-ui"},
        )
    except ValueError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    detail = agent_trace_store.get_agent_query_detail(repository, query_run_id)
    return {
        "query_run_id": query_run_id,
        "decision": decision,
        "reviewer": reviewer,
        "pattern": detail.get("pattern") if detail else None,
    }


def replay_agent_run(
    *,
    repository: Any,
    agent_service: Any,
    query_run_id: str,
    reuse_session: bool,
    top_k: int | None,
    query_mode: str | None,
) -> dict:
    detail = agent_trace_store.get_agent_query_detail(repository, query_run_id)
    if detail is None:
        raise HTTPException(status_code=404, detail="Agent query run not found")
    run = detail["query_run"]
    tenant_id = str(run.get("tenant_id") or "shared")
    stored_snapshot_id = str(run.get("corpus_snapshot_id") or "").strip()
    current_snapshot_id = ""
    if stored_snapshot_id:
        current_snapshot_id = str(repository.get_latest_corpus_snapshot_id(tenant_id) or "").strip()
    if stored_snapshot_id and current_snapshot_id and stored_snapshot_id != current_snapshot_id:
        raise HTTPException(
            status_code=409,
            detail={
                "message": "Replay rejected because the corpus snapshot has changed since the original run.",
                "stored_corpus_snapshot_id": stored_snapshot_id,
                "current_corpus_snapshot_id": current_snapshot_id,
            },
        )
    prompt_payload = dict(run.get("prompt_payload") or {})
    request_scope = dict(prompt_payload.get("request_scope") or {})
    document_ids = [str(item) for item in (request_scope.get("document_ids") or []) if str(item).strip()]
    if not document_ids:
        document_ids = sorted(
            {
                str(item["document_id"])
                for item in detail["sources"]
                if item.get("source_kind") == "chunk" and item.get("document_id")
            }
        )
    replay_top_k = top_k
    if replay_top_k is None:
        scoped_top_k = request_scope.get("top_k")
        replay_top_k = int(scoped_top_k) if scoped_top_k is not None else None
    replay_query_mode = query_mode
    if replay_query_mode is None:
        scoped_query_mode = str(request_scope.get("query_mode") or "").strip().lower()
        replay_query_mode = scoped_query_mode or None
    try:
        chat_kwargs = {
            "question": str(run.get("question") or ""),
            "session_id": str(run.get("session_id")) if reuse_session and run.get("session_id") else None,
            "auth_user_id": str(request_scope.get("auth_user_id") or "").strip() or None,
            "tenant_id": tenant_id,
            "document_ids": document_ids or None,
            "top_k": replay_top_k,
            "query_mode": replay_query_mode,
            "trusted_tenant": True,
            "trusted_session_reuse": True,
        }
        workspace_kind = str(request_scope.get("workspace_kind") or "").strip().lower()
        if workspace_kind:
            chat_kwargs["workspace_kind"] = workspace_kind
        return agent_service.chat(**chat_kwargs)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    except AgentQueryError as exc:
        raise HTTPException(status_code=502, detail=str(exc)) from exc
