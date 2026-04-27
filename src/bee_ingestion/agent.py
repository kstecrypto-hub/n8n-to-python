"""Read-only retrieval agent built on top of the ingestion pipeline.

The serving path here is intentionally narrow:
- normalize and classify the question
- retrieve chunks plus supporting KG records
- build a bounded prompt
- synthesize a grounded answer or abstain
- persist a full trace for audit and replay
"""

from __future__ import annotations

import json
import math
import re
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any
from uuid import uuid4

import httpx

from src.bee_ingestion.agent_runtime import (
    coerce_agent_runtime_config,
    default_agent_runtime_config,
    merged_agent_runtime_config,
)
from src.bee_ingestion.chroma_store import ChromaStore
from src.bee_ingestion.chunking import sanitize_text
from src.bee_ingestion.embedding import Embedder
from src.bee_ingestion.query_router import classify_question_fallback, route_question_cached
from src.bee_ingestion.repository import Repository
from src.bee_ingestion.settings import settings


class AgentQueryError(RuntimeError):
    pass


_SAFE_ABSTAIN_REASONS = {
    "no_retrieval_results",
    "no_valid_citations",
    "low_confidence",
    "model_abstained",
    "agent_provider_disabled",
    "agent_generation_error",
    "empty_answer",
    "weak_grounding",
}

_MEMORY_KEYWORD_STOPWORDS = {
    "about", "after", "again", "also", "answer", "beekeeping", "being", "books", "chunk", "chunks",
    "could", "does", "dont", "each", "evidence", "from", "have", "into", "just", "like", "main",
    "need", "onto", "only", "over", "page", "pages", "part", "really", "said", "should", "some",
    "than", "that", "them", "then", "there", "these", "they", "this", "those", "through", "using",
    "very", "what", "when", "where", "which", "while", "with", "would", "your",
}


@dataclass(slots=True)
class AgentContextBundle:
    """Normalized evidence bundle assembled from chunks and KG rows."""
    chunks: list[dict[str, Any]]
    assets: list[dict[str, Any]]
    sensor_rows: list[dict[str, Any]]
    assertions: list[dict[str, Any]]
    evidence: list[dict[str, Any]]
    entities: list[dict[str, Any]]
    graph_chains: list[dict[str, Any]]
    sources: list[dict[str, Any]]


@dataclass(slots=True)
class AgentPromptBundle:
    """Prompt-ready slice of the context bundle after budgeting/trimming."""
    prior_context: list[dict[str, Any]]
    profile_summary: dict[str, Any] | None
    session_summary: dict[str, Any] | None
    chunk_payload: list[dict[str, Any]]
    asset_payload: list[dict[str, Any]]
    sensor_payload: list[dict[str, Any]]
    assertion_payload: list[dict[str, Any]]
    entity_payload: list[dict[str, Any]]
    graph_chain_payload: list[dict[str, Any]]
    evidence_payload: list[dict[str, Any]]
    stats: dict[str, Any]


class AgentService:
    def __init__(
        self,
        repository: Repository | None = None,
        store: ChromaStore | None = None,
        embedder: Embedder | None = None,
    ) -> None:
        self.repository = repository or Repository()
        self.store = store or ChromaStore()
        self.embedder = embedder or Embedder()

    def _load_runtime_config(self, tenant_id: str) -> dict[str, Any]:
        persisted = self.repository.get_agent_runtime_config(tenant_id)
        persisted_settings = dict((persisted or {}).get("settings_json") or {})
        legacy_api_key_override = str(persisted_settings.pop("api_key_override", "") or "").strip()
        secret_row = self.repository.get_agent_runtime_secret(tenant_id, include_value=True) or {}
        effective = merged_agent_runtime_config(persisted_settings)
        effective = coerce_agent_runtime_config(effective)
        effective["api_key_override"] = str(secret_row.get("api_key_override") or legacy_api_key_override or "").strip()
        effective["tenant_id"] = tenant_id
        return effective

    def _embed_query_or_raise(self, normalized_query: str) -> list[float]:
        try:
            return self.embedder.embed([normalized_query])[0]
        except httpx.HTTPStatusError as exc:
            error_text = exc.response.text.strip()
            detail = f"Embedding request failed: {exc}"
            if error_text:
                detail = f"{detail} | body={error_text}"
            raise AgentQueryError(detail) from exc
        except httpx.HTTPError as exc:
            raise AgentQueryError(f"Embedding request failed: {exc}") from exc
        except ValueError as exc:
            raise AgentQueryError(str(exc)) from exc

    def _embed_query_cached_or_raise(
        self,
        *,
        tenant_id: str,
        normalized_query: str,
        runtime_config: dict[str, Any],
    ) -> list[float]:
        cache_enabled = bool(runtime_config.get("embedding_cache_enabled", True))
        cache_identity = _embedding_cache_identity()
        max_age_seconds = max(0, int(runtime_config.get("embedding_cache_max_age_seconds") or 0))
        if cache_enabled and max_age_seconds > 0:
            cached = self.repository.get_cached_query_embedding(
                tenant_id=tenant_id,
                normalized_query=normalized_query,
                cache_identity=cache_identity,
            )
            cached_at = cached.get("cached_at") if cached else None
            if cached and isinstance(cached_at, datetime):
                age_seconds = (datetime.now(timezone.utc) - cached_at.astimezone(timezone.utc)).total_seconds()
                vector = cached.get("embedding_json")
                if age_seconds <= max_age_seconds and isinstance(vector, list) and vector:
                    self.repository.touch_cached_query_embedding_hit(
                        tenant_id=tenant_id,
                        normalized_query=normalized_query,
                        cache_identity=cache_identity,
                    )
                    return [float(value) for value in vector]
        embedding = self._embed_query_or_raise(normalized_query)
        if cache_enabled:
            self.repository.save_cached_query_embedding(
                tenant_id=tenant_id,
                normalized_query=normalized_query,
                cache_identity=cache_identity,
                embedding=embedding,
            )
        return embedding

    def _retrieve_chunk_candidates(
        self,
        *,
        normalized_query: str,
        retrieval_plan: dict[str, Any],
        tenant_id: str,
        document_ids: list[str] | None,
        runtime_config: dict[str, Any],
    ) -> tuple[list[float] | None, list[dict[str, Any]], str | None]:
        query_embedding: list[float] | None = None
        dense_matches: list[dict[str, Any]] = []
        embedding_error: str | None = None
        lexical_limit = max(4, min(int(retrieval_plan.get("search_k") or 0) or 8, int(runtime_config.get("max_search_k") or 24)))
        lexical_matches = self.repository.search_chunk_records_lexical(
            normalized_query,
            tenant_id=tenant_id,
            document_ids=document_ids,
            limit=lexical_limit,
        )
        search_k = int(retrieval_plan.get("search_k") or 0)
        if search_k > 0:
            try:
                query_embedding = self._embed_query_cached_or_raise(
                    tenant_id=tenant_id,
                    normalized_query=normalized_query,
                    runtime_config=runtime_config,
                )
                dense_matches = self.store.search(
                    query_embedding=query_embedding,
                    top_k=search_k,
                    tenant_id=tenant_id,
                    document_ids=document_ids,
                )
            except AgentQueryError as exc:
                embedding_error = str(exc)
        if embedding_error and not lexical_matches:
            raise AgentQueryError(embedding_error)
        raw_matches = _merge_hybrid_matches(dense_matches, lexical_matches, id_key="chunk_id")
        raw_matches = _rerank_matches(normalized_query, raw_matches, runtime_config)
        return query_embedding, raw_matches, embedding_error

    def query(
        self,
        question: str,
        session_id: str | None = None,
        session_token: str | None = None,
        profile_id: str | None = None,
        profile_token: str | None = None,
        auth_user_id: str | None = None,
        tenant_id: str = "shared",
        document_ids: list[str] | None = None,
        top_k: int | None = None,
        query_mode: str | None = None,
        workspace_kind: str | None = None,
        trusted_tenant: bool = False,
        trusted_session_reuse: bool = False,
    ) -> dict[str, Any]:
        """Run one read-only QA turn and persist a replayable trace."""
        started_at = time.perf_counter()
        normalized_query = _normalize_query(question)
        if not normalized_query:
            raise ValueError("Question cannot be empty")
        if not trusted_tenant:
            tenant_id = settings.agent_public_tenant_id or "shared"
        resolved_workspace_kind = str(workspace_kind or "").strip().lower() or None
        if resolved_workspace_kind not in {None, "general", "hive"}:
            resolved_workspace_kind = None

        # Sessions carry short-term conversation state and are leased so concurrent
        # turns cannot interleave writes into the same transcript.
        session_row = self.repository.get_agent_session(session_id, tenant_id=tenant_id) if session_id else None
        if session_id and session_row is None:
            raise ValueError("Session not found")
        session_token_value = session_token or ""
        profile_token_value = profile_token or ""
        profile_row: dict[str, Any] | None = None
        if session_row is None:
            profile_row, profile_token_value = self._resolve_or_create_profile(
                tenant_id=tenant_id,
                profile_id=profile_id,
                profile_token=profile_token,
                auth_user_id=auth_user_id,
            )
            session_id = self.repository.create_agent_session(
                tenant_id=tenant_id,
                title=_build_session_title(normalized_query),
                profile_id=str(profile_row.get("profile_id") or ""),
                auth_user_id=auth_user_id,
                workspace_kind=resolved_workspace_kind or "general",
            )
            session_token_value = str(uuid4())
            self.repository.set_agent_session_token(session_id, session_token_value)
        else:
            session_auth_user_id = str(session_row.get("auth_user_id") or "").strip()
            session_workspace_kind = str(session_row.get("workspace_kind") or "general").strip().lower() or "general"
            if auth_user_id and session_auth_user_id and session_auth_user_id != auth_user_id:
                raise ValueError("Session does not belong to the authenticated user")
            if resolved_workspace_kind and session_workspace_kind != resolved_workspace_kind:
                raise ValueError("Session kind mismatch")
            if auth_user_id and not session_auth_user_id and not trusted_session_reuse:
                raise ValueError("Session must be reset before authenticated reuse")
            if not trusted_session_reuse and not self.repository.verify_agent_session_token(
                session_id,
                session_token,
                tenant_id=tenant_id,
                auth_user_id=auth_user_id,
            ):
                raise ValueError("Session token is required for session reuse")
            tenant_id = str(session_row["tenant_id"])
            session_profile_id = str(session_row.get("profile_id") or "")
            if session_profile_id:
                profile_row = self.repository.get_agent_profile(session_profile_id, tenant_id=tenant_id)
                profile_auth_user_id = str((profile_row or {}).get("auth_user_id") or "").strip()
                if auth_user_id and profile_auth_user_id and profile_auth_user_id != auth_user_id:
                    raise ValueError("Session profile does not belong to the authenticated user")
            if profile_row is None:
                profile_row, profile_token_value = self._resolve_or_create_profile(
                    tenant_id=tenant_id,
                    profile_id=profile_id,
                    profile_token=profile_token,
                    auth_user_id=auth_user_id,
                )
                self.repository.attach_agent_profile_to_session(session_id, str(profile_row.get("profile_id") or ""))
        runtime_config = self._load_runtime_config(tenant_id)
        session_worker_id = str(uuid4())
        if not self.repository.claim_agent_session(session_id, session_worker_id, runtime_config["session_lease_seconds"]):
            raise ValueError("Session is busy")
        try:
            corpus_snapshot_id = self.repository.get_latest_corpus_snapshot_id(tenant_id)
            resolved_document_ids = self._resolve_document_scope(document_ids, tenant_id)
            resolved_profile_id = str((profile_row or {}).get("profile_id") or "").strip() or None
            self._enforce_session_scope(
                session_id,
                tenant_id,
                resolved_document_ids,
                auth_user_id=auth_user_id,
                profile_id=resolved_profile_id,
            )
            routing = route_question_cached(
                normalized_query,
                runtime_config,
                tenant_id=tenant_id,
                repository=self.repository,
            )
            question_type = str(routing.get("question_type") or classify_question_fallback(normalized_query))
            top_k, top_k_source = _resolve_query_top_k(
                question_type,
                top_k,
                runtime_config,
                routing.get("top_k"),
                routing.get("source"),
            )
            resolved_query_mode = _normalize_query_mode(query_mode)
            include_sensor_context = _should_include_sensor_context_for_mode(
                resolved_query_mode,
                normalized_query,
                question_type,
                auth_user_id=auth_user_id,
                runtime_config=runtime_config,
            )
            sensor_first = _should_use_sensor_only_context_for_mode(
                resolved_query_mode,
                normalized_query,
                question_type,
                auth_user_id=auth_user_id,
                document_ids=resolved_document_ids,
                runtime_config=runtime_config,
            )
            system_prompt_variant = _resolve_system_prompt_variant(
                resolved_query_mode,
                include_sensor_context=include_sensor_context,
                sensor_first=sensor_first,
            )
            runtime_config = {
                **runtime_config,
                "system_prompt": _resolve_system_prompt(runtime_config, system_prompt_variant),
            }
            request_scope = {
                "tenant_id": tenant_id,
                "auth_user_id": auth_user_id or "",
                "workspace_kind": str((session_row or {}).get("workspace_kind") or resolved_workspace_kind or "general"),
                "document_ids": list(resolved_document_ids or []),
                "corpus_snapshot_id": corpus_snapshot_id,
                "requested_top_k": top_k if top_k_source == "manual" else None,
                "top_k": top_k,
                "top_k_source": top_k_source,
                "query_mode": resolved_query_mode,
                "system_prompt_variant": system_prompt_variant,
                "include_sensor_context": include_sensor_context,
                "routing": routing,
            }

            recent_message_limit = max(
                1,
                int(runtime_config.get("memory_recent_messages") or 8),
                int(runtime_config.get("profile_recent_messages") or 12),
            )
            prior_messages = self.repository.list_agent_messages(
                session_id,
                limit=recent_message_limit,
                tenant_id=tenant_id,
                auth_user_id=auth_user_id,
                profile_id=resolved_profile_id,
            )
            session_memory = self.repository.get_agent_session_memory(
                session_id,
                tenant_id=tenant_id,
                auth_user_id=auth_user_id,
                profile_id=resolved_profile_id,
            )
            profile_summary = profile_row
            user_message_id = str(uuid4())

            # The retrieval plan controls breadth, neighbor expansion, and whether
            # lightweight KG search should augment the chunk evidence.
            retrieval_plan = _select_retrieval_plan(question_type, top_k, runtime_config, routing)
            retrieval_mode = retrieval_plan["mode"]
            request_scope["question_type"] = question_type
            if sensor_first:
                retrieval_plan = {
                    **retrieval_plan,
                    "search_k": 0,
                    "select_k": 0,
                    "expand_neighbors": False,
                    "prefer_assets": False,
                    "mode": "sensor_first",
                }
                retrieval_mode = "sensor_first"
                request_scope["sensor_first"] = True

            query_embedding: list[float] | None = None
            raw_matches: list[dict[str, Any]] = []
            embedding_error: str | None = None
            if not sensor_first:
                query_embedding, raw_matches, embedding_error = self._retrieve_chunk_candidates(
                    normalized_query=normalized_query,
                    retrieval_plan=retrieval_plan,
                    tenant_id=tenant_id,
                    document_ids=resolved_document_ids,
                    runtime_config=runtime_config,
                )
            bundle = self._build_context_bundle(
                normalized_query=normalized_query,
                query_embedding=query_embedding,
                raw_matches=raw_matches,
                tenant_id=tenant_id,
                auth_user_id=auth_user_id,
                question_type=question_type,
                retrieval_plan=retrieval_plan,
                include_sensor_context=include_sensor_context,
                document_ids=resolved_document_ids,
                runtime_config=runtime_config,
            )
            prompt_bundle = self._build_prompt_bundle(
                question=question,
                normalized_query=normalized_query,
                prior_messages=prior_messages,
                profile_summary=profile_summary,
                session_memory=session_memory,
                bundle=bundle,
                question_type=question_type,
                runtime_config=runtime_config,
                workspace_kind=str(request_scope.get("workspace_kind") or "general"),
            )
            prompt_bundle.stats["request_scope"] = request_scope
            final_bundle = _trim_context_bundle_to_prompt(bundle, prompt_bundle)

            artifact: dict[str, Any] | None = None
            response: dict[str, Any] | None = None
            review_status = ""
            review_reason = ""

            open_world_error: str | None = None
            if not final_bundle.chunks and not final_bundle.assets and not final_bundle.sensor_rows:
                try:
                    artifact, response = self._run_open_world_fallback(
                        question=question,
                        normalized_query=normalized_query,
                        question_type=question_type,
                        bundle=final_bundle,
                        runtime_config=runtime_config,
                    )
                    review_status, review_reason = _derive_agent_review_state(response, runtime_config)
                except AgentQueryError as exc:
                    open_world_error = str(exc)
                    artifact = None
                    response = None

            if artifact is None and response is None and not final_bundle.chunks and not final_bundle.assets and not final_bundle.sensor_rows:
                result = self._build_last_resort_response(
                    question=question,
                    question_type=question_type,
                    bundle=final_bundle,
                    reason=open_world_error or "no_retrieval_results",
                    runtime_config=runtime_config,
                )
                result.update({
                    "session_id": session_id,
                    "user_message_id": user_message_id,
                    "question": question,
                    "citations": [],
                    "question_type": question_type,
                    "retrieval_mode": retrieval_mode,
                    "query_mode": resolved_query_mode,
                    "system_prompt_variant": system_prompt_variant,
                    "top_k": top_k,
                    "top_k_source": top_k_source,
                    "routing": routing,
                    "retrieval_plan": retrieval_plan,
                    "query_run_id": None,
                })
                query_run_id = self.repository.persist_agent_turn(
                    session_id=session_id,
                    tenant_id=tenant_id,
                    user_message_id=user_message_id,
                    question=question,
                    normalized_query=normalized_query,
                    question_type=question_type,
                    retrieval_mode=retrieval_mode,
                    status="completed",
                    answer=result["answer"],
                    confidence=float(result.get("confidence") or 0.0),
                    abstained=False,
                    abstain_reason=None,
                    provider="fallback",
                    model="last-resort",
                    prompt_version=runtime_config["prompt_version"],
                    metrics={
                    "retrieved_chunks": 0,
                    "retrieved_assets": 0,
                    "retrieved_sensor_rows": 0,
                    "selected_chunks": 0,
                    "selected_assets": 0,
                    "selected_sensor_rows": 0,
                    "selected_assertions": 0,
                        "selected_entities": 0,
                        "latency_ms": round((time.perf_counter() - started_at) * 1000, 2),
                        "embedding_fallback_used": bool(embedding_error),
                        "routing": routing,
                        "profile_id": str((profile_row or {}).get("profile_id") or ""),
                        "runtime_config": runtime_config,
                        "prompt_context": prompt_bundle.stats,
                        "last_resort_fallback_used": True,
                    },
                    prompt_payload={
                        "question": question,
                        "normalized_query": normalized_query,
                        "request_scope": request_scope,
                        "retrieval_plan": retrieval_plan,
                        "routing": routing,
                        "profile": dict((profile_row or {}).get("summary_json") or {}),
                        "runtime_config": runtime_config,
                        "prompt_context": prompt_bundle.stats,
                        "fallback_reason": open_world_error or "no_retrieval_results",
                    },
                    raw_response_payload={"error": open_world_error} if open_world_error else {},
                    final_response_payload=result,
                    review_status="needs_review",
                    review_reason="last_resort_fallback",
                    sources=final_bundle.sources,
                    corpus_snapshot_id=corpus_snapshot_id,
                    assistant_metadata={
                        "abstained": False,
                        "citations": result.get("citations") or [],
                        "asset_citations": [item for item in (result.get("citations") or []) if item.get("citation_kind") == "asset"],
                        "sensor_citations": [item for item in (result.get("citations") or []) if item.get("citation_kind") == "sensor"],
                        "evidence_citations": [item for item in (result.get("citations") or []) if item.get("citation_kind") == "kg_evidence"],
                        "supporting_assertions": result.get("supporting_assertions") or [],
                        "corpus_snapshot_id": corpus_snapshot_id,
                        "review_status": "needs_review",
                        "review_reason": "last_resort_fallback",
                        "grounding_check": result.get("grounding_check") or {},
                    },
                )
                result["query_run_id"] = query_run_id
                result["corpus_snapshot_id"] = corpus_snapshot_id
                result["review_status"] = "needs_review"
                result["review_reason"] = "last_resort_fallback"
                result["latency_ms"] = round((time.perf_counter() - started_at) * 1000, 2)
                result["fallback_used"] = True
                result["session_token"] = session_token_value
                result["profile_token"] = profile_token_value
                result["profile_id"] = str((profile_row or {}).get("profile_id") or "")
                memory_snapshot = self._refresh_session_memory(
                    session_id=session_id,
                    tenant_id=tenant_id,
                    query_run_id=query_run_id,
                    question=question,
                    normalized_query=normalized_query,
                    response=result,
                    bundle=final_bundle,
                    request_scope=request_scope,
                    prior_summary=session_memory,
                    recent_messages=prior_messages,
                    runtime_config=runtime_config,
                    auth_user_id=auth_user_id,
                    profile_id=resolved_profile_id,
                )
                if memory_snapshot is not None:
                    result["session_memory"] = memory_snapshot
                profile_snapshot = self._refresh_agent_profile(
                    profile_id=str((profile_row or {}).get("profile_id") or ""),
                    tenant_id=tenant_id,
                    session_id=session_id,
                    question=question,
                    normalized_query=normalized_query,
                    response=result,
                    session_memory=memory_snapshot or session_memory,
                    prior_profile=profile_row,
                    recent_messages=prior_messages,
                    runtime_config=runtime_config,
                )
                if profile_snapshot is not None:
                    result["profile"] = profile_snapshot
                return result

            if artifact is None or response is None:
                try:
                # Normal path: synthesize a grounded answer from the selected evidence.
                    artifact = self._generate_answer(question, normalized_query, prompt_bundle, question_type, runtime_config)
                    final_bundle = _trim_context_bundle_to_prompt(bundle, prompt_bundle)
                    response = _coerce_agent_response(artifact["content"], bundle=final_bundle)
                    response = self._finalize_response(final_bundle, response, normalized_query, question_type, runtime_config)
                    if bool(response.get("abstained")) and str(response.get("abstain_reason") or "").strip() in {
                        "low_confidence",
                        "model_abstained",
                        "no_valid_citations",
                        "weak_grounding",
                    }:
                        try:
                            artifact, response = self._run_open_world_fallback(
                                question=question,
                                normalized_query=normalized_query,
                                question_type=question_type,
                                bundle=final_bundle,
                                runtime_config=runtime_config,
                            )
                        except AgentQueryError:
                            pass
                    review_status, review_reason = _derive_agent_review_state(response, runtime_config)
                except AgentQueryError as exc:
                # Fallback path: if the model call fails, degrade to a deterministic
                # evidence summary instead of turning the request into a hard error.
                    final_bundle = _trim_context_bundle_to_prompt(bundle, prompt_bundle)
                    try:
                        artifact, response = self._run_open_world_fallback(
                            question=question,
                            normalized_query=normalized_query,
                            question_type=question_type,
                            bundle=final_bundle,
                            runtime_config=runtime_config,
                        )
                        review_status, review_reason = _derive_agent_review_state(response, runtime_config)
                    except AgentQueryError as open_world_exc:
                        response = self._build_last_resort_response(
                            question=question,
                            question_type=question_type,
                            bundle=final_bundle,
                            reason=f"{exc} | open_world={open_world_exc}",
                            runtime_config=runtime_config,
                        )
                        artifact = {
                            "provider": "fallback",
                            "model": "last-resort",
                            "prompt_version": runtime_config["prompt_version"],
                            "prompt_payload": {
                                "fallback_reason": str(exc),
                                "open_world_fallback_reason": str(open_world_exc),
                                "normalized_query": normalized_query,
                                "question_type": question_type,
                                "request_scope": request_scope,
                                "routing": routing,
                                "runtime_config": runtime_config,
                                "prompt_context": prompt_bundle.stats,
                            },
                            "raw_payload": {"error": str(exc), "open_world_error": str(open_world_exc)},
                        }
                        review_status = "needs_review"
                        review_reason = "last_resort_fallback"

            artifact_prompt_payload = dict(artifact.get("prompt_payload") or {})
            artifact_prompt_payload["request_scope"] = request_scope
            artifact["prompt_payload"] = artifact_prompt_payload

            query_run_id = self.repository.persist_agent_turn(
                session_id=session_id,
                tenant_id=tenant_id,
                user_message_id=user_message_id,
                question=question,
                normalized_query=normalized_query,
                question_type=question_type,
                retrieval_mode=retrieval_mode,
                status="completed",
                answer=response["answer"],
                confidence=response["confidence"],
                abstained=response["abstained"],
                abstain_reason=response.get("abstain_reason"),
                provider=artifact["provider"],
                model=artifact["model"],
                prompt_version=artifact["prompt_version"],
                metrics={
                    "retrieved_chunks": len(raw_matches),
                    "retrieved_assets": len([item for item in bundle.sources if item.get("source_kind") == "asset_hit"]),
                    "retrieved_sensor_rows": len(bundle.sensor_rows),
                    "embedding_fallback_used": bool(embedding_error),
                    "selected_chunks": len(final_bundle.chunks),
                    "selected_assets": len(final_bundle.assets),
                    "selected_sensor_rows": len(final_bundle.sensor_rows),
                    "selected_assertions": len(final_bundle.assertions),
                    "selected_entities": len(final_bundle.entities),
                    "selected_citations": len(response["citations"]),
                    "latency_ms": round((time.perf_counter() - started_at) * 1000, 2),
                    "fallback_used": artifact["provider"] == "fallback",
                    "routing": routing,
                    "runtime_config": runtime_config,
                    "prompt_context": prompt_bundle.stats,
                },
                prompt_payload=artifact_prompt_payload,
                raw_response_payload=artifact.get("raw_payload"),
                final_response_payload=response,
                review_status=review_status,
                review_reason=review_reason,
                sources=final_bundle.sources,
                corpus_snapshot_id=corpus_snapshot_id,
                assistant_metadata={
                    "abstained": response["abstained"],
                    "citations_payload": response["citations"],
                    "citations": [item.get("chunk_id") or item.get("asset_id") or item.get("sensor_row_id") or item.get("evidence_id") for item in response["citations"]],
                    "sensor_citations": [item.get("sensor_row_id") for item in response["citations"] if item.get("sensor_row_id")],
                    "evidence_citations": [item.get("evidence_id") for item in response["citations"] if item.get("evidence_id")],
                    "supporting_assertions": response["supporting_assertions"],
                    "corpus_snapshot_id": corpus_snapshot_id,
                    "review_status": review_status,
                    "review_reason": review_reason,
                },
            )

            response.update(
                {
                    "session_id": session_id,
                    "user_message_id": user_message_id,
                    "question": question,
                    "question_type": question_type,
                    "retrieval_mode": retrieval_mode,
                    "query_mode": resolved_query_mode,
                    "system_prompt_variant": system_prompt_variant,
                    "top_k": top_k,
                    "top_k_source": top_k_source,
                    "routing": routing,
                    "retrieval_plan": retrieval_plan,
                    "query_run_id": query_run_id,
                    "review_status": review_status,
                    "review_reason": review_reason,
                    "corpus_snapshot_id": corpus_snapshot_id,
                    "embedding_fallback_used": bool(embedding_error),
                    "latency_ms": round((time.perf_counter() - started_at) * 1000, 2),
                    "fallback_used": artifact["provider"] == "fallback",
                    "session_token": session_token_value,
                    "profile_token": profile_token_value,
                    "profile_id": str((profile_row or {}).get("profile_id") or ""),
                }
            )
            memory_snapshot = self._refresh_session_memory(
                session_id=session_id,
                tenant_id=tenant_id,
                query_run_id=query_run_id,
                question=question,
                normalized_query=normalized_query,
                response=response,
                bundle=final_bundle,
                request_scope=request_scope,
                prior_summary=session_memory,
                recent_messages=prior_messages,
                runtime_config=runtime_config,
                auth_user_id=auth_user_id,
                profile_id=resolved_profile_id,
            )
            if memory_snapshot is not None:
                response["session_memory"] = memory_snapshot
            profile_snapshot = self._refresh_agent_profile(
                profile_id=str((profile_row or {}).get("profile_id") or ""),
                tenant_id=tenant_id,
                session_id=session_id,
                question=question,
                normalized_query=normalized_query,
                response=response,
                session_memory=memory_snapshot or session_memory,
                prior_profile=profile_row,
                recent_messages=prior_messages,
                runtime_config=runtime_config,
            )
            if profile_snapshot is not None:
                response["profile"] = profile_snapshot
            return response
        finally:
            self.repository.release_agent_session(session_id, session_worker_id)

    def chat(
        self,
        question: str,
        session_id: str | None = None,
        session_token: str | None = None,
        profile_id: str | None = None,
        profile_token: str | None = None,
        auth_user_id: str | None = None,
        tenant_id: str = "shared",
        document_ids: list[str] | None = None,
        top_k: int | None = None,
        query_mode: str | None = None,
        workspace_kind: str | None = None,
        trusted_tenant: bool = False,
        trusted_session_reuse: bool = False,
    ) -> dict[str, Any]:
        result = self.query(
            question=question,
            session_id=session_id,
            session_token=session_token,
            profile_id=profile_id,
            profile_token=profile_token,
            auth_user_id=auth_user_id,
            tenant_id=tenant_id,
            document_ids=document_ids,
            top_k=top_k,
            query_mode=query_mode,
            workspace_kind=workspace_kind,
            trusted_tenant=trusted_tenant,
            trusted_session_reuse=trusted_session_reuse,
        )
        result["messages"] = self.repository.list_agent_messages(
            result["session_id"],
            limit=20,
            tenant_id=str(settings.agent_public_tenant_id or "shared"),
            auth_user_id=auth_user_id,
            profile_id=str(result.get("profile_id") or "").strip() or None,
        )
        return result

    def _resolve_or_create_profile(
        self,
        *,
        tenant_id: str,
        profile_id: str | None,
        profile_token: str | None,
        auth_user_id: str | None = None,
    ) -> tuple[dict[str, Any], str]:
        profile_row = self.repository.get_agent_profile(profile_id, tenant_id=tenant_id) if profile_id else None
        if profile_row is None and auth_user_id:
            profile_row = self.repository.get_agent_profile_by_auth_user(auth_user_id, tenant_id=tenant_id)
        profile_token_value = profile_token or ""
        if profile_id and profile_row is None:
            raise ValueError("Profile not found")
        if profile_row is not None:
            profile_id_value = str(profile_row.get("profile_id") or "")
            token_ok = self.repository.verify_agent_profile_token(profile_id_value, profile_token, tenant_id=tenant_id)
            if token_ok:
                return profile_row, profile_token_value
            profile_auth_user_id = str(profile_row.get("auth_user_id") or "").strip()
            if auth_user_id and profile_auth_user_id and profile_auth_user_id == auth_user_id:
                profile_token_value = str(uuid4())
                self.repository.set_agent_profile_token(profile_id_value, profile_token_value)
                return profile_row, profile_token_value
            raise ValueError("Profile token is required for profile reuse")
        created_profile_id = self.repository.create_agent_profile(tenant_id=tenant_id, auth_user_id=auth_user_id)
        profile_token_value = str(uuid4())
        self.repository.set_agent_profile_token(created_profile_id, profile_token_value)
        created_profile = self.repository.get_agent_profile(created_profile_id, tenant_id=tenant_id)
        if created_profile is None:
            raise ValueError("Profile creation failed")
        return created_profile, profile_token_value

    def _refresh_session_memory(
        self,
        *,
        session_id: str,
        tenant_id: str,
        query_run_id: str,
        question: str,
        normalized_query: str,
        response: dict[str, Any],
        bundle: AgentContextBundle,
        request_scope: dict[str, Any],
        prior_summary: dict[str, Any] | None,
        recent_messages: list[dict[str, Any]],
        runtime_config: dict[str, Any],
        auth_user_id: str | None = None,
        profile_id: str | None = None,
    ) -> dict[str, Any] | None:
        if not runtime_config.get("memory_enabled", True):
            return prior_summary
        summary_json, summary_text, provider, model, prompt_version = _summarize_session_memory(
            question=question,
            normalized_query=normalized_query,
            response=response,
            bundle=bundle,
            request_scope=request_scope,
            prior_summary=prior_summary,
            recent_messages=recent_messages,
            runtime_config=runtime_config,
        )
        if not summary_json and not summary_text:
            return prior_summary
        self.repository.save_agent_session_memory(
            session_id=session_id,
            summary_json=summary_json,
            summary_text=summary_text,
            source_provider=provider,
            source_model=model,
            prompt_version=prompt_version,
        )
        return self.repository.get_agent_session_memory(
            session_id,
            tenant_id=tenant_id,
            auth_user_id=auth_user_id,
            profile_id=profile_id,
        )

    def _refresh_agent_profile(
        self,
        *,
        profile_id: str,
        tenant_id: str,
        session_id: str,
        question: str,
        normalized_query: str,
        response: dict[str, Any],
        session_memory: dict[str, Any] | None,
        prior_profile: dict[str, Any] | None,
        recent_messages: list[dict[str, Any]],
        runtime_config: dict[str, Any],
    ) -> dict[str, Any] | None:
        if not profile_id or not runtime_config.get("profile_enabled", True):
            return prior_profile
        summary_json, summary_text, provider, model, prompt_version = _summarize_agent_profile(
            question=question,
            normalized_query=normalized_query,
            response=response,
            session_memory=session_memory,
            prior_profile=prior_profile,
            recent_messages=recent_messages,
            runtime_config=runtime_config,
        )
        if not summary_json and not summary_text:
            return prior_profile
        self.repository.save_agent_profile(
            profile_id=profile_id,
            summary_json=summary_json,
            summary_text=summary_text,
            source_provider=provider,
            source_model=model,
            prompt_version=prompt_version,
        )
        return self.repository.get_agent_profile(profile_id, tenant_id=tenant_id)

    def inspect_retrieval(
        self,
        question: str,
        tenant_id: str = "shared",
        document_ids: list[str] | None = None,
        top_k: int | None = None,
        query_mode: str | None = None,
        auth_user_id: str | None = None,
        trusted_tenant: bool = False,
    ) -> dict[str, Any]:
        """Run the retrieval path without answer synthesis so tuning can inspect the real evidence bundle."""
        normalized_query = _normalize_query(question)
        if not normalized_query:
            raise ValueError("Question cannot be empty")
        if not trusted_tenant:
            tenant_id = settings.agent_public_tenant_id or "shared"

        runtime_config = self._load_runtime_config(tenant_id)
        resolved_document_ids = self._resolve_document_scope(document_ids, tenant_id)
        routing = route_question_cached(
            normalized_query,
            runtime_config,
            tenant_id=tenant_id,
            repository=self.repository,
        )
        question_type = str(routing.get("question_type") or classify_question_fallback(normalized_query))
        top_k, top_k_source = _resolve_query_top_k(
            question_type,
            top_k,
            runtime_config,
            routing.get("top_k"),
            routing.get("source"),
        )
        resolved_query_mode = _normalize_query_mode(query_mode)
        include_sensor_context = _should_include_sensor_context_for_mode(
            resolved_query_mode,
            normalized_query,
            question_type,
            auth_user_id=auth_user_id,
            runtime_config=runtime_config,
        )
        retrieval_plan = _select_retrieval_plan(question_type, top_k, runtime_config, routing)
        query_embedding, raw_matches, embedding_error = self._retrieve_chunk_candidates(
            normalized_query=normalized_query,
            retrieval_plan=retrieval_plan,
            tenant_id=tenant_id,
            document_ids=resolved_document_ids,
            runtime_config=runtime_config,
        )
        bundle = self._build_context_bundle(
            normalized_query=normalized_query,
            query_embedding=query_embedding,
            raw_matches=raw_matches,
            tenant_id=tenant_id,
            auth_user_id=auth_user_id,
            question_type=question_type,
            retrieval_plan=retrieval_plan,
            include_sensor_context=include_sensor_context,
            document_ids=resolved_document_ids,
            runtime_config=runtime_config,
        )
        prompt_bundle = self._build_prompt_bundle(
            question=question,
            normalized_query=normalized_query,
            prior_messages=[],
            profile_summary=None,
            session_memory=None,
            bundle=bundle,
            question_type=question_type,
            runtime_config=runtime_config,
        )
        final_bundle = _trim_context_bundle_to_prompt(bundle, prompt_bundle)
        return {
            "question": question,
            "normalized_query": normalized_query,
            "tenant_id": tenant_id,
            "document_ids": resolved_document_ids or [],
            "top_k": top_k,
            "top_k_source": top_k_source,
            "query_mode": resolved_query_mode,
            "routing": routing,
            "question_type": question_type,
            "retrieval_mode": retrieval_plan["mode"],
            "retrieval_plan": retrieval_plan,
            "embedding_fallback_used": bool(embedding_error),
            "embedding_error": embedding_error,
            "raw_chunk_matches": raw_matches,
            "sources": final_bundle.sources,
            "chunks": final_bundle.chunks,
            "assets": final_bundle.assets,
            "sensor_rows": final_bundle.sensor_rows,
            "assertions": final_bundle.assertions,
            "entities": final_bundle.entities,
            "graph_chains": final_bundle.graph_chains,
            "evidence": final_bundle.evidence,
            "prompt_chunk_payload": prompt_bundle.chunk_payload,
            "prompt_asset_payload": prompt_bundle.asset_payload,
            "prompt_sensor_payload": prompt_bundle.sensor_payload,
            "prompt_assertion_payload": prompt_bundle.assertion_payload,
            "prompt_entity_payload": prompt_bundle.entity_payload,
            "prompt_graph_chain_payload": prompt_bundle.graph_chain_payload,
            "prompt_evidence_payload": prompt_bundle.evidence_payload,
            "prompt_context": prompt_bundle.stats,
        }

    def _enforce_session_scope(
        self,
        session_id: str,
        tenant_id: str,
        document_ids: list[str] | None,
        auth_user_id: str | None = None,
        profile_id: str | None = None,
    ) -> None:
        latest_scope = self.repository.get_latest_agent_session_scope(
            session_id,
            tenant_id=tenant_id,
            auth_user_id=auth_user_id,
            profile_id=profile_id,
        )
        if not latest_scope:
            return
        latest_tenant = str(latest_scope.get("tenant_id") or tenant_id)
        latest_document_ids = sorted(str(item) for item in (latest_scope.get("document_ids") or []) if str(item).strip())
        incoming_document_ids = sorted(str(item) for item in (document_ids or []) if str(item).strip())
        if latest_tenant != tenant_id or latest_document_ids != incoming_document_ids:
            raise ValueError("Session scope changed. Start a new session before changing tenant or document filters.")

    def _resolve_document_scope(self, document_ids: list[str] | None, tenant_id: str) -> list[str] | None:
        # Validate document filters against Postgres before they become vector-store filters.
        if not document_ids:
            return None
        # Validate the explicit scope against persisted document ownership before it
        # reaches the vector store.
        rows = self.repository.list_documents_by_ids(document_ids)
        row_map = {str(row["document_id"]): row for row in rows}
        missing = [item for item in document_ids if item not in row_map]
        if missing:
            raise ValueError(f"Unknown document ids: {', '.join(missing[:5])}")
        invalid = [item for item, row in row_map.items() if str(row.get("tenant_id") or "") != tenant_id]
        if invalid:
            raise ValueError("Document scope violates tenant boundary")
        return document_ids

    def _expand_chunks_from_synopsis_hits(
        self,
        *,
        normalized_query: str,
        tenant_id: str,
        question_type: str,
        runtime_config: dict[str, Any],
        document_ids: list[str] | None = None,
    ) -> list[dict[str, Any]]:
        if question_type not in {"explanation", "procedure", "comparison"}:
            return []
        section_hits = self.repository.search_section_synopses_lexical(
            normalized_query,
            tenant_id=tenant_id,
            document_ids=document_ids,
            limit=min(8, max(4, runtime_config["max_context_chunks"])),
        )
        document_hits = self.repository.search_document_synopses_lexical(
            normalized_query,
            tenant_id=tenant_id,
            document_ids=document_ids,
            limit=min(4, max(2, runtime_config["max_context_chunks"] // 2)),
        )
        expanded_rows: list[dict[str, Any]] = []
        seen_chunk_ids: set[str] = set()
        for hit in section_hits[:4]:
            target_path = [str(item) for item in (hit.get("section_path") or [])]
            candidates = self.repository.list_chunk_records(
                document_id=str(hit.get("document_id") or ""),
                status="accepted",
                limit=max(12, runtime_config["max_context_chunks"] * 2),
                offset=0,
            )
            kept_for_section = 0
            for row in candidates:
                if [str(item) for item in (row.get("section_path") or [])] != target_path:
                    continue
                chunk_id = str(row.get("chunk_id") or "")
                if not chunk_id or chunk_id in seen_chunk_ids:
                    continue
                boosted = dict(row)
                boosted["_retrieval_score"] = max(
                    float(boosted.get("_retrieval_score") or 0.0),
                    min(0.42, float(hit.get("lexical_score") or 0.0) / 40.0 + 0.08),
                )
                boosted["_synopsis_guided"] = "section"
                expanded_rows.append(boosted)
                seen_chunk_ids.add(chunk_id)
                kept_for_section += 1
                if kept_for_section >= 3:
                    break
        seen_documents = {str(row.get("document_id") or "") for row in expanded_rows if str(row.get("document_id") or "")}
        for hit in document_hits[:3]:
            document_id = str(hit.get("document_id") or "")
            if not document_id or document_id in seen_documents:
                continue
            candidates = self.repository.list_chunk_records(
                document_id=document_id,
                status="accepted",
                limit=max(6, runtime_config["max_context_chunks"]),
                offset=0,
            )
            kept_for_document = 0
            for row in candidates:
                chunk_id = str(row.get("chunk_id") or "")
                if not chunk_id or chunk_id in seen_chunk_ids:
                    continue
                boosted = dict(row)
                boosted["_retrieval_score"] = max(
                    float(boosted.get("_retrieval_score") or 0.0),
                    min(0.28, float(hit.get("lexical_score") or 0.0) / 48.0 + 0.04),
                )
                boosted["_synopsis_guided"] = "document"
                expanded_rows.append(boosted)
                seen_chunk_ids.add(chunk_id)
                kept_for_document += 1
                if kept_for_document >= 2:
                    break
        return expanded_rows

    def _build_context_bundle(
        self,
        normalized_query: str,
        query_embedding: list[float] | None,
        raw_matches: list[dict[str, Any]],
        tenant_id: str,
        auth_user_id: str | None,
        question_type: str,
        retrieval_plan: dict[str, Any],
        include_sensor_context: bool,
        document_ids: list[str] | None = None,
        runtime_config: dict[str, Any] | None = None,
    ) -> AgentContextBundle:
        # Vector hits are the primary evidence; linked image assets and KG rows add
        # context, not authority. Asset hits are retrieved through the separate asset
        # collection using OCR/description text rather than raw image vectors.
        runtime_config = runtime_config or default_agent_runtime_config()
        prefer_assets = bool(retrieval_plan.get("prefer_assets"))
        asset_vector_search = bool(retrieval_plan.get("asset_vector_search"))
        asset_match_limit = max(
            0,
            min(
                runtime_config["max_asset_search_k"],
                max(
                    runtime_config["max_context_assets"] * (6 if prefer_assets else 2),
                    min(6 if prefer_assets else 4, runtime_config["max_asset_search_k"]),
                ),
            ),
        )
        raw_asset_dense_matches = self.store.search_assets(
            query_embedding=query_embedding,
            top_k=asset_match_limit,
            tenant_id=tenant_id,
            document_ids=document_ids,
        ) if asset_vector_search and asset_match_limit and query_embedding else []
        raw_asset_lexical_matches = self.repository.search_page_assets_lexical(
            normalized_query,
            tenant_id=tenant_id,
            document_ids=document_ids,
            limit=max(2, min(asset_match_limit or runtime_config["max_context_assets"], runtime_config["max_asset_search_k"])),
        ) if asset_vector_search and asset_match_limit else []
        raw_asset_matches = _merge_hybrid_matches(raw_asset_dense_matches, raw_asset_lexical_matches, id_key="asset_id")
        raw_asset_matches = _rerank_asset_matches(normalized_query, raw_asset_matches, runtime_config)

        selected_ids: list[str] = []
        source_rows: list[dict[str, Any]] = []
        # Keep the entire initial retrieval set for traceability, then mark which
        # matches survived selection into the prompt bundle.
        selected_match_ids = set(_select_diverse_match_ids(raw_matches, retrieval_plan["select_k"], runtime_config))
        for match in raw_matches:
            chunk_id = str(match["chunk_id"])
            if chunk_id in selected_match_ids:
                selected_ids.append(chunk_id)
            source_rows.append(
                {
                    "source_kind": "chunk",
                    "source_id": chunk_id,
                    "document_id": match.get("metadata", {}).get("document_id"),
                    "chunk_id": chunk_id,
                    "rank": match.get("rank"),
                    "score": _distance_to_score(match.get("distance")) or float(match.get("rerank_score") or match.get("lexical_score") or 0.0),
                    "selected": chunk_id in selected_match_ids,
                    "payload": {
                        "distance": match.get("distance"),
                        "metadata": match.get("metadata", {}),
                        "rerank_score": match.get("rerank_score"),
                        "match_source": match.get("match_source"),
                        "match_sources": list(match.get("match_sources") or []),
                    },
                }
            )

        base_chunks = self.repository.list_chunk_records_by_ids(selected_ids)
        match_by_chunk_id = {str(match.get("chunk_id") or ""): match for match in raw_matches}
        for row in base_chunks:
            match = match_by_chunk_id.get(str(row.get("chunk_id") or ""))
            if match:
                row["_retrieval_score"] = float(match.get("rerank_score") or 0.0)
                row["_retrieval_rank"] = int(match.get("rank") or 0)
                row["_retrieval_distance"] = match.get("distance")
        chunk_by_id = {row["chunk_id"]: row for row in base_chunks}

        if retrieval_plan["expand_neighbors"]:
            neighbor_ids: list[str] = []
            for row in base_chunks[: min(3, len(base_chunks))]:
                for candidate in (row.get("prev_chunk_id"), row.get("next_chunk_id")):
                    if candidate and candidate not in selected_ids and candidate not in neighbor_ids:
                        neighbor_ids.append(candidate)
            if neighbor_ids:
                for row in self.repository.list_chunk_records_by_ids(neighbor_ids[: runtime_config["neighbor_window"] * 6]):
                    chunk_by_id.setdefault(row["chunk_id"], row)
        for row in self._expand_chunks_from_synopsis_hits(
            normalized_query=normalized_query,
            tenant_id=tenant_id,
            question_type=question_type,
            runtime_config=runtime_config,
            document_ids=document_ids,
        ):
            existing = chunk_by_id.get(row["chunk_id"])
            if existing is None:
                chunk_by_id[row["chunk_id"]] = row
                continue
            existing["_retrieval_score"] = max(
                float(existing.get("_retrieval_score") or 0.0),
                float(row.get("_retrieval_score") or 0.0),
            )
            if row.get("_synopsis_guided"):
                existing["_synopsis_guided"] = row.get("_synopsis_guided")

        chunks = _compress_chunks(
            normalized_query,
            list(chunk_by_id.values()),
            runtime_config,
            question_type,
            document_spread=str(retrieval_plan.get("document_spread") or "few"),
        )
        chunk_ids = [row["chunk_id"] for row in chunks]
        selected_chunk_document_ids = {str(row.get("document_id") or "") for row in chunks if str(row.get("document_id") or "")}
        section_synopsis_rows = self.repository.list_section_synopses_for_chunk_ids(
            chunk_ids,
            limit=max(6, min(len(chunk_ids) * 2, 24)),
        ) if chunk_ids else []
        section_synopsis_by_key = {
            (
                str(row.get("document_id") or ""),
                tuple(str(item) for item in (row.get("section_path") or [])),
            ): row
            for row in section_synopsis_rows
        }
        document_synopsis_by_document_id = {
            str(row.get("document_id") or ""): row
            for row in self.repository.list_document_synopses_by_ids(sorted(selected_chunk_document_ids))
        } if selected_chunk_document_ids else {}
        for row in chunks:
            document_id = str(row.get("document_id") or "")
            section_key = (
                document_id,
                tuple(str(item) for item in (row.get("section_path") or [])),
            )
            section_synopsis = section_synopsis_by_key.get(section_key)
            if section_synopsis:
                row["_section_synopsis"] = str(section_synopsis.get("synopsis_text") or "")
                row["_section_synopsis_id"] = str(section_synopsis.get("section_id") or "")
                row["_section_synopsis_title"] = str(section_synopsis.get("section_title") or "")
            document_synopsis = document_synopsis_by_document_id.get(document_id)
            if document_synopsis:
                row["_document_synopsis"] = str(document_synopsis.get("synopsis_text") or "")
                row["_document_synopsis_title"] = str(document_synopsis.get("title") or "")
        filtered_asset_matches = raw_asset_matches
        if selected_chunk_document_ids and not prefer_assets:
            same_document_asset_matches = [
                match
                for match in raw_asset_matches
                if str((match.get("metadata", {}) or {}).get("document_id") or "") in selected_chunk_document_ids
            ]
            if same_document_asset_matches:
                filtered_asset_matches = same_document_asset_matches

        selected_asset_ids = _select_diverse_asset_ids(filtered_asset_matches, runtime_config["max_context_assets"])
        direct_asset_rows = self.repository.list_page_assets_by_ids(selected_asset_ids)
        match_by_asset_id = {str(match.get("asset_id") or ""): match for match in filtered_asset_matches}
        for row in direct_asset_rows:
            match = match_by_asset_id.get(str(row.get("asset_id") or ""))
            if match:
                row["_retrieval_score"] = float(match.get("rerank_score") or 0.0)
                row["_retrieval_rank"] = int(match.get("rank") or 0)
                row["_retrieval_distance"] = match.get("distance")

        if question_type == "visual_lookup":
            primary_assets = _compress_assets(
                normalized_query,
                direct_asset_rows,
                runtime_config,
                question_type,
                document_spread=str(retrieval_plan.get("document_spread") or "few"),
            )
            visual_chunk_rows = self.repository.list_chunk_records_for_asset_pages(
                primary_assets,
                limit=max(runtime_config["max_context_chunks"] * 3, 18),
            )
            asset_page_lookup = {
                (str(asset.get("document_id") or ""), int(asset.get("page_number") or 0)): float(asset.get("_retrieval_score") or 0.0)
                for asset in primary_assets
                if str(asset.get("document_id") or "") and int(asset.get("page_number") or 0) > 0
            }
            for row in visual_chunk_rows:
                document_id = str(row.get("document_id") or "")
                page_start = int(row.get("page_start") or 0)
                page_end = int(row.get("page_end") or page_start or 0)
                overlap_scores = [
                    score
                    for (asset_document_id, asset_page), score in asset_page_lookup.items()
                    if asset_document_id == document_id and page_start <= asset_page <= page_end
                ]
                if overlap_scores:
                    row["_retrieval_score"] = max(float(row.get("_retrieval_score") or 0.0), max(overlap_scores) + 0.1)
            chunks = _compress_chunks(
                normalized_query,
                visual_chunk_rows,
                runtime_config,
                question_type,
                document_spread="few",
            )
            chunk_ids = [row["chunk_id"] for row in chunks]
            linked_assets = self.repository.list_page_assets_for_chunks(chunk_ids, limit=max(50, runtime_config["max_context_assets"] * 8))
            asset_by_id = {row["asset_id"]: row for row in primary_assets}
            for asset in linked_assets:
                asset_by_id.setdefault(asset["asset_id"], asset)
            ordered_assets = list(asset_by_id.values())
        else:
            linked_assets = self.repository.list_page_assets_for_chunks(chunk_ids, limit=max(50, runtime_config["max_context_assets"] * 8))
            ordered_assets = linked_assets

        asset_links = self.repository.list_chunk_asset_links_for_chunks(chunk_ids, limit=max(50, runtime_config["max_context_assets"] * 8))
        chunk_score_lookup = {
            str(row.get("chunk_id") or ""): float(row.get("_retrieval_score") or 0.0)
            for row in chunks
            if str(row.get("chunk_id") or "")
        }
        link_support_by_asset: dict[str, float] = {}
        link_details_by_asset: dict[str, list[dict[str, Any]]] = {}
        for link in asset_links:
            asset_id = str(link.get("asset_id") or "")
            chunk_id = str(link.get("chunk_id") or "")
            if not asset_id or not chunk_id:
                continue
            support = min(
                0.32,
                float(link.get("confidence") or 0.0) * max(0.1, chunk_score_lookup.get(chunk_id, 0.0) + 0.15),
            )
            link_support_by_asset[asset_id] = max(link_support_by_asset.get(asset_id, 0.0), support)
            link_details_by_asset.setdefault(asset_id, []).append(
                {
                    "chunk_id": chunk_id,
                    "link_type": str(link.get("link_type") or ""),
                    "confidence": float(link.get("confidence") or 0.0),
                }
            )
        chunk_id_set = {str(chunk_id) for chunk_id in chunk_ids}
        for asset_row in ordered_assets:
            asset_id = str(asset_row.get("asset_id") or "")
            if not asset_id:
                continue
            asset_row["_link_support_score"] = round(link_support_by_asset.get(asset_id, 0.0), 6)
            asset_row["_link_details"] = link_details_by_asset.get(asset_id, [])[:6]
        assets = _compress_assets(
            normalized_query,
            ordered_assets,
            runtime_config,
            question_type,
            document_spread=str(retrieval_plan.get("document_spread") or "few"),
        )
        asset_ids = [row["asset_id"] for row in assets]
        for asset_row in assets:
            metadata = dict(asset_row.get("metadata_json") or {})
            asset_id = str(asset_row.get("asset_id") or "")
            if asset_id in link_details_by_asset:
                metadata["linked_chunk_ids"] = [item["chunk_id"] for item in link_details_by_asset[asset_id][:6]]
                metadata["link_types"] = sorted({item["link_type"] for item in link_details_by_asset[asset_id] if item.get("link_type")})
                metadata["max_link_confidence"] = max(item["confidence"] for item in link_details_by_asset[asset_id])
                asset_row["metadata_json"] = metadata
        if question_type == "visual_lookup" and assets:
            aligned_chunks = _filter_chunks_for_asset_scope(chunks, assets, strict_pages=True)
            if not aligned_chunks:
                asset_scoped_chunk_rows = self.repository.list_chunk_records_for_asset_pages(
                    assets,
                    limit=max(runtime_config["max_context_chunks"] * 3, 18),
                )
                _boost_visual_chunk_rows_from_assets(asset_scoped_chunk_rows, assets)
                aligned_chunks = _compress_chunks(
                    normalized_query,
                    asset_scoped_chunk_rows,
                    runtime_config,
                    question_type,
                    document_spread="few",
                )
            if not aligned_chunks:
                aligned_chunks = _filter_chunks_for_asset_scope(chunks, assets, strict_pages=False)
            if not aligned_chunks:
                asset_document_ids = list(
                    dict.fromkeys(
                        str(asset.get("document_id") or "")
                        for asset in assets
                        if str(asset.get("document_id") or "")
                    )
                )
                document_scoped_rows: list[dict[str, Any]] = []
                for asset_document_id in asset_document_ids:
                    document_scoped_rows.extend(
                        self.repository.list_chunk_records(
                            document_id=asset_document_id,
                            status="accepted",
                            limit=max(runtime_config["max_context_chunks"] * 4, 16),
                            offset=0,
                        )
                    )
                aligned_chunks = _compress_chunks(
                    normalized_query,
                    document_scoped_rows,
                    runtime_config,
                    question_type,
                    document_spread="few",
                )
            if aligned_chunks:
                chunks = aligned_chunks
                chunk_ids = [row["chunk_id"] for row in chunks]
                chunk_id_set = {str(chunk_id) for chunk_id in chunk_ids}
                asset_links = self.repository.list_chunk_asset_links_for_chunks(
                    chunk_ids,
                    limit=max(50, runtime_config["max_context_assets"] * 8),
                )

        for match in filtered_asset_matches:
            asset_id = str(match["asset_id"])
            source_rows.append(
                {
                    "source_kind": "asset_hit",
                    "source_id": asset_id,
                    "document_id": match.get("metadata", {}).get("document_id"),
                    "chunk_id": None,
                    "asset_id": asset_id,
                    "rank": match.get("rank"),
                    "score": _distance_to_score(match.get("distance")) or float(match.get("rerank_score") or match.get("lexical_score") or 0.0),
                    "selected": asset_id in asset_ids,
                    "payload": {
                        "distance": match.get("distance"),
                        "metadata": match.get("metadata", {}),
                        "rerank_score": match.get("rerank_score"),
                        "match_source": match.get("match_source"),
                        "match_sources": list(match.get("match_sources") or []),
                    },
                }
            )
        for link in asset_links:
            asset_id = str(link["asset_id"])
            chunk_id = str(link.get("chunk_id") or "")
            if asset_id not in asset_ids:
                continue
            if chunk_id not in chunk_id_set:
                continue
            source_rows.append(
                {
                    "source_kind": "asset_link",
                    "source_id": asset_id,
                    "document_id": None,
                    "chunk_id": chunk_id,
                    "asset_id": asset_id,
                    "rank": None,
                    "score": float(link.get("confidence") or 0.0),
                    "selected": True,
                    "payload": link,
                }
            )

        sensor_rows: list[dict[str, Any]] = []
        if auth_user_id and include_sensor_context:
            sensor_rows = self.repository.build_user_sensor_context(
                tenant_id=tenant_id,
                auth_user_id=auth_user_id,
                normalized_query=normalized_query,
                max_rows=int(runtime_config.get("max_context_sensor_readings") or 0),
                hours=int(runtime_config.get("sensor_recent_hours") or 72),
                points_per_metric=int(runtime_config.get("sensor_points_per_metric") or 6),
            )
            for index, row in enumerate(sensor_rows, start=1):
                source_rows.append(
                    {
                        "source_kind": "sensor",
                        "source_id": row["sensor_row_id"],
                        "sensor_row_id": row["sensor_row_id"],
                        "sensor_id": row["sensor_id"],
                        "document_id": None,
                        "chunk_id": None,
                        "rank": index,
                        "score": float(row.get("_relevance_score") or 0.0),
                        "selected": True,
                        "payload": row,
                    }
                )

        per_chunk_assertion_limit = max(
            1,
            min(
                3,
                runtime_config["max_context_assertions"] // max(1, min(len(chunk_ids), runtime_config["max_context_chunks"])) or 1,
            ),
        )
        assertions = self.repository.list_kg_assertions_for_chunks(
            chunk_ids,
            limit=runtime_config["max_context_assertions"],
            per_chunk_limit=per_chunk_assertion_limit,
        )
        assertion_ids = [row["assertion_id"] for row in assertions]
        evidence = self.repository.list_kg_evidence_for_assertions(
            assertion_ids,
            limit=runtime_config["max_context_assertions"] * 2,
            per_assertion_limit=2,
        )
        entity_ids = sorted(
            {
                entity_id
                for row in assertions
                for entity_id in (row.get("subject_entity_id"), row.get("object_entity_id"))
                if entity_id
            }
        )
        entities = self.repository.list_kg_entities_by_ids(entity_ids)
        if retrieval_plan["kg_search"]:
            matched_entities = self.repository.search_kg_entities_for_query(
                normalized_query,
                tenant_id=tenant_id,
                document_ids=document_ids,
                limit=runtime_config["kg_search_limit"],
            )
            existing_entity_ids = {row["entity_id"] for row in entities}
            for entity in matched_entities:
                if entity["entity_id"] not in existing_entity_ids:
                    entities.append(entity)
                    existing_entity_ids.add(entity["entity_id"])
                source_rows.append(
                    {
                        "source_kind": "entity",
                        "source_id": entity["entity_id"],
                        "document_id": None,
                        "chunk_id": None,
                        "assertion_id": None,
                        "entity_id": entity["entity_id"],
                        "rank": None,
                        "score": None,
                        "selected": True,
                        "payload": entity,
                    }
                )

        graph_assertions: list[dict[str, Any]] = []
        graph_evidence: list[dict[str, Any]] = []
        graph_entities: list[dict[str, Any]] = []
        graph_chains: list[dict[str, Any]] = []
        if retrieval_plan.get("graph_expand"):
            graph_assertions, graph_evidence, graph_entities, graph_chains = self._expand_graph_context(
                normalized_query=normalized_query,
                tenant_id=tenant_id,
                question_type=question_type,
                runtime_config=runtime_config,
                retrieval_plan=retrieval_plan,
                document_ids=document_ids or sorted(selected_chunk_document_ids),
                chunks=chunks,
                assertions=assertions,
                evidence=evidence,
                entities=entities,
            )
            existing_assertion_ids = {str(row.get("assertion_id") or "") for row in assertions}
            for row in graph_assertions:
                assertion_id = str(row.get("assertion_id") or "")
                if assertion_id and assertion_id not in existing_assertion_ids:
                    assertions.append(row)
                    existing_assertion_ids.add(assertion_id)
            existing_evidence_ids = {str(row.get("evidence_id") or "") for row in evidence}
            for row in graph_evidence:
                evidence_id = str(row.get("evidence_id") or "")
                if evidence_id and evidence_id not in existing_evidence_ids:
                    evidence.append(row)
                    existing_evidence_ids.add(evidence_id)
            existing_entity_ids = {str(row.get("entity_id") or "") for row in entities}
            for row in graph_entities:
                entity_id = str(row.get("entity_id") or "")
                if entity_id and entity_id not in existing_entity_ids:
                    entities.append(row)
                    existing_entity_ids.add(entity_id)

        for index, assertion in enumerate(assertions, start=1):
            source_rows.append(
                {
                    "source_kind": "assertion",
                    "source_id": assertion["assertion_id"],
                    "document_id": assertion.get("document_id"),
                    "chunk_id": assertion.get("chunk_id"),
                    "assertion_id": assertion["assertion_id"],
                    "entity_id": None,
                    "rank": index,
                    "score": float(assertion.get("confidence") or 0.0),
                    "selected": True,
                    "payload": assertion,
                }
            )
        for index, entity in enumerate(entities, start=1):
            source_rows.append(
                {
                    "source_kind": "entity",
                    "source_id": entity["entity_id"],
                    "document_id": None,
                    "chunk_id": None,
                    "assertion_id": None,
                    "entity_id": entity["entity_id"],
                    "rank": index,
                    "score": None,
                    "selected": True,
                    "payload": entity,
                }
            )

        return AgentContextBundle(
            chunks=chunks,
            assets=assets,
            sensor_rows=sensor_rows,
            assertions=assertions,
            evidence=evidence,
            entities=entities,
            graph_chains=graph_chains,
            sources=source_rows,
        )

    def _expand_graph_context(
        self,
        *,
        normalized_query: str,
        tenant_id: str,
        question_type: str,
        runtime_config: dict[str, Any],
        retrieval_plan: dict[str, Any],
        document_ids: list[str] | None,
        chunks: list[dict[str, Any]],
        assertions: list[dict[str, Any]],
        evidence: list[dict[str, Any]],
        entities: list[dict[str, Any]],
    ) -> tuple[list[dict[str, Any]], list[dict[str, Any]], list[dict[str, Any]], list[dict[str, Any]]]:
        if not retrieval_plan.get("graph_expand"):
            return [], [], [], []
        graph_expansion_limit = int(runtime_config.get("graph_expansion_limit") or 0)
        per_entity_limit = int(runtime_config.get("graph_per_entity_limit") or 0)
        max_context_graph_chains = int(runtime_config.get("max_context_graph_chains") or 0)
        if graph_expansion_limit <= 0 or max_context_graph_chains <= 0:
            return [], [], [], []

        selected_document_ids = {
            str(row.get("document_id") or "")
            for row in chunks
            if str(row.get("document_id") or "")
        }
        scoped_document_ids = [
            str(document_id).strip()
            for document_id in (document_ids or sorted(selected_document_ids))
            if str(document_id).strip()
        ]
        seed_entity_ids = list(
            dict.fromkeys(
                [
                    str(entity_id).strip()
                    for row in assertions
                    for entity_id in (row.get("subject_entity_id"), row.get("object_entity_id"))
                    if str(entity_id or "").strip()
                ]
                + [
                    str(row.get("entity_id") or "").strip()
                    for row in entities
                    if str(row.get("entity_id") or "").strip()
                ]
            )
        )
        if not seed_entity_ids:
            return [], [], [], []
        seed_entity_ids = seed_entity_ids[: max(graph_expansion_limit * 2, int(runtime_config.get("kg_search_limit") or 0), 6)]
        existing_assertion_ids = {
            str(row.get("assertion_id") or "")
            for row in assertions
            if str(row.get("assertion_id") or "")
        }
        raw_neighbors = self.repository.list_kg_neighbor_assertions_for_entities(
            seed_entity_ids,
            tenant_id=tenant_id,
            document_ids=scoped_document_ids or None,
            exclude_assertion_ids=sorted(existing_assertion_ids),
            limit=max(graph_expansion_limit * 3, graph_expansion_limit + 4),
            per_entity_limit=per_entity_limit,
        )
        if not raw_neighbors:
            return [], [], [], []

        query_terms = set(_query_terms(normalized_query))
        existing_entity_map = {
            str(row.get("entity_id") or ""): dict(row)
            for row in entities
            if str(row.get("entity_id") or "")
        }
        collapsed_neighbors: dict[str, dict[str, Any]] = {}
        for row in raw_neighbors:
            assertion_id = str(row.get("assertion_id") or "")
            if not assertion_id:
                continue
            seed_entity_id = str(row.get("seed_entity_id") or "").strip()
            current = collapsed_neighbors.get(assertion_id)
            if current is None:
                item = dict(row)
                item["seed_entity_ids"] = [seed_entity_id] if seed_entity_id else []
                collapsed_neighbors[assertion_id] = item
                continue
            seed_entity_ids_for_row = list(current.get("seed_entity_ids") or [])
            if seed_entity_id and seed_entity_id not in seed_entity_ids_for_row:
                seed_entity_ids_for_row.append(seed_entity_id)
            current["seed_entity_ids"] = seed_entity_ids_for_row
            current["evidence_count"] = max(
                int(current.get("evidence_count") or 0),
                int(row.get("evidence_count") or 0),
            )

        missing_entity_ids = sorted(
            {
                str(entity_id).strip()
                for row in collapsed_neighbors.values()
                for entity_id in (
                    row.get("subject_entity_id"),
                    row.get("object_entity_id"),
                    row.get("neighbor_entity_id"),
                )
                if str(entity_id or "").strip() and str(entity_id).strip() not in existing_entity_map
            }
        )
        if missing_entity_ids:
            for row in self.repository.list_kg_entities_by_ids(missing_entity_ids):
                entity_id = str(row.get("entity_id") or "")
                if entity_id:
                    existing_entity_map[entity_id] = dict(row)

        graph_evidence_rows = self.repository.list_kg_evidence_for_assertions(
            list(collapsed_neighbors.keys()),
            limit=max(graph_expansion_limit * 3, max_context_graph_chains * 4),
            per_assertion_limit=2,
        )
        evidence_by_assertion: dict[str, list[dict[str, Any]]] = {}
        for row in [*evidence, *graph_evidence_rows]:
            assertion_id = str(row.get("assertion_id") or "")
            if not assertion_id:
                continue
            evidence_by_assertion.setdefault(assertion_id, [])
            if any(str(existing.get("evidence_id") or "") == str(row.get("evidence_id") or "") for existing in evidence_by_assertion[assertion_id]):
                continue
            evidence_by_assertion[assertion_id].append(dict(row))

        def _entity_name(entity_id: str | None) -> str:
            if not entity_id:
                return ""
            row = existing_entity_map.get(str(entity_id) or "")
            return str((row or {}).get("canonical_name") or entity_id or "")

        def _assertion_surface(row: dict[str, Any]) -> str:
            return " ".join(
                part
                for part in (
                    _entity_name(str(row.get("subject_entity_id") or "")),
                    str(row.get("predicate") or ""),
                    _entity_name(str(row.get("object_entity_id") or "")),
                    str(row.get("object_literal") or ""),
                )
                if part
            )

        scored_neighbors: list[dict[str, Any]] = []
        for row in collapsed_neighbors.values():
            lexical_score = _lexical_overlap(query_terms, _assertion_surface(row))
            predicate_bonus = _graph_predicate_relevance_bonus(
                question_type,
                str(row.get("predicate") or ""),
            )
            evidence_bonus = min(0.12, 0.04 * len(evidence_by_assertion.get(str(row.get("assertion_id") or ""), [])))
            seed_bonus = min(0.12, 0.04 * len(row.get("seed_entity_ids") or []))
            same_document_bonus = 0.08 if str(row.get("document_id") or "") in selected_document_ids else 0.0
            graph_score = round(
                float(row.get("confidence") or 0.0)
                + lexical_score * 0.35
                + predicate_bonus
                + evidence_bonus
                + seed_bonus
                + same_document_bonus,
                6,
            )
            item = dict(row)
            item["_graph_score"] = graph_score
            item["_graph_expanded"] = True
            scored_neighbors.append(item)
        scored_neighbors.sort(
            key=lambda row: (
                float(row.get("_graph_score") or 0.0),
                float(row.get("confidence") or 0.0),
                int(row.get("evidence_count") or 0),
            ),
            reverse=True,
        )
        selected_neighbor_assertions = scored_neighbors[:graph_expansion_limit]
        if not selected_neighbor_assertions:
            return [], [], [], []

        base_assertions_by_entity: dict[str, list[dict[str, Any]]] = {}
        for row in assertions:
            for entity_id in (row.get("subject_entity_id"), row.get("object_entity_id")):
                entity_id = str(entity_id or "").strip()
                if not entity_id:
                    continue
                base_assertions_by_entity.setdefault(entity_id, []).append(dict(row))

        graph_chains: list[dict[str, Any]] = []
        seen_chain_ids: set[str] = set()
        for neighbor in selected_neighbor_assertions:
            neighbor_assertion_id = str(neighbor.get("assertion_id") or "")
            seed_ids_for_neighbor = [str(item).strip() for item in (neighbor.get("seed_entity_ids") or []) if str(item).strip()]
            anchor_candidates: list[tuple[str | None, dict[str, Any] | None]] = []
            for seed_entity_id in seed_ids_for_neighbor:
                anchors = [
                    row
                    for row in base_assertions_by_entity.get(seed_entity_id, [])
                    if str(row.get("assertion_id") or "") != neighbor_assertion_id
                ]
                if anchors:
                    anchor_candidates.extend((seed_entity_id, row) for row in anchors[:2])
                else:
                    anchor_candidates.append((seed_entity_id, None))
            if not anchor_candidates:
                anchor_candidates.append((None, None))
            for shared_entity_id, anchor in anchor_candidates[:2]:
                step_rows = [item for item in (anchor, neighbor) if item is not None]
                assertion_ids = [str(item.get("assertion_id") or "") for item in step_rows if str(item.get("assertion_id") or "")]
                chain_id = "|".join(assertion_ids)
                if not chain_id or chain_id in seen_chain_ids:
                    continue
                seen_chain_ids.add(chain_id)
                step_payload: list[dict[str, Any]] = []
                supporting_assertion_ids: list[str] = []
                supporting_evidence_ids: list[str] = []
                for step in step_rows:
                    assertion_id = str(step.get("assertion_id") or "")
                    supporting_assertion_ids.append(assertion_id)
                    evidence_ids = [
                        str(item.get("evidence_id") or "")
                        for item in evidence_by_assertion.get(assertion_id, [])
                        if str(item.get("evidence_id") or "")
                    ]
                    supporting_evidence_ids.extend(evidence_ids)
                    step_payload.append(
                        {
                            "assertion_id": assertion_id,
                            "chunk_id": str(step.get("chunk_id") or ""),
                            "document_id": str(step.get("document_id") or ""),
                            "subject_entity_id": str(step.get("subject_entity_id") or ""),
                            "subject_name": _entity_name(str(step.get("subject_entity_id") or "")),
                            "predicate": str(step.get("predicate") or ""),
                            "object_entity_id": str(step.get("object_entity_id") or ""),
                            "object_name": _entity_name(str(step.get("object_entity_id") or "")),
                            "object_literal": str(step.get("object_literal") or ""),
                            "confidence": float(step.get("confidence") or 0.0),
                            "evidence_ids": evidence_ids[:2],
                        }
                    )
                chain_surface = " ".join(
                    part
                    for step in step_payload
                    for part in (
                        step.get("subject_name"),
                        step.get("predicate"),
                        step.get("object_name"),
                        step.get("object_literal"),
                    )
                    if part
                )
                chain_score = round(
                    sum(float(item.get("confidence") or 0.0) for item in step_payload)
                    + _lexical_overlap(query_terms, chain_surface) * 0.3
                    + min(0.1, 0.03 * len(set(supporting_evidence_ids)))
                    + (0.04 if shared_entity_id else 0.0)
                    - (0.06 * max(0, len(step_payload) - 1)),
                    6,
                )
                graph_chains.append(
                    {
                        "chain_id": chain_id,
                        "shared_entity_id": shared_entity_id,
                        "shared_entity_name": _entity_name(shared_entity_id),
                        "chain_score": chain_score,
                        "steps": step_payload,
                        "supporting_assertion_ids": supporting_assertion_ids,
                        "supporting_evidence_ids": list(dict.fromkeys(supporting_evidence_ids)),
                    }
                )
        graph_chains.sort(key=lambda row: float(row.get("chain_score") or 0.0), reverse=True)
        graph_chains = graph_chains[:max_context_graph_chains]

        selected_graph_entity_ids = sorted(
            {
                str(entity_id).strip()
                for row in selected_neighbor_assertions
                for entity_id in (
                    row.get("subject_entity_id"),
                    row.get("object_entity_id"),
                    row.get("neighbor_entity_id"),
                )
                if str(entity_id or "").strip()
            }
        )
        graph_entity_rows = []
        for entity_id in selected_graph_entity_ids:
            entity_row = existing_entity_map.get(entity_id)
            if not entity_row or any(str(existing.get("entity_id") or "") == entity_id for existing in entities):
                continue
            graph_entity_rows.append(
                {
                    **entity_row,
                    "_graph_expanded": True,
                }
            )
        graph_evidence = [
            {
                **row,
                "_graph_expanded": True,
            }
            for assertion in selected_neighbor_assertions
            for row in evidence_by_assertion.get(str(assertion.get("assertion_id") or ""), [])[:2]
        ]
        graph_assertions = selected_neighbor_assertions
        return graph_assertions, graph_evidence, graph_entity_rows, graph_chains

    def _build_prompt_bundle(
        self,
        question: str,
        normalized_query: str,
        prior_messages: list[dict[str, Any]],
        profile_summary: dict[str, Any] | None,
        session_memory: dict[str, Any] | None,
        bundle: AgentContextBundle,
        question_type: str,
        runtime_config: dict[str, Any],
        workspace_kind: str = "general",
    ) -> AgentPromptBundle:
        prior_context = _budget_prior_messages(prior_messages, runtime_config["history_char_budget"])
        profile_summary_payload = _budget_profile_summary(profile_summary, runtime_config["profile_char_budget"])
        session_summary = _budget_session_summary(session_memory, runtime_config["memory_char_budget"])
        pre_gated_memory_counts = {
            "profile_topics": len((profile_summary_payload or {}).get("recurring_topics") or []),
            "profile_goals": len((profile_summary_payload or {}).get("learning_goals") or []),
            "session_facts": len((session_summary or {}).get("stable_facts") or []),
            "open_threads": len((session_summary or {}).get("open_threads") or []),
            "resolved_threads": len((session_summary or {}).get("resolved_threads") or []),
        }
        profile_summary_payload = _filter_profile_summary_for_prompt(
            profile_summary_payload,
            question=question,
            normalized_query=normalized_query,
            workspace_kind=workspace_kind,
        )
        session_summary = _filter_session_summary_for_prompt(
            session_summary,
            question=question,
            normalized_query=normalized_query,
            bundle=bundle,
        )
        chunk_payload = _budget_chunk_payload(bundle.chunks, runtime_config["chunk_char_budget"])
        asset_payload = _budget_asset_payload(bundle.assets, runtime_config["asset_char_budget"])
        sensor_payload = _budget_sensor_payload(bundle.sensor_rows, runtime_config["sensor_char_budget"])
        assertion_payload = _budget_assertion_payload(bundle.assertions, runtime_config["assertion_char_budget"])
        entity_payload = _budget_entity_payload(bundle.entities, runtime_config["entity_char_budget"])
        graph_chain_payload = _budget_graph_chain_payload(
            getattr(bundle, "graph_chains", []) or [],
            runtime_config["graph_char_budget"],
        )
        evidence_payload = _budget_evidence_payload(bundle.evidence, runtime_config["evidence_char_budget"])
        prompt_bundle = AgentPromptBundle(
            prior_context=prior_context,
            profile_summary=profile_summary_payload,
            session_summary=session_summary,
            chunk_payload=chunk_payload,
            asset_payload=asset_payload,
            sensor_payload=sensor_payload,
            assertion_payload=assertion_payload,
            entity_payload=entity_payload,
            graph_chain_payload=graph_chain_payload,
            evidence_payload=evidence_payload,
            stats={},
        )
        original_counts = {
            "history_messages": len(prompt_bundle.prior_context),
            "profile_topics": len((prompt_bundle.profile_summary or {}).get("recurring_topics") or []),
            "session_facts": len((prompt_bundle.session_summary or {}).get("stable_facts") or []),
            "chunks": len(prompt_bundle.chunk_payload),
            "assets": len(prompt_bundle.asset_payload),
            "sensor_rows": len(prompt_bundle.sensor_payload),
            "assertions": len(prompt_bundle.assertion_payload),
            "entities": len(prompt_bundle.entity_payload),
            "graph_chains": len(prompt_bundle.graph_chain_payload),
            "evidence": len(prompt_bundle.evidence_payload),
        }
        prompt_bundle = _fit_prompt_bundle(question, normalized_query, question_type, prompt_bundle, runtime_config)
        prompt_bundle.stats = {
            "question": question,
            "normalized_query": normalized_query,
            "question_type": question_type,
            "budgets": {
                "prompt_chars": runtime_config["prompt_char_budget"],
                "history_chars": runtime_config["history_char_budget"],
                "profile_chars": runtime_config["profile_char_budget"],
                "memory_chars": runtime_config["memory_char_budget"],
                "chunk_chars": runtime_config["chunk_char_budget"],
                "asset_chars": runtime_config["asset_char_budget"],
                "sensor_chars": runtime_config["sensor_char_budget"],
                "assertion_chars": runtime_config["assertion_char_budget"],
                "entity_chars": runtime_config["entity_char_budget"],
                "graph_chars": runtime_config["graph_char_budget"],
                "evidence_chars": runtime_config["evidence_char_budget"],
            },
            "counts": {
                "history_messages": len(prompt_bundle.prior_context),
                "profile_topics": len((prompt_bundle.profile_summary or {}).get("recurring_topics") or []),
                "session_facts": len((prompt_bundle.session_summary or {}).get("stable_facts") or []),
                "chunks": len(prompt_bundle.chunk_payload),
                "assets": len(prompt_bundle.asset_payload),
                "sensor_rows": len(prompt_bundle.sensor_payload),
                "assertions": len(prompt_bundle.assertion_payload),
                "entities": len(prompt_bundle.entity_payload),
                "graph_chains": len(prompt_bundle.graph_chain_payload),
                "evidence": len(prompt_bundle.evidence_payload),
            },
            "trimmed": {
                "history_messages": max(0, original_counts["history_messages"] - len(prompt_bundle.prior_context)),
                "profile_topics": max(0, original_counts["profile_topics"] - len((prompt_bundle.profile_summary or {}).get("recurring_topics") or [])),
                "session_facts": max(0, original_counts["session_facts"] - len((prompt_bundle.session_summary or {}).get("stable_facts") or [])),
                "chunks": max(0, original_counts["chunks"] - len(prompt_bundle.chunk_payload)),
                "assets": max(0, original_counts["assets"] - len(prompt_bundle.asset_payload)),
                "sensor_rows": max(0, original_counts["sensor_rows"] - len(prompt_bundle.sensor_payload)),
                "assertions": max(0, original_counts["assertions"] - len(prompt_bundle.assertion_payload)),
                "entities": max(0, original_counts["entities"] - len(prompt_bundle.entity_payload)),
                "graph_chains": max(0, original_counts["graph_chains"] - len(prompt_bundle.graph_chain_payload)),
                "evidence": max(0, original_counts["evidence"] - len(prompt_bundle.evidence_payload)),
            },
            "memory_relevance": {
                "profile_topics": max(0, pre_gated_memory_counts["profile_topics"] - len((prompt_bundle.profile_summary or {}).get("recurring_topics") or [])),
                "profile_goals": max(0, pre_gated_memory_counts["profile_goals"] - len((prompt_bundle.profile_summary or {}).get("learning_goals") or [])),
                "session_facts": max(0, pre_gated_memory_counts["session_facts"] - len((prompt_bundle.session_summary or {}).get("stable_facts") or [])),
                "open_threads": max(0, pre_gated_memory_counts["open_threads"] - len((prompt_bundle.session_summary or {}).get("open_threads") or [])),
                "resolved_threads": max(0, pre_gated_memory_counts["resolved_threads"] - len((prompt_bundle.session_summary or {}).get("resolved_threads") or [])),
            },
            "estimated_chars": {
                "history": _estimate_json_chars(prompt_bundle.prior_context),
                "profile_summary": _estimate_json_chars(prompt_bundle.profile_summary or {}),
                "session_summary": _estimate_json_chars(prompt_bundle.session_summary or {}),
                "chunks": _estimate_json_chars(prompt_bundle.chunk_payload),
                "assets": _estimate_json_chars(prompt_bundle.asset_payload),
                "sensor_rows": _estimate_json_chars(prompt_bundle.sensor_payload),
                "assertions": _estimate_json_chars(prompt_bundle.assertion_payload),
                "entities": _estimate_json_chars(prompt_bundle.entity_payload),
                "graph_chains": _estimate_json_chars(prompt_bundle.graph_chain_payload),
                "evidence": _estimate_json_chars(prompt_bundle.evidence_payload),
                "total_prompt": _estimate_prompt_chars(question, normalized_query, question_type, prompt_bundle, runtime_config),
            },
            "final_ids": {
                "chunk_ids": [item["chunk_id"] for item in prompt_bundle.chunk_payload],
                "asset_ids": [item["asset_id"] for item in prompt_bundle.asset_payload],
                "sensor_row_ids": [item["sensor_row_id"] for item in prompt_bundle.sensor_payload],
                "assertion_ids": [item["assertion_id"] for item in prompt_bundle.assertion_payload],
                "entity_ids": [item["entity_id"] for item in prompt_bundle.entity_payload],
                "graph_chain_ids": [item["chain_id"] for item in prompt_bundle.graph_chain_payload],
                "evidence_ids": [item["evidence_id"] for item in prompt_bundle.evidence_payload],
            },
            "profile_used": bool(prompt_bundle.profile_summary),
            "session_memory_used": bool(prompt_bundle.session_summary),
        }
        return prompt_bundle

    def _generate_answer(
        self,
        question: str,
        normalized_query: str,
        prompt_bundle: AgentPromptBundle,
        question_type: str,
        runtime_config: dict[str, Any],
    ) -> dict[str, Any]:
        # The answer call is schema-constrained so the finalizer can enforce citations and provenance.
        provider = _resolve_provider(runtime_config)
        if provider != "openai":
            return {
                "provider": provider,
                "model": "disabled",
                "prompt_version": runtime_config["prompt_version"],
                "content": json.dumps(
                    {
                        "answer": "The answer model is not configured. Retrieved evidence is available, but synthesis is disabled.",
                        "confidence": 0.0,
                        "abstained": True,
                        "abstain_reason": "agent_provider_disabled",
                        "used_chunk_ids": [],
                        "used_asset_ids": [],
                        "used_sensor_row_ids": [],
                        "used_evidence_ids": [],
                        "supporting_assertions": [],
                        "supporting_entities": [],
                    }
                ),
                "prompt_payload": {
                    "runtime_config": runtime_config,
                    "question": question,
                    "normalized_query": normalized_query,
                    "question_type": question_type,
                    "context_budget": prompt_bundle.stats,
                },
                "raw_payload": {},
            }

        api_key = runtime_config.get("api_key_override") or settings.agent_api_key
        if not api_key:
            raise AgentQueryError("AGENT_API_KEY is required for agent answers")

        system_prompt = runtime_config["system_prompt"]
        user_prompt = _build_user_prompt(question, normalized_query, question_type, prompt_bundle)
        payload = _build_chat_payload(system_prompt, user_prompt, prompt_bundle, runtime_config)
        while _estimate_request_chars(payload) > runtime_config["prompt_char_budget"]:
            if not _shrink_prompt_bundle_once(prompt_bundle):
                break
            prompt_bundle.stats["request_trimmed"] = True
            user_prompt = _build_user_prompt(question, normalized_query, question_type, prompt_bundle)
            payload = _build_chat_payload(system_prompt, user_prompt, prompt_bundle, runtime_config)
        if _estimate_request_chars(payload) > runtime_config["prompt_char_budget"]:
            raise AgentQueryError("Prompt budget exceeded after trimming")
        prompt_bundle.stats["counts"] = {
            "history_messages": len(prompt_bundle.prior_context),
            "profile_topics": len((prompt_bundle.profile_summary or {}).get("recurring_topics") or []),
            "session_facts": len((prompt_bundle.session_summary or {}).get("stable_facts") or []),
            "chunks": len(prompt_bundle.chunk_payload),
            "assets": len(prompt_bundle.asset_payload),
            "sensor_rows": len(prompt_bundle.sensor_payload),
            "assertions": len(prompt_bundle.assertion_payload),
            "entities": len(prompt_bundle.entity_payload),
            "graph_chains": len(prompt_bundle.graph_chain_payload),
            "evidence": len(prompt_bundle.evidence_payload),
        }
        prompt_bundle.stats.setdefault("estimated_chars", {})
        prompt_bundle.stats["estimated_chars"]["history"] = _estimate_json_chars(prompt_bundle.prior_context)
        prompt_bundle.stats["estimated_chars"]["profile_summary"] = _estimate_json_chars(prompt_bundle.profile_summary or {})
        prompt_bundle.stats["estimated_chars"]["session_summary"] = _estimate_json_chars(prompt_bundle.session_summary or {})
        prompt_bundle.stats["estimated_chars"]["chunks"] = _estimate_json_chars(prompt_bundle.chunk_payload)
        prompt_bundle.stats["estimated_chars"]["assets"] = _estimate_json_chars(prompt_bundle.asset_payload)
        prompt_bundle.stats["estimated_chars"]["sensor_rows"] = _estimate_json_chars(prompt_bundle.sensor_payload)
        prompt_bundle.stats["estimated_chars"]["assertions"] = _estimate_json_chars(prompt_bundle.assertion_payload)
        prompt_bundle.stats["estimated_chars"]["entities"] = _estimate_json_chars(prompt_bundle.entity_payload)
        prompt_bundle.stats["estimated_chars"]["graph_chains"] = _estimate_json_chars(prompt_bundle.graph_chain_payload)
        prompt_bundle.stats["estimated_chars"]["evidence"] = _estimate_json_chars(prompt_bundle.evidence_payload)
        prompt_bundle.stats["profile_used"] = bool(prompt_bundle.profile_summary)
        prompt_bundle.stats["session_memory_used"] = bool(prompt_bundle.session_summary)
        prompt_bundle.stats["final_ids"] = {
            "chunk_ids": [item["chunk_id"] for item in prompt_bundle.chunk_payload],
            "asset_ids": [item["asset_id"] for item in prompt_bundle.asset_payload],
            "sensor_row_ids": [item["sensor_row_id"] for item in prompt_bundle.sensor_payload],
            "assertion_ids": [item["assertion_id"] for item in prompt_bundle.assertion_payload],
            "entity_ids": [item["entity_id"] for item in prompt_bundle.entity_payload],
            "graph_chain_ids": [item["chain_id"] for item in prompt_bundle.graph_chain_payload],
            "evidence_ids": [item["evidence_id"] for item in prompt_bundle.evidence_payload],
        }
        prompt_bundle.stats["estimated_chars"]["total_prompt"] = _estimate_prompt_chars(
            question,
            normalized_query,
            question_type,
            prompt_bundle,
            runtime_config,
        )
        prompt_bundle.stats["estimated_chars"]["total_request"] = _estimate_request_chars(payload)
        headers = {
            "Authorization": f"Bearer {api_key}",
            "Content-Type": "application/json",
        }
        base_url = (runtime_config["base_url"] or settings.kg_base_url or settings.embedding_base_url).rstrip("/")

        try:
            with httpx.Client(timeout=runtime_config["timeout_seconds"]) as client:
                response = client.post(
                    f"{base_url}/chat/completions",
                    headers=headers,
                    json=payload,
                )
                response.raise_for_status()
                body = response.json()
        except httpx.HTTPStatusError as exc:
            error_text = exc.response.text.strip()
            if error_text:
                raise AgentQueryError(f"Agent answer request failed: {exc} | body={error_text}") from exc
            raise AgentQueryError(f"Agent answer request failed: {exc}") from exc
        except httpx.HTTPError as exc:
            raise AgentQueryError(f"Agent answer request failed: {exc}") from exc

        content = _extract_message_content(body)
        if not str(content or "").strip():
            recovery_payload = _build_plaintext_recovery_payload(
                payload,
                instruction=(
                    "Structured output failed. Reply in plain text only. "
                    "Do not return JSON, markdown fences, or an empty response."
                ),
            )
            try:
                content, body, used_model = _recover_nonempty_text_content(
                    base_url=base_url,
                    api_key=str(api_key),
                    timeout_seconds=float(runtime_config["timeout_seconds"]),
                    primary_payload=recovery_payload,
                    fallback_model=str(runtime_config.get("fallback_model") or ""),
                    fallback_reasoning_effort=str(runtime_config.get("fallback_reasoning_effort") or "none"),
                )
            except Exception as exc:
                raise AgentQueryError(f"Agent answer recovery request failed: {exc}") from exc
            runtime_model = used_model or runtime_config["model"]
        else:
            runtime_model = runtime_config["model"]
        return {
            "provider": "openai",
            "model": runtime_model,
            "prompt_version": runtime_config["prompt_version"],
            "content": content,
            "prompt_payload": {
                "request": payload,
                "runtime_config": runtime_config,
                "context_budget": prompt_bundle.stats,
            },
            "raw_payload": body,
        }

    def _generate_open_world_answer(
        self,
        question: str,
        normalized_query: str,
        question_type: str,
        bundle: AgentContextBundle,
        runtime_config: dict[str, Any],
    ) -> dict[str, Any]:
        provider = _resolve_provider(runtime_config)
        if provider != "openai":
            raise AgentQueryError("Open-world fallback is not configured")
        api_key = runtime_config.get("api_key_override") or settings.agent_api_key
        if not api_key:
            raise AgentQueryError("AGENT_API_KEY is required for open-world fallback answers")
        model = str(runtime_config.get("model") or "").strip()
        if not model:
            raise AgentQueryError("Open-world fallback model is not configured")
        user_payload = _build_open_world_user_payload(
            question=question,
            normalized_query=normalized_query,
            question_type=question_type,
            bundle=bundle,
        )
        payload = _build_model_json_payload(
            model=model,
            reasoning_effort=str(runtime_config.get("reasoning_effort") or ""),
            system_prompt=str(runtime_config.get("open_world_system_prompt") or runtime_config.get("system_prompt") or "").strip(),
            user_payload=user_payload,
            schema_name="agent_query_result",
            schema=_agent_response_schema(),
            temperature=float(runtime_config.get("open_world_temperature") or runtime_config.get("temperature") or 0.0),
            max_completion_tokens=int(runtime_config.get("open_world_max_completion_tokens") or runtime_config.get("max_completion_tokens") or 1200),
        )
        base_url = (runtime_config["base_url"] or settings.kg_base_url or settings.embedding_base_url).rstrip("/")
        try:
            body = _call_openai_json_payload(
                base_url=base_url,
                api_key=str(api_key),
                payload=payload,
                timeout_seconds=float(runtime_config.get("open_world_timeout_seconds") or runtime_config.get("timeout_seconds") or 60.0),
            )
        except Exception as exc:
            raise AgentQueryError(f"Open-world fallback request failed: {exc}") from exc
        content = _extract_message_content(body)
        if not str(content or "").strip():
            recovery_payload = _build_plaintext_recovery_payload(
                payload,
                instruction=(
                    "Structured output failed. Reply in plain text only. "
                    "Do not return JSON, markdown fences, or an empty response."
                ),
            )
            try:
                content, body, used_model = _recover_nonempty_text_content(
                    base_url=base_url,
                    api_key=str(api_key),
                    timeout_seconds=float(runtime_config.get("open_world_timeout_seconds") or runtime_config.get("timeout_seconds") or 60.0),
                    primary_payload=recovery_payload,
                    fallback_model=str(runtime_config.get("fallback_model") or ""),
                    fallback_reasoning_effort=str(runtime_config.get("fallback_reasoning_effort") or "none"),
                )
            except Exception as exc:
                raise AgentQueryError(f"Open-world recovery request failed: {exc}") from exc
            model = used_model or model
        return {
            "provider": "openai",
            "model": model,
            "prompt_version": str(runtime_config.get("open_world_prompt_version") or "v1"),
            "content": content,
            "prompt_payload": {
                "fallback_mode": "open_world",
                "question": question,
                "normalized_query": normalized_query,
                "question_type": question_type,
                "context_hints": user_payload.get("context_hints") or {},
            },
            "raw_payload": body,
        }

    def _run_open_world_fallback(
        self,
        *,
        question: str,
        normalized_query: str,
        question_type: str,
        bundle: AgentContextBundle,
        runtime_config: dict[str, Any],
    ) -> tuple[dict[str, Any], dict[str, Any]]:
        artifact = self._generate_open_world_answer(
            question=question,
            normalized_query=normalized_query,
            question_type=question_type,
            bundle=bundle,
            runtime_config=runtime_config,
        )
        response = _coerce_agent_response(artifact["content"], bundle=bundle)
        if str(response.get("answer") or "").strip():
            response["abstained"] = False
            response["abstain_reason"] = None
        open_world_runtime_config = dict(runtime_config)
        open_world_runtime_config["min_answer_confidence"] = 0.0
        response = self._finalize_response(bundle, response, normalized_query, question_type, open_world_runtime_config)
        if bool(response.get("abstained")) or not str(response.get("answer") or "").strip():
            response = self._build_last_resort_response(
                question=question,
                question_type=question_type,
                bundle=bundle,
                reason=str(response.get("abstain_reason") or "open_world_abstained"),
                runtime_config=runtime_config,
            )
        response["grounding_check"] = {
            **dict(response.get("grounding_check") or {}),
            "open_world_answer_used": True,
        }
        return artifact, response

    def _finalize_response(
        self,
        bundle: AgentContextBundle,
        response: dict[str, Any],
        normalized_query: str,
        question_type: str,
        runtime_config: dict[str, Any],
    ) -> dict[str, Any]:
        # Filter model output back down to evidence we actually retrieved before anything is shown or persisted.
        chunk_map = {row["chunk_id"]: row for row in bundle.chunks}
        asset_map = {row["asset_id"]: row for row in bundle.assets}
        sensor_row_map = {str(row["sensor_row_id"]): row for row in bundle.sensor_rows if str(row.get("sensor_row_id") or "")}
        assertion_map = {row["assertion_id"]: row for row in bundle.assertions}
        evidence_map = {row["evidence_id"]: row for row in bundle.evidence if str(row.get("evidence_id") or "")}
        assertion_ids = set(assertion_map)
        entity_ids = {row["entity_id"] for row in bundle.entities}

        supporting_assertions = [
            item for item in (response.get("supporting_assertions") or []) if item in assertion_ids
        ]
        supporting_entities = [
            item for item in (response.get("supporting_entities") or []) if item in entity_ids
        ]
        used_chunk_ids = [
            chunk_id
            for chunk_id in (response.get("used_chunk_ids") or [])
            if chunk_id in chunk_map
        ]
        used_asset_ids = [
            asset_id
            for asset_id in (response.get("used_asset_ids") or [])
            if asset_id in asset_map
        ]
        used_sensor_row_ids = [
            sensor_row_id
            for sensor_row_id in (response.get("used_sensor_row_ids") or [])
            if sensor_row_id in sensor_row_map
        ]
        used_evidence_ids = [
            evidence_id
            for evidence_id in (response.get("used_evidence_ids") or [])
            if evidence_id in evidence_map
        ]
        if not supporting_assertions and used_evidence_ids:
            evidence_assertion_ids = [
                row.get("assertion_id")
                for evidence_id, row in evidence_map.items()
                if evidence_id in used_evidence_ids and row.get("assertion_id") in assertion_map
            ]
            supporting_assertions = [assertion_id for assertion_id in evidence_assertion_ids if assertion_id]
        if not used_chunk_ids:
            assertion_chunk_ids = [
                row.get("chunk_id")
                for row in assertion_map.values()
                if row["assertion_id"] in supporting_assertions and row.get("chunk_id") in chunk_map
            ]
            used_chunk_ids = [chunk_id for chunk_id in assertion_chunk_ids if chunk_id]
        if not used_asset_ids and used_chunk_ids:
            linked_asset_ids = [
                item["asset_id"]
                for item in bundle.sources
                if item.get("source_kind") == "asset_link"
                and bool(item.get("selected"))
                and item.get("chunk_id") in used_chunk_ids
                and item.get("asset_id") in asset_map
            ]
            used_asset_ids = list(dict.fromkeys(linked_asset_ids))
        if not used_sensor_row_ids and not used_chunk_ids and not used_asset_ids and len(sensor_row_map) == 1:
            used_sensor_row_ids = list(sensor_row_map.keys())
        citations = _build_backend_citations(
            normalized_query,
            chunk_map,
            used_chunk_ids,
            asset_map,
            used_asset_ids,
            sensor_row_map,
            used_sensor_row_ids,
            assertion_map,
            evidence_map,
            used_evidence_ids,
            runtime_config["citation_excerpt_chars"],
        )

        answer = _clean_user_facing_answer_text(str(response.get("answer") or "").strip())
        confidence = float(response.get("confidence") or 0.0)
        model_abstained = bool(response.get("abstained", False))
        abstained = model_abstained
        abstain_reason: str | None = None
        grounding_check = _verify_answer_grounding(
            answer=answer,
            question_type=question_type,
            normalized_query=normalized_query,
            chunk_map=chunk_map,
            used_chunk_ids=used_chunk_ids,
            asset_map=asset_map,
            used_asset_ids=used_asset_ids,
            sensor_row_map=sensor_row_map,
            used_sensor_row_ids=used_sensor_row_ids,
            assertion_map=assertion_map,
            evidence_map=evidence_map,
            used_evidence_ids=used_evidence_ids,
            runtime_config=runtime_config,
        )
        if (
            not grounding_check["passed"]
            and str(grounding_check.get("method") or "").strip() in {"lexical_fallback", "claim_verifier"}
            and question_type not in {"source_lookup", "visual_lookup"}
        ):
            supported_subset = _build_supported_subset_answer(
                grounding_check=grounding_check,
                question_type=question_type,
            )
            if supported_subset is not None:
                answer, supported_evidence_ids = supported_subset
                confidence = min(confidence, 0.72)
                if supported_evidence_ids:
                    used_evidence_ids = [
                        evidence_id
                        for evidence_id in used_evidence_ids
                        if evidence_id in supported_evidence_ids
                    ] or used_evidence_ids
                grounding_check = {
                    **grounding_check,
                    "original_method": grounding_check.get("method"),
                    "method": "supported_subset",
                    "passed": True,
                    "supported_subset_used": True,
                }
                citations = _build_backend_citations(
                    normalized_query,
                    chunk_map,
                    used_chunk_ids,
                    asset_map,
                    used_asset_ids,
                    sensor_row_map,
                    used_sensor_row_ids,
                    assertion_map,
                    evidence_map,
                    used_evidence_ids,
                    runtime_config["citation_excerpt_chars"],
                )

        allow_open_world_fallback = bool(answer) and confidence >= runtime_config["min_answer_confidence"]
        if not citations and not allow_open_world_fallback:
            abstained = True
            abstain_reason = "no_valid_citations"
        elif confidence < runtime_config["min_answer_confidence"]:
            abstained = True
            abstain_reason = "low_confidence"
        elif not answer:
            abstained = True
            abstain_reason = "empty_answer"
        elif model_abstained:
            abstained = True
            abstain_reason = "model_abstained"
        elif not grounding_check["passed"]:
            if allow_open_world_fallback:
                grounding_check = {
                    **grounding_check,
                    "open_world_fallback_used": True,
                }
            else:
                abstained = True
                abstain_reason = "weak_grounding"
        if abstained:
            abstain_reason = _normalize_reason_code(abstain_reason, fallback="model_abstained")
            answer = "I can't give you a solid answer on that."

        return {
            "answer": answer,
            "confidence": round(confidence, 4),
            "abstained": abstained,
            "abstain_reason": abstain_reason,
            "citations": citations,
            "used_chunk_ids": used_chunk_ids,
            "used_asset_ids": used_asset_ids,
            "used_sensor_row_ids": used_sensor_row_ids,
            "used_evidence_ids": used_evidence_ids,
            "supporting_assertions": supporting_assertions,
            "supporting_entities": supporting_entities,
            "grounding_check": grounding_check,
        }

    def _build_fallback_response(self, bundle: AgentContextBundle, reason: str, runtime_config: dict[str, Any]) -> dict[str, Any]:
        # This response stays conservative on purpose: show the strongest excerpts we
        # already trust instead of attempting free-form synthesis without the model.
        citations: list[dict[str, Any]] = []
        summary_lines: list[str] = []
        for chunk in bundle.chunks[: min(3, len(bundle.chunks))]:
            text = re.sub(r"\s+", " ", str(chunk.get("text") or "")).strip()
            excerpt = _extract_citation_excerpt(text, "", runtime_config["citation_excerpt_chars"])
            citations.append(
                {
                    "citation_kind": "chunk",
                    "chunk_id": chunk["chunk_id"],
                    "document_id": str(chunk["document_id"]),
                    "page_start": chunk.get("page_start"),
                    "page_end": chunk.get("page_end"),
                    "section_title": chunk.get("metadata_json", {}).get("section_title"),
                    "quote": excerpt,
                }
            )
            label = _clean_fallback_label(str(chunk.get("metadata_json", {}).get("section_title") or chunk["chunk_id"]))
            excerpt = _strip_repeated_fallback_prefix(label, excerpt)
            summary_lines.append(f"- {label}: {excerpt}")
        if not citations:
            for asset in bundle.assets[: min(2, len(bundle.assets))]:
                trusted_text = _trusted_asset_grounding_text(asset)
                if not trusted_text:
                    continue
                excerpt = _extract_citation_excerpt(trusted_text, "", runtime_config["citation_excerpt_chars"])
                citations.append(
                    {
                        "citation_kind": "asset",
                        "asset_id": asset["asset_id"],
                        "document_id": str(asset["document_id"]),
                        "page_number": asset.get("page_number"),
                        "asset_type": asset.get("asset_type"),
                        "quote": excerpt,
                        "image_url": f"/agent/assets/{asset['asset_id']}/image",
                    }
                )
                label = _clean_fallback_label(str((asset.get("metadata_json") or {}).get("label") or f"asset {asset.get('page_number')}"))
                excerpt = _strip_repeated_fallback_prefix(label, excerpt)
                summary_lines.append(f"- {label}: {excerpt}")
        if not citations:
            for sensor_row in bundle.sensor_rows[: min(3, len(bundle.sensor_rows))]:
                trusted_text = _trusted_sensor_grounding_text(sensor_row)
                if not trusted_text:
                    continue
                excerpt = _extract_citation_excerpt(trusted_text, "", runtime_config["citation_excerpt_chars"])
                citations.append(
                    {
                        "citation_kind": "sensor",
                        "sensor_row_id": sensor_row["sensor_row_id"],
                        "sensor_id": sensor_row.get("sensor_id"),
                        "sensor_name": sensor_row.get("sensor_name"),
                        "place_id": sensor_row.get("place_id"),
                        "place_name": sensor_row.get("place_name"),
                        "external_place_id": sensor_row.get("external_place_id"),
                        "hive_id": sensor_row.get("hive_id"),
                        "external_hive_id": sensor_row.get("external_hive_id"),
                        "metric_name": sensor_row.get("metric_name"),
                        "latest_observed_at": sensor_row.get("latest_observed_at"),
                        "window_start_at": sensor_row.get("window_start_at"),
                        "window_end_at": sensor_row.get("window_end_at"),
                        "reading_ids": list(sensor_row.get("reading_ids") or [])[:8],
                        "quote": excerpt,
                    }
                )
                label = _clean_fallback_label(
                    f"sensor {sensor_row.get('sensor_name')} {sensor_row.get('metric_name')}"
                )
                excerpt = _strip_repeated_fallback_prefix(label, excerpt)
                summary_lines.append(f"- {label}: {excerpt}")
        answer = "The answer model failed. Here are the strongest retrieved excerpts:\n" + "\n".join(summary_lines)
        return {
            "answer": answer.strip(),
            "confidence": 0.0,
            "abstained": True,
            "abstain_reason": "agent_generation_error",
            "citations": citations,
            "used_chunk_ids": [item["chunk_id"] for item in citations if item.get("chunk_id")],
            "used_asset_ids": [item["asset_id"] for item in citations if item.get("asset_id")],
            "used_sensor_row_ids": [item["sensor_row_id"] for item in citations if item.get("sensor_row_id")],
            "used_evidence_ids": [item["evidence_id"] for item in citations if item.get("evidence_id")],
            "supporting_assertions": [row["assertion_id"] for row in bundle.assertions[:3]],
            "supporting_entities": [row["entity_id"] for row in bundle.entities[:3]],
        }

    def _build_last_resort_response(
        self,
        *,
        question: str,
        question_type: str,
        bundle: AgentContextBundle,
        reason: str,
        runtime_config: dict[str, Any],
    ) -> dict[str, Any]:
        fallback = self._build_fallback_response(bundle, reason, runtime_config)
        citations = list(fallback.get("citations") or [])
        if citations:
            answer = _compose_extractive_fallback_answer(question, question_type, citations)
            if not answer:
                answer = str(fallback.get("answer") or "").strip()
                answer = answer.replace(
                    "The answer model failed. Here are the strongest retrieved excerpts:\n",
                    "Here is the most useful material I have right now:\n",
                ).strip()
            confidence = 0.42
        else:
            cleaned_question = re.sub(r"\s+", " ", question).strip().rstrip("?")
            if question_type in {"procedure", "explanation", "comparison"}:
                answer = (
                    f"Here is the best practical answer I can give right now about {cleaned_question}: "
                    "start with the clearest signal, compare it against the recent baseline, "
                    "and make decisions on sustained changes rather than one isolated reading."
                )
            else:
                answer = (
                    f"Here is the best direct answer I can give right now about {cleaned_question}: "
                    "use the simplest explanation that matches the signs you actually have, "
                    "and verify it against a second signal before you act."
                )
            confidence = 0.28
        return {
            "answer": _clean_user_facing_answer_text(answer),
            "confidence": confidence,
            "abstained": False,
            "abstain_reason": None,
            "citations": citations,
            "used_chunk_ids": list(fallback.get("used_chunk_ids") or []),
            "used_asset_ids": list(fallback.get("used_asset_ids") or []),
            "used_sensor_row_ids": list(fallback.get("used_sensor_row_ids") or []),
            "used_evidence_ids": list(fallback.get("used_evidence_ids") or []),
            "supporting_assertions": list(fallback.get("supporting_assertions") or []),
            "supporting_entities": list(fallback.get("supporting_entities") or []),
            "grounding_check": {
                "passed": True,
                "method": "last_resort_fallback",
                "last_resort_fallback_used": True,
                "reason": reason,
            },
        }


def _build_backend_citations(
    normalized_query: str,
    chunk_map: dict[str, dict[str, Any]],
    used_chunk_ids: list[str],
    asset_map: dict[str, dict[str, Any]],
    used_asset_ids: list[str],
    sensor_row_map: dict[str, dict[str, Any]],
    used_sensor_row_ids: list[str],
    assertion_map: dict[str, dict[str, Any]],
    evidence_map: dict[str, dict[str, Any]],
    used_evidence_ids: list[str],
    max_excerpt: int,
) -> list[dict[str, Any]]:
    citations: list[dict[str, Any]] = []
    seen: set[str] = set()
    for chunk_id in used_chunk_ids:
        if chunk_id in seen or chunk_id not in chunk_map:
            continue
        seen.add(chunk_id)
        chunk = chunk_map[chunk_id]
        citations.append(
            {
                "citation_kind": "chunk",
                "chunk_id": chunk_id,
                "document_id": str(chunk["document_id"]),
                "page_start": chunk.get("page_start"),
                "page_end": chunk.get("page_end"),
                "section_title": chunk.get("metadata_json", {}).get("section_title"),
                "quote": _extract_citation_excerpt(str(chunk.get("text") or ""), normalized_query, max_excerpt),
            }
        )
    for asset_id in used_asset_ids:
        if asset_id in seen or asset_id not in asset_map:
            continue
        seen.add(asset_id)
        asset = asset_map[asset_id]
        trusted_text = _trusted_asset_grounding_text(asset)
        if not trusted_text:
            continue
        citations.append(
            {
                "citation_kind": "asset",
                "asset_id": asset_id,
                "document_id": str(asset["document_id"]),
                "page_number": asset.get("page_number"),
                "asset_type": asset.get("asset_type"),
                "label": str((asset.get("metadata_json") or {}).get("label") or ""),
                "quote": _extract_citation_excerpt(trusted_text, normalized_query, max_excerpt),
                "image_url": f"/agent/assets/{asset_id}/image",
            }
        )
    for sensor_row_id in used_sensor_row_ids:
        if sensor_row_id in seen or sensor_row_id not in sensor_row_map:
            continue
        seen.add(sensor_row_id)
        sensor_row = sensor_row_map[sensor_row_id]
        trusted_text = _trusted_sensor_grounding_text(sensor_row)
        if not trusted_text:
            continue
        citations.append(
            {
                "citation_kind": "sensor",
                "sensor_row_id": sensor_row_id,
                "sensor_id": sensor_row.get("sensor_id"),
                "sensor_name": sensor_row.get("sensor_name"),
                "sensor_type": sensor_row.get("sensor_type"),
                "place_id": sensor_row.get("place_id"),
                "place_name": sensor_row.get("place_name"),
                "external_place_id": sensor_row.get("external_place_id"),
                "hive_id": sensor_row.get("hive_id"),
                "external_hive_id": sensor_row.get("external_hive_id"),
                "hive_name": sensor_row.get("hive_name"),
                "metric_name": sensor_row.get("metric_name"),
                "unit": sensor_row.get("unit"),
                "latest_observed_at": sensor_row.get("latest_observed_at"),
                "window_start_at": sensor_row.get("window_start_at"),
                "window_end_at": sensor_row.get("window_end_at"),
                "reading_ids": list(sensor_row.get("reading_ids") or [])[:8],
                "quote": _extract_citation_excerpt(trusted_text, normalized_query, max_excerpt),
            }
        )
    for evidence_id in used_evidence_ids:
        if evidence_id in seen or evidence_id not in evidence_map:
            continue
        seen.add(evidence_id)
        evidence = evidence_map[evidence_id]
        assertion = assertion_map.get(str(evidence.get("assertion_id") or ""))
        excerpt = str(evidence.get("excerpt") or "").strip()
        if not excerpt:
            continue
        citations.append(
            {
                "citation_kind": "kg_evidence",
                "evidence_id": evidence_id,
                "assertion_id": evidence.get("assertion_id"),
                "document_id": str((assertion or {}).get("document_id") or ""),
                "chunk_id": (assertion or {}).get("chunk_id"),
                "quote": _extract_citation_excerpt(excerpt, normalized_query, max_excerpt),
            }
        )
    return citations


def _is_generic_asset_label(label: str) -> bool:
    normalized = sanitize_text(label).strip().lower()
    if not normalized:
        return True
    return bool(
        re.fullmatch(r"page\s+\d+\s+(image|asset\s+\d+)", normalized)
        or re.fullmatch(r"asset\s+\d+", normalized)
    )


def _trusted_asset_grounding_text(asset: dict[str, Any]) -> str:
    metadata = asset.get("metadata_json") or {}
    label = sanitize_text(str(metadata.get("label") or "")).strip()
    label = "" if _is_generic_asset_label(label) else label
    ocr_text = sanitize_text(str(asset.get("ocr_text") or "")).strip()
    return sanitize_text("\n".join(part for part in [label, ocr_text] if part)).strip()


def _trusted_sensor_grounding_text(sensor_row: dict[str, Any]) -> str:
    summary_text = sanitize_text(str(sensor_row.get("summary_text") or "")).strip()
    if summary_text:
        return summary_text
    parts = [
        str(sensor_row.get("sensor_name") or "").strip(),
        str(sensor_row.get("sensor_type") or "").strip(),
        str(sensor_row.get("place_name") or "").strip(),
        str(sensor_row.get("external_place_id") or "").strip(),
        str(sensor_row.get("hive_id") or "").strip(),
        str(sensor_row.get("external_hive_id") or "").strip(),
        str(sensor_row.get("hive_name") or "").strip(),
        str(sensor_row.get("location_label") or "").strip(),
        str(sensor_row.get("metric_name") or "").strip(),
        f"latest {sensor_row.get('latest_value')}" if sensor_row.get("latest_value") not in (None, "") else "",
        str(sensor_row.get("unit") or "").strip(),
        str(sensor_row.get("latest_observed_at") or "").strip(),
    ]
    return sanitize_text(" | ".join(part for part in parts if part)).strip()


def _sensor_grounding_series_text(sensor_row: dict[str, Any]) -> str:
    trusted_text = _trusted_sensor_grounding_text(sensor_row)
    points = []
    for point in list(sensor_row.get("recent_points") or [])[:8]:
        observed_at = str(point.get("observed_at") or "").strip()
        value = point.get("value")
        if value in (None, ""):
            continue
        points.append(f"{observed_at}={value}")
    if not points:
        return trusted_text
    series_text = "recent_points: " + " | ".join(points)
    return sanitize_text("\n".join(part for part in [trusted_text, series_text] if part)).strip()


def _extract_citation_excerpt(text: str, normalized_query: str, max_excerpt: int) -> str:
    cleaned = re.sub(r"\s+", " ", text).strip()
    if len(cleaned) <= max_excerpt:
        return cleaned
    query_terms = [term for term in _query_terms(normalized_query) if len(term) >= 4]
    lowered = cleaned.lower()
    for term in query_terms:
        position = lowered.find(term.lower())
        if position != -1:
            start = max(0, position - max_excerpt // 3)
            end = min(len(cleaned), start + max_excerpt)
            snippet = cleaned[start:end].strip()
            if start > 0:
                snippet = "..." + snippet
            if end < len(cleaned):
                snippet = snippet + "..."
            return snippet
    return cleaned[:max_excerpt].rstrip() + "..."


def _clean_fallback_label(label: str) -> str:
    cleaned = sanitize_text(label).strip()
    cleaned = re.sub(r"\s+", " ", cleaned)
    return cleaned.strip(" :;-")[:120] or "excerpt"


def _strip_repeated_fallback_prefix(label: str, excerpt: str) -> str:
    cleaned_label = sanitize_text(label).strip(" :;-")
    cleaned_excerpt = sanitize_text(excerpt).strip()
    if not cleaned_label or not cleaned_excerpt:
        return cleaned_excerpt
    lowered_label = cleaned_label.lower()
    lowered_excerpt = cleaned_excerpt.lower()
    if lowered_excerpt.startswith(lowered_label):
        trimmed = cleaned_excerpt[len(cleaned_label):].lstrip(" :;,-.)(")
        if trimmed:
            return trimmed
    return cleaned_excerpt


def _compose_extractive_fallback_answer(
    question: str,
    question_type: str,
    citations: list[dict[str, Any]],
) -> str:
    normalized_query = _normalize_query(question).lower()
    keywords = {token for token in re.split(r"[^a-z0-9]+", normalized_query) if len(token) >= 4}
    scored_sentences: list[tuple[int, str]] = []
    seen: set[str] = set()
    for citation in citations[:4]:
        quote = sanitize_text(str(citation.get("quote") or "")).strip()
        if not quote:
            continue
        sentences = [sentence.strip() for sentence in re.split(r"(?<=[\.\!\?])\s+", quote) if sentence.strip()]
        for index, sentence in enumerate(sentences[:4]):
            cleaned = re.sub(r"\s+", " ", sentence).strip(" :;,-")
            if len(cleaned) < 24:
                continue
            fingerprint = cleaned.lower()
            if fingerprint in seen:
                continue
            seen.add(fingerprint)
            score = 0
            if index == 0:
                score += 2
            score += sum(1 for token in keywords if token in fingerprint)
            scored_sentences.append((score, cleaned))
    scored_sentences.sort(key=lambda item: (-item[0], len(item[1])))
    selected = [sentence for _, sentence in scored_sentences[:3]]
    if not selected:
        return ""
    if question_type in {"definition", "fact", "explanation", "comparison", "procedure"}:
        return " ".join(selected).strip()
    return " ".join(selected[:2]).strip()


def _mark_selected_sources_for_prompt(sources: list[dict[str, Any]], prompt_bundle: AgentPromptBundle) -> list[dict[str, Any]]:
    selected_chunk_ids = {item["chunk_id"] for item in prompt_bundle.chunk_payload}
    selected_asset_ids = {item["asset_id"] for item in prompt_bundle.asset_payload}
    selected_sensor_row_ids = {item["sensor_row_id"] for item in prompt_bundle.sensor_payload}
    selected_assertion_ids = {item["assertion_id"] for item in prompt_bundle.assertion_payload}
    selected_entity_ids = {item["entity_id"] for item in prompt_bundle.entity_payload}
    selected_evidence_ids = {item["evidence_id"] for item in prompt_bundle.evidence_payload}
    finalized: list[dict[str, Any]] = []
    for source in sources:
        item = dict(source)
        kind = str(item.get("source_kind") or "")
        selected = False
        if kind == "chunk":
            selected = str(item.get("chunk_id") or "") in selected_chunk_ids
        elif kind == "asset_hit":
            selected = str(item.get("asset_id") or "") in selected_asset_ids
        elif kind == "asset_link":
            selected = (
                str(item.get("chunk_id") or "") in selected_chunk_ids
                and str(item.get("asset_id") or "") in selected_asset_ids
            )
        elif kind == "sensor":
            selected = str(item.get("sensor_row_id") or item.get("source_id") or "") in selected_sensor_row_ids
        elif kind == "assertion":
            selected = str(item.get("assertion_id") or "") in selected_assertion_ids
        elif kind == "entity":
            selected = str(item.get("entity_id") or "") in selected_entity_ids
        elif kind == "evidence":
            selected = str(item.get("evidence_id") or item.get("source_id") or "") in selected_evidence_ids
        item["selected"] = selected
        finalized.append(item)
    return finalized


def _trim_context_bundle_to_prompt(bundle: AgentContextBundle, prompt_bundle: AgentPromptBundle) -> AgentContextBundle:
    selected_chunk_ids = {item["chunk_id"] for item in prompt_bundle.chunk_payload}
    selected_asset_ids = {item["asset_id"] for item in prompt_bundle.asset_payload}
    selected_sensor_row_ids = {item["sensor_row_id"] for item in prompt_bundle.sensor_payload}
    selected_assertion_ids = {item["assertion_id"] for item in prompt_bundle.assertion_payload}
    selected_entity_ids = {item["entity_id"] for item in prompt_bundle.entity_payload}
    selected_graph_chain_ids = {item["chain_id"] for item in prompt_bundle.graph_chain_payload}
    selected_evidence_ids = {item["evidence_id"] for item in prompt_bundle.evidence_payload}
    final_sources = _mark_selected_sources_for_prompt(bundle.sources, prompt_bundle)
    existing_chunk_ids = {
        str(item.get("chunk_id") or "")
        for item in final_sources
        if item.get("source_kind") == "chunk" and str(item.get("chunk_id") or "")
    }
    existing_asset_ids = {
        str(item.get("asset_id") or item.get("source_id") or "")
        for item in final_sources
        if item.get("source_kind") == "asset_hit" and str(item.get("asset_id") or item.get("source_id") or "")
    }
    existing_sensor_row_ids = {
        str(item.get("sensor_row_id") or item.get("source_id") or "")
        for item in final_sources
        if item.get("source_kind") == "sensor" and str(item.get("sensor_row_id") or item.get("source_id") or "")
    }
    existing_evidence_ids = {
        str(item.get("evidence_id") or item.get("source_id") or "")
        for item in final_sources
        if item.get("source_kind") == "evidence" and str(item.get("evidence_id") or item.get("source_id") or "")
    }
    for row in bundle.chunks:
        chunk_id = row["chunk_id"]
        if chunk_id not in selected_chunk_ids or chunk_id in existing_chunk_ids:
            continue
        final_sources.append(
            {
                "source_kind": "chunk",
                "source_id": chunk_id,
                "document_id": row.get("document_id"),
                "chunk_id": chunk_id,
                "rank": None,
                "score": float(row.get("_retrieval_score") or 0.0),
                "selected": True,
                "payload": {
                    "derived_from": "final_context_bundle",
                    "page_start": row.get("page_start"),
                    "page_end": row.get("page_end"),
                },
            }
        )
    for row in bundle.assets:
        asset_id = row["asset_id"]
        if asset_id not in selected_asset_ids or asset_id in existing_asset_ids:
            continue
        final_sources.append(
            {
                "source_kind": "asset_hit",
                "source_id": asset_id,
                "document_id": row.get("document_id"),
                "chunk_id": None,
                "asset_id": asset_id,
                "rank": None,
                "score": float(row.get("_retrieval_score") or 0.0),
                "selected": True,
                "payload": {
                    "derived_from": "final_context_bundle",
                    "page_number": row.get("page_number"),
                    "asset_type": row.get("asset_type"),
                },
            }
        )
    for row in bundle.sensor_rows:
        sensor_row_id = str(row.get("sensor_row_id") or "")
        if not sensor_row_id or sensor_row_id not in selected_sensor_row_ids or sensor_row_id in existing_sensor_row_ids:
            continue
        final_sources.append(
            {
                "source_kind": "sensor",
                "source_id": sensor_row_id,
                "document_id": None,
                "chunk_id": None,
                "sensor_row_id": sensor_row_id,
                "sensor_id": row.get("sensor_id"),
                "rank": None,
                "score": float(row.get("_relevance_score") or 0.0),
                "selected": True,
                "payload": {
                    "derived_from": "final_context_bundle",
                    "metric_name": row.get("metric_name"),
                    "latest_observed_at": row.get("latest_observed_at"),
                },
            }
        )
    for row in bundle.evidence:
        evidence_id = str(row.get("evidence_id") or "")
        if not evidence_id or evidence_id not in selected_evidence_ids or evidence_id in existing_evidence_ids:
            continue
        final_sources.append(
            {
                "source_kind": "evidence",
                "source_id": evidence_id,
                "document_id": None,
                "chunk_id": None,
                "evidence_id": evidence_id,
                "rank": None,
                "score": None,
                "selected": True,
                "payload": {
                    "derived_from": "final_context_bundle",
                    "assertion_id": row.get("assertion_id"),
                },
            }
        )
    return AgentContextBundle(
        chunks=[row for row in bundle.chunks if row["chunk_id"] in selected_chunk_ids],
        assets=[row for row in bundle.assets if row["asset_id"] in selected_asset_ids],
        sensor_rows=[row for row in bundle.sensor_rows if str(row.get("sensor_row_id") or "") in selected_sensor_row_ids],
        assertions=[row for row in bundle.assertions if row["assertion_id"] in selected_assertion_ids],
        graph_chains=[row for row in bundle.graph_chains if str(row.get("chain_id") or "") in selected_graph_chain_ids],
        evidence=[
            row
            for row in bundle.evidence
            if str(row.get("evidence_id") or "") in selected_evidence_ids
            or row["assertion_id"] in selected_assertion_ids
        ],
        entities=[row for row in bundle.entities if row["entity_id"] in selected_entity_ids],
        sources=final_sources,
    )


_GROUNDING_STOPWORDS = {
    "about", "after", "also", "among", "because", "been", "being", "between",
    "could", "does", "doing", "each", "from", "have", "into", "more", "most",
    "only", "other", "over", "same", "than", "that", "their", "them", "then",
    "there", "these", "they", "this", "those", "through", "under", "very",
    "what", "when", "where", "which", "while", "with", "would", "your",
}


def _grounding_terms(text: str) -> set[str]:
    return {
        token
        for token in [
            raw.strip(".,;:()[]{}")
            for raw in re.findall(r"[a-z0-9%:/\.-]{3,}", sanitize_text(text).lower())
        ]
        if token and token not in _GROUNDING_STOPWORDS
    }


def _split_grounding_claims(answer: str, *, limit: int = 8) -> list[str]:
    cleaned = sanitize_text(answer).strip()
    if not cleaned:
        return []
    parts = [
        part.strip()
        for part in re.split(r"(?<=[\.\!\?;])\s+|\n+", cleaned)
        if part.strip()
    ]
    claims = [part[:320] for part in parts if len(_grounding_terms(part)) >= 2 or any(char.isdigit() for char in part)]
    if not claims and cleaned:
        claims = [cleaned[:320]]
    return claims[:limit]


def _contains_numeric_claim(claim: str) -> bool:
    return any(char.isdigit() for char in sanitize_text(claim))


def _claim_evidence_support(
    claim: str,
    normalized_query: str,
    evidence_rows: list[dict[str, Any]],
) -> tuple[bool, list[str]]:
    claim_terms = _grounding_terms(claim)
    if not claim_terms:
        return False, []
    query_terms = _grounding_terms(normalized_query)
    novel_terms = sorted(claim_terms - query_terms)
    numeric_terms = {term for term in claim_terms if any(char.isdigit() for char in term)}
    evidence_ids: list[str] = []
    supported_novel: set[str] = set()
    supported_numeric: set[str] = set()
    broad_overlap_hits = 0
    for row in evidence_rows:
        row_terms = _grounding_terms(str(row.get("text") or ""))
        if not row_terms:
            continue
        overlap = claim_terms & row_terms
        if overlap:
            broad_overlap_hits += 1
        novel_overlap = set(novel_terms) & row_terms
        numeric_overlap = numeric_terms & row_terms
        if novel_overlap or numeric_overlap:
            evidence_ids.append(str(row.get("id") or ""))
        supported_novel |= novel_overlap
        supported_numeric |= numeric_overlap
    if numeric_terms and supported_numeric != numeric_terms:
        return False, list(dict.fromkeys(item for item in evidence_ids if item))
    if not novel_terms:
        return broad_overlap_hits > 0, list(dict.fromkeys(item for item in evidence_ids if item))
    required_ratio = 1.0 if len(novel_terms) <= 2 else 0.6
    passed = (len(supported_novel) / max(1, len(novel_terms))) >= required_ratio
    return passed, list(dict.fromkeys(item for item in evidence_ids if item))


def _lexical_grounding_check(
    *,
    answer: str,
    question_type: str,
    normalized_query: str,
    evidence_rows: list[dict[str, Any]],
    runtime_config: dict[str, Any],
) -> dict[str, Any]:
    claims = _split_grounding_claims(answer)
    if not claims:
        return {
            "method": "lexical_fallback",
            "passed": False,
            "supported_ratio": 0.0,
            "unsupported_claims": [],
            "claims": [],
        }
    claim_rows: list[dict[str, Any]] = []
    unsupported_claims: list[str] = []
    supported_claim_count = 0
    for claim in claims:
        supported, evidence_ids = _claim_evidence_support(claim, normalized_query, evidence_rows)
        if supported:
            supported_claim_count += 1
        else:
            unsupported_claims.append(claim)
        claim_rows.append(
            {
                "claim": claim,
                "supported": supported,
                "evidence_ids": evidence_ids[:4],
            }
        )
    supported_ratio = round(supported_claim_count / max(1, len(claim_rows)), 4)
    configured_min_supported_ratio = float(runtime_config.get("claim_verifier_min_supported_ratio") or 0.66)
    relaxed_question_types = {"explanation", "procedure", "comparison"}
    if len(claim_rows) <= 2:
        min_supported_ratio = 1.0
    elif question_type in relaxed_question_types:
        min_supported_ratio = configured_min_supported_ratio
    else:
        min_supported_ratio = max(0.75, configured_min_supported_ratio)
    unsupported_numeric_claims = [
        claim
        for claim in unsupported_claims
        if _contains_numeric_claim(claim)
    ]
    unsupported_non_numeric_claims = [
        claim
        for claim in unsupported_claims
        if claim not in unsupported_numeric_claims
    ]
    if unsupported_numeric_claims:
        unsupported_ok = False
    elif question_type in relaxed_question_types and len(claim_rows) >= 3:
        unsupported_ok = len(unsupported_non_numeric_claims) <= 1
    else:
        unsupported_ok = not unsupported_claims
    passed = supported_ratio >= min_supported_ratio and unsupported_ok
    return {
        "method": "lexical_fallback",
        "passed": passed,
        "supported_ratio": supported_ratio,
        "unsupported_claims": unsupported_claims[:6],
        "claims": claim_rows[:8],
    }


def _build_supported_subset_answer(
    *,
    grounding_check: dict[str, Any],
    question_type: str,
) -> tuple[str, list[str]] | None:
    if question_type in {"source_lookup", "visual_lookup"}:
        return None
    claims = [item for item in (grounding_check.get("claims") or []) if isinstance(item, dict)]
    supported_claims = [
        str(item.get("claim") or "").strip()
        for item in claims
        if item.get("supported") and str(item.get("claim") or "").strip()
    ]
    if not supported_claims:
        return None
    unsupported_claims = [
        str(item).strip()
        for item in (grounding_check.get("unsupported_claims") or [])
        if str(item).strip()
    ]
    if any(_contains_numeric_claim(claim) for claim in unsupported_claims):
        return None
    supported_ratio = float(grounding_check.get("supported_ratio") or 0.0)
    if supported_ratio < 0.25:
        return None
    evidence_ids = list(
        dict.fromkeys(
            evidence_id
            for item in claims
            if item.get("supported")
            for evidence_id in [str(value).strip() for value in (item.get("evidence_ids") or []) if str(value).strip()]
        )
    )
    supported_text = " ".join(supported_claims[:3]).strip()
    if not supported_text:
        return None
    supported_text = _clean_supported_subset_claim_text(supported_text)
    answer = supported_text
    return answer.strip(), evidence_ids


def _clean_supported_subset_claim_text(text: str) -> str:
    cleaned = str(text or "").strip()
    cleaned = re.sub(
        r"^(based on (the )?(provided )?(sources|indexed corpus|retrieved evidence)\s*:\s*)",
        "",
        cleaned,
        flags=re.IGNORECASE,
    )
    cleaned = re.sub(
        r"^(according to (the )?(provided )?(sources|indexed corpus|retrieved evidence)\s*[:,]?\s*)",
        "",
        cleaned,
        flags=re.IGNORECASE,
    )
    cleaned = re.sub(
        r"^(from (the )?(provided )?(sources|indexed corpus|retrieved evidence)\s*[:,]?\s*)",
        "",
        cleaned,
        flags=re.IGNORECASE,
    )
    cleaned = re.sub(r"\s+", " ", cleaned).strip()
    return cleaned


def _clean_user_facing_answer_text(text: str) -> str:
    cleaned = _clean_supported_subset_claim_text(text)
    cleaned = re.sub(
        r"^(i (think|believe|guess|would say)\s+)",
        "",
        cleaned,
        flags=re.IGNORECASE,
    )
    cleaned = re.sub(
        r"^(i('m| am) not sure(,? but)?\s+)",
        "",
        cleaned,
        flags=re.IGNORECASE,
    )
    cleaned = re.sub(
        r"^(it (seems|looks like|appears)\s+)",
        "",
        cleaned,
        flags=re.IGNORECASE,
    )
    cleaned = re.sub(
        r"^(here'?s (what|the short version|the simple version)\s*[:\-]\s*)",
        "",
        cleaned,
        flags=re.IGNORECASE,
    )
    cleaned = re.sub(
        r"^(in plain terms\s*[:\-]\s*)",
        "",
        cleaned,
        flags=re.IGNORECASE,
    )
    cleaned = re.sub(r"\s+", " ", cleaned).strip()
    return cleaned


def _verify_answer_grounding(
    *,
    answer: str,
    question_type: str,
    normalized_query: str,
    chunk_map: dict[str, dict[str, Any]],
    used_chunk_ids: list[str],
    asset_map: dict[str, dict[str, Any]],
    used_asset_ids: list[str],
    sensor_row_map: dict[str, dict[str, Any]],
    used_sensor_row_ids: list[str],
    assertion_map: dict[str, dict[str, Any]],
    evidence_map: dict[str, dict[str, Any]],
    used_evidence_ids: list[str],
    runtime_config: dict[str, Any],
) -> dict[str, Any]:
    if not answer.strip():
        return {
            "method": "empty_answer",
            "passed": False,
            "supported_ratio": 0.0,
            "unsupported_claims": [],
            "claims": [],
        }
    evidence_rows = _grounding_evidence_rows(
        chunk_map,
        used_chunk_ids,
        asset_map,
        used_asset_ids,
        sensor_row_map,
        used_sensor_row_ids,
        assertion_map,
        evidence_map,
        used_evidence_ids,
    )
    lexical_result: dict[str, Any] | None = None
    if runtime_config.get("claim_verifier_enabled", True):
        verifier_result = _verify_answer_claims_with_model(
            answer=answer,
            normalized_query=normalized_query,
            evidence_rows=evidence_rows,
            runtime_config=runtime_config,
        )
        if verifier_result is not None:
            if str(verifier_result.get("method") or "").strip() == "claim_verifier_error":
                lexical_result = _lexical_grounding_check(
                    answer=answer,
                    question_type=question_type,
                    normalized_query=normalized_query,
                    evidence_rows=evidence_rows,
                    runtime_config=runtime_config,
                )
                lexical_result["verifier_fallback_reason"] = "claim_verifier_error"
                lexical_result["verifier_error"] = {
                    "provider": verifier_result.get("provider"),
                    "model": verifier_result.get("model"),
                    "unsupported_claims": list(verifier_result.get("unsupported_claims") or [])[:6],
                }
                return lexical_result
            return verifier_result
    lexical_result = _lexical_grounding_check(
        answer=answer,
        question_type=question_type,
        normalized_query=normalized_query,
        evidence_rows=evidence_rows,
        runtime_config=runtime_config,
    )
    return lexical_result


def _grounding_evidence_rows(
    chunk_map: dict[str, dict[str, Any]],
    used_chunk_ids: list[str],
    asset_map: dict[str, dict[str, Any]],
    used_asset_ids: list[str],
    sensor_row_map: dict[str, dict[str, Any]],
    used_sensor_row_ids: list[str],
    assertion_map: dict[str, dict[str, Any]],
    evidence_map: dict[str, dict[str, Any]],
    used_evidence_ids: list[str],
) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for chunk_id in used_chunk_ids:
        chunk = chunk_map.get(chunk_id)
        if not chunk:
            continue
        rows.append(
            {
                "kind": "chunk",
                "id": chunk_id,
                "document_id": str(chunk.get("document_id") or ""),
                "page_start": chunk.get("page_start"),
                "page_end": chunk.get("page_end"),
                "text": str(chunk.get("text") or "")[:1600],
            }
        )
    for asset_id in used_asset_ids:
        asset = asset_map.get(asset_id)
        if not asset:
            continue
        trusted_text = _trusted_asset_grounding_text(asset)
        if not trusted_text:
            continue
        rows.append(
            {
                "kind": "asset",
                "id": asset_id,
                "document_id": str(asset.get("document_id") or ""),
                "page_number": asset.get("page_number"),
                "text": trusted_text[:1600],
            }
        )
    for sensor_row_id in used_sensor_row_ids:
        sensor_row = sensor_row_map.get(sensor_row_id)
        if not sensor_row:
            continue
        trusted_text = _sensor_grounding_series_text(sensor_row)
        if not trusted_text:
            continue
        rows.append(
            {
                "kind": "sensor",
                "id": sensor_row_id,
                "sensor_id": sensor_row.get("sensor_id"),
                "metric_name": sensor_row.get("metric_name"),
                "text": trusted_text[:1600],
            }
        )
    for evidence_id in used_evidence_ids:
        evidence = evidence_map.get(evidence_id)
        if not evidence:
            continue
        excerpt = str(evidence.get("excerpt") or "").strip()
        if not excerpt:
            continue
        assertion = assertion_map.get(str(evidence.get("assertion_id") or ""))
        rows.append(
            {
                "kind": "kg_evidence",
                "id": evidence_id,
                "assertion_id": evidence.get("assertion_id"),
                "document_id": str((assertion or {}).get("document_id") or ""),
                "chunk_id": (assertion or {}).get("chunk_id"),
                "text": excerpt[:1600],
            }
        )
    return rows


def _verify_answer_claims_with_model(
    *,
    answer: str,
    normalized_query: str,
    evidence_rows: list[dict[str, Any]],
    runtime_config: dict[str, Any],
) -> dict[str, Any] | None:
    provider = str(runtime_config.get("claim_verifier_provider") or runtime_config.get("provider") or "").strip().lower()
    if provider not in {"", "auto", "openai"}:
        return None
    api_key = runtime_config.get("api_key_override") or settings.agent_api_key
    if not api_key or not evidence_rows:
        return None
    model = str(runtime_config.get("claim_verifier_model") or "").strip()
    if not model:
        return None
    payload = _build_model_json_payload(
        model=model,
        reasoning_effort=str(runtime_config.get("claim_verifier_reasoning_effort") or "none"),
        system_prompt=str(runtime_config.get("claim_verifier_system_prompt") or "").strip(),
        user_payload={
            "question": normalized_query,
            "answer": answer,
            "evidence_rows": evidence_rows,
        },
        schema_name="claim_verifier_result",
        schema=_claim_verifier_schema(),
        temperature=float(runtime_config.get("claim_verifier_temperature") or 0.0),
        max_completion_tokens=int(runtime_config.get("claim_verifier_max_completion_tokens") or 500),
    )
    base_url = str(runtime_config.get("claim_verifier_base_url") or runtime_config.get("base_url") or settings.agent_base_url).rstrip("/")
    try:
        body = _call_openai_json_payload(
            base_url=base_url,
            api_key=str(api_key),
            payload=payload,
            timeout_seconds=float(runtime_config.get("claim_verifier_timeout_seconds") or 25.0),
        )
        content = _extract_message_content(body)
        data = json.loads(content)
        expected_claims = _split_grounding_claims(answer)
        allowed_evidence_ids = {str(item.get("id") or "") for item in evidence_rows if str(item.get("id") or "")}
        claims: list[dict[str, Any]] = []
        unsupported_claims: list[str] = [str(item).strip() for item in (data.get("unsupported_claims") or []) if str(item).strip()]
        supported_claim_count = 0
        for item in (data.get("claims") or []):
            if not isinstance(item, dict):
                continue
            claim_text = str(item.get("claim") or "").strip()
            evidence_ids = [
                evidence_id
                for evidence_id in [str(value).strip() for value in (item.get("evidence_ids") or []) if str(value).strip()]
                if evidence_id in allowed_evidence_ids
            ]
            support_basis = str(item.get("support_basis") or "").strip().lower()
            evidence_supported = bool(item.get("supported")) and bool(evidence_ids)
            world_knowledge_supported = (
                bool(item.get("supported"))
                and support_basis == "world_knowledge"
                and not evidence_ids
                and not _contains_numeric_claim(claim_text)
            )
            supported = evidence_supported or world_knowledge_supported
            if supported:
                supported_claim_count += 1
            elif claim_text and claim_text not in unsupported_claims:
                unsupported_claims.append(claim_text)
            claims.append(
                {
                    "claim": claim_text,
                    "supported": supported,
                    "evidence_ids": evidence_ids,
                    "support_basis": "evidence" if evidence_supported else ("world_knowledge" if world_knowledge_supported else "unsupported"),
                }
            )
        observed_claim_count = len([item for item in claims if str(item.get("claim") or "").strip()])
        expected_claim_count = max(1, len(expected_claims))
        claim_coverage_ratio = round(observed_claim_count / max(1, expected_claim_count), 4)
        supported_ratio = round(supported_claim_count / max(1, len(claims)), 4) if claims else 0.0
        min_supported_ratio = float(runtime_config.get("claim_verifier_min_supported_ratio") or 0.66)
        min_claim_coverage = 1.0 if expected_claim_count <= 2 else 0.75
        passed = (
            bool(data.get("verdict", False))
            and bool(claims)
            and supported_claim_count >= 1
            and supported_ratio >= min_supported_ratio
            and claim_coverage_ratio >= min_claim_coverage
            and not unsupported_claims
        )
        return {
            "method": "claim_verifier",
            "passed": passed,
            "supported_ratio": supported_ratio,
            "unsupported_claims": unsupported_claims[:6],
            "claims": claims[:8],
            "world_knowledge_claims": [
                item["claim"]
                for item in claims
                if str(item.get("support_basis") or "") == "world_knowledge"
            ][:6],
            "expected_claim_count": expected_claim_count,
            "observed_claim_count": observed_claim_count,
            "claim_coverage_ratio": claim_coverage_ratio,
            "provider": provider or "openai",
            "model": model,
        }
    except Exception:
        return {
            "method": "claim_verifier_error",
            "passed": False,
            "supported_ratio": 0.0,
            "unsupported_claims": ["claim_verifier_request_failed"],
            "claims": [],
            "provider": provider or "openai",
            "model": model,
        }


def _claim_verifier_schema() -> dict[str, Any]:
    return {
        "type": "object",
        "additionalProperties": False,
        "properties": {
            "claims": {
                "type": "array",
                "items": {
                    "type": "object",
                    "additionalProperties": False,
                    "properties": {
                        "claim": {"type": "string"},
                        "supported": {"type": "boolean"},
                        "evidence_ids": {"type": "array", "items": {"type": "string"}},
                        "support_basis": {"type": "string", "enum": ["evidence", "world_knowledge", "unsupported"]},
                    },
                    "required": ["claim", "supported", "evidence_ids", "support_basis"],
                },
            },
            "unsupported_claims": {"type": "array", "items": {"type": "string"}},
            "supported_ratio": {"type": "number"},
            "verdict": {"type": "boolean"},
        },
        "required": ["claims", "unsupported_claims", "supported_ratio", "verdict"],
    }


def _budget_prior_messages(messages: list[dict[str, Any]], char_budget: int) -> list[dict[str, Any]]:
    kept: list[dict[str, Any]] = []
    used_chars = 0
    for item in reversed(messages):
        role = str(item.get("role") or "")
        content = str(item.get("content") or "")
        if role not in {"user", "assistant"} or not content:
            continue
        candidate = {"role": role, "content": content}
        size = len(json.dumps(candidate, ensure_ascii=False))
        if kept and used_chars + size > char_budget:
            continue
        if not kept and size > char_budget:
            max_content = max(40, char_budget - 120)
            candidate["content"] = content[-max_content:]
            size = len(json.dumps(candidate, ensure_ascii=False))
            if size > char_budget:
                candidate["content"] = candidate["content"][-max(16, char_budget // 2):]
                size = len(json.dumps(candidate, ensure_ascii=False))
        if used_chars + size > char_budget and kept:
            continue
        kept.append(candidate)
        used_chars += size
    kept.reverse()
    return kept


def _budget_chunk_payload(chunks: list[dict[str, Any]], char_budget: int) -> list[dict[str, Any]]:
    return _budget_structured_rows(chunks, char_budget, _render_chunk_payload)


def _budget_asset_payload(assets: list[dict[str, Any]], char_budget: int) -> list[dict[str, Any]]:
    return _budget_structured_rows(assets, char_budget, _render_asset_payload)


def _budget_sensor_payload(sensor_rows: list[dict[str, Any]], char_budget: int) -> list[dict[str, Any]]:
    return _budget_structured_rows(sensor_rows, char_budget, _render_sensor_payload)


def _budget_assertion_payload(assertions: list[dict[str, Any]], char_budget: int) -> list[dict[str, Any]]:
    return _budget_structured_rows(assertions, char_budget, _render_assertion_payload)


def _budget_entity_payload(entities: list[dict[str, Any]], char_budget: int) -> list[dict[str, Any]]:
    return _budget_structured_rows(entities, char_budget, _render_entity_payload)


def _budget_graph_chain_payload(graph_chains: list[dict[str, Any]], char_budget: int) -> list[dict[str, Any]]:
    return _budget_structured_rows(graph_chains, char_budget, _render_graph_chain_payload)


def _budget_evidence_payload(evidence: list[dict[str, Any]], char_budget: int) -> list[dict[str, Any]]:
    return _budget_structured_rows(evidence, char_budget, _render_evidence_payload)


def _budget_structured_rows(
    rows: list[dict[str, Any]],
    char_budget: int,
    render_fn,
) -> list[dict[str, Any]]:
    kept: list[dict[str, Any]] = []
    used_chars = 0
    for row in rows:
        remaining = max(0, char_budget - used_chars)
        candidate = render_fn(row, remaining)
        size = len(json.dumps(_json_safe(candidate), ensure_ascii=False))
        if kept and used_chars + size > char_budget:
            continue
        if not kept and size > char_budget:
            candidate = render_fn(row, max(40, char_budget))
            candidate["truncated"] = True
            size = len(json.dumps(_json_safe(candidate), ensure_ascii=False))
            while size > char_budget and any(
                isinstance(candidate.get(field_name), str) and len(candidate.get(field_name) or "") > 24
                for field_name in ("text", "summary_text", "excerpt", "object_literal", "content")
            ):
                for field_name in ("text", "summary_text", "excerpt", "object_literal", "content"):
                    field_value = candidate.get(field_name)
                    if isinstance(field_value, str) and len(field_value) > 24:
                        candidate[field_name] = field_value[: max(24, len(field_value) // 2)].rstrip() + "..."
                        break
                size = len(json.dumps(_json_safe(candidate), ensure_ascii=False))
            kept.append(candidate)
            break
        if used_chars + size > char_budget and kept:
            continue
        kept.append(candidate)
        used_chars += size
    return kept


def _render_chunk_payload(row: dict[str, Any], remaining_chars: int) -> dict[str, Any]:
    text = str(row.get("text") or "")
    max_text = max(200, remaining_chars - 700)
    candidate = {
        "chunk_id": row["chunk_id"],
        "document_id": str(row["document_id"]),
        "chunk_index": row.get("chunk_index"),
        "page_start": row.get("page_start"),
        "page_end": row.get("page_end"),
        "section_path": list(row.get("section_path") or [])[:6],
        "section_title": row.get("metadata_json", {}).get("section_title"),
        "chunk_role": row.get("metadata_json", {}).get("chunk_role"),
        "text": text[:max_text],
    }
    section_synopsis = str(row.get("_section_synopsis") or "").strip()
    if section_synopsis and remaining_chars > 560:
        candidate["section_synopsis"] = section_synopsis[:220]
        if str(row.get("_section_synopsis_title") or "").strip():
            candidate["section_synopsis_title"] = str(row.get("_section_synopsis_title") or "").strip()[:120]
    document_synopsis = str(row.get("_document_synopsis") or "").strip()
    if document_synopsis and remaining_chars > 760:
        candidate["document_synopsis"] = document_synopsis[:220]
        if str(row.get("_document_synopsis_title") or "").strip():
            candidate["document_synopsis_title"] = str(row.get("_document_synopsis_title") or "").strip()[:120]
    if len(text) > max_text:
        candidate["truncated"] = True
    return candidate


def _render_assertion_payload(row: dict[str, Any], remaining_chars: int) -> dict[str, Any]:
    candidate = {
        "assertion_id": row["assertion_id"],
        "chunk_id": str(row["chunk_id"]),
        "subject_entity_id": row["subject_entity_id"],
        "predicate": row["predicate"],
        "object_entity_id": row.get("object_entity_id"),
        "object_literal": row.get("object_literal"),
        "confidence": float(row.get("confidence") or 0.0),
    }
    text = json.dumps(candidate, ensure_ascii=False)
    if len(text) > remaining_chars and remaining_chars > 0:
        candidate["object_literal"] = str(candidate.get("object_literal") or "")[: max(0, remaining_chars - 200)]
        candidate["truncated"] = True
    return candidate


def _render_asset_payload(row: dict[str, Any], remaining_chars: int) -> dict[str, Any]:
    metadata = row.get("metadata_json") or {}
    trusted_text = _trusted_asset_grounding_text(row)
    generated_text = sanitize_text(
        "\n".join(
            part
            for part in [
                str(row.get("description_text") or ""),
                " ".join(metadata.get("important_terms") or []),
                str(row.get("search_text") or ""),
            ]
            if str(part).strip()
        )
    ).strip()
    text = trusted_text or generated_text
    max_text = max(160, remaining_chars - 480)
    candidate = {
        "asset_id": row["asset_id"],
        "document_id": str(row["document_id"]),
        "page_number": row.get("page_number"),
        "asset_type": row.get("asset_type"),
        "label": metadata.get("label") or "",
        "important_terms": (metadata.get("important_terms") or [])[:8],
        "linked_chunk_ids": (metadata.get("linked_chunk_ids") or [])[:6],
        "link_types": (metadata.get("link_types") or [])[:4],
        "max_link_confidence": metadata.get("max_link_confidence"),
        "text_trust": "deterministic" if trusted_text else "generated",
        "text": text[:max_text],
    }
    if trusted_text and str(metadata.get("page_summary") or "").strip():
        candidate["page_summary"] = str(metadata.get("page_summary") or "")[:220]
    elif str(metadata.get("page_summary") or "").strip() and remaining_chars > 760:
        candidate["generated_page_summary"] = str(metadata.get("page_summary") or "")[:220]
    if trusted_text and generated_text and generated_text != trusted_text and remaining_chars > 720:
        candidate["generated_description"] = generated_text[: min(max_text, 320)]
    if len(text) > max_text:
        candidate["truncated"] = True
    return candidate


def _render_sensor_payload(row: dict[str, Any], remaining_chars: int) -> dict[str, Any]:
    summary_text = _trusted_sensor_grounding_text(row)
    max_summary = max(140, remaining_chars - 520)
    candidate = {
        "sensor_row_id": row["sensor_row_id"],
        "sensor_id": row["sensor_id"],
        "sensor_name": str(row.get("sensor_name") or ""),
        "sensor_type": str(row.get("sensor_type") or ""),
        "place_id": str(row.get("place_id") or ""),
        "place_name": str(row.get("place_name") or ""),
        "external_place_id": str(row.get("external_place_id") or ""),
        "hive_id": str(row.get("hive_id") or ""),
        "external_hive_id": str(row.get("external_hive_id") or ""),
        "hive_name": str(row.get("hive_name") or ""),
        "location_label": str(row.get("location_label") or ""),
        "metric_name": str(row.get("metric_name") or ""),
        "unit": row.get("unit"),
        "latest_value": row.get("latest_value"),
        "latest_observed_at": row.get("latest_observed_at"),
        "window_start_at": row.get("window_start_at"),
        "window_end_at": row.get("window_end_at"),
        "sample_count": row.get("sample_count"),
        "min_value": row.get("min_value"),
        "max_value": row.get("max_value"),
        "avg_value": row.get("avg_value"),
        "delta_value": row.get("delta_value"),
        "reading_ids": list(row.get("reading_ids") or [])[:8],
        "recent_points": list(row.get("recent_points") or [])[:6],
        "summary_text": summary_text[:max_summary],
    }
    if len(summary_text) > max_summary:
        candidate["truncated"] = True
    return candidate


def _render_entity_payload(row: dict[str, Any], remaining_chars: int) -> dict[str, Any]:
    candidate = {
        "entity_id": row["entity_id"],
        "canonical_name": row["canonical_name"],
        "entity_type": row["entity_type"],
    }
    aliases = row.get("aliases") or []
    if aliases and remaining_chars > 220:
        candidate["aliases"] = aliases[:4]
    return candidate


def _render_graph_chain_payload(row: dict[str, Any], remaining_chars: int) -> dict[str, Any]:
    candidate = {
        "chain_id": str(row.get("chain_id") or ""),
        "shared_entity_id": str(row.get("shared_entity_id") or ""),
        "shared_entity_name": str(row.get("shared_entity_name") or ""),
        "chain_score": float(row.get("chain_score") or 0.0),
        "supporting_assertion_ids": list(row.get("supporting_assertion_ids") or [])[:4],
        "supporting_evidence_ids": list(row.get("supporting_evidence_ids") or [])[:6],
        "steps": [],
    }
    step_budget = max(160, remaining_chars - 260)
    step_count = 0
    for step in list(row.get("steps") or [])[:2]:
        if step_count >= 2:
            break
        step_payload = {
            "assertion_id": str(step.get("assertion_id") or ""),
            "subject_name": str(step.get("subject_name") or ""),
            "predicate": str(step.get("predicate") or ""),
            "object_name": str(step.get("object_name") or ""),
            "object_literal": str(step.get("object_literal") or "")[: max(0, step_budget // 3)],
            "confidence": float(step.get("confidence") or 0.0),
            "evidence_ids": list(step.get("evidence_ids") or [])[:2],
        }
        candidate["steps"].append(step_payload)
        step_count += 1
    text = json.dumps(candidate, ensure_ascii=False)
    if len(text) > remaining_chars and candidate["steps"]:
        candidate["steps"] = candidate["steps"][:1]
        candidate["truncated"] = True
    return candidate


def _render_evidence_payload(row: dict[str, Any], remaining_chars: int) -> dict[str, Any]:
    excerpt = str(row.get("excerpt") or "")
    max_excerpt = max(120, remaining_chars - 180)
    candidate = {
        "evidence_id": row["evidence_id"],
        "assertion_id": row["assertion_id"],
        "excerpt": excerpt[:max_excerpt],
    }
    if len(excerpt) > max_excerpt:
        candidate["truncated"] = True
    return candidate


def _fit_prompt_bundle(
    question: str,
    normalized_query: str,
    question_type: str,
    prompt_bundle: AgentPromptBundle,
    runtime_config: dict[str, Any],
) -> AgentPromptBundle:
    while _estimate_prompt_chars(question, normalized_query, question_type, prompt_bundle, runtime_config) > runtime_config["prompt_char_budget"]:
        if prompt_bundle.profile_summary and (prompt_bundle.profile_summary.get("recurring_topics") or prompt_bundle.profile_summary.get("persistent_constraints")):
            if _shrink_prompt_bundle_once(prompt_bundle):
                continue
        if prompt_bundle.session_summary and (prompt_bundle.session_summary.get("open_threads") or prompt_bundle.session_summary.get("stable_facts")):
            if _shrink_prompt_bundle_once(prompt_bundle):
                continue
        if prompt_bundle.graph_chain_payload:
            prompt_bundle.graph_chain_payload.pop()
            continue
        if prompt_bundle.evidence_payload:
            prompt_bundle.evidence_payload.pop()
            continue
        if prompt_bundle.sensor_payload:
            prompt_bundle.sensor_payload.pop()
            continue
        if prompt_bundle.asset_payload:
            prompt_bundle.asset_payload.pop()
            continue
        if prompt_bundle.entity_payload:
            prompt_bundle.entity_payload.pop()
            continue
        if prompt_bundle.assertion_payload:
            prompt_bundle.assertion_payload.pop()
            continue
        if len(prompt_bundle.chunk_payload) > 1:
            prompt_bundle.chunk_payload.pop()
            continue
        if len(prompt_bundle.prior_context) > 1:
            prompt_bundle.prior_context.pop(0)
            continue
        if _truncate_prompt_bundle_once(prompt_bundle):
            continue
        break
    return prompt_bundle


def _shrink_prompt_bundle_once(prompt_bundle: AgentPromptBundle) -> bool:
    if prompt_bundle.profile_summary:
        profile = dict(prompt_bundle.profile_summary)
        recurring_topics = list(profile.get("recurring_topics") or [])
        if recurring_topics:
            recurring_topics.pop()
            profile["recurring_topics"] = recurring_topics
            prompt_bundle.profile_summary = _refresh_profile_summary_text(profile)
            return True
        persistent_constraints = list(profile.get("persistent_constraints") or [])
        if persistent_constraints:
            persistent_constraints.pop()
            profile["persistent_constraints"] = persistent_constraints
            prompt_bundle.profile_summary = _refresh_profile_summary_text(profile)
            return True
    if prompt_bundle.session_summary:
        summary = dict(prompt_bundle.session_summary)
        open_threads = list(summary.get("open_threads") or [])
        if open_threads:
            open_threads.pop()
            summary["open_threads"] = open_threads
            prompt_bundle.session_summary = _refresh_memory_summary_text(summary)
            return True
        facts = list(summary.get("stable_facts") or [])
        if len(facts) > 1:
            facts.pop()
            summary["stable_facts"] = facts
            prompt_bundle.session_summary = _refresh_memory_summary_text(summary)
            return True
    if prompt_bundle.graph_chain_payload:
        prompt_bundle.graph_chain_payload.pop()
        return True
    if prompt_bundle.evidence_payload:
        prompt_bundle.evidence_payload.pop()
        return True
    if prompt_bundle.sensor_payload:
        prompt_bundle.sensor_payload.pop()
        return True
    if prompt_bundle.asset_payload:
        prompt_bundle.asset_payload.pop()
        return True
    if prompt_bundle.entity_payload:
        prompt_bundle.entity_payload.pop()
        return True
    if prompt_bundle.assertion_payload:
        prompt_bundle.assertion_payload.pop()
        return True
    if len(prompt_bundle.chunk_payload) > 1:
        prompt_bundle.chunk_payload.pop()
        return True
    if len(prompt_bundle.prior_context) > 1:
        prompt_bundle.prior_context.pop(0)
        return True
    return _truncate_prompt_bundle_once(prompt_bundle)


def _truncate_prompt_bundle_once(prompt_bundle: AgentPromptBundle) -> bool:
    if prompt_bundle.profile_summary:
        summary_text = str((prompt_bundle.profile_summary or {}).get("summary_text") or "")
        if len(summary_text) > 140:
            prompt_bundle.profile_summary = {
                **prompt_bundle.profile_summary,
                "summary_text": summary_text[: max(140, len(summary_text) // 2)].rstrip() + "...",
            }
            return True
    if prompt_bundle.session_summary:
        summary_text = str((prompt_bundle.session_summary or {}).get("summary_text") or "")
        if len(summary_text) > 160:
            prompt_bundle.session_summary = {
                **prompt_bundle.session_summary,
                "summary_text": summary_text[: max(160, len(summary_text) // 2)].rstrip() + "...",
            }
            return True
    for payload in (
        prompt_bundle.graph_chain_payload,
        prompt_bundle.evidence_payload,
        prompt_bundle.sensor_payload,
        prompt_bundle.asset_payload,
        prompt_bundle.chunk_payload,
        prompt_bundle.assertion_payload,
    ):
        if not payload:
            continue
        candidate = payload[-1]
        for field_name, min_len in (("text", 120), ("summary_text", 120), ("excerpt", 80), ("object_literal", 80)):
            value = candidate.get(field_name)
            if isinstance(value, str) and len(value) > min_len:
                candidate[field_name] = value[: max(min_len, len(value) // 2)].rstrip() + "..."
                candidate["truncated"] = True
                return True
    if prompt_bundle.prior_context:
        candidate = prompt_bundle.prior_context[0]
        content = str(candidate.get("content") or "")
        if len(content) > 120:
            candidate["content"] = "..." + content[-max(120, len(content) // 2):]
            return True
    return False


def _build_user_prompt(
    question: str,
    normalized_query: str,
    question_type: str,
    prompt_bundle: AgentPromptBundle,
) -> str:
    return "\n".join(
        [
            f"question_type: {question_type}",
            f"question: {question}",
            f"normalized_query: {normalized_query}",
            f"allowed_chunk_ids: {json.dumps([item['chunk_id'] for item in prompt_bundle.chunk_payload], ensure_ascii=False)}",
            f"allowed_asset_ids: {json.dumps([item['asset_id'] for item in prompt_bundle.asset_payload], ensure_ascii=False)}",
            f"allowed_sensor_row_ids: {json.dumps([item['sensor_row_id'] for item in prompt_bundle.sensor_payload], ensure_ascii=False)}",
            f"allowed_assertion_ids: {json.dumps([item['assertion_id'] for item in prompt_bundle.assertion_payload], ensure_ascii=False)}",
            f"allowed_entity_ids: {json.dumps([item['entity_id'] for item in prompt_bundle.entity_payload], ensure_ascii=False)}",
            f"allowed_evidence_ids: {json.dumps([item['evidence_id'] for item in prompt_bundle.evidence_payload], ensure_ascii=False)}",
            "Use graph chains only as structural support. When they help, cite the underlying assertion ids and evidence ids rather than the chain itself.",
            "Return only the chunk ids, asset ids, sensor row ids, and evidence ids you actually used in used_chunk_ids, used_asset_ids, used_sensor_row_ids, and used_evidence_ids. Do not write source quotes.",
            f"context_chunks: {json.dumps(_json_safe(prompt_bundle.chunk_payload), ensure_ascii=False)}",
            f"context_assets: {json.dumps(_json_safe(prompt_bundle.asset_payload), ensure_ascii=False)}",
            f"context_sensors: {json.dumps(_json_safe(prompt_bundle.sensor_payload), ensure_ascii=False)}",
            f"context_assertions: {json.dumps(_json_safe(prompt_bundle.assertion_payload), ensure_ascii=False)}",
            f"context_entities: {json.dumps(_json_safe(prompt_bundle.entity_payload), ensure_ascii=False)}",
            f"context_graph_chains: {json.dumps(_json_safe(prompt_bundle.graph_chain_payload), ensure_ascii=False)}",
            f"context_evidence: {json.dumps(_json_safe(prompt_bundle.evidence_payload), ensure_ascii=False)}",
            f"context_budget: {json.dumps(_json_safe(prompt_bundle.stats), ensure_ascii=False)}",
        ]
    )


def _build_open_world_user_payload(
    *,
    question: str,
    normalized_query: str,
    question_type: str,
    bundle: AgentContextBundle,
) -> dict[str, Any]:
    return {
        "question": question,
        "normalized_query": normalized_query,
        "question_type": question_type,
        "policy": {
            "may_use_general_world_knowledge": True,
            "do_not_claim_corpus_provenance_without_support": True,
            "prefer_context_hints_when_relevant": True,
            "do_not_abstain_if_a_helpful_best_effort_answer_is_possible": True,
        },
        "context_hints": {
            "chunks": [
                {
                    "chunk_id": row["chunk_id"],
                    "section_title": str((row.get("metadata_json") or {}).get("section_title") or ""),
                    "excerpt": _extract_citation_excerpt(str(row.get("text") or ""), normalized_query, 220),
                }
                for row in list(bundle.chunks or [])[:2]
            ],
            "assertions": [
                {
                    "assertion_id": row["assertion_id"],
                    "subject_entity_id": str(row.get("subject_entity_id") or ""),
                    "predicate": str(row.get("predicate") or ""),
                    "object_entity_id": str(row.get("object_entity_id") or ""),
                    "object_literal": str(row.get("object_literal") or ""),
                }
                for row in list(bundle.assertions or [])[:3]
            ],
            "assets": [
                {
                    "asset_id": row["asset_id"],
                    "label": str((row.get("metadata_json") or {}).get("label") or ""),
                    "excerpt": _extract_citation_excerpt(_trusted_asset_grounding_text(row), normalized_query, 180),
                }
                for row in list(bundle.assets or [])[:2]
                if _trusted_asset_grounding_text(row)
            ],
            "sensors": [
                {
                    "sensor_row_id": str(row.get("sensor_row_id") or ""),
                    "sensor_name": str(row.get("sensor_name") or ""),
                    "metric_name": str(row.get("metric_name") or ""),
                    "summary_text": _extract_citation_excerpt(_trusted_sensor_grounding_text(row), normalized_query, 180),
                }
                for row in list(bundle.sensor_rows or [])[:2]
                if _trusted_sensor_grounding_text(row)
            ],
        },
    }


def _build_chat_payload(
    system_prompt: str,
    user_prompt: str,
    prompt_bundle: AgentPromptBundle,
    runtime_config: dict[str, Any],
) -> dict[str, Any]:
    model = str(runtime_config["model"] or "")
    reasoning_effort = str(runtime_config["reasoning_effort"] or "")
    messages = [{"role": "system", "content": system_prompt}]
    if prompt_bundle.profile_summary:
        messages.append(
            {
                "role": "system",
                "content": "user_profile: " + json.dumps(_json_safe(prompt_bundle.profile_summary), ensure_ascii=False),
            }
        )
    if prompt_bundle.session_summary:
        messages.append(
            {
                "role": "system",
                "content": "session_memory: " + json.dumps(_json_safe(prompt_bundle.session_summary), ensure_ascii=False),
            }
        )
    payload = {
        "model": model,
        "reasoning_effort": reasoning_effort,
        "messages": [*messages, *prompt_bundle.prior_context, {"role": "user", "content": user_prompt}],
        "response_format": {
            "type": "json_schema",
            "json_schema": {
                "name": "agent_query_result",
                "strict": True,
                "schema": _agent_response_schema(),
            },
        },
    }
    if runtime_config["temperature"] > 0 and _supports_temperature(model, reasoning_effort):
        payload["temperature"] = runtime_config["temperature"]
    if runtime_config["max_completion_tokens"] > 0:
        payload["max_completion_tokens"] = runtime_config["max_completion_tokens"]
    return payload


def _build_plaintext_recovery_payload(
    payload: dict[str, Any],
    *,
    instruction: str,
) -> dict[str, Any]:
    messages = list(payload.get("messages") or [])
    insert_index = 1 if messages and messages[0].get("role") == "system" else 0
    messages.insert(
        insert_index,
        {
            "role": "system",
            "content": instruction,
        },
    )
    recovery_payload = {
        key: value
        for key, value in payload.items()
        if key != "response_format"
    }
    recovery_payload["messages"] = messages
    return recovery_payload


def _with_model_override(payload: dict[str, Any], model: str, reasoning_effort: str) -> dict[str, Any]:
    updated = {**payload, "model": model, "reasoning_effort": reasoning_effort}
    if "temperature" in updated and not _supports_temperature(model, reasoning_effort):
        updated.pop("temperature", None)
    return updated


def _recover_nonempty_text_content(
    *,
    base_url: str,
    api_key: str,
    timeout_seconds: float,
    primary_payload: dict[str, Any],
    fallback_model: str,
    fallback_reasoning_effort: str,
) -> tuple[str, dict[str, Any], str]:
    body = _call_openai_json_payload(
        base_url=base_url,
        api_key=api_key,
        payload=primary_payload,
        timeout_seconds=timeout_seconds,
    )
    content = _extract_message_content(body)
    if str(content or "").strip():
        return content, body, str(primary_payload.get("model") or "")
    fallback_model = str(fallback_model or "").strip()
    if fallback_model and fallback_model != str(primary_payload.get("model") or "").strip():
        fallback_payload = _with_model_override(primary_payload, fallback_model, fallback_reasoning_effort)
        body = _call_openai_json_payload(
            base_url=base_url,
            api_key=api_key,
            payload=fallback_payload,
            timeout_seconds=timeout_seconds,
        )
        content = _extract_message_content(body)
        if str(content or "").strip():
            return content, body, fallback_model
    raise AgentQueryError("Agent answer returned empty content")


def _estimate_prompt_chars(
    question: str,
    normalized_query: str,
    question_type: str,
    prompt_bundle: AgentPromptBundle,
    runtime_config: dict[str, Any],
) -> int:
    system_prompt = runtime_config["system_prompt"]
    user_prompt = _build_user_prompt(question, normalized_query, question_type, prompt_bundle)
    profile_summary = json.dumps(_json_safe(prompt_bundle.profile_summary or {}), ensure_ascii=False) if prompt_bundle.profile_summary else ""
    session_summary = json.dumps(_json_safe(prompt_bundle.session_summary or {}), ensure_ascii=False) if prompt_bundle.session_summary else ""
    return len(system_prompt) + len(profile_summary) + len(session_summary) + len(user_prompt) + sum(len(str(item.get("content") or "")) for item in prompt_bundle.prior_context)


def _estimate_request_chars(payload: dict[str, Any]) -> int:
    return len(json.dumps(_json_safe(payload), ensure_ascii=False))


def _supports_temperature(model: str, reasoning_effort: str) -> bool:
    lowered = model.lower().strip()
    if lowered.startswith("gpt-5.4") or lowered.startswith("gpt-5.2"):
        return reasoning_effort == "none"
    if lowered.startswith("gpt-5"):
        return False
    return True


def _estimate_json_chars(payload: Any) -> int:
    return len(json.dumps(_json_safe(payload), ensure_ascii=False))


def _refresh_memory_summary_text(summary: dict[str, Any]) -> dict[str, Any]:
    payload = dict(summary or {})
    payload["summary_text"] = _memory_summary_text(payload)
    return payload


def _refresh_profile_summary_text(summary: dict[str, Any]) -> dict[str, Any]:
    payload = dict(summary or {})
    payload["summary_text"] = _profile_summary_text(payload)
    return payload


def _budget_profile_summary(profile: dict[str, Any] | None, char_budget: int) -> dict[str, Any] | None:
    if not profile:
        return None
    summary_json = _coerce_profile_summary(_sanitize_profile_summary_payload(profile.get("summary_json") or profile))
    if not summary_json:
        return None
    payload = dict(summary_json)
    while _estimate_json_chars(payload) > char_budget:
        learning_goals = list(payload.get("learning_goals") or [])
        if learning_goals:
            learning_goals.pop()
            payload["learning_goals"] = learning_goals
            payload = _refresh_profile_summary_text(payload)
            continue
        recurring_topics = list(payload.get("recurring_topics") or [])
        if recurring_topics:
            recurring_topics.pop()
            payload["recurring_topics"] = recurring_topics
            payload = _refresh_profile_summary_text(payload)
            continue
        answer_preferences = list(payload.get("answer_preferences") or [])
        if len(answer_preferences) > 1:
            answer_preferences.pop()
            payload["answer_preferences"] = answer_preferences
            payload = _refresh_profile_summary_text(payload)
            continue
        persistent_constraints = list(payload.get("persistent_constraints") or [])
        if persistent_constraints:
            persistent_constraints.pop()
            payload["persistent_constraints"] = persistent_constraints
            payload = _refresh_profile_summary_text(payload)
            continue
        preferred_document_ids = list(payload.get("preferred_document_ids") or [])
        if preferred_document_ids:
            preferred_document_ids.pop()
            payload["preferred_document_ids"] = preferred_document_ids
            payload = _refresh_profile_summary_text(payload)
            continue
        communication_style = str(payload.get("communication_style") or "").strip()
        if len(communication_style) > 80:
            payload["communication_style"] = communication_style[: max(80, len(communication_style) // 2)].rstrip() + "..."
            payload = _refresh_profile_summary_text(payload)
            continue
        for field, floor in (("user_background", 120), ("beekeeping_context", 120), ("experience_level", 40), ("last_query", 120)):
            value = str(payload.get(field) or "").strip()
            if len(value) > floor:
                payload[field] = value[: max(floor, len(value) // 2)].rstrip() + "..."
                payload = _refresh_profile_summary_text(payload)
                break
        else:
            break
    payload.pop("summary_text", None)
    return payload


def _budget_session_summary(session_memory: dict[str, Any] | None, char_budget: int) -> dict[str, Any] | None:
    if not session_memory:
        return None
    summary_json = _coerce_memory_summary(
        dict(session_memory.get("summary_json") or {}),
        max_facts=12,
        max_open_threads=12,
        max_resolved_threads=12,
        max_preferences=12,
        max_topics=16,
    )
    if not summary_json:
        return None
    payload = dict(summary_json)
    while _estimate_json_chars(payload) > char_budget:
        open_threads = list(payload.get("open_threads") or [])
        if open_threads:
            open_threads.pop()
            payload["open_threads"] = open_threads
            payload = _refresh_memory_summary_text(payload)
            continue
        resolved_threads = list(payload.get("resolved_threads") or [])
        if resolved_threads:
            resolved_threads.pop()
            payload["resolved_threads"] = resolved_threads
            payload = _refresh_memory_summary_text(payload)
            continue
        topic_keywords = list(payload.get("topic_keywords") or [])
        if topic_keywords:
            topic_keywords.pop()
            payload["topic_keywords"] = topic_keywords
            payload = _refresh_memory_summary_text(payload)
            continue
        user_preferences = list(payload.get("user_preferences") or [])
        if len(user_preferences) > 1:
            user_preferences.pop()
            payload["user_preferences"] = user_preferences
            payload = _refresh_memory_summary_text(payload)
            continue
        stable_facts = list(payload.get("stable_facts") or [])
        if len(stable_facts) > 1:
            stable_facts = sorted(stable_facts, key=_stable_fact_priority, reverse=True)
            stable_facts.pop()
            payload["stable_facts"] = stable_facts
            payload = _refresh_memory_summary_text(payload)
            continue
        active_constraints = list(payload.get("active_constraints") or [])
        if len(active_constraints) > 1:
            active_constraints = sorted(active_constraints, key=_constraint_priority, reverse=True)
            active_constraints.pop()
            payload["active_constraints"] = active_constraints
            payload = _refresh_memory_summary_text(payload)
            continue
        for field, floor in (("session_goal", 120), ("scope_signature", 80), ("last_query", 120)):
            value = str(payload.get(field) or "").strip()
            if len(value) > floor:
                payload[field] = value[: max(floor, len(value) // 2)].rstrip() + "..."
                payload = _refresh_memory_summary_text(payload)
                break
        else:
            break
    payload.pop("summary_text", None)
    return payload


def _summarize_session_memory(
    *,
    question: str,
    normalized_query: str,
    response: dict[str, Any],
    bundle: AgentContextBundle,
    request_scope: dict[str, Any],
    prior_summary: dict[str, Any] | None,
    recent_messages: list[dict[str, Any]],
    runtime_config: dict[str, Any],
) -> tuple[dict[str, Any], str, str, str, str]:
    fallback_summary = _build_fallback_session_memory(
        question=question,
        normalized_query=normalized_query,
        response=response,
        bundle=bundle,
        request_scope=request_scope,
        prior_summary=prior_summary,
        recent_messages=recent_messages,
        runtime_config=runtime_config,
    )
    if not runtime_config.get("memory_enabled", True):
        return fallback_summary, _memory_summary_text(fallback_summary), "disabled", "disabled", str(runtime_config.get("memory_prompt_version") or "v1")
    provider = str(runtime_config.get("memory_provider") or runtime_config.get("provider") or "").strip().lower()
    api_key = runtime_config.get("api_key_override") or settings.agent_api_key
    model = str(runtime_config.get("memory_model") or "").strip()
    if provider not in {"", "auto", "openai"} or not api_key or not model:
        return fallback_summary, _memory_summary_text(fallback_summary), provider or "fallback", model or "fallback", str(runtime_config.get("memory_prompt_version") or "v1")
    evidence_rows = _grounding_evidence_rows(
        {row["chunk_id"]: row for row in bundle.chunks},
        list(response.get("used_chunk_ids") or []),
        {row["asset_id"]: row for row in bundle.assets},
        list(response.get("used_asset_ids") or []),
        {},
        [],
        {row["assertion_id"]: row for row in bundle.assertions},
        {row["evidence_id"]: row for row in bundle.evidence},
        list(response.get("used_evidence_ids") or []),
    )[:8]
    payload = _build_model_json_payload(
        model=model,
        reasoning_effort=str(runtime_config.get("memory_reasoning_effort") or "none"),
        system_prompt=str(runtime_config.get("memory_system_prompt") or "").strip(),
        user_payload={
            "prior_summary": dict((prior_summary or {}).get("summary_json") or {}),
            "prior_summary_text": str((prior_summary or {}).get("summary_text") or "").strip()[:1200],
            "recent_messages": _compact_recent_messages(
                recent_messages,
                limit=int(runtime_config.get("memory_recent_messages") or 8),
            ),
            "question": question,
            "normalized_query": normalized_query,
            "answer": str(response.get("answer") or ""),
            "abstained": bool(response.get("abstained")),
            "abstain_reason": str(response.get("abstain_reason") or ""),
            "review_status": str(response.get("review_status") or ""),
            "grounding_check": dict(response.get("grounding_check") or {}),
            "question_type": str(response.get("question_type") or ""),
            "used_chunk_ids": list(response.get("used_chunk_ids") or []),
            "used_asset_ids": list(response.get("used_asset_ids") or []),
            "used_evidence_ids": list(response.get("used_evidence_ids") or []),
            "supporting_assertions": list(response.get("supporting_assertions") or []),
            "request_scope": request_scope,
            "evidence_rows": evidence_rows,
            "memory_write_policy": {
                "facts_require_evidence_or_explicit_self_report": True,
                "unresolved_threads_are_short_session": True,
                "preferences_are_preferences_not_facts": True,
            },
            "constraints": {
                "max_facts": int(runtime_config.get("memory_max_facts") or 6),
                "max_open_threads": int(runtime_config.get("memory_max_open_threads") or 6),
                "max_resolved_threads": int(runtime_config.get("memory_max_resolved_threads") or 6),
                "max_preferences": int(runtime_config.get("memory_max_preferences") or 6),
                "max_topics": int(runtime_config.get("memory_max_topics") or 8),
            },
        },
        schema_name="agent_session_memory",
        schema=_memory_summary_schema(),
        temperature=float(runtime_config.get("memory_temperature") or 0.0),
        max_completion_tokens=int(runtime_config.get("memory_max_completion_tokens") or 400),
    )
    base_url = str(runtime_config.get("memory_base_url") or runtime_config.get("base_url") or settings.agent_base_url).rstrip("/")
    try:
        body = _call_openai_json_payload(
            base_url=base_url,
            api_key=str(api_key),
            payload=payload,
            timeout_seconds=float(runtime_config.get("memory_timeout_seconds") or 20.0),
        )
        data = json.loads(_extract_message_content(body))
        summary_json = _coerce_memory_summary(
            data,
            max_facts=int(runtime_config.get("memory_max_facts") or 6),
            max_open_threads=int(runtime_config.get("memory_max_open_threads") or 6),
            max_resolved_threads=int(runtime_config.get("memory_max_resolved_threads") or 6),
            max_preferences=int(runtime_config.get("memory_max_preferences") or 6),
            max_topics=int(runtime_config.get("memory_max_topics") or 8),
        )
        return summary_json, _memory_summary_text(summary_json), provider or "openai", model, str(runtime_config.get("memory_prompt_version") or "v1")
    except Exception:
        return fallback_summary, _memory_summary_text(fallback_summary), provider or "fallback", model or "fallback", str(runtime_config.get("memory_prompt_version") or "v1")


def _build_fallback_session_memory(
    *,
    question: str,
    normalized_query: str,
    response: dict[str, Any],
    bundle: AgentContextBundle,
    request_scope: dict[str, Any],
    prior_summary: dict[str, Any] | None,
    recent_messages: list[dict[str, Any]],
    runtime_config: dict[str, Any],
) -> dict[str, Any]:
    prior_json = dict((prior_summary or {}).get("summary_json") or {})
    active_constraints = list(prior_json.get("active_constraints") or [])
    document_ids = list(request_scope.get("document_ids") or [])
    scope_signature = _scope_signature(request_scope)
    if document_ids:
        constraint = "document_scope:" + ",".join(document_ids[:8])
        active_constraints.insert(
            0,
            {
                "constraint": constraint,
                "kind": "document_scope",
                "source": "request_scope",
            },
        )
    if scope_signature:
        active_constraints.insert(
            0,
            {
                "constraint": scope_signature,
                "kind": "scope_signature",
                "source": "request_scope",
            },
        )
    active_constraints = _normalize_constraint_items(
        sorted(active_constraints, key=_constraint_priority, reverse=True),
        limit=8,
    )
    stable_facts = list(prior_json.get("stable_facts") or [])
    if not response.get("abstained"):
        for fact in reversed(
            _evidence_backed_facts(
                bundle=bundle,
                used_chunk_ids=list(response.get("used_chunk_ids") or [])[:6],
                used_asset_ids=list(response.get("used_asset_ids") or [])[:4],
                assertion_ids=list(response.get("supporting_assertions") or [])[:6],
                evidence_ids=list(response.get("used_evidence_ids") or [])[:6],
                limit=max(1, min(3, int(runtime_config.get("memory_max_facts") or 6))),
            )
        ):
            stable_facts.insert(0, fact)
    stable_facts = _dedupe_stable_facts(
        _normalize_memory_fact_items(stable_facts, limit=max(12, int(runtime_config.get("memory_max_facts") or 6) * 2)),
        limit=int(runtime_config.get("memory_max_facts") or 6),
    )
    open_threads = list(prior_json.get("open_threads") or [])
    resolved_threads = list(prior_json.get("resolved_threads") or [])
    if response.get("abstained"):
        open_threads.insert(
            0,
            {
                "thread": question[:220],
                "source": "abstained_turn",
                "source_query": normalized_query[:220],
                "question_type": str(response.get("question_type") or ""),
                "expiry_policy": "short_session",
            },
        )
    else:
        resolved_threads.insert(
            0,
            {
                "thread": _resolved_thread_label(question, response),
                "source": "answered_turn",
                "source_query": normalized_query[:220],
                "question_type": str(response.get("question_type") or ""),
                "expiry_policy": "resolved_session",
            },
        )
        open_threads = [item for item in open_threads if not _thread_matches_question(item, question)]
    open_threads = _retain_relevant_open_threads(open_threads, question, keep_unmatched=1)
    recent_preference_items = _normalize_named_items(
        _derive_preference_hints_from_messages(recent_messages + [{"role": "user", "content": question}]),
        value_key="preference",
        limit=int(runtime_config.get("memory_max_preferences") or 6),
        max_len=180,
        source_default="interaction_pattern",
    )
    retained_preference_items = _retain_reaffirmed_named_items(
        list(prior_json.get("user_preferences") or []),
        recent_preference_items,
        value_key="preference",
        sticky_sources={"user", "legacy", "manual", "admin", "user_self_report"},
    )
    user_preferences = _normalize_named_items(
        recent_preference_items + retained_preference_items,
        value_key="preference",
        limit=int(runtime_config.get("memory_max_preferences") or 6),
        max_len=180,
        source_default="interaction_pattern",
    )
    topic_keywords = _normalize_string_list(
        _derive_topic_keywords(
            [normalized_query] + [str(item.get("content") or "") for item in recent_messages if str(item.get("role") or "") == "user"],
            limit=int(runtime_config.get("memory_max_topics") or 8),
        ) + list(prior_json.get("topic_keywords") or []),
        limit=int(runtime_config.get("memory_max_topics") or 8),
    )
    prior_goal = str(prior_json.get("session_goal") or "").strip()
    current_goal = sanitize_text(question).strip()[:220]
    summary = {
        "summary_version": "v3",
        "session_goal": _select_session_goal(prior_goal, current_goal, recent_messages),
        "active_constraints": active_constraints[:8],
        "stable_facts": stable_facts[: int(runtime_config.get("memory_max_facts") or 6)],
        "open_threads": _normalize_thread_items(
            open_threads,
            limit=int(runtime_config.get("memory_max_open_threads") or 6),
            expiry_policy="short_session",
            source_default="session_history",
        ),
        "resolved_threads": _normalize_thread_items(
            resolved_threads,
            limit=int(runtime_config.get("memory_max_resolved_threads") or 6),
            expiry_policy="resolved_session",
            source_default="session_history",
        ),
        "user_preferences": user_preferences,
        "topic_keywords": topic_keywords,
        "preferred_document_ids": document_ids[:8],
        "scope_signature": scope_signature,
        "last_query": normalized_query[:220],
    }
    summary["summary_text"] = _memory_summary_text(summary)
    return summary


def _memory_summary_text(summary: dict[str, Any]) -> str:
    facts = list(summary.get("stable_facts") or [])
    open_threads = list(summary.get("open_threads") or [])
    resolved_threads = list(summary.get("resolved_threads") or [])
    constraints = list(summary.get("active_constraints") or [])
    preferences = list(summary.get("user_preferences") or [])
    topic_keywords = list(summary.get("topic_keywords") or [])
    parts = [
        f"goal: {str(summary.get('session_goal') or '').strip()}".strip(),
    ]
    if constraints:
        parts.append("constraints: " + "; ".join(_typed_values(constraints[:6], "constraint")))
    if preferences:
        parts.append("preferences: " + "; ".join(_typed_values(preferences[:6], "preference")))
    if topic_keywords:
        parts.append("topics: " + ", ".join(str(item) for item in topic_keywords[:8]))
    if facts:
        parts.append(
            "facts: " + " | ".join(
                str((item or {}).get("fact") or "").strip()
                for item in facts[:4]
                if str((item or {}).get("fact") or "").strip()
            )
        )
    if resolved_threads:
        parts.append("resolved: " + " | ".join(_typed_values(resolved_threads[:4], "thread")))
    if open_threads:
        parts.append("open_threads: " + " | ".join(_typed_values(open_threads[:4], "thread")))
    return "\n".join(part for part in parts if part.strip()).strip()


def _coerce_memory_summary(
    payload: dict[str, Any],
    *,
    max_facts: int,
    max_open_threads: int,
    max_resolved_threads: int,
    max_preferences: int,
    max_topics: int,
) -> dict[str, Any]:
    stable_facts = _normalize_memory_fact_items(list(payload.get("stable_facts") or []), limit=max_facts)
    summary = {
        "summary_version": "v3",
        "session_goal": str(payload.get("session_goal") or "").strip()[:220],
        "active_constraints": _normalize_constraint_items(payload.get("active_constraints") or [], limit=8, max_len=220),
        "stable_facts": stable_facts,
        "open_threads": _normalize_thread_items(
            payload.get("open_threads") or [],
            limit=max_open_threads,
            expiry_policy="short_session",
            source_default="legacy",
        ),
        "resolved_threads": _normalize_thread_items(
            payload.get("resolved_threads") or [],
            limit=max_resolved_threads,
            expiry_policy="resolved_session",
            source_default="legacy",
        ),
        "user_preferences": _normalize_named_items(
            payload.get("user_preferences") or [],
            value_key="preference",
            limit=max_preferences,
            max_len=180,
            source_default="legacy",
        ),
        "topic_keywords": _normalize_string_list(payload.get("topic_keywords") or [], limit=max_topics, max_len=80),
        "preferred_document_ids": [str(item).strip() for item in (payload.get("preferred_document_ids") or []) if str(item).strip()][:8],
        "scope_signature": str(payload.get("scope_signature") or "").strip()[:220],
        "last_query": str(payload.get("last_query") or "").strip()[:220],
    }
    summary["summary_text"] = _memory_summary_text(summary)
    return summary


def _memory_summary_schema() -> dict[str, Any]:
    return {
        "type": "object",
        "additionalProperties": False,
        "properties": {
            "session_goal": {"type": "string"},
            "active_constraints": {"type": "array", "items": {"type": "string"}},
            "stable_facts": {
                "type": "array",
                "items": {
                    "type": "object",
                    "additionalProperties": False,
                    "properties": {
                        "fact": {"type": "string"},
                        "fact_type": {"type": "string"},
                        "source_type": {"type": "string"},
                        "confidence": {"type": "number"},
                        "review_policy": {"type": "string"},
                        "chunk_ids": {"type": "array", "items": {"type": "string"}},
                        "asset_ids": {"type": "array", "items": {"type": "string"}},
                        "assertion_ids": {"type": "array", "items": {"type": "string"}},
                        "evidence_ids": {"type": "array", "items": {"type": "string"}},
                    },
                    "required": ["fact", "fact_type", "source_type", "confidence", "review_policy", "chunk_ids", "asset_ids", "assertion_ids", "evidence_ids"],
                },
            },
            "open_threads": {
                "type": "array",
                "items": {
                    "type": "object",
                    "additionalProperties": False,
                    "properties": {
                        "thread": {"type": "string"},
                        "source": {"type": "string"},
                        "source_query": {"type": "string"},
                        "question_type": {"type": "string"},
                        "expiry_policy": {"type": "string"},
                    },
                    "required": ["thread", "source", "source_query", "question_type", "expiry_policy"],
                },
            },
            "resolved_threads": {
                "type": "array",
                "items": {
                    "type": "object",
                    "additionalProperties": False,
                    "properties": {
                        "thread": {"type": "string"},
                        "source": {"type": "string"},
                        "source_query": {"type": "string"},
                        "question_type": {"type": "string"},
                        "expiry_policy": {"type": "string"},
                    },
                    "required": ["thread", "source", "source_query", "question_type", "expiry_policy"],
                },
            },
            "user_preferences": {
                "type": "array",
                "items": {
                    "type": "object",
                    "additionalProperties": False,
                    "properties": {
                        "preference": {"type": "string"},
                        "source": {"type": "string"},
                    },
                    "required": ["preference", "source"],
                },
            },
            "topic_keywords": {"type": "array", "items": {"type": "string"}},
            "preferred_document_ids": {"type": "array", "items": {"type": "string"}},
            "scope_signature": {"type": "string"},
            "last_query": {"type": "string"},
            "summary_version": {"type": "string"},
        },
        "required": [
            "session_goal",
            "active_constraints",
            "stable_facts",
            "open_threads",
            "resolved_threads",
            "user_preferences",
            "topic_keywords",
            "preferred_document_ids",
            "scope_signature",
            "last_query",
            "summary_version",
        ],
    }


def _summarize_agent_profile(
    *,
    question: str,
    normalized_query: str,
    response: dict[str, Any],
    session_memory: dict[str, Any] | None,
    prior_profile: dict[str, Any] | None,
    recent_messages: list[dict[str, Any]],
    runtime_config: dict[str, Any],
) -> tuple[dict[str, Any], str, str, str, str]:
    fallback_profile = _build_fallback_agent_profile(
        question=question,
        normalized_query=normalized_query,
        response=response,
        session_memory=session_memory,
        prior_profile=prior_profile,
        recent_messages=recent_messages,
        runtime_config=runtime_config,
    )
    if not runtime_config.get("profile_enabled", True):
        return fallback_profile, _profile_summary_text(fallback_profile), "disabled", "disabled", str(runtime_config.get("profile_prompt_version") or "v1")
    provider = str(runtime_config.get("profile_provider") or runtime_config.get("provider") or "").strip().lower()
    api_key = runtime_config.get("api_key_override") or settings.agent_api_key
    model = str(runtime_config.get("profile_model") or "").strip()
    if provider not in {"", "auto", "openai"} or not api_key or not model:
        return fallback_profile, _profile_summary_text(fallback_profile), provider or "fallback", model or "fallback", str(runtime_config.get("profile_prompt_version") or "v1")
    payload = _build_model_json_payload(
        model=model,
        reasoning_effort=str(runtime_config.get("profile_reasoning_effort") or "none"),
        system_prompt=str(runtime_config.get("profile_system_prompt") or "").strip(),
        user_payload={
            "prior_profile": dict((prior_profile or {}).get("summary_json") or {}),
            "prior_profile_text": str((prior_profile or {}).get("summary_text") or "").strip()[:1200],
            "session_memory": dict((session_memory or {}).get("summary_json") or {}),
            "session_memory_text": str((session_memory or {}).get("summary_text") or "").strip()[:1200],
            "recent_messages": _compact_recent_messages(
                recent_messages,
                limit=int(runtime_config.get("profile_recent_messages") or 12),
            ),
            "question": question,
            "normalized_query": normalized_query,
            "answer": str(response.get("answer") or ""),
            "abstained": bool(response.get("abstained")),
            "abstain_reason": str(response.get("abstain_reason") or ""),
            "profile_write_policy": {
                "preferences_and_topics_may_be_inferred_but_must_be_typed": True,
                "domain_facts_do_not_belong_in_profile_memory": True,
            },
        },
        schema_name="agent_profile_summary",
        schema=_profile_summary_schema(),
        temperature=float(runtime_config.get("profile_temperature") or 0.0),
        max_completion_tokens=int(runtime_config.get("profile_max_completion_tokens") or 400),
    )
    base_url = str(runtime_config.get("profile_base_url") or runtime_config.get("base_url") or settings.agent_base_url).rstrip("/")
    try:
        body = _call_openai_json_payload(
            base_url=base_url,
            api_key=str(api_key),
            payload=payload,
            timeout_seconds=float(runtime_config.get("profile_timeout_seconds") or 20.0),
        )
        data = json.loads(_extract_message_content(body))
        profile_json = _coerce_profile_summary(data)
        return profile_json, _profile_summary_text(profile_json), provider or "openai", model, str(runtime_config.get("profile_prompt_version") or "v1")
    except Exception:
        return fallback_profile, _profile_summary_text(fallback_profile), provider or "fallback", model or "fallback", str(runtime_config.get("profile_prompt_version") or "v1")


def _build_fallback_agent_profile(
    *,
    question: str,
    normalized_query: str,
    response: dict[str, Any],
    session_memory: dict[str, Any] | None,
    prior_profile: dict[str, Any] | None,
    recent_messages: list[dict[str, Any]],
    runtime_config: dict[str, Any],
) -> dict[str, Any]:
    prior_json = dict((prior_profile or {}).get("summary_json") or {})
    session_json = dict((session_memory or {}).get("summary_json") or {})
    recent_topic_items = _normalize_named_items(
        _derive_topic_keywords(
            [normalized_query] + [str(item.get("content") or "") for item in recent_messages if str(item.get("role") or "") == "user"],
            limit=int(runtime_config.get("profile_max_topics") or 8),
        ),
        value_key="topic",
        limit=int(runtime_config.get("profile_max_topics") or 8),
        max_len=180,
        source_default="interaction_pattern",
    )
    retained_topic_items = _retain_reaffirmed_named_items(
        list(prior_json.get("recurring_topics") or []),
        recent_topic_items,
        value_key="topic",
        sticky_sources={"user", "legacy", "manual", "admin", "profile_history"},
    )
    recurring_topics = _normalize_named_items(
        recent_topic_items + retained_topic_items,
        value_key="topic",
        limit=int(runtime_config.get("profile_max_topics") or 8),
        max_len=180,
        source_default="interaction_pattern",
    )
    persistent_constraints = _normalize_constraint_items(
        sorted(list(prior_json.get("persistent_constraints") or []), key=_constraint_priority, reverse=True),
        limit=int(runtime_config.get("profile_max_constraints") or 8),
    )
    recent_answer_preferences = _normalize_named_items(
        _derive_preference_hints_from_messages(recent_messages + [{"role": "user", "content": question}]),
        value_key="preference",
        limit=int(runtime_config.get("profile_max_preferences") or 8),
        max_len=160,
        source_default="interaction_pattern",
    )
    retained_answer_preferences = _retain_reaffirmed_named_items(
        list(prior_json.get("answer_preferences") or []),
        recent_answer_preferences,
        value_key="preference",
        sticky_sources={"user", "legacy", "manual", "admin", "profile_history", "user_self_report"},
    )
    answer_preferences = _normalize_named_items(
        recent_answer_preferences + retained_answer_preferences,
        value_key="preference",
        limit=int(runtime_config.get("profile_max_preferences") or 8),
        max_len=160,
        source_default="interaction_pattern",
    )
    if response.get("abstained"):
        answer_preferences = _normalize_named_items(
            [{"preference": "prefer_explicit_evidence", "source": "abstained_turn"}, *answer_preferences],
            value_key="preference",
            limit=int(runtime_config.get("profile_max_preferences") or 8),
            max_len=160,
            source_default="interaction_pattern",
        )
    communication_style = str(prior_json.get("communication_style") or "").strip()
    if not communication_style:
        communication_style = _derive_communication_style(recent_messages + [{"role": "user", "content": question}], answer_preferences)
    learning_goals = _normalize_named_items(
        list(prior_json.get("learning_goals") or []),
        value_key="goal",
        limit=6,
        max_len=180,
        source_default="profile_history",
    )
    profile = {
        "summary_version": "v3",
        "user_background": str(prior_json.get("user_background") or "").strip()[:220],
        "beekeeping_context": str(prior_json.get("beekeeping_context") or "").strip()[:220],
        "experience_level": str(prior_json.get("experience_level") or "").strip()[:80],
        "communication_style": communication_style[:120],
        "answer_preferences": answer_preferences[: int(runtime_config.get("profile_max_preferences") or 8)],
        "recurring_topics": recurring_topics[: int(runtime_config.get("profile_max_topics") or 8)],
        "learning_goals": learning_goals[:6],
        "persistent_constraints": persistent_constraints[: int(runtime_config.get("profile_max_constraints") or 8)],
        "preferred_document_ids": [str(item).strip() for item in (prior_json.get("preferred_document_ids") or []) if str(item).strip()][:8],
        "last_query": normalized_query[:220],
    }
    return _refresh_profile_summary_text(profile)


def _profile_summary_text(summary: dict[str, Any]) -> str:
    parts = []
    if str(summary.get("user_background") or "").strip():
        parts.append("background: " + str(summary.get("user_background") or "").strip())
    if str(summary.get("beekeeping_context") or "").strip():
        parts.append("context: " + str(summary.get("beekeeping_context") or "").strip())
    if str(summary.get("experience_level") or "").strip():
        parts.append("experience: " + str(summary.get("experience_level") or "").strip())
    if str(summary.get("communication_style") or "").strip():
        parts.append("style: " + str(summary.get("communication_style") or "").strip())
    preferences = _typed_values(summary.get("answer_preferences") or [], "preference")
    if preferences:
        parts.append("preferences: " + "; ".join(preferences[:6]))
    recurring_topics = _typed_values(summary.get("recurring_topics") or [], "topic")
    if recurring_topics:
        parts.append("topics: " + " | ".join(recurring_topics[:4]))
    learning_goals = _typed_values(summary.get("learning_goals") or [], "goal")
    if learning_goals:
        parts.append("goals: " + " | ".join(learning_goals[:4]))
    constraints = _typed_values(summary.get("persistent_constraints") or [], "constraint")
    if constraints:
        parts.append("constraints: " + "; ".join(constraints[:6]))
    preferred_document_ids = [str(item).strip() for item in (summary.get("preferred_document_ids") or []) if str(item).strip()]
    if preferred_document_ids:
        parts.append("preferred_docs: " + ", ".join(preferred_document_ids[:4]))
    return "\n".join(parts).strip()


def _coerce_profile_summary(payload: dict[str, Any]) -> dict[str, Any]:
    profile = {
        "summary_version": "v3",
        "user_background": str(payload.get("user_background") or "").strip()[:220],
        "beekeeping_context": str(payload.get("beekeeping_context") or "").strip()[:220],
        "experience_level": str(payload.get("experience_level") or "").strip()[:80],
        "communication_style": str(payload.get("communication_style") or "").strip()[:120],
        "answer_preferences": _normalize_named_items(payload.get("answer_preferences") or [], value_key="preference", limit=8, max_len=160, source_default="legacy"),
        "recurring_topics": _normalize_named_items(payload.get("recurring_topics") or [], value_key="topic", limit=8, max_len=180, source_default="legacy"),
        "learning_goals": _normalize_named_items(payload.get("learning_goals") or [], value_key="goal", limit=6, max_len=180, source_default="legacy"),
        "persistent_constraints": _normalize_constraint_items(payload.get("persistent_constraints") or [], limit=8, max_len=220),
        "preferred_document_ids": [str(item).strip() for item in (payload.get("preferred_document_ids") or []) if str(item).strip()][:8],
        "last_query": str(payload.get("last_query") or "").strip()[:220],
    }
    return _refresh_profile_summary_text(profile)


def _profile_summary_schema() -> dict[str, Any]:
    return {
        "type": "object",
        "additionalProperties": False,
        "properties": {
            "user_background": {"type": "string"},
            "beekeeping_context": {"type": "string"},
            "experience_level": {"type": "string"},
            "communication_style": {"type": "string"},
            "answer_preferences": {
                "type": "array",
                "items": {
                    "type": "object",
                    "additionalProperties": False,
                    "properties": {
                        "preference": {"type": "string"},
                        "source": {"type": "string"},
                    },
                    "required": ["preference", "source"],
                },
            },
            "recurring_topics": {
                "type": "array",
                "items": {
                    "type": "object",
                    "additionalProperties": False,
                    "properties": {
                        "topic": {"type": "string"},
                        "source": {"type": "string"},
                    },
                    "required": ["topic", "source"],
                },
            },
            "learning_goals": {
                "type": "array",
                "items": {
                    "type": "object",
                    "additionalProperties": False,
                    "properties": {
                        "goal": {"type": "string"},
                        "source": {"type": "string"},
                    },
                    "required": ["goal", "source"],
                },
            },
            "persistent_constraints": {
                "type": "array",
                "items": {
                    "type": "object",
                    "additionalProperties": False,
                    "properties": {
                        "constraint": {"type": "string"},
                        "kind": {"type": "string"},
                        "source": {"type": "string"},
                    },
                    "required": ["constraint", "kind", "source"],
                },
            },
            "preferred_document_ids": {"type": "array", "items": {"type": "string"}},
            "last_query": {"type": "string"},
            "summary_version": {"type": "string"},
        },
        "required": [
            "user_background",
            "beekeeping_context",
            "experience_level",
            "communication_style",
            "answer_preferences",
            "recurring_topics",
            "learning_goals",
            "persistent_constraints",
            "preferred_document_ids",
            "last_query",
            "summary_version",
        ],
    }


def _compact_recent_messages(messages: list[dict[str, Any]], limit: int) -> list[dict[str, str]]:
    if limit <= 0:
        return []
    compacted: list[dict[str, str]] = []
    for item in list(messages or [])[-limit:]:
        role = str(item.get("role") or "").strip().lower()
        content = sanitize_text(str(item.get("content") or "")).strip()
        if not role or not content:
            continue
        compacted.append({"role": role[:24], "content": content[:600]})
    return compacted


def _sanitize_profile_summary_payload(summary: dict[str, Any] | None) -> dict[str, Any]:
    if not isinstance(summary, dict):
        return {}
    allowed_fields = {
        "summary_version",
        "user_background",
        "beekeeping_context",
        "experience_level",
        "communication_style",
        "answer_preferences",
        "recurring_topics",
        "learning_goals",
        "persistent_constraints",
        "preferred_document_ids",
        "last_query",
        "summary_text",
    }
    return {key: _json_safe(value) for key, value in summary.items() if key in allowed_fields}


def _typed_item_value(item: Any, key: str) -> str:
    if isinstance(item, dict):
        return sanitize_text(str(item.get(key) or "")).strip()
    return sanitize_text(str(item or "")).strip()


def _typed_item_source(item: Any, default: str) -> str:
    if isinstance(item, dict):
        source = sanitize_text(str(item.get("source") or default)).strip()
        return (source or default)[:80]
    return default[:80]


def _normalize_named_items(
    values: list[Any],
    *,
    value_key: str,
    limit: int,
    max_len: int = 220,
    source_default: str = "legacy",
    kind_default: str | None = None,
) -> list[dict[str, str]]:
    if limit <= 0:
        return []
    normalized: list[dict[str, str]] = []
    seen: set[str] = set()
    for value in values:
        text = _typed_item_value(value, value_key)
        if not text:
            continue
        dedupe_key = text.lower()
        if dedupe_key in seen:
            continue
        seen.add(dedupe_key)
        item: dict[str, str] = {
            value_key: text[:max_len],
            "source": _typed_item_source(value, source_default),
        }
        item_kind = ""
        if isinstance(value, dict):
            item_kind = sanitize_text(str(value.get("kind") or "")).strip()[:80]
        if not item_kind and kind_default:
            item_kind = kind_default[:80]
        if item_kind:
            item["kind"] = item_kind
        normalized.append(item)
        if len(normalized) >= limit:
            break
    return normalized


def _normalize_string_list(values: list[Any], *, limit: int, max_len: int = 220) -> list[str]:
    if limit <= 0:
        return []
    normalized: list[str] = []
    seen: set[str] = set()
    for value in values:
        text = sanitize_text(str(value or "")).strip()
        if not text:
            continue
        key = text.lower()
        if key in seen:
            continue
        seen.add(key)
        normalized.append(text[:max_len])
        if len(normalized) >= limit:
            break
    return normalized


def _constraint_label(item: Any) -> str:
    return _typed_item_value(item, "constraint")


def _constraint_kind_for_text(text: str) -> str:
    lowered = sanitize_text(text).strip().lower()
    if lowered.startswith("document_scope:"):
        return "document_scope"
    if lowered.startswith("scope:"):
        return "scope_signature"
    return "session"


def _normalize_constraint_items(values: list[Any], *, limit: int, max_len: int = 220) -> list[dict[str, str]]:
    normalized: list[dict[str, str]] = []
    seen: set[str] = set()
    for value in values:
        text = _constraint_label(value)
        if not text:
            continue
        key = text.lower()
        if key in seen:
            continue
        seen.add(key)
        kind = _constraint_kind_for_text(text)
        if isinstance(value, dict):
            kind = sanitize_text(str(value.get("kind") or kind)).strip()[:80] or kind
        normalized.append(
            {
                "constraint": text[:max_len],
                "kind": kind,
                "source": _typed_item_source(value, "request_scope"),
            }
        )
        if len(normalized) >= limit:
            break
    return normalized


def _thread_label_text(item: Any) -> str:
    return _typed_item_value(item, "thread")


def _normalize_thread_items(
    values: list[Any],
    *,
    limit: int,
    expiry_policy: str,
    source_default: str,
) -> list[dict[str, str]]:
    normalized: list[dict[str, str]] = []
    seen: set[str] = set()
    for value in values:
        text = _thread_label_text(value)
        if not text:
            continue
        key = text.lower()
        if key in seen:
            continue
        seen.add(key)
        source_query = ""
        question_type = ""
        item_expiry_policy = expiry_policy
        if isinstance(value, dict):
            source_query = sanitize_text(str(value.get("source_query") or "")).strip()[:220]
            question_type = sanitize_text(str(value.get("question_type") or "")).strip()[:80]
            item_expiry_policy = sanitize_text(str(value.get("expiry_policy") or expiry_policy)).strip()[:80] or expiry_policy
        normalized.append(
            {
                "thread": text[:220],
                "source": _typed_item_source(value, source_default),
                "source_query": source_query,
                "question_type": question_type,
                "expiry_policy": item_expiry_policy,
            }
        )
        if len(normalized) >= limit:
            break
    return normalized


def _normalize_memory_fact_items(values: list[Any], *, limit: int) -> list[dict[str, Any]]:
    normalized: list[dict[str, Any]] = []
    seen: set[str] = set()
    for value in values:
        if not isinstance(value, dict):
            value = {"fact": value}
        fact_text = sanitize_text(str(value.get("fact") or "")).strip()[:320]
        if not fact_text:
            continue
        key = fact_text.lower()
        if key in seen:
            continue
        chunk_ids = [str(item).strip() for item in (value.get("chunk_ids") or []) if str(item).strip()][:8]
        asset_ids = [str(item).strip() for item in (value.get("asset_ids") or []) if str(item).strip()][:8]
        assertion_ids = [str(item).strip() for item in (value.get("assertion_ids") or []) if str(item).strip()][:8]
        evidence_ids = [str(item).strip() for item in (value.get("evidence_ids") or []) if str(item).strip()][:8]
        source_type = sanitize_text(str(value.get("source_type") or "legacy")).strip()[:80] or "legacy"
        support_present = bool(chunk_ids or asset_ids or assertion_ids or evidence_ids)
        if source_type != "user_self_report" and not support_present:
            continue
        confidence_raw = value.get("confidence")
        try:
            confidence = float(confidence_raw)
        except (TypeError, ValueError):
            confidence = 1.0 if source_type == "user_self_report" else (0.8 if support_present else 0.0)
        confidence = max(0.0, min(1.0, confidence))
        review_policy = sanitize_text(
            str(
                value.get("review_policy")
                or ("persist_until_explicit_change" if source_type == "user_self_report" else "revalidate_on_missing_support")
            )
        ).strip()[:120]
        fact_type = sanitize_text(str(value.get("fact_type") or "grounded_fact")).strip()[:80] or "grounded_fact"
        normalized.append(
            {
                "fact": fact_text,
                "fact_type": fact_type,
                "source_type": source_type,
                "confidence": round(confidence, 4),
                "review_policy": review_policy,
                "chunk_ids": chunk_ids,
                "asset_ids": asset_ids,
                "assertion_ids": assertion_ids,
                "evidence_ids": evidence_ids,
            }
        )
        seen.add(key)
        if len(normalized) >= limit:
            break
    return normalized


def _typed_values(items: list[Any], key: str) -> list[str]:
    values: list[str] = []
    for item in items:
        text = _typed_item_value(item, key)
        if text:
            values.append(text)
    return values


def _retain_reaffirmed_named_items(
    prior_items: list[Any],
    recent_items: list[Any],
    *,
    value_key: str,
    sticky_sources: set[str],
) -> list[Any]:
    recent_values = {
        _typed_item_value(item, value_key).strip().lower()
        for item in recent_items
        if _typed_item_value(item, value_key).strip()
    }
    retained: list[Any] = []
    for item in prior_items:
        value = _typed_item_value(item, value_key).strip().lower()
        if not value:
            continue
        source = _typed_item_source(item, "legacy").strip().lower()
        if source in sticky_sources or value in recent_values:
            retained.append(item)
    return retained


def _retain_relevant_open_threads(prior_threads: list[Any], question: str, *, keep_unmatched: int = 1) -> list[Any]:
    retained: list[Any] = []
    unmatched_kept = 0
    for item in prior_threads:
        if _thread_matches_question(item, question):
            retained.append(item)
            continue
        if unmatched_kept < keep_unmatched:
            retained.append(item)
            unmatched_kept += 1
    return retained


def _scope_signature(request_scope: dict[str, Any]) -> str:
    tenant_id = str(request_scope.get("tenant_id") or "").strip()
    document_ids = [str(item).strip() for item in (request_scope.get("document_ids") or []) if str(item).strip()]
    if not tenant_id and not document_ids:
        return ""
    if document_ids:
        return f"scope:{tenant_id}:{','.join(document_ids[:8])}"
    return f"scope:{tenant_id}:all"


def _summarize_answer_fact(question: str, answer: str) -> str:
    cleaned = sanitize_text(answer).strip()
    if not cleaned:
        return sanitize_text(question).strip()[:220]
    sentences = re.split(r"(?<=[\.\!\?])\s+", cleaned)
    summary = " ".join(sentence.strip() for sentence in sentences[:2] if sentence.strip()).strip()
    return (summary or cleaned)[:320]


def _split_answer_into_facts(
    answer: str,
    *,
    chunk_ids: list[str],
    asset_ids: list[str],
    assertion_ids: list[str],
    limit: int,
) -> list[dict[str, Any]]:
    cleaned = sanitize_text(answer).strip()
    if not cleaned:
        return []
    sentences = [sentence.strip() for sentence in re.split(r"(?<=[\.\!\?])\s+", cleaned) if sentence.strip()]
    facts: list[dict[str, Any]] = []
    for sentence in sentences[:limit]:
        facts.append(
            {
                "fact": sentence[:320],
                "chunk_ids": list(chunk_ids),
                "asset_ids": list(asset_ids),
                "assertion_ids": list(assertion_ids),
            }
        )
    if not facts:
        facts.append(
            {
                "fact": cleaned[:320],
                "chunk_ids": list(chunk_ids),
                "asset_ids": list(asset_ids),
                "assertion_ids": list(assertion_ids),
            }
        )
    return facts


def _thread_matches_question(thread: str, question: str) -> bool:
    thread_text = _thread_label_text(thread)
    thread_tokens = set(_derive_topic_keywords([thread_text], limit=6))
    question_tokens = set(_derive_topic_keywords([question], limit=8))
    return bool(thread_tokens and question_tokens and len(thread_tokens & question_tokens) >= max(1, min(len(thread_tokens), 2)))


def _resolved_thread_label(question: str, response: dict[str, Any]) -> str:
    prefix = "resolved"
    if response.get("abstained"):
        prefix = "unresolved"
    question_label = sanitize_text(question).strip()[:180]
    return f"{prefix}: {question_label}".strip()


def _derive_preference_hints_from_messages(messages: list[dict[str, Any]]) -> list[str]:
    text = " ".join(sanitize_text(str(item.get("content") or "")) for item in messages if str(item.get("role") or "").lower() == "user").lower()
    hints: list[str] = []
    pattern_map = {
        "prefer_step_by_step": ("step by step", "steps", "walk me through"),
        "prefer_brief_answers": ("brief", "short answer", "keep it short"),
        "prefer_technical_depth": ("technical", "really technical", "deeply technical", "detailed"),
        "prefer_simple_language": ("simple", "plain language", "easy to understand"),
        "prefer_explicit_citations": ("cite", "citations", "sources", "evidence"),
        "prefer_visual_evidence": ("image", "images", "figure", "diagram", "visual"),
    }
    for hint, patterns in pattern_map.items():
        if any(pattern in text for pattern in patterns):
            hints.append(hint)
    return hints


def _derive_topic_keywords(texts: list[str], limit: int) -> list[str]:
    counts: dict[str, int] = {}
    for text in texts:
        for token in re.findall(r"[a-z0-9][a-z0-9\-]{2,}", sanitize_text(text).lower()):
            if token in _MEMORY_KEYWORD_STOPWORDS or token.isdigit():
                continue
            counts[token] = counts.get(token, 0) + 1
    ranked = sorted(counts.items(), key=lambda item: (-item[1], item[0]))
    return [token for token, _ in ranked[:limit]]


def _memory_text_matches_query(text: str, query_terms: set[str]) -> bool:
    if not text or not query_terms:
        return False
    text_terms = set(_derive_topic_keywords([text], limit=8))
    return bool(text_terms & query_terms)


def _support_ids_overlap(item: dict[str, Any], allowed_ids: set[str], key: str) -> bool:
    values = {str(value).strip() for value in (item.get(key) or []) if str(value).strip()}
    return bool(values & allowed_ids)


def _filter_session_summary_for_prompt(
    session_summary: dict[str, Any] | None,
    *,
    question: str,
    normalized_query: str,
    bundle: AgentContextBundle,
) -> dict[str, Any] | None:
    if not session_summary:
        return None
    query_terms = set(_derive_topic_keywords([question, normalized_query], limit=10))
    chunk_ids = {str(item.get("chunk_id") or "").strip() for item in (bundle.chunks or []) if str(item.get("chunk_id") or "").strip()}
    assertion_ids = {str(item.get("assertion_id") or "").strip() for item in (bundle.assertions or []) if str(item.get("assertion_id") or "").strip()}
    evidence_ids = {str(item.get("evidence_id") or "").strip() for item in (bundle.evidence or []) if str(item.get("evidence_id") or "").strip()}
    stable_facts = []
    for item in list(session_summary.get("stable_facts") or []):
        if not isinstance(item, dict):
            continue
        if (
            _memory_text_matches_query(str(item.get("fact") or ""), query_terms)
            or _support_ids_overlap(item, chunk_ids, "chunk_ids")
            or _support_ids_overlap(item, assertion_ids, "assertion_ids")
            or _support_ids_overlap(item, evidence_ids, "evidence_ids")
        ):
            stable_facts.append(item)
    open_threads = [item for item in list(session_summary.get("open_threads") or []) if _thread_matches_question(item, question)]
    resolved_threads = [item for item in list(session_summary.get("resolved_threads") or []) if _thread_matches_question(item, question)]
    topic_keywords = [item for item in list(session_summary.get("topic_keywords") or []) if str(item).strip().lower() in query_terms]
    filtered = {
        **session_summary,
        "stable_facts": stable_facts,
        "open_threads": open_threads,
        "resolved_threads": resolved_threads,
        "topic_keywords": topic_keywords,
    }
    filtered.pop("summary_text", None)
    return filtered


def _filter_profile_summary_for_prompt(
    profile_summary: dict[str, Any] | None,
    *,
    question: str,
    normalized_query: str,
    workspace_kind: str = "general",
) -> dict[str, Any] | None:
    if not profile_summary:
        return None
    query_terms = set(_derive_topic_keywords([question, normalized_query], limit=10))
    normalized_workspace_kind = str(workspace_kind or "general").strip().lower()
    recurring_topics = [
        item
        for item in list(profile_summary.get("recurring_topics") or [])
        if _memory_text_matches_query(_typed_item_value(item, "topic"), query_terms)
    ]
    learning_goals = [
        item
        for item in list(profile_summary.get("learning_goals") or [])
        if _memory_text_matches_query(_typed_item_value(item, "goal"), query_terms)
    ]
    if normalized_workspace_kind == "general":
        recurring_topics = []
        learning_goals = []
    filtered = {
        **profile_summary,
        "answer_preferences": [],
        "recurring_topics": recurring_topics,
        "learning_goals": learning_goals,
    }
    filtered.pop("summary_text", None)
    return filtered


def _derive_session_goal(question: str, recent_messages: list[dict[str, Any]]) -> str:
    recent_user_messages = [
        sanitize_text(str(item.get("content") or "")).strip()
        for item in recent_messages
        if str(item.get("role") or "").lower() == "user"
    ]
    if recent_user_messages:
        return recent_user_messages[-1][:220]
    return sanitize_text(question).strip()[:220]


def _select_session_goal(prior_goal: str, current_goal: str, recent_messages: list[dict[str, Any]]) -> str:
    prior_goal = sanitize_text(prior_goal).strip()[:220]
    current_goal = sanitize_text(current_goal).strip()[:220]
    if not prior_goal:
        return current_goal or _derive_session_goal(current_goal, recent_messages)
    if current_goal and not _thread_matches_question(prior_goal, current_goal):
        return current_goal
    return prior_goal


def _stable_fact_priority(item: dict[str, Any]) -> tuple[int, int, int, int]:
    chunk_ids = [str(value).strip() for value in (item.get("chunk_ids") or []) if str(value).strip()]
    asset_ids = [str(value).strip() for value in (item.get("asset_ids") or []) if str(value).strip()]
    assertion_ids = [str(value).strip() for value in (item.get("assertion_ids") or []) if str(value).strip()]
    evidence_ids = [str(value).strip() for value in (item.get("evidence_ids") or []) if str(value).strip()]
    source_type = sanitize_text(str(item.get("source_type") or "")).strip().lower()
    confidence = float(item.get("confidence") or 0.0)
    fact = sanitize_text(str(item.get("fact") or "")).strip()
    return (
        2 if source_type == "user_self_report" else (1 if chunk_ids or asset_ids or assertion_ids or evidence_ids else 0),
        int(round(confidence * 1000)),
        len(assertion_ids),
        len(chunk_ids) + len(asset_ids) + len(evidence_ids),
        len(fact),
    )


def _constraint_priority(value: str) -> tuple[int, int]:
    text = _constraint_label(value).lower()
    return (
        2 if text.startswith("document_scope:") or text.startswith("scope:") else 1,
        len(text),
    )


def _dedupe_stable_facts(facts: list[dict[str, Any]], *, limit: int) -> list[dict[str, Any]]:
    deduped: list[dict[str, Any]] = []
    seen: set[str] = set()
    for item in facts:
        fact_text = sanitize_text(str((item or {}).get("fact") or "")).strip()
        if not fact_text:
            continue
        key = fact_text.lower()
        if key in seen:
            continue
        seen.add(key)
        deduped.append(item)
    deduped.sort(key=_stable_fact_priority, reverse=True)
    return deduped[:limit]


def _evidence_backed_facts(
    *,
    bundle: AgentContextBundle,
    used_chunk_ids: list[str],
    used_asset_ids: list[str],
    assertion_ids: list[str],
    evidence_ids: list[str],
    limit: int,
) -> list[dict[str, Any]]:
    chunk_map = {row["chunk_id"]: row for row in bundle.chunks}
    asset_map = {row["asset_id"]: row for row in bundle.assets}
    facts: list[dict[str, Any]] = []
    for chunk_id in used_chunk_ids:
        chunk = chunk_map.get(chunk_id)
        if not chunk:
            continue
        facts.append(
            {
                "fact": _summarize_answer_fact("", str(chunk.get("text") or "")),
                "fact_type": "grounded_answer",
                "source_type": "retrieval_grounded",
                "confidence": 0.85,
                "review_policy": "revalidate_on_missing_support",
                "chunk_ids": [chunk_id],
                "asset_ids": [],
                "assertion_ids": list(assertion_ids[:4]),
                "evidence_ids": list(evidence_ids[:4]),
            }
        )
        if len(facts) >= limit:
            return facts[:limit]
    for asset_id in used_asset_ids:
        asset = asset_map.get(asset_id)
        trusted_text = _trusted_asset_grounding_text(asset or {})
        if not asset or not trusted_text:
            continue
        facts.append(
            {
                "fact": _summarize_answer_fact("", trusted_text),
                "fact_type": "grounded_answer",
                "source_type": "retrieval_grounded",
                "confidence": 0.8,
                "review_policy": "revalidate_on_missing_support",
                "chunk_ids": [],
                "asset_ids": [asset_id],
                "assertion_ids": list(assertion_ids[:4]),
                "evidence_ids": list(evidence_ids[:4]),
            }
        )
        if len(facts) >= limit:
            return facts[:limit]
    return facts[:limit]


def _derive_communication_style(messages: list[dict[str, Any]], preferences: list[str] | list[dict[str, Any]]) -> str:
    text = " ".join(sanitize_text(str(item.get("content") or "")) for item in messages if str(item.get("role") or "").lower() == "user").lower()
    preference_values = [value.lower() for value in _typed_values(preferences, "preference")]
    if "prefer_technical_depth" in preference_values:
        return "technical"
    if "prefer_simple_language" in preference_values:
        return "plain"
    if "prefer_brief_answers" in preference_values:
        return "concise"
    if "step by step" in text or "walk me through" in text:
        return "procedural"
    return ""


def _build_model_json_payload(
    *,
    model: str,
    reasoning_effort: str,
    system_prompt: str,
    user_payload: dict[str, Any],
    schema_name: str,
    schema: dict[str, Any],
    temperature: float,
    max_completion_tokens: int,
) -> dict[str, Any]:
    payload = {
        "model": model,
        "reasoning_effort": reasoning_effort,
        "messages": [
            {"role": "system", "content": system_prompt},
            {"role": "user", "content": json.dumps(_json_safe(user_payload), ensure_ascii=False)},
        ],
        "response_format": {
            "type": "json_schema",
            "json_schema": {
                "name": schema_name,
                "strict": True,
                "schema": schema,
            },
        },
    }
    if temperature > 0 and _supports_temperature(model, reasoning_effort):
        payload["temperature"] = temperature
    if max_completion_tokens > 0:
        payload["max_completion_tokens"] = max_completion_tokens
    return payload


def _call_openai_json_payload(
    *,
    base_url: str,
    api_key: str,
    payload: dict[str, Any],
    timeout_seconds: float,
) -> dict[str, Any]:
    headers = {
        "Authorization": f"Bearer {api_key}",
        "Content-Type": "application/json",
    }
    with httpx.Client(timeout=timeout_seconds) as client:
        response = client.post(f"{base_url}/chat/completions", headers=headers, json=payload)
        response.raise_for_status()
        return response.json()


def _normalize_query(question: str) -> str:
    return re.sub(r"\s+", " ", question.replace("\x00", " ").strip())


def _build_session_title(question: str) -> str:
    return question[:80]


def _classify_question(question: str) -> str:
    return classify_question_fallback(question)


def _resolve_query_top_k(
    question_type: str,
    requested_top_k: int | None,
    runtime_config: dict[str, Any],
    router_top_k: int | None = None,
    router_source: str | None = None,
) -> tuple[int, str]:
    if requested_top_k is not None:
        return max(1, min(int(requested_top_k), runtime_config["max_top_k"])), "manual"
    if router_top_k is not None and router_source and router_source.startswith("router"):
        return max(1, min(int(router_top_k), runtime_config["max_top_k"])), router_source
    if runtime_config.get("dynamic_top_k_enabled", True):
        policy_key = f"{question_type}_top_k"
        policy_value = runtime_config.get(policy_key, runtime_config["default_top_k"])
        return max(1, min(int(policy_value), runtime_config["max_top_k"])), f"dynamic:{question_type}"
    return max(1, min(int(runtime_config["default_top_k"]), runtime_config["max_top_k"])), "default"


def _select_retrieval_plan(
    question_type: str,
    top_k: int,
    runtime_config: dict[str, Any],
    routing: dict[str, Any] | None = None,
) -> dict[str, Any]:
    routing = routing or {}
    max_search_k = runtime_config["max_search_k"]
    prefer_assets = bool(routing.get("requires_visual")) or question_type == "visual_lookup"
    document_spread = str(routing.get("document_spread") or "").strip().lower()
    if document_spread not in {"single", "few", "broad"}:
        if question_type == "comparison":
            document_spread = "few"
        elif question_type in {"fact", "explanation", "procedure"}:
            document_spread = "few"
        else:
            document_spread = "single"
    if question_type == "visual_lookup":
        return {
            "mode": "multimodal_focus",
            "search_k": min(max(top_k * 2, top_k + 4), max_search_k),
            "select_k": min(max(3, top_k), runtime_config["max_context_chunks"]),
            "expand_neighbors": False,
            "kg_search": False,
            "graph_expand": False,
            "prefer_assets": True,
            "asset_vector_search": True,
            "document_spread": "few" if document_spread == "single" else document_spread,
        }
    if question_type in {"definition", "fact", "source_lookup"}:
        return {
            "mode": "hybrid_visual_support" if prefer_assets else "hybrid_kg_support",
            "search_k": min(max(top_k * (3 if prefer_assets else 2), top_k + (6 if prefer_assets else 4)), max_search_k),
            "select_k": min(max(4, top_k), runtime_config["max_context_chunks"]),
            "expand_neighbors": False,
            "kg_search": runtime_config["kg_search_limit"] > 0,
            "graph_expand": False,
            "prefer_assets": prefer_assets,
            "asset_vector_search": prefer_assets,
            "document_spread": document_spread,
        }
    if question_type == "comparison":
        return {
            "mode": "hybrid_compare",
            "search_k": min(max(top_k * 2, top_k + 4), max_search_k),
            "select_k": min(max(5, top_k), runtime_config["max_context_chunks"]),
            "expand_neighbors": True,
            "kg_search": runtime_config["kg_search_limit"] > 0,
            "graph_expand": runtime_config["graph_expansion_limit"] > 0,
            "prefer_assets": prefer_assets,
            "asset_vector_search": prefer_assets,
            "document_spread": "few" if document_spread == "single" else document_spread,
        }
    return {
        "mode": "neighbor_visual_expansion" if prefer_assets else "neighbor_expansion",
        "search_k": min(max(top_k * (3 if prefer_assets else 2), top_k + (6 if prefer_assets else 3)), max_search_k),
        "select_k": min(max(4, top_k), runtime_config["max_context_chunks"]),
        "expand_neighbors": True,
        "kg_search": runtime_config["kg_search_limit"] > 0,
        "graph_expand": question_type in {"explanation", "procedure"} and runtime_config["graph_expansion_limit"] > 0,
        "prefer_assets": prefer_assets,
        "asset_vector_search": prefer_assets,
        "document_spread": document_spread,
    }


def _merge_hybrid_matches(
    dense_matches: list[dict[str, Any]],
    lexical_matches: list[dict[str, Any]],
    *,
    id_key: str,
) -> list[dict[str, Any]]:
    merged: dict[str, dict[str, Any]] = {}

    def _merge_one(match: dict[str, Any], source: str) -> None:
        record_id = str(match.get(id_key) or "")
        if not record_id:
            return
        current = merged.get(record_id)
        match_sources = list(match.get("match_sources") or [])
        if source not in match_sources:
            match_sources.append(source)
        if current is None:
            item = dict(match)
            item["match_source"] = source
            item["match_sources"] = match_sources
            merged[record_id] = item
            return
        if current.get("distance") is None and match.get("distance") is not None:
            current["distance"] = match.get("distance")
        if not current.get("document") and match.get("document"):
            current["document"] = match.get("document")
        current_metadata = dict(current.get("metadata") or {})
        incoming_metadata = dict(match.get("metadata") or {})
        current["metadata"] = {**current_metadata, **incoming_metadata}
        current["rank"] = min(int(current.get("rank") or 10**6), int(match.get("rank") or 10**6))
        current["lexical_score"] = max(
            float(current.get("lexical_score") or 0.0),
            float(match.get("lexical_score") or 0.0),
        )
        combined_sources = list(dict.fromkeys(list(current.get("match_sources") or []) + match_sources))
        current["match_sources"] = combined_sources
        current["match_source"] = "+".join(combined_sources)

    for match in dense_matches:
        _merge_one(match, "dense")
    for match in lexical_matches:
        _merge_one(match, "lexical")
    return list(merged.values())


def _rerank_matches(normalized_query: str, raw_matches: list[dict[str, Any]], runtime_config: dict[str, Any]) -> list[dict[str, Any]]:
    if not raw_matches:
        return []
    query_terms = set(_query_terms(normalized_query))
    ranked: list[dict[str, Any]] = []
    for match in raw_matches:
        distance_score = _distance_to_score(match.get("distance")) or 0.0
        metadata = match.get("metadata", {}) or {}
        section = str(metadata.get("section_title") or metadata.get("section") or "")
        title = str(metadata.get("title") or "")
        text = str(match.get("document") or "")
        lexical = _lexical_overlap(query_terms, f"{section} {text}")
        section_bonus = _lexical_overlap(query_terms, section)
        title_bonus = _lexical_overlap(query_terms, title)
        exact_phrase_bonus = 1.0 if normalized_query.lower() in f"{title} {section} {text}".lower() else 0.0
        ontology_bonus = _metadata_overlap(query_terms, metadata.get("ontology_classes"))
        rerank_score = round(
            distance_score * runtime_config["rerank_distance_weight"]
            + lexical * runtime_config["rerank_lexical_weight"]
            + section_bonus * runtime_config["rerank_section_weight"]
            + title_bonus * runtime_config["rerank_title_weight"]
            + exact_phrase_bonus * runtime_config["rerank_exact_phrase_weight"]
            + ontology_bonus * runtime_config["rerank_ontology_weight"],
            6,
        )
        item = dict(match)
        item["rerank_score"] = rerank_score
        ranked.append(item)
    ranked.sort(key=lambda item: (item.get("rerank_score") or 0.0, item.get("rank") or 0), reverse=True)
    for index, item in enumerate(ranked, start=1):
        item["rank"] = index
    return ranked


def _rerank_asset_matches(normalized_query: str, raw_matches: list[dict[str, Any]], runtime_config: dict[str, Any]) -> list[dict[str, Any]]:
    if not raw_matches:
        return []
    query_terms = set(_query_terms(normalized_query))
    visual_query = _is_visual_query(normalized_query)
    ranked: list[dict[str, Any]] = []
    for match in raw_matches:
        distance_score = _distance_to_score(match.get("distance")) or 0.0
        metadata = match.get("metadata", {}) or {}
        label = str(metadata.get("label") or "")
        asset_type = str(metadata.get("asset_type") or "")
        page_hint = str(metadata.get("page_number") or "")
        text = str(match.get("document") or "")
        lexical = _lexical_overlap(query_terms, f"{label} {text}")
        exact_phrase_bonus = 1.0 if normalized_query.lower() in f"{label} {text}".lower() else 0.0
        ontology_bonus = _metadata_overlap(query_terms, metadata.get("important_terms"))
        asset_type_bonus = 0.08 if asset_type == "page_image" else 0.0
        page_bonus = 0.03 if page_hint and any(term == page_hint for term in query_terms) else 0.0
        visual_bonus = 0.0
        if visual_query:
            lowered_text = f"{label} {text}".lower()
            if asset_type == "page_image":
                visual_bonus += 0.08
            if any(token in lowered_text for token in ("scanned", "scan", "title page", "illustration", "diagram", "figure", "seal image", "engraving")):
                visual_bonus += 0.12
            if len(text.strip()) >= 120:
                visual_bonus += 0.04
        rerank_score = round(
            distance_score * runtime_config["rerank_distance_weight"]
            + lexical * runtime_config["rerank_lexical_weight"]
            + _lexical_overlap(query_terms, label) * runtime_config["rerank_title_weight"]
            + exact_phrase_bonus * runtime_config["rerank_exact_phrase_weight"]
            + ontology_bonus * runtime_config["rerank_ontology_weight"]
            + asset_type_bonus
            + page_bonus,
            6,
        )
        rerank_score = round(
            rerank_score + visual_bonus,
            6,
        )
        item = dict(match)
        item["rerank_score"] = rerank_score
        ranked.append(item)
    ranked.sort(key=lambda item: (item.get("rerank_score") or 0.0, item.get("rank") or 0), reverse=True)
    for index, item in enumerate(ranked, start=1):
        item["rank"] = index
    return ranked


def _compress_chunks(
    normalized_query: str,
    chunks: list[dict[str, Any]],
    runtime_config: dict[str, Any],
    question_type: str,
    document_spread: str = "few",
) -> list[dict[str, Any]]:
    query_terms = set(_query_terms(normalized_query))
    max_chunks = runtime_config["max_context_chunks"]
    scored_rows: list[tuple[float, dict[str, Any]]] = []
    for row in chunks:
        text = str(row.get("text") or "").strip()
        if not text:
            continue
        if len(text) < 45 and _lexical_overlap(query_terms, text) == 0.0:
            continue
        metadata = row.get("metadata_json", {}) or {}
        retrieval_score = float(row.get("_retrieval_score") or 0.0)
        kg_assertion_count = int(row.get("kg_assertion_count") or metadata.get("kg_assertion_count") or 0)
        kg_bonus = min(0.18, 0.05 * kg_assertion_count) if kg_assertion_count > 0 else 0.0
        linked_asset_bonus = min(0.12, 0.04 * int(metadata.get("linked_asset_count") or 0))
        score = (
            retrieval_score
            + _lexical_overlap(query_terms, text) * runtime_config["rerank_lexical_weight"]
            + _lexical_overlap(query_terms, str(metadata.get("section_title") or "")) * runtime_config["rerank_section_weight"]
            + _lexical_overlap(query_terms, str(metadata.get("title") or "")) * runtime_config["rerank_title_weight"]
            + _metadata_overlap(query_terms, metadata.get("ontology_classes")) * runtime_config["rerank_ontology_weight"]
            + kg_bonus
            + linked_asset_bonus
        )
        scored_rows.append((score, row))
    scored_rows.sort(key=lambda item: item[0], reverse=True)
    if question_type != "comparison":
        document_scores: dict[str, float] = {}
        for score, row in scored_rows:
            document_id = str(row.get("document_id") or "")
            if not document_id:
                continue
            document_scores[document_id] = document_scores.get(document_id, 0.0) + max(0.05, score)
        ranked_documents = sorted(document_scores.items(), key=lambda item: item[1], reverse=True)
        if ranked_documents:
            if document_spread == "broad":
                allowed_documents = {item[0] for item in ranked_documents[: min(4, len(ranked_documents))]}
            elif question_type == "visual_lookup":
                if len(ranked_documents) > 1 and ranked_documents[0][1] < ranked_documents[1][1] * 1.25:
                    allowed_documents = {item[0] for item in ranked_documents[:2]}
                else:
                    allowed_documents = {ranked_documents[0][0]}
            elif len(ranked_documents) == 1:
                allowed_documents = {ranked_documents[0][0]}
            elif document_spread == "single":
                allowed_documents = {ranked_documents[0][0]}
            elif document_spread == "few":
                allowed_documents = {item[0] for item in ranked_documents[: min(3, len(ranked_documents))]}
            elif question_type in {"definition", "source_lookup"} and ranked_documents[0][1] >= ranked_documents[1][1] * 1.2:
                allowed_documents = {ranked_documents[0][0]}
            elif question_type in {"definition", "source_lookup"}:
                allowed_documents = {item[0] for item in ranked_documents[:2]}
            elif question_type in {"fact", "explanation", "procedure"} and len(ranked_documents) >= 3:
                allowed_documents = {item[0] for item in ranked_documents[:3]}
            else:
                allowed_documents = {item[0] for item in ranked_documents[:2]}
            scored_rows = [item for item in scored_rows if str(item[1].get("document_id") or "") in allowed_documents]

    kept: list[dict[str, Any]] = []
    seen_signatures: set[str] = set()
    for _, row in scored_rows:
        signature = re.sub(r"\s+", " ", str(row.get("text") or "").lower())[:280]
        if signature in seen_signatures:
            continue
        if kept:
            max_overlap = max(_text_jaccard(signature, re.sub(r"\s+", " ", str(item.get("text") or "").lower())[:280]) for item in kept)
            if max_overlap >= runtime_config["diversity_penalty"]:
                continue
        seen_signatures.add(signature)
        kept.append(row)
        if len(kept) >= max_chunks:
            break
    return kept


def _compress_assets(
    normalized_query: str,
    assets: list[dict[str, Any]],
    runtime_config: dict[str, Any],
    question_type: str,
    document_spread: str = "few",
) -> list[dict[str, Any]]:
    query_terms = set(_query_terms(normalized_query))
    visual_query = _is_visual_query(normalized_query)
    max_assets = runtime_config["max_context_assets"]
    scored_rows: list[tuple[float, dict[str, Any]]] = []
    for row in assets:
        search_text = str(row.get("search_text") or "").strip()
        if not search_text:
            continue
        retrieval_score = float(row.get("_retrieval_score") or 0.0)
        if (
            len(search_text) < settings.asset_embedding_min_chars
            and _lexical_overlap(query_terms, search_text) == 0.0
            and retrieval_score <= 0.0
            and str(row.get("asset_type") or "") != "page_image"
        ):
            continue
        metadata = row.get("metadata_json", {}) or {}
        visual_bonus = 0.0
        link_support_bonus = min(0.28, float(row.get("_link_support_score") or 0.0))
        if visual_query:
            if bool(metadata.get("vision_used")):
                visual_bonus += 0.14
            if row.get("asset_type") == "page_image":
                visual_bonus += 0.08
            lowered_text = search_text.lower()
            if any(token in lowered_text for token in ("scanned", "scan", "title page", "illustration", "diagram", "figure", "seal image", "engraving")):
                visual_bonus += 0.12
            if len(search_text) >= 120:
                visual_bonus += 0.05
        score = (
            retrieval_score
            + _lexical_overlap(query_terms, search_text) * runtime_config["rerank_lexical_weight"]
            + _lexical_overlap(query_terms, str(metadata.get("label") or "")) * runtime_config["rerank_title_weight"]
            + _metadata_overlap(query_terms, metadata.get("important_terms")) * runtime_config["rerank_ontology_weight"]
            + (0.08 if row.get("asset_type") == "page_image" else 0.0)
            + link_support_bonus
            + visual_bonus
        )
        scored_rows.append((score, row))
    scored_rows.sort(key=lambda item: item[0], reverse=True)
    document_scores: dict[str, float] = {}
    for score, row in scored_rows:
        document_id = str(row.get("document_id") or "")
        if not document_id:
            continue
        document_scores[document_id] = document_scores.get(document_id, 0.0) + max(0.05, score)
    ranked_documents = sorted(document_scores.items(), key=lambda item: item[1], reverse=True)
    if ranked_documents:
        if document_spread == "broad":
            allowed_documents = {item[0] for item in ranked_documents[: min(4, len(ranked_documents))]}
        elif question_type == "visual_lookup":
            if len(ranked_documents) > 1 and ranked_documents[0][1] < ranked_documents[1][1] * 1.25:
                allowed_documents = {item[0] for item in ranked_documents[:2]}
            else:
                allowed_documents = {ranked_documents[0][0]}
        elif len(ranked_documents) == 1:
            allowed_documents = {ranked_documents[0][0]}
        elif document_spread == "single":
            allowed_documents = {ranked_documents[0][0]}
        elif document_spread == "few":
            allowed_documents = {item[0] for item in ranked_documents[: min(3, len(ranked_documents))]}
        elif question_type in {"definition", "source_lookup"} and ranked_documents[0][1] >= ranked_documents[1][1] * 1.2:
            allowed_documents = {ranked_documents[0][0]}
        elif question_type in {"definition", "source_lookup"}:
            allowed_documents = {item[0] for item in ranked_documents[:2]}
        elif question_type in {"fact", "explanation", "procedure"} and len(ranked_documents) >= 3:
            allowed_documents = {item[0] for item in ranked_documents[:3]}
        else:
            allowed_documents = {item[0] for item in ranked_documents[:2]}
        scored_rows = [item for item in scored_rows if str(item[1].get("document_id") or "") in allowed_documents]

    kept: list[dict[str, Any]] = []
    seen_signatures: set[str] = set()
    for _, row in scored_rows:
        signature = re.sub(r"\s+", " ", str(row.get("search_text") or "").lower())[:220]
        if signature in seen_signatures:
            continue
        if kept:
            max_overlap = max(
                _text_jaccard(signature, re.sub(r"\s+", " ", str(item.get("search_text") or "").lower())[:220])
                for item in kept
            )
            if max_overlap >= runtime_config["diversity_penalty"]:
                continue
        seen_signatures.add(signature)
        kept.append(row)
        if len(kept) >= max_assets:
            break
    return kept


def _select_diverse_match_ids(
    raw_matches: list[dict[str, Any]],
    select_k: int,
    runtime_config: dict[str, Any],
) -> list[str]:
    if select_k <= 0:
        return []
    selected: list[dict[str, Any]] = []
    for match in raw_matches:
        chunk_id = str(match.get("chunk_id") or "")
        if not chunk_id or any(str(item.get("chunk_id") or "") == chunk_id for item in selected):
            continue
        score = float(match.get("rerank_score") or 0.0)
        match_text = str(match.get("document") or match.get("text") or "")
        if selected:
            max_overlap = max(_text_jaccard(match_text, str(item.get("document") or item.get("text") or "")) for item in selected)
            score -= runtime_config["diversity_penalty"] * max_overlap
        candidate = dict(match)
        candidate["selection_score"] = score
        selected.append(candidate)
        selected.sort(key=lambda item: float(item.get("selection_score") or 0.0), reverse=True)
        if len(selected) > select_k:
            selected = selected[:select_k]
    return [str(item.get("chunk_id") or "") for item in selected if str(item.get("chunk_id") or "")]


def _select_diverse_asset_ids(raw_matches: list[dict[str, Any]], select_k: int) -> list[str]:
    if select_k <= 0:
        return []
    selected: list[dict[str, Any]] = []
    seen_docs: set[tuple[str, int, str]] = set()
    for match in raw_matches:
        asset_id = str(match.get("asset_id") or "")
        metadata = match.get("metadata", {}) or {}
        signature = (
            str(metadata.get("document_id") or ""),
            int(metadata.get("page_number") or 0),
            str(metadata.get("asset_type") or ""),
        )
        if not asset_id or any(str(item.get("asset_id") or "") == asset_id for item in selected):
            continue
        if signature in seen_docs and len(selected) >= max(2, select_k // 2):
            continue
        score = float(match.get("rerank_score") or 0.0)
        match_text = str(match.get("document") or "")
        if selected:
            max_overlap = max(_text_jaccard(match_text, str(item.get("document") or "")) for item in selected)
            score -= 0.18 * max_overlap
        candidate = dict(match)
        candidate["selection_score"] = score
        selected.append(candidate)
        selected.sort(key=lambda item: float(item.get("selection_score") or 0.0), reverse=True)
        if len(selected) > select_k:
            selected = selected[:select_k]
        seen_docs.add(signature)
    return [str(item.get("asset_id") or "") for item in selected if str(item.get("asset_id") or "")]


def _filter_chunks_for_asset_scope(
    chunks: list[dict[str, Any]],
    assets: list[dict[str, Any]],
    strict_pages: bool,
) -> list[dict[str, Any]]:
    asset_pages = {
        (str(asset.get("document_id") or ""), int(asset.get("page_number") or 0))
        for asset in assets
        if str(asset.get("document_id") or "") and int(asset.get("page_number") or 0) > 0
    }
    asset_documents = {document_id for document_id, _ in asset_pages}
    filtered: list[dict[str, Any]] = []
    for row in chunks:
        document_id = str(row.get("document_id") or "")
        if document_id not in asset_documents:
            continue
        if not strict_pages:
            filtered.append(row)
            continue
        page_start = int(row.get("page_start") or 0)
        page_end = int(row.get("page_end") or page_start or 0)
        if any(asset_document_id == document_id and page_start <= asset_page <= page_end for asset_document_id, asset_page in asset_pages):
            filtered.append(row)
    return filtered


def _boost_visual_chunk_rows_from_assets(rows: list[dict[str, Any]], assets: list[dict[str, Any]]) -> None:
    asset_page_lookup = {
        (str(asset.get("document_id") or ""), int(asset.get("page_number") or 0)): float(asset.get("_retrieval_score") or 0.0)
        for asset in assets
        if str(asset.get("document_id") or "") and int(asset.get("page_number") or 0) > 0
    }
    for row in rows:
        document_id = str(row.get("document_id") or "")
        page_start = int(row.get("page_start") or 0)
        page_end = int(row.get("page_end") or page_start or 0)
        overlap_scores = [
            score
            for (asset_document_id, asset_page), score in asset_page_lookup.items()
            if asset_document_id == document_id and page_start <= asset_page <= page_end
        ]
        if overlap_scores:
            row["_retrieval_score"] = max(float(row.get("_retrieval_score") or 0.0), max(overlap_scores) + 0.1)


def _query_terms(text: str) -> list[str]:
    return [item for item in re.split(r"[^a-z0-9]+", text.lower()) if len(item) >= 3]


def _is_visual_query(text: str) -> bool:
    lowered = text.lower()
    return any(term in lowered for term in ("image", "visual", "diagram", "figure", "illustration", "scanned", "scan", "photo"))


def _normalize_query_mode(query_mode: str | None) -> str:
    value = str(query_mode or "auto").strip().lower()
    if value not in {"auto", "general", "sensor"}:
        return "auto"
    return value


def _resolve_system_prompt(runtime_config: dict[str, Any], system_prompt_variant: str) -> str:
    if system_prompt_variant == "sensor":
        return str(runtime_config.get("sensor_system_prompt") or runtime_config.get("system_prompt") or "").strip()
    return str(runtime_config.get("system_prompt") or "").strip()


def _resolve_system_prompt_variant(
    query_mode: str,
    *,
    include_sensor_context: bool,
    sensor_first: bool,
) -> str:
    if query_mode == "sensor":
        return "sensor"
    if query_mode == "general":
        return "general"
    return "sensor" if include_sensor_context or sensor_first else "general"


def _should_include_sensor_context(normalized_query: str, question_type: str, runtime_config: dict[str, Any]) -> bool:
    if not bool(runtime_config.get("sensor_context_enabled", False)):
        return False
    if int(runtime_config.get("max_context_sensor_readings") or 0) <= 0:
        return False
    lowered = normalized_query.lower()
    sensor_terms = (
        "sensor",
        "reading",
        "temperature",
        "humidity",
        "weight",
        "scale",
        "battery",
        "telemetry",
        "metric",
        "observation",
        "observed",
        "trend",
        "today",
        "yesterday",
        "now",
        "latest",
        "current",
        "recent",
        "apiary",
        "place",
        "hive",
    )
    if any(term in lowered for term in sensor_terms):
        return True
    if re.search(r"\b(sensor|hive|apiary|place)\s+#?\d+\b", lowered):
        return True
    return False


def _should_include_sensor_context_for_mode(
    query_mode: str,
    normalized_query: str,
    question_type: str,
    *,
    auth_user_id: str | None,
    runtime_config: dict[str, Any],
) -> bool:
    if not auth_user_id:
        return False
    if query_mode == "general":
        return False
    if query_mode == "sensor":
        return bool(runtime_config.get("sensor_context_enabled", False)) and int(
            runtime_config.get("max_context_sensor_readings") or 0
        ) > 0
    return _should_include_sensor_context(normalized_query, question_type, runtime_config)


def _should_use_sensor_only_context(
    normalized_query: str,
    question_type: str,
    *,
    auth_user_id: str | None,
    document_ids: list[str] | None,
    runtime_config: dict[str, Any],
) -> bool:
    if not auth_user_id or document_ids:
        return False
    if not _should_include_sensor_context(normalized_query, question_type, runtime_config):
        return False
    lowered = normalized_query.lower()
    sensor_first_terms = (
        "sensor",
        "reading",
        "temperature",
        "humidity",
        "weight",
        "scale",
        "battery",
        "telemetry",
        "trend",
        "current",
        "latest",
        "recent",
        "today",
        "yesterday",
        "now",
        "observed",
        "measured",
    )
    return any(term in lowered for term in sensor_first_terms)


def _should_use_sensor_only_context_for_mode(
    query_mode: str,
    normalized_query: str,
    question_type: str,
    *,
    auth_user_id: str | None,
    document_ids: list[str] | None,
    runtime_config: dict[str, Any],
) -> bool:
    if query_mode == "general":
        return False
    if query_mode == "sensor":
        return bool(auth_user_id) and not document_ids and bool(runtime_config.get("sensor_context_enabled", False))
    return _should_use_sensor_only_context(
        normalized_query,
        question_type,
        auth_user_id=auth_user_id,
        document_ids=document_ids,
        runtime_config=runtime_config,
    )


def _lexical_overlap(query_terms: set[str], text: str) -> float:
    if not query_terms:
        return 0.0
    haystack = set(_query_terms(text))
    if not haystack:
        return 0.0
    return round(len(query_terms & haystack) / max(1, len(query_terms)), 6)


def _metadata_overlap(query_terms: set[str], value: Any) -> float:
    if value is None:
        return 0.0
    if isinstance(value, str):
        haystack = value
    elif isinstance(value, list):
        haystack = " ".join(str(item) for item in value)
    else:
        haystack = str(value)
    return _lexical_overlap(query_terms, haystack)


def _graph_predicate_relevance_bonus(question_type: str, predicate: str) -> float:
    lowered = predicate.strip().lower()
    if not lowered:
        return 0.0
    causal_markers = ("cause", "causes", "because", "lead", "leads", "result", "results", "affect", "affects", "increase", "decrease", "prevent", "depends")
    procedural_markers = ("step", "steps", "requires", "require", "before", "after", "during", "produces", "uses", "contains", "part_of", "needs", "enables")
    comparison_markers = ("higher", "lower", "same", "different", "contains", "produces", "affects", "has")
    if question_type == "explanation" and any(marker in lowered for marker in causal_markers):
        return 0.14
    if question_type == "procedure" and any(marker in lowered for marker in procedural_markers):
        return 0.12
    if question_type == "comparison" and any(marker in lowered for marker in comparison_markers):
        return 0.1
    return 0.03 if "_" in lowered else 0.0


def _text_jaccard(left: str, right: str) -> float:
    left_terms = set(_query_terms(left))
    right_terms = set(_query_terms(right))
    if not left_terms or not right_terms:
        return 0.0
    return len(left_terms & right_terms) / max(1, len(left_terms | right_terms))


def _distance_to_score(distance: Any) -> float | None:
    if distance is None:
        return None
    try:
        value = float(distance)
    except (TypeError, ValueError):
        return None
    return round(max(0.0, 1.0 - value), 6)


def _embedding_cache_identity() -> str:
    payload = {
        "provider": str(settings.embedding_provider or ""),
        "base_url": str(settings.embedding_base_url or ""),
        "model": str(settings.embedding_model or ""),
    }
    return json.dumps(payload, ensure_ascii=False, sort_keys=True)


def _resolve_provider(runtime_config: dict[str, Any] | None = None) -> str:
    configured = (runtime_config or {}).get("provider") or settings.agent_provider
    provider = str(configured).strip().lower()
    if provider in {"", "auto"}:
        provider = "openai"
    if provider != "openai":
        return provider
    return "openai" if settings.agent_api_key else "disabled"


def _extract_message_content(body: dict[str, Any]) -> str:
    choices = body.get("choices") or []
    if not choices:
        raise AgentQueryError("Agent answer returned no choices")
    message = choices[0].get("message") or {}
    refusal = message.get("refusal")
    if refusal:
        raise AgentQueryError(f"Agent answer refused: {refusal}")

    content = message.get("content")
    if isinstance(content, str) and content.strip():
        return content
    if isinstance(content, list):
        parts: list[str] = []
        for item in content:
            if isinstance(item, dict):
                text = item.get("text")
                if isinstance(text, dict):
                    value = text.get("value")
                    if value:
                        parts.append(str(value))
                elif text:
                    parts.append(str(text))
        if parts:
            return "".join(parts)
    parsed = message.get("parsed")
    if isinstance(parsed, (dict, list)):
        return json.dumps(_json_safe(parsed), ensure_ascii=False)
    if isinstance(content, str):
        return content
    raise AgentQueryError("Agent answer returned no textual content")


def _extract_json_candidate(content: str) -> str | None:
    text = str(content or "").strip()
    if not text:
        return None
    fenced = re.match(r"^```(?:json)?\s*(.*?)\s*```$", text, flags=re.DOTALL | re.IGNORECASE)
    if fenced:
        text = fenced.group(1).strip()
    if text.startswith("{") and text.endswith("}"):
        return text
    start = text.find("{")
    end = text.rfind("}")
    if start >= 0 and end > start:
        return text[start : end + 1]
    return None


def _coerce_plaintext_agent_response(content: str, bundle: AgentContextBundle | None = None) -> dict[str, Any]:
    answer = _clean_user_facing_answer_text(str(content or "").strip())
    if not answer:
        raise AgentQueryError("Agent answer returned empty content")
    used_chunk_ids = [str(row.get("chunk_id") or "") for row in ((bundle.chunks if bundle else [])[:3]) if str(row.get("chunk_id") or "").strip()]
    used_asset_ids = [str(row.get("asset_id") or "") for row in ((bundle.assets if bundle else [])[:2]) if str(row.get("asset_id") or "").strip()]
    used_sensor_row_ids = [str(row.get("sensor_row_id") or "") for row in ((bundle.sensor_rows if bundle else [])[:2]) if str(row.get("sensor_row_id") or "").strip()]
    used_evidence_ids = [str(row.get("evidence_id") or "") for row in ((bundle.evidence if bundle else [])[:2]) if str(row.get("evidence_id") or "").strip()]
    supporting_assertions = [str(row.get("assertion_id") or "") for row in ((bundle.assertions if bundle else [])[:3]) if str(row.get("assertion_id") or "").strip()]
    supporting_entities = [str(row.get("entity_id") or "") for row in ((bundle.entities if bundle else [])[:5]) if str(row.get("entity_id") or "").strip()]
    return {
        "answer": answer,
        "confidence": 0.74,
        "abstained": False,
        "abstain_reason": None,
        "used_chunk_ids": used_chunk_ids,
        "used_asset_ids": used_asset_ids,
        "used_sensor_row_ids": used_sensor_row_ids,
        "used_evidence_ids": used_evidence_ids,
        "supporting_assertions": supporting_assertions,
        "supporting_entities": supporting_entities,
    }


def _coerce_agent_response(content: str, bundle: AgentContextBundle | None = None) -> dict[str, Any]:
    payload: dict[str, Any] | None = None
    try:
        payload = json.loads(content)
    except json.JSONDecodeError as exc:
        candidate = _extract_json_candidate(content)
        if candidate:
            try:
                payload = json.loads(candidate)
            except json.JSONDecodeError:
                payload = None
        if payload is None:
            return _coerce_plaintext_agent_response(content, bundle=bundle)
    used_chunk_ids = [str(item).strip() for item in (payload.get("used_chunk_ids") or []) if str(item).strip()]
    used_asset_ids = [str(item).strip() for item in (payload.get("used_asset_ids") or []) if str(item).strip()]
    used_sensor_row_ids = [str(item).strip() for item in (payload.get("used_sensor_row_ids") or []) if str(item).strip()]
    used_evidence_ids = [str(item).strip() for item in (payload.get("used_evidence_ids") or []) if str(item).strip()]
    if not used_chunk_ids:
        used_chunk_ids = [
            str(item.get("chunk_id") or "").strip()
            for item in (payload.get("citations") or [])
            if isinstance(item, dict) and str(item.get("chunk_id") or "").strip()
        ]
    if not used_asset_ids:
        used_asset_ids = [
            str(item.get("asset_id") or "").strip()
            for item in (payload.get("citations") or [])
            if isinstance(item, dict) and str(item.get("asset_id") or "").strip()
        ]
    if not used_sensor_row_ids:
        used_sensor_row_ids = [
            str(item.get("sensor_row_id") or "").strip()
            for item in (payload.get("citations") or [])
            if isinstance(item, dict) and str(item.get("sensor_row_id") or "").strip()
        ]
    if not used_evidence_ids:
        used_evidence_ids = [
            str(item.get("evidence_id") or "").strip()
            for item in (payload.get("citations") or [])
            if isinstance(item, dict) and str(item.get("evidence_id") or "").strip()
        ]
    return {
        "answer": str(payload.get("answer") or "").strip(),
        "confidence": float(payload.get("confidence") or 0.0),
        "abstained": bool(payload.get("abstained", False)),
        "abstain_reason": payload.get("abstain_reason"),
        "used_chunk_ids": used_chunk_ids,
        "used_asset_ids": used_asset_ids,
        "used_sensor_row_ids": used_sensor_row_ids,
        "used_evidence_ids": used_evidence_ids,
        "supporting_assertions": list(payload.get("supporting_assertions") or []),
        "supporting_entities": list(payload.get("supporting_entities") or []),
    }


def _agent_response_schema() -> dict[str, Any]:
    return {
        "type": "object",
        "additionalProperties": False,
        "properties": {
            "answer": {"type": "string"},
            "confidence": {"type": "number"},
            "abstained": {"type": "boolean"},
            "abstain_reason": {"type": ["string", "null"]},
            "used_chunk_ids": {
                "type": "array",
                "items": {"type": "string"},
            },
            "used_asset_ids": {
                "type": "array",
                "items": {"type": "string"},
            },
            "used_sensor_row_ids": {
                "type": "array",
                "items": {"type": "string"},
            },
            "used_evidence_ids": {
                "type": "array",
                "items": {"type": "string"},
            },
            "supporting_assertions": {
                "type": "array",
                "items": {"type": "string"},
            },
            "supporting_entities": {
                "type": "array",
                "items": {"type": "string"},
            },
        },
        "required": [
            "answer",
            "confidence",
            "abstained",
            "abstain_reason",
            "used_chunk_ids",
            "used_asset_ids",
            "used_sensor_row_ids",
            "used_evidence_ids",
            "supporting_assertions",
            "supporting_entities",
        ],
    }


def _json_safe(value: Any) -> Any:
    if isinstance(value, dict):
        return {str(key): _json_safe(item) for key, item in value.items()}
    if isinstance(value, list):
        return [_json_safe(item) for item in value]
    if isinstance(value, tuple):
        return [_json_safe(item) for item in value]
    if value is None or isinstance(value, (str, int, float, bool)):
        return value
    return str(value)


def _derive_agent_review_state(response: dict[str, Any], runtime_config: dict[str, Any]) -> tuple[str, str]:
    if response.get("abstained"):
        return "needs_review", _normalize_reason_code(response.get("abstain_reason"), fallback="abstained")
    grounding_check = response.get("grounding_check") or {}
    if bool(grounding_check.get("open_world_answer_used")):
        return "needs_review", "open_world_answer"
    if bool(grounding_check.get("open_world_fallback_used")):
        return "needs_review", "open_world_fallback"
    if bool(grounding_check.get("last_resort_fallback_used")):
        return "needs_review", "last_resort_fallback"
    grounding_method = str(grounding_check.get("method") or "").strip()
    if grounding_method in {"claim_verifier_error", "lexical_fallback", "supported_subset"}:
        return "needs_review", grounding_method or "grounding_fallback"
    if list(grounding_check.get("world_knowledge_claims") or []):
        return "needs_review", "world_knowledge_support"
    if list(grounding_check.get("unsupported_claims") or []):
        return "needs_review", "unsupported_claims"
    if float(grounding_check.get("supported_ratio") or 0.0) < max(
        0.75,
        float(runtime_config.get("claim_verifier_min_supported_ratio") or 0.66),
    ):
        return "needs_review", "weak_grounding_support"
    confidence = float(response.get("confidence") or 0.0)
    if confidence < runtime_config["review_confidence_threshold"]:
        return "needs_review", "low_answer_confidence"
    if (
        not response.get("supporting_assertions")
        and not response.get("used_asset_ids")
        and not response.get("used_sensor_row_ids")
        and not response.get("used_evidence_ids")
    ):
        return "unreviewed", "chunk_only_answer"
    return "unreviewed", ""


def _normalize_reason_code(value: Any, fallback: str) -> str:
    normalized = re.sub(r"[^a-z0-9_]+", "_", str(value or "").strip().lower()).strip("_")
    if normalized in _SAFE_ABSTAIN_REASONS:
        return normalized
    if fallback in _SAFE_ABSTAIN_REASONS:
        return fallback
    return "model_abstained"
