"""Agent trace and retrieval-cache persistence.

This module owns query-run read models, replayable trace metadata, router/cache
pattern state, and embedding-cache persistence. It does not own session memory,
profile persistence, HTTP translation, or prompt construction.
"""

from __future__ import annotations

import json
from typing import Any
from uuid import uuid4

import psycopg
from psycopg.rows import dict_row
from psycopg.types.json import Jsonb


def get_latest_agent_session_scope(
    repo: Any,
    session_id: str,
    *,
    tenant_id: str | None = None,
    auth_user_id: str | None = None,
    profile_id: str | None = None,
) -> dict | None:
    with psycopg.connect(repo.dsn, row_factory=dict_row) as conn:
        with conn.cursor() as cur:
            clauses = ["q.session_id = %s"]
            params: list[object] = [session_id]
            if tenant_id is not None:
                clauses.append("q.tenant_id = %s")
                params.append(tenant_id)
            if auth_user_id is not None:
                clauses.append("q.auth_user_id = %s")
                params.append(repo._sanitize_text(auth_user_id))
            if profile_id is not None:
                clauses.append("q.profile_id = %s")
                params.append(profile_id)
            cur.execute(
                f"""
                SELECT prompt_payload
                FROM agent_query_runs q
                WHERE {' AND '.join(clauses)}
                ORDER BY created_at DESC
                LIMIT 1
                """,
                tuple(params),
            )
            row = cur.fetchone()
            if row is None:
                return None
            prompt_payload = dict(row.get("prompt_payload") or {})
            scope = prompt_payload.get("request_scope")
            return dict(scope) if isinstance(scope, dict) else None


def count_agent_query_runs(
    repo: Any,
    *,
    session_id: str | None = None,
    tenant_id: str | None = None,
    status: str | None = None,
    abstained: bool | None = None,
    review_status: str | None = None,
    auth_user_id: str | None = None,
    profile_id: str | None = None,
) -> int:
    clauses: list[str] = []
    params: list[object] = []
    if session_id:
        clauses.append("session_id = %s")
        params.append(session_id)
    if tenant_id:
        clauses.append("tenant_id = %s")
        params.append(tenant_id)
    if auth_user_id:
        clauses.append("auth_user_id = %s")
        params.append(repo._sanitize_text(auth_user_id))
    if profile_id:
        clauses.append("profile_id = %s")
        params.append(profile_id)
    if status:
        clauses.append("status = %s")
        params.append(status)
    if abstained is not None:
        clauses.append("abstained = %s")
        params.append(abstained)
    if review_status:
        clauses.append("review_status = %s")
        params.append(review_status)
    where_clause = f"WHERE {' AND '.join(clauses)}" if clauses else ""
    return repo._fetch_scalar(f"SELECT COUNT(*) AS value FROM agent_query_runs {where_clause}", tuple(params))


def list_agent_query_runs(
    repo: Any,
    *,
    session_id: str | None = None,
    tenant_id: str | None = None,
    status: str | None = None,
    abstained: bool | None = None,
    review_status: str | None = None,
    auth_user_id: str | None = None,
    profile_id: str | None = None,
    limit: int = 100,
    offset: int = 0,
) -> list[dict]:
    clauses: list[str] = []
    params: list[object] = []
    if session_id:
        clauses.append("q.session_id = %s")
        params.append(session_id)
    if tenant_id:
        clauses.append("q.tenant_id = %s")
        params.append(tenant_id)
    if auth_user_id:
        clauses.append("q.auth_user_id = %s")
        params.append(repo._sanitize_text(auth_user_id))
    if profile_id:
        clauses.append("q.profile_id = %s")
        params.append(profile_id)
    if status:
        clauses.append("q.status = %s")
        params.append(status)
    if abstained is not None:
        clauses.append("q.abstained = %s")
        params.append(abstained)
    if review_status:
        clauses.append("q.review_status = %s")
        params.append(review_status)
    where_clause = f"WHERE {' AND '.join(clauses)}" if clauses else ""
    params.extend([limit, offset])
    with psycopg.connect(repo.dsn, row_factory=dict_row) as conn:
        with conn.cursor() as cur:
            cur.execute(
                f"""
                SELECT
                  q.query_run_id,
                  q.session_id,
                  q.tenant_id,
                  q.question,
                  q.normalized_query,
                  q.query_signature,
                  q.query_keywords,
                  q.question_type,
                  q.retrieval_mode,
                  q.status,
                  q.confidence,
                  q.abstained,
                  q.abstain_reason,
                  q.provider,
                  q.model,
                  q.prompt_version,
                  q.review_status,
                  q.review_reason,
                  q.reviewed_at,
                  q.reviewed_by,
                  q.corpus_snapshot_id,
                  q.metrics_json,
                  q.created_at,
                  s.title AS session_title
                FROM agent_query_runs q
                LEFT JOIN agent_sessions s ON s.session_id = q.session_id
                {where_clause}
                ORDER BY q.created_at DESC
                LIMIT %s OFFSET %s
                """,
                tuple(params),
            )
            return [dict(row) for row in cur.fetchall()]


def get_agent_query_detail(
    repo: Any,
    query_run_id: str,
    *,
    tenant_id: str | None = None,
    session_id: str | None = None,
    auth_user_id: str | None = None,
    profile_id: str | None = None,
) -> dict | None:
    with psycopg.connect(repo.dsn, row_factory=dict_row) as conn:
        with conn.cursor() as cur:
            clauses = ["q.query_run_id = %s"]
            params: list[object] = [query_run_id]
            if tenant_id:
                clauses.append("q.tenant_id = %s")
                params.append(tenant_id)
            if session_id:
                clauses.append("q.session_id = %s")
                params.append(session_id)
            if auth_user_id:
                clauses.append("q.auth_user_id = %s")
                params.append(repo._sanitize_text(auth_user_id))
            if profile_id:
                clauses.append("q.profile_id = %s")
                params.append(profile_id)
            cur.execute(
                f"""
                SELECT q.*, s.title AS session_title, s.status AS session_status, s.profile_id
                FROM agent_query_runs q
                LEFT JOIN agent_sessions s ON s.session_id = q.session_id
                WHERE {' AND '.join(clauses)}
                """,
                tuple(params),
            )
            run = cur.fetchone()
            if run is None:
                return None

            cur.execute(
                """
                SELECT
                  source_link_id,
                  query_run_id,
                  source_kind,
                  source_id,
                  document_id,
                  chunk_id,
                  assertion_id,
                  entity_id,
                  rank,
                  score,
                  selected,
                  payload,
                  corpus_snapshot_id,
                  created_at
                FROM agent_query_sources
                WHERE query_run_id = %s
                ORDER BY selected DESC, rank NULLS LAST, created_at ASC
                """,
                (query_run_id,),
            )
            sources = [dict(row) for row in cur.fetchall()]

            cur.execute(
                """
                SELECT review_id, query_run_id, decision, reviewer, notes, payload, created_at
                FROM agent_answer_reviews
                WHERE query_run_id = %s
                ORDER BY created_at DESC
                """,
                (query_run_id,),
            )
            reviews = [dict(row) for row in cur.fetchall()]

        run_dict = dict(run)

    session_memory = None
    if run_dict.get("session_id"):
        session_memory = repo.get_agent_session_memory(
            str(run_dict["session_id"]),
            auth_user_id=auth_user_id,
            profile_id=profile_id,
        )
    profile = None
    run_profile_id = str(run_dict.get("profile_id") or "")
    if run_profile_id:
        profile = repo.get_agent_profile(run_profile_id)
    pattern = None
    query_signature = str(run_dict.get("query_signature") or "")
    if query_signature:
        pattern = get_agent_query_pattern(
            repo,
            str(run_dict.get("tenant_id") or "shared"),
            query_signature,
        )

    return {
        "query_run": repo._redact_sensitive_json_value(run_dict),
        "sources": [repo._redact_sensitive_json_value(item) for item in sources],
        "reviews": [repo._redact_sensitive_json_value(item) for item in reviews],
        "pattern": repo._redact_sensitive_json_value(pattern) if pattern is not None else None,
        "session_memory": repo._redact_sensitive_json_value(session_memory) if session_memory is not None else None,
        "profile": repo._redact_sensitive_json_value(profile) if profile is not None else None,
    }


def save_agent_query_run(
    repo: Any,
    session_id: str | None,
    tenant_id: str,
    question: str,
    normalized_query: str,
    question_type: str,
    retrieval_mode: str,
    status: str,
    answer: str | None,
    confidence: float,
    abstained: bool,
    abstain_reason: str | None,
    provider: str,
    model: str,
    prompt_version: str,
    metrics: dict | None = None,
    error_message: str | None = None,
    prompt_payload: dict | None = None,
    raw_response_payload: dict | None = None,
    final_response_payload: dict | None = None,
    review_status: str = "unreviewed",
    review_reason: str | None = None,
    corpus_snapshot_id: str | None = None,
) -> str:
    query_run_id = str(uuid4())
    with psycopg.connect(repo.dsn) as conn:
        with conn.cursor() as cur:
            profile_id = None
            auth_user_id = None
            if session_id:
                cur.execute(
                    "SELECT profile_id, auth_user_id FROM agent_sessions WHERE session_id = %s",
                    (session_id,),
                )
                session_row = cur.fetchone()
                if session_row is None:
                    raise ValueError("Session not found")
                profile_id, auth_user_id = session_row
            cur.execute(
                """
                INSERT INTO agent_query_runs (
                  query_run_id, session_id, profile_id, auth_user_id, tenant_id, question, normalized_query, question_type,
                  retrieval_mode, status, answer, confidence, abstained, abstain_reason,
                  provider, model, prompt_version, metrics_json, error_message,
                  prompt_payload, raw_response_payload, final_response_payload,
                  review_status, review_reason, corpus_snapshot_id
                )
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                """,
                (
                    query_run_id,
                    session_id,
                    profile_id,
                    repo._sanitize_text(str(auth_user_id or "")) or None,
                    tenant_id,
                    repo._sanitize_text(question),
                    repo._sanitize_text(normalized_query),
                    question_type,
                    retrieval_mode,
                    status,
                    repo._sanitize_text(answer or ""),
                    confidence,
                    abstained,
                    repo._sanitize_text(abstain_reason or ""),
                    provider,
                    model,
                    prompt_version,
                    json.dumps(repo._redact_sensitive_json_value(metrics or {})),
                    repo._sanitize_text(error_message or ""),
                    json.dumps(repo._redact_sensitive_json_value(prompt_payload or {})),
                    json.dumps(repo._redact_sensitive_json_value(raw_response_payload or {})),
                    json.dumps(repo._redact_sensitive_json_value(final_response_payload or {})),
                    review_status,
                    repo._sanitize_text(review_reason or ""),
                    corpus_snapshot_id,
                ),
            )
            if session_id:
                cur.execute(
                    "UPDATE agent_sessions SET updated_at = now() WHERE session_id = %s",
                    (session_id,),
                )
        conn.commit()
    return query_run_id


def save_agent_query_sources(
    repo: Any,
    query_run_id: str,
    sources: list[dict],
    corpus_snapshot_id: str | None = None,
) -> None:
    if not sources:
        return
    with psycopg.connect(repo.dsn) as conn:
        with conn.cursor() as cur:
            for source in sources:
                cur.execute(
                    """
                    INSERT INTO agent_query_sources (
                      query_run_id, source_kind, source_id, document_id, chunk_id, assertion_id, entity_id,
                      rank, score, selected, payload, corpus_snapshot_id
                    )
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    """,
                    (
                        query_run_id,
                        source.get("source_kind"),
                        source.get("source_id"),
                        source.get("document_id"),
                        source.get("chunk_id"),
                        source.get("assertion_id"),
                        source.get("entity_id"),
                        source.get("rank"),
                        source.get("score"),
                        bool(source.get("selected", False)),
                        json.dumps(repo._redact_sensitive_json_value(source.get("payload", {}))),
                        corpus_snapshot_id,
                    ),
                )
        conn.commit()


def list_agent_query_patterns(
    repo: Any,
    *,
    tenant_id: str = "shared",
    search: str | None = None,
    limit: int = 50,
    offset: int = 0,
) -> list[dict]:
    clauses: list[str] = ["tenant_id = %s"]
    params: list[object] = [tenant_id]
    if search:
        clauses.append("(pattern_signature ILIKE %s OR example_query ILIKE %s)")
        needle = f"%{search}%"
        params.extend([needle, needle])
    where_clause = "WHERE " + " AND ".join(clauses)
    with psycopg.connect(repo.dsn, row_factory=dict_row) as conn:
        with conn.cursor() as cur:
            cur.execute(
                f"""
                SELECT
                  tenant_id,
                  pattern_signature,
                  keywords_json,
                  example_query,
                  approved_count,
                  rejected_count,
                  needs_review_count,
                  total_feedback_count,
                  router_cache_json,
                  router_cached_at,
                  router_cache_hits,
                  router_model,
                  last_query_run_id,
                  last_feedback_at,
                  last_feedback_by,
                  created_at,
                  updated_at
                FROM agent_query_patterns
                {where_clause}
                ORDER BY approved_count DESC, total_feedback_count DESC, updated_at DESC
                LIMIT %s OFFSET %s
                """,
                tuple(params + [limit, offset]),
            )
            return [dict(row) for row in cur.fetchall()]


def count_agent_query_patterns(repo: Any, *, tenant_id: str = "shared", search: str | None = None) -> int:
    clauses: list[str] = ["tenant_id = %s"]
    params: list[object] = [tenant_id]
    if search:
        clauses.append("(pattern_signature ILIKE %s OR example_query ILIKE %s)")
        needle = f"%{search}%"
        params.extend([needle, needle])
    where_clause = "WHERE " + " AND ".join(clauses)
    return repo._fetch_scalar(f"SELECT COUNT(*) AS value FROM agent_query_patterns {where_clause}", tuple(params))


def get_agent_metrics(repo: Any) -> dict:
    with psycopg.connect(repo.dsn, row_factory=dict_row) as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT
                  COUNT(*) AS total_runs,
                  COUNT(*) FILTER (WHERE abstained = true) AS abstentions,
                  COUNT(*) FILTER (WHERE review_status = 'needs_review') AS needs_review,
                  COUNT(*) FILTER (WHERE review_status = 'approved') AS approved,
                  COUNT(*) FILTER (WHERE review_status = 'rejected') AS rejected,
                  COUNT(*) FILTER (WHERE COALESCE(abstain_reason, '') = 'no_valid_citations') AS no_citation_answers,
                  COALESCE(AVG(confidence), 0) AS avg_confidence,
                  COALESCE(AVG(NULLIF(metrics_json->>'latency_ms', '')::numeric), 0) AS avg_latency_ms,
                  COALESCE(MAX(NULLIF(metrics_json->>'latency_ms', '')::numeric), 0) AS max_latency_ms
                FROM agent_query_runs
                """
            )
            row = dict(cur.fetchone() or {})
            return {
                "total_runs": int(row.get("total_runs") or 0),
                "abstentions": int(row.get("abstentions") or 0),
                "needs_review": int(row.get("needs_review") or 0),
                "approved": int(row.get("approved") or 0),
                "rejected": int(row.get("rejected") or 0),
                "no_citation_answers": int(row.get("no_citation_answers") or 0),
                "avg_confidence": float(row.get("avg_confidence") or 0.0),
                "avg_latency_ms": float(row.get("avg_latency_ms") or 0.0),
                "max_latency_ms": float(row.get("max_latency_ms") or 0.0),
            }


def get_agent_query_pattern(repo: Any, tenant_id: str, pattern_signature: str) -> dict | None:
    with psycopg.connect(repo.dsn, row_factory=dict_row) as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT
                  tenant_id,
                  pattern_signature,
                  keywords_json,
                  example_query,
                  approved_count,
                  rejected_count,
                  needs_review_count,
                  total_feedback_count,
                  router_cache_json,
                  router_cached_at,
                  router_cache_hits,
                  router_model,
                  last_query_run_id,
                  last_feedback_at,
                  last_feedback_by,
                  created_at,
                  updated_at
                FROM agent_query_patterns
                WHERE tenant_id = %s AND pattern_signature = %s
                """,
                (tenant_id, pattern_signature),
            )
            row = cur.fetchone()
            return dict(row) if row else None


def get_cached_query_embedding(repo: Any, tenant_id: str, normalized_query: str, cache_identity: str) -> dict | None:
    query_hash = repo._query_hash(normalized_query)
    with psycopg.connect(repo.dsn, row_factory=dict_row) as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT
                  tenant_id,
                  query_hash,
                  normalized_query,
                  cache_identity,
                  embedding_json,
                  embedding_dimensions,
                  cache_hits,
                  cached_at,
                  created_at,
                  updated_at
                FROM agent_query_embeddings
                WHERE tenant_id = %s AND query_hash = %s AND cache_identity = %s
                """,
                (tenant_id, query_hash, repo._sanitize_text(cache_identity)),
            )
            row = cur.fetchone()
            return dict(row) if row else None


def save_cached_query_embedding(repo: Any, tenant_id: str, normalized_query: str, cache_identity: str, embedding: list[float]) -> None:
    query_hash = repo._query_hash(normalized_query)
    vector = [float(value) for value in embedding]
    with psycopg.connect(repo.dsn) as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO agent_query_embeddings (
                  tenant_id,
                  query_hash,
                  normalized_query,
                  cache_identity,
                  embedding_json,
                  embedding_dimensions,
                  cache_hits,
                  cached_at
                )
                VALUES (%s, %s, %s, %s, %s, %s, 0, now())
                ON CONFLICT (tenant_id, query_hash, cache_identity) DO UPDATE SET
                  normalized_query = EXCLUDED.normalized_query,
                  embedding_json = EXCLUDED.embedding_json,
                  embedding_dimensions = EXCLUDED.embedding_dimensions,
                  cached_at = EXCLUDED.cached_at,
                  updated_at = now()
                """,
                (
                    tenant_id,
                    query_hash,
                    repo._sanitize_text(normalized_query),
                    repo._sanitize_text(cache_identity),
                    Jsonb(vector),
                    len(vector),
                ),
            )
        conn.commit()


def touch_cached_query_embedding_hit(repo: Any, tenant_id: str, normalized_query: str, cache_identity: str) -> None:
    query_hash = repo._query_hash(normalized_query)
    with psycopg.connect(repo.dsn) as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                UPDATE agent_query_embeddings
                SET cache_hits = cache_hits + 1,
                    updated_at = now()
                WHERE tenant_id = %s AND query_hash = %s AND cache_identity = %s
                """,
                (tenant_id, query_hash, repo._sanitize_text(cache_identity)),
            )
        conn.commit()


def save_agent_query_pattern_route(
    repo: Any,
    tenant_id: str,
    pattern_signature: str,
    query_keywords: list[str],
    example_query: str,
    route_payload: dict[str, Any],
    router_model: str,
) -> None:
    with psycopg.connect(repo.dsn) as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO agent_query_patterns (
                  tenant_id,
                  pattern_signature,
                  keywords_json,
                  example_query,
                  router_cache_json,
                  router_cached_at,
                  router_model,
                  router_cache_hits
                )
                VALUES (%s, %s, %s, %s, %s, now(), %s, 0)
                ON CONFLICT (tenant_id, pattern_signature) DO UPDATE SET
                  keywords_json = EXCLUDED.keywords_json,
                  example_query = COALESCE(agent_query_patterns.example_query, EXCLUDED.example_query),
                  router_cache_json = EXCLUDED.router_cache_json,
                  router_cached_at = EXCLUDED.router_cached_at,
                  router_model = EXCLUDED.router_model,
                  updated_at = now()
                """,
                (
                    tenant_id,
                    pattern_signature,
                    json.dumps(repo._sanitize_json_value(query_keywords)),
                    repo._sanitize_text(example_query),
                    json.dumps(repo._sanitize_json_value(route_payload)),
                    repo._sanitize_text(router_model),
                ),
            )
        conn.commit()


def touch_agent_query_pattern_route_hit(repo: Any, tenant_id: str, pattern_signature: str) -> None:
    with psycopg.connect(repo.dsn) as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                UPDATE agent_query_patterns
                SET router_cache_hits = router_cache_hits + 1,
                    updated_at = now()
                WHERE tenant_id = %s AND pattern_signature = %s
                """,
                (tenant_id, pattern_signature),
            )
        conn.commit()


def update_agent_query_pattern(repo: Any, tenant_id: str, pattern_signature: str, patch: dict) -> dict | None:
    assignments, params = repo._build_allowed_patch(
        patch,
        allowed_fields={
            "keywords_json": "keywords_json",
            "example_query": "example_query",
            "approved_count": "approved_count",
            "rejected_count": "rejected_count",
            "needs_review_count": "needs_review_count",
            "total_feedback_count": "total_feedback_count",
            "last_query_run_id": "last_query_run_id",
            "last_feedback_by": "last_feedback_by",
        },
        json_fields={"keywords_json"},
        text_fields={"example_query", "last_query_run_id", "last_feedback_by"},
    )
    with psycopg.connect(repo.dsn, row_factory=dict_row) as conn:
        with conn.cursor() as cur:
            cur.execute(
                f"""
                UPDATE agent_query_patterns
                SET {", ".join(assignments)}, updated_at = now()
                WHERE tenant_id = %s AND pattern_signature = %s
                RETURNING
                  tenant_id,
                  pattern_signature,
                  keywords_json,
                  example_query,
                  approved_count,
                  rejected_count,
                  needs_review_count,
                  total_feedback_count,
                  last_query_run_id,
                  last_feedback_at,
                  last_feedback_by,
                  created_at,
                  updated_at
                """,
                tuple(params + [tenant_id, pattern_signature]),
            )
            row = cur.fetchone()
        conn.commit()
    return dict(row) if row else None


def delete_agent_query_pattern(repo: Any, tenant_id: str, pattern_signature: str) -> int:
    with psycopg.connect(repo.dsn) as conn:
        with conn.cursor() as cur:
            cur.execute(
                "DELETE FROM agent_query_patterns WHERE tenant_id = %s AND pattern_signature = %s",
                (tenant_id, pattern_signature),
            )
            deleted = cur.rowcount
        conn.commit()
    return deleted
