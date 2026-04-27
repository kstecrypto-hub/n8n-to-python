"""Agent answer-review persistence.

This module owns durable review and feedback rows for agent runs. It does not
own HTTP translation, prompt construction, or general query-run persistence.
"""

from __future__ import annotations

import json
from typing import Any

import psycopg
from psycopg.rows import dict_row


def save_agent_answer_review(
    repo: Any,
    *,
    query_run_id: str,
    decision: str,
    reviewer: str = "admin",
    notes: str | None = None,
    payload: dict | None = None,
    tenant_id: str | None = None,
    session_id: str | None = None,
) -> None:
    decision = repo._sanitize_text(decision or "").strip().lower()
    reviewer = repo._sanitize_text(reviewer or "admin").strip() or "admin"
    allowed_decisions = getattr(repo, "ALLOWED_AGENT_REVIEW_DECISIONS", {"approved", "rejected", "needs_review"})
    if decision not in allowed_decisions:
        raise ValueError("Invalid review decision")
    is_quality_judgment = reviewer != "user-ui"
    with psycopg.connect(repo.dsn, row_factory=dict_row) as conn:
        with conn.cursor() as cur:
            clauses = ["query_run_id = %s"]
            params: list[object] = [query_run_id]
            if tenant_id:
                clauses.append("tenant_id = %s")
                params.append(tenant_id)
            if session_id:
                clauses.append("session_id = %s")
                params.append(session_id)
            cur.execute(
                f"""
                SELECT tenant_id, question, normalized_query, query_signature, query_keywords
                FROM agent_query_runs
                WHERE {' AND '.join(clauses)}
                """,
                tuple(params),
            )
            run = cur.fetchone()
            if run is None:
                raise ValueError("Agent query run not found")
            tenant_id = str(run["tenant_id"] or "shared")
            question = str(run["question"] or "")
            normalized_query = str(run["normalized_query"] or "")
            query_signature = str(run["query_signature"] or "")
            query_keywords = list(run["query_keywords"] or [])
            if not query_signature:
                query_signature, query_keywords = repo._build_query_pattern(normalized_query)
                cur.execute(
                    """
                    UPDATE agent_query_runs
                    SET query_signature = %s,
                        query_keywords = %s
                    WHERE query_run_id = %s
                    """,
                    (query_signature, json.dumps(query_keywords), query_run_id),
                )

            cur.execute(
                """
                SELECT decision
                FROM agent_answer_reviews
                WHERE query_run_id = %s
                  AND reviewer <> 'user-ui'
                ORDER BY created_at DESC
                LIMIT 1
                """,
                (query_run_id,),
            )
            previous_review = cur.fetchone()
            previous_decision = str(previous_review["decision"]) if previous_review else None

            cur.execute(
                """
                INSERT INTO agent_answer_reviews (query_run_id, decision, reviewer, notes, payload)
                VALUES (%s, %s, %s, %s, %s)
                """,
                (
                    query_run_id,
                    decision,
                    reviewer,
                    repo._sanitize_text(notes or ""),
                    json.dumps(repo._redact_sensitive_json_value(payload or {})),
                ),
            )
            if is_quality_judgment:
                cur.execute(
                    """
                    UPDATE agent_query_runs
                    SET review_status = %s,
                        review_reason = %s,
                        reviewed_at = now(),
                        reviewed_by = %s
                    WHERE query_run_id = %s
                    """,
                    (decision, repo._sanitize_text(notes or ""), reviewer, query_run_id),
                )
                repo._apply_agent_query_pattern_feedback(
                    cur=cur,
                    tenant_id=tenant_id,
                    query_signature=query_signature,
                    query_keywords=query_keywords,
                    example_query=question,
                    previous_decision=previous_decision,
                    decision=decision,
                    reviewer=reviewer,
                    query_run_id=query_run_id,
                )
        conn.commit()


def list_agent_answer_reviews(repo: Any, *, decision: str | None = None, limit: int = 100, offset: int = 0) -> list[dict]:
    clauses: list[str] = []
    params: list[object] = []
    if decision:
        clauses.append("ar.decision = %s")
        params.append(decision)
    where_clause = f"WHERE {' AND '.join(clauses)}" if clauses else ""
    with psycopg.connect(repo.dsn, row_factory=dict_row) as conn:
        with conn.cursor() as cur:
            cur.execute(
                f"""
                SELECT
                  ar.review_id,
                  ar.query_run_id,
                  ar.decision,
                  ar.reviewer,
                  ar.notes,
                  ar.payload,
                  ar.created_at,
                  qr.question,
                  qr.query_signature,
                  qr.query_keywords,
                  qr.session_id,
                  qr.confidence,
                  qr.abstained
                FROM agent_answer_reviews ar
                JOIN agent_query_runs qr ON qr.query_run_id = ar.query_run_id
                {where_clause}
                ORDER BY ar.created_at DESC
                LIMIT %s OFFSET %s
                """,
                tuple(params + [limit, offset]),
            )
            return [dict(row) for row in cur.fetchall()]


def count_agent_answer_reviews(repo: Any, *, decision: str | None = None) -> int:
    clauses: list[str] = []
    params: list[object] = []
    if decision:
        clauses.append("decision = %s")
        params.append(decision)
    where_clause = f"WHERE {' AND '.join(clauses)}" if clauses else ""
    return repo._fetch_scalar(f"SELECT COUNT(*) AS value FROM agent_answer_reviews {where_clause}", tuple(params))
