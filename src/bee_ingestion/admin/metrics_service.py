"""Admin observability and evaluation workflows.

This module owns operator-facing overview, process snapshots, route inspection,
and retrieval/agent evaluation flows. It does not own HTTP route declarations
or persistence internals.
"""

from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from fastapi import HTTPException

from src.bee_ingestion.agent_eval import read_agent_evaluation, run_agent_evaluation
from src.bee_ingestion.retrieval_eval import read_retrieval_evaluation, run_retrieval_evaluation

WORKSPACE_ROOT = Path(__file__).resolve().parents[3]
EVALUATION_ROOT = (WORKSPACE_ROOT / "data" / "evaluation").resolve()


def resolve_evaluation_path(path: str, *, must_exist: bool = False) -> Path:
    candidate = Path(path)
    if not candidate.is_absolute():
        candidate = (WORKSPACE_ROOT / candidate).resolve()
    else:
        candidate = candidate.resolve()
    try:
        candidate.relative_to(WORKSPACE_ROOT)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail="Evaluation path must stay inside the workspace.") from exc
    if must_exist and not candidate.exists():
        raise FileNotFoundError(f"File not found: {candidate}")
    return candidate


def get_overview(*, repository: Any) -> dict:
    return repository.get_dashboard_overview()


def get_system_processes(*, repository: Any, limit: int) -> dict:
    documents = repository.list_documents(limit=100, offset=0)
    active_documents = [row for row in documents if row.get("status") not in {"completed", "failed"}][:limit]
    return {
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "overview": repository.get_dashboard_overview(),
        "active_documents": active_documents,
        "recent_stage_runs": repository.list_stage_runs(limit=limit, offset=0),
        "recent_review_runs": repository.list_chunk_review_runs(limit=limit, offset=0),
    }


def list_system_routes(routes: list[Any]) -> list[dict]:
    items: list[dict] = []
    for route in routes:
        path = getattr(route, "path", None)
        methods = sorted(
            method
            for method in (getattr(route, "methods", None) or set())
            if method not in {"HEAD", "OPTIONS"}
        )
        if not path or not methods:
            continue
        if not (path.startswith("/admin") or path.startswith("/ingest") or path.startswith("/agent")):
            continue
        items.append({"path": path, "methods": methods})
    items.sort(key=lambda item: item["path"])
    return items


def get_agent_metrics(*, repository: Any) -> dict:
    return repository.get_agent_metrics()


def run_retrieval_eval_job(*, tenant_id: str, queries_file: str, output: str, top_k: int) -> dict:
    queries = resolve_evaluation_path(queries_file, must_exist=True)
    output_path = resolve_evaluation_path(output)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    return run_retrieval_evaluation(
        queries_file=str(queries),
        tenant_id=tenant_id,
        top_k=max(1, min(top_k, 20)),
        output=str(output_path),
    )


def read_retrieval_eval_result(*, path: str) -> dict:
    try:
        return read_retrieval_evaluation(str(resolve_evaluation_path(path, must_exist=True)))
    except FileNotFoundError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc


def inspect_retrieval(*, agent_service: Any, question: str, tenant_id: str, document_ids: list[str] | None, top_k: int | None, query_mode: str | None) -> dict:
    try:
        return agent_service.inspect_retrieval(
            question=question,
            tenant_id=tenant_id,
            document_ids=document_ids,
            top_k=top_k,
            query_mode=query_mode,
            trusted_tenant=True,
        )
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


def run_agent_eval_job(*, tenant_id: str, queries_file: str, output: str, top_k: int) -> dict:
    queries = resolve_evaluation_path(queries_file, must_exist=True)
    output_path = resolve_evaluation_path(output)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    return run_agent_evaluation(
        queries_file=str(queries),
        tenant_id=tenant_id,
        top_k=max(1, min(top_k, 20)),
        output=str(output_path),
    )


def read_agent_eval_result(*, path: str) -> dict:
    try:
        return read_agent_evaluation(str(resolve_evaluation_path(path, must_exist=True)))
    except FileNotFoundError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
