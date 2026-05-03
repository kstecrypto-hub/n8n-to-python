"""HTTP routes for admin metrics and evaluation tooling."""

from __future__ import annotations

from fastapi import APIRouter, Query
from pydantic import BaseModel

from src.bee_ingestion.admin import metrics_service
from src.bee_ingestion.http_api.dependencies import agent_service, repository

router = APIRouter()


class RetrievalEvalRequest(BaseModel):
    queries_file: str = "data/evaluation/retrieval_eval_queries.json"
    tenant_id: str = "shared"
    top_k: int = 5
    output: str = "data/evaluation/latest-admin-eval.json"


class AgentEvalRequest(BaseModel):
    queries_file: str = "data/evaluation/agent_eval_queries.json"
    tenant_id: str = "shared"
    top_k: int = 5
    output: str = "data/evaluation/latest-agent-eval.json"


class RetrievalInspectRequest(BaseModel):
    question: str
    tenant_id: str = "shared"
    document_ids: list[str] | None = None
    top_k: int | None = None
    query_mode: str | None = None


@router.get("/admin/api/agent/metrics")
def admin_agent_metrics() -> dict:
    return metrics_service.get_agent_metrics(repository=repository)


@router.post("/admin/api/retrieval/evaluate")
def admin_run_retrieval_eval(request: RetrievalEvalRequest) -> dict:
    return metrics_service.run_retrieval_eval_job(
        tenant_id=request.tenant_id,
        queries_file=request.queries_file,
        output=request.output,
        top_k=request.top_k,
    )


@router.get("/admin/api/retrieval/evaluation")
def admin_get_retrieval_eval(path: str = Query(default="data/evaluation/latest-admin-eval.json")) -> dict:
    return metrics_service.read_retrieval_eval_result(path=path)


@router.post("/admin/api/retrieval/inspect")
def admin_inspect_retrieval(request: RetrievalInspectRequest) -> dict:
    return metrics_service.inspect_retrieval(
        agent_service=agent_service,
        question=request.question,
        tenant_id=request.tenant_id,
        document_ids=request.document_ids,
        top_k=request.top_k,
        query_mode=request.query_mode,
    )


@router.post("/admin/api/agent/evaluate")
def admin_run_agent_eval(request: AgentEvalRequest) -> dict:
    return metrics_service.run_agent_eval_job(
        tenant_id=request.tenant_id,
        queries_file=request.queries_file,
        output=request.output,
        top_k=request.top_k,
    )


@router.get("/admin/api/agent/evaluation")
def admin_get_agent_eval(path: str = Query(default="data/evaluation/latest-agent-eval.json")) -> dict:
    return metrics_service.read_agent_eval_result(path=path)
