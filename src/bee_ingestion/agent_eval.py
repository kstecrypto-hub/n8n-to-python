from __future__ import annotations

from pathlib import Path
import re
from typing import Any

from src.bee_ingestion.agent import AgentService
from src.bee_ingestion.eval_common import (
    document_filename_map,
    load_eval_queries,
    resolve_document_ids_by_filename,
    term_hits,
)

EVAL_STOPWORDS = {"and", "the", "with", "from", "into", "that", "this", "your", "have", "been"}

def _expected_terms(value: str | list[str]) -> list[str]:
    if isinstance(value, list):
        return [str(item).strip() for item in value if str(item).strip()]
    return [
        token
        for token in re.split(r"[^a-z0-9]+", str(value or "").lower())
        if len(token) >= 4 and token not in EVAL_STOPWORDS
    ]


def read_agent_evaluation(path: str) -> dict[str, Any]:
    target = Path(path)
    if not target.exists():
        raise FileNotFoundError(f"Agent evaluation output not found: {path}")
    return json.loads(target.read_text(encoding="utf-8"))


def run_agent_evaluation(
    queries_file: str = "data/evaluation/agent_small_queries.json",
    tenant_id: str | None = None,
    top_k: int = 5,
    output: str | None = None,
) -> dict[str, Any]:
    queries = load_eval_queries(queries_file)
    agent_service = AgentService()
    rows: list[dict[str, Any]] = []
    passed = 0
    abstention_cases = 0
    abstention_cases_passed = 0

    for query in queries:
        query_text = str(query["query"])
        query_id = str(query.get("id", query_text))
        query_tenant = str(query.get("tenant_id") or tenant_id or "shared")
        query_top_k = max(1, min(int(query.get("top_k") or top_k), 24))
        document_ids = [str(item) for item in (query.get("document_ids") or []) if str(item).strip()]
        scope_filenames = [str(item).strip() for item in (query.get("scope_document_filenames") or []) if str(item).strip()]
        if not document_ids and scope_filenames:
            document_ids = resolve_document_ids_by_filename(agent_service, query_tenant, scope_filenames)
        document_ids = document_ids or None

        result = agent_service.query(
            question=query_text,
            tenant_id=query_tenant,
            document_ids=document_ids,
            top_k=query_top_k,
            trusted_tenant=True,
        )

        citation_document_ids = list(
            dict.fromkeys(str(item.get("document_id") or "") for item in (result.get("citations") or []) if str(item.get("document_id") or ""))
        )
        filename_map = document_filename_map(agent_service, citation_document_ids)
        citation_filenames = [filename_map[item] for item in citation_document_ids if filename_map.get(item)]
        citation_kinds = sorted({str(item.get("citation_kind") or "") for item in (result.get("citations") or []) if str(item.get("citation_kind") or "")})
        expected_question_type = str(query.get("expected_question_type") or "").strip()
        expected_abstained = query.get("expected_abstained")
        expected_documents = [str(item).strip() for item in (query.get("expected_document_filenames") or []) if str(item).strip()]
        forbidden_documents = [str(item).strip() for item in (query.get("forbidden_document_filenames") or []) if str(item).strip()]
        expected_answer_terms = _expected_terms(query.get("expected_answer_terms") or query.get("expected_focus") or [])
        expected_citation_kinds = [str(item).strip() for item in (query.get("expected_citation_kinds") or []) if str(item).strip()]

        router_ok = True if not expected_question_type else result.get("question_type") == expected_question_type
        abstention_ok = True if expected_abstained is None else bool(result.get("abstained")) is bool(expected_abstained)
        if expected_abstained is True:
            abstention_cases += 1
            if abstention_ok:
                abstention_cases_passed += 1
        doc_ok = True if not expected_documents else any(
            any(expected.lower() in filename.lower() for filename in citation_filenames)
            for expected in expected_documents
        )
        forbidden_doc_ok = not any(
            any(forbidden.lower() in filename.lower() for filename in citation_filenames)
            for forbidden in forbidden_documents
        )
        answer_term_hits = term_hits(expected_answer_terms, str(result.get("answer") or ""))
        answer_term_coverage = round(len(answer_term_hits) / max(1, len(expected_answer_terms)), 3) if expected_answer_terms else 1.0
        citation_text = "\n".join(
            str(item.get("quote") or "")
            for item in (result.get("citations") or [])
            if str(item.get("quote") or "").strip()
        )
        citation_term_hits = term_hits(expected_answer_terms, citation_text)
        citation_term_coverage = round(len(citation_term_hits) / max(1, len(expected_answer_terms)), 3) if expected_answer_terms else 1.0
        citation_kind_ok = True if not expected_citation_kinds else all(kind in citation_kinds for kind in expected_citation_kinds)
        grounding_check = dict(result.get("grounding_check") or {})
        grounding_ok = bool(grounding_check.get("passed")) if not result.get("abstained") else abstention_ok and bool(result.get("abstain_reason"))
        citation_ok = bool(result.get("citations") or []) if not result.get("abstained") else True
        confidence_floor = float(query.get("min_confidence") or 0.0)
        confidence_ok = float(result.get("confidence") or 0.0) >= confidence_floor or bool(result.get("abstained"))
        answer_support_ok = answer_term_coverage >= float(query.get("min_answer_term_coverage", 0.35 if expected_answer_terms else 0.0))
        citation_support_ok = citation_term_coverage >= float(query.get("min_citation_term_coverage", 0.25 if expected_answer_terms and not result.get("abstained") else 0.0))
        passed_row = all(
            [
                router_ok,
                abstention_ok,
                doc_ok,
                forbidden_doc_ok,
                citation_kind_ok,
                grounding_ok,
                citation_ok,
                confidence_ok,
                answer_support_ok,
                citation_support_ok,
            ]
        )
        if passed_row:
            passed += 1
        rows.append(
            {
                "id": query_id,
                "query": query_text,
                "tenant_id": query_tenant,
                "top_k": query_top_k,
                "question_type": result.get("question_type"),
                "routing": result.get("routing") or {},
                "router_ok": router_ok,
                "expected_question_type": expected_question_type or None,
                "abstained": bool(result.get("abstained")),
                "expected_abstained": expected_abstained,
                "abstention_ok": abstention_ok,
                "abstain_reason": result.get("abstain_reason"),
                "review_status": result.get("review_status"),
                "confidence": float(result.get("confidence") or 0.0),
                "grounding_check": grounding_check,
                "citation_document_filenames": citation_filenames,
                "citation_kinds": citation_kinds,
                "expected_document_filenames": expected_documents,
                "forbidden_document_filenames": forbidden_documents,
                "doc_ok": doc_ok,
                "forbidden_doc_ok": forbidden_doc_ok,
                "expected_citation_kinds": expected_citation_kinds,
                "citation_kind_ok": citation_kind_ok,
                "expected_answer_terms": expected_answer_terms,
                "answer_term_hits": answer_term_hits,
                "answer_term_coverage": answer_term_coverage,
                "citation_term_hits": citation_term_hits,
                "citation_term_coverage": citation_term_coverage,
                "answer_support_ok": answer_support_ok,
                "citation_support_ok": citation_support_ok,
                "citations": result.get("citations") or [],
                "passed": passed_row,
            }
        )

    summary = {
        "queries_file": queries_file,
        "tenant_id": tenant_id,
        "top_k": top_k,
        "passed": passed,
        "total": len(rows),
        "pass_rate": round(passed / max(1, len(rows)), 3),
        "abstention_cases": abstention_cases,
        "abstention_cases_passed": abstention_cases_passed,
        "rows": rows,
    }
    if output:
        target = Path(output)
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_text(json.dumps(summary, ensure_ascii=False, indent=2), encoding="utf-8")
    return summary
