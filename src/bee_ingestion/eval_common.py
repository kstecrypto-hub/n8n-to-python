from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from src.bee_ingestion.agent import AgentService


def load_eval_queries(queries_file: str) -> list[dict[str, Any]]:
    queries = json.loads(Path(queries_file).read_text(encoding="utf-8"))
    if not isinstance(queries, list) or not queries:
        raise ValueError("Queries file must contain a non-empty JSON array of query objects.")
    return [dict(item) for item in queries]


def resolve_document_ids_by_filename(
    agent_service: AgentService,
    tenant_id: str,
    filenames: list[str],
) -> list[str]:
    if not filenames:
        return []
    wanted = {item.lower() for item in filenames if item.strip()}
    rows = agent_service.repository.list_documents(limit=5000, offset=0)
    resolved: list[str] = []
    for row in rows:
        if str(row.get("tenant_id") or "") != tenant_id:
            continue
        filename = str(row.get("filename") or "")
        if filename.lower() in wanted:
            resolved.append(str(row.get("document_id") or ""))
    return [item for item in resolved if item]


def document_filename_map(agent_service: AgentService, document_ids: list[str]) -> dict[str, str]:
    mapping: dict[str, str] = {}
    for document_id in document_ids:
        detail = agent_service.repository.get_document_detail(document_id)
        if detail is not None:
            mapping[document_id] = str(detail.get("document", {}).get("filename") or "")
    return mapping


def term_hits(expected_terms: list[str], text: str) -> list[str]:
    haystack = text.lower()
    return [term for term in expected_terms if term.lower() in haystack]
