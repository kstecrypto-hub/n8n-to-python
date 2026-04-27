from __future__ import annotations

import argparse
import json
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

FOCUS_STOPWORDS = {"and", "the", "with", "from", "into", "that", "this", "your", "have", "been"}


def _selected_document_ids(bundle: dict[str, Any]) -> list[str]:
    document_ids: list[str] = []
    for row in bundle.get("prompt_chunk_payload") or bundle.get("chunks") or []:
        document_id = str(row.get("document_id") or "")
        if document_id and document_id not in document_ids:
            document_ids.append(document_id)
    for row in bundle.get("prompt_asset_payload") or bundle.get("assets") or []:
        document_id = str(row.get("document_id") or "")
        if document_id and document_id not in document_ids:
            document_ids.append(document_id)
    return document_ids


def _expected_focus_terms(expected_focus: str) -> list[str]:
    return [
        token
        for token in re.split(r"[^a-z0-9]+", (expected_focus or "").lower())
        if len(token) >= 4 and token not in FOCUS_STOPWORDS
    ]


def _bundle_text(bundle: dict[str, Any]) -> str:
    parts: list[str] = []
    for row in bundle.get("prompt_chunk_payload") or bundle.get("chunks") or []:
        parts.append(str(row.get("text") or ""))
    for row in bundle.get("prompt_asset_payload") or bundle.get("assets") or []:
        parts.append(str(row.get("search_text") or ""))
        parts.append(str(row.get("description_text") or ""))
        parts.append(str(row.get("ocr_text") or ""))
    return "\n".join(part for part in parts if part.strip())


def _selected_source_rows(bundle: dict[str, Any]) -> list[dict[str, Any]]:
    return [dict(row) for row in (bundle.get("sources") or []) if bool(row.get("selected"))]


def _source_text_maps(bundle: dict[str, Any]) -> tuple[dict[str, str], dict[str, str]]:
    chunk_map = {
        str(row.get("chunk_id") or ""): str(row.get("text") or "")
        for row in (bundle.get("prompt_chunk_payload") or bundle.get("chunks") or [])
        if str(row.get("chunk_id") or "")
    }
    asset_map = {
        str(row.get("asset_id") or ""): "\n".join(
            part for part in [
                str(row.get("text") or ""),
                str(row.get("search_text") or ""),
                str(row.get("description_text") or ""),
                str(row.get("ocr_text") or ""),
                " ".join(str(item) for item in ((row.get("metadata_json") or {}).get("important_terms") or []) if str(item).strip()),
            ] if part.strip()
        )
        for row in (bundle.get("prompt_asset_payload") or bundle.get("assets") or [])
        if str(row.get("asset_id") or "")
    }
    return chunk_map, asset_map


def _selected_source_text(bundle: dict[str, Any]) -> str:
    chunk_map, asset_map = _source_text_maps(bundle)
    parts: list[str] = []
    for row in _selected_source_rows(bundle):
        kind = str(row.get("source_kind") or "")
        if kind == "chunk":
            parts.append(chunk_map.get(str(row.get("chunk_id") or ""), ""))
        elif kind in {"asset_hit", "asset_link"}:
            parts.append(asset_map.get(str(row.get("asset_id") or row.get("source_id") or ""), ""))
            parts.append(" ".join(str(item) for item in ((row.get("payload") or {}).get("shared_terms") or []) if str(item).strip()))
    return "\n".join(part for part in parts if part.strip())


def _selected_source_texts(bundle: dict[str, Any]) -> list[str]:
    chunk_map, asset_map = _source_text_maps(bundle)
    texts: list[str] = []
    for row in _selected_source_rows(bundle):
        kind = str(row.get("source_kind") or "")
        if kind == "chunk":
            text = chunk_map.get(str(row.get("chunk_id") or ""), "")
        elif kind in {"asset_hit", "asset_link"}:
            shared_terms = " ".join(str(item) for item in ((row.get("payload") or {}).get("shared_terms") or []) if str(item).strip())
            text = "\n".join(
                part
                for part in [
                    asset_map.get(str(row.get("asset_id") or row.get("source_id") or ""), ""),
                    shared_terms,
                ]
                if part.strip()
            )
        else:
            text = ""
        if text.strip():
            texts.append(text)
    return texts


def _asset_bundle_text(bundle: dict[str, Any]) -> str:
    parts: list[str] = []
    for row in bundle.get("prompt_asset_payload") or bundle.get("assets") or []:
        parts.append(str(row.get("text") or ""))
        parts.append(str(row.get("search_text") or ""))
        parts.append(str(row.get("description_text") or ""))
        parts.append(str(row.get("ocr_text") or ""))
        metadata = row.get("metadata_json") or {}
        parts.append(" ".join(str(item) for item in (metadata.get("important_terms") or []) if str(item).strip()))
    return "\n".join(part for part in parts if part.strip())


def _selected_asset_texts(bundle: dict[str, Any]) -> list[str]:
    _, asset_map = _source_text_maps(bundle)
    texts: list[str] = []
    for row in _selected_source_rows(bundle):
        if str(row.get("source_kind") or "") not in {"asset_hit", "asset_link"}:
            continue
        text = asset_map.get(str(row.get("asset_id") or row.get("source_id") or ""), "")
        if text.strip():
            texts.append(text)
    return texts


def _selected_asset_link_stats(bundle: dict[str, Any]) -> dict[str, Any]:
    rows = [
        dict(row)
        for row in (bundle.get("sources") or [])
        if row.get("source_kind") == "asset_link" and bool(row.get("selected"))
    ]
    confidences = [float(row.get("score") or 0.0) for row in rows]
    return {
        "count": len(rows),
        "max_confidence": max(confidences) if confidences else 0.0,
        "rows": rows,
    }


def _max_row_term_coverage(expected_terms: list[str], row_texts: list[str]) -> float:
    if not expected_terms:
        return 1.0
    if not row_texts:
        return 0.0
    return round(
        max(len(term_hits(expected_terms, text)) / max(1, len(expected_terms)) for text in row_texts),
        3,
    )


def _phrase_in_rows(phrase: str, row_texts: list[str]) -> bool:
    target = phrase.lower().strip()
    if not target:
        return False
    return any(target in text.lower() for text in row_texts)


def _match_expected_documents(expected: list[str], filenames: list[str]) -> list[str]:
    expected_lower = [item.lower() for item in expected]
    hits: list[str] = []
    for filename in filenames:
        normalized = filename.lower()
        if any(item in normalized for item in expected_lower):
            hits.append(filename)
    return hits


def _match_forbidden_documents(forbidden: list[str], filenames: list[str]) -> list[str]:
    forbidden_lower = [item.lower() for item in forbidden]
    hits: list[str] = []
    for filename in filenames:
        normalized = filename.lower()
        if any(item in normalized for item in forbidden_lower):
            hits.append(filename)
    return hits


def _json_safe(value: Any) -> Any:
    if isinstance(value, dict):
        return {str(key): _json_safe(item) for key, item in value.items()}
    if isinstance(value, list):
        return [_json_safe(item) for item in value]
    if isinstance(value, tuple):
        return [_json_safe(item) for item in value]
    if value is None or isinstance(value, (str, int, float, bool)):
        return value
    if hasattr(value, "isoformat"):
        try:
            return value.isoformat()
        except Exception:
            return str(value)
    return str(value)


def read_retrieval_evaluation(path: str) -> dict[str, Any]:
    target = Path(path)
    if not target.exists():
        raise FileNotFoundError(f"Retrieval evaluation output not found: {path}")
    return json.loads(target.read_text(encoding="utf-8"))


def run_retrieval_evaluation(
    queries_file: str = "data/evaluation/retrieval_small_queries.json",
    tenant_id: str | None = None,
    top_k: int = 5,
    output: str | None = None,
) -> dict[str, Any]:
    queries = load_eval_queries(queries_file)
    agent_service = AgentService()

    result_rows: list[dict[str, Any]] = []
    passed = 0
    asset_queries = 0
    asset_queries_passed = 0

    for query in queries:
        query_text = str(query["query"])
        query_id = str(query.get("id", query_text))
        query_tenant = str(query.get("tenant_id") or tenant_id or "shared")
        query_top_k = max(1, min(int(query.get("top_k") or top_k), 20))
        document_ids = [str(item) for item in (query.get("document_ids") or []) if str(item).strip()]
        scope_filenames = [str(item).strip() for item in (query.get("scope_document_filenames") or []) if str(item).strip()]
        if not document_ids and scope_filenames:
            document_ids = resolve_document_ids_by_filename(agent_service, query_tenant, scope_filenames)
        document_ids = document_ids or None

        bundle = agent_service.inspect_retrieval(
            question=query_text,
            tenant_id=query_tenant,
            document_ids=document_ids,
            top_k=query_top_k,
            trusted_tenant=True,
        )

        selected_document_ids = _selected_document_ids(bundle)
        filename_map = document_filename_map(agent_service, selected_document_ids)
        selected_filenames = [filename_map[item] for item in selected_document_ids if filename_map.get(item)]
        selected_text = _bundle_text(bundle)
        selected_source_text = _selected_source_text(bundle)
        selected_source_texts = _selected_source_texts(bundle)
        selected_asset_text = _asset_bundle_text(bundle)
        selected_asset_texts = _selected_asset_texts(bundle)
        asset_link_stats = _selected_asset_link_stats(bundle)

        expected_focus = str(query.get("expected_focus") or "").strip()
        expected_terms = [str(item).strip() for item in (query.get("expected_terms") or []) if str(item).strip()]
        if not expected_terms and expected_focus:
            expected_terms = _expected_focus_terms(expected_focus)
        matched_terms = term_hits(expected_terms, selected_source_text)
        term_coverage = round(len(matched_terms) / max(1, len(expected_terms)), 3) if expected_terms else 1.0
        focus_phrase_hit = bool(expected_focus) and expected_focus.lower() in selected_source_text.lower()
        row_term_coverage = _max_row_term_coverage(expected_terms, selected_source_texts)
        row_focus_hit = _phrase_in_rows(expected_focus, selected_source_texts)

        expected_documents = [str(item).strip() for item in (query.get("expected_document_filenames") or []) if str(item).strip()]
        document_hits = _match_expected_documents(expected_documents, selected_filenames)
        document_expectation_met = True if not expected_documents else bool(document_hits)
        forbidden_documents = [str(item).strip() for item in (query.get("forbidden_document_filenames") or []) if str(item).strip()]
        forbidden_document_hits = _match_forbidden_documents(forbidden_documents, selected_filenames)
        forbidden_document_expectation_met = not forbidden_document_hits

        prompt_assets = bundle.get("prompt_asset_payload") or []
        prompt_chunks = bundle.get("prompt_chunk_payload") or []
        prompt_assertions = bundle.get("prompt_assertion_payload") or []
        prompt_entities = bundle.get("prompt_entity_payload") or []

        expect_assets = bool(query.get("expect_assets", False))
        if expect_assets:
            asset_queries += 1
        expected_asset_terms = [str(item).strip() for item in (query.get("expected_asset_terms") or []) if str(item).strip()]
        if not expected_asset_terms:
            expected_asset_terms = list(expected_terms)
        asset_term_matches = term_hits(expected_asset_terms, selected_asset_text)
        asset_term_coverage = round(len(asset_term_matches) / max(1, len(expected_asset_terms)), 3) if expected_asset_terms else 1.0
        asset_row_term_coverage = _max_row_term_coverage(expected_asset_terms, selected_asset_texts)
        min_asset_link_confidence = float(query.get("min_asset_link_confidence", 0.68) or 0.68)
        asset_expectation_met = (
            not expect_assets
            or (
                bool(prompt_assets)
                and (document_expectation_met if expected_documents else True)
                and (asset_term_coverage >= max(0.3, float(query.get("min_asset_term_coverage", 0.3))) if expected_asset_terms else True)
                and (asset_row_term_coverage >= max(0.3, float(query.get("min_asset_term_coverage", 0.3))) if expected_asset_terms else True)
                and asset_link_stats["max_confidence"] >= min_asset_link_confidence
            )
        )
        if expect_assets and asset_expectation_met:
            asset_queries_passed += 1

        min_term_coverage = float(query.get("min_term_coverage", 0.35 if expected_terms else 0.0))
        min_selected_chunks = int(query.get("min_selected_chunks", 0) or 0)
        min_selected_assets = int(query.get("min_selected_assets", 0) or 0)
        max_selected_documents = int(query.get("max_selected_documents", 0) or 0)
        focus_expectation_met = True
        if expected_focus:
            focus_expectation_met = focus_phrase_hit or row_focus_hit or row_term_coverage >= max(min_term_coverage, 0.5)
        forbidden_terms = [str(item).strip() for item in (query.get("forbidden_terms") or []) if str(item).strip()]
        forbidden_term_hits = term_hits(forbidden_terms, selected_source_text)
        passed_row = (
            document_expectation_met
            and forbidden_document_expectation_met
            and asset_expectation_met
            and focus_expectation_met
            and term_coverage >= min_term_coverage
            and row_term_coverage >= max(0.25, min(min_term_coverage, 0.5))
            and len(prompt_chunks) >= min_selected_chunks
            and len(prompt_assets) >= min_selected_assets
            and (len(selected_document_ids) <= max_selected_documents if max_selected_documents > 0 else True)
            and not forbidden_term_hits
        )
        if passed_row:
            passed += 1

        result_rows.append(
            {
                "id": query_id,
                "query": query_text,
                "tenant_id": query_tenant,
                "top_k": query_top_k,
                "question_type": bundle["question_type"],
                "retrieval_mode": bundle["retrieval_mode"],
                "expected_document_filenames": expected_documents,
                "scope_document_filenames": scope_filenames,
                "expected_focus": expected_focus,
                "selected_document_ids": selected_document_ids,
                "selected_document_filenames": selected_filenames,
                "expected_document_hits": document_hits,
                "expected_terms": expected_terms,
                "term_hits": matched_terms,
                "term_coverage": term_coverage,
                "row_term_coverage": row_term_coverage,
                "focus_phrase_hit": focus_phrase_hit,
                "row_focus_hit": row_focus_hit,
                "min_term_coverage": min_term_coverage,
                "expect_assets": expect_assets,
                "expected_asset_terms": expected_asset_terms,
                "asset_expectation_met": asset_expectation_met,
                "asset_term_hits": asset_term_matches,
                "asset_term_coverage": asset_term_coverage,
                "asset_row_term_coverage": asset_row_term_coverage,
                "asset_link_count": asset_link_stats["count"],
                "asset_link_max_confidence": asset_link_stats["max_confidence"],
                "min_asset_link_confidence": min_asset_link_confidence,
                "forbidden_document_filenames": forbidden_documents,
                "forbidden_document_hits": forbidden_document_hits,
                "forbidden_terms": forbidden_terms,
                "forbidden_term_hits": forbidden_term_hits,
                "min_selected_chunks": min_selected_chunks,
                "min_selected_assets": min_selected_assets,
                "max_selected_documents": max_selected_documents,
                "selected_chunk_count": len(prompt_chunks),
                "selected_asset_count": len(prompt_assets),
                "selected_assertion_count": len(prompt_assertions),
                "selected_entity_count": len(prompt_entities),
                "prompt_context": bundle.get("prompt_context") or {},
                "passed": passed_row,
                "chunks": [
                    {
                        "chunk_id": row.get("chunk_id"),
                        "document_id": row.get("document_id"),
                        "page_start": row.get("page_start"),
                        "page_end": row.get("page_end"),
                        "section_title": (row.get("metadata_json") or {}).get("section_title", ""),
                        "linked_asset_count": (row.get("metadata_json") or {}).get("linked_asset_count", 0),
                        "preview": str(row.get("text") or "")[:320],
                    }
                    for row in prompt_chunks
                ],
                "assets": [
                    {
                        "asset_id": row.get("asset_id"),
                        "document_id": row.get("document_id"),
                        "page_number": row.get("page_number"),
                        "asset_type": row.get("asset_type"),
                        "label": (row.get("metadata_json") or {}).get("label", ""),
                        "preview": str(row.get("search_text") or "")[:320],
                    }
                    for row in prompt_assets
                ],
                "sources": bundle.get("sources") or [],
            }
        )

    payload = {
        "queries_file": queries_file,
        "tenant_id": tenant_id or "",
        "top_k": top_k,
        "summary": {
            "queries": len(result_rows),
            "passed": passed,
            "failed": len(result_rows) - passed,
            "pass_rate": round(passed / max(1, len(result_rows)), 3),
            "asset_queries": asset_queries,
            "asset_queries_passed": asset_queries_passed,
        },
        "results": result_rows,
    }
    payload = _json_safe(payload)
    if output:
        Path(output).write_text(json.dumps(payload, indent=2, ensure_ascii=True), encoding="utf-8")
    return payload


def main() -> None:
    parser = argparse.ArgumentParser(description="Run retrieval evaluation queries against the live agent retrieval path.")
    parser.add_argument("--queries-file", default="data/evaluation/retrieval_small_queries.json")
    parser.add_argument("--tenant-id")
    parser.add_argument("--top-k", type=int, default=5)
    parser.add_argument("--output")
    args = parser.parse_args()

    payload = run_retrieval_evaluation(
        queries_file=args.queries_file,
        tenant_id=args.tenant_id,
        top_k=args.top_k,
        output=args.output,
    )
    if not args.output:
        print(json.dumps(payload, indent=2, ensure_ascii=True))


if __name__ == "__main__":
    main()
