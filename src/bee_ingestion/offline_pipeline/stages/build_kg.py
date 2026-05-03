"""Knowledge-graph stage ownership for offline ingestion."""

from __future__ import annotations

from dataclasses import asdict
from typing import Any, Callable

from src.bee_ingestion.kg import (
    KGExtractionError,
    canonicalize_extraction,
    extract_candidates_with_meta,
    prune_extraction,
    validate_extraction,
)
from src.bee_ingestion.models import Chunk, KGExtractionResult, PageAsset
from src.bee_ingestion.settings import settings


def run_kg_pipeline(
    service: Any,
    *,
    document_id: str,
    chunk: Chunk,
    linked_assets: list[PageAsset] | None = None,
    persist: bool = True,
    pre_persist_check: Callable[[], None] | None = None,
) -> dict:
    try:
        ontology = service._current_ontology()
        effective_chunk = service._build_kg_input_chunk(chunk, linked_assets or [])
        artifact = extract_candidates_with_meta(effective_chunk, ontology)
        pruned_result, warnings = prune_extraction(artifact.result, ontology, settings.kg_min_confidence)
        canonical_result = canonicalize_extraction(pruned_result, ontology)
        valid, validation_errors = validate_extraction(canonical_result, ontology, settings.kg_min_confidence)
        errors = list(warnings) + validation_errors
        has_relations = bool(canonical_result.candidate_relations)
        if not has_relations:
            errors.append("empty_relation_set")
        if not has_relations:
            kg_status = "skipped"
        else:
            kg_status = "validated" if valid else "review"
        payload = {
            "chunk_id": chunk.chunk_id,
            "status": kg_status,
            "errors": errors,
            "provider": artifact.provider,
            "model": artifact.model,
            "prompt_version": artifact.prompt_version,
            "result": asdict(canonical_result),
            "_result_obj": canonical_result,
            "_raw_payload": {
                "model_payload": artifact.raw_payload,
                "linked_asset_ids": [asset.asset_id for asset in (linked_assets or [])][:8],
            },
        }
        if persist:
            if pre_persist_check is not None:
                pre_persist_check()
            service._persist_kg_outcome(document_id, chunk.chunk_id, payload)
        return payload
    except KGExtractionError as exc:
        errors = [f"extractor_error:{exc}"]
        empty_result = empty_kg_result(document_id, chunk.chunk_id)
        payload = {
            "chunk_id": chunk.chunk_id,
            "status": "quarantined",
            "errors": errors,
            "provider": settings.kg_extraction_provider,
            "model": settings.kg_model,
            "prompt_version": settings.kg_prompt_version,
            "result": asdict(empty_result),
            "_result_obj": empty_result,
            "_raw_payload": {"error": str(exc)},
        }
        if persist:
            if pre_persist_check is not None:
                pre_persist_check()
            service._persist_kg_outcome(document_id, chunk.chunk_id, payload)
        return payload


def run_build_kg_stage(
    service: Any,
    *,
    document_id: str,
    accepted_chunks: list[Chunk],
    linked_assets_by_chunk: dict[str, list[PageAsset]],
    job_id: str,
    ensure_lease_active: Callable[[], None],
    start_stage: Callable[[str, str, str, dict[str, Any] | None], None],
    finish_stage: Callable[..., None],
) -> tuple[list[dict], list[dict], dict[str, Any]]:
    kg_results: list[dict] = []
    kg_failures: list[dict] = []
    start_stage(
        "kg_validated",
        "kg",
        f"Running KG extraction over {len(accepted_chunks)} accepted chunks.",
        metrics={"kg_total": len(accepted_chunks), "kg_completed": 0, "kg_failures": 0},
    )
    for index, chunk in enumerate(accepted_chunks, start=1):
        ensure_lease_active()
        service._emit_progress(
            phase="kg",
            detail=f"KG extraction {index}/{len(accepted_chunks)}.",
            document_id=document_id,
            job_id=job_id,
            metrics={
                "kg_total": len(accepted_chunks),
                "kg_completed": index - 1,
                "kg_failures": len(kg_failures),
            },
        )
        kg_result = run_kg_pipeline(
            service,
            document_id=document_id,
            chunk=chunk,
            linked_assets=linked_assets_by_chunk.get(chunk.chunk_id, []),
            pre_persist_check=ensure_lease_active,
        )
        kg_results.append(kg_result)
        if kg_result["status"] not in {"validated", "skipped"}:
            kg_failures.append({"chunk_id": chunk.chunk_id, "status": kg_result["status"], "errors": kg_result["errors"]})
    kg_metrics = service._build_graph_quality_metrics(document_id, len(accepted_chunks))
    finish_stage(
        "kg",
        "Completed KG extraction.",
        "completed" if not kg_failures else "review",
        metrics=kg_metrics,
        error_message=None if not kg_failures else str(kg_failures),
    )
    return kg_results, kg_failures, kg_metrics


def empty_kg_result(document_id: str, chunk_id: str) -> KGExtractionResult:
    return KGExtractionResult(
        source_id=document_id,
        segment_id=chunk_id,
        mentions=[],
        candidate_entities=[],
        candidate_relations=[],
        evidence=[],
    )
