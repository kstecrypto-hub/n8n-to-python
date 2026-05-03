"""Chunking and validation stage ownership for offline ingestion."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Callable

from src.bee_ingestion.chunking import build_chunks, parse_text
from src.bee_ingestion.models import Chunk, PageAsset, SourceDocument
from src.bee_ingestion.multimodal import MultimodalPDFPayload
from src.bee_ingestion.settings import settings
from src.bee_ingestion.validation import validate_chunk


@dataclass
class ChunkDocumentsResult:
    blocks: list[Any]
    chunks: list[Chunk]
    accepted_chunks: list[Chunk]
    validation_metrics: dict[str, int]
    linked_assets_by_chunk: dict[str, list[PageAsset]]
    synopsis_metrics: dict[str, Any]


def run_chunk_documents_stage(
    service: Any,
    *,
    document_id: str,
    source: SourceDocument,
    normalized_text: str,
    multimodal_payload: MultimodalPDFPayload | None,
    job_id: str,
    ensure_lease_active: Callable[[], None],
    start_stage: Callable[[str, str, str, dict[str, Any] | None], None],
    finish_stage: Callable[..., None],
) -> ChunkDocumentsResult:
    linked_assets_by_chunk: dict[str, list[PageAsset]] = {}

    start_stage(
        "parsed",
        "parsing",
        "Parsing normalized text into structural blocks.",
        {"source_chars": len(normalized_text)},
    )
    blocks = parse_text(document_id=document_id, text=normalized_text, document_class=source.document_class)
    ensure_lease_active()
    service.repository.save_blocks(blocks)
    finish_stage(
        "parsing",
        f"Parsed {len(blocks)} structural blocks.",
        "completed",
        metrics={"blocks": len(blocks)},
    )

    start_stage(
        "chunked",
        "chunking",
        "Building persisted chunk records and chunk-asset links.",
        {"blocks": len(blocks)},
    )
    chunks = build_chunks(
        document_id=document_id,
        tenant_id=source.tenant_id,
        blocks=blocks,
        parser_version=source.parser_version,
        chunker_version=settings.chunker_version,
        document_class=source.document_class,
        filename=source.filename,
    )
    ensure_lease_active()
    service.repository.save_chunks(chunks)
    if multimodal_payload is not None:
        links = service._relink_chunks_with_assets(document_id, chunks, multimodal_payload.assets, persist=True)
        linked_assets_by_chunk = service._group_assets_by_chunk(links, multimodal_payload.assets)
    finish_stage(
        "chunking",
        f"Built {len(chunks)} chunks.",
        "completed",
        metrics={"chunks": len(chunks)},
    )

    start_stage(
        "chunks_validated",
        "validating",
        "Scoring chunks and classifying them into accepted/review/rejected buckets.",
        {"chunks": len(chunks)},
    )
    validations = [validate_chunk(chunk) for chunk in chunks]
    ensure_lease_active()
    service.repository.save_validations(validations)
    accepted_chunks = [chunk for chunk, validation in zip(chunks, validations) if validation.status == "accepted"]
    validation_metrics = {
        "accepted": len(accepted_chunks),
        "review": len([item for item in validations if item.status == "review"]),
        "rejected": len([item for item in validations if item.status == "rejected"]),
    }
    finish_stage(
        "validating",
        "Completed chunk validation.",
        "completed",
        metrics=validation_metrics,
    )

    for chunk, validation in zip(chunks, validations):
        service._apply_chunk_enrichment(chunk, validation.status, validation.quality_score, validation.reasons)
        service.repository.update_chunk_metadata(chunk.chunk_id, chunk.metadata)

    synopsis_metrics = service._refresh_document_synopses(
        document_id,
        accepted_chunks=accepted_chunks,
        source_stage="chunks_validated",
    )
    service._emit_progress(
        phase="synopsis",
        detail=f"Prepared {int(synopsis_metrics['sections'])} section synopses.",
        document_id=document_id,
        job_id=job_id,
        metrics=synopsis_metrics,
    )

    return ChunkDocumentsResult(
        blocks=blocks,
        chunks=chunks,
        accepted_chunks=accepted_chunks,
        validation_metrics=validation_metrics,
        linked_assets_by_chunk=linked_assets_by_chunk,
        synopsis_metrics=synopsis_metrics,
    )
