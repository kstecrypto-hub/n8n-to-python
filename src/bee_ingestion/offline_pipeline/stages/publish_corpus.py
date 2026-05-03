"""Retrieval-visibility publish stage for offline ingestion."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Callable

from src.bee_ingestion.models import Chunk, SourceDocument
from src.bee_ingestion.multimodal import MultimodalPDFPayload


@dataclass
class PublishCorpusResult:
    indexed_chunks: int
    indexed_assets: int
    corpus_snapshot_id: str | None
    final_status: str


def run_publish_corpus_stage(
    service: Any,
    *,
    document_id: str,
    source: SourceDocument,
    blocks_count: int,
    chunks_count: int,
    accepted_chunks: list[Chunk],
    multimodal_payload: MultimodalPDFPayload | None,
    kg_failures: list[dict[str, Any]],
    job_id: str,
    ensure_lease_active: Callable[[], None],
    start_stage: Callable[[str, str, str, dict[str, Any] | None], None],
    finish_stage: Callable[..., None],
) -> PublishCorpusResult:
    final_status = "completed" if not kg_failures else "review"
    indexed_chunks = 0
    indexed_assets = 0
    start_stage(
        "indexed",
        "embedding",
        "Generating vectors and publishing accepted chunks and assets.",
        {
            "indexed_chunks": 0,
            "indexed_assets": 0,
            "publish_skipped_due_to_kg_review": bool(kg_failures),
        },
    )
    corpus_snapshot_id = None
    if not kg_failures:
        corpus_snapshot_id = service.repository.create_pending_corpus_snapshot(
            source.tenant_id,
            "document_publish",
            document_id=document_id,
            job_id=job_id,
            summary=f"Published document {source.filename} into the retrieval-visible corpus.",
            metrics={
                "blocks": blocks_count,
                "chunks": chunks_count,
                "accepted_chunks": len(accepted_chunks),
                "pages": len(multimodal_payload.pages) if multimodal_payload else 0,
                "page_assets": len(multimodal_payload.assets) if multimodal_payload else 0,
                "indexed_chunks": 0,
                "indexed_assets": 0,
            },
            metadata={
                "filename": source.filename,
                "document_class": source.document_class,
                "source_type": source.source_type,
            },
        )
        ensure_lease_active()
        chunk_embeddings = service.embedder.embed(
            [chunk.text for chunk in accepted_chunks],
            progress_callback=(
                lambda update: service._emit_progress(
                    phase="embedding",
                    detail=f"Embedding accepted chunks {int(update.get('completed') or 0)}/{int(update.get('total') or 0)}.",
                    document_id=document_id,
                    job_id=job_id,
                    metrics={
                        "embedding_target": "chunks",
                        "completed": int(update.get("completed") or 0),
                        "total": int(update.get("total") or 0),
                        "batch_size": int(update.get("batch_size") or 0),
                    },
                )
            ) if accepted_chunks else None,
        ) if accepted_chunks else []
        ensure_lease_active()
        indexable_assets = [asset for asset in (multimodal_payload.assets if multimodal_payload else []) if service._is_indexable_asset(asset)]
        asset_embeddings = service.embedder.embed(
            [asset.search_text for asset in indexable_assets],
            progress_callback=(
                lambda update: service._emit_progress(
                    phase="embedding",
                    detail=f"Embedding assets {int(update.get('completed') or 0)}/{int(update.get('total') or 0)}.",
                    document_id=document_id,
                    job_id=job_id,
                    metrics={
                        "embedding_target": "assets",
                        "completed": int(update.get("completed") or 0),
                        "total": int(update.get("total") or 0),
                        "batch_size": int(update.get("batch_size") or 0),
                    },
                )
            ) if indexable_assets else None,
        ) if indexable_assets else []
        ensure_lease_active()
        indexed_chunks, indexed_assets = service._publish_fresh_document_vectors(
            document_id=document_id,
            accepted_chunks=accepted_chunks,
            chunk_embeddings=chunk_embeddings,
            indexable_assets=indexable_assets,
            asset_embeddings=asset_embeddings,
        )
        service.repository.activate_corpus_snapshot(
            corpus_snapshot_id,
            document_id=document_id,
            job_id=job_id,
            metadata_patch={
                "indexed_chunks": indexed_chunks,
                "indexed_assets": indexed_assets,
            },
            metrics_patch={
                "indexed_chunks": indexed_chunks,
                "indexed_assets": indexed_assets,
            },
        )

    finish_stage(
        "embedding" if not kg_failures else "review",
        "Completed vector publish." if not kg_failures else "Skipped vector publish because KG review is required.",
        "completed" if not kg_failures else "review",
        metrics=service._build_completion_metrics(
            document_id,
            indexed_chunks=indexed_chunks,
            indexed_assets=indexed_assets,
            publish_skipped_due_to_kg_review=bool(kg_failures),
        ),
        terminal_job_status=final_status,
    )

    return PublishCorpusResult(
        indexed_chunks=indexed_chunks,
        indexed_assets=indexed_assets,
        corpus_snapshot_id=corpus_snapshot_id,
        final_status=final_status,
    )
