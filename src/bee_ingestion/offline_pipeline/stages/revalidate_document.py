"""Offline pipeline ownership for the revalidate_document operation."""

from __future__ import annotations

from src.bee_ingestion.models import Chunk
from src.bee_ingestion.validation import validate_chunk


def revalidate_document(service, document_id: str, rerun_kg: bool = True) -> dict:
    with service.repository.advisory_lock("document-mutate", document_id):
        rows = service.repository.list_document_chunk_records(document_id=document_id)
        if not rows:
            raise ValueError("Document has no stored chunks")

        # Revalidation rebuilds the in-memory chunk objects from persisted rows so the
        # current validator and enrichment rules can be applied without re-parsing.
        chunks = [service._chunk_from_record(row) for row in rows]
        assets = service._list_all_page_assets(document_id)
        links = service._relink_chunks_with_assets(document_id, chunks, assets, persist=False)
        linked_assets_by_chunk = service._group_assets_by_chunk(links, assets)
        validations = [validate_chunk(chunk) for chunk in chunks]
        accepted_chunks: list[Chunk] = []
        rejected_chunk_ids: list[str] = []
        kg_results: list[dict] = []
        chunk_updates: list[dict[str, object]] = []

        for chunk, validation in zip(chunks, validations):
            service._apply_chunk_enrichment(chunk, validation.status, validation.quality_score, validation.reasons)
            chunk_updates.append(
                {
                    "chunk_id": chunk.chunk_id,
                    "status": validation.status,
                    "quality_score": validation.quality_score,
                    "reasons": list(validation.reasons),
                    "metadata": dict(chunk.metadata),
                }
            )
            if validation.status == "accepted":
                accepted_chunks.append(chunk)
            else:
                rejected_chunk_ids.append(chunk.chunk_id)

        embeddings = service.embedder.embed([chunk.text for chunk in accepted_chunks]) if accepted_chunks else []

        if rerun_kg:
            for chunk in accepted_chunks:
                kg_results.append(
                    service._run_kg_pipeline(
                        document_id,
                        chunk,
                        linked_assets=linked_assets_by_chunk.get(chunk.chunk_id, []),
                        persist=False,
                    )
                )

        if accepted_chunks:
            service.store.upsert_chunks(accepted_chunks, embeddings)
        kg_counts = {"validated": 0, "review": 0, "skipped": 0, "quarantined": 0}
        service.repository.apply_revalidation_state(
            document_id,
            links,
            chunk_updates,
            kg_results=kg_results if rerun_kg else None,
            remove_chunk_kg_ids=rejected_chunk_ids if not rerun_kg else None,
        )
        for chunk_id in rejected_chunk_ids:
            service.store.delete_chunk(chunk_id)
        if rerun_kg:
            for kg_result in kg_results:
                kg_counts[str(kg_result["status"])] = kg_counts.get(str(kg_result["status"]), 0) + 1
        service._refresh_document_synopses(
            document_id,
            accepted_chunks=accepted_chunks,
            source_stage="revalidated",
        )

        return {
            "document_id": document_id,
            "chunks": len(chunks),
            "accepted": len(accepted_chunks),
            "review": len([item for item in validations if item.status == "review"]),
            "rejected": len([item for item in validations if item.status == "rejected"]),
            "kg": kg_counts,
        }

