"""Offline pipeline ownership for the reprocess_kg operation."""

from __future__ import annotations



def reprocess_kg(service, document_id: str | None = None, batch_size: int = 200, prune_orphans: bool = True) -> dict:
    lock_parts = ("document-mutate", document_id) if document_id else ("pipeline-kg-replay", "all")
    with service.repository.advisory_lock(*lock_parts):
        processed = 0
        validated = 0
        review = 0
        skipped = 0
        quarantined = 0
        offset = 0

        while True:
            # KG replay only targets accepted chunks. Chunk review/rejection happens
            # upstream and is treated as a prerequisite for KG work.
            rows = service.repository.list_chunk_records_for_kg(document_id=document_id, limit=batch_size, offset=offset)
            if not rows:
                break
            for row in rows:
                chunk = service._chunk_from_record(row)
                kg_record = service._run_kg_pipeline(chunk.document_id, chunk, linked_assets=service._load_assets_for_chunk(chunk.chunk_id))
                processed += 1
                if kg_record["status"] == "validated":
                    validated += 1
                elif kg_record["status"] == "review":
                    review += 1
                elif kg_record["status"] == "skipped":
                    skipped += 1
                elif kg_record["status"] == "quarantined":
                    quarantined += 1
            offset += len(rows)

        pruned_entities = service.repository.prune_orphan_kg_entities() if prune_orphans else 0
        return {
            "document_id": document_id,
            "processed_chunks": processed,
            "validated": validated,
            "review": review,
            "skipped": skipped,
            "quarantined": quarantined,
            "pruned_entities": pruned_entities,
        }

