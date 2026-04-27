"""Offline pipeline ownership for the reindex_document operation."""

from __future__ import annotations

from src.bee_ingestion.models import Chunk, PageAsset


def reindex_document(service, document_id: str) -> dict:
    with service.repository.advisory_lock("document-mutate", document_id):
        rows = service.repository.list_document_chunk_records(document_id=document_id)
        if not rows:
            raise ValueError("Document has no stored chunks")

        # Reindexing never reparses the document. It only takes the currently accepted
        # chunks, re-enriches their metadata, and rewrites the vector store.
        accepted_rows = [row for row in rows if row["validation_status"] == "accepted"]
        accepted_chunks: list[Chunk] = []
        for row in accepted_rows:
            chunk = service._chunk_from_record(row)
            service._apply_chunk_enrichment(
                chunk,
                row["validation_status"],
                float(row.get("quality_score") or 0.0),
                list(row.get("reasons") or []),
            )
            accepted_chunks.append(chunk)
        asset_objects = service._list_all_page_assets(document_id)
        service._relink_chunks_with_assets(document_id, accepted_chunks, asset_objects, persist=True)
        for chunk in accepted_chunks:
            service.repository.update_chunk_metadata(chunk.chunk_id, chunk.metadata)

        if asset_objects:
            service.repository.save_page_assets(asset_objects)
        chunk_embeddings = service.embedder.embed([chunk.text for chunk in accepted_chunks]) if accepted_chunks else []
        indexable_assets = [asset for asset in asset_objects if service._is_indexable_asset(asset)]
        asset_embeddings = service.embedder.embed([asset.search_text for asset in indexable_assets]) if indexable_assets else []
        if accepted_chunks:
            service.store.upsert_chunks(accepted_chunks, chunk_embeddings)
        if indexable_assets:
            service.store.upsert_assets(indexable_assets, asset_embeddings)
        accepted_chunk_ids = {chunk.chunk_id for chunk in accepted_chunks}
        for row in rows:
            chunk_id = str(row["chunk_id"])
            if chunk_id not in accepted_chunk_ids:
                service.store.delete_chunk(chunk_id)
        indexed_asset_ids = {asset.asset_id for asset in indexable_assets}
        for asset in asset_objects:
            if asset.asset_id not in indexed_asset_ids:
                service.store.delete_asset(asset.asset_id)

        return {
            "document_id": document_id,
            "accepted": len(accepted_chunks),
            "removed": len(rows) - len(accepted_chunks),
        }

