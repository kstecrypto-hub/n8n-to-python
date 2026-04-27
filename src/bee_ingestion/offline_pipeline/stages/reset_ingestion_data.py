"""Offline pipeline ownership for the reset_ingestion_data operation."""

from __future__ import annotations



def reset_ingestion_data(service, document_id: str | None = None) -> dict:
    if document_id:
        with service.repository.advisory_lock("document-mutate", document_id):
            deleted = service.repository.delete_document(document_id)
            if not deleted:
                raise ValueError("Document not found")
            service.store.delete_document(document_id)
            service._delete_page_asset_files(document_id)
            return {"document_id": document_id, "cleared": "document"}
    with service.repository.advisory_lock("pipeline-reset", "all"):
        service.repository.clear_ingestion_data()
        service.store.reset_collection()
        service._delete_page_asset_files()
        return {"cleared": "all"}

