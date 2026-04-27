"""Offline pipeline ownership for the delete_document operation."""

from __future__ import annotations



def delete_document(service, document_id: str) -> dict:
    with service.repository.advisory_lock("document-mutate", document_id):
        deleted = service.repository.delete_document(document_id)
        if not deleted:
            raise ValueError("Document not found")
        service.store.delete_document(document_id)
        service._delete_page_asset_files(document_id)
        service.repository.prune_orphan_kg_entities()
        return {"document_id": document_id, "deleted": True}

