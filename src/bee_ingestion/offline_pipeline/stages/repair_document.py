"""Offline pipeline ownership for the repair_document operation."""

from __future__ import annotations



def repair_document(service, document_id: str, rerun_kg: bool = True) -> dict:
    return service.revalidate_document(document_id=document_id, rerun_kg=rerun_kg)

