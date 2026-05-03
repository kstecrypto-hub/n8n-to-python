"""Multimodal extraction stage ownership for offline ingestion."""

from __future__ import annotations

from typing import Any

from src.bee_ingestion.models import SourceDocument
from src.bee_ingestion.multimodal import MultimodalPDFPayload, extract_pdf_multimodal_payload


def extract_multimodal_payload(service: Any, *, document_id: str, source: SourceDocument) -> MultimodalPDFPayload | None:
    if source.source_type != "pdf":
        return None
    path = service._resolve_replayable_pdf_path(source)
    if path is None:
        return None
    page_range = source.metadata.get("page_range") or {}
    progress_callback = None
    if service.progress_callback is not None:

        def progress_callback(update: dict[str, Any]) -> None:
            service._emit_progress(
                phase="preparing",
                detail=str(update.get("detail") or "Preparing multimodal PDF assets."),
                document_id=document_id,
                metrics=dict(update.get("metrics") or {}),
            )

    return extract_pdf_multimodal_payload(
        document_id=document_id,
        tenant_id=source.tenant_id,
        path=str(path),
        filename=source.filename,
        page_start=page_range.get("start"),
        page_end=page_range.get("end"),
        progress_callback=progress_callback,
    )
