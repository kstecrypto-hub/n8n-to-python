"""Offline pipeline ownership for the rebuild_document operation."""

from __future__ import annotations

from src.bee_ingestion.models import SourceDocument


def rebuild_document(service, document_id: str) -> dict:
    with service.repository.advisory_lock("document-mutate", document_id):
        source_row = service.repository.get_latest_document_source(document_id)
        if source_row is None:
            raise ValueError("Document source not found")

        source = service._prepare_source(
            SourceDocument(
                tenant_id=str(source_row["tenant_id"]),
                source_type=str(source_row["source_type"]),
                filename=str(source_row["filename"]),
                raw_text=str(source_row["raw_text"]),
                normalized_text=str(source_row.get("normalized_text") or ""),
                extraction_metrics=dict(source_row.get("extraction_metrics_json") or {}),
                metadata=dict(source_row.get("metadata_json") or {}),
                document_class=str(source_row["document_class"]),
                parser_version=str(source_row.get("parser_version") or "v1"),
                ocr_engine=source_row.get("ocr_engine"),
                ocr_model=source_row.get("ocr_model"),
                content_hash_value=str(source_row.get("content_hash") or ""),
            )
        )

        if source.source_type == "pdf":
            service._resolve_replayable_pdf_path(source)

        replacement_document_id, source_id = service.repository.register_document(source)
        result = service._process_registered_document(
            document_id=replacement_document_id,
            source_id=source_id,
            source=source,
        )
        service.store.delete_document(document_id)
        service._delete_page_asset_files(document_id)
        service.repository.delete_document(document_id)
        result["rebuilt_document_id"] = replacement_document_id
        result["superseded_document_id"] = document_id
        return result

