"""Source preparation stage ownership for offline ingestion."""

from __future__ import annotations

from src.bee_ingestion.chunking import build_extraction_metrics, normalize_text, sanitize_text
from src.bee_ingestion.models import SourceDocument


def prepare_source_document(source: SourceDocument) -> SourceDocument:
    source.raw_text = sanitize_text(source.raw_text)
    source.normalized_text = sanitize_text(source.normalized_text or "")
    normalized_text = source.normalized_text or normalize_text(source.raw_text)
    normalized_text = sanitize_text(normalized_text)
    extraction_metrics = source.extraction_metrics or build_extraction_metrics(source.raw_text, normalized_text)
    source.normalized_text = normalized_text
    source.extraction_metrics = extraction_metrics
    return source
