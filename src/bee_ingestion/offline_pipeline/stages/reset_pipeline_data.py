"""Offline pipeline ownership for the reset_pipeline_data operation."""

from __future__ import annotations



def reset_pipeline_data(service) -> dict:
    result = service.reset_ingestion_data(document_id=None)
    result["reset"] = True
    return result

