"""Temporary wrapper around the existing monolithic ingestion execution path."""

from __future__ import annotations

from datetime import datetime, timezone

from src.bee_ingestion.offline_pipeline.context import OfflinePipelineContext
from src.bee_ingestion.offline_pipeline.contracts import OfflinePipelineStage, OfflinePipelineState, StageResult

UTC = timezone.utc


class LegacyIngestStage(OfflinePipelineStage):
    name = "legacy_ingest"
    job_status_on_start = "processing"
    # First-pass equivalent of the publish boundary while the monolith is still wrapped.
    retrieval_visibility_boundary = True

    async def run(self, context: OfflinePipelineContext, state: OfflinePipelineState) -> StageResult:
        output = context.service._execute_legacy_ingest_command(state.command)
        retrieval_visible = bool(
            output.get("corpus_snapshot_id")
            or output.get("indexed_chunks")
            or output.get("indexed_assets")
        )
        return StageResult(
            status="completed",
            metrics={
                "document_id": state.command.document_id,
                "job_id": state.command.job_id,
                "retrieval_visible": retrieval_visible,
            },
            output=output,
            retrieval_visible=retrieval_visible,
            finished_at=datetime.now(UTC),
        )
