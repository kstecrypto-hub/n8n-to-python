"""Runner for the offline ingestion pipeline."""

from __future__ import annotations

from datetime import datetime, timezone

from src.bee_ingestion.offline_pipeline.context import OfflinePipelineContext
from src.bee_ingestion.offline_pipeline.contracts import OfflinePipelineCommand, OfflinePipelineStage, OfflinePipelineState, StageResult

UTC = timezone.utc


class OfflinePipelineRunner:
    def __init__(self, stages: list[OfflinePipelineStage]) -> None:
        self.stages = list(stages)

    async def run(
        self,
        *,
        context: OfflinePipelineContext,
        command: OfflinePipelineCommand,
    ) -> OfflinePipelineState:
        state = OfflinePipelineState(command=command)
        for stage in self.stages:
            started_at = datetime.now(UTC)
            try:
                result = await stage.run(context, state)
            except Exception as exc:
                result = StageResult(
                    status="failed",
                    error_message=str(exc),
                    finished_at=datetime.now(UTC),
                )
            finished_at = result.finished_at or datetime.now(UTC)
            metrics = dict(result.metrics or {})
            if hasattr(context.repository, "record_stage"):
                context.repository.record_stage(
                    job_id=command.job_id,
                    document_id=command.document_id,
                    stage_name=stage.name,
                    job_status="failed" if result.status == "failed" else stage.job_status_on_start,
                    stage_outcome=result.status,
                    metrics=metrics,
                    error_message=result.error_message,
                    worker_version=context.worker_version,
                    input_version=context.input_version,
                    started_at=started_at,
                    finished_at=finished_at,
                )
            state.stage_metrics[stage.name] = metrics
            state.finished_stage_names.append(stage.name)
            if result.output is not None:
                state.result_payload = result.output
            state.retrieval_visible = state.retrieval_visible or bool(result.retrieval_visible or stage.retrieval_visibility_boundary)
            if result.status == "failed":
                if result.error_message:
                    state.errors.append(result.error_message)
                break
        return state
