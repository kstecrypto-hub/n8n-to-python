"""Typed contracts for the offline ingestion pipeline."""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from typing import Any, Literal, Protocol


StageStatus = Literal["completed", "failed", "skipped"]


@dataclass(slots=True)
class OfflinePipelineCommand:
    job_id: str
    document_id: str
    source_id: str
    tenant_id: str
    source_type: str
    delete_document_on_failure: bool = False
    metadata: dict[str, Any] = field(default_factory=dict)


@dataclass(slots=True)
class OfflinePipelineState:
    command: OfflinePipelineCommand
    result_payload: dict[str, Any] | None = None
    retrieval_visible: bool = False
    finished_stage_names: list[str] = field(default_factory=list)
    stage_metrics: dict[str, dict[str, Any]] = field(default_factory=dict)
    errors: list[str] = field(default_factory=list)


@dataclass(slots=True)
class StageResult:
    status: StageStatus
    metrics: dict[str, Any] = field(default_factory=dict)
    output: dict[str, Any] | None = None
    error_message: str | None = None
    retrieval_visible: bool = False
    finished_at: datetime | None = None


class OfflinePipelineStage(Protocol):
    name: str
    job_status_on_start: str
    retrieval_visibility_boundary: bool

    async def run(self, context: Any, state: OfflinePipelineState) -> StageResult:
        ...
