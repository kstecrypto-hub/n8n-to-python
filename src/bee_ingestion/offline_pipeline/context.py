"""Context construction for offline pipeline execution."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any


@dataclass(slots=True)
class OfflinePipelineContext:
    repository: Any
    service: Any
    worker_id: str
    input_version: str | None = None
    worker_version: str | None = None
