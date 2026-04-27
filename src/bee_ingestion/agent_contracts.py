"""Compatibility shim for runtime contracts.

The single source of truth now lives in ``src.bee_ingestion.agent_runtime``.
This module remains only to preserve existing imports while the refactor lands.
"""

from src.bee_ingestion.agent_runtime.contracts import (
    AgentContextBundle,
    AgentPromptBundle,
    AgentQueryError,
    EvidenceBundle,
)

__all__ = [
    "AgentContextBundle",
    "AgentPromptBundle",
    "AgentQueryError",
    "EvidenceBundle",
]
