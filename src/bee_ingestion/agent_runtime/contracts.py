"""Typed contracts for the deterministic agent runtime pipeline.

These contracts belong under the runtime package because they are shared across
prompt assembly, generation, verification, and orchestration.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any


class AgentQueryError(RuntimeError):
    pass


@dataclass(slots=True)
class AgentContextBundle:
    """Normalized evidence bundle assembled from chunks and KG rows."""

    chunks: list[dict[str, Any]]
    assets: list[dict[str, Any]]
    sensor_rows: list[dict[str, Any]]
    assertions: list[dict[str, Any]]
    evidence: list[dict[str, Any]]
    entities: list[dict[str, Any]]
    graph_chains: list[dict[str, Any]]
    sources: list[dict[str, Any]]


@dataclass(slots=True)
class AgentPromptBundle:
    """Prompt-ready slice of the context bundle after budgeting/trimming."""

    prior_context: list[dict[str, Any]]
    profile_summary: dict[str, Any] | None
    session_summary: dict[str, Any] | None
    chunk_payload: list[dict[str, Any]]
    asset_payload: list[dict[str, Any]]
    sensor_payload: list[dict[str, Any]]
    assertion_payload: list[dict[str, Any]]
    entity_payload: list[dict[str, Any]]
    graph_chain_payload: list[dict[str, Any]]
    evidence_payload: list[dict[str, Any]]
    stats: dict[str, Any]


EvidenceBundle = AgentContextBundle
