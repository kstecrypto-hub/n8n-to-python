from __future__ import annotations

from dataclasses import dataclass, field
from hashlib import sha256
from typing import Any


@dataclass(slots=True)
class SourceDocument:
    """Transport object for one ingest request before anything is persisted."""
    tenant_id: str
    source_type: str
    filename: str
    raw_text: str
    normalized_text: str | None = None
    extraction_metrics: dict[str, Any] = field(default_factory=dict)
    metadata: dict[str, Any] = field(default_factory=dict)
    document_class: str = "note"
    parser_version: str = "v1"
    ocr_engine: str | None = None
    ocr_model: str | None = None
    content_hash_value: str | None = None

    @property
    def content_hash(self) -> str:
        # Text ingest can keep using raw-text identity, but PDF/image-heavy sources may
        # inject a precomputed file-level hash so scanned documents do not all collapse
        # to the hash of an empty extracted-text string.
        if self.content_hash_value:
            return self.content_hash_value
        return f"sha256:{sha256(self.raw_text.encode('utf-8')).hexdigest()}"


@dataclass(slots=True)
class ParsedBlock:
    """A normalized structural unit extracted from the source text before chunking."""
    block_id: str
    document_id: str
    page: int | None
    section_path: list[str]
    block_type: str
    char_start: int
    char_end: int
    text: str


@dataclass(slots=True)
class DocumentPage:
    """One persisted page record with both raw extractable text and image-derived text."""
    document_id: str
    page_number: int
    extracted_text: str
    ocr_text: str = ""
    merged_text: str = ""
    page_image_path: str | None = None
    metadata: dict[str, Any] = field(default_factory=dict)


@dataclass(slots=True)
class PageAsset:
    """One image-bearing asset discovered on a page and linked back to chunks later."""
    asset_id: str
    document_id: str
    tenant_id: str
    page_number: int
    asset_index: int
    asset_type: str
    asset_path: str
    bbox: list[float] | None = None
    ocr_text: str = ""
    description_text: str = ""
    metadata: dict[str, Any] = field(default_factory=dict)

    @property
    def content_hash(self) -> str:
        payload = "|".join(
            [
                self.document_id,
                str(self.page_number),
                str(self.asset_index),
                self.asset_type,
                self.asset_path,
                self.ocr_text,
                self.description_text,
            ]
        )
        return f"sha256:{sha256(payload.encode('utf-8')).hexdigest()}"

    @property
    def search_text(self) -> str:
        # Asset search remains text-first for now: OCR + description become the retrieval surrogate.
        label = str(self.metadata.get("label") or "").strip()
        document_label = str(self.metadata.get("document_label") or "").strip()
        page_hint = f"page {self.page_number}"
        asset_kind = self.asset_type.replace("_", " ").strip()
        page_summary = str(self.metadata.get("page_summary") or "").strip() if self.asset_type == "page_image" else ""
        page_terms = " ".join(self.metadata.get("page_terms", []) or []).strip() if self.asset_type == "page_image" else ""
        important_terms = " ".join(self.metadata.get("important_terms", []) or []).strip()
        linked_terms = " ".join(self.metadata.get("linked_terms", []) or []).strip()
        role_terms_map = {
            "page_image": "page scan visual evidence",
            "diagram": "diagram figure visual evidence",
            "table": "table visual evidence",
            "chart": "chart graph visual evidence",
            "illustration": "illustration figure visual evidence",
            "photo": "photo visual evidence",
            "embedded_image": "image visual evidence",
        }
        visual_role = role_terms_map.get(self.asset_type, "image visual evidence")
        parts = [
            document_label,
            label,
            page_hint,
            asset_kind,
            page_summary,
            self.description_text,
            self.ocr_text,
            page_terms,
            important_terms,
            linked_terms,
            visual_role,
        ]
        text = "\n".join(part.strip() for part in parts if str(part).strip()).strip()
        if len(text) < 24:
            fallback_terms = visual_role
            text = "\n".join(part for part in [text, fallback_terms] if part).strip()
        return text


@dataclass(slots=True)
class ChunkAssetLink:
    """Join row linking a retrieval chunk to the exact page/image assets it can cite."""
    chunk_id: str
    asset_id: str
    link_type: str
    confidence: float = 1.0
    metadata: dict[str, Any] = field(default_factory=dict)


@dataclass(slots=True)
class Chunk:
    """The retrieval/KG unit that is persisted, validated, indexed, and cited by the agent."""
    chunk_id: str
    document_id: str
    tenant_id: str
    chunk_index: int
    page_start: int | None
    page_end: int | None
    section_path: list[str]
    prev_chunk_id: str | None
    next_chunk_id: str | None
    char_start: int
    char_end: int
    text: str
    parser_version: str
    chunker_version: str
    content_type: str = "text"
    metadata: dict[str, Any] = field(default_factory=dict)

    @property
    def content_hash(self) -> str:
        # Chunk ids are deterministic, but we still keep a content hash for drift/replay checks.
        return f"sha256:{sha256(self.text.encode('utf-8')).hexdigest()}"


@dataclass(slots=True)
class ChunkValidation:
    """Validator output that decides whether the chunk is indexed automatically, reviewed, or rejected."""
    chunk_id: str
    status: str
    quality_score: float
    reasons: list[str] = field(default_factory=list)


@dataclass(slots=True)
class ChunkReviewDecision:
    """Normalized decision returned by the LLM-assisted chunk reviewer."""
    decision: str
    confidence: float
    detected_role: str
    reason: str


@dataclass(slots=True)
class KGExtractionResult:
    """Ontology-constrained candidate graph extracted from a single accepted chunk."""
    source_id: str
    segment_id: str
    mentions: list[dict[str, Any]]
    candidate_entities: list[dict[str, Any]]
    candidate_relations: list[dict[str, Any]]
    evidence: list[dict[str, Any]]
    candidate_events: list[dict[str, Any]] = field(default_factory=list)


@dataclass(slots=True)
class UserPlace:
    """One authenticated user's registered apiary or physical location."""

    tenant_id: str
    auth_user_id: str
    external_place_id: str
    place_name: str
    status: str = "active"
    metadata: dict[str, Any] = field(default_factory=dict)
    place_id: str = ""


@dataclass(slots=True)
class UserHive:
    """One authenticated user's hive, optionally attached to a place."""

    tenant_id: str
    auth_user_id: str
    external_hive_id: str
    hive_name: str
    place_id: str | None = None
    status: str = "active"
    metadata: dict[str, Any] = field(default_factory=dict)
    hive_id: str = ""


@dataclass(slots=True)
class UserSensor:
    """One authenticated user's registered sensor device."""
    tenant_id: str
    auth_user_id: str
    external_sensor_id: str
    sensor_name: str
    sensor_type: str = "environment"
    place_id: str | None = None
    hive_id: str | None = None
    hive_name: str | None = None
    location_label: str | None = None
    status: str = "active"
    metadata: dict[str, Any] = field(default_factory=dict)
    sensor_id: str = ""


@dataclass(slots=True)
class SensorReading:
    """One sensor measurement row stored for agent and analytics use."""
    sensor_id: str
    tenant_id: str
    auth_user_id: str
    observed_at: str
    metric_name: str
    unit: str | None = None
    numeric_value: float | None = None
    text_value: str | None = None
    quality_score: float | None = None
    metadata: dict[str, Any] = field(default_factory=dict)
