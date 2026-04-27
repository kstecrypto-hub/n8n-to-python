"""LLM-assisted secondary review for chunks that remain ambiguous after heuristics."""

from __future__ import annotations

import json
from dataclasses import dataclass
from typing import Any

import httpx

from src.bee_ingestion.models import Chunk, ChunkReviewDecision
from src.bee_ingestion.settings import settings


@dataclass(slots=True)
class ChunkReviewArtifact:
    result: ChunkReviewDecision
    raw_payload: dict[str, Any]
    provider: str
    model: str
    prompt_version: str


class ChunkReviewError(RuntimeError):
    pass


def review_chunk_with_meta(chunk: Chunk, reasons: list[str], quality_score: float) -> ChunkReviewArtifact:
    # Review is reserved for ambiguous chunks; accepted/rejected chunks should already be deterministic.
    provider = _resolve_provider()
    if provider != "openai":
        raise ChunkReviewError(f"Unsupported chunk review provider: {provider}")
    return _openai_review(chunk, reasons, quality_score)


def _resolve_provider() -> str:
    provider = settings.review_provider.strip().lower()
    if provider == "auto":
        return "openai" if settings.review_api_key else "disabled"
    return provider


def _openai_review(chunk: Chunk, reasons: list[str], quality_score: float) -> ChunkReviewArtifact:
    # The prompt is narrowly scoped to an indexing decision so the model cannot wander into broader analysis.
    api_key = settings.review_api_key
    if not api_key:
        raise ChunkReviewError("REVIEW_API_KEY is required for LLM chunk review")

    # The reviewer prompt is intentionally narrow: it classifies one chunk for
    # indexing suitability, not general document understanding.
    system_prompt = (
        "You are a retrieval-ingestion reviewer.\n"
        "Your only task is to decide whether a single chunk that was previously marked review should be indexed.\n"
        "Accept chunks that contain coherent, meaningful body text even if the source layout is old, ragged-right, indented, centered, or uneven.\n"
        "Reject chunks that are contents, indexes, front matter, back matter, catalogue pages, headers, footers, isolated headings, or useless fragments.\n"
        "Do not reject merely because the section path is missing.\n"
        "Do not reject merely because line widths are uneven.\n"
        "Return JSON only and follow the schema exactly."
    )
    user_prompt = json.dumps(
        {
            "document_id": chunk.document_id,
            "chunk_id": chunk.chunk_id,
            "document_class": str(chunk.metadata.get("document_class") or ""),
            "current_chunk_role": str(chunk.metadata.get("chunk_role") or ""),
            "section_path": list(chunk.section_path),
            "page_range": {"start": chunk.page_start, "end": chunk.page_end},
            "current_review_reasons": [str(reason)[:160] for reason in reasons[:8]],
            "current_quality_score": round(float(quality_score), 4),
            "chunk_text": chunk.text[:6000],
        },
        ensure_ascii=False,
    )

    payload = {
        "model": settings.review_model,
        "messages": [
            {"role": "system", "content": system_prompt},
            {"role": "user", "content": user_prompt},
        ],
        "response_format": {
            "type": "json_schema",
            "json_schema": {
                "name": "chunk_review_decision",
                "strict": True,
                "schema": _review_response_schema(),
            },
        },
    }
    headers = {
        "Authorization": f"Bearer {api_key}",
        "Content-Type": "application/json",
    }
    base_url = settings.review_base_url.rstrip("/")

    try:
        with httpx.Client(timeout=settings.review_timeout_seconds) as client:
            response = client.post(f"{base_url}/chat/completions", headers=headers, json=payload)
            response.raise_for_status()
            body = response.json()
    except httpx.HTTPError as exc:
        raise ChunkReviewError(f"OpenAI chunk review request failed: {exc}") from exc

    content = _extract_message_content(body)
    try:
        parsed = json.loads(content)
    except json.JSONDecodeError as exc:
        raise ChunkReviewError(f"OpenAI chunk review returned invalid JSON: {exc}") from exc

    result = _validated_review_decision(parsed)
    return ChunkReviewArtifact(
        result=result,
        raw_payload=body,
        provider="openai",
        model=settings.review_model,
        prompt_version=settings.review_prompt_version,
    )


def _extract_message_content(body: dict[str, Any]) -> str:
    choices = body.get("choices") or []
    if not choices:
        raise ChunkReviewError("OpenAI chunk review returned no choices")
    message = choices[0].get("message") or {}
    refusal = message.get("refusal")
    if refusal:
        raise ChunkReviewError(f"OpenAI chunk review refused: {refusal}")

    content = message.get("content")
    if isinstance(content, str):
        return content
    if isinstance(content, list):
        parts: list[str] = []
        for item in content:
            if isinstance(item, dict):
                text = item.get("text")
                if text:
                    parts.append(str(text))
        if parts:
            return "".join(parts)
    raise ChunkReviewError("OpenAI chunk review returned no textual content")


def _validated_review_decision(payload: dict[str, Any]) -> ChunkReviewDecision:
    allowed_decisions = {"accept", "reject"}
    allowed_roles = {"body", "front_matter", "contents", "back_matter", "heading_fragment", "other"}
    decision = str(payload.get("decision") or "").strip()
    if decision not in allowed_decisions:
        raise ChunkReviewError("OpenAI chunk review returned an invalid decision")
    confidence_raw = payload.get("confidence")
    try:
        confidence = float(confidence_raw)
    except (TypeError, ValueError) as exc:
        raise ChunkReviewError("OpenAI chunk review returned a non-numeric confidence") from exc
    if confidence < 0.0 or confidence > 1.0:
        raise ChunkReviewError("OpenAI chunk review returned an out-of-range confidence")
    detected_role = str(payload.get("detected_role") or "").strip()
    if detected_role not in allowed_roles:
        raise ChunkReviewError("OpenAI chunk review returned an invalid detected_role")
    reason = str(payload.get("reason") or "").strip()
    if not reason:
        raise ChunkReviewError("OpenAI chunk review returned an empty reason")
    return ChunkReviewDecision(
        decision=decision,
        confidence=confidence,
        detected_role=detected_role,
        reason=reason,
    )


def _review_response_schema() -> dict[str, Any]:
    return {
        "type": "object",
        "additionalProperties": False,
        "properties": {
            "decision": {"type": "string", "enum": ["accept", "reject"]},
            "confidence": {"type": "number"},
            "detected_role": {
                "type": "string",
                "enum": ["body", "front_matter", "contents", "back_matter", "heading_fragment", "other"],
            },
            "reason": {"type": "string"},
        },
        "required": ["decision", "confidence", "detected_role", "reason"],
    }
