"""Chunk validation rules for automatic indexing decisions."""

from __future__ import annotations

import re
from collections import Counter

from src.bee_ingestion.models import Chunk, ChunkValidation


def _ocr_noise_score(text: str) -> float:
    # This is intentionally coarse: it only flags chunks that look obviously corrupt, not historically styled prose.
    if not text:
        return 1.0
    weird = len(re.findall(r"[^A-Za-z0-9\s\.,;:'\"!\?\-\(\)\[\]/%]", text))
    return weird / max(len(text), 1)


def _append_reason(reasons: list[str], reason: str) -> None:
    if reason not in reasons:
        reasons.append(reason)


def validate_chunk(chunk: Chunk) -> ChunkValidation:
    # The validator decides automatic indexing eligibility. It is conservative but not intended to replace review.
    reasons: list[str] = []
    score = 1.0
    text = chunk.text.strip()
    words = re.findall(r"\w+", text.lower())
    has_sentence_punctuation = bool(re.search(r"[.!?;:]", text))
    page_marker_fragment = bool(re.fullmatch(r"\[\[page\s+\d+\]\]\s*\d*", text, flags=re.IGNORECASE))
    linked_asset_count = int(chunk.metadata.get("linked_asset_count") or 0)
    linked_asset_max_confidence = float(chunk.metadata.get("linked_asset_max_confidence") or 0.0)
    has_visual_support = linked_asset_count > 0 and linked_asset_max_confidence >= 0.75
    short_coherent_body = (
        chunk.metadata.get("chunk_role") == "body"
        and 3 <= len(words) <= 10
        and len(text) >= 32
        and has_sentence_punctuation
    )

    # The score is heuristic, but the status mapping is deterministic. Reasons are kept
    # explicit so the UI and the LLM reviewer can explain why a chunk was downgraded.
    if page_marker_fragment:
        _append_reason(reasons, "page_marker_fragment")
        score -= 0.85

    if len(words) <= 2 and len(text) < 80:
        if has_visual_support and len(text) >= 24:
            _append_reason(reasons, "visual_context")
            score -= 0.2
        else:
            _append_reason(reasons, "too_few_words")
            score -= 0.65
    elif len(words) <= 10 and len(text) < 160:
        if has_visual_support and len(text) >= 48:
            _append_reason(reasons, "visual_context")
            score -= 0.1
        else:
            _append_reason(reasons, "too_few_words")
            score -= 0.35

    if len(text) < 20:
        _append_reason(reasons, "too_short")
        score -= 0.55
    elif len(text) < 40 and len(words) < 5:
        if has_visual_support:
            _append_reason(reasons, "visual_context")
            score -= 0.08
        else:
            _append_reason(reasons, "too_short")
            score -= 0.25
    elif len(text) < 120 and len(words) < 4:
        if has_visual_support:
            _append_reason(reasons, "visual_context")
            score -= 0.05
        else:
            _append_reason(reasons, "too_short")
            score -= 0.15
    if len(text) > 2500:
        _append_reason(reasons, "too_long")
        score -= 0.15
    if _ocr_noise_score(text) > 0.12:
        _append_reason(reasons, "ocr_noise")
        score -= 0.45

    if len(words) >= 20:
        top_word_count = Counter(words).most_common(1)[0][1]
        if top_word_count / len(words) > 0.18:
            _append_reason(reasons, "repetitive")
            score -= 0.2
    elif not words:
        _append_reason(reasons, "no_words")
        score -= 0.5

    coherent_body = (
        chunk.metadata.get("chunk_role") == "body"
        and len(words) >= 25
        and len(text) >= 180
        and has_sentence_punctuation
    )
    if not chunk.section_path and not coherent_body and not has_visual_support:
        _append_reason(reasons, "missing_section")
        score -= 0.1
    elif not chunk.section_path:
        _append_reason(reasons, "missing_section")
        score -= 0.0 if has_visual_support else 0.02
    if chunk.metadata.get("chunk_role") in {"front_matter", "contents", "back_matter"}:
        _append_reason(reasons, chunk.metadata["chunk_role"])
        score -= 0.55
    if chunk.metadata.get("block_types") == ["heading"]:
        _append_reason(reasons, "heading_only")
        score -= 0.4
    if chunk.metadata.get("document_class") not in {"book", "manual", "article", "research_paper", "note", "practical_experience"}:
        _append_reason(reasons, "unknown_document_class")
        score -= 0.05

    rejection_reasons = {"no_words", "ocr_noise", "heading_only", "page_marker_fragment"}
    review_reasons = {"front_matter", "contents", "back_matter", "too_long", "repetitive"}
    short_body_accept_reasons = {"too_few_words", "missing_section"}

    # Status precedence matters: obvious garbage is rejected first, borderline chunks
    # go to review, and only the remaining set is accepted automatically.
    status = "accepted"
    if any(reason in rejection_reasons for reason in reasons):
        status = "rejected"
    elif "too_few_words" in reasons and len(words) <= 2:
        status = "rejected"
    elif "too_few_words" in reasons:
        status = "accepted" if short_coherent_body and set(reasons).issubset(short_body_accept_reasons) else "review"
    elif "too_short" in reasons and len(text) < 20:
        status = "rejected"
    elif any(reason in review_reasons for reason in reasons):
        status = "review"
    elif score < 0.7:
        status = "review"

    return ChunkValidation(
        chunk_id=chunk.chunk_id,
        status=status,
        quality_score=max(0.0, round(score, 4)),
        reasons=reasons or ["ok"],
    )
