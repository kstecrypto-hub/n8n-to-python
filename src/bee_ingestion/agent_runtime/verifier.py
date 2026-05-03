"""Answer verification for the deterministic agent runtime.

This module owns answer/citation/grounding verification against an already
resolved evidence bundle. It does not retrieve data or build prompts.
"""

from __future__ import annotations

import json
import re
from typing import Any, Callable

from src.bee_ingestion.agent_generation import (
    build_model_json_payload,
)
from src.bee_ingestion.chunking import sanitize_text
from src.bee_ingestion.settings import settings

_SAFE_ABSTAIN_REASONS = {
    "no_retrieval_results",
    "no_valid_citations",
    "low_confidence",
    "model_abstained",
    "agent_provider_disabled",
    "agent_generation_error",
    "empty_answer",
    "weak_grounding",
}

_GROUNDING_STOPWORDS = {
    "about", "after", "also", "among", "because", "been", "being", "between",
    "could", "does", "doing", "each", "from", "have", "into", "more", "most",
    "only", "other", "over", "same", "than", "that", "their", "them", "then",
    "there", "these", "they", "this", "those", "through", "under", "very",
    "what", "when", "where", "which", "while", "with", "would", "your",
}


class AnswerVerifier:
    def __init__(
        self,
        *,
        trusted_asset_grounding_text: Callable[[dict[str, Any]], str],
        sensor_grounding_series_text: Callable[[dict[str, Any]], str],
        lexical_grounding_check_fn: Callable[..., dict[str, Any]] | None = None,
        call_json_payload: Callable[..., dict[str, Any]],
        extract_message_content_fn: Callable[[dict[str, Any]], str],
    ) -> None:
        self._trusted_asset_grounding_text = trusted_asset_grounding_text
        self._sensor_grounding_series_text = sensor_grounding_series_text
        self._lexical_grounding_check = lexical_grounding_check_fn or _lexical_grounding_check
        self._call_json_payload = call_json_payload
        self._extract_message_content = extract_message_content_fn

    def compose_extractive_fallback_answer(
        self,
        *,
        question: str,
        question_type: str,
        citations: list[dict[str, Any]],
    ) -> str:
        normalized_query = _normalize_query(question).lower()
        keywords = {token for token in re.split(r"[^a-z0-9]+", normalized_query) if len(token) >= 4}
        scored_sentences: list[tuple[int, str]] = []
        seen: set[str] = set()
        for citation in citations[:4]:
            quote = sanitize_text(str(citation.get("quote") or "")).strip()
            if not quote:
                continue
            sentences = [sentence.strip() for sentence in re.split(r"(?<=[\.\!\?])\s+", quote) if sentence.strip()]
            for index, sentence in enumerate(sentences[:4]):
                cleaned = re.sub(r"\s+", " ", sentence).strip(" :;,-")
                if len(cleaned) < 24:
                    continue
                fingerprint = cleaned.lower()
                if fingerprint in seen:
                    continue
                seen.add(fingerprint)
                score = 2 if index == 0 else 0
                score += sum(1 for token in keywords if token in fingerprint)
                scored_sentences.append((score, cleaned))
        scored_sentences.sort(key=lambda item: (-item[0], len(item[1])))
        selected = [sentence for _, sentence in scored_sentences[:3]]
        if not selected:
            return ""
        if question_type in {"definition", "fact", "explanation", "comparison", "procedure"}:
            return " ".join(selected).strip()
        return " ".join(selected[:2]).strip()

    def verify_answer_grounding(
        self,
        *,
        answer: str,
        question_type: str,
        normalized_query: str,
        chunk_map: dict[str, dict[str, Any]],
        used_chunk_ids: list[str],
        asset_map: dict[str, dict[str, Any]],
        used_asset_ids: list[str],
        sensor_row_map: dict[str, dict[str, Any]],
        used_sensor_row_ids: list[str],
        assertion_map: dict[str, dict[str, Any]],
        evidence_map: dict[str, dict[str, Any]],
        used_evidence_ids: list[str],
        runtime_config: dict[str, Any],
    ) -> dict[str, Any]:
        if not answer.strip():
            return {
                "method": "empty_answer",
                "passed": False,
                "supported_ratio": 0.0,
                "unsupported_claims": [],
                "claims": [],
            }
        evidence_rows = self.grounding_evidence_rows(
            chunk_map=chunk_map,
            used_chunk_ids=used_chunk_ids,
            asset_map=asset_map,
            used_asset_ids=used_asset_ids,
            sensor_row_map=sensor_row_map,
            used_sensor_row_ids=used_sensor_row_ids,
            assertion_map=assertion_map,
            evidence_map=evidence_map,
            used_evidence_ids=used_evidence_ids,
        )
        lexical_result: dict[str, Any] | None = None
        if runtime_config.get("claim_verifier_enabled", True):
            verifier_result = self.verify_answer_claims_with_model(
                answer=answer,
                normalized_query=normalized_query,
                evidence_rows=evidence_rows,
                runtime_config=runtime_config,
            )
            if verifier_result is not None:
                if verifier_result.get("method") == "claim_verifier_error":
                    lexical_result = self._lexical_grounding_check(
                        answer=answer,
                        question_type=question_type,
                        normalized_query=normalized_query,
                        evidence_rows=evidence_rows,
                        runtime_config=runtime_config,
                    )
                    lexical_result["original_method"] = verifier_result.get("method")
                    lexical_result["verifier_fallback_reason"] = verifier_result.get("method")
                    lexical_result["verifier_provider"] = verifier_result.get("provider")
                    lexical_result["verifier_model"] = verifier_result.get("model")
                    lexical_result["unsupported_claims"] = list(verifier_result.get("unsupported_claims") or [])[:6]
                    return lexical_result
                return verifier_result
        lexical_result = self._lexical_grounding_check(
            answer=answer,
            question_type=question_type,
            normalized_query=normalized_query,
            evidence_rows=evidence_rows,
            runtime_config=runtime_config,
        )
        return lexical_result

    def grounding_evidence_rows(
        self,
        *,
        chunk_map: dict[str, dict[str, Any]],
        used_chunk_ids: list[str],
        asset_map: dict[str, dict[str, Any]],
        used_asset_ids: list[str],
        sensor_row_map: dict[str, dict[str, Any]],
        used_sensor_row_ids: list[str],
        assertion_map: dict[str, dict[str, Any]],
        evidence_map: dict[str, dict[str, Any]],
        used_evidence_ids: list[str],
    ) -> list[dict[str, Any]]:
        rows: list[dict[str, Any]] = []
        for chunk_id in used_chunk_ids:
            chunk = chunk_map.get(chunk_id)
            if not chunk:
                continue
            rows.append(
                {
                    "kind": "chunk",
                    "id": chunk_id,
                    "document_id": str(chunk.get("document_id") or ""),
                    "page_start": chunk.get("page_start"),
                    "page_end": chunk.get("page_end"),
                    "text": str(chunk.get("text") or "")[:1600],
                }
            )
        for asset_id in used_asset_ids:
            asset = asset_map.get(asset_id)
            if not asset:
                continue
            trusted_text = self._trusted_asset_grounding_text(asset)
            if not trusted_text:
                continue
            rows.append(
                {
                    "kind": "asset",
                    "id": asset_id,
                    "document_id": str(asset.get("document_id") or ""),
                    "page_number": asset.get("page_number"),
                    "text": trusted_text[:1600],
                }
            )
        for sensor_row_id in used_sensor_row_ids:
            sensor_row = sensor_row_map.get(sensor_row_id)
            if not sensor_row:
                continue
            trusted_text = self._sensor_grounding_series_text(sensor_row)
            if not trusted_text:
                continue
            rows.append(
                {
                    "kind": "sensor",
                    "id": sensor_row_id,
                    "sensor_id": sensor_row.get("sensor_id"),
                    "metric_name": sensor_row.get("metric_name"),
                    "text": trusted_text[:1600],
                }
            )
        for evidence_id in used_evidence_ids:
            evidence = evidence_map.get(evidence_id)
            if not evidence:
                continue
            excerpt = str(evidence.get("excerpt") or "").strip()
            if not excerpt:
                continue
            assertion = assertion_map.get(str(evidence.get("assertion_id") or ""))
            rows.append(
                {
                    "kind": "kg_evidence",
                    "id": evidence_id,
                    "assertion_id": evidence.get("assertion_id"),
                    "document_id": str((assertion or {}).get("document_id") or ""),
                    "chunk_id": (assertion or {}).get("chunk_id"),
                    "text": excerpt[:1600],
                }
            )
        return rows

    def verify_answer_claims_with_model(
        self,
        *,
        answer: str,
        normalized_query: str,
        evidence_rows: list[dict[str, Any]],
        runtime_config: dict[str, Any],
    ) -> dict[str, Any] | None:
        provider = str(runtime_config.get("claim_verifier_provider") or runtime_config.get("provider") or "").strip().lower()
        if provider not in {"", "auto", "openai"}:
            return None
        api_key = runtime_config.get("api_key_override") or settings.agent_api_key
        if not api_key or not evidence_rows:
            return None
        model = str(runtime_config.get("claim_verifier_model") or "").strip()
        if not model:
            return None
        payload = build_model_json_payload(
            model=model,
            reasoning_effort=str(runtime_config.get("claim_verifier_reasoning_effort") or "none"),
            system_prompt=str(runtime_config.get("claim_verifier_system_prompt") or "").strip(),
            user_payload={
                "question": normalized_query,
                "answer": answer,
                "evidence_rows": evidence_rows,
            },
            schema_name="claim_verifier_result",
            schema=_claim_verifier_schema(),
            temperature=float(runtime_config.get("claim_verifier_temperature") or 0.0),
            max_completion_tokens=int(runtime_config.get("claim_verifier_max_completion_tokens") or 500),
        )
        base_url = str(runtime_config.get("claim_verifier_base_url") or runtime_config.get("base_url") or settings.agent_base_url).rstrip("/")
        try:
            body = self._call_json_payload(
                base_url=base_url,
                api_key=str(api_key),
                payload=payload,
                timeout_seconds=float(runtime_config.get("claim_verifier_timeout_seconds") or 25.0),
            )
            content = self._extract_message_content(body)
            data = json.loads(content)
            expected_claims = _split_grounding_claims(answer)
            allowed_evidence_ids = {str(item.get("id") or "") for item in evidence_rows if str(item.get("id") or "")}
            claims: list[dict[str, Any]] = []
            unsupported_claims: list[str] = [str(item).strip() for item in (data.get("unsupported_claims") or []) if str(item).strip()]
            supported_claim_count = 0
            for item in (data.get("claims") or []):
                if not isinstance(item, dict):
                    continue
                claim_text = str(item.get("claim") or "").strip()
                evidence_ids = [
                    evidence_id
                    for evidence_id in [str(value).strip() for value in (item.get("evidence_ids") or []) if str(value).strip()]
                    if evidence_id in allowed_evidence_ids
                ]
                support_basis = str(item.get("support_basis") or "").strip().lower()
                evidence_supported = bool(item.get("supported")) and bool(evidence_ids)
                world_knowledge_supported = (
                    bool(item.get("supported"))
                    and support_basis == "world_knowledge"
                    and not evidence_ids
                    and not _contains_numeric_claim(claim_text)
                )
                supported = evidence_supported or world_knowledge_supported
                if supported:
                    supported_claim_count += 1
                elif claim_text and claim_text not in unsupported_claims:
                    unsupported_claims.append(claim_text)
                claims.append(
                    {
                        "claim": claim_text,
                        "supported": supported,
                        "evidence_ids": evidence_ids,
                        "support_basis": "evidence" if evidence_supported else ("world_knowledge" if world_knowledge_supported else "unsupported"),
                    }
                )
            observed_claim_count = len([item for item in claims if str(item.get("claim") or "").strip()])
            expected_claim_count = max(1, len(expected_claims))
            claim_coverage_ratio = round(observed_claim_count / max(1, expected_claim_count), 4)
            supported_ratio = round(supported_claim_count / max(1, len(claims)), 4) if claims else 0.0
            min_supported_ratio = float(runtime_config.get("claim_verifier_min_supported_ratio") or 0.66)
            min_claim_coverage = 1.0 if expected_claim_count <= 2 else 0.75
            passed = (
                bool(data.get("verdict", False))
                and bool(claims)
                and supported_claim_count >= 1
                and supported_ratio >= min_supported_ratio
                and claim_coverage_ratio >= min_claim_coverage
                and not unsupported_claims
            )
            return {
                "method": "claim_verifier",
                "passed": passed,
                "supported_ratio": supported_ratio,
                "unsupported_claims": unsupported_claims[:6],
                "claims": claims[:8],
                "world_knowledge_claims": [
                    item["claim"]
                    for item in claims
                    if str(item.get("support_basis") or "") == "world_knowledge"
                ][:6],
                "expected_claim_count": expected_claim_count,
                "observed_claim_count": observed_claim_count,
                "claim_coverage_ratio": claim_coverage_ratio,
                "provider": provider or "openai",
                "model": model,
            }
        except Exception:
            return {
                "method": "claim_verifier_error",
                "passed": False,
                "supported_ratio": 0.0,
                "unsupported_claims": ["claim_verifier_request_failed"],
                "claims": [],
                "provider": provider or "openai",
                "model": model,
            }

    def derive_agent_review_state(self, response: dict[str, Any], runtime_config: dict[str, Any]) -> tuple[str, str]:
        if response.get("abstained"):
            return "needs_review", _normalize_reason_code(response.get("abstain_reason"), fallback="abstained")
        grounding_check = response.get("grounding_check") or {}
        if bool(grounding_check.get("open_world_answer_used")):
            return "needs_review", "open_world_answer"
        if bool(grounding_check.get("open_world_fallback_used")):
            return "needs_review", "open_world_fallback"
        if bool(grounding_check.get("last_resort_fallback_used")):
            return "needs_review", "last_resort_fallback"
        grounding_method = str(grounding_check.get("method") or "").strip()
        if grounding_method in {"claim_verifier_error", "lexical_fallback", "supported_subset"}:
            return "needs_review", grounding_method or "grounding_fallback"
        if list(grounding_check.get("world_knowledge_claims") or []):
            return "needs_review", "world_knowledge_support"
        if list(grounding_check.get("unsupported_claims") or []):
            return "needs_review", "unsupported_claims"
        if float(grounding_check.get("supported_ratio") or 0.0) < max(
            0.75,
            float(runtime_config.get("claim_verifier_min_supported_ratio") or 0.66),
        ):
            return "needs_review", "weak_grounding_support"
        confidence = float(response.get("confidence") or 0.0)
        if confidence < runtime_config["review_confidence_threshold"]:
            return "needs_review", "low_answer_confidence"
        if (
            not response.get("supporting_assertions")
            and not response.get("used_asset_ids")
            and not response.get("used_sensor_row_ids")
            and not response.get("used_evidence_ids")
        ):
            return "unreviewed", "chunk_only_answer"
        return "unreviewed", ""


def _grounding_terms(text: str) -> set[str]:
    return {
        token
        for token in [
            raw.strip(".,;:()[]{}")
            for raw in re.findall(r"[a-z0-9%:/\\.-]{3,}", sanitize_text(text).lower())
        ]
        if token and token not in _GROUNDING_STOPWORDS
    }


def _split_grounding_claims(answer: str, *, limit: int = 8) -> list[str]:
    cleaned = sanitize_text(answer).strip()
    if not cleaned:
        return []
    parts = [
        part.strip()
        for part in re.split(r"(?<=[\\.\\!\\?;])\\s+|\\n+", cleaned)
        if part.strip()
    ]
    claims = [part[:320] for part in parts if len(_grounding_terms(part)) >= 2 or any(char.isdigit() for char in part)]
    if not claims and cleaned:
        claims = [cleaned[:320]]
    return claims[:limit]


def _contains_numeric_claim(claim: str) -> bool:
    return any(char.isdigit() for char in sanitize_text(claim))


def _claim_evidence_support(
    claim: str,
    normalized_query: str,
    evidence_rows: list[dict[str, Any]],
) -> tuple[bool, list[str]]:
    claim_terms = _grounding_terms(claim)
    if not claim_terms:
        return False, []
    query_terms = _grounding_terms(normalized_query)
    novel_terms = sorted(claim_terms - query_terms)
    numeric_terms = {term for term in claim_terms if any(char.isdigit() for char in term)}
    evidence_ids: list[str] = []
    supported_novel: set[str] = set()
    supported_numeric: set[str] = set()
    broad_overlap_hits = 0
    for row in evidence_rows:
        row_terms = _grounding_terms(str(row.get("text") or ""))
        if not row_terms:
            continue
        overlap = claim_terms & row_terms
        if overlap:
            broad_overlap_hits += 1
        novel_overlap = set(novel_terms) & row_terms
        numeric_overlap = numeric_terms & row_terms
        if novel_overlap or numeric_overlap:
            evidence_ids.append(str(row.get("id") or ""))
        supported_novel |= novel_overlap
        supported_numeric |= numeric_overlap
    if numeric_terms and supported_numeric != numeric_terms:
        return False, list(dict.fromkeys(item for item in evidence_ids if item))
    if not novel_terms:
        return broad_overlap_hits > 0, list(dict.fromkeys(item for item in evidence_ids if item))
    required_ratio = 1.0 if len(novel_terms) <= 2 else 0.6
    passed = (len(supported_novel) / max(1, len(novel_terms))) >= required_ratio
    return passed, list(dict.fromkeys(item for item in evidence_ids if item))


def _lexical_grounding_check(
    *,
    answer: str,
    question_type: str,
    normalized_query: str,
    evidence_rows: list[dict[str, Any]],
    runtime_config: dict[str, Any],
) -> dict[str, Any]:
    claims = _split_grounding_claims(answer)
    if not claims:
        return {
            "method": "lexical_fallback",
            "passed": False,
            "supported_ratio": 0.0,
            "unsupported_claims": [],
            "claims": [],
        }
    claim_rows: list[dict[str, Any]] = []
    unsupported_claims: list[str] = []
    supported_claim_count = 0
    for claim in claims:
        supported, evidence_ids = _claim_evidence_support(claim, normalized_query, evidence_rows)
        if supported:
            supported_claim_count += 1
        else:
            unsupported_claims.append(claim)
        claim_rows.append(
            {
                "claim": claim,
                "supported": supported,
                "evidence_ids": evidence_ids[:4],
            }
        )
    supported_ratio = round(supported_claim_count / max(1, len(claim_rows)), 4)
    configured_min_supported_ratio = float(runtime_config.get("claim_verifier_min_supported_ratio") or 0.66)
    relaxed_question_types = {"explanation", "procedure", "comparison"}
    if len(claim_rows) <= 2:
        min_supported_ratio = 1.0
    elif question_type in relaxed_question_types:
        min_supported_ratio = configured_min_supported_ratio
    else:
        min_supported_ratio = max(0.75, configured_min_supported_ratio)
    unsupported_numeric_claims = [claim for claim in unsupported_claims if _contains_numeric_claim(claim)]
    unsupported_non_numeric_claims = [claim for claim in unsupported_claims if claim not in unsupported_numeric_claims]
    if unsupported_numeric_claims:
        unsupported_ok = False
    elif question_type in relaxed_question_types and len(claim_rows) >= 3:
        unsupported_ok = len(unsupported_non_numeric_claims) <= 1
    else:
        unsupported_ok = not unsupported_claims
    passed = supported_ratio >= min_supported_ratio and unsupported_ok
    return {
        "method": "lexical_fallback",
        "passed": passed,
        "supported_ratio": supported_ratio,
        "unsupported_claims": unsupported_claims[:6],
        "claims": claim_rows[:8],
    }


def build_supported_subset_answer(
    *,
    grounding_check: dict[str, Any],
    question_type: str,
) -> tuple[str, list[str]] | None:
    if question_type in {"source_lookup", "visual_lookup"}:
        return None
    claims = [item for item in (grounding_check.get("claims") or []) if isinstance(item, dict)]
    supported_claims = [
        str(item.get("claim") or "").strip()
        for item in claims
        if item.get("supported") and str(item.get("claim") or "").strip()
    ]
    if not supported_claims:
        return None
    unsupported_claims = [
        str(item).strip()
        for item in (grounding_check.get("unsupported_claims") or [])
        if str(item).strip()
    ]
    if any(_contains_numeric_claim(claim) for claim in unsupported_claims):
        return None
    supported_ratio = float(grounding_check.get("supported_ratio") or 0.0)
    if supported_ratio < 0.25:
        return None
    evidence_ids = list(
        dict.fromkeys(
            evidence_id
            for item in claims
            if item.get("supported")
            for evidence_id in [str(value).strip() for value in (item.get("evidence_ids") or []) if str(value).strip()]
        )
    )
    supported_text = " ".join(supported_claims[:3]).strip()
    if not supported_text:
        return None
    supported_text = clean_supported_subset_claim_text(supported_text)
    return supported_text.strip(), evidence_ids


def clean_supported_subset_claim_text(text: str) -> str:
    cleaned = str(text or "").strip()
    cleaned = re.sub(r"^(based on (the )?(provided )?(sources|indexed corpus|retrieved evidence)\s*:\s*)", "", cleaned, flags=re.IGNORECASE)
    cleaned = re.sub(r"^(according to (the )?(provided )?(sources|indexed corpus|retrieved evidence)\s*[:,]?\s*)", "", cleaned, flags=re.IGNORECASE)
    cleaned = re.sub(r"^(from (the )?(provided )?(sources|indexed corpus|retrieved evidence)\s*[:,]?\s*)", "", cleaned, flags=re.IGNORECASE)
    cleaned = re.sub(r"\s+", " ", cleaned).strip()
    return cleaned


def clean_user_facing_answer_text(text: str) -> str:
    cleaned = clean_supported_subset_claim_text(text)
    cleaned = re.sub(r"^(i (think|believe|guess|would say)\s+)", "", cleaned, flags=re.IGNORECASE)
    cleaned = re.sub(r"^(i('m| am) not sure(,? but)?\s+)", "", cleaned, flags=re.IGNORECASE)
    cleaned = re.sub(r"^(it (seems|looks like|appears)\s+)", "", cleaned, flags=re.IGNORECASE)
    cleaned = re.sub(r"^(here'?s (what|the short version|the simple version)\s*[:\\-]\s*)", "", cleaned, flags=re.IGNORECASE)
    cleaned = re.sub(r"^(in plain terms\s*[:\\-]\s*)", "", cleaned, flags=re.IGNORECASE)
    cleaned = re.sub(r"\s+", " ", cleaned).strip()
    return cleaned


def _claim_verifier_schema() -> dict[str, Any]:
    return {
        "type": "object",
        "additionalProperties": False,
        "properties": {
            "claims": {
                "type": "array",
                "items": {
                    "type": "object",
                    "additionalProperties": False,
                    "properties": {
                        "claim": {"type": "string"},
                        "supported": {"type": "boolean"},
                        "evidence_ids": {"type": "array", "items": {"type": "string"}},
                        "support_basis": {"type": "string", "enum": ["evidence", "world_knowledge", "unsupported"]},
                    },
                    "required": ["claim", "supported", "evidence_ids", "support_basis"],
                },
            },
            "unsupported_claims": {"type": "array", "items": {"type": "string"}},
            "supported_ratio": {"type": "number"},
            "verdict": {"type": "boolean"},
        },
        "required": ["claims", "unsupported_claims", "supported_ratio", "verdict"],
    }


def _normalize_query(value: str) -> str:
    return sanitize_text(value).strip()


def _normalize_reason_code(value: Any, fallback: str) -> str:
    normalized = re.sub(r"[^a-z0-9_]+", "_", str(value or "").strip().lower()).strip("_")
    if normalized in _SAFE_ABSTAIN_REASONS:
        return normalized
    if fallback in _SAFE_ABSTAIN_REASONS:
        return fallback
    return "model_abstained"
