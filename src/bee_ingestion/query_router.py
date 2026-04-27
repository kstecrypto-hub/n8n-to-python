from __future__ import annotations

from datetime import datetime, timezone
import json
import re
from typing import Any

import httpx

from src.bee_ingestion.settings import settings


QUERY_TYPES = (
    "visual_lookup",
    "definition",
    "source_lookup",
    "procedure",
    "comparison",
    "explanation",
    "fact",
)
_MAX_ROUTER_QUESTION_CHARS = 4000

QUERY_ROUTER_SYSTEM_PROMPT = (
    "You are a routing classifier for a beekeeping QA system.\n"
    "Infer the user's intent from the meaning of the question, not the opening phrase.\n"
    "Many questions begin with a scenario description or context before the actual ask.\n"
    "Return JSON only and follow the schema exactly.\n"
    "Choose the smallest question_type that still preserves enough retrieval breadth.\n"
    "If the question asks about diagrams, figures, scans, images, or visually described content, mark it as visual_lookup.\n"
    "If the question is asking for a meaning or definition, choose definition.\n"
    "If the question asks where or when something appears in the corpus, choose source_lookup.\n"
    "If the question is asking how to do something or what to do next, choose procedure.\n"
    "If the question compares two or more things, choose comparison.\n"
    "If the question asks why something happens or asks for explanation, choose explanation.\n"
    "Use fact for all other corpus-grounded questions.\n"
)


def classify_question_fallback(question: str) -> str:
    lowered = question.lower()
    if any(term in lowered for term in ("image", "visual", "diagram", "figure", "illustration", "scanned", "scan")):
        return "visual_lookup"
    if lowered.startswith("what is") or lowered.startswith("who is") or lowered.startswith("define"):
        return "definition"
    if lowered.startswith("where ") or lowered.startswith("when "):
        return "source_lookup"
    if lowered.startswith("how ") or "how do" in lowered or "how can" in lowered:
        return "procedure"
    if lowered.startswith("compare") or "difference" in lowered or "versus" in lowered or "advantages" in lowered or "limitations" in lowered:
        return "comparison"
    if lowered.startswith("why ") or "explain" in lowered:
        return "explanation"
    return "fact"


def route_question(question: str, runtime_config: dict[str, Any]) -> dict[str, Any]:
    normalized = _normalize_query(question)
    fallback_question_type = classify_question_fallback(normalized)
    fallback_top_k = _top_k_for_question_type(fallback_question_type, runtime_config)

    if not runtime_config.get("router_enabled", True):
        return _route_payload(
            source="fallback:disabled",
            question_type=fallback_question_type,
            top_k=fallback_top_k,
            confidence=0.0,
            reason="Router disabled",
            provider="disabled",
            model="disabled",
        )

    provider = _resolve_router_provider(runtime_config)
    if provider != "openai":
        return _route_payload(
            source="fallback:provider_disabled",
            question_type=fallback_question_type,
            top_k=fallback_top_k,
            confidence=0.0,
            reason="Router provider is not openai",
            provider=provider,
            model=str(runtime_config.get("router_model") or ""),
        )

    api_key = _resolve_router_api_key(runtime_config)
    if not api_key:
        return _route_payload(
            source="fallback:no_api_key",
            question_type=fallback_question_type,
            top_k=fallback_top_k,
            confidence=0.0,
            reason="No API key configured for routing",
            provider=provider,
            model=str(runtime_config.get("router_model") or ""),
        )

    model = str(runtime_config.get("router_model") or settings.agent_router_model or "gpt-5-nano").strip()
    reasoning_effort = str(runtime_config.get("router_reasoning_effort") or settings.agent_router_reasoning_effort or "none").strip()
    system_prompt = str(runtime_config.get("router_system_prompt") or QUERY_ROUTER_SYSTEM_PROMPT).strip() or QUERY_ROUTER_SYSTEM_PROMPT
    payload = {
        "model": model,
        "reasoning_effort": reasoning_effort,
        "messages": [
            {"role": "system", "content": system_prompt},
            {
                "role": "user",
                "content": json.dumps(
                    {
                        "question": normalized,
                        "allowed_question_types": QUERY_TYPES,
                        "output_requirements": {
                            "question_type": "One of the allowed values.",
                            "suggested_top_k": "Integer breadth hint for retrieval.",
                            "confidence": "0 to 1.",
                            "requires_visual": "True only if visual evidence is likely needed to answer.",
                            "document_spread": "single, few, or broad depending on how many documents should remain in contention.",
                            "reason": "Short explanation.",
                        },
                    },
                    ensure_ascii=False,
                ),
            },
        ],
        "response_format": {
            "type": "json_schema",
            "json_schema": {
                "name": "agent_query_router",
                "strict": True,
                "schema": _router_response_schema(),
            },
        },
    }
    temperature = float(runtime_config.get("router_temperature") or 0.0)
    if temperature > 0 and _supports_temperature(model, reasoning_effort):
        payload["temperature"] = temperature
    max_completion_tokens = int(runtime_config.get("router_max_completion_tokens") or 128)
    if max_completion_tokens > 0:
        payload["max_completion_tokens"] = max_completion_tokens

    headers = {
        "Authorization": f"Bearer {api_key}",
        "Content-Type": "application/json",
    }
    base_url = _resolve_router_base_url(runtime_config)
    timeout_seconds = float(runtime_config.get("router_timeout_seconds") or 20.0)
    try:
        with httpx.Client(timeout=timeout_seconds) as client:
            response = client.post(f"{base_url}/chat/completions", headers=headers, json=payload)
            response.raise_for_status()
            body = response.json()
        content = _extract_message_content(body)
        data = json.loads(content)
        question_type = str(data.get("question_type") or fallback_question_type).strip()
        if question_type not in QUERY_TYPES:
            question_type = fallback_question_type
        confidence = float(data.get("confidence") or 0.0)
        if confidence < float(runtime_config.get("router_confidence_threshold") or 0.55):
            return _route_payload(
                source="fallback:low_confidence",
                question_type=fallback_question_type,
                top_k=fallback_top_k,
                confidence=confidence,
                reason=str(data.get("reason") or "Router confidence below threshold"),
                provider=provider,
                model=model,
                raw_router=data,
            )
        return _route_payload(
            source="router",
            question_type=question_type,
            top_k=_clamp_top_k(data.get("suggested_top_k"), runtime_config),
            confidence=confidence,
            reason=str(data.get("reason") or ""),
            requires_visual=bool(data.get("requires_visual", question_type == "visual_lookup")),
            document_spread=_coerce_document_spread(data.get("document_spread")),
            provider=provider,
            model=model,
            raw_router=data,
        )
    except (httpx.HTTPError, json.JSONDecodeError, KeyError, TypeError, ValueError):
        return _route_payload(
            source="fallback:error",
            question_type=fallback_question_type,
            top_k=fallback_top_k,
            confidence=0.0,
            reason="router_request_failed",
            provider=provider,
            model=model,
        )


def route_question_cached(
    question: str,
    runtime_config: dict[str, Any],
    *,
    tenant_id: str,
    repository,
) -> dict[str, Any]:
    normalized = _normalize_query(question)
    if not normalized or not runtime_config.get("router_cache_enabled", True):
        return route_question(question, runtime_config)
    pattern_signature, query_keywords = repository.build_query_pattern(normalized)
    if not pattern_signature.strip() or len(query_keywords) < 2:
        return route_question(question, runtime_config)
    pattern = repository.get_agent_query_pattern(tenant_id, pattern_signature)
    cached_route = _read_cached_route(pattern, runtime_config)
    if cached_route is not None:
        repository.touch_agent_query_pattern_route_hit(tenant_id, pattern_signature)
        return cached_route
    routed = route_question(question, runtime_config)
    if str(routed.get("source") or "").startswith("router"):
        routed = dict(routed)
        routed["cache_identity"] = _router_cache_identity(runtime_config)
        repository.save_agent_query_pattern_route(
            tenant_id=tenant_id,
            pattern_signature=pattern_signature,
            query_keywords=query_keywords,
            example_query=normalized,
            route_payload=routed,
            router_model=str(routed.get("model") or runtime_config.get("router_model") or ""),
        )
    return routed


def _route_payload(
    *,
    source: str,
    question_type: str,
    top_k: int | None,
    confidence: float,
    reason: str,
    provider: str = "",
    model: str = "",
    requires_visual: bool = False,
    document_spread: str = "few",
    raw_router: dict[str, Any] | None = None,
) -> dict[str, Any]:
    return {
        "source": source,
        "question_type": question_type,
        "top_k": top_k,
        "confidence": round(float(confidence), 4),
        "reason": reason,
        "requires_visual": bool(requires_visual) or question_type == "visual_lookup",
        "document_spread": document_spread,
        "provider": provider,
        "model": model,
        "raw_router": raw_router or {},
    }


def _clamp_top_k(value: Any, runtime_config: dict[str, Any]) -> int | None:
    if value is None:
        return None
    try:
        top_k = int(value)
    except (TypeError, ValueError):
        return None
    return max(1, min(top_k, int(runtime_config.get("max_top_k") or 12)))


def _top_k_for_question_type(question_type: str, runtime_config: dict[str, Any]) -> int:
    policy_key = f"{question_type}_top_k"
    policy_value = runtime_config.get(policy_key, runtime_config.get("default_top_k", 8))
    try:
        return max(1, min(int(policy_value), int(runtime_config.get("max_top_k") or 12)))
    except (TypeError, ValueError):
        return max(1, min(8, int(runtime_config.get("max_top_k") or 12)))


def _coerce_document_spread(value: Any) -> str:
    normalized = str(value or "").strip().lower()
    if normalized in {"single", "few", "broad"}:
        return normalized
    return "few"


def _supports_temperature(model: str, reasoning_effort: str) -> bool:
    lowered = model.lower().strip()
    if lowered.startswith("gpt-5.4") or lowered.startswith("gpt-5.2"):
        return reasoning_effort == "none"
    if lowered.startswith("gpt-5"):
        return False
    return True


def _normalize_query(question: str) -> str:
    normalized = re.sub(r"\s+", " ", question.replace("\x00", " ").strip())
    return normalized[:_MAX_ROUTER_QUESTION_CHARS]


def _read_cached_route(pattern: dict[str, Any] | None, runtime_config: dict[str, Any]) -> dict[str, Any] | None:
    if not pattern:
        return None
    cache_payload = dict(pattern.get("router_cache_json") or {})
    if not cache_payload:
        return None
    cached_at = pattern.get("router_cached_at")
    if isinstance(cached_at, datetime):
        age_seconds = (datetime.now(timezone.utc) - cached_at.astimezone(timezone.utc)).total_seconds()
        if age_seconds > float(runtime_config.get("router_cache_max_age_seconds") or 0):
            return None
    else:
        return None
    if str(cache_payload.get("cache_identity") or "") != _router_cache_identity(runtime_config):
        return None
    validated = _validate_cached_route_payload(cache_payload, runtime_config)
    if validated is None:
        return None
    validated["source"] = "router_cache"
    return validated


def _router_cache_identity(runtime_config: dict[str, Any]) -> str:
    payload = {
        "provider": str(runtime_config.get("router_provider") or runtime_config.get("provider") or ""),
        "base_url": str(runtime_config.get("router_base_url") or runtime_config.get("base_url") or ""),
        "model": str(runtime_config.get("router_model") or ""),
        "reasoning_effort": str(runtime_config.get("router_reasoning_effort") or ""),
        "prompt_version": str(runtime_config.get("router_prompt_version") or ""),
        "system_prompt": str(runtime_config.get("router_system_prompt") or ""),
        "temperature": float(runtime_config.get("router_temperature") or 0.0),
        "max_completion_tokens": int(runtime_config.get("router_max_completion_tokens") or 0),
        "confidence_threshold": float(runtime_config.get("router_confidence_threshold") or 0.0),
    }
    return json.dumps(payload, ensure_ascii=False, sort_keys=True)


def _extract_message_content(body: dict[str, Any]) -> str:
    choices = body.get("choices") or []
    if not choices:
        raise ValueError("Router response is missing choices")
    message = choices[0].get("message") or {}
    content = message.get("content")
    if isinstance(content, str) and content.strip():
        return content
    if isinstance(content, list):
        parts: list[str] = []
        for item in content:
            if isinstance(item, dict) and str(item.get("type") or "").strip() in {"output_text", "text"}:
                text = str(item.get("text") or "").strip()
                if text:
                    parts.append(text)
        if parts:
            return "".join(parts)
    raise ValueError("Router response is missing content")


def _resolve_router_provider(runtime_config: dict[str, Any]) -> str:
    configured = str(runtime_config.get("router_provider") or "").strip().lower()
    if configured in {"", "auto"}:
        configured = str(runtime_config.get("provider") or settings.agent_provider or "").strip().lower()
    if configured in {"", "auto"}:
        configured = "openai"
    return configured


def _resolve_router_api_key(runtime_config: dict[str, Any]) -> str:
    return str(runtime_config.get("api_key_override") or settings.agent_api_key or "").strip()


def _resolve_router_base_url(runtime_config: dict[str, Any]) -> str:
    return str(runtime_config.get("router_base_url") or runtime_config.get("base_url") or settings.agent_base_url).rstrip("/")


def _validate_cached_route_payload(cache_payload: dict[str, Any], runtime_config: dict[str, Any]) -> dict[str, Any] | None:
    question_type = str(cache_payload.get("question_type") or "").strip()
    if question_type not in QUERY_TYPES:
        return None
    top_k = _clamp_top_k(cache_payload.get("top_k"), runtime_config)
    if top_k is None:
        return None
    try:
        confidence = float(cache_payload.get("confidence") or 0.0)
    except (TypeError, ValueError):
        return None
    payload = {
        "source": str(cache_payload.get("source") or "router_cache").strip() or "router_cache",
        "question_type": question_type,
        "top_k": top_k,
        "confidence": max(0.0, min(1.0, confidence)),
        "reason": str(cache_payload.get("reason") or "").strip(),
        "requires_visual": bool(cache_payload.get("requires_visual", question_type == "visual_lookup")),
        "document_spread": _coerce_document_spread(cache_payload.get("document_spread")),
        "provider": str(cache_payload.get("provider") or "").strip(),
        "model": str(cache_payload.get("model") or "").strip(),
        "raw_router": dict(cache_payload.get("raw_router") or {}),
        "cache_identity": str(cache_payload.get("cache_identity") or ""),
    }
    return payload


def _router_response_schema() -> dict[str, Any]:
    return {
        "type": "object",
        "properties": {
            "question_type": {
                "type": "string",
                "enum": list(QUERY_TYPES),
            },
            "suggested_top_k": {
                "type": "integer",
                "minimum": 1,
                "maximum": 24,
            },
            "confidence": {
                "type": "number",
                "minimum": 0.0,
                "maximum": 1.0,
            },
            "requires_visual": {
                "type": "boolean",
            },
            "document_spread": {
                "type": "string",
                "enum": ["single", "few", "broad"],
            },
            "reason": {
                "type": "string",
            },
        },
        "required": ["question_type", "suggested_top_k", "confidence", "requires_visual", "document_spread", "reason"],
        "additionalProperties": False,
    }
