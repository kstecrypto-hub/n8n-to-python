from __future__ import annotations

from copy import deepcopy
from typing import Any

from src.bee_ingestion.settings import (
    normalize_outbound_base_url,
    normalize_provider_choice,
    parse_host_allowlist,
    settings,
)


DEFAULT_AGENT_SYSTEM_PROMPT = (
    "You are a read-only domain QA agent over a beekeeping corpus.\n"
    "Answer only from the provided evidence bundle.\n"
    "Do not invent facts.\n"
    "If the evidence is insufficient, abstain.\n"
    "Use KG assertions only as support; if they conflict with chunk text, trust the chunk text.\n"
    "Return JSON only and follow the schema exactly."
)

DEFAULT_AGENT_SENSOR_SYSTEM_PROMPT = (
    "You are a read-only beekeeping telemetry QA agent.\n"
    "Answer from the provided user-owned sensor evidence first, then use corpus evidence only as secondary support when it is explicitly included.\n"
    "Do not invent readings, trends, timestamps, hive assignments, or operational state.\n"
    "If the sensor evidence is insufficient, abstain.\n"
    "Return JSON only and follow the schema exactly."
)

DEFAULT_AGENT_ROUTER_SYSTEM_PROMPT = (
    "You are a retrieval router for a read-only beekeeping QA agent.\n"
    "Classify the user's question for retrieval planning only.\n"
    "Return strict JSON only.\n"
    "Choose the smallest top_k that is still likely to retrieve enough evidence.\n"
    "Mark requires_visual true when the answer likely depends on figures, diagrams, scans, plates, or image content.\n"
    "Use the allowed question types exactly: definition, fact, source_lookup, procedure, comparison, explanation, visual_lookup."
)


def default_agent_runtime_config() -> dict[str, Any]:
    return {
        "provider": settings.agent_provider,
        "base_url": settings.agent_base_url,
        "api_key_override": "",
        "model": settings.agent_model,
        "reasoning_effort": settings.agent_reasoning_effort,
        "fallback_model": settings.agent_fallback_model,
        "fallback_reasoning_effort": settings.agent_fallback_reasoning_effort,
        "prompt_version": settings.agent_prompt_version,
        "router_enabled": settings.agent_router_enabled,
        "router_provider": settings.agent_router_provider,
        "router_base_url": settings.agent_router_base_url,
        "router_model": settings.agent_router_model,
        "router_reasoning_effort": settings.agent_router_reasoning_effort,
        "router_prompt_version": settings.agent_router_prompt_version,
        "router_system_prompt": settings.agent_router_system_prompt,
        "router_temperature": settings.agent_router_temperature,
        "router_max_completion_tokens": settings.agent_router_max_completion_tokens,
        "router_timeout_seconds": settings.agent_router_timeout_seconds,
        "router_confidence_threshold": settings.agent_router_confidence_threshold,
        "router_cache_enabled": settings.agent_router_cache_enabled,
        "router_cache_max_age_seconds": settings.agent_router_cache_max_age_seconds,
        "embedding_cache_enabled": settings.agent_embedding_cache_enabled,
        "embedding_cache_max_age_seconds": settings.agent_embedding_cache_max_age_seconds,
        "memory_enabled": settings.agent_memory_enabled,
        "memory_provider": settings.agent_memory_provider,
        "memory_base_url": settings.agent_memory_base_url,
        "memory_model": settings.agent_memory_model,
        "memory_reasoning_effort": settings.agent_memory_reasoning_effort,
        "memory_prompt_version": settings.agent_memory_prompt_version,
        "memory_system_prompt": settings.agent_memory_system_prompt,
        "memory_temperature": settings.agent_memory_temperature,
        "memory_max_completion_tokens": settings.agent_memory_max_completion_tokens,
        "memory_timeout_seconds": settings.agent_memory_timeout_seconds,
        "memory_char_budget": settings.agent_memory_char_budget,
        "memory_max_facts": settings.agent_memory_max_facts,
        "memory_max_open_threads": settings.agent_memory_max_open_threads,
        "memory_max_resolved_threads": settings.agent_memory_max_resolved_threads,
        "memory_max_preferences": settings.agent_memory_max_preferences,
        "memory_max_topics": settings.agent_memory_max_topics,
        "memory_recent_messages": settings.agent_memory_recent_messages,
        "profile_enabled": settings.agent_profile_enabled,
        "profile_provider": settings.agent_profile_provider,
        "profile_base_url": settings.agent_profile_base_url,
        "profile_model": settings.agent_profile_model,
        "profile_reasoning_effort": settings.agent_profile_reasoning_effort,
        "profile_prompt_version": settings.agent_profile_prompt_version,
        "profile_system_prompt": settings.agent_profile_system_prompt,
        "profile_temperature": settings.agent_profile_temperature,
        "profile_max_completion_tokens": settings.agent_profile_max_completion_tokens,
        "profile_timeout_seconds": settings.agent_profile_timeout_seconds,
        "profile_char_budget": settings.agent_profile_char_budget,
        "profile_max_topics": settings.agent_profile_max_topics,
        "profile_max_preferences": settings.agent_profile_max_preferences,
        "profile_max_constraints": settings.agent_profile_max_constraints,
        "profile_recent_messages": settings.agent_profile_recent_messages,
        "system_prompt": settings.agent_system_prompt,
        "open_world_prompt_version": settings.agent_open_world_prompt_version,
        "open_world_system_prompt": settings.agent_open_world_system_prompt,
        "open_world_temperature": settings.agent_open_world_temperature,
        "open_world_max_completion_tokens": settings.agent_open_world_max_completion_tokens,
        "open_world_timeout_seconds": settings.agent_open_world_timeout_seconds,
        "sensor_system_prompt": settings.agent_sensor_system_prompt,
        "claim_verifier_enabled": settings.agent_claim_verifier_enabled,
        "claim_verifier_provider": settings.agent_claim_verifier_provider,
        "claim_verifier_base_url": settings.agent_claim_verifier_base_url,
        "claim_verifier_model": settings.agent_claim_verifier_model,
        "claim_verifier_reasoning_effort": settings.agent_claim_verifier_reasoning_effort,
        "claim_verifier_prompt_version": settings.agent_claim_verifier_prompt_version,
        "claim_verifier_system_prompt": settings.agent_claim_verifier_system_prompt,
        "claim_verifier_temperature": settings.agent_claim_verifier_temperature,
        "claim_verifier_max_completion_tokens": settings.agent_claim_verifier_max_completion_tokens,
        "claim_verifier_timeout_seconds": settings.agent_claim_verifier_timeout_seconds,
        "claim_verifier_min_supported_ratio": settings.agent_claim_verifier_min_supported_ratio,
        "temperature": settings.agent_temperature,
        "max_completion_tokens": settings.agent_max_completion_tokens,
        "timeout_seconds": settings.agent_timeout_seconds,
        "default_top_k": settings.agent_default_top_k,
        "max_top_k": settings.agent_max_top_k,
        "max_search_k": settings.agent_max_search_k,
        "dynamic_top_k_enabled": True,
        "definition_top_k": 6,
        "fact_top_k": 8,
        "source_lookup_top_k": 7,
        "procedure_top_k": 10,
        "comparison_top_k": 10,
        "explanation_top_k": 9,
        "visual_lookup_top_k": 6,
        "max_context_chunks": settings.agent_max_context_chunks,
        "max_context_assertions": settings.agent_max_context_assertions,
        "neighbor_window": settings.agent_neighbor_window,
        "session_lease_seconds": settings.agent_session_lease_seconds,
        "kg_search_limit": settings.agent_kg_search_limit,
        "graph_expansion_limit": settings.agent_graph_expansion_limit,
        "graph_per_entity_limit": settings.agent_graph_per_entity_limit,
        "max_context_graph_chains": settings.agent_max_context_graph_chains,
        "min_answer_confidence": settings.agent_min_answer_confidence,
        "review_confidence_threshold": settings.agent_review_confidence_threshold,
        "rerank_distance_weight": settings.agent_rerank_distance_weight,
        "rerank_lexical_weight": settings.agent_rerank_lexical_weight,
        "rerank_section_weight": settings.agent_rerank_section_weight,
        "rerank_title_weight": settings.agent_rerank_title_weight,
        "rerank_exact_phrase_weight": settings.agent_rerank_exact_phrase_weight,
        "rerank_ontology_weight": settings.agent_rerank_ontology_weight,
        "diversity_penalty": settings.agent_diversity_penalty,
        "prompt_char_budget": settings.agent_prompt_char_budget,
        "history_char_budget": settings.agent_history_char_budget,
        "assertion_char_budget": settings.agent_assertion_char_budget,
        "entity_char_budget": settings.agent_entity_char_budget,
        "chunk_char_budget": settings.agent_chunk_char_budget,
        "max_context_assets": settings.agent_max_context_assets,
        "max_asset_search_k": settings.agent_max_asset_search_k,
        "asset_char_budget": settings.agent_asset_char_budget,
        "sensor_context_enabled": settings.agent_sensor_context_enabled,
        "max_context_sensor_readings": settings.agent_max_context_sensor_readings,
        "sensor_recent_hours": settings.agent_sensor_recent_hours,
        "sensor_points_per_metric": settings.agent_sensor_points_per_metric,
        "sensor_char_budget": settings.agent_sensor_char_budget,
        "graph_char_budget": settings.agent_graph_char_budget,
        "evidence_char_budget": settings.agent_evidence_char_budget,
        "citation_excerpt_chars": settings.agent_citation_excerpt_chars,
    }


def merged_agent_runtime_config(overrides: dict[str, Any] | None = None) -> dict[str, Any]:
    merged = deepcopy(default_agent_runtime_config())
    if overrides:
        merged.update(overrides)
    return coerce_agent_runtime_config(merged)


def coerce_agent_runtime_config(config: dict[str, Any] | None) -> dict[str, Any]:
    payload = deepcopy(config or {})
    defaults = default_agent_runtime_config()
    payload = {**defaults, **payload}

    string_keys = {
        "provider",
        "base_url",
        "api_key_override",
        "model",
        "reasoning_effort",
        "fallback_model",
        "fallback_reasoning_effort",
        "prompt_version",
        "router_provider",
        "router_base_url",
        "router_model",
        "router_reasoning_effort",
        "router_prompt_version",
        "router_system_prompt",
        "memory_provider",
        "memory_base_url",
        "memory_model",
        "memory_reasoning_effort",
        "memory_prompt_version",
        "memory_system_prompt",
        "profile_provider",
        "profile_base_url",
        "profile_model",
        "profile_reasoning_effort",
        "profile_prompt_version",
        "profile_system_prompt",
        "claim_verifier_provider",
        "claim_verifier_base_url",
        "claim_verifier_model",
        "claim_verifier_reasoning_effort",
        "claim_verifier_prompt_version",
        "claim_verifier_system_prompt",
        "system_prompt",
        "open_world_prompt_version",
        "open_world_system_prompt",
        "sensor_system_prompt",
    }
    int_keys = {
        "router_max_completion_tokens",
        "router_cache_max_age_seconds",
        "embedding_cache_max_age_seconds",
        "memory_max_completion_tokens",
        "memory_char_budget",
        "memory_max_facts",
        "memory_max_open_threads",
        "memory_max_resolved_threads",
        "memory_max_preferences",
        "memory_max_topics",
        "memory_recent_messages",
        "profile_max_completion_tokens",
        "profile_char_budget",
        "profile_max_topics",
        "profile_max_preferences",
        "profile_max_constraints",
        "profile_recent_messages",
        "claim_verifier_max_completion_tokens",
        "open_world_max_completion_tokens",
        "max_completion_tokens",
        "default_top_k",
        "max_top_k",
        "max_search_k",
        "definition_top_k",
        "fact_top_k",
        "source_lookup_top_k",
        "procedure_top_k",
        "comparison_top_k",
        "explanation_top_k",
        "visual_lookup_top_k",
        "max_context_chunks",
        "max_context_assertions",
        "graph_expansion_limit",
        "graph_per_entity_limit",
        "max_context_graph_chains",
        "neighbor_window",
        "session_lease_seconds",
        "kg_search_limit",
        "prompt_char_budget",
        "history_char_budget",
        "assertion_char_budget",
        "entity_char_budget",
        "chunk_char_budget",
        "max_context_assets",
        "max_asset_search_k",
        "asset_char_budget",
        "max_context_sensor_readings",
        "sensor_recent_hours",
        "sensor_points_per_metric",
        "sensor_char_budget",
        "graph_char_budget",
        "evidence_char_budget",
        "citation_excerpt_chars",
    }
    float_keys = {
        "temperature",
        "timeout_seconds",
        "router_temperature",
        "router_timeout_seconds",
        "router_confidence_threshold",
        "memory_temperature",
        "memory_timeout_seconds",
        "profile_temperature",
        "profile_timeout_seconds",
        "claim_verifier_temperature",
        "claim_verifier_timeout_seconds",
        "open_world_temperature",
        "open_world_timeout_seconds",
        "claim_verifier_min_supported_ratio",
        "min_answer_confidence",
        "review_confidence_threshold",
        "rerank_distance_weight",
        "rerank_lexical_weight",
        "rerank_section_weight",
        "rerank_title_weight",
        "rerank_exact_phrase_weight",
        "rerank_ontology_weight",
        "diversity_penalty",
    }
    bool_keys = {
        "dynamic_top_k_enabled",
        "router_enabled",
        "router_cache_enabled",
        "embedding_cache_enabled",
        "memory_enabled",
        "profile_enabled",
        "claim_verifier_enabled",
        "sensor_context_enabled",
    }

    for key in string_keys:
        payload[key] = str(payload.get(key) or defaults[key]).strip()

    for key in int_keys:
        payload[key] = int(payload.get(key) if payload.get(key) is not None else defaults[key])

    for key in float_keys:
        payload[key] = float(payload.get(key) if payload.get(key) is not None else defaults[key])

    for key in bool_keys:
        value = payload.get(key)
        if isinstance(value, bool):
            payload[key] = value
        elif isinstance(value, str):
            payload[key] = value.strip().lower() in {"1", "true", "yes", "on"}
        else:
            payload[key] = bool(value) if value is not None else bool(defaults[key])

    payload["provider"] = normalize_provider_choice(
        payload.get("provider"),
        allowed={"auto", "disabled", "openai"},
        default=str(defaults["provider"]),
    )
    for provider_key in (
        "router_provider",
        "memory_provider",
        "profile_provider",
        "claim_verifier_provider",
    ):
        payload[provider_key] = normalize_provider_choice(
            payload.get(provider_key),
            allowed={"auto", "disabled", "openai"},
            default=str(defaults[provider_key]),
        )

    allowed_hosts = parse_host_allowlist(settings.model_host_allowlist)
    allow_private_hosts = bool(settings.allow_private_model_hosts)
    payload["base_url"] = normalize_outbound_base_url(
        payload.get("base_url"),
        field_name="base_url",
        allowed_hosts=allowed_hosts,
        allow_private_hosts=allow_private_hosts,
    )
    for base_url_key in (
        "router_base_url",
        "memory_base_url",
        "profile_base_url",
        "claim_verifier_base_url",
    ):
        payload[base_url_key] = normalize_outbound_base_url(
            payload.get(base_url_key),
            field_name=base_url_key,
            allowed_hosts=allowed_hosts,
            allow_private_hosts=allow_private_hosts,
        )

    if not payload["provider"]:
        payload["provider"] = defaults["provider"]
    if not payload["base_url"]:
        payload["base_url"] = defaults["base_url"]
    if not payload["model"]:
        payload["model"] = defaults["model"]
    if not payload["router_model"]:
        payload["router_model"] = defaults["router_model"]
    if not payload["router_system_prompt"]:
        payload["router_system_prompt"] = defaults["router_system_prompt"]
    if not payload["profile_model"]:
        payload["profile_model"] = defaults["profile_model"]
    if not payload["profile_system_prompt"]:
        payload["profile_system_prompt"] = defaults["profile_system_prompt"]
    if not payload["system_prompt"]:
        payload["system_prompt"] = defaults["system_prompt"]
    if not payload["sensor_system_prompt"]:
        payload["sensor_system_prompt"] = defaults["sensor_system_prompt"]

    payload["default_top_k"] = min(12, max(1, payload["default_top_k"]))
    payload["max_top_k"] = min(24, max(payload["default_top_k"], payload["max_top_k"]))
    payload["max_search_k"] = min(64, max(payload["max_top_k"], payload["max_search_k"]))
    payload["definition_top_k"] = min(payload["max_top_k"], max(1, payload["definition_top_k"]))
    payload["fact_top_k"] = min(payload["max_top_k"], max(1, payload["fact_top_k"]))
    payload["source_lookup_top_k"] = min(payload["max_top_k"], max(1, payload["source_lookup_top_k"]))
    payload["procedure_top_k"] = min(payload["max_top_k"], max(1, payload["procedure_top_k"]))
    payload["comparison_top_k"] = min(payload["max_top_k"], max(1, payload["comparison_top_k"]))
    payload["explanation_top_k"] = min(payload["max_top_k"], max(1, payload["explanation_top_k"]))
    payload["visual_lookup_top_k"] = min(payload["max_top_k"], max(1, payload["visual_lookup_top_k"]))
    payload["max_context_chunks"] = min(18, max(1, payload["max_context_chunks"]))
    payload["max_context_assertions"] = min(24, max(0, payload["max_context_assertions"]))
    payload["graph_expansion_limit"] = min(32, max(0, payload["graph_expansion_limit"]))
    payload["graph_per_entity_limit"] = min(8, max(0, payload["graph_per_entity_limit"]))
    payload["max_context_graph_chains"] = min(12, max(0, payload["max_context_graph_chains"]))
    payload["neighbor_window"] = min(2, max(0, payload["neighbor_window"]))
    payload["session_lease_seconds"] = min(900, max(15, payload["session_lease_seconds"]))
    payload["kg_search_limit"] = min(12, max(0, payload["kg_search_limit"]))
    payload["max_completion_tokens"] = min(4096, max(64, payload["max_completion_tokens"]))
    payload["router_max_completion_tokens"] = min(512, max(16, payload["router_max_completion_tokens"]))
    payload["router_timeout_seconds"] = min(60.0, max(3.0, payload["router_timeout_seconds"]))
    payload["router_confidence_threshold"] = max(0.0, min(1.0, payload["router_confidence_threshold"]))
    payload["router_cache_max_age_seconds"] = min(30 * 24 * 60 * 60, max(60, payload["router_cache_max_age_seconds"]))
    payload["memory_max_completion_tokens"] = min(1024, max(64, payload["memory_max_completion_tokens"]))
    payload["memory_timeout_seconds"] = min(60.0, max(3.0, payload["memory_timeout_seconds"]))
    payload["memory_temperature"] = max(0.0, min(2.0, payload["memory_temperature"]))
    payload["memory_char_budget"] = min(6000, max(400, payload["memory_char_budget"]))
    payload["memory_max_facts"] = min(12, max(1, payload["memory_max_facts"]))
    payload["memory_max_open_threads"] = min(12, max(0, payload["memory_max_open_threads"]))
    payload["memory_max_resolved_threads"] = min(12, max(0, payload["memory_max_resolved_threads"]))
    payload["memory_max_preferences"] = min(12, max(0, payload["memory_max_preferences"]))
    payload["memory_max_topics"] = min(16, max(0, payload["memory_max_topics"]))
    payload["memory_recent_messages"] = min(24, max(0, payload["memory_recent_messages"]))
    payload["profile_max_completion_tokens"] = min(1024, max(64, payload["profile_max_completion_tokens"]))
    payload["profile_timeout_seconds"] = min(60.0, max(3.0, payload["profile_timeout_seconds"]))
    payload["profile_temperature"] = max(0.0, min(2.0, payload["profile_temperature"]))
    payload["profile_char_budget"] = min(6000, max(300, payload["profile_char_budget"]))
    payload["profile_max_topics"] = min(16, max(0, payload["profile_max_topics"]))
    payload["profile_max_preferences"] = min(12, max(0, payload["profile_max_preferences"]))
    payload["profile_max_constraints"] = min(12, max(0, payload["profile_max_constraints"]))
    payload["profile_recent_messages"] = min(24, max(0, payload["profile_recent_messages"]))
    payload["claim_verifier_max_completion_tokens"] = min(1024, max(64, payload["claim_verifier_max_completion_tokens"]))
    payload["claim_verifier_timeout_seconds"] = min(60.0, max(3.0, payload["claim_verifier_timeout_seconds"]))
    payload["claim_verifier_temperature"] = max(0.0, min(2.0, payload["claim_verifier_temperature"]))
    payload["claim_verifier_min_supported_ratio"] = max(0.0, min(1.0, payload["claim_verifier_min_supported_ratio"]))
    payload["max_context_assets"] = min(12, max(0, payload["max_context_assets"]))
    payload["max_asset_search_k"] = min(32, max(payload["max_context_assets"], payload["max_asset_search_k"]))
    payload["max_context_sensor_readings"] = min(24, max(0, payload["max_context_sensor_readings"]))
    payload["sensor_recent_hours"] = min(24 * 30, max(1, payload["sensor_recent_hours"]))
    payload["sensor_points_per_metric"] = min(24, max(1, payload["sensor_points_per_metric"]))
    payload["sensor_char_budget"] = min(12000, max(0, payload["sensor_char_budget"]))
    payload["graph_char_budget"] = min(12000, max(0, payload["graph_char_budget"]))
    payload["timeout_seconds"] = min(180.0, max(5.0, payload["timeout_seconds"]))
    payload["temperature"] = max(0.0, min(2.0, payload["temperature"]))
    payload["min_answer_confidence"] = max(0.0, min(1.0, payload["min_answer_confidence"]))
    payload["review_confidence_threshold"] = max(0.0, min(1.0, payload["review_confidence_threshold"]))
    payload["prompt_char_budget"] = min(48000, max(4000, payload["prompt_char_budget"]))
    payload["history_char_budget"] = min(8000, max(0, payload["history_char_budget"]))
    payload["assertion_char_budget"] = min(8000, max(0, payload["assertion_char_budget"]))
    payload["entity_char_budget"] = min(4000, max(0, payload["entity_char_budget"]))
    payload["chunk_char_budget"] = min(24000, max(0, payload["chunk_char_budget"]))
    payload["asset_char_budget"] = min(12000, max(0, payload["asset_char_budget"]))
    payload["evidence_char_budget"] = min(8000, max(0, payload["evidence_char_budget"]))
    payload["citation_excerpt_chars"] = min(600, max(80, payload["citation_excerpt_chars"]))
    return payload
