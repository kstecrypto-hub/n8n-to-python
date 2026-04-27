"""Model payload and response helpers for the deterministic answer path.

This module owns the runtime's model-transport concerns:
- JSON payload construction
- response schema declaration
- answer-path transport recovery
- message-content extraction

It does not own retrieval, memory policy, or route concerns.
"""

from __future__ import annotations

import json
from typing import Any

import httpx

from src.bee_ingestion.agent_contracts import AgentPromptBundle, AgentQueryError


def build_chat_payload(
    system_prompt: str,
    user_prompt: str,
    prompt_bundle: AgentPromptBundle,
    runtime_config: dict[str, Any],
) -> dict[str, Any]:
    model = str(runtime_config["model"] or "")
    reasoning_effort = str(runtime_config["reasoning_effort"] or "")
    messages = [{"role": "system", "content": system_prompt}]
    if prompt_bundle.profile_summary:
        messages.append(
            {
                "role": "system",
                "content": "user_profile: " + json.dumps(_json_safe(prompt_bundle.profile_summary), ensure_ascii=False),
            }
        )
    if prompt_bundle.session_summary:
        messages.append(
            {
                "role": "system",
                "content": "session_memory: " + json.dumps(_json_safe(prompt_bundle.session_summary), ensure_ascii=False),
            }
        )
    payload = {
        "model": model,
        "reasoning_effort": reasoning_effort,
        "messages": [*messages, *prompt_bundle.prior_context, {"role": "user", "content": user_prompt}],
        "response_format": {
            "type": "json_schema",
            "json_schema": {
                "name": "agent_query_result",
                "strict": True,
                "schema": agent_response_schema(),
            },
        },
    }
    if runtime_config["temperature"] > 0 and supports_temperature(model, reasoning_effort):
        payload["temperature"] = runtime_config["temperature"]
    if runtime_config["max_completion_tokens"] > 0:
        payload["max_completion_tokens"] = runtime_config["max_completion_tokens"]
    return payload


def build_plaintext_recovery_payload(
    payload: dict[str, Any],
    *,
    instruction: str,
) -> dict[str, Any]:
    messages = list(payload.get("messages") or [])
    insert_index = 1 if messages and messages[0].get("role") == "system" else 0
    messages.insert(
        insert_index,
        {
            "role": "system",
            "content": instruction,
        },
    )
    recovery_payload = {
        key: value
        for key, value in payload.items()
        if key != "response_format"
    }
    recovery_payload["messages"] = messages
    return recovery_payload


def with_model_override(payload: dict[str, Any], model: str, reasoning_effort: str) -> dict[str, Any]:
    updated = {**payload, "model": model, "reasoning_effort": reasoning_effort}
    if "temperature" in updated and not supports_temperature(model, reasoning_effort):
        updated.pop("temperature", None)
    return updated


def recover_nonempty_text_content(
    *,
    base_url: str,
    api_key: str,
    timeout_seconds: float,
    primary_payload: dict[str, Any],
    fallback_model: str,
    fallback_reasoning_effort: str,
) -> tuple[str, dict[str, Any], str]:
    body = call_openai_json_payload(
        base_url=base_url,
        api_key=api_key,
        payload=primary_payload,
        timeout_seconds=timeout_seconds,
    )
    content = extract_message_content(body)
    if str(content or "").strip():
        return content, body, str(primary_payload.get("model") or "")
    fallback_model = str(fallback_model or "").strip()
    if fallback_model and fallback_model != str(primary_payload.get("model") or "").strip():
        fallback_payload = with_model_override(primary_payload, fallback_model, fallback_reasoning_effort)
        body = call_openai_json_payload(
            base_url=base_url,
            api_key=api_key,
            payload=fallback_payload,
            timeout_seconds=timeout_seconds,
        )
        content = extract_message_content(body)
        if str(content or "").strip():
            return content, body, fallback_model
    raise AgentQueryError("Agent answer returned empty content")


def supports_temperature(model: str, reasoning_effort: str) -> bool:
    lowered = model.lower().strip()
    if lowered.startswith("gpt-5.4") or lowered.startswith("gpt-5.2"):
        return reasoning_effort == "none"
    if lowered.startswith("gpt-5"):
        return False
    return True


def build_model_json_payload(
    *,
    model: str,
    reasoning_effort: str,
    system_prompt: str,
    user_payload: dict[str, Any],
    schema_name: str,
    schema: dict[str, Any],
    temperature: float,
    max_completion_tokens: int,
) -> dict[str, Any]:
    payload = {
        "model": model,
        "reasoning_effort": reasoning_effort,
        "messages": [
            {"role": "system", "content": system_prompt},
            {"role": "user", "content": json.dumps(_json_safe(user_payload), ensure_ascii=False)},
        ],
        "response_format": {
            "type": "json_schema",
            "json_schema": {
                "name": schema_name,
                "strict": True,
                "schema": schema,
            },
        },
    }
    if temperature > 0 and supports_temperature(model, reasoning_effort):
        payload["temperature"] = temperature
    if max_completion_tokens > 0:
        payload["max_completion_tokens"] = max_completion_tokens
    return payload


def call_openai_json_payload(
    *,
    base_url: str,
    api_key: str,
    payload: dict[str, Any],
    timeout_seconds: float,
) -> dict[str, Any]:
    headers = {
        "Authorization": f"Bearer {api_key}",
        "Content-Type": "application/json",
    }
    with httpx.Client(timeout=timeout_seconds) as client:
        response = client.post(f"{base_url}/chat/completions", headers=headers, json=payload)
        response.raise_for_status()
        return response.json()


def extract_message_content(body: dict[str, Any]) -> str:
    choices = body.get("choices") or []
    if not choices:
        raise AgentQueryError("Agent answer returned no choices")
    message = choices[0].get("message") or {}
    refusal = message.get("refusal")
    if refusal:
        raise AgentQueryError(f"Agent answer refused: {refusal}")

    content = message.get("content")
    if isinstance(content, str) and content.strip():
        return content
    if isinstance(content, list):
        parts: list[str] = []
        for item in content:
            if isinstance(item, dict):
                text = item.get("text")
                if isinstance(text, dict):
                    value = text.get("value")
                    if value:
                        parts.append(str(value))
                elif text:
                    parts.append(str(text))
        if parts:
            return "".join(parts)
    parsed = message.get("parsed")
    if isinstance(parsed, (dict, list)):
        return json.dumps(_json_safe(parsed), ensure_ascii=False)
    if isinstance(content, str):
        return content
    raise AgentQueryError("Agent answer returned no textual content")


def agent_response_schema() -> dict[str, Any]:
    return {
        "type": "object",
        "additionalProperties": False,
        "properties": {
            "answer": {"type": "string"},
            "confidence": {"type": "number"},
            "abstained": {"type": "boolean"},
            "abstain_reason": {"type": ["string", "null"]},
            "used_chunk_ids": {
                "type": "array",
                "items": {"type": "string"},
            },
            "used_asset_ids": {
                "type": "array",
                "items": {"type": "string"},
            },
            "used_sensor_row_ids": {
                "type": "array",
                "items": {"type": "string"},
            },
            "used_evidence_ids": {
                "type": "array",
                "items": {"type": "string"},
            },
            "supporting_assertions": {
                "type": "array",
                "items": {"type": "string"},
            },
            "supporting_entities": {
                "type": "array",
                "items": {"type": "string"},
            },
        },
        "required": [
            "answer",
            "confidence",
            "abstained",
            "abstain_reason",
            "used_chunk_ids",
            "used_asset_ids",
            "used_sensor_row_ids",
            "used_evidence_ids",
            "supporting_assertions",
            "supporting_entities",
        ],
    }


def _json_safe(value: Any) -> Any:
    if isinstance(value, dict):
        return {str(key): _json_safe(item) for key, item in value.items()}
    if isinstance(value, list):
        return [_json_safe(item) for item in value]
    if isinstance(value, tuple):
        return [_json_safe(item) for item in value]
    if value is None or isinstance(value, (str, int, float, bool)):
        return value
    return str(value)
