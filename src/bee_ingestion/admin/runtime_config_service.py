"""Admin workflows for runtime config, startup config, and ontology updates.

This module owns operator configuration policy and file-backed config workflows.
It must not own HTTP route declarations, retrieval logic, or chat runtime
execution.
"""

from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path, PureWindowsPath
from typing import Any

from fastapi import HTTPException

from src.bee_ingestion.agent_runtime import (
    coerce_agent_runtime_config,
    default_agent_runtime_config,
    merged_agent_runtime_config,
)
from src.bee_ingestion.kg import load_ontology
from src.bee_ingestion.settings import settings, workspace_root
from src.bee_ingestion.storage import runtime_config_store

SYSTEM_CONFIG_SECRET_MASK = "<unchanged>"
WORKSPACE_ROOT = Path(__file__).resolve().parents[3]
ENV_FILE_PATH = (WORKSPACE_ROOT / ".env").resolve()
WINDOWS_WORKSPACE_ROOT = PureWindowsPath(r"E:\n8n to python")
SYSTEM_CONFIG_GROUPS: dict[str, list[str]] = {
    "platform": [
        "app_env",
        "api_host",
        "api_port",
        "admin_api_token",
        "auth_postgres_dsn",
        "auth_postgres_schema",
        "auth_legacy_sqlite_path",
        "auth_cookie_secure",
        "browser_origin_allowlist",
        "auth_session_max_age_seconds",
        "auth_public_registration_enabled",
        "auth_password_min_length",
        "auth_login_rate_limit_window_seconds",
        "auth_login_rate_limit_max_attempts",
        "public_agent_rate_limit_window_seconds",
        "public_agent_rate_limit_max_requests",
        "runtime_secret_encryption_key",
        "postgres_dsn",
        "chroma_host",
        "chroma_port",
        "chroma_ssl",
        "chroma_path",
        "chroma_collection",
        "chroma_asset_collection",
        "chroma_upsert_batch_size",
        "allow_private_model_hosts",
        "model_host_allowlist",
        "upload_max_bytes",
    ],
    "rate_limits": [
        "auth_login_rate_limit_window_seconds",
        "auth_login_rate_limit_max_attempts",
        "public_agent_rate_limit_window_seconds",
        "public_agent_rate_limit_max_requests",
    ],
    "ingestion": [
        "extractor_version",
        "normalizer_version",
        "chunker_version",
        "validator_version",
        "worker_version",
        "job_lease_seconds",
        "asset_embedding_min_chars",
        "sensor_ingest_max_batch",
        "agent_profile_token_max_age_seconds",
    ],
    "embedding": [
        "embedding_provider",
        "embedding_base_url",
        "embedding_api_key",
        "embedding_model",
        "embedding_batch_size",
        "embedding_timeout_seconds",
    ],
    "vision": [
        "vision_enabled",
        "vision_base_url",
        "vision_api_key",
        "vision_model",
        "vision_reasoning_effort",
        "vision_prompt_version",
        "vision_timeout_seconds",
        "vision_page_min_chars",
        "vision_max_assets_per_page",
        "vision_page_render_dpi",
        "vision_asset_render_dpi",
    ],
    "review": [
        "review_provider",
        "review_base_url",
        "review_api_key",
        "review_model",
        "review_prompt_version",
        "review_min_confidence",
        "review_timeout_seconds",
    ],
    "kg": [
        "kg_ontology_path",
        "kg_min_confidence",
        "kg_extraction_provider",
        "kg_base_url",
        "kg_api_key",
        "kg_model",
        "kg_reasoning_effort",
        "kg_prompt_version",
        "kg_timeout_seconds",
    ],
    "agent_defaults": [
        "agent_provider",
        "agent_base_url",
        "agent_api_key",
        "agent_public_tenant_id",
        "agent_model",
        "agent_reasoning_effort",
        "agent_prompt_version",
        "agent_router_enabled",
        "agent_router_provider",
        "agent_router_base_url",
        "agent_router_model",
        "agent_router_reasoning_effort",
        "agent_router_prompt_version",
        "agent_router_system_prompt",
        "agent_router_temperature",
        "agent_router_max_completion_tokens",
        "agent_router_timeout_seconds",
        "agent_router_confidence_threshold",
        "agent_router_cache_enabled",
        "agent_router_cache_max_age_seconds",
        "agent_memory_enabled",
        "agent_memory_provider",
        "agent_memory_base_url",
        "agent_memory_model",
        "agent_memory_reasoning_effort",
        "agent_memory_prompt_version",
        "agent_memory_system_prompt",
        "agent_memory_temperature",
        "agent_memory_max_completion_tokens",
        "agent_memory_timeout_seconds",
        "agent_memory_char_budget",
        "agent_memory_max_facts",
        "agent_memory_max_open_threads",
        "agent_profile_enabled",
        "agent_profile_provider",
        "agent_profile_base_url",
        "agent_profile_model",
        "agent_profile_reasoning_effort",
        "agent_profile_prompt_version",
        "agent_profile_system_prompt",
        "agent_profile_temperature",
        "agent_profile_max_completion_tokens",
        "agent_profile_timeout_seconds",
        "agent_confidence_threshold",
        "agent_review_threshold",
        "agent_claim_verifier_provider",
        "agent_claim_verifier_base_url",
        "agent_claim_verifier_model",
        "agent_claim_verifier_reasoning_effort",
        "agent_claim_verifier_prompt_version",
        "agent_claim_verifier_timeout_seconds",
        "agent_top_k",
        "agent_visual_top_k",
        "agent_graph_expansion_enabled",
        "agent_graph_chain_limit",
    ],
}


def _display_workspace_path(path: Path) -> str:
    try:
        relative = path.resolve().relative_to(Path("/app").resolve())
        return str(WINDOWS_WORKSPACE_ROOT / PureWindowsPath(str(relative)))
    except ValueError:
        return str(path)


def _env_key_for_field(field_name: str) -> str:
    return str(getattr(settings.model_fields[field_name], "alias", None) or field_name).upper()


def _read_env_map() -> dict[str, str]:
    env_map: dict[str, str] = {}
    if not ENV_FILE_PATH.exists():
        return env_map
    for line in ENV_FILE_PATH.read_text(encoding="utf-8").splitlines():
        stripped = line.strip()
        if not stripped or stripped.startswith("#") or "=" not in line:
            continue
        key, raw_value = line.split("=", 1)
        env_map[key.strip()] = raw_value.strip()
    return env_map


def _serialize_env_value(value: object) -> str:
    if value is None:
        return ""
    if isinstance(value, bool):
        return "true" if value else "false"
    if isinstance(value, (int, float)):
        return str(value)
    if isinstance(value, (dict, list)):
        return json.dumps(value)
    return str(value)


def _deserialize_env_value(field_name: str, raw_value: str):
    default_value = getattr(settings, field_name)
    if isinstance(default_value, bool):
        return raw_value.strip().lower() in {"1", "true", "yes", "on"}
    if isinstance(default_value, int):
        try:
            return int(raw_value)
        except ValueError:
            return default_value
    if isinstance(default_value, float):
        try:
            return float(raw_value)
        except ValueError:
            return default_value
    if isinstance(default_value, (list, dict)):
        try:
            return json.loads(raw_value)
        except Exception:
            return default_value
    return raw_value


def _system_group_fields(group: str) -> list[str]:
    fields = SYSTEM_CONFIG_GROUPS.get(group)
    if not fields:
        raise HTTPException(status_code=404, detail=f"Unknown system config group '{group}'")
    return fields


def _is_system_secret_field(field_name: str) -> bool:
    return field_name.endswith("_api_key") or field_name in {"admin_api_token", "runtime_secret_encryption_key"}


def _first_configured_source(*pairs: tuple[str, object | None]) -> dict[str, object]:
    for source_name, raw_value in pairs:
        value = str(raw_value or "").strip()
        if value:
            return {"configured": True, "source": source_name}
    return {"configured": False, "source": None}


def provider_key_sources(secret_row: dict | None = None) -> dict[str, dict[str, object]]:
    key_sources = {
        "embedding": {**_first_configured_source(("EMBEDDING_API_KEY", settings.embedding_api_key)), "fallback_chain": ["EMBEDDING_API_KEY"]},
        "kg": {**_first_configured_source(("KG_API_KEY", settings.kg_api_key)), "fallback_chain": ["KG_API_KEY"]},
        "review": {**_first_configured_source(("REVIEW_API_KEY", settings.review_api_key)), "fallback_chain": ["REVIEW_API_KEY"]},
        "vision": {**_first_configured_source(("VISION_API_KEY", settings.vision_api_key)), "fallback_chain": ["VISION_API_KEY"]},
        "router": {**_first_configured_source(("AGENT_API_KEY", settings.agent_api_key)), "fallback_chain": ["AGENT_API_KEY"]},
        "memory": {**_first_configured_source(("AGENT_API_KEY", settings.agent_api_key)), "fallback_chain": ["AGENT_API_KEY"]},
        "profile": {**_first_configured_source(("AGENT_API_KEY", settings.agent_api_key)), "fallback_chain": ["AGENT_API_KEY"]},
        "claim_verifier": {**_first_configured_source(("AGENT_API_KEY", settings.agent_api_key)), "fallback_chain": ["AGENT_API_KEY"]},
    }
    if secret_row and bool(secret_row.get("has_api_key_override")):
        key_sources["agent"] = {
            "configured": True,
            "source": "agent_runtime_secrets.api_key_override",
            "fallback_chain": ["agent_runtime_secrets.api_key_override", "AGENT_API_KEY"],
        }
    else:
        key_sources["agent"] = {**_first_configured_source(("AGENT_API_KEY", settings.agent_api_key)), "fallback_chain": ["AGENT_API_KEY"]}
    return key_sources


def build_system_config_payload(*, chroma_store: Any, group: str) -> dict:
    fields = _system_group_fields(group)
    env_map = _read_env_map()
    editable_config: dict[str, object] = {}
    effective_config: dict[str, object] = {}
    secret_keys: list[str] = []
    for field_name in fields:
        env_key = _env_key_for_field(field_name)
        effective_value = getattr(settings, field_name)
        if _is_system_secret_field(field_name):
            secret_keys.append(env_key)
            effective_config[env_key] = SYSTEM_CONFIG_SECRET_MASK if effective_value else ""
            editable_config[env_key] = SYSTEM_CONFIG_SECRET_MASK if env_key in env_map else ""
        elif env_key in env_map:
            effective_config[env_key] = effective_value
            editable_config[env_key] = _deserialize_env_value(field_name, env_map[env_key])
        else:
            effective_config[env_key] = effective_value
            editable_config[env_key] = effective_value
    return {
        "group": group,
        "groups": {name: [_env_key_for_field(field) for field in group_fields] for name, group_fields in SYSTEM_CONFIG_GROUPS.items()},
        "editable_config": editable_config,
        "effective_config": effective_config,
        "secret_keys": secret_keys,
        "provider_key_sources": provider_key_sources(),
        "collection_defaults": {
            "chunk": chroma_store.collection.name,
            "asset": chroma_store.asset_collection.name,
        },
        "env_path": _display_workspace_path(ENV_FILE_PATH),
        "restart_required": True,
        "note": "Changes to startup settings in .env apply after the API and worker are restarted. Secret fields are redacted; keep '<unchanged>' to preserve the current value.",
    }


def write_system_group_config(*, group: str, config: dict) -> None:
    fields = _system_group_fields(group)
    allowed_keys = {_env_key_for_field(field_name): field_name for field_name in fields}
    updates: dict[str, object | None] = {}
    for key, value in dict(config or {}).items():
        key_str = str(key).strip().upper()
        if key_str not in allowed_keys:
            raise HTTPException(status_code=400, detail=f"Key '{key_str}' is not editable in group '{group}'")
        field_name = allowed_keys[key_str]
        if _is_system_secret_field(field_name) and str(value or "").strip() == SYSTEM_CONFIG_SECRET_MASK:
            continue
        updates[key_str] = value

    lines = ENV_FILE_PATH.read_text(encoding="utf-8").splitlines() if ENV_FILE_PATH.exists() else []
    updated_lines: list[str] = []
    written_keys: set[str] = set()
    for raw_line in lines:
        stripped = raw_line.strip()
        if not stripped or stripped.startswith("#") or "=" not in raw_line:
            updated_lines.append(raw_line)
            continue
        key, _ = raw_line.split("=", 1)
        key_str = key.strip()
        if key_str not in updates:
            updated_lines.append(raw_line)
            continue
        written_keys.add(key_str)
        value = updates[key_str]
        if value is None:
            continue
        updated_lines.append(f"{key_str}={_serialize_env_value(value)}")

    for key_str, value in updates.items():
        if key_str in written_keys or value is None:
            continue
        updated_lines.append(f"{key_str}={_serialize_env_value(value)}")

    ENV_FILE_PATH.write_text("\n".join(updated_lines).rstrip() + "\n", encoding="utf-8")


def reset_system_group_config(*, group: str) -> None:
    fields = _system_group_fields(group)
    keys_to_remove = {_env_key_for_field(field_name) for field_name in fields}
    lines = ENV_FILE_PATH.read_text(encoding="utf-8").splitlines() if ENV_FILE_PATH.exists() else []
    updated_lines: list[str] = []
    for raw_line in lines:
        stripped = raw_line.strip()
        if not stripped or stripped.startswith("#") or "=" not in raw_line:
            updated_lines.append(raw_line)
            continue
        key, _ = raw_line.split("=", 1)
        if key.strip() in keys_to_remove:
            continue
        updated_lines.append(raw_line)
    ENV_FILE_PATH.write_text("\n".join(updated_lines).rstrip() + "\n", encoding="utf-8")


def _ontology_path() -> Path:
    path = Path(settings.kg_ontology_path).resolve()
    try:
        path.relative_to(WORKSPACE_ROOT.resolve())
    except ValueError as exc:
        raise HTTPException(status_code=400, detail="Ontology path must stay inside the workspace.") from exc
    return path


def build_ontology_payload() -> dict:
    path = _ontology_path()
    content = path.read_text(encoding="utf-8") if path.exists() else ""
    stats: dict[str, object] | None = None
    parse_error: str | None = None
    try:
        ontology = load_ontology(str(path))
        stats = {
            "classes": len(ontology.classes),
            "predicates": len(ontology.predicates),
            "sample_classes": sorted(list(ontology.classes))[:20],
            "sample_predicates": sorted(list(ontology.predicates))[:20],
        }
    except Exception as exc:  # pragma: no cover - defensive admin path
        parse_error = str(exc)
    return {
        "path": _display_workspace_path(path),
        "content": content,
        "stats": stats,
        "parse_error": parse_error,
        "updated_at": datetime.fromtimestamp(path.stat().st_mtime, tz=timezone.utc).isoformat() if path.exists() else None,
    }


def update_ontology(*, content: str) -> dict:
    path = _ontology_path()
    temp_path = path.with_suffix(path.suffix + ".tmp")
    temp_path.write_text(content, encoding="utf-8")
    try:
        load_ontology(str(temp_path))
    except Exception as exc:
        temp_path.unlink(missing_ok=True)
        raise HTTPException(status_code=400, detail=f"Ontology validation failed: {exc}") from exc
    temp_path.replace(path)
    return build_ontology_payload()


def get_agent_config(*, repository: Any, tenant_id: str = "shared") -> dict:
    row = runtime_config_store.get_agent_runtime_config(repository, tenant_id)
    secret_row = runtime_config_store.get_agent_runtime_secret(repository, tenant_id)
    config = merged_agent_runtime_config((row or {}).get("settings_json") or {})
    config["api_key_override"] = ""
    stored_override = dict((row or {}).get("settings_json") or {})
    stored_override["api_key_override"] = ""
    provider_sources = provider_key_sources(secret_row)
    return {
        "tenant_id": tenant_id,
        "defaults": default_agent_runtime_config(),
        "config": config,
        "stored_override": stored_override,
        "has_api_key_override": bool((secret_row or {}).get("has_api_key_override")),
        "provider_key_sources": provider_sources,
        "effective_api_key_source": provider_sources.get("agent", {}).get("source"),
        "updated_at": row.get("updated_at") if row else None,
        "updated_by": row.get("updated_by") if row else None,
    }


def update_agent_config(
    *,
    repository: Any,
    tenant_id: str,
    config: dict[str, Any],
    updated_by: str,
    clear_api_key_override: bool = False,
) -> dict:
    raw_config = dict(config or {})
    api_key_override = str(raw_config.pop("api_key_override", "") or "").strip()
    cleaned = coerce_agent_runtime_config(raw_config)
    cleaned["api_key_override"] = ""
    runtime_config_store.save_agent_runtime_config(
        repository,
        tenant_id=tenant_id,
        settings_json=cleaned,
        updated_by=updated_by,
    )
    if clear_api_key_override:
        runtime_config_store.delete_agent_runtime_secret(repository, tenant_id)
    elif api_key_override:
        try:
            runtime_config_store.save_agent_runtime_secret(repository, tenant_id, api_key_override, updated_by=updated_by)
        except RuntimeError as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc
    return get_agent_config(repository=repository, tenant_id=tenant_id)


def reset_agent_config(*, repository: Any, tenant_id: str = "shared") -> dict:
    runtime_config_store.delete_agent_runtime_config(repository, tenant_id)
    runtime_config_store.delete_agent_runtime_secret(repository, tenant_id)
    return get_agent_config(repository=repository, tenant_id=tenant_id)


def get_system_config(*, chroma_store: Any, group: str = "platform") -> dict:
    return build_system_config_payload(chroma_store=chroma_store, group=group)


def update_system_config(*, chroma_store: Any, group: str, config: dict[str, Any]) -> dict:
    write_system_group_config(group=group, config=config)
    return build_system_config_payload(chroma_store=chroma_store, group=group)


def reset_system_config(*, chroma_store: Any, group: str = "platform") -> dict:
    reset_system_group_config(group=group)
    return build_system_config_payload(chroma_store=chroma_store, group=group)
