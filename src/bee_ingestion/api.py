"""FastAPI surface for ingestion, admin operations, and the read-only agent.

The API is intentionally split into three groups:
- /ingest/* for corpus creation and replay
- /admin/api/* for operator inspection and control
- /agent/* for user-facing question answering over the indexed corpus
"""

from __future__ import annotations

import json
import os
import signal
import subprocess
import sys
from pathlib import Path, PureWindowsPath
from datetime import datetime, timezone
import re
from typing import Any, Callable, Literal
from urllib.parse import urlparse
from uuid import UUID, uuid4

from fastapi import FastAPI, File, Form, HTTPException, Query, Request, UploadFile
from fastapi.responses import FileResponse, HTMLResponse, JSONResponse
from pydantic import BaseModel, Field

from src.bee_ingestion.admin_ui import ADMIN_HTML
from src.bee_ingestion.auth_store import (
    ALLOWED_AUTH_PERMISSIONS,
    ALLOWED_AUTH_ROLES,
    ALLOWED_AUTH_STATUSES,
    DEFAULT_ROLE_PERMISSIONS,
    AuthStore,
)
from src.bee_ingestion.agent_eval import read_agent_evaluation, run_agent_evaluation
from src.bee_ingestion.agent import AgentQueryError, AgentService
from src.bee_ingestion.agent_runtime import coerce_agent_runtime_config, default_agent_runtime_config, merged_agent_runtime_config
from src.bee_ingestion.agent_ui import AGENT_UI_HTML
from src.bee_ingestion.chroma_store import ChromaStore
from src.bee_ingestion.frontend import frontend_index_response, frontend_path_response, frontend_redirect
from src.bee_ingestion.kg import load_ontology
from src.bee_ingestion.models import SensorReading, SourceDocument, UserSensor
from src.bee_ingestion.pdf_utils import build_pdf_content_hash
from src.bee_ingestion.rate_limit import SlidingWindowRateLimiter
from src.bee_ingestion.repository import ALLOWED_AGENT_REVIEW_DECISIONS, Repository
from src.bee_ingestion.retrieval_eval import read_retrieval_evaluation, run_retrieval_evaluation
from src.bee_ingestion.service import IngestionService
from src.bee_ingestion.settings import settings, workspace_root

app = FastAPI(title="Bee Ingestion API")


class _LazyProxy:
    def __init__(self, factory: Callable[[], object]) -> None:
        self._factory = factory
        self._instance: object | None = None

    def _get(self) -> object:
        if self._instance is None:
            self._instance = self._factory()
        return self._instance

    def __getattr__(self, item: str):
        return getattr(self._get(), item)

    def _call(self, name: str, *args, **kwargs):
        return getattr(self._get(), name)(*args, **kwargs)

    # These explicit pass-throughs keep monkeypatch/test doubles compatible even
    # when the current backing instance is a lightweight stub.
    def get_agent_session(self, *args, **kwargs):
        return self._call("get_agent_session", *args, **kwargs)

    def set_agent_session_token(self, *args, **kwargs):
        return self._call("set_agent_session_token", *args, **kwargs)

    def get_agent_session_memory(self, *args, **kwargs):
        return self._call("get_agent_session_memory", *args, **kwargs)

    def list_agent_messages(self, *args, **kwargs):
        return self._call("list_agent_messages", *args, **kwargs)

    def save_agent_answer_review(self, *args, **kwargs):
        return self._call("save_agent_answer_review", *args, **kwargs)

    def list_agent_answer_reviews(self, *args, **kwargs):
        return self._call("list_agent_answer_reviews", *args, **kwargs)

    def count_agent_answer_reviews(self, *args, **kwargs):
        return self._call("count_agent_answer_reviews", *args, **kwargs)

    def get_agent_metrics(self, *args, **kwargs):
        return self._call("get_agent_metrics", *args, **kwargs)

    def get_latest_corpus_snapshot_id(self, *args, **kwargs):
        return self._call("get_latest_corpus_snapshot_id", *args, **kwargs)


repository = _LazyProxy(Repository)


def _identity_repository_factory() -> Repository:
    auth_dsn = str(settings.auth_postgres_dsn or "").strip()
    if not auth_dsn:
        raise RuntimeError("AUTH_POSTGRES_DSN must be configured for identity operations")
    return Repository(dsn=auth_dsn)


identity_repository = _LazyProxy(_identity_repository_factory)
auth_store = _LazyProxy(AuthStore)
chroma_store = _LazyProxy(ChromaStore)
service = _LazyProxy(lambda: IngestionService(repository=repository._get(), store=chroma_store._get()))
agent_service = _LazyProxy(lambda: AgentService(repository=repository._get(), store=chroma_store._get()))
rate_limiter = SlidingWindowRateLimiter(
    dsn=str(settings.auth_postgres_dsn or "").strip(),
    schema_name=str(settings.auth_postgres_schema or "auth"),
)
WORKSPACE_ROOT = Path(__file__).resolve().parents[2]
EVALUATION_ROOT = (WORKSPACE_ROOT / "data" / "evaluation").resolve()
ENV_FILE_PATH = (WORKSPACE_ROOT / ".env").resolve()
PAGE_ASSET_ROOT = (workspace_root() / "data" / "page_assets").resolve()
APP_WORKSPACE_ROOT = Path("/app").resolve()
WINDOWS_WORKSPACE_ROOT = PureWindowsPath(r"E:\n8n to python")
REINGEST_PROGRESS_PATH = (workspace_root() / "data" / "logs" / "reingest-progress.json").resolve()
REINGEST_LAUNCH_LOG_PATH = (workspace_root() / "data" / "logs" / "reingest-launch.log").resolve()
REINGEST_PID_PATH = (workspace_root() / "data" / "logs" / "reingest-runner.pid").resolve()
REINGEST_STALE_PROGRESS_SECONDS = max(60, int(os.environ.get("REINGEST_STALE_PROGRESS_SECONDS", "900")))
AGENT_SESSION_ID_COOKIE = "bee_agent_session_id"
AGENT_SESSION_TOKEN_COOKIE = "bee_agent_session_token"
AGENT_PROFILE_ID_COOKIE = "bee_agent_profile_id"
AGENT_PROFILE_TOKEN_COOKIE = "bee_agent_profile_token"
AUTH_SESSION_ID_COOKIE = "bee_auth_session_id"
AUTH_SESSION_TOKEN_COOKIE = "bee_auth_session_token"
ADMIN_DATABASE_KEYS = {"app", "identity"}
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
        "agent_profile_char_budget",
        "agent_system_prompt",
        "agent_sensor_system_prompt",
        "agent_claim_verifier_enabled",
        "agent_claim_verifier_provider",
        "agent_claim_verifier_base_url",
        "agent_claim_verifier_model",
        "agent_claim_verifier_reasoning_effort",
        "agent_claim_verifier_prompt_version",
        "agent_claim_verifier_system_prompt",
        "agent_claim_verifier_temperature",
        "agent_claim_verifier_max_completion_tokens",
        "agent_claim_verifier_timeout_seconds",
        "agent_claim_verifier_min_supported_ratio",
        "agent_temperature",
        "agent_max_completion_tokens",
        "agent_timeout_seconds",
        "agent_default_top_k",
        "agent_max_top_k",
        "agent_max_search_k",
        "agent_max_context_chunks",
        "agent_max_context_assertions",
        "agent_neighbor_window",
        "agent_session_lease_seconds",
        "agent_session_token_max_age_seconds",
        "agent_min_answer_confidence",
        "agent_review_confidence_threshold",
        "agent_rerank_distance_weight",
        "agent_rerank_lexical_weight",
        "agent_rerank_section_weight",
        "agent_rerank_title_weight",
        "agent_rerank_exact_phrase_weight",
        "agent_rerank_ontology_weight",
        "agent_diversity_penalty",
        "agent_prompt_char_budget",
        "agent_history_char_budget",
        "agent_assertion_char_budget",
        "agent_entity_char_budget",
        "agent_kg_search_limit",
        "agent_chunk_char_budget",
        "agent_max_context_assets",
        "agent_max_asset_search_k",
        "agent_asset_char_budget",
        "agent_sensor_context_enabled",
        "agent_max_context_sensor_readings",
        "agent_sensor_recent_hours",
        "agent_sensor_points_per_metric",
        "agent_sensor_char_budget",
        "agent_evidence_char_budget",
        "agent_citation_excerpt_chars",
    ],
}
SYSTEM_CONFIG_SECRET_FIELDS = {
    "admin_api_token",
    "runtime_secret_encryption_key",
    "postgres_dsn",
    "auth_postgres_dsn",
    "embedding_api_key",
    "vision_api_key",
    "review_api_key",
    "kg_api_key",
    "agent_api_key",
}
SYSTEM_CONFIG_SECRET_MASK = "<unchanged>"


def _resolve_evaluation_path(path: str, *, must_exist: bool = False) -> Path:
    """Keep retrieval-eval artifacts inside the workspace evaluation directory."""

    candidate = Path(path)
    if not candidate.is_absolute():
        candidate = (WORKSPACE_ROOT / candidate).resolve()
    else:
        candidate = candidate.resolve()
    try:
        candidate.relative_to(EVALUATION_ROOT)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail="Evaluation path must stay inside data/evaluation.") from exc
    if candidate.suffix.lower() != ".json":
        raise HTTPException(status_code=400, detail="Evaluation path must point to a JSON file.")
    if must_exist and not candidate.exists():
        raise HTTPException(status_code=404, detail=f"Retrieval evaluation output not found: {candidate}")
    return candidate


def _env_key_for_field(field_name: str) -> str:
    return field_name.upper()


def _read_env_map() -> dict[str, str]:
    if not ENV_FILE_PATH.exists():
        return {}
    values: dict[str, str] = {}
    for raw_line in ENV_FILE_PATH.read_text(encoding="utf-8").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#") or "=" not in raw_line:
            continue
        key, value = raw_line.split("=", 1)
        values[key.strip()] = value
    return values


def _serialize_env_value(value) -> str:
    if isinstance(value, bool):
        return "true" if value else "false"
    if value is None:
        return ""
    return str(value)


def _deserialize_env_value(field_name: str, raw_value: str):
    current_value = getattr(settings, field_name)
    if isinstance(current_value, bool):
        return raw_value.strip().lower() in {"1", "true", "yes", "on"}
    if isinstance(current_value, int):
        try:
            return int(raw_value)
        except ValueError:
            return raw_value
    if isinstance(current_value, float):
        try:
            return float(raw_value)
        except ValueError:
            return raw_value
    return raw_value


def _system_group_fields(group: str) -> list[str]:
    if group not in SYSTEM_CONFIG_GROUPS:
        raise HTTPException(status_code=400, detail=f"Unknown config group '{group}'")
    return SYSTEM_CONFIG_GROUPS[group]


def _is_system_secret_field(field_name: str) -> bool:
    return field_name in SYSTEM_CONFIG_SECRET_FIELDS


def _display_workspace_path(path: Path) -> str:
    try:
        relative = path.resolve().relative_to(APP_WORKSPACE_ROOT)
        return str(WINDOWS_WORKSPACE_ROOT / PureWindowsPath(str(relative)))
    except ValueError:
        return str(path)


def _build_pdf_content_hash(path: str | Path, page_start: int | None = None, page_end: int | None = None) -> str:
    return build_pdf_content_hash(path, page_start=page_start, page_end=page_end)


def _json_safe(value: Any) -> Any:
    if isinstance(value, dict):
        return {str(key): _json_safe(item) for key, item in value.items()}
    if isinstance(value, list):
        return [_json_safe(item) for item in value]
    if isinstance(value, tuple):
        return [_json_safe(item) for item in value]
    if isinstance(value, UUID):
        return str(value)
    if value is None or isinstance(value, (str, int, float, bool)):
        return value
    if hasattr(value, "isoformat"):
        try:
            return value.isoformat()
        except Exception:
            return str(value)
    return str(value)


def _build_system_config_payload(group: str) -> dict:
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
        "provider_key_sources": _provider_key_sources(),
        "collection_defaults": {
            "chunk": chroma_store.collection.name,
            "asset": chroma_store.asset_collection.name,
        },
        "env_path": _display_workspace_path(ENV_FILE_PATH),
        "restart_required": True,
        "note": "Changes to startup settings in .env apply after the API and worker are restarted. Secret fields are redacted; keep '<unchanged>' to preserve the current value.",
    }


def _ontology_path() -> Path:
    path = Path(settings.kg_ontology_path).resolve()
    try:
        path.relative_to(WORKSPACE_ROOT.resolve())
    except ValueError as exc:
        raise HTTPException(status_code=400, detail="Ontology path must stay inside the workspace.") from exc
    return path


def _build_ontology_payload() -> dict:
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


def _first_configured_source(*pairs: tuple[str, object | None]) -> dict[str, object]:
    for source_name, raw_value in pairs:
        value = str(raw_value or "").strip()
        if value:
            return {"configured": True, "source": source_name}
    return {"configured": False, "source": None}


def _safe_page_asset_path(raw_path: str | Path) -> Path:
    candidate = Path(str(raw_path or "")).resolve()
    try:
        candidate.relative_to(PAGE_ASSET_ROOT)
    except ValueError as exc:
        raise HTTPException(status_code=404, detail="Asset file not available") from exc
    if not candidate.exists() or not candidate.is_file():
        raise HTTPException(status_code=404, detail="Asset file not found")
    return candidate


def _provider_key_sources(secret_row: dict | None = None) -> dict[str, dict[str, object]]:
    key_sources = {
        "embedding": {
            **_first_configured_source(("EMBEDDING_API_KEY", settings.embedding_api_key)),
            "fallback_chain": ["EMBEDDING_API_KEY"],
        },
        "kg": {
            **_first_configured_source(("KG_API_KEY", settings.kg_api_key)),
            "fallback_chain": ["KG_API_KEY"],
        },
        "review": {
            **_first_configured_source(("REVIEW_API_KEY", settings.review_api_key)),
            "fallback_chain": ["REVIEW_API_KEY"],
        },
        "vision": {
            **_first_configured_source(("VISION_API_KEY", settings.vision_api_key)),
            "fallback_chain": ["VISION_API_KEY"],
        },
        "router": {
            **_first_configured_source(("AGENT_API_KEY", settings.agent_api_key)),
            "fallback_chain": ["AGENT_API_KEY"],
        },
        "memory": {
            **_first_configured_source(("AGENT_API_KEY", settings.agent_api_key)),
            "fallback_chain": ["AGENT_API_KEY"],
        },
        "profile": {
            **_first_configured_source(("AGENT_API_KEY", settings.agent_api_key)),
            "fallback_chain": ["AGENT_API_KEY"],
        },
        "claim_verifier": {
            **_first_configured_source(("AGENT_API_KEY", settings.agent_api_key)),
            "fallback_chain": ["AGENT_API_KEY"],
        },
    }
    if secret_row and bool(secret_row.get("has_api_key_override")):
        key_sources["agent"] = {
            "configured": True,
            "source": "agent_runtime_secrets.api_key_override",
            "fallback_chain": ["agent_runtime_secrets.api_key_override", "AGENT_API_KEY"],
        }
    else:
        key_sources["agent"] = {
            **_first_configured_source(("AGENT_API_KEY", settings.agent_api_key)),
            "fallback_chain": ["AGENT_API_KEY"],
        }
    return key_sources


def _write_system_group_config(group: str, config: dict) -> None:
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


def _reset_system_group_config(group: str) -> None:
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


EDITOR_DELETABLE_TYPES = {"asset", "asset_link", "kg_entity", "kg_assertion", "kg_raw", "agent_session", "agent_profile", "agent_pattern"}
EDITOR_SYNCABLE_TYPES = {"document", "chunk", "asset"}
SESSION_MEMORY_CLEAR_RULES: dict[str, dict[str, Any]] = {
    "facts": {"stable_facts": []},
    "open_threads": {"open_threads": []},
    "resolved_threads": {"resolved_threads": []},
    "preferences": {"user_preferences": []},
    "constraints": {"active_constraints": []},
    "scope": {
        "topic_keywords": [],
        "preferred_document_ids": [],
        "scope_signature": "",
        "last_query": "",
    },
    "goal": {"session_goal": ""},
}
PROFILE_MEMORY_CLEAR_RULES: dict[str, dict[str, Any]] = {
    "background": {"user_background": ""},
    "beekeeping_context": {"beekeeping_context": ""},
    "experience_level": {"experience_level": ""},
    "communication_style": {"communication_style": ""},
    "preferences": {"answer_preferences": []},
    "topics": {"recurring_topics": []},
    "learning_goals": {"learning_goals": []},
    "constraints": {"persistent_constraints": []},
}


def _normalize_memory_clear_sections(
    requested: list[str] | None,
    allowed_rules: dict[str, dict[str, Any]],
) -> list[str]:
    normalized = [str(item or "").strip().lower() for item in (requested or []) if str(item or "").strip()]
    if not normalized:
        raise HTTPException(status_code=400, detail="At least one memory section is required")
    if "all" in normalized:
        return list(allowed_rules.keys())
    unknown = sorted({item for item in normalized if item not in allowed_rules})
    if unknown:
        raise HTTPException(status_code=400, detail=f"Unsupported memory sections: {', '.join(unknown)}")
    deduped: list[str] = []
    for item in normalized:
        if item not in deduped:
            deduped.append(item)
    return deduped


def _apply_memory_clear_rules(
    summary_json: dict[str, Any] | None,
    *,
    sections: list[str],
    allowed_rules: dict[str, dict[str, Any]],
) -> dict[str, Any]:
    payload = dict(summary_json or {})
    for section in sections:
        payload.update(allowed_rules.get(section, {}))
    return payload


def _coerce_and_refresh_session_memory(summary_json: dict[str, Any]) -> dict[str, Any]:
    from src.bee_ingestion.agent import _coerce_memory_summary, _refresh_memory_summary_text

    return _refresh_memory_summary_text(
        _coerce_memory_summary(
            dict(summary_json or {}),
            max_facts=int(settings.agent_memory_max_facts or 6),
            max_open_threads=int(settings.agent_memory_max_open_threads or 6),
            max_resolved_threads=int(settings.agent_memory_max_resolved_threads or 6),
            max_preferences=int(settings.agent_memory_max_preferences or 6),
            max_topics=int(settings.agent_memory_max_topics or 8),
        )
    )


def _coerce_and_refresh_profile_summary(summary_json: dict[str, Any]) -> dict[str, Any]:
    from src.bee_ingestion.agent import _coerce_profile_summary, _refresh_profile_summary_text

    return _refresh_profile_summary_text(_coerce_profile_summary(dict(summary_json or {})))


def _normalize_editor_type(value: str) -> str:
    normalized = (value or "").strip().lower()
    aliases = {
        "document_source": "source",
        "document_page": "page",
        "page_asset": "asset",
        "chunk_asset_link": "asset_link",
        "kgentity": "kg_entity",
        "kgassertion": "kg_assertion",
        "kgraw": "kg_raw",
        "session": "agent_session",
        "profile": "agent_profile",
        "session_memory": "agent_session_memory",
        "pattern": "agent_pattern",
    }
    return aliases.get(normalized, normalized)


def _parse_editor_page_number(value: str | None) -> int:
    try:
        return int(str(value or "").strip())
    except ValueError as exc:
        raise HTTPException(status_code=400, detail="Page editors require a numeric secondary_id page number") from exc


def _load_editor_record(record_type: str, record_id: str, secondary_id: str | None = None) -> dict:
    record_type = _normalize_editor_type(record_type)
    if record_type == "document":
        row = repository.get_document_record(record_id)
    elif record_type == "source":
        row = repository.get_document_source(record_id)
    elif record_type == "page":
        row = repository.get_document_page(record_id, _parse_editor_page_number(secondary_id))
    elif record_type == "chunk":
        row = repository.get_chunk_record(record_id)
    elif record_type == "asset":
        detail = repository.get_page_asset_detail(record_id)
        row = detail.get("asset") if detail else None
    elif record_type == "asset_link":
        row = repository.get_chunk_asset_link(record_id)
    elif record_type == "kg_entity":
        row = repository.get_kg_entity_record(record_id)
    elif record_type == "kg_assertion":
        row = repository.get_kg_assertion(record_id)
    elif record_type == "kg_raw":
        row = repository.get_kg_raw_extraction(record_id)
    elif record_type == "agent_session":
        row = repository.get_agent_session(record_id)
    elif record_type == "agent_profile":
        row = repository.get_agent_profile(record_id)
    elif record_type == "agent_session_memory":
        row = repository.get_agent_session_memory(record_id)
    elif record_type == "agent_pattern":
        row = repository.get_agent_query_pattern(secondary_id or "shared", record_id)
    else:
        raise HTTPException(status_code=400, detail=f"Unsupported editor record_type '{record_type}'")
    if row is None:
        raise HTTPException(status_code=404, detail=f"{record_type} not found")
    return {
        "record_type": record_type,
        "record_id": record_id,
        "secondary_id": secondary_id,
        "record": row,
        "capabilities": {
            "delete": record_type in EDITOR_DELETABLE_TYPES,
            "sync_index": record_type in EDITOR_SYNCABLE_TYPES,
        },
    }


def _save_editor_record(record_type: str, record_id: str, payload: dict[str, Any], secondary_id: str | None = None, *, sync_index: bool = False) -> dict:
    record_type = _normalize_editor_type(record_type)
    try:
        if record_type == "document":
            row = repository.update_document_record(record_id, payload)
        elif record_type == "source":
            row = repository.update_document_source(record_id, payload)
        elif record_type == "page":
            row = repository.update_document_page(record_id, _parse_editor_page_number(secondary_id), payload)
        elif record_type == "chunk":
            row = repository.update_chunk_record_admin(record_id, payload)
        elif record_type == "asset":
            row = repository.update_page_asset(record_id, payload)
        elif record_type == "asset_link":
            row = repository.update_chunk_asset_link(record_id, payload)
        elif record_type == "kg_entity":
            row = repository.update_kg_entity(record_id, payload)
        elif record_type == "kg_assertion":
            row = repository.update_kg_assertion(record_id, payload)
        elif record_type == "kg_raw":
            row = repository.update_kg_raw_extraction(record_id, payload)
        elif record_type == "agent_session":
            row = repository.update_agent_session_record(record_id, payload)
        elif record_type == "agent_profile":
            row = repository.update_agent_profile_record(record_id, payload)
        elif record_type == "agent_session_memory":
            row = repository.update_agent_session_memory_record(record_id, payload)
        elif record_type == "agent_pattern":
            row = repository.update_agent_query_pattern(secondary_id or "shared", record_id, payload)
        else:
            raise HTTPException(status_code=400, detail=f"Unsupported editor record_type '{record_type}'")
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    if row is None:
        raise HTTPException(status_code=404, detail=f"{record_type} not found")
    sync_result = None
    if sync_index:
        if record_type == "document":
            sync_result = service.reindex_document(record_id)
        elif record_type == "chunk":
            sync_result = service.sync_chunk_index(record_id)
        elif record_type == "asset":
            sync_result = service.sync_asset_index(record_id)
        else:
            raise HTTPException(status_code=400, detail=f"Record type '{record_type}' does not support sync_index")
    return {
        "record_type": record_type,
        "record_id": record_id,
        "secondary_id": secondary_id,
        "record": row,
        "sync_result": sync_result,
        "capabilities": {
            "delete": record_type in EDITOR_DELETABLE_TYPES,
            "sync_index": record_type in EDITOR_SYNCABLE_TYPES,
        },
    }


def _delete_editor_record(record_type: str, record_id: str, secondary_id: str | None = None, *, sync_index: bool = False) -> dict:
    record_type = _normalize_editor_type(record_type)
    if record_type not in EDITOR_DELETABLE_TYPES:
        raise HTTPException(status_code=400, detail=f"Record type '{record_type}' is not deletable through the editor")
    try:
        if record_type == "asset":
            if sync_index:
                chroma_store.delete_asset(record_id)
            deleted = repository.delete_page_asset(record_id)
        elif record_type == "asset_link":
            deleted = repository.delete_chunk_asset_link(record_id)
        elif record_type == "kg_entity":
            deleted = repository.delete_kg_entity(record_id)
        elif record_type == "kg_assertion":
            deleted = repository.delete_kg_assertion(record_id)
            repository.prune_orphan_kg_entities()
        elif record_type == "kg_raw":
            deleted = repository.delete_kg_raw_extraction(record_id)
        elif record_type == "agent_session":
            deleted = repository.delete_agent_session(record_id)
        elif record_type == "agent_profile":
            deleted = repository.delete_agent_profile(record_id)
        elif record_type == "agent_pattern":
            deleted = repository.delete_agent_query_pattern(secondary_id or "shared", record_id)
        else:
            raise HTTPException(status_code=400, detail=f"Unsupported editor record_type '{record_type}'")
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    if not deleted:
        raise HTTPException(status_code=404, detail=f"{record_type} not found")
    return {"record_type": record_type, "record_id": record_id, "secondary_id": secondary_id, "deleted": True}


def _resync_editor_record(record_type: str, record_id: str) -> dict:
    record_type = _normalize_editor_type(record_type)
    if record_type == "document":
        return service.reindex_document(record_id)
    if record_type == "chunk":
        return service.sync_chunk_index(record_id)
    if record_type == "asset":
        return service.sync_asset_index(record_id)
    raise HTTPException(status_code=400, detail=f"Record type '{record_type}' does not support vector resync")


def _resolve_agent_session_credentials(request: Request, payload: AgentQueryRequest) -> tuple[str | None, str | None]:
    return (
        payload.session_id or request.cookies.get(AGENT_SESSION_ID_COOKIE),
        payload.session_token or request.cookies.get(AGENT_SESSION_TOKEN_COOKIE),
    )


def _resolve_agent_session_cookies(request: Request) -> tuple[str | None, str | None]:
    return (
        request.cookies.get(AGENT_SESSION_ID_COOKIE),
        request.cookies.get(AGENT_SESSION_TOKEN_COOKIE),
    )


def _resolve_agent_profile_credentials(request: Request) -> tuple[str | None, str | None]:
    return (
        request.cookies.get(AGENT_PROFILE_ID_COOKIE),
        request.cookies.get(AGENT_PROFILE_TOKEN_COOKIE),
    )


def _apply_agent_session_cookies(response: JSONResponse, result: dict[str, Any]) -> None:
    session_id = str(result.get("session_id") or "").strip()
    session_token = str(result.get("session_token") or "").strip()
    if session_id:
        response.set_cookie(
            key=AGENT_SESSION_ID_COOKIE,
            value=session_id,
            httponly=True,
            samesite="lax",
            secure=bool(settings.auth_cookie_secure),
            max_age=settings.agent_session_token_max_age_seconds,
            path="/",
        )
    if session_token:
        response.set_cookie(
            key=AGENT_SESSION_TOKEN_COOKIE,
            value=session_token,
            httponly=True,
            samesite="lax",
            secure=bool(settings.auth_cookie_secure),
            max_age=settings.agent_session_token_max_age_seconds,
            path="/",
        )


def _apply_agent_profile_cookies(response: JSONResponse, result: dict[str, Any]) -> None:
    profile_id = str(result.get("profile_id") or "").strip()
    profile_token = str(result.get("profile_token") or "").strip()
    if profile_id:
        response.set_cookie(
            key=AGENT_PROFILE_ID_COOKIE,
            value=profile_id,
            httponly=True,
            samesite="lax",
            secure=bool(settings.auth_cookie_secure),
            max_age=settings.agent_profile_token_max_age_seconds,
            path="/",
        )
    if profile_token:
        response.set_cookie(
            key=AGENT_PROFILE_TOKEN_COOKIE,
            value=profile_token,
            httponly=True,
            samesite="lax",
            secure=bool(settings.auth_cookie_secure),
            max_age=settings.agent_profile_token_max_age_seconds,
            path="/",
        )


def _clear_agent_session_cookies(response: JSONResponse) -> None:
    response.delete_cookie(AGENT_SESSION_ID_COOKIE, path="/")
    response.delete_cookie(AGENT_SESSION_TOKEN_COOKIE, path="/")


def _clear_agent_profile_cookies(response: JSONResponse) -> None:
    response.delete_cookie(AGENT_PROFILE_ID_COOKIE, path="/")
    response.delete_cookie(AGENT_PROFILE_TOKEN_COOKIE, path="/")


def _resolve_auth_session_credentials(request: Request) -> tuple[str | None, str | None]:
    return (
        request.cookies.get(AUTH_SESSION_ID_COOKIE),
        request.cookies.get(AUTH_SESSION_TOKEN_COOKIE),
    )


def _apply_auth_session_cookies(response: JSONResponse, result: dict[str, Any]) -> None:
    session_id = str(result.get("auth_session_id") or "").strip()
    session_token = str(result.get("auth_session_token") or "").strip()
    if session_id:
        response.set_cookie(
            key=AUTH_SESSION_ID_COOKIE,
            value=session_id,
            httponly=True,
            samesite="lax",
            secure=bool(settings.auth_cookie_secure),
            max_age=settings.auth_session_max_age_seconds,
            path="/",
        )
    if session_token:
        response.set_cookie(
            key=AUTH_SESSION_TOKEN_COOKIE,
            value=session_token,
            httponly=True,
            samesite="lax",
            secure=bool(settings.auth_cookie_secure),
            max_age=settings.auth_session_max_age_seconds,
            path="/",
        )


def _clear_auth_session_cookies(response: JSONResponse) -> None:
    response.delete_cookie(AUTH_SESSION_ID_COOKIE, path="/")
    response.delete_cookie(AUTH_SESSION_TOKEN_COOKIE, path="/")


def _normalized_browser_origin(value: str | None) -> str | None:
    raw_value = str(value or "").strip()
    if not raw_value:
        return None
    parsed = urlparse(raw_value)
    scheme = parsed.scheme.strip().lower()
    hostname = (parsed.hostname or "").strip().lower()
    if scheme not in {"http", "https"} or not hostname:
        return None
    default_port = 443 if scheme == "https" else 80
    port = parsed.port
    port_part = f":{port}" if port not in {None, default_port} else ""
    return f"{scheme}://{hostname}{port_part}"


def _allowed_browser_origins(request: Request) -> set[str]:
    allowed: set[str] = set()
    current_origin = _normalized_browser_origin(str(request.base_url))
    if current_origin:
        allowed.add(current_origin)
    for raw_value in str(settings.browser_origin_allowlist or "").split(","):
        normalized = _normalized_browser_origin(raw_value)
        if normalized:
            allowed.add(normalized)
    return allowed


def _request_origin(request: Request) -> str | None:
    return _normalized_browser_origin(request.headers.get("Origin")) or _normalized_browser_origin(request.headers.get("Referer"))


def _requires_same_origin(request: Request) -> bool:
    if request.method.upper() in {"GET", "HEAD", "OPTIONS"}:
        return False
    path = request.url.path
    return (
        path.startswith("/auth/")
        or path.startswith("/agent/")
        or path.startswith("/places")
        or path.startswith("/hives")
        or path.startswith("/sensors")
    )


def _enforce_same_origin(request: Request) -> None:
    if not _requires_same_origin(request):
        return
    request_origin = _request_origin(request)
    if request_origin is None:
        return
    allowed_origins = _allowed_browser_origins(request)
    if request_origin not in allowed_origins:
        raise HTTPException(status_code=403, detail="Cross-site browser request blocked")


def _auth_audit_context(request: Request) -> dict[str, str | None]:
    forwarded = str(request.headers.get("x-forwarded-for") or "").strip()
    ip_address = forwarded.split(",")[0].strip() if forwarded else str(getattr(getattr(request, "client", None), "host", "") or "").strip()
    return {
        "ip_address": ip_address or None,
        "user_agent": str(request.headers.get("user-agent") or "").strip()[:512] or None,
    }


def _require_authenticated_public_user(request: Request) -> dict[str, Any]:
    auth_session = _resolve_authenticated_session(request)
    public_tenant = settings.agent_public_tenant_id or "shared"
    if auth_session is None:
        raise HTTPException(status_code=401, detail="Login required")
    user = auth_session.get("user") or {}
    permissions = {str(item or "").strip().lower() for item in list(user.get("permissions") or []) if str(item or "").strip()}
    if "chat.use" not in permissions:
        raise HTTPException(status_code=403, detail="Account is not allowed to use chat")
    if str(user.get("tenant_id") or public_tenant) != public_tenant:
        raise HTTPException(status_code=403, detail="Account is not allowed in the public tenant")
    return auth_session


def _require_authenticated_sensor_user(request: Request, *, write: bool) -> dict[str, Any]:
    auth_session = _require_authenticated_public_user(request)
    user = auth_session.get("user") or {}
    permissions = {str(item or "").strip().lower() for item in list(user.get("permissions") or []) if str(item or "").strip()}
    required_permission = "sensor.write" if write else "sensor.read"
    if required_permission not in permissions:
        raise HTTPException(status_code=403, detail=f"Account is not allowed to {required_permission}")
    return auth_session


def _resolve_public_query_mode(auth_session: dict[str, Any], requested_query_mode: Literal["auto", "general", "sensor"] | None) -> Literal["auto", "general", "sensor"] | None:
    user = auth_session.get("user") or {}
    permissions = {str(item or "").strip().lower() for item in list(user.get("permissions") or []) if str(item or "").strip()}
    if "sensor.read" in permissions:
        return requested_query_mode
    if requested_query_mode == "sensor":
        raise HTTPException(status_code=403, detail="Account is not allowed to use sensor mode")
    return "general"


def _public_auth_payload(result: dict[str, Any] | None) -> dict[str, Any]:
    if not result:
        return {"authenticated": False, "user": None}
    return {
        "authenticated": bool(result.get("authenticated")),
        "user": result.get("user"),
    }


def _request_client_identity(request: Request) -> str:
    forwarded = str(request.headers.get("x-forwarded-for") or "").strip()
    if forwarded:
        return forwarded.split(",")[0].strip()
    client = getattr(request, "client", None)
    host = getattr(client, "host", None)
    return str(host or "unknown")


def _enforce_rate_limit(request: Request, *, bucket: str, limit: int, window_seconds: int, subject: str | None = None) -> None:
    identity_parts = [bucket, _request_client_identity(request)]
    if subject:
        identity_parts.append(str(subject).strip())
    retry_after = rate_limiter.check("|".join(identity_parts), limit=limit, window_seconds=window_seconds)
    if retry_after is not None:
        raise HTTPException(status_code=429, detail=f"Rate limit exceeded. Retry in {retry_after} seconds.")


CONTROL_PLANE_READ_PERMISSIONS = sorted(
    permission for permission in ALLOWED_AUTH_PERMISSIONS if permission not in {"chat.use", "chat.history.read"}
)


def _resolve_authenticated_session(request: Request) -> dict[str, Any] | None:
    cached = getattr(request.state, "auth_session", None)
    if cached is not None:
        return cached
    auth_session_id, auth_session_token = _resolve_auth_session_credentials(request)
    auth_session = auth_store.verify_session(auth_session_id, auth_session_token)
    request.state.auth_session = auth_session
    if (
        auth_session is not None
        and bool(auth_session.get("refresh_cookie"))
        and auth_session_id
        and auth_session_token
    ):
        request.state.auth_session_cookie_refresh = {
            "auth_session_id": auth_session_id,
            "auth_session_token": auth_session_token,
        }
    return auth_session


def _session_permissions(auth_session: dict[str, Any] | None) -> set[str]:
    user = (auth_session or {}).get("user") or {}
    return {
        str(item or "").strip().lower()
        for item in list(user.get("permissions") or [])
        if str(item or "").strip()
    }


def _session_has_any_permission(auth_session: dict[str, Any] | None, permissions: list[str] | tuple[str, ...] | set[str]) -> bool:
    allowed = _session_permissions(auth_session)
    return any(str(permission or "").strip().lower() in allowed for permission in permissions)


def _has_valid_control_plane_token(request: Request) -> bool:
    expected = (settings.admin_api_token or "").strip()
    provided = (request.headers.get("X-Admin-Token") or "").strip()
    return bool(expected and provided == expected)


def _normalize_admin_database_key(value: str | None) -> str:
    normalized = str(value or "app").strip().lower()
    if normalized not in ADMIN_DATABASE_KEYS:
        raise HTTPException(status_code=400, detail=f"Unknown admin database '{normalized}'")
    return normalized


def _admin_repository_for_database(database: str):
    normalized = _normalize_admin_database_key(database)
    return identity_repository if normalized == "identity" else repository


def _enforce_admin_database_scope(request: Request, database: str, *, mode: str) -> None:
    normalized = _normalize_admin_database_key(database)
    if _has_valid_control_plane_token(request):
        return
    auth_session = _resolve_authenticated_session(request)
    if auth_session is None:
        raise HTTPException(status_code=401, detail="Operator login or admin token required")
    if mode == "sql":
        required = ["db.sql.write"]
    elif normalized == "identity" and mode == "read":
        required = ["accounts.read", "accounts.write", "db.sql.write"]
    elif normalized == "identity" and mode == "write":
        required = ["accounts.write", "db.sql.write"]
    elif mode == "read":
        required = ["db.rows.write", "db.sql.write"]
    elif mode == "write":
        required = ["db.rows.write", "db.sql.write"]
    else:
        raise HTTPException(status_code=400, detail=f"Unsupported admin database access mode '{mode}'")
    if not _session_has_any_permission(auth_session, required):
        detail = "Insufficient permissions for the identity database" if normalized == "identity" else "Insufficient permissions for the application database"
        raise HTTPException(status_code=403, detail=detail)


def _control_plane_permissions_for_request(request: Request) -> list[str]:
    path = request.url.path
    method = request.method.upper()
    if path.startswith("/ingest/") or path.startswith("/admin/api/uploads/ingest"):
        return ["documents.write"]
    if path == "/admin/api/reset":
        return ["db.sql.write"]
    if path.startswith("/admin/api/db/sql"):
        return ["db.sql.write"]
    if path.startswith("/admin/api/db/"):
        return ["db.rows.write", "accounts.read", "accounts.write", "db.sql.write"] if method == "GET" else ["db.rows.write", "accounts.write", "db.sql.write"]
    if path.startswith("/admin/api/auth/users"):
        return ["accounts.read", "accounts.write"] if method == "GET" else ["accounts.write"]
    if path.startswith("/admin/api/agent/config") or path.startswith("/admin/api/system/config"):
        return ["runtime.read", "runtime.write", "rate_limits.write"] if method == "GET" else ["runtime.write", "rate_limits.write"]
    if path == "/admin/api/overview" or path.startswith("/admin/api/system/"):
        return CONTROL_PLANE_READ_PERMISSIONS
    if path.startswith("/admin/api/ontology"):
        return ["kg.read", "kg.write"] if method == "GET" else ["kg.write"]
    if path.startswith("/admin/api/kg/"):
        return ["kg.read", "kg.write"] if method == "GET" else ["kg.write"]
    if path.startswith("/admin/api/agent/") or path.startswith("/admin/api/retrieval/"):
        return ["agent.review"]
    if (
        path.startswith("/admin/api/documents")
        or path.startswith("/admin/api/chunks")
        or path.startswith("/admin/api/chroma")
        or path.startswith("/admin/api/metadata")
    ):
        return ["documents.read", "documents.write"] if method == "GET" else ["documents.write"]
    return CONTROL_PLANE_READ_PERMISSIONS


class AuthRegisterRequest(BaseModel):
    email: str
    password: str
    display_name: str | None = None


class AuthLoginRequest(BaseModel):
    email: str
    password: str


class IngestRequest(BaseModel):
    tenant_id: str = "shared"
    source_type: str = "text"
    filename: str
    raw_text: str
    document_class: str = "note"
    parser_version: str = "v1"
    ocr_engine: str | None = None
    ocr_model: str | None = None


class IngestPdfRequest(BaseModel):
    tenant_id: str = "shared"
    path: str
    filename: str | None = None
    document_class: str = "book"
    parser_version: str = "v1"
    page_start: int | None = None
    page_end: int | None = None


class ChunkDecisionRequest(BaseModel):
    action: str


class ReviewBatchRequest(BaseModel):
    document_id: str | None = None
    batch_size: int = 100


class RevalidateDocumentRequest(BaseModel):
    rerun_kg: bool = True


class RetrievalEvalRequest(BaseModel):
    tenant_id: str | None = None
    top_k: int = 5
    queries_file: str = "data/evaluation/retrieval_small_queries.json"
    output: str = "data/evaluation/latest-admin-eval.json"


class AgentEvalRequest(BaseModel):
    tenant_id: str | None = None
    top_k: int = 5
    queries_file: str = "data/evaluation/agent_small_queries.json"
    output: str = "data/evaluation/latest-agent-eval.json"


class RetrievalInspectRequest(BaseModel):
    question: str
    tenant_id: str = "shared"
    document_ids: list[str] | None = None
    top_k: int | None = None
    query_mode: Literal["auto", "general", "sensor"] | None = None


class AgentQueryRequest(BaseModel):
    question: str
    session_id: str | None = None
    session_token: str | None = None
    tenant_id: str = "shared"
    document_ids: list[str] | None = None
    top_k: int | None = None
    query_mode: Literal["auto", "general", "sensor"] | None = None
    workspace_kind: Literal["general", "hive"] | None = None


class AgentProfileUpdateRequest(BaseModel):
    display_name: str | None = None
    user_background: str | None = None
    beekeeping_context: str | None = None
    experience_level: str | None = None
    answer_preferences: list[str] | None = None
    recurring_topics: list[str] | None = None
    persistent_constraints: list[str] | None = None


class UserSensorUpsertRequest(BaseModel):
    external_sensor_id: str
    sensor_name: str
    sensor_type: str = "environment"
    place_id: UUID | None = None
    hive_id: UUID | None = None
    hive_name: str | None = None
    location_label: str | None = None
    status: str = "active"
    metadata_json: dict[str, Any] | None = None


class UserPlaceUpsertRequest(BaseModel):
    external_place_id: str
    place_name: str
    status: str = "active"
    metadata_json: dict[str, Any] | None = None


class UserHiveUpsertRequest(BaseModel):
    external_hive_id: str
    hive_name: str
    place_id: UUID | None = None
    status: str = "active"
    metadata_json: dict[str, Any] | None = None


class SensorReadingItemRequest(BaseModel):
    observed_at: datetime
    metric_name: str
    unit: str | None = None
    numeric_value: float | None = None
    text_value: str | None = None
    quality_score: float | None = None
    metadata_json: dict[str, Any] | None = None


class SensorReadingsIngestRequest(BaseModel):
    readings: list[SensorReadingItemRequest] = Field(min_length=1, max_length=settings.sensor_ingest_max_batch)


class AgentReviewRequest(BaseModel):
    decision: str
    notes: str | None = None
    reviewer: str = "admin"


class AgentFeedbackRequest(BaseModel):
    feedback: str
    notes: str | None = None


class AgentReplayRequest(BaseModel):
    reuse_session: bool = False
    top_k: int | None = None
    query_mode: Literal["auto", "general", "sensor"] | None = None


class AgentConfigUpdateRequest(BaseModel):
    tenant_id: str = "shared"
    config: dict
    updated_by: str = "admin"
    clear_api_key_override: bool = False


class SystemConfigUpdateRequest(BaseModel):
    group: str
    config: dict
    updated_by: str = "admin"


class OntologyUpdateRequest(BaseModel):
    content: str
    updated_by: str = "admin"


class AdminMemoryClearRequest(BaseModel):
    sections: list[str] = Field(min_length=1)


class AdminEditorRequest(BaseModel):
    record_type: str
    record_id: str
    secondary_id: str | None = None
    payload: dict[str, Any] | None = None
    sync_index: bool = False
    updated_by: str = "admin-ui"


class AdminDbRowRequest(BaseModel):
    database: str = "app"
    relation_name: str
    schema_name: str = "public"
    key: dict[str, Any] | None = None
    values: dict[str, Any] | None = None


class AdminSqlRequest(BaseModel):
    database: str = "app"
    sql: str


class AdminAuthUserCreateRequest(BaseModel):
    email: str
    password: str
    display_name: str | None = None
    tenant_id: str = "shared"
    role: str = "member"
    status: str = "active"
    permissions: list[str] | None = None


class AdminAuthUserUpdateRequest(BaseModel):
    email: str | None = None
    password: str | None = None
    display_name: str | None = None
    tenant_id: str | None = None
    role: str | None = None
    status: str | None = None
    permissions: list[str] | None = None


def _resolve_workspace_pdf_path(path: str) -> Path:
    requested = PureWindowsPath(path.replace("/", "\\"))
    requested_parts = list(requested.parts)
    root_parts = list(WINDOWS_WORKSPACE_ROOT.parts)
    if len(requested_parts) <= len(root_parts):
        raise ValueError("PDF ingest path must point to a file inside the workspace")
    if [part.lower() for part in requested_parts[: len(root_parts)]] != [part.lower() for part in root_parts]:
        raise ValueError("PDF ingest path must stay inside the workspace")
    resolved = (APP_WORKSPACE_ROOT.joinpath(*requested_parts[len(root_parts) :])).resolve()
    try:
        resolved.relative_to(APP_WORKSPACE_ROOT)
    except ValueError as exc:
        raise ValueError("PDF ingest path must stay inside the workspace") from exc
    return resolved


@app.middleware("http")
async def protect_control_plane(request: Request, call_next):
    try:
        _enforce_same_origin(request)
        if request.url.path.startswith("/admin/api/") or request.url.path.startswith("/ingest/"):
            if _has_valid_control_plane_token(request):
                request.state.control_plane_token_authenticated = True
                response = await call_next(request)
            else:
                auth_session = _resolve_authenticated_session(request)
                if auth_session is None:
                    return JSONResponse(status_code=401, content={"detail": "Operator login or admin token required"})
                required_permissions = _control_plane_permissions_for_request(request)
                if not _session_has_any_permission(auth_session, required_permissions):
                    return JSONResponse(status_code=403, content={"detail": "Insufficient permissions for this operation"})
                response = await call_next(request)
        else:
            response = await call_next(request)
    except HTTPException as exc:
        return JSONResponse(status_code=exc.status_code, content={"detail": exc.detail})
    refresh_payload = getattr(request.state, "auth_session_cookie_refresh", None)
    if refresh_payload and not bool(getattr(request.state, "suppress_auth_cookie_refresh", False)):
        _apply_auth_session_cookies(response, refresh_payload)
    return response


def _workspace_upload_dir() -> Path:
    # Uploaded files are stored under the mounted workspace data directory inside the container.
    upload_dir = Path("/app/data/uploads")
    upload_dir.mkdir(parents=True, exist_ok=True)
    return upload_dir


def _sanitize_upload_filename(filename: str | None, fallback: str = "upload.bin") -> str:
    # Normalize user-provided names so uploads cannot smuggle traversal characters or odd bytes.
    raw = (filename or fallback).strip().replace("\x00", "")
    raw = Path(raw).name
    sanitized = re.sub(r"[^A-Za-z0-9._-]+", "_", raw).strip("._")
    return sanitized or fallback


def _get_chroma_payload(document_id: str | None = None, limit: int = 50, offset: int = 0, collection_name: str | None = None) -> dict:
    try:
        # The admin UI needs a stable envelope even when Chroma is unavailable, so the
        # helper returns a data+error structure instead of raising directly.
        total = chroma_store.count_records(document_id=document_id, collection_name=collection_name)
        records = chroma_store.list_records(document_id=document_id, limit=limit, offset=offset, collection_name=collection_name)
        return {"records": records, "total": total, "error": None}
    except Exception as exc:  # pragma: no cover - defensive admin path
        return {"records": [], "total": 0, "error": str(exc)}


def _get_chroma_parity(document_id: str | None = None) -> dict:
    # Parity compares Postgres "accepted chunk" truth with Chroma records so the
    # operator can spot drift after replays or manual review actions.
    accepted_rows = repository.list_chunk_records_for_kg(document_id=document_id, limit=5000, offset=0)
    accepted_ids = {row["chunk_id"] for row in accepted_rows}
    chroma_payload = _get_chroma_payload(document_id=document_id, limit=5000, offset=0)
    vector_ids = {row["id"] for row in chroma_payload["records"]}
    missing_vectors = sorted(accepted_ids - vector_ids)
    extra_vectors = sorted(vector_ids - accepted_ids)
    return {
        "document_id": document_id,
        "accepted_chunks": len(accepted_ids),
        "vectors": len(vector_ids),
        "missing_vectors": missing_vectors[:25],
        "extra_vectors": extra_vectors[:25],
        "missing_vectors_total": len(missing_vectors),
        "extra_vectors_total": len(extra_vectors),
        "error": chroma_payload["error"],
    }


@app.get("/health")
def health() -> dict[str, str]:
    return {"status": "ok"}


@app.get("/", response_class=HTMLResponse)
def agent_home():
    return frontend_redirect("/app", fallback_html=AGENT_UI_HTML)


@app.get("/app", response_class=HTMLResponse)
def agent_app():
    return frontend_index_response(fallback_html=AGENT_UI_HTML)


@app.get("/app/{frontend_path:path}")
def agent_app_path(frontend_path: str):
    return frontend_path_response(frontend_path, fallback_html=AGENT_UI_HTML)


@app.get("/admin/app")
def admin_app_redirect():
    return frontend_redirect("/app/control", fallback_html=ADMIN_HTML)


@app.get("/auth/session")
def auth_current_session(request: Request) -> JSONResponse:
    auth_session_id, auth_session_token = _resolve_auth_session_credentials(request)
    auth_session = auth_store.verify_session(auth_session_id, auth_session_token)
    if auth_session is None:
        response = JSONResponse({"authenticated": False, "user": None})
        _clear_auth_session_cookies(response)
        return response
    return JSONResponse(_json_safe(_public_auth_payload(auth_session)))


@app.post("/auth/register")
def auth_register(payload: AuthRegisterRequest, request: Request) -> JSONResponse:
    _enforce_rate_limit(
        request,
        bucket="auth-register",
        limit=settings.auth_login_rate_limit_max_attempts,
        window_seconds=settings.auth_login_rate_limit_window_seconds,
    )
    if not settings.auth_public_registration_enabled:
        raise HTTPException(status_code=403, detail="Self-service registration is disabled")
    public_tenant = settings.agent_public_tenant_id or "shared"
    try:
        user = auth_store.create_user(
            payload.email,
            payload.password,
            display_name=payload.display_name,
            tenant_id=public_tenant,
        )
        result = auth_store.create_session(str(user.get("user_id") or ""))
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    response = JSONResponse(_json_safe(_public_auth_payload(result)))
    _apply_auth_session_cookies(response, result)
    return response


@app.post("/auth/login")
def auth_login(payload: AuthLoginRequest, request: Request) -> JSONResponse:
    _enforce_rate_limit(
        request,
        bucket="auth-login",
        limit=settings.auth_login_rate_limit_max_attempts,
        window_seconds=settings.auth_login_rate_limit_window_seconds,
    )
    try:
        user = auth_store.authenticate_user(payload.email, payload.password)
    except ValueError:
        user = None
    if user is None:
        raise HTTPException(status_code=401, detail="Invalid credentials")
    result = auth_store.create_session(str(user.get("user_id") or ""))
    response = JSONResponse(_json_safe(_public_auth_payload(result)))
    _apply_auth_session_cookies(response, result)
    return response


@app.post("/auth/logout")
def auth_logout(request: Request) -> JSONResponse:
    auth_session_id, auth_session_token = _resolve_auth_session_credentials(request)
    auth_store.revoke_session(auth_session_id, auth_session_token)
    response = JSONResponse({"ok": True, "authenticated": False})
    _clear_auth_session_cookies(response)
    _clear_agent_session_cookies(response)
    _clear_agent_profile_cookies(response)
    return response


@app.get("/admin")
def admin_redirect():
    return frontend_redirect("/app/control", fallback_html=ADMIN_HTML)


@app.get("/admin/legacy", response_class=HTMLResponse)
def admin_legacy() -> str:
    return ADMIN_HTML


@app.get("/admin/api/overview")
def admin_overview() -> dict:
    return repository.get_dashboard_overview()


def _read_reingest_progress_payload() -> dict | None:
    if not REINGEST_PROGRESS_PATH.exists():
        return None
    try:
        payload = json.loads(REINGEST_PROGRESS_PATH.read_text(encoding="utf-8"))
    except Exception:
        return None
    if not isinstance(payload, dict):
        return None
    return payload


def _read_reingest_pid() -> int | None:
    if not REINGEST_PID_PATH.exists():
        return None
    try:
        value = REINGEST_PID_PATH.read_text(encoding="utf-8").strip()
        pid = int(value)
    except Exception:
        return None
    return pid if pid > 0 else None


def _write_reingest_pid(pid: int) -> None:
    REINGEST_PID_PATH.parent.mkdir(parents=True, exist_ok=True)
    REINGEST_PID_PATH.write_text(str(int(pid)), encoding="utf-8")


def _clear_reingest_pid() -> None:
    REINGEST_PID_PATH.unlink(missing_ok=True)


def _iter_reingest_processes() -> list[dict[str, Any]]:
    processes: list[dict[str, Any]] = []
    proc_root = Path("/proc")
    if not proc_root.exists():
        return processes
    for entry in proc_root.iterdir():
        if not entry.name.isdigit():
            continue
        pid = int(entry.name)
        cmdline_path = entry / "cmdline"
        status_path = entry / "status"
        try:
            cmdline_raw = cmdline_path.read_bytes()
            status_text = status_path.read_text(encoding="utf-8", errors="ignore") if status_path.exists() else ""
        except OSError:
            continue
        if re.search(r"^State:\s+Z\b", status_text, flags=re.MULTILINE):
            continue
        cmdline = cmdline_raw.replace(b"\x00", b" ").decode("utf-8", "ignore").strip()
        if not cmdline:
            continue
        script_name = None
        if "tools/reingest_all_pdfs.py" in cmdline:
            script_name = "reingest_all_pdfs.py"
        elif "tools/continue_interrupted_reingest.py" in cmdline:
            script_name = "continue_interrupted_reingest.py"
        if not script_name:
            continue
        processes.append({"pid": pid, "cmdline": cmdline, "script": script_name})
    processes.sort(key=lambda item: item["pid"])
    return processes


def _is_reingest_process_alive(pid: int | None) -> bool:
    if not pid:
        return False
    status_path = Path(f"/proc/{pid}/status")
    if status_path.exists():
        try:
            status_text = status_path.read_text(encoding="utf-8", errors="ignore")
        except OSError:
            return False
        if re.search(r"^State:\s+Z\b", status_text, flags=re.MULTILINE):
            return False
    try:
        os.kill(pid, 0)
    except OSError:
        return False
    return True


def _current_reingest_pid() -> int | None:
    pid = _read_reingest_pid()
    if _is_reingest_process_alive(pid):
        return pid
    _clear_reingest_pid()
    processes = _iter_reingest_processes()
    if not processes:
        return None
    fallback_pid = int(processes[0]["pid"])
    _write_reingest_pid(fallback_pid)
    return fallback_pid


def _has_resumable_reingest_state() -> bool:
    progress = _with_stale_reingest_status(_read_reingest_progress_payload())
    if progress:
        status = str(progress.get("status") or "").strip().lower()
        total_files = int(progress.get("total_files") or 0)
        completed_files = int(progress.get("completed_files") or 0)
        current_file = str(progress.get("current_file") or "").strip()
        if current_file:
            return True
        if status in {"running", "aborted", "completed_with_errors"} and (not total_files or completed_files < total_files):
            return True
    active_documents = [
        row
        for row in repository.list_documents(limit=100, offset=0)
        if str(row.get("status") or "").strip().lower() not in {"completed", "failed", "review"}
    ]
    return bool(active_documents)


def _start_detached_reingest_runner(*, require_resume: bool = False) -> dict[str, Any]:
    current_pid = _current_reingest_pid()
    if current_pid:
        raise RuntimeError(f"Reingest runner already active (pid={current_pid})")
    REINGEST_LAUNCH_LOG_PATH.parent.mkdir(parents=True, exist_ok=True)
    resumable_state = _has_resumable_reingest_state()
    if require_resume and not resumable_state:
        raise RuntimeError("No resumable ingest state found")
    if not resumable_state:
        REINGEST_LAUNCH_LOG_PATH.unlink(missing_ok=True)
        REINGEST_PROGRESS_PATH.unlink(missing_ok=True)
    runner_script_name = "continue_interrupted_reingest.py" if resumable_state else "reingest_all_pdfs.py"
    runner_script = (APP_WORKSPACE_ROOT / "tools" / runner_script_name).resolve()
    if not runner_script.exists():
        raise RuntimeError("Reingest runner script not found")
    process_env = os.environ.copy()
    process_env["PYTHONPATH"] = str(APP_WORKSPACE_ROOT)
    with REINGEST_LAUNCH_LOG_PATH.open("ab") as log_handle:
        process = subprocess.Popen(
            [sys.executable, str(runner_script)],
            cwd=str(APP_WORKSPACE_ROOT),
            stdout=log_handle,
            stderr=subprocess.STDOUT,
            start_new_session=True,
            env=process_env,
        )
    _write_reingest_pid(process.pid)
    return {
        "pid": process.pid,
        "script": runner_script_name,
        "resumed": resumable_state,
        "launch_log": str(REINGEST_LAUNCH_LOG_PATH),
        "progress_file": str(REINGEST_PROGRESS_PATH),
    }


def _stop_detached_reingest_runner() -> dict[str, Any]:
    processes = _iter_reingest_processes()
    if not processes:
        _clear_reingest_pid()
        return {"stopped": False, "reason": "not_running"}
    stopped_pids: list[int] = []
    for process in processes:
        pid = int(process["pid"])
        try:
            os.kill(pid, signal.SIGTERM)
            stopped_pids.append(pid)
        except OSError:
            continue
    _clear_reingest_pid()
    if not stopped_pids:
        return {"stopped": False, "reason": "not_running"}
    return {"stopped": True, "pid": stopped_pids[0], "pids": stopped_pids}


def _with_stale_reingest_status(progress: dict[str, Any] | None) -> dict[str, Any] | None:
    if progress is None:
        return None
    if str(progress.get("status") or "").strip().lower() != "running":
        return progress
    heartbeat_text = str(
        progress.get("last_progress_at")
        or progress.get("last_heartbeat_at")
        or progress.get("finished_at")
        or progress.get("started_at")
        or ""
    ).strip()
    if not heartbeat_text:
        return progress
    try:
        heartbeat_at = datetime.fromisoformat(heartbeat_text.replace("Z", "+00:00"))
    except ValueError:
        return progress
    if heartbeat_at.tzinfo is None:
        heartbeat_at = heartbeat_at.replace(tzinfo=timezone.utc)
    age_seconds = (datetime.now(timezone.utc) - heartbeat_at).total_seconds()
    if age_seconds < REINGEST_STALE_PROGRESS_SECONDS:
        return progress
    updated = dict(progress)
    updated["status"] = "aborted"
    updated["stale_detected"] = True
    updated["stale_age_seconds"] = round(age_seconds, 3)
    updated.setdefault("finished_at", datetime.now(timezone.utc).isoformat())
    return updated


def _build_reingest_activity_snapshot() -> dict:
    runner_processes = _iter_reingest_processes()
    runner_pid = _current_reingest_pid()
    raw_progress = _read_reingest_progress_payload()
    progress = raw_progress if runner_processes else _with_stale_reingest_status(raw_progress)
    snapshot: dict[str, Any] = {
        "progress": progress,
        "has_progress_file": progress is not None,
        "launch_log_exists": REINGEST_LAUNCH_LOG_PATH.exists(),
        "runner_pid": runner_pid,
        "runner_active": bool(runner_processes or runner_pid),
        "runner_processes": runner_processes,
    }
    active_documents = [row for row in repository.list_documents(limit=20, offset=0) if row.get("status") not in {"completed", "failed"}]
    if active_documents:
        snapshot["active_documents"] = active_documents[:5]
    primary_document = active_documents[0] if active_documents else None
    recent_document_stages: list[dict[str, Any]] = []
    if primary_document:
        recent_document_stages = repository.list_stage_runs(document_id=str(primary_document.get("document_id") or ""), limit=12, offset=0)
        snapshot["recent_document_stages"] = recent_document_stages
    asset_root = PAGE_ASSET_ROOT
    asset_files: list[Path] = []
    if active_documents:
        active_document_id = str(active_documents[0].get("document_id") or "").strip()
        if active_document_id:
            active_dir = asset_root / active_document_id
            if active_dir.exists():
                asset_files = [path for path in active_dir.rglob("*") if path.is_file()]
                snapshot["active_asset_directory"] = str(active_dir)
                snapshot["active_asset_document_id"] = active_document_id
    if not asset_files and asset_root.exists():
        asset_files = [path for path in asset_root.rglob("*") if path.is_file()]
    if asset_files:
        existing_asset_files: list[tuple[Path, os.stat_result]] = []
        for path in asset_files:
            try:
                stat_result = path.stat()
            except OSError:
                continue
            existing_asset_files.append((path, stat_result))
        existing_asset_files.sort(key=lambda item: item[1].st_mtime, reverse=True)
        if existing_asset_files:
            latest, latest_stat = existing_asset_files[0]
            snapshot["asset_file_count"] = len(existing_asset_files)
            snapshot["latest_asset_write"] = {
                "path": str(latest),
                "modified_at": datetime.fromtimestamp(latest_stat.st_mtime, tz=timezone.utc).isoformat(),
                "size_bytes": latest_stat.st_size,
            }
        else:
            snapshot["asset_file_count"] = 0
            snapshot["latest_asset_write"] = None
    else:
        snapshot["asset_file_count"] = 0
        snapshot["latest_asset_write"] = None
    snapshot["phase"] = _infer_reingest_phase(
        progress=progress,
        primary_document=primary_document,
        recent_document_stages=recent_document_stages,
        asset_file_count=int(snapshot.get("asset_file_count") or 0),
        latest_asset_write=snapshot.get("latest_asset_write"),
    )
    return snapshot


def _infer_reingest_phase(
    *,
    progress: dict[str, Any] | None,
    primary_document: dict[str, Any] | None,
    recent_document_stages: list[dict[str, Any]],
    asset_file_count: int,
    latest_asset_write: dict[str, Any] | None,
) -> dict[str, Any]:
    phase_key = "idle"
    label = "Idle"
    detail = ""
    explicit_progress_phase = str((progress or {}).get("phase") or "").strip().lower()
    explicit_phase_detail = str((progress or {}).get("phase_detail") or "").strip()
    explicit_phase_metrics = dict((progress or {}).get("phase_metrics") or {})
    latest_stage = recent_document_stages[0] if recent_document_stages else None
    latest_stage_name = str(latest_stage.get("stage_name") or "").strip() if latest_stage else ""
    latest_stage_metrics = latest_stage.get("metrics_json") if isinstance(latest_stage, dict) else None
    document_status = str(primary_document.get("status") or "").strip() if primary_document else ""

    if progress and str(progress.get("status") or "").strip().lower() == "running":
        if explicit_progress_phase:
            phase_key = explicit_progress_phase
            label = {
                "starting": "Starting",
                "preparing": "Preparing",
                "parsing": "Parsing",
                "chunking": "Chunking",
                "validating": "Validating",
                "kg": "KG extraction",
                "embedding": "Embedding / indexing",
                "review": "Review",
                "failed": "Failed",
            }.get(explicit_progress_phase, explicit_progress_phase.replace("_", " ").title())
            detail = explicit_phase_detail
        elif document_status in {"completed", "review", "failed"}:
            phase_key = document_status
            label = {"completed": "Completed", "review": "Review", "failed": "Failed"}.get(document_status, document_status.title())
        elif document_status == "kg_validated":
            phase_key = "embedding"
            label = "Embedding / indexing"
            detail = "Generating vectors and publishing accepted chunks/assets."
        elif document_status == "chunks_validated" or latest_stage_name == "chunks_validated":
            phase_key = "kg"
            label = "KG extraction"
            accepted = int((latest_stage_metrics or {}).get("accepted") or primary_document.get("accepted_chunks") or 0)
            detail = f"Running KG extraction over accepted chunks ({accepted})."
        elif document_status == "chunked" or latest_stage_name == "chunked":
            phase_key = "validating"
            label = "Validating chunks"
            detail = "Scoring and classifying chunks into accepted/review/rejected."
        elif document_status == "parsed" or latest_stage_name == "parsed":
            phase_key = "chunking"
            label = "Chunking"
            detail = "Building persisted chunk records from parsed blocks."
        elif document_status == "content_available" or latest_stage_name == "content_available":
            phase_key = "parsing"
            label = "Parsing"
            detail = "Normalizing extracted text and generating structural blocks."
        elif asset_file_count > 0 or latest_asset_write:
            phase_key = "preparing"
            label = "Preparing"
            detail = "Rendering pages and extracting multimodal assets before parsing."
        else:
            phase_key = "starting"
            label = "Starting"
            detail = "Creating the document and preparing the first ingest pass."
    elif progress:
        status = str(progress.get("status") or "").strip().lower()
        if status:
            phase_key = status
            label = status.replace("_", " ").title()

    return {
        "key": phase_key,
        "label": label,
        "detail": detail,
        "metrics": explicit_phase_metrics,
        "document_status": document_status or None,
        "latest_stage_name": latest_stage_name or None,
        "latest_stage_status": str(latest_stage.get("status") or "").strip() if latest_stage else None,
    }


@app.get("/admin/api/system/processes")
def admin_system_processes(limit: int = Query(default=10, ge=1, le=100)) -> dict:
    # This is a persisted-state monitor, not a live worker inspector. It reports the
    # latest document and stage state that has reached storage.
    documents = repository.list_documents(limit=100, offset=0)
    active_documents = [row for row in documents if row.get("status") not in {"completed", "failed"}][:limit]
    return {
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "overview": repository.get_dashboard_overview(),
        "active_documents": active_documents,
        "recent_stage_runs": repository.list_stage_runs(limit=limit, offset=0),
        "recent_review_runs": repository.list_chunk_review_runs(limit=limit, offset=0),
    }


@app.get("/admin/api/system/ingest-progress")
def admin_system_ingest_progress() -> dict:
    snapshot = _build_reingest_activity_snapshot()
    snapshot["timestamp"] = datetime.now(timezone.utc).isoformat()
    return snapshot


@app.post("/admin/api/system/reingest/start")
def admin_start_reingest() -> dict:
    try:
        payload = _start_detached_reingest_runner()
    except RuntimeError as exc:
        raise HTTPException(status_code=409, detail=str(exc)) from exc
    snapshot = _build_reingest_activity_snapshot()
    snapshot["timestamp"] = datetime.now(timezone.utc).isoformat()
    return {"ok": True, "started": True, "runner": payload, "snapshot": _json_safe(snapshot)}


@app.post("/admin/api/system/reingest/resume")
def admin_resume_reingest() -> dict:
    try:
        payload = _start_detached_reingest_runner(require_resume=True)
    except RuntimeError as exc:
        raise HTTPException(status_code=409, detail=str(exc)) from exc
    snapshot = _build_reingest_activity_snapshot()
    snapshot["timestamp"] = datetime.now(timezone.utc).isoformat()
    return {"ok": True, "started": True, "runner": payload, "snapshot": _json_safe(snapshot)}


@app.post("/admin/api/system/reingest/stop")
def admin_stop_reingest() -> dict:
    payload = _stop_detached_reingest_runner()
    snapshot = _build_reingest_activity_snapshot()
    snapshot["timestamp"] = datetime.now(timezone.utc).isoformat()
    return {"ok": True, **payload, "snapshot": _json_safe(snapshot)}


@app.get("/admin/api/system/routes")
def admin_system_routes() -> list[dict]:
    routes: list[dict] = []
    for route in app.routes:
        path = getattr(route, "path", None)
        methods = sorted(method for method in (getattr(route, "methods", None) or set()) if method not in {"HEAD", "OPTIONS"})
        if not path or not methods:
            continue
        if not (path.startswith("/admin") or path.startswith("/ingest") or path.startswith("/agent")):
            continue
        routes.append({"path": path, "methods": methods})
    routes.sort(key=lambda item: item["path"])
    return routes


@app.get("/admin/api/db/relations")
def admin_db_relations(request: Request, search: str | None = None, schema_name: str | None = None, database: str = Query(default="app")) -> dict:
    normalized_database = _normalize_admin_database_key(database)
    _enforce_admin_database_scope(request, normalized_database, mode="read")
    items = _admin_repository_for_database(normalized_database).list_admin_relations(search=search, schema_name=schema_name)
    return {
        "items": _json_safe(items),
        "total": len(items),
        "database": normalized_database,
        "schema_name": schema_name or "all",
    }


@app.get("/admin/api/db/relations/{relation_name}")
def admin_db_relation_detail(
    request: Request,
    relation_name: str,
    database: str = Query(default="app"),
    schema_name: str = Query(default="public"),
    limit: int = Query(default=50, ge=1, le=250),
    offset: int = Query(default=0, ge=0),
) -> dict:
    try:
        normalized_database = _normalize_admin_database_key(database)
        _enforce_admin_database_scope(request, normalized_database, mode="read")
        payload = _admin_repository_for_database(normalized_database).list_admin_relation_rows(
            relation_name,
            schema_name=schema_name,
            limit=limit,
            offset=offset,
        )
        payload["database"] = normalized_database
        return _json_safe(payload)
    except ValueError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc


@app.post("/admin/api/db/rows")
def admin_db_insert_row(http_request: Request, request: AdminDbRowRequest) -> dict:
    normalized_database = _normalize_admin_database_key(request.database)
    _enforce_admin_database_scope(http_request, normalized_database, mode="write")
    try:
        row = _admin_repository_for_database(normalized_database).insert_admin_relation_row(
            request.relation_name,
            dict(request.values or {}),
            schema_name=request.schema_name,
        )
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    return {
        "database": normalized_database,
        "schema_name": request.schema_name,
        "relation_name": request.relation_name,
        "row": _json_safe(row),
    }


@app.put("/admin/api/db/rows")
def admin_db_update_row(http_request: Request, request: AdminDbRowRequest) -> dict:
    normalized_database = _normalize_admin_database_key(request.database)
    _enforce_admin_database_scope(http_request, normalized_database, mode="write")
    try:
        row = _admin_repository_for_database(normalized_database).update_admin_relation_row(
            request.relation_name,
            dict(request.key or {}),
            dict(request.values or {}),
            schema_name=request.schema_name,
        )
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    if row is None:
        raise HTTPException(status_code=404, detail="Row not found")
    return {
        "database": normalized_database,
        "schema_name": request.schema_name,
        "relation_name": request.relation_name,
        "row": _json_safe(row),
    }


@app.delete("/admin/api/db/rows")
def admin_db_delete_row(http_request: Request, request: AdminDbRowRequest) -> dict:
    normalized_database = _normalize_admin_database_key(request.database)
    _enforce_admin_database_scope(http_request, normalized_database, mode="write")
    try:
        deleted = _admin_repository_for_database(normalized_database).delete_admin_relation_row(
            request.relation_name,
            dict(request.key or {}),
            schema_name=request.schema_name,
        )
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    if not deleted:
        raise HTTPException(status_code=404, detail="Row not found")
    return {
        "database": normalized_database,
        "schema_name": request.schema_name,
        "relation_name": request.relation_name,
        "deleted": True,
    }


@app.post("/admin/api/db/sql")
def admin_db_execute_sql(http_request: Request, request: AdminSqlRequest) -> dict:
    normalized_database = _normalize_admin_database_key(request.database)
    _enforce_admin_database_scope(http_request, normalized_database, mode="sql")
    try:
        payload = _admin_repository_for_database(normalized_database).execute_admin_sql(request.sql)
        payload["database"] = normalized_database
        return _json_safe(payload)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@app.get("/admin/api/auth/users")
def admin_auth_users(
    search: str | None = None,
    tenant_id: str | None = None,
    role: str | None = None,
    status: str | None = None,
    limit: int = Query(default=100, ge=1, le=250),
    offset: int = Query(default=0, ge=0),
) -> dict:
    try:
        items = auth_store.list_users(
            search=search,
            tenant_id=tenant_id,
            role=role,
            status=status,
            limit=limit,
            offset=offset,
        )
        total = auth_store.count_users(search=search, tenant_id=tenant_id, role=role, status=status)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    return {
        "items": _json_safe(items),
        "total": total,
        "limit": limit,
        "offset": offset,
        "available_roles": sorted(ALLOWED_AUTH_ROLES),
        "available_statuses": sorted(ALLOWED_AUTH_STATUSES),
        "available_permissions": sorted(ALLOWED_AUTH_PERMISSIONS),
        "role_permission_presets": _json_safe(DEFAULT_ROLE_PERMISSIONS),
    }


@app.get("/admin/api/auth/users/{user_id}")
def admin_auth_user_detail(user_id: str) -> dict:
    user = auth_store.get_user(user_id)
    if user is None:
        raise HTTPException(status_code=404, detail="Auth user not found")
    return {
        "user": _json_safe(user),
        "sessions": _json_safe(auth_store.list_sessions(user_id=user_id, limit=50, offset=0)),
    }


@app.post("/admin/api/auth/users")
def admin_auth_user_create(request: AdminAuthUserCreateRequest) -> dict:
    try:
        user = auth_store.create_user(
            request.email,
            request.password,
            display_name=request.display_name,
            tenant_id=request.tenant_id,
            role=request.role,
            status=request.status,
            permissions=request.permissions,
        )
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    return {"user": _json_safe(user)}


@app.put("/admin/api/auth/users/{user_id}")
def admin_auth_user_update(user_id: str, request: AdminAuthUserUpdateRequest) -> dict:
    prior_user = auth_store.get_user(user_id)
    try:
        user = auth_store.update_user(
            user_id,
            email=request.email,
            password=request.password,
            display_name=request.display_name,
            tenant_id=request.tenant_id,
            role=request.role,
            status=request.status,
            permissions=request.permissions,
        )
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    if user is None:
        raise HTTPException(status_code=404, detail="Auth user not found")
    if str(user.get("status") or "") != "active":
        cleanup_tenants: list[str] = []
        for candidate in (
            str((prior_user or {}).get("tenant_id") or "").strip(),
            str(user.get("tenant_id") or "").strip(),
            str(settings.agent_public_tenant_id or "shared").strip(),
        ):
            if candidate and candidate not in cleanup_tenants:
                cleanup_tenants.append(candidate)
        for tenant_id in cleanup_tenants:
            repository.delete_sensor_data_for_auth_user(user_id, tenant_id=tenant_id)
    return {"user": _json_safe(user)}


@app.post("/admin/api/auth/users/{user_id}/revoke-sessions")
def admin_auth_user_revoke_sessions(user_id: str) -> dict:
    revoked = auth_store.revoke_user_sessions(user_id)
    return {"user_id": user_id, "revoked_sessions": revoked}


@app.delete("/admin/api/auth/users/{user_id}")
def admin_auth_user_delete(user_id: str) -> dict:
    existing_user = auth_store.get_user(user_id)
    cleanup_tenant = str((existing_user or {}).get("tenant_id") or settings.agent_public_tenant_id or "shared")
    repository.delete_sensor_data_for_auth_user(user_id, tenant_id=cleanup_tenant)
    deleted = auth_store.delete_user(user_id)
    if not deleted:
        raise HTTPException(status_code=404, detail="Auth user not found")
    return {"user_id": user_id, "deleted": True}


@app.get("/admin/api/documents")
def admin_documents(limit: int = Query(default=25, ge=1, le=250), offset: int = Query(default=0, ge=0)) -> dict:
    return {
        "items": repository.list_documents(limit=limit, offset=offset),
        "total": repository.count_documents(),
        "limit": limit,
        "offset": offset,
    }


@app.get("/admin/api/documents/{document_id}")
def admin_document_detail(document_id: str) -> dict:
    detail = repository.get_document_detail(document_id)
    if detail is None:
        raise HTTPException(status_code=404, detail="Document not found")
    return detail


@app.get("/admin/api/documents/{document_id}/bundle")
def admin_document_bundle(document_id: str, limit: int = Query(default=250, ge=25, le=1000)) -> dict:
    detail = repository.get_document_detail(document_id)
    if detail is None:
        raise HTTPException(status_code=404, detail="Document not found")
    sources = repository.list_document_sources(document_id=document_id, limit=limit)
    pages = repository.list_document_pages(document_id=document_id, limit=limit)
    assets = repository.list_page_assets(document_id=document_id, limit=limit)
    chunk_asset_links = repository.list_chunk_asset_links(document_id=document_id, limit=limit)
    chunks = repository.list_chunks(document_id=document_id, limit=limit)
    metadata = repository.list_chunk_metadata(document_id=document_id, limit=limit)
    kg_assertions = repository.list_kg_assertions(document_id=document_id, limit=limit)
    kg_entities = repository.list_kg_entities(document_id=document_id, limit=limit)
    kg_evidence = repository.list_kg_evidence(document_id=document_id, limit=limit)
    kg_raw = repository.list_kg_raw_extractions(document_id=document_id, limit=limit)
    chroma = _get_chroma_payload(document_id=document_id, limit=limit, offset=0)
    asset_chroma = _get_chroma_payload(document_id=document_id, limit=limit, offset=0, collection_name=chroma_store.asset_collection.name)
    counts = repository.get_document_related_counts(document_id=document_id)
    counts["vectors"] = chroma["total"]
    counts["asset_vectors"] = asset_chroma["total"]
    counts["parity"] = _get_chroma_parity(document_id=document_id)
    # The bundle endpoint is the admin drilldown payload: one request returns every
    # major storage view for a document so the UI can cross-link them client-side.
    return {
        **detail,
        "sources": sources,
        "pages": pages,
        "page_assets": assets,
        "chunk_asset_links": chunk_asset_links,
        "chunks": chunks,
        "chunk_metadata": metadata,
        "kg_assertions": kg_assertions,
        "kg_entities": kg_entities,
        "kg_evidence": kg_evidence,
        "kg_raw": kg_raw,
        "chroma_records": chroma["records"],
        "chroma_error": chroma["error"],
        "asset_chroma_records": asset_chroma["records"],
        "asset_chroma_error": asset_chroma["error"],
        "counts": counts,
        "bundle_limit": limit,
    }


@app.get("/admin/api/activity/stages")
def admin_stage_runs(
    document_id: str | None = None,
    status: str | None = None,
    limit: int = Query(default=100, ge=1, le=250),
    offset: int = Query(default=0, ge=0),
) -> dict:
    return {
        "items": repository.list_stage_runs(document_id=document_id, status=status, limit=limit, offset=offset),
        "total": repository.count_stage_runs(document_id=document_id, status=status),
        "limit": limit,
        "offset": offset,
    }


@app.get("/admin/api/activity/reviews")
def admin_review_runs(
    document_id: str | None = None,
    decision: str | None = None,
    limit: int = Query(default=100, ge=1, le=250),
    offset: int = Query(default=0, ge=0),
) -> dict:
    return {
        "items": repository.list_chunk_review_runs(document_id=document_id, decision=decision, limit=limit, offset=offset),
        "total": repository.count_chunk_review_runs(document_id=document_id, decision=decision),
        "limit": limit,
        "offset": offset,
    }


@app.get("/admin/api/agent/sessions")
def admin_agent_sessions(
    status: str | None = None,
    limit: int = Query(default=100, ge=1, le=250),
    offset: int = Query(default=0, ge=0),
) -> dict:
    return {
        "items": repository.list_agent_sessions(status=status, limit=limit, offset=offset),
        "total": repository.count_agent_sessions(status=status),
        "limit": limit,
        "offset": offset,
    }


@app.get("/admin/api/agent/sessions/{session_id}")
def admin_agent_session_detail(session_id: str) -> dict:
    session = repository.get_agent_session(session_id)
    if session is None:
        raise HTTPException(status_code=404, detail="Agent session not found")
    return {
        "session": session,
        "profile": repository.get_agent_profile(str(session.get("profile_id") or "")) if session.get("profile_id") else None,
        "memory": repository.get_agent_session_memory(session_id),
        "messages": repository.list_agent_messages(session_id, limit=100),
        "query_runs": repository.list_agent_query_runs(session_id=session_id, limit=100, offset=0),
    }


@app.post("/admin/api/agent/sessions/{session_id}/memory/clear")
def admin_clear_agent_session_memory(session_id: str, request: AdminMemoryClearRequest) -> dict:
    memory_row = repository.get_agent_session_memory(session_id)
    if memory_row is None:
        raise HTTPException(status_code=404, detail="Agent session memory not found")
    cleared_sections = _normalize_memory_clear_sections(request.sections, SESSION_MEMORY_CLEAR_RULES)
    summary_json = _apply_memory_clear_rules(
        dict(memory_row.get("summary_json") or {}),
        sections=cleared_sections,
        allowed_rules=SESSION_MEMORY_CLEAR_RULES,
    )
    refreshed_summary = _coerce_and_refresh_session_memory(summary_json)
    updated = repository.update_agent_session_memory_record(
        session_id,
        {
            "summary_json": refreshed_summary,
            "summary_text": str(refreshed_summary.get("summary_text") or ""),
        },
    )
    return {
        "session_id": session_id,
        "cleared_sections": cleared_sections,
        "available_sections": list(SESSION_MEMORY_CLEAR_RULES.keys()),
        "memory": updated,
    }


@app.get("/admin/api/agent/profiles")
def admin_agent_profiles(
    tenant_id: str = "shared",
    status: str | None = None,
    limit: int = Query(default=100, ge=1, le=250),
    offset: int = Query(default=0, ge=0),
) -> dict:
    return {
        "items": repository.list_agent_profiles(tenant_id=tenant_id, status=status, limit=limit, offset=offset),
        "total": repository.count_agent_profiles(tenant_id=tenant_id, status=status),
        "limit": limit,
        "offset": offset,
    }


@app.get("/admin/api/agent/profiles/{profile_id}")
def admin_agent_profile_detail(profile_id: str) -> dict:
    profile = repository.get_agent_profile(profile_id)
    if profile is None:
        raise HTTPException(status_code=404, detail="Agent profile not found")
    return profile


@app.post("/admin/api/agent/profiles/{profile_id}/memory/clear")
def admin_clear_agent_profile_memory(profile_id: str, request: AdminMemoryClearRequest) -> dict:
    profile = repository.get_agent_profile(profile_id)
    if profile is None:
        raise HTTPException(status_code=404, detail="Agent profile not found")
    cleared_sections = _normalize_memory_clear_sections(request.sections, PROFILE_MEMORY_CLEAR_RULES)
    summary_json = _apply_memory_clear_rules(
        dict(profile.get("summary_json") or {}),
        sections=cleared_sections,
        allowed_rules=PROFILE_MEMORY_CLEAR_RULES,
    )
    refreshed_summary = _coerce_and_refresh_profile_summary(summary_json)
    updated = repository.update_agent_profile_record(
        profile_id,
        {
            "summary_json": refreshed_summary,
            "summary_text": str(refreshed_summary.get("summary_text") or ""),
        },
    )
    return {
        "profile_id": profile_id,
        "cleared_sections": cleared_sections,
        "available_sections": list(PROFILE_MEMORY_CLEAR_RULES.keys()),
        "profile": updated,
    }


@app.get("/admin/api/agent/runs")
def admin_agent_runs(
    session_id: str | None = None,
    status: str | None = None,
    abstained: bool | None = None,
    review_status: str | None = None,
    limit: int = Query(default=100, ge=1, le=250),
    offset: int = Query(default=0, ge=0),
) -> dict:
    return {
        "items": repository.list_agent_query_runs(
            session_id=session_id,
            status=status,
            abstained=abstained,
            review_status=review_status,
            limit=limit,
            offset=offset,
        ),
        "total": repository.count_agent_query_runs(session_id=session_id, status=status, abstained=abstained, review_status=review_status),
        "limit": limit,
        "offset": offset,
    }


@app.get("/admin/api/agent/runs/{query_run_id}")
def admin_agent_run_detail(query_run_id: str) -> dict:
    detail = repository.get_agent_query_detail(query_run_id)
    if detail is None:
        raise HTTPException(status_code=404, detail="Agent query run not found")
    return detail


@app.get("/admin/api/agent/reviews")
def admin_agent_reviews(
    decision: str | None = None,
    limit: int = Query(default=100, ge=1, le=250),
    offset: int = Query(default=0, ge=0),
) -> dict:
    return {
        "items": repository.list_agent_answer_reviews(decision=decision, limit=limit, offset=offset),
        "total": repository.count_agent_answer_reviews(decision=decision),
        "limit": limit,
        "offset": offset,
    }


@app.get("/admin/api/agent/patterns")
def admin_agent_patterns(
    tenant_id: str = "shared",
    search: str | None = None,
    limit: int = Query(default=50, ge=1, le=250),
    offset: int = Query(default=0, ge=0),
) -> dict:
    return {
        "items": repository.list_agent_query_patterns(tenant_id=tenant_id, search=search, limit=limit, offset=offset),
        "total": repository.count_agent_query_patterns(tenant_id=tenant_id, search=search),
        "limit": limit,
        "offset": offset,
    }


@app.get("/admin/api/agent/metrics")
def admin_agent_metrics() -> dict:
    return repository.get_agent_metrics()


@app.get("/admin/api/agent/config")
def admin_agent_config(tenant_id: str = "shared") -> dict:
    row = repository.get_agent_runtime_config(tenant_id)
    secret_row = repository.get_agent_runtime_secret(tenant_id)
    config = merged_agent_runtime_config((row or {}).get("settings_json") or {})
    config["api_key_override"] = ""
    stored_override = dict((row or {}).get("settings_json") or {})
    stored_override["api_key_override"] = ""
    return {
        "tenant_id": tenant_id,
        "defaults": default_agent_runtime_config(),
        "config": config,
        "stored_override": stored_override,
        "has_api_key_override": bool((secret_row or {}).get("has_api_key_override")),
        "provider_key_sources": _provider_key_sources(secret_row),
        "effective_api_key_source": _provider_key_sources(secret_row).get("agent", {}).get("source"),
        "updated_at": row.get("updated_at") if row else None,
        "updated_by": row.get("updated_by") if row else None,
    }


@app.put("/admin/api/agent/config")
def admin_update_agent_config(request: AgentConfigUpdateRequest) -> dict:
    raw_config = dict(request.config or {})
    api_key_override = str(raw_config.pop("api_key_override", "") or "").strip()
    config = coerce_agent_runtime_config(raw_config)
    config["api_key_override"] = ""
    repository.save_agent_runtime_config(
        tenant_id=request.tenant_id,
        settings_json=config,
        updated_by=request.updated_by,
    )
    if request.clear_api_key_override:
        repository.delete_agent_runtime_secret(request.tenant_id)
    elif api_key_override:
        try:
            repository.save_agent_runtime_secret(request.tenant_id, api_key_override, updated_by=request.updated_by)
        except RuntimeError as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc
    return admin_agent_config(tenant_id=request.tenant_id)


@app.delete("/admin/api/agent/config")
def admin_reset_agent_config(tenant_id: str = "shared") -> dict:
    repository.delete_agent_runtime_config(tenant_id)
    repository.delete_agent_runtime_secret(tenant_id)
    return admin_agent_config(tenant_id=tenant_id)


@app.get("/admin/api/system/config")
def admin_system_config(group: str = "platform") -> dict:
    return _build_system_config_payload(group)


@app.put("/admin/api/system/config")
def admin_update_system_config(request: SystemConfigUpdateRequest) -> dict:
    _write_system_group_config(request.group, request.config)
    return _build_system_config_payload(request.group)


@app.delete("/admin/api/system/config")
def admin_reset_system_config(group: str = "platform") -> dict:
    _reset_system_group_config(group)
    return _build_system_config_payload(group)


@app.get("/admin/api/ontology")
def admin_ontology() -> dict:
    return _build_ontology_payload()


@app.put("/admin/api/ontology")
def admin_update_ontology(request: OntologyUpdateRequest) -> dict:
    path = _ontology_path()
    temp_path = path.with_suffix(path.suffix + ".tmp")
    temp_path.write_text(request.content, encoding="utf-8")
    try:
        load_ontology(str(temp_path))
    except Exception as exc:
        temp_path.unlink(missing_ok=True)
        raise HTTPException(status_code=400, detail=f"Ontology validation failed: {exc}") from exc
    temp_path.replace(path)
    return _build_ontology_payload()


@app.post("/admin/api/editor/load")
def admin_editor_load(request: AdminEditorRequest) -> dict:
    return _load_editor_record(request.record_type, request.record_id, request.secondary_id)


@app.put("/admin/api/editor/save")
def admin_editor_save(request: AdminEditorRequest) -> dict:
    return _save_editor_record(
        request.record_type,
        request.record_id,
        dict(request.payload or {}),
        request.secondary_id,
        sync_index=request.sync_index,
    )


@app.post("/admin/api/editor/delete")
def admin_editor_delete(request: AdminEditorRequest) -> dict:
    return _delete_editor_record(
        request.record_type,
        request.record_id,
        request.secondary_id,
        sync_index=request.sync_index,
    )


@app.post("/admin/api/editor/resync")
def admin_editor_resync(request: AdminEditorRequest) -> dict:
    return _resync_editor_record(request.record_type, request.record_id)


@app.post("/admin/api/agent/runs/{query_run_id}/review")
def admin_agent_run_review(query_run_id: str, request: AgentReviewRequest) -> dict:
    detail = repository.get_agent_query_detail(query_run_id)
    if detail is None:
        raise HTTPException(status_code=404, detail="Agent query run not found")
    if request.decision not in ALLOWED_AGENT_REVIEW_DECISIONS:
        raise HTTPException(status_code=400, detail="Invalid review decision")
    try:
        repository.save_agent_answer_review(
            query_run_id=query_run_id,
            decision=request.decision,
            reviewer=request.reviewer,
            notes=request.notes,
            payload={"manual": True, "source": "admin-ui"},
        )
    except ValueError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    detail = repository.get_agent_query_detail(query_run_id)
    return {
        "query_run_id": query_run_id,
        "decision": request.decision,
        "reviewer": request.reviewer,
        "pattern": detail.get("pattern") if detail else None,
    }


@app.post("/admin/api/agent/runs/{query_run_id}/replay")
def admin_agent_run_replay(query_run_id: str, request: AgentReplayRequest) -> dict:
    detail = repository.get_agent_query_detail(query_run_id)
    if detail is None:
        raise HTTPException(status_code=404, detail="Agent query run not found")
    run = detail["query_run"]
    tenant_id = str(run.get("tenant_id") or "shared")
    stored_snapshot_id = str(run.get("corpus_snapshot_id") or "").strip()
    current_snapshot_id = str(repository.get_latest_corpus_snapshot_id(tenant_id) or "").strip()
    if stored_snapshot_id and current_snapshot_id and stored_snapshot_id != current_snapshot_id:
        raise HTTPException(
            status_code=409,
            detail={
                "message": "Replay rejected because the corpus snapshot has changed since the original run.",
                "stored_corpus_snapshot_id": stored_snapshot_id,
                "current_corpus_snapshot_id": current_snapshot_id,
            },
        )
    prompt_payload = dict(run.get("prompt_payload") or {})
    request_scope = dict(prompt_payload.get("request_scope") or {})
    document_ids = [str(item) for item in (request_scope.get("document_ids") or []) if str(item).strip()]
    if not document_ids:
        document_ids = sorted(
            {
                str(item["document_id"])
                for item in detail["sources"]
                if item.get("source_kind") == "chunk" and item.get("document_id")
            }
        )
    replay_top_k = request.top_k
    if replay_top_k is None:
        scoped_top_k = request_scope.get("top_k")
        replay_top_k = int(scoped_top_k) if scoped_top_k is not None else None
    replay_query_mode = request.query_mode
    if replay_query_mode is None:
        scoped_query_mode = str(request_scope.get("query_mode") or "").strip().lower()
        replay_query_mode = scoped_query_mode or None
    try:
        # Replay intentionally rebuilds the query from persisted run state instead of
        # trusting the UI to reconstruct the original request.
        chat_kwargs = {
            "question": str(run.get("question") or ""),
            "session_id": str(run.get("session_id")) if request.reuse_session and run.get("session_id") else None,
            "auth_user_id": str(request_scope.get("auth_user_id") or "").strip() or None,
            "tenant_id": tenant_id,
            "document_ids": document_ids or None,
            "top_k": replay_top_k,
            "query_mode": replay_query_mode,
            "trusted_tenant": True,
            "trusted_session_reuse": True,
        }
        workspace_kind = str(request_scope.get("workspace_kind") or "").strip().lower()
        if workspace_kind:
            chat_kwargs["workspace_kind"] = workspace_kind
        return agent_service.chat(
            **chat_kwargs,
        )
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    except AgentQueryError as exc:
        raise HTTPException(status_code=502, detail=str(exc)) from exc


@app.post("/agent/runs/{query_run_id}/feedback")
def agent_run_feedback(query_run_id: str, request: Request, payload: AgentFeedbackRequest) -> dict:
    auth_session = _require_authenticated_sensor_user(request, write=False)
    user = auth_session.get("user") or {}
    public_tenant = settings.agent_public_tenant_id or "shared"
    session_id, session_token = _resolve_agent_session_cookies(request)
    user_id = str(user.get("user_id") or "").strip()
    if not session_id or not repository.verify_agent_session_token(
        session_id,
        session_token,
        tenant_id=public_tenant,
        auth_user_id=user_id,
    ):
        raise HTTPException(status_code=400, detail="Active session is required for feedback")
    detail = repository.get_agent_query_detail(
        query_run_id,
        tenant_id=public_tenant,
        session_id=session_id,
        auth_user_id=user_id,
    )
    if detail is None:
        raise HTTPException(status_code=404, detail="Agent query run not found")
    feedback = (payload.feedback or "").strip().lower()
    if feedback not in {"like", "dislike"}:
        raise HTTPException(status_code=400, detail="Feedback must be like or dislike")
    decision = "approved" if feedback == "like" else "rejected"
    try:
        repository.save_agent_answer_review(
            query_run_id=query_run_id,
            decision=decision,
            reviewer="user-ui",
            notes=payload.notes,
            payload={"manual": True, "source": "user-ui", "feedback": feedback},
            tenant_id=public_tenant,
            session_id=session_id,
        )
    except ValueError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    detail = repository.get_agent_query_detail(
        query_run_id,
        tenant_id=public_tenant,
        session_id=session_id,
        auth_user_id=user_id,
    )
    return {
        "query_run_id": query_run_id,
        "feedback": feedback,
        "decision": decision,
        "pattern": detail.get("pattern") if detail else None,
    }


@app.get("/agent/session")
def agent_current_session(request: Request) -> JSONResponse:
    auth_session = _require_authenticated_public_user(request)
    user = auth_session.get("user") or {}
    public_tenant = settings.agent_public_tenant_id or "shared"
    session_id = request.cookies.get(AGENT_SESSION_ID_COOKIE)
    session_token = request.cookies.get(AGENT_SESSION_TOKEN_COOKIE)
    profile_id = request.cookies.get(AGENT_PROFILE_ID_COOKIE)
    if not session_id:
        return JSONResponse({"session_id": None, "active": False, "profile_id": profile_id})
    session = repository.get_agent_session(session_id, tenant_id=public_tenant)
    if session is None or not repository.verify_agent_session_token(
        session_id,
        session_token,
        tenant_id=public_tenant,
        auth_user_id=str(user.get("user_id") or "").strip(),
    ):
        response = JSONResponse({"session_id": None, "active": False, "profile_id": profile_id}, status_code=400)
        _clear_agent_session_cookies(response)
        return response
    return JSONResponse(
        _json_safe(
            {
                "session_id": session_id,
                "active": True,
                "title": session.get("title"),
                "status": session.get("status"),
                "workspace_kind": session.get("workspace_kind") or "general",
                "updated_at": session.get("updated_at"),
                "profile_id": session.get("profile_id") or profile_id,
            }
        )
    )


@app.get("/agent/sessions")
def agent_list_sessions(
    request: Request,
    workspace_kind: Literal["general", "hive"] | None = Query(default="general"),
    limit: int = Query(default=24, ge=1, le=100),
    offset: int = Query(default=0, ge=0),
) -> JSONResponse:
    auth_session = _require_authenticated_public_user(request)
    public_tenant = settings.agent_public_tenant_id or "shared"
    auth_user_id = str(((auth_session.get("user") or {}).get("user_id")) or "").strip()
    items = repository.list_agent_sessions(
        tenant_id=public_tenant,
        auth_user_id=auth_user_id,
        workspace_kind=workspace_kind or None,
        limit=limit,
        offset=offset,
    )
    total = repository.count_agent_sessions(
        tenant_id=public_tenant,
        auth_user_id=auth_user_id,
        workspace_kind=workspace_kind or None,
    )
    return JSONResponse(_json_safe({"items": items, "total": total}))


@app.post("/agent/sessions/{session_id}/activate")
def agent_activate_session(
    request: Request,
    session_id: str,
    limit: int = Query(default=200, ge=1, le=400),
) -> JSONResponse:
    auth_session = _require_authenticated_public_user(request)
    public_tenant = settings.agent_public_tenant_id or "shared"
    auth_user_id = str(((auth_session.get("user") or {}).get("user_id")) or "").strip()
    session = repository.get_agent_session(session_id, tenant_id=public_tenant)
    if session is None or str(session.get("auth_user_id") or "").strip() != auth_user_id:
        raise HTTPException(status_code=404, detail="Session not found")
    session_token = str(uuid4())
    repository.set_agent_session_token(session_id, session_token)
    response = JSONResponse(
        _json_safe(
            {
                "session": session,
                "memory": repository.get_agent_session_memory(session_id, tenant_id=public_tenant, auth_user_id=auth_user_id),
                "messages": repository.list_agent_messages(session_id, limit=limit, tenant_id=public_tenant, auth_user_id=auth_user_id),
            }
        )
    )
    _apply_agent_session_cookies(response, {"session_id": session_id, "session_token": session_token})
    return response


@app.post("/agent/session/reset")
def agent_reset_session(request: Request) -> JSONResponse:
    _require_authenticated_public_user(request)
    response = JSONResponse({"ok": True, "session_id": None})
    _clear_agent_session_cookies(response)
    return response


@app.get("/agent/profile")
def agent_current_profile(request: Request) -> JSONResponse:
    auth_session = _require_authenticated_public_user(request)
    public_tenant = settings.agent_public_tenant_id or "shared"
    auth_user_id = str(((auth_session.get("user") or {}).get("user_id")) or "").strip()
    profile_id, profile_token = _resolve_agent_profile_credentials(request)
    profile = repository.get_agent_profile(profile_id, tenant_id=public_tenant) if profile_id else None
    if profile is None and auth_user_id:
        profile = repository.get_agent_profile_by_auth_user(auth_user_id, tenant_id=public_tenant)
    if profile is None:
        display_name = str((auth_session.get("user") or {}).get("display_name") or "").strip() or None
        created_profile_id = repository.create_agent_profile(
            tenant_id=public_tenant,
            display_name=display_name,
            auth_user_id=auth_user_id or None,
        )
        created_profile_token = str(uuid4())
        repository.set_agent_profile_token(created_profile_id, created_profile_token)
        profile = repository.get_agent_profile(created_profile_id, tenant_id=public_tenant)
        response = JSONResponse(
            _json_safe(
                {
                    "profile_id": created_profile_id,
                    "active": profile is not None,
                    "profile": profile,
                }
            )
        )
        _apply_agent_profile_cookies(
            response,
            {
                "profile_id": created_profile_id,
                "profile_token": created_profile_token,
            },
        )
        return response
    profile_id_value = str(profile.get("profile_id") or "")
    effective_profile_token = profile_token or ""
    if not repository.verify_agent_profile_token(profile_id_value, effective_profile_token, tenant_id=public_tenant):
        if auth_user_id and str(profile.get("auth_user_id") or "").strip() == auth_user_id:
            effective_profile_token = str(uuid4())
            repository.set_agent_profile_token(profile_id_value, effective_profile_token)
        else:
            response = JSONResponse({"profile_id": None, "active": False, "profile": None}, status_code=400)
            _clear_agent_profile_cookies(response)
            return response
    response = JSONResponse(_json_safe({"profile_id": profile_id_value, "active": True, "profile": profile}))
    _apply_agent_profile_cookies(response, {"profile_id": profile_id_value, "profile_token": effective_profile_token})
    return response


@app.put("/agent/profile")
def agent_update_profile(request: Request, payload: AgentProfileUpdateRequest) -> JSONResponse:
    auth_session = _require_authenticated_public_user(request)
    public_tenant = settings.agent_public_tenant_id or "shared"
    auth_user_id = str(((auth_session.get("user") or {}).get("user_id")) or "").strip()
    profile_id, profile_token = _resolve_agent_profile_credentials(request)
    profile = repository.get_agent_profile(profile_id, tenant_id=public_tenant) if profile_id else None
    if profile is None and auth_user_id:
        profile = repository.get_agent_profile_by_auth_user(auth_user_id, tenant_id=public_tenant)
    if profile is None:
        raise HTTPException(status_code=400, detail="Profile token is required")
    profile_id_value = str(profile.get("profile_id") or "")
    effective_profile_token = profile_token or ""
    if not repository.verify_agent_profile_token(profile_id_value, effective_profile_token, tenant_id=public_tenant):
        if auth_user_id and str(profile.get("auth_user_id") or "").strip() == auth_user_id:
            effective_profile_token = str(uuid4())
            repository.set_agent_profile_token(profile_id_value, effective_profile_token)
        else:
            raise HTTPException(status_code=400, detail="Profile token is required")
    summary_json = dict(profile.get("summary_json") or {})
    summary_json.update(
        {
            "user_background": str(payload.user_background or summary_json.get("user_background") or "").strip()[:220],
            "beekeeping_context": str(payload.beekeeping_context or summary_json.get("beekeeping_context") or "").strip()[:220],
            "experience_level": str(payload.experience_level or summary_json.get("experience_level") or "").strip()[:80],
            "answer_preferences": [str(item).strip()[:160] for item in (payload.answer_preferences or summary_json.get("answer_preferences") or []) if str(item).strip()][:8],
            "recurring_topics": [str(item).strip()[:180] for item in (payload.recurring_topics or summary_json.get("recurring_topics") or []) if str(item).strip()][:8],
            "persistent_constraints": [str(item).strip()[:220] for item in (payload.persistent_constraints or summary_json.get("persistent_constraints") or []) if str(item).strip()][:8],
            "last_query": str(summary_json.get("last_query") or "").strip()[:220],
        }
    )
    summary_text = "\n".join(
        part
        for part in [
            f"background: {summary_json['user_background']}".strip() if summary_json.get("user_background") else "",
            f"context: {summary_json['beekeeping_context']}".strip() if summary_json.get("beekeeping_context") else "",
            f"experience: {summary_json['experience_level']}".strip() if summary_json.get("experience_level") else "",
            "preferences: " + "; ".join(summary_json.get("answer_preferences") or []) if summary_json.get("answer_preferences") else "",
            "topics: " + " | ".join(summary_json.get("recurring_topics") or []) if summary_json.get("recurring_topics") else "",
            "constraints: " + "; ".join(summary_json.get("persistent_constraints") or []) if summary_json.get("persistent_constraints") else "",
        ]
        if part
    )
    repository.save_agent_profile(
        profile_id=str(profile.get("profile_id") or ""),
        summary_json=summary_json,
        summary_text=summary_text,
        source_provider=str(profile.get("source_provider") or "user-ui"),
        source_model=str(profile.get("source_model") or "user-ui"),
        prompt_version=str(profile.get("prompt_version") or "manual"),
        display_name=payload.display_name,
    )
    updated = repository.get_agent_profile(str(profile.get("profile_id") or ""), tenant_id=public_tenant)
    response = JSONResponse(
        _json_safe({"profile_id": str(profile.get("profile_id") or ""), "active": updated is not None, "profile": updated})
    )
    _apply_agent_profile_cookies(response, {"profile_id": str(profile.get("profile_id") or ""), "profile_token": effective_profile_token})
    return response


@app.get("/sensors")
def list_current_user_sensors(
    request: Request,
    status: str | None = None,
    limit: int = Query(default=100, ge=1, le=500),
    offset: int = Query(default=0, ge=0),
) -> dict:
    auth_session = _require_authenticated_sensor_user(request, write=False)
    user = auth_session.get("user") or {}
    public_tenant = settings.agent_public_tenant_id or "shared"
    user_id = str(user.get("user_id") or "").strip()
    _enforce_rate_limit(
        request,
        bucket="sensor-read",
        limit=settings.public_agent_rate_limit_max_requests,
        window_seconds=settings.public_agent_rate_limit_window_seconds,
        subject=user_id,
    )
    try:
        return {
            "items": repository.list_user_sensors(
                tenant_id=public_tenant,
                auth_user_id=user_id,
                status=status,
                limit=limit,
                offset=offset,
            ),
            "total": repository.count_user_sensors(
                tenant_id=public_tenant,
                auth_user_id=user_id,
                status=status,
            ),
            "limit": limit,
            "offset": offset,
        }
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@app.get("/places")
def list_current_user_places(
    request: Request,
    status: str | None = None,
    limit: int = Query(default=100, ge=1, le=500),
    offset: int = Query(default=0, ge=0),
) -> dict:
    auth_session = _require_authenticated_sensor_user(request, write=False)
    user = auth_session.get("user") or {}
    public_tenant = settings.agent_public_tenant_id or "shared"
    user_id = str(user.get("user_id") or "").strip()
    _enforce_rate_limit(
        request,
        bucket="sensor-read",
        limit=settings.public_agent_rate_limit_max_requests,
        window_seconds=settings.public_agent_rate_limit_window_seconds,
        subject=user_id,
    )
    try:
        return {
            "items": repository.list_user_places(
                tenant_id=public_tenant,
                auth_user_id=user_id,
                status=status,
                limit=limit,
                offset=offset,
            ),
            "total": repository.count_user_places(
                tenant_id=public_tenant,
                auth_user_id=user_id,
                status=status,
            ),
            "limit": limit,
            "offset": offset,
        }
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@app.post("/places")
def upsert_current_user_place(request: Request, payload: UserPlaceUpsertRequest) -> dict:
    auth_session = _require_authenticated_sensor_user(request, write=True)
    user = auth_session.get("user") or {}
    public_tenant = settings.agent_public_tenant_id or "shared"
    user_id = str(user.get("user_id") or "").strip()
    _enforce_rate_limit(
        request,
        bucket="sensor-write",
        limit=max(10, settings.public_agent_rate_limit_max_requests // 2),
        window_seconds=settings.public_agent_rate_limit_window_seconds,
        subject=user_id,
    )
    try:
        place = repository.upsert_user_place(
            tenant_id=public_tenant,
            auth_user_id=user_id,
            external_place_id=payload.external_place_id,
            place_name=payload.place_name,
            status=payload.status,
            metadata_json=payload.metadata_json,
        )
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    return {"place": place}


@app.get("/places/{place_id}")
def current_user_place_detail(request: Request, place_id: UUID) -> dict:
    auth_session = _require_authenticated_sensor_user(request, write=False)
    user = auth_session.get("user") or {}
    public_tenant = settings.agent_public_tenant_id or "shared"
    user_id = str(user.get("user_id") or "").strip()
    place = repository.get_user_place(str(place_id), tenant_id=public_tenant, auth_user_id=user_id)
    if place is None:
        raise HTTPException(status_code=404, detail="Place not found")
    return {"place": place}


@app.get("/places/{place_id}/hives")
def list_current_user_hives_for_place(
    request: Request,
    place_id: UUID,
    status: str | None = None,
    limit: int = Query(default=100, ge=1, le=500),
    offset: int = Query(default=0, ge=0),
) -> dict:
    auth_session = _require_authenticated_sensor_user(request, write=False)
    user = auth_session.get("user") or {}
    public_tenant = settings.agent_public_tenant_id or "shared"
    user_id = str(user.get("user_id") or "").strip()
    if repository.get_user_place(str(place_id), tenant_id=public_tenant, auth_user_id=user_id) is None:
        raise HTTPException(status_code=404, detail="Place not found")
    try:
        return {
            "items": repository.list_user_hives(
                tenant_id=public_tenant,
                auth_user_id=user_id,
                status=status,
                place_id=str(place_id),
                limit=limit,
                offset=offset,
            ),
            "total": repository.count_user_hives(
                tenant_id=public_tenant,
                auth_user_id=user_id,
                status=status,
                place_id=str(place_id),
            ),
            "limit": limit,
            "offset": offset,
        }
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@app.get("/hives")
def list_current_user_hives(
    request: Request,
    status: str | None = None,
    place_id: UUID | None = None,
    limit: int = Query(default=100, ge=1, le=500),
    offset: int = Query(default=0, ge=0),
) -> dict:
    auth_session = _require_authenticated_sensor_user(request, write=False)
    user = auth_session.get("user") or {}
    public_tenant = settings.agent_public_tenant_id or "shared"
    user_id = str(user.get("user_id") or "").strip()
    _enforce_rate_limit(
        request,
        bucket="sensor-read",
        limit=settings.public_agent_rate_limit_max_requests,
        window_seconds=settings.public_agent_rate_limit_window_seconds,
        subject=user_id,
    )
    if place_id and repository.get_user_place(str(place_id), tenant_id=public_tenant, auth_user_id=user_id) is None:
        raise HTTPException(status_code=404, detail="Place not found")
    try:
        return {
            "items": repository.list_user_hives(
                tenant_id=public_tenant,
                auth_user_id=user_id,
                status=status,
                place_id=str(place_id) if place_id else None,
                limit=limit,
                offset=offset,
            ),
            "total": repository.count_user_hives(
                tenant_id=public_tenant,
                auth_user_id=user_id,
                status=status,
                place_id=str(place_id) if place_id else None,
            ),
            "limit": limit,
            "offset": offset,
        }
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@app.post("/hives")
def upsert_current_user_hive(request: Request, payload: UserHiveUpsertRequest) -> dict:
    auth_session = _require_authenticated_sensor_user(request, write=True)
    user = auth_session.get("user") or {}
    public_tenant = settings.agent_public_tenant_id or "shared"
    user_id = str(user.get("user_id") or "").strip()
    _enforce_rate_limit(
        request,
        bucket="sensor-write",
        limit=max(10, settings.public_agent_rate_limit_max_requests // 2),
        window_seconds=settings.public_agent_rate_limit_window_seconds,
        subject=user_id,
    )
    try:
        hive = repository.upsert_user_hive(
            tenant_id=public_tenant,
            auth_user_id=user_id,
            external_hive_id=payload.external_hive_id,
            hive_name=payload.hive_name,
            place_id=str(payload.place_id) if payload.place_id else None,
            status=payload.status,
            metadata_json=payload.metadata_json,
        )
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    return {"hive": hive}


@app.get("/hives/{hive_id}")
def current_user_hive_detail(request: Request, hive_id: UUID) -> dict:
    auth_session = _require_authenticated_sensor_user(request, write=False)
    user = auth_session.get("user") or {}
    public_tenant = settings.agent_public_tenant_id or "shared"
    user_id = str(user.get("user_id") or "").strip()
    hive = repository.get_user_hive(str(hive_id), tenant_id=public_tenant, auth_user_id=user_id)
    if hive is None:
        raise HTTPException(status_code=404, detail="Hive not found")
    return {"hive": hive}


@app.post("/sensors")
def upsert_current_user_sensor(request: Request, payload: UserSensorUpsertRequest) -> dict:
    auth_session = _require_authenticated_sensor_user(request, write=True)
    user = auth_session.get("user") or {}
    public_tenant = settings.agent_public_tenant_id or "shared"
    user_id = str(user.get("user_id") or "").strip()
    _enforce_rate_limit(
        request,
        bucket="sensor-write",
        limit=max(10, settings.public_agent_rate_limit_max_requests // 2),
        window_seconds=settings.public_agent_rate_limit_window_seconds,
        subject=user_id,
    )
    try:
        sensor = repository.upsert_user_sensor(
            tenant_id=public_tenant,
            auth_user_id=user_id,
            external_sensor_id=payload.external_sensor_id,
            sensor_name=payload.sensor_name,
            sensor_type=payload.sensor_type,
            place_id=str(payload.place_id) if payload.place_id else None,
            hive_id=str(payload.hive_id) if payload.hive_id else None,
            hive_name=payload.hive_name,
            location_label=payload.location_label,
            status=payload.status,
            metadata_json=payload.metadata_json,
        )
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    return {"sensor": sensor}


@app.get("/sensors/context")
def current_user_sensor_context(
    request: Request,
    question: str = Query(..., min_length=1),
    limit: int = Query(default=8, ge=1, le=50),
    hours: int = Query(default=72, ge=1, le=24 * 30),
    points_per_metric: int = Query(default=6, ge=1, le=24),
) -> dict:
    auth_session = _require_authenticated_sensor_user(request, write=False)
    user = auth_session.get("user") or {}
    public_tenant = settings.agent_public_tenant_id or "shared"
    user_id = str(user.get("user_id") or "").strip()
    _enforce_rate_limit(
        request,
        bucket="sensor-read",
        limit=settings.public_agent_rate_limit_max_requests,
        window_seconds=settings.public_agent_rate_limit_window_seconds,
        subject=user_id,
    )
    rows = repository.build_user_sensor_context(
        tenant_id=public_tenant,
        auth_user_id=user_id,
        normalized_query=question,
        max_rows=limit,
        hours=hours,
        points_per_metric=points_per_metric,
    )
    return {"items": _json_safe(rows), "total": len(rows)}


@app.get("/sensors/{sensor_id}")
def current_user_sensor_detail(request: Request, sensor_id: UUID) -> dict:
    auth_session = _require_authenticated_sensor_user(request, write=False)
    user = auth_session.get("user") or {}
    public_tenant = settings.agent_public_tenant_id or "shared"
    user_id = str(user.get("user_id") or "").strip()
    sensor = repository.get_user_sensor(str(sensor_id), tenant_id=public_tenant, auth_user_id=user_id)
    if sensor is None:
        raise HTTPException(status_code=404, detail="Sensor not found")
    return {"sensor": sensor}


@app.get("/sensors/{sensor_id}/readings")
def current_user_sensor_readings(
    request: Request,
    sensor_id: UUID,
    metric_name: str | None = None,
    start_at: datetime | None = None,
    end_at: datetime | None = None,
    limit: int = Query(default=200, ge=1, le=1000),
    offset: int = Query(default=0, ge=0),
) -> dict:
    auth_session = _require_authenticated_sensor_user(request, write=False)
    user = auth_session.get("user") or {}
    public_tenant = settings.agent_public_tenant_id or "shared"
    user_id = str(user.get("user_id") or "").strip()
    if repository.get_user_sensor(str(sensor_id), tenant_id=public_tenant, auth_user_id=user_id) is None:
        raise HTTPException(status_code=404, detail="Sensor not found")
    readings = repository.list_sensor_readings(
        tenant_id=public_tenant,
        auth_user_id=user_id,
        sensor_id=str(sensor_id),
        metric_name=metric_name,
        start_at=start_at,
        end_at=end_at,
        limit=limit,
        offset=offset,
    )
    return {"items": _json_safe(readings), "limit": limit, "offset": offset}


@app.post("/sensors/{sensor_id}/readings")
def ingest_current_user_sensor_readings(
    request: Request,
    sensor_id: UUID,
    payload: SensorReadingsIngestRequest,
) -> dict:
    auth_session = _require_authenticated_sensor_user(request, write=True)
    user = auth_session.get("user") or {}
    public_tenant = settings.agent_public_tenant_id or "shared"
    user_id = str(user.get("user_id") or "").strip()
    _enforce_rate_limit(
        request,
        bucket="sensor-write",
        limit=max(10, settings.public_agent_rate_limit_max_requests // 2),
        window_seconds=settings.public_agent_rate_limit_window_seconds,
        subject=user_id,
    )
    try:
        inserted = repository.save_sensor_readings(
            sensor_id=str(sensor_id),
            tenant_id=public_tenant,
            auth_user_id=user_id,
            readings=[item.model_dump() for item in payload.readings],
        )
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    return {"items": _json_safe(inserted), "count": len(inserted)}


@app.get("/admin/api/chunks")
def admin_chunks(
    document_id: str | None = None,
    status: str | None = None,
    limit: int = Query(default=100, ge=1, le=250),
    offset: int = Query(default=0, ge=0),
) -> dict:
    return {
        "items": repository.list_chunks(document_id=document_id, status=status, limit=limit, offset=offset),
        "total": repository.count_chunks(document_id=document_id, status=status),
        "limit": limit,
        "offset": offset,
    }


@app.post("/admin/api/chunks/{chunk_id}/decision")
def admin_chunk_decision(chunk_id: str, request: ChunkDecisionRequest) -> dict:
    try:
        return service.review_chunk_decision(chunk_id, request.action)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@app.get("/admin/api/chunks/{chunk_id}")
def admin_chunk_detail(chunk_id: str) -> dict:
    detail = repository.get_chunk_detail(chunk_id)
    if detail is None:
        raise HTTPException(status_code=404, detail="Chunk not found")

    chroma_record = None
    chroma_error = None
    try:
        chroma_record = chroma_store.get_record(chunk_id)
    except Exception as exc:  # pragma: no cover - defensive admin path
        chroma_error = str(exc)

    return {
        **detail,
        "chroma_record": chroma_record,
        "chroma_error": chroma_error,
    }


@app.get("/admin/api/assets/{asset_id}")
def admin_asset_detail(asset_id: str) -> dict:
    detail = repository.get_page_asset_detail(asset_id)
    if detail is None:
        raise HTTPException(status_code=404, detail="Asset not found")
    chroma_record = None
    chroma_error = None
    try:
        chroma_record = chroma_store.get_asset_record(asset_id)
    except Exception as exc:  # pragma: no cover - defensive admin path
        chroma_error = str(exc)
    return {**detail, "chroma_record": chroma_record, "chroma_error": chroma_error}


@app.get("/admin/api/assets/{asset_id}/image")
def admin_asset_image(asset_id: str):
    detail = repository.get_page_asset_detail(asset_id)
    if detail is None:
        raise HTTPException(status_code=404, detail="Asset not found")
    asset_path = _safe_page_asset_path(str(detail["asset"].get("asset_path") or ""))
    return FileResponse(asset_path)


@app.get("/agent/assets/{asset_id}/image")
def public_agent_asset_image(asset_id: str, request: Request):
    _require_authenticated_public_user(request)
    detail = repository.get_page_asset_detail(asset_id)
    if detail is None:
        raise HTTPException(status_code=404, detail="Asset not found")
    asset = detail.get("asset") or {}
    if str(asset.get("tenant_id") or "") != (settings.agent_public_tenant_id or "shared"):
        raise HTTPException(status_code=404, detail="Asset not available")
    asset_path = _safe_page_asset_path(str(asset.get("asset_path") or ""))
    return FileResponse(asset_path)


@app.post("/admin/api/chunks/review/auto")
def admin_auto_review_chunks(request: ReviewBatchRequest) -> dict:
    return service.auto_review_chunks(
        document_id=request.document_id,
        batch_size=max(1, min(request.batch_size, 500)),
    )


@app.post("/admin/api/documents/{document_id}/revalidate")
def admin_revalidate_document(document_id: str, request: RevalidateDocumentRequest) -> dict:
    try:
        return service.revalidate_document(document_id=document_id, rerun_kg=request.rerun_kg)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@app.post("/admin/api/documents/{document_id}/rebuild")
def admin_rebuild_document(document_id: str) -> dict:
    try:
        return service.rebuild_document(document_id=document_id)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@app.post("/admin/api/documents/{document_id}/reindex")
def admin_reindex_document(document_id: str) -> dict:
    try:
        return service.reindex_document(document_id=document_id)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@app.post("/admin/api/documents/{document_id}/reprocess-kg")
def admin_reprocess_document_kg(document_id: str, request: ReviewBatchRequest) -> dict:
    return service.reprocess_kg(document_id=document_id, batch_size=max(1, min(request.batch_size, 500)))


@app.post("/admin/api/documents/{document_id}/delete")
def admin_delete_document(document_id: str) -> dict:
    try:
        return service.delete_document(document_id=document_id)
    except ValueError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc


@app.get("/admin/api/kg/entities")
def admin_kg_entities(
    document_id: str | None = None,
    search: str | None = None,
    entity_type: str | None = None,
    limit: int = Query(default=100, ge=1, le=250),
    offset: int = Query(default=0, ge=0),
) -> dict:
    return {
        "items": repository.list_kg_entities(document_id=document_id, search=search, entity_type=entity_type, limit=limit, offset=offset),
        "total": repository.count_kg_entities(document_id=document_id, search=search, entity_type=entity_type),
        "limit": limit,
        "offset": offset,
    }


@app.get("/admin/api/kg/entities/{entity_id}")
def admin_kg_entity_detail(entity_id: str) -> dict:
    detail = repository.get_kg_entity_detail(entity_id)
    if detail is None:
        raise HTTPException(status_code=404, detail="KG entity not found")
    return detail


@app.get("/admin/api/kg/assertions")
def admin_kg_assertions(
    document_id: str | None = None,
    entity_id: str | None = None,
    predicate: str | None = None,
    status: str | None = None,
    chunk_id: str | None = None,
    limit: int = Query(default=100, ge=1, le=250),
    offset: int = Query(default=0, ge=0),
) -> dict:
    return {
        "items": repository.list_kg_assertions(
            document_id=document_id,
            entity_id=entity_id,
            predicate=predicate,
            status=status,
            chunk_id=chunk_id,
            limit=limit,
            offset=offset,
        ),
        "total": repository.count_kg_assertions(
            document_id=document_id,
            entity_id=entity_id,
            predicate=predicate,
            status=status,
            chunk_id=chunk_id,
        ),
        "limit": limit,
        "offset": offset,
    }


@app.get("/admin/api/kg/raw")
def admin_kg_raw(
    document_id: str | None = None,
    chunk_id: str | None = None,
    status: str | None = None,
    limit: int = Query(default=100, ge=1, le=250),
    offset: int = Query(default=0, ge=0),
) -> dict:
    return {
        "items": repository.list_kg_raw_extractions(document_id=document_id, chunk_id=chunk_id, status=status, limit=limit, offset=offset),
        "total": repository.count_kg_raw_extractions(document_id=document_id, chunk_id=chunk_id, status=status),
        "limit": limit,
        "offset": offset,
    }


@app.get("/admin/api/chroma/collections")
def admin_chroma_collections() -> list[dict]:
    collections = []
    chunk_collection_name = chroma_store.collection.name
    asset_collection_name = chroma_store.asset_collection.name
    for collection in chroma_store.list_collections():
        target = chroma_store.client.get_collection(collection.name)
        collections.append(
            {
                "name": collection.name,
                "count": target.count(),
                "metadata": getattr(target, "metadata", None) or getattr(collection, "metadata", None) or {},
                "is_default_chunk": collection.name == chunk_collection_name,
                "is_default_asset": collection.name == asset_collection_name,
            }
        )
    return collections


@app.get("/admin/api/chroma/records")
def admin_chroma_records(
    collection_name: str | None = None,
    document_id: str | None = None,
    limit: int = Query(default=50, ge=1, le=250),
    offset: int = Query(default=0, ge=0),
) -> dict:
    allowed = {collection.name for collection in chroma_store.list_collections()}
    target_collection = collection_name or chroma_store.collection.name
    if target_collection not in allowed:
        raise HTTPException(status_code=400, detail="Unknown Chroma collection")
    payload = _get_chroma_payload(document_id=document_id, limit=limit, offset=offset, collection_name=target_collection)
    return {"items": payload["records"], "total": payload["total"], "limit": limit, "offset": offset, "error": payload["error"]}


@app.get("/admin/api/metadata/chunks")
def admin_chunk_metadata(
    document_id: str | None = None,
    status: str | None = None,
    limit: int = Query(default=100, ge=1, le=250),
    offset: int = Query(default=0, ge=0),
) -> dict:
    return {
        "items": repository.list_chunk_metadata(document_id=document_id, status=status, limit=limit, offset=offset),
        "total": repository.count_chunk_metadata(document_id=document_id, status=status),
        "limit": limit,
        "offset": offset,
    }


@app.get("/admin/api/chroma/parity")
def admin_chroma_parity(document_id: str | None = None) -> dict:
    return _get_chroma_parity(document_id=document_id)


@app.post("/admin/api/retrieval/evaluate")
def admin_run_retrieval_eval(request: RetrievalEvalRequest) -> dict:
    queries_file = _resolve_evaluation_path(request.queries_file, must_exist=True)
    output = _resolve_evaluation_path(request.output)
    output.parent.mkdir(parents=True, exist_ok=True)
    return run_retrieval_evaluation(
        queries_file=str(queries_file),
        tenant_id=request.tenant_id,
        top_k=max(1, min(request.top_k, 20)),
        output=str(output),
    )


@app.get("/admin/api/retrieval/evaluation")
def admin_get_retrieval_eval(path: str = Query(default="data/evaluation/latest-admin-eval.json")) -> dict:
    try:
        return read_retrieval_evaluation(str(_resolve_evaluation_path(path, must_exist=True)))
    except FileNotFoundError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc


@app.post("/admin/api/retrieval/inspect")
def admin_inspect_retrieval(request: RetrievalInspectRequest) -> dict:
    try:
        return agent_service.inspect_retrieval(
            question=request.question,
            tenant_id=request.tenant_id,
            document_ids=request.document_ids,
            top_k=request.top_k,
            query_mode=request.query_mode,
            trusted_tenant=True,
        )
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@app.post("/admin/api/agent/evaluate")
def admin_run_agent_eval(request: AgentEvalRequest) -> dict:
    queries_file = _resolve_evaluation_path(request.queries_file, must_exist=True)
    output = _resolve_evaluation_path(request.output)
    output.parent.mkdir(parents=True, exist_ok=True)
    return run_agent_evaluation(
        queries_file=str(queries_file),
        tenant_id=request.tenant_id,
        top_k=max(1, min(request.top_k, 20)),
        output=str(output),
    )


@app.get("/admin/api/agent/evaluation")
def admin_get_agent_eval(path: str = Query(default="data/evaluation/latest-agent-eval.json")) -> dict:
    try:
        return read_agent_evaluation(str(_resolve_evaluation_path(path, must_exist=True)))
    except FileNotFoundError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc


@app.post("/admin/api/reset")
def admin_reset_pipeline() -> dict:
    _stop_detached_reingest_runner()
    return service.reset_pipeline_data()


@app.post("/agent/query")
def agent_query(request: Request, payload: AgentQueryRequest) -> JSONResponse:
    try:
        auth_session = (
            _require_authenticated_sensor_user(request, write=False)
            if payload.query_mode == "sensor"
            else _require_authenticated_public_user(request)
        )
        user = auth_session.get("user") or {}
        _enforce_rate_limit(
            request,
            bucket="public-agent",
            limit=settings.public_agent_rate_limit_max_requests,
            window_seconds=settings.public_agent_rate_limit_window_seconds,
            subject=str(user.get("user_id") or ""),
        )
        public_tenant = settings.agent_public_tenant_id or "shared"
        if str(payload.tenant_id or public_tenant).strip() != public_tenant:
            raise HTTPException(status_code=400, detail="Public agent tenant_id is fixed")
        session_id, session_token = _resolve_agent_session_credentials(request, payload)
        profile_id, profile_token = _resolve_agent_profile_credentials(request)
        result = agent_service.query(
            question=payload.question,
            session_id=session_id,
            session_token=session_token,
            profile_id=profile_id,
            profile_token=profile_token,
            auth_user_id=str(user.get("user_id") or "").strip() or None,
            tenant_id=public_tenant,
            document_ids=payload.document_ids,
            top_k=payload.top_k,
            query_mode=_resolve_public_query_mode(auth_session, payload.query_mode),
            workspace_kind=payload.workspace_kind,
            trusted_tenant=False,
        )
        response = JSONResponse(_json_safe(result))
        _apply_agent_session_cookies(response, result)
        _apply_agent_profile_cookies(response, result)
        return response
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    except AgentQueryError as exc:
        raise HTTPException(status_code=502, detail=str(exc)) from exc


@app.post("/agent/chat")
def agent_chat(request: Request, payload: AgentQueryRequest) -> JSONResponse:
    try:
        auth_session = (
            _require_authenticated_sensor_user(request, write=False)
            if payload.query_mode == "sensor"
            else _require_authenticated_public_user(request)
        )
        user = auth_session.get("user") or {}
        _enforce_rate_limit(
            request,
            bucket="public-agent",
            limit=settings.public_agent_rate_limit_max_requests,
            window_seconds=settings.public_agent_rate_limit_window_seconds,
            subject=str(user.get("user_id") or ""),
        )
        public_tenant = settings.agent_public_tenant_id or "shared"
        if str(payload.tenant_id or public_tenant).strip() != public_tenant:
            raise HTTPException(status_code=400, detail="Public agent tenant_id is fixed")
        session_id, session_token = _resolve_agent_session_credentials(request, payload)
        profile_id, profile_token = _resolve_agent_profile_credentials(request)
        result = agent_service.chat(
            question=payload.question,
            session_id=session_id,
            session_token=session_token,
            profile_id=profile_id,
            profile_token=profile_token,
            auth_user_id=str(user.get("user_id") or "").strip() or None,
            tenant_id=public_tenant,
            document_ids=payload.document_ids,
            top_k=payload.top_k,
            query_mode=_resolve_public_query_mode(auth_session, payload.query_mode),
            workspace_kind=payload.workspace_kind,
            trusted_tenant=False,
        )
        response = JSONResponse(_json_safe(result))
        _apply_agent_session_cookies(response, result)
        _apply_agent_profile_cookies(response, result)
        return response
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    except AgentQueryError as exc:
        raise HTTPException(status_code=502, detail=str(exc)) from exc


@app.post("/ingest/text")
def ingest_text(request: IngestRequest) -> dict:
    return service.ingest_text(
        SourceDocument(
            tenant_id=request.tenant_id,
            source_type=request.source_type,
            filename=request.filename,
            raw_text=request.raw_text,
            document_class=request.document_class,
            parser_version=request.parser_version,
            ocr_engine=request.ocr_engine,
            ocr_model=request.ocr_model,
        )
    )


@app.post("/ingest/pdf")
def ingest_pdf(request: IngestPdfRequest) -> dict:
    try:
        source_path = _resolve_workspace_pdf_path(request.path)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    if source_path.suffix.lower() != ".pdf":
        raise HTTPException(status_code=400, detail="PDF ingest requires a .pdf file")
    if not source_path.exists():
        raise HTTPException(status_code=404, detail="PDF file not found")

    metadata = {"source_path": str(source_path)}
    if request.page_start is not None or request.page_end is not None:
        metadata["page_range"] = {"start": request.page_start, "end": request.page_end}

    # PDF ingest is normalized into the same text-ingest pipeline after extraction so
    # downstream stages do not care where the text originated. The actual PDF parsing
    # happens inside the ingestion service so we do not pay the PDF-open/render cost twice.
    return service.ingest_text(
        SourceDocument(
            tenant_id=request.tenant_id,
            source_type="pdf",
            filename=request.filename or PureWindowsPath(request.path).name,
            raw_text="",
            metadata=metadata,
            document_class=request.document_class,
            parser_version=request.parser_version,
            content_hash_value=_build_pdf_content_hash(source_path, request.page_start, request.page_end),
        )
    )


@app.post("/admin/api/uploads/ingest")
async def admin_upload_and_ingest(
    file: UploadFile = File(...),
    tenant_id: str = Form(default="shared"),
    document_class: str = Form(default="book"),
    parser_version: str = Form(default="v1"),
    source_type: str | None = Form(default=None),
    filename: str | None = Form(default=None),
    page_start: int | None = Form(default=None),
    page_end: int | None = Form(default=None),
) -> dict:
    # Upload-and-ingest keeps uploaded files inside the workspace, then dispatches
    # into the regular ingestion service so uploads and path-based ingests behave the same.
    safe_name = _sanitize_upload_filename(filename or file.filename, fallback="upload.bin")
    suffix = Path(safe_name).suffix.lower()
    resolved_source_type = (source_type or ("pdf" if suffix == ".pdf" else "text")).strip().lower()
    if resolved_source_type not in {"pdf", "text"}:
        raise HTTPException(status_code=400, detail="Upload ingest only supports pdf or text source types")
    if resolved_source_type == "pdf" and suffix != ".pdf":
        raise HTTPException(status_code=400, detail="PDF upload ingest requires a .pdf file")

    upload_dir = _workspace_upload_dir()
    stored_name = f"{uuid4()}-{safe_name}"
    stored_path = upload_dir / stored_name
    total_bytes = 0
    with stored_path.open("wb") as handle:
        while True:
            chunk = await file.read(1024 * 1024)
            if not chunk:
                break
            total_bytes += len(chunk)
            if total_bytes > settings.upload_max_bytes:
                handle.close()
                stored_path.unlink(missing_ok=True)
                raise HTTPException(status_code=413, detail="Upload exceeds configured size limit")
            handle.write(chunk)
    await file.close()

    metadata = {
        "uploaded_filename": safe_name,
        "uploaded_path": str(stored_path),
        "source_path": str(stored_path),
        "uploaded_size_bytes": total_bytes,
    }

    if resolved_source_type == "pdf":
        raw_text = ""
        if page_start is not None or page_end is not None:
            metadata["page_range"] = {"start": page_start, "end": page_end}
    else:
        try:
            raw_text = stored_path.read_text(encoding="utf-8")
        except UnicodeDecodeError:
            raw_text = stored_path.read_text(encoding="utf-8", errors="replace")
            metadata["encoding_warning"] = "utf8_decode_replaced_invalid_bytes"

    result = service.ingest_text(
        SourceDocument(
            tenant_id=tenant_id,
            source_type=resolved_source_type,
            filename=safe_name,
            raw_text=raw_text,
            metadata=metadata,
            document_class=document_class,
            parser_version=parser_version,
            content_hash_value=(
                _build_pdf_content_hash(stored_path, page_start, page_end)
                if resolved_source_type == "pdf"
                else None
            ),
        )
    )
    return {
        "stored_path": str(stored_path),
        "filename": safe_name,
        "source_type": resolved_source_type,
        "ingestion": result,
    }
