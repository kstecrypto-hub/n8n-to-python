from __future__ import annotations

from pathlib import Path
import ipaddress
from urllib.parse import urlparse

from pydantic import Field, field_validator, model_validator
from pydantic_settings import BaseSettings, SettingsConfigDict


_WORKSPACE_ROOT = Path(__file__).resolve().parents[2]
_DEFAULT_MODEL_HOST_ALLOWLIST = "api.openai.com"


def workspace_root() -> Path:
    return _WORKSPACE_ROOT


def resolve_workspace_path(raw_path: str | Path) -> str:
    candidate = Path(raw_path)
    if not candidate.is_absolute():
        candidate = (_WORKSPACE_ROOT / candidate).resolve()
    else:
        candidate = candidate.resolve()
    return str(candidate)


def parse_host_allowlist(raw_hosts: str | None) -> set[str]:
    return {
        host.strip().lower()
        for host in str(raw_hosts or _DEFAULT_MODEL_HOST_ALLOWLIST).split(",")
        if host.strip()
    }


def _is_private_hostname(hostname: str) -> bool:
    normalized = hostname.strip().strip("[]").lower()
    if not normalized:
        return True
    if normalized in {"localhost", "localhost.localdomain"}:
        return True
    try:
        ip = ipaddress.ip_address(normalized)
    except ValueError:
        return normalized.endswith(".local")
    return (
        ip.is_private
        or ip.is_loopback
        or ip.is_link_local
        or ip.is_reserved
        or ip.is_multicast
        or ip.is_unspecified
    )


def normalize_outbound_base_url(
    value: str | None,
    *,
    field_name: str,
    allowed_hosts: set[str] | None = None,
    allow_private_hosts: bool = False,
) -> str:
    raw_value = str(value or "").strip()
    if not raw_value:
        raise ValueError(f"{field_name} must not be empty")
    parsed = urlparse(raw_value)
    if parsed.scheme.lower() != "https":
        raise ValueError(f"{field_name} must use https")
    if parsed.username or parsed.password:
        raise ValueError(f"{field_name} must not include embedded credentials")
    if parsed.query or parsed.fragment:
        raise ValueError(f"{field_name} must not include query or fragment components")
    hostname = (parsed.hostname or "").strip().lower()
    if not hostname:
        raise ValueError(f"{field_name} must include a hostname")
    if not allow_private_hosts and _is_private_hostname(hostname):
        raise ValueError(f"{field_name} must not target localhost or private network hosts")
    host_allowlist = allowed_hosts or set()
    if host_allowlist and not any(hostname == host or hostname.endswith(f".{host}") for host in host_allowlist):
        raise ValueError(f"{field_name} host '{hostname}' is not in the allowlist")
    port = f":{parsed.port}" if parsed.port is not None else ""
    path = parsed.path.rstrip("/")
    return f"https://{hostname}{port}{path}"


def normalize_provider_choice(value: str | None, *, allowed: set[str], default: str) -> str:
    normalized = str(value or default).strip().lower()
    if not normalized:
        normalized = default
    if normalized not in allowed:
        raise ValueError(f"Unsupported provider '{normalized}'")
    return normalized


class Settings(BaseSettings):
    app_env: str = "development"
    api_host: str = "0.0.0.0"
    api_port: int = 8000
    admin_api_token: str | None = None
    auth_postgres_db: str | None = None
    auth_postgres_user: str | None = None
    auth_postgres_password: str | None = None
    auth_postgres_dsn: str | None = None
    auth_postgres_schema: str = "auth"
    auth_legacy_sqlite_path: str = str(Path("data/identity/public_auth.sqlite3"))
    auth_cookie_secure: bool = False
    browser_origin_allowlist: str = ""
    auth_session_max_age_seconds: int = Field(default=14 * 24 * 60 * 60, ge=300, le=365 * 24 * 60 * 60)
    auth_public_registration_enabled: bool = False
    auth_password_min_length: int = Field(default=12, ge=8, le=128)
    auth_login_rate_limit_window_seconds: int = Field(default=5 * 60, ge=1, le=24 * 60 * 60)
    auth_login_rate_limit_max_attempts: int = Field(default=10, ge=1, le=10_000)
    public_agent_rate_limit_window_seconds: int = Field(default=60, ge=1, le=24 * 60 * 60)
    public_agent_rate_limit_max_requests: int = Field(default=30, ge=1, le=10_000)
    runtime_secret_encryption_key: str | None = None
    postgres_dsn: str = "postgresql://bee:bee@localhost:5432/bee_ingestion"
    chroma_host: str | None = None
    chroma_port: int = 8000
    chroma_ssl: bool = False
    chroma_path: str = str(Path("data/chroma"))
    chroma_collection: str = "kb_chunks_t3large_v1"
    chroma_asset_collection: str = "kb_assets_v1"
    chroma_upsert_batch_size: int = 64
    allow_private_model_hosts: bool = False
    model_host_allowlist: str = _DEFAULT_MODEL_HOST_ALLOWLIST
    embedding_provider: str = "dummy"
    embedding_base_url: str = "https://api.openai.com/v1"
    embedding_api_key: str | None = None
    embedding_model: str = "text-embedding-3-large"
    embedding_batch_size: int = Field(default=32, ge=1, le=256)
    embedding_timeout_seconds: float = Field(default=120.0, gt=0.0, le=300.0)
    embedding_max_retries: int = Field(default=3, ge=0, le=10)
    embedding_retry_backoff_seconds: float = Field(default=1.5, gt=0.0, le=30.0)
    asset_embedding_min_chars: int = Field(default=24, ge=0, le=2048)
    extractor_version: str = "pymupdf_v1"
    normalizer_version: str = "v1"
    chunker_version: str = "v1"
    validator_version: str = "v1"
    synopsis_version: str = "extractive-v1"
    worker_version: str = "bee_ingestion_worker_v1"
    job_lease_seconds: int = Field(default=300, ge=15, le=3600)
    kg_ontology_path: str = str(Path("data/beecore.ttl"))
    kg_min_confidence: float = Field(default=0.7, ge=0.0, le=1.0)
    kg_extraction_provider: str = "auto"
    kg_base_url: str = "https://api.openai.com/v1"
    kg_api_key: str | None = None
    kg_model: str = "gpt-5-mini"
    kg_reasoning_effort: str = "minimal"
    kg_prompt_version: str = "v2"
    kg_timeout_seconds: float = Field(default=90.0, gt=0.0, le=300.0)
    kg_max_retries: int = Field(default=2, ge=0, le=10)
    kg_retry_backoff_seconds: float = Field(default=2.0, gt=0.0, le=30.0)
    review_provider: str = "auto"
    review_base_url: str = "https://api.openai.com/v1"
    review_api_key: str | None = None
    review_model: str = "gpt-5-mini"
    review_prompt_version: str = "v1"
    review_min_confidence: float = Field(default=0.65, ge=0.0, le=1.0)
    review_timeout_seconds: float = Field(default=60.0, gt=0.0, le=300.0)
    vision_enabled: bool = True
    vision_base_url: str = "https://api.openai.com/v1"
    vision_api_key: str | None = None
    vision_model: str = "gpt-5-mini"
    vision_reasoning_effort: str = "minimal"
    vision_prompt_version: str = "v1"
    vision_timeout_seconds: float = Field(default=90.0, gt=0.0, le=300.0)
    vision_max_retries: int = Field(default=1, ge=0, le=10)
    vision_retry_backoff_seconds: float = Field(default=1.5, gt=0.0, le=30.0)
    vision_page_min_chars: int = Field(default=700, ge=0, le=20000)
    vision_max_assets_per_page: int = Field(default=4, ge=0, le=32)
    vision_page_render_dpi: int = Field(default=110, ge=36, le=300)
    vision_asset_render_dpi: int = Field(default=150, ge=36, le=300)
    agent_provider: str = "openai"
    agent_base_url: str = "https://api.openai.com/v1"
    agent_api_key: str | None = None
    agent_public_tenant_id: str = "shared"
    agent_model: str = "gpt-5-mini"
    agent_reasoning_effort: str = "low"
    agent_fallback_model: str = "gpt-5-mini"
    agent_fallback_reasoning_effort: str = "low"
    agent_prompt_version: str = "v2"
    agent_router_enabled: bool = True
    agent_router_provider: str = "openai"
    agent_router_base_url: str = "https://api.openai.com/v1"
    agent_router_model: str = "gpt-5.4-nano"
    agent_router_reasoning_effort: str = "none"
    agent_router_prompt_version: str = "v1"
    agent_router_system_prompt: str = (
        "You are a retrieval router for a read-only beekeeping QA agent.\n"
        "Classify the user's question for retrieval planning only.\n"
        "Return strict JSON only.\n"
        "Choose the smallest top_k that is still likely to retrieve enough evidence.\n"
        "Mark requires_visual true when the answer likely depends on figures, diagrams, scans, plates, or image content.\n"
        "Use the allowed question types exactly: definition, fact, source_lookup, procedure, comparison, explanation, visual_lookup."
    )
    agent_router_temperature: float = Field(default=0.0, ge=0.0, le=2.0)
    agent_router_max_completion_tokens: int = Field(default=128, ge=16, le=1024)
    agent_router_timeout_seconds: float = Field(default=20.0, gt=0.0, le=120.0)
    agent_router_confidence_threshold: float = Field(default=0.55, ge=0.0, le=1.0)
    agent_router_cache_enabled: bool = True
    agent_router_cache_max_age_seconds: int = Field(default=7 * 24 * 60 * 60, ge=60, le=30 * 24 * 60 * 60)
    agent_embedding_cache_enabled: bool = True
    agent_embedding_cache_max_age_seconds: int = Field(default=30 * 24 * 60 * 60, ge=60, le=90 * 24 * 60 * 60)
    agent_memory_enabled: bool = True
    agent_memory_provider: str = "openai"
    agent_memory_base_url: str = "https://api.openai.com/v1"
    agent_memory_model: str = "gpt-5.4-nano"
    agent_memory_reasoning_effort: str = "none"
    agent_memory_prompt_version: str = "v1"
    agent_memory_system_prompt: str = (
        "You maintain structured session memory for a read-only beekeeping QA agent.\n"
        "Compress the session into durable goals, constraints, evidence-backed facts, user preferences, topic keywords, resolved threads, and open threads.\n"
        "Prefer preserving explicit user constraints and evidence-backed facts over conversational filler.\n"
        "Return strict JSON only.\n"
        "Do not invent facts that were not supported by the cited evidence in the latest turn."
    )
    agent_memory_temperature: float = Field(default=0.0, ge=0.0, le=2.0)
    agent_memory_max_completion_tokens: int = Field(default=400, ge=64, le=2048)
    agent_memory_timeout_seconds: float = Field(default=20.0, gt=0.0, le=120.0)
    agent_memory_char_budget: int = Field(default=2200, ge=200, le=12000)
    agent_memory_max_facts: int = Field(default=6, ge=1, le=20)
    agent_memory_max_open_threads: int = Field(default=6, ge=0, le=20)
    agent_memory_max_resolved_threads: int = Field(default=6, ge=0, le=20)
    agent_memory_max_preferences: int = Field(default=6, ge=0, le=20)
    agent_memory_max_topics: int = Field(default=8, ge=0, le=32)
    agent_memory_recent_messages: int = Field(default=8, ge=0, le=50)
    agent_system_prompt: str = (
        "You are a read-only domain QA agent over a beekeeping corpus.\n"
        "Answer only from the provided evidence bundle.\n"
        "Do not invent facts.\n"
        "Write like a practical human expert talking to a beekeeper in plain language.\n"
        "Prefer simple wording, short sentences, and direct explanations over academic or robotic phrasing.\n"
        "Do not mention the corpus, evidence bundle, retrieved evidence, or sources unless the user explicitly asks about them.\n"
        "Use the retrieved chunk text and KG support to give a direct answer, not a defensive or hesitant one.\n"
        "Only state a step in a process if the evidence bundle directly supports that step.\n"
        "If some parts are supported and other parts are not, give the supported answer directly and stop there without a disclaimer.\n"
        "Do not hedge with phrases like 'I think', 'I am not sure', 'it seems', or 'I can't be sure' unless the user explicitly asks about uncertainty.\n"
        "Do not add meta-claims such as saying that all points are supported by the cited sources.\n"
        "Do not abstain just because the evidence is thin; give the strongest direct answer the available material supports.\n"
        "Use KG assertions only as support; if they conflict with chunk text, trust the chunk text.\n"
        "Return JSON only and follow the schema exactly."
    )
    agent_open_world_prompt_version: str = "v1"
    agent_open_world_system_prompt: str = (
        "You are a helpful general-answer fallback for a beekeeping assistant.\n"
        "Answer the user's question even when the retrieved corpus evidence is weak, missing, or irrelevant.\n"
        "You may use general world knowledge.\n"
        "Write like a practical human expert talking to an everyday beekeeper or farmer.\n"
        "Use plain language, not academic language, and do not sound like a system message.\n"
        "Sound direct and confident, not hesitant or self-protective.\n"
        "Do not hedge with phrases like 'I think', 'I am not sure', 'it seems', or 'I can't be sure' unless the user explicitly asks about uncertainty.\n"
        "Do not talk about evidence, sources, or provenance unless the user asks.\n"
        "If corpus evidence is included and clearly useful, you may use it, but do not claim unsupported provenance.\n"
        "Do not pretend that general knowledge came from the corpus.\n"
        "Do not abstain unless the question is nonsensical or impossible to answer at all.\n"
        "Return JSON only and follow the schema exactly."
    )
    agent_open_world_temperature: float = Field(default=0.4, ge=0.0, le=2.0)
    agent_open_world_max_completion_tokens: int = Field(default=1200, ge=64, le=8192)
    agent_open_world_timeout_seconds: float = Field(default=60.0, gt=0.0, le=300.0)
    agent_sensor_system_prompt: str = (
        "You are a read-only beekeeping telemetry QA agent.\n"
        "Answer from the provided user-owned sensor evidence first, then use corpus evidence only as secondary support when it is explicitly included.\n"
        "Do not invent readings, trends, timestamps, hive assignments, or operational state.\n"
        "If the sensor evidence is insufficient, abstain.\n"
        "Return JSON only and follow the schema exactly."
    )
    agent_claim_verifier_enabled: bool = True
    agent_claim_verifier_provider: str = "openai"
    agent_claim_verifier_base_url: str = "https://api.openai.com/v1"
    agent_claim_verifier_model: str = "gpt-5.4-nano"
    agent_claim_verifier_reasoning_effort: str = "none"
    agent_claim_verifier_prompt_version: str = "v1"
    agent_claim_verifier_system_prompt: str = (
        "You verify whether an answer is fully supported by a bounded evidence bundle.\n"
        "Break the answer into a small set of atomic claims.\n"
        "Mark each claim supported if either the provided evidence directly supports it or it is standard general domain knowledge consistent with the evidence bundle.\n"
        "Use support_basis='evidence' when the claim is directly grounded in the provided evidence and include the matching evidence_ids.\n"
        "Use support_basis='world_knowledge' only for general background knowledge that does not depend on specific numeric values, dates, source attribution, or document-specific provenance; leave evidence_ids empty in that case.\n"
        "Do not use world knowledge to justify precise numbers, measurements, dates, locations, or source-specific claims.\n"
        "Return strict JSON only.\n"
        "Do not mark a claim supported unless it is clearly grounded by evidence or safe general domain knowledge."
    )
    agent_claim_verifier_temperature: float = Field(default=0.0, ge=0.0, le=2.0)
    agent_claim_verifier_max_completion_tokens: int = Field(default=500, ge=64, le=2048)
    agent_claim_verifier_timeout_seconds: float = Field(default=25.0, gt=0.0, le=120.0)
    agent_claim_verifier_min_supported_ratio: float = Field(default=0.66, ge=0.0, le=1.0)
    agent_temperature: float = Field(default=0.2, ge=0.0, le=2.0)
    agent_max_completion_tokens: int = Field(default=1200, ge=64, le=8192)
    agent_timeout_seconds: float = Field(default=90.0, gt=0.0, le=300.0)
    agent_default_top_k: int = Field(default=8, ge=1, le=64)
    agent_max_top_k: int = Field(default=12, ge=1, le=64)
    agent_max_search_k: int = Field(default=24, ge=1, le=128)
    agent_max_context_chunks: int = Field(default=10, ge=1, le=24)
    agent_max_context_assertions: int = Field(default=12, ge=0, le=48)
    agent_neighbor_window: int = Field(default=1, ge=0, le=4)
    agent_session_lease_seconds: int = Field(default=180, ge=15, le=3600)
    agent_session_token_max_age_seconds: int = Field(default=12 * 60 * 60, ge=60, le=90 * 24 * 60 * 60)
    agent_profile_token_max_age_seconds: int = Field(default=30 * 24 * 60 * 60, ge=60, le=365 * 24 * 60 * 60)
    agent_profile_enabled: bool = True
    agent_profile_provider: str = "openai"
    agent_profile_base_url: str = "https://api.openai.com/v1"
    agent_profile_model: str = "gpt-5.4-nano"
    agent_profile_reasoning_effort: str = "none"
    agent_profile_prompt_version: str = "v1"
    agent_profile_system_prompt: str = (
        "You maintain a stable browser-scoped user profile for a read-only beekeeping QA agent.\n"
        "Keep only durable user background, communication style, answer preferences, recurring interests, learning goals, preferred document scope, and long-lived constraints.\n"
        "Do not copy transient turn-specific details into the profile.\n"
        "Return strict JSON only.\n"
        "Do not invent personal facts that are not present in the prior profile, explicit user text, or recent evidence-backed memory."
    )
    agent_profile_temperature: float = Field(default=0.0, ge=0.0, le=2.0)
    agent_profile_max_completion_tokens: int = Field(default=400, ge=64, le=2048)
    agent_profile_timeout_seconds: float = Field(default=20.0, gt=0.0, le=120.0)
    agent_profile_char_budget: int = Field(default=1800, ge=200, le=12000)
    agent_profile_max_topics: int = Field(default=8, ge=0, le=32)
    agent_profile_max_preferences: int = Field(default=8, ge=0, le=20)
    agent_profile_max_constraints: int = Field(default=8, ge=0, le=20)
    agent_profile_recent_messages: int = Field(default=12, ge=0, le=50)
    agent_min_answer_confidence: float = Field(default=0.55, ge=0.0, le=1.0)
    agent_review_confidence_threshold: float = Field(default=0.72, ge=0.0, le=1.0)
    agent_rerank_distance_weight: float = Field(default=1.0, ge=0.0, le=10.0)
    agent_rerank_lexical_weight: float = Field(default=0.25, ge=0.0, le=10.0)
    agent_rerank_section_weight: float = Field(default=0.1, ge=0.0, le=10.0)
    agent_rerank_title_weight: float = Field(default=0.14, ge=0.0, le=10.0)
    agent_rerank_exact_phrase_weight: float = Field(default=0.22, ge=0.0, le=10.0)
    agent_rerank_ontology_weight: float = Field(default=0.14, ge=0.0, le=10.0)
    agent_diversity_penalty: float = Field(default=0.22, ge=0.0, le=10.0)
    agent_prompt_char_budget: int = Field(default=32000, ge=4000, le=64000)
    agent_history_char_budget: int = Field(default=2800, ge=0, le=12000)
    agent_assertion_char_budget: int = Field(default=4000, ge=0, le=12000)
    agent_entity_char_budget: int = Field(default=1800, ge=0, le=12000)
    agent_kg_search_limit: int = Field(default=6, ge=0, le=32)
    agent_graph_expansion_limit: int = Field(default=8, ge=0, le=32)
    agent_graph_per_entity_limit: int = Field(default=2, ge=0, le=8)
    agent_max_context_graph_chains: int = Field(default=4, ge=0, le=12)
    agent_chunk_char_budget: int = Field(default=14000, ge=0, le=32000)
    agent_max_context_assets: int = Field(default=6, ge=0, le=16)
    agent_max_asset_search_k: int = Field(default=16, ge=0, le=64)
    agent_asset_char_budget: int = Field(default=3200, ge=0, le=16000)
    agent_sensor_context_enabled: bool = True
    agent_max_context_sensor_readings: int = Field(default=8, ge=0, le=32)
    agent_sensor_recent_hours: int = Field(default=24, ge=1, le=24 * 30)
    agent_sensor_points_per_metric: int = Field(default=6, ge=1, le=24)
    agent_sensor_char_budget: int = Field(default=2600, ge=0, le=16000)
    sensor_ingest_max_batch: int = Field(default=200, ge=1, le=2000)
    agent_graph_char_budget: int = Field(default=2600, ge=0, le=12000)
    agent_evidence_char_budget: int = Field(default=4000, ge=0, le=12000)
    agent_citation_excerpt_chars: int = Field(default=260, ge=80, le=2000)
    upload_max_bytes: int = Field(default=32 * 1024 * 1024, ge=1024, le=512 * 1024 * 1024)

    model_config = SettingsConfigDict(env_file=".env", env_file_encoding="utf-8")

    @field_validator("embedding_provider", mode="before")
    @classmethod
    def _validate_embedding_provider(cls, value: str | None) -> str:
        return normalize_provider_choice(
            value,
            allowed={"dummy", "openai", "openai_compatible"},
            default="dummy",
        )

    @field_validator("kg_extraction_provider", mode="before")
    @classmethod
    def _validate_kg_provider(cls, value: str | None) -> str:
        return normalize_provider_choice(
            value,
            allowed={"auto", "disabled", "heuristic", "openai"},
            default="auto",
        )

    @field_validator(
        "review_provider",
        "agent_provider",
        "agent_router_provider",
        "agent_memory_provider",
        "agent_claim_verifier_provider",
        "agent_profile_provider",
        mode="before",
    )
    @classmethod
    def _validate_openai_provider(cls, value: str | None) -> str:
        return normalize_provider_choice(
            value,
            allowed={"auto", "disabled", "openai"},
            default="openai",
        )

    @model_validator(mode="after")
    def _normalize_paths_and_urls(self) -> Settings:
        if not str(self.auth_postgres_dsn or "").strip():
            raise ValueError("AUTH_POSTGRES_DSN must be configured; auth storage must not fall back to the application database")
        if self.app_env.strip().lower() == "production" and not bool(self.auth_cookie_secure):
            raise ValueError("AUTH_COOKIE_SECURE must be enabled in production")
        self.auth_legacy_sqlite_path = resolve_workspace_path(self.auth_legacy_sqlite_path)
        self.chroma_path = resolve_workspace_path(self.chroma_path)
        self.kg_ontology_path = resolve_workspace_path(self.kg_ontology_path)
        allowed_hosts = parse_host_allowlist(self.model_host_allowlist)
        allow_private = bool(self.allow_private_model_hosts)
        self.embedding_base_url = normalize_outbound_base_url(
            self.embedding_base_url,
            field_name="embedding_base_url",
            allowed_hosts=allowed_hosts,
            allow_private_hosts=allow_private,
        )
        self.kg_base_url = normalize_outbound_base_url(
            self.kg_base_url,
            field_name="kg_base_url",
            allowed_hosts=allowed_hosts,
            allow_private_hosts=allow_private,
        )
        self.review_base_url = normalize_outbound_base_url(
            self.review_base_url,
            field_name="review_base_url",
            allowed_hosts=allowed_hosts,
            allow_private_hosts=allow_private,
        )
        self.vision_base_url = normalize_outbound_base_url(
            self.vision_base_url,
            field_name="vision_base_url",
            allowed_hosts=allowed_hosts,
            allow_private_hosts=allow_private,
        )
        self.agent_base_url = normalize_outbound_base_url(
            self.agent_base_url,
            field_name="agent_base_url",
            allowed_hosts=allowed_hosts,
            allow_private_hosts=allow_private,
        )
        self.agent_router_base_url = normalize_outbound_base_url(
            self.agent_router_base_url,
            field_name="agent_router_base_url",
            allowed_hosts=allowed_hosts,
            allow_private_hosts=allow_private,
        )
        self.agent_memory_base_url = normalize_outbound_base_url(
            self.agent_memory_base_url,
            field_name="agent_memory_base_url",
            allowed_hosts=allowed_hosts,
            allow_private_hosts=allow_private,
        )
        self.agent_claim_verifier_base_url = normalize_outbound_base_url(
            self.agent_claim_verifier_base_url,
            field_name="agent_claim_verifier_base_url",
            allowed_hosts=allowed_hosts,
            allow_private_hosts=allow_private,
        )
        self.agent_profile_base_url = normalize_outbound_base_url(
            self.agent_profile_base_url,
            field_name="agent_profile_base_url",
            allowed_hosts=allowed_hosts,
            allow_private_hosts=allow_private,
        )
        return self


settings = Settings()
