import json
import re

import src.bee_ingestion.agent as agent_module
from src.bee_ingestion.agent import AgentQueryError, AgentService
from src.bee_ingestion.agent_runtime.prompt_builder import PromptBuilder
from src.bee_ingestion.agent_runtime.verifier import AnswerVerifier
from src.bee_ingestion.settings import settings


class FakeAgentRepository:
    def __init__(self) -> None:
        self.sessions = {}
        self.profiles = {}
        self.profile_tokens = {}
        self.session_tokens = {}
        self.session_memories = {}
        self.messages = {}
        self.query_runs = []
        self.query_sources = []
        self.embedding_cache = {}
        self.latest_corpus_snapshot_id = "snapshot-1"
        self.next_session = 1
        self.next_message = 1
        self.next_run = 1
        self.next_profile = 1
        self.last_message_list_filters = None
        self.last_session_memory_filters = None
        self.last_session_scope_filters = None

    def get_agent_runtime_config(self, tenant_id):
        return {
            "settings_json": {
                "memory_enabled": False,
                "profile_enabled": False,
            }
        }

    def get_agent_runtime_secret(self, tenant_id, include_value=False):
        return None

    def build_query_pattern(self, normalized_query):
        tokens = [item for item in re.split(r"[^a-z0-9]+", normalized_query.lower()) if len(item) >= 3]
        signature = "|".join(tokens[:8])
        return signature, tokens[:8]

    def get_agent_query_pattern(self, tenant_id, pattern_signature):
        return None

    def touch_agent_query_pattern_route_hit(self, tenant_id, pattern_signature):
        return None

    def save_agent_query_pattern_route(self, tenant_id, pattern_signature, query_keywords, example_query, route_payload, router_model):
        return None

    def get_cached_query_embedding(self, tenant_id, normalized_query, cache_identity):
        return self.embedding_cache.get((tenant_id, normalized_query, cache_identity))

    def save_cached_query_embedding(self, tenant_id, normalized_query, cache_identity, embedding):
        self.embedding_cache[(tenant_id, normalized_query, cache_identity)] = {
            "tenant_id": tenant_id,
            "normalized_query": normalized_query,
            "cache_identity": cache_identity,
            "embedding_json": list(embedding),
            "cached_at": agent_module.datetime.now(agent_module.timezone.utc),
            "cache_hits": 0,
        }

    def touch_cached_query_embedding_hit(self, tenant_id, normalized_query, cache_identity):
        row = self.embedding_cache.get((tenant_id, normalized_query, cache_identity))
        if row is not None:
            row["cache_hits"] = int(row.get("cache_hits") or 0) + 1

    def get_latest_corpus_snapshot_id(self, tenant_id="shared"):
        return self.latest_corpus_snapshot_id

    def get_agent_session(self, session_id, tenant_id=None):
        session = self.sessions.get(session_id)
        if session is None:
            return None
        if tenant_id and session.get("tenant_id") != tenant_id:
            return None
        return session

    def create_agent_session(self, tenant_id="shared", title=None, profile_id=None, auth_user_id=None, workspace_kind="general"):
        session_id = f"session-{self.next_session}"
        self.next_session += 1
        self.sessions[session_id] = {
            "session_id": session_id,
            "tenant_id": tenant_id,
            "title": title,
            "status": "active",
            "profile_id": profile_id,
            "auth_user_id": auth_user_id,
            "workspace_kind": workspace_kind,
        }
        self.messages[session_id] = []
        return session_id

    def set_agent_session_token(self, session_id, token):
        self.session_tokens[session_id] = token

    def verify_agent_session_token(self, session_id, token, tenant_id=None, auth_user_id=None):
        return self.session_tokens.get(session_id) == token

    def attach_agent_profile_to_session(self, session_id, profile_id):
        if session_id in self.sessions:
            self.sessions[session_id]["profile_id"] = profile_id

    def claim_agent_session(self, session_id, worker_id, lease_seconds):
        session = self.sessions.get(session_id)
        if session is None:
            raise ValueError("Session not found")
        if session.get("claimed_by") and session.get("claimed_by") != worker_id:
            return False
        session["claimed_by"] = worker_id
        return True

    def release_agent_session(self, session_id, worker_id):
        session = self.sessions.get(session_id)
        if session and session.get("claimed_by") == worker_id:
            session["claimed_by"] = None

    def list_agent_messages(self, session_id, limit=20, tenant_id=None, auth_user_id=None, profile_id=None):
        self.last_message_list_filters = {
            "session_id": session_id,
            "tenant_id": tenant_id,
            "auth_user_id": auth_user_id,
            "profile_id": profile_id,
            "limit": limit,
        }
        return self.messages.get(session_id, [])[-limit:]

    def get_agent_profile(self, profile_id, tenant_id=None):
        profile = self.profiles.get(profile_id)
        if profile is None:
            return None
        if tenant_id and profile.get("tenant_id") != tenant_id:
            return None
        return profile

    def get_agent_profile_by_auth_user(self, auth_user_id, tenant_id=None):
        for profile in self.profiles.values():
            if profile.get("auth_user_id") == auth_user_id and (tenant_id is None or profile.get("tenant_id") == tenant_id):
                return profile
        return None

    def create_agent_profile(self, tenant_id="shared", auth_user_id=None):
        profile_id = f"profile-{self.next_profile}"
        self.next_profile += 1
        self.profiles[profile_id] = {
            "profile_id": profile_id,
            "tenant_id": tenant_id,
            "auth_user_id": auth_user_id,
            "status": "active",
            "summary_json": {},
            "summary_text": "",
        }
        return profile_id

    def set_agent_profile_token(self, profile_id, token):
        self.profile_tokens[profile_id] = token

    def verify_agent_profile_token(self, profile_id, token, tenant_id=None):
        return self.profile_tokens.get(profile_id) == token

    def get_latest_agent_session_scope(self, session_id, tenant_id=None, auth_user_id=None, profile_id=None):
        self.last_session_scope_filters = {
            "session_id": session_id,
            "tenant_id": tenant_id,
            "auth_user_id": auth_user_id,
            "profile_id": profile_id,
        }
        return None

    def get_agent_session_memory(self, session_id, tenant_id=None, auth_user_id=None, profile_id=None):
        self.last_session_memory_filters = {
            "session_id": session_id,
            "tenant_id": tenant_id,
            "auth_user_id": auth_user_id,
            "profile_id": profile_id,
        }
        return self.session_memories.get(session_id)

    def save_agent_session_memory(self, session_id, summary_json, summary_text, source_provider, source_model, prompt_version):
        self.session_memories[session_id] = {
            "session_id": session_id,
            "summary_json": summary_json,
            "summary_text": summary_text,
            "source_provider": source_provider,
            "source_model": source_model,
            "prompt_version": prompt_version,
        }

    def save_agent_profile(self, profile_id, summary_json, summary_text, source_provider, source_model, prompt_version):
        profile = self.profiles.get(profile_id)
        if profile is not None:
            profile.update(
                {
                    "summary_json": summary_json,
                    "summary_text": summary_text,
                    "source_provider": source_provider,
                    "source_model": source_model,
                    "prompt_version": prompt_version,
                }
            )

    def persist_agent_turn(self, session_id, tenant_id, user_message_id, question, **kwargs):
        query_run_id = f"run-{self.next_run}"
        self.next_run += 1
        self.messages.setdefault(session_id, []).append(
            {
                "message_id": user_message_id,
                "session_id": session_id,
                "role": "user",
                "content": question,
                "metadata_json": {
                    "normalized_query": kwargs.get("normalized_query"),
                    "question_type": kwargs.get("question_type"),
                    "retrieval_mode": kwargs.get("retrieval_mode"),
                },
            }
        )
        self.messages.setdefault(session_id, []).append(
            {
                "message_id": f"message-{self.next_message}",
                "session_id": session_id,
                "role": "assistant",
                "content": kwargs.get("answer") or "",
                "metadata_json": {
                    **(kwargs.get("assistant_metadata") or {}),
                    "query_run_id": query_run_id,
                },
            }
        )
        self.next_message += 1
        self.query_runs.append({"query_run_id": query_run_id, "tenant_id": tenant_id, "question": question, **kwargs})
        self.query_sources.append({"query_run_id": query_run_id, "sources": kwargs.get("sources") or []})
        return query_run_id

    def list_documents_by_ids(self, document_ids):
        return [
            {
                "document_id": document_id,
                "tenant_id": "shared",
                "filename": "doc.txt",
                "status": "completed",
                "document_class": "note",
            }
            for document_id in document_ids
        ]

    def list_chunk_records_by_ids(self, chunk_ids):
        rows = []
        for index, chunk_id in enumerate(chunk_ids):
            rows.append(
                {
                    "chunk_id": chunk_id,
                    "document_id": "doc-1",
                    "tenant_id": "shared",
                    "chunk_index": index,
                    "page_start": 1,
                    "page_end": 1,
                    "section_path": ["Preface"],
                    "prev_chunk_id": None,
                    "next_chunk_id": None,
                    "char_start": 0,
                    "char_end": 64,
                    "content_type": "text",
                    "text": "Honey bees produce honey and maintain the hive.",
                    "parser_version": "v1",
                    "chunker_version": "v1",
                    "metadata_json": {"section_title": "Preface"},
                    "kg_assertion_count": 1,
                    "validation_status": "accepted",
                    "quality_score": 1.0,
                    "reasons": ["ok"],
                }
            )
        return rows

    def list_document_synopses_by_ids(self, document_ids):
        return [
            {
                "document_id": document_id,
                "tenant_id": "shared",
                "title": "doc.txt",
                "synopsis_text": "This document explains how honey bees produce honey and maintain the hive through coordinated colony work.",
                "accepted_chunk_count": 1,
                "section_count": 1,
                "source_stage": "chunks_validated",
                "synopsis_version": "extractive-v1",
                "metadata_json": {},
            }
            for document_id in document_ids
        ]

    def list_section_synopses_for_chunk_ids(self, chunk_ids, limit=24):
        if not chunk_ids:
            return []
        return [
            {
                "section_id": "doc-1:section:preface",
                "document_id": "doc-1",
                "tenant_id": "shared",
                "parent_section_id": None,
                "section_path": ["Preface"],
                "section_level": 1,
                "section_title": "Preface",
                "page_start": 1,
                "page_end": 1,
                "char_start": 0,
                "char_end": 64,
                "first_chunk_id": chunk_ids[0],
                "last_chunk_id": chunk_ids[0],
                "accepted_chunk_count": 1,
                "total_chunk_count": 1,
                "synopsis_text": "The section describes honey production and basic hive maintenance in strong colonies.",
                "synopsis_version": "extractive-v1",
                "metadata_json": {},
            }
        ]

    def search_chunk_records_lexical(self, query_text, tenant_id="shared", document_ids=None, limit=12):
        return []

    def search_document_synopses_lexical(self, query_text, tenant_id="shared", document_ids=None, limit=6):
        return []

    def search_section_synopses_lexical(self, query_text, tenant_id="shared", document_ids=None, limit=12):
        return []

    def list_chunk_records_for_asset_pages(self, assets, limit=120):
        return []

    def list_chunk_records(self, document_id=None, status=None, limit=50, offset=0):
        if not document_id:
            return []
        return [
            {
                "chunk_id": "chunk-visual",
                "document_id": document_id,
                "tenant_id": "shared",
                "chunk_index": 0,
                "page_start": 1,
                "page_end": 1,
                "section_path": ["Figures"],
                "prev_chunk_id": None,
                "next_chunk_id": None,
                "char_start": 0,
                "char_end": 64,
                "content_type": "text",
                "text": "Figure showing honey bee anatomy.",
                "parser_version": "v1",
                "chunker_version": "v1",
                "metadata_json": {"section_title": "Figures"},
                "kg_assertion_count": 0,
                "validation_status": status or "accepted",
                "quality_score": 1.0,
                "reasons": ["ok"],
            }
        ]

    def list_page_assets_by_ids(self, asset_ids):
        return [
            {
                "asset_id": asset_id,
                "document_id": "doc-1",
                "tenant_id": "shared",
                "page_number": 1,
                "asset_index": 0,
                "asset_type": "figure",
                "asset_path": "data/page_assets/example.png",
                "ocr_text": "Honey bee diagram",
                "description_text": "Diagram of a honey bee.",
                "search_text": "Honey bee diagram anatomy figure",
                "metadata_json": {"label": "Figure 1", "important_terms": ["honey", "bee", "diagram"]},
            }
            for asset_id in asset_ids
        ]

    def list_page_assets_for_chunks(self, chunk_ids, limit=200):
        return []

    def search_page_assets_lexical(self, query_text, tenant_id="shared", document_ids=None, limit=8):
        return []

    def list_chunk_asset_links_for_chunks(self, chunk_ids, limit=250):
        return []

    def build_user_sensor_context(self, tenant_id, auth_user_id, normalized_query, max_rows=8, hours=72, points_per_metric=6):
        return []

    def list_kg_assertions_for_chunks(self, chunk_ids, limit=200, per_chunk_limit=None):
        if not chunk_ids:
            return []
        return [
            {
                "assertion_id": "a-1",
                "document_id": "doc-1",
                "chunk_id": chunk_ids[0],
                "subject_entity_id": "colony_colony",
                "predicate": "produces",
                "object_entity_id": "honey_honey",
                "object_literal": None,
                "confidence": 0.91,
                "qualifiers": {},
                "status": "accepted",
            }
        ]

    def list_kg_evidence_for_assertions(self, assertion_ids, limit=200, per_assertion_limit=None):
        if not assertion_ids:
            return []
        return [{"evidence_id": "ev-1", "assertion_id": assertion_ids[0], "excerpt": "Honey bees produce honey.", "start_offset": 0, "end_offset": 24}]

    def list_kg_entities_by_ids(self, entity_ids):
        if not entity_ids:
            return []
        return [
            {"entity_id": entity_ids[0], "canonical_name": "Colony", "entity_type": "Colony"},
            {"entity_id": entity_ids[1], "canonical_name": "Honey", "entity_type": "Honey"},
        ]

    def search_kg_entities_for_query(self, query_text, tenant_id="shared", document_ids=None, limit=6):
        return []

    def list_kg_neighbor_assertions_for_entities(
        self,
        entity_ids,
        tenant_id="shared",
        document_ids=None,
        exclude_assertion_ids=None,
        limit=16,
        per_entity_limit=None,
    ):
        return []


class FakeAgentStore:
    def search(self, query_embedding, top_k=8, tenant_id=None, document_ids=None):
        return [
            {
                "chunk_id": "chunk-1",
                "document": "Honey bees produce honey and maintain the hive.",
                "metadata": {"document_id": "doc-1", "section_title": "Preface"},
                "distance": 0.08,
                "rank": 1,
            }
        ]

    def search_assets(self, query_embedding, top_k=8, tenant_id=None, document_ids=None):
        return [
            {
                "asset_id": "asset-1",
                "document": "Honey bee diagram anatomy figure",
                "metadata": {"document_id": "doc-1", "label": "Figure 1", "asset_type": "figure", "important_terms": ["honey", "bee", "diagram"]},
                "distance": 0.09,
                "rank": 1,
            }
        ]


class FakeEmbedder:
    def embed(self, texts):
        return [[0.1, 0.2, 0.3] for _ in texts]


def test_agent_query_records_trace_and_citations(monkeypatch) -> None:
    repository = FakeAgentRepository()
    service = AgentService(repository=repository, store=FakeAgentStore(), embedder=FakeEmbedder())

    monkeypatch.setattr(
        service,
        "_generate_answer",
        lambda question, normalized_query, prompt_bundle, question_type, runtime_config: {
            "provider": "openai",
            "model": "gpt-5-mini",
            "prompt_version": "v1",
            "prompt_payload": {"question": question},
            "raw_payload": {"id": "resp-1"},
            "content": json.dumps(
                {
                    "answer": "Honey bees produce honey.",
                    "confidence": 0.84,
                    "abstained": False,
                    "abstain_reason": None,
                    "citations": [{"chunk_id": "chunk-1", "quote": "Honey bees produce honey"}],
                    "supporting_assertions": ["a-1"],
                    "supporting_entities": ["colony_colony", "honey_honey"],
                }
            ),
        },
    )

    result = service.query("How do bees produce honey?")

    assert result["session_id"] == "session-1"
    assert result["query_run_id"] == "run-1"
    assert result["corpus_snapshot_id"] == "snapshot-1"
    assert result["abstained"] is False
    assert result["review_status"] == "needs_review"
    assert result["citations"][0]["chunk_id"] == "chunk-1"
    assert repository.query_runs[0]["question_type"] == "procedure"
    assert repository.query_runs[0]["corpus_snapshot_id"] == "snapshot-1"
    assert repository.query_sources[0]["sources"]


def test_agent_query_uses_open_world_answer_when_no_results(monkeypatch) -> None:
    class EmptyStore:
        def search(self, query_embedding, top_k=8, tenant_id=None, document_ids=None):
            return []

    repository = FakeAgentRepository()
    service = AgentService(repository=repository, store=EmptyStore(), embedder=FakeEmbedder())
    monkeypatch.setattr(
        service,
        "_generate_open_world_answer",
        lambda **kwargs: {
            "provider": "openai",
            "model": "gpt-5-mini",
            "prompt_version": "v1",
            "prompt_payload": {"fallback_mode": "open_world"},
            "raw_payload": {"id": "resp-open-world"},
            "content": json.dumps(
                {
                    "answer": "Varroa destructor is a parasitic mite that weakens honey bee colonies.",
                    "confidence": 0.82,
                    "abstained": False,
                    "abstain_reason": None,
                    "used_chunk_ids": [],
                    "used_asset_ids": [],
                    "used_sensor_row_ids": [],
                    "used_evidence_ids": [],
                    "supporting_assertions": [],
                    "supporting_entities": [],
                }
            ),
        },
    )

    result = service.query("What is varroa?")

    assert result["abstained"] is False
    assert result["answer"].startswith("Varroa destructor is a parasitic mite")
    assert result["grounding_check"]["open_world_answer_used"] is True
    assert result["review_reason"] == "open_world_answer"
    assert result["review_status"] == "needs_review"
    assert result["corpus_snapshot_id"] == "snapshot-1"
    assert repository.query_runs[0]["abstained"] is False


def test_agent_query_uses_last_resort_answer_when_no_results_and_open_world_fails(monkeypatch) -> None:
    class EmptyStore:
        def search(self, query_embedding, top_k=8, tenant_id=None, document_ids=None):
            return []

    repository = FakeAgentRepository()
    service = AgentService(repository=repository, store=EmptyStore(), embedder=FakeEmbedder())
    monkeypatch.setattr(service, "_generate_open_world_answer", lambda **kwargs: (_ for _ in ()).throw(AgentQueryError("open-world unavailable")))

    result = service.query("How should I interpret humidity changes inside a hive?")

    assert result["abstained"] is False
    assert result["review_status"] == "needs_review"
    assert result["review_reason"] == "last_resort_fallback"
    assert "I do not have enough indexed evidence" not in result["answer"]
    assert result["grounding_check"]["last_resort_fallback_used"] is True
    assert repository.query_runs[0]["provider"] == "fallback"
    assert repository.query_runs[0]["abstained"] is False


def test_agent_chat_returns_transcript(monkeypatch) -> None:
    repository = FakeAgentRepository()
    service = AgentService(repository=repository, store=FakeAgentStore(), embedder=FakeEmbedder())

    monkeypatch.setattr(
        service,
        "_generate_answer",
        lambda question, normalized_query, prompt_bundle, question_type, runtime_config: {
            "provider": "openai",
            "model": "gpt-5-mini",
            "prompt_version": "v1",
            "prompt_payload": {"question": question},
            "raw_payload": {"id": "resp-1"},
            "content": json.dumps(
                {
                    "answer": "Honey bees produce honey.",
                    "confidence": 0.84,
                    "abstained": False,
                    "abstain_reason": None,
                    "citations": [{"chunk_id": "chunk-1", "quote": "Honey bees produce honey"}],
                    "supporting_assertions": ["a-1"],
                    "supporting_entities": ["colony_colony", "honey_honey"],
                }
            ),
        },
    )

    result = service.chat("How do bees produce honey?")

    assert result["session_id"] == "session-1"
    assert len(result["messages"]) == 2


def test_agent_query_recovers_when_model_returns_plain_text_instead_of_json(monkeypatch) -> None:
    repository = FakeAgentRepository()
    service = AgentService(repository=repository, store=FakeAgentStore(), embedder=FakeEmbedder())

    monkeypatch.setattr(
        service,
        "_generate_answer",
        lambda question, normalized_query, prompt_bundle, question_type, runtime_config: {
            "provider": "openai",
            "model": "gpt-5.4",
            "prompt_version": "v2",
            "prompt_payload": {"question": question},
            "raw_payload": {"id": "resp-plain"},
            "content": "Varroa is a parasitic mite that lives on honey bees and weakens the colony by feeding on developing brood and adult bees.",
        },
    )

    result = service.query("What is varroa?")

    assert result["abstained"] is False
    assert result["review_reason"] != "last_resort_fallback"
    assert result["answer"].startswith("Varroa is a parasitic mite")
    assert result["citations"]


def test_agent_query_uses_owner_scoped_message_and_memory_reads(monkeypatch) -> None:
    repository = FakeAgentRepository()
    service = AgentService(repository=repository, store=FakeAgentStore(), embedder=FakeEmbedder())

    monkeypatch.setattr(
        service,
        "_generate_answer",
        lambda question, normalized_query, prompt_bundle, question_type, runtime_config: {
            "provider": "openai",
            "model": "gpt-5-mini",
            "prompt_version": "v1",
            "prompt_payload": {"question": question},
            "raw_payload": {"id": "resp-1"},
            "content": json.dumps(
                {
                    "answer": "Honey bees produce honey.",
                    "confidence": 0.84,
                    "abstained": False,
                    "abstain_reason": None,
                    "citations": [{"chunk_id": "chunk-1", "quote": "Honey bees produce honey"}],
                    "supporting_assertions": ["a-1"],
                    "supporting_entities": ["colony_colony", "honey_honey"],
                }
            ),
        },
    )

    result = service.query("How do bees produce honey?", auth_user_id="user-1")

    assert result["session_id"] == "session-1"
    assert repository.last_message_list_filters["auth_user_id"] == "user-1"
    assert repository.last_message_list_filters["profile_id"] == "profile-1"
    assert repository.last_session_memory_filters["auth_user_id"] == "user-1"
    assert repository.last_session_memory_filters["profile_id"] == "profile-1"
    assert repository.last_session_scope_filters["auth_user_id"] == "user-1"
    assert repository.last_session_scope_filters["profile_id"] == "profile-1"


def test_agent_query_falls_back_to_lexical_grounding_when_claim_verifier_errors(monkeypatch) -> None:
    repository = FakeAgentRepository()
    service = AgentService(repository=repository, store=FakeAgentStore(), embedder=FakeEmbedder())

    monkeypatch.setattr(
        service,
        "_generate_answer",
        lambda question, normalized_query, prompt_bundle, question_type, runtime_config: {
            "provider": "openai",
            "model": "gpt-5-mini",
            "prompt_version": "v1",
            "prompt_payload": {"question": question},
            "raw_payload": {"id": "resp-1"},
            "content": json.dumps(
                {
                    "answer": "Honey bees produce honey and maintain the hive.",
                    "confidence": 0.84,
                    "abstained": False,
                    "abstain_reason": None,
                    "citations": [{"chunk_id": "chunk-1", "quote": "Honey bees produce honey and maintain the hive."}],
                    "supporting_assertions": ["a-1"],
                    "supporting_entities": ["colony_colony", "honey_honey"],
                }
            ),
        },
    )
    monkeypatch.setattr(
        agent_module,
        "_verify_answer_claims_with_model",
        lambda **kwargs: {
            "method": "claim_verifier_error",
            "passed": False,
            "supported_ratio": 0.0,
            "unsupported_claims": ["claim_verifier_request_failed"],
            "claims": [],
            "provider": "openai",
            "model": "gpt-5.4-nano",
        },
    )

    result = service.query("How do bees produce honey?")

    assert result["abstained"] is False
    assert result["grounding_check"]["method"] == "lexical_fallback"
    assert result["grounding_check"]["passed"] is True
    assert result["grounding_check"]["verifier_fallback_reason"] == "claim_verifier_error"
    assert result["review_status"] == "needs_review"


def test_agent_query_uses_open_world_fallback_on_real_claim_verifier_failure(monkeypatch) -> None:
    repository = FakeAgentRepository()
    service = AgentService(repository=repository, store=FakeAgentStore(), embedder=FakeEmbedder())

    monkeypatch.setattr(
        service,
        "_generate_answer",
        lambda question, normalized_query, prompt_bundle, question_type, runtime_config: {
            "provider": "openai",
            "model": "gpt-5-mini",
            "prompt_version": "v1",
            "prompt_payload": {"question": question},
            "raw_payload": {"id": "resp-1"},
            "content": json.dumps(
                {
                    "answer": "Honey bees produce honey and maintain the hive.",
                    "confidence": 0.84,
                    "abstained": False,
                    "abstain_reason": None,
                    "citations": [{"chunk_id": "chunk-1", "quote": "Honey bees produce honey and maintain the hive."}],
                    "supporting_assertions": ["a-1"],
                    "supporting_entities": ["colony_colony", "honey_honey"],
                }
            ),
        },
    )
    monkeypatch.setattr(
        agent_module,
        "_verify_answer_claims_with_model",
        lambda **kwargs: {
            "method": "claim_verifier",
            "passed": False,
            "supported_ratio": 0.0,
            "unsupported_claims": ["Honey bees produce honey and maintain the hive."],
            "claims": [
                {
                    "claim": "Honey bees produce honey and maintain the hive.",
                    "supported": False,
                    "evidence_ids": [],
                }
            ],
            "provider": "openai",
            "model": "gpt-5.4-nano",
        },
    )
    monkeypatch.setattr(
        service,
        "_generate_open_world_answer",
        lambda **kwargs: {
            "provider": "openai",
            "model": "gpt-5-mini",
            "prompt_version": "v1",
            "prompt_payload": {"fallback_mode": "open_world"},
            "raw_payload": {"id": "resp-open-world"},
            "content": json.dumps(
                {
                    "answer": "Honey bees produce honey and maintain their colony through coordinated worker activity.",
                    "confidence": 0.86,
                    "abstained": False,
                    "abstain_reason": None,
                    "used_chunk_ids": [],
                    "used_asset_ids": [],
                    "used_sensor_row_ids": [],
                    "used_evidence_ids": [],
                    "supporting_assertions": [],
                    "supporting_entities": [],
                }
            ),
        },
    )

    result = service.query("How do bees produce honey?")

    assert result["abstained"] is False
    assert result["grounding_check"]["open_world_fallback_used"] is True
    assert result["review_status"] == "needs_review"
    assert result["review_reason"] == "open_world_fallback"


def test_claim_verifier_accepts_world_knowledge_support(monkeypatch) -> None:
    runtime_config = agent_module.coerce_agent_runtime_config(agent_module.default_agent_runtime_config())
    monkeypatch.setattr(agent_module.settings, "agent_api_key", "test-key")
    monkeypatch.setattr(
        agent_module,
        "_call_openai_json_payload",
        lambda **kwargs: {
            "choices": [
                {
                    "message": {
                        "content": json.dumps(
                            {
                                "verdict": True,
                                "supported_ratio": 1.0,
                                "unsupported_claims": [],
                                "claims": [
                                    {
                                        "claim": "Honey is produced from floral nectar by bees.",
                                        "supported": True,
                                        "evidence_ids": [],
                                        "support_basis": "world_knowledge",
                                    }
                                ],
                            }
                        )
                    }
                }
            ]
        },
    )

    result = agent_module._verify_answer_claims_with_model(
        answer="Honey is produced from floral nectar by bees.",
        normalized_query="How do bees produce honey?",
        evidence_rows=[{"id": "ev-1", "text": "Bees collect nectar from flowers."}],
        runtime_config=runtime_config,
    )

    assert result is not None
    assert result["method"] == "claim_verifier"
    assert result["passed"] is True
    assert result["supported_ratio"] == 1.0
    assert result["world_knowledge_claims"] == ["Honey is produced from floral nectar by bees."]
    assert result["claims"][0]["support_basis"] == "world_knowledge"


def test_claim_verifier_rejects_numeric_world_knowledge_support(monkeypatch) -> None:
    runtime_config = agent_module.coerce_agent_runtime_config(agent_module.default_agent_runtime_config())
    monkeypatch.setattr(agent_module.settings, "agent_api_key", "test-key")
    monkeypatch.setattr(
        agent_module,
        "_call_openai_json_payload",
        lambda **kwargs: {
            "choices": [
                {
                    "message": {
                        "content": json.dumps(
                            {
                                "verdict": True,
                                "supported_ratio": 1.0,
                                "unsupported_claims": [],
                                "claims": [
                                    {
                                        "claim": "A queen lays 3000 eggs per day.",
                                        "supported": True,
                                        "evidence_ids": [],
                                        "support_basis": "world_knowledge",
                                    }
                                ],
                            }
                        )
                    }
                }
            ]
        },
    )

    result = agent_module._verify_answer_claims_with_model(
        answer="A queen lays 3000 eggs per day.",
        normalized_query="How many eggs does a queen lay?",
        evidence_rows=[{"id": "ev-1", "text": "Queens lay eggs in brood cells."}],
        runtime_config=runtime_config,
    )

    assert result is not None
    assert result["method"] == "claim_verifier"
    assert result["passed"] is False
    assert result["unsupported_claims"] == ["A queen lays 3000 eggs per day."]
    assert result["world_knowledge_claims"] == []
    assert result["claims"][0]["support_basis"] == "unsupported"


def test_prompt_builder_builds_prompt_from_provided_context_only() -> None:
    builder = PromptBuilder(
        budget_profile_summary=lambda payload, budget: payload.get("summary_json") if payload else None,
        budget_session_summary=lambda payload, budget: payload.get("summary_json") if payload else None,
        filter_profile_summary=lambda payload, **kwargs: payload,
        filter_session_summary=lambda payload, **kwargs: payload,
        refresh_memory_summary=lambda payload: {**payload, "summary_text": "memory"},
        refresh_profile_summary=lambda payload: {**payload, "summary_text": "profile"},
        trusted_asset_grounding_text=lambda row: str(row.get("search_text") or ""),
        trusted_sensor_grounding_text=lambda row: str(row.get("summary_text") or ""),
        extract_citation_excerpt=lambda text, query, limit: str(text)[:limit],
    )
    bundle = agent_module.AgentContextBundle(
        chunks=[
            {
                "chunk_id": "chunk-1",
                "document_id": "doc-1",
                "chunk_index": 0,
                "page_start": 1,
                "page_end": 1,
                "metadata_json": {"section_title": "Honey", "chunk_role": "body"},
                "text": "Worker bees convert nectar into honey.",
            }
        ],
        assets=[],
        sensor_rows=[],
        assertions=[{"assertion_id": "a-1", "chunk_id": "chunk-1", "subject_entity_id": "bee", "predicate": "converts", "object_entity_id": None, "object_literal": "nectar into honey", "confidence": 0.9}],
        evidence=[{"evidence_id": "ev-1", "assertion_id": "a-1", "excerpt": "Worker bees convert nectar into honey."}],
        entities=[{"entity_id": "entity-1", "canonical_name": "Honey", "entity_type": "HiveProduct"}],
        graph_chains=[],
        sources=[],
    )
    prompt_bundle = builder.build_prompt_bundle(
        question="How do bees make honey?",
        normalized_query="How do bees make honey?",
        prior_messages=[{"role": "user", "content": "previous"}],
        profile_summary=None,
        session_memory=None,
        bundle=bundle,
        question_type="procedure",
        runtime_config=agent_module.coerce_agent_runtime_config(agent_module.default_agent_runtime_config()),
        workspace_kind="general",
    )

    user_prompt = builder.build_user_prompt(
        question="How do bees make honey?",
        normalized_query="How do bees make honey?",
        question_type="procedure",
        prompt_bundle=prompt_bundle,
    )

    assert prompt_bundle.chunk_payload[0]["chunk_id"] == "chunk-1"
    assert prompt_bundle.assertion_payload[0]["assertion_id"] == "a-1"
    assert prompt_bundle.evidence_payload[0]["evidence_id"] == "ev-1"
    assert '"chunk-1"' in user_prompt
    assert "context_chunks:" in user_prompt


def test_answer_verifier_rejects_unknown_citation_ids() -> None:
    verifier = AnswerVerifier(
        trusted_asset_grounding_text=lambda row: "",
        sensor_grounding_series_text=lambda row: "",
        call_json_payload=lambda **kwargs: {
            "choices": [
                {
                    "message": {
                        "content": json.dumps(
                            {
                                "verdict": True,
                                "supported_ratio": 1.0,
                                "unsupported_claims": [],
                                "claims": [
                                    {
                                        "claim": "Honey comes from nectar.",
                                        "supported": True,
                                        "evidence_ids": ["ev-missing"],
                                        "support_basis": "evidence",
                                    }
                                ],
                            }
                        )
                    }
                }
            ]
        },
        extract_message_content_fn=lambda body: body["choices"][0]["message"]["content"],
    )
    runtime_config = agent_module.coerce_agent_runtime_config(agent_module.default_agent_runtime_config())
    runtime_config["api_key_override"] = "test-key"

    result = verifier.verify_answer_claims_with_model(
        answer="Honey comes from nectar.",
        normalized_query="How do bees make honey?",
        evidence_rows=[{"id": "ev-1", "text": "Worker bees collect nectar."}],
        runtime_config=runtime_config,
    )

    assert result is not None
    assert result["passed"] is False
    assert result["unsupported_claims"] == ["Honey comes from nectar."]
    assert result["claims"][0]["evidence_ids"] == []


def test_answer_verifier_rejects_non_abstention_without_evidence() -> None:
    verifier = AnswerVerifier(
        trusted_asset_grounding_text=lambda row: "",
        sensor_grounding_series_text=lambda row: "",
        call_json_payload=lambda **kwargs: {},
        extract_message_content_fn=lambda body: "",
    )
    result = verifier.verify_answer_grounding(
        answer="Bees make honey from nectar.",
        question_type="fact",
        normalized_query="How do bees make honey?",
        chunk_map={},
        used_chunk_ids=[],
        asset_map={},
        used_asset_ids=[],
        sensor_row_map={},
        used_sensor_row_ids=[],
        assertion_map={},
        evidence_map={},
        used_evidence_ids=[],
        runtime_config=agent_module.coerce_agent_runtime_config(agent_module.default_agent_runtime_config()),
    )

    assert result["passed"] is False
    assert result["method"] == "lexical_fallback"


def test_derive_agent_review_state_keeps_lexical_fallback_reviewable() -> None:
    runtime_config = agent_module.coerce_agent_runtime_config(agent_module.default_agent_runtime_config())
    state, reason = agent_module._derive_agent_review_state(
        {
            "abstained": False,
            "grounding_check": {
                "method": "lexical_fallback",
                "passed": True,
                "supported_ratio": 1.0,
                "unsupported_claims": [],
            },
            "confidence": 0.9,
            "supporting_assertions": ["a-1"],
            "used_asset_ids": [],
            "used_sensor_row_ids": [],
            "used_evidence_ids": ["ev-1"],
        },
        runtime_config,
    )

    assert state == "needs_review"
    assert reason == "lexical_fallback"


def test_agent_query_preserves_public_response_shape() -> None:
    repository = FakeAgentRepository()
    service = AgentService(repository=repository, store=FakeAgentStore(), embedder=FakeEmbedder())

    result = service.query("How do bees produce honey?")

    assert {"answer", "confidence", "abstained", "citations", "query_run_id", "session_id", "profile_id"}.issubset(result.keys())


def test_derive_agent_review_state_marks_open_world_fallback_reviewable() -> None:
    runtime_config = agent_module.coerce_agent_runtime_config(agent_module.default_agent_runtime_config())
    state, reason = agent_module._derive_agent_review_state(
        {
            "abstained": False,
            "grounding_check": {
                "method": "claim_verifier",
                "passed": False,
                "supported_ratio": 0.0,
                "unsupported_claims": ["Honey bees produce honey and maintain the hive."],
                "open_world_fallback_used": True,
            },
            "confidence": 0.84,
            "supporting_assertions": ["a-1"],
            "used_asset_ids": [],
            "used_sensor_row_ids": [],
            "used_evidence_ids": [],
        },
        runtime_config,
    )

    assert state == "needs_review"
    assert reason == "open_world_fallback"


def test_derive_agent_review_state_marks_world_knowledge_support_reviewable() -> None:
    runtime_config = agent_module.coerce_agent_runtime_config(agent_module.default_agent_runtime_config())
    state, reason = agent_module._derive_agent_review_state(
        {
            "abstained": False,
            "grounding_check": {
                "method": "claim_verifier",
                "passed": True,
                "supported_ratio": 1.0,
                "unsupported_claims": [],
                "world_knowledge_claims": ["Honey is produced from floral nectar by bees."],
            },
            "confidence": 0.9,
            "supporting_assertions": ["a-1"],
            "used_asset_ids": [],
            "used_sensor_row_ids": [],
            "used_evidence_ids": ["ev-1"],
        },
        runtime_config,
    )

    assert state == "needs_review"
    assert reason == "world_knowledge_support"


def test_build_supported_subset_answer_for_explanation() -> None:
    answer, evidence_ids = agent_module._build_supported_subset_answer(
        grounding_check={
            "method": "lexical_fallback",
            "passed": False,
            "supported_ratio": 0.4,
            "unsupported_claims": ["The bees then fan the nectar until it ripens."],
            "claims": [
                {
                    "claim": "Bees collect nectar from flowers and store it in a honey-stomach.",
                    "supported": True,
                    "evidence_ids": ["ev-1", "ev-2"],
                },
                {
                    "claim": "The bees then fan the nectar until it ripens.",
                    "supported": False,
                    "evidence_ids": [],
                },
            ],
        },
        question_type="explanation",
    ) or ("", [])

    assert answer.startswith("Bees collect nectar from flowers and store it in a honey-stomach.")
    assert "honey-stomach" in answer
    assert "I can't be sure" not in answer
    assert evidence_ids == ["ev-1", "ev-2"]


def test_build_supported_subset_answer_rejects_numeric_unsupported_claims() -> None:
    result = agent_module._build_supported_subset_answer(
        grounding_check={
            "method": "lexical_fallback",
            "passed": False,
            "supported_ratio": 0.5,
            "unsupported_claims": ["A queen lays 3000 eggs per day."],
            "claims": [
                {
                    "claim": "Queens lay eggs in brood cells.",
                    "supported": True,
                    "evidence_ids": ["ev-1"],
                }
            ],
        },
        question_type="explanation",
    )

    assert result is None


def test_build_supported_subset_answer_for_fact() -> None:
    answer, evidence_ids = agent_module._build_supported_subset_answer(
        grounding_check={
            "method": "claim_verifier",
            "passed": False,
            "supported_ratio": 0.5,
            "unsupported_claims": ["Varroa also spreads rapidly between neighboring apiaries."],
            "claims": [
                {
                    "claim": "Varroa destructor is a parasitic mite that affects honey bees.",
                    "supported": True,
                    "evidence_ids": ["ev-1"],
                },
                {
                    "claim": "Varroa also spreads rapidly between neighboring apiaries.",
                    "supported": False,
                    "evidence_ids": [],
                },
            ],
        },
        question_type="fact",
    ) or ("", [])

    assert answer.startswith("Varroa destructor is a parasitic mite")
    assert "Based on the indexed corpus" not in answer
    assert "I can't be sure" not in answer
    assert evidence_ids == ["ev-1"]


def test_agent_query_uses_supported_subset_when_lexical_grounding_is_partial(monkeypatch) -> None:
    repository = FakeAgentRepository()
    service = AgentService(repository=repository, store=FakeAgentStore(), embedder=FakeEmbedder())

    monkeypatch.setattr(
        service,
        "_generate_answer",
        lambda question, normalized_query, prompt_bundle, question_type, runtime_config: {
            "provider": "openai",
            "model": "gpt-5-mini",
            "prompt_version": "v1",
            "prompt_payload": {"question": question},
            "raw_payload": {"id": "resp-1"},
            "content": json.dumps(
                {
                    "answer": "Bees collect nectar from flowers and store it in a honey-stomach. The bees then fan the nectar until it ripens.",
                    "confidence": 0.84,
                    "abstained": False,
                    "abstain_reason": None,
                    "citations": [{"chunk_id": "chunk-1", "quote": "Honey bees produce honey."}],
                    "supporting_assertions": ["a-1"],
                    "supporting_entities": ["colony_colony", "honey_honey"],
                    "used_evidence_ids": ["ev-1", "ev-2"],
                }
            ),
        },
    )
    monkeypatch.setattr(
        agent_module,
        "_verify_answer_claims_with_model",
        lambda **kwargs: {
            "method": "claim_verifier_error",
            "passed": False,
            "supported_ratio": 0.0,
            "unsupported_claims": ["claim_verifier_request_failed"],
            "claims": [],
            "provider": "openai",
            "model": "gpt-5.4-nano",
        },
    )
    monkeypatch.setattr(
        agent_module,
        "_lexical_grounding_check",
        lambda **kwargs: {
            "method": "lexical_fallback",
            "passed": False,
            "supported_ratio": 0.4,
            "unsupported_claims": ["The bees then fan the nectar until it ripens."],
            "claims": [
                {
                    "claim": "Bees collect nectar from flowers and store it in a honey-stomach.",
                    "supported": True,
                    "evidence_ids": ["ev-1"],
                },
                {
                    "claim": "The bees then fan the nectar until it ripens.",
                    "supported": False,
                    "evidence_ids": [],
                },
            ],
        },
    )

    result = service.query("How do bees produce honey?")

    assert result["abstained"] is False
    assert result["grounding_check"]["method"] == "supported_subset"
    assert result["grounding_check"]["passed"] is True
    assert result["grounding_check"]["supported_subset_used"] is True
    assert result["answer"].startswith("Bees collect nectar from flowers and store it in a honey-stomach.")
    assert "I can't be sure" not in result["answer"]
    assert result["review_status"] == "needs_review"


def test_lexical_grounding_is_relaxed_for_explanation() -> None:
    evidence_rows = [
        {
            "evidence_id": "ev-1",
            "text": "Bees collect nectar from flowers and store it in a honey-stomach before carrying it back to the hive.",
            "kind": "chunk",
        },
        {
            "evidence_id": "ev-2",
            "text": "Workers pass nectar between house bees, which gradually reduces moisture and converts it into honey.",
            "kind": "chunk",
        },
    ]

    result = agent_module._lexical_grounding_check(
        answer=(
            "Bees collect nectar from flowers and store it in a honey-stomach. "
            "Workers pass nectar between house bees, which reduces moisture and turns it into honey. "
            "They then fan the nectar until it ripens."
        ),
        question_type="explanation",
        normalized_query="How do bees produce honey?",
        evidence_rows=evidence_rows,
        runtime_config={"claim_verifier_min_supported_ratio": 0.66},
    )

    assert abs(result["supported_ratio"] - 0.6667) < 1e-4
    assert result["passed"] is True
    assert result["unsupported_claims"] == ["They then fan the nectar until it ripens."]


def test_lexical_grounding_remains_strict_for_fact() -> None:
    evidence_rows = [
        {
            "evidence_id": "ev-1",
            "text": "Queens lay eggs in brood cells.",
            "kind": "chunk",
        },
        {
            "evidence_id": "ev-2",
            "text": "Worker bees feed larvae in open brood.",
            "kind": "chunk",
        },
    ]

    result = agent_module._lexical_grounding_check(
        answer=(
            "Queens lay eggs in brood cells. "
            "Worker bees feed larvae in open brood. "
            "A queen lays 3000 eggs per day."
        ),
        question_type="fact",
        normalized_query="How many eggs does a queen lay?",
        evidence_rows=evidence_rows,
        runtime_config={"claim_verifier_min_supported_ratio": 0.66},
    )

    assert abs(result["supported_ratio"] - 0.6667) < 1e-4
    assert result["passed"] is False
    assert result["unsupported_claims"] == ["A queen lays 3000 eggs per day."]


def test_agent_query_uses_supported_subset_when_claim_verifier_is_partial(monkeypatch) -> None:
    repository = FakeAgentRepository()
    service = AgentService(repository=repository, store=FakeAgentStore(), embedder=FakeEmbedder())

    monkeypatch.setattr(
        service,
        "_generate_answer",
        lambda question, normalized_query, prompt_bundle, question_type, runtime_config: {
            "provider": "openai",
            "model": "gpt-5-mini",
            "prompt_version": "v2",
            "prompt_payload": {"question": question},
            "raw_payload": {"id": "resp-1"},
            "content": json.dumps(
                {
                    "answer": (
                        "Bees collect nectar from flowers and store it in a honey-stomach. "
                        "Workers pass nectar between house bees, which reduces moisture and turns it into honey. "
                        "Bees regurgitate the processed nectar from the honey-stomach into comb cells in the hive."
                    ),
                    "confidence": 0.86,
                    "abstained": False,
                    "abstain_reason": None,
                    "citations": [{"chunk_id": "chunk-1", "quote": "Honey bees produce honey."}],
                    "supporting_assertions": ["a-1"],
                    "supporting_entities": ["colony_colony", "honey_honey"],
                    "used_evidence_ids": ["ev-1", "ev-2"],
                }
            ),
        },
    )
    monkeypatch.setattr(
        agent_module,
        "_verify_answer_claims_with_model",
        lambda **kwargs: {
            "method": "claim_verifier",
            "passed": False,
            "supported_ratio": 0.8,
            "unsupported_claims": [
                "Bees regurgitate the processed nectar from the honey-stomach into comb cells in the hive."
            ],
            "claims": [
                {
                    "claim": "Bees collect nectar from flowers and store it in a honey-stomach.",
                    "supported": True,
                    "evidence_ids": ["ev-1"],
                },
                {
                    "claim": "Workers pass nectar between house bees, which reduces moisture and turns it into honey.",
                    "supported": True,
                    "evidence_ids": ["ev-2"],
                },
                {
                    "claim": "Bees regurgitate the processed nectar from the honey-stomach into comb cells in the hive.",
                    "supported": False,
                    "evidence_ids": [],
                },
            ],
            "provider": "openai",
            "model": "gpt-5.4-nano",
        },
    )

    result = service.query("How do bees produce honey?")

    assert result["abstained"] is False
    assert result["grounding_check"]["method"] == "supported_subset"
    assert result["grounding_check"]["original_method"] == "claim_verifier"
    assert result["grounding_check"]["passed"] is True
    assert result["grounding_check"]["supported_subset_used"] is True
    assert result["answer"].startswith("Bees collect nectar from flowers and store it in a honey-stomach.")
    assert "I can't be sure" not in result["answer"]
    assert result["review_status"] == "needs_review"


def test_agent_query_uses_supported_subset_when_fact_grounding_is_partial(monkeypatch) -> None:
    repository = FakeAgentRepository()
    service = AgentService(repository=repository, store=FakeAgentStore(), embedder=FakeEmbedder())

    monkeypatch.setattr(
        service,
        "_generate_answer",
        lambda question, normalized_query, prompt_bundle, question_type, runtime_config: {
            "provider": "openai",
            "model": "gpt-5-mini",
            "prompt_version": "v2",
            "prompt_payload": {"question": question},
            "raw_payload": {"id": "resp-1"},
            "content": json.dumps(
                {
                    "answer": (
                        "Varroa destructor is a parasitic mite that affects honey bees. "
                        "It also spreads rapidly between neighboring apiaries."
                    ),
                    "confidence": 0.83,
                    "abstained": False,
                    "abstain_reason": None,
                    "citations": [{"chunk_id": "chunk-1", "quote": "Honey bees produce honey."}],
                    "supporting_assertions": ["a-1"],
                    "supporting_entities": ["colony_colony", "honey_honey"],
                    "used_evidence_ids": ["ev-1"],
                }
            ),
        },
    )
    monkeypatch.setattr(
        agent_module,
        "_verify_answer_claims_with_model",
        lambda **kwargs: {
            "method": "claim_verifier",
            "passed": False,
            "supported_ratio": 0.5,
            "unsupported_claims": ["It also spreads rapidly between neighboring apiaries."],
            "claims": [
                {
                    "claim": "Varroa destructor is a parasitic mite that affects honey bees.",
                    "supported": True,
                    "evidence_ids": ["ev-1"],
                },
                {
                    "claim": "It also spreads rapidly between neighboring apiaries.",
                    "supported": False,
                    "evidence_ids": [],
                },
            ],
            "provider": "openai",
            "model": "gpt-5.4-nano",
        },
    )

    result = service.query("What is varroa destructor?")

    assert result["abstained"] is False
    assert result["grounding_check"]["method"] == "supported_subset"
    assert result["grounding_check"]["original_method"] == "claim_verifier"
    assert result["grounding_check"]["passed"] is True
    assert result["review_status"] == "needs_review"
    assert result["answer"].startswith("Varroa destructor is a parasitic mite")
    assert "Based on the indexed corpus" not in result["answer"]


def test_agent_prompt_budget_trims_context() -> None:
    repository = FakeAgentRepository()
    service = AgentService(repository=repository, store=FakeAgentStore(), embedder=FakeEmbedder())
    bundle = service._build_prompt_bundle(
        question="Explain how bees produce honey and maintain the hive during long nectar flows.",
        normalized_query="Explain how bees produce honey and maintain the hive during long nectar flows.",
        prior_messages=[
            {"role": "user", "content": "x" * 1600},
            {"role": "assistant", "content": "y" * 1600},
            {"role": "user", "content": "z" * 1600},
        ],
        profile_summary=None,
        session_memory=None,
        bundle=type(
            "Bundle",
            (),
            {
                "chunks": [
                    {
                        "chunk_id": f"chunk-{index}",
                        "document_id": "doc-1",
                        "chunk_index": index,
                        "page_start": 1,
                        "page_end": 1,
                        "metadata_json": {"section_title": "Preface", "chunk_role": "body"},
                        "text": "Honey bees maintain colony organization and process nectar into honey. " * 90,
                    }
                    for index in range(8)
                ],
                "assertions": [
                    {
                        "assertion_id": f"a-{index}",
                        "chunk_id": f"chunk-{index}",
                        "subject_entity_id": "colony_colony",
                        "predicate": "produces",
                        "object_entity_id": "honey_honey",
                        "object_literal": None,
                        "confidence": 0.9,
                    }
                    for index in range(8)
                ],
                "assets": [],
                "sensor_rows": [],
                "entities": [{"entity_id": f"entity-{index}", "canonical_name": "Honey", "entity_type": "HiveProduct"} for index in range(8)],
                "evidence": [{"evidence_id": f"ev-{index}", "assertion_id": f"a-{index}", "excerpt": "Honey bees produce honey. " * 30} for index in range(8)],
            },
        )(),
        question_type="procedure",
        runtime_config=agent_module.coerce_agent_runtime_config(agent_module.default_agent_runtime_config()),
    )

    assert bundle.stats["estimated_chars"]["total_prompt"] <= settings.agent_prompt_char_budget
    assert bundle.stats["trimmed"]["chunks"] >= 0


def test_coerce_memory_summary_requires_support_for_non_user_facts() -> None:
    summary = agent_module._coerce_memory_summary(
        {
            "summary_version": "v2",
            "session_goal": "Inspect honey production",
            "active_constraints": ["document_scope:doc-1"],
            "stable_facts": [
                {
                    "fact": "Honey is stored in comb cells.",
                    "source_type": "retrieval_grounded",
                    "chunk_ids": ["chunk-1"],
                    "asset_ids": [],
                    "assertion_ids": ["a-1"],
                    "evidence_ids": ["ev-1"],
                },
                {
                    "fact": "The user owns 40 hives.",
                    "source_type": "retrieval_grounded",
                    "chunk_ids": [],
                    "asset_ids": [],
                    "assertion_ids": [],
                    "evidence_ids": [],
                },
                {
                    "fact": "The user keeps bees in Attica.",
                    "source_type": "user_self_report",
                    "chunk_ids": [],
                    "asset_ids": [],
                    "assertion_ids": [],
                    "evidence_ids": [],
                },
            ],
            "open_threads": ["Need a robbing explanation"],
            "resolved_threads": [],
            "user_preferences": ["prefer_step_by_step"],
            "topic_keywords": ["honey"],
            "preferred_document_ids": ["doc-1"],
            "scope_signature": "scope:shared:doc-1",
            "last_query": "Explain honey storage",
        },
        max_facts=6,
        max_open_threads=6,
        max_resolved_threads=6,
        max_preferences=6,
        max_topics=8,
    )

    assert summary["summary_version"] == "v3"
    assert len(summary["stable_facts"]) == 2
    assert summary["stable_facts"][0]["evidence_ids"] == ["ev-1"]
    assert summary["stable_facts"][1]["source_type"] == "user_self_report"
    assert summary["active_constraints"][0]["kind"] == "document_scope"
    assert summary["user_preferences"][0]["preference"] == "prefer_step_by_step"
    assert summary["open_threads"][0]["thread"] == "Need a robbing explanation"


def test_fallback_session_memory_carries_typed_evidence_backed_facts() -> None:
    bundle = agent_module.AgentContextBundle(
        chunks=[
            {
                "chunk_id": "chunk-1",
                "document_id": "doc-1",
                "text": "Worker bees transform nectar into honey and store it in comb cells.",
            }
        ],
        assets=[],
        sensor_rows=[],
        assertions=[],
        evidence=[],
        entities=[],
        graph_chains=[],
        sources=[],
    )
    summary = agent_module._build_fallback_session_memory(
        question="Explain how bees turn nectar into honey.",
        normalized_query="Explain how bees turn nectar into honey.",
        response={
            "abstained": False,
            "question_type": "explanation",
            "used_chunk_ids": ["chunk-1"],
            "used_asset_ids": [],
            "used_evidence_ids": ["ev-1"],
            "supporting_assertions": ["a-1"],
        },
        bundle=bundle,
        request_scope={"tenant_id": "shared", "document_ids": ["doc-1"]},
        prior_summary=None,
        recent_messages=[],
        runtime_config=agent_module.coerce_agent_runtime_config(agent_module.default_agent_runtime_config()),
    )

    fact = summary["stable_facts"][0]
    assert fact["source_type"] == "retrieval_grounded"
    assert fact["evidence_ids"] == ["ev-1"]
    assert summary["open_threads"] == []
    assert summary["active_constraints"][0]["kind"] in {"document_scope", "scope_signature"}


def test_fallback_session_memory_prunes_stale_open_threads_and_inferred_preferences() -> None:
    summary = agent_module._build_fallback_session_memory(
        question="Why is robbing pressure increasing?",
        normalized_query="Why is robbing pressure increasing?",
        response={
            "abstained": True,
            "question_type": "explanation",
            "used_chunk_ids": [],
            "used_asset_ids": [],
            "used_evidence_ids": [],
            "supporting_assertions": [],
        },
        bundle=agent_module.AgentContextBundle(
            chunks=[],
            assets=[],
            sensor_rows=[],
            assertions=[],
            evidence=[],
            entities=[],
            graph_chains=[],
            sources=[],
        ),
        request_scope={"tenant_id": "shared", "document_ids": []},
        prior_summary={
            "summary_json": {
                "summary_version": "v3",
                "session_goal": "robbing diagnosis",
                "active_constraints": [],
                "stable_facts": [],
                "open_threads": [
                    {"thread": "Need varroa treatment plan", "source": "abstained_turn", "source_query": "varroa treatment", "question_type": "procedure", "expiry_policy": "short_session"},
                    {"thread": "Need winter feeding advice", "source": "abstained_turn", "source_query": "winter feeding", "question_type": "procedure", "expiry_policy": "short_session"},
                    {"thread": "Explain robbing pressure", "source": "abstained_turn", "source_query": "robbing pressure", "question_type": "explanation", "expiry_policy": "short_session"},
                ],
                "resolved_threads": [],
                "user_preferences": [
                    {"preference": "prefer_brief_answers", "source": "interaction_pattern"},
                    {"preference": "prefer_explicit_citations", "source": "user"},
                ],
                "topic_keywords": [],
                "preferred_document_ids": [],
                "scope_signature": "",
                "last_query": "robbing pressure",
            }
        },
        recent_messages=[],
        runtime_config=agent_module.coerce_agent_runtime_config(agent_module.default_agent_runtime_config()),
    )

    open_threads = [item["thread"] for item in summary["open_threads"]]
    preferences = [item["preference"] for item in summary["user_preferences"]]

    assert "Explain robbing pressure" in open_threads
    assert "Need varroa treatment plan" in open_threads
    assert "Need winter feeding advice" not in open_threads
    assert "prefer_explicit_citations" in preferences
    assert "prefer_brief_answers" not in preferences


def test_budget_session_summary_omits_duplicate_summary_text_from_prompt() -> None:
    summary = {
        "summary_json": {
            "summary_version": "v3",
            "session_goal": "Inspect honey production",
            "active_constraints": [{"constraint": "document_scope:doc-1", "kind": "document_scope", "source": "request_scope"}],
            "stable_facts": [
                {
                    "fact": "Honey is stored in comb cells.",
                    "fact_type": "grounded_fact",
                    "source_type": "retrieval_grounded",
                    "confidence": 0.82,
                    "review_policy": "revalidate_on_missing_support",
                    "chunk_ids": ["chunk-1"],
                    "asset_ids": [],
                    "assertion_ids": ["a-1"],
                    "evidence_ids": ["ev-1"],
                }
            ],
            "open_threads": [],
            "resolved_threads": [],
            "user_preferences": [{"preference": "prefer_step_by_step", "source": "interaction_pattern"}],
            "topic_keywords": ["honey"],
            "preferred_document_ids": ["doc-1"],
            "scope_signature": "scope:shared:doc-1",
            "last_query": "Explain honey storage",
            "summary_text": "duplicate summary text that should not go into the prompt payload",
        },
        "summary_text": "duplicate summary text that should not go into the prompt payload",
    }

    payload = agent_module._budget_session_summary(summary, char_budget=4000)

    assert payload is not None
    assert "summary_text" not in payload
    assert payload["user_preferences"][0]["preference"] == "prefer_step_by_step"


def test_coerce_profile_summary_uses_typed_items() -> None:
    profile = agent_module._coerce_profile_summary(
        {
            "summary_version": "v2",
            "user_background": "",
            "beekeeping_context": "",
            "experience_level": "advanced",
            "communication_style": "concise",
            "answer_preferences": ["prefer_explicit_citations"],
            "recurring_topics": ["swarming"],
            "learning_goals": ["wintering"],
            "persistent_constraints": ["document_scope:doc-2"],
            "preferred_document_ids": ["doc-2"],
            "last_query": "How should I prepare hives for winter?",
        }
    )

    assert profile["summary_version"] == "v3"
    assert profile["answer_preferences"][0]["preference"] == "prefer_explicit_citations"
    assert profile["recurring_topics"][0]["topic"] == "swarming"
    assert profile["learning_goals"][0]["goal"] == "wintering"
    assert profile["persistent_constraints"][0]["constraint"] == "document_scope:doc-2"


def test_fallback_profile_drops_unreaffirmed_inferred_preferences_and_topics() -> None:
    profile = agent_module._build_fallback_agent_profile(
        question="How should I prepare hives for winter?",
        normalized_query="How should I prepare hives for winter?",
        response={"abstained": False},
        session_memory=None,
        prior_profile={
            "summary_json": {
                "summary_version": "v3",
                "user_background": "commercial beekeeper",
                "beekeeping_context": "",
                "experience_level": "advanced",
                "communication_style": "direct",
                "answer_preferences": [
                    {"preference": "prefer_technical_depth", "source": "interaction_pattern"},
                    {"preference": "prefer_explicit_citations", "source": "user"},
                ],
                "recurring_topics": [
                    {"topic": "swarming", "source": "interaction_pattern"},
                    {"topic": "varroa", "source": "profile_history"},
                ],
                "learning_goals": [],
                "persistent_constraints": [],
                "preferred_document_ids": [],
                "last_query": "queen rearing",
            }
        },
        recent_messages=[],
        runtime_config=agent_module.coerce_agent_runtime_config(agent_module.default_agent_runtime_config()),
    )

    preferences = [item["preference"] for item in profile["answer_preferences"]]
    topics = [item["topic"] for item in profile["recurring_topics"]]

    assert "prefer_explicit_citations" in preferences
    assert "prefer_technical_depth" not in preferences
    assert "varroa" in topics
    assert "swarming" not in topics


def test_prompt_bundle_filters_irrelevant_memory_slices() -> None:
    service = AgentService(repository=FakeAgentRepository(), store=FakeAgentStore(), embedder=FakeEmbedder())
    bundle = agent_module.AgentContextBundle(
        chunks=[
            {
                "chunk_id": "chunk-1",
                "document_id": "doc-1",
                "text": "Late summer nectar dearth can increase robbing pressure in weak colonies.",
            }
        ],
        assets=[],
        sensor_rows=[],
        assertions=[
            {
                "assertion_id": "a-1",
                "chunk_id": "chunk-1",
                "subject_entity_id": "robbing_pressure",
                "predicate": "increases_during",
                "object_literal": "late summer dearth",
            }
        ],
        evidence=[
            {
                "evidence_id": "ev-1",
                "assertion_id": "a-1",
                "excerpt": "Late summer nectar dearth can increase robbing pressure.",
            }
        ],
        entities=[],
        graph_chains=[],
        sources=[],
    )
    prompt_bundle = service._build_prompt_bundle(
        question="Why does robbing pressure increase in late summer?",
        normalized_query="Why does robbing pressure increase in late summer?",
        prior_messages=[],
        profile_summary={
            "summary_json": {
                "summary_version": "v3",
                "user_background": "commercial beekeeper",
                "beekeeping_context": "",
                "experience_level": "advanced",
                "communication_style": "direct",
                "answer_preferences": [{"preference": "prefer_explicit_citations", "source": "user"}],
                "recurring_topics": [
                    {"topic": "robbing", "source": "interaction_pattern"},
                    {"topic": "swarming", "source": "interaction_pattern"},
                ],
                "learning_goals": [
                    {"goal": "understand robbing", "source": "user"},
                    {"goal": "improve queen rearing", "source": "user"},
                ],
                "persistent_constraints": [],
                "preferred_document_ids": [],
                "last_query": "queen rearing",
            }
        },
        session_memory={
            "summary_json": {
                "summary_version": "v3",
                "session_goal": "diagnose robbing",
                "active_constraints": [],
                "stable_facts": [
                    {
                        "fact": "Late summer dearth increases robbing pressure.",
                        "fact_type": "grounded_fact",
                        "source_type": "retrieval_grounded",
                        "confidence": 0.95,
                        "review_policy": "revalidate_on_missing_support",
                        "chunk_ids": ["chunk-1"],
                        "asset_ids": [],
                        "assertion_ids": ["a-1"],
                        "evidence_ids": ["ev-1"],
                    },
                    {
                        "fact": "Queens usually swarm in spring.",
                        "fact_type": "grounded_fact",
                        "source_type": "retrieval_grounded",
                        "confidence": 0.9,
                        "review_policy": "revalidate_on_missing_support",
                        "chunk_ids": ["chunk-2"],
                        "asset_ids": [],
                        "assertion_ids": ["a-2"],
                        "evidence_ids": ["ev-2"],
                    },
                ],
                "open_threads": [
                    {"thread": "Explain robbing pressure", "source": "abstained_turn", "source_query": "robbing pressure", "question_type": "explanation", "expiry_policy": "short_session"},
                    {"thread": "Explain queen rearing", "source": "abstained_turn", "source_query": "queen rearing", "question_type": "procedure", "expiry_policy": "short_session"},
                ],
                "resolved_threads": [
                    {"thread": "resolved: queen rearing", "source": "answered_turn", "source_query": "queen rearing", "question_type": "procedure", "expiry_policy": "resolved_session"},
                ],
                "user_preferences": [{"preference": "prefer_explicit_citations", "source": "user"}],
                "topic_keywords": ["robbing", "queen"],
                "preferred_document_ids": [],
                "scope_signature": "",
                "last_query": "queen rearing",
            }
        },
        bundle=bundle,
        question_type="explanation",
        runtime_config=agent_module.coerce_agent_runtime_config(agent_module.default_agent_runtime_config()),
        workspace_kind="hive",
    )

    session_summary = prompt_bundle.session_summary or {}
    profile_summary = prompt_bundle.profile_summary or {}

    assert [item["fact"] for item in session_summary["stable_facts"]] == ["Late summer dearth increases robbing pressure."]
    assert [item["thread"] for item in session_summary["open_threads"]] == ["Explain robbing pressure"]
    assert session_summary["resolved_threads"] == []
    assert session_summary["topic_keywords"] == ["robbing"]
    assert [item["topic"] for item in profile_summary["recurring_topics"]] == ["robbing"]
    assert [item["goal"] for item in profile_summary["learning_goals"]] == ["understand robbing"]
    assert prompt_bundle.stats["memory_relevance"]["session_facts"] == 1
    assert prompt_bundle.stats["memory_relevance"]["profile_topics"] == 1


def test_general_prompt_bundle_strips_shared_profile_content_memory() -> None:
    repository = FakeAgentRepository()
    service = AgentService(repository=repository, store=FakeAgentStore(), embedder=FakeEmbedder())
    bundle = agent_module.AgentContextBundle(chunks=[], assets=[], sensor_rows=[], assertions=[], entities=[], graph_chains=[], evidence=[], sources=[])
    prompt_bundle = service._build_prompt_bundle(
        question="What are signs of swarming?",
        normalized_query="What are signs of swarming?",
        prior_messages=[],
        profile_summary={
            "summary_json": {
                "summary_version": "v3",
                "user_background": "",
                "beekeeping_context": "",
                "experience_level": "advanced",
                "communication_style": "direct",
                "answer_preferences": [{"preference": "state uncertainty when evidence is insufficient", "source": "interaction_pattern"}],
                "recurring_topics": [
                    {"topic": "nosema", "source": "interaction_pattern"},
                    {"topic": "swarming", "source": "interaction_pattern"},
                ],
                "learning_goals": [
                    {"goal": "understand nosema management", "source": "interaction_pattern"},
                ],
                "persistent_constraints": [{"constraint": "scope:shared:all", "source": "system"}],
                "preferred_document_ids": [],
                "last_query": "nosema treatment",
            }
        },
        session_memory=None,
        bundle=bundle,
        question_type="fact",
        runtime_config=agent_module.coerce_agent_runtime_config(agent_module.default_agent_runtime_config()),
        workspace_kind="general",
    )

    profile_summary = prompt_bundle.profile_summary or {}

    assert profile_summary.get("answer_preferences") == []
    assert profile_summary.get("recurring_topics") == []
    assert profile_summary.get("learning_goals") == []
    assert profile_summary.get("communication_style") == "direct"
    assert prompt_bundle.stats["profile_used"] is True
    assert len((prompt_bundle.profile_summary or {}).get("recurring_topics") or []) == 0


def test_agent_query_rejects_busy_session() -> None:
    repository = FakeAgentRepository()
    session_id = repository.create_agent_session()
    repository.sessions[session_id]["claimed_by"] = "other-worker"
    service = AgentService(repository=repository, store=FakeAgentStore(), embedder=FakeEmbedder())

    try:
        service.query("How do bees produce honey?", session_id=session_id)
    except ValueError as exc:
        assert str(exc) == "Session is busy"
    else:
        raise AssertionError("Expected busy-session query to fail")


def test_agent_query_falls_back_to_last_resort_answer(monkeypatch) -> None:
    repository = FakeAgentRepository()
    service = AgentService(repository=repository, store=FakeAgentStore(), embedder=FakeEmbedder())

    monkeypatch.setattr(service, "_generate_answer", lambda *args, **kwargs: (_ for _ in ()).throw(AgentQueryError("upstream timeout")))
    monkeypatch.setattr(service, "_generate_open_world_answer", lambda **kwargs: (_ for _ in ()).throw(AgentQueryError("open-world unavailable")))

    result = service.query("How do bees produce honey?")

    assert result["abstained"] is False
    assert result["abstain_reason"] is None
    assert result["fallback_used"] is True
    assert result["review_reason"] == "last_resort_fallback"
    assert result["grounding_check"]["last_resort_fallback_used"] is True
    assert result["answer"].startswith("Honey bees produce honey")
    assert repository.query_runs[0]["provider"] == "fallback"


def test_build_fallback_response_cleans_excerpt_formatting() -> None:
    repository = FakeAgentRepository()
    service = AgentService(repository=repository, store=FakeAgentStore(), embedder=FakeEmbedder())
    bundle = agent_module.AgentContextBundle(
        chunks=[
            {
                "chunk_id": "chunk-1",
                "document_id": "doc-1",
                "page_start": 1,
                "page_end": 1,
                "metadata_json": {"section_title": "Caron)"},
                "text": "Caron) Most swarming in the Mid-Atlantic region occurs during May and June. Swarm management should begin in April and continue through May.",
            }
        ],
        assets=[],
        sensor_rows=[],
        assertions=[],
        graph_chains=[],
        evidence=[],
        entities=[],
        sources=[],
    )
    runtime_config = agent_module.coerce_agent_runtime_config(agent_module.default_agent_runtime_config())
    runtime_config["citation_excerpt_chars"] = 80

    result = service._build_fallback_response(bundle, "upstream timeout", runtime_config)

    assert "Caron): Caron)" not in result["answer"]
    assert "- Caron): Most swarming in the Mid-Atlantic region" in result["answer"]
    assert "..." in result["answer"]


def test_agent_query_supports_kg_evidence_citations(monkeypatch) -> None:
    repository = FakeAgentRepository()
    service = AgentService(repository=repository, store=FakeAgentStore(), embedder=FakeEmbedder())

    monkeypatch.setattr(
        service,
        "_generate_answer",
        lambda question, normalized_query, prompt_bundle, question_type, runtime_config: {
            "provider": "openai",
            "model": "gpt-5-mini",
            "prompt_version": "v1",
            "prompt_payload": {"question": question},
            "raw_payload": {"id": "resp-2"},
            "content": json.dumps(
                {
                    "answer": "Honey bees produce honey.",
                    "confidence": 0.84,
                    "abstained": False,
                    "abstain_reason": None,
                    "used_chunk_ids": [],
                    "used_asset_ids": [],
                    "used_sensor_row_ids": [],
                    "used_evidence_ids": ["ev-1"],
                    "supporting_assertions": ["a-1"],
                    "supporting_entities": ["colony_colony", "honey_honey"],
                }
            ),
        },
    )

    result = service.query("How do bees produce honey?")

    assert result["abstained"] is False
    assert result["used_evidence_ids"] == ["ev-1"]
    assert any(item["citation_kind"] == "kg_evidence" and item["evidence_id"] == "ev-1" for item in result["citations"])
    assert repository.query_runs[0]["assistant_metadata"]["evidence_citations"] == ["ev-1"]


def test_coerce_agent_response_reads_evidence_ids_from_citations() -> None:
    result = agent_module._coerce_agent_response(
        json.dumps(
            {
                "answer": "Honey bees produce honey.",
                "confidence": 0.5,
                "abstained": False,
                "abstain_reason": None,
                "used_chunk_ids": [],
                "used_asset_ids": [],
                "used_sensor_row_ids": [],
                "supporting_assertions": [],
                "supporting_entities": [],
                "citations": [{"evidence_id": "ev-9"}],
            }
        )
    )

    assert result["used_evidence_ids"] == ["ev-9"]


def test_retrieval_plan_enables_kg_for_procedure_without_asset_vector_search() -> None:
    runtime_config = agent_module.coerce_agent_runtime_config(agent_module.default_agent_runtime_config())
    plan = agent_module._select_retrieval_plan(
        question_type="procedure",
        top_k=6,
        runtime_config=runtime_config,
        routing={"requires_visual": False, "document_spread": "few"},
    )

    assert plan["kg_search"] is True
    assert plan["asset_vector_search"] is False


def test_inspect_retrieval_skips_asset_vector_search_for_nonvisual_procedure(monkeypatch) -> None:
    class CountingStore(FakeAgentStore):
        def __init__(self) -> None:
            self.asset_search_calls = 0

        def search_assets(self, query_embedding, top_k=8, tenant_id=None, document_ids=None):
            self.asset_search_calls += 1
            return super().search_assets(query_embedding, top_k=top_k, tenant_id=tenant_id, document_ids=document_ids)

    repository = FakeAgentRepository()
    store = CountingStore()
    service = AgentService(repository=repository, store=store, embedder=FakeEmbedder())

    monkeypatch.setattr(
        agent_module,
        "route_question_cached",
        lambda *args, **kwargs: {
            "question_type": "procedure",
            "top_k": 4,
            "source": "router",
            "requires_visual": False,
            "document_spread": "few",
        },
    )

    result = service.inspect_retrieval("How do bees produce honey?")

    assert result["question_type"] == "procedure"
    assert result["retrieval_plan"]["kg_search"] is True
    assert result["retrieval_plan"]["asset_vector_search"] is False
    assert store.asset_search_calls == 0


def test_inspect_retrieval_includes_synopsis_context_in_chunk_payload() -> None:
    repository = FakeAgentRepository()
    service = AgentService(repository=repository, store=FakeAgentStore(), embedder=FakeEmbedder())

    result = service.inspect_retrieval("How do bees produce honey?")

    assert result["prompt_chunk_payload"]
    payload = result["prompt_chunk_payload"][0]
    assert "section_synopsis" in payload
    assert "document_synopsis" in payload
    assert payload["section_synopsis_title"] == "Preface"


def test_inspect_retrieval_can_expand_candidates_from_synopsis_hits() -> None:
    class SynopsisGuidedRepository(FakeAgentRepository):
        def search_section_synopses_lexical(self, query_text, tenant_id="shared", document_ids=None, limit=12):
            return [
                {
                    "section_id": "doc-2:section:process",
                    "document_id": "doc-2",
                    "tenant_id": "shared",
                    "section_path": ["Process"],
                    "section_title": "Process",
                    "synopsis_text": "The section explains how bees transform nectar into honey inside the hive.",
                    "accepted_chunk_count": 1,
                    "section_level": 1,
                    "lexical_score": 28.0,
                }
            ]

        def list_chunk_records(self, document_id=None, status=None, limit=50, offset=0):
            if document_id != "doc-2":
                return []
            return [
                {
                    "chunk_id": "chunk-guided-1",
                    "document_id": "doc-2",
                    "tenant_id": "shared",
                    "chunk_index": 0,
                    "page_start": 3,
                    "page_end": 3,
                    "section_path": ["Process"],
                    "prev_chunk_id": None,
                    "next_chunk_id": None,
                    "char_start": 0,
                    "char_end": 120,
                    "content_type": "text",
                    "text": "Worker bees repeatedly concentrate nectar and fan it until honey can be stored in comb cells.",
                    "parser_version": "v1",
                    "chunker_version": "v1",
                    "metadata_json": {"section_title": "Process"},
                    "kg_assertion_count": 0,
                    "validation_status": status or "accepted",
                    "quality_score": 1.0,
                    "reasons": ["ok"],
                }
            ]

        def list_document_synopses_by_ids(self, document_ids):
            return [
                {
                    "document_id": "doc-2",
                    "tenant_id": "shared",
                    "title": "process.txt",
                    "synopsis_text": "This document covers the honey-making process.",
                    "accepted_chunk_count": 1,
                    "section_count": 1,
                    "source_stage": "chunks_validated",
                    "synopsis_version": "extractive-v1",
                    "metadata_json": {},
                }
            ]

        def list_section_synopses_for_chunk_ids(self, chunk_ids, limit=24):
            return [
                {
                    "section_id": "doc-2:section:process",
                    "document_id": "doc-2",
                    "tenant_id": "shared",
                    "parent_section_id": None,
                    "section_path": ["Process"],
                    "section_level": 1,
                    "section_title": "Process",
                    "page_start": 3,
                    "page_end": 3,
                    "char_start": 0,
                    "char_end": 120,
                    "first_chunk_id": "chunk-guided-1",
                    "last_chunk_id": "chunk-guided-1",
                    "accepted_chunk_count": 1,
                    "total_chunk_count": 1,
                    "synopsis_text": "The section explains how bees transform nectar into honey inside the hive.",
                    "synopsis_version": "extractive-v1",
                    "metadata_json": {},
                }
            ]

    service = AgentService(repository=SynopsisGuidedRepository(), store=FakeAgentStore(), embedder=FakeEmbedder())

    result = service.inspect_retrieval("Explain how bees turn nectar into honey.")

    guided_chunk = next(chunk for chunk in result["chunks"] if chunk["chunk_id"] == "chunk-guided-1")
    assert guided_chunk["_synopsis_guided"] == "section"
    assert any(item["chunk_id"] == "chunk-guided-1" for item in result["prompt_chunk_payload"])


def test_inspect_retrieval_builds_bounded_graph_chains_for_explanations() -> None:
    class GraphRepository(FakeAgentRepository):
        def list_kg_assertions_for_chunks(self, chunk_ids, limit=200, per_chunk_limit=None):
            return [
                {
                    "assertion_id": "a-1",
                    "document_id": "doc-1",
                    "chunk_id": chunk_ids[0],
                    "subject_entity_id": "nectar_nectar",
                    "predicate": "transformed_into",
                    "object_entity_id": "honey_honey",
                    "object_literal": None,
                    "confidence": 0.93,
                    "qualifiers": {},
                    "status": "accepted",
                }
            ]

        def list_kg_neighbor_assertions_for_entities(
            self,
            entity_ids,
            tenant_id="shared",
            document_ids=None,
            exclude_assertion_ids=None,
            limit=16,
            per_entity_limit=None,
        ):
            return [
                {
                    "seed_entity_id": "honey_honey",
                    "assertion_id": "a-2",
                    "document_id": "doc-1",
                    "chunk_id": "chunk-graph-2",
                    "subject_entity_id": "honey_honey",
                    "predicate": "stored_in",
                    "object_entity_id": "comb_comb",
                    "object_literal": None,
                    "confidence": 0.84,
                    "qualifiers": {},
                    "status": "accepted",
                    "neighbor_entity_id": "comb_comb",
                    "evidence_count": 1,
                }
            ]

        def list_kg_evidence_for_assertions(self, assertion_ids, limit=200, per_assertion_limit=None):
            rows = []
            for assertion_id in assertion_ids:
                excerpt = {
                    "a-1": "Worker bees transform nectar into honey.",
                    "a-2": "Honey is stored in comb cells for the colony.",
                }.get(assertion_id, "Evidence excerpt.")
                rows.append(
                    {
                        "evidence_id": f"ev-{assertion_id}",
                        "assertion_id": assertion_id,
                        "excerpt": excerpt,
                        "start_offset": 0,
                        "end_offset": len(excerpt),
                    }
                )
            return rows

        def list_kg_entities_by_ids(self, entity_ids):
            names = {
                "nectar_nectar": ("Nectar", "Substance"),
                "honey_honey": ("Honey", "Substance"),
                "comb_comb": ("Comb cell", "HiveStructure"),
            }
            return [
                {
                    "entity_id": entity_id,
                    "canonical_name": names.get(entity_id, (entity_id, "Thing"))[0],
                    "entity_type": names.get(entity_id, (entity_id, "Thing"))[1],
                }
                for entity_id in entity_ids
            ]

    service = AgentService(repository=GraphRepository(), store=FakeAgentStore(), embedder=FakeEmbedder())

    result = service.inspect_retrieval("Explain how bees turn nectar into honey.")

    assert result["graph_chains"]
    assert result["prompt_graph_chain_payload"]
    assert any(item["assertion_id"] == "a-2" for item in result["prompt_assertion_payload"])
    chain = result["prompt_graph_chain_payload"][0]
    assert chain["supporting_assertion_ids"] == ["a-1", "a-2"]


def test_inspect_retrieval_uses_chunk_aware_kg_quota(monkeypatch) -> None:
    class QuotaRepository(FakeAgentRepository):
        def __init__(self) -> None:
            super().__init__()
            self.last_per_chunk_limit = None

        def list_kg_assertions_for_chunks(self, chunk_ids, limit=200, per_chunk_limit=None):
            self.last_per_chunk_limit = per_chunk_limit
            rows = []
            for index, chunk_id in enumerate(chunk_ids):
                rows.append(
                    {
                        "assertion_id": f"a-{index}",
                        "document_id": "doc-1",
                        "chunk_id": chunk_id,
                        "subject_entity_id": "colony_colony",
                        "predicate": "produces",
                        "object_entity_id": "honey_honey",
                        "object_literal": None,
                        "confidence": 0.9 - (index * 0.01),
                        "qualifiers": {},
                        "status": "accepted",
                    }
                )
            return rows

    class MultiChunkStore(FakeAgentStore):
        def search(self, query_embedding, top_k=8, tenant_id=None, document_ids=None):
            return [
                {
                    "chunk_id": "chunk-1",
                    "document": "Honey bees produce honey and maintain the hive.",
                    "metadata": {"document_id": "doc-1", "section_title": "Preface"},
                    "distance": 0.08,
                    "rank": 1,
                },
                {
                    "chunk_id": "chunk-2",
                    "document": "Workers process nectar and store honey in comb.",
                    "metadata": {"document_id": "doc-1", "section_title": "Chapter 1"},
                    "distance": 0.09,
                    "rank": 2,
                },
            ]

    repository = QuotaRepository()
    service = AgentService(repository=repository, store=MultiChunkStore(), embedder=FakeEmbedder())

    monkeypatch.setattr(
        agent_module,
        "route_question_cached",
        lambda *args, **kwargs: {
            "question_type": "explanation",
            "top_k": 6,
            "source": "router",
            "requires_visual": False,
            "document_spread": "few",
        },
    )

    result = service.inspect_retrieval("Explain how bees turn nectar into honey.")

    assert result["question_type"] == "explanation"
    assert repository.last_per_chunk_limit is not None
    assert repository.last_per_chunk_limit >= 1


def test_inspect_retrieval_uses_lexical_chunk_candidates_when_dense_empty(monkeypatch) -> None:
    class EmptyDenseStore(FakeAgentStore):
        def search(self, query_embedding, top_k=8, tenant_id=None, document_ids=None):
            return []

    class LexicalRepository(FakeAgentRepository):
        def search_chunk_records_lexical(self, query_text, tenant_id="shared", document_ids=None, limit=12):
            return [
                {
                    "chunk_id": "chunk-lex",
                    "document": "Varroa destructor is a parasitic mite affecting honey bees.",
                    "metadata": {"document_id": "doc-1", "section_title": "Pests", "title": "Varroa"},
                    "distance": None,
                    "rank": 1,
                    "lexical_score": 42.0,
                    "match_source": "lexical",
                }
            ]

    repository = LexicalRepository()
    service = AgentService(repository=repository, store=EmptyDenseStore(), embedder=FakeEmbedder())

    result = service.inspect_retrieval("What is varroa destructor?")

    assert result["raw_chunk_matches"]
    assert result["raw_chunk_matches"][0]["chunk_id"] == "chunk-lex"
    assert "lexical" in result["raw_chunk_matches"][0]["match_sources"]
    assert result["chunks"]
    assert result["chunks"][0]["chunk_id"] == "chunk-lex"


def test_inspect_retrieval_survives_embedding_failure_with_lexical_fallback(monkeypatch) -> None:
    class LexicalRepository(FakeAgentRepository):
        def search_chunk_records_lexical(self, query_text, tenant_id="shared", document_ids=None, limit=12):
            return [
                {
                    "chunk_id": "chunk-1",
                    "document": "Honey bees produce honey and maintain the hive.",
                    "metadata": {"document_id": "doc-1", "section_title": "Preface"},
                    "distance": None,
                    "rank": 1,
                    "lexical_score": 20.0,
                    "match_source": "lexical",
                }
            ]

    repository = LexicalRepository()
    service = AgentService(repository=repository, store=FakeAgentStore(), embedder=FakeEmbedder())
    monkeypatch.setattr(service, "_embed_query_or_raise", lambda normalized_query: (_ for _ in ()).throw(AgentQueryError("embedding unavailable")))

    result = service.inspect_retrieval("How do bees produce honey?")

    assert result["embedding_fallback_used"] is True
    assert result["embedding_error"] == "embedding unavailable"
    assert result["chunks"]


def test_inspect_retrieval_reuses_cached_query_embedding() -> None:
    class CountingEmbedder(FakeEmbedder):
        def __init__(self) -> None:
            self.calls = 0

        def embed(self, texts):
            self.calls += len(texts)
            return super().embed(texts)

    repository = FakeAgentRepository()
    embedder = CountingEmbedder()
    service = AgentService(repository=repository, store=FakeAgentStore(), embedder=embedder)

    first = service.inspect_retrieval("How do bees produce honey?")
    second = service.inspect_retrieval("How do bees produce honey?")

    assert first["raw_chunk_matches"]
    assert second["raw_chunk_matches"]
    assert embedder.calls == 1
    cached_rows = list(repository.embedding_cache.values())
    assert len(cached_rows) == 1
    assert cached_rows[0]["cache_hits"] == 1


def test_inspect_retrieval_uses_lexical_asset_candidates_for_visual_query(monkeypatch) -> None:
    class NoAssetDenseStore(FakeAgentStore):
        def search_assets(self, query_embedding, top_k=8, tenant_id=None, document_ids=None):
            return []

    class LexicalAssetRepository(FakeAgentRepository):
        def search_page_assets_lexical(self, query_text, tenant_id="shared", document_ids=None, limit=8):
            return [
                {
                    "asset_id": "asset-lex",
                    "document": "Detailed honey bee anatomy diagram with labeled abdomen and thorax.",
                    "metadata": {"document_id": "doc-1", "label": "Figure anatomy", "asset_type": "figure", "page_number": 1},
                    "distance": None,
                    "rank": 1,
                    "lexical_score": 18.0,
                    "match_source": "lexical",
                }
            ]

    repository = LexicalAssetRepository()
    store = NoAssetDenseStore()
    service = AgentService(repository=repository, store=store, embedder=FakeEmbedder())

    monkeypatch.setattr(
        agent_module,
        "route_question_cached",
        lambda *args, **kwargs: {
            "question_type": "visual_lookup",
            "top_k": 4,
            "source": "router",
            "requires_visual": True,
            "document_spread": "few",
        },
    )

    result = service.inspect_retrieval("Show me the honey bee anatomy diagram.")

    assert result["assets"]
    assert any(asset["asset_id"] == "asset-lex" for asset in result["assets"])
    assert any(source.get("source_kind") == "asset_hit" and source.get("asset_id") == "asset-lex" for source in result["sources"])
