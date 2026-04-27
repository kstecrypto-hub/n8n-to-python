"""HTTP-layer dependency ownership for the serving app."""

from __future__ import annotations

from typing import Callable

from src.bee_ingestion.agent import AgentService
from src.bee_ingestion.auth_store import AuthStore
from src.bee_ingestion.chroma_store import ChromaStore
from src.bee_ingestion.rate_limit import SlidingWindowRateLimiter
from src.bee_ingestion.repository import Repository
from src.bee_ingestion.service import IngestionService
from src.bee_ingestion.settings import settings
from src.bee_ingestion.storage.bootstrap import ensure_app_storage_compatibility


class LazyProxy:
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

    def get_agent_session(self, *args, **kwargs):
        return self._call("get_agent_session", *args, **kwargs)

    def set_agent_session_token(self, *args, **kwargs):
        return self._call("set_agent_session_token", *args, **kwargs)

    def get_agent_session_memory(self, *args, **kwargs):
        return self._call("get_agent_session_memory", *args, **kwargs)

    def update_agent_session_memory_record(self, *args, **kwargs):
        return self._call("update_agent_session_memory_record", *args, **kwargs)

    def get_agent_profile(self, *args, **kwargs):
        return self._call("get_agent_profile", *args, **kwargs)

    def update_agent_profile_record(self, *args, **kwargs):
        return self._call("update_agent_profile_record", *args, **kwargs)

    def list_agent_messages(self, *args, **kwargs):
        return self._call("list_agent_messages", *args, **kwargs)

    def list_agent_query_runs(self, *args, **kwargs):
        return self._call("list_agent_query_runs", *args, **kwargs)

    def get_agent_query_detail(self, *args, **kwargs):
        return self._call("get_agent_query_detail", *args, **kwargs)

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

    def list_admin_relations(self, *args, **kwargs):
        return self._call("list_admin_relations", *args, **kwargs)

    def list_admin_relation_rows(self, *args, **kwargs):
        return self._call("list_admin_relation_rows", *args, **kwargs)

    def get_document_detail(self, *args, **kwargs):
        return self._call("get_document_detail", *args, **kwargs)

    def list_documents(self, *args, **kwargs):
        return self._call("list_documents", *args, **kwargs)

    def count_documents(self, *args, **kwargs):
        return self._call("count_documents", *args, **kwargs)

    def list_document_sources(self, *args, **kwargs):
        return self._call("list_document_sources", *args, **kwargs)

    def list_document_pages(self, *args, **kwargs):
        return self._call("list_document_pages", *args, **kwargs)

    def list_page_assets(self, *args, **kwargs):
        return self._call("list_page_assets", *args, **kwargs)

    def list_chunk_asset_links(self, *args, **kwargs):
        return self._call("list_chunk_asset_links", *args, **kwargs)

    def list_chunks(self, *args, **kwargs):
        return self._call("list_chunks", *args, **kwargs)

    def count_chunks(self, *args, **kwargs):
        return self._call("count_chunks", *args, **kwargs)

    def get_chunk_detail(self, *args, **kwargs):
        return self._call("get_chunk_detail", *args, **kwargs)

    def list_chunk_metadata(self, *args, **kwargs):
        return self._call("list_chunk_metadata", *args, **kwargs)

    def count_chunk_metadata(self, *args, **kwargs):
        return self._call("count_chunk_metadata", *args, **kwargs)

    def list_kg_assertions(self, *args, **kwargs):
        return self._call("list_kg_assertions", *args, **kwargs)

    def count_kg_assertions(self, *args, **kwargs):
        return self._call("count_kg_assertions", *args, **kwargs)

    def list_kg_entities(self, *args, **kwargs):
        return self._call("list_kg_entities", *args, **kwargs)

    def count_kg_entities(self, *args, **kwargs):
        return self._call("count_kg_entities", *args, **kwargs)

    def get_kg_entity_detail(self, *args, **kwargs):
        return self._call("get_kg_entity_detail", *args, **kwargs)

    def list_kg_evidence(self, *args, **kwargs):
        return self._call("list_kg_evidence", *args, **kwargs)

    def list_kg_raw_extractions(self, *args, **kwargs):
        return self._call("list_kg_raw_extractions", *args, **kwargs)

    def count_kg_raw_extractions(self, *args, **kwargs):
        return self._call("count_kg_raw_extractions", *args, **kwargs)

    def get_document_related_counts(self, *args, **kwargs):
        return self._call("get_document_related_counts", *args, **kwargs)

    def list_chunk_records_for_kg(self, *args, **kwargs):
        return self._call("list_chunk_records_for_kg", *args, **kwargs)

    def insert_admin_relation_row(self, *args, **kwargs):
        return self._call("insert_admin_relation_row", *args, **kwargs)

    def update_admin_relation_row(self, *args, **kwargs):
        return self._call("update_admin_relation_row", *args, **kwargs)

    def delete_admin_relation_row(self, *args, **kwargs):
        return self._call("delete_admin_relation_row", *args, **kwargs)

    def execute_admin_sql(self, *args, **kwargs):
        return self._call("execute_admin_sql", *args, **kwargs)

    def get_record(self, *args, **kwargs):
        return self._call("get_record", *args, **kwargs)

    def get_asset_record(self, *args, **kwargs):
        return self._call("get_asset_record", *args, **kwargs)

    def list_collections(self, *args, **kwargs):
        return self._call("list_collections", *args, **kwargs)

    def count_records(self, *args, **kwargs):
        return self._call("count_records", *args, **kwargs)

    def list_records(self, *args, **kwargs):
        return self._call("list_records", *args, **kwargs)

    def review_chunk_decision(self, *args, **kwargs):
        return self._call("review_chunk_decision", *args, **kwargs)

    def auto_review_chunks(self, *args, **kwargs):
        return self._call("auto_review_chunks", *args, **kwargs)

    def revalidate_document(self, *args, **kwargs):
        return self._call("revalidate_document", *args, **kwargs)

    def rebuild_document(self, *args, **kwargs):
        return self._call("rebuild_document", *args, **kwargs)

    def reindex_document(self, *args, **kwargs):
        return self._call("reindex_document", *args, **kwargs)

    def reprocess_kg(self, *args, **kwargs):
        return self._call("reprocess_kg", *args, **kwargs)

    def delete_document(self, *args, **kwargs):
        return self._call("delete_document", *args, **kwargs)

    def reset_pipeline_data(self, *args, **kwargs):
        return self._call("reset_pipeline_data", *args, **kwargs)

    def sync_chunk_index(self, *args, **kwargs):
        return self._call("sync_chunk_index", *args, **kwargs)

    def sync_asset_index(self, *args, **kwargs):
        return self._call("sync_asset_index", *args, **kwargs)

    def query(self, *args, **kwargs):
        return self._call("query", *args, **kwargs)

    def chat(self, *args, **kwargs):
        return self._call("chat", *args, **kwargs)

    def inspect_retrieval(self, *args, **kwargs):
        return self._call("inspect_retrieval", *args, **kwargs)


def repository_factory() -> Repository:
    ensure_app_storage_compatibility()
    return Repository()


def identity_repository_factory() -> Repository:
    auth_dsn = str(settings.auth_postgres_dsn or "").strip()
    if not auth_dsn:
        raise RuntimeError("AUTH_POSTGRES_DSN must be configured for identity operations")
    return Repository(dsn=auth_dsn)


repository = LazyProxy(repository_factory)
identity_repository = LazyProxy(identity_repository_factory)
auth_store = LazyProxy(AuthStore)
chroma_store = LazyProxy(ChromaStore)
service = LazyProxy(lambda: IngestionService(repository=repository._get(), store=chroma_store._get()))
agent_service = LazyProxy(lambda: AgentService(repository=repository._get(), store=chroma_store._get()))
rate_limiter = SlidingWindowRateLimiter(
    dsn=str(settings.auth_postgres_dsn or "").strip(),
    schema_name=str(settings.auth_postgres_schema or "auth"),
)
