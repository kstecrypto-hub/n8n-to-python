from types import SimpleNamespace

import pytest

from src.bee_ingestion.models import Chunk, KGExtractionResult, SourceDocument
from src.bee_ingestion.service import IngestionService


class FakeRepository:
    def __init__(self) -> None:
        self.stages = []
        self.chunk_record = None
        self.chunk_records_for_kg = []
        self.pending_kg_chunk_records = []
        self.review_chunk_records = []
        self.updated_validation = None
        self.claimed = []
        self.released = []
        self.pruned_entities = 0
        self.review_runs = []
        self.deleted_documents = []
        self.reset_all = False
        self.latest_source = None
        self.reset_document_id = None
        self.replaced_source = None
        self.stage_run_counter = 0
        self.stage_runs = {}
        self.applied_revalidation_state = None
        self.replaced_kg_results = None
        self.latest_job = None
        self.running_stage = None
        self.document_pages = []
        self.page_asset_rows = []
        self.kg_raw_counts = {}
        self.corpus_snapshots = []
        self.document_synopsis = None
        self.section_synopses = []

    class _NullLock:
        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc, tb):
            return False

    def advisory_lock(self, *args):
        return self._NullLock()

    def register_document(self, source):
        return "doc-1", "src-1"

    def find_existing_document(self, source):
        return None

    def create_job(self, **kwargs):
        return "job-1"

    def create_corpus_snapshot(self, tenant_id, snapshot_kind, **kwargs):
        snapshot_id = self.create_pending_corpus_snapshot(tenant_id, snapshot_kind, **kwargs)
        self.activate_corpus_snapshot(
            snapshot_id,
            document_id=kwargs.get("document_id"),
            job_id=kwargs.get("job_id"),
        )
        return snapshot_id

    def create_pending_corpus_snapshot(self, tenant_id, snapshot_kind, **kwargs):
        snapshot_id = f"snapshot-{len(self.corpus_snapshots) + 1}"
        self.corpus_snapshots.append(
            {
                "snapshot_id": snapshot_id,
                "tenant_id": tenant_id,
                "snapshot_kind": snapshot_kind,
                "status": "pending",
                **kwargs,
            }
        )
        return snapshot_id

    def activate_corpus_snapshot(self, snapshot_id, **kwargs):
        for snapshot in self.corpus_snapshots:
            if snapshot["snapshot_id"] == snapshot_id:
                snapshot["status"] = "active"
                snapshot["activation"] = kwargs
                return None
        raise ValueError("Corpus snapshot not found")

    def claim_job(self, job_id, worker_id, lease_seconds, preserve_status=False):
        self.claimed.append((job_id, worker_id, lease_seconds, preserve_status))
        return True

    def release_job(self, job_id, worker_id):
        self.released.append((job_id, worker_id))

    def save_blocks(self, blocks):
        self.blocks = blocks

    def save_document_pages(self, pages):
        self.pages = pages

    def save_page_assets(self, assets):
        self.page_assets = assets

    def save_chunks(self, chunks):
        self.chunks = chunks

    def save_validations(self, validations):
        self.validations = validations

    def save_kg_result(self, document_id, chunk_id, result, status, errors, raw_payload=None, provider=None, model=None, prompt_version=None):
        self.kg_result = (document_id, chunk_id, status, errors, provider, model, prompt_version)

    def record_stage(self, job_id, document_id, stage_name, job_status, stage_outcome, metrics=None, error_message=None, worker_version=None, input_version=None):
        self.stages.append(
            {
                "job_id": job_id,
                "document_id": document_id,
                "stage_name": stage_name,
                "job_status": job_status,
                "stage_outcome": stage_outcome,
                "metrics": metrics,
                "error_message": error_message,
                "worker_version": worker_version,
                "input_version": input_version,
            }
        )

    def start_stage_run(self, job_id, document_id, stage_name, job_status, metrics=None, worker_version=None, input_version=None, started_at=None):
        self.stage_run_counter += 1
        stage_run_id = f"stage-{self.stage_run_counter}"
        record = {
            "stage_run_id": stage_run_id,
            "job_id": job_id,
            "document_id": document_id,
            "stage_name": stage_name,
            "job_status": job_status,
            "stage_outcome": "running",
            "metrics": metrics,
            "error_message": None,
            "worker_version": worker_version,
            "input_version": input_version,
            "started_at": started_at,
            "finished_at": None,
        }
        self.stage_runs[stage_run_id] = record
        self.stages.append(record)
        return stage_run_id

    def finish_stage_run(self, stage_run_id, job_id, document_id, stage_outcome, job_status=None, metrics=None, error_message=None, finished_at=None):
        record = self.stage_runs[stage_run_id]
        record["stage_outcome"] = stage_outcome
        record["job_status"] = job_status or record["job_status"]
        record["metrics"] = metrics
        record["error_message"] = error_message
        record["finished_at"] = finished_at

    def get_chunk_record(self, chunk_id):
        return self.chunk_record

    def list_page_assets(self, document_id=None, chunk_id=None, limit=100, offset=0):
        return self.page_asset_rows[offset : offset + limit]

    def list_chunk_records_for_kg(self, document_id=None, limit=200, offset=0):
        return self.chunk_records_for_kg[offset : offset + limit]

    def list_pending_kg_chunk_records(self, document_id, limit=200, offset=0):
        return self.pending_kg_chunk_records[offset : offset + limit]

    def list_review_chunk_records(self, document_id=None, limit=200, offset=0):
        rows = [row for row in self.review_chunk_records if row.get("validation_status") == "review"]
        return rows[offset : offset + limit]

    def update_chunk_validation(self, chunk_id, status, quality_score, reasons):
        self.updated_validation = (chunk_id, status, quality_score, reasons)
        if self.chunk_record and self.chunk_record.get("chunk_id") == chunk_id:
            self.chunk_record["validation_status"] = status
            self.chunk_record["quality_score"] = quality_score
            self.chunk_record["reasons"] = reasons
        for row in self.review_chunk_records:
            if row.get("chunk_id") == chunk_id:
                row["validation_status"] = status
                row["quality_score"] = quality_score
                row["reasons"] = reasons

    def update_chunk_metadata(self, chunk_id, metadata):
        self.updated_chunk_metadata = (chunk_id, metadata)
        if self.chunk_record and self.chunk_record.get("chunk_id") == chunk_id:
            self.chunk_record["metadata_json"] = metadata
        for row in getattr(self, "chunk_records", []):
            if row.get("chunk_id") == chunk_id:
                row["metadata_json"] = metadata
        for row in self.review_chunk_records:
            if row.get("chunk_id") == chunk_id:
                row["metadata_json"] = metadata

    def prune_orphan_kg_entities(self):
        return self.pruned_entities

    def save_chunk_review_run(self, **kwargs):
        self.review_runs.append(kwargs)

    def get_latest_document_source(self, document_id):
        if self.latest_source and self.latest_source.get("document_id") == document_id:
            return self.latest_source
        return None

    def get_latest_job_for_document(self, document_id):
        if self.latest_job and self.latest_job.get("document_id") == document_id:
            return self.latest_job
        return None

    def get_running_stage_run(self, job_id):
        if self.running_stage and self.running_stage.get("job_id") == job_id:
            return self.running_stage
        return None

    def replace_document_source(self, document_id, source):
        self.replaced_source = (document_id, source)
        return "src-rebuilt-1"

    def update_document_source(self, source_id, patch):
        self.updated_source = (source_id, patch)

    def reset_document_pipeline_state(self, document_id):
        self.reset_document_id = document_id

    def delete_document(self, document_id):
        self.deleted_documents.append(document_id)
        return 1

    def delete_document_kg(self, document_id):
        self.deleted_document_kg_id = document_id

    def clear_ingestion_data(self):
        self.reset_all = True

    def apply_revalidation_state(self, document_id, links, chunk_updates, kg_results=None, remove_chunk_kg_ids=None):
        self.applied_revalidation_state = {
            "document_id": document_id,
            "links": links,
            "chunk_updates": chunk_updates,
            "kg_results": kg_results,
            "remove_chunk_kg_ids": remove_chunk_kg_ids,
        }
        if remove_chunk_kg_ids:
            self.deleted_kg_chunk_id = remove_chunk_kg_ids[-1]

    def replace_document_kg_results(self, document_id, kg_results):
        self.replaced_kg_results = (document_id, kg_results)

    def list_chunk_records(self, document_id=None, status=None, limit=200, offset=0):
        rows = getattr(self, "chunk_records", [])
        if document_id:
            rows = [row for row in rows if row.get("document_id") == document_id]
        if status:
            rows = [row for row in rows if row.get("validation_status") == status]
        return rows[offset : offset + limit]

    def delete_kg_for_chunk(self, chunk_id):
        self.deleted_kg_chunk_id = chunk_id

    def list_document_chunk_records(self, document_id, status=None):
        rows = list(getattr(self, "chunk_records", []) or [])
        if self.chunk_record:
            rows.append(self.chunk_record)
        rows = [row for row in rows if row.get("document_id") == document_id]
        if status:
            rows = [row for row in rows if row.get("validation_status") == status]
        return rows

    def get_document_record(self, document_id):
        rows = list(getattr(self, "chunk_records", []) or [])
        if self.chunk_record:
            rows.append(self.chunk_record)
        rows = [row for row in rows if row.get("document_id") == document_id]
        if rows:
            first = rows[0]
            return {
                "document_id": document_id,
                "tenant_id": first.get("tenant_id", "tenant-1"),
                "filename": "doc.pdf",
                "status": "registered",
            }
        if self.latest_source and self.latest_source.get("document_id") == document_id:
            return {
                "document_id": document_id,
                "tenant_id": self.latest_source.get("tenant_id", "tenant-1"),
                "filename": self.latest_source.get("filename", "doc.pdf"),
                "status": "registered",
            }
        return None

    def replace_document_synopsis(
        self,
        *,
        document_id,
        tenant_id,
        title,
        synopsis_text,
        accepted_chunk_count,
        section_count,
        source_stage,
        synopsis_version,
        metadata_json=None,
    ):
        self.document_synopsis = {
            "document_id": document_id,
            "tenant_id": tenant_id,
            "title": title,
            "synopsis_text": synopsis_text,
            "accepted_chunk_count": accepted_chunk_count,
            "section_count": section_count,
            "source_stage": source_stage,
            "synopsis_version": synopsis_version,
            "metadata_json": metadata_json or {},
        }

    def replace_section_synopses(self, document_id, synopses):
        self.section_synopses = [dict(item) for item in synopses if item]

    def list_document_pages(self, document_id=None, limit=200, offset=0):
        rows = self.document_pages
        if document_id:
            rows = [row for row in rows if row.get("document_id") == document_id]
        return rows[offset : offset + limit]

    def get_document_related_counts(self, document_id):
        validations = list(getattr(self, "validations", []) or [])
        accepted_chunks = len([item for item in validations if getattr(item, "status", "") == "accepted"])
        review_chunks = len([item for item in validations if getattr(item, "status", "") == "review"])
        rejected_chunks = len([item for item in validations if getattr(item, "status", "") == "rejected"])
        return {
            "sources": 1,
            "pages": len(getattr(self, "pages", []) or []),
            "page_assets": len(getattr(self, "page_assets", []) or []),
            "chunk_asset_links": 0,
            "chunks": len(getattr(self, "chunks", []) or []) or len(self.chunk_records_for_kg or []) or len(self.pending_kg_chunk_records or []),
            "metadata": 0,
            "accepted_chunks": accepted_chunks or len(self.chunk_records_for_kg or []) or len(self.pending_kg_chunk_records or []),
            "review_chunks": review_chunks,
            "rejected_chunks": rejected_chunks,
            "kg_entities": 0,
            "kg_assertions": 0,
            "kg_evidence": 0,
            "kg_raw": int(self.kg_raw_counts.get("__all__", 0)),
            "kg_validated": int(self.kg_raw_counts.get("validated", 0)),
            "kg_review": int(self.kg_raw_counts.get("review", 0)),
            "kg_skipped": int(self.kg_raw_counts.get("skipped", 0)),
            "kg_quarantined": int(self.kg_raw_counts.get("quarantined", 0)),
        }

    def delete_chunk_kg(self, chunk_id):
        self.deleted_kg_chunk_id = chunk_id

    def renew_job_lease(self, job_id, worker_id, lease_seconds):
        self.renewed_lease = (job_id, worker_id, lease_seconds)
        return True

    def count_kg_raw_extractions(self, document_id=None, chunk_id=None, status=None):
        key = status or "__all__"
        return int(self.kg_raw_counts.get(key, 0))


class FakeStore:
    def __init__(self) -> None:
        self.upserted = None
        self.deleted = None
        self.deleted_document = None
        self.reset_called = False
        self.upserted_assets = None
        self.chunk_publish_state_updates = []
        self.asset_publish_state_updates = []
        self.deleted_chunks = []
        self.deleted_assets = []
        self.records = {}

    def upsert_chunks(self, chunks, embeddings, publish_state="ready"):
        self.upserted = (chunks, embeddings, publish_state)
        for chunk in chunks:
            self.records[chunk.chunk_id] = {
                "id": chunk.chunk_id,
                "document": chunk.text,
                "metadata": {"publish_state": publish_state},
            }

    def upsert_assets(self, assets, embeddings, publish_state="ready"):
        self.upserted_assets = (assets, embeddings, publish_state)

    def set_chunk_publish_state(self, chunk_ids, publish_state):
        self.chunk_publish_state_updates.append((list(chunk_ids), publish_state))
        for chunk_id in chunk_ids:
            if chunk_id in self.records:
                self.records[chunk_id]["metadata"]["publish_state"] = publish_state

    def set_asset_publish_state(self, asset_ids, publish_state):
        self.asset_publish_state_updates.append((list(asset_ids), publish_state))

    def delete_chunk(self, chunk_id):
        self.deleted = chunk_id
        self.records.pop(chunk_id, None)

    def delete_chunks(self, chunk_ids):
        self.deleted_chunks.extend(list(chunk_ids))
        for chunk_id in chunk_ids:
            self.records.pop(chunk_id, None)

    def delete_document(self, document_id):
        self.deleted_document = document_id

    def delete_assets(self, asset_ids):
        self.deleted_assets.extend(list(asset_ids))

    def reset_collection(self):
        self.reset_called = True

    def get_record(self, chunk_id):
        return self.records.get(chunk_id)


@pytest.fixture(autouse=True)
def _stub_external_ingestion_dependencies(monkeypatch, request):
    def fake_embed(self, texts, progress_callback=None):
        values = list(texts)
        total = len(values)
        if progress_callback is not None:
            progress_callback({"completed": total, "total": total, "batch_size": total})
        return [[0.1, 0.2, 0.3, 0.4] for _ in values]

    monkeypatch.setattr("src.bee_ingestion.service.Embedder.embed", fake_embed)

    real_kg_tests = {
        "test_run_kg_pipeline_marks_empty_relation_set_as_skipped",
        "test_run_kg_pipeline_skips_invalid_relation_attempts_when_nothing_survives",
        "test_review_chunk_accept_quarantines_when_extractor_errors",
    }
    if request.node.name not in real_kg_tests:
        def fake_run_kg_pipeline(self, document_id, chunk, linked_assets=None, **kwargs):
            return {
                "chunk_id": chunk.chunk_id,
                "status": "validated",
                "errors": [],
                "provider": "stub",
                "model": "stub",
                "prompt_version": "test",
                "result": {},
                "_result_obj": KGExtractionResult(
                    source_id=document_id,
                    segment_id=chunk.chunk_id,
                    mentions=[],
                    candidate_entities=[],
                    candidate_relations=[],
                    evidence=[],
                ),
                "_raw_payload": {"stub": True},
            }

        monkeypatch.setattr(IngestionService, "_run_kg_pipeline", fake_run_kg_pipeline)


def test_service_ingests_text_without_external_services() -> None:
    repository = FakeRepository()
    service = IngestionService(repository=repository, store=FakeStore())
    result = service.ingest_text(
        SourceDocument(
            tenant_id="tenant-1",
            source_type="text",
            filename="sample.txt",
            raw_text=(
                "INTRODUCTION\n\n"
                "Honey bees produce honey and wax for the colony while workers regulate temperature, feed brood, and maintain the hive through coordinated labour.\n\n"
                "VARROA\n\n"
                "Varroa affects colonies and monitoring is required throughout the season because infestation pressure changes with brood levels, weather, and management choices."
            ),
        )
    )

    assert result["document_id"] == "doc-1"
    assert result["chunks"] >= 1
    assert result["accepted_chunks"] >= 1
    assert result["corpus_snapshot_id"] == "snapshot-1"
    assert [stage["stage_name"] for stage in repository.stages] == [
        "content_available",
        "parsed",
        "chunked",
        "chunks_validated",
        "kg_validated",
        "indexed",
    ]
    assert repository.corpus_snapshots[0]["snapshot_kind"] == "document_publish"
    assert repository.claimed
    assert repository.released
    assert repository.document_synopsis is not None
    assert repository.document_synopsis["accepted_chunk_count"] >= 1
    assert repository.document_synopsis["source_stage"] == "chunks_validated"
    assert repository.section_synopses


def test_review_chunk_accept_embeds_and_updates_validation() -> None:
    repository = FakeRepository()
    repository.chunk_record = {
        "chunk_id": "chunk-1",
        "document_id": "doc-1",
        "tenant_id": "tenant-1",
        "chunk_index": 1,
        "page_start": 1,
        "page_end": 1,
        "section_path": ["Preface"],
        "prev_chunk_id": None,
        "next_chunk_id": None,
        "char_start": 0,
        "char_end": 64,
        "content_type": "text",
        "text": "Honey bees produce honey in strong colonies.",
        "parser_version": "v1",
        "chunker_version": "v1",
        "metadata_json": {"chunk_role": "body", "document_class": "book", "section_title": "Preface"},
        "validation_status": "review",
        "quality_score": 0.55,
        "reasons": ["front_matter"],
    }
    store = FakeStore()
    service = IngestionService(repository=repository, store=store)

    result = service.review_chunk_decision("chunk-1", "accept")

    assert result["status"] == "accepted"
    assert store.upserted is not None
    assert store.upserted[2] == "staged"
    assert store.chunk_publish_state_updates == [(["chunk-1"], "ready")]
    assert repository.updated_validation[1] == "accepted"
    assert result["kg_status"] in {"validated", "review", "quarantined"}
    assert repository.document_synopsis is not None
    assert repository.document_synopsis["source_stage"] == "review_accept"
    assert repository.document_synopsis["accepted_chunk_count"] == 1
    assert len(repository.section_synopses) == 1


def test_review_chunk_reject_removes_from_chroma() -> None:
    repository = FakeRepository()
    repository.chunk_record = {
        "chunk_id": "chunk-2",
        "document_id": "doc-1",
        "tenant_id": "tenant-1",
        "chunk_index": 2,
        "page_start": 1,
        "page_end": 1,
        "section_path": ["Contents"],
        "prev_chunk_id": None,
        "next_chunk_id": None,
        "char_start": 0,
        "char_end": 25,
        "content_type": "text",
        "text": "Great Reductions in this Catalogue",
        "parser_version": "v1",
        "chunker_version": "v1",
        "metadata_json": {"chunk_role": "back_matter", "document_class": "book", "section_title": "Contents"},
        "validation_status": "review",
        "quality_score": 0.52,
        "reasons": ["back_matter"],
    }
    store = FakeStore()
    store.records["chunk-2"] = {"id": "chunk-2", "document": "Great Reductions in this Catalogue", "metadata": {"publish_state": "ready"}}
    service = IngestionService(repository=repository, store=store)

    result = service.review_chunk_decision("chunk-2", "reject")

    assert result["status"] == "rejected"
    assert store.chunk_publish_state_updates == [(["chunk-2"], "staged")]
    assert store.deleted == "chunk-2"
    assert repository.updated_validation[1] == "rejected"


def test_review_chunk_auto_accept_embeds_and_updates_validation(monkeypatch) -> None:
    repository = FakeRepository()
    repository.chunk_record = {
        "chunk_id": "chunk-auto-1",
        "document_id": "doc-1",
        "tenant_id": "tenant-1",
        "chunk_index": 1,
        "page_start": 1,
        "page_end": 1,
        "section_path": [],
        "prev_chunk_id": None,
        "next_chunk_id": None,
        "char_start": 0,
        "char_end": 120,
        "content_type": "text",
        "text": "Among those who were instrumental in introducing advanced methods in bee-culture among the beekeepers of Europe...",
        "parser_version": "v1",
        "chunker_version": "v1",
        "metadata_json": {"chunk_role": "contents", "document_class": "book", "section_title": ""},
        "validation_status": "review",
        "quality_score": 0.52,
        "reasons": ["contents", "missing_section"],
    }
    store = FakeStore()
    service = IngestionService(repository=repository, store=store)

    from types import SimpleNamespace

    monkeypatch.setattr(
        "src.bee_ingestion.service.review_chunk_with_meta",
        lambda *args, **kwargs: SimpleNamespace(
            result=SimpleNamespace(decision="accept", confidence=0.92, detected_role="body", reason="coherent body text"),
            raw_payload={"ok": True},
            provider="openai",
            model="gpt-5-mini",
            prompt_version="v1",
        ),
    )

    result = service.review_chunk_decision("chunk-auto-1", "auto")

    assert result["status"] == "accepted"
    assert store.upserted is not None
    assert store.upserted[2] == "staged"
    assert store.chunk_publish_state_updates == [(["chunk-auto-1"], "ready")]
    assert repository.updated_validation[1] == "accepted"
    assert repository.review_runs[0]["decision"] == "accept"


def test_auto_review_chunks_processes_review_records(monkeypatch) -> None:
    repository = FakeRepository()
    repository.review_chunk_records = [
        {
            "chunk_id": "chunk-review-1",
            "document_id": "doc-1",
            "tenant_id": "tenant-1",
            "chunk_index": 1,
            "page_start": 1,
            "page_end": 1,
            "section_path": [],
            "prev_chunk_id": None,
            "next_chunk_id": None,
            "char_start": 0,
            "char_end": 90,
            "content_type": "text",
            "text": "Great reductions in this catalogue.",
            "parser_version": "v1",
            "chunker_version": "v1",
            "metadata_json": {"chunk_role": "contents", "document_class": "book", "section_title": ""},
            "validation_status": "review",
            "quality_score": 0.45,
            "reasons": ["contents"],
        }
    ]
    store = FakeStore()
    service = IngestionService(repository=repository, store=store)

    from types import SimpleNamespace

    monkeypatch.setattr(
        "src.bee_ingestion.service.review_chunk_with_meta",
        lambda *args, **kwargs: SimpleNamespace(
            result=SimpleNamespace(decision="reject", confidence=0.88, detected_role="contents", reason="table-of-contents style fragment"),
            raw_payload={"ok": True},
            provider="openai",
            model="gpt-5-mini",
            prompt_version="v1",
        ),
    )

    result = service.auto_review_chunks(document_id="doc-1", batch_size=10)

    assert result["processed_chunks"] == 1
    assert result["rejected"] == 1
    assert store.deleted == "chunk-review-1"
    assert repository.review_runs[0]["decision"] == "reject"


def test_review_chunk_accept_quarantines_when_extractor_errors(monkeypatch) -> None:
    repository = FakeRepository()
    repository.chunk_record = {
        "chunk_id": "chunk-3",
        "document_id": "doc-1",
        "tenant_id": "tenant-1",
        "chunk_index": 3,
        "page_start": 1,
        "page_end": 1,
        "section_path": ["Body"],
        "prev_chunk_id": None,
        "next_chunk_id": None,
        "char_start": 0,
        "char_end": 64,
        "content_type": "text",
        "text": "Honey bees produce honey in strong colonies.",
        "parser_version": "v1",
        "chunker_version": "v1",
        "metadata_json": {"chunk_role": "body", "document_class": "book", "section_title": "Body"},
        "validation_status": "review",
        "quality_score": 0.6,
        "reasons": ["manual_review"],
    }
    store = FakeStore()
    service = IngestionService(repository=repository, store=store)

    from src.bee_ingestion.kg import KGExtractionError

    monkeypatch.setattr(
        "src.bee_ingestion.offline_pipeline.stages.build_kg.extract_candidates_with_meta",
        lambda *args, **kwargs: (_ for _ in ()).throw(KGExtractionError("bad schema")),
    )

    result = service.review_chunk_decision("chunk-3", "accept")

    assert result["kg_status"] == "quarantined"


def test_review_chunk_accept_rolls_back_staged_state_on_validation_failure(monkeypatch) -> None:
    repository = FakeRepository()
    repository.chunk_record = {
        "chunk_id": "chunk-rollback-1",
        "document_id": "doc-1",
        "tenant_id": "tenant-1",
        "chunk_index": 1,
        "page_start": 1,
        "page_end": 1,
        "section_path": ["Body"],
        "prev_chunk_id": None,
        "next_chunk_id": None,
        "char_start": 0,
        "char_end": 64,
        "content_type": "text",
        "text": "Honey bees produce honey in strong colonies.",
        "parser_version": "v1",
        "chunker_version": "v1",
        "metadata_json": {"chunk_role": "body", "document_class": "book", "section_title": "Body"},
        "validation_status": "review",
        "quality_score": 0.6,
        "reasons": ["manual_review"],
    }
    store = FakeStore()
    service = IngestionService(repository=repository, store=store)

    def fail_update(*args, **kwargs):
        raise RuntimeError("validation write failed")

    monkeypatch.setattr(repository, "update_chunk_validation", fail_update)

    with pytest.raises(RuntimeError, match="validation write failed"):
        service.review_chunk_decision("chunk-rollback-1", "accept")

    assert store.upserted is not None
    assert store.upserted[2] == "staged"
    assert store.deleted == "chunk-rollback-1"
    assert store.get_record("chunk-rollback-1") is None


def test_revalidate_document_reindexes_and_removes_nonaccepted_chunks() -> None:
    repository = FakeRepository()
    repository.chunk_records = [
        {
            "chunk_id": "chunk-accepted",
            "document_id": "doc-1",
            "tenant_id": "tenant-1",
            "chunk_index": 1,
            "page_start": 17,
            "page_end": 17,
            "section_path": ["Introduction"],
            "prev_chunk_id": None,
            "next_chunk_id": None,
            "char_start": 0,
            "char_end": 260,
            "content_type": "text",
            "text": "Honey bees organize brood rearing, maintain hive temperature, and distribute food across the colony while workers coordinate labour throughout the day.",
            "parser_version": "v1",
            "chunker_version": "v1",
            "metadata_json": {"chunk_role": "body", "document_class": "book", "section_title": "Introduction"},
            "validation_status": "review",
            "quality_score": 0.45,
            "reasons": ["missing_section"],
        },
        {
            "chunk_id": "chunk-rejected",
            "document_id": "doc-1",
            "tenant_id": "tenant-1",
            "chunk_index": 2,
            "page_start": 17,
            "page_end": 17,
            "section_path": [],
            "prev_chunk_id": "chunk-accepted",
            "next_chunk_id": None,
            "char_start": 261,
            "char_end": 274,
            "content_type": "text",
            "text": "[[Page 17]] 2",
            "parser_version": "v1",
            "chunker_version": "v1",
            "metadata_json": {"chunk_role": "body", "document_class": "book", "section_title": ""},
            "validation_status": "review",
            "quality_score": 0.2,
            "reasons": ["too_short"],
        },
    ]
    store = FakeStore()
    service = IngestionService(repository=repository, store=store)

    result = service.revalidate_document("doc-1", rerun_kg=False)

    assert result["chunks"] == 2
    assert result["accepted"] == 1
    assert result["rejected"] == 1
    assert store.upserted is not None
    assert store.deleted == "chunk-rejected"
    assert repository.applied_revalidation_state is not None
    assert repository.deleted_kg_chunk_id == "chunk-rejected"
    assert repository.document_synopsis is not None
    assert repository.document_synopsis["accepted_chunk_count"] == 1
    assert repository.document_synopsis["source_stage"] == "revalidated"
    assert len(repository.section_synopses) == 1


def test_reprocess_kg_replays_only_accepted_chunks() -> None:
    repository = FakeRepository()
    repository.pruned_entities = 2
    repository.chunk_records_for_kg = [
        {
            "chunk_id": "chunk-accepted-1",
            "document_id": "doc-1",
            "tenant_id": "tenant-1",
            "chunk_index": 1,
            "page_start": 1,
            "page_end": 1,
            "section_path": ["Body"],
            "prev_chunk_id": None,
            "next_chunk_id": None,
            "char_start": 0,
            "char_end": 80,
            "content_type": "text",
            "text": "The colony produces honey and uses frames for hive management across the season.",
            "parser_version": "v1",
            "chunker_version": "v1",
            "metadata_json": {"chunk_role": "body", "document_class": "book", "section_title": "Body"},
            "validation_status": "accepted",
            "quality_score": 0.91,
            "reasons": [],
        },
        {
            "chunk_id": "chunk-accepted-2",
            "document_id": "doc-1",
            "tenant_id": "tenant-1",
            "chunk_index": 2,
            "page_start": 2,
            "page_end": 2,
            "section_path": ["Body"],
            "prev_chunk_id": "chunk-accepted-1",
            "next_chunk_id": None,
            "char_start": 81,
            "char_end": 160,
            "content_type": "text",
            "text": "Disease in the colony may be treated by management action and equipment changes.",
            "parser_version": "v1",
            "chunker_version": "v1",
            "metadata_json": {"chunk_role": "body", "document_class": "book", "section_title": "Body"},
            "validation_status": "accepted",
            "quality_score": 0.9,
            "reasons": [],
        },
    ]
    service = IngestionService(repository=repository, store=FakeStore())

    result = service.reprocess_kg(document_id="doc-1", batch_size=1)

    assert result["processed_chunks"] == 2
    assert result["pruned_entities"] == 2


def test_reprocess_kg_counts_skipped_results(monkeypatch) -> None:
    repository = FakeRepository()
    repository.chunk_records_for_kg = [
        {
            "chunk_id": "chunk-skipped-1",
            "document_id": "doc-1",
            "tenant_id": "tenant-1",
            "chunk_index": 1,
            "page_start": 1,
            "page_end": 1,
            "section_path": ["Body"],
            "prev_chunk_id": None,
            "next_chunk_id": None,
            "char_start": 0,
            "char_end": 80,
            "content_type": "text",
            "text": "The queen is present in the colony.",
            "parser_version": "v1",
            "chunker_version": "v1",
            "metadata_json": {"chunk_role": "body", "document_class": "book", "section_title": "Body"},
            "validation_status": "accepted",
            "quality_score": 0.9,
            "reasons": [],
        }
    ]
    store = FakeStore()
    service = IngestionService(repository=repository, store=store)
    monkeypatch.setattr(service, "_run_kg_pipeline", lambda document_id, chunk, **kwargs: {"status": "skipped", "errors": []})

    result = service.reprocess_kg(document_id="doc-1", batch_size=10)

    assert result["processed_chunks"] == 1
    assert result["skipped"] == 1


def test_run_kg_pipeline_marks_empty_relation_set_as_skipped(monkeypatch) -> None:
    repository = FakeRepository()
    store = FakeStore()
    service = IngestionService(repository=repository, store=store)
    chunk = Chunk(
        chunk_id="chunk-kg-empty",
        document_id="doc-1",
        tenant_id="tenant-1",
        chunk_index=1,
        page_start=1,
        page_end=1,
        section_path=["Body"],
        prev_chunk_id=None,
        next_chunk_id=None,
        char_start=0,
        char_end=64,
        content_type="text",
        text="The queen is present in the colony.",
        parser_version="v1",
        chunker_version="v1",
        metadata={"chunk_role": "body", "document_class": "book", "section_title": "Body"},
    )
    empty_result = KGExtractionResult(
        source_id="doc-1",
        segment_id="chunk-kg-empty",
        mentions=[],
        candidate_entities=[],
        candidate_relations=[],
        evidence=[],
    )

    from types import SimpleNamespace

    monkeypatch.setattr(
        "src.bee_ingestion.offline_pipeline.stages.build_kg.extract_candidates_with_meta",
        lambda *args, **kwargs: SimpleNamespace(
            result=empty_result,
            raw_payload={"ok": True},
            provider="openai",
            model="gpt-5-mini",
            prompt_version="v1",
        ),
    )
    monkeypatch.setattr("src.bee_ingestion.offline_pipeline.stages.build_kg.prune_extraction", lambda result, ontology, min_conf: (result, []))
    monkeypatch.setattr("src.bee_ingestion.offline_pipeline.stages.build_kg.canonicalize_extraction", lambda result, ontology: result)
    monkeypatch.setattr("src.bee_ingestion.offline_pipeline.stages.build_kg.validate_extraction", lambda result, ontology, min_conf: (True, []))

    result = service._run_kg_pipeline("doc-1", chunk)

    assert result["status"] == "skipped"
    assert "empty_relation_set" in result["errors"]


def test_run_kg_pipeline_skips_invalid_relation_attempts_when_nothing_survives(monkeypatch) -> None:
    repository = FakeRepository()
    store = FakeStore()
    service = IngestionService(repository=repository, store=store)
    chunk = Chunk(
        chunk_id="chunk-kg-review",
        document_id="doc-1",
        tenant_id="tenant-1",
        chunk_index=1,
        page_start=1,
        page_end=1,
        section_path=["Body"],
        prev_chunk_id=None,
        next_chunk_id=None,
        char_start=0,
        char_end=64,
        content_type="text",
        text="The queen produces eggs.",
        parser_version="v1",
        chunker_version="v1",
        metadata={"chunk_role": "body", "document_class": "book", "section_title": "Body"},
    )
    empty_result = KGExtractionResult(
        source_id="doc-1",
        segment_id="chunk-kg-review",
        mentions=[],
        candidate_entities=[],
        candidate_relations=[],
        evidence=[],
    )

    from types import SimpleNamespace

    monkeypatch.setattr(
        "src.bee_ingestion.offline_pipeline.stages.build_kg.extract_candidates_with_meta",
        lambda *args, **kwargs: SimpleNamespace(
            result=empty_result,
            raw_payload={"ok": True},
            provider="openai",
            model="gpt-5-mini",
            prompt_version="v1",
        ),
    )
    monkeypatch.setattr(
        "src.bee_ingestion.offline_pipeline.stages.build_kg.prune_extraction",
        lambda result, ontology, min_conf: (result, ["invalid_subject_type:r1:Queen->Colony"]),
    )
    monkeypatch.setattr("src.bee_ingestion.offline_pipeline.stages.build_kg.canonicalize_extraction", lambda result, ontology: result)
    monkeypatch.setattr("src.bee_ingestion.offline_pipeline.stages.build_kg.validate_extraction", lambda result, ontology, min_conf: (False, []))

    result = service._run_kg_pipeline("doc-1", chunk)

    assert result["status"] == "skipped"
    assert "invalid_subject_type:r1:Queen->Colony" in result["errors"]


def test_rebuild_document_replays_from_stored_source() -> None:
    repository = FakeRepository()
    repository.latest_source = {
        "document_id": "doc-1",
        "tenant_id": "tenant-1",
        "source_type": "text",
        "filename": "stored.txt",
        "document_class": "note",
        "parser_version": "v1",
        "ocr_engine": None,
        "ocr_model": None,
        "source_id": "src-1",
        "raw_text": (
            "INTRODUCTION\n\n"
            "Honey bees work together to regulate brood temperature and food distribution inside the hive."
        ),
        "normalized_text": "",
        "extraction_metrics_json": {},
        "metadata_json": {"source_path": "E:\\n8n to python\\stored.txt"},
    }
    store = FakeStore()
    service = IngestionService(repository=repository, store=store)

    result = service.rebuild_document("doc-1")

    assert result["rebuilt_document_id"] == "doc-1"
    assert result["superseded_document_id"] == "doc-1"
    assert repository.updated_source is not None
    assert store.deleted_document == "doc-1"


def test_resume_document_ingest_continues_from_missing_kg_chunk(monkeypatch) -> None:
    repository = FakeRepository()
    repository.latest_source = {
        "document_id": "doc-1",
        "tenant_id": "tenant-1",
        "source_type": "pdf",
        "filename": "manual.pdf",
        "document_class": "book",
        "parser_version": "v1",
        "ocr_engine": None,
        "ocr_model": None,
        "content_hash": "sha256:test",
        "source_id": "src-1",
        "raw_text": "",
        "normalized_text": "",
        "extraction_metrics_json": {},
        "metadata_json": {"source_path": "E:\\n8n to python\\manual.pdf"},
    }
    repository.latest_job = {
        "job_id": "job-2",
        "document_id": "doc-1",
        "status": "kg_validated",
    }
    repository.running_stage = {
        "stage_run_id": "stage-running-kg",
        "job_id": "job-2",
        "document_id": "doc-1",
        "stage_name": "kg_validated",
        "status": "running",
        "metrics_json": {"kg_total": 3, "kg_completed": 2},
    }
    repository.stage_runs["stage-running-kg"] = {
        "stage_run_id": "stage-running-kg",
        "job_id": "job-2",
        "document_id": "doc-1",
        "stage_name": "kg_validated",
        "job_status": "kg_validated",
        "stage_outcome": "running",
        "metrics": {"kg_total": 3, "kg_completed": 2},
        "error_message": None,
        "worker_version": "test",
        "input_version": "test",
        "started_at": None,
        "finished_at": None,
    }
    repository.chunk_records_for_kg = [
        {
            "chunk_id": "chunk-1",
            "document_id": "doc-1",
            "tenant_id": "tenant-1",
            "chunk_index": 1,
            "page_start": 1,
            "page_end": 1,
            "section_path": ["Body"],
            "prev_chunk_id": None,
            "next_chunk_id": "chunk-2",
            "char_start": 0,
            "char_end": 10,
            "content_type": "text",
            "text": "one",
            "parser_version": "v1",
            "chunker_version": "v1",
            "metadata_json": {},
            "validation_status": "accepted",
            "quality_score": 0.9,
            "reasons": [],
        },
        {
            "chunk_id": "chunk-2",
            "document_id": "doc-1",
            "tenant_id": "tenant-1",
            "chunk_index": 2,
            "page_start": 1,
            "page_end": 1,
            "section_path": ["Body"],
            "prev_chunk_id": "chunk-1",
            "next_chunk_id": "chunk-3",
            "char_start": 11,
            "char_end": 20,
            "content_type": "text",
            "text": "two",
            "parser_version": "v1",
            "chunker_version": "v1",
            "metadata_json": {},
            "validation_status": "accepted",
            "quality_score": 0.9,
            "reasons": [],
        },
        {
            "chunk_id": "chunk-3",
            "document_id": "doc-1",
            "tenant_id": "tenant-1",
            "chunk_index": 3,
            "page_start": 1,
            "page_end": 1,
            "section_path": ["Body"],
            "prev_chunk_id": "chunk-2",
            "next_chunk_id": None,
            "char_start": 21,
            "char_end": 30,
            "content_type": "text",
            "text": "three",
            "parser_version": "v1",
            "chunker_version": "v1",
            "metadata_json": {},
            "validation_status": "accepted",
            "quality_score": 0.9,
            "reasons": [],
        },
    ]
    repository.pending_kg_chunk_records = [repository.chunk_records_for_kg[-1]]
    repository.document_pages = [{"document_id": "doc-1", "page_number": 1}]
    repository.kg_raw_counts = {"validated": 2, "__all__": 2}
    store = FakeStore()
    service = IngestionService(repository=repository, store=store)

    monkeypatch.setattr(service, "_resolve_replayable_pdf_path", lambda source: None)

    def fake_resume_kg(document_id, chunk, linked_assets=None, pre_persist_check=None, **kwargs):
        if pre_persist_check is not None:
            pre_persist_check()
        repository.kg_raw_counts["validated"] = repository.kg_raw_counts.get("validated", 0) + 1
        repository.kg_raw_counts["__all__"] = repository.kg_raw_counts.get("__all__", 0) + 1
        return {
            "chunk_id": chunk.chunk_id,
            "status": "validated",
            "errors": [],
            "provider": "stub",
            "model": "stub",
            "prompt_version": "test",
            "result": {},
            "_result_obj": KGExtractionResult(
                source_id=document_id,
                segment_id=chunk.chunk_id,
                mentions=[],
                candidate_entities=[],
                candidate_relations=[],
                evidence=[],
            ),
            "_raw_payload": {"stub": True},
        }

    monkeypatch.setattr(service, "_run_kg_pipeline", fake_resume_kg)

    result = service.resume_document_ingest("doc-1")

    assert result["resumed"] is True
    assert result["resume_from_status"] == "kg_validated"
    assert result["corpus_snapshot_id"] == "snapshot-1"
    assert repository.claimed
    assert repository.claimed[0][0] == "job-2"
    assert repository.claimed[0][1] == service.worker_id
    assert repository.claimed[0][3] is True
    assert store.upserted is not None
    assert store.upserted[2] == "staged"
    assert store.chunk_publish_state_updates == [(["chunk-1", "chunk-2", "chunk-3"], "ready")]
    assert repository.stage_runs["stage-running-kg"]["stage_outcome"] == "completed"
    assert any(stage["stage_name"] == "indexed" and stage["stage_outcome"] == "completed" for stage in repository.stages)
    assert repository.corpus_snapshots[0]["snapshot_kind"] == "document_publish"
    assert repository.document_synopsis is not None
    assert repository.document_synopsis["source_stage"] == "chunks_validated"
    assert repository.document_synopsis["accepted_chunk_count"] == 3


def test_reset_ingestion_data_clears_document_or_all() -> None:
    repository = FakeRepository()
    store = FakeStore()
    service = IngestionService(repository=repository, store=store)

    document_result = service.reset_ingestion_data(document_id="doc-1")
    all_result = service.reset_ingestion_data()

    assert document_result == {"document_id": "doc-1", "cleared": "document"}
    assert repository.deleted_documents == ["doc-1"]
    assert all_result == {"cleared": "all"}
    assert store.reset_called is True
    assert repository.reset_all is True
