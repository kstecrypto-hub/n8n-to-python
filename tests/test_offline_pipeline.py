from types import SimpleNamespace

from src.bee_ingestion.offline_pipeline.context import OfflinePipelineContext
from src.bee_ingestion.offline_pipeline.contracts import OfflinePipelineCommand, OfflinePipelineState, StageResult
from src.bee_ingestion.offline_pipeline.runner import OfflinePipelineRunner
from src.bee_ingestion.offline_pipeline.stages import build_kg as build_kg_stage_module
from src.bee_ingestion.offline_pipeline.stages import chunk_documents as chunk_stage_module
from src.bee_ingestion.offline_pipeline.stages.extract_multimodal import extract_multimodal_payload
from src.bee_ingestion.offline_pipeline.stages.legacy_ingest import LegacyIngestStage
from src.bee_ingestion.offline_pipeline.stages import publish_corpus as publish_stage_module
from src.bee_ingestion.offline_pipeline.stages.source_prep import prepare_source_document


class FakePipelineRepository:
    def __init__(self) -> None:
        self.stage_records = []

    def record_stage(self, **kwargs):
        self.stage_records.append(kwargs)


class _Stage:
    def __init__(self, name: str, status: str = "completed", retrieval_visibility_boundary: bool = False) -> None:
        self.name = name
        self.job_status_on_start = "processing"
        self.retrieval_visibility_boundary = retrieval_visibility_boundary
        self.status = status

    async def run(self, context, state):
        if self.status == "failed":
            raise RuntimeError(f"{self.name} failed")
        return StageResult(status="completed", metrics={"stage": self.name})


def test_offline_pipeline_runner_executes_stages_in_order() -> None:
    repository = FakePipelineRepository()
    context = OfflinePipelineContext(repository=repository, service=object(), worker_id="worker-1")
    command = OfflinePipelineCommand(job_id="job-1", document_id="doc-1", source_id="src-1", tenant_id="shared", source_type="text")
    runner = OfflinePipelineRunner([_Stage("resolve_source"), _Stage("legacy_ingest", retrieval_visibility_boundary=True)])

    state = __import__("asyncio").run(runner.run(context=context, command=command))

    assert state.finished_stage_names == ["resolve_source", "legacy_ingest"]
    assert [item["stage_name"] for item in repository.stage_records] == ["resolve_source", "legacy_ingest"]
    assert state.retrieval_visible is True


def test_offline_pipeline_runner_marks_failed_stage() -> None:
    repository = FakePipelineRepository()
    context = OfflinePipelineContext(repository=repository, service=object(), worker_id="worker-1")
    command = OfflinePipelineCommand(job_id="job-1", document_id="doc-1", source_id="src-1", tenant_id="shared", source_type="text")
    runner = OfflinePipelineRunner([_Stage("resolve_source"), _Stage("legacy_ingest", status="failed")])

    state = __import__("asyncio").run(runner.run(context=context, command=command))

    assert state.errors == ["legacy_ingest failed"]
    assert repository.stage_records[-1]["stage_outcome"] == "failed"
    assert repository.stage_records[-1]["job_status"] == "failed"


def test_legacy_ingest_stage_preserves_existing_execution_path() -> None:
    class FakeService:
        def __init__(self) -> None:
            self.commands = []

        def _execute_legacy_ingest_command(self, command):
            self.commands.append(command)
            return {"document_id": command.document_id, "corpus_snapshot_id": "snapshot-1"}

    service = FakeService()
    context = OfflinePipelineContext(repository=FakePipelineRepository(), service=service, worker_id="worker-1")
    stage = LegacyIngestStage()
    state = OfflinePipelineState(
        command=OfflinePipelineCommand(job_id="job-1", document_id="doc-1", source_id="src-1", tenant_id="shared", source_type="text")
    )

    result = __import__("asyncio").run(stage.run(context, state))

    assert service.commands[0].job_id == "job-1"
    assert result.output == {"document_id": "doc-1", "corpus_snapshot_id": "snapshot-1"}
    assert result.retrieval_visible is True


def test_legacy_ingest_stage_is_the_current_retrieval_visibility_boundary() -> None:
    assert LegacyIngestStage.retrieval_visibility_boundary is True


def test_chunk_documents_stage_owns_parsing_chunking_and_validation(monkeypatch) -> None:
    blocks = [SimpleNamespace(block_id="block-1")]
    chunk = SimpleNamespace(chunk_id="chunk-1", text="Honey bees swarm.", metadata={}, chunk_index=0)
    validation = SimpleNamespace(status="accepted", quality_score=0.9, reasons=[])
    monkeypatch.setattr(chunk_stage_module, "parse_text", lambda **kwargs: blocks)
    monkeypatch.setattr(chunk_stage_module, "build_chunks", lambda **kwargs: [chunk])
    monkeypatch.setattr(chunk_stage_module, "validate_chunk", lambda chunk_obj: validation)

    class FakeRepository:
        def __init__(self) -> None:
            self.saved_blocks = None
            self.saved_chunks = None
            self.saved_validations = None
            self.updated_chunk_ids = []

        def save_blocks(self, value):
            self.saved_blocks = value

        def save_chunks(self, value):
            self.saved_chunks = value

        def save_validations(self, value):
            self.saved_validations = value

        def update_chunk_metadata(self, chunk_id, metadata):
            self.updated_chunk_ids.append(chunk_id)

    stage_events: list[tuple[str, str]] = []

    class FakeService:
        def __init__(self) -> None:
            self.repository = FakeRepository()
            self.synopsis_calls = []

        def _relink_chunks_with_assets(self, document_id, chunks, assets, persist=True):
            return []

        def _group_assets_by_chunk(self, links, assets):
            return {}

        def _apply_chunk_enrichment(self, chunk_obj, status, quality_score, reasons):
            chunk_obj.metadata["validation_status"] = status

        def _refresh_document_synopses(self, document_id, *, accepted_chunks=None, source_stage="chunks_validated"):
            self.synopsis_calls.append((document_id, source_stage, len(accepted_chunks or [])))
            return {"sections": 1}

        def _emit_progress(self, **kwargs):
            stage_events.append(("progress", str(kwargs.get("phase") or "")))

    service = FakeService()

    result = chunk_stage_module.run_chunk_documents_stage(
        service,
        document_id="doc-1",
        source=SimpleNamespace(
            tenant_id="shared",
            parser_version="v1",
            document_class="book",
            filename="bees.txt",
        ),
        normalized_text="Honey bees swarm.",
        multimodal_payload=None,
        job_id="job-1",
        ensure_lease_active=lambda: None,
        start_stage=lambda name, phase, detail, metrics=None: stage_events.append(("start", name)),
        finish_stage=lambda phase, detail, outcome, **kwargs: stage_events.append(("finish", phase)),
    )

    assert result.blocks == blocks
    assert result.chunks == [chunk]
    assert result.accepted_chunks == [chunk]
    assert result.validation_metrics == {"accepted": 1, "review": 0, "rejected": 0}
    assert service.repository.saved_blocks == blocks
    assert service.repository.saved_chunks == [chunk]
    assert service.repository.saved_validations == [validation]
    assert service.repository.updated_chunk_ids == ["chunk-1"]
    assert service.synopsis_calls == [("doc-1", "chunks_validated", 1)]
    assert ("start", "parsed") in stage_events
    assert ("start", "chunked") in stage_events
    assert ("start", "chunks_validated") in stage_events


def test_publish_corpus_stage_owns_retrieval_visibility_boundary() -> None:
    stage_events: list[tuple[str, str]] = []
    created_snapshots: list[dict] = []

    class FakeEmbedder:
        def embed(self, texts, progress_callback=None):
            if progress_callback is not None:
                progress_callback({"completed": len(texts), "total": len(texts), "batch_size": len(texts)})
            return [[0.1, 0.2] for _ in texts]

    class FakeRepository:
        def create_pending_corpus_snapshot(self, tenant_id, snapshot_type, **kwargs):
            created_snapshots.append({"tenant_id": tenant_id, "snapshot_type": snapshot_type, **kwargs})
            return "snapshot-1"

        def activate_corpus_snapshot(self, snapshot_id, **kwargs):
            created_snapshots[-1]["activated_snapshot_id"] = snapshot_id
            created_snapshots[-1]["activation"] = kwargs

    class FakeService:
        def __init__(self) -> None:
            self.embedder = FakeEmbedder()
            self.repository = FakeRepository()
            self.progress = []

        def _emit_progress(self, **kwargs):
            self.progress.append(kwargs)

        def _is_indexable_asset(self, asset):
            return True

        def _publish_fresh_document_vectors(self, **kwargs):
            return 1, 1

        def _build_completion_metrics(self, document_id, *, indexed_chunks, indexed_assets, publish_skipped_due_to_kg_review):
            return {
                "document_id": document_id,
                "indexed_chunks": indexed_chunks,
                "indexed_assets": indexed_assets,
                "publish_skipped_due_to_kg_review": publish_skipped_due_to_kg_review,
            }

    service = FakeService()
    result = publish_stage_module.run_publish_corpus_stage(
        service,
        document_id="doc-1",
        source=SimpleNamespace(tenant_id="shared", filename="bees.txt", document_class="book", source_type="text"),
        blocks_count=1,
        chunks_count=1,
        accepted_chunks=[SimpleNamespace(text="Honey bees swarm.")],
        multimodal_payload=SimpleNamespace(
            assets=[SimpleNamespace(search_text="bee image")],
            pages=[{"page_number": 1}],
        ),
        kg_failures=[],
        job_id="job-1",
        ensure_lease_active=lambda: None,
        start_stage=lambda name, phase, detail, metrics=None: stage_events.append(("start", name)),
        finish_stage=lambda phase, detail, outcome, **kwargs: stage_events.append(("finish", phase)),
    )

    assert result.indexed_chunks == 1
    assert result.indexed_assets == 1
    assert result.corpus_snapshot_id == "snapshot-1"
    assert result.final_status == "completed"
    assert created_snapshots[0]["snapshot_type"] == "document_publish"
    assert ("start", "indexed") in stage_events
    assert ("finish", "embedding") in stage_events


def test_prepare_source_document_owns_normalization_and_metrics() -> None:
    source = SimpleNamespace(
        raw_text="Honey\x00 bees\n\nswarm.",
        normalized_text="",
        extraction_metrics=None,
    )

    prepared = prepare_source_document(source)

    assert "\x00" not in prepared.raw_text
    assert prepared.normalized_text
    assert isinstance(prepared.extraction_metrics, dict)
    assert prepared.extraction_metrics["raw_chars"] >= len(prepared.raw_text)


def test_extract_multimodal_payload_stage_uses_service_progress_boundary(monkeypatch) -> None:
    calls = {}

    monkeypatch.setattr(
        "src.bee_ingestion.offline_pipeline.stages.extract_multimodal.extract_pdf_multimodal_payload",
        lambda **kwargs: calls.update(kwargs) or {"pages": [], "assets": [], "metrics": {}, "merged_text": ""},
    )

    progress = []
    service = SimpleNamespace(
        progress_callback=True,
        _resolve_replayable_pdf_path=lambda source: "E:\\n8n to python\\sample.pdf",
        _emit_progress=lambda **kwargs: progress.append(kwargs),
    )
    source = SimpleNamespace(
        source_type="pdf",
        tenant_id="shared",
        filename="sample.pdf",
        metadata={"page_range": {"start": 1, "end": 2}},
    )

    extract_multimodal_payload(service, document_id="doc-1", source=source)
    calls["progress_callback"]({"detail": "preparing", "metrics": {"pages": 2}})

    assert calls["document_id"] == "doc-1"
    assert calls["page_start"] == 1
    assert progress[0]["phase"] == "preparing"


def test_build_kg_stage_owns_iteration_and_failure_tracking(monkeypatch) -> None:
    stage_events: list[tuple[str, str]] = []
    progress_events = []
    monkeypatch.setattr(
        build_kg_stage_module,
        "run_kg_pipeline",
        lambda service, document_id, chunk, linked_assets=None, persist=True, pre_persist_check=None: {
            "chunk_id": chunk.chunk_id,
            "status": "review" if chunk.chunk_id == "chunk-2" else "validated",
            "errors": ["needs_review"] if chunk.chunk_id == "chunk-2" else [],
        },
    )

    service = SimpleNamespace(
        _emit_progress=lambda **kwargs: progress_events.append(kwargs),
        _build_graph_quality_metrics=lambda document_id, accepted_chunk_count: {"document_id": document_id, "accepted_chunks": accepted_chunk_count},
    )

    results, failures, metrics = build_kg_stage_module.run_build_kg_stage(
        service,
        document_id="doc-1",
        accepted_chunks=[SimpleNamespace(chunk_id="chunk-1"), SimpleNamespace(chunk_id="chunk-2")],
        linked_assets_by_chunk={},
        job_id="job-1",
        ensure_lease_active=lambda: None,
        start_stage=lambda name, phase, detail, metrics=None: stage_events.append(("start", name)),
        finish_stage=lambda phase, detail, outcome, **kwargs: stage_events.append(("finish", outcome)),
    )

    assert [item["chunk_id"] for item in results] == ["chunk-1", "chunk-2"]
    assert failures == [{"chunk_id": "chunk-2", "status": "review", "errors": ["needs_review"]}]
    assert metrics["accepted_chunks"] == 2
    assert ("start", "kg_validated") in stage_events
    assert ("finish", "review") in stage_events
    assert len(progress_events) == 2
