from uuid import uuid4

import psycopg
import pytest

from src.bee_ingestion.models import KGExtractionResult, SourceDocument
from src.bee_ingestion.repository import Repository
from src.bee_ingestion.settings import settings
from src.bee_ingestion.storage.bootstrap import ensure_app_storage_compatibility


def _source() -> SourceDocument:
    nonce = str(uuid4())
    return SourceDocument(
        tenant_id="test-tenant",
        source_type="text",
        filename=f"repo-{nonce}.txt",
        raw_text=f"Repository test text {nonce}",
        document_class="note",
    )


def _repository() -> Repository:
    ensure_app_storage_compatibility()
    return Repository()


def _create_job(repository: Repository, document_id: str) -> str:
    return repository.create_job(
        document_id=document_id,
        extractor_version=settings.extractor_version,
        normalizer_version=settings.normalizer_version,
        parser_version="v1",
        chunker_version=settings.chunker_version,
        validator_version=settings.validator_version,
        embedding_version=settings.embedding_model,
        kg_version="v1",
    )


def test_create_job_persists_versions_and_registered_status() -> None:
    repository = _repository()
    document_id, _ = repository.register_document(_source())

    job_id = _create_job(repository, document_id)
    job = repository.get_job(job_id)

    assert job is not None
    assert job["status"] == "registered"
    assert job["extractor_version"] == settings.extractor_version
    assert job["normalizer_version"] == settings.normalizer_version
    assert job["validator_version"] == settings.validator_version


def test_claim_job_prevents_second_active_worker() -> None:
    repository = _repository()
    document_id, _ = repository.register_document(_source())
    job_id = _create_job(repository, document_id)

    assert repository.claim_job(job_id, "worker-a", 300) is True
    assert repository.claim_job(job_id, "worker-b", 300) is False


def test_record_stage_rejects_invalid_transition() -> None:
    repository = _repository()
    document_id, _ = repository.register_document(_source())
    job_id = _create_job(repository, document_id)

    with pytest.raises(ValueError):
        repository.record_stage(
            job_id=job_id,
            document_id=document_id,
            stage_name="indexed",
            job_status="indexed",
            stage_outcome="completed",
            worker_version=settings.worker_version,
            input_version="test",
        )


def test_record_stage_updates_job_status_for_valid_transition() -> None:
    repository = _repository()
    document_id, _ = repository.register_document(_source())
    job_id = _create_job(repository, document_id)

    repository.record_stage(
        job_id=job_id,
        document_id=document_id,
        stage_name="content_available",
        job_status="content_available",
        stage_outcome="completed",
        worker_version=settings.worker_version,
        input_version="test",
    )
    job = repository.get_job(job_id)

    assert job is not None
    assert job["status"] == "content_available"


def test_save_kg_result_replaces_prior_chunk_assertions() -> None:
    repository = _repository()
    document_id, _ = repository.register_document(_source())
    chunk_id = f"chunk-{uuid4()}"

    first = KGExtractionResult(
        source_id=document_id,
        segment_id=chunk_id,
        mentions=[
            {"mention_id": "m1", "text": "Queen", "type_hint": "Queen", "start": 0, "end": 5, "confidence": 0.9},
            {"mention_id": "m2", "text": "Honey", "type_hint": "Honey", "start": 10, "end": 15, "confidence": 0.9},
        ],
        candidate_entities=[
            {"candidate_id": "e1", "mention_ids": ["m1"], "proposed_type": "Queen", "canonical_name": "Queen", "external_ids": [], "confidence": 0.9},
            {"candidate_id": "e2", "mention_ids": ["m2"], "proposed_type": "Honey", "canonical_name": "Honey", "external_ids": [], "confidence": 0.9},
        ],
        candidate_relations=[
            {
                "relation_id": "r1",
                "subject_candidate_id": "e1",
                "predicate_text": "produces",
                "object_candidate_id": "e2",
                "object_literal": None,
                "qualifiers": {},
                "confidence": 0.9,
            }
        ],
        candidate_events=[],
        evidence=[
            {"evidence_id": "ev1", "supports": ["r1"], "excerpt": "Queen produces honey", "start": 0, "end": 20}
        ],
    )
    second = KGExtractionResult(
        source_id=document_id,
        segment_id=chunk_id,
        mentions=[],
        candidate_entities=[],
        candidate_relations=[],
        candidate_events=[],
        evidence=[],
    )

    with psycopg.connect(settings.postgres_dsn) as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO document_chunks (
                  chunk_id, document_id, tenant_id, chunk_index, page_start, page_end, section_path,
                  prev_chunk_id, next_chunk_id, char_start, char_end, content_type, text,
                  parser_version, chunker_version, content_hash, metadata_json, status
                ) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
                """,
                (
                    chunk_id,
                    document_id,
                    "test-tenant",
                    1,
                    1,
                    1,
                    [],
                    None,
                    None,
                    0,
                    20,
                    "text",
                    "Queen produces honey",
                    "v1",
                    "v1",
                    "sha256:test",
                    "{}",
                    "accepted",
                ),
            )
        conn.commit()

    repository.save_kg_result(document_id, chunk_id, first, "validated", [], provider="heuristic", model="rules", prompt_version="v1")
    repository.save_kg_result(document_id, chunk_id, second, "review", ["missing_evidence"], provider="openai", model="gpt-5-mini", prompt_version="v2")

    with psycopg.connect(settings.postgres_dsn) as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT COUNT(*) FROM kg_assertions WHERE chunk_id = %s", (chunk_id,))
            assertion_count = cur.fetchone()[0]
            cur.execute("SELECT COUNT(*) FROM kg_raw_extractions WHERE chunk_id = %s", (chunk_id,))
            raw_count = cur.fetchone()[0]
            cur.execute(
                "SELECT status FROM kg_raw_extractions WHERE chunk_id = %s ORDER BY created_at DESC LIMIT 1",
                (chunk_id,),
            )
            latest_status = cur.fetchone()[0]

    assert assertion_count == 0
    assert raw_count == 1
    assert latest_status == "review"


def test_list_kg_raw_extractions_filters_by_status() -> None:
    repository = _repository()
    document_id, _ = repository.register_document(_source())
    chunk_id = f"chunk-{uuid4()}"

    with psycopg.connect(settings.postgres_dsn) as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO document_chunks (
                  chunk_id, document_id, tenant_id, chunk_index, page_start, page_end, section_path,
                  prev_chunk_id, next_chunk_id, char_start, char_end, content_type, text,
                  parser_version, chunker_version, content_hash, metadata_json, status
                ) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
                """,
                (
                    chunk_id,
                    document_id,
                    "test-tenant",
                    1,
                    1,
                    1,
                    [],
                    None,
                    None,
                    0,
                    12,
                    "text",
                    "bad extraction",
                    "v1",
                    "v1",
                    "sha256:test-raw",
                    "{}",
                    "accepted",
                ),
            )
        conn.commit()

    repository.save_kg_result(
        document_id,
        chunk_id,
        KGExtractionResult(
            source_id=document_id,
            segment_id=chunk_id,
            mentions=[],
            candidate_entities=[],
            candidate_relations=[],
            candidate_events=[],
            evidence=[],
        ),
        "quarantined",
        ["extractor_error:test"],
        raw_payload={"error": "test"},
        provider="openai",
        model="gpt-5-mini",
        prompt_version="v2",
    )

    rows = repository.list_kg_raw_extractions(document_id=document_id, status="quarantined", limit=10)

    assert rows
    assert rows[0]["status"] == "quarantined"


def test_save_kg_result_sanitizes_nul_characters_in_payload_and_columns() -> None:
    repository = _repository()
    document_id, _ = repository.register_document(_source())
    chunk_id = f"chunk-{uuid4()}"

    with psycopg.connect(settings.postgres_dsn) as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO document_chunks (
                  chunk_id, document_id, tenant_id, chunk_index, page_start, page_end, section_path,
                  prev_chunk_id, next_chunk_id, char_start, char_end, content_type, text,
                  parser_version, chunker_version, content_hash, metadata_json, status
                ) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
                """,
                (
                    chunk_id,
                    document_id,
                    "test-tenant",
                    1,
                    1,
                    1,
                    [],
                    None,
                    None,
                    0,
                    24,
                    "text",
                    "Queen antenna brood",
                    "v1",
                    "v1",
                    "sha256:test-sanitize",
                    "{}",
                    "accepted",
                ),
            )
        conn.commit()

    result = KGExtractionResult(
        source_id=document_id,
        segment_id=chunk_id,
        mentions=[
            {"mention_id": "m1", "text": "antenn\x00a", "type_hint": "Queen", "start": 0, "end": 7, "confidence": 0.9},
            {"mention_id": "m2", "text": "Honey", "type_hint": "Honey", "start": 8, "end": 13, "confidence": 0.9},
        ],
        candidate_entities=[
            {"candidate_id": "e1", "mention_ids": ["m1"], "proposed_type": "Queen", "canonical_name": "Que\x00en", "external_ids": [], "confidence": 0.9},
            {"candidate_id": "e2", "mention_ids": ["m2"], "proposed_type": "Honey", "canonical_name": "Hon\x00ey", "external_ids": [], "confidence": 0.9},
        ],
        candidate_relations=[
            {
                "relation_id": "r1",
                "subject_candidate_id": "e1",
                "predicate_text": "produ\x00ces",
                "object_candidate_id": "e2",
                "object_literal": "liq\x00uid",
                "qualifiers": {"note": "qual\x00ifier"},
                "confidence": 0.9,
            }
        ],
        candidate_events=[],
        evidence=[
            {"evidence_id": "ev1", "supports": ["r1"], "excerpt": "antenn\x00a evidence", "start": 0, "end": 16}
        ],
    )

    repository.save_kg_result(
        document_id=document_id,
        chunk_id=chunk_id,
        result=result,
        status="validated",
        errors=[],
        raw_payload={"raw": "bad\x00payload"},
        provider="openai",
        model="gpt-5-mini",
        prompt_version="v2",
    )

    with psycopg.connect(settings.postgres_dsn) as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT payload::text FROM kg_raw_extractions WHERE chunk_id = %s ORDER BY created_at DESC LIMIT 1", (chunk_id,))
            payload_text = cur.fetchone()[0]
            cur.execute("SELECT canonical_name FROM kg_entities WHERE entity_id = %s", ("queen_queen",))
            queen_name = cur.fetchone()[0]
            cur.execute("SELECT predicate, object_literal FROM kg_assertions WHERE chunk_id = %s LIMIT 1", (chunk_id,))
            predicate, object_literal = cur.fetchone()
            cur.execute("SELECT excerpt FROM kg_assertion_evidence WHERE assertion_id = %s LIMIT 1", (f"{chunk_id}:r1",))
            excerpt = cur.fetchone()[0]

    assert "\x00" not in payload_text
    assert queen_name == "Queen"
    assert predicate == "produces"
    assert object_literal == "liquid"
    assert excerpt == "antenna evidence"
