"""Offline pipeline ownership for the replay_quarantined_kg operation."""

from __future__ import annotations

import logging
import threading
from datetime import UTC, datetime
from typing import Any

from src.bee_ingestion.settings import settings

from src.bee_ingestion.models import Chunk

logger = logging.getLogger(__name__)


def replay_quarantined_kg(
    service,
    *,
    document_id: str | None = None,
    chunk_ids: list[str] | None = None,
    publish_if_clean: bool = True,
) -> dict:
    requested_chunk_ids = [str(chunk_id).strip() for chunk_id in (chunk_ids or []) if str(chunk_id).strip()]
    document_targets: dict[str, set[str]] = {}

    if requested_chunk_ids:
        for chunk_id in requested_chunk_ids:
            extraction_rows = service._list_all_kg_raw_extractions(chunk_id=chunk_id, status="quarantined", batch_size=10)
            if not extraction_rows:
                continue
            chunk_document_id = str(extraction_rows[0]["document_id"])
            document_targets.setdefault(chunk_document_id, set()).add(chunk_id)
    elif document_id:
        document_targets[str(document_id)] = set()
    else:
        for extraction in service._list_all_kg_raw_extractions(status="quarantined"):
            document_targets.setdefault(str(extraction["document_id"]), set()).add(str(extraction["chunk_id"]))

    document_results: list[dict[str, Any]] = []
    replayed_chunks = 0
    published_documents = 0

    for target_document_id, target_chunk_ids in document_targets.items():
        result = _replay_quarantined_document_kg(service,
            target_document_id,
            target_chunk_ids=sorted(target_chunk_ids) if target_chunk_ids else None,
            publish_if_clean=publish_if_clean,
        )
        replayed_chunks += int(result.get("replayed_chunks") or 0)
        if result.get("published"):
            published_documents += 1
        document_results.append(result)

    return {
        "document_results": document_results,
        "documents": len(document_results),
        "replayed_chunks": replayed_chunks,
        "published_documents": published_documents,
    }


def _replay_quarantined_document_kg(
    service,
    document_id: str,
    *,
    target_chunk_ids: list[str] | None = None,
    publish_if_clean: bool = True,
) -> dict[str, Any]:
    with service.repository.advisory_lock("document-mutate", document_id):
        source_row = service.repository.get_latest_document_source(document_id)
        if source_row is None:
            raise ValueError("Document source not found")

        accepted_rows = service._list_all_chunk_records_for_kg(document_id)
        accepted_rows_by_id = {str(row["chunk_id"]): row for row in accepted_rows}
        quarantined_rows = service._list_all_kg_raw_extractions(document_id=document_id, status="quarantined")
        quarantined_chunk_ids = {str(row["chunk_id"]) for row in quarantined_rows}
        requested_chunk_ids = {str(chunk_id) for chunk_id in (target_chunk_ids or []) if str(chunk_id)}
        selected_chunk_ids = requested_chunk_ids or quarantined_chunk_ids
        target_rows = [accepted_rows_by_id[chunk_id] for chunk_id in selected_chunk_ids if chunk_id in accepted_rows_by_id]
        target_rows.sort(key=lambda row: int(row.get("chunk_index") or 0))
        skipped_chunk_ids = sorted(selected_chunk_ids - {str(row["chunk_id"]) for row in target_rows})

        if not target_rows and not publish_if_clean:
            return {
                "document_id": document_id,
                "job_id": None,
                "replayed_chunks": 0,
                "requested_chunks": len(selected_chunk_ids),
                "skipped_chunks": skipped_chunk_ids,
                "published": False,
                "status": str((service.repository.get_document_detail(document_id) or {}).get("document", {}).get("status") or ""),
            }

        job_id = service.repository.create_job(
            document_id=document_id,
            extractor_version=settings.extractor_version,
            normalizer_version=settings.normalizer_version,
            parser_version=str(source_row.get("parser_version") or "v1"),
            chunker_version=settings.chunker_version,
            validator_version=settings.validator_version,
            embedding_version=settings.embedding_model,
            kg_version=f"{settings.kg_extraction_provider}:{settings.kg_model}:{settings.kg_prompt_version}",
        )
        if not service.repository.claim_job(job_id, service.worker_id, settings.job_lease_seconds):
            raise ValueError("Unable to claim KG replay job")

        input_version = service._build_input_version(str(source_row.get("parser_version") or "v1"))
        lease_stop = threading.Event()
        lease_lost = threading.Event()
        lease_interval_seconds = max(5, min(settings.job_lease_seconds // 3, 60))
        active_stage_run_id: str | None = None

        def renew_lease_loop() -> None:
            while not lease_stop.wait(lease_interval_seconds):
                try:
                    renewed = service.repository.renew_job_lease(job_id, service.worker_id, settings.job_lease_seconds)
                except Exception:
                    logger.warning("Failed to renew KG replay job lease for %s", job_id, exc_info=True)
                    continue
                if not renewed:
                    logger.warning("Lost KG replay job lease for %s", job_id)
                    lease_lost.set()
                    break

        def ensure_lease_active() -> None:
            if lease_lost.is_set():
                raise RuntimeError("KG replay job lease was lost during processing")

        def record_completed_stage(stage_name: str, job_status: str, detail: str, metrics: dict[str, Any]) -> None:
            started_at = datetime.now(UTC)
            service.repository.record_stage(
                job_id=job_id,
                document_id=document_id,
                stage_name=stage_name,
                job_status=job_status,
                stage_outcome="completed",
                metrics=metrics,
                worker_version=settings.worker_version,
                input_version=input_version,
                started_at=started_at,
                finished_at=started_at,
            )
            service._emit_progress(
                phase=stage_name,
                detail=detail,
                document_id=document_id,
                job_id=job_id,
                metrics=metrics,
            )

        def start_stage(stage_name: str, phase: str, detail: str, metrics: dict[str, Any]) -> None:
            nonlocal active_stage_run_id
            active_stage_run_id = service.repository.start_stage_run(
                job_id=job_id,
                document_id=document_id,
                stage_name=stage_name,
                job_status=stage_name,
                metrics=metrics,
                worker_version=settings.worker_version,
                input_version=input_version,
                started_at=datetime.now(UTC),
            )
            service._emit_progress(
                phase=phase,
                detail=detail,
                document_id=document_id,
                job_id=job_id,
                metrics=metrics,
            )

        def finish_stage(
            phase: str,
            detail: str,
            outcome: str,
            *,
            metrics: dict[str, Any],
            job_status: str | None = None,
            error_message: str | None = None,
        ) -> None:
            nonlocal active_stage_run_id
            if active_stage_run_id is None:
                return
            service.repository.finish_stage_run(
                stage_run_id=active_stage_run_id,
                job_id=job_id,
                document_id=document_id,
                stage_outcome=outcome,
                job_status=job_status,
                metrics=metrics,
                error_message=error_message,
                finished_at=datetime.now(UTC),
            )
            service._emit_progress(
                phase=phase,
                detail=detail,
                document_id=document_id,
                job_id=job_id,
                metrics=metrics,
            )
            active_stage_run_id = None

        lease_thread = threading.Thread(target=renew_lease_loop, name=f"kg-replay-lease-{job_id}", daemon=True)
        lease_thread.start()
        try:
            summary_metrics = {
                "repair": True,
                "requested_quarantined_chunks": len(selected_chunk_ids),
                "replay_chunks": len(target_rows),
                "skipped_chunks": len(skipped_chunk_ids),
            }
            record_completed_stage("content_available", "content_available", "Using persisted source and assets for targeted KG replay.", summary_metrics)
            record_completed_stage("parsed", "parsed", "Using previously parsed blocks.", summary_metrics)
            record_completed_stage("chunked", "chunked", "Using previously chunked accepted content.", summary_metrics)
            record_completed_stage("chunks_validated", "chunks_validated", "Using previously validated accepted chunks.", summary_metrics)

            replay_metrics = {
                "repair": True,
                "kg_total": len(target_rows),
                "kg_completed": 0,
                "kg_failures": 0,
                "skipped_chunks": len(skipped_chunk_ids),
            }
            start_stage(
                "kg_validated",
                "kg",
                f"Replaying quarantined KG chunks {0}/{len(target_rows)}.",
                replay_metrics,
            )

            replayed_chunks = 0
            replay_status_counts: dict[str, int] = {"validated": 0, "review": 0, "skipped": 0, "quarantined": 0}
            for index, row in enumerate(target_rows, start=1):
                ensure_lease_active()
                chunk = service._chunk_from_record(row)
                kg_record = service._run_kg_pipeline(
                    document_id,
                    chunk,
                    linked_assets=service._load_assets_for_chunk(chunk.chunk_id),
                    pre_persist_check=ensure_lease_active,
                )
                replayed_chunks += 1
                replay_status = str(kg_record["status"])
                replay_status_counts[replay_status] = replay_status_counts.get(replay_status, 0) + 1
                service._emit_progress(
                    phase="kg",
                    detail=f"Replaying quarantined KG chunks {index}/{len(target_rows)}.",
                    document_id=document_id,
                    job_id=job_id,
                    metrics={
                        "repair": True,
                        "kg_total": len(target_rows),
                        "kg_completed": index,
                        "kg_failures": replay_status_counts.get("review", 0) + replay_status_counts.get("quarantined", 0),
                        "skipped_chunks": len(skipped_chunk_ids),
                    },
                )

            review_count = service.repository.count_kg_raw_extractions(document_id=document_id, status="review")
            skipped_count = service.repository.count_kg_raw_extractions(document_id=document_id, status="skipped")
            quarantined_count = service.repository.count_kg_raw_extractions(document_id=document_id, status="quarantined")
            validated_count = service.repository.count_kg_raw_extractions(document_id=document_id, status="validated")
            kg_failure_count = review_count + quarantined_count
            final_kg_metrics = {
                "repair": True,
                "requested_quarantined_chunks": len(selected_chunk_ids),
                "replayed_chunks": replayed_chunks,
                "skipped_chunks": len(skipped_chunk_ids),
                "kg_total": validated_count + review_count + skipped_count + quarantined_count,
                "kg_validated": validated_count,
                "kg_review": review_count,
                "kg_skipped": skipped_count,
                "kg_quarantined": quarantined_count,
            }

            if kg_failure_count > 0:
                finish_stage(
                    "review",
                    "KG replay finished but review is still required.",
                    "review",
                    metrics=final_kg_metrics,
                    job_status="review",
                    error_message=None if review_count == 0 and quarantined_count == 0 else str({"review": review_count, "quarantined": quarantined_count}),
                )
                return {
                    "document_id": document_id,
                    "job_id": job_id,
                    "replayed_chunks": replayed_chunks,
                    "requested_chunks": len(selected_chunk_ids),
                    "skipped_chunks": skipped_chunk_ids,
                    "published": False,
                    "status": "review",
                    "kg_review": review_count,
                    "kg_quarantined": quarantined_count,
                }

            finish_stage(
                "kg",
                "Completed targeted KG replay.",
                "completed",
                metrics=final_kg_metrics,
            )

            indexed_chunks = 0
            indexed_assets = 0
            corpus_snapshot_id = None
            if publish_if_clean:
                all_assets = service._list_all_page_assets(document_id)
                accepted_chunks = [service._chunk_from_record(row) for row in accepted_rows]
                indexable_assets = [asset for asset in all_assets if service._is_indexable_asset(asset)]
                start_stage(
                    "indexed",
                    "embedding",
                    "Publishing vectors for repaired document.",
                    {
                        "repair": True,
                        "indexed_chunks": 0,
                        "indexed_assets": 0,
                        "accepted_chunks": len(accepted_chunks),
                        "indexable_assets": len(indexable_assets),
                    },
                )
                corpus_snapshot_id = service.repository.create_pending_corpus_snapshot(
                    str(source_row["tenant_id"]),
                    "repair_publish",
                    document_id=document_id,
                    job_id=job_id,
                    summary="Published repaired document state into the retrieval-visible corpus.",
                    metrics={
                        "repair": True,
                        "accepted_chunks": len(accepted_chunks),
                        "indexable_assets": len(indexable_assets),
                        "indexed_chunks": 0,
                        "indexed_assets": 0,
                    },
                    metadata={
                        "repair": True,
                        "requested_quarantined_chunks": len(selected_chunk_ids),
                        "replayed_chunks": replayed_chunks,
                    },
                )
                ensure_lease_active()
                chunk_embeddings = service.embedder.embed([chunk.text for chunk in accepted_chunks]) if accepted_chunks else []
                ensure_lease_active()
                asset_embeddings = service.embedder.embed([asset.search_text for asset in indexable_assets]) if indexable_assets else []
                ensure_lease_active()
                indexed_chunks, indexed_assets = service._publish_fresh_document_vectors(
                    document_id=document_id,
                    accepted_chunks=accepted_chunks,
                    chunk_embeddings=chunk_embeddings,
                    indexable_assets=indexable_assets,
                    asset_embeddings=asset_embeddings,
                )
                service.repository.activate_corpus_snapshot(
                    corpus_snapshot_id,
                    document_id=document_id,
                    job_id=job_id,
                    metadata_patch={
                        "indexed_chunks": indexed_chunks,
                        "indexed_assets": indexed_assets,
                    },
                    metrics_patch={
                        "indexed_chunks": indexed_chunks,
                        "indexed_assets": indexed_assets,
                    },
                )
                finish_stage(
                    "embedding",
                    "Completed vector publish for repaired document.",
                    "completed",
                    metrics={
                        "repair": True,
                        "indexed_chunks": indexed_chunks,
                        "indexed_assets": indexed_assets,
                        "accepted_chunks": len(accepted_chunks),
                        "indexable_assets": len(indexable_assets),
                    },
                    job_status="completed",
                )
            else:
                service.repository.update_document_record(document_id, {"status": "completed"})
                service.repository.record_stage(
                    job_id=job_id,
                    document_id=document_id,
                    stage_name="indexed",
                    job_status="completed",
                    stage_outcome="completed",
                    metrics={"repair": True, "publish_skipped": True},
                    worker_version=settings.worker_version,
                    input_version=input_version,
                    started_at=datetime.now(UTC),
                    finished_at=datetime.now(UTC),
                )

            return {
                "document_id": document_id,
                "job_id": job_id,
                "replayed_chunks": replayed_chunks,
                "requested_chunks": len(selected_chunk_ids),
                "skipped_chunks": skipped_chunk_ids,
                "published": bool(publish_if_clean),
                "status": "completed",
                "indexed_chunks": indexed_chunks,
                "indexed_assets": indexed_assets,
                "corpus_snapshot_id": corpus_snapshot_id,
                "kg_review": 0,
                "kg_quarantined": 0,
            }
        except Exception:
            if active_stage_run_id is not None:
                try:
                    finish_stage(
                        "failed",
                        "Targeted KG replay failed.",
                        "failed",
                        metrics={"repair": True},
                        job_status="failed",
                        error_message="targeted_kg_replay_failed",
                    )
                except Exception:
                    pass
            raise
        finally:
            lease_stop.set()
            lease_thread.join(timeout=5)
            service.repository.release_job(job_id, service.worker_id)

