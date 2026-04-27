"""Offline pipeline ownership for the resume_document_ingest operation."""

from __future__ import annotations

import logging
import threading
from datetime import UTC, datetime
from typing import Any

from src.bee_ingestion.settings import settings

from src.bee_ingestion.models import Chunk, SourceDocument

logger = logging.getLogger(__name__)


def resume_document_ingest(service, document_id: str) -> dict:
    with service.repository.advisory_lock("document-mutate", document_id):
        source_row = service.repository.get_latest_document_source(document_id)
        if source_row is None:
            raise ValueError("Document source not found")
        job = service.repository.get_latest_job_for_document(document_id)
        if job is None:
            raise ValueError("Ingestion job not found")

        current_status = str(job.get("status") or "").strip().lower()
        if current_status in {"completed", "review"}:
            detail = service.repository.get_document_detail(document_id)
            document = dict((detail or {}).get("document") or {})
            assets = service._list_all_page_assets(document_id)
            return {
                "job_id": str(job["job_id"]),
                "document_id": document_id,
                "source_id": str(source_row["source_id"]),
                "blocks": 0,
                "chunks": int(document.get("total_chunks") or 0),
                "accepted_chunks": int(document.get("accepted_chunks") or 0),
                "pages": len(service.repository.list_document_pages(document_id=document_id, limit=5000)),
                "page_assets": len(assets),
                "indexed_assets": len([asset for asset in assets if service._is_indexable_asset(asset)]),
                "kg_failures": [],
                "resumed": True,
                "resume_from_status": current_status,
            }
        if current_status not in {"chunks_validated", "kg_validated", "indexed"}:
            raise ValueError(f"Resume is not supported from job status '{current_status}'")

        source = service._prepare_source(
            SourceDocument(
                tenant_id=str(source_row["tenant_id"]),
                source_type=str(source_row["source_type"]),
                filename=str(source_row["filename"]),
                raw_text=str(source_row["raw_text"]),
                normalized_text=str(source_row.get("normalized_text") or ""),
                extraction_metrics=dict(source_row.get("extraction_metrics_json") or {}),
                metadata=dict(source_row.get("metadata_json") or {}),
                document_class=str(source_row["document_class"]),
                parser_version=str(source_row.get("parser_version") or "v1"),
                ocr_engine=source_row.get("ocr_engine"),
                ocr_model=source_row.get("ocr_model"),
                content_hash_value=str(source_row.get("content_hash") or ""),
            )
        )
        if source.source_type == "pdf":
            service._resolve_replayable_pdf_path(source)

        job_id = str(job["job_id"])
        if not service.repository.claim_job(job_id, service.worker_id, settings.job_lease_seconds, preserve_status=True):
            raise ValueError("Unable to claim interrupted ingestion job")

        input_version = service._build_input_version(source.parser_version)
        lease_stop = threading.Event()
        lease_lost = threading.Event()
        lease_interval_seconds = max(5, min(settings.job_lease_seconds // 3, 60))

        def renew_lease_loop() -> None:
            while not lease_stop.wait(lease_interval_seconds):
                try:
                    renewed = service.repository.renew_job_lease(job_id, service.worker_id, settings.job_lease_seconds)
                except Exception:
                    logger.warning("Failed to renew interrupted-ingest job lease for %s", job_id, exc_info=True)
                    continue
                if not renewed:
                    logger.warning("Lost interrupted-ingest job lease for %s", job_id)
                    lease_lost.set()
                    break

        def ensure_lease_active() -> None:
            if lease_lost.is_set():
                raise RuntimeError("Ingestion job lease was lost during processing")

        lease_thread = threading.Thread(target=renew_lease_loop, name=f"ingest-resume-lease-{job_id}", daemon=True)
        lease_thread.start()
        running_stage = service.repository.get_running_stage_run(job_id)
        active_stage_run_id = str(running_stage["stage_run_id"]) if running_stage else None
        active_stage_name = str(running_stage["stage_name"]) if running_stage else None

        def start_stage(stage_name: str, phase: str, detail: str, metrics: dict[str, Any] | None = None) -> str:
            nonlocal active_stage_run_id, active_stage_name
            if active_stage_run_id and active_stage_name == stage_name:
                service._emit_progress(
                    phase=phase,
                    detail=detail,
                    document_id=document_id,
                    job_id=job_id,
                    metrics=metrics,
                )
                return active_stage_run_id
            active_stage_name = stage_name
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
            return active_stage_run_id

        def finish_stage(
            phase: str,
            detail: str,
            stage_outcome: str,
            *,
            metrics: dict[str, Any] | None = None,
            error_message: str | None = None,
            terminal_job_status: str | None = None,
        ) -> None:
            nonlocal active_stage_run_id, active_stage_name
            if active_stage_run_id is None:
                return
            service.repository.finish_stage_run(
                stage_run_id=active_stage_run_id,
                job_id=job_id,
                document_id=document_id,
                stage_outcome=stage_outcome,
                job_status=terminal_job_status,
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
            active_stage_name = None

        try:
            accepted_rows = service._list_all_chunk_records_for_kg(document_id)
            accepted_chunks = [service._chunk_from_record(row) for row in accepted_rows]
            all_assets = service._list_all_page_assets(document_id)
            synopsis_metrics = service._refresh_document_synopses(
                document_id,
                accepted_chunks=accepted_chunks,
                source_stage="chunks_validated",
            )
            service._emit_progress(
                phase="synopsis",
                detail=f"Prepared {int(synopsis_metrics['sections'])} section synopses.",
                document_id=document_id,
                job_id=job_id,
                metrics={**synopsis_metrics, "resumed": True},
            )

            if current_status in {"chunks_validated", "kg_validated"}:
                pending_rows = service._list_pending_kg_chunk_records(document_id)
                start_stage(
                    "kg_validated",
                    "kg",
                    f"Resuming KG extraction over accepted chunks ({len(accepted_chunks)} total).",
                    metrics={
                        "kg_total": len(accepted_chunks),
                        "kg_completed": max(0, len(accepted_chunks) - len(pending_rows)),
                        "kg_failures": 0,
                        "resumed": True,
                    },
                )
                processed_before = max(0, len(accepted_chunks) - len(pending_rows))
                for index, row in enumerate(pending_rows, start=1):
                    ensure_lease_active()
                    chunk = service._chunk_from_record(row)
                    service._emit_progress(
                        phase="kg",
                        detail=f"KG extraction {processed_before + index}/{len(accepted_chunks)}.",
                        document_id=document_id,
                        job_id=job_id,
                        metrics={
                            "kg_total": len(accepted_chunks),
                            "kg_completed": processed_before + index - 1,
                            "kg_failures": service.repository.count_kg_raw_extractions(document_id=document_id, status="review")
                            + service.repository.count_kg_raw_extractions(document_id=document_id, status="quarantined"),
                            "resumed": True,
                        },
                    )
                    service._run_kg_pipeline(
                        document_id,
                        chunk,
                        linked_assets=service._load_assets_for_chunk(chunk.chunk_id),
                        pre_persist_check=ensure_lease_active,
                    )

                review_count = service.repository.count_kg_raw_extractions(document_id=document_id, status="review")
                skipped_count = service.repository.count_kg_raw_extractions(document_id=document_id, status="skipped")
                quarantined_count = service.repository.count_kg_raw_extractions(document_id=document_id, status="quarantined")
                validated_count = service.repository.count_kg_raw_extractions(document_id=document_id, status="validated")
                kg_failure_count = review_count + quarantined_count
                kg_metrics = {
                    **service._build_graph_quality_metrics(document_id, len(accepted_chunks)),
                    "resumed": True,
                }
                finish_stage(
                    "kg",
                    "Completed KG extraction.",
                    "completed" if kg_failure_count == 0 else "review",
                    metrics=kg_metrics,
                    error_message=None if kg_failure_count == 0 else str({"review": review_count, "quarantined": quarantined_count}),
                )
            else:
                review_count = service.repository.count_kg_raw_extractions(document_id=document_id, status="review")
                skipped_count = service.repository.count_kg_raw_extractions(document_id=document_id, status="skipped")
                quarantined_count = service.repository.count_kg_raw_extractions(document_id=document_id, status="quarantined")
                validated_count = service.repository.count_kg_raw_extractions(document_id=document_id, status="validated")
                kg_failure_count = review_count + quarantined_count
                kg_metrics = {
                    **service._build_graph_quality_metrics(document_id, len(accepted_chunks)),
                    "resumed": True,
                }

            start_stage(
                "indexed",
                "embedding",
                "Resuming vector publish for accepted chunks and assets.",
                metrics={
                    "indexed_chunks": 0,
                    "indexed_assets": 0,
                    "publish_skipped_due_to_kg_review": bool(kg_failure_count),
                    "resumed": True,
                },
            )
            indexed_chunks = 0
            indexed_assets = 0
            corpus_snapshot_id = None
            if kg_failure_count == 0:
                corpus_snapshot_id = service.repository.create_pending_corpus_snapshot(
                    str(source_row["tenant_id"]),
                    "document_publish",
                    document_id=document_id,
                    job_id=job_id,
                    summary=f"Published resumed document {source.filename} into the retrieval-visible corpus.",
                    metrics={
                        "accepted_chunks": len(accepted_chunks),
                        "indexed_chunks": 0,
                        "indexed_assets": 0,
                        "pages": len(service.repository.list_document_pages(document_id=document_id, limit=5000)),
                        "page_assets": len(all_assets),
                        "resumed": True,
                    },
                    metadata={
                        "filename": source.filename,
                        "document_class": str(source_row.get("document_class") or ""),
                        "resume_from_status": current_status,
                    },
                )
                ensure_lease_active()
                chunk_embeddings = service.embedder.embed(
                    [chunk.text for chunk in accepted_chunks],
                    progress_callback=(
                        lambda update: service._emit_progress(
                            phase="embedding",
                            detail=f"Embedding accepted chunks {int(update.get('completed') or 0)}/{int(update.get('total') or 0)}.",
                            document_id=document_id,
                            job_id=job_id,
                            metrics={
                                "embedding_target": "chunks",
                                "completed": int(update.get("completed") or 0),
                                "total": int(update.get("total") or 0),
                                "batch_size": int(update.get("batch_size") or 0),
                                "resumed": True,
                            },
                        )
                    ) if accepted_chunks else None,
                ) if accepted_chunks else []
                ensure_lease_active()
                indexable_assets = [asset for asset in all_assets if service._is_indexable_asset(asset)]
                asset_embeddings = service.embedder.embed(
                    [asset.search_text for asset in indexable_assets],
                    progress_callback=(
                        lambda update: service._emit_progress(
                            phase="embedding",
                            detail=f"Embedding assets {int(update.get('completed') or 0)}/{int(update.get('total') or 0)}.",
                            document_id=document_id,
                            job_id=job_id,
                            metrics={
                                "embedding_target": "assets",
                                "completed": int(update.get("completed") or 0),
                                "total": int(update.get("total") or 0),
                                "batch_size": int(update.get("batch_size") or 0),
                                "resumed": True,
                            },
                        )
                    ) if indexable_assets else None,
                ) if indexable_assets else []
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
            else:
                indexable_assets = [asset for asset in all_assets if service._is_indexable_asset(asset)]
            finish_stage(
                "embedding" if kg_failure_count == 0 else "review",
                "Completed vector publish." if kg_failure_count == 0 else "Skipped vector publish because KG review is required.",
                "completed" if kg_failure_count == 0 else "review",
                metrics={
                    **service._build_completion_metrics(
                        document_id,
                        indexed_chunks=indexed_chunks,
                        indexed_assets=indexed_assets,
                        publish_skipped_due_to_kg_review=bool(kg_failure_count),
                    ),
                    "resumed": True,
                },
                terminal_job_status="completed" if kg_failure_count == 0 else "review",
            )
            completion_counts = service.repository.get_document_related_counts(document_id)
            return {
                "job_id": job_id,
                "document_id": document_id,
                "source_id": str(source_row["source_id"]),
                "blocks": 0,
                "chunks": int(completion_counts.get("chunks") or len(accepted_rows)),
                "accepted_chunks": int(completion_counts.get("accepted_chunks") or len(accepted_chunks)),
                "review_chunks": int(completion_counts.get("review_chunks") or 0),
                "rejected_chunks": int(completion_counts.get("rejected_chunks") or 0),
                "pages": len(service.repository.list_document_pages(document_id=document_id, limit=5000)),
                "page_assets": len(all_assets),
                "indexed_chunks": indexed_chunks,
                "indexed_assets": indexed_assets,
                **service._build_graph_quality_metrics(document_id, len(accepted_chunks)),
                "kg_failures": [] if kg_failure_count == 0 else [{"review": review_count, "quarantined": quarantined_count}],
                "corpus_snapshot_id": corpus_snapshot_id,
                "resumed": True,
                "resume_from_status": current_status,
            }
        finally:
            lease_stop.set()
            lease_thread.join(timeout=5)
            service.repository.release_job(job_id, service.worker_id)

