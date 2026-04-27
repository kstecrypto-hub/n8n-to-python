"""Primary ingestion orchestration service.

This module stitches the pipeline stages together:
- persist raw and normalized source text
- parse blocks and assemble chunks
- validate chunks and decide indexing eligibility
- write accepted chunks into Chroma
- run ontology-constrained KG extraction
- expose replay, review, and repair operations used by the admin console
"""

from __future__ import annotations

import asyncio
from datetime import UTC, datetime
from hashlib import sha256
import logging
from pathlib import Path
import re
import shutil
import threading
from typing import Any, Callable
from uuid import uuid4

from src.bee_ingestion.chunking import normalize_text, sanitize_text
from src.bee_ingestion.chroma_store import ChromaStore
from src.bee_ingestion.embedding import Embedder
from src.bee_ingestion.kg import (
    chunk_ontology_tags,
    load_ontology,
)
from src.bee_ingestion.models import Chunk, ChunkAssetLink, KGExtractionResult, PageAsset, SourceDocument
from src.bee_ingestion.multimodal import MultimodalPDFPayload
from src.bee_ingestion.offline_pipeline.context import OfflinePipelineContext
from src.bee_ingestion.offline_pipeline.contracts import OfflinePipelineCommand
from src.bee_ingestion.offline_pipeline.runner import OfflinePipelineRunner
from src.bee_ingestion.offline_pipeline.stages import (
    LegacyIngestStage,
    extract_multimodal_payload,
    prepare_source_document,
    run_build_kg_stage,
    run_chunk_documents_stage,
    run_kg_pipeline,
    run_publish_corpus_stage,
)
from src.bee_ingestion.offline_pipeline.stages import delete_document as delete_document_stage
from src.bee_ingestion.offline_pipeline.stages import rebuild_document as rebuild_document_stage
from src.bee_ingestion.offline_pipeline.stages import reindex_document as reindex_document_stage
from src.bee_ingestion.offline_pipeline.stages import repair_document as repair_document_stage
from src.bee_ingestion.offline_pipeline.stages import replay_quarantined_kg as replay_quarantined_kg_stage
from src.bee_ingestion.offline_pipeline.stages import reprocess_kg as reprocess_kg_stage
from src.bee_ingestion.offline_pipeline.stages import reset_ingestion_data as reset_ingestion_data_stage
from src.bee_ingestion.offline_pipeline.stages import reset_pipeline_data as reset_pipeline_data_stage
from src.bee_ingestion.offline_pipeline.stages import resume_ingest as resume_ingest_stage
from src.bee_ingestion.offline_pipeline.stages import revalidate_document as revalidate_document_stage
from src.bee_ingestion.repository import Repository
from src.bee_ingestion.reviewer import ChunkReviewError, review_chunk_with_meta
from src.bee_ingestion.settings import settings, workspace_root
from src.bee_ingestion.storage.bootstrap import ensure_app_storage_compatibility
from src.bee_ingestion.validation import validate_chunk

_ASSET_LINK_STOPWORDS = {
    "about", "after", "again", "bees", "been", "being", "book", "chapter", "could", "figure",
    "from", "have", "here", "into", "more", "page", "said", "scan", "than", "that", "their",
    "there", "these", "this", "those", "very", "with", "would",
}
_VISUAL_REFERENCE_MARKERS = ("figure", "fig.", "plate", "illustration", "diagram", "image", "table", "pictured")
logger = logging.getLogger(__name__)


class IngestionService:
    def __init__(
        self,
        repository: Repository | None = None,
        store: ChromaStore | None = None,
        progress_callback: Callable[[dict[str, Any]], None] | None = None,
    ) -> None:
        if repository is None:
            ensure_app_storage_compatibility()
        self.repository = repository or Repository()
        self.store = store or ChromaStore()
        self.embedder = Embedder()
        self.ontology = load_ontology()
        self._ontology_path = Path(settings.kg_ontology_path)
        self._ontology_mtime = self._ontology_path.stat().st_mtime if self._ontology_path.exists() else None
        self.worker_id = f"{settings.worker_version}:{uuid4()}"
        self.progress_callback = progress_callback

    @staticmethod
    def _page_assets_root() -> Path:
        return (workspace_root() / "data" / "page_assets").resolve()

    def _current_ontology(self):
        current_mtime = self._ontology_path.stat().st_mtime if self._ontology_path.exists() else None
        if current_mtime != self._ontology_mtime:
            self.ontology = load_ontology()
            self._ontology_mtime = current_mtime
        return self.ontology

    def _emit_progress(
        self,
        *,
        phase: str,
        detail: str = "",
        document_id: str | None = None,
        job_id: str | None = None,
        metrics: dict[str, Any] | None = None,
    ) -> None:
        if self.progress_callback is None:
            return
        payload = {
            "phase": phase,
            "detail": detail,
            "document_id": document_id,
            "job_id": job_id,
            "metrics": metrics or {},
            "timestamp": datetime.now(UTC).isoformat(),
        }
        try:
            self.progress_callback(payload)
        except Exception:
            return

    @staticmethod
    def _safe_ratio(numerator: int, denominator: int) -> float:
        if denominator <= 0:
            return 0.0
        return round(float(numerator) / float(denominator), 4)

    def _build_graph_quality_metrics(self, document_id: str, accepted_chunk_count: int) -> dict[str, Any]:
        counts = self.repository.get_document_related_counts(document_id)
        validated = int(counts.get("kg_validated") or 0)
        review = int(counts.get("kg_review") or 0)
        skipped = int(counts.get("kg_skipped") or 0)
        quarantined = int(counts.get("kg_quarantined") or 0)
        assertions = int(counts.get("kg_assertions") or 0)
        entities = int(counts.get("kg_entities") or 0)
        evidence = int(counts.get("kg_evidence") or 0)
        return {
            "kg_chunks": accepted_chunk_count,
            "kg_total": accepted_chunk_count,
            "kg_completed": validated + review + skipped + quarantined,
            "kg_failures": review + quarantined,
            "kg_validated": validated,
            "kg_review": review,
            "kg_skipped": skipped,
            "kg_quarantined": quarantined,
            "kg_assertions": assertions,
            "kg_entities": entities,
            "kg_evidence": evidence,
            "kg_assertions_per_validated_chunk": self._safe_ratio(assertions, validated),
            "kg_evidence_per_assertion": self._safe_ratio(evidence, assertions),
            "kg_review_rate": self._safe_ratio(review, accepted_chunk_count),
            "kg_quarantine_rate": self._safe_ratio(quarantined, accepted_chunk_count),
        }

    def _build_completion_metrics(
        self,
        document_id: str,
        *,
        indexed_chunks: int,
        indexed_assets: int,
        publish_skipped_due_to_kg_review: bool,
    ) -> dict[str, Any]:
        counts = self.repository.get_document_related_counts(document_id)
        return {
            "chunks": int(counts.get("chunks") or 0),
            "accepted_chunks": int(counts.get("accepted_chunks") or 0),
            "review_chunks": int(counts.get("review_chunks") or 0),
            "rejected_chunks": int(counts.get("rejected_chunks") or 0),
            "pages": int(counts.get("pages") or 0),
            "page_assets": int(counts.get("page_assets") or 0),
            "indexed_chunks": indexed_chunks,
            "indexed_assets": indexed_assets,
            "publish_skipped_due_to_kg_review": bool(publish_skipped_due_to_kg_review),
            "document_completed": not bool(publish_skipped_due_to_kg_review),
        }

    @staticmethod
    def _normalize_section_path(section_path: list[str] | None) -> list[str]:
        normalized: list[str] = []
        for item in list(section_path or []):
            value = sanitize_text(str(item or "")).strip()
            if value:
                normalized.append(value)
        return normalized

    @staticmethod
    def _section_id(document_id: str, section_path: list[str]) -> str:
        basis = "|".join(section_path) if section_path else "__root__"
        return f"{document_id}:section:{sha256(basis.encode('utf-8')).hexdigest()[:16]}"

    @staticmethod
    def _synopsis_fragments(text: str) -> list[str]:
        cleaned = re.sub(r"\s+", " ", sanitize_text(text or "")).strip()
        if not cleaned:
            return []
        fragments = re.split(r"(?<=[.!?])\s+|(?<=:)\s+|\n+", cleaned)
        seen: set[str] = set()
        results: list[str] = []
        for fragment in fragments:
            candidate = fragment.strip(" -;:,")
            if len(candidate) < 32:
                continue
            key = candidate.lower()
            if key in seen:
                continue
            seen.add(key)
            results.append(candidate)
        return results or [cleaned]

    @classmethod
    def _compose_synopsis_text(cls, texts: list[str], *, max_fragments: int, max_chars: int) -> str:
        chosen: list[str] = []
        seen: set[str] = set()
        for text in texts:
            for fragment in cls._synopsis_fragments(text):
                key = fragment.lower()
                if key in seen:
                    continue
                seen.add(key)
                candidate = " ".join(chosen + [fragment]).strip()
                if len(candidate) > max_chars and chosen:
                    return " ".join(chosen).strip()
                if len(candidate) > max_chars:
                    return fragment[:max_chars].rstrip()
                chosen.append(fragment)
                break
            if len(chosen) >= max_fragments:
                break
        if chosen:
            return " ".join(chosen).strip()
        collapsed = re.sub(r"\s+", " ", " ".join(texts)).strip()
        if len(collapsed) > max_chars:
            return collapsed[:max_chars].rstrip() + "..."
        return collapsed

    def _refresh_document_synopses(
        self,
        document_id: str,
        *,
        accepted_chunks: list[Chunk] | None = None,
        source_stage: str = "chunks_validated",
    ) -> dict[str, Any]:
        if accepted_chunks is None:
            accepted_rows = self.repository.list_document_chunk_records(document_id=document_id, status="accepted")
            accepted_chunks = [self._chunk_from_record(row) for row in accepted_rows]
        accepted_chunks = sorted(list(accepted_chunks), key=lambda chunk: chunk.chunk_index)
        document_record = self.repository.get_document_record(document_id) or {}
        tenant_id = str(document_record.get("tenant_id") or (accepted_chunks[0].tenant_id if accepted_chunks else "shared"))
        title = sanitize_text(str(document_record.get("filename") or "")).strip()
        if not title and accepted_chunks:
            title = sanitize_text(
                str(
                    accepted_chunks[0].metadata.get("title")
                    or accepted_chunks[0].metadata.get("section_title")
                    or accepted_chunks[0].metadata.get("section_heading")
                    or ""
                )
            ).strip()

        section_groups: dict[tuple[str, ...], list[Chunk]] = {}
        for chunk in accepted_chunks:
            path = tuple(self._normalize_section_path(chunk.section_path))
            section_groups.setdefault(path, []).append(chunk)

        section_synopses: list[dict[str, Any]] = []
        for path, chunks in sorted(
            section_groups.items(),
            key=lambda item: (
                item[1][0].page_start or 0,
                item[1][0].chunk_index,
                len(item[0]),
                item[0],
            ),
        ):
            normalized_path = list(path)
            first_chunk = chunks[0]
            last_chunk = chunks[-1]
            page_starts = [chunk.page_start for chunk in chunks if chunk.page_start is not None]
            page_ends = [chunk.page_end for chunk in chunks if chunk.page_end is not None]
            char_starts = [chunk.char_start for chunk in chunks if chunk.char_start is not None]
            char_ends = [chunk.char_end for chunk in chunks if chunk.char_end is not None]
            section_title = sanitize_text(
                str(
                    first_chunk.metadata.get("section_title")
                    or first_chunk.metadata.get("section_heading")
                    or (normalized_path[-1] if normalized_path else title or "Document overview")
                )
            ).strip()
            synopsis_text = self._compose_synopsis_text(
                [chunk.text for chunk in chunks],
                max_fragments=3,
                max_chars=720,
            )
            section_synopses.append(
                {
                    "section_id": self._section_id(document_id, normalized_path),
                    "tenant_id": tenant_id,
                    "parent_section_id": self._section_id(document_id, normalized_path[:-1]) if normalized_path else None,
                    "section_path": normalized_path,
                    "section_level": len(normalized_path),
                    "section_title": section_title,
                    "page_start": min(page_starts) if page_starts else None,
                    "page_end": max(page_ends) if page_ends else None,
                    "char_start": min(char_starts) if char_starts else None,
                    "char_end": max(char_ends) if char_ends else None,
                    "first_chunk_id": first_chunk.chunk_id,
                    "last_chunk_id": last_chunk.chunk_id,
                    "accepted_chunk_count": len(chunks),
                    "total_chunk_count": len(chunks),
                    "synopsis_text": synopsis_text,
                    "synopsis_version": settings.synopsis_version,
                    "metadata_json": {
                        "source_stage": source_stage,
                        "first_chunk_index": first_chunk.chunk_index,
                        "last_chunk_index": last_chunk.chunk_index,
                    },
                }
            )

        document_synopsis = self._compose_synopsis_text(
            [section["synopsis_text"] for section in section_synopses] or [chunk.text for chunk in accepted_chunks],
            max_fragments=4,
            max_chars=960,
        )
        self.repository.replace_section_synopses(document_id, section_synopses)
        self.repository.replace_document_synopsis(
            document_id=document_id,
            tenant_id=tenant_id,
            title=title,
            synopsis_text=document_synopsis,
            accepted_chunk_count=len(accepted_chunks),
            section_count=len(section_synopses),
            source_stage=source_stage,
            synopsis_version=settings.synopsis_version,
            metadata_json={
                "source_stage": source_stage,
                "document_status": str(document_record.get("status") or ""),
            },
        )
        return {
            "accepted_chunks": len(accepted_chunks),
            "sections": len(section_synopses),
            "source_stage": source_stage,
        }

    def _delete_page_asset_files(self, document_id: str | None = None) -> None:
        root = self._page_assets_root().resolve()
        if document_id:
            target = (root / document_id).resolve()
            try:
                target.relative_to(root)
            except ValueError as exc:
                raise ValueError("Invalid document id for page-asset cleanup") from exc
            if target.exists():
                shutil.rmtree(target)
            return
        if not root.exists():
            return
        for child in list(root.iterdir()):
            if child.is_dir():
                shutil.rmtree(child)
            else:
                child.unlink()

    def _resolve_replayable_pdf_path(self, source: SourceDocument) -> Path | None:
        source_path = str(source.metadata.get("source_path") or source.metadata.get("uploaded_path") or "").strip()
        if not source_path:
            return None
        path = Path(source_path).resolve()
        allowed_roots = {
            workspace_root().resolve(),
            Path("/app").resolve(),
        }
        if not any(root == path or root in path.parents for root in allowed_roots):
            raise ValueError("PDF source path must stay inside the workspace")
        if not path.exists() or path.suffix.lower() != ".pdf":
            raise ValueError("Stored PDF source is not replayable")
        return path

    @staticmethod
    def _page_asset_from_row(row: dict) -> PageAsset:
        return PageAsset(
            asset_id=str(row["asset_id"]),
            document_id=str(row["document_id"]),
            tenant_id=str(row["tenant_id"]),
            page_number=int(row["page_number"]),
            asset_index=int(row["asset_index"]),
            asset_type=str(row["asset_type"]),
            asset_path=str(row["asset_path"]),
            bbox=list(row.get("bbox_json") or []),
            ocr_text=str(row.get("ocr_text") or ""),
            description_text=str(row.get("description_text") or ""),
            metadata=dict(row.get("metadata_json") or {}),
        )

    @staticmethod
    def _reset_chunk_asset_metadata(chunk: Chunk) -> None:
        chunk.metadata.pop("linked_asset_count", None)
        chunk.metadata.pop("linked_asset_ids", None)
        chunk.metadata.pop("linked_asset_types", None)
        chunk.metadata.pop("linked_asset_max_confidence", None)
        chunk.metadata.pop("linked_asset_link_types", None)

    def _relink_chunks_with_assets(self, document_id: str, chunks: list[Chunk], assets: list[PageAsset], persist: bool = True) -> list[ChunkAssetLink]:
        for chunk in chunks:
            self._reset_chunk_asset_metadata(chunk)
        links = self._link_chunks_to_assets(chunks, assets)
        if persist:
            self.repository.delete_document_chunk_asset_links(document_id)
            self.repository.save_chunk_asset_links(links)
        return links

    def _load_assets_for_chunk(self, chunk_id: str, limit: int = 6) -> list[PageAsset]:
        rows = self.repository.list_page_assets(chunk_id=chunk_id, limit=limit)
        return [self._page_asset_from_row(row) for row in rows]

    def _list_all_page_assets(self, document_id: str, batch_size: int = 1000) -> list[PageAsset]:
        assets: list[PageAsset] = []
        offset = 0
        while True:
            rows = self.repository.list_page_assets(document_id=document_id, limit=batch_size, offset=offset)
            if not rows:
                break
            assets.extend(self._page_asset_from_row(row) for row in rows)
            if len(rows) < batch_size:
                break
            offset += len(rows)
        return assets

    def _list_all_chunk_records_for_kg(self, document_id: str, batch_size: int = 500) -> list[dict]:
        rows: list[dict] = []
        offset = 0
        while True:
            batch = self.repository.list_chunk_records_for_kg(document_id=document_id, limit=batch_size, offset=offset)
            if not batch:
                break
            rows.extend(batch)
            if len(batch) < batch_size:
                break
            offset += len(batch)
        return rows

    def _list_pending_kg_chunk_records(self, document_id: str, batch_size: int = 500) -> list[dict]:
        rows: list[dict] = []
        offset = 0
        while True:
            batch = self.repository.list_pending_kg_chunk_records(document_id=document_id, limit=batch_size, offset=offset)
            if not batch:
                break
            rows.extend(batch)
            if len(batch) < batch_size:
                break
            offset += len(batch)
        return rows

    def _list_all_kg_raw_extractions(
        self,
        *,
        document_id: str | None = None,
        chunk_id: str | None = None,
        status: str | None = None,
        batch_size: int = 500,
    ) -> list[dict]:
        rows: list[dict] = []
        offset = 0
        while True:
            batch = self.repository.list_kg_raw_extractions(
                document_id=document_id,
                chunk_id=chunk_id,
                status=status,
                limit=batch_size,
                offset=offset,
            )
            if not batch:
                break
            rows.extend(batch)
            if len(batch) < batch_size:
                break
            offset += len(batch)
        return rows

    @staticmethod
    def _group_assets_by_chunk(links: list[ChunkAssetLink], assets: list[PageAsset]) -> dict[str, list[PageAsset]]:
        assets_by_id = {asset.asset_id: asset for asset in assets}
        grouped: dict[str, list[PageAsset]] = {}
        for link in links:
            asset = assets_by_id.get(link.asset_id)
            if asset is None:
                continue
            grouped.setdefault(link.chunk_id, []).append(asset)
        return grouped

    @staticmethod
    def _is_indexable_asset(asset: PageAsset) -> bool:
        search_text = asset.search_text.strip()
        if not search_text:
            return False
        trusted_anchor = _trusted_asset_anchor_text(asset)
        has_meaningful_text = bool(asset.ocr_text.strip())
        has_terms = bool(_trusted_asset_reference_keys(asset))
        if asset.asset_type == "page_image":
            return len(trusted_anchor) >= settings.asset_embedding_min_chars or has_meaningful_text or has_terms
        return len(trusted_anchor) >= settings.asset_embedding_min_chars or has_meaningful_text or has_terms

    @staticmethod
    def _build_kg_input_chunk(chunk: Chunk, linked_assets: list[PageAsset]) -> Chunk:
        if not linked_assets:
            return chunk
        asset_lines: list[str] = []
        linked_asset_ids: list[str] = []
        for asset in linked_assets[:3]:
            asset_text = _trusted_asset_anchor_text(asset)
            if not asset_text:
                continue
            linked_asset_ids.append(asset.asset_id)
            asset_lines.append(f"[page {asset.page_number} {asset.asset_type}] {asset_text}")
        if not asset_lines:
            return chunk
        augmented_text = f"{chunk.text}\n\nLinked visual evidence:\n" + "\n".join(f"- {line}" for line in asset_lines)
        metadata = dict(chunk.metadata)
        metadata["kg_linked_asset_ids"] = linked_asset_ids
        metadata["kg_visual_evidence_count"] = len(asset_lines)
        return Chunk(
            chunk_id=chunk.chunk_id,
            document_id=chunk.document_id,
            tenant_id=chunk.tenant_id,
            chunk_index=chunk.chunk_index,
            page_start=chunk.page_start,
            page_end=chunk.page_end,
            section_path=list(chunk.section_path),
            prev_chunk_id=chunk.prev_chunk_id,
            next_chunk_id=chunk.next_chunk_id,
            char_start=chunk.char_start,
            char_end=chunk.char_end,
            text=augmented_text,
            parser_version=chunk.parser_version,
            chunker_version=chunk.chunker_version,
            content_type=chunk.content_type,
            metadata=metadata,
        )

    def ingest_text(self, source: SourceDocument) -> dict:
        source = self._prepare_source(source)
        if source.source_type == "pdf":
            self._resolve_replayable_pdf_path(source)
        with self.repository.advisory_lock("ingest-source", source.tenant_id, source.source_type, source.content_hash, source.filename):
            existing_document_id = self.repository.find_existing_document(source)
            if existing_document_id:
                replacement_document_id, source_id = self.repository.register_document(source)
                result = self._process_registered_document(
                    document_id=replacement_document_id,
                    source_id=source_id,
                    source=source,
                )
                with self.repository.advisory_lock("document-mutate", existing_document_id):
                    self.store.delete_document(existing_document_id)
                    self._delete_page_asset_files(existing_document_id)
                    self.repository.delete_document(existing_document_id)
                result["replacement_document_id"] = replacement_document_id
                result["superseded_document_id"] = existing_document_id
                return result
            document_id, source_id = self.repository.register_document(source)
            return self._process_registered_document(
                document_id=document_id,
                source_id=source_id,
                source=source,
                delete_document_on_failure=False,
            )

    def enqueue_text(self, source: SourceDocument) -> dict:
        source = self._prepare_source(source)
        if source.source_type == "pdf":
            self._resolve_replayable_pdf_path(source)
        with self.repository.advisory_lock("ingest-source", source.tenant_id, source.source_type, source.content_hash, source.filename):
            existing_document_id = self.repository.find_existing_document(source)
            document_id, source_id = self.repository.register_document(source)
            job_id = self._create_ingestion_job_for_source(document_id, source)
            payload = {
                "job_id": job_id,
                "document_id": document_id,
                "source_id": source_id,
                "status": "registered",
                "queued": True,
                "source_type": source.source_type,
                "filename": source.filename,
            }
            if existing_document_id:
                payload["replacement_document_id"] = document_id
                payload["superseded_document_id"] = existing_document_id
            return payload

    def enqueue_maintenance_job(
        self,
        *,
        operation: str,
        document_id: str | None = None,
        parameters: dict[str, Any] | None = None,
    ) -> dict:
        operation_name = str(operation or "").strip()
        if not operation_name:
            raise ValueError("Maintenance operation is required")
        job_id = None
        if document_id:
            job_id = self.repository.create_job(
                document_id=document_id,
                extractor_version=settings.extractor_version,
                normalizer_version=settings.normalizer_version,
                parser_version=settings.extractor_version,
                chunker_version=settings.chunker_version,
                validator_version=settings.validator_version,
                embedding_version=settings.embedding_model,
                kg_version=f"{settings.kg_extraction_provider}:{settings.kg_model}:{settings.kg_prompt_version}",
            )
            self.repository.record_stage(
                job_id=job_id,
                document_id=document_id,
                stage_name=f"maintenance:{operation_name}",
                job_status="registered",
                stage_outcome="registered",
                metrics={"operation": operation_name, "parameters": dict(parameters or {})},
                worker_version=settings.worker_version,
                input_version="maintenance",
            )
        return {
            "status": "registered",
            "operation": operation_name,
            "job_id": job_id,
            "document_id": document_id,
            "parameters": dict(parameters or {}),
            "execution": "offline_worker",
        }

    def run_registered_job(self, job_id: str) -> dict:
        job = self.repository.get_job(job_id)
        if job is None:
            raise ValueError("Ingestion job not found")
        document_id = str(job.get("document_id") or "").strip()
        if not document_id:
            raise ValueError("Ingestion job is missing a document_id")
        source_row = self.repository.get_latest_document_source(document_id)
        if source_row is None:
            raise ValueError("Document source not found")
        command = OfflinePipelineCommand(
            job_id=job_id,
            document_id=document_id,
            source_id=str(source_row.get("source_id") or ""),
            tenant_id=str(source_row.get("tenant_id") or "shared"),
            source_type=str(source_row.get("source_type") or ""),
            delete_document_on_failure=bool(str(job.get("status") or "").strip().lower() != "registered"),
            metadata={
                "filename": str(source_row.get("filename") or ""),
                "document_class": str(source_row.get("document_class") or ""),
            },
        )
        context = OfflinePipelineContext(
            repository=self.repository,
            service=self,
            worker_id=self.worker_id,
            input_version=self._build_input_version(str(source_row.get("parser_version") or "v1")),
            worker_version=settings.worker_version,
        )
        runner = OfflinePipelineRunner([LegacyIngestStage()])
        state = asyncio.run(runner.run(context=context, command=command))
        if state.result_payload is not None:
            return state.result_payload
        if state.errors:
            raise RuntimeError(state.errors[-1])
        return {
            "job_id": job_id,
            "document_id": document_id,
            "status": "processing" if not state.finished_stage_names else "completed",
            "retrieval_visible": state.retrieval_visible,
        }

    def _create_ingestion_job_for_source(self, document_id: str, source: SourceDocument) -> str:
        return self.repository.create_job(
            document_id=document_id,
            extractor_version=settings.extractor_version,
            normalizer_version=settings.normalizer_version,
            parser_version=source.parser_version,
            chunker_version=settings.chunker_version,
            validator_version=settings.validator_version,
            embedding_version=settings.embedding_model,
            kg_version=f"{settings.kg_extraction_provider}:{settings.kg_model}:{settings.kg_prompt_version}",
        )

    def _source_from_source_row(self, source_row: dict[str, Any]) -> SourceDocument:
        source = self._prepare_source(
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
            self._resolve_replayable_pdf_path(source)
        return source

    def _execute_legacy_ingest_command(self, command: OfflinePipelineCommand) -> dict:
        source_row = self.repository.get_latest_document_source(command.document_id)
        if source_row is None:
            raise ValueError("Document source not found")
        source = self._source_from_source_row(source_row)
        return self._process_registered_document(
            document_id=command.document_id,
            source_id=command.source_id,
            source=source,
            delete_document_on_failure=command.delete_document_on_failure,
            job_id=command.job_id,
        )

    def rebuild_document(self, document_id: str) -> dict:
        return rebuild_document_stage.rebuild_document(self, document_id)

    def repair_document(self, document_id: str, rerun_kg: bool = True) -> dict:
        return repair_document_stage.repair_document(self, document_id, rerun_kg=rerun_kg)

    def resume_document_ingest(self, document_id: str) -> dict:
        return resume_ingest_stage.resume_document_ingest(self, document_id)

    def revalidate_document(self, document_id: str, rerun_kg: bool = True) -> dict:
        return revalidate_document_stage.revalidate_document(self, document_id, rerun_kg=rerun_kg)

    def reindex_document(self, document_id: str) -> dict:
        return reindex_document_stage.reindex_document(self, document_id)

    def sync_chunk_index(self, chunk_id: str) -> dict:
        row = self.repository.get_chunk_record(chunk_id)
        if row is None:
            raise ValueError("Chunk not found")
        chunk = self._chunk_from_record(row)
        self._apply_chunk_enrichment(
            chunk,
            row["validation_status"],
            float(row.get("quality_score") or 0.0),
            list(row.get("reasons") or []),
        )
        self.repository.update_chunk_metadata(chunk_id, chunk.metadata)
        if row["validation_status"] == "accepted":
            self.store.upsert_chunks([chunk], self.embedder.embed([chunk.text]))
            return {"chunk_id": chunk_id, "indexed": True, "status": row["validation_status"]}
        self.store.delete_chunk(chunk_id)
        return {"chunk_id": chunk_id, "indexed": False, "status": row["validation_status"]}

    def sync_asset_index(self, asset_id: str) -> dict:
        detail = self.repository.get_page_asset_detail(asset_id)
        if detail is None:
            raise ValueError("Asset not found")
        asset_row = detail["asset"]
        asset = PageAsset(
            asset_id=str(asset_row["asset_id"]),
            document_id=str(asset_row["document_id"]),
            tenant_id=str(asset_row["tenant_id"]),
            page_number=int(asset_row["page_number"]),
            asset_index=int(asset_row["asset_index"]),
            asset_type=str(asset_row["asset_type"]),
            asset_path=str(asset_row["asset_path"]),
            bbox=list(asset_row.get("bbox_json") or []),
            ocr_text=str(asset_row.get("ocr_text") or ""),
            description_text=str(asset_row.get("description_text") or ""),
            metadata=dict(asset_row.get("metadata_json") or {}),
        )
        is_indexable = self._is_indexable_asset(asset)
        self.repository.save_page_assets([asset])
        if is_indexable:
            self.store.upsert_assets([asset], self.embedder.embed([asset.search_text]))
            return {"asset_id": asset_id, "indexed": True}
        self.store.delete_asset(asset_id)
        return {"asset_id": asset_id, "indexed": False}

    def delete_document(self, document_id: str) -> dict:
        return delete_document_stage.delete_document(self, document_id)

    def reset_pipeline_data(self) -> dict:
        return reset_pipeline_data_stage.reset_pipeline_data(self)

    def reset_ingestion_data(self, document_id: str | None = None) -> dict:
        return reset_ingestion_data_stage.reset_ingestion_data(self, document_id=document_id)

    def review_chunk_decision(self, chunk_id: str, action: str) -> dict:
        with self.repository.advisory_lock("chunk-review", chunk_id):
            record = self.repository.get_chunk_record(chunk_id)
            if record is None:
                raise ValueError("Chunk not found")
            current_status = record["validation_status"]
            if current_status != "review":
                if action == "accept" and current_status == "accepted":
                    return {"chunk_id": chunk_id, "status": "accepted", "noop": True}
                if action == "reject" and current_status == "rejected":
                    return {"chunk_id": chunk_id, "status": "rejected", "noop": True}
                raise ValueError(f"Chunk is currently '{current_status}', not review")

            reasons = list(record.get("reasons") or [])
            quality_score = float(record.get("quality_score") or 0.5)
            chunk = self._chunk_from_record(record)
            original_metadata = dict(record.get("metadata_json") or {})

            if action == "auto":
                return self._auto_review_chunk(
                    chunk,
                    quality_score,
                    reasons,
                    original_status=current_status,
                    original_metadata=original_metadata,
                )

            if action == "accept":
                final_reasons = self._append_reason(reasons, "manual_accept")
                final_score = max(quality_score, 0.75)
                return self._accept_review_chunk(
                    chunk,
                    final_score=final_score,
                    final_reasons=final_reasons,
                    original_status=current_status,
                    original_score=quality_score,
                    original_reasons=reasons,
                    original_metadata=original_metadata,
                )

            if action == "reject":
                final_reasons = self._append_reason(reasons, "manual_reject")
                return self._remove_review_chunk_from_live_state(
                    chunk,
                    final_status="rejected",
                    final_score=quality_score,
                    final_reasons=final_reasons,
                    original_status=current_status,
                    original_score=quality_score,
                    original_reasons=reasons,
                    original_metadata=original_metadata,
                )

            raise ValueError("Unsupported action")

    def auto_review_chunks(self, document_id: str | None = None, batch_size: int = 200) -> dict:
        processed = 0
        accepted = 0
        rejected = 0
        left_in_review = 0
        errors: list[dict] = []

        while True:
            # Always reread the review queue from storage because decisions inside the
            # loop change the queue membership in-place.
            rows = self.repository.list_review_chunk_records(document_id=document_id, limit=batch_size, offset=0)
            if not rows:
                break
            progressed = 0
            for row in rows:
                chunk = self._chunk_from_record(row)
                quality_score = float(row.get("quality_score") or 0.5)
                reasons = list(row.get("reasons") or [])
                try:
                    result = self._auto_review_chunk(chunk, quality_score, reasons)
                except Exception as exc:
                    errors.append({"chunk_id": chunk.chunk_id, "error": str(exc)})
                    left_in_review += 1
                    processed += 1
                    continue
                processed += 1
                if result["status"] == "accepted":
                    accepted += 1
                    progressed += 1
                elif result["status"] == "rejected":
                    rejected += 1
                    progressed += 1
                else:
                    left_in_review += 1
            if progressed == 0:
                break

        return {
            "document_id": document_id,
            "processed_chunks": processed,
            "accepted": accepted,
            "rejected": rejected,
            "review": left_in_review,
            "errors": errors,
        }

    def reprocess_kg(self, document_id: str | None = None, batch_size: int = 200, prune_orphans: bool = True) -> dict:
        return reprocess_kg_stage.reprocess_kg(self, document_id=document_id, batch_size=batch_size, prune_orphans=prune_orphans)

    def replay_quarantined_kg(
        self,
        *,
        document_id: str | None = None,
        chunk_ids: list[str] | None = None,
        publish_if_clean: bool = True,
    ) -> dict:
        return replay_quarantined_kg_stage.replay_quarantined_kg(self, document_id=document_id, chunk_ids=chunk_ids, publish_if_clean=publish_if_clean)

    def _replay_quarantined_document_kg(
        self,
        document_id: str,
        *,
        target_chunk_ids: list[str] | None = None,
        publish_if_clean: bool = True,
    ) -> dict[str, Any]:
        return replay_quarantined_kg_stage._replay_quarantined_document_kg(self, document_id, target_chunk_ids=target_chunk_ids, publish_if_clean=publish_if_clean)

    @staticmethod
    def _prepare_source(source: SourceDocument) -> SourceDocument:
        return prepare_source_document(source)

    def _process_registered_document(
        self,
        document_id: str,
        source_id: str,
        source: SourceDocument,
        delete_document_on_failure: bool = True,
        job_id: str | None = None,
    ) -> dict:
        job_id = job_id or self._create_ingestion_job_for_source(document_id, source)
        if not self.repository.claim_job(job_id, self.worker_id, settings.job_lease_seconds):
            raise ValueError("Unable to claim ingestion job")

        input_version = self._build_input_version(source.parser_version)
        extraction_metrics = dict(source.extraction_metrics or {})
        normalized_text = source.normalized_text or source.raw_text
        multimodal_payload: MultimodalPDFPayload | None = None
        linked_assets_by_chunk: dict[str, list[PageAsset]] = {}
        lease_stop = threading.Event()
        lease_lost = threading.Event()
        lease_interval_seconds = max(5, min(settings.job_lease_seconds // 3, 60))

        def renew_lease_loop() -> None:
            while not lease_stop.wait(lease_interval_seconds):
                try:
                    renewed = self.repository.renew_job_lease(job_id, self.worker_id, settings.job_lease_seconds)
                except Exception:
                    logger.warning("Failed to renew ingest job lease for %s", job_id, exc_info=True)
                    continue
                if not renewed:
                    logger.warning("Lost ingest job lease for %s", job_id)
                    lease_lost.set()
                    break

        def ensure_lease_active() -> None:
            if lease_lost.is_set():
                raise RuntimeError("Ingestion job lease was lost during processing")

        lease_thread = threading.Thread(target=renew_lease_loop, name=f"ingest-lease-{job_id}", daemon=True)
        lease_thread.start()
        active_stage_run_id: str | None = None
        active_stage_name: str | None = None
        active_stage_started_at: datetime | None = None

        def start_stage(stage_name: str, phase: str, detail: str, metrics: dict[str, Any] | None = None) -> None:
            nonlocal active_stage_run_id, active_stage_name, active_stage_started_at
            active_stage_started_at = datetime.now(UTC)
            active_stage_name = stage_name
            active_stage_run_id = self.repository.start_stage_run(
                job_id=job_id,
                document_id=document_id,
                stage_name=stage_name,
                job_status=stage_name,
                metrics=metrics,
                worker_version=settings.worker_version,
                input_version=input_version,
                started_at=active_stage_started_at,
            )
            self._emit_progress(
                phase=phase,
                detail=detail,
                document_id=document_id,
                job_id=job_id,
                metrics=metrics,
            )

        def finish_stage(
            phase: str,
            detail: str,
            stage_outcome: str,
            *,
            metrics: dict[str, Any] | None = None,
            error_message: str | None = None,
            terminal_job_status: str | None = None,
        ) -> None:
            nonlocal active_stage_run_id, active_stage_name, active_stage_started_at
            if active_stage_run_id is None:
                return
            self.repository.finish_stage_run(
                stage_run_id=active_stage_run_id,
                job_id=job_id,
                document_id=document_id,
                stage_outcome=stage_outcome,
                job_status=terminal_job_status,
                metrics=metrics,
                error_message=error_message,
                finished_at=datetime.now(UTC),
            )
            self._emit_progress(
                phase=phase,
                detail=detail,
                document_id=document_id,
                job_id=job_id,
                metrics=metrics,
            )
            active_stage_run_id = None
            active_stage_name = None
            active_stage_started_at = None

        try:
            start_stage(
                "content_available",
                "preparing",
                "Preparing source text, rendered pages, and page assets.",
                metrics=extraction_metrics,
            )
            multimodal_payload = self._extract_multimodal_payload(document_id, source)
            ensure_lease_active()
            if multimodal_payload is not None:
                self.repository.save_document_pages(multimodal_payload.pages)
                self.repository.save_page_assets(multimodal_payload.assets)
                extraction_metrics.update(multimodal_payload.metrics)
                normalized_text = normalize_text(multimodal_payload.merged_text or normalized_text)
                source.normalized_text = normalized_text
            source.extraction_metrics = extraction_metrics
            self.repository.update_document_source(
                source_id,
                {
                    "normalized_text": normalized_text,
                    "extraction_metrics_json": extraction_metrics,
                    "metadata_json": source.metadata,
                },
            )
            finish_stage(
                "preparing",
                "Prepared source text and multimodal assets.",
                "completed",
                metrics=extraction_metrics,
            )

            chunk_stage = run_chunk_documents_stage(
                self,
                document_id=document_id,
                source=source,
                normalized_text=normalized_text,
                multimodal_payload=multimodal_payload,
                job_id=job_id,
                ensure_lease_active=ensure_lease_active,
                start_stage=start_stage,
                finish_stage=finish_stage,
            )
            blocks = chunk_stage.blocks
            chunks = chunk_stage.chunks
            accepted_chunks = chunk_stage.accepted_chunks
            validation_metrics = chunk_stage.validation_metrics
            linked_assets_by_chunk = chunk_stage.linked_assets_by_chunk

            kg_results, kg_failures, kg_metrics = run_build_kg_stage(
                self,
                document_id=document_id,
                accepted_chunks=accepted_chunks,
                linked_assets_by_chunk=linked_assets_by_chunk,
                job_id=job_id,
                ensure_lease_active=ensure_lease_active,
                start_stage=start_stage,
                finish_stage=finish_stage,
            )

            publish_result = run_publish_corpus_stage(
                self,
                document_id=document_id,
                source=source,
                blocks_count=len(blocks),
                chunks_count=len(chunks),
                accepted_chunks=accepted_chunks,
                multimodal_payload=multimodal_payload,
                kg_failures=kg_failures,
                job_id=job_id,
                ensure_lease_active=ensure_lease_active,
                start_stage=start_stage,
                finish_stage=finish_stage,
            )
            final_status = publish_result.final_status
            indexed_chunks = publish_result.indexed_chunks
            indexed_assets = publish_result.indexed_assets
            corpus_snapshot_id = publish_result.corpus_snapshot_id

            return {
                "job_id": job_id,
                "document_id": document_id,
                "source_id": source_id,
                "blocks": len(blocks),
                "chunks": len(chunks),
                "accepted_chunks": len(accepted_chunks),
                "review_chunks": validation_metrics["review"],
                "rejected_chunks": validation_metrics["rejected"],
                "pages": len(multimodal_payload.pages) if multimodal_payload else 0,
                "page_assets": len(multimodal_payload.assets) if multimodal_payload else 0,
                "indexed_chunks": indexed_chunks,
                "indexed_assets": indexed_assets,
                **kg_metrics,
                "corpus_snapshot_id": corpus_snapshot_id,
                "kg_failures": kg_failures,
            }
        except Exception as exc:
            self.store.delete_document(document_id)
            self.repository.delete_document_kg(document_id)
            if active_stage_run_id is not None:
                finish_stage(
                    "failed",
                    f"{active_stage_name or 'stage'} failed: {exc}",
                    "failed",
                    error_message=str(exc),
                    terminal_job_status="failed",
                )
            else:
                self.repository.record_stage(
                    job_id,
                    document_id,
                    "failed",
                    "failed",
                    "failed",
                    None,
                    str(exc),
                    worker_version=settings.worker_version,
                    input_version=input_version,
                )
            self._emit_progress(
                phase="failed",
                detail=f"Ingest failed: {exc}",
                document_id=document_id,
                job_id=job_id,
                metrics={"error": str(exc)},
            )
            self._delete_page_asset_files(document_id)
            if delete_document_on_failure:
                self.repository.delete_document(document_id)
            else:
                self.repository.reset_document_pipeline_state(document_id, status="failed")
            raise
        finally:
            lease_stop.set()
            lease_thread.join(timeout=5)
            self.repository.release_job(job_id, self.worker_id)

    def _extract_multimodal_payload(self, document_id: str, source: SourceDocument) -> MultimodalPDFPayload | None:
        return extract_multimodal_payload(self, document_id=document_id, source=source)

    def _link_chunks_to_assets(self, chunks: list[Chunk], assets: list[PageAsset]) -> list[ChunkAssetLink]:
        if not chunks or not assets:
            return []
        assets_by_page: dict[int, list[PageAsset]] = {}
        for asset in assets:
            assets_by_page.setdefault(asset.page_number, []).append(asset)
        chunks_by_page: dict[int, list[Chunk]] = {}
        for chunk in chunks:
            if chunk.page_start is None or chunk.page_end is None:
                continue
            for page_number in range(chunk.page_start, chunk.page_end + 1):
                chunks_by_page.setdefault(page_number, []).append(chunk)
        candidate_links: list[ChunkAssetLink] = []
        seen_pairs: set[tuple[str, str]] = set()
        for chunk in chunks:
            if chunk.page_start is None or chunk.page_end is None:
                continue
            for page_number in range(chunk.page_start, chunk.page_end + 1):
                for asset in assets_by_page.get(page_number, []):
                    link = _score_chunk_asset_link(
                        chunk=chunk,
                        asset=asset,
                        page_chunk_count=len(chunks_by_page.get(page_number, [])),
                    )
                    if link is None:
                        continue
                    pair = (chunk.chunk_id, asset.asset_id)
                    if pair in seen_pairs:
                        continue
                    seen_pairs.add(pair)
                    candidate_links.append(
                        ChunkAssetLink(
                            chunk_id=chunk.chunk_id,
                            asset_id=asset.asset_id,
                            link_type=link["link_type"],
                            confidence=link["confidence"],
                            metadata={
                                "page_number": page_number,
                                "asset_type": asset.asset_type,
                                "page_chunk_count": len(chunks_by_page.get(page_number, [])),
                                "shared_terms": link["shared_terms"],
                                "reason": link["reason"],
                            },
                        )
                    )
        links_by_asset: dict[str, list[ChunkAssetLink]] = {}
        for link in candidate_links:
            links_by_asset.setdefault(link.asset_id, []).append(link)

        links: list[ChunkAssetLink] = []
        for asset in assets:
            asset_links = links_by_asset.get(asset.asset_id, [])
            if not asset_links:
                continue
            asset_links.sort(key=lambda item: (item.confidence, item.link_type == "text_overlap"), reverse=True)
            best_confidence = asset_links[0].confidence
            max_links = 1 if asset.asset_type == "page_image" else 2
            kept_for_asset = 0
            for link in asset_links:
                if kept_for_asset >= max_links:
                    break
                if link.confidence < 0.68:
                    continue
                if link.confidence + 0.08 < best_confidence:
                    continue
                links.append(link)
                kept_for_asset += 1

        links_by_chunk: dict[str, list[ChunkAssetLink]] = {}
        for link in links:
            links_by_chunk.setdefault(link.chunk_id, []).append(link)
        for chunk in chunks:
            chunk_links = links_by_chunk.get(chunk.chunk_id, [])
            if not chunk_links:
                continue
            unique_asset_ids = list(dict.fromkeys(link.asset_id for link in chunk_links))
            unique_asset_types = list(
                dict.fromkeys(
                    str(link.metadata.get("asset_type") or "")
                    for link in chunk_links
                    if str(link.metadata.get("asset_type") or "").strip()
                )
            )
            unique_link_types = list(dict.fromkeys(link.link_type for link in chunk_links))
            chunk.metadata["linked_asset_count"] = len(unique_asset_ids)
            chunk.metadata["linked_asset_ids"] = unique_asset_ids[:24]
            chunk.metadata["linked_asset_types"] = unique_asset_types[:8]
            chunk.metadata["linked_asset_link_types"] = unique_link_types[:8]
            chunk.metadata["linked_asset_max_confidence"] = max(link.confidence for link in chunk_links)
        return links

    def _index_multimodal_assets(
        self,
        assets_or_payload: MultimodalPDFPayload | list[PageAsset] | None,
        embeddings: list[list[float]] | None = None,
    ) -> int:
        if assets_or_payload is None:
            return 0
        if isinstance(assets_or_payload, MultimodalPDFPayload):
            indexable_assets = [asset for asset in assets_or_payload.assets if self._is_indexable_asset(asset)]
        else:
            indexable_assets = [asset for asset in assets_or_payload if self._is_indexable_asset(asset)]
        if not indexable_assets:
            return 0
        vector_payload = embeddings or self.embedder.embed([asset.search_text for asset in indexable_assets])
        self.store.upsert_assets(indexable_assets, vector_payload)
        return len(indexable_assets)

    def _publish_fresh_document_vectors(
        self,
        document_id: str,
        accepted_chunks: list[Chunk],
        chunk_embeddings: list[list[float]],
        indexable_assets: list[PageAsset],
        asset_embeddings: list[list[float]],
    ) -> tuple[int, int]:
        staged_chunk_ids: list[str] = []
        staged_asset_ids: list[str] = []
        try:
            if accepted_chunks:
                self.store.upsert_chunks(accepted_chunks, chunk_embeddings, publish_state="staged")
                staged_chunk_ids = [chunk.chunk_id for chunk in accepted_chunks]
            if indexable_assets:
                self.store.upsert_assets(indexable_assets, asset_embeddings, publish_state="staged")
                staged_asset_ids = [asset.asset_id for asset in indexable_assets]
            if staged_chunk_ids:
                self.store.set_chunk_publish_state(staged_chunk_ids, "ready")
            if staged_asset_ids:
                self.store.set_asset_publish_state(staged_asset_ids, "ready")
            return len(staged_chunk_ids), len(staged_asset_ids)
        except Exception:
            # Best-effort cleanup remains for storage hygiene, but staged vectors are
            # hidden from retrieval until they are promoted to ready.
            if staged_chunk_ids:
                self.store.delete_chunks(staged_chunk_ids)
            if staged_asset_ids:
                self.store.delete_assets(staged_asset_ids)
            raise

    def _apply_chunk_enrichment(self, chunk: Chunk, status: str, quality_score: float, reasons: list[str]) -> None:
        # Enrichment denormalizes review-critical metadata onto the chunk row so the UI
        # and downstream agent logic do not have to recompute it.
        chunk.metadata["validation_status"] = status
        chunk.metadata["quality_score"] = quality_score
        chunk.metadata["quality_flags"] = sorted(set(chunk.metadata.get("quality_flags", [])) | set(reasons))
        chunk.metadata["ontology_classes"] = chunk_ontology_tags(chunk, self._current_ontology())

    def backfill_chunk_metadata(self, document_id: str | None = None) -> dict:
        rows = self.repository.list_document_chunk_records(document_id=document_id)
        updated = 0
        for row in rows:
            chunk = self._chunk_from_record(row)
            self._apply_chunk_enrichment(
                chunk,
                row["validation_status"],
                float(row.get("quality_score") or 0.0),
                list(row.get("reasons") or []),
            )
            self.repository.update_chunk_metadata(chunk.chunk_id, chunk.metadata)
            updated += 1
        return {"document_id": document_id, "updated_chunks": updated}

    def _auto_review_chunk(
        self,
        chunk: Chunk,
        quality_score: float,
        reasons: list[str],
        *,
        original_status: str = "review",
        original_metadata: dict[str, Any] | None = None,
    ) -> dict:
        try:
            artifact = review_chunk_with_meta(chunk, reasons, quality_score)
        except ChunkReviewError as exc:
            self.repository.save_chunk_review_run(
                document_id=chunk.document_id,
                chunk_id=chunk.chunk_id,
                provider=settings.review_provider,
                model=settings.review_model,
                prompt_version=settings.review_prompt_version,
                decision="review_error",
                confidence=0.0,
                detected_role="other",
                reason=str(exc),
                payload={"error": str(exc), "prior_reasons": reasons},
            )
            return {"chunk_id": chunk.chunk_id, "status": "review", "error": str(exc)}

        decision = artifact.result.decision
        confidence = artifact.result.confidence
        detected_role = artifact.result.detected_role
        reason = artifact.result.reason or "llm_review"

        if decision == "accept" and confidence >= settings.review_min_confidence:
            final_status = "accepted"
            final_reasons = ["auto_review_accept", f"llm_role:{detected_role}"]
            final_score = max(quality_score, confidence)
            result = self._accept_review_chunk(
                chunk,
                final_score=final_score,
                final_reasons=final_reasons,
                original_status=original_status,
                original_score=quality_score,
                original_reasons=reasons,
                original_metadata=dict(original_metadata or chunk.metadata),
            )
            kg_result = {"status": result["kg_status"], "errors": result["kg_errors"]}
        else:
            final_status = "rejected" if decision == "reject" else "review"
            final_reasons = [f"auto_review_{final_status}", f"llm_role:{detected_role}"]
            final_score = min(quality_score, confidence) if decision == "reject" else quality_score
            result = self._remove_review_chunk_from_live_state(
                chunk,
                final_status=final_status,
                final_score=final_score,
                final_reasons=final_reasons,
                original_status=original_status,
                original_score=quality_score,
                original_reasons=reasons,
                original_metadata=dict(original_metadata or chunk.metadata),
            )
            kg_result = None

        self.repository.save_chunk_review_run(
            document_id=chunk.document_id,
            chunk_id=chunk.chunk_id,
            provider=artifact.provider,
            model=artifact.model,
            prompt_version=artifact.prompt_version,
            decision=decision,
            confidence=confidence,
            detected_role=detected_role,
            reason=reason,
            payload={
                "raw_payload": artifact.raw_payload,
                "prior_reasons": reasons,
                "prior_quality_score": quality_score,
                "final_status": final_status,
            },
        )
        result = {
            "chunk_id": chunk.chunk_id,
            "status": result["status"],
            "decision": decision,
            "confidence": confidence,
            "detected_role": detected_role,
            "reason": reason,
        }
        if kg_result is not None:
            result["kg_status"] = kg_result["status"]
            result["kg_errors"] = kg_result["errors"]
        return result

    def _accept_review_chunk(
        self,
        chunk: Chunk,
        *,
        final_score: float,
        final_reasons: list[str],
        original_status: str,
        original_score: float,
        original_reasons: list[str],
        original_metadata: dict[str, Any],
    ) -> dict:
        self._apply_chunk_enrichment(chunk, "accepted", final_score, final_reasons)
        linked_assets = self._load_assets_for_chunk(chunk.chunk_id)
        staged = False
        validation_updated = False
        metadata_updated = False
        kg_persisted = False
        try:
            embeddings = self.embedder.embed([chunk.text])
            self.store.upsert_chunks([chunk], embeddings, publish_state="staged")
            staged = True
            kg_result = self._run_kg_pipeline(
                chunk.document_id,
                chunk,
                linked_assets=linked_assets,
                persist=False,
            )
            self.repository.update_chunk_validation(chunk.chunk_id, "accepted", final_score, final_reasons)
            validation_updated = True
            self.repository.update_chunk_metadata(chunk.chunk_id, chunk.metadata)
            metadata_updated = True
            self._persist_kg_outcome(chunk.document_id, chunk.chunk_id, kg_result)
            kg_persisted = True
            self.store.set_chunk_publish_state([chunk.chunk_id], "ready")
            self._refresh_document_synopses(chunk.document_id, source_stage="review_accept")
            return {
                "chunk_id": chunk.chunk_id,
                "status": "accepted",
                "kg_status": kg_result["status"],
                "kg_errors": kg_result["errors"],
            }
        except Exception:
            if staged:
                try:
                    self.store.delete_chunk(chunk.chunk_id)
                except Exception:
                    pass
            if kg_persisted:
                try:
                    self.repository.delete_chunk_kg(chunk.chunk_id)
                except Exception:
                    pass
            if metadata_updated:
                try:
                    self.repository.update_chunk_metadata(chunk.chunk_id, dict(original_metadata))
                except Exception:
                    pass
            if validation_updated:
                try:
                    self.repository.update_chunk_validation(
                        chunk.chunk_id,
                        original_status,
                        original_score,
                        list(original_reasons),
                    )
                except Exception:
                    pass
            raise

    def _remove_review_chunk_from_live_state(
        self,
        chunk: Chunk,
        *,
        final_status: str,
        final_score: float,
        final_reasons: list[str],
        original_status: str,
        original_score: float,
        original_reasons: list[str],
        original_metadata: dict[str, Any],
    ) -> dict:
        self._apply_chunk_enrichment(chunk, final_status, final_score, final_reasons)
        existing_record = self.store.get_record(chunk.chunk_id)
        vector_hidden = False
        validation_updated = False
        metadata_updated = False
        try:
            if existing_record is not None:
                self.store.set_chunk_publish_state([chunk.chunk_id], "staged")
                vector_hidden = True
            self.repository.update_chunk_validation(chunk.chunk_id, final_status, final_score, final_reasons)
            validation_updated = True
            self.repository.update_chunk_metadata(chunk.chunk_id, chunk.metadata)
            metadata_updated = True
            self._refresh_document_synopses(chunk.document_id, source_stage=f"review_{final_status}")
            self.repository.delete_chunk_kg(chunk.chunk_id)
            self.store.delete_chunk(chunk.chunk_id)
            return {"chunk_id": chunk.chunk_id, "status": final_status}
        except Exception:
            if vector_hidden:
                try:
                    self.store.set_chunk_publish_state([chunk.chunk_id], "ready")
                except Exception:
                    pass
            if metadata_updated:
                try:
                    self.repository.update_chunk_metadata(chunk.chunk_id, dict(original_metadata))
                except Exception:
                    pass
            if validation_updated:
                try:
                    self.repository.update_chunk_validation(
                        chunk.chunk_id,
                        original_status,
                        original_score,
                        list(original_reasons),
                    )
                except Exception:
                    pass
            raise

    def _persist_kg_outcome(self, document_id: str, chunk_id: str, kg_result: dict) -> None:
        result_obj = kg_result.get("_result_obj")
        if not isinstance(result_obj, KGExtractionResult):
            raise ValueError("KG outcome is missing the in-memory extraction result")
        self.repository.save_kg_result(
            document_id=document_id,
            chunk_id=chunk_id,
            result=result_obj,
            status=str(kg_result["status"]),
            errors=list(kg_result.get("errors") or []),
            raw_payload=dict(kg_result.get("_raw_payload") or {}),
            provider=kg_result.get("provider"),
            model=kg_result.get("model"),
            prompt_version=kg_result.get("prompt_version"),
        )

    def _run_kg_pipeline(
        self,
        document_id: str,
        chunk: Chunk,
        linked_assets: list[PageAsset] | None = None,
        *,
        persist: bool = True,
        pre_persist_check: Callable[[], None] | None = None,
    ) -> dict:
        return run_kg_pipeline(
            self,
            document_id=document_id,
            chunk=chunk,
            linked_assets=linked_assets,
            persist=persist,
            pre_persist_check=pre_persist_check,
        )

    @staticmethod
    def _append_reason(reasons: list[str], extra: str) -> list[str]:
        updated = list(reasons)
        if extra not in updated:
            updated.append(extra)
        return updated

    @staticmethod
    def _build_input_version(parser_version: str) -> str:
        return "|".join(
            [
                f"extractor={settings.extractor_version}",
                f"normalizer={settings.normalizer_version}",
                f"parser={parser_version}",
                f"chunker={settings.chunker_version}",
                f"validator={settings.validator_version}",
                f"embedding={settings.embedding_model}",
                f"kg={settings.kg_extraction_provider}:{settings.kg_model}:{settings.kg_prompt_version}",
            ]
        )

    @staticmethod
    def _empty_kg_result(document_id: str, chunk_id: str) -> KGExtractionResult:
        from src.bee_ingestion.offline_pipeline.stages import empty_kg_result

        return empty_kg_result(document_id, chunk_id)

    @staticmethod
    def _chunk_from_record(record: dict) -> Chunk:
        return Chunk(
            chunk_id=record["chunk_id"],
            document_id=str(record["document_id"]),
            tenant_id=str(record["tenant_id"]),
            chunk_index=record["chunk_index"],
            page_start=record["page_start"],
            page_end=record["page_end"],
            section_path=list(record["section_path"] or []),
            prev_chunk_id=str(record["prev_chunk_id"]) if record["prev_chunk_id"] else None,
            next_chunk_id=str(record["next_chunk_id"]) if record["next_chunk_id"] else None,
            char_start=record["char_start"],
            char_end=record["char_end"],
            text=record["text"],
            parser_version=record["parser_version"],
            chunker_version=record["chunker_version"],
            content_type=record["content_type"],
            metadata=dict(record.get("metadata_json") or {}),
        )


def _asset_link_tokens(text: str) -> set[str]:
    return {
        token
        for token in re.findall(r"[a-z0-9]{4,}", sanitize_text(text).lower())
        if token not in _ASSET_LINK_STOPWORDS
    }


def _visual_reference_keys(text: str) -> set[str]:
    normalized = sanitize_text(text).lower()
    keys: set[str] = set()
    for kind, marker in re.findall(r"\b(fig(?:ure)?|diagram|plate|table|illustration|image)\s+([a-z0-9ivx]+)\b", normalized):
        keys.add(f"{kind[:3]}:{marker}")
    return keys


def _is_generic_asset_label(label: str) -> bool:
    normalized = sanitize_text(label).strip().lower()
    if not normalized:
        return True
    return bool(
        re.fullmatch(r"page\s+\d+\s+(image|asset\s+\d+)", normalized)
        or re.fullmatch(r"asset\s+\d+", normalized)
    )


def _trusted_asset_label(asset: PageAsset) -> str:
    label = sanitize_text(str(asset.metadata.get("label") or "")).strip()
    return "" if _is_generic_asset_label(label) else label


def _trusted_asset_reference_keys(asset: PageAsset) -> set[str]:
    return _visual_reference_keys(
        "\n".join(
            part
            for part in [
                _trusted_asset_label(asset),
                asset.ocr_text,
                asset.description_text,
                " ".join(str(item).strip() for item in (asset.metadata.get("important_terms") or []) if str(item).strip()),
            ]
            if str(part).strip()
        )
    )


def _trusted_asset_anchor_text(asset: PageAsset) -> str:
    return sanitize_text(
        "\n".join(
            part
            for part in [
                _trusted_asset_label(asset),
                asset.ocr_text.strip(),
                asset.description_text.strip(),
                " ".join(str(item).strip() for item in (asset.metadata.get("important_terms") or []) if str(item).strip()),
                " ".join(str(item).strip() for item in (asset.metadata.get("linked_terms") or []) if str(item).strip()),
            ]
            if str(part).strip()
        )
    ).strip()


def _score_chunk_asset_link(chunk: Chunk, asset: PageAsset, page_chunk_count: int) -> dict | None:
    chunk_text = sanitize_text(chunk.text)
    asset_label = _trusted_asset_label(asset)
    asset_text = _trusted_asset_anchor_text(asset)
    if not asset_text:
        return None
    chunk_tokens = _asset_link_tokens(chunk_text)
    asset_tokens = _asset_link_tokens(asset_text)
    shared_terms = sorted(chunk_tokens & asset_tokens)[:8]
    overlap_ratio = len(shared_terms) / max(1, min(len(asset_tokens), 6)) if asset_tokens else 0.0
    chunk_reference_keys = _visual_reference_keys(chunk_text)
    asset_reference_keys = _trusted_asset_reference_keys(asset)
    shared_reference_keys = sorted(chunk_reference_keys & asset_reference_keys)
    label_tokens = _asset_link_tokens(asset_label)
    label_overlap = sorted(chunk_tokens & label_tokens)
    section_title = sanitize_text(str(chunk.metadata.get("section_title") or chunk.metadata.get("section_heading") or "")).strip()
    section_tokens = _asset_link_tokens(section_title)
    section_overlap = sorted(section_tokens & label_tokens)[:4]

    if asset.asset_type == "page_image" and shared_terms and overlap_ratio >= 0.16:
        return {
            "link_type": "text_overlap",
            "confidence": min(0.95, round(0.64 + overlap_ratio, 4)),
            "shared_terms": shared_terms,
            "reason": "page_image_text_overlap",
        }
    if (
        shared_reference_keys
        and asset.asset_type != "page_image"
        and len(shared_terms) >= 2
        and (len(label_overlap) >= 1 or len(section_overlap) >= 1)
        and overlap_ratio >= 0.18
    ):
        return {
            "link_type": "visual_reference",
            "confidence": min(0.9, round(0.66 + overlap_ratio + (0.04 if page_chunk_count == 1 else 0.0), 4)),
            "shared_terms": sorted(set(shared_reference_keys[:4] + shared_terms[:4] + label_overlap[:2] + section_overlap[:2])),
            "reason": "visual_reference_with_layout_support",
        }
    return None
