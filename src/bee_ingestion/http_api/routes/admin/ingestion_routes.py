"""HTTP routes for admin ingestion and maintenance controls."""

from __future__ import annotations

from fastapi import APIRouter
from pydantic import BaseModel

from src.bee_ingestion.admin import ingestion_admin_service
from src.bee_ingestion.http_api.dependencies import repository, service

router = APIRouter()


class ReviewBatchRequest(BaseModel):
    document_id: str | None = None
    batch_size: int = 25


class RevalidateDocumentRequest(BaseModel):
    rerun_kg: bool = True


@router.post("/admin/api/system/reingest/start")
def admin_start_reingest() -> dict:
    return ingestion_admin_service.start_reingest(repository=repository, require_resume=False)


@router.post("/admin/api/system/reingest/resume")
def admin_resume_reingest() -> dict:
    return ingestion_admin_service.start_reingest(repository=repository, require_resume=True)


@router.post("/admin/api/system/reingest/stop")
def admin_stop_reingest() -> dict:
    return ingestion_admin_service.stop_reingest(repository=repository)


@router.post("/admin/api/chunks/review/auto")
def admin_auto_review_chunks(request: ReviewBatchRequest) -> dict:
    return ingestion_admin_service.auto_review_chunks(
        service=service,
        document_id=request.document_id,
        batch_size=request.batch_size,
    )


@router.post("/admin/api/documents/{document_id}/revalidate")
def admin_revalidate_document(document_id: str, request: RevalidateDocumentRequest) -> dict:
    return ingestion_admin_service.revalidate_document(service=service, document_id=document_id, rerun_kg=request.rerun_kg)


@router.post("/admin/api/documents/{document_id}/rebuild")
def admin_rebuild_document(document_id: str) -> dict:
    return ingestion_admin_service.rebuild_document(service=service, document_id=document_id)


@router.post("/admin/api/documents/{document_id}/reindex")
def admin_reindex_document(document_id: str) -> dict:
    return ingestion_admin_service.reindex_document(service=service, document_id=document_id)


@router.post("/admin/api/documents/{document_id}/reprocess-kg")
def admin_reprocess_document_kg(document_id: str, request: ReviewBatchRequest) -> dict:
    return ingestion_admin_service.reprocess_document_kg(
        service=service,
        document_id=document_id,
        batch_size=request.batch_size,
    )


@router.post("/admin/api/documents/{document_id}/delete")
def admin_delete_document(document_id: str) -> dict:
    return ingestion_admin_service.delete_document(service=service, document_id=document_id)


@router.post("/admin/api/reset")
def admin_reset_pipeline() -> dict:
    return ingestion_admin_service.reset_pipeline(service=service)

