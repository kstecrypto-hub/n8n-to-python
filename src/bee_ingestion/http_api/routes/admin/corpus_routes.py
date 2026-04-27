"""HTTP routes for admin corpus inspection and editor workflows."""

from __future__ import annotations

from typing import Any

from fastapi import APIRouter, Query, Request
from fastapi.responses import FileResponse
from pydantic import BaseModel

from src.bee_ingestion.admin import corpus_inspection_service
from src.bee_ingestion.http_api.dependencies import chroma_store, repository, service
from src.bee_ingestion.http_api.request_auth import require_authenticated_public_user

router = APIRouter()


class AdminEditorRequest(BaseModel):
    record_type: str
    record_id: str
    secondary_id: str | None = None
    payload: dict[str, Any] | None = None
    sync_index: bool = False


@router.get("/admin/api/documents")
def admin_documents(limit: int = Query(default=25, ge=1, le=250), offset: int = Query(default=0, ge=0)) -> dict:
    return corpus_inspection_service.list_documents(repository=repository, limit=limit, offset=offset)


@router.get("/admin/api/documents/{document_id}")
def admin_document_detail(document_id: str) -> dict:
    return corpus_inspection_service.get_document_detail(repository=repository, document_id=document_id)


@router.get("/admin/api/documents/{document_id}/bundle")
def admin_document_bundle(document_id: str, limit: int = Query(default=250, ge=25, le=1000)) -> dict:
    return corpus_inspection_service.get_document_bundle(
        repository=repository,
        chroma_store=chroma_store,
        document_id=document_id,
        limit=limit,
    )


@router.get("/admin/api/chunks")
def admin_chunks(
    document_id: str | None = None,
    status: str | None = None,
    limit: int = Query(default=100, ge=1, le=250),
    offset: int = Query(default=0, ge=0),
) -> dict:
    return corpus_inspection_service.list_chunks(repository=repository, document_id=document_id, status=status, limit=limit, offset=offset)


@router.get("/admin/api/chunks/{chunk_id}")
def admin_chunk_detail(chunk_id: str) -> dict:
    return corpus_inspection_service.get_chunk_detail(repository=repository, chroma_store=chroma_store, chunk_id=chunk_id)


@router.get("/admin/api/assets/{asset_id}")
def admin_asset_detail(asset_id: str) -> dict:
    return corpus_inspection_service.get_asset_detail(repository=repository, chroma_store=chroma_store, asset_id=asset_id)


@router.get("/admin/api/assets/{asset_id}/image")
def admin_asset_image(asset_id: str):
    return FileResponse(corpus_inspection_service.get_admin_asset_image_path(repository=repository, asset_id=asset_id))


@router.get("/agent/assets/{asset_id}/image")
def public_agent_asset_image(asset_id: str, request: Request):
    require_authenticated_public_user(request)
    return FileResponse(corpus_inspection_service.get_public_asset_image_path(repository=repository, asset_id=asset_id))


@router.get("/admin/api/kg/entities")
def admin_kg_entities(
    document_id: str | None = None,
    search: str | None = None,
    entity_type: str | None = None,
    limit: int = Query(default=100, ge=1, le=250),
    offset: int = Query(default=0, ge=0),
) -> dict:
    return corpus_inspection_service.list_kg_entities(
        repository=repository,
        document_id=document_id,
        search=search,
        entity_type=entity_type,
        limit=limit,
        offset=offset,
    )


@router.get("/admin/api/kg/entities/{entity_id}")
def admin_kg_entity_detail(entity_id: str) -> dict:
    return corpus_inspection_service.get_kg_entity_detail(repository=repository, entity_id=entity_id)


@router.get("/admin/api/kg/assertions")
def admin_kg_assertions(
    document_id: str | None = None,
    entity_id: str | None = None,
    predicate: str | None = None,
    status: str | None = None,
    chunk_id: str | None = None,
    limit: int = Query(default=100, ge=1, le=250),
    offset: int = Query(default=0, ge=0),
) -> dict:
    return corpus_inspection_service.list_kg_assertions(
        repository=repository,
        document_id=document_id,
        entity_id=entity_id,
        predicate=predicate,
        status=status,
        chunk_id=chunk_id,
        limit=limit,
        offset=offset,
    )


@router.get("/admin/api/kg/raw")
def admin_kg_raw(
    document_id: str | None = None,
    chunk_id: str | None = None,
    status: str | None = None,
    limit: int = Query(default=100, ge=1, le=250),
    offset: int = Query(default=0, ge=0),
) -> dict:
    return corpus_inspection_service.list_kg_raw(
        repository=repository,
        document_id=document_id,
        chunk_id=chunk_id,
        status=status,
        limit=limit,
        offset=offset,
    )


@router.get("/admin/api/chroma/collections")
def admin_chroma_collections() -> list[dict]:
    return corpus_inspection_service.list_chroma_collections(chroma_store=chroma_store)


@router.get("/admin/api/chroma/records")
def admin_chroma_records(
    collection_name: str | None = None,
    document_id: str | None = None,
    limit: int = Query(default=50, ge=1, le=250),
    offset: int = Query(default=0, ge=0),
) -> dict:
    return corpus_inspection_service.list_chroma_records(
        chroma_store=chroma_store,
        collection_name=collection_name,
        document_id=document_id,
        limit=limit,
        offset=offset,
    )


@router.get("/admin/api/metadata/chunks")
def admin_chunk_metadata(
    document_id: str | None = None,
    status: str | None = None,
    limit: int = Query(default=100, ge=1, le=250),
    offset: int = Query(default=0, ge=0),
) -> dict:
    return corpus_inspection_service.list_chunk_metadata(
        repository=repository,
        document_id=document_id,
        status=status,
        limit=limit,
        offset=offset,
    )


@router.get("/admin/api/chroma/parity")
def admin_chroma_parity(document_id: str | None = None) -> dict:
    return corpus_inspection_service.get_chroma_parity(repository=repository, chroma_store=chroma_store, document_id=document_id)


@router.post("/admin/api/editor/load")
def admin_editor_load(request: AdminEditorRequest) -> dict:
    return corpus_inspection_service.load_editor_record(
        repository=repository,
        record_type=request.record_type,
        record_id=request.record_id,
        secondary_id=request.secondary_id,
    )


@router.put("/admin/api/editor/save")
def admin_editor_save(request: AdminEditorRequest) -> dict:
    return corpus_inspection_service.save_editor_record(
        repository=repository,
        service=service,
        chroma_store=chroma_store,
        record_type=request.record_type,
        record_id=request.record_id,
        payload=dict(request.payload or {}),
        secondary_id=request.secondary_id,
        sync_index=request.sync_index,
    )


@router.post("/admin/api/editor/delete")
def admin_editor_delete(request: AdminEditorRequest) -> dict:
    return corpus_inspection_service.delete_editor_record(
        repository=repository,
        chroma_store=chroma_store,
        record_type=request.record_type,
        record_id=request.record_id,
        secondary_id=request.secondary_id,
        sync_index=request.sync_index,
    )


@router.post("/admin/api/editor/resync")
def admin_editor_resync(request: AdminEditorRequest) -> dict:
    return corpus_inspection_service.resync_editor_record(
        service=service,
        record_type=request.record_type,
        record_id=request.record_id,
    )
