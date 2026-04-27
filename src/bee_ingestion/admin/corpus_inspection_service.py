"""Admin corpus inspection and editor workflows.

This module owns operator drill-down views over corpus, chunk, asset, KG, and
derived vector state. It also owns editor-style record mutation workflows. It
does not own HTTP routes or generic repository wiring.
"""

from __future__ import annotations

import os
from pathlib import Path
from typing import Any
from uuid import UUID

from fastapi import HTTPException

from src.bee_ingestion.settings import settings, workspace_root
from src.bee_ingestion.storage import agent_profile_store, agent_trace_store, memory_store

PAGE_ASSET_ROOT = (workspace_root() / "data" / "page_assets").resolve()
EDITOR_DELETABLE_TYPES = {"asset", "asset_link", "kg_entity", "kg_assertion", "kg_raw", "agent_session", "agent_profile", "agent_pattern"}
EDITOR_SYNCABLE_TYPES = {"document", "chunk", "asset"}


def _json_safe(value: Any) -> Any:
    if isinstance(value, dict):
        return {str(key): _json_safe(item) for key, item in value.items()}
    if isinstance(value, list):
        return [_json_safe(item) for item in value]
    if isinstance(value, tuple):
        return [_json_safe(item) for item in value]
    if isinstance(value, UUID):
        return str(value)
    if value is None or isinstance(value, (str, int, float, bool)):
        return value
    if hasattr(value, "isoformat"):
        try:
            return value.isoformat()
        except Exception:
            return str(value)
    return str(value)


def get_chroma_payload(*, chroma_store: Any, document_id: str | None = None, limit: int = 50, offset: int = 0, collection_name: str | None = None) -> dict:
    try:
        total = chroma_store.count_records(document_id=document_id, collection_name=collection_name)
        records = chroma_store.list_records(document_id=document_id, limit=limit, offset=offset, collection_name=collection_name)
        return {"records": records, "total": total, "error": None}
    except Exception as exc:  # pragma: no cover - defensive admin path
        return {"records": [], "total": 0, "error": str(exc)}


def get_chroma_parity(*, repository: Any, chroma_store: Any, document_id: str | None = None) -> dict:
    accepted_rows = repository.list_chunk_records_for_kg(document_id=document_id, limit=5000, offset=0)
    accepted_ids = {row["chunk_id"] for row in accepted_rows}
    chroma_payload = get_chroma_payload(chroma_store=chroma_store, document_id=document_id, limit=5000, offset=0)
    vector_ids = {row["id"] for row in chroma_payload["records"]}
    missing_vectors = sorted(accepted_ids - vector_ids)
    extra_vectors = sorted(vector_ids - accepted_ids)
    return {
        "document_id": document_id,
        "accepted_chunks": len(accepted_ids),
        "vectors": len(vector_ids),
        "missing_vectors": missing_vectors[:25],
        "extra_vectors": extra_vectors[:25],
        "missing_vectors_total": len(missing_vectors),
        "extra_vectors_total": len(extra_vectors),
        "error": chroma_payload["error"],
    }


def _safe_page_asset_path(raw_path: str | Path) -> Path:
    candidate = Path(str(raw_path or "")).resolve()
    try:
        candidate.relative_to(PAGE_ASSET_ROOT)
    except ValueError as exc:
        raise HTTPException(status_code=404, detail="Asset file not available") from exc
    if not candidate.exists() or not candidate.is_file():
        raise HTTPException(status_code=404, detail="Asset file not found")
    return candidate


def list_documents(*, repository: Any, limit: int, offset: int) -> dict:
    return {
        "items": repository.list_documents(limit=limit, offset=offset),
        "total": repository.count_documents(),
        "limit": limit,
        "offset": offset,
    }


def get_document_detail(*, repository: Any, document_id: str) -> dict:
    detail = repository.get_document_detail(document_id)
    if detail is None:
        raise HTTPException(status_code=404, detail="Document not found")
    return detail


def get_document_bundle(*, repository: Any, chroma_store: Any, document_id: str, limit: int) -> dict:
    detail = get_document_detail(repository=repository, document_id=document_id)
    sources = repository.list_document_sources(document_id=document_id, limit=limit)
    pages = repository.list_document_pages(document_id=document_id, limit=limit)
    assets = repository.list_page_assets(document_id=document_id, limit=limit)
    chunk_asset_links = repository.list_chunk_asset_links(document_id=document_id, limit=limit)
    chunks = repository.list_chunks(document_id=document_id, limit=limit)
    metadata = repository.list_chunk_metadata(document_id=document_id, limit=limit)
    kg_assertions = repository.list_kg_assertions(document_id=document_id, limit=limit)
    kg_entities = repository.list_kg_entities(document_id=document_id, limit=limit)
    kg_evidence = repository.list_kg_evidence(document_id=document_id, limit=limit)
    kg_raw = repository.list_kg_raw_extractions(document_id=document_id, limit=limit)
    chroma = get_chroma_payload(chroma_store=chroma_store, document_id=document_id, limit=limit, offset=0)
    asset_chroma = get_chroma_payload(
        chroma_store=chroma_store,
        document_id=document_id,
        limit=limit,
        offset=0,
        collection_name=chroma_store.asset_collection.name,
    )
    counts = repository.get_document_related_counts(document_id=document_id)
    counts["vectors"] = chroma["total"]
    counts["asset_vectors"] = asset_chroma["total"]
    counts["parity"] = get_chroma_parity(repository=repository, chroma_store=chroma_store, document_id=document_id)
    return {
        **detail,
        "sources": sources,
        "pages": pages,
        "page_assets": assets,
        "chunk_asset_links": chunk_asset_links,
        "chunks": chunks,
        "chunk_metadata": metadata,
        "kg_assertions": kg_assertions,
        "kg_entities": kg_entities,
        "kg_evidence": kg_evidence,
        "kg_raw": kg_raw,
        "chroma_records": chroma["records"],
        "chroma_error": chroma["error"],
        "asset_chroma_records": asset_chroma["records"],
        "asset_chroma_error": asset_chroma["error"],
        "counts": counts,
        "bundle_limit": limit,
    }


def list_chunks(*, repository: Any, document_id: str | None, status: str | None, limit: int, offset: int) -> dict:
    return {
        "items": repository.list_chunks(document_id=document_id, status=status, limit=limit, offset=offset),
        "total": repository.count_chunks(document_id=document_id, status=status),
        "limit": limit,
        "offset": offset,
    }


def get_chunk_detail(*, repository: Any, chroma_store: Any, chunk_id: str) -> dict:
    detail = repository.get_chunk_detail(chunk_id)
    if detail is None:
        raise HTTPException(status_code=404, detail="Chunk not found")
    chroma_record = None
    chroma_error = None
    try:
        chroma_record = chroma_store.get_record(chunk_id)
    except Exception as exc:  # pragma: no cover
        chroma_error = str(exc)
    return {**detail, "chroma_record": chroma_record, "chroma_error": chroma_error}


def get_asset_detail(*, repository: Any, chroma_store: Any, asset_id: str) -> dict:
    detail = repository.get_page_asset_detail(asset_id)
    if detail is None:
        raise HTTPException(status_code=404, detail="Asset not found")
    chroma_record = None
    chroma_error = None
    try:
        chroma_record = chroma_store.get_asset_record(asset_id)
    except Exception as exc:  # pragma: no cover
        chroma_error = str(exc)
    return {**detail, "chroma_record": chroma_record, "chroma_error": chroma_error}


def get_admin_asset_image_path(*, repository: Any, asset_id: str) -> Path:
    detail = repository.get_page_asset_detail(asset_id)
    if detail is None:
        raise HTTPException(status_code=404, detail="Asset not found")
    return _safe_page_asset_path(str(detail["asset"].get("asset_path") or ""))


def get_public_asset_image_path(*, repository: Any, asset_id: str) -> Path:
    detail = repository.get_page_asset_detail(asset_id)
    if detail is None:
        raise HTTPException(status_code=404, detail="Asset not found")
    asset = detail.get("asset") or {}
    if str(asset.get("tenant_id") or "") != (settings.agent_public_tenant_id or "shared"):
        raise HTTPException(status_code=404, detail="Asset not available")
    return _safe_page_asset_path(str(asset.get("asset_path") or ""))


def list_kg_entities(*, repository: Any, document_id: str | None, search: str | None, entity_type: str | None, limit: int, offset: int) -> dict:
    return {
        "items": repository.list_kg_entities(document_id=document_id, search=search, entity_type=entity_type, limit=limit, offset=offset),
        "total": repository.count_kg_entities(document_id=document_id, search=search, entity_type=entity_type),
        "limit": limit,
        "offset": offset,
    }


def get_kg_entity_detail(*, repository: Any, entity_id: str) -> dict:
    detail = repository.get_kg_entity_detail(entity_id)
    if detail is None:
        raise HTTPException(status_code=404, detail="KG entity not found")
    return detail


def list_kg_assertions(*, repository: Any, document_id: str | None, entity_id: str | None, predicate: str | None, status: str | None, chunk_id: str | None, limit: int, offset: int) -> dict:
    return {
        "items": repository.list_kg_assertions(
            document_id=document_id,
            entity_id=entity_id,
            predicate=predicate,
            status=status,
            chunk_id=chunk_id,
            limit=limit,
            offset=offset,
        ),
        "total": repository.count_kg_assertions(
            document_id=document_id,
            entity_id=entity_id,
            predicate=predicate,
            status=status,
            chunk_id=chunk_id,
        ),
        "limit": limit,
        "offset": offset,
    }


def list_kg_raw(*, repository: Any, document_id: str | None, chunk_id: str | None, status: str | None, limit: int, offset: int) -> dict:
    return {
        "items": repository.list_kg_raw_extractions(document_id=document_id, chunk_id=chunk_id, status=status, limit=limit, offset=offset),
        "total": repository.count_kg_raw_extractions(document_id=document_id, chunk_id=chunk_id, status=status),
        "limit": limit,
        "offset": offset,
    }


def list_chroma_collections(*, chroma_store: Any) -> list[dict]:
    collections = []
    chunk_collection_name = chroma_store.collection.name
    asset_collection_name = chroma_store.asset_collection.name
    for collection in chroma_store.list_collections():
        target = chroma_store.client.get_collection(collection.name)
        collections.append(
            {
                "name": collection.name,
                "count": target.count(),
                "metadata": getattr(target, "metadata", None) or getattr(collection, "metadata", None) or {},
                "is_default_chunk": collection.name == chunk_collection_name,
                "is_default_asset": collection.name == asset_collection_name,
            }
        )
    return collections


def list_chroma_records(*, chroma_store: Any, collection_name: str | None, document_id: str | None, limit: int, offset: int) -> dict:
    allowed = {collection.name for collection in chroma_store.list_collections()}
    target_collection = collection_name or chroma_store.collection.name
    if target_collection not in allowed:
        raise HTTPException(status_code=400, detail="Unknown Chroma collection")
    payload = get_chroma_payload(chroma_store=chroma_store, document_id=document_id, limit=limit, offset=offset, collection_name=target_collection)
    return {"items": payload["records"], "total": payload["total"], "limit": limit, "offset": offset, "error": payload["error"]}


def list_chunk_metadata(*, repository: Any, document_id: str | None, status: str | None, limit: int, offset: int) -> dict:
    return {
        "items": repository.list_chunk_metadata(document_id=document_id, status=status, limit=limit, offset=offset),
        "total": repository.count_chunk_metadata(document_id=document_id, status=status),
        "limit": limit,
        "offset": offset,
    }


def _normalize_editor_type(value: str) -> str:
    normalized = (value or "").strip().lower()
    aliases = {
        "document_source": "source",
        "document_page": "page",
        "page_asset": "asset",
        "chunk_asset_link": "asset_link",
        "kgentity": "kg_entity",
        "kgassertion": "kg_assertion",
        "kgraw": "kg_raw",
        "session": "agent_session",
        "profile": "agent_profile",
        "session_memory": "agent_session_memory",
        "pattern": "agent_pattern",
    }
    return aliases.get(normalized, normalized)


def _parse_editor_page_number(value: str | None) -> int:
    try:
        return int(str(value or "").strip())
    except ValueError as exc:
        raise HTTPException(status_code=400, detail="Page editors require a numeric secondary_id page number") from exc


def load_editor_record(*, repository: Any, record_type: str, record_id: str, secondary_id: str | None = None) -> dict:
    record_type = _normalize_editor_type(record_type)
    if record_type == "document":
        row = repository.get_document_record(record_id)
    elif record_type == "source":
        row = repository.get_document_source(record_id)
    elif record_type == "page":
        row = repository.get_document_page(record_id, _parse_editor_page_number(secondary_id))
    elif record_type == "chunk":
        row = repository.get_chunk_record(record_id)
    elif record_type == "asset":
        detail = repository.get_page_asset_detail(record_id)
        row = detail.get("asset") if detail else None
    elif record_type == "asset_link":
        row = repository.get_chunk_asset_link(record_id)
    elif record_type == "kg_entity":
        row = repository.get_kg_entity_record(record_id)
    elif record_type == "kg_assertion":
        row = repository.get_kg_assertion(record_id)
    elif record_type == "kg_raw":
        row = repository.get_kg_raw_extraction(record_id)
    elif record_type == "agent_session":
        row = repository.get_agent_session(record_id)
    elif record_type == "agent_profile":
        row = agent_profile_store.get_agent_profile(repository, record_id)
    elif record_type == "agent_session_memory":
        row = memory_store.get_agent_session_memory(repository, record_id)
    elif record_type == "agent_pattern":
        row = agent_trace_store.get_agent_query_pattern(repository, secondary_id or "shared", record_id)
    else:
        raise HTTPException(status_code=400, detail=f"Unsupported editor record_type '{record_type}'")
    if row is None:
        raise HTTPException(status_code=404, detail=f"{record_type} not found")
    return {
        "record_type": record_type,
        "record_id": record_id,
        "secondary_id": secondary_id,
        "record": row,
        "capabilities": {
            "delete": record_type in EDITOR_DELETABLE_TYPES,
            "sync_index": record_type in EDITOR_SYNCABLE_TYPES,
        },
    }


def save_editor_record(
    *,
    repository: Any,
    service: Any,
    chroma_store: Any,
    record_type: str,
    record_id: str,
    payload: dict[str, Any],
    secondary_id: str | None = None,
    sync_index: bool = False,
) -> dict:
    record_type = _normalize_editor_type(record_type)
    try:
        if record_type == "document":
            row = repository.update_document_record(record_id, payload)
        elif record_type == "source":
            row = repository.update_document_source(record_id, payload)
        elif record_type == "page":
            row = repository.update_document_page(record_id, _parse_editor_page_number(secondary_id), payload)
        elif record_type == "chunk":
            row = repository.update_chunk_record_admin(record_id, payload)
        elif record_type == "asset":
            row = repository.update_page_asset(record_id, payload)
        elif record_type == "asset_link":
            row = repository.update_chunk_asset_link(record_id, payload)
        elif record_type == "kg_entity":
            row = repository.update_kg_entity(record_id, payload)
        elif record_type == "kg_assertion":
            row = repository.update_kg_assertion(record_id, payload)
        elif record_type == "kg_raw":
            row = repository.update_kg_raw_extraction(record_id, payload)
        elif record_type == "agent_session":
            row = repository.update_agent_session_record(record_id, payload)
        elif record_type == "agent_profile":
            row = agent_profile_store.update_agent_profile_record(repository, record_id, payload)
        elif record_type == "agent_session_memory":
            row = memory_store.update_agent_session_memory_record(repository, record_id, payload)
        elif record_type == "agent_pattern":
            row = agent_trace_store.update_agent_query_pattern(repository, secondary_id or "shared", record_id, payload)
        else:
            raise HTTPException(status_code=400, detail=f"Unsupported editor record_type '{record_type}'")
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    if row is None:
        raise HTTPException(status_code=404, detail=f"{record_type} not found")
    sync_result = None
    if sync_index:
        if record_type == "document":
            sync_result = service.reindex_document(record_id)
        elif record_type == "chunk":
            sync_result = service.sync_chunk_index(record_id)
        elif record_type == "asset":
            sync_result = service.sync_asset_index(record_id)
        else:
            raise HTTPException(status_code=400, detail=f"Record type '{record_type}' does not support sync_index")
    return {
        "record_type": record_type,
        "record_id": record_id,
        "secondary_id": secondary_id,
        "record": row,
        "sync_result": sync_result,
        "capabilities": {
            "delete": record_type in EDITOR_DELETABLE_TYPES,
            "sync_index": record_type in EDITOR_SYNCABLE_TYPES,
        },
    }


def delete_editor_record(
    *,
    repository: Any,
    chroma_store: Any,
    record_type: str,
    record_id: str,
    secondary_id: str | None = None,
    sync_index: bool = False,
) -> dict:
    record_type = _normalize_editor_type(record_type)
    if record_type not in EDITOR_DELETABLE_TYPES:
        raise HTTPException(status_code=400, detail=f"Record type '{record_type}' is not deletable through the editor")
    try:
        if record_type == "asset":
            if sync_index:
                chroma_store.delete_asset(record_id)
            deleted = repository.delete_page_asset(record_id)
        elif record_type == "asset_link":
            deleted = repository.delete_chunk_asset_link(record_id)
        elif record_type == "kg_entity":
            deleted = repository.delete_kg_entity(record_id)
        elif record_type == "kg_assertion":
            deleted = repository.delete_kg_assertion(record_id)
            repository.prune_orphan_kg_entities()
        elif record_type == "kg_raw":
            deleted = repository.delete_kg_raw_extraction(record_id)
        elif record_type == "agent_session":
            deleted = repository.delete_agent_session(record_id)
        elif record_type == "agent_profile":
            deleted = repository.delete_agent_profile(record_id)
        elif record_type == "agent_pattern":
            deleted = repository.delete_agent_query_pattern(secondary_id or "shared", record_id)
        else:
            raise HTTPException(status_code=400, detail=f"Unsupported editor record_type '{record_type}'")
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    if not deleted:
        raise HTTPException(status_code=404, detail=f"{record_type} not found")
    return {"record_type": record_type, "record_id": record_id, "secondary_id": secondary_id, "deleted": True}


def resync_editor_record(*, service: Any, record_type: str, record_id: str) -> dict:
    record_type = _normalize_editor_type(record_type)
    if record_type == "document":
        return service.reindex_document(record_id)
    if record_type == "chunk":
        return service.sync_chunk_index(record_id)
    if record_type == "asset":
        return service.sync_asset_index(record_id)
    raise HTTPException(status_code=400, detail=f"Record type '{record_type}' does not support vector resync")
