"""Ingest routes for registering offline ingestion jobs."""

from __future__ import annotations

from pathlib import Path, PureWindowsPath
from uuid import uuid4

from fastapi import APIRouter, File, Form, HTTPException, UploadFile
from pydantic import BaseModel

from src.bee_ingestion.http_api.dependencies import service
from src.bee_ingestion.models import SourceDocument
from src.bee_ingestion.pdf_utils import build_pdf_content_hash
from src.bee_ingestion.settings import settings, workspace_root

router = APIRouter()


class IngestRequest(BaseModel):
    tenant_id: str = "shared"
    source_type: str = "text"
    filename: str = "manual.txt"
    raw_text: str
    document_class: str = "book"
    parser_version: str = "v1"
    ocr_engine: str | None = None
    ocr_model: str | None = None


class IngestPdfRequest(BaseModel):
    tenant_id: str = "shared"
    path: str
    filename: str | None = None
    document_class: str = "book"
    parser_version: str = "v1"
    page_start: int | None = None
    page_end: int | None = None


def _resolve_workspace_pdf_path(raw_path: str) -> Path:
    candidate = Path(raw_path).expanduser()
    if not candidate.is_absolute():
        candidate = (workspace_root() / raw_path).resolve()
    else:
        candidate = candidate.resolve()
    return candidate


def _sanitize_upload_filename(filename: str | None, *, fallback: str) -> str:
    cleaned = "".join(ch for ch in str(filename or "").strip() if ch not in {'"', "'", "\x00", "/", "\\", ":"})
    cleaned = cleaned.strip()
    return cleaned or fallback


def _workspace_upload_dir() -> Path:
    target = (workspace_root() / "data" / "uploads").resolve()
    target.mkdir(parents=True, exist_ok=True)
    return target


@router.post("/ingest/text")
def ingest_text(request: IngestRequest) -> dict:
    return service.enqueue_text(
        SourceDocument(
            tenant_id=request.tenant_id,
            source_type=request.source_type,
            filename=request.filename,
            raw_text=request.raw_text,
            document_class=request.document_class,
            parser_version=request.parser_version,
            ocr_engine=request.ocr_engine,
            ocr_model=request.ocr_model,
        )
    )


@router.post("/ingest/pdf")
def ingest_pdf(request: IngestPdfRequest) -> dict:
    try:
        source_path = _resolve_workspace_pdf_path(request.path)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    if source_path.suffix.lower() != ".pdf":
        raise HTTPException(status_code=400, detail="PDF ingest requires a .pdf file")
    if not source_path.exists():
        raise HTTPException(status_code=404, detail="PDF file not found")

    metadata = {"source_path": str(source_path)}
    if request.page_start is not None or request.page_end is not None:
        metadata["page_range"] = {"start": request.page_start, "end": request.page_end}

    return service.enqueue_text(
        SourceDocument(
            tenant_id=request.tenant_id,
            source_type="pdf",
            filename=request.filename or PureWindowsPath(request.path).name,
            raw_text="",
            metadata=metadata,
            document_class=request.document_class,
            parser_version=request.parser_version,
            content_hash_value=build_pdf_content_hash(source_path, request.page_start, request.page_end),
        )
    )


@router.post("/admin/api/uploads/ingest")
async def admin_upload_and_ingest(
    file: UploadFile = File(...),
    tenant_id: str = Form(default="shared"),
    document_class: str = Form(default="book"),
    parser_version: str = Form(default="v1"),
    source_type: str | None = Form(default=None),
    filename: str | None = Form(default=None),
    page_start: int | None = Form(default=None),
    page_end: int | None = Form(default=None),
) -> dict:
    safe_name = _sanitize_upload_filename(filename or file.filename, fallback="upload.bin")
    suffix = Path(safe_name).suffix.lower()
    resolved_source_type = (source_type or ("pdf" if suffix == ".pdf" else "text")).strip().lower()
    if resolved_source_type not in {"pdf", "text"}:
        raise HTTPException(status_code=400, detail="Upload ingest only supports pdf or text source types")
    if resolved_source_type == "pdf" and suffix != ".pdf":
        raise HTTPException(status_code=400, detail="PDF upload ingest requires a .pdf file")

    upload_dir = _workspace_upload_dir()
    stored_name = f"{uuid4()}-{safe_name}"
    stored_path = upload_dir / stored_name
    total_bytes = 0
    with stored_path.open("wb") as handle:
        while True:
            chunk = await file.read(1024 * 1024)
            if not chunk:
                break
            total_bytes += len(chunk)
            if total_bytes > settings.upload_max_bytes:
                handle.close()
                stored_path.unlink(missing_ok=True)
                raise HTTPException(status_code=413, detail="Upload exceeds configured size limit")
            handle.write(chunk)
    await file.close()

    metadata = {
        "uploaded_filename": safe_name,
        "uploaded_path": str(stored_path),
        "source_path": str(stored_path),
        "uploaded_size_bytes": total_bytes,
    }

    if resolved_source_type == "pdf":
        raw_text = ""
        if page_start is not None or page_end is not None:
            metadata["page_range"] = {"start": page_start, "end": page_end}
    else:
        try:
            raw_text = stored_path.read_text(encoding="utf-8")
        except UnicodeDecodeError:
            raw_text = stored_path.read_text(encoding="utf-8", errors="replace")
            metadata["encoding_warning"] = "utf8_decode_replaced_invalid_bytes"

    job = service.enqueue_text(
        SourceDocument(
            tenant_id=tenant_id,
            source_type=resolved_source_type,
            filename=safe_name,
            raw_text=raw_text,
            metadata=metadata,
            document_class=document_class,
            parser_version=parser_version,
            content_hash_value=(
                build_pdf_content_hash(stored_path, page_start, page_end)
                if resolved_source_type == "pdf"
                else None
            ),
        )
    )
    return {
        "stored_path": str(stored_path),
        "filename": safe_name,
        "source_type": resolved_source_type,
        "job": job,
    }
