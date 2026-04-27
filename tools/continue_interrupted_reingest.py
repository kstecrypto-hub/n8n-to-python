from __future__ import annotations

import json
import os
import sys
import time
import threading
import traceback
from dataclasses import asdict, dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from src.bee_ingestion.models import SourceDocument
from src.bee_ingestion.pdf_utils import build_pdf_content_hash
from src.bee_ingestion.pipeline import is_terminal_job_status
from src.bee_ingestion.service import IngestionService


APP_ROOT = Path("/app")
LOG_DIR = APP_ROOT / "data" / "logs"
PROGRESS_PATH = LOG_DIR / "reingest-progress.json"
SUMMARY_PATH = LOG_DIR / "reingest-summary.json"
MANIFEST_PATH = APP_ROOT / "data" / "reingest-manifest.json"
EXCLUDED_DIR_NAMES = {
    ".git",
    ".hg",
    ".svn",
    ".venv",
    "venv",
    "__pycache__",
    "node_modules",
    "page_assets",
}
CONTINUE_ON_ERROR = os.environ.get("REINGEST_CONTINUE_ON_ERROR", "").strip().lower() in {"1", "true", "yes", "on"}
HEARTBEAT_SECONDS = max(5, int(os.environ.get("REINGEST_HEARTBEAT_SECONDS", "15")))


@dataclass
class FileRun:
    filename: str
    started_at: str
    finished_at: str | None
    duration_seconds: float | None
    status: str
    result: dict | None
    error: str | None


def _utc_now() -> str:
    return datetime.now(UTC).isoformat()


def _write_json(path: Path, payload: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp_path = path.with_name(f".{path.name}.tmp")
    tmp_path.write_text(json.dumps(payload, indent=2, ensure_ascii=True), encoding="utf-8")
    tmp_path.replace(path)


def _progress_payload(
    *,
    runs: list[FileRun],
    total_files: int,
    started_at: str,
    current_file: str | None,
    current_file_started_at: str | None,
    current_document_id: str | None,
    current_job_id: str | None,
    phase: str | None,
    phase_started_at: str | None,
    phase_detail: str | None,
    phase_metrics: dict[str, Any] | None,
    last_progress_at: str | None,
) -> dict:
    return {
        "started_at": started_at,
        "status": "running",
        "total_files": total_files,
        "completed_files": len(runs),
        "current_file": current_file,
        "current_file_started_at": current_file_started_at,
        "current_document_id": current_document_id,
        "current_job_id": current_job_id,
        "phase": phase,
        "phase_started_at": phase_started_at,
        "phase_detail": phase_detail,
        "phase_metrics": phase_metrics or {},
        "last_progress_at": last_progress_at or _utc_now(),
        "last_heartbeat_at": _utc_now(),
        "runs": [asdict(run) for run in runs],
    }


def _iter_pdf_paths(root: Path) -> list[Path]:
    if MANIFEST_PATH.exists():
        manifest = json.loads(MANIFEST_PATH.read_text(encoding="utf-8"))
        if not isinstance(manifest, list):
            raise ValueError("Reingest manifest must be a JSON list of relative PDF paths")
        selected: list[Path] = []
        seen: set[Path] = set()
        for item in manifest:
            if not isinstance(item, str) or not item.strip():
                raise ValueError("Reingest manifest entries must be non-empty strings")
            candidate = (root / item).resolve()
            try:
                candidate.relative_to(root.resolve())
            except ValueError as exc:
                raise ValueError(f"Manifest entry escapes corpus root: {item}") from exc
            if candidate in seen:
                continue
            if candidate.suffix.lower() != ".pdf":
                raise ValueError(f"Manifest entry is not a PDF: {item}")
            if not candidate.exists():
                raise ValueError(f"Manifest entry does not exist: {item}")
            seen.add(candidate)
            selected.append(candidate)
        return sorted(selected)
    pdfs: list[Path] = []
    for path in root.rglob("*.pdf"):
        if any(part in EXCLUDED_DIR_NAMES for part in path.parts):
            continue
        pdfs.append(path)
    return sorted(pdfs)


def _gate_ingest_result(path: Path, result: dict) -> str | None:
    chunks = int(result.get("chunks") or 0)
    accepted_chunks = int(result.get("accepted_chunks") or 0)
    pages = int(result.get("pages") or 0)
    kg_failures = list(result.get("kg_failures") or [])

    if chunks <= 0:
        return "gate_failed:no_chunks"
    if pages >= 3 and accepted_chunks <= 0:
        return "gate_failed:no_accepted_chunks"
    if accepted_chunks > 0 and len(kg_failures) > max(3, accepted_chunks // 2):
        return "gate_failed:kg_failure_spike"
    return None


def _source_for_path(path: Path) -> SourceDocument:
    filename = path.relative_to(APP_ROOT).as_posix()
    return SourceDocument(
        tenant_id="shared",
        source_type="pdf",
        filename=filename,
        raw_text="",
        metadata={"source_path": str(path)},
        document_class="book",
        content_hash_value=_build_pdf_content_hash(path),
    )


def _find_resume_checkpoint(service: IngestionService, pdfs: list[Path]) -> tuple[list[FileRun], int, dict[str, str] | None]:
    completed_runs: list[FileRun] = []
    checkpoint: dict[str, str] | None = None
    checkpoint_index = 0
    for index, path in enumerate(pdfs):
        source = _source_for_path(path)
        existing_document_id = service.repository.find_existing_document(source)
        if not existing_document_id:
            checkpoint_index = index
            break
        latest_job = service.repository.get_latest_job_for_document(existing_document_id)
        if latest_job is None:
            checkpoint_index = index
            break
        latest_status = str(latest_job.get("status") or "").strip().lower()
        if not is_terminal_job_status(latest_status):
            checkpoint = {
                "filename": source.filename,
                "document_id": str(existing_document_id),
                "job_id": str(latest_job["job_id"]),
                "status": latest_status,
            }
            checkpoint_index = index
            break
        if latest_status not in {"completed", "review"}:
            checkpoint_index = index
            break
        completed_runs.append(
            FileRun(
                filename=source.filename,
                started_at=_utc_now(),
                finished_at=_utc_now(),
                duration_seconds=None,
                status="ok",
                result={
                    "job_id": str(latest_job["job_id"]),
                    "document_id": str(existing_document_id),
                    "resumed_terminal": True,
                    "resume_from_status": latest_status,
                },
                error=None,
            )
        )
        checkpoint_index = index + 1
    return completed_runs, checkpoint_index, checkpoint


def main() -> None:
    pdfs = _iter_pdf_paths(APP_ROOT)
    started_at = _utc_now()
    heartbeat_state: dict[str, Any] = {
        "current_file": None,
        "current_file_started_at": None,
        "current_document_id": None,
        "current_job_id": None,
        "phase": None,
        "phase_started_at": None,
        "phase_detail": None,
        "phase_metrics": {},
        "last_progress_at": started_at,
    }

    runs: list[FileRun] = []

    def on_service_progress(update: dict[str, Any]) -> None:
        phase = str(update.get("phase") or heartbeat_state["phase"] or "").strip() or None
        if phase != heartbeat_state["phase"]:
            heartbeat_state["phase_started_at"] = _utc_now()
        heartbeat_state["phase"] = phase
        heartbeat_state["phase_detail"] = str(update.get("detail") or "").strip() or None
        heartbeat_state["phase_metrics"] = dict(update.get("metrics") or {})
        heartbeat_state["current_document_id"] = str(update.get("document_id") or heartbeat_state["current_document_id"] or "").strip() or None
        heartbeat_state["current_job_id"] = str(update.get("job_id") or heartbeat_state["current_job_id"] or "").strip() or None
        heartbeat_state["last_progress_at"] = _utc_now()

    service = IngestionService(progress_callback=on_service_progress)
    runs, start_index, checkpoint = _find_resume_checkpoint(service, pdfs)
    runs = list(runs)
    current_document_id = checkpoint["document_id"] if checkpoint else None
    current_job_id = checkpoint["job_id"] if checkpoint else None
    current_file = checkpoint["filename"] if checkpoint else (pdfs[start_index].relative_to(APP_ROOT).as_posix() if start_index < len(pdfs) else None)
    heartbeat_state["current_file"] = current_file
    heartbeat_state["current_file_started_at"] = _utc_now() if current_file else None
    heartbeat_state["current_document_id"] = current_document_id
    heartbeat_state["current_job_id"] = current_job_id
    heartbeat_state["phase"] = "resuming" if checkpoint else None
    heartbeat_state["phase_started_at"] = _utc_now() if checkpoint else None
    heartbeat_state["phase_detail"] = f"Resuming {checkpoint['filename']} from persisted job state." if checkpoint else None
    heartbeat_state["phase_metrics"] = {"resumed": bool(checkpoint)}
    heartbeat_state["last_progress_at"] = _utc_now()

    heartbeat_stop = threading.Event()

    def heartbeat_loop() -> None:
        while not heartbeat_stop.wait(HEARTBEAT_SECONDS):
            _write_json(
                PROGRESS_PATH,
                _progress_payload(
                    runs=runs,
                    total_files=len(pdfs),
                    started_at=started_at,
                    current_file=heartbeat_state["current_file"],
                    current_file_started_at=heartbeat_state["current_file_started_at"],
                    current_document_id=heartbeat_state["current_document_id"],
                    current_job_id=heartbeat_state["current_job_id"],
                    phase=heartbeat_state["phase"],
                    phase_started_at=heartbeat_state["phase_started_at"],
                    phase_detail=heartbeat_state["phase_detail"],
                    phase_metrics=heartbeat_state["phase_metrics"],
                    last_progress_at=heartbeat_state["last_progress_at"],
                ),
            )

    heartbeat_thread = threading.Thread(target=heartbeat_loop, name="continue-reingest-heartbeat", daemon=True)
    heartbeat_thread.start()

    _write_json(
        PROGRESS_PATH,
        _progress_payload(
            runs=runs,
            total_files=len(pdfs),
            started_at=started_at,
            current_file=current_file,
            current_file_started_at=_utc_now() if current_file else None,
            current_document_id=current_document_id,
            current_job_id=current_job_id,
            phase="resuming" if checkpoint else None,
            phase_started_at=_utc_now() if checkpoint else None,
            phase_detail=f"Resuming {checkpoint['filename']} from persisted job state." if checkpoint else None,
            phase_metrics={"resumed": bool(checkpoint)},
            last_progress_at=_utc_now(),
        ),
    )

    try:
        for index in range(start_index, len(pdfs)):
            path = pdfs[index]
            filename = path.relative_to(APP_ROOT).as_posix()
            file_started_at = _utc_now()
            started = time.time()
            heartbeat_state["current_file"] = filename
            heartbeat_state["current_file_started_at"] = file_started_at
            heartbeat_state["current_document_id"] = current_document_id
            heartbeat_state["current_job_id"] = current_job_id
            heartbeat_state["phase"] = "resuming" if checkpoint and checkpoint.get("filename") == filename else "starting"
            heartbeat_state["phase_started_at"] = file_started_at
            heartbeat_state["phase_detail"] = (
                f"Resuming {filename} from persisted job state."
                if checkpoint and checkpoint.get("filename") == filename
                else f"Registering ingest for {filename}."
            )
            heartbeat_state["phase_metrics"] = {"resumed": bool(checkpoint and checkpoint.get("filename") == filename)}
            heartbeat_state["last_progress_at"] = file_started_at
            _write_json(
                PROGRESS_PATH,
                _progress_payload(
                    runs=runs,
                    total_files=len(pdfs),
                    started_at=started_at,
                    current_file=filename,
                    current_file_started_at=file_started_at,
                    current_document_id=current_document_id,
                    current_job_id=current_job_id,
                    phase=heartbeat_state["phase"],
                    phase_started_at=heartbeat_state["phase_started_at"],
                    phase_detail=heartbeat_state["phase_detail"],
                    phase_metrics=heartbeat_state["phase_metrics"],
                    last_progress_at=file_started_at,
                ),
            )
            print(f"[{index + 1}/{len(pdfs)}] ingesting {filename}", flush=True)
            try:
                if checkpoint and checkpoint.get("filename") == filename:
                    result = service.resume_document_ingest(checkpoint["document_id"])
                    checkpoint = None
                else:
                    result = service.ingest_text(_source_for_path(path))
                gate_error = _gate_ingest_result(path, result)
                run = FileRun(
                    filename=filename,
                    started_at=file_started_at,
                    finished_at=_utc_now(),
                    duration_seconds=round(time.time() - started, 3),
                    status="ok" if gate_error is None else "error",
                    result=result,
                    error=gate_error,
                )
                print(
                    json.dumps(
                        {"file": filename, "status": run.status, "result": result, "gate_error": gate_error},
                        ensure_ascii=True,
                    ),
                    flush=True,
                )
            except Exception as exc:  # pragma: no cover - operational batch runner
                run = FileRun(
                    filename=filename,
                    started_at=file_started_at,
                    finished_at=_utc_now(),
                    duration_seconds=round(time.time() - started, 3),
                    status="error",
                    result=None,
                    error="".join(traceback.format_exception(exc)),
                )
                print(
                    json.dumps(
                        {"file": filename, "status": "error", "error": f"{type(exc).__name__}: {exc}"},
                        ensure_ascii=True,
                    ),
                    flush=True,
                )
            runs.append(run)
            current_document_id = None
            current_job_id = None
            heartbeat_state["current_file"] = None
            heartbeat_state["current_file_started_at"] = None
            heartbeat_state["current_document_id"] = None
            heartbeat_state["current_job_id"] = None
            heartbeat_state["phase"] = None
            heartbeat_state["phase_started_at"] = None
            heartbeat_state["phase_detail"] = None
            heartbeat_state["phase_metrics"] = {}
            heartbeat_state["last_progress_at"] = _utc_now()
            _write_json(
                PROGRESS_PATH,
                _progress_payload(
                    runs=runs,
                    total_files=len(pdfs),
                    started_at=started_at,
                    current_file=None,
                    current_file_started_at=None,
                    current_document_id=None,
                    current_job_id=None,
                    phase=None,
                    phase_started_at=None,
                    phase_detail=None,
                    phase_metrics={},
                    last_progress_at=heartbeat_state["last_progress_at"],
                ),
            )
            if run.status == "error" and not CONTINUE_ON_ERROR:
                break
    finally:
        heartbeat_stop.set()
        heartbeat_thread.join(timeout=1)

    error_files = sum(1 for run in runs if run.status == "error")
    stopped_early = bool(error_files and len(runs) < len(pdfs) and not CONTINUE_ON_ERROR)
    summary = {
        "started_at": started_at,
        "finished_at": _utc_now(),
        "status": "completed" if error_files == 0 else ("aborted" if stopped_early else "completed_with_errors"),
        "total_files": len(pdfs),
        "completed_files": len(runs),
        "attempted_files": len(runs),
        "remaining_files": max(0, len(pdfs) - len(runs)),
        "stopped_early": stopped_early,
        "ok_files": sum(1 for run in runs if run.status == "ok"),
        "error_files": error_files,
        "last_progress_at": _utc_now(),
        "last_heartbeat_at": _utc_now(),
        "runs": [asdict(run) for run in runs],
    }
    _write_json(PROGRESS_PATH, summary)
    _write_json(SUMMARY_PATH, summary)


if __name__ == "__main__":
    main()
