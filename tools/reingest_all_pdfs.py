from __future__ import annotations

import json
import os
import threading
import time
import traceback
from dataclasses import asdict, dataclass
from datetime import UTC, datetime
from pathlib import Path
import sys
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
LAUNCH_LOG_PATH = LOG_DIR / "reingest-launch.log"
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
STALE_PROGRESS_SECONDS = max(60, int(os.environ.get("REINGEST_STALE_PROGRESS_SECONDS", "900")))


@dataclass
class FileRun:
    filename: str
    started_at: str
    finished_at: str | None
    duration_seconds: float | None
    status: str
    result: dict | None
    error: str | None


@dataclass
class ResumeState:
    started_at: str | None
    runs: list[FileRun]
    current_file: str | None


def _aggregate_run_results(runs: list[FileRun], keys: list[str]) -> dict[str, int | float]:
    totals: dict[str, int | float] = {key: 0 for key in keys}
    for run in runs:
        result = run.result if isinstance(run.result, dict) else {}
        for key in keys:
            value = result.get(key)
            if isinstance(value, bool):
                totals[key] += int(value)
            elif isinstance(value, (int, float)):
                totals[key] += value
    return totals


def _utc_now() -> str:
    return datetime.now(UTC).isoformat()


def _write_json(path: Path, payload: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp_path = path.with_name(f".{path.name}.tmp")
    tmp_path.write_text(json.dumps(payload, indent=2, ensure_ascii=True), encoding="utf-8")
    tmp_path.replace(path)


def _mark_stale_progress_if_needed(path: Path) -> None:
    if not path.exists():
        return
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return
    if str(payload.get("status") or "").strip().lower() != "running":
        return
    heartbeat_text = str(
        payload.get("last_progress_at")
        or payload.get("last_heartbeat_at")
        or payload.get("finished_at")
        or payload.get("started_at")
        or ""
    ).strip()
    if not heartbeat_text:
        return
    try:
        heartbeat_at = datetime.fromisoformat(heartbeat_text.replace("Z", "+00:00"))
    except ValueError:
        return
    if heartbeat_at.tzinfo is None:
        heartbeat_at = heartbeat_at.replace(tzinfo=UTC)
    age_seconds = (datetime.now(UTC) - heartbeat_at).total_seconds()
    if age_seconds < STALE_PROGRESS_SECONDS:
        return
    payload["status"] = "aborted"
    payload["finished_at"] = _utc_now()
    payload["stale_detected"] = True
    payload["stale_age_seconds"] = round(age_seconds, 3)
    _write_json(path, payload)


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


def _parse_file_run(payload: dict[str, Any]) -> FileRun | None:
    filename = str(payload.get("filename") or payload.get("file") or "").strip()
    if not filename:
        return None
    started_at = str(payload.get("started_at") or "").strip() or _utc_now()
    finished_at = str(payload.get("finished_at") or "").strip() or None
    duration_seconds = payload.get("duration_seconds")
    if duration_seconds in ("", None):
        parsed_duration = None
    else:
        try:
            parsed_duration = float(duration_seconds)
        except (TypeError, ValueError):
            parsed_duration = None
    status = str(payload.get("status") or "error").strip() or "error"
    result = payload.get("result") if isinstance(payload.get("result"), dict) else None
    error = payload.get("error")
    if error is not None:
        error = str(error)
    return FileRun(
        filename=filename,
        started_at=started_at,
        finished_at=finished_at,
        duration_seconds=parsed_duration,
        status=status,
        result=result,
        error=error,
    )


def _load_resume_state_from_progress() -> ResumeState | None:
    if not PROGRESS_PATH.exists():
        return None
    try:
        payload = json.loads(PROGRESS_PATH.read_text(encoding="utf-8"))
    except Exception:
        return None
    if not isinstance(payload, dict):
        return None
    runs_payload = payload.get("runs")
    runs: list[FileRun] = []
    if isinstance(runs_payload, list):
        for item in runs_payload:
            if not isinstance(item, dict):
                continue
            parsed = _parse_file_run(item)
            if parsed is not None:
                runs.append(parsed)
    current_file = str(payload.get("current_file") or "").strip() or None
    started_at = str(payload.get("started_at") or "").strip() or None
    status = str(payload.get("status") or "").strip().lower()
    total_files = int(payload.get("total_files") or 0)
    completed_files = int(payload.get("completed_files") or len(runs))
    if status == "completed" and not current_file and total_files and completed_files >= total_files:
        return None
    if not runs and not current_file:
        return None
    return ResumeState(started_at=started_at, runs=runs, current_file=current_file)


def _load_resume_state_from_launch_log() -> ResumeState | None:
    if not LAUNCH_LOG_PATH.exists():
        return None
    runs: list[FileRun] = []
    current_file: str | None = None
    started_at = datetime.fromtimestamp(LAUNCH_LOG_PATH.stat().st_mtime, tz=UTC).isoformat()
    for raw_line in LAUNCH_LOG_PATH.read_text(encoding="utf-8", errors="replace").splitlines():
        line = raw_line.strip()
        if not line:
            continue
        if line.startswith("[") and "] ingesting " in line:
            _, _, suffix = line.partition("] ingesting ")
            filename = suffix.strip()
            if filename:
                current_file = filename
            continue
        if not line.startswith("{"):
            continue
        try:
            payload = json.loads(line)
        except Exception:
            continue
        if not isinstance(payload, dict):
            continue
        parsed = _parse_file_run(payload)
        if parsed is None:
            continue
        runs.append(parsed)
        if current_file == parsed.filename:
            current_file = None
    if not current_file:
        return None
    return ResumeState(started_at=started_at, runs=runs, current_file=current_file)


def _load_resume_state() -> ResumeState:
    progress_state = _load_resume_state_from_progress()
    if progress_state is not None:
        return progress_state
    launch_state = _load_resume_state_from_launch_log()
    if launch_state is not None:
        return launch_state
    return ResumeState(started_at=None, runs=[], current_file=None)


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
    page_assets = int(result.get("page_assets") or 0)
    indexed_assets = int(result.get("indexed_assets") or 0)
    kg_failures = list(result.get("kg_failures") or [])

    if chunks <= 0:
        return "gate_failed:no_chunks"
    if pages >= 3 and accepted_chunks <= 0:
        return "gate_failed:no_accepted_chunks"
    if accepted_chunks > 0 and len(kg_failures) > max(3, accepted_chunks // 2):
        return "gate_failed:kg_failure_spike"
    return None


def main() -> None:
    pdfs = _iter_pdf_paths(APP_ROOT)
    resume_state = _load_resume_state()
    runs: list[FileRun] = list(resume_state.runs)
    _mark_stale_progress_if_needed(PROGRESS_PATH)
    started_at = resume_state.started_at or _utc_now()
    completed_filenames = {run.filename for run in runs}
    resume_mode = bool(resume_state.runs or resume_state.current_file)
    heartbeat_stop = threading.Event()
    heartbeat_state = {
        "current_file": resume_state.current_file,
        "current_file_started_at": None,
        "current_document_id": None,
        "current_job_id": None,
        "phase": "resuming" if resume_mode else None,
        "phase_started_at": None,
        "phase_detail": f"Resuming from {resume_state.current_file}." if resume_state.current_file else None,
        "phase_metrics": {},
        "last_progress_at": started_at,
    }

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

    heartbeat_thread = threading.Thread(target=heartbeat_loop, name="reingest-heartbeat", daemon=True)
    heartbeat_thread.start()

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
            last_progress_at=started_at,
        ),
    )

    try:
        for index, path in enumerate(pdfs, start=1):
            filename = path.relative_to(APP_ROOT).as_posix()
            if filename in completed_filenames:
                continue
            file_started_at = _utc_now()
            started = time.time()
            heartbeat_state["current_file"] = filename
            heartbeat_state["current_file_started_at"] = file_started_at
            heartbeat_state["current_document_id"] = None
            heartbeat_state["current_job_id"] = None
            heartbeat_state["phase"] = "starting"
            heartbeat_state["phase_started_at"] = file_started_at
            heartbeat_state["phase_detail"] = f"Registering ingest for {filename}."
            heartbeat_state["phase_metrics"] = {}
            heartbeat_state["last_progress_at"] = file_started_at
            print(f"[{index}/{len(pdfs)}] ingesting {filename}", flush=True)
            _write_json(
                PROGRESS_PATH,
                _progress_payload(
                    runs=runs,
                    total_files=len(pdfs),
                    started_at=started_at,
                    current_file=filename,
                    current_file_started_at=file_started_at,
                    current_document_id=heartbeat_state["current_document_id"],
                    current_job_id=heartbeat_state["current_job_id"],
                    phase=heartbeat_state["phase"],
                    phase_started_at=heartbeat_state["phase_started_at"],
                    phase_detail=heartbeat_state["phase_detail"],
                    phase_metrics=heartbeat_state["phase_metrics"],
                    last_progress_at=heartbeat_state["last_progress_at"],
                ),
            )
            try:
                source = SourceDocument(
                    tenant_id="shared",
                    source_type="pdf",
                    filename=filename,
                    raw_text="",
                    metadata={"source_path": str(path)},
                    document_class="book",
                    content_hash_value=_build_pdf_content_hash(path),
                )
                resumable_document_id: str | None = None
                if resume_mode:
                    existing_document_id = service.repository.find_existing_document(source)
                    if existing_document_id:
                        latest_job = service.repository.get_latest_job_for_document(existing_document_id)
                        latest_status = str((latest_job or {}).get("status") or "").strip().lower()
                        if latest_job and not is_terminal_job_status(latest_status):
                            resumable_document_id = str(existing_document_id)
                if resumable_document_id:
                    heartbeat_state["phase"] = "resuming"
                    heartbeat_state["phase_started_at"] = _utc_now()
                    heartbeat_state["phase_detail"] = f"Resuming {filename} from persisted job state."
                    heartbeat_state["phase_metrics"] = {"resumed": True}
                    result = service.resume_document_ingest(resumable_document_id)
                else:
                    result = service.ingest_text(
                        source
                    )
                resume_mode = False
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
            completed_filenames.add(filename)
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
        heartbeat_thread.join(timeout=HEARTBEAT_SECONDS + 1)

    error_files = sum(1 for run in runs if run.status == "error")
    stopped_early = bool(error_files and len(runs) < len(pdfs) and not CONTINUE_ON_ERROR)
    aggregate_keys = [
        "blocks",
        "chunks",
        "accepted_chunks",
        "review_chunks",
        "rejected_chunks",
        "pages",
        "page_assets",
        "indexed_chunks",
        "indexed_assets",
        "kg_validated",
        "kg_review",
        "kg_skipped",
        "kg_quarantined",
        "kg_assertions",
        "kg_entities",
        "kg_evidence",
    ]
    aggregate_totals = _aggregate_run_results(runs, aggregate_keys)
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
        "documents_completed": sum(
            1
            for run in runs
            if run.status == "ok" and isinstance(run.result, dict) and not bool((run.result or {}).get("kg_failures"))
        ),
        "documents_in_review": sum(
            1
            for run in runs
            if isinstance(run.result, dict) and bool((run.result or {}).get("kg_failures"))
        ),
        "documents_failed": error_files,
        **aggregate_totals,
        "last_progress_at": heartbeat_state["last_progress_at"],
        "last_heartbeat_at": _utc_now(),
        "runs": [asdict(run) for run in runs],
    }
    _write_json(PROGRESS_PATH, summary)
    _write_json(SUMMARY_PATH, summary)
    print(json.dumps(summary, ensure_ascii=True), flush=True)
    if error_files:
        raise SystemExit(1)


if __name__ == "__main__":
    main()
