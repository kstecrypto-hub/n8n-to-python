"""Admin workflows for worker-facing ingestion and maintenance operations.

This module owns operator-triggered ingestion controls, detached reingest
management, and maintenance actions that run against the offline pipeline. It
must not own HTTP routes or chat runtime behavior.
"""

from __future__ import annotations

from datetime import datetime, timezone
import json
import os
from pathlib import Path
import re
import signal
import subprocess
import sys
from typing import Any

from fastapi import HTTPException

from src.bee_ingestion.settings import workspace_root

PAGE_ASSET_ROOT = (workspace_root() / "data" / "page_assets").resolve()
APP_WORKSPACE_ROOT = Path("/app").resolve()
REINGEST_PROGRESS_PATH = (workspace_root() / "data" / "logs" / "reingest-progress.json").resolve()
REINGEST_LAUNCH_LOG_PATH = (workspace_root() / "data" / "logs" / "reingest-launch.log").resolve()
REINGEST_PID_PATH = (workspace_root() / "data" / "logs" / "reingest-runner.pid").resolve()
REINGEST_STALE_PROGRESS_SECONDS = max(60, int(os.environ.get("REINGEST_STALE_PROGRESS_SECONDS", "900")))


def _read_reingest_progress_payload() -> dict | None:
    if not REINGEST_PROGRESS_PATH.exists():
        return None
    try:
        payload = json.loads(REINGEST_PROGRESS_PATH.read_text(encoding="utf-8"))
    except Exception:
        return None
    return payload if isinstance(payload, dict) else None


def _read_reingest_pid() -> int | None:
    if not REINGEST_PID_PATH.exists():
        return None
    try:
        value = REINGEST_PID_PATH.read_text(encoding="utf-8").strip()
        pid = int(value)
    except Exception:
        return None
    return pid if pid > 0 else None


def _write_reingest_pid(pid: int) -> None:
    REINGEST_PID_PATH.parent.mkdir(parents=True, exist_ok=True)
    REINGEST_PID_PATH.write_text(str(int(pid)), encoding="utf-8")


def _clear_reingest_pid() -> None:
    REINGEST_PID_PATH.unlink(missing_ok=True)


def _iter_reingest_processes() -> list[dict[str, Any]]:
    processes: list[dict[str, Any]] = []
    proc_root = Path("/proc")
    if not proc_root.exists():
        return processes
    for entry in proc_root.iterdir():
        if not entry.name.isdigit():
            continue
        pid = int(entry.name)
        cmdline_path = entry / "cmdline"
        status_path = entry / "status"
        try:
            cmdline_raw = cmdline_path.read_bytes()
            status_text = status_path.read_text(encoding="utf-8", errors="ignore") if status_path.exists() else ""
        except OSError:
            continue
        if re.search(r"^State:\s+Z\b", status_text, flags=re.MULTILINE):
            continue
        cmdline = cmdline_raw.replace(b"\x00", b" ").decode("utf-8", "ignore").strip()
        if not cmdline:
            continue
        script_name = None
        if "tools/reingest_all_pdfs.py" in cmdline:
            script_name = "reingest_all_pdfs.py"
        elif "tools/continue_interrupted_reingest.py" in cmdline:
            script_name = "continue_interrupted_reingest.py"
        if not script_name:
            continue
        processes.append({"pid": pid, "cmdline": cmdline, "script": script_name})
    processes.sort(key=lambda item: item["pid"])
    return processes


def _is_reingest_process_alive(pid: int | None) -> bool:
    if not pid:
        return False
    status_path = Path(f"/proc/{pid}/status")
    if status_path.exists():
        try:
            status_text = status_path.read_text(encoding="utf-8", errors="ignore")
        except OSError:
            return False
        if re.search(r"^State:\s+Z\b", status_text, flags=re.MULTILINE):
            return False
    try:
        os.kill(pid, 0)
    except OSError:
        return False
    return True


def _current_reingest_pid() -> int | None:
    pid = _read_reingest_pid()
    if _is_reingest_process_alive(pid):
        return pid
    _clear_reingest_pid()
    processes = _iter_reingest_processes()
    if not processes:
        return None
    fallback_pid = int(processes[0]["pid"])
    _write_reingest_pid(fallback_pid)
    return fallback_pid


def _with_stale_reingest_status(progress: dict[str, Any] | None) -> dict[str, Any] | None:
    if progress is None or str(progress.get("status") or "").strip().lower() != "running":
        return progress
    heartbeat_text = str(
        progress.get("last_progress_at")
        or progress.get("last_heartbeat_at")
        or progress.get("finished_at")
        or progress.get("started_at")
        or ""
    ).strip()
    if not heartbeat_text:
        return progress
    try:
        heartbeat_at = datetime.fromisoformat(heartbeat_text.replace("Z", "+00:00"))
    except ValueError:
        return progress
    if heartbeat_at.tzinfo is None:
        heartbeat_at = heartbeat_at.replace(tzinfo=timezone.utc)
    age_seconds = (datetime.now(timezone.utc) - heartbeat_at).total_seconds()
    if age_seconds < REINGEST_STALE_PROGRESS_SECONDS:
        return progress
    updated = dict(progress)
    updated["status"] = "aborted"
    updated["stale_detected"] = True
    updated["stale_age_seconds"] = round(age_seconds, 3)
    updated.setdefault("finished_at", datetime.now(timezone.utc).isoformat())
    return updated


def _has_resumable_reingest_state(*, repository: Any) -> bool:
    progress = _with_stale_reingest_status(_read_reingest_progress_payload())
    if progress:
        status = str(progress.get("status") or "").strip().lower()
        total_files = int(progress.get("total_files") or 0)
        completed_files = int(progress.get("completed_files") or 0)
        current_file = str(progress.get("current_file") or "").strip()
        if current_file:
            return True
        if status in {"running", "aborted", "completed_with_errors"} and (not total_files or completed_files < total_files):
            return True
    active_documents = [
        row
        for row in repository.list_documents(limit=100, offset=0)
        if str(row.get("status") or "").strip().lower() not in {"completed", "failed", "review"}
    ]
    return bool(active_documents)


def _start_detached_reingest_runner(*, repository: Any, require_resume: bool = False) -> dict[str, Any]:
    current_pid = _current_reingest_pid()
    if current_pid:
        raise RuntimeError(f"Reingest runner already active (pid={current_pid})")
    REINGEST_LAUNCH_LOG_PATH.parent.mkdir(parents=True, exist_ok=True)
    resumable_state = _has_resumable_reingest_state(repository=repository)
    if require_resume and not resumable_state:
        raise RuntimeError("No resumable ingest state found")
    if not resumable_state:
        REINGEST_LAUNCH_LOG_PATH.unlink(missing_ok=True)
        REINGEST_PROGRESS_PATH.unlink(missing_ok=True)
    runner_script_name = "continue_interrupted_reingest.py" if resumable_state else "reingest_all_pdfs.py"
    runner_script = (APP_WORKSPACE_ROOT / "tools" / runner_script_name).resolve()
    if not runner_script.exists():
        raise RuntimeError("Reingest runner script not found")
    process_env = os.environ.copy()
    process_env["PYTHONPATH"] = str(APP_WORKSPACE_ROOT)
    with REINGEST_LAUNCH_LOG_PATH.open("ab") as log_handle:
        process = subprocess.Popen(
            [sys.executable, str(runner_script)],
            cwd=str(APP_WORKSPACE_ROOT),
            stdout=log_handle,
            stderr=subprocess.STDOUT,
            start_new_session=True,
            env=process_env,
        )
    _write_reingest_pid(process.pid)
    return {
        "pid": process.pid,
        "script": runner_script_name,
        "resumed": resumable_state,
        "launch_log": str(REINGEST_LAUNCH_LOG_PATH),
        "progress_file": str(REINGEST_PROGRESS_PATH),
    }


def _stop_detached_reingest_runner() -> dict[str, Any]:
    processes = _iter_reingest_processes()
    if not processes:
        _clear_reingest_pid()
        return {"stopped": False, "reason": "not_running"}
    stopped_pids: list[int] = []
    for process in processes:
        pid = int(process["pid"])
        try:
            os.kill(pid, signal.SIGTERM)
            stopped_pids.append(pid)
        except OSError:
            continue
    _clear_reingest_pid()
    if not stopped_pids:
        return {"stopped": False, "reason": "not_running"}
    return {"stopped": True, "pid": stopped_pids[0], "pids": stopped_pids}


def _infer_reingest_phase(
    *,
    progress: dict[str, Any] | None,
    primary_document: dict[str, Any] | None,
    recent_document_stages: list[dict[str, Any]],
    asset_file_count: int,
    latest_asset_write: dict[str, Any] | None,
) -> dict[str, Any]:
    phase_key = "idle"
    label = "Idle"
    detail = ""
    explicit_progress_phase = str((progress or {}).get("phase") or "").strip().lower()
    explicit_phase_detail = str((progress or {}).get("phase_detail") or "").strip()
    explicit_phase_metrics = dict((progress or {}).get("phase_metrics") or {})
    latest_stage = recent_document_stages[0] if recent_document_stages else None
    latest_stage_name = str(latest_stage.get("stage_name") or "").strip() if latest_stage else ""
    latest_stage_metrics = latest_stage.get("metrics_json") if isinstance(latest_stage, dict) else None
    document_status = str(primary_document.get("status") or "").strip() if primary_document else ""

    if progress and str(progress.get("status") or "").strip().lower() == "running":
        if explicit_progress_phase:
            phase_key = explicit_progress_phase
            label = {
                "starting": "Starting",
                "preparing": "Preparing",
                "parsing": "Parsing",
                "chunking": "Chunking",
                "validating": "Validating",
                "kg": "KG extraction",
                "embedding": "Embedding / indexing",
                "review": "Review",
                "failed": "Failed",
            }.get(explicit_progress_phase, explicit_progress_phase.replace("_", " ").title())
            detail = explicit_phase_detail
        elif document_status in {"completed", "review", "failed"}:
            phase_key = document_status
            label = {"completed": "Completed", "review": "Review", "failed": "Failed"}.get(document_status, document_status.title())
        elif document_status == "kg_validated":
            phase_key = "embedding"
            label = "Embedding / indexing"
            detail = "Generating vectors and publishing accepted chunks/assets."
        elif document_status == "chunks_validated" or latest_stage_name == "chunks_validated":
            phase_key = "kg"
            label = "KG extraction"
            accepted = int((latest_stage_metrics or {}).get("accepted") or primary_document.get("accepted_chunks") or 0)
            detail = f"Running KG extraction over accepted chunks ({accepted})."
        elif document_status == "chunked" or latest_stage_name == "chunked":
            phase_key = "validating"
            label = "Validating chunks"
            detail = "Scoring and classifying chunks into accepted/review/rejected."
        elif document_status == "parsed" or latest_stage_name == "parsed":
            phase_key = "chunking"
            label = "Chunking"
            detail = "Building persisted chunk records from parsed blocks."
        elif document_status == "content_available" or latest_stage_name == "content_available":
            phase_key = "parsing"
            label = "Parsing"
            detail = "Normalizing extracted text and generating structural blocks."
        elif asset_file_count > 0 or latest_asset_write:
            phase_key = "preparing"
            label = "Preparing"
            detail = "Rendering pages and extracting multimodal assets before parsing."
        else:
            phase_key = "starting"
            label = "Starting"
            detail = "Creating the document and preparing the first ingest pass."
    elif progress:
        status = str(progress.get("status") or "").strip().lower()
        if status:
            phase_key = status
            label = status.replace("_", " ").title()
    return {
        "key": phase_key,
        "label": label,
        "detail": detail,
        "metrics": explicit_phase_metrics,
        "document_status": document_status or None,
        "latest_stage_name": latest_stage_name or None,
        "latest_stage_status": str(latest_stage.get("status") or "").strip() if latest_stage else None,
    }


def build_reingest_activity_snapshot(*, repository: Any) -> dict:
    runner_processes = _iter_reingest_processes()
    runner_pid = _current_reingest_pid()
    raw_progress = _read_reingest_progress_payload()
    progress = raw_progress if runner_processes else _with_stale_reingest_status(raw_progress)
    snapshot: dict[str, Any] = {
        "progress": progress,
        "has_progress_file": progress is not None,
        "launch_log_exists": REINGEST_LAUNCH_LOG_PATH.exists(),
        "runner_pid": runner_pid,
        "runner_active": bool(runner_processes or runner_pid),
        "runner_processes": runner_processes,
    }
    active_documents = [row for row in repository.list_documents(limit=20, offset=0) if row.get("status") not in {"completed", "failed"}]
    if active_documents:
        snapshot["active_documents"] = active_documents[:5]
    primary_document = active_documents[0] if active_documents else None
    recent_document_stages: list[dict[str, Any]] = []
    if primary_document:
        recent_document_stages = repository.list_stage_runs(document_id=str(primary_document.get("document_id") or ""), limit=12, offset=0)
        snapshot["recent_document_stages"] = recent_document_stages
    asset_root = PAGE_ASSET_ROOT
    asset_files: list[Path] = []
    if active_documents:
        active_document_id = str(active_documents[0].get("document_id") or "").strip()
        if active_document_id:
            active_dir = asset_root / active_document_id
            if active_dir.exists():
                asset_files = [path for path in active_dir.rglob("*") if path.is_file()]
                snapshot["active_asset_directory"] = str(active_dir)
                snapshot["active_asset_document_id"] = active_document_id
    if not asset_files and asset_root.exists():
        asset_files = [path for path in asset_root.rglob("*") if path.is_file()]
    if asset_files:
        existing_asset_files: list[tuple[Path, os.stat_result]] = []
        for path in asset_files:
            try:
                stat_result = path.stat()
            except OSError:
                continue
            existing_asset_files.append((path, stat_result))
        existing_asset_files.sort(key=lambda item: item[1].st_mtime, reverse=True)
        if existing_asset_files:
            latest, latest_stat = existing_asset_files[0]
            snapshot["asset_file_count"] = len(existing_asset_files)
            snapshot["latest_asset_write"] = {
                "path": str(latest),
                "modified_at": datetime.fromtimestamp(latest_stat.st_mtime, tz=timezone.utc).isoformat(),
                "size_bytes": latest_stat.st_size,
            }
        else:
            snapshot["asset_file_count"] = 0
            snapshot["latest_asset_write"] = None
    else:
        snapshot["asset_file_count"] = 0
        snapshot["latest_asset_write"] = None
    snapshot["phase"] = _infer_reingest_phase(
        progress=progress,
        primary_document=primary_document,
        recent_document_stages=recent_document_stages,
        asset_file_count=int(snapshot.get("asset_file_count") or 0),
        latest_asset_write=snapshot.get("latest_asset_write"),
    )
    return snapshot


def start_reingest(*, repository: Any, require_resume: bool = False) -> dict:
    try:
        payload = _start_detached_reingest_runner(repository=repository, require_resume=require_resume)
    except RuntimeError as exc:
        raise HTTPException(status_code=409, detail=str(exc)) from exc
    snapshot = build_reingest_activity_snapshot(repository=repository)
    snapshot["timestamp"] = datetime.now(timezone.utc).isoformat()
    return {"ok": True, "started": True, "runner": payload, "snapshot": snapshot}


def stop_reingest(*, repository: Any) -> dict:
    payload = _stop_detached_reingest_runner()
    snapshot = build_reingest_activity_snapshot(repository=repository)
    snapshot["timestamp"] = datetime.now(timezone.utc).isoformat()
    return {"ok": True, **payload, "snapshot": snapshot}


def get_ingest_progress(*, repository: Any) -> dict:
    snapshot = build_reingest_activity_snapshot(repository=repository)
    snapshot["timestamp"] = datetime.now(timezone.utc).isoformat()
    return snapshot


def auto_review_chunks(*, service: Any, document_id: str | None, batch_size: int) -> dict:
    batch = max(1, min(batch_size, 500))
    return service.enqueue_maintenance_job(
        operation="auto_review_chunks",
        document_id=document_id,
        parameters={"batch_size": batch},
    )


def revalidate_document(*, service: Any, document_id: str, rerun_kg: bool) -> dict:
    try:
        return service.enqueue_maintenance_job(
            operation="revalidate_document",
            document_id=document_id,
            parameters={"rerun_kg": bool(rerun_kg)},
        )
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


def rebuild_document(*, service: Any, document_id: str) -> dict:
    try:
        return service.enqueue_maintenance_job(operation="rebuild_document", document_id=document_id)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


def reindex_document(*, service: Any, document_id: str) -> dict:
    try:
        return service.enqueue_maintenance_job(operation="reindex_document", document_id=document_id)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


def reprocess_document_kg(*, service: Any, document_id: str, batch_size: int) -> dict:
    batch = max(1, min(batch_size, 500))
    return service.enqueue_maintenance_job(
        operation="reprocess_kg",
        document_id=document_id,
        parameters={"batch_size": batch},
    )


def delete_document(*, service: Any, document_id: str) -> dict:
    try:
        return service.enqueue_maintenance_job(operation="delete_document", document_id=document_id)
    except ValueError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc


def reset_pipeline(*, service: Any) -> dict:
    _stop_detached_reingest_runner()
    return service.enqueue_maintenance_job(operation="reset_pipeline_data")
