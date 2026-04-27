from __future__ import annotations

import json
import os
import shutil
import subprocess
import sys
import time
from datetime import UTC, datetime
from pathlib import Path
from urllib.error import URLError
from urllib.request import urlopen


ROOT = Path(r"E:\n8n to python")
LOG_DIR = ROOT / "data" / "logs"
PAGE_ASSETS_DIR = ROOT / "data" / "page_assets"
PROGRESS_PATH = LOG_DIR / "reingest-progress.json"
SUMMARY_SNAPSHOT_PATH = LOG_DIR / "pre-ontology-rerun-summary.json"
STATE_PATH = LOG_DIR / "ontology-rerun-state.json"
ORCHESTRATOR_LOG = LOG_DIR / "ontology-rerun-orchestrator.log"
LOCK_PATH = LOG_DIR / "ontology-rerun.lock"

API_CONTAINER = os.environ.get("API_CONTAINER", "n8ntopython-api-1")
POSTGRES_CONTAINER = os.environ.get("POSTGRES_CONTAINER", "n8ntopython-postgres-1")
HEALTH_URL = os.environ.get("API_HEALTH_URL", "http://localhost:38100/health")
POLL_SECONDS = int(os.environ.get("INGEST_POLL_SECONDS", "60"))
MAX_WAIT_SECONDS = int(os.environ.get("INGEST_WAIT_MAX_SECONDS", str(24 * 60 * 60)))
MAX_IDLE_PROGRESS_SECONDS = int(os.environ.get("INGEST_IDLE_PROGRESS_SECONDS", "1800"))


def _utc_now() -> str:
    return datetime.now(UTC).isoformat()


def _append_log(message: str) -> None:
    LOG_DIR.mkdir(parents=True, exist_ok=True)
    line = f"[{_utc_now()}] {message}\n"
    with ORCHESTRATOR_LOG.open("a", encoding="utf-8") as handle:
        handle.write(line)
    print(line.strip(), flush=True)


def _write_state(payload: dict) -> None:
    LOG_DIR.mkdir(parents=True, exist_ok=True)
    tmp_path = STATE_PATH.with_name(f".{STATE_PATH.name}.tmp")
    tmp_path.write_text(json.dumps(payload, indent=2, ensure_ascii=True), encoding="utf-8")
    tmp_path.replace(STATE_PATH)


def _run(command: list[str], input_text: str | None = None) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        command,
        input=input_text,
        text=True,
        capture_output=True,
        cwd=str(ROOT),
        check=True,
    )


def _read_progress() -> dict | None:
    if not PROGRESS_PATH.exists():
        return None
    try:
        return json.loads(PROGRESS_PATH.read_text(encoding="utf-8"))
    except json.JSONDecodeError:
        return None


def _wait_for_current_ingest() -> dict:
    _append_log("Waiting for current detached re-ingest to reach a terminal status.")
    started_wait = time.time()
    while True:
        progress = _read_progress()
        if progress and str(progress.get("status") or "") == "completed":
            SUMMARY_SNAPSHOT_PATH.write_text(json.dumps(progress, indent=2, ensure_ascii=True), encoding="utf-8")
            _append_log(
                f"Detected terminal ingest status={progress.get('status')} completed_files={progress.get('completed_files')}/{progress.get('total_files')}."
            )
            return progress
        if progress and str(progress.get("status") or "") == "completed_with_errors":
            SUMMARY_SNAPSHOT_PATH.write_text(json.dumps(progress, indent=2, ensure_ascii=True), encoding="utf-8")
            raise RuntimeError(
                f"Current ingest completed with errors ({progress.get('error_files')}/{progress.get('total_files')}); inspect before rerun."
            )
        if time.time() - started_wait > MAX_WAIT_SECONDS:
            raise RuntimeError("Timed out waiting for the current ingest to reach a terminal status.")
        if PROGRESS_PATH.exists() and (time.time() - PROGRESS_PATH.stat().st_mtime) > MAX_IDLE_PROGRESS_SECONDS:
            raise RuntimeError("Progress file has gone stale while waiting for the current ingest.")
        time.sleep(POLL_SECONDS)


def _reset_ingestion_scope() -> None:
    _append_log("Resetting approved ingestion/KG/session/profile tables in Postgres.")
    sql = """
TRUNCATE TABLE
  kg_assertion_evidence,
  kg_assertions,
  kg_raw_extractions,
  kg_entities,
  chunk_asset_links,
  chunk_review_runs,
  chunk_validations,
  document_chunks,
  parsed_blocks,
  page_assets,
  document_pages,
  document_sources,
  ingestion_stage_runs,
  ingestion_jobs,
  documents,
  agent_answer_reviews,
  agent_query_sources,
  agent_query_runs,
  agent_messages,
  agent_session_memories,
  agent_sessions,
  agent_query_patterns,
  agent_profiles
RESTART IDENTITY CASCADE;
"""
    _run(["docker", "exec", "-i", POSTGRES_CONTAINER, "psql", "-U", "bee", "-d", "bee_ingestion"], input_text=sql)

    _append_log("Resetting Chroma chunk and asset collections.")
    _run(
        [
            "docker",
            "exec",
            API_CONTAINER,
            "python",
            "-c",
            (
                "from src.bee_ingestion.chroma_store import ChromaStore; "
                "s=ChromaStore(); s.reset_collection(); "
                "print({'chunk_count': s.collection.count(), 'asset_count': s.asset_collection.count()})"
            ),
        ]
    )

    _append_log("Clearing derived page_assets files on disk.")
    PAGE_ASSETS_DIR.mkdir(parents=True, exist_ok=True)
    for child in list(PAGE_ASSETS_DIR.iterdir()):
        if child.is_dir():
            shutil.rmtree(child)
        else:
            child.unlink()


def _wait_for_api() -> None:
    _append_log("Waiting for API health after restart.")
    deadline = time.time() + 300
    while time.time() < deadline:
        try:
            with urlopen(HEALTH_URL, timeout=5) as response:
                if response.status == 200:
                    _append_log("API health check passed.")
                    return
        except URLError:
            pass
        time.sleep(5)
    raise RuntimeError("API health check did not recover after restart.")


def _restart_api_container() -> None:
    _append_log("Restarting API container so the updated ontology is loaded into memory.")
    _run(["docker", "restart", API_CONTAINER])
    _wait_for_api()


def _start_fresh_reingest() -> None:
    _append_log("Launching fresh detached re-ingest under the updated live ontology.")
    launch_started_at = _utc_now()
    if PROGRESS_PATH.exists():
        PROGRESS_PATH.unlink()
    _run(
        [
            "docker",
            "exec",
            "-d",
            API_CONTAINER,
            "sh",
            "-lc",
            "cd /app && mkdir -p data/logs && python tools/reingest_all_pdfs.py > data/logs/reingest-ontology-rerun.log 2>&1",
        ]
    )
    _wait_for_new_ingest_start(launch_started_at)


def _wait_for_new_ingest_start(launch_started_at: str) -> None:
    _append_log("Waiting for the detached re-ingest to publish fresh progress.")
    deadline = time.time() + 120
    while time.time() < deadline:
        progress = _read_progress()
        if progress and str(progress.get("started_at") or "") >= launch_started_at and str(progress.get("status") or "") == "running":
            _append_log(
                f"Fresh ingest progress detected: completed_files={progress.get('completed_files')}/{progress.get('total_files')} current_file={progress.get('current_file')}."
            )
            return
        time.sleep(2)
    raise RuntimeError("Detached re-ingest did not publish fresh progress after relaunch.")


def _acquire_lock() -> None:
    LOG_DIR.mkdir(parents=True, exist_ok=True)
    try:
        fd = os.open(str(LOCK_PATH), os.O_CREAT | os.O_EXCL | os.O_WRONLY)
    except FileExistsError as exc:
        raise RuntimeError("Ontology rerun watcher is already active") from exc
    with os.fdopen(fd, "w", encoding="utf-8") as handle:
        handle.write(_utc_now())


def _release_lock() -> None:
    try:
        LOCK_PATH.unlink()
    except FileNotFoundError:
        return


def main() -> None:
    _acquire_lock()
    state = {
        "started_at": _utc_now(),
        "status": "waiting_for_current_ingest",
    }
    _write_state(state)
    try:
        finished = _wait_for_current_ingest()
        state["current_ingest_summary"] = finished
        state["status"] = "resetting"
        _write_state(state)

        _reset_ingestion_scope()

        state["status"] = "restarting_api"
        _write_state(state)
        _restart_api_container()

        state["status"] = "starting_new_ingest"
        _write_state(state)
        _start_fresh_reingest()

        state["status"] = "completed"
        state["finished_at"] = _utc_now()
        _write_state(state)
        _append_log("Watcher workflow completed. Fresh ontology-aware re-ingest has been started.")
    except Exception as exc:  # pragma: no cover - operational script
        state["status"] = "failed"
        state["finished_at"] = _utc_now()
        state["error"] = f"{type(exc).__name__}: {exc}"
        _write_state(state)
        _append_log(f"Watcher workflow failed: {type(exc).__name__}: {exc}")
        raise
    finally:
        _release_lock()


if __name__ == "__main__":
    main()
