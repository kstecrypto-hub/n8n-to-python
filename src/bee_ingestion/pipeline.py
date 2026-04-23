from __future__ import annotations

TERMINAL_JOB_STATUSES = {"completed", "review", "failed", "quarantined"}

JOB_TRANSITIONS: dict[str, set[str]] = {
    "registered": {"processing", "content_available", "failed", "quarantined"},
    "processing": {"content_available", "failed", "quarantined"},
    "content_available": {"parsed", "failed", "quarantined"},
    "parsed": {"chunked", "failed", "quarantined"},
    "chunked": {"chunks_validated", "failed", "quarantined"},
    "chunks_validated": {"kg_validated", "indexed", "failed", "quarantined"},
    "kg_validated": {"indexed", "completed", "review", "failed", "quarantined"},
    "indexed": {"completed", "review", "failed", "quarantined"},
    "completed": set(),
    "review": set(),
    "failed": set(),
    "quarantined": set(),
}

STAGE_NAMES = {"content_available", "parsed", "chunked", "chunks_validated", "indexed", "kg_validated", "failed"}
STAGE_OUTCOMES = {"running", "completed", "review", "failed", "quarantined"}


def is_terminal_job_status(status: str) -> bool:
    return status in TERMINAL_JOB_STATUSES


def validate_job_transition(current: str, nxt: str) -> None:
    if nxt not in JOB_TRANSITIONS:
        raise ValueError(f"Unknown target job status '{nxt}'")
    if current not in JOB_TRANSITIONS:
        raise ValueError(f"Unknown current job status '{current}'")
    if nxt not in JOB_TRANSITIONS[current]:
        raise ValueError(f"Invalid job status transition '{current}' -> '{nxt}'")


def validate_stage_run(stage_name: str, stage_outcome: str) -> None:
    if stage_name not in STAGE_NAMES:
        raise ValueError(f"Unknown stage name '{stage_name}'")
    if stage_outcome not in STAGE_OUTCOMES:
        raise ValueError(f"Unknown stage outcome '{stage_outcome}'")
