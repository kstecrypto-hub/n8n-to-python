import pytest

from src.bee_ingestion.pipeline import validate_job_transition, validate_stage_run


def test_valid_job_transition_path() -> None:
    validate_job_transition("registered", "content_available")
    validate_job_transition("content_available", "parsed")
    validate_job_transition("parsed", "chunked")
    validate_job_transition("chunked", "chunks_validated")
    validate_job_transition("chunks_validated", "indexed")
    validate_job_transition("indexed", "completed")


def test_invalid_job_transition_is_rejected() -> None:
    with pytest.raises(ValueError):
        validate_job_transition("registered", "indexed")


def test_terminal_job_transition_is_rejected() -> None:
    with pytest.raises(ValueError):
        validate_job_transition("completed", "chunked")


def test_invalid_stage_outcome_is_rejected() -> None:
    with pytest.raises(ValueError):
        validate_stage_run("parsed", "indexed")
