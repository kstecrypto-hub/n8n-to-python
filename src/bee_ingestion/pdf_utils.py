from __future__ import annotations

from hashlib import sha256
from pathlib import Path


def build_pdf_content_hash(source_path: Path, page_start: int | None = None, page_end: int | None = None) -> str:
    hasher = sha256()
    with source_path.open("rb") as handle:
        while True:
            chunk = handle.read(1024 * 1024)
            if not chunk:
                break
            hasher.update(chunk)
    hasher.update(f"|page_start={page_start}|page_end={page_end}".encode("utf-8"))
    return f"sha256:{hasher.hexdigest()}"
