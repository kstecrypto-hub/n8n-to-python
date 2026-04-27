"""Document extraction, normalization, parsing, and chunk assembly helpers.

The pipeline here is deliberately staged:
- extract page-aware text from PDFs
- normalize noisy historical layout into cleaner blocks
- classify blocks into structural roles
- build atomic units that preserve heading context
- assemble retrieval chunks with stable provenance metadata
"""

from __future__ import annotations

import re
from dataclasses import dataclass
from hashlib import sha256

try:
    import fitz
except ImportError:  # pragma: no cover - fallback for environments without PyMuPDF
    fitz = None

from pypdf import PdfReader

from src.bee_ingestion.models import Chunk, ParsedBlock

PAGE_BREAK = "\f"
PAGE_MARKER_RE = re.compile(r"^\[\[page\s+(\d+)\]\]$", re.IGNORECASE)

FRONT_MATTER_MARKERS = (
    "cornell university library",
    "there are no known copyright restrictions",
    "everett franklin phillips",
    "albert r. mann",
    "http://www.archive.org",
    "copyright",
)

BACK_MATTER_MARKERS = (
    "great reductions in this catalogue",
    "messrs w. h. allen",
    "w. h. allen & co.",
)

LEADING_SECTION_MARKERS = (
    "introduction",
    "preface",
    "contents",
    "synopsis of contents",
    "list of illustrations",
)


@dataclass(slots=True)
class AtomicUnit:
    """Smallest semantic unit the chunker merges while preserving heading/page provenance."""
    unit_id: str
    document_id: str
    page_start: int | None
    page_end: int | None
    section_path: list[str]
    unit_type: str
    char_start: int
    char_end: int
    text: str
    block_ids: list[str]
    block_types: list[str]
    heading_context: list[str]


def sanitize_text(text: str) -> str:
    # Shared low-level cleanup used before normalization and before persistence.
    if not text:
        return ""
    text = text.replace("\x00", "")
    return text


def extract_pdf_text(path: str, page_start: int | None = None, page_end: int | None = None) -> str:
    if fitz is not None:
        return _extract_pdf_text_pymupdf(path, page_start=page_start, page_end=page_end)
    return _extract_pdf_text_pypdf(path, page_start=page_start, page_end=page_end)


def _extract_pdf_text_pymupdf(path: str, page_start: int | None = None, page_end: int | None = None) -> str:
    document = fitz.open(path)
    try:
        parts: list[str] = []
        start_index, end_index = _resolve_page_bounds(document.page_count, page_start, page_end)
        for page_index in range(start_index, end_index + 1):
            page = document.load_page(page_index)
            blocks = page.get_text("blocks", sort=True)
            page_blocks: list[str] = []
            for block in blocks:
                text = (block[4] or "").strip()
                if not text:
                    continue
                page_blocks.append(sanitize_text(text))
            page_body = sanitize_text("\n\n".join(page_blocks).strip())
            if page_body:
                parts.append(f"[[Page {page_index + 1}]]\n\n{page_body}")
        return sanitize_text(PAGE_BREAK.join(part for part in parts if part).strip())
    finally:
        document.close()


def _extract_pdf_text_pypdf(path: str, page_start: int | None = None, page_end: int | None = None) -> str:
    reader = PdfReader(path)
    parts: list[str] = []
    start_index, end_index = _resolve_page_bounds(len(reader.pages), page_start, page_end)
    for page_index in range(start_index, end_index + 1):
        page = reader.pages[page_index]
        page_text = sanitize_text(page.extract_text() or "")
        page_body = page_text.strip()
        if page_body:
            parts.append(f"[[Page {page_index + 1}]]\n\n{page_body}")
    return sanitize_text(PAGE_BREAK.join(parts).strip())


def _resolve_page_bounds(total_pages: int, page_start: int | None, page_end: int | None) -> tuple[int, int]:
    if total_pages <= 0:
        return 0, -1
    if page_start is None:
        page_start = 1
    if page_end is None:
        page_end = total_pages
    if page_start < 1 or page_end < 1:
        raise ValueError("page_start and page_end must be positive")
    if page_start > page_end:
        raise ValueError("page_start cannot be greater than page_end")
    start_index = max(0, page_start - 1)
    end_index = min(total_pages - 1, page_end - 1)
    if start_index > end_index:
        raise ValueError("Requested page range is outside the document")
    return start_index, end_index


def normalize_text(text: str) -> str:
    # Normalize one page at a time so page-local structure survives into parsing and
    # later provenance remains meaningful.
    text = sanitize_text(text)
    pages = text.split(PAGE_BREAK) if PAGE_BREAK in text else [text]
    normalized_pages = [_normalize_page_text(page) for page in pages]
    return PAGE_BREAK.join(page for page in normalized_pages if page).strip()


def build_extraction_metrics(raw_text: str, normalized_text: str) -> dict[str, int | float]:
    raw_pages = raw_text.split(PAGE_BREAK) if PAGE_BREAK in raw_text else [raw_text]
    normalized_pages = normalized_text.split(PAGE_BREAK) if PAGE_BREAK in normalized_text else [normalized_text]
    raw_lengths = [len(page.strip()) for page in raw_pages]
    normalized_lengths = [len(page.strip()) for page in normalized_pages]
    empty_pages = sum(1 for length in normalized_lengths if length == 0)
    suspicious_pages = sum(1 for page in normalized_pages if _page_word_count(page) <= 10 and len(page.strip()) > 0)
    return {
        "raw_pages": len(raw_pages),
        "normalized_pages": len(normalized_pages),
        "raw_chars": len(raw_text),
        "normalized_chars": len(normalized_text),
        "empty_pages": empty_pages,
        "suspicious_pages": suspicious_pages,
        "front_matter_marker_hits": sum(normalized_text.lower().count(marker) for marker in FRONT_MATTER_MARKERS),
        "back_matter_marker_hits": sum(normalized_text.lower().count(marker) for marker in BACK_MATTER_MARKERS),
        "contents_marker_hits": sum(normalized_text.lower().count(marker) for marker in ("contents", "synopsis of contents", "list of illustrations")),
        "avg_raw_chars_per_page": round(sum(raw_lengths) / max(len(raw_lengths), 1), 2),
        "avg_normalized_chars_per_page": round(sum(normalized_lengths) / max(len(normalized_lengths), 1), 2),
        "compression_ratio": round(len(normalized_text) / max(len(raw_text), 1), 4),
    }


def _page_word_count(text: str) -> int:
    return len(re.findall(r"\w+", text))


def _normalize_page_text(text: str) -> str:
    text = sanitize_text(text)
    lines = [line.rstrip() for line in text.replace("\r\n", "\n").replace("\r", "\n").split("\n")]
    blocks: list[str] = []
    current: list[str] = []

    def flush_current() -> None:
        nonlocal current
        if current:
            blocks.append(" ".join(current).strip())
            current = []

    for raw_line in lines:
        line = _normalize_line(raw_line)
        if not line:
            flush_current()
            continue

        if _is_heading_line(line):
            # Headings are emitted as stand-alone blocks so later stages can decide
            # whether to carry them into the following paragraph or keep them separate.
            flush_current()
            blocks.append(line)
            continue

        if current and current[-1].endswith("-"):
            current[-1] = f"{current[-1][:-1]}{line}"
        else:
            current.append(line)

    flush_current()
    return "\n\n".join(block for block in blocks if block)


def _normalize_line(text: str) -> str:
    text = sanitize_text(text).strip()
    text = text.replace("\u00ad", "")
    text = text.replace("ﬁ", "fi").replace("ﬂ", "fl")
    text = re.sub(r"(?<=[a-z])(?=[A-Z])", " ", text)
    text = re.sub(r"(?<=[A-Z])(?=[A-Z][a-z])", " ", text)
    text = re.sub(r"(?<=\d)(?=[A-Za-z])", " ", text)
    text = re.sub(r"(?<=[\.,;:!?])(?=[A-Za-z])", " ", text)
    text = re.sub(r"(?<=[A-Za-z])(?=[\(\[])"," ", text)
    text = re.sub(r"(?<=[\)\]])(?=[A-Za-z])"," ", text)
    text = re.sub(r"(?<=\")(?=[A-Za-z])", " ", text)
    text = re.sub(r"\s+", " ", text)
    return text


def _is_heading_line(line: str) -> bool:
    if len(line) > 120:
        return False
    if re.search(r"[.!?]\s*$", line) and len(line.split()) > 4:
        return False
    alpha = re.sub(r"[^A-Za-z]", "", line)
    if not alpha:
        return False
    if len(alpha) < 4:
        return False
    upper_ratio = sum(1 for char in alpha if char.isupper()) / max(len(alpha), 1)
    if upper_ratio > 0.8:
        return True
    words = re.findall(r"[A-Za-z][A-Za-z'\-]*", line)
    if not words or len(words) > 10:
        return False
    if any(char.isdigit() for char in line):
        return False
    title_like = sum(1 for word in words if word[:1].isupper()) / max(len(words), 1)
    return title_like >= 0.8 and not re.search(r"[,;:]", line)


def _looks_like_contents_block(text: str) -> bool:
    lowered = text.lower().strip()
    if lowered.startswith(("contents", "synopsis of contents", "list of illustrations")):
        return True
    words = re.findall(r"\w+", text)
    if len(words) >= 60 and re.search(r"[.!?]", text):
        return False
    numbered_entries = len(re.findall(r"\b\d+\.\s+[A-Za-z]", text))
    dot_leaders = len(re.findall(r"\.{2,}\s*\d{1,4}\b", text))
    roman_entries = len(re.findall(r"\b[ivxlcdm]+\.\s+[A-Za-z]", lowered))
    short_lines = len([part for part in re.split(r"\n+|(?<=\.)\s{2,}", text) if len(part.split()) <= 12 and part.strip()])
    return (
        numbered_entries >= 5
        or dot_leaders >= 3
        or roman_entries >= 5
        or ((numbered_entries >= 3 or roman_entries >= 3) and short_lines >= 4 and len(words) <= 120)
    )


def _looks_like_index_block(text: str) -> bool:
    lowered = text.lower().strip()
    if lowered.startswith("index"):
        return True
    page_refs = len(re.findall(r"\b\d{1,4}\b", text))
    commas = text.count(",")
    return page_refs >= 5 and commas >= 3 and len(text.split()) <= 120


def _classify_block(text: str, page_number: int | None = None) -> str:
    lowered = text.lower()
    if any(marker in lowered for marker in FRONT_MATTER_MARKERS):
        return "front_matter"
    if any(marker in lowered for marker in BACK_MATTER_MARKERS):
        return "back_matter"
    if page_number is not None and page_number > 200 and "catalogue" in lowered:
        return "back_matter"
    if _looks_like_contents_block(text):
        return "contents"
    if _looks_like_index_block(text):
        return "back_matter"
    if _is_heading_line(text):
        return "heading"
    if re.search(r"^\s*[-*]\s+", text):
        return "list"
    return "paragraph"


def _heading_level(text: str, page_number: int | None) -> int:
    raw = re.sub(r"\s+", " ", text).strip(" .:-")
    cleaned = _normalize_heading(text)
    word_count = len(re.findall(r"\w+", cleaned))
    if raw.isupper() and word_count <= 5:
        return 1 if page_number is not None and page_number <= 20 else 2
    if cleaned.istitle() and word_count <= 6:
        return 2
    return 3


def _page_role_for_blocks(page_blocks: list[str], page_number: int, document_class: str) -> str | None:
    if not page_blocks:
        return None
    lowered_page = " ".join(page_blocks).lower()
    if any(marker in lowered_page for marker in FRONT_MATTER_MARKERS):
        return "front_matter"
    if any(marker in lowered_page for marker in BACK_MATTER_MARKERS) or (page_number > 220 and "catalogue" in lowered_page):
        return "back_matter"

    short_blocks = sum(1 for block in page_blocks if len(block.split()) <= 12)
    heading_like = sum(1 for block in page_blocks if _is_heading_line(block))
    contents_like = sum(1 for block in page_blocks if _looks_like_contents_block(block) or _looks_like_index_block(block))
    long_paragraphs = sum(1 for block in page_blocks if len(block.split()) >= 35 and not _looks_like_contents_block(block))
    page_words = len(re.findall(r"\w+", " ".join(page_blocks)))
    starts_with_contents = page_blocks[0].lower().startswith(("contents", "synopsis of contents", "list of illustrations"))

    # Page-level role detection is intentionally conservative because mislabeling a
    # prose page as contents/front matter causes avoidable review or rejection later.
    if starts_with_contents and contents_like >= 1 and page_number <= 40:
        return "contents"
    if page_number <= 40 and contents_like >= 3 and long_paragraphs == 0 and page_words <= 220:
        return "contents"
    if (
        page_number <= 30
        and len(page_blocks) >= 7
        and short_blocks >= max(5, (len(page_blocks) * 2) // 3)
        and heading_like >= 4
        and long_paragraphs == 0
        and page_words <= 180
    ):
        return "contents"
    return None


def parse_text(document_id: str, text: str, document_class: str = "note") -> list[ParsedBlock]:
    blocks: list[ParsedBlock] = []
    cursor = 0
    section_path: list[str] = []
    hierarchy: dict[int, str] = {}

    pages = text.split(PAGE_BREAK) if PAGE_BREAK in text else [text]
    for page_index, raw_page in enumerate(pages, start=1):
        raw_blocks = raw_page.split("\n\n")
        page_blocks = [block.strip() for block in raw_blocks if block.strip()]
        actual_page_number = page_index
        if page_blocks:
            marker_match = PAGE_MARKER_RE.fullmatch(page_blocks[0])
            if marker_match:
                actual_page_number = int(marker_match.group(1))
                page_blocks = page_blocks[1:]
        page_role = _page_role_for_blocks(page_blocks, actual_page_number, document_class)
        for index, raw_block in enumerate(raw_blocks):
            block_text = raw_block.strip()
            if not block_text:
                cursor += len(raw_block) + 2
                continue
            marker_match = PAGE_MARKER_RE.fullmatch(block_text)
            if marker_match:
                start = text.find(raw_block, cursor)
                cursor = start + len(raw_block) + 2
                continue

            block_type = _classify_block(block_text, actual_page_number)
            if page_role in {"front_matter", "contents", "back_matter"} and block_type in {"heading", "paragraph", "list"}:
                # Page-level classification can override otherwise normal-looking blocks
                # when the whole page is strongly identified as non-body material.
                block_type = page_role
            section_hint = _detect_leading_section(block_text)
            if block_type == "heading":
                heading = _normalize_heading(block_text)
                level = _heading_level(block_text, actual_page_number)
                hierarchy[level] = heading
                for extra_level in [key for key in hierarchy if key > level]:
                    hierarchy.pop(extra_level)
                section_path = [hierarchy[key] for key in sorted(hierarchy)]
                block_text = heading
            elif section_hint:
                hierarchy[2] = section_hint
                for extra_level in [key for key in hierarchy if key > 2]:
                    hierarchy.pop(extra_level)
                section_path = [hierarchy[key] for key in sorted(hierarchy)]

            start = text.find(raw_block, cursor)
            end = start + len(raw_block)
            blocks.append(
                ParsedBlock(
                    block_id=f"{document_id}:p{actual_page_number:04d}:block:{len(blocks)}",
                    document_id=document_id,
                    page=actual_page_number,
                    section_path=section_path.copy(),
                    block_type=block_type,
                    char_start=start,
                    char_end=end,
                    text=block_text,
                )
            )
            cursor = end + 2
    return blocks


def _normalize_heading(text: str) -> str:
    cleaned = re.sub(r"\s+", " ", text).strip(" .:-")
    if cleaned.isupper():
        return cleaned.title()
    return cleaned


def _detect_leading_section(text: str) -> str | None:
    lowered = text.lower()
    for marker in LEADING_SECTION_MARKERS:
        if lowered.startswith(marker):
            return marker.title()
    return None


def build_atomic_units(document_id: str, blocks: list[ParsedBlock]) -> list[AtomicUnit]:
    # Atomic units let us attach a heading to the paragraph it governs without losing the original block ids.
    units: list[AtomicUnit] = []
    pending_heading: ParsedBlock | None = None

    for block in blocks:
        if block.block_type == "heading":
            # Hold headings briefly so they can be merged into the next semantic unit
            # when the following block belongs to the same section.
            if pending_heading is not None:
                units.append(
                    AtomicUnit(
                        unit_id=f"{document_id}:unit:{len(units)}",
                        document_id=document_id,
                        page_start=pending_heading.page,
                        page_end=pending_heading.page,
                        section_path=list(pending_heading.section_path),
                        unit_type="heading",
                        char_start=pending_heading.char_start,
                        char_end=pending_heading.char_end,
                        text=pending_heading.text,
                        block_ids=[pending_heading.block_id],
                        block_types=[pending_heading.block_type],
                        heading_context=[pending_heading.text],
                    )
                )
            pending_heading = block
            continue

        text_parts: list[str] = []
        heading_context: list[str] = []
        block_ids = [block.block_id]
        block_types = [block.block_type]
        char_start = block.char_start

        if pending_heading is not None and pending_heading.section_path == block.section_path:
            text_parts.append(pending_heading.text)
            heading_context.append(pending_heading.text)
            block_ids.insert(0, pending_heading.block_id)
            block_types.insert(0, pending_heading.block_type)
            char_start = pending_heading.char_start
            pending_heading = None

        text_parts.append(block.text)
        units.append(
            AtomicUnit(
                unit_id=f"{document_id}:unit:{len(units)}",
                document_id=document_id,
                page_start=block.page,
                page_end=block.page,
                section_path=list(block.section_path),
                unit_type=block.block_type,
                char_start=char_start,
                char_end=block.char_end,
                text="\n\n".join(part for part in text_parts if part).strip(),
                block_ids=block_ids,
                block_types=block_types,
                heading_context=heading_context,
            )
        )

    if pending_heading is not None:
        units.append(
            AtomicUnit(
                unit_id=f"{document_id}:unit:{len(units)}",
                document_id=document_id,
                page_start=pending_heading.page,
                page_end=pending_heading.page,
                section_path=list(pending_heading.section_path),
                unit_type="heading",
                char_start=pending_heading.char_start,
                char_end=pending_heading.char_end,
                text=pending_heading.text,
                block_ids=[pending_heading.block_id],
                block_types=[pending_heading.block_type],
                heading_context=[pending_heading.text],
            )
        )

    return units


def build_chunks(
    document_id: str,
    tenant_id: str,
    blocks: list[ParsedBlock],
    parser_version: str = "v1",
    chunker_version: str = "v1",
    target_chars: int = 900,
    min_chars: int = 300,
    document_class: str = "note",
    filename: str | None = None,
) -> list[Chunk]:
    # Chunking operates on atomic units so headings and paragraphs stay together while ids/provenance remain stable.
    # Chunk assembly works from atomic units, not raw parsed blocks, so paragraph
    # bodies can inherit local heading context without duplicating provenance logic.
    atomic_units = build_atomic_units(document_id=document_id, blocks=blocks)
    chunks: list[Chunk] = []
    pending: list[AtomicUnit] = []
    index = 0
    title = _resolve_document_title(blocks, filename)

    def flush_pending() -> None:
        nonlocal index, pending
        if not pending:
            return

        text = "\n\n".join(unit.text for unit in pending).strip()
        if not text:
            pending = []
            return

        first = pending[0]
        last = pending[-1]
        roles = sorted({block_type for unit in pending for block_type in unit.block_types})
        section_path = last.section_path or first.section_path
        chunk_role = _resolve_chunk_role(roles, first.page_start, text, document_class)
        section_title = _resolve_section_title(section_path, text)
        digest = sha256(
            f"{document_id}|{first.char_start}|{last.char_end}|{text}".encode("utf-8")
        ).hexdigest()[:16]
        chunk_id = f"{document_id}:c{index:04d}:{digest}"
        prev_chunk_id = chunks[-1].chunk_id if chunks else None
        token_count = len(re.findall(r"\w+", text))
        surface_terms = _surface_terms(text, section_path)
        canonical_terms = _canonical_terms(surface_terms)
        # Every chunk carries enough metadata to reconstruct where it came from and
        # how it should behave in retrieval and operator tooling.
        chunk = Chunk(
            chunk_id=chunk_id,
            document_id=document_id,
            tenant_id=tenant_id,
            chunk_index=index,
            page_start=first.page_start,
            page_end=last.page_end,
            section_path=section_path,
            prev_chunk_id=prev_chunk_id,
            next_chunk_id=None,
            char_start=first.char_start,
            char_end=last.char_end,
            text=text,
            parser_version=parser_version,
            chunker_version=chunker_version,
            content_type="text",
            metadata={
                "chunk_role": chunk_role,
                "document_class": document_class,
                "filename": filename or "",
                "block_types": roles,
                "block_count": len(pending),
                "section_title": section_title,
                "section_heading": section_title,
                "hierarchy_path": section_path,
                "title": title,
                "language": "en",
                "token_count": token_count,
                "provenance_ref": f"{document_id}:{first.char_start}:{last.char_end}",
                "quality_flags": [chunk_role] if chunk_role != "body" else [],
                "surface_terms": surface_terms,
                "canonical_terms": canonical_terms,
                "atomic_unit_ids": [unit.unit_id for unit in pending],
                "source_block_ids": [block_id for unit in pending for block_id in unit.block_ids],
            },
        )
        if chunks:
            chunks[-1].next_chunk_id = chunk.chunk_id
        chunks.append(chunk)
        index += 1
        pending = []

    for unit in atomic_units:
        current_size = sum(len(item.text) for item in pending) + len(unit.text)
        if pending and (
            current_size > target_chars
            and sum(len(item.text) for item in pending) >= min_chars
            or _should_force_flush(pending, unit)
            or _semantic_boundary(pending[-1], unit)
        ):
            flush_pending()
        pending.append(unit)

    flush_pending()
    return chunks


def _should_force_flush(pending: list[AtomicUnit], next_unit: AtomicUnit) -> bool:
    existing_pages = {unit.page_start for unit in pending if unit.page_start is not None}
    return bool(existing_pages) and next_unit.page_start not in existing_pages and sum(len(unit.text) for unit in pending) >= 500


def _semantic_boundary(current_unit: AtomicUnit, next_unit: AtomicUnit) -> bool:
    if current_unit.section_path and next_unit.section_path and current_unit.section_path != next_unit.section_path:
        return True
    current_non_body = current_unit.unit_type in {"front_matter", "contents", "back_matter"}
    next_non_body = next_unit.unit_type in {"front_matter", "contents", "back_matter"}
    if current_non_body != next_non_body:
        return True
    return False


def _resolve_chunk_role(block_types: list[str], page_start: int | None, text: str, document_class: str) -> str:
    if "front_matter" in block_types:
        return "front_matter"
    if "back_matter" in block_types:
        return "back_matter"
    if "contents" in block_types:
        words = re.findall(r"\w+", text)
        prose_signals = len(words) >= 45 and len(re.findall(r"[.!?;:]", text)) >= 3
        if not prose_signals and _looks_like_contents_block(text):
            return "contents"
    lowered = text.lower().strip()
    if lowered.startswith(("contents", "synopsis of contents", "list of illustrations")) and _looks_like_contents_block(text):
        return "contents"
    if _looks_like_index_block(text):
        return "back_matter"
    if (
        document_class in {"book", "manual", "article", "research_paper"}
        and page_start is not None
        and page_start <= 20
        and lowered.startswith(("contents", "synopsis of contents"))
        and len(re.findall(r"\b\d+\.", text)) >= 4
    ):
        return "contents"
    return "body"


def _resolve_section_title(section_path: list[str], text: str) -> str:
    if section_path:
        return section_path[-1]
    detected = _detect_leading_section(text)
    if detected:
        return detected
    return ""


def _resolve_document_title(blocks: list[ParsedBlock], filename: str | None) -> str:
    for block in blocks[:10]:
        if block.block_type == "heading" and block.text:
            return block.text
    return filename or ""


def _surface_terms(text: str, section_path: list[str]) -> list[str]:
    terms = [term for term in section_path if term]
    phrases = re.findall(r"\b(?:[A-Z][a-z]+(?:\s+[A-Z][a-z]+)*)\b", text)
    for phrase in phrases[:6]:
        if phrase not in terms:
            terms.append(phrase)
    return terms[:8]


def _canonical_terms(surface_terms: list[str]) -> list[str]:
    canonical: list[str] = []
    for term in surface_terms:
        normalized = re.sub(r"[^a-z0-9]+", "_", term.lower()).strip("_")
        if normalized and normalized not in canonical:
            canonical.append(normalized)
    return canonical[:8]
