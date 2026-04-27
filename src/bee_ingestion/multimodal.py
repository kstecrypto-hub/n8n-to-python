"""PDF page/image extraction and vision enrichment helpers.

This module keeps multimodal ingestion additive:
- extract page images and embedded-image crops from PDFs
- optionally enrich low-text pages and image assets with OCR/vision descriptions
- produce structured page/asset records plus merged page text for the existing parser
"""

from __future__ import annotations

import base64
import json
import time
from dataclasses import dataclass
from hashlib import sha256
from pathlib import Path
import re
from typing import Any, Callable

import httpx

from src.bee_ingestion.chunking import PAGE_BREAK, _resolve_page_bounds, sanitize_text
from src.bee_ingestion.models import DocumentPage, PageAsset
from src.bee_ingestion.settings import settings

try:
    import fitz
except ImportError:  # pragma: no cover - PyMuPDF is present in the runtime container
    fitz = None


@dataclass(slots=True)
class MultimodalPDFPayload:
    """Structured multimodal PDF extraction result persisted before chunking begins."""
    pages: list[DocumentPage]
    assets: list[PageAsset]
    merged_text: str
    metrics: dict[str, Any]


def extract_pdf_multimodal_payload(
    document_id: str,
    tenant_id: str,
    path: str,
    filename: str,
    page_start: int | None = None,
    page_end: int | None = None,
    progress_callback: Callable[[dict[str, Any]], None] | None = None,
) -> MultimodalPDFPayload:
    if fitz is None:  # pragma: no cover - safety fallback only
        raise RuntimeError("PyMuPDF is required for multimodal PDF extraction")

    pdf = fitz.open(path)
    try:
        start_index, end_index = _resolve_page_bounds(pdf.page_count, page_start, page_end)
        base_dir = Path("data") / "page_assets" / document_id
        base_dir.mkdir(parents=True, exist_ok=True)

        pages: list[DocumentPage] = []
        assets: list[PageAsset] = []
        page_parts: list[str] = []
        vision_pages_used = 0
        vision_assets_used = 0
        vision_page_failures = 0
        vision_asset_failures = 0
        page_render_failures = 0
        asset_render_failures = 0
        total_pages = (end_index - start_index) + 1

        for page_index in range(start_index, end_index + 1):
            page = pdf.load_page(page_index)
            page_number = page_index + 1
            page_position = (page_index - start_index) + 1
            page_area = max(float(page.rect.width * page.rect.height), 1.0)
            page_dir = base_dir / f"page-{page_number:04d}"
            page_dir.mkdir(parents=True, exist_ok=True)
            if progress_callback is not None:
                progress_callback(
                    {
                        "detail": f"Preparing page {page_position}/{total_pages} (page {page_number}).",
                        "metrics": {
                            "page_current": page_position,
                            "page_total": total_pages,
                            "pages_prepared": len(pages),
                            "assets_extracted": len(assets),
                            "vision_pages_used": vision_pages_used,
                            "vision_assets_used": vision_assets_used,
                            "page_render_failures": page_render_failures,
                            "asset_render_failures": asset_render_failures,
                        },
                    }
                )

            extracted_text = _extract_page_text(page)
            page_image_path = page_dir / f"page-{page_number:04d}.png"
            rendered_page_image, page_render_error = _render_page_image(page, page_image_path, dpi=settings.vision_page_render_dpi)
            if not rendered_page_image:
                page_render_failures += 1

            image_assets, image_asset_failures = _extract_embedded_image_assets(
                document_id=document_id,
                tenant_id=tenant_id,
                page=page,
                page_number=page_number,
                page_dir=page_dir,
                filename=filename,
            )
            asset_render_failures += image_asset_failures

            page_vision = None
            if rendered_page_image and _should_enrich_page(extracted_text, image_assets, page_area):
                page_vision = _describe_page_image(
                    image_path=page_image_path,
                    filename=filename,
                    page_number=page_number,
                    extracted_text=extracted_text,
                )
                if page_vision and not page_vision.get("_vision_error"):
                    vision_pages_used += 1
                elif page_vision and page_vision.get("_vision_error"):
                    vision_page_failures += 1

            page_summary = sanitize_text((page_vision or {}).get("page_summary") or "")
            page_terms = [sanitize_text(str(item)) for item in ((page_vision or {}).get("important_terms") or []) if str(item).strip()]
            for asset in image_assets:
                asset.metadata["page_label"] = f"Page {page_number}"

            described_assets: list[PageAsset] = []
            for asset in image_assets:
                if asset.asset_index > settings.vision_max_assets_per_page:
                    described_assets.append(asset)
                    continue
                enriched = _describe_asset_image(asset, filename=filename)
                if enriched is not None:
                    asset = enriched
                    if asset.metadata.get("vision_error"):
                        vision_asset_failures += 1
                    elif asset.description_text.strip() or asset.ocr_text.strip() or asset.metadata.get("important_terms"):
                        vision_assets_used += 1
                described_assets.append(asset)

            page_asset = _build_page_asset(
                document_id=document_id,
                tenant_id=tenant_id,
                page_number=page_number,
                page_image_path=page_image_path if rendered_page_image else None,
                page_vision=page_vision,
                image_assets=described_assets,
                filename=filename,
            )
            if page_asset is not None:
                if page_render_error:
                    page_asset.metadata["render_error"] = page_render_error
                assets.append(page_asset)
            assets.extend(described_assets)

            ocr_text = sanitize_text((page_vision or {}).get("ocr_text") or "")
            merged_text = _merge_page_text(extracted_text, ocr_text)
            page_parts.append(f"[[Page {page_number}]]\n\n{merged_text}".strip())
            pages.append(
                DocumentPage(
                    document_id=document_id,
                    page_number=page_number,
                    extracted_text=extracted_text,
                    ocr_text=ocr_text,
                    merged_text=merged_text,
                    page_image_path=str(page_image_path) if rendered_page_image else None,
                    metadata={
                        "page_summary": page_summary,
                        "important_terms": page_terms[:12],
                        "image_asset_count": len(described_assets),
                        "vision_used": bool(page_vision and not page_vision.get("_vision_error")),
                        "vision_error": str((page_vision or {}).get("_vision_error") or ""),
                        "render_error": page_render_error or "",
                        "asset_render_failures": image_asset_failures,
                    },
                )
            )
            if progress_callback is not None:
                progress_callback(
                    {
                        "detail": f"Prepared page {page_position}/{total_pages} (page {page_number}).",
                        "metrics": {
                            "page_current": page_position,
                            "page_total": total_pages,
                            "pages_prepared": len(pages),
                            "assets_extracted": len(assets),
                            "vision_pages_used": vision_pages_used,
                            "vision_assets_used": vision_assets_used,
                            "page_render_failures": page_render_failures,
                            "asset_render_failures": asset_render_failures,
                        },
                    }
                )

        merged_text = PAGE_BREAK.join(part for part in page_parts if part).strip()
        metrics = {
            "pages": len(pages),
            "page_assets": len(assets),
            "vision_pages_used": vision_pages_used,
            "vision_assets_used": vision_assets_used,
            "vision_page_failures": vision_page_failures,
            "vision_asset_failures": vision_asset_failures,
            "page_render_failures": page_render_failures,
            "asset_render_failures": asset_render_failures,
            "image_heavy_pages": sum(1 for page in pages if int(page.metadata.get("image_asset_count") or 0) > 0),
            "ocr_pages": sum(1 for page in pages if page.ocr_text.strip()),
        }
        return MultimodalPDFPayload(pages=pages, assets=assets, merged_text=merged_text, metrics=metrics)
    finally:
        pdf.close()


def _extract_page_text(page) -> str:
    blocks = page.get_text("blocks", sort=True)
    parts: list[str] = []
    for block in blocks:
        text = sanitize_text((block[4] or "").strip())
        if not text:
            continue
        parts.append(text)
    return sanitize_text("\n\n".join(parts).strip())


def _render_page_image(page, target_path: Path, dpi: int) -> tuple[bool, str | None]:
    zoom = max(dpi, 72) / 72.0
    pixmap = page.get_pixmap(matrix=fitz.Matrix(zoom, zoom), alpha=False)
    return _save_pixmap(target_path, pixmap)


def _extract_embedded_image_assets(
    document_id: str,
    tenant_id: str,
    page,
    page_number: int,
    page_dir: Path,
    filename: str,
) -> tuple[list[PageAsset], int]:
    try:
        payload = page.get_text("dict", sort=True)
    except Exception:
        return [], 1
    assets: list[PageAsset] = []
    failures = 0
    asset_counter = 0
    page_area = max(float(page.rect.width * page.rect.height), 1.0)
    for block in payload.get("blocks", []):
        if int(block.get("type", 0)) != 1:
            continue
        bbox = [float(value) for value in (block.get("bbox") or [])]
        if len(bbox) != 4:
            continue
        if _is_full_page_image(bbox, page_area):
            # Full-page image scans are already represented by the page_image asset.
            # Keeping them again as embedded-image assets just duplicates page-level OCR/description.
            continue
        asset_counter += 1
        asset_path = page_dir / f"asset-{asset_counter:02d}.png"
        clip = fitz.Rect(*bbox)
        if clip.width <= 1 or clip.height <= 1:
            failures += 1
            continue
        try:
            pixmap = page.get_pixmap(
                matrix=fitz.Matrix(settings.vision_asset_render_dpi / 72.0, settings.vision_asset_render_dpi / 72.0),
                clip=clip,
                alpha=False,
            )
        except Exception:
            failures += 1
            continue
        saved, render_error = _save_pixmap(asset_path, pixmap)
        if not saved:
            failures += 1
            continue
        assets.append(
            PageAsset(
                asset_id=_asset_id(document_id, page_number, asset_counter, "embedded_image", str(asset_path)),
                document_id=document_id,
                tenant_id=tenant_id,
                page_number=page_number,
                asset_index=asset_counter,
                asset_type="embedded_image",
                asset_path=str(asset_path),
                bbox=bbox,
                metadata={
                    "label": f"Page {page_number} asset {asset_counter}",
                    "document_label": filename,
                    "width": pixmap.width,
                    "height": pixmap.height,
                    "render_error": render_error or "",
                },
            )
        )
    return assets, failures


def _asset_id(document_id: str, page_number: int, asset_index: int, asset_type: str, asset_path: str) -> str:
    digest = sha256(f"{document_id}|{page_number}|{asset_index}|{asset_type}|{asset_path}".encode("utf-8")).hexdigest()[:16]
    return f"{document_id}:p{page_number:04d}:{asset_type}:{asset_index:02d}:{digest}"


def _is_full_page_image(bbox: list[float], page_area: float) -> bool:
    width = max(0.0, bbox[2] - bbox[0])
    height = max(0.0, bbox[3] - bbox[1])
    bbox_area = width * height
    return (bbox_area / page_area) >= 0.82


def _should_enrich_page(extracted_text: str, image_assets: list[PageAsset], page_area: float) -> bool:
    if not settings.vision_enabled:
        return False
    if not _vision_api_key():
        return False
    # Page-level LLM OCR should only run when the PDF text layer is weak. Embedded
    # images are described separately as page assets and linked back to chunks.
    extracted = sanitize_text(extracted_text).strip()
    if len(extracted) < settings.vision_page_min_chars:
        return True
    if not image_assets:
        return False
    visual_marker = bool(re.search(r"\b(fig(?:ure)?|plate|diagram|illustration|table|image)\b", extracted, flags=re.IGNORECASE))
    if visual_marker and len(extracted) < (settings.vision_page_min_chars * 2):
        return True
    if len(image_assets) >= 2 and len(extracted) < int(settings.vision_page_min_chars * 1.5):
        return True
    if image_assets:
        total_visual_area = 0.0
        largest_visual_ratio = 0.0
        normalized_page_area = max(float(page_area or 0.0), 1.0)
        for asset in image_assets:
            bbox = list(asset.bbox or [])
            if len(bbox) != 4:
                continue
            width = max(0.0, float(bbox[2]) - float(bbox[0]))
            height = max(0.0, float(bbox[3]) - float(bbox[1]))
            ratio = (width * height) / normalized_page_area
            total_visual_area += ratio
            largest_visual_ratio = max(largest_visual_ratio, ratio)
        if largest_visual_ratio >= 0.16:
            return True
        if total_visual_area >= 0.24:
            return True
    return False


def _build_page_asset(
    document_id: str,
    tenant_id: str,
    page_number: int,
    page_image_path: Path | None,
    page_vision: dict[str, Any] | None,
    image_assets: list[PageAsset],
    filename: str,
) -> PageAsset | None:
    summary = sanitize_text((page_vision or {}).get("page_summary") or "")
    ocr_text = sanitize_text((page_vision or {}).get("ocr_text") or "")
    if page_image_path is None:
        return None
    if not page_vision and not image_assets:
        return None
    return PageAsset(
        asset_id=_asset_id(document_id, page_number, 0, "page_image", str(page_image_path)),
        document_id=document_id,
        tenant_id=tenant_id,
        page_number=page_number,
        asset_index=0,
        asset_type="page_image",
        asset_path=str(page_image_path),
        bbox=None,
        ocr_text=ocr_text,
        description_text=summary,
        metadata={
            "label": f"Page {page_number} image",
            "document_label": filename,
            "page_summary": summary,
            "important_terms": list((page_vision or {}).get("important_terms") or []),
            "linked_embedded_assets": len(image_assets),
            "vision_used": bool(page_vision and not (page_vision or {}).get("_vision_error")),
            "vision_error": str((page_vision or {}).get("_vision_error") or ""),
        },
    )


def _save_pixmap(target_path: Path, pixmap) -> tuple[bool, str | None]:
    width = int(getattr(pixmap, "width", 0) or 0)
    height = int(getattr(pixmap, "height", 0) or 0)
    if width <= 0 or height <= 0:
        return False, "invalid_pixmap_dimensions"
    try:
        pixmap.save(str(target_path))
        return True, None
    except Exception as exc:
        tobytes = getattr(pixmap, "tobytes", None)
        if callable(tobytes):
            try:
                target_path.write_bytes(tobytes("png"))
                return True, f"save_failed_fallback_tobytes:{type(exc).__name__}"
            except Exception:
                pass
        return False, f"{type(exc).__name__}:{exc}"


def _merge_page_text(extracted_text: str, ocr_text: str) -> str:
    extracted = sanitize_text(extracted_text).strip()
    ocr = sanitize_text(ocr_text).strip()
    if not extracted:
        return ocr
    if not ocr:
        return extracted
    extracted_lower = extracted.lower()
    if ocr.lower() in extracted_lower:
        return extracted
    if extracted_lower in ocr.lower() and len(ocr) > len(extracted):
        return ocr
    return "\n\n".join([extracted, ocr]).strip()


def _describe_page_image(
    image_path: Path,
    filename: str,
    page_number: int,
    extracted_text: str,
) -> dict[str, Any] | None:
    system_prompt = (
        "You enrich a PDF ingestion pipeline.\n"
        "Return JSON only and follow the schema exactly.\n"
        "You are looking at one PDF page image.\n"
        "Extract legible text that the page image contains but the current extractor may have missed.\n"
        "Summarize only the visually meaningful information from the page.\n"
        "Do not invent details.\n"
        "Prefer empty strings over speculation."
    )
    user_prompt = "\n".join(
        [
            f"document: {filename}",
            f"page_number: {page_number}",
            f"existing_text_excerpt: {extracted_text[:2400]}",
        ]
    )
    return _vision_request(
        system_prompt=system_prompt,
        user_prompt=user_prompt,
        image_path=image_path,
        schema=_page_vision_schema(),
        schema_name="page_vision_enrichment",
    )


def _describe_asset_image(asset: PageAsset, filename: str) -> PageAsset | None:
    system_prompt = (
        "You enrich a PDF ingestion pipeline.\n"
        "Return JSON only and follow the schema exactly.\n"
        "You are looking at one image asset cropped from a PDF page.\n"
        "Describe what the asset contains in factual terms.\n"
        "Extract legible text inside the asset if present.\n"
        "Choose the closest asset_type from the allowed enum.\n"
        "Prefer empty strings over speculation."
    )
    user_prompt = "\n".join(
        [
            f"document: {filename}",
            f"page_number: {asset.page_number}",
            f"asset_label: {asset.metadata.get('label') or ''}",
        ]
    )
    result = _vision_request(
        system_prompt=system_prompt,
        user_prompt=user_prompt,
        image_path=Path(asset.asset_path),
        schema=_asset_vision_schema(),
        schema_name="asset_vision_enrichment",
    )
    if result is None:
        return None
    if result.get("_vision_error"):
        asset.metadata["vision_error"] = str(result.get("_vision_error") or "")
        return asset
    asset.asset_type = str(result.get("asset_type") or asset.asset_type or "embedded_image").strip() or "embedded_image"
    asset.description_text = sanitize_text(str(result.get("description") or ""))
    asset.ocr_text = sanitize_text(str(result.get("ocr_text") or ""))
    important_terms = [sanitize_text(str(item)) for item in (result.get("important_terms") or []) if str(item).strip()]
    asset.metadata["important_terms"] = important_terms[:12]
    derived_label = _derive_asset_label(asset.description_text, asset.ocr_text)
    if derived_label:
        asset.metadata["label"] = derived_label
    elif not asset.metadata.get("label") and asset.description_text:
        asset.metadata["label"] = asset.description_text[:80]
    reference_keys = sorted(
        set(_extract_reference_keys(str(asset.metadata.get("label") or "")))
        | set(_extract_reference_keys(asset.description_text))
        | set(_extract_reference_keys(asset.ocr_text))
    )
    if reference_keys:
        asset.metadata["reference_keys"] = reference_keys[:8]
    linked_terms = sorted(
        {
            sanitize_text(str(item)).lower()
            for item in [*(asset.metadata.get("important_terms") or []), asset.metadata.get("label") or ""]
            if sanitize_text(str(item)).strip()
        }
    )
    if linked_terms:
        asset.metadata["linked_terms"] = linked_terms[:12]
    return asset


def _vision_request(
    system_prompt: str,
    user_prompt: str,
    image_path: Path,
    schema: dict[str, Any],
    schema_name: str,
) -> dict[str, Any] | None:
    api_key = _vision_api_key()
    if not settings.vision_enabled or not api_key:
        return None

    try:
        image_bytes = image_path.read_bytes()
    except OSError:
        return {"_vision_error": "image_read_failed"}
    data_url = f"data:image/png;base64,{base64.b64encode(image_bytes).decode('ascii')}"
    headers = {
        "Authorization": f"Bearer {api_key}",
        "Content-Type": "application/json",
    }
    payload = {
        "model": settings.vision_model,
        "reasoning_effort": settings.vision_reasoning_effort,
        "messages": [
            {"role": "system", "content": system_prompt},
            {
                "role": "user",
                "content": [
                    {"type": "text", "text": user_prompt},
                    {"type": "image_url", "image_url": {"url": data_url, "detail": "high"}},
                ],
            },
        ],
        "response_format": {
            "type": "json_schema",
            "json_schema": {
                "name": schema_name,
                "strict": True,
                "schema": schema,
            },
        },
    }
    base_url = settings.vision_base_url.rstrip("/")
    retryable_statuses = {408, 409, 429, 500, 502, 503, 504}
    max_retries = max(0, settings.vision_max_retries)
    backoff = max(0.25, settings.vision_retry_backoff_seconds)
    try:
        with httpx.Client(timeout=settings.vision_timeout_seconds) as client:
            last_error: httpx.HTTPError | None = None
            for attempt in range(max_retries + 1):
                try:
                    response = client.post(f"{base_url}/chat/completions", headers=headers, json=payload)
                    if response.status_code in retryable_statuses and attempt < max_retries:
                        time.sleep(backoff * (2 ** attempt))
                        continue
                    response.raise_for_status()
                    body = response.json()
                    break
                except httpx.HTTPError as exc:
                    last_error = exc
                    if attempt >= max_retries:
                        raise
                    time.sleep(backoff * (2 ** attempt))
            else:  # pragma: no cover - defensive only
                if last_error is not None:
                    raise last_error
                return {"_vision_error": "request_failed_without_response"}
    except httpx.HTTPError as exc:
        return {"_vision_error": f"http_error:{type(exc).__name__}"}

    content = _extract_message_content(body)
    try:
        return json.loads(content)
    except json.JSONDecodeError:
        return {"_vision_error": "json_decode_error"}


def _vision_api_key() -> str | None:
    return settings.vision_api_key


def _extract_message_content(body: dict[str, Any]) -> str:
    choices = body.get("choices") or []
    if not choices:
        raise ValueError("Vision response returned no choices")
    message = choices[0].get("message") or {}
    content = message.get("content")
    if isinstance(content, str):
        return content
    if isinstance(content, list):
        parts: list[str] = []
        for item in content:
            if not isinstance(item, dict):
                continue
            text = item.get("text")
            if text:
                parts.append(str(text))
        return "\n".join(part for part in parts if part)
    raise ValueError("Vision response content was not parseable")


def _extract_reference_keys(text: str) -> list[str]:
    normalized = sanitize_text(text).lower()
    keys: list[str] = []
    for kind, marker in re.findall(r"\b(fig(?:ure)?|diagram|plate|table|illustration|image)\s+([a-z0-9ivx]+)\b", normalized):
        keys.append(f"{kind[:3]}:{marker}")
    return keys


def _derive_asset_label(description_text: str, ocr_text: str) -> str:
    combined = sanitize_text("\n".join(part for part in [ocr_text, description_text] if part.strip()))
    if not combined:
        return ""
    match = re.search(r"\b(fig(?:ure)?|diagram|plate|table|illustration|image)\s+([a-z0-9ivx]+)\b[^.\n:]{0,80}", combined, flags=re.IGNORECASE)
    if match:
        return sanitize_text(match.group(0))[:100]
    return ""


def _page_vision_schema() -> dict[str, Any]:
    return {
        "type": "object",
        "additionalProperties": False,
        "properties": {
            "ocr_text": {"type": "string"},
            "page_summary": {"type": "string"},
            "important_terms": {
                "type": "array",
                "items": {"type": "string"},
            },
        },
        "required": ["ocr_text", "page_summary", "important_terms"],
    }


def _asset_vision_schema() -> dict[str, Any]:
    return {
        "type": "object",
        "additionalProperties": False,
        "properties": {
            "asset_type": {
                "type": "string",
                "enum": ["embedded_image", "diagram", "table", "chart", "illustration", "photo", "other"],
            },
            "description": {"type": "string"},
            "ocr_text": {"type": "string"},
            "important_terms": {
                "type": "array",
                "items": {"type": "string"},
            },
        },
        "required": ["asset_type", "description", "ocr_text", "important_terms"],
    }
