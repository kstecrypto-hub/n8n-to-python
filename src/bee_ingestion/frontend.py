from __future__ import annotations

from pathlib import Path

from fastapi.responses import FileResponse, HTMLResponse, RedirectResponse, Response


WORKSPACE_ROOT = Path(__file__).resolve().parents[2]
FRONTEND_DIST = (WORKSPACE_ROOT / "frontend" / "dist").resolve()
FRONTEND_INDEX = FRONTEND_DIST / "index.html"


def frontend_is_built() -> bool:
    return FRONTEND_INDEX.exists()


def frontend_index_response(*, fallback_html: str | None = None) -> Response:
    if frontend_is_built():
        return FileResponse(FRONTEND_INDEX)
    if fallback_html is None:
        return HTMLResponse("<h1>Frontend build not found</h1>", status_code=503)
    return HTMLResponse(fallback_html)


def frontend_path_response(path: str, *, fallback_html: str | None = None) -> Response:
    if not frontend_is_built():
        return frontend_index_response(fallback_html=fallback_html)
    requested = (FRONTEND_DIST / path).resolve()
    try:
        requested.relative_to(FRONTEND_DIST)
    except ValueError:
        return HTMLResponse("Not found", status_code=404)
    if requested.is_file():
        return FileResponse(requested)
    return FileResponse(FRONTEND_INDEX)


def frontend_redirect(path: str, *, fallback_html: str | None = None) -> Response:
    if frontend_is_built():
        return RedirectResponse(path, status_code=307)
    if fallback_html is None:
        return HTMLResponse("<h1>Frontend build not found</h1>", status_code=503)
    return HTMLResponse(fallback_html)
