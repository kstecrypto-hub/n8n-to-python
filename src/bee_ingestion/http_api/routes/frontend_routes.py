"""Frontend-facing HTML route ownership."""

from __future__ import annotations

from fastapi import APIRouter
from fastapi.responses import HTMLResponse

from src.bee_ingestion.admin_ui import ADMIN_HTML
from src.bee_ingestion.agent_ui import AGENT_UI_HTML
from src.bee_ingestion.frontend import frontend_index_response, frontend_path_response, frontend_redirect

router = APIRouter()


@router.get("/", response_class=HTMLResponse)
def agent_home():
    return frontend_redirect("/app", fallback_html=AGENT_UI_HTML)


@router.get("/app", response_class=HTMLResponse)
def agent_app():
    return frontend_index_response(fallback_html=AGENT_UI_HTML)


@router.get("/app/{frontend_path:path}")
def agent_app_path(frontend_path: str):
    return frontend_path_response(frontend_path, fallback_html=AGENT_UI_HTML)


@router.get("/admin/app")
def admin_app_redirect():
    return frontend_redirect("/app/control", fallback_html=ADMIN_HTML)


@router.get("/admin")
def admin_redirect():
    return frontend_redirect("/app/control", fallback_html=ADMIN_HTML)


@router.get("/admin/legacy", response_class=HTMLResponse)
def admin_legacy() -> str:
    return ADMIN_HTML
