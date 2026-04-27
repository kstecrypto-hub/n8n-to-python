"""Compatibility entrypoint for the serving HTTP application.

This module owns only app composition and legacy import compatibility.
It must not own admin route bodies, persistence workflows, retrieval logic, or
offline pipeline behavior.
"""

from __future__ import annotations

from src.bee_ingestion.api_workspace_routes import create_workspace_router
from src.bee_ingestion.auth_store import AuthStore
from src.bee_ingestion.agent import AgentService
from src.bee_ingestion.http_api.app_factory import create_app
from src.bee_ingestion.http_api.dependencies import (
    agent_service,
    auth_store,
    chroma_store,
    identity_repository,
    rate_limiter,
    repository,
    service,
)
from src.bee_ingestion.http_api.request_auth import (
    enforce_rate_limit,
    register_control_plane_middleware,
    require_authenticated_sensor_user,
)
from src.bee_ingestion.http_api.routes.admin import router as admin_router
from src.bee_ingestion.http_api.routes.agent_routes import router as agent_router
from src.bee_ingestion.http_api.routes.auth_routes import router as auth_router
from src.bee_ingestion.http_api.routes.frontend_routes import router as frontend_router
from src.bee_ingestion.http_api.routes.health_routes import router as health_router
from src.bee_ingestion.http_api.routes.ingest_routes import router as ingest_router
from src.bee_ingestion.repository import Repository
from src.bee_ingestion.settings import settings

app = create_app()
register_control_plane_middleware(app)

app.include_router(frontend_router)
app.include_router(auth_router)
app.include_router(agent_router)
app.include_router(health_router)
app.include_router(ingest_router)
app.include_router(admin_router)
app.include_router(
    create_workspace_router(
        repository=repository,
        require_authenticated_sensor_user=require_authenticated_sensor_user,
        enforce_rate_limit=enforce_rate_limit,
    )
)

__all__ = [
    "app",
    "settings",
    "Repository",
    "AuthStore",
    "AgentService",
    "repository",
    "identity_repository",
    "auth_store",
    "chroma_store",
    "service",
    "agent_service",
    "rate_limiter",
]
