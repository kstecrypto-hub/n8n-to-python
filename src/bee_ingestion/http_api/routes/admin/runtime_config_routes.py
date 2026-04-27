"""HTTP routes for admin runtime and startup configuration."""

from __future__ import annotations

from typing import Any

from fastapi import APIRouter
from pydantic import BaseModel

from src.bee_ingestion.admin import runtime_config_service
from src.bee_ingestion.http_api.dependencies import chroma_store, repository

router = APIRouter()


class AgentConfigUpdateRequest(BaseModel):
    tenant_id: str = "shared"
    config: dict[str, Any]
    updated_by: str = "admin"
    clear_api_key_override: bool = False


class SystemConfigUpdateRequest(BaseModel):
    group: str = "platform"
    config: dict[str, Any]


class OntologyUpdateRequest(BaseModel):
    content: str


@router.get("/admin/api/agent/config")
def admin_agent_config(tenant_id: str = "shared") -> dict:
    return runtime_config_service.get_agent_config(repository=repository, tenant_id=tenant_id)


@router.put("/admin/api/agent/config")
def admin_update_agent_config(request: AgentConfigUpdateRequest) -> dict:
    return runtime_config_service.update_agent_config(
        repository=repository,
        tenant_id=request.tenant_id,
        config=request.config,
        updated_by=request.updated_by,
        clear_api_key_override=request.clear_api_key_override,
    )


@router.delete("/admin/api/agent/config")
def admin_reset_agent_config(tenant_id: str = "shared") -> dict:
    return runtime_config_service.reset_agent_config(repository=repository, tenant_id=tenant_id)


@router.get("/admin/api/system/config")
def admin_system_config(group: str = "platform") -> dict:
    return runtime_config_service.get_system_config(chroma_store=chroma_store, group=group)


@router.put("/admin/api/system/config")
def admin_update_system_config(request: SystemConfigUpdateRequest) -> dict:
    return runtime_config_service.update_system_config(
        chroma_store=chroma_store,
        group=request.group,
        config=request.config,
    )


@router.delete("/admin/api/system/config")
def admin_reset_system_config(group: str = "platform") -> dict:
    return runtime_config_service.reset_system_config(chroma_store=chroma_store, group=group)


@router.get("/admin/api/ontology")
def admin_ontology() -> dict:
    return runtime_config_service.build_ontology_payload()


@router.put("/admin/api/ontology")
def admin_update_ontology(request: OntologyUpdateRequest) -> dict:
    return runtime_config_service.update_ontology(content=request.content)
