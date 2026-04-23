from fastapi.testclient import TestClient
from fastapi.responses import HTMLResponse, JSONResponse
from pathlib import Path
from types import SimpleNamespace
from uuid import uuid4

from src.bee_ingestion.api import app
from src.bee_ingestion import api as api_module
from src.bee_ingestion.auth_store import AuthStore


def _test_auth_dsn() -> str:
    return str(api_module.settings.auth_postgres_dsn or "postgresql://bee_auth:bee_auth@127.0.0.1:35433/bee_identity")


def _login_public_user(client: TestClient, tmp_path) -> None:
    api_module.auth_store._instance = AuthStore(_workspace_auth_db("public-auth"), dsn=_test_auth_dsn())
    api_module.auth_store.create_user("beekeeper@example.com", "very-secure-hive-password", display_name="Bee Keeper")
    response = client.post("/auth/login", json={"email": "beekeeper@example.com", "password": "very-secure-hive-password"}, headers=_browser_headers())
    assert response.status_code == 200


def _admin_headers() -> dict[str, str]:
    token = str(api_module.settings.admin_api_token or "").strip()
    return {"X-Admin-Token": token} if token else {}


def _browser_headers() -> dict[str, str]:
    return {"Origin": "http://testserver"}


def _workspace_auth_db(name: str) -> Path:
    root = Path(r"E:\n8n to python\.tmp\pytest-auth")
    root.mkdir(parents=True, exist_ok=True)
    path = root / f"{name}-{uuid4().hex}.pgschema"
    return path


def test_health_endpoint() -> None:
    client = TestClient(app)

    response = client.get("/health")

    assert response.status_code == 200
    assert response.json() == {"status": "ok"}


def test_agent_home_uses_frontend_redirect(monkeypatch) -> None:
    client = TestClient(app)

    monkeypatch.setattr(
        api_module,
        "frontend_redirect",
        lambda path, fallback_html=None: JSONResponse({"path": path, "fallback": bool(fallback_html)}),
    )

    response = client.get("/")

    assert response.status_code == 200
    assert response.json() == {"path": "/app", "fallback": True}


def test_agent_app_uses_frontend_index_response(monkeypatch) -> None:
    client = TestClient(app)

    monkeypatch.setattr(
        api_module,
        "frontend_index_response",
        lambda fallback_html=None: HTMLResponse("frontend-index"),
    )

    response = client.get("/app")

    assert response.status_code == 200
    assert response.text == "frontend-index"


def test_agent_app_path_uses_frontend_path_response(monkeypatch) -> None:
    client = TestClient(app)

    monkeypatch.setattr(
        api_module,
        "frontend_path_response",
        lambda frontend_path, fallback_html=None: HTMLResponse(f"frontend-path:{frontend_path}"),
    )

    response = client.get("/app/chat")

    assert response.status_code == 200
    assert response.text == "frontend-path:chat"


def test_admin_app_redirect_uses_frontend_redirect(monkeypatch) -> None:
    client = TestClient(app)

    monkeypatch.setattr(
        api_module,
        "frontend_redirect",
        lambda path, fallback_html=None: JSONResponse({"path": path, "fallback": bool(fallback_html)}),
    )

    response = client.get("/admin/app")

    assert response.status_code == 200
    assert response.json() == {"path": "/app/control", "fallback": True}


def test_admin_page_renders() -> None:
    client = TestClient(app)
    original_frontend_redirect = api_module.frontend_redirect
    api_module.frontend_redirect = lambda path, fallback_html=None: JSONResponse({"path": path, "fallback": bool(fallback_html)})
    try:
        response = client.get("/admin")
    finally:
        api_module.frontend_redirect = original_frontend_redirect

    assert response.status_code == 200
    assert response.json() == {"path": "/app/control", "fallback": True}


def test_admin_db_row_crud_endpoints(monkeypatch) -> None:
    client = TestClient(app)

    monkeypatch.setattr(
        api_module.repository,
        "insert_admin_relation_row",
        lambda relation_name, values, schema_name="public": {"document_id": "doc-1", **values},
    )
    monkeypatch.setattr(
        api_module.repository,
        "update_admin_relation_row",
        lambda relation_name, key, values, schema_name="public": {**key, **values},
    )
    monkeypatch.setattr(
        api_module.repository,
        "delete_admin_relation_row",
        lambda relation_name, key, schema_name="public": 1,
    )

    create_response = client.post(
        "/admin/api/db/rows",
        json={"relation_name": "documents", "values": {"tenant_id": "shared"}},
        headers=_admin_headers(),
    )
    assert create_response.status_code == 200
    assert create_response.json()["row"]["tenant_id"] == "shared"

    update_response = client.put(
        "/admin/api/db/rows",
        json={"relation_name": "documents", "key": {"document_id": "doc-1"}, "values": {"status": "completed"}},
        headers=_admin_headers(),
    )
    assert update_response.status_code == 200
    assert update_response.json()["row"]["status"] == "completed"

    delete_response = client.request(
        "DELETE",
        "/admin/api/db/rows",
        json={"relation_name": "documents", "key": {"document_id": "doc-1"}},
        headers=_admin_headers(),
    )
    assert delete_response.status_code == 200
    assert delete_response.json()["deleted"] is True


def test_admin_db_sql_endpoint(monkeypatch) -> None:
    client = TestClient(app)

    monkeypatch.setattr(
        api_module.repository,
        "execute_admin_sql",
        lambda statement: {
            "statement_type": "select",
            "columns": ["table_name"],
            "rows": [{"table_name": "documents"}],
            "row_count": 1,
            "truncated": False,
        },
    )

    response = client.post(
        "/admin/api/db/sql",
        json={"sql": "SELECT table_name FROM information_schema.tables"},
        headers=_admin_headers(),
    )

    assert response.status_code == 200
    payload = response.json()
    assert payload["statement_type"] == "select"
    assert payload["rows"][0]["table_name"] == "documents"


def test_admin_auth_user_create_stores_permissions() -> None:
    client = TestClient(app)
    api_module.auth_store._instance = AuthStore(_workspace_auth_db("auth-permissions"), dsn=_test_auth_dsn())

    response = client.post(
        "/admin/api/auth/users",
        json={
            "email": "owner@example.com",
            "password": "very-secure-hive-password",
            "display_name": "Owner",
            "role": "platform_owner",
            "status": "active",
            "permissions": ["accounts.write", "db.sql.write", "runtime.write"],
        },
        headers=_admin_headers(),
    )

    assert response.status_code == 200
    payload = response.json()
    assert payload["user"]["email"] == "owner@example.com"
    assert payload["user"]["permissions"] == ["accounts.write", "db.sql.write", "runtime.write"]


def test_admin_clear_session_memory_sections(monkeypatch) -> None:
    client = TestClient(app)
    session_row = {
        "session_id": "session-1",
        "summary_json": {
            "summary_version": "v3",
            "session_goal": "understand robbing pressure",
            "stable_facts": [
                {
                    "fact": "late summer dearth increases robbing pressure",
                    "fact_type": "domain",
                    "source_type": "chunk",
                    "confidence": 0.96,
                    "review_policy": "evidence_required",
                    "chunk_ids": ["chunk-1"],
                    "asset_ids": [],
                    "assertion_ids": ["assertion-1"],
                    "evidence_ids": ["evidence-1"],
                }
            ],
            "open_threads": [
                {
                    "thread": "Explain robbing pressure",
                    "source": "user",
                    "source_query": "Why is robbing happening?",
                    "question_type": "explanation",
                    "expiry_policy": "short_session",
                }
            ],
            "resolved_threads": [],
            "user_preferences": [{"preference": "brief answers", "source": "user"}],
            "active_constraints": [{"constraint": "cite sources", "kind": "style", "source": "user"}],
            "topic_keywords": ["robbing"],
            "preferred_document_ids": ["doc-1"],
            "last_query": "why is robbing happening",
        },
        "summary_text": "stale",
        "source_provider": "openai",
        "source_model": "gpt-5.4-mini",
        "prompt_version": "v3",
    }
    captured: dict[str, object] = {}

    monkeypatch.setattr(api_module.repository, "get_agent_session_memory", lambda session_id: session_row if session_id == "session-1" else None)

    def _update(session_id: str, patch: dict):
        captured["session_id"] = session_id
        captured["patch"] = patch
        return {**session_row, **patch}

    monkeypatch.setattr(api_module.repository, "update_agent_session_memory_record", _update)

    response = client.post(
        "/admin/api/agent/sessions/session-1/memory/clear",
        json={"sections": ["facts", "scope"]},
        headers=_admin_headers(),
    )

    assert response.status_code == 200
    payload = response.json()
    assert payload["cleared_sections"] == ["facts", "scope"]
    saved = captured["patch"]
    assert isinstance(saved, dict)
    assert saved["summary_json"]["stable_facts"] == []
    assert saved["summary_json"]["topic_keywords"] == []
    assert saved["summary_json"]["preferred_document_ids"] == []
    assert saved["summary_json"]["last_query"] == ""
    assert saved["summary_json"]["session_goal"] == "understand robbing pressure"
    assert "late summer dearth" not in str(saved["summary_text"])


def test_admin_clear_profile_memory_sections(monkeypatch) -> None:
    client = TestClient(app)
    profile_row = {
        "profile_id": "profile-1",
        "summary_json": {
            "summary_version": "v3",
            "user_background": "commercial beekeeper",
            "beekeeping_context": "operates 40 hives",
            "experience_level": "advanced",
            "communication_style": "direct",
            "answer_preferences": [{"preference": "lead with diagnosis", "source": "user"}],
            "recurring_topics": [{"topic": "varroa", "source": "history"}],
            "learning_goals": [{"goal": "improve overwintering", "source": "user"}],
            "persistent_constraints": [{"constraint": "avoid vague advice", "kind": "style", "source": "user"}],
        },
        "summary_text": "stale profile",
        "source_provider": "openai",
        "source_model": "gpt-5.4-mini",
        "prompt_version": "v3",
    }
    captured: dict[str, object] = {}

    monkeypatch.setattr(api_module.repository, "get_agent_profile", lambda profile_id: profile_row if profile_id == "profile-1" else None)

    def _update(profile_id: str, patch: dict):
        captured["profile_id"] = profile_id
        captured["patch"] = patch
        return {**profile_row, **patch}

    monkeypatch.setattr(api_module.repository, "update_agent_profile_record", _update)

    response = client.post(
        "/admin/api/agent/profiles/profile-1/memory/clear",
        json={"sections": ["topics", "learning_goals"]},
        headers=_admin_headers(),
    )

    assert response.status_code == 200
    payload = response.json()
    assert payload["cleared_sections"] == ["topics", "learning_goals"]
    saved = captured["patch"]
    assert isinstance(saved, dict)
    assert saved["summary_json"]["recurring_topics"] == []
    assert saved["summary_json"]["learning_goals"] == []
    assert saved["summary_json"]["user_background"] == "commercial beekeeper"
    assert "varroa" not in str(saved["summary_text"])


def test_login_rate_limit_enforced(monkeypatch) -> None:
    client = TestClient(app)
    api_module.auth_store._instance = AuthStore(_workspace_auth_db("login-rate-limit"), dsn=_test_auth_dsn())
    api_module.auth_store.create_user("beekeeper@example.com", "very-secure-hive-password", display_name="Bee Keeper")
    api_module.rate_limiter.clear()
    monkeypatch.setattr(api_module.settings, "auth_login_rate_limit_max_attempts", 1)
    monkeypatch.setattr(api_module.settings, "auth_login_rate_limit_window_seconds", 60)

    first = client.post("/auth/login", json={"email": "beekeeper@example.com", "password": "very-secure-hive-password"}, headers=_browser_headers())
    second = client.post("/auth/login", json={"email": "beekeeper@example.com", "password": "very-secure-hive-password"}, headers=_browser_headers())

    api_module.rate_limiter.clear()

    assert first.status_code == 200
    assert second.status_code == 429


def test_platform_owner_can_access_admin_sql_without_token(monkeypatch) -> None:
    client = TestClient(app)
    api_module.auth_store._instance = AuthStore(_workspace_auth_db("platform-owner-sql"), dsn=_test_auth_dsn())
    api_module.auth_store.create_user(
        "owner@example.com",
        "very-secure-hive-password",
        display_name="Owner",
        tenant_id="shared",
        role="platform_owner",
    )
    login = client.post("/auth/login", json={"email": "owner@example.com", "password": "very-secure-hive-password"}, headers=_browser_headers())
    assert login.status_code == 200

    monkeypatch.setattr(
        api_module.repository,
        "execute_admin_sql",
        lambda statement: {
            "statement_type": "select",
            "columns": ["value"],
            "rows": [{"value": 1}],
            "row_count": 1,
            "truncated": False,
        },
    )

    response = client.post("/admin/api/db/sql", json={"sql": "SELECT 1"})

    assert response.status_code == 200
    assert response.json()["rows"][0]["value"] == 1


def test_tenant_admin_cannot_run_admin_sql_without_permission() -> None:
    client = TestClient(app)
    api_module.auth_store._instance = AuthStore(_workspace_auth_db("tenant-admin-sql"), dsn=_test_auth_dsn())
    api_module.auth_store.create_user(
        "tenant@example.com",
        "very-secure-hive-password",
        display_name="Tenant Admin",
        tenant_id="shared",
        role="tenant_admin",
    )
    login = client.post("/auth/login", json={"email": "tenant@example.com", "password": "very-secure-hive-password"}, headers=_browser_headers())
    assert login.status_code == 200

    response = client.post("/admin/api/db/sql", json={"sql": "SELECT 1"})

    assert response.status_code == 403


def test_accounts_reader_can_browse_identity_db_but_not_app_db(monkeypatch) -> None:
    client = TestClient(app)
    api_module.auth_store._instance = AuthStore(_workspace_auth_db("accounts-reader"), dsn=_test_auth_dsn())
    api_module.auth_store.create_user(
        "auditor@example.com",
        "very-secure-hive-password",
        display_name="Identity Auditor",
        tenant_id="shared",
        role="member",
        permissions=["accounts.read"],
    )
    login = client.post("/auth/login", json={"email": "auditor@example.com", "password": "very-secure-hive-password"}, headers=_browser_headers())
    assert login.status_code == 200

    monkeypatch.setattr(
        api_module.identity_repository,
        "_instance",
        SimpleNamespace(
            list_admin_relations=lambda search=None, schema_name=None: [
                {
                    "schema_name": "auth",
                    "relation_name": "auth_users",
                    "relation_type": "table",
                    "estimated_rows": 1,
                    "has_primary_key": True,
                }
            ]
        ),
        raising=False,
    )

    identity_response = client.get("/admin/api/db/relations?database=identity")
    app_response = client.get("/admin/api/db/relations?database=app")

    assert identity_response.status_code == 200
    assert identity_response.json()["database"] == "identity"
    assert identity_response.json()["items"][0]["relation_name"] == "auth_users"
    assert app_response.status_code == 403


def test_tenant_admin_cannot_run_identity_sql_without_db_sql_permission() -> None:
    client = TestClient(app)
    api_module.auth_store._instance = AuthStore(_workspace_auth_db("tenant-admin-identity-sql"), dsn=_test_auth_dsn())
    api_module.auth_store.create_user(
        "identity-admin@example.com",
        "very-secure-hive-password",
        display_name="Identity Admin",
        tenant_id="shared",
        role="tenant_admin",
    )
    login = client.post("/auth/login", json={"email": "identity-admin@example.com", "password": "very-secure-hive-password"}, headers=_browser_headers())
    assert login.status_code == 200

    response = client.post("/admin/api/db/sql", json={"sql": "SELECT 1", "database": "identity"})

    assert response.status_code == 403


def test_agent_query_requires_chat_use_permission(monkeypatch) -> None:
    client = TestClient(app)
    api_module.auth_store._instance = AuthStore(_workspace_auth_db("chat-permission"), dsn=_test_auth_dsn())
    api_module.auth_store.create_user(
        "restricted@example.com",
        "very-secure-hive-password",
        display_name="Restricted User",
        tenant_id="shared",
        role="member",
        permissions=[],
    )
    login = client.post("/auth/login", json={"email": "restricted@example.com", "password": "very-secure-hive-password"}, headers=_browser_headers())
    assert login.status_code == 200

    api_module.agent_service._instance = SimpleNamespace(
        query=lambda **kwargs: {"query_run_id": "run-1", "answer": "ok", "abstained": False},
    )

    response = client.post("/agent/query", json={"question": "test"}, headers=_browser_headers())

    assert response.status_code == 403


def test_admin_kg_raw_endpoint(monkeypatch) -> None:
    client = TestClient(app)

    monkeypatch.setattr(
        api_module.repository,
        "list_kg_raw_extractions",
        lambda document_id=None, chunk_id=None, status=None, limit=100, offset=0: [
            {
                "extraction_id": "ex-1",
                "chunk_id": "chunk-1",
                "document_id": "doc-1",
                "status": "review",
                "payload": {"errors": ["invalid_predicate:test"]},
                "created_at": "2026-03-17T00:00:00Z",
            }
        ],
    )
    monkeypatch.setattr(api_module.repository, "count_kg_raw_extractions", lambda document_id=None, chunk_id=None, status=None: 1)

    response = client.get("/admin/api/kg/raw?status=review", headers=_admin_headers())

    assert response.status_code == 200
    assert response.json()["items"][0]["status"] == "review"


def test_admin_document_bundle_endpoint(monkeypatch) -> None:
    client = TestClient(app)

    monkeypatch.setattr(
        api_module.repository,
        "get_document_detail",
        lambda document_id: {
            "document": {
                "document_id": document_id,
                "filename": "doc.txt",
                "document_class": "note",
                "tenant_id": "shared",
                "source_type": "text",
                "status": "completed",
                "content_hash": "sha256:test",
            },
            "jobs": [],
            "stages": [],
        },
    )
    monkeypatch.setattr(api_module.repository, "list_document_sources", lambda document_id=None, limit=100: [{"source_id": "src-1"}] if document_id else [])
    monkeypatch.setattr(api_module.repository, "list_document_pages", lambda document_id=None, limit=100: [{"document_id": document_id, "page_number": 1}] if document_id else [])
    monkeypatch.setattr(api_module.repository, "list_page_assets", lambda document_id=None, limit=100: [{"asset_id": "asset-1"}] if document_id else [])
    monkeypatch.setattr(api_module.repository, "list_chunk_asset_links", lambda document_id=None, limit=100: [{"link_id": "link-1"}] if document_id else [])
    monkeypatch.setattr(api_module.repository, "list_chunks", lambda document_id=None, status=None, limit=100, offset=0: [{"chunk_id": "chunk-1"}] if document_id else [])
    monkeypatch.setattr(api_module.repository, "list_chunk_metadata", lambda document_id=None, status=None, limit=100, offset=0: [{"chunk_id": "chunk-1"}] if document_id else [])
    monkeypatch.setattr(api_module.repository, "list_kg_assertions", lambda document_id=None, entity_id=None, predicate=None, status=None, chunk_id=None, limit=100, offset=0: [{"assertion_id": "a-1"}] if document_id else [])
    monkeypatch.setattr(api_module.repository, "list_kg_entities", lambda document_id=None, search=None, entity_type=None, limit=100, offset=0: [{"entity_id": "e-1"}] if document_id else [])
    monkeypatch.setattr(api_module.repository, "list_kg_evidence", lambda document_id=None, limit=100: [{"evidence_id": "ev-1"}] if document_id else [])
    monkeypatch.setattr(api_module.repository, "list_kg_raw_extractions", lambda document_id=None, chunk_id=None, status=None, limit=100, offset=0: [{"extraction_id": "x-1"}] if document_id else [])
    monkeypatch.setattr(api_module.repository, "get_document_related_counts", lambda document_id: {"sources": 1, "chunks": 1, "metadata": 1, "kg_entities": 1, "kg_assertions": 1, "kg_evidence": 1, "kg_raw": 1})
    monkeypatch.setattr(
        api_module,
        "_get_chroma_payload",
        lambda document_id=None, limit=50, offset=0, collection_name=None: {
            "records": [{"id": "vec-1"}] if document_id else [],
            "total": 1 if document_id else 0,
            "error": None,
        },
    )
    monkeypatch.setattr(api_module, "_get_chroma_parity", lambda document_id=None: {"accepted_chunks": 1, "vectors": 1, "missing_vectors": [], "extra_vectors": [], "missing_vectors_total": 0, "extra_vectors_total": 0, "error": None})

    response = client.get("/admin/api/documents/doc-1/bundle", headers=_admin_headers())

    assert response.status_code == 200
    payload = response.json()
    assert payload["document"]["document_id"] == "doc-1"
    assert payload["sources"][0]["source_id"] == "src-1"
    assert payload["chunks"][0]["chunk_id"] == "chunk-1"
    assert payload["kg_assertions"][0]["assertion_id"] == "a-1"
    assert payload["kg_evidence"][0]["evidence_id"] == "ev-1"
    assert payload["chroma_records"][0]["id"] == "vec-1"
    assert payload["counts"]["vectors"] == 1


def test_admin_auto_review_chunks_endpoint(monkeypatch) -> None:
    client = TestClient(app)

    monkeypatch.setattr(
        api_module.service,
        "auto_review_chunks",
        lambda document_id=None, batch_size=100: {
            "document_id": document_id,
            "processed_chunks": 2,
            "accepted": 1,
            "rejected": 1,
            "review": 0,
            "errors": [],
        },
    )

    response = client.post("/admin/api/chunks/review/auto", json={"document_id": "doc-1", "batch_size": 25}, headers=_admin_headers())

    assert response.status_code == 200
    payload = response.json()
    assert payload["document_id"] == "doc-1"
    assert payload["processed_chunks"] == 2


def test_admin_revalidate_document_endpoint(monkeypatch) -> None:
    client = TestClient(app)

    monkeypatch.setattr(
        api_module.service,
        "revalidate_document",
        lambda document_id, rerun_kg=True: {
            "document_id": document_id,
            "chunks": 3,
            "accepted": 2,
            "review": 1,
            "rejected": 0,
            "kg": {"validated": 2, "review": 0, "quarantined": 0},
        },
    )

    response = client.post("/admin/api/documents/doc-1/revalidate", json={"rerun_kg": True}, headers=_admin_headers())

    assert response.status_code == 200
    payload = response.json()
    assert payload["document_id"] == "doc-1"
    assert payload["chunks"] == 3


def test_admin_chunk_detail_endpoint(monkeypatch) -> None:
    client = TestClient(app)

    monkeypatch.setattr(
        api_module.repository,
        "get_chunk_detail",
        lambda chunk_id: {
            "chunk": {"chunk_id": chunk_id, "document_id": "doc-1"},
            "metadata": {"chunk_role": "body"},
            "assertions": [],
            "raw_extractions": [],
            "evidence": [],
            "neighbors": [],
        },
    )
    monkeypatch.setattr(api_module.chroma_store, "get_record", lambda chunk_id: {"id": chunk_id, "metadata": {"chunk_id": chunk_id}})

    response = client.get("/admin/api/chunks/chunk-1", headers=_admin_headers())

    assert response.status_code == 200
    payload = response.json()
    assert payload["chunk"]["chunk_id"] == "chunk-1"
    assert payload["metadata"]["chunk_role"] == "body"
    assert payload["chroma_record"]["id"] == "chunk-1"


def test_ingest_pdf_endpoint(monkeypatch) -> None:
    client = TestClient(app)
    pdf_path = "E:\\n8n to python\\sample.pdf"

    monkeypatch.setattr(api_module.Path, "exists", lambda self: True)
    monkeypatch.setattr(api_module, "_build_pdf_content_hash", lambda path, page_start=None, page_end=None: "sha256:test-pdf")
    monkeypatch.setattr(
        api_module.service,
        "ingest_text",
        lambda source: {
            "filename": source.filename,
            "source_type": source.source_type,
            "raw_text": source.raw_text,
            "metadata": source.metadata,
        },
    )

    response = client.post(
        "/ingest/pdf",
        json={"path": pdf_path, "page_start": 17, "page_end": 21},
        headers=_admin_headers(),
    )

    assert response.status_code == 200
    payload = response.json()
    assert payload["filename"] == "sample.pdf"
    assert payload["source_type"] == "pdf"
    assert payload["metadata"]["page_range"] == {"start": 17, "end": 21}


def test_admin_kg_entity_detail_endpoint(monkeypatch) -> None:
    client = TestClient(app)

    monkeypatch.setattr(
        api_module.repository,
        "get_kg_entity_detail",
        lambda entity_id: {
            "entity": {"entity_id": entity_id, "canonical_name": "Queen", "entity_type": "Queen", "source": "doc-1"},
            "assertions": [{"assertion_id": "a-1"}],
            "evidence": [{"evidence_id": "ev-1"}],
            "chunks": [{"chunk_id": "chunk-1"}],
            "documents": [{"document_id": "doc-1"}],
        },
    )

    response = client.get("/admin/api/kg/entities/queen_queen", headers=_admin_headers())

    assert response.status_code == 200
    payload = response.json()
    assert payload["entity"]["entity_id"] == "queen_queen"
    assert payload["assertions"][0]["assertion_id"] == "a-1"


def test_admin_documents_endpoint_is_paginated(monkeypatch) -> None:
    client = TestClient(app)

    monkeypatch.setattr(api_module.repository, "list_documents", lambda limit=25, offset=0: [{"document_id": "doc-1"}])
    monkeypatch.setattr(api_module.repository, "count_documents", lambda: 1)

    response = client.get("/admin/api/documents?limit=10&offset=0", headers=_admin_headers())

    assert response.status_code == 200
    payload = response.json()
    assert payload["items"][0]["document_id"] == "doc-1"
    assert payload["total"] == 1


def test_admin_chroma_parity_endpoint(monkeypatch) -> None:
    client = TestClient(app)

    monkeypatch.setattr(api_module.repository, "list_chunk_records_for_kg", lambda document_id=None, limit=5000, offset=0: [{"chunk_id": "chunk-1"}, {"chunk_id": "chunk-2"}])
    monkeypatch.setattr(api_module, "_get_chroma_payload", lambda document_id=None, limit=5000, offset=0: {"records": [{"id": "chunk-1"}, {"id": "chunk-extra"}], "total": 2, "error": None})

    response = client.get("/admin/api/chroma/parity", headers=_admin_headers())

    assert response.status_code == 200
    payload = response.json()
    assert payload["accepted_chunks"] == 2
    assert payload["vectors"] == 2
    assert payload["missing_vectors_total"] == 1
    assert payload["extra_vectors_total"] == 1


def test_agent_query_endpoint(monkeypatch, tmp_path) -> None:
    client = TestClient(app)
    _login_public_user(client, tmp_path)

    api_module.agent_service._instance = SimpleNamespace(
        query=lambda question, session_id=None, session_token=None, profile_id=None, profile_token=None, auth_user_id=None, tenant_id="shared", document_ids=None, top_k=None, query_mode=None, workspace_kind=None, trusted_tenant=False: {
            "session_id": session_id or "session-1",
            "question": question,
            "answer": "Honey bees produce honey.",
            "confidence": 0.8,
            "abstained": False,
            "abstain_reason": None,
            "citations": [{"chunk_id": "chunk-1"}],
            "supporting_entities": ["honey_honey"],
            "supporting_assertions": ["a-1"],
            "query_run_id": "run-1",
            "review_status": "unreviewed",
            "review_reason": "",
        },
    )

    response = client.post("/agent/query", json={"question": "How do bees produce honey?"}, headers=_browser_headers())

    assert response.status_code == 200
    payload = response.json()
    assert payload["query_run_id"] == "run-1"
    assert payload["abstained"] is False


def test_admin_agent_runs_endpoint(monkeypatch) -> None:
    client = TestClient(app)

    monkeypatch.setattr(
        api_module.repository,
        "list_agent_query_runs",
        lambda session_id=None, status=None, abstained=None, review_status=None, limit=100, offset=0: [{"query_run_id": "run-1", "question": "How do bees produce honey?"}],
    )
    monkeypatch.setattr(
        api_module.repository,
        "count_agent_query_runs",
        lambda session_id=None, status=None, abstained=None, review_status=None: 1,
    )

    response = client.get("/admin/api/agent/runs", headers=_admin_headers())

    assert response.status_code == 200
    payload = response.json()
    assert payload["items"][0]["query_run_id"] == "run-1"
    assert payload["total"] == 1


def test_admin_agent_run_detail_endpoint(monkeypatch) -> None:
    client = TestClient(app)

    monkeypatch.setattr(
        api_module.repository,
        "get_agent_query_detail",
        lambda query_run_id: {
            "query_run": {"query_run_id": query_run_id, "answer": "Honey bees produce honey."},
            "sources": [{"source_kind": "chunk", "source_id": "chunk-1"}],
            "reviews": [],
        },
    )

    response = client.get("/admin/api/agent/runs/run-1", headers=_admin_headers())

    assert response.status_code == 200
    payload = response.json()
    assert payload["query_run"]["query_run_id"] == "run-1"
    assert payload["sources"][0]["source_id"] == "chunk-1"


def test_agent_chat_endpoint(monkeypatch, tmp_path) -> None:
    client = TestClient(app)
    _login_public_user(client, tmp_path)

    api_module.agent_service._instance = SimpleNamespace(
        chat=lambda question, session_id=None, session_token=None, profile_id=None, profile_token=None, auth_user_id=None, tenant_id="shared", document_ids=None, top_k=None, query_mode=None, workspace_kind=None, trusted_tenant=False: {
            "session_id": session_id or "session-1",
            "messages": [{"role": "user", "content": question}],
            "answer": "Honey bees produce honey.",
            "query_run_id": "run-1",
        },
    )

    response = client.post("/agent/chat", json={"question": "How do bees produce honey?"}, headers=_browser_headers())

    assert response.status_code == 200
    payload = response.json()
    assert payload["messages"][0]["content"] == "How do bees produce honey?"


def test_public_agent_sessions_endpoint(monkeypatch, tmp_path) -> None:
    client = TestClient(app)
    monkeypatch.setattr(api_module, "_require_authenticated_public_user", lambda request: {"user": {"user_id": "user-1"}})
    api_module.repository._instance = SimpleNamespace(
        list_agent_sessions=lambda status=None, limit=100, offset=0, tenant_id=None, auth_user_id=None, workspace_kind=None: [
            {
                "session_id": "session-1",
                "title": "Honey production",
                "workspace_kind": workspace_kind or "general",
                "updated_at": "2026-04-22T12:00:00Z",
                "message_count": 4,
                "last_message_content": "How do bees produce honey?",
            }
        ],
        count_agent_sessions=lambda status=None, tenant_id=None, auth_user_id=None, workspace_kind=None: 1,
    )

    response = client.get("/agent/sessions?workspace_kind=general", headers=_browser_headers())

    assert response.status_code == 200
    payload = response.json()
    assert payload["total"] == 1
    assert payload["items"][0]["session_id"] == "session-1"
    assert payload["items"][0]["workspace_kind"] == "general"


def test_public_agent_session_activate_endpoint(monkeypatch, tmp_path) -> None:
    client = TestClient(app)
    monkeypatch.setattr(api_module, "_require_authenticated_public_user", lambda request: {"user": {"user_id": "user-1"}})
    saved = {}
    captured = {}
    api_module.repository._instance = SimpleNamespace(
        get_agent_session=lambda session_id, tenant_id=None: {
            "session_id": session_id,
            "auth_user_id": "user-1",
            "profile_id": "profile-1",
            "workspace_kind": "general",
            "title": "Honey production",
            "status": "active",
            "updated_at": "2026-04-22T12:00:00Z",
        },
        set_agent_session_token=lambda session_id, token: saved.update({"session_id": session_id, "token": token}),
        get_agent_session_memory=lambda session_id, tenant_id=None, auth_user_id=None, profile_id=None: captured.update(
            {"memory_auth_user_id": auth_user_id, "memory_profile_id": profile_id}
        ) or {"session_id": session_id, "summary_text": "goal: honey"},
        list_agent_messages=lambda session_id, limit=20, tenant_id=None, auth_user_id=None, profile_id=None: captured.update(
            {"messages_auth_user_id": auth_user_id, "messages_profile_id": profile_id}
        ) or [
            {"message_id": "m1", "role": "user", "content": "How do bees produce honey?", "metadata_json": {}, "created_at": "2026-04-22T12:00:00Z"}
        ],
    )

    response = client.post("/agent/sessions/session-1/activate", headers=_browser_headers())

    assert response.status_code == 200
    payload = response.json()
    assert payload["session"]["session_id"] == "session-1"
    assert payload["memory"]["summary_text"] == "goal: honey"
    assert payload["messages"][0]["content"] == "How do bees produce honey?"
    assert saved["session_id"] == "session-1"
    assert captured["memory_auth_user_id"] == "user-1"
    assert captured["messages_auth_user_id"] == "user-1"


def test_admin_agent_run_review_endpoint(monkeypatch) -> None:
    client = TestClient(app)
    saved = {}

    monkeypatch.setattr(
        api_module.repository,
        "get_agent_query_detail",
        lambda query_run_id: {"query_run": {"query_run_id": query_run_id}, "sources": [], "reviews": []},
    )
    monkeypatch.setattr(
        api_module.repository,
        "save_agent_answer_review",
        lambda query_run_id, decision, reviewer="admin", notes=None, payload=None: saved.update(
            {"query_run_id": query_run_id, "decision": decision, "reviewer": reviewer, "notes": notes}
        ),
    )

    response = client.post("/admin/api/agent/runs/run-1/review", json={"decision": "approved", "notes": "looks grounded"}, headers=_admin_headers())

    assert response.status_code == 200
    assert saved["decision"] == "approved"


def test_admin_agent_reviews_endpoint(monkeypatch) -> None:
    client = TestClient(app)

    monkeypatch.setattr(
        api_module.repository,
        "list_agent_answer_reviews",
        lambda decision=None, limit=100, offset=0: [{"query_run_id": "run-1", "decision": "approved"}],
    )
    monkeypatch.setattr(api_module.repository, "count_agent_answer_reviews", lambda decision=None: 1)

    response = client.get("/admin/api/agent/reviews", headers=_admin_headers())

    assert response.status_code == 200
    assert response.json()["items"][0]["decision"] == "approved"


def test_admin_agent_metrics_endpoint(monkeypatch) -> None:
    client = TestClient(app)

    monkeypatch.setattr(
        api_module.repository,
        "get_agent_metrics",
        lambda: {
            "total_runs": 4,
            "needs_review": 1,
            "abstentions": 1,
            "approved": 2,
            "no_citation_answers": 0,
            "avg_confidence": 0.72,
            "avg_latency_ms": 812.3,
            "max_latency_ms": 1400.0,
        },
    )

    response = client.get("/admin/api/agent/metrics", headers=_admin_headers())

    assert response.status_code == 200
    payload = response.json()
    assert payload["total_runs"] == 4
    assert payload["avg_latency_ms"] == 812.3


def test_admin_agent_run_replay_endpoint(monkeypatch) -> None:
    client = TestClient(app)

    monkeypatch.setattr(
        api_module.repository,
        "get_agent_query_detail",
        lambda query_run_id: {
            "query_run": {
                "query_run_id": query_run_id,
                "question": "How do bees produce honey?",
                "session_id": "session-1",
                "tenant_id": "shared",
            },
            "sources": [{"source_kind": "chunk", "document_id": "doc-1"}],
            "reviews": [],
        },
    )
    monkeypatch.setattr(
        api_module.agent_service,
        "chat",
        lambda question, session_id=None, auth_user_id=None, tenant_id="shared", document_ids=None, top_k=None, query_mode=None, trusted_tenant=False, trusted_session_reuse=False: {
            "session_id": session_id or "session-2",
            "query_run_id": "run-2",
            "answer": "Honey bees produce honey.",
        },
    )

    response = client.post("/admin/api/agent/runs/run-1/replay", json={"reuse_session": False}, headers=_admin_headers())

    assert response.status_code == 200
    assert response.json()["query_run_id"] == "run-2"
