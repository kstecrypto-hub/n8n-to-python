from fastapi.testclient import TestClient
from pathlib import Path
from uuid import uuid4

from src.bee_ingestion.api import app
from src.bee_ingestion import api as api_module
from src.bee_ingestion.auth_store import AuthStore


def _test_auth_dsn() -> str:
    return "postgresql://bee_auth:bee_auth@127.0.0.1:35433/bee_identity"


def _workspace_auth_hint(name: str) -> Path:
    root = Path(r"E:\n8n to python\.tmp\pytest-auth")
    root.mkdir(parents=True, exist_ok=True)
    return root / f"{name}-{uuid4().hex}.pgschema"


def _configure_auth_store(tmp_path) -> None:
    del tmp_path
    api_module.auth_store._instance = AuthStore(_workspace_auth_hint("public-auth"), dsn=_test_auth_dsn())


def _provision_user(email: str = "beekeeper@example.com", password: str = "very-secure-hive-password") -> None:
    api_module.auth_store.create_user(email, password, display_name="Bee Keeper")


def test_auth_registration_is_disabled_by_default(tmp_path) -> None:
    _configure_auth_store(tmp_path)
    client = TestClient(app)

    response = client.post(
        "/auth/register",
        json={
            "email": "beekeeper@example.com",
            "password": "very-secure-hive-password",
            "display_name": "Bee Keeper",
        },
    )

    assert response.status_code == 403
    assert response.json()["detail"] == "Self-service registration is disabled"


def test_auth_login_and_session_endpoints(tmp_path) -> None:
    _configure_auth_store(tmp_path)
    _provision_user()
    client = TestClient(app)

    response = client.post(
        "/auth/login",
        json={
            "email": "beekeeper@example.com",
            "password": "very-secure-hive-password",
        },
    )

    assert response.status_code == 200
    payload = response.json()
    assert payload["authenticated"] is True
    assert payload["user"]["email"] == "beekeeper@example.com"

    session_response = client.get("/auth/session")
    assert session_response.status_code == 200
    assert session_response.json()["authenticated"] is True


def test_auth_login_rejects_bad_credentials(tmp_path) -> None:
    _configure_auth_store(tmp_path)
    _provision_user()
    client = TestClient(app)

    response = client.post(
        "/auth/login",
        json={
            "email": "beekeeper@example.com",
            "password": "wrong-password-value",
        },
    )

    assert response.status_code == 401
    assert response.json()["detail"] == "Invalid credentials"


def test_agent_session_requires_login() -> None:
    client = TestClient(app)

    response = client.get("/agent/session")

    assert response.status_code == 401
    assert response.json()["detail"] == "Login required"


def test_auth_logout_clears_auth_state(tmp_path) -> None:
    _configure_auth_store(tmp_path)
    _provision_user()
    client = TestClient(app)

    client.post("/auth/login", json={"email": "beekeeper@example.com", "password": "very-secure-hive-password"})

    logout_response = client.post("/auth/logout")
    assert logout_response.status_code == 200
    assert logout_response.json()["ok"] is True

    session_response = client.get("/auth/session")
    assert session_response.status_code == 200
    assert session_response.json()["authenticated"] is False
