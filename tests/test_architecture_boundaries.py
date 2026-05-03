import ast
import re
import subprocess
import sys
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]


def _python_imports(path: Path) -> set[str]:
    tree = ast.parse(path.read_text(encoding="utf-8"))
    modules: set[str] = set()
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            for alias in node.names:
                modules.add(alias.name)
        elif isinstance(node, ast.ImportFrom):
            if node.module:
                modules.add(node.module)
    return modules


def _ts_imports(path: Path) -> set[str]:
    pattern = re.compile(r'^\s*import\s+.*?\s+from\s+[\'"]([^\'"]+)[\'"]', re.MULTILINE)
    return set(pattern.findall(path.read_text(encoding="utf-8")))


def test_http_routes_do_not_import_repository_or_storage_directly() -> None:
    route_dir = ROOT / "src" / "bee_ingestion" / "http_api" / "routes"
    forbidden_prefixes = ("src.bee_ingestion.storage",)
    forbidden_modules = {
        "src.bee_ingestion.repository",
        "src.bee_ingestion.chroma_store",
    }

    for path in route_dir.rglob("*.py"):
        if path.name == "__init__.py":
            continue
        imports = _python_imports(path)
        assert not any(module in forbidden_modules for module in imports), path
        assert not any(module.startswith(forbidden_prefixes) for module in imports), path


def test_prompt_builder_has_no_storage_repository_vector_or_llm_imports() -> None:
    path = ROOT / "src" / "bee_ingestion" / "agent_runtime" / "prompt_builder.py"
    imports = _python_imports(path)

    forbidden_prefixes = (
        "src.bee_ingestion.storage",
        "src.bee_ingestion.chroma_store",
        "openai",
    )
    forbidden_modules = {"src.bee_ingestion.repository"}

    assert not any(module in forbidden_modules for module in imports)
    assert not any(module.startswith(forbidden_prefixes) for module in imports)


def test_verifier_does_not_import_prompt_builder() -> None:
    path = ROOT / "src" / "bee_ingestion" / "agent_runtime" / "verifier.py"
    imports = _python_imports(path)

    assert "src.bee_ingestion.agent_runtime.prompt_builder" not in imports


def test_offline_pipeline_does_not_import_http_api() -> None:
    pipeline_dir = ROOT / "src" / "bee_ingestion" / "offline_pipeline"

    for path in pipeline_dir.rglob("*.py"):
        if path.name == "__init__.py":
            continue
        imports = _python_imports(path)
        assert not any(module.startswith("src.bee_ingestion.http_api") for module in imports), path
        assert "src.bee_ingestion.agent_runtime.prompt_builder" not in imports, path


def test_storage_modules_do_not_import_http_api_frontend_or_prompt_modules() -> None:
    storage_dir = ROOT / "src" / "bee_ingestion" / "storage"

    for path in storage_dir.rglob("*.py"):
        if path.name == "__init__.py":
            continue
        imports = _python_imports(path)
        assert not any(module.startswith("src.bee_ingestion.http_api") for module in imports), path
        assert not any(module.startswith("frontend") for module in imports), path
        assert "src.bee_ingestion.agent_runtime.prompt_builder" not in imports, path
        assert "src.bee_ingestion.agent_runtime.verifier" not in imports, path


def test_repository_extracted_persistence_surface_is_compatibility_only() -> None:
    path = ROOT / "src" / "bee_ingestion" / "repository.py"
    source = path.read_text(encoding="utf-8")
    module = ast.parse(source)
    repository_class = next(
        node for node in module.body if isinstance(node, ast.ClassDef) and node.name == "Repository"
    )
    source_lines = source.splitlines()

    compatibility_methods = {
        "list_admin_relations",
        "get_admin_relation_schema",
        "count_admin_relation_rows",
        "list_admin_relation_rows",
        "insert_admin_relation_row",
        "update_admin_relation_row",
        "delete_admin_relation_row",
        "execute_admin_sql",
        "get_agent_runtime_secret",
        "create_agent_profile",
        "set_agent_profile_token",
        "verify_agent_profile_token",
        "get_agent_profile",
        "get_agent_profile_by_auth_user",
        "save_agent_profile",
        "update_agent_profile_record",
        "list_agent_profiles",
        "count_agent_profiles",
        "delete_agent_profile",
        "create_agent_session",
        "set_agent_session_token",
        "bind_agent_session_auth_user",
        "verify_agent_session_token",
        "claim_agent_session",
        "release_agent_session",
        "attach_agent_profile_to_session",
        "get_agent_session",
        "get_agent_session_memory",
        "save_agent_session_memory",
        "update_agent_session_memory_record",
        "update_agent_session",
        "update_agent_session_record",
        "delete_agent_session",
        "get_agent_runtime_config",
        "save_agent_runtime_config",
        "save_agent_runtime_secret",
        "delete_agent_runtime_config",
        "delete_agent_runtime_secret",
        "save_agent_message",
        "list_agent_messages",
        "get_latest_agent_session_scope",
        "count_agent_sessions",
        "list_agent_sessions",
        "save_agent_query_run",
        "save_agent_query_sources",
        "count_agent_query_runs",
        "list_agent_query_runs",
        "get_agent_query_detail",
        "save_agent_answer_review",
        "list_agent_answer_reviews",
        "count_agent_answer_reviews",
        "list_agent_query_patterns",
        "count_agent_query_patterns",
        "get_agent_query_pattern",
        "get_cached_query_embedding",
        "save_cached_query_embedding",
        "touch_cached_query_embedding_hit",
        "save_agent_query_pattern_route",
        "touch_agent_query_pattern_route_hit",
        "update_agent_query_pattern",
        "delete_agent_query_pattern",
    }
    allowed_remaining_owners = {
        "build_query_pattern",
        "list_review_chunk_records",
        "save_chunk_review_run",
        "count_chunk_review_runs",
        "list_chunk_review_runs",
        "update_chunk_record_admin",
        "search_kg_entities_for_query",
    }
    category_tokens = (
        "admin",
        "session",
        "profile",
        "message",
        "memory",
        "query",
        "review",
        "runtime",
        "secret",
    )

    found_category_methods: set[str] = set()
    for node in repository_class.body:
        if not isinstance(node, ast.FunctionDef):
            continue
        if node.name.startswith("_"):
            continue
        if not any(token in node.name for token in category_tokens):
            continue
        found_category_methods.add(node.name)
        if node.name in compatibility_methods:
            method_source = "\n".join(source_lines[node.lineno - 1 : node.end_lineno])
            assert any(
                marker in method_source
                for marker in (
                    "admin_inspection_store.",
                    "agent_profile_store.",
                    "agent_session_store.",
                    "agent_message_store.",
                    "agent_trace_store.",
                    "agent_feedback_store.",
                    "memory_store.",
                    "runtime_config_store.",
                )
            ), node.name
        else:
            assert node.name in allowed_remaining_owners, node.name

    assert found_category_methods == compatibility_methods | allowed_remaining_owners


def test_admin_page_does_not_import_low_level_transport_modules() -> None:
    path = ROOT / "frontend" / "src" / "features" / "admin" / "AdminPage.tsx"
    imports = _ts_imports(path)

    assert not any(module.startswith("@/lib/api/") for module in imports)
    assert not any(module.startswith("@/features/admin/api/") for module in imports)


def test_admin_extended_sections_is_composition_only() -> None:
    path = ROOT / "frontend" / "src" / "features" / "admin" / "AdminExtendedSections.tsx"
    assert len(path.read_text(encoding="utf-8").splitlines()) <= 80


def test_architecture_completion_script_passes() -> None:
    result = subprocess.run(
        [sys.executable, str(ROOT / "scripts" / "verify_architecture_completion.py")],
        cwd=ROOT,
        text=True,
        capture_output=True,
        check=False,
    )
    assert result.returncode == 0, result.stdout + result.stderr


def test_secret_scan_script_passes() -> None:
    result = subprocess.run(
        [sys.executable, str(ROOT / "scripts" / "scan_hardcoded_secrets.py"), "--list-tracked-env"],
        cwd=ROOT,
        text=True,
        capture_output=True,
        check=False,
    )
    assert result.returncode == 0, result.stdout + result.stderr
