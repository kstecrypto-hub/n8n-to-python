from __future__ import annotations

import ast
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]


def fail(message: str, failures: list[str]) -> None:
    failures.append(message)


def read(path: str) -> str:
    return (ROOT / path).read_text(encoding="utf-8")


def line_count(path: str) -> int:
    return len(read(path).splitlines())


def check_api_residue(failures: list[str]) -> None:
    api_text = read("src/bee_ingestion/api.py")
    forbidden_names = [
        "_resolve_evaluation_path",
        "_env_key_for_field",
        "_read_env_map",
        "_serialize_env_value",
        "_deserialize_env_value",
        "_system_group_fields",
        "_is_system_secret_field",
        "_display_workspace_path",
        "_build_system_config_payload",
        "_ontology_path",
        "_build_ontology_payload",
        "_first_configured_source",
        "_write_system_group_config",
        "_reset_system_group_config",
        "_normalize_memory_clear_sections",
        "_apply_memory_clear_rules",
        "_coerce_and_refresh_session_memory",
        "_coerce_and_refresh_profile_summary",
        "_normalize_editor_type",
        "_parse_editor_page_number",
        "_load_editor_record",
        "_save_editor_record",
        "_delete_editor_record",
        "_resync_editor_record",
        "_normalize_admin_database_key",
        "_admin_repository_for_database",
        "_enforce_admin_database_scope",
        "_get_chroma_payload",
        "_get_chroma_parity",
        "protect_control_plane",
        "_control_plane_permissions_for_request",
        "_json_safe",
    ]
    for name in forbidden_names:
        if name in api_text:
            fail(f"api.py still contains old admin/control-plane residue: {name}", failures)


def check_service_delegates(failures: list[str]) -> None:
    service_tree = ast.parse(read("src/bee_ingestion/service.py"))
    expected = {
        "rebuild_document": "rebuild_document_stage.rebuild_document",
        "repair_document": "repair_document_stage.repair_document",
        "resume_document_ingest": "resume_ingest_stage.resume_document_ingest",
        "revalidate_document": "revalidate_document_stage.revalidate_document",
        "reindex_document": "reindex_document_stage.reindex_document",
        "delete_document": "delete_document_stage.delete_document",
        "reset_pipeline_data": "reset_pipeline_data_stage.reset_pipeline_data",
        "reset_ingestion_data": "reset_ingestion_data_stage.reset_ingestion_data",
        "reprocess_kg": "reprocess_kg_stage.reprocess_kg",
        "replay_quarantined_kg": "replay_quarantined_kg_stage.replay_quarantined_kg",
    }
    class_node = next((node for node in service_tree.body if isinstance(node, ast.ClassDef) and node.name == "IngestionService"), None)
    if class_node is None:
        fail("IngestionService class not found", failures)
        return
    methods = {node.name: node for node in class_node.body if isinstance(node, ast.FunctionDef)}
    for name, target in expected.items():
        stage_name = target.split(".")[0].replace("_stage", "")
        stage_path = ROOT / "src" / "bee_ingestion" / "offline_pipeline" / "stages" / f"{stage_name}.py"
        if not stage_path.exists():
            fail(f"missing offline stage module for {name}: {stage_path.relative_to(ROOT)}", failures)
        method = methods.get(name)
        if method is None:
            fail(f"IngestionService missing compatibility delegate {name}", failures)
            continue
        non_doc_body = [stmt for stmt in method.body if not isinstance(stmt, ast.Expr) or not isinstance(getattr(stmt, "value", None), ast.Constant)]
        if len(non_doc_body) != 1 or not isinstance(non_doc_body[0], ast.Return):
            fail(f"IngestionService.{name} is not a single return delegate", failures)
            continue
        segment = ast.get_source_segment(read("src/bee_ingestion/service.py"), non_doc_body[0]) or ""
        if target not in segment:
            fail(f"IngestionService.{name} does not delegate to {target}", failures)


def check_frontend(failures: list[str]) -> None:
    path = ROOT / "frontend" / "src" / "features" / "admin" / "AdminExtendedSections.tsx"
    if not path.exists():
        return
    count = line_count("frontend/src/features/admin/AdminExtendedSections.tsx")
    if count > 80:
        fail(f"AdminExtendedSections.tsx is too large for a composition file: {count} lines", failures)


def check_guard_scripts(failures: list[str]) -> None:
    if not (ROOT / "scripts" / "scan_hardcoded_secrets.py").exists():
        fail("missing scripts/scan_hardcoded_secrets.py", failures)
    gitignore = read(".gitignore") if (ROOT / ".gitignore").exists() else ""
    for pattern in [".env", ".env.*", "*.env", "!.env.example"]:
        if pattern not in gitignore:
            fail(f".gitignore missing env pattern: {pattern}", failures)


def main() -> int:
    failures: list[str] = []
    check_api_residue(failures)
    check_service_delegates(failures)
    check_frontend(failures)
    check_guard_scripts(failures)
    if failures:
        print("Architecture completion check failed:")
        for failure in failures:
            print(f"- {failure}")
        return 1
    print("Architecture completion checks passed.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
