from __future__ import annotations

import re
from typing import Any

from src.bee_ingestion.query_router import classify_question_fallback


def normalize_query(question: str) -> str:
    return re.sub(r"\s+", " ", question.replace("\x00", " ").strip())


def build_session_title(question: str) -> str:
    return question[:80]


def classify_question(question: str) -> str:
    return classify_question_fallback(question)


def resolve_query_top_k(
    question_type: str,
    requested_top_k: int | None,
    runtime_config: dict[str, Any],
    router_top_k: int | None = None,
    router_source: str | None = None,
) -> tuple[int, str]:
    if requested_top_k is not None:
        return max(1, min(int(requested_top_k), runtime_config["max_top_k"])), "manual"
    if router_top_k is not None and router_source and router_source.startswith("router"):
        return max(1, min(int(router_top_k), runtime_config["max_top_k"])), router_source
    if runtime_config.get("dynamic_top_k_enabled", True):
        policy_key = f"{question_type}_top_k"
        policy_value = runtime_config.get(policy_key, runtime_config["default_top_k"])
        return max(1, min(int(policy_value), runtime_config["max_top_k"])), f"dynamic:{question_type}"
    return max(1, min(int(runtime_config["default_top_k"]), runtime_config["max_top_k"])), "default"


def select_retrieval_plan(
    question_type: str,
    top_k: int,
    runtime_config: dict[str, Any],
    routing: dict[str, Any] | None = None,
) -> dict[str, Any]:
    routing = routing or {}
    max_search_k = runtime_config["max_search_k"]
    prefer_assets = bool(routing.get("requires_visual")) or question_type == "visual_lookup"
    document_spread = str(routing.get("document_spread") or "").strip().lower()
    if document_spread not in {"single", "few", "broad"}:
        if question_type == "comparison":
            document_spread = "few"
        elif question_type in {"fact", "explanation", "procedure"}:
            document_spread = "few"
        else:
            document_spread = "single"
    if question_type == "visual_lookup":
        return {
            "mode": "multimodal_focus",
            "search_k": min(max(top_k * 2, top_k + 4), max_search_k),
            "select_k": min(max(3, top_k), runtime_config["max_context_chunks"]),
            "expand_neighbors": False,
            "kg_search": False,
            "graph_expand": False,
            "prefer_assets": True,
            "asset_vector_search": True,
            "document_spread": "few" if document_spread == "single" else document_spread,
        }
    if question_type in {"definition", "fact", "source_lookup"}:
        return {
            "mode": "hybrid_visual_support" if prefer_assets else "hybrid_kg_support",
            "search_k": min(max(top_k * (3 if prefer_assets else 2), top_k + (6 if prefer_assets else 4)), max_search_k),
            "select_k": min(max(4, top_k), runtime_config["max_context_chunks"]),
            "expand_neighbors": False,
            "kg_search": runtime_config["kg_search_limit"] > 0,
            "graph_expand": False,
            "prefer_assets": prefer_assets,
            "asset_vector_search": prefer_assets,
            "document_spread": document_spread,
        }
    if question_type == "comparison":
        return {
            "mode": "hybrid_compare",
            "search_k": min(max(top_k * 2, top_k + 4), max_search_k),
            "select_k": min(max(5, top_k), runtime_config["max_context_chunks"]),
            "expand_neighbors": True,
            "kg_search": runtime_config["kg_search_limit"] > 0,
            "graph_expand": runtime_config["graph_expansion_limit"] > 0,
            "prefer_assets": prefer_assets,
            "asset_vector_search": prefer_assets,
            "document_spread": "few" if document_spread == "single" else document_spread,
        }
    return {
        "mode": "neighbor_visual_expansion" if prefer_assets else "neighbor_expansion",
        "search_k": min(max(top_k * (3 if prefer_assets else 2), top_k + (6 if prefer_assets else 3)), max_search_k),
        "select_k": min(max(4, top_k), runtime_config["max_context_chunks"]),
        "expand_neighbors": True,
        "kg_search": runtime_config["kg_search_limit"] > 0,
        "graph_expand": question_type in {"explanation", "procedure"} and runtime_config["graph_expansion_limit"] > 0,
        "prefer_assets": prefer_assets,
        "asset_vector_search": prefer_assets,
        "document_spread": document_spread,
    }
