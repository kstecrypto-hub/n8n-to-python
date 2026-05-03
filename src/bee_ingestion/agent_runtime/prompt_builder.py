"""Prompt assembly for the deterministic agent runtime.

This module owns only prompt-stage concerns:
- shaping already-resolved evidence into prompt payloads
- budgeting and trimming prompt content
- assembling user payloads/messages

It does not retrieve data, mutate repositories, or call models.
"""

from __future__ import annotations

import json
import re
from typing import Any, Callable

from src.bee_ingestion.agent_runtime.contracts import AgentContextBundle, AgentPromptBundle
from src.bee_ingestion.chunking import sanitize_text


class PromptBuilder:
    def __init__(
        self,
        *,
        budget_profile_summary: Callable[[dict[str, Any] | None, int], dict[str, Any] | None],
        budget_session_summary: Callable[[dict[str, Any] | None, int], dict[str, Any] | None],
        filter_profile_summary: Callable[..., dict[str, Any] | None],
        filter_session_summary: Callable[..., dict[str, Any] | None],
        refresh_memory_summary: Callable[[dict[str, Any]], dict[str, Any]],
        refresh_profile_summary: Callable[[dict[str, Any]], dict[str, Any]],
        trusted_asset_grounding_text: Callable[[dict[str, Any]], str],
        trusted_sensor_grounding_text: Callable[[dict[str, Any]], str],
        extract_citation_excerpt: Callable[[str, str, int], str],
    ) -> None:
        self._budget_profile_summary = budget_profile_summary
        self._budget_session_summary = budget_session_summary
        self._filter_profile_summary = filter_profile_summary
        self._filter_session_summary = filter_session_summary
        self._refresh_memory_summary = refresh_memory_summary
        self._refresh_profile_summary = refresh_profile_summary
        self._trusted_asset_grounding_text = trusted_asset_grounding_text
        self._trusted_sensor_grounding_text = trusted_sensor_grounding_text
        self._extract_citation_excerpt = extract_citation_excerpt

    def build_prompt_bundle(
        self,
        *,
        question: str,
        normalized_query: str,
        prior_messages: list[dict[str, Any]],
        profile_summary: dict[str, Any] | None,
        session_memory: dict[str, Any] | None,
        bundle: AgentContextBundle,
        question_type: str,
        runtime_config: dict[str, Any],
        workspace_kind: str = "general",
    ) -> AgentPromptBundle:
        prior_context = _budget_prior_messages(prior_messages, runtime_config["history_char_budget"])
        profile_summary_payload = self._budget_profile_summary(profile_summary, runtime_config["profile_char_budget"])
        session_summary = self._budget_session_summary(session_memory, runtime_config["memory_char_budget"])
        pre_gated_memory_counts = {
            "profile_topics": len((profile_summary_payload or {}).get("recurring_topics") or []),
            "profile_goals": len((profile_summary_payload or {}).get("learning_goals") or []),
            "session_facts": len((session_summary or {}).get("stable_facts") or []),
            "open_threads": len((session_summary or {}).get("open_threads") or []),
            "resolved_threads": len((session_summary or {}).get("resolved_threads") or []),
        }
        profile_summary_payload = self._filter_profile_summary(
            profile_summary_payload,
            question=question,
            normalized_query=normalized_query,
            workspace_kind=workspace_kind,
        )
        session_summary = self._filter_session_summary(
            session_summary,
            question=question,
            normalized_query=normalized_query,
            bundle=bundle,
        )
        prompt_bundle = AgentPromptBundle(
            prior_context=prior_context,
            profile_summary=profile_summary_payload,
            session_summary=session_summary,
            chunk_payload=_budget_chunk_payload(bundle.chunks, runtime_config["chunk_char_budget"], self._trusted_asset_grounding_text),
            asset_payload=_budget_asset_payload(bundle.assets, runtime_config["asset_char_budget"], self._trusted_asset_grounding_text),
            sensor_payload=_budget_sensor_payload(bundle.sensor_rows, runtime_config["sensor_char_budget"], self._trusted_sensor_grounding_text),
            assertion_payload=_budget_assertion_payload(bundle.assertions, runtime_config["assertion_char_budget"]),
            entity_payload=_budget_entity_payload(bundle.entities, runtime_config["entity_char_budget"]),
            graph_chain_payload=_budget_graph_chain_payload(
                getattr(bundle, "graph_chains", []) or [],
                runtime_config["graph_char_budget"],
            ),
            evidence_payload=_budget_evidence_payload(bundle.evidence, runtime_config["evidence_char_budget"]),
            stats={},
        )
        original_counts = {
            "history_messages": len(prompt_bundle.prior_context),
            "profile_topics": len((prompt_bundle.profile_summary or {}).get("recurring_topics") or []),
            "session_facts": len((prompt_bundle.session_summary or {}).get("stable_facts") or []),
            "chunks": len(prompt_bundle.chunk_payload),
            "assets": len(prompt_bundle.asset_payload),
            "sensor_rows": len(prompt_bundle.sensor_payload),
            "assertions": len(prompt_bundle.assertion_payload),
            "entities": len(prompt_bundle.entity_payload),
            "graph_chains": len(prompt_bundle.graph_chain_payload),
            "evidence": len(prompt_bundle.evidence_payload),
        }
        prompt_bundle = self.fit_prompt_bundle(
            question=question,
            normalized_query=normalized_query,
            question_type=question_type,
            prompt_bundle=prompt_bundle,
            runtime_config=runtime_config,
        )
        prompt_bundle.stats = {
            "question": question,
            "normalized_query": normalized_query,
            "question_type": question_type,
            "budgets": {
                "prompt_chars": runtime_config["prompt_char_budget"],
                "history_chars": runtime_config["history_char_budget"],
                "profile_chars": runtime_config["profile_char_budget"],
                "memory_chars": runtime_config["memory_char_budget"],
                "chunk_chars": runtime_config["chunk_char_budget"],
                "asset_chars": runtime_config["asset_char_budget"],
                "sensor_chars": runtime_config["sensor_char_budget"],
                "assertion_chars": runtime_config["assertion_char_budget"],
                "entity_chars": runtime_config["entity_char_budget"],
                "graph_chars": runtime_config["graph_char_budget"],
                "evidence_chars": runtime_config["evidence_char_budget"],
            },
            "counts": {
                "history_messages": len(prompt_bundle.prior_context),
                "profile_topics": len((prompt_bundle.profile_summary or {}).get("recurring_topics") or []),
                "session_facts": len((prompt_bundle.session_summary or {}).get("stable_facts") or []),
                "chunks": len(prompt_bundle.chunk_payload),
                "assets": len(prompt_bundle.asset_payload),
                "sensor_rows": len(prompt_bundle.sensor_payload),
                "assertions": len(prompt_bundle.assertion_payload),
                "entities": len(prompt_bundle.entity_payload),
                "graph_chains": len(prompt_bundle.graph_chain_payload),
                "evidence": len(prompt_bundle.evidence_payload),
            },
            "trimmed": {
                "history_messages": max(0, original_counts["history_messages"] - len(prompt_bundle.prior_context)),
                "profile_topics": max(0, original_counts["profile_topics"] - len((prompt_bundle.profile_summary or {}).get("recurring_topics") or [])),
                "session_facts": max(0, original_counts["session_facts"] - len((prompt_bundle.session_summary or {}).get("stable_facts") or [])),
                "chunks": max(0, original_counts["chunks"] - len(prompt_bundle.chunk_payload)),
                "assets": max(0, original_counts["assets"] - len(prompt_bundle.asset_payload)),
                "sensor_rows": max(0, original_counts["sensor_rows"] - len(prompt_bundle.sensor_payload)),
                "assertions": max(0, original_counts["assertions"] - len(prompt_bundle.assertion_payload)),
                "entities": max(0, original_counts["entities"] - len(prompt_bundle.entity_payload)),
                "graph_chains": max(0, original_counts["graph_chains"] - len(prompt_bundle.graph_chain_payload)),
                "evidence": max(0, original_counts["evidence"] - len(prompt_bundle.evidence_payload)),
            },
            "memory_relevance": {
                "profile_topics": max(0, pre_gated_memory_counts["profile_topics"] - len((prompt_bundle.profile_summary or {}).get("recurring_topics") or [])),
                "profile_goals": max(0, pre_gated_memory_counts["profile_goals"] - len((prompt_bundle.profile_summary or {}).get("learning_goals") or [])),
                "session_facts": max(0, pre_gated_memory_counts["session_facts"] - len((prompt_bundle.session_summary or {}).get("stable_facts") or [])),
                "open_threads": max(0, pre_gated_memory_counts["open_threads"] - len((prompt_bundle.session_summary or {}).get("open_threads") or [])),
                "resolved_threads": max(0, pre_gated_memory_counts["resolved_threads"] - len((prompt_bundle.session_summary or {}).get("resolved_threads") or [])),
            },
            "estimated_chars": {
                "history": _estimate_json_chars(prompt_bundle.prior_context),
                "profile_summary": _estimate_json_chars(prompt_bundle.profile_summary or {}),
                "session_summary": _estimate_json_chars(prompt_bundle.session_summary or {}),
                "chunks": _estimate_json_chars(prompt_bundle.chunk_payload),
                "assets": _estimate_json_chars(prompt_bundle.asset_payload),
                "sensor_rows": _estimate_json_chars(prompt_bundle.sensor_payload),
                "assertions": _estimate_json_chars(prompt_bundle.assertion_payload),
                "entities": _estimate_json_chars(prompt_bundle.entity_payload),
                "graph_chains": _estimate_json_chars(prompt_bundle.graph_chain_payload),
                "evidence": _estimate_json_chars(prompt_bundle.evidence_payload),
                "total_prompt": self.estimate_prompt_chars(
                    question=question,
                    normalized_query=normalized_query,
                    question_type=question_type,
                    prompt_bundle=prompt_bundle,
                    runtime_config=runtime_config,
                ),
            },
            "final_ids": {
                "chunk_ids": [item["chunk_id"] for item in prompt_bundle.chunk_payload],
                "asset_ids": [item["asset_id"] for item in prompt_bundle.asset_payload],
                "sensor_row_ids": [item["sensor_row_id"] for item in prompt_bundle.sensor_payload],
                "assertion_ids": [item["assertion_id"] for item in prompt_bundle.assertion_payload],
                "entity_ids": [item["entity_id"] for item in prompt_bundle.entity_payload],
                "graph_chain_ids": [item["chain_id"] for item in prompt_bundle.graph_chain_payload],
                "evidence_ids": [item["evidence_id"] for item in prompt_bundle.evidence_payload],
            },
            "profile_used": bool(prompt_bundle.profile_summary),
            "session_memory_used": bool(prompt_bundle.session_summary),
        }
        return prompt_bundle

    def fit_prompt_bundle(
        self,
        *,
        question: str,
        normalized_query: str,
        question_type: str,
        prompt_bundle: AgentPromptBundle,
        runtime_config: dict[str, Any],
    ) -> AgentPromptBundle:
        while self.estimate_prompt_chars(
            question=question,
            normalized_query=normalized_query,
            question_type=question_type,
            prompt_bundle=prompt_bundle,
            runtime_config=runtime_config,
        ) > runtime_config["prompt_char_budget"]:
            if prompt_bundle.profile_summary and (prompt_bundle.profile_summary.get("recurring_topics") or prompt_bundle.profile_summary.get("persistent_constraints")):
                if self.shrink_prompt_bundle_once(prompt_bundle):
                    continue
            if prompt_bundle.session_summary and (prompt_bundle.session_summary.get("open_threads") or prompt_bundle.session_summary.get("stable_facts")):
                if self.shrink_prompt_bundle_once(prompt_bundle):
                    continue
            if prompt_bundle.graph_chain_payload:
                prompt_bundle.graph_chain_payload.pop()
                continue
            if prompt_bundle.evidence_payload:
                prompt_bundle.evidence_payload.pop()
                continue
            if prompt_bundle.sensor_payload:
                prompt_bundle.sensor_payload.pop()
                continue
            if prompt_bundle.asset_payload:
                prompt_bundle.asset_payload.pop()
                continue
            if prompt_bundle.entity_payload:
                prompt_bundle.entity_payload.pop()
                continue
            if prompt_bundle.assertion_payload:
                prompt_bundle.assertion_payload.pop()
                continue
            if len(prompt_bundle.chunk_payload) > 1:
                prompt_bundle.chunk_payload.pop()
                continue
            if len(prompt_bundle.prior_context) > 1:
                prompt_bundle.prior_context.pop(0)
                continue
            if self.truncate_prompt_bundle_once(prompt_bundle):
                continue
            break
        return prompt_bundle

    def shrink_prompt_bundle_once(self, prompt_bundle: AgentPromptBundle) -> bool:
        if prompt_bundle.profile_summary:
            profile = dict(prompt_bundle.profile_summary)
            recurring_topics = list(profile.get("recurring_topics") or [])
            if recurring_topics:
                recurring_topics.pop()
                profile["recurring_topics"] = recurring_topics
                prompt_bundle.profile_summary = self._refresh_profile_summary(profile)
                return True
            persistent_constraints = list(profile.get("persistent_constraints") or [])
            if persistent_constraints:
                persistent_constraints.pop()
                profile["persistent_constraints"] = persistent_constraints
                prompt_bundle.profile_summary = self._refresh_profile_summary(profile)
                return True
        if prompt_bundle.session_summary:
            summary = dict(prompt_bundle.session_summary)
            open_threads = list(summary.get("open_threads") or [])
            if open_threads:
                open_threads.pop()
                summary["open_threads"] = open_threads
                prompt_bundle.session_summary = self._refresh_memory_summary(summary)
                return True
            facts = list(summary.get("stable_facts") or [])
            if len(facts) > 1:
                facts.pop()
                summary["stable_facts"] = facts
                prompt_bundle.session_summary = self._refresh_memory_summary(summary)
                return True
        if prompt_bundle.graph_chain_payload:
            prompt_bundle.graph_chain_payload.pop()
            return True
        if prompt_bundle.evidence_payload:
            prompt_bundle.evidence_payload.pop()
            return True
        if prompt_bundle.sensor_payload:
            prompt_bundle.sensor_payload.pop()
            return True
        if prompt_bundle.asset_payload:
            prompt_bundle.asset_payload.pop()
            return True
        if prompt_bundle.entity_payload:
            prompt_bundle.entity_payload.pop()
            return True
        if prompt_bundle.assertion_payload:
            prompt_bundle.assertion_payload.pop()
            return True
        if len(prompt_bundle.chunk_payload) > 1:
            prompt_bundle.chunk_payload.pop()
            return True
        if len(prompt_bundle.prior_context) > 1:
            prompt_bundle.prior_context.pop(0)
            return True
        return self.truncate_prompt_bundle_once(prompt_bundle)

    def truncate_prompt_bundle_once(self, prompt_bundle: AgentPromptBundle) -> bool:
        if prompt_bundle.profile_summary:
            summary_text = str((prompt_bundle.profile_summary or {}).get("summary_text") or "")
            if len(summary_text) > 140:
                prompt_bundle.profile_summary = {
                    **prompt_bundle.profile_summary,
                    "summary_text": summary_text[: max(140, len(summary_text) // 2)].rstrip() + "...",
                }
                return True
        if prompt_bundle.session_summary:
            summary_text = str((prompt_bundle.session_summary or {}).get("summary_text") or "")
            if len(summary_text) > 160:
                prompt_bundle.session_summary = {
                    **prompt_bundle.session_summary,
                    "summary_text": summary_text[: max(160, len(summary_text) // 2)].rstrip() + "...",
                }
                return True
        for payload in (
            prompt_bundle.graph_chain_payload,
            prompt_bundle.evidence_payload,
            prompt_bundle.sensor_payload,
            prompt_bundle.asset_payload,
            prompt_bundle.chunk_payload,
            prompt_bundle.assertion_payload,
        ):
            if not payload:
                continue
            candidate = payload[-1]
            for field_name, min_len in (("text", 120), ("summary_text", 120), ("excerpt", 80), ("object_literal", 80)):
                value = candidate.get(field_name)
                if isinstance(value, str) and len(value) > min_len:
                    candidate[field_name] = value[: max(min_len, len(value) // 2)].rstrip() + "..."
                    candidate["truncated"] = True
                    return True
        if prompt_bundle.prior_context:
            candidate = prompt_bundle.prior_context[0]
            content = str(candidate.get("content") or "")
            if len(content) > 120:
                candidate["content"] = "..." + content[-max(120, len(content) // 2):]
                return True
        return False

    def build_user_prompt(
        self,
        *,
        question: str,
        normalized_query: str,
        question_type: str,
        prompt_bundle: AgentPromptBundle,
    ) -> str:
        return "\n".join(
            [
                f"question_type: {question_type}",
                f"question: {question}",
                f"normalized_query: {normalized_query}",
                f"allowed_chunk_ids: {json.dumps([item['chunk_id'] for item in prompt_bundle.chunk_payload], ensure_ascii=False)}",
                f"allowed_asset_ids: {json.dumps([item['asset_id'] for item in prompt_bundle.asset_payload], ensure_ascii=False)}",
                f"allowed_sensor_row_ids: {json.dumps([item['sensor_row_id'] for item in prompt_bundle.sensor_payload], ensure_ascii=False)}",
                f"allowed_assertion_ids: {json.dumps([item['assertion_id'] for item in prompt_bundle.assertion_payload], ensure_ascii=False)}",
                f"allowed_entity_ids: {json.dumps([item['entity_id'] for item in prompt_bundle.entity_payload], ensure_ascii=False)}",
                f"allowed_evidence_ids: {json.dumps([item['evidence_id'] for item in prompt_bundle.evidence_payload], ensure_ascii=False)}",
                "Use graph chains only as structural support. When they help, cite the underlying assertion ids and evidence ids rather than the chain itself.",
                "Return only the chunk ids, asset ids, sensor row ids, and evidence ids you actually used in used_chunk_ids, used_asset_ids, used_sensor_row_ids, and used_evidence_ids. Do not write source quotes.",
                f"context_chunks: {json.dumps(_json_safe(prompt_bundle.chunk_payload), ensure_ascii=False)}",
                f"context_assets: {json.dumps(_json_safe(prompt_bundle.asset_payload), ensure_ascii=False)}",
                f"context_sensors: {json.dumps(_json_safe(prompt_bundle.sensor_payload), ensure_ascii=False)}",
                f"context_assertions: {json.dumps(_json_safe(prompt_bundle.assertion_payload), ensure_ascii=False)}",
                f"context_entities: {json.dumps(_json_safe(prompt_bundle.entity_payload), ensure_ascii=False)}",
                f"context_graph_chains: {json.dumps(_json_safe(prompt_bundle.graph_chain_payload), ensure_ascii=False)}",
                f"context_evidence: {json.dumps(_json_safe(prompt_bundle.evidence_payload), ensure_ascii=False)}",
                f"context_budget: {json.dumps(_json_safe(prompt_bundle.stats), ensure_ascii=False)}",
            ]
        )

    def build_open_world_user_payload(
        self,
        *,
        question: str,
        normalized_query: str,
        question_type: str,
        bundle: AgentContextBundle,
    ) -> dict[str, Any]:
        return {
            "question": question,
            "normalized_query": normalized_query,
            "question_type": question_type,
            "policy": {
                "may_use_general_world_knowledge": True,
                "do_not_claim_corpus_provenance_without_support": True,
                "prefer_context_hints_when_relevant": True,
                "do_not_abstain_if_a_helpful_best_effort_answer_is_possible": True,
            },
            "context_hints": {
                "chunks": [
                    {
                        "chunk_id": row["chunk_id"],
                        "section_title": str((row.get("metadata_json") or {}).get("section_title") or ""),
                        "excerpt": self._extract_citation_excerpt(str(row.get("text") or ""), normalized_query, 220),
                    }
                    for row in list(bundle.chunks or [])[:2]
                ],
                "assertions": [
                    {
                        "assertion_id": row["assertion_id"],
                        "subject_entity_id": str(row.get("subject_entity_id") or ""),
                        "predicate": str(row.get("predicate") or ""),
                        "object_entity_id": str(row.get("object_entity_id") or ""),
                        "object_literal": str(row.get("object_literal") or ""),
                    }
                    for row in list(bundle.assertions or [])[:3]
                ],
                "assets": [
                    {
                        "asset_id": row["asset_id"],
                        "label": str((row.get("metadata_json") or {}).get("label") or ""),
                        "excerpt": self._extract_citation_excerpt(self._trusted_asset_grounding_text(row), normalized_query, 180),
                    }
                    for row in list(bundle.assets or [])[:2]
                    if self._trusted_asset_grounding_text(row)
                ],
                "sensors": [
                    {
                        "sensor_row_id": str(row.get("sensor_row_id") or ""),
                        "sensor_name": str(row.get("sensor_name") or ""),
                        "metric_name": str(row.get("metric_name") or ""),
                        "summary_text": self._extract_citation_excerpt(self._trusted_sensor_grounding_text(row), normalized_query, 180),
                    }
                    for row in list(bundle.sensor_rows or [])[:2]
                    if self._trusted_sensor_grounding_text(row)
                ],
            },
        }

    def estimate_prompt_chars(
        self,
        *,
        question: str,
        normalized_query: str,
        question_type: str,
        prompt_bundle: AgentPromptBundle,
        runtime_config: dict[str, Any],
    ) -> int:
        system_prompt = runtime_config["system_prompt"]
        user_prompt = self.build_user_prompt(
            question=question,
            normalized_query=normalized_query,
            question_type=question_type,
            prompt_bundle=prompt_bundle,
        )
        profile_summary = json.dumps(_json_safe(prompt_bundle.profile_summary or {}), ensure_ascii=False) if prompt_bundle.profile_summary else ""
        session_summary = json.dumps(_json_safe(prompt_bundle.session_summary or {}), ensure_ascii=False) if prompt_bundle.session_summary else ""
        return (
            len(system_prompt)
            + len(profile_summary)
            + len(session_summary)
            + len(user_prompt)
            + sum(len(str(item.get("content") or "")) for item in prompt_bundle.prior_context)
        )

    def mark_selected_sources_for_prompt(
        self,
        sources: list[dict[str, Any]],
        prompt_bundle: AgentPromptBundle,
    ) -> list[dict[str, Any]]:
        selected_chunk_ids = {item["chunk_id"] for item in prompt_bundle.chunk_payload}
        selected_asset_ids = {item["asset_id"] for item in prompt_bundle.asset_payload}
        selected_sensor_row_ids = {item["sensor_row_id"] for item in prompt_bundle.sensor_payload}
        selected_assertion_ids = {item["assertion_id"] for item in prompt_bundle.assertion_payload}
        selected_entity_ids = {item["entity_id"] for item in prompt_bundle.entity_payload}
        selected_evidence_ids = {item["evidence_id"] for item in prompt_bundle.evidence_payload}
        finalized: list[dict[str, Any]] = []
        for source in sources:
            item = dict(source)
            kind = str(item.get("source_kind") or "")
            selected = False
            if kind == "chunk":
                selected = str(item.get("chunk_id") or "") in selected_chunk_ids
            elif kind == "asset_hit":
                selected = str(item.get("asset_id") or "") in selected_asset_ids
            elif kind == "asset_link":
                selected = (
                    str(item.get("chunk_id") or "") in selected_chunk_ids
                    and str(item.get("asset_id") or "") in selected_asset_ids
                )
            elif kind == "sensor":
                selected = str(item.get("sensor_row_id") or item.get("source_id") or "") in selected_sensor_row_ids
            elif kind == "assertion":
                selected = str(item.get("assertion_id") or "") in selected_assertion_ids
            elif kind == "entity":
                selected = str(item.get("entity_id") or "") in selected_entity_ids
            elif kind == "evidence":
                selected = str(item.get("evidence_id") or item.get("source_id") or "") in selected_evidence_ids
            item["selected"] = selected
            finalized.append(item)
        return finalized

    def trim_context_bundle_to_prompt(
        self,
        bundle: AgentContextBundle,
        prompt_bundle: AgentPromptBundle,
    ) -> AgentContextBundle:
        selected_chunk_ids = {item["chunk_id"] for item in prompt_bundle.chunk_payload}
        selected_asset_ids = {item["asset_id"] for item in prompt_bundle.asset_payload}
        selected_sensor_row_ids = {item["sensor_row_id"] for item in prompt_bundle.sensor_payload}
        selected_assertion_ids = {item["assertion_id"] for item in prompt_bundle.assertion_payload}
        selected_entity_ids = {item["entity_id"] for item in prompt_bundle.entity_payload}
        selected_graph_chain_ids = {item["chain_id"] for item in prompt_bundle.graph_chain_payload}
        selected_evidence_ids = {item["evidence_id"] for item in prompt_bundle.evidence_payload}
        final_sources = self.mark_selected_sources_for_prompt(bundle.sources, prompt_bundle)
        existing_chunk_ids = {
            str(item.get("chunk_id") or "")
            for item in final_sources
            if item.get("source_kind") == "chunk" and str(item.get("chunk_id") or "")
        }
        existing_asset_ids = {
            str(item.get("asset_id") or item.get("source_id") or "")
            for item in final_sources
            if item.get("source_kind") == "asset_hit" and str(item.get("asset_id") or item.get("source_id") or "")
        }
        existing_sensor_row_ids = {
            str(item.get("sensor_row_id") or item.get("source_id") or "")
            for item in final_sources
            if item.get("source_kind") == "sensor" and str(item.get("sensor_row_id") or item.get("source_id") or "")
        }
        existing_evidence_ids = {
            str(item.get("evidence_id") or item.get("source_id") or "")
            for item in final_sources
            if item.get("source_kind") == "evidence" and str(item.get("evidence_id") or item.get("source_id") or "")
        }
        for row in bundle.chunks:
            chunk_id = row["chunk_id"]
            if chunk_id not in selected_chunk_ids or chunk_id in existing_chunk_ids:
                continue
            final_sources.append(
                {
                    "source_kind": "chunk",
                    "source_id": chunk_id,
                    "document_id": row.get("document_id"),
                    "chunk_id": chunk_id,
                    "rank": None,
                    "score": float(row.get("_retrieval_score") or 0.0),
                    "selected": True,
                    "payload": {
                        "derived_from": "final_context_bundle",
                        "page_start": row.get("page_start"),
                        "page_end": row.get("page_end"),
                    },
                }
            )
        for row in bundle.assets:
            asset_id = row["asset_id"]
            if asset_id not in selected_asset_ids or asset_id in existing_asset_ids:
                continue
            final_sources.append(
                {
                    "source_kind": "asset_hit",
                    "source_id": asset_id,
                    "document_id": row.get("document_id"),
                    "chunk_id": None,
                    "asset_id": asset_id,
                    "rank": None,
                    "score": float(row.get("_retrieval_score") or 0.0),
                    "selected": True,
                    "payload": {
                        "derived_from": "final_context_bundle",
                        "page_number": row.get("page_number"),
                        "asset_type": row.get("asset_type"),
                    },
                }
            )
        for row in bundle.sensor_rows:
            sensor_row_id = str(row.get("sensor_row_id") or "")
            if not sensor_row_id or sensor_row_id not in selected_sensor_row_ids or sensor_row_id in existing_sensor_row_ids:
                continue
            final_sources.append(
                {
                    "source_kind": "sensor",
                    "source_id": sensor_row_id,
                    "document_id": None,
                    "chunk_id": None,
                    "sensor_row_id": sensor_row_id,
                    "sensor_id": row.get("sensor_id"),
                    "rank": None,
                    "score": float(row.get("_relevance_score") or 0.0),
                    "selected": True,
                    "payload": {
                        "derived_from": "final_context_bundle",
                        "metric_name": row.get("metric_name"),
                        "latest_observed_at": row.get("latest_observed_at"),
                    },
                }
            )
        for row in bundle.evidence:
            evidence_id = str(row.get("evidence_id") or "")
            if not evidence_id or evidence_id not in selected_evidence_ids or evidence_id in existing_evidence_ids:
                continue
            final_sources.append(
                {
                    "source_kind": "evidence",
                    "source_id": evidence_id,
                    "document_id": None,
                    "chunk_id": None,
                    "evidence_id": evidence_id,
                    "rank": None,
                    "score": None,
                    "selected": True,
                    "payload": {
                        "derived_from": "final_context_bundle",
                        "assertion_id": row.get("assertion_id"),
                    },
                }
            )
        return AgentContextBundle(
            chunks=[row for row in bundle.chunks if row["chunk_id"] in selected_chunk_ids],
            assets=[row for row in bundle.assets if row["asset_id"] in selected_asset_ids],
            sensor_rows=[row for row in bundle.sensor_rows if str(row.get("sensor_row_id") or "") in selected_sensor_row_ids],
            assertions=[row for row in bundle.assertions if row["assertion_id"] in selected_assertion_ids],
            graph_chains=[row for row in bundle.graph_chains if str(row.get("chain_id") or "") in selected_graph_chain_ids],
            evidence=[
                row
                for row in bundle.evidence
                if str(row.get("evidence_id") or "") in selected_evidence_ids
                or row["assertion_id"] in selected_assertion_ids
            ],
            entities=[row for row in bundle.entities if row["entity_id"] in selected_entity_ids],
            sources=final_sources,
        )


def _budget_prior_messages(messages: list[dict[str, Any]], char_budget: int) -> list[dict[str, Any]]:
    kept: list[dict[str, Any]] = []
    used_chars = 0
    for item in reversed(messages):
        role = str(item.get("role") or "")
        content = str(item.get("content") or "")
        if role not in {"user", "assistant"} or not content:
            continue
        candidate = {"role": role, "content": content}
        size = len(json.dumps(candidate, ensure_ascii=False))
        if kept and used_chars + size > char_budget:
            continue
        if not kept and size > char_budget:
            max_content = max(40, char_budget - 120)
            candidate["content"] = content[-max_content:]
            size = len(json.dumps(candidate, ensure_ascii=False))
            if size > char_budget:
                candidate["content"] = candidate["content"][-max(16, char_budget // 2):]
                size = len(json.dumps(candidate, ensure_ascii=False))
        if used_chars + size > char_budget and kept:
            continue
        kept.append(candidate)
        used_chars += size
    kept.reverse()
    return kept


def _budget_chunk_payload(
    chunks: list[dict[str, Any]],
    char_budget: int,
    trusted_asset_grounding_text: Callable[[dict[str, Any]], str],
) -> list[dict[str, Any]]:
    return _budget_structured_rows(chunks, char_budget, lambda row, remaining: _render_chunk_payload(row, remaining))


def _budget_asset_payload(
    assets: list[dict[str, Any]],
    char_budget: int,
    trusted_asset_grounding_text: Callable[[dict[str, Any]], str],
) -> list[dict[str, Any]]:
    return _budget_structured_rows(assets, char_budget, lambda row, remaining: _render_asset_payload(row, remaining, trusted_asset_grounding_text))


def _budget_sensor_payload(
    sensor_rows: list[dict[str, Any]],
    char_budget: int,
    trusted_sensor_grounding_text: Callable[[dict[str, Any]], str],
) -> list[dict[str, Any]]:
    return _budget_structured_rows(sensor_rows, char_budget, lambda row, remaining: _render_sensor_payload(row, remaining, trusted_sensor_grounding_text))


def _budget_assertion_payload(assertions: list[dict[str, Any]], char_budget: int) -> list[dict[str, Any]]:
    return _budget_structured_rows(assertions, char_budget, _render_assertion_payload)


def _budget_entity_payload(entities: list[dict[str, Any]], char_budget: int) -> list[dict[str, Any]]:
    return _budget_structured_rows(entities, char_budget, _render_entity_payload)


def _budget_graph_chain_payload(graph_chains: list[dict[str, Any]], char_budget: int) -> list[dict[str, Any]]:
    return _budget_structured_rows(graph_chains, char_budget, _render_graph_chain_payload)


def _budget_evidence_payload(evidence: list[dict[str, Any]], char_budget: int) -> list[dict[str, Any]]:
    return _budget_structured_rows(evidence, char_budget, _render_evidence_payload)


def _budget_structured_rows(
    rows: list[dict[str, Any]],
    char_budget: int,
    render_fn: Callable[[dict[str, Any], int], dict[str, Any]],
) -> list[dict[str, Any]]:
    kept: list[dict[str, Any]] = []
    used_chars = 0
    for row in rows:
        remaining = max(0, char_budget - used_chars)
        candidate = render_fn(row, remaining)
        size = len(json.dumps(_json_safe(candidate), ensure_ascii=False))
        if kept and used_chars + size > char_budget:
            continue
        if not kept and size > char_budget:
            candidate = render_fn(row, max(40, char_budget))
            candidate["truncated"] = True
            size = len(json.dumps(_json_safe(candidate), ensure_ascii=False))
            while size > char_budget and any(
                isinstance(candidate.get(field_name), str) and len(candidate.get(field_name) or "") > 24
                for field_name in ("text", "summary_text", "excerpt", "object_literal", "content")
            ):
                for field_name in ("text", "summary_text", "excerpt", "object_literal", "content"):
                    field_value = candidate.get(field_name)
                    if isinstance(field_value, str) and len(field_value) > 24:
                        candidate[field_name] = field_value[: max(24, len(field_value) // 2)].rstrip() + "..."
                        break
                size = len(json.dumps(_json_safe(candidate), ensure_ascii=False))
            kept.append(candidate)
            break
        if used_chars + size > char_budget and kept:
            continue
        kept.append(candidate)
        used_chars += size
    return kept


def _render_chunk_payload(row: dict[str, Any], remaining_chars: int) -> dict[str, Any]:
    text = str(row.get("text") or "")
    max_text = max(200, remaining_chars - 700)
    candidate = {
        "chunk_id": row["chunk_id"],
        "document_id": str(row["document_id"]),
        "chunk_index": row.get("chunk_index"),
        "page_start": row.get("page_start"),
        "page_end": row.get("page_end"),
        "section_path": list(row.get("section_path") or [])[:6],
        "section_title": row.get("metadata_json", {}).get("section_title"),
        "chunk_role": row.get("metadata_json", {}).get("chunk_role"),
        "text": text[:max_text],
    }
    section_synopsis = str(row.get("_section_synopsis") or "").strip()
    if section_synopsis and remaining_chars > 560:
        candidate["section_synopsis"] = section_synopsis[:220]
        if str(row.get("_section_synopsis_title") or "").strip():
            candidate["section_synopsis_title"] = str(row.get("_section_synopsis_title") or "").strip()[:120]
    document_synopsis = str(row.get("_document_synopsis") or "").strip()
    if document_synopsis and remaining_chars > 760:
        candidate["document_synopsis"] = document_synopsis[:220]
        if str(row.get("_document_synopsis_title") or "").strip():
            candidate["document_synopsis_title"] = str(row.get("_document_synopsis_title") or "").strip()[:120]
    if len(text) > max_text:
        candidate["truncated"] = True
    return candidate


def _render_assertion_payload(row: dict[str, Any], remaining_chars: int) -> dict[str, Any]:
    candidate = {
        "assertion_id": row["assertion_id"],
        "chunk_id": str(row["chunk_id"]),
        "subject_entity_id": row["subject_entity_id"],
        "predicate": row["predicate"],
        "object_entity_id": row.get("object_entity_id"),
        "object_literal": row.get("object_literal"),
        "confidence": float(row.get("confidence") or 0.0),
    }
    text = json.dumps(candidate, ensure_ascii=False)
    if len(text) > remaining_chars and remaining_chars > 0:
        candidate["object_literal"] = str(candidate.get("object_literal") or "")[: max(0, remaining_chars - 200)]
        candidate["truncated"] = True
    return candidate


def _render_asset_payload(
    row: dict[str, Any],
    remaining_chars: int,
    trusted_asset_grounding_text: Callable[[dict[str, Any]], str],
) -> dict[str, Any]:
    metadata = row.get("metadata_json") or {}
    trusted_text = trusted_asset_grounding_text(row)
    generated_text = sanitize_text(
        "\n".join(
            part
            for part in [
                str(row.get("description_text") or ""),
                " ".join(metadata.get("important_terms") or []),
                str(row.get("search_text") or ""),
            ]
            if str(part).strip()
        )
    ).strip()
    text = trusted_text or generated_text
    max_text = max(160, remaining_chars - 480)
    candidate = {
        "asset_id": row["asset_id"],
        "document_id": str(row["document_id"]),
        "page_number": row.get("page_number"),
        "asset_type": row.get("asset_type"),
        "label": metadata.get("label") or "",
        "important_terms": (metadata.get("important_terms") or [])[:8],
        "linked_chunk_ids": (metadata.get("linked_chunk_ids") or [])[:6],
        "link_types": (metadata.get("link_types") or [])[:4],
        "max_link_confidence": metadata.get("max_link_confidence"),
        "text_trust": "deterministic" if trusted_text else "generated",
        "text": text[:max_text],
    }
    if trusted_text and str(metadata.get("page_summary") or "").strip():
        candidate["page_summary"] = str(metadata.get("page_summary") or "")[:220]
    elif str(metadata.get("page_summary") or "").strip() and remaining_chars > 760:
        candidate["generated_page_summary"] = str(metadata.get("page_summary") or "")[:220]
    if trusted_text and generated_text and generated_text != trusted_text and remaining_chars > 720:
        candidate["generated_description"] = generated_text[: min(max_text, 320)]
    if len(text) > max_text:
        candidate["truncated"] = True
    return candidate


def _render_sensor_payload(
    row: dict[str, Any],
    remaining_chars: int,
    trusted_sensor_grounding_text: Callable[[dict[str, Any]], str],
) -> dict[str, Any]:
    summary_text = trusted_sensor_grounding_text(row)
    max_summary = max(140, remaining_chars - 520)
    candidate = {
        "sensor_row_id": row["sensor_row_id"],
        "sensor_id": row["sensor_id"],
        "sensor_name": str(row.get("sensor_name") or ""),
        "sensor_type": str(row.get("sensor_type") or ""),
        "place_id": str(row.get("place_id") or ""),
        "place_name": str(row.get("place_name") or ""),
        "external_place_id": str(row.get("external_place_id") or ""),
        "hive_id": str(row.get("hive_id") or ""),
        "external_hive_id": str(row.get("external_hive_id") or ""),
        "hive_name": str(row.get("hive_name") or ""),
        "location_label": str(row.get("location_label") or ""),
        "metric_name": str(row.get("metric_name") or ""),
        "unit": row.get("unit"),
        "latest_value": row.get("latest_value"),
        "latest_observed_at": row.get("latest_observed_at"),
        "window_start_at": row.get("window_start_at"),
        "window_end_at": row.get("window_end_at"),
        "sample_count": row.get("sample_count"),
        "min_value": row.get("min_value"),
        "max_value": row.get("max_value"),
        "avg_value": row.get("avg_value"),
        "delta_value": row.get("delta_value"),
        "reading_ids": list(row.get("reading_ids") or [])[:8],
        "recent_points": list(row.get("recent_points") or [])[:6],
        "summary_text": summary_text[:max_summary],
    }
    if len(summary_text) > max_summary:
        candidate["truncated"] = True
    return candidate


def _render_entity_payload(row: dict[str, Any], remaining_chars: int) -> dict[str, Any]:
    candidate = {
        "entity_id": row["entity_id"],
        "canonical_name": row["canonical_name"],
        "entity_type": row["entity_type"],
    }
    aliases = row.get("aliases") or []
    if aliases and remaining_chars > 220:
        candidate["aliases"] = aliases[:4]
    return candidate


def _render_graph_chain_payload(row: dict[str, Any], remaining_chars: int) -> dict[str, Any]:
    candidate = {
        "chain_id": str(row.get("chain_id") or ""),
        "shared_entity_id": str(row.get("shared_entity_id") or ""),
        "shared_entity_name": str(row.get("shared_entity_name") or ""),
        "chain_score": float(row.get("chain_score") or 0.0),
        "supporting_assertion_ids": list(row.get("supporting_assertion_ids") or [])[:4],
        "supporting_evidence_ids": list(row.get("supporting_evidence_ids") or [])[:6],
        "steps": [],
    }
    step_budget = max(160, remaining_chars - 260)
    step_count = 0
    for step in list(row.get("steps") or [])[:2]:
        if step_count >= 2:
            break
        step_payload = {
            "assertion_id": str(step.get("assertion_id") or ""),
            "subject_name": str(step.get("subject_name") or ""),
            "predicate": str(step.get("predicate") or ""),
            "object_name": str(step.get("object_name") or ""),
            "object_literal": str(step.get("object_literal") or "")[: max(0, step_budget // 3)],
            "confidence": float(step.get("confidence") or 0.0),
            "evidence_ids": list(step.get("evidence_ids") or [])[:2],
        }
        candidate["steps"].append(step_payload)
        step_count += 1
    text = json.dumps(candidate, ensure_ascii=False)
    if len(text) > remaining_chars and candidate["steps"]:
        candidate["steps"] = candidate["steps"][:1]
        candidate["truncated"] = True
    return candidate


def _render_evidence_payload(row: dict[str, Any], remaining_chars: int) -> dict[str, Any]:
    excerpt = str(row.get("excerpt") or "")
    max_excerpt = max(120, remaining_chars - 180)
    candidate = {
        "evidence_id": row["evidence_id"],
        "assertion_id": row["assertion_id"],
        "excerpt": excerpt[:max_excerpt],
    }
    if len(excerpt) > max_excerpt:
        candidate["truncated"] = True
    return candidate


def _estimate_json_chars(payload: Any) -> int:
    return len(json.dumps(_json_safe(payload), ensure_ascii=False))


def _json_safe(value: Any) -> Any:
    if isinstance(value, dict):
        return {str(key): _json_safe(item) for key, item in value.items()}
    if isinstance(value, list):
        return [_json_safe(item) for item in value]
    if isinstance(value, tuple):
        return [_json_safe(item) for item in value]
    if value is None or isinstance(value, (str, int, float, bool)):
        return value
    return str(value)
