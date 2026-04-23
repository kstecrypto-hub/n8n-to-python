"""Ontology loading, candidate extraction, canonicalization, and KG validation.

This module intentionally separates four concerns:
- load ontology constraints from Turtle/JSON
- extract model or heuristic candidates from one chunk
- canonicalize entity naming into stable graph identities
- prune and validate results before persistence
"""

from __future__ import annotations

import json
import time
import re
import unicodedata
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import httpx

from src.bee_ingestion.models import Chunk, KGExtractionResult
from src.bee_ingestion.settings import settings


@dataclass(slots=True)
class Ontology:
    classes: set[str]
    predicates: set[str]
    labels: dict[str, str]
    subclass_of: dict[str, set[str]]
    ancestors: dict[str, set[str]]
    predicate_domains: dict[str, str]
    predicate_ranges: dict[str, str]
    predicate_kinds: dict[str, str]
    class_aliases: dict[str, set[str]]


@dataclass(slots=True)
class KGExtractionArtifact:
    result: KGExtractionResult
    raw_payload: dict[str, Any]
    provider: str
    model: str
    prompt_version: str


class KGExtractionError(RuntimeError):
    pass


def load_ontology(path: str | None = None) -> Ontology:
    source_path = Path(path or settings.kg_ontology_path)
    if source_path.suffix.lower() == ".json":
        raw = json.loads(source_path.read_text(encoding="utf-8"))
        classes = set(raw.get("classes", []))
        predicates = set(raw.get("predicates", []))
        labels = {item: item for item in classes | predicates}
        subclass_of: dict[str, set[str]] = {item: set() for item in classes}
        ancestors = {item: set() for item in classes}
        return Ontology(
            classes=classes,
            predicates=predicates,
            labels=labels,
            subclass_of=subclass_of,
            ancestors=ancestors,
            predicate_domains={},
            predicate_ranges={},
            predicate_kinds={},
            class_aliases={item: _build_class_aliases(item, labels.get(item, item), set()) for item in classes},
        )
    return _load_turtle_ontology(source_path)


def _load_turtle_ontology(path: Path) -> Ontology:
    text = path.read_text(encoding="utf-8")
    blocks = _parse_turtle_blocks(text)
    classes: set[str] = set()
    predicates: set[str] = set()
    labels: dict[str, str] = {}
    subclass_of: dict[str, set[str]] = {}
    predicate_domains: dict[str, str] = {}
    predicate_ranges: dict[str, str] = {}
    predicate_kinds: dict[str, str] = {}
    extra_aliases: dict[str, set[str]] = {}

    for subject, body in blocks:
        label_match = re.search(r'rdfs:label\s+"([^"]+)"', body)
        if label_match:
            labels[subject] = label_match.group(1)
        alt_labels = {match.strip() for match in re.findall(r'skos:altLabel\s+"([^"]+)"', body) if match.strip()}
        if alt_labels:
            extra_aliases.setdefault(subject, set()).update(alt_labels)

        if re.search(r"\ba\s+owl:Class\b", body):
            classes.add(subject)
            parents = set(re.findall(r"rdfs:subClassOf\s+beecore:(\w+)", body))
            subclass_of.setdefault(subject, set()).update(parents)

        predicate_type = None
        if re.search(r"\ba\s+owl:ObjectProperty\b", body):
            predicate_type = "object"
        elif re.search(r"\ba\s+owl:DatatypeProperty\b", body):
            predicate_type = "datatype"
        if predicate_type:
            predicates.add(subject)
            predicate_kinds[subject] = predicate_type
            domain_match = re.search(r"rdfs:domain\s+([A-Za-z0-9_]+:\w+)", body)
            range_match = re.search(r"rdfs:range\s+([A-Za-z0-9_]+:\w+)", body)
            if domain_match:
                predicate_domains[subject] = _strip_curie(domain_match.group(1))
            if range_match:
                predicate_ranges[subject] = _strip_curie(range_match.group(1))

    for ontology_class in classes:
        subclass_of.setdefault(ontology_class, set())
        labels.setdefault(ontology_class, ontology_class)
    for predicate in predicates:
        labels.setdefault(predicate, predicate)

    ancestors = {ontology_class: _compute_ancestors(ontology_class, subclass_of) for ontology_class in classes}
    return Ontology(
        classes=classes,
        predicates=predicates,
        labels=labels,
        subclass_of=subclass_of,
        ancestors=ancestors,
        predicate_domains=predicate_domains,
        predicate_ranges=predicate_ranges,
        predicate_kinds=predicate_kinds,
        class_aliases={item: _build_class_aliases(item, labels.get(item, item), extra_aliases.get(item, set())) for item in classes},
    )


def _parse_turtle_blocks(text: str) -> list[tuple[str, str]]:
    blocks: list[tuple[str, str]] = []
    current_subject: str | None = None
    current_lines: list[str] = []
    for raw_line in text.splitlines():
        line = raw_line.strip()
        if not line or line.startswith("@prefix") or line.startswith("##########") or line.startswith("#"):
            continue
        if current_subject is None:
            match = re.match(r"beecore:(\w+)\s+(.*)", line)
            if not match:
                continue
            current_subject = match.group(1)
            current_lines = [match.group(2)]
            if line.endswith("."):
                blocks.append((current_subject, " ".join(current_lines)))
                current_subject = None
                current_lines = []
            continue
        current_lines.append(line)
        if line.endswith("."):
            blocks.append((current_subject, " ".join(current_lines)))
            current_subject = None
            current_lines = []
    return blocks


def _strip_curie(value: str) -> str:
    if ":" not in value:
        return value
    prefix, suffix = value.split(":", 1)
    if prefix == "beecore":
        return suffix
    return f"{prefix}:{suffix}"


def _compute_ancestors(node: str, subclass_of: dict[str, set[str]], trail: set[str] | None = None) -> set[str]:
    seen = set(trail or set())
    parents = subclass_of.get(node, set())
    results: set[str] = set()
    for parent in parents:
        if parent in seen:
            continue
        results.add(parent)
        results.update(_compute_ancestors(parent, subclass_of, seen | {parent}))
    return results


def is_type_compatible(actual: str, expected: str, ontology: Ontology) -> bool:
    if not actual or not expected:
        return False
    if actual == expected:
        return True
    return expected in ontology.ancestors.get(actual, set())


def chunk_ontology_tags(chunk: Chunk, ontology: Ontology) -> list[str]:
    # These tags are lexical hints for metadata and review, not proof that a graph relation exists.
    lowered = _normalize_entity_surface(chunk.text)
    hits: list[str] = []
    for key, aliases in ontology.class_aliases.items():
        if any(alias and _surface_matches_alias(lowered, alias) for alias in aliases):
            hits.append(key)
    return sorted(set(hits))


CANONICAL_ALIAS_HINTS: dict[str, set[str]] = {
    "Queen": {"queen", "queen bee", "mother bee", "mother-bee", "fertile queen"},
    "Worker": {"worker", "workers", "worker bee", "worker bees"},
    "Drone": {"drone", "drones", "drone bee", "drone bees", "male bee", "male bees"},
    "Colony": {"colony", "colonies", "bee colony", "bee colonies", "colony of bees", "hive of bees"},
    "Hive": {"hive", "hives", "hive body", "hive-body"},
    "Apiary": {"apiary", "apiaries"},
    "Egg": {"egg", "eggs"},
    "Larva": {"larva", "larvae"},
    "Pupa": {"pupa", "pupae"},
    "Comb": {"comb", "combs", "honeycomb", "honey comb"},
    "Brood": {"brood"},
    "Honey": {"honey"},
    "Wax": {"wax", "beeswax", "bee wax"},
}
GENERIC_CANONICAL_TYPES = {"Worker", "Drone", "Bee", "Colony", "Apiary", "Egg", "Larva", "Pupa", "Comb", "Brood", "Honey", "Wax"}
GENERIC_ALIAS_TAIL_TOKENS = {
    "about",
    "around",
    "few",
    "for",
    "from",
    "hundred",
    "hundreds",
    "in",
    "many",
    "more",
    "most",
    "of",
    "or",
    "over",
    "several",
    "some",
    "than",
    "thousand",
    "thousands",
    "to",
    "under",
    "with",
}


def _build_class_aliases(ontology_class: str, label: str, extra_aliases: set[str] | None = None) -> set[str]:
    aliases = {
        _normalize_entity_surface(label),
        _normalize_entity_surface(re.sub(r"(?<!^)([A-Z])", r" \1", ontology_class)),
        _normalize_entity_surface(ontology_class),
    }
    aliases.update(_normalize_entity_surface(item) for item in (extra_aliases or set()))
    aliases.update(_normalize_entity_surface(item) for item in CANONICAL_ALIAS_HINTS.get(ontology_class, set()))
    return {alias for alias in aliases if alias}


def _surface_matches_alias(surface: str, alias: str) -> bool:
    return bool(re.search(rf"(?<!\w){re.escape(alias)}(?!\w)", surface))


def _normalize_entity_tokens(value: str) -> list[str]:
    tokens = _normalize_entity_surface(value).split()
    normalized: list[str] = []
    for token in tokens:
        if token == "bees":
            normalized.append("bee")
        elif token.endswith("ies") and len(token) > 4:
            normalized.append(token[:-3] + "y")
        elif token.endswith("s") and len(token) > 3 and not token.endswith("ss"):
            normalized.append(token[:-1])
        else:
            normalized.append(token)
    return normalized


def _contains_alias_tokens(surface_tokens: list[str], alias_tokens: list[str]) -> bool:
    if not alias_tokens or len(surface_tokens) < len(alias_tokens):
        return False
    for index in range(0, len(surface_tokens) - len(alias_tokens) + 1):
        if surface_tokens[index : index + len(alias_tokens)] == alias_tokens:
            return True
    return False


def _alias_with_generic_tail(surface_tokens: list[str], alias_tokens: list[str]) -> bool:
    if not alias_tokens or len(surface_tokens) <= len(alias_tokens):
        return False

    candidates = []
    if surface_tokens[: len(alias_tokens)] == alias_tokens:
        candidates.append(surface_tokens[len(alias_tokens) :])
    if surface_tokens[-len(alias_tokens) :] == alias_tokens:
        candidates.append(surface_tokens[: -len(alias_tokens)])

    for tail in candidates:
        if tail and all(token.isdigit() or token in GENERIC_ALIAS_TAIL_TOKENS for token in tail):
            return True
    return False


def _should_use_ontology_label(entity_type: str, normalized_surfaces: list[str], aliases: set[str]) -> bool:
    if any(surface in aliases for surface in normalized_surfaces):
        return True
    if entity_type not in GENERIC_CANONICAL_TYPES:
        return False
    normalized_alias_tokens = [_normalize_entity_tokens(alias) for alias in aliases if alias]
    return any(
        (
            _contains_alias_tokens(_normalize_entity_tokens(surface), alias_tokens)
            and _alias_with_generic_tail(_normalize_entity_tokens(surface), alias_tokens)
        )
        or _alias_with_generic_tail(_normalize_entity_tokens(surface), alias_tokens)
        for surface in normalized_surfaces
        for alias_tokens in normalized_alias_tokens
    )


def _normalize_entity_surface(value: str) -> str:
    cleaned = unicodedata.normalize("NFKC", value or "").lower()
    cleaned = cleaned.replace("\x00", "")
    cleaned = re.sub(r"\(([^)]*)\)", r" \1 ", cleaned)
    cleaned = cleaned.replace("/", " ").replace("-", " ")
    cleaned = re.sub(r"^[\"'`]+|[\"'`]+$", "", cleaned)
    cleaned = re.sub(r"\b(the|a|an)\b", " ", cleaned)
    cleaned = re.sub(r"[^a-z0-9\s]", " ", cleaned)
    cleaned = re.sub(r"\s+", " ", cleaned).strip()
    if cleaned.endswith(" bees"):
        cleaned = cleaned[:-5] + " bee"
    elif cleaned.endswith("ies") and len(cleaned) > 4:
        cleaned = cleaned[:-3] + "y"
    elif cleaned.endswith("us") or cleaned.endswith("is"):
        return cleaned
    elif cleaned.endswith("s") and len(cleaned) > 3 and not cleaned.endswith("ss"):
        singular = cleaned[:-1]
        if singular:
            cleaned = singular
    return cleaned


def _to_display_name(value: str) -> str:
    return " ".join(part.capitalize() for part in value.split())


def _stable_entity_key(entity_type: str, canonical_name: str) -> str:
    raw = f"{entity_type}:{canonical_name}"
    normalized = unicodedata.normalize("NFKC", raw).lower()
    normalized = re.sub(r"[^a-z0-9]+", "_", normalized).strip("_")
    normalized = re.sub(r"_+", "_", normalized)
    return normalized


def canonicalize_extraction(result: KGExtractionResult, ontology: Ontology) -> KGExtractionResult:
    # Canonicalization preserves mention provenance while collapsing stable ontology-
    # backed entities onto a consistent graph identity.
    mentions_by_id = {mention["mention_id"]: mention for mention in result.mentions}
    canonical_entities = []

    for entity in result.candidate_entities:
        entity_type = entity["proposed_type"]
        aliases = ontology.class_aliases.get(entity_type, set())
        surfaces = [entity.get("canonical_name", "")]
        surfaces.extend(
            mentions_by_id[mention_id]["text"]
            for mention_id in entity.get("mention_ids", [])
            if mention_id in mentions_by_id
        )
        normalized_surfaces = [_normalize_entity_surface(surface) for surface in surfaces if surface]

        canonical_name = None
        if _should_use_ontology_label(entity_type, normalized_surfaces, aliases):
            canonical_name = ontology.labels.get(entity_type, entity_type)
        else:
            for surface in normalized_surfaces:
                if not surface:
                    continue
                canonical_name = _to_display_name(surface)
                break

        canonical_name = canonical_name or ontology.labels.get(entity_type, entity_type)
        canonical_key = _stable_entity_key(entity_type, canonical_name)
        canonical_entities.append(
            {
                **entity,
                "canonical_name": canonical_name,
                "canonical_key": canonical_key,
            }
        )

    return KGExtractionResult(
        source_id=result.source_id,
        segment_id=result.segment_id,
        mentions=result.mentions,
        candidate_entities=canonical_entities,
        candidate_relations=result.candidate_relations,
        evidence=result.evidence,
    )


def extract_candidates(chunk: Chunk, ontology: Ontology) -> KGExtractionResult:
    return extract_candidates_with_meta(chunk, ontology).result


def extract_candidates_with_meta(chunk: Chunk, ontology: Ontology) -> KGExtractionArtifact:
    provider = _resolve_provider()
    if provider == "heuristic":
        result = _heuristic_extract(chunk, ontology)
        return KGExtractionArtifact(
            result=result,
            raw_payload={"mode": "heuristic", "ontology_tags": chunk_ontology_tags(chunk, ontology)},
            provider="heuristic",
            model="rules_v1",
            prompt_version="heuristic_v1",
        )
    if provider == "openai":
        return _openai_extract(chunk, ontology)
    raise KGExtractionError(f"Unsupported KG extraction provider: {provider}")


def _resolve_provider() -> str:
    provider = settings.kg_extraction_provider.strip().lower()
    if provider == "auto":
        return "openai" if settings.kg_api_key else "heuristic"
    return provider


def _heuristic_extract(chunk: Chunk, ontology: Ontology) -> KGExtractionResult:
    # The heuristic path is a deterministic fallback used when no external KG model is
    # configured. It is intentionally narrow and relation-light.
    mentions = []
    candidate_entities = []
    candidate_relations = []
    evidence = []
    lowered = chunk.text.lower()

    for ontology_class in chunk_ontology_tags(chunk, ontology):
        if ontology_class not in ontology.classes:
            continue
        label = ontology.labels.get(ontology_class, ontology_class)
        token = label.lower()
        start = lowered.find(token)
        if start == -1:
            alt = re.sub(r"(?<!^)([A-Z])", r" \1", ontology_class).lower()
            start = lowered.find(alt)
            token = alt if start != -1 else token
        if start == -1:
            continue

        mention_id = f"m{len(mentions) + 1}"
        candidate_id = f"e{len(candidate_entities) + 1}"
        mentions.append(
            {
                "mention_id": mention_id,
                "text": label,
                "type_hint": ontology_class,
                "start": start,
                "end": start + len(token),
                "confidence": 0.82,
            }
        )
        candidate_entities.append(
            {
                "candidate_id": candidate_id,
                "mention_ids": [mention_id],
                "proposed_type": ontology_class,
                "canonical_name": label,
                "external_ids": [],
                "confidence": 0.82,
            }
        )

    relation_specs = (
        ("produces", "produces"),
        ("affected by", "affectedBy"),
        ("treated by", "treatedBy"),
        ("requires equipment", "requiresEquipment"),
    )

    for phrase, predicate_name in relation_specs:
        if predicate_name not in ontology.predicates or phrase not in lowered or len(candidate_entities) < 2:
            continue
        subject = candidate_entities[0]["candidate_id"]
        object_id = candidate_entities[1]["candidate_id"]
        relation_id = f"r{len(candidate_relations) + 1}"
        candidate_relations.append(
            {
                "relation_id": relation_id,
                "subject_candidate_id": subject,
                "predicate_text": predicate_name,
                "object_candidate_id": object_id,
                "object_literal": None,
                "qualifiers": {},
                "confidence": 0.75,
            }
        )
        evidence.append(
            {
                "evidence_id": f"ev{len(evidence) + 1}",
                "supports": [relation_id],
                "excerpt": chunk.text[:240],
                "start": None,
                "end": None,
            }
        )

    return KGExtractionResult(
        source_id=chunk.document_id,
        segment_id=chunk.chunk_id,
        mentions=mentions,
        candidate_entities=candidate_entities,
        candidate_relations=candidate_relations,
        evidence=evidence,
    )


def _openai_extract(chunk: Chunk, ontology: Ontology) -> KGExtractionArtifact:
    # The model proposes candidates only; pruning and validation later decide what can be persisted.
    api_key = settings.kg_api_key
    if not api_key:
        raise KGExtractionError("KG_API_KEY is required for OpenAI KG extraction")

    # The prompt is strict on purpose: candidate extraction should stay ontology-
    # bounded and evidence-backed so later validation can remain deterministic.
    system_prompt = (
        "You extract candidate knowledge-graph objects from one source segment.\n"
        "Return JSON only and follow the schema exactly.\n"
        "Extract only what is directly supported by the segment.\n"
        "Use only allowed ontology class and predicate names.\n"
        "Do not invent new ontology terms.\n"
        "Predicate text must be exactly one allowed predicate string, with no suffixes, explanations, or parenthetical notes.\n"
        "If a relation is only implied, comparative, or speculative, omit it instead of inventing a predicate.\n"
        "Keep unsupported candidates out of the result.\n"
        "Separate mentions from canonical entities.\n"
        "Do not merge entities unless the segment itself provides stable identity.\n"
        "Respect predicate domain and range constraints.\n"
        "If a candidate subject or object would violate the predicate domain or range, omit that relation completely.\n"
        "Emit a relation only when its confidence is at least the minimum confidence threshold provided by the caller.\n"
        "Include evidence for every relation.\n"
        "If evidence is weak, lower confidence instead of inventing details.\n"
        "Prefer empty arrays over speculative output.\n"
        "Do not extract historical people, publishers, libraries, or generic book metadata unless they participate in an allowed ontology relation.\n"
        "Do not emit standalone entities if the segment does not support at least one allowed relation."
    )
    predicate_constraints = {
        predicate: {
            "kind": ontology.predicate_kinds.get(predicate),
            "domain": ontology.predicate_domains.get(predicate),
            "range": ontology.predicate_ranges.get(predicate),
        }
        for predicate in sorted(ontology.predicates)
    }
    predicate_rules = [
        {
            "predicate": predicate,
            "kind": ontology.predicate_kinds.get(predicate),
            "subject_type": ontology.predicate_domains.get(predicate),
            "object_type": ontology.predicate_ranges.get(predicate),
        }
        for predicate in sorted(ontology.predicates)
    ]
    user_prompt = "\n".join(
        [
            f"source_id: {chunk.document_id}",
            f"segment_id: {chunk.chunk_id}",
            f"document_title: {chunk.metadata.get('title') or chunk.metadata.get('section_title') or ''}",
            f"section_path: {' > '.join(chunk.section_path)}",
            f"allowed_classes: {json.dumps(sorted(ontology.classes), ensure_ascii=False)}",
            f"allowed_predicates: {json.dumps(sorted(ontology.predicates), ensure_ascii=False)}",
            f"predicate_constraints: {json.dumps(predicate_constraints, ensure_ascii=False)}",
            f"predicate_rules: {json.dumps(predicate_rules, ensure_ascii=False)}",
            f"minimum_confidence: {settings.kg_min_confidence}",
            "segment_text:",
            chunk.text,
        ]
    )

    response_schema = _kg_response_schema()
    headers = {
        "Authorization": f"Bearer {api_key}",
        "Content-Type": "application/json",
    }
    payload = {
        "model": settings.kg_model,
        "reasoning_effort": settings.kg_reasoning_effort,
        "messages": [
            {"role": "system", "content": system_prompt},
            {"role": "user", "content": user_prompt},
        ],
        "response_format": {
            "type": "json_schema",
            "json_schema": {
                "name": "kg_extraction_result",
                "strict": True,
                "schema": response_schema,
            },
        },
    }

    base_url = settings.kg_base_url.rstrip("/")
    retryable_statuses = {408, 409, 429, 500, 502, 503, 504}
    max_retries = max(0, settings.kg_max_retries)
    backoff = max(0.25, settings.kg_retry_backoff_seconds)

    try:
        with httpx.Client(timeout=settings.kg_timeout_seconds) as client:
            last_error: httpx.HTTPError | None = None
            for attempt in range(max_retries + 1):
                try:
                    response = client.post(
                        f"{base_url}/chat/completions",
                        headers=headers,
                        json=payload,
                    )
                    status_code = getattr(response, "status_code", None)
                    if status_code in retryable_statuses and attempt < max_retries:
                        time.sleep(backoff * (2 ** attempt))
                        continue
                    response.raise_for_status()
                    body = response.json()
                    break
                except httpx.HTTPError as exc:
                    last_error = exc
                    if attempt >= max_retries:
                        raise
                    time.sleep(backoff * (2 ** attempt))
            else:  # pragma: no cover - defensive only
                if last_error is not None:
                    raise last_error
                raise KGExtractionError("KG extraction request failed without a response payload")
    except httpx.HTTPError as exc:
        raise KGExtractionError(f"OpenAI KG extraction request failed: {exc}") from exc

    content = _extract_message_content(body)
    try:
        parsed = json.loads(content)
    except json.JSONDecodeError as exc:
        raise KGExtractionError(f"OpenAI KG extraction returned invalid JSON: {exc}") from exc

    result = _result_from_payload(chunk, parsed)
    return KGExtractionArtifact(
        result=result,
        raw_payload=body,
        provider="openai",
        model=settings.kg_model,
        prompt_version=settings.kg_prompt_version,
    )


def _extract_message_content(body: dict[str, Any]) -> str:
    choices = body.get("choices") or []
    if not choices:
        raise KGExtractionError("OpenAI KG extraction returned no choices")
    message = choices[0].get("message") or {}
    refusal = message.get("refusal")
    if refusal:
        raise KGExtractionError(f"OpenAI KG extraction refused: {refusal}")

    content = message.get("content")
    if isinstance(content, str):
        return content
    if isinstance(content, list):
        parts: list[str] = []
        for item in content:
            if isinstance(item, dict):
                text = item.get("text")
                if text:
                    parts.append(str(text))
        if parts:
            return "".join(parts)
    raise KGExtractionError("OpenAI KG extraction returned no textual content")


def _result_from_payload(chunk: Chunk, payload: dict[str, Any]) -> KGExtractionResult:
    # Normalize model output into the internal KG result shape so downstream pruning
    # and validation can operate on one canonical structure.
    mentions = [_normalize_mention(chunk.text, item, idx) for idx, item in enumerate(payload.get("mentions") or [], start=1)]
    mention_ids = {item["mention_id"] for item in mentions}

    candidate_entities = [
        _normalize_entity(item, idx, mention_ids)
        for idx, item in enumerate(payload.get("candidate_entities") or [], start=1)
    ]
    candidate_ids = {item["candidate_id"] for item in candidate_entities}

    candidate_relations = [
        _normalize_relation(item, idx, candidate_ids)
        for idx, item in enumerate(payload.get("candidate_relations") or [], start=1)
    ]
    relation_ids = {item["relation_id"] for item in candidate_relations}

    evidence = [
        _normalize_evidence(item, idx, relation_ids)
        for idx, item in enumerate(payload.get("evidence") or [], start=1)
    ]

    return KGExtractionResult(
        source_id=chunk.document_id,
        segment_id=chunk.chunk_id,
        mentions=mentions,
        candidate_entities=candidate_entities,
        candidate_relations=candidate_relations,
        evidence=evidence,
    )


def _normalize_mention(text: str, item: dict[str, Any], index: int) -> dict[str, Any]:
    mention_text = str(item.get("text") or "").strip()
    mention_id = str(item.get("mention_id") or f"m{index}")
    start = item.get("start")
    end = item.get("end")

    if mention_text and (not isinstance(start, int) or not isinstance(end, int) or start < 0 or end <= start):
        lowered = text.lower()
        needle = mention_text.lower()
        found = lowered.find(needle)
        if found != -1:
            start = found
            end = found + len(mention_text)

    return {
        "mention_id": mention_id,
        "text": mention_text,
        "type_hint": str(item.get("type_hint") or item.get("proposed_type") or "Unknown"),
        "start": start if isinstance(start, int) and start >= 0 else None,
        "end": end if isinstance(end, int) and end >= 0 else None,
        "confidence": _coerce_confidence(item.get("confidence"), default=0.5),
    }


def _normalize_entity(item: dict[str, Any], index: int, mention_ids: set[str]) -> dict[str, Any]:
    linked_mentions = [str(item_id) for item_id in item.get("mention_ids") or [] if str(item_id) in mention_ids]
    return {
        "candidate_id": str(item.get("candidate_id") or f"e{index}"),
        "mention_ids": linked_mentions,
        "proposed_type": str(item.get("proposed_type") or "").strip(),
        "canonical_name": _normalize_whitespace(str(item.get("canonical_name") or "")),
        "external_ids": list(item.get("external_ids") or []),
        "confidence": _coerce_confidence(item.get("confidence"), default=0.5),
    }


def _normalize_relation(item: dict[str, Any], index: int, candidate_ids: set[str]) -> dict[str, Any]:
    subject_id = str(item.get("subject_candidate_id") or "")
    object_id = str(item.get("object_candidate_id") or "") or None
    if subject_id not in candidate_ids:
        subject_id = subject_id
    if object_id and object_id not in candidate_ids:
        object_id = object_id

    object_literal = item.get("object_literal")
    if object_literal is not None:
        object_literal = _normalize_whitespace(str(object_literal))
    if object_literal == "":
        object_literal = None

    return {
        "relation_id": str(item.get("relation_id") or f"r{index}"),
        "subject_candidate_id": subject_id,
        "predicate_text": str(item.get("predicate_text") or "").strip(),
        "object_candidate_id": object_id,
        "object_literal": object_literal,
        "qualifiers": dict(item.get("qualifiers") or {}),
        "confidence": _coerce_confidence(item.get("confidence"), default=0.5),
    }


def _normalize_evidence(item: dict[str, Any], index: int, relation_ids: set[str]) -> dict[str, Any]:
    supports = [str(rel_id) for rel_id in item.get("supports") or [] if str(rel_id) in relation_ids]
    start = item.get("start")
    end = item.get("end")
    return {
        "evidence_id": str(item.get("evidence_id") or f"ev{index}"),
        "supports": supports,
        "excerpt": _normalize_whitespace(str(item.get("excerpt") or "")),
        "start": start if isinstance(start, int) and start >= 0 else None,
        "end": end if isinstance(end, int) and end >= 0 else None,
    }


def validate_extraction(result: KGExtractionResult, ontology: Ontology, min_confidence: float) -> tuple[bool, list[str]]:
    # Validation is strict and exhaustive. The output is a boolean plus machine-readable
    # errors so the caller can persist "review" state with specific reasons.
    errors: list[str] = []
    mention_ids = {mention["mention_id"] for mention in result.mentions}
    entity_ids = {entity["candidate_id"] for entity in result.candidate_entities}
    entities_by_id = {entity["candidate_id"]: entity for entity in result.candidate_entities}

    for mention in result.mentions:
        if not mention["text"]:
            errors.append(f"empty_mention_text:{mention['mention_id']}")

    for entity in result.candidate_entities:
        if entity["proposed_type"] not in ontology.classes:
            errors.append(f"invalid_entity_type:{entity['proposed_type']}")
        if not entity["canonical_name"]:
            errors.append(f"empty_entity_name:{entity['candidate_id']}")
        if not entity["mention_ids"]:
            errors.append(f"missing_mentions:{entity['candidate_id']}")
        for mention_id in entity["mention_ids"]:
            if mention_id not in mention_ids:
                errors.append(f"unknown_mention:{entity['candidate_id']}:{mention_id}")
        if entity["confidence"] < min_confidence:
            errors.append(f"low_entity_confidence:{entity['candidate_id']}")

    evidence_support = {item for ev in result.evidence for item in ev["supports"]}
    for relation in result.candidate_relations:
        predicate = relation["predicate_text"]
        relation_id = relation["relation_id"]
        if predicate not in ontology.predicates:
            errors.append(f"invalid_predicate:{predicate}")
        if relation["subject_candidate_id"] not in entity_ids:
            errors.append(f"missing_subject:{relation_id}")
        object_id = relation.get("object_candidate_id")
        object_literal = relation.get("object_literal")
        if object_id is not None and object_id not in entity_ids:
            errors.append(f"missing_object:{relation_id}")
        if object_id is None and not object_literal:
            errors.append(f"missing_relation_object:{relation_id}")
        if relation["confidence"] < min_confidence:
            errors.append(f"low_relation_confidence:{relation_id}")
        if relation_id not in evidence_support:
            errors.append(f"missing_evidence:{relation_id}")

        subject = entities_by_id.get(relation["subject_candidate_id"])
        expected_domain = ontology.predicate_domains.get(predicate)
        if subject and expected_domain and expected_domain in ontology.classes:
            actual_subject_type = subject["proposed_type"]
            if not is_type_compatible(actual_subject_type, expected_domain, ontology):
                errors.append(f"invalid_subject_type:{relation_id}:{actual_subject_type}->{expected_domain}")

        expected_range = ontology.predicate_ranges.get(predicate)
        predicate_kind = ontology.predicate_kinds.get(predicate)
        if object_id is not None:
            obj = entities_by_id.get(object_id)
            if predicate_kind == "datatype":
                errors.append(f"invalid_object_candidate_for_datatype:{relation_id}")
            elif obj and expected_range and expected_range in ontology.classes:
                actual_object_type = obj["proposed_type"]
                if not is_type_compatible(actual_object_type, expected_range, ontology):
                    errors.append(f"invalid_object_type:{relation_id}:{actual_object_type}->{expected_range}")
        elif object_literal:
            if predicate_kind == "object":
                errors.append(f"invalid_literal_for_object_property:{relation_id}")
            elif expected_range == "xsd:decimal":
                try:
                    float(object_literal)
                except (TypeError, ValueError):
                    errors.append(f"invalid_decimal_literal:{relation_id}")

    for evidence in result.evidence:
        if not evidence["supports"]:
            errors.append(f"orphan_evidence:{evidence['evidence_id']}")
        if not evidence["excerpt"]:
            errors.append(f"empty_evidence_excerpt:{evidence['evidence_id']}")

    return (len(errors) == 0, errors)


def prune_extraction(result: KGExtractionResult, ontology: Ontology, min_confidence: float) -> tuple[KGExtractionResult, list[str]]:
    # Pruning removes obviously invalid entities/relations before final validation.
    # This keeps the review queue focused on genuinely ambiguous cases.
    warnings: list[str] = []

    valid_mentions = [mention for mention in result.mentions if mention["text"]]
    mention_ids = {mention["mention_id"] for mention in valid_mentions}

    valid_entities = []
    for entity in result.candidate_entities:
        entity_errors: list[str] = []
        if entity["proposed_type"] not in ontology.classes:
            entity_errors.append(f"invalid_entity_type:{entity['proposed_type']}")
        if not entity["canonical_name"]:
            entity_errors.append(f"empty_entity_name:{entity['candidate_id']}")
        if entity["confidence"] < min_confidence:
            entity_errors.append(f"low_entity_confidence:{entity['candidate_id']}")
        linked_mentions = [mention_id for mention_id in entity["mention_ids"] if mention_id in mention_ids]
        if not linked_mentions:
            entity_errors.append(f"missing_mentions:{entity['candidate_id']}")

        if entity_errors:
            warnings.extend(entity_errors)
            continue

        valid_entities.append({**entity, "mention_ids": linked_mentions})

    entity_ids = {entity["candidate_id"] for entity in valid_entities}
    entities_by_id = {entity["candidate_id"]: entity for entity in valid_entities}

    preliminary_relations = []
    for relation in result.candidate_relations:
        relation_id = relation["relation_id"]
        predicate = relation["predicate_text"]
        relation_errors: list[str] = []

        if predicate not in ontology.predicates:
            relation_errors.append(f"invalid_predicate:{predicate}")
        if relation["subject_candidate_id"] not in entity_ids:
            relation_errors.append(f"missing_subject:{relation_id}")

        object_id = relation.get("object_candidate_id")
        object_literal = relation.get("object_literal")
        if object_id is not None and object_id not in entity_ids:
            relation_errors.append(f"missing_object:{relation_id}")
        if object_id is None and not object_literal:
            relation_errors.append(f"missing_relation_object:{relation_id}")
        if relation["confidence"] < min_confidence:
            relation_errors.append(f"low_relation_confidence:{relation_id}")

        subject = entities_by_id.get(relation["subject_candidate_id"])
        expected_domain = ontology.predicate_domains.get(predicate)
        if subject and expected_domain and expected_domain in ontology.classes:
            actual_subject_type = subject["proposed_type"]
            if not is_type_compatible(actual_subject_type, expected_domain, ontology):
                relation_errors.append(f"invalid_subject_type:{relation_id}:{actual_subject_type}->{expected_domain}")

        expected_range = ontology.predicate_ranges.get(predicate)
        predicate_kind = ontology.predicate_kinds.get(predicate)
        if object_id is not None:
            obj = entities_by_id.get(object_id)
            if predicate_kind == "datatype":
                relation_errors.append(f"invalid_object_candidate_for_datatype:{relation_id}")
            elif obj and expected_range and expected_range in ontology.classes:
                actual_object_type = obj["proposed_type"]
                if not is_type_compatible(actual_object_type, expected_range, ontology):
                    relation_errors.append(f"invalid_object_type:{relation_id}:{actual_object_type}->{expected_range}")
        elif object_literal:
            if predicate_kind == "object":
                relation_errors.append(f"invalid_literal_for_object_property:{relation_id}")
            elif expected_range == "xsd:decimal":
                try:
                    float(object_literal)
                except (TypeError, ValueError):
                    relation_errors.append(f"invalid_decimal_literal:{relation_id}")

        if relation_errors:
            warnings.extend(relation_errors)
            continue

        preliminary_relations.append(relation)

    preliminary_relation_ids = {relation["relation_id"] for relation in preliminary_relations}
    valid_evidence = []
    supported_relation_ids: set[str] = set()
    for evidence in result.evidence:
        if not evidence["excerpt"]:
            warnings.append(f"empty_evidence_excerpt:{evidence['evidence_id']}")
            continue
        supports = [relation_id for relation_id in evidence["supports"] if relation_id in preliminary_relation_ids]
        if not supports:
            warnings.append(f"orphan_evidence:{evidence['evidence_id']}")
            continue
        valid_evidence.append({**evidence, "supports": supports})
        supported_relation_ids.update(supports)

    valid_relations = []
    for relation in preliminary_relations:
        relation_id = relation["relation_id"]
        if relation_id not in supported_relation_ids:
            warnings.append(f"missing_evidence:{relation_id}")
            continue
        valid_relations.append(relation)

    if not valid_relations:
        return (
            KGExtractionResult(
                source_id=result.source_id,
                segment_id=result.segment_id,
                mentions=[],
                candidate_entities=[],
                candidate_relations=[],
                evidence=[],
            ),
            warnings,
        )

    referenced_entity_ids = {
        entity_id
        for relation in valid_relations
        for entity_id in [relation["subject_candidate_id"], relation.get("object_candidate_id")]
        if entity_id
    }
    if referenced_entity_ids:
        valid_entities = [entity for entity in valid_entities if entity["candidate_id"] in referenced_entity_ids]
        entity_ids = {entity["candidate_id"] for entity in valid_entities}
        valid_mentions = [
            mention
            for mention in valid_mentions
            if any(mention["mention_id"] in entity["mention_ids"] for entity in valid_entities)
        ]

    return (
        KGExtractionResult(
            source_id=result.source_id,
            segment_id=result.segment_id,
            mentions=valid_mentions,
            candidate_entities=valid_entities,
            candidate_relations=valid_relations,
            evidence=valid_evidence,
        ),
        warnings,
    )


def _normalize_whitespace(value: str) -> str:
    return re.sub(r"\s+", " ", value).strip()


def _coerce_confidence(value: Any, default: float) -> float:
    try:
        numeric = float(value)
    except (TypeError, ValueError):
        return default
    return max(0.0, min(1.0, numeric))


def _kg_response_schema() -> dict[str, Any]:
    return {
        "type": "object",
        "additionalProperties": False,
        "properties": {
            "mentions": {
                "type": "array",
                "items": {
                    "type": "object",
                    "additionalProperties": False,
                    "properties": {
                        "mention_id": {"type": "string"},
                        "text": {"type": "string"},
                        "type_hint": {"type": "string"},
                        "start": {"type": ["integer", "null"]},
                        "end": {"type": ["integer", "null"]},
                        "confidence": {"type": "number"},
                    },
                    "required": ["mention_id", "text", "type_hint", "start", "end", "confidence"],
                },
            },
            "candidate_entities": {
                "type": "array",
                "items": {
                    "type": "object",
                    "additionalProperties": False,
                    "properties": {
                        "candidate_id": {"type": "string"},
                        "mention_ids": {"type": "array", "items": {"type": "string"}},
                        "proposed_type": {"type": "string"},
                        "canonical_name": {"type": "string"},
                        "external_ids": {"type": "array", "items": {"type": "string"}},
                        "confidence": {"type": "number"},
                    },
                    "required": ["candidate_id", "mention_ids", "proposed_type", "canonical_name", "external_ids", "confidence"],
                },
            },
            "candidate_relations": {
                "type": "array",
                "items": {
                    "type": "object",
                    "additionalProperties": False,
                    "properties": {
                        "relation_id": {"type": "string"},
                        "subject_candidate_id": {"type": "string"},
                        "predicate_text": {"type": "string"},
                        "object_candidate_id": {"type": ["string", "null"]},
                        "object_literal": {"type": ["string", "null"]},
                        "qualifiers": {"type": "object", "additionalProperties": False, "properties": {}},
                        "confidence": {"type": "number"},
                    },
                    "required": [
                        "relation_id",
                        "subject_candidate_id",
                        "predicate_text",
                        "object_candidate_id",
                        "object_literal",
                        "qualifiers",
                        "confidence",
                    ],
                },
            },
            "evidence": {
                "type": "array",
                "items": {
                    "type": "object",
                    "additionalProperties": False,
                    "properties": {
                        "evidence_id": {"type": "string"},
                        "supports": {"type": "array", "items": {"type": "string"}},
                        "excerpt": {"type": "string"},
                        "start": {"type": ["integer", "null"]},
                        "end": {"type": ["integer", "null"]},
                    },
                    "required": ["evidence_id", "supports", "excerpt", "start", "end"],
                },
            },
        },
        "required": ["mentions", "candidate_entities", "candidate_relations", "evidence"],
    }
