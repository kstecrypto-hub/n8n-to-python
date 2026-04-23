import json

from src.bee_ingestion.chunking import build_chunks, parse_text
from src.bee_ingestion.models import KGExtractionResult
from src.bee_ingestion.kg import canonicalize_extraction, extract_candidates, extract_candidates_with_meta, load_ontology, prune_extraction, validate_extraction
from src.bee_ingestion.settings import settings


def test_kg_extraction_uses_allowed_ontology() -> None:
    ontology = load_ontology("data/beecore.ttl")
    blocks = parse_text(
        "doc1",
        "BEES\n\nVarroa affects bees. Bees produce honey in strong colonies.",
    )
    chunk = build_chunks("doc1", "tenant1", blocks, target_chars=400, min_chars=20)[0]
    result = extract_candidates(chunk, ontology)
    valid, errors = validate_extraction(result, ontology, 0.7)

    assert result.candidate_entities
    assert valid is True
    assert errors == []


def test_openai_kg_extraction_repairs_offsets_and_keeps_schema(monkeypatch) -> None:
    ontology = load_ontology("data/beecore.ttl")
    blocks = parse_text(
        "doc-openai",
        "Colony\n\nThe colony produces honey for the apiary.",
    )
    chunk = build_chunks("doc-openai", "tenant1", blocks, target_chars=400, min_chars=20)[0]

    payload = {
        "mentions": [
            {
                "mention_id": "m1",
                "text": "colony",
                "type_hint": "Colony",
                "start": None,
                "end": None,
                "confidence": 0.93,
            },
            {
                "mention_id": "m2",
                "text": "honey",
                "type_hint": "Honey",
                "start": None,
                "end": None,
                "confidence": 0.91,
            },
        ],
        "candidate_entities": [
            {
                "candidate_id": "e1",
                "mention_ids": ["m1"],
                "proposed_type": "Colony",
                "canonical_name": "Colony",
                "external_ids": [],
                "confidence": 0.93,
            },
            {
                "candidate_id": "e2",
                "mention_ids": ["m2"],
                "proposed_type": "Honey",
                "canonical_name": "Honey",
                "external_ids": [],
                "confidence": 0.91,
            },
        ],
        "candidate_relations": [
            {
                "relation_id": "r1",
                "subject_candidate_id": "e1",
                "predicate_text": "produces",
                "object_candidate_id": "e2",
                "object_literal": None,
                "qualifiers": {},
                "confidence": 0.92,
            }
        ],
        "candidate_events": [],
        "evidence": [
            {
                "evidence_id": "ev1",
                "supports": ["r1"],
                "excerpt": "The colony produces honey for the apiary.",
                "start": None,
                "end": None,
            }
        ],
    }

    class DummyResponse:
        def raise_for_status(self) -> None:
            return None

        def json(self):
            return {
                "choices": [
                    {
                        "message": {
                            "content": json.dumps(payload),
                        }
                    }
                ]
            }

    class DummyClient:
        def __init__(self, *args, **kwargs):
            pass

        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc, tb):
            return False

        def post(self, *args, **kwargs):
            return DummyResponse()

    monkeypatch.setattr(settings, "kg_extraction_provider", "openai")
    monkeypatch.setattr(settings, "kg_api_key", "test-key")
    monkeypatch.setattr(settings, "kg_base_url", "https://example.invalid/v1")
    monkeypatch.setattr("src.bee_ingestion.kg.httpx.Client", DummyClient)

    artifact = extract_candidates_with_meta(chunk, ontology)
    valid, errors = validate_extraction(artifact.result, ontology, 0.7)

    assert artifact.provider == "openai"
    assert artifact.result.mentions[0]["start"] is not None
    assert artifact.result.source_id == chunk.document_id
    assert artifact.result.segment_id == chunk.chunk_id
    assert valid is True
    assert errors == []


def test_relation_domain_and_range_validation_rejects_invalid_triple() -> None:
    ontology = load_ontology("data/beecore.ttl")
    result = KGExtractionResult(
        source_id="doc-1",
        segment_id="chunk-1",
        mentions=[
            {"mention_id": "m1", "text": "Drone", "type_hint": "Drone", "start": 0, "end": 5, "confidence": 0.9},
            {"mention_id": "m2", "text": "egg", "type_hint": "Egg", "start": 15, "end": 18, "confidence": 0.9},
        ],
        candidate_entities=[
            {"candidate_id": "e1", "mention_ids": ["m1"], "proposed_type": "Drone", "canonical_name": "Drone", "external_ids": [], "confidence": 0.9},
            {"candidate_id": "e2", "mention_ids": ["m2"], "proposed_type": "Egg", "canonical_name": "egg", "external_ids": [], "confidence": 0.9},
        ],
        candidate_relations=[
            {
                "relation_id": "r1",
                "subject_candidate_id": "e1",
                "predicate_text": "produces",
                "object_candidate_id": "e2",
                "object_literal": None,
                "qualifiers": {},
                "confidence": 0.95,
            }
        ],
        candidate_events=[],
        evidence=[{"evidence_id": "ev1", "supports": ["r1"], "excerpt": "Drone produces egg", "start": 0, "end": 18}],
    )

    valid, errors = validate_extraction(result, ontology, 0.7)

    assert valid is False
    assert any(error.startswith("invalid_subject_type:r1:Drone->Colony") for error in errors)
    assert any(error.startswith("invalid_object_type:r1:Egg->HiveProduct") for error in errors)


def test_prune_extraction_drops_invalid_relations_and_orphan_evidence() -> None:
    ontology = load_ontology("data/beecore.ttl")
    result = KGExtractionResult(
        source_id="doc-1",
        segment_id="chunk-1",
        mentions=[
            {"mention_id": "m1", "text": "Colony", "type_hint": "Colony", "start": 0, "end": 6, "confidence": 0.95},
            {"mention_id": "m2", "text": "Honey", "type_hint": "Honey", "start": 20, "end": 25, "confidence": 0.95},
            {"mention_id": "m3", "text": "Drone", "type_hint": "Drone", "start": 30, "end": 35, "confidence": 0.95},
        ],
        candidate_entities=[
            {"candidate_id": "e1", "mention_ids": ["m1"], "proposed_type": "Colony", "canonical_name": "Colony", "external_ids": [], "confidence": 0.95},
            {"candidate_id": "e2", "mention_ids": ["m2"], "proposed_type": "Honey", "canonical_name": "Honey", "external_ids": [], "confidence": 0.95},
            {"candidate_id": "e3", "mention_ids": ["m3"], "proposed_type": "Drone", "canonical_name": "Drone", "external_ids": [], "confidence": 0.95},
        ],
        candidate_relations=[
            {
                "relation_id": "r1",
                "subject_candidate_id": "e1",
                "predicate_text": "produces",
                "object_candidate_id": "e2",
                "object_literal": None,
                "qualifiers": {},
                "confidence": 0.95,
            },
            {
                "relation_id": "r2",
                "subject_candidate_id": "e3",
                "predicate_text": "produces",
                "object_candidate_id": "e2",
                "object_literal": None,
                "qualifiers": {},
                "confidence": 0.95,
            },
        ],
        candidate_events=[],
        evidence=[
            {"evidence_id": "ev1", "supports": ["r1"], "excerpt": "The colony produces honey.", "start": 0, "end": 25},
            {"evidence_id": "ev2", "supports": ["r2"], "excerpt": "Drone produces honey.", "start": 30, "end": 50},
            {"evidence_id": "ev3", "supports": [], "excerpt": "orphan", "start": 51, "end": 57},
        ],
    )

    pruned, warnings = prune_extraction(result, ontology, 0.7)
    valid, errors = validate_extraction(pruned, ontology, 0.7)

    assert valid is True
    assert errors == []
    assert [relation["relation_id"] for relation in pruned.candidate_relations] == ["r1"]
    assert [evidence["evidence_id"] for evidence in pruned.evidence] == ["ev1"]
    assert any(error.startswith("invalid_subject_type:r2:Drone->Colony") for error in warnings)
    assert any(error.startswith("orphan_evidence:ev3") for error in warnings)


def test_canonicalize_extraction_maps_generic_bee_roles_to_ontology_labels() -> None:
    ontology = load_ontology("data/beecore.ttl")
    result = KGExtractionResult(
        source_id="doc-1",
        segment_id="chunk-1",
        mentions=[
            {"mention_id": "m1", "text": "the queen", "type_hint": "Queen", "start": 0, "end": 9, "confidence": 0.9},
            {"mention_id": "m2", "text": "worker bees", "type_hint": "Worker", "start": 10, "end": 22, "confidence": 0.9},
            {"mention_id": "m3", "text": "drones", "type_hint": "Drone", "start": 23, "end": 29, "confidence": 0.9},
        ],
        candidate_entities=[
            {"candidate_id": "e1", "mention_ids": ["m1"], "proposed_type": "Queen", "canonical_name": "Queen (female bee)", "external_ids": [], "confidence": 0.9},
            {"candidate_id": "e2", "mention_ids": ["m2"], "proposed_type": "Worker", "canonical_name": "worker bees", "external_ids": [], "confidence": 0.9},
            {"candidate_id": "e3", "mention_ids": ["m3"], "proposed_type": "Drone", "canonical_name": "drone bees", "external_ids": [], "confidence": 0.9},
        ],
        candidate_relations=[],
        candidate_events=[],
        evidence=[],
    )

    canonical = canonicalize_extraction(result, ontology)

    assert canonical.candidate_entities[0]["canonical_name"] == "Queen"
    assert canonical.candidate_entities[0]["canonical_key"] == "queen_queen"
    assert canonical.candidate_entities[1]["canonical_name"] == "Worker"
    assert canonical.candidate_entities[1]["canonical_key"] == "worker_worker"
    assert canonical.candidate_entities[2]["canonical_name"] == "Drone"
    assert canonical.candidate_entities[2]["canonical_key"] == "drone_drone"


def test_canonicalize_extraction_does_not_over_merge_specific_surface_forms() -> None:
    ontology = load_ontology("data/beecore.ttl")
    result = KGExtractionResult(
        source_id="doc-1",
        segment_id="chunk-1",
        mentions=[
            {"mention_id": "m1", "text": "queen cell", "type_hint": "Queen", "start": 0, "end": 10, "confidence": 0.72},
        ],
        candidate_entities=[
            {"candidate_id": "e1", "mention_ids": ["m1"], "proposed_type": "Queen", "canonical_name": "queen cell", "external_ids": [], "confidence": 0.72},
        ],
        candidate_relations=[],
        candidate_events=[],
        evidence=[],
    )

    canonical = canonicalize_extraction(result, ontology)

    assert canonical.candidate_entities[0]["canonical_name"] == "Queen Cell"
    assert canonical.candidate_entities[0]["canonical_key"] == "queen_queen_cell"


def test_canonicalize_extraction_collapses_verbose_generic_class_mentions() -> None:
    ontology = load_ontology("data/beecore.ttl")
    result = KGExtractionResult(
        source_id="doc-1",
        segment_id="chunk-1",
        mentions=[
            {"mention_id": "m1", "text": "worker bees many thousand", "type_hint": "Worker", "start": 0, "end": 25, "confidence": 0.95},
            {"mention_id": "m2", "text": "hive of bees june composition", "type_hint": "Colony", "start": 26, "end": 55, "confidence": 0.95},
        ],
        candidate_entities=[
            {"candidate_id": "e1", "mention_ids": ["m1"], "proposed_type": "Worker", "canonical_name": "worker bees many thousand", "external_ids": [], "confidence": 0.95},
            {"candidate_id": "e2", "mention_ids": ["m2"], "proposed_type": "Colony", "canonical_name": "hive of bees june composition", "external_ids": [], "confidence": 0.95},
        ],
        candidate_relations=[],
        candidate_events=[],
        evidence=[],
    )

    canonical = canonicalize_extraction(result, ontology)

    assert canonical.candidate_entities[0]["canonical_name"] == "Worker"
    assert canonical.candidate_entities[0]["canonical_key"] == "worker_worker"
    assert canonical.candidate_entities[1]["canonical_name"] == "Hive Of Bees June Composition"
    assert canonical.candidate_entities[1]["canonical_key"] == "colony_hive_of_bees_june_composition"


def test_canonicalize_extraction_collapses_quantified_drone_mentions() -> None:
    ontology = load_ontology("data/beecore.ttl")
    result = KGExtractionResult(
        source_id="doc-1",
        segment_id="chunk-1",
        mentions=[
            {"mention_id": "m1", "text": "drone bees several hundred to few thousand", "type_hint": "Drone", "start": 0, "end": 42, "confidence": 0.94},
        ],
        candidate_entities=[
            {"candidate_id": "e1", "mention_ids": ["m1"], "proposed_type": "Drone", "canonical_name": "drone bees several hundred to few thousand", "external_ids": [], "confidence": 0.94},
        ],
        candidate_relations=[],
        candidate_events=[],
        evidence=[],
    )

    canonical = canonicalize_extraction(result, ontology)

    assert canonical.candidate_entities[0]["canonical_name"] == "Drone"
    assert canonical.candidate_entities[0]["canonical_key"] == "drone_drone"
