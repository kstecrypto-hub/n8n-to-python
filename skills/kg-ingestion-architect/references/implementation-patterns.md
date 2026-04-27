# Implementation Patterns

## Table of contents
1. Recommended architecture
2. Property graph persistence pattern
3. RDF/OWL persistence pattern
4. Hybrid pattern
5. Identifier strategy
6. Example write patterns
7. Testing strategy
8. Operational guidance

## 1. Recommended architecture

Default architecture for agent-facing graph ingestion:

- source connectors
- parser layer
- segment store
- extraction service
- normalization and resolution service
- ontology mapping service
- validation layer
- graph writer
- raw text/evidence store
- metrics and replay queue

Keep the pipeline modular. The graph writer should not be responsible for parsing raw PDFs or inferring ontology classes from scratch.

## 2. Property graph persistence pattern

Typical node families:
- canonical entities
- mentions
- events
- source artifacts
- extraction runs

Typical edge families:
- world facts
- mention-to-entity links
- source/evidence links
- event participation links

### Example conceptual pattern

Nodes:
- `(:Organization {id, name, normalized_name})`
- `(:Mention {id, text, type, source_segment_id})`
- `(:Document {id, uri, hash})`
- `(:AcquisitionEvent {id, effective_date})`

Edges:
- `(mention)-[:REFERS_TO]->(organization)`
- `(document)-[:HAS_SEGMENT]->(segment)`
- `(event)-[:ACQUIRER]->(org1)`
- `(event)-[:ACQUIRED]->(org2)`
- `(event)-[:EVIDENCED_BY]->(segment)`

### Cypher upsert mindset

Use `MERGE` for canonical keys, not display names.
Attach evidence separately instead of replacing old facts silently.

Pseudo-pattern:
```cypher
MERGE (o:Organization {id: $org_id})
ON CREATE SET o.name = $name, o.normalized_name = $normalized_name
ON MATCH SET o.last_seen_at = timestamp()
```

For assertions/events, create deterministic ids when possible so reprocessing is idempotent.

## 3. RDF/OWL persistence pattern

Use IRIs for canonical entities and named graphs or provenance nodes when source traceability matters.

Pattern:
- canonical entity IRI
- triple assertion
- provenance triple set or named graph

Example conceptually:
- `ex:org_ibm rdf:type ex:Organization`
- `ex:acq_evt_2019 ex:acquirer ex:org_ibm`
- `ex:acq_evt_2019 ex:acquired ex:org_redhat`
- `ex:acq_evt_2019 prov:wasDerivedFrom ex:segment_doc123_p4_s2`

## 4. Hybrid pattern

A hybrid design often works well for agent systems:

- property graph for operational traversal and application queries
- RDF/OWL layer for ontology-backed exchange or specialized reasoning
- shared canonical ids or crosswalk table between both representations

Only use hybrid if there is a real reason to maintain two semantic views.

## 5. Identifier strategy

Use stable identifiers that do not depend on current display text.

Good identifier sources:
- source system ids
- external vocabulary ids
- deterministic derived ids from trusted attributes
- UUIDs with persistent mapping registry

Store separately:
- internal canonical id
- external ids
- aliases
- display labels

## 6. Example write patterns

### Pattern A: mention then resolve
1. store mention
2. attempt canonical resolution
3. if resolved, link `REFERS_TO`
4. if unresolved, keep mention and queue for later review

### Pattern B: event-first modeling
For temporally rich facts:
1. create event node
2. attach participant role edges
3. attach date and qualifiers
4. link evidence

### Pattern C: accepted assertion layer
For high-trust systems:
1. store candidate extraction outside production graph
2. create accepted assertion objects only after validation
3. materialize simpler direct edges for retrieval if helpful

## 7. Testing strategy

Every ingestion mechanism should be tested at multiple levels:

### Unit tests
- normalization functions
- mapping functions
- deterministic ID generation
- schema validation

### Extraction tests
- gold examples for entity/relation/event extraction
- ambiguous examples for merge behavior
- negation/speculation examples where applicable

### End-to-end tests
- one source -> expected graph snapshot
- re-ingestion -> no duplicate canonical entities
- ontology mapping change -> controlled migration outcome

### Property-based checks
- every accepted assertion has evidence
- canonical ids are unique
- invalid predicate/type combinations are rejected

## 8. Operational guidance

- Keep replay capability for failed or improved runs.
- Version extraction prompts and mapping tables.
- Log unresolved entities explicitly.
- Track merge reversibility for risky domains.
- Keep production graph writes behind validation and metrics.
