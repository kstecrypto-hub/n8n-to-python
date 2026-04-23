# Ingestion Pipeline

## Table of contents
1. Pipeline overview
2. Stage-by-stage architecture
3. Recommended data contracts
4. Staging vs production graph writes
5. Incremental ingestion and reprocessing
6. Provenance and auditability
7. Monitoring and quality metrics
8. End-to-end example

## 1. Pipeline overview

A robust automatic ingestion mechanism is usually a multi-stage pipeline:

1. source acquisition
2. source parsing
3. segmentation into extraction units
4. candidate extraction
5. canonicalization
6. entity resolution
7. ontology/schema mapping
8. validation
9. upsert into graph
10. post-write checks and metrics

Do not compress all of this into one LLM call unless the scope is trivial and the graph is disposable.

## 2. Stage-by-stage architecture

### Stage 1: Source acquisition

Inputs may include:
- PDFs
- HTML pages
- emails
- transcripts
- tables or CSVs
- APIs
- event streams
- database dumps

Store source metadata immediately:
- source_id
- source_type
- URI/path
- retrieval timestamp
- checksum or content hash
- access scope or tenant

### Stage 2: Parsing

Convert raw source into machine-usable structure.

Examples:
- PDF -> pages, sections, paragraphs, tables
- HTML -> DOM-derived blocks
- CSV -> rows with headers
- transcript -> speaker turns

Preserve structure because it improves extraction and provenance.

### Stage 3: Segmentation

Split parsed content into extraction units.

The unit should be:
- small enough for precise extraction
- large enough to preserve local semantics
- stable enough for idempotent reprocessing

Recommended metadata per segment:
- segment_id
- parent_source_id
- page/section/row references
- byte/char offsets when available
- text content or serialized structured content

### Stage 4: Candidate extraction

Extract candidates, not final truth.

Typical outputs:
- mentions
- candidate entities
- candidate relations
- candidate events
- candidate attributes
- evidence anchors
- confidence scores

This stage can be hybrid:
- deterministic regex/rules for IDs, dates, measurements
- NER/model-based detection for entity spans
- LLM extraction for relation/event semantics

### Stage 5: Canonicalization

Normalize surface forms before linking.

Examples:
- case folding
- punctuation normalization
- unit normalization
- date normalization
- synonym normalization
- identifier normalization
- namespace normalization

Canonicalization outputs should remain separate from final entity resolution decisions.

### Stage 6: Entity resolution

Determine whether a mention or extracted candidate refers to an existing canonical entity.

Resolution evidence may include:
- exact identifier match
- normalized name match
- alias dictionary
- ontology/vocabulary mapping
- contextual features
- embedding similarity
- structural neighborhood
- LLM adjudication on ambiguous pairs

Resolution policy must define thresholds and fallback behavior.

### Stage 7: Ontology/schema mapping

Map extracted types and relation phrases into controlled graph vocabulary.

Examples:
- `drug` -> `Drug`
- `works at` -> `EMPLOYED_BY`
- `diagnosed with` -> `DiagnosisEvent`

Reject or quarantine candidates that cannot be mapped confidently.

### Stage 8: Validation

Validate before writing:
- required fields present
- type compatibility
- relation direction valid
- provenance present
- confidence above threshold if auto-accepting
- no impossible duplicates

### Stage 9: Upsert

Use idempotent writes.

Common write strategies:
- upsert canonical entities by stable key
- upsert mentions by source span id
- upsert facts by deterministic fact hash or explicit assertion id
- attach evidence edges rather than overwriting history blindly

### Stage 10: Post-write checks

Measure:
- number of segments processed
- number of extracted candidates
- accepted vs rejected assertions
- duplicate merge rate
- unresolved entity rate
- ontology mapping failure rate
- provenance completeness

## 3. Recommended data contracts

Use explicit JSON contracts between stages.

### Segment contract
```json
{
  "segment_id": "doc123:p4:s2",
  "source_id": "doc123",
  "text": "IBM acquired Red Hat in 2019.",
  "location": {"page": 4, "section": "M&A history"}
}
```

### Extraction contract
```json
{
  "segment_id": "doc123:p4:s2",
  "mentions": [
    {"mention_id": "m1", "text": "IBM", "type": "organization", "start": 0, "end": 3},
    {"mention_id": "m2", "text": "Red Hat", "type": "organization", "start": 13, "end": 20}
  ],
  "candidate_relations": [
    {
      "relation_id": "r1",
      "predicate_text": "acquired",
      "subject_mention_id": "m1",
      "object_mention_id": "m2",
      "time_text": "2019"
    }
  ]
}
```

### Normalized assertion contract
```json
{
  "assertion_id": "sha256:...",
  "subject": {"canonical_key": "org:ibm", "type": "Organization"},
  "predicate": "ACQUIRED",
  "object": {"canonical_key": "org:red_hat", "type": "Organization"},
  "qualifiers": {"effective_date": "2019"},
  "evidence": [{"segment_id": "doc123:p4:s2", "mention_ids": ["m1", "m2"]}],
  "confidence": 0.94
}
```

## 4. Staging vs production graph writes

For high-value systems, do not write directly from extractor to production graph.

Use at least one staging layer:
- raw extraction store
- normalized candidate store
- validated accepted assertion stream

Benefits:
- auditability
- easier replay
- safer ontology changes
- human review option
- debugging of merge errors

## 5. Incremental ingestion and reprocessing

A real ingestion mechanism must handle:
- new documents
- updated documents
- deleted or superseded sources
- improved extraction models
- changed ontology mappings

Recommended controls:
- source content hash
- extraction run id
- model version
- mapping rules version
- reprocess policy by source or segment

## 6. Provenance and auditability

Store enough metadata to answer:
- which source produced this fact?
- which segment or row supported it?
- which extractor or model version proposed it?
- when was it ingested?
- was it accepted automatically or after review?

## 7. Monitoring and quality metrics

Track at minimum:
- extraction precision/recall if labeled data exists
- unresolved mention rate
- duplicate canonical entity creation rate
- relation normalization coverage
- ontology mapping rejection rate
- average evidence count per accepted assertion
- stale source reprocessing backlog

## 8. End-to-end example

Text: “Alice joined Contoso in March 2025 as VP Engineering.”

Expected flow:
1. parse sentence as one segment
2. extract mentions: Alice, Contoso, March 2025, VP Engineering
3. canonicalize date and title
4. resolve Alice and Contoso if existing
5. map relation into event or role model
6. validate role structure
7. write canonical entities, employment/role event, and evidence links

Prefer this model over a single weak edge like `Alice WORKS_AT Contoso` if title and start date matter.
