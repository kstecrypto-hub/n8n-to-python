# Ingestion Architecture

## Objective

Rebuild the current ingestion mechanism as a clean, inspectable pipeline that:

- ingests source documents into a stable system of record
- creates logical, metadata-rich chunks
- validates chunk quality before embedding
- writes retrieval records into Chroma
- extracts knowledge-graph candidates under strict contracts
- validates KG output before persistence
- supports replay, versioning, and failure recovery

This document defines the target architecture for the rebuild.

## Scope

The rebuilt ingestion pipeline owns:

- source registration
- OCR result intake
- parsing and normalization
- chunk construction
- chunk validation and quarantine
- embedding generation
- Chroma write and delete/update behavior
- KG candidate extraction
- KG validation, canonicalization, and persistence
- ingestion job state, metrics, replay, and auditability

The rebuilt ingestion pipeline does not own:

- chat routing
- response generation
- agent serving logic
- old n8n chat branches
- general application authentication flows

## Design Principles

- Postgres is the system of record.
- Chroma is the vector retrieval layer, not the source of truth.
- KG writes must be staged and validated before acceptance.
- Every accepted chunk and graph assertion must have provenance.
- Every stage must be replayable.
- Every write path must be idempotent.
- Quality gates must block bad OCR, bad chunks, and malformed KG output from entering downstream stores.

## High-Level Architecture

```text
source intake
  -> source registry
  -> OCR content available
  -> parser
  -> structured blocks
  -> chunk builder
  -> chunk validator
      -> accepted chunks -> embedder -> Chroma
      -> accepted chunks -> KG extractor -> KG validator -> Postgres KG tables
      -> rejected chunks -> quarantine
  -> completion metrics and audit log
```

## Runtime Components

### API

Responsibilities:

- register documents and ingestion jobs
- accept uploads or references to OCR-complete artifacts
- expose job status and errors
- expose replay endpoints later

### Worker

Responsibilities:

- run parsing, chunking, validation, embedding, KG extraction, and persistence
- update job and stage status
- write audit and metrics records

### Postgres

Responsibilities:

- system of record for documents, jobs, chunks, validation results, KG, and audit data

### Chroma

Responsibilities:

- store accepted chunk embeddings and retrieval metadata

### Optional Queue

Later addition if needed:

- decouple API submission from worker execution
- retries and concurrency control

## Stage Model

Each source document flows through these stages:

1. `registered`
2. `content_available`
3. `parsed`
4. `chunked`
5. `chunks_validated`
6. `embedded`
7. `indexed`
8. `kg_extracted`
9. `kg_validated`
10. `kg_persisted`
11. `completed`
12. `failed`
13. `quarantined`

Each stage must record:

- `job_id`
- `document_id`
- `stage_name`
- `status`
- `started_at`
- `finished_at`
- `attempt`
- `worker_version`
- `input_version`
- `error_code`
- `error_message`
- `metrics_json`

## Source Intake Contract

The ingestion pipeline starts from content that is already acquired. OCR may be upstream, but ingestion must still treat OCR output as structured input, not final truth.

### Source Record

```json
{
  "source_id": "src_...",
  "document_id": "doc_...",
  "tenant_id": "tenant_...",
  "source_type": "pdf|image|html|text",
  "filename": "bees-book.pdf",
  "content_hash": "sha256:...",
  "parser_version": "v1",
  "ocr_engine": "gemini_document_node",
  "ocr_model": "models/gemini-2.5-flash",
  "content_text": "..."
}
```

## Parsing Contract

The parser converts raw text or OCR text into structured blocks.

### Parsed Block

```json
{
  "block_id": "doc123:block:17",
  "document_id": "doc123",
  "page": 4,
  "section_path": ["Chapter 2", "Swarm Control"],
  "block_type": "paragraph",
  "char_start": 1820,
  "char_end": 2380,
  "text": "..."
}
```

Rules:

- preserve page and section information whenever available
- preserve positional offsets when available
- keep headers, paragraphs, tables, and figure text distinct

## Chunk Construction Contract

Chunking must be structure-aware, not raw fixed-width slicing.

### Chunk Rules

- prefer paragraph and section boundaries
- merge small adjacent blocks only when they are semantically connected
- apply overlap only when needed for continuity
- preserve section inheritance
- generate deterministic chunk IDs

### Chunk Record

```json
{
  "chunk_id": "doc123:sec2:p04:c0007:sha256(...)",
  "document_id": "doc123",
  "tenant_id": "tenant_1",
  "chunk_index": 7,
  "page_start": 4,
  "page_end": 4,
  "section_path": ["Chapter 2", "Swarm Control"],
  "prev_chunk_id": "doc123:...:c0006:...",
  "next_chunk_id": "doc123:...:c0008:...",
  "char_start": 1820,
  "char_end": 2710,
  "content_type": "text",
  "text": "...",
  "parser_version": "v1",
  "chunker_version": "v1"
}
```

## Chunk Validation

Chunk validation is a hard gate before embedding or KG extraction.

### Validation Checks

- non-empty text
- minimum and maximum content length
- low OCR noise score
- not mostly repeated boilerplate
- not mostly navigation/footer/header noise
- sufficient semantic density
- required provenance fields present
- adjacency consistency is valid

### Validation Result

```json
{
  "chunk_id": "doc123:...",
  "status": "accepted|rejected|review",
  "quality_score": 0.87,
  "reasons": ["low_boilerplate", "good_length", "provenance_complete"]
}
```

Rejected chunks go to quarantine and are not embedded.

## Embedding and Chroma Contract

Accepted chunks are embedded and written to Chroma.

### Chroma Strategy

- Chroma is local
- use deterministic chunk IDs
- use one collection strategy intentionally
- keep embedding model version explicit
- on embedding model changes, create a new collection version or clean re-index path

### Initial Collection Strategy

Phase 1 default:

- one collection per tenant and embedding version

Example:

- `tenant_default__embed_v1`

If tenant isolation is not needed immediately, use:

- `kb_chunks_v1`

### Chroma Metadata

```json
{
  "chunk_id": "doc123:...",
  "document_id": "doc123",
  "source_id": "src_123",
  "tenant_id": "tenant_1",
  "filename": "bees-book.pdf",
  "section": "Swarm Control",
  "page_start": 4,
  "page_end": 4,
  "chunk_index": 7,
  "prev_chunk_id": "doc123:...:c0006:...",
  "next_chunk_id": "doc123:...:c0008:...",
  "content_type": "text",
  "language": "en",
  "parser_version": "v1",
  "chunker_version": "v1",
  "embedding_version": "text-embedding-3-large",
  "ingested_at": "2026-03-16T00:00:00Z"
}
```

## KG Extraction Architecture

KG extraction runs only on accepted chunks.

The KG pipeline is staged:

1. mention extraction
2. candidate entity extraction
3. candidate relation or event extraction
4. canonicalization
5. ontology mapping
6. entity resolution
7. assertion validation
8. persistence

### Core Rule

The extractor produces candidates, not final truth.

### Extraction Contract

```json
{
  "source_id": "src_123",
  "segment_id": "doc123:chunk:7",
  "mentions": [
    {
      "mention_id": "m1",
      "text": "Varroa mite",
      "type_hint": "parasite",
      "start": 15,
      "end": 26,
      "confidence": 0.95
    }
  ],
  "candidate_entities": [
    {
      "candidate_id": "e1",
      "mention_ids": ["m1"],
      "proposed_type": "Parasite",
      "canonical_name": "Varroa mite",
      "external_ids": [],
      "confidence": 0.91
    }
  ],
  "candidate_relations": [
    {
      "relation_id": "r1",
      "subject_candidate_id": "e1",
      "predicate_text": "infests",
      "object_candidate_id": "e2",
      "qualifiers": {},
      "confidence": 0.82
    }
  ],
  "candidate_events": [],
  "evidence": [
    {
      "evidence_id": "ev1",
      "supports": ["r1"],
      "excerpt": "..."
    }
  ]
}
```

### KG Validation Rules

- JSON schema must validate
- evidence is mandatory for accepted assertions
- ontology classes and predicates must be from allowed inventory
- relation direction must be valid
- required fields must be present
- confidence thresholds must be enforced
- unresolved mappings go to review or quarantine

### Persistence Rule

Do not write extractor output directly into final KG tables.

Write flow:

- raw extraction table
- validated assertion table
- accepted canonical entities and assertions

## Postgres Storage Model

Postgres remains the source of truth.

### Required Tables

- `documents`
- `document_sources`
- `ingestion_jobs`
- `ingestion_stage_runs`
- `parsed_blocks`
- `document_chunks`
- `chunk_validations`
- `kg_raw_extractions`
- `kg_entity_mentions`
- `kg_candidate_assertions`
- `kg_entities`
- `kg_assertions`
- `kg_assertion_evidence`
- `ingestion_audit_log`

### Minimal Chunk Table Fields

- `chunk_id`
- `document_id`
- `tenant_id`
- `chunk_index`
- `page_start`
- `page_end`
- `section_path`
- `prev_chunk_id`
- `next_chunk_id`
- `char_start`
- `char_end`
- `text`
- `parser_version`
- `chunker_version`
- `status`
- `content_hash`

## Replay and Versioning

Replay is a first-class requirement.

Triggers for replay:

- parser version changes
- chunker version changes
- embedding model changes
- ontology changes
- KG prompt/schema changes
- source content changes

Replay requirements:

- deterministic IDs where possible
- stage-specific reruns
- ability to remove or replace old Chroma records
- ability to invalidate and rebuild KG assertions derived from affected chunks

## Failure Handling

Every stage must fail explicitly.

Failure classes:

- parser failure
- chunk validation failure
- embedding failure
- vector write failure
- KG extraction failure
- KG schema validation failure
- KG mapping failure
- persistence failure

Failure policy:

- retry transient infrastructure failures
- do not retry deterministic validation failures blindly
- quarantine malformed content
- keep raw outputs for debugging

## Runtime Topology

### Phase 1

- `api`
- `worker`
- `postgres`
- `chroma`

### Phase 2

- add `redis` or queue if needed

### Container Responsibilities

#### `api`

- accepts document registrations
- starts jobs
- exposes job status

#### `worker`

- parser
- chunker
- validator
- embedder
- Chroma writer
- KG extractor
- KG validator
- KG persister

#### `postgres`

- all durable relational and KG state

#### `chroma`

- vector retrieval store

## Docker and Compose Requirements

The runtime must include:

- one `compose.yaml`
- named volumes for Postgres and Chroma persistence
- health checks for Postgres and Chroma
- service names used for internal networking
- explicit `.env.example`
- app and worker built separately if they differ in lifecycle

## Phased Implementation Plan

### Phase 1: Foundation

- create new Postgres schema for ingestion state
- build source registration flow
- build parser contract
- build chunker
- build chunk validation

### Phase 2: Vector Indexing

- add embedding stage
- add Chroma collection management
- add deterministic Chroma writes
- add retrieval smoke tests

### Phase 3: KG Candidate Pipeline

- add raw extraction contract
- add ontology inventory and mapping rules
- add KG validation layer
- persist raw and accepted outputs separately

### Phase 4: Replay and Operations

- add replay by document and by version
- add metrics and audit views
- add quarantine and review workflow
- add cleanup and delete flows

### Phase 5: Local Runtime

- add Dockerfiles
- add Compose stack
- add runbook, logs, reset, and recovery steps

## Acceptance Criteria

The rebuilt ingestion pipeline is acceptable only if:

- chunk boundaries are inspectable and logical
- every chunk has provenance and adjacency metadata
- bad chunks are blocked before embedding
- Chroma entries are deterministic and filterable
- retrieval quality is evaluated with real test queries
- KG output is schema-constrained and evidence-backed
- malformed KG output is quarantined
- document reprocessing does not corrupt state
- the stack runs locally with persistent Postgres and Chroma data

## Immediate Next Artifacts

The next implementation documents to create from this architecture are:

1. Postgres schema spec
2. chunking and validation spec
3. Chroma collection and metadata spec
4. KG extraction contract spec
5. worker module layout
6. Docker Compose spec
