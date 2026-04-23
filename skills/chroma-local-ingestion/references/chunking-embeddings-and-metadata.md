# Chunking, Embeddings, and Metadata

## Table of contents
1. Chunking principles
2. Chunk id strategy
3. Embedding strategy
4. Collection strategy
5. Metadata schema design
6. Retrieval quality implications
7. Anti-patterns

## 1. Chunking principles

Chunk for the retrieval question, not for arbitrary token counts alone.

Good chunking preserves:
- semantic cohesion
- enough local context to answer a query
- provenance boundaries
- repeatable reconstruction on re-ingest

Examples:
- policy docs: section-aware paragraph chunks
- code: function/class and nearby comments
- transcripts: grouped speaker turns with timestamps
- tables: row-level summaries with key fields in metadata

## 2. Chunk id strategy

Chunk ids must be stable enough for idempotent updates.

Good chunk id ingredients:
- source identifier
- location identifier or structural path
- normalized text hash or version component

Examples:
- `doc123:sec4:p2:sha256(...)`
- `repoA:file_x.py:function_parse_config:v3`

Bad practice:
- random UUID on every ingestion run when the chunk represents the same logical unit

## 3. Embedding strategy

Embedding choice is not a side detail. It defines what semantic similarity means in the system.

Rules:
- use one consistent model family per collection
- use the same model for ingestion and text query embedding
- keep language/domain fit in mind
- define when a model change causes a new collection

If the collection is configured to embed documents and queries, the application must still document that contract clearly.

## 4. Collection strategy

Possible strategies:

### Per tenant
Use when isolation by customer or workspace matters.

### Per domain or corpus
Use when different corpora have distinct semantics or lifecycle.

### Per modality
Use when text and image retrieval are separate concerns.

### Per embedding version
Use when changing embeddings requires clean separation and re-index.

Do not place unrelated corpora with different retrieval behavior into one collection without a compelling reason.

## 5. Metadata schema design

Metadata is critical for retrieval quality and operational control.

Useful metadata categories:

### Provenance
- source id
- path or URI
- page/section/line range
- version

### Filtering
- tenant
- doc type
- language
- access scope
- product area

### Operational
- ingestion timestamp
- parser version
- chunker version
- embedding version label

Make metadata field names stable. Ingestion pipelines rot when metadata conventions drift.

## 6. Retrieval quality implications

Poor metadata and chunking create failure modes such as:
- wrong document family retrieved
- duplicates crowding top-k
- result fragments with no context
- inability to filter stale or unauthorized material
- impossible debugging because chunk provenance is missing

## 7. Anti-patterns

- one huge chunk per file
- tiny sentence shards with no parent context
- missing source ids in metadata
- no version fields when updates matter
- mixing chunks produced by incompatible embedding strategies in one collection
