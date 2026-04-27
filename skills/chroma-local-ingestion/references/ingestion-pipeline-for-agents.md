# Ingestion Pipeline for Agents

## Table of contents
1. Pipeline overview
2. Source parsing
3. Chunk construction
4. Metadata construction
5. Embedding stage
6. Collection write stage
7. Update and delete flow
8. Retrieval assembly
9. Evaluation and replay

## 1. Pipeline overview

A robust local Chroma ingestion pipeline for an agent usually has these stages:

1. source acquisition
2. parsing and normalization
3. chunk construction
4. metadata construction
5. id generation
6. embedding
7. write or upsert into Chroma
8. validation and retrieval evaluation

The vector store should be downstream of parsing and chunk design, not the first place raw documents land.

## 2. Source parsing

Parse source materials into a stable intermediate representation before chunking.

Examples:
- PDF -> pages, headings, paragraph blocks
- HTML -> cleaned text blocks with structure
- code repo -> files, symbols, functions, README sections
- transcript -> speaker turns with timestamps
- CSV -> row narratives or normalized records

The parser should preserve enough structure to support good chunk boundaries and metadata.

## 3. Chunk construction

Chunking is one of the main determinants of retrieval quality.

Choose chunk units based on retrieval behavior:
- policies or docs -> section-aware paragraphs
- code -> function/class or logical snippet chunks
- transcripts -> turn bundles or time windows
- tables -> row summaries or row groups

Chunking rules should define:
- max length target
- overlap policy
- heading inheritance
- parent document id
- source offsets or locations
- chunk text normalization policy

## 4. Metadata construction

Every chunk should carry metadata that helps retrieval and debugging.

Good default metadata fields:
- `source_id`
- `document_id`
- `chunk_id`
- `section`
- `page` or `line_range`
- `content_type`
- `tenant`
- `created_at` or `updated_at`
- `version`
- `tags`

Do not depend on embedded text alone for provenance or filtering.

## 5. Embedding stage

The embedding stage must be explicit even if Chroma performs embedding implicitly via the collection’s embedding function.

Questions to answer:
- who creates embeddings: Chroma collection or upstream code?
- which local model is used?
- is the same model used for writes and queries?
- what batch size is safe locally?
- how will model/version changes trigger re-index?

## 6. Collection write stage

Recommended write policy:
- use deterministic ids per chunk
- batch writes
- keep metadata stable and filterable
- separate initial writes from updates when possible
- validate record counts after batch operations

A deterministic chunk id often combines:
- source id
- location or structural position
- normalized content hash
- version if needed

## 7. Update and delete flow

A local agent pipeline must answer:
- what happens when a source file changes?
- what happens when a source disappears?
- what happens when chunk boundaries change?
- what happens when the embedding model changes?

Suggested policies:
- content change -> regenerate affected chunk ids or version-aware replacements
- source delete -> remove chunks by filter on source metadata
- parser/chunker change -> re-index affected collection or corpus version
- embedding model change -> create a new collection and backfill

## 8. Retrieval assembly

At query time, the agent usually:
1. receives user task or question
2. converts it to query text or embedding input
3. queries Chroma with filters if needed
4. retrieves top-k chunks
5. optionally reranks or groups results
6. assembles context for the model

The ingestion pipeline must support this by ensuring stored chunks are interpretable and metadata-rich.

## 9. Evaluation and replay

Do not stop at “write succeeded.”

Evaluate with:
- representative user queries
- precision at top-k
- redundancy rate
- missing-context failures
- filter correctness
- stale-content behavior after updates

Keep replay ability so ingestion can be rerun after parser, chunker, or embedding changes.
