---
name: chroma-local-ingestion
description: design and implement fully local chroma-based vector store ingestion pipelines for agents. use when chatgpt must explain how chroma collections, embeddings, persistence, chunking, metadata, ids, querying, updates, deletes, and retrieval actually work so it can build a local vector database and ingestion mechanism without relying on cloud services.
---

# Chroma Local Ingestion

Build the local vector store layer of the agent correctly.

This skill is for tasks where the target system must ingest documents or semi-structured content into a **local ChromaDB-backed vector store** that an agent will use for retrieval. Explain the theory only as far as it improves implementation: what a vector store is, how Chroma collections work, how embedding functions shape the retrieval contract, and how to design a local-only ingestion pipeline that is stable, replayable, and maintainable.

## Working mode

Use this sequence unless the user explicitly asks for something narrower:

1. **Clarify the retrieval objective**
   Determine what the agent will retrieve: paragraphs, code chunks, policies, tickets, transcripts, multimodal items, or hybrid records.
2. **Choose the local Chroma deployment mode**
   Decide whether to use embedded local persistence or a locally hosted Chroma server. Prefer embedded local persistence unless a local service boundary is required.
3. **Define the ingestion contract**
   Specify source types, chunking unit, ids, metadata schema, embedding strategy, collection strategy, and replay/update behavior.
4. **Design the ingestion pipeline**
   Break the pipeline into parsing, chunking, normalization, embedding, write/upsert, validation, and retrieval evaluation.
5. **Generate implementation artifacts**
   Produce local setup instructions, collection layout, schemas, code scaffolds, and operational checks.
6. **Add local operational safeguards**
   Include persistence path discipline, deterministic ids, re-index rules, duplicate handling, and process-safety constraints.

## Decision logic

### 1) Decide what Chroma stores

Model the vector store as a retrieval layer, not as the system of record.

Chroma should normally store:
- chunk ids
- chunk text or content payloads
- metadata for filtering and tracing
- embeddings
- collection-level retrieval configuration

Keep raw documents, parsing artifacts, and canonical truth in adjacent storage layers when possible. Do not overload Chroma to be the only persistence layer for everything.

### 2) Decide deployment shape

Use **embedded local persistence** when the ingestion pipeline and retrieval layer run inside one local application or one local worker process.

Use a **local Chroma server** only when multiple local processes or components need a network boundary, service separation, or language interoperability.

If everything must remain local and simple, prefer embedded local persistence first.

### 3) Decide retrieval unit

Choose the unit the agent should retrieve:
- sentence bundle
- paragraph
- section chunk
- code block
- transcript turn bundle
- table row summary
- image/text pair if multimodal is required

The retrieval unit should match the user’s downstream questions. Overly large chunks hurt precision; overly small chunks destroy context.

## Required output structure

When using this skill, produce results in this default structure unless the user asks for code only:

1. **Objective and local deployment mode**
2. **What Chroma is storing**
3. **Collection design**
4. **Embedding strategy**
5. **Chunking and metadata contract**
6. **Ingestion/update/delete flow**
7. **Retrieval/query flow**
8. **Local setup and persistence rules**
9. **Implementation plan or code scaffold**
10. **Failure modes and mitigations**

## Non-negotiable design rules

- Treat the **embedding function + collection configuration** as part of the retrieval contract.
- Use **deterministic chunk ids** so re-ingestion is idempotent.
- Store enough **metadata** to filter, trace, and debug retrieval results.
- Keep **raw source identifiers** outside the embedding text and inside metadata fields too.
- Separate **source parsing**, **chunking**, **embedding**, and **write** into inspectable stages.
- Do not switch embedding models on an existing collection casually; plan a new collection and re-index.
- Keep local persistence paths explicit and stable.
- Avoid concurrent writer designs that assume unlimited process safety on one local path.
- Evaluate retrieval quality with real queries, not only successful writes.

## Delivery guidance

### If the user asks for Chroma theory

Explain the theory only to support implementation. Use:

- `references/chroma-concepts-and-architecture.md`
- `references/setup-and-persistence.md`

### If the user asks for an ingestion mechanism

Use these references first:

- `references/ingestion-pipeline-for-agents.md`
- `references/chunking-embeddings-and-metadata.md`
- `references/operations-and-retrieval.md`
- `references/local-implementation-patterns.md`

### If the user asks for code

Prefer Python examples for local-first pipelines unless another stack is explicitly requested.

## Minimum conceptual model to teach before coding

Before generating code, ensure the answer is grounded in these concepts:

- A **vector store** indexes embeddings so semantically similar items can be retrieved.
- In Chroma, the **collection** is the fundamental storage and query unit.
- A collection typically binds together ids, documents, metadata, embeddings, and an embedding function strategy.
- If the collection has an embedding function, Chroma can embed added documents and text queries for that collection.
- If no embedding function is attached, the pipeline must provide embeddings directly for writes and queries.
- The collection’s effective embedding dimension and retrieval setup must remain consistent.
- Query-time retrieval is not the same as exact lookup; exact retrieval uses ids and filters, semantic retrieval uses similarity search.

If any of these are missing, the answer is usually too shallow to build a dependable ingestion layer.

## Implementation preferences

When generating code or scaffolding:

- Prefer `PersistentClient` for local persisted pipelines unless a local server is explicitly needed.
- Prefer explicit metadata schemas and deterministic chunk ids.
- Prefer batched ingestion and inspectable intermediate chunk objects.
- Prefer one collection strategy chosen intentionally: per-tenant, per-domain, per-modality, or per-use-case.
- Prefer explicit retrieval evaluation queries and acceptance checks.

## What to avoid

Avoid these anti-patterns unless the user explicitly requests them:

- dumping entire documents into one giant vector
- switching embedding models in-place on an existing collection
- storing no metadata beyond raw text
- generating random ids on every run when updates are required
- using Chroma as the only source of truth for raw documents
- ignoring persistence path, re-index, or delete behavior
- assuming perfect local concurrency across multiple writer processes

## Reference map

- `references/chroma-concepts-and-architecture.md`: what Chroma is, what collections are, and how local vector retrieval works
- `references/setup-and-persistence.md`: install, local setup, persistence modes, and operational constraints
- `references/ingestion-pipeline-for-agents.md`: end-to-end local ingestion flow for agent retrieval
- `references/chunking-embeddings-and-metadata.md`: chunking strategy, embedding choices, metadata design, and ids
- `references/operations-and-retrieval.md`: add, upsert, update, delete, query, get, filters, and evaluation
- `references/local-implementation-patterns.md`: implementation templates and local-only architecture patterns
