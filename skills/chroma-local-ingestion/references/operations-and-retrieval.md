# Operations and Retrieval

## Table of contents
1. Core operations
2. Add vs upsert vs update mindset
3. Query semantics
4. Get semantics
5. Filtering
6. Delete behavior
7. Collection replacement strategy
8. Retrieval evaluation checklist

## 1. Core operations

At a practical level, a Chroma-backed ingestion layer needs to reason about:
- create/get collection
- add records
- upsert or update existing records as required by the chosen API and workflow
- retrieve semantically with query
- retrieve directly with get
- delete stale records
- rebuild or replace collections when retrieval contracts change

## 2. Add vs upsert vs update mindset

Implementation guidance:

- treat initial ingestion as a controlled write of known chunk ids
- treat re-ingestion as an idempotent reconciliation problem
- define whether same-id records replace, update, or should be deleted and recreated based on the chosen code path

Codex should make the update behavior explicit in the implementation rather than leaving it implicit.

## 3. Query semantics

Semantic retrieval should answer:
- which chunks are nearest to this query in embedding space?
- how many results are needed?
- should metadata filters narrow the search?
- should results be deduplicated or grouped by source afterwards?

Query guidance:
- use text queries when the collection embeds queries via its configured embedding function
- use direct query embeddings when the application owns embedding generation
- keep top-k intentional
- evaluate with real user questions

## 4. Get semantics

Use direct retrieval when you need exactness rather than similarity.

Examples:
- fetch by ids for debugging
- paginate through a collection
- inspect stored metadata
- remove or verify all chunks from one source

## 5. Filtering

Filtering is often the difference between a toy retrieval system and a usable one.

Design metadata so filters can support:
- tenant isolation
- source type restriction
- document family selection
- version targeting
- product or domain narrowing

If a future retrieval rule matters, the relevant metadata likely needs to be stored during ingestion.

## 6. Delete behavior

Deletion policy must be explicit.

Examples:
- delete all chunks from a removed source
- delete a prior version of one document before writing the replacement
- mark stale versions in metadata before full cleanup

The agent pipeline should know how to prevent stale chunks from surviving silently.

## 7. Collection replacement strategy

Some changes should trigger a fresh collection rather than incremental mutation:
- embedding model change
- retrieval contract change
- major chunking redesign
- incompatible metadata schema redesign

Recommended pattern:
1. create new collection
2. backfill from source corpus
3. validate retrieval quality
4. switch application traffic
5. retire old collection

## 8. Retrieval evaluation checklist

Require Codex to check at least:
- can the expected source be retrieved by representative queries?
- are top results overly redundant?
- do filters work as intended?
- does exact get by source id return the expected chunks?
- does re-ingestion avoid duplicate explosion?
- does collection replacement preserve correctness?
