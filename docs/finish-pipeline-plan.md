# Finish Pipeline Plan

This is the current plan to finish the ingestion pipeline with minimal-data validation until the system is ready for heavier QA.

## What is already in place

- PDF ingest through the API and CLI
- raw text + normalized text storage
- block parsing, chunking, validation, embeddings, and Chroma indexing
- manual review and LLM auto-review for review chunks
- KG extraction with ontology validation and canonicalization
- document rebuild, revalidate, reindex, KG replay, delete, and reset controls
- admin inspection surfaces for documents, chunks, KG, metadata, and vectors

## What is left to finish the pipeline

## 1. Stabilize the sample-corpus path

- keep validation on small page ranges only
- remove remaining false-review and false-reject cases
- confirm revalidate/reindex/rebuild/delete/reset all work cleanly on real sample slices

Exit criteria:
- accepted chunks are coherent
- review chunks are genuinely ambiguous
- reset and replay operations leave Postgres and Chroma in sync

## 2. Make retrieval evaluation first-class

- keep the retrieval-eval API and CLI as the quality gate before scaling
- run the fixed bee query set against the sample corpus
- inspect whether top-k results match the chunk and KG state

Exit criteria:
- in-corpus queries retrieve useful chunks
- parity stays clean
- failures are mostly corpus-coverage failures, not pipeline failures

## 3. Tighten KG quality on the sample corpus

- inspect raw extractions and accepted assertions
- tighten prompt + validation around weak entity typing and weak relations
- keep only validated assertions durable

Exit criteria:
- no structurally invalid assertions
- canonical entity keys remain stable
- evidence is traceable to source chunks

## 4. Scale from small slices to medium slices

- move from 5-page samples to larger document slices
- use the same replay and evaluation controls
- only increase slice size if parity and KG quality stay stable

Exit criteria:
- medium slices ingest cleanly
- revalidate/reindex/reprocess still work without manual SQL

## 5. Full-corpus ingest

- clear project data
- ingest the chosen real corpus
- use manual/LLM review only where needed
- monitor parity, retrieval, and KG output throughout the ingest

Exit criteria:
- full corpus indexed
- no accepted chunk/vector parity drift
- KG remains stable across the corpus

## 6. Only then switch to hotfix mode

- once the pipeline is complete on the real corpus
- focus on targeted bug fixes and QA findings instead of structural changes

## Immediate implementation order

1. finish stabilizing the sample slices
2. run retrieval evaluation through the API on the sample corpus
3. tighten KG quality based on those sample results
4. scale to medium slices
5. expose the remaining pipeline controls more fully in the admin UI
