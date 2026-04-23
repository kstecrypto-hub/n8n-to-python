# Ingestion Design Template

## Goal

- RAG use case:
- Target users:
- Failure modes to prevent:

## Corpus Inventory

- Source types:
- File formats:
- Languages:
- Layout or OCR issues:
- Stable entities:
- Stable relations:
- Temporal or spatial signals:
- Known aliases, acronyms, or shorthand:

## Pipeline Shape

- Chosen architecture:
- Why this shape fits the corpus:
- Why heavier alternatives were rejected:

## Normalization

- Text cleanup:
- Layout handling:
- Alias or acronym handling:
- Canonicalization strategy:
- Provenance strategy:

## Atomic Units And Chunking

- Atomic unit definition:
- Structural split rules:
- Semantic refinement rules:
- Chunk size target:
- Overlap policy:
- Adjacency and parent-child links:

## Metadata Schema

- Required chunk fields:
- Optional advanced fields:
- Access control fields:
- Quality flags:

## Ontology Or KG Plan

- Ontology or KG needed:
- Entity types:
- Relation types:
- Chunk-to-graph linking strategy:
- Workflow or timeline nodes:

## Embeddings And Indexes

- Embedding model:
- Tuning plan:
- Dense, sparse, or hybrid retrieval:
- Index type:
- Collections or namespaces:

## Update Policy

- Incremental ingestion:
- Entity consolidation:
- Re-embedding trigger:
- Reindex trigger:
- Versioning fields:

## Evaluation

- Retrieval metrics:
- Domain-specific test queries:
- Provenance checks:
- Regression suite:

## Implementation Notes

- Services or libraries:
- Storage layout:
- Operational risks:
- Open questions:
