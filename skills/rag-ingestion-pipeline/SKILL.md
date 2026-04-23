---
name: rag-ingestion-pipeline
description: Design or improve RAG, GraphRAG, and ontology-backed ingestion pipelines that turn raw corpora into retrieval-ready chunks, metadata, graph structures, and update workflows. Use when Codex needs to choose or build normalization, chunking, ontology or knowledge-graph extraction, embedding and indexing, provenance, temporal or spatial metadata, or ingestion evaluation for a RAG agent.
---

# RAG Ingestion Pipeline

Read `references/pipeline-playbook.md` first.

Read `references/paper-patterns.md` when you need corpus-derived heuristics, examples, or tradeoffs.

Read `references/evaluation-checklist.md` before finalizing design choices.

Use `assets/ingestion-design-template.md` if the user wants a concrete ingestion spec or implementation plan.

## Work In This Order

1. Inventory the corpus.
   Record source types, document structure, layout quality, language, timestamps, stable entities, and whether the corpus follows a recurring process or ontology.
2. Choose the ingestion shape before writing code.
   Use plain vector ingestion only for flat corpora.
   Add an ontology or KG control plane when the corpus has stable entities, aliases, workflows, or typed relations.
   Add timeline or process graphs when the documents describe recurring stages or event sequences.
   Add observability memory only when agent traces, tool logs, or prior runs should be queryable knowledge.
3. Normalize the corpus before chunking.
   Clean OCR noise, headers, footers, layout artifacts, aliases, acronyms, and obvious lexical variants.
   Canonicalize entities when the domain is stable, but keep original surface forms for traceability.
4. Chunk in two stages.
   Split on document-native structure first.
   Refine boundaries semantically second.
   Preserve adjacency, hierarchy, and sequence metadata so context can be reconstructed later.
5. Enrich chunks at ingestion time.
   Add ontology labels, entity links, domain tags, provenance, and any workflow or temporal anchors before embedding.
6. Keep text and structured knowledge separate but linked.
   Embed retrieval-ready text.
   Store ontology or KG data in a graph or structured store.
   Connect chunks to graph entities and relations through IDs and metadata.
7. Choose retrieval storage per corpus.
   Use ontology-aligned collections or sub-indexes when domains are modular.
   Prefer hybrid dense and sparse retrieval when vocabulary mismatch is common.
   Consider multi-vector or graph-backed retrieval only when the corpus needs it.
8. Evaluate ingestion directly.
   Measure extraction quality, chunk quality, retrieval metrics, provenance completeness, and graph quality.
   Do not judge ingestion only by final answer quality.

## Default Rules

- Prefer hierarchical semantic chunking over fixed windows.
- Preserve entity continuity across neighboring chunks.
- Build atomic knowledge units first when the corpus contains clauses, incidents, records, or evidence fragments that should remain independently retrievable.
- Keep parent, child, previous, and next links for every chunk.
- Treat provenance as mandatory metadata, not a later add-on.
- Use ontology-backed normalization for aliases, acronyms, and domain shorthand when terminology mismatch is a known failure mode.
- Do not embed the ontology itself by default; use it to guide labeling, routing, enrichment, and constraints.
- Treat time, location, and confidence as first-class fields when the domain is temporal, spatial, uncertain, or workflow-heavy.
- Soft-canonicalize open-world entities instead of forcing hard deduplication too early.

## Output Expectations

When the user asks for a pipeline design or implementation plan, produce:

1. A source inventory.
2. A chunking and normalization strategy.
3. A metadata schema.
4. An ontology or KG plan if needed.
5. An embedding and index strategy.
6. An update and reindex policy.
7. An evaluation plan with concrete metrics.

If implementation is requested, turn that design into code and preserve the same structure.
