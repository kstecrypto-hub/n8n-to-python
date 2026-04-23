# Pipeline Playbook

## 1. Choose The Pipeline Shape

Use this decision rule before implementation:

- Choose `vector-only` when the corpus is mostly flat prose and answers depend on local passages.
- Choose `vector + ontology labels` when the corpus has stable vocabulary, aliases, acronyms, taxonomies, or domain modules.
- Choose `vector + lightweight KG` when retrieval must preserve relations across chunks or support multi-hop evidence assembly.
- Choose `vector + workflow or timeline graph` when documents describe repeated stages, events, cases, claims, or regulated procedures.
- Choose `dual memory` when the system must retrieve both domain facts and prior agent traces, tool executions, or validated runs.

Do not default to a heavy GraphRAG build unless the corpus actually needs graph structure.

## 2. Run The Ingestion Pipeline In These Stages

### Stage A: Inventory And Profiling

Collect:

- source types
- layout and OCR quality
- languages
- recurring document structures
- timestamps and temporal ranges
- entity types
- relation types
- known aliases, acronyms, and shorthand
- security, tenancy, and provenance constraints

Decide whether the corpus has:

- modular domains
- typed workflows
- open-world entities
- temporal or spatial facts
- uncertainty that should survive ingestion

### Stage B: Normalization

Normalize before chunking:

- strip headers, footers, page numbers, and OCR garbage
- preserve section structure and page spans
- canonicalize obvious formatting variants
- extract or expand aliases and acronyms
- retain original surface forms beside normalized forms
- assign stable `source_doc_id` and `content_hash`

If terminology mismatch is a known problem, perform ingestion-time enrichment:

- inject canonical or alternate forms near the first mention inside the chunk
- tag the enrichment with its ontology or alias source
- cap enrichment so short chunks are not semantically diluted

### Stage C: Structural Parsing

Extract the document-native hierarchy first:

- book -> chapter -> section -> paragraph
- regulation -> title -> article -> clause
- report -> event -> subsection -> evidence
- ticket bundle -> case -> update -> attachment

Keep hierarchy as metadata even if the retrieval layer is flat.

### Stage D: Chunking

Use a two-stage chunker:

1. Build atomic units.
   These are the smallest coherent evidence units such as a clause, paragraph, event, table row group, incident note, or claim basis.
2. Build retrieval chunks from atomic units.
   Cluster nearby units when they belong to the same concept, entity chain, or workflow step.

Prefer chunk boundaries that preserve:

- semantic coherence
- named-entity continuity
- temporal order
- relation-bearing sentences
- section meaning

Avoid:

- pure fixed-size windows as the default
- splitting the subject and relation across chunk boundaries
- mixing unrelated sections only because they fit a token budget

Store for every chunk:

- `chunk_id`
- `source_doc_id`
- `parent_chunk_id`
- `prev_chunk_id`
- `next_chunk_id`
- `hierarchy_path`
- `page_start`
- `page_end`
- `token_count`
- `content_hash`

### Stage E: Metadata And Schema Enrichment

Attach retrieval-oriented metadata during ingestion, not after indexing.

Minimum chunk metadata:

- `title`
- `section_heading`
- `document_type`
- `language`
- `created_at`
- `effective_at`
- `entity_ids`
- `relation_ids`
- `keyword_tags`
- `domain_labels`
- `provenance_ref`
- `quality_flags`

Useful advanced metadata:

- `summary`
- `canonical_terms`
- `surface_terms`
- `workflow_stage`
- `time_start`
- `time_end`
- `time_granularity`
- `location`
- `spatial_extent`
- `confidence`
- `confidence_scope`
- `uncertainty_type`
- `tenant_id`
- `access_policy`

### Stage F: Ontology Or KG Layer

Use the ontology or KG as a control plane, not as a dump of raw text.

Recommended pattern:

- vector store holds chunk text and chunk metadata
- graph store holds entities, relations, workflow stages, aliases, and provenance nodes
- links connect chunk IDs to entity IDs, relation IDs, and workflow nodes

Use the ontology or KG to support:

- alias and acronym normalization
- chunk labeling
- collection routing
- query rewriting
- constrained prompting
- multi-hop evidence assembly
- provenance lookup

Do not embed the ontology itself by default.
Embed textual renderings or node summaries only when query-time retrieval needs them.

### Stage G: Embeddings And Indexes

Pick the retriever for the corpus, not the benchmark.

Use:

- general dense embeddings for broad semantic corpora
- domain-tuned embeddings when terminology mismatch is systematic
- hybrid dense + sparse retrieval when exact wording matters
- multi-vector retrieval when token-level or passage-level structure matters
- ANN indexes such as HNSW or FAISS for scale

When tuning embeddings:

- curate query-to-evidence pairs from real or synthetic tasks
- include hard negatives from alias collisions or related concepts
- re-embed the corpus after tuning

### Stage H: Updates

Support delta ingestion:

- ingest new documents incrementally
- soft-match entities and aliases
- update affected chunk neighborhoods
- avoid full reprocessing when only a subset changed

Version:

- ontology revisions
- embedding model revisions
- chunker revisions
- extraction model revisions

Store enough metadata to reindex selectively.

## 3. Special Patterns

### Ontology-Heavy Corpora

Use ontology-backed normalization and ontology-aligned collections.
Add canonical IDs, aliases, and controlled tags during ingestion.

### Workflow Or Timeline Corpora

Instantiate the workflow as a graph.
Map each evidence unit to process nodes and sequence edges.
Keep stage and chronological position as first-class metadata.

### Open KG Corpora

Keep both:

- structural nodes and edges
- textual surface forms and descriptions

Soft-canonicalize entities with alias clusters.
Do not force hard deduplication too early.

### Temporal, Spatial, Or Uncertain Corpora

Store more than simple triples.
Preserve:

- time
- intervals
- location
- spatial extent
- confidence
- confidence scope
- uncertainty type

Use element-level confidence if the domain needs it.

## 4. Recommended Deliverable

The final ingestion design should define:

1. input source classes
2. normalization rules
3. atomic unit definition
4. chunking policy
5. metadata schema
6. ontology or KG schema
7. embedding and index strategy
8. update policy
9. evaluation plan
