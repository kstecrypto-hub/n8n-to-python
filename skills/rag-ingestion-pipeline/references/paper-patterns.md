# Paper Patterns

This file distills the uploaded paper set into reusable ingestion guidance.

## Strong Cross-Paper Signals

### 1. Use Ontology-Backed Normalization At Ingestion Time

`1-s2.0-S002002552600112X-main.txt` shows that alias and acronym mismatches can break dense retrieval even when the right chunk already exists.

Use ingestion-time enrichment for:

- acronyms
- aliases
- controlled vocabulary
- domain shorthand

Keep both the original form and the canonical or expanded form.
Cap enrichment so short chunks do not become noisy.

### 2. Make Chunks Metadata-Rich Before Embedding

`1-s2.0-S0169023X26000261-main.txt` argues for ontology-guided semantic descriptions, domain labels, and collection routing at indexing time.

Implication:

- do not treat chunk text as the only retrievable unit
- attach domain labels, entity links, and constraints before embedding
- split heterogeneous corpora into ontology-aligned collections when domains are clearly separated

### 3. Prefer Hierarchical Semantic Chunking

`electronics-14-02878.txt` and `SC-LKM_A_Semantic_Chunking_an.txt` show the strongest support for:

- structural splitting first
- semantic refinement second
- preserving vertical, horizontal, and temporal context

`1-s2.0-S1474034625009206-main.txt` reinforces this with a two-stage design that starts from atomic knowledge units and then clusters them into coherent retrieval chunks.

### 4. Keep KG Structure Lightweight But Useful

`1-s2.0-S0925231225026049-main.txt`, `1-s2.0-S0278612526000452-main.txt`, and `1-s2.0-S0926580526000579-main.txt` all support a middle path:

- keep chunks as primary retrieval objects
- connect them to entities and relations with a graph layer
- use the graph for relation paths, traceability, and multi-hop assembly

Do not assume a massive graph is always better.

### 5. Use Workflow Or Timeline Graphs For Repetitive Case Files

`1-s2.0-S1474034625009851-main.txt` shows the value of a timeline ontology when documents follow a recurring process.

Use this pattern for:

- claims
- case files
- audits
- investigations
- regulated workflows

Map evidence units to process nodes, preserve chronology, and reuse prompt or report mappings where possible.

### 6. Treat Provenance As Retrieval Data

`1-s2.0-S0926580526000579-main.txt` and `1-s2.0-S0950705126002121-main.txt` push beyond plain source citations.

Store:

- source spans
- extraction method
- validation status
- reasoning traces
- tool outputs
- reflective or diagnostic notes

Use a separate observability memory only if those traces should be queryable for future reasoning.

### 7. Tune The Retriever Before You Retrain The LLM

`1-s2.0-S1474034625009206-main.txt`, `1-s2.0-S0278612526000452-main.txt`, and `1-s2.0-S002002552600112X-main.txt` all prefer retriever or embedding optimization over full model retraining.

Implication:

- build query-to-evidence pairs
- mine hard negatives
- fine-tune the embedder if the domain vocabulary is causing retrieval misses
- re-embed after tuning

### 8. Preserve Time, Space, And Uncertainty When The Domain Needs It

`1-s2.0-S0925231225032096-main.txt`, `1-s2.0-S0952197626007244-main.txt`, and `1-s2.0-S095741742600463X-main.txt` argue against flattening temporal, spatial, or uncertain facts into plain text.

If the corpus is temporal, spatial, or uncertain, store:

- time or interval fields
- location and spatial extent
- confidence
- confidence scope
- uncertainty type

For open-world KGs, `1-s2.0-S0925231226000184-main.txt` suggests preserving both structural and textual views of entities and relations.

## Useful Tradeoffs

### Ingestion-Time Enrichment

Helps recall and semantic alignment.
Can hurt precision if expansions are ambiguous or over-injected.

### Fine-Grained Chunking

Improves evidence precision.
Increases index size and linking complexity.

### KG-Backed Organization

Improves traceability and multi-hop retrieval.
Can become expensive or redundant if the domain is mostly flat prose.

### Rich Schemas

Improve reasoning quality in temporal, spatial, or uncertain domains.
Increase extraction and maintenance cost.

## Redundant Or Duplicate Inputs

`electronics-14-02878.txt` and `SC-LKM_A_Semantic_Chunking_an.txt` appear to describe the same SC-LKM paper.
Do not double-count them when weighing evidence.
