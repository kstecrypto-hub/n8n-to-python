# Agent Next-Pass Review

Date: 2026-03-28

This review was prepared while the live corpus re-ingest was running. It is based on the current code in:

- [E:\n8n to python\src\bee_ingestion\agent.py](E:\n8n%20to%20python\src\bee_ingestion\agent.py)
- [E:\n8n to python\src\bee_ingestion\query_router.py](E:\n8n%20to%20python\src\bee_ingestion\query_router.py)
- [E:\n8n to python\src\bee_ingestion\retrieval_eval.py](E:\n8n%20to%20python\src\bee_ingestion\retrieval_eval.py)

It does not change the running stack.

## Highest-priority next-pass items

### 1. Claim-level grounding is still not strong enough

The current verifier is better than plain citation-id checks, but it is still not a true claim decomposition and support check.

Practical consequence:

- a synthesized answer can still pass with one partially related citation
- paraphrased weak support is hard to separate from strong support

Recommended next step:

- split the answer into short claim units
- verify each claim against the selected evidence rows
- fail the whole answer if unsupported claims exceed a threshold

### 2. Session memory exists, but conflict resolution is still shallow

The session memory/profile path now exists, but the runtime still needs clearer precedence rules when these sources disagree:

- user profile preference
- session summary
- recent turns
- current request scope

Recommended next step:

- define a strict precedence order in code and expose it in admin inspection
- treat document scope and safety constraints as hard overrides

### 3. Router caching is useful but should become measurable

The router cache is now operational, but it should expose:

- hit rate
- stale-hit rate after runtime changes
- disagreement rate between cached route and fresh route on sampled queries

Recommended next step:

- add drift sampling and cache-hit telemetry
- expose these metrics in the admin UI

### 4. Retrieval still collapses evidence aggressively

The retrieval path is materially better, but it still tends to narrow to a small document set early for many non-comparison queries.

Practical consequence:

- complementary evidence can be dropped too early
- broad corpus questions may inherit the bias of the first strong-scoring document

Recommended next step:

- make collapse thresholds query-type-specific
- keep at least one secondary document alive longer for explanatory/procedural questions

### 5. Multimodal provenance is improved but not final

Asset links are now evidence-based, but the next improvement is still figure/span/layout-aware linking.

Recommended next step:

- capture figure labels and caption spans explicitly
- link chunks to assets through caption references or bounding-box overlap, not only term overlap

### 6. Evaluation is still mostly heuristic

The current retrieval and agent evaluation files are good smoke checks, not a release gate.

Recommended next step:

- run the stronger judged set in [E:\n8n to python\data\evaluation\agent_gold_queries_draft.json](E:\n8n%20to%20python\data\evaluation\agent_gold_queries_draft.json)
- score:
  - router choice
  - citation precision
  - grounding failures
  - abstention correctness
  - multimodal evidence correctness

## Operational findings from this review

### Batch ingest runner

The detached re-ingest runner needed two corrections before it was safe:

- PDF identity had to use file-byte hashing, not raw extracted text
- partial-corpus failures must produce a non-success summary/exit

These corrections were applied before the current live ingest continued.

### Reset/rebuild risk that remains

Cross-store reset remains operationally non-atomic:

- vector reset happens separately from Postgres truncation
- a failure between them can leave the system half-reset

Recommended next step:

- move reset/rebuild orchestration behind a single safer administrative workflow
- record a reset transaction/run record before mutating either store

## Recommended order after the current ingest finishes

1. evaluate the fresh corpus with the stronger judged set
2. inspect router behavior and document-collapse behavior on the failures
3. implement claim-level support verification
4. tighten multimodal provenance to caption/span-aware linking
5. then revisit ontology adoption from the draft overlay
