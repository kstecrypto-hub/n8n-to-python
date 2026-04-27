# Pre-KG Implementation Backlog

## Objective

Reach the highest practical ingestion quality before introducing LLM-based KG extraction.

Pre-KG readiness means:

- extracted text is reliable enough to read and embed
- accepted chunks are coherent, useful, and low-noise
- rejected and review flows are stable and explainable
- retrieval quality is measured and good enough on real queries
- the pipeline is safe to run concurrently for many documents and users
- operational controls exist for replay, failure handling, and inspection

This backlog assumes:

- Postgres is the system of record
- Chroma is a derived retrieval index
- embedding model is `text-embedding-3-large`
- current tenancy mode is shared, but the design must remain multi-tenant safe

## Delivery Strategy

The work is split into eight execution tracks:

1. pipeline state and idempotency
2. extraction and normalization quality
3. structural parsing and chunk assembly
4. validation and review workflow
5. retrieval quality evaluation
6. scale, concurrency, and replay
7. observability and operator controls
8. pre-KG readiness gate

The order matters. Retrieval and later KG quality depend on clean upstream stages.

## Track 1: Pipeline State And Idempotency

### Goal

Make ingestion deterministic, replayable, and safe under concurrent load.

### Tasks

1. Formalize stage transitions.
   - Enforce allowed transitions in code.
   - Prevent illegal jumps such as `registered -> indexed`.
   - Store transition timestamps and actor/version data.

2. Introduce stage version fields consistently.
   - `extractor_version`
   - `normalizer_version`
   - `parser_version`
   - `chunker_version`
   - `validator_version`
   - `embedding_version`

3. Make document ingest idempotent by content hash and source identity.
   - Re-ingesting the same file should not create uncontrolled duplicates.
   - Add explicit handling for:
     - exact duplicate
     - same document, new content
     - same content, newer pipeline version

4. Add document-level ingest locks.
   - Ensure one active write pipeline per document at a time.
   - Use Postgres row-level locking or a lease field on jobs.

5. Add stage-level retries.
   - Retry transient infrastructure failures only.
   - Do not retry deterministic content failures.

### Acceptance Criteria

- same file can be submitted twice without corrupting state
- concurrent processing of the same document is prevented
- every stage has explicit version metadata
- failed jobs can be resumed or replayed intentionally

## Track 2: Extraction And Normalization Quality

### Goal

Maximize the quality of the text before parsing or chunking.

### Tasks

1. Split extraction output from normalized output in storage.
   - Keep raw extracted text for audit/debug.
   - Store normalized text separately.

2. Add extraction metrics.
   - pages seen
   - pages with no text
   - characters per page
   - suspicious compression ratio
   - repeated header/footer candidates
   - proportion of non-word tokens

3. Improve normalization rules.
   - fix line-wrap hyphenation
   - normalize whitespace
   - normalize punctuation spacing
   - preserve intentional paragraph breaks
   - strip repeated running headers/footers when detected

4. Add page-level quality classification.
   - `body`
   - `front_matter`
   - `contents`
   - `appendix`
   - `back_matter`
   - `suspect`

5. Add extraction fallback policy.
   - current path: embedded-text extraction
   - future path: OCR only when extraction yields poor quality or empty text
   - record which path was used

6. Add quarantine rules for very poor extraction.
   - too many empty pages
   - severe word compression
   - mostly non-body material

### Acceptance Criteria

- normalized text is materially cleaner than raw extracted text
- repeated page noise is removed reliably
- page-level classification is inspectable
- low-quality extraction can be quarantined before chunking

## Track 3: Structural Parsing And Chunk Assembly

### Goal

Produce coherent chunks from structured document blocks, not raw text streams.

### Tasks

1. Harden block parsing.
   - classify blocks as:
     - heading
     - paragraph
     - list
     - table_like
     - front_matter
     - contents
     - appendix
     - back_matter
   - preserve block order and page provenance

2. Improve section detection.
   - separate section heading from body text
   - build stable `section_path`
   - keep `section_title` human-readable

3. Track block provenance in chunk metadata.
   - block ids included
   - page range
   - offsets
   - block types included

4. Rework chunk assembly rules.
   - target coherent thought units
   - preserve paragraph integrity
   - avoid cutting in the middle of a topic transition if possible
   - avoid heading-only chunks unless deliberately review-only
   - avoid body + non-body mixed chunks where possible

5. Add chunk shape metrics.
   - words per chunk
   - paragraphs per chunk
   - heading-to-body ratio
   - page span
   - block count

6. Add deterministic chunk identity rules.
   - chunk id should depend on document, chunk index, and content hash
   - replay with identical input should produce identical chunk ids

### Acceptance Criteria

- accepted chunks can be read independently without confusion
- section metadata is usually correct
- chunks preserve adjacency and provenance
- repeated runs on the same input produce stable chunk ids

## Track 4: Validation And Review Workflow

### Goal

Ensure only worthwhile chunks enter Chroma automatically, with stable semantics for `accepted`, `review`, and `rejected`.

### Tasks

1. Refine validation rules from actual failure classes.
   - too short
   - heading only
   - repeated boilerplate
   - low-information fragment
   - front matter
   - contents
   - back matter
   - malformed mixed chunk

2. Separate hard rejection from operator review clearly.
   - `rejected` = not worth indexing
   - `review` = coherent but needs decision

3. Add review-decision audit trail.
   - chunk id
   - old status
   - new status
   - timestamp
   - actor
   - reason

4. Add review queue prioritization.
   - sort by highest retrieval value candidates first
   - examples:
     - body-like chunks with uncertain role
     - short but semantically dense chunks

5. Add duplicate suppression.
   - exact duplicate
   - near-duplicate within same document
   - repeated per-page noise

6. Make validation model/version explicit.
   - changing validation rules should support replay of affected documents

### Acceptance Criteria

- review and rejection behavior matches operator expectations
- manual accept/reject is auditable
- duplicate low-value chunks do not reach Chroma
- validator version changes are traceable

## Track 5: Retrieval Quality Evaluation

### Goal

Measure retrieval quality before KG extraction, not after.

### Tasks

1. Build a fixed evaluation query set.
   - include:
     - bee biology
     - queen behavior
     - disease/treatment
     - hive equipment
     - practical beekeeping procedures
     - historical/contextual questions

2. Define retrieval evaluation criteria.
   - top-k relevance
   - readability
   - duplicate rate
   - noise leakage
   - context completeness
   - section appropriateness

3. Add a retrieval evaluation script.
   - query Chroma
   - record top-k results
   - output a human-reviewable report

4. Add a baseline comparison workflow.
   - compare current parser/chunker version to prior version
   - report regressions and improvements

5. Add document-class-aware evaluation.
   - books
   - manuals
   - articles
   - research papers
   - notes
   - practical experience

### Acceptance Criteria

- retrieval quality is measured, not guessed
- parser/chunker changes can be evaluated against the same query set
- noise and duplication trends are visible release to release

## Track 6: Scale, Concurrency, And Replay

### Goal

Ensure the ingestion pipeline continues to work under many users and large document volumes.

### Tasks

1. Prepare for multi-tenant safety.
   - keep `tenant_id` on all relevant records
   - ensure all writes and deletes are document/tenant scoped
   - avoid collection-wide destructive operations

2. Define collection strategy for scale.
   - current practical path:
     - shared corpus collection by embedding version
   - future safe path:
     - per-tenant collection or filtered shared collection
   - document the threshold for splitting collections

3. Add batch embedding controls.
   - cap batch size
   - track latency and failures per batch
   - support partial retry on failed batches

4. Add chunk replay strategy.
   - replay affected chunks only when:
     - normalization changed
     - parser/chunker changed
     - validator changed
   - full re-embed only when text or embedding model changes

5. Add delete/update semantics.
   - deleting a document removes:
     - chunk rows
     - validation rows
     - Chroma records
     - derived KG data later
   - updating a document supersedes old chunk/index state deterministically

6. Add backpressure controls.
   - limit simultaneous embedding/index jobs
   - keep long documents from starving short ones

7. Add stress scenarios.
   - large batch ingest
   - repeated same-document ingest
   - restart during indexing
   - manual review during active ingest

### Acceptance Criteria

- no cross-document or cross-tenant corruption under concurrent load
- replay can be done safely and intentionally
- large batches do not destabilize the system
- deletes and updates keep Postgres and Chroma consistent

## Track 7: Observability And Operator Controls

### Goal

Make the pipeline diagnosable without ad hoc SQL or container spelunking.

### Tasks

1. Extend the admin UI.
   - extraction metrics
   - stage timing and failures
   - metadata inspection
   - Chroma record inspection
   - document replay controls later

2. Add structured logging per stage.
   - job id
   - document id
   - stage
   - attempt
   - duration
   - outcome

3. Add operator reports.
   - accepted/review/rejected trend
   - top rejection reasons
   - most common review reasons
   - per-document chunk counts
   - retrieval evaluation report links

4. Add data consistency checks.
   - chunks marked accepted but missing in Chroma
   - Chroma records missing Postgres chunk rows
   - stale jobs
   - duplicated active jobs

5. Add safe repair scripts.
   - reindex one document
   - remove one document from Chroma
   - rebuild chunk metadata view
   - verify Chroma/Postgres consistency

### Acceptance Criteria

- operators can understand ingest failures from the UI and logs
- basic consistency problems are detectable without manual investigation
- document-level repair is possible without full environment resets

## Track 8: Pre-KG Readiness Gate

### Goal

Set explicit criteria for when the system is ready for KG candidate extraction.

### Readiness Checklist

1. Text quality
   - accepted chunks are readable to a human
   - paragraph boundaries are mostly correct
   - repeated header/footer noise is low

2. Structural quality
   - section titles are mostly correct
   - non-body material is rarely accepted
   - chunk adjacency is reliable

3. Validation quality
   - review and rejected buckets make operational sense
   - review queue volume is manageable

4. Retrieval quality
   - top-k results are generally relevant on the fixed query set
   - duplicate and noise rate are acceptable

5. Operational quality
   - replays are deterministic
   - concurrent ingests do not corrupt state
   - admin UI exposes enough context to diagnose problems

Only after these criteria are met should LLM-based KG extraction begin.

## Implementation Sequence

### Phase A: Stability First

1. formalize stage transitions and idempotency
2. add version fields and replay rules
3. add ingest locking

### Phase B: Better Text

1. split raw vs normalized text
2. add extraction metrics
3. improve normalization
4. add page-level classification

### Phase C: Better Structure

1. improve block typing
2. improve section detection
3. improve chunk assembly
4. stabilize chunk ids

### Phase D: Better Decisions

1. tune validator
2. add audit trail for review actions
3. add duplicate suppression
4. refine review queue

### Phase E: Better Measurement

1. create retrieval evaluation query set
2. implement evaluation script/report
3. compare parser/chunker versions

### Phase F: Scale And Operations

1. harden multi-user safety
2. add batch/replay/delete semantics
3. add consistency checks
4. add stress testing

### Phase G: Pre-KG Signoff

1. run readiness checklist
2. fix remaining blockers
3. freeze pre-KG baseline

## Suggested Immediate Backlog

These are the next implementation items I would execute in code, in order:

1. Add versioned stage transitions and job locking.
2. Split raw extracted text from normalized text in storage.
3. Add extraction metrics and page-level classification.
4. Improve block typing and section-path reliability.
5. Rework chunk assembly around semantic block grouping.
6. Add validator audit trail and duplicate suppression.
7. Build retrieval evaluation harness and fixed bee-query set.
8. Add consistency checks between Postgres and Chroma.
9. Add replay semantics for validator/chunker changes.
10. Run concurrency and batch-ingest stress tests.

## Done Definition

The pre-KG phase is done when:

- the system reliably produces high-quality accepted chunks
- the vector index is clean enough to support good retrieval
- the ingestion process is deterministic, observable, and replayable
- operator review is practical
- scaling to more users and more documents does not require redesign

