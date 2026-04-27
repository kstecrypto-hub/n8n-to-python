# Post-Ingest Validation Runbook

Date: 2026-03-28

Use this immediately after the active corpus ingest finishes.

## 1. Confirm the runner reached a terminal status

Check:

- [E:\n8n to python\data\logs\reingest-progress.json](/E:/n8n%20to%20python/data/logs/reingest-progress.json)
- [E:\n8n to python\data\logs\reingest-20260328.log](/E:/n8n%20to%20python/data/logs/reingest-20260328.log)

Required:

- `status = "completed"`
- `error_files = 0`
- `completed_files = total_files`

If any file failed, stop and inspect the per-file error before any KG replay or eval run.

## 2. Snapshot overview counts

Check admin/API:

- [http://localhost:38100/admin](http://localhost:38100/admin)
- `GET /admin/api/overview`

Capture:

- `documents`
- `jobs`
- `pages`
- `page_assets`
- `chunks`
- `accepted_chunks`
- `review_chunks`
- `rejected_chunks`
- `kg_raw_extractions`
- `kg_assertions`
- `kg_entities`

Immediate failure signs:

- `documents < 13`
- `accepted_chunks = 0`
- `kg_raw_extractions = 0`
- `page_assets = 0` for a clearly image-heavy corpus

## 3. Check Chroma parity

Check:

- `GET /admin/api/chroma/collections`
- `GET /admin/api/chroma/parity`

Required:

- chunk vector count matches accepted chunk count
- asset vector count is plausible against `page_assets`
- `missing_vectors_total = 0`
- `extra_vectors_total = 0`

## 4. Inspect document distribution

Check:

- `GET /admin/api/documents?limit=100&offset=0`

For each document, inspect:

- filename
- chunk count
- accepted/review/rejected distribution
- page count
- asset count

Failure signs:

- a non-trivial PDF with `0` chunks
- an image-heavy PDF with `0` assets
- an obviously textual PDF with almost everything in `review`

## 5. Inspect multimodal linkage quality

Check in admin UI:

- `Documents`
- `Chunks`
- `Pages`
- `Assets`
- `Chunk Asset Links`

Spot-check at least:

- one text-heavy book
- one image-heavy/scanned book
- one practical/manual book

Required:

- linked assets actually belong to the cited page/chunk
- asset descriptions/OCR are not empty for visually important pages
- there is no page-wide irrelevant asset spray on normal chunks

## 6. Inspect KG status distribution

Check:

- `GET /admin/api/kg/raw`
- `GET /admin/api/kg/assertions`
- `GET /admin/api/kg/entities`

Capture:

- validated
- skipped
- review
- quarantined

Failure signs:

- very high `review` on otherwise clean accepted chunks
- many predicates outside practical beekeeping semantics
- large numbers of duplicate near-identical entities

## 7. Run judged evaluation

Run after ingest is complete:

- retrieval smoke set
- agent judged set

Files prepared:

- [E:\n8n to python\data\evaluation\retrieval_small_queries.json](/E:/n8n%20to%20python/data/evaluation/retrieval_small_queries.json)
- [E:\n8n to python\data\evaluation\agent_small_queries.json](/E:/n8n%20to%20python/data/evaluation/agent_small_queries.json)
- [E:\n8n to python\data\evaluation\agent_gold_queries_draft.json](/E:/n8n%20to%20python/data/evaluation/agent_gold_queries_draft.json)

Judge:

- router choice
- citation kind
- citation precision
- grounding quality
- abstention correctness
- multimodal evidence quality

## 8. Inspect session memory/profile behavior

After at least 2 to 3 test conversations:

Check:

- `agent_session_memories`
- `agent_profiles`
- Admin `Agent` view

Required:

- session memory preserves:
  - constraints
  - evidence-backed facts
  - open threads
  - resolved threads
  - topic keywords
- profile preserves:
  - communication style
  - answer preferences
  - recurring topics
  - preferred document scope

Failure signs:

- profiles absorb transient one-off questions as long-term preferences
- memory drops document scope or explicit user constraints too quickly
- summaries contain unsupported facts with no anchors

## 9. Decide the next action from evidence

Use this order:

1. fix hard ingest failures
2. fix vector parity drift
3. fix multimodal linkage errors
4. fix KG review/quarantine spikes
5. run retrieval/agent tuning
6. only then widen the corpus or rerun with further ontology changes
