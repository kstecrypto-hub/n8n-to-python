# Agent Runtime Technical Walkthrough

## Scope

This document describes the current read-only agent runtime implemented in:

- [E:\n8n to python\src\bee_ingestion\agent.py](E:\n8n%20to%20python\src\bee_ingestion\agent.py)
- [E:\n8n to python\src\bee_ingestion\api.py](E:\n8n%20to%20python\src\bee_ingestion\api.py)
- [E:\n8n to python\src\bee_ingestion\repository.py](E:\n8n%20to%20python\src\bee_ingestion\repository.py)
- [E:\n8n to python\src\bee_ingestion\chroma_store.py](E:\n8n%20to%20python\src\bee_ingestion\chroma_store.py)

This is not the ingestion pipeline. It is the serving layer that sits on top of:

- accepted chunks in Postgres
- vectors in Chroma
- accepted KG assertions/entities/evidence in Postgres
- persisted agent sessions, runs, and source traces in Postgres

## High-Level Flow

The runtime path is:

1. API receives `POST /agent/query` or `POST /agent/chat`
2. `AgentService.query()` normalizes the question and claims the session lease
3. the service validates the requested document scope against the tenant
4. it classifies the question and chooses a retrieval plan
5. it embeds the normalized query
6. it searches Chroma for chunk candidates
7. it reranks candidates lexically
8. it expands context with neighboring chunks and KG rows when configured
9. it budgets the context into a bounded prompt bundle
10. it calls the answer model with strict JSON schema output
11. it filters the model response back through the retrieved ids
12. it persists the full turn trace
13. it releases the session lease

If the answer model fails, the runtime degrades to a deterministic evidence summary.

## API Surface

Main serving endpoints:

- `POST /agent/query`
- `POST /agent/chat`

Admin inspection endpoints for the runtime:

- `GET /admin/api/agent/sessions`
- `GET /admin/api/agent/sessions/{session_id}`
- `GET /admin/api/agent/runs`
- `GET /admin/api/agent/runs/{query_run_id}`
- `GET /admin/api/agent/reviews`
- `GET /admin/api/agent/metrics`
- `POST /admin/api/agent/runs/{query_run_id}/review`
- `POST /admin/api/agent/runs/{query_run_id}/replay`

## Session Model

Sessions are stored in `agent_sessions`.

Important fields:

- `session_id`
- `tenant_id`
- `title`
- `status`
- `claimed_by`
- `claimed_at`
- `lease_expires_at`

Why the lease exists:

- multiple UI/browser requests can hit the same session
- without a lease, two concurrent turns could append messages in the wrong order
- `Repository.claim_agent_session()` uses row-level locking and lease expiration to prevent that

Runtime behavior:

- if `session_id` is absent, a new session is created
- if present, the runtime loads the existing session and forces the request tenant to match the stored session tenant
- if the lease is already active for another worker, the query fails with `Session is busy`

## Message Model

Messages are stored in `agent_messages`.

Each successful or abstained turn writes:

- one user message
- one assistant message

User message metadata stores:

- normalized query
- question type
- retrieval mode

Assistant message metadata stores:

- abstention flag
- cited chunk ids
- supporting assertion ids
- review status and reason
- linked `query_run_id`

## Query Run Model

Query-level traces are stored in `agent_query_runs`.

Important fields:

- `query_run_id`
- `session_id`
- `tenant_id`
- `question`
- `normalized_query`
- `question_type`
- `retrieval_mode`
- `status`
- `answer`
- `confidence`
- `abstained`
- `abstain_reason`
- `provider`
- `model`
- `prompt_version`
- `metrics_json`
- `prompt_payload`
- `raw_response_payload`
- `final_response_payload`
- `review_status`
- `review_reason`

This is the authoritative trace row for one agent turn.

## Source Trace Model

Every run can link to multiple sources through `agent_query_sources`.

Supported source kinds:

- `chunk`
- `assertion`
- `entity`

Stored fields include:

- `source_kind`
- `source_id`
- `document_id`
- `chunk_id`
- `assertion_id`
- `entity_id`
- `rank`
- `score`
- `selected`
- `payload`

This lets the admin UI reconstruct:

- what retrieval returned
- what was selected into final context
- what graph support was included

## Query Normalization

`AgentService.query()` first applies `_normalize_query()`.

Behavior:

- trims whitespace
- replaces NUL bytes
- collapses repeated whitespace

Why:

- embeddings should be generated from stable text
- trace rows should use one canonical query representation
- question classification should not vary on formatting noise

## Question Classification

The runtime uses `_classify_question()`.

Current classes:

- `definition`
- `source_lookup`
- `procedure`
- `comparison`
- `explanation`
- `fact`

This is a heuristic classifier based on leading phrases and keyword checks.

It is used only to select retrieval behavior. It does not change the answer schema.

## Retrieval Planning

Retrieval planning is handled by `_select_retrieval_plan(question_type, top_k)`.

Returned plan fields:

- `mode`
- `search_k`
- `select_k`
- `expand_neighbors`
- `kg_search`

Current modes:

- `hybrid_kg_support`
- `hybrid_compare`
- `neighbor_expansion`

Meaning:

- `search_k`: how many Chroma matches to retrieve initially
- `select_k`: how many of those are considered primary selected matches
- `expand_neighbors`: whether to pull `prev_chunk_id` and `next_chunk_id` context
- `kg_search`: whether to supplement with KG entity lookup

## Embedding and Chunk Retrieval

The agent uses the same embedding provider stack as ingestion through:

- [E:\n8n to python\src\bee_ingestion\embedding.py](E:\n8n%20to%20python\src\bee_ingestion\embedding.py)

Process:

1. embed normalized query with `Embedder.embed([normalized_query])`
2. call `ChromaStore.search()`
3. pass `tenant_id`
4. optionally pass `document_ids`

`ChromaStore.search()` returns:

- `chunk_id`
- `document`
- `metadata`
- `distance`
- `rank`

## Reranking

Raw vector matches are reranked by `_rerank_matches()`.

Inputs used:

- distance score from Chroma
- lexical overlap between query terms and chunk text
- lexical overlap between query terms and section title

Rerank score:

- base vector score from `_distance_to_score(distance)`
- plus lexical weight
- plus section-title bonus

Result:

- reranked rows replace the original order
- `rank` is rewritten after sorting

## Context Bundle Construction

`AgentService._build_context_bundle()` converts reranked chunk matches into an `AgentContextBundle`.

Bundle fields:

- `chunks`
- `assertions`
- `evidence`
- `entities`
- `sources`

Detailed behavior:

1. mark top `select_k` chunk ids as selected
2. create source-trace rows for every retrieved chunk candidate
3. load canonical chunk rows from Postgres with `list_chunk_records_by_ids()`
4. if neighbor expansion is enabled:
   - load `prev_chunk_id`
   - load `next_chunk_id`
5. compress chunk set with `_compress_chunks()`
6. load KG assertions for the selected chunk ids
7. load KG evidence for those assertions
8. derive entity ids from the assertion endpoints
9. load entity rows
10. if KG search is enabled:
   - search KG entities by query text
   - append those entities to bundle and trace rows
11. add assertion and entity trace rows

## Chunk Compression

`_compress_chunks()` reduces retrieved chunk rows before prompt assembly.

Rules:

- skip empty text
- remove near-duplicates by text signature
- skip very short text if it has no lexical overlap with the query
- stop at `settings.agent_max_context_chunks`

Goal:

- keep prompt context bounded
- keep citations meaningful
- reduce repeated evidence

## KG Support

KG support is not a primary retrieval engine in v1.

Current role:

- chunk retrieval remains primary
- assertions and entities are supplemental context
- answer prompt explicitly tells the model to trust chunk text over KG if they conflict

Current KG support inputs:

- `list_kg_assertions_for_chunks()`
- `list_kg_evidence_for_assertions()`
- `list_kg_entities_by_ids()`
- optionally `search_kg_entities_for_query()`

## Prompt Bundle

Prompt assembly is handled by `AgentService._build_prompt_bundle()`.

It produces:

- `prior_context`
- `chunk_payload`
- `assertion_payload`
- `entity_payload`
- `evidence_payload`
- `stats`

Each payload category has its own budget helper:

- `_budget_prior_messages()`
- `_budget_chunk_payload()`
- `_budget_assertion_payload()`
- `_budget_entity_payload()`
- `_budget_evidence_payload()`

Then `_fit_prompt_bundle()` trims categories until the estimated prompt size fits the configured budget.

The final `stats` object records:

- configured budgets
- counts kept
- counts trimmed
- estimated chars per category
- estimated total prompt size

## Prompt Construction

`_build_user_prompt()` serializes the bounded context into one structured user message.

It includes:

- `question_type`
- `question`
- `normalized_query`
- `allowed_chunk_ids`
- `allowed_assertion_ids`
- `allowed_entity_ids`
- serialized context chunks
- serialized context assertions
- serialized context entities
- serialized context evidence
- context budget stats

The `allowed_*` fields are there so the model can only cite ids already in the selected bundle.

## Answer Generation

`AgentService._generate_answer()`:

1. resolves provider
2. builds system prompt
3. builds user prompt
4. builds JSON-schema response format
5. estimates serialized request size
6. shrinks prompt bundle until request fits
7. sends `POST {base_url}/chat/completions`
8. extracts message content
9. returns:
   - provider
   - model
   - prompt version
   - content
   - prompt payload
   - raw payload

The answer model is read-only. It never writes to the database directly.

## Response Schema

The model must return:

- `answer`
- `confidence`
- `abstained`
- `abstain_reason`
- `citations`
- `supporting_assertions`
- `supporting_entities`

Each citation must include:

- `chunk_id`
- `quote`

## Response Finalization

`_finalize_response()` filters model output against the actual selected bundle.

What it enforces:

- citations must refer to selected chunk ids
- supporting assertion ids must exist in selected assertions
- supporting entity ids must exist in selected entities
- no valid citations => abstain
- confidence below threshold => abstain
- abstained with empty answer => inject standard abstention message

This is the main anti-hallucination boundary after the model call.

## Fallback Path

If `_generate_answer()` raises `AgentQueryError`, the runtime uses `_build_fallback_response()`.

Behavior:

- take up to 3 selected chunks
- create short excerpts
- build direct evidence-summary answer
- mark:
  - `confidence = 0`
  - `abstained = true`
  - `abstain_reason = agent_generation_error`

This preserves retrieval evidence even when the answer model fails.

## Review State

The runtime uses `_derive_agent_review_state()` after finalization.

Current rules:

- if abstained: `needs_review`
- if confidence below review threshold: `needs_review`
- if no supporting assertions but otherwise answerable: `unreviewed` with `chunk_only_answer`
- otherwise: `unreviewed`

This is an operator-facing signal, not a hard answer blocker.

## Persistence Semantics

`Repository.persist_agent_turn()` writes all turn components in one transaction:

1. user message
2. `agent_query_runs` row
3. `agent_query_sources` rows
4. assistant message
5. session `updated_at`

This gives a coherent audit trail for:

- replay
- manual review
- citation inspection
- agent debugging

## Replay

`POST /admin/api/agent/runs/{query_run_id}/replay`:

1. loads the prior run detail
2. derives document scope from source rows
3. optionally reuses the original session
4. re-executes `agent_service.chat(...)`

Replay is not a raw prompt replay. It re-runs retrieval and generation against current corpus state.

## Metrics

Per-run metrics are stored in `metrics_json`.

Important fields:

- `retrieved_chunks`
- `selected_chunks`
- `selected_assertions`
- `selected_entities`
- `selected_citations`
- `latency_ms`
- `fallback_used`
- `prompt_context`

Admin aggregate metrics come from `Repository.get_agent_metrics()`.

## Failure Modes

Known major runtime failure classes:

- no retrieval results
- busy session lease
- answer model HTTP failure
- answer model invalid JSON
- no valid citations after post-filtering
- document scope outside tenant boundary

These all result in:

- explicit error responses, or
- explicit abstention/review states

not silent degradation.

## Current Architectural Limits

The current runtime is intentionally minimal.

Not implemented yet:

- reranker model
- session summarization / memory compression
- graph-first retrieval
- tool-using action agent
- websocket push updates
- tenant auth / end-user auth
- OCR-aware answer routing

## Most Important Runtime Invariants

These are the invariants the runtime tries to maintain:

1. one session turn at a time through leases
2. retrieval scoped to tenant and optional document ids
3. answer model only sees a bounded prompt bundle
4. model citations are filtered back through retrieved ids
5. every turn is durably traceable in Postgres
6. fallback path remains evidence-grounded

## What To Inspect In Admin UI

For runtime debugging, the useful UI surfaces are:

- `Agent -> Sessions`
- `Agent -> Runs`
- run detail:
  - citations
  - selected sources
  - prompt payload
  - raw model response
  - final filtered response
  - review records

That path is the fastest way to inspect:

- bad retrieval
- citation stripping
- low-confidence abstentions
- model-format failures
- replay differences
