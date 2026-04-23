# Bee Knowledge Agent Design

## Use Case

- Workflow: answer beekeeping questions against the ingested corpus using retrieval from Chroma plus supporting knowledge-graph facts from Postgres.
- Users: you first; later other users who should be able to query the same shared corpus.
- Business goal: turn the ingestion pipeline into a usable question-answering agent with grounded answers, citations, and inspectable evidence.
- Why an agent is justified:
  - the ingestion pipeline already exists and works on real documents
  - the manual process is now repeatable: retrieve chunks, inspect supporting graph facts, answer with citations
  - the corpus is structured enough to support a bounded retrieval agent
  - the operational risk is manageable because v1 can remain read-only

## Opportunity Score

| Category | Score (1-5) | Reason |
|---|---:|---|
- `time_investment` | `4` | Querying and cross-checking long books manually is slow and repetitive.
- `strategic_value` | `4` | A usable grounded agent is the actual product outcome of the pipeline.
- `error_reduction` | `4` | Grounded retrieval plus KG support reduces unsupported answers compared with free-form chat.
- `scalability` | `5` | The same agent pattern scales across more documents and more users.
- `process_standardization` | `4` | The retrieval and answer flow is already defined enough for v1.
- `data_readiness` | `4` | The corpus is ingested, chunked, indexed, and linked to KG evidence.
- `tool_access` | `5` | The required stores and APIs are already in this workspace.
- `integration_difficulty` | `3` | Moderate: the data plane exists, but the serving layer still needs to be built.
- `decision_risk` | `3` | Wrong answers matter, but a read-only, citation-required agent keeps risk contained.
- `privacy_risk` | `2` | Current corpus is local/shared and not obviously high-sensitivity.
- `rollback_strength` | `5` | The serving layer can be isolated from ingestion and rolled back cleanly.
- `human_override` | `5` | Users can inspect chunks, KG, and citations directly in the admin UI.

## Recommendation

- Final recommendation: `build now`
- Why:
  - high impact
  - solid feasibility
  - low-to-moderate operational risk for a read-only first version
  - existing pipeline already provides the necessary retrieval substrates

## Agent Boundary

- Agent name: `Bee Knowledge Agent`
- Mission: answer corpus-grounded questions using retrieved chunks and validated KG evidence, while exposing citations and refusing unsupported claims.

### In scope

- question answering over the ingested corpus
- retrieval from Chroma
- KG lookup from Postgres
- evidence assembly
- cited answer generation
- answer trace logging
- session-level conversational continuity

### Out of scope

- changing ingestion policy from the chat path
- mutating KG or vectors from user chat
- autonomous file ingestion
- autonomous pipeline resets, deletes, rebuilds, or replay operations
- open-web research
- fully autonomous multi-agent workflows

### Human responsibilities

- upload and ingest documents
- approve or reject review chunks
- run admin operations
- inspect weak answers and traces
- decide when new tools or data sources are added

### Escalation triggers

- not enough evidence retrieved
- retrieved evidence is contradictory
- no citation-worthy chunk passes threshold
- KG facts conflict with chunk evidence
- session asks for an operation that mutates the corpus
- query falls outside the domain of the ingested corpus

## SPAR Design

### Sensing

- Inputs:
  - user question
  - optional session id
  - optional prior messages in that session
- Source systems:
  - Chroma for dense retrieval
  - Postgres for chunks, metadata, KG entities/assertions/evidence, session state
- Trigger conditions:
  - `POST /agent/query`
  - optional `POST /agent/session/{id}/message`

### Planning

- Decision points:
  - classify question type: `fact`, `procedure`, `comparison`, `definition`, `source_lookup`, `insufficient_domain`
  - choose retrieval mode: `chunks-only`, `chunks+kg`, `neighbor-expansion`, `section-focused`
  - decide whether answer confidence is high enough to respond or whether to abstain
- Routing logic:
  - if query is narrow factual and ontology-aligned, run `chunks+kg`
  - if query is procedural/explanatory, prioritize chunks and only use KG as support
  - if retrieval confidence is low, broaden retrieval once; do not recurse indefinitely
- Constraints:
  - v1 should remain a single agent, not a manager-plus-specialists topology
  - no tool calls that mutate ingestion state
  - no answer without evidence references

### Acting

- Tools:
  - `search_chunks(query, k, filters)`
  - `get_chunk_neighbors(chunk_ids)`
  - `get_chunk_details(chunk_ids)`
  - `search_kg(query, chunk_ids|document_ids)`
  - `get_entity_details(entity_ids)`
  - `get_assertion_evidence(assertion_ids)`
  - `build_context_bundle(...)`
  - `generate_grounded_answer(...)`
- Action sequence:
  1. normalize query
  2. classify question type
  3. retrieve top chunks from Chroma
  4. expand to neighbors where chunk continuity is needed
  5. fetch aligned KG facts/evidence for the retrieved scope
  6. build a bounded context bundle
  7. generate answer with citations only from the bundle
  8. run self-check for unsupported claims
  9. persist query trace and response
- Output contract:
  - `answer`
  - `confidence`
  - `citations`
  - `supporting_entities`
  - `supporting_assertions`
  - `trace_id`
  - `abstained`
  - `abstain_reason`
- Fallbacks:
  - if KG lookup is weak, answer from chunks only
  - if chunk retrieval is weak, abstain instead of guessing
  - if answer model fails, return evidence summary instead of fabricated prose

### Reflecting

- Verification checks:
  - every material claim in the answer must map to at least one citation
  - citations must correspond to retrieved chunks, not arbitrary document ids
  - if KG facts are cited, they must map back to assertion evidence chunks
  - if confidence is below threshold, return abstention or partial answer
- Feedback collection:
  - thumbs up/down later
  - operator review in admin console
  - trace inspection for failed answers
- Adaptation loop:
  - failed traces become retrieval/KG tuning cases
  - no automatic self-modification of prompts or thresholds in v1

## Topology

- Pattern: `single agent with bounded retrieval tools`
- Why:
  - the current problem is not routing between unrelated tasks
  - ingestion/admin operations already exist separately
  - the first version should maximize reliability and inspectability

## Tool Matrix

| Tool | Purpose | Control | Impact | Fallback |
|---|---|---|---|---|
- `Chroma retrieval` | semantic chunk recall | high | high | broaden retrieval once, then abstain |
- `Postgres chunk detail` | provenance and metadata | high | high | none; required |
- `Postgres KG lookup` | relation support | high | medium | answer from chunks only |
- `Answer model` | synthesis | medium | high | evidence summary fallback |

## Memory Plan

### Short-term memory

- active session state
- prior user turns
- selected retrieval bundle ids
- recent answer trace ids

### Long-term memory

- semantic corpus memory:
  - Chroma chunk index
- structured knowledge memory:
  - Postgres KG tables
- procedural/operator memory:
  - existing admin and ingestion activity logs

### Feedback loop

- `agent_query_runs`
- `agent_query_sources`
- `agent_sessions`
- `agent_messages`
- later:
  - answer ratings
  - abstention reasons
  - retrieval miss tags

### Storage choices

- Postgres:
  - sessions
  - messages
  - query traces
  - source selections
  - answer audits
- Chroma:
  - existing chunk retrieval only
- No new store is required for v1

## Guardrails

- read-only agent path
- no ingestion/admin mutation tools exposed to the query agent
- citation-required output
- abstain when evidence is weak
- cap number of chunks and assertions in context
- log every retrieval call and selected source
- keep answer model separate from admin controls
- rate-limit query endpoint
- redact API secrets from traces

## Human-In-The-Loop Controls

- operator can inspect:
  - chosen chunks
  - chosen KG assertions
  - final answer trace
  - abstain reasons
- operator can replay a query
- operator can compare answer with source chunks in admin UI
- operator cannot be bypassed for corpus mutation through the agent

## Implementation Plan

### Phase 1: Query Substrate

- add Postgres tables:
  - `agent_sessions`
  - `agent_messages`
  - `agent_query_runs`
  - `agent_query_sources`
- add retrieval service module:
  - query normalization
  - chunk search
  - neighbor expansion
  - KG lookup
  - context bundle builder

### Phase 2: Minimal Agent Endpoint

- add endpoint:
  - `POST /agent/query`
- request:
  - `question`
  - `session_id` optional
  - `document_ids` optional
  - `top_k` optional
- response:
  - grounded answer contract described above

### Phase 3: Reflection And Traceability

- add self-check step after answer generation
- persist:
  - selected chunks
  - selected assertions
  - final confidence
  - abstention reason when applicable

### Phase 4: Admin Visibility

- extend admin console to show:
  - query traces
  - sessions
  - source bundles
  - answer/citation pairing

## Minimal Serving Contract

### Input

```json
{
  "question": "How do bees produce honey?",
  "session_id": "optional",
  "document_ids": ["optional"],
  "top_k": 8
}
```

### Output

```json
{
  "answer": "string",
  "confidence": 0.0,
  "abstained": false,
  "abstain_reason": null,
  "citations": [
    {
      "chunk_id": "string",
      "document_id": "string",
      "filename": "string",
      "page_start": 1,
      "page_end": 2,
      "section_title": "string"
    }
  ],
  "supporting_entities": ["string"],
  "supporting_assertions": ["string"],
  "trace_id": "string"
}
```

## Test Plan

### Happy path

- direct factual bee questions answered with citations
- procedural beekeeping question uses chunks plus neighbor continuity
- definition question grounded in one or two strong chunks

### Edge cases

- question outside corpus
- query with ambiguous terminology
- contradictory evidence between chunks
- KG fact present but chunk support weak

### Tool failures

- Chroma unavailable
- Postgres query failure
- answer model timeout
- partial KG lookup failure

### Ambiguous inputs

- very short question
- pronoun-heavy follow-up without session history
- user asks for unsupported admin action through query agent

## Rollout Plan

### Pilot scope

- single-user, read-only querying over the current shared corpus

### Success metrics

- citation coverage rate
- abstention correctness
- answer usefulness on a fixed query set
- retrieval parity issues found during traces

### Monitoring

- query latency
- retrieval count distribution
- answer model failures
- abstention rate
- no-source answer rate, target zero

### Expansion plan

1. v1 single-agent QA over shared corpus
2. add session continuity and trace inspection
3. add document scoping and richer query filters
4. add optional agent-side task decomposition only if the use cases demand it

## What Should Be Built Next

- build the retrieval/context service
- add the agent query endpoint
- add query trace tables
- keep the answering path read-only
- only after that consider agent-side planning beyond retrieval QA
