# Agent Build Log

Append-only running log for the Bee ingestion to agent implementation.

## Session 2026-03-20

### Goal

Build a read-only Bee knowledge agent on top of the existing ingestion stack without redesigning the backend later. The agent must be citation-grounded, traceable, and additive to the current pipeline.

### Current System State

- Ingestion pipeline is live in Docker on `bee_ingestion_test-api-1`, `bee_ingestion_test-postgres-1`, and `bee_ingestion_test-chroma-1`.
- Full corpus ingestion has been completed for the two bee PDFs.
- Postgres stores chunks, validations, KG raw extractions, KG assertions, and ontology metadata.
- Chroma stores accepted chunk vectors and is parity-aligned with accepted chunks.
- Admin UI is live at `http://localhost:18000/admin` and includes pipeline controls, activity, and API console.
- Dark theme is enabled.
- Chunk ontology metadata is now persisted in `document_chunks.metadata_json` and visible through the admin metadata view.

### Planned Workstreams

1. Add a read-only agent query layer that retrieves chunks and KG evidence.
2. Keep the agent boundary narrow so ingestion/admin operations remain separate.
3. Add trace tables for agent sessions, messages, queries, and source bundles.
4. Expose agent traces in the admin UI for inspection.
5. Keep the implementation modular so later agent capabilities can be added without redesign.

### Completed Milestones

- [x] Agent opportunity assessed.
- [x] Agent boundary defined.
- [x] SPAR design drafted.
- [x] Agent tables added.
- [x] `AgentService` added.
- [x] `POST /agent/query` added.
- [x] Admin agent monitoring UI added.
- [x] Live query persistence bug fixes applied, including UUID serialization handling.
- [x] Targeted tests passed.
- [x] Implementation scaffold created.
- [x] Query endpoint added.
- [x] Admin trace visibility added.

### Verification Notes

- [x] Targeted container tests passed:
  - `tests/test_agent.py`
  - `tests/test_api.py`
  - `tests/test_repository.py`
- [x] Agent and admin route wiring is present in the live API.
- [ ] Minimal end-to-end query test on the live stack.
- [ ] Source citation check for answer output.
- [ ] Admin trace inspection check.
- [ ] Failure and abstention behavior check.
- [ ] Live browser smoke test for the agent monitoring UI.

### Notes

- The agent layer is intentionally read-only and additive to the existing ingestion system.
- The remaining verification work is live behavior validation, not core implementation scaffolding.

## Session 2026-03-20 - Agent Scope And Persistence

### Goal

Harden the agent layer for tenant-safe document scoping and durable query trace persistence, while keeping the agent read-only.

### Completed Milestones

- [x] Tenant-safe document scope enforcement added to agent retrieval.
- [x] Prompt, raw response, and final response payloads persisted in `agent_query_runs`.
- [x] Richer agent run detail UI added with structured citations and source links.
- [x] Database schema applied for the agent trace fields.
- [x] Focused tests passed (`16 passed`).
- [x] Live `POST /agent/query` succeeded with persisted run detail and source bundle.
- [x] Invalid document scope returned a boundary error.

### Verification Notes

- [x] `agent_query_runs` includes `prompt_payload`, `raw_response_payload`, and `final_response_payload`.
- [x] Admin agent run detail view returns query traces and source bundle rows.
- [x] Live agent query persisted a query run and linked source rows.
- [x] Scope violation fails closed with `Document scope violates tenant boundary`.
- [ ] Browser smoke test for the agent run detail UI.
- [ ] Full end-to-end citation inspection in the browser UI.

### Notes

- The agent remains read-only.
- Scope enforcement is now part of retrieval, not a post-hoc UI filter.

## Session 2026-03-20 - Next Agent Phase

### Goal

Extend the read-only agent into a more useful chat experience without breaking the current retrieval and traceability guarantees.

### Planned Workstreams

1. Add a chat endpoint for conversational query handling.
2. Improve retrieval strategy and reranking so the agent selects better source bundles.
3. Add context compression so the prompt stays bounded as the corpus grows.
4. Add an answer-quality review workflow for weak or uncertain answers.
5. Extend admin UI monitoring for live agent runs, traces, and review states.
6. Run focused validation after each implementation step.

### In Progress Milestones

- [x] Chat endpoint implemented.
- [x] Retrieval strategy and reranking implemented.
- [x] Context compression implemented.
- [x] Answer-quality review workflow implemented.
- [x] Admin UI monitoring extended for the next agent phase.
- [x] Focused validation completed for the next agent phase.

### Verification Notes

- [x] Live chat endpoint smoke test.
- [x] Source citation check on a conversational answer.
- [x] Trace persistence check for multi-turn queries.
- [x] UI monitoring check for the new chat flow.

### Notes

- Keep the agent read-only.
- Preserve traceability and citation requirements.
- Prefer additive changes over redesigns.

## Session 2026-03-20 - Next Agent Phase Results

### Completed Milestones

- [x] `POST /agent/chat` added.
- [x] Retrieval planning, reranking, and context compression added.
- [x] Answer review persistence added.
- [x] Admin UI session transcript and review queue added.
- [x] Focused tests passed (`20 passed`).
- [x] Live smoke checks passed.
- [x] Invalid document scope now fails with `400`.

### Verification Notes

- [x] Live `POST /agent/chat` returned a persisted `query_run_id`.
- [x] Live `POST /agent/chat` with invalid document scope returned `STATUS=400`.
- [x] Agent query runs persist prompt, raw response, and final response payloads.
- [x] Admin UI exposes transcript and review queue views for sessions.

### Notes

- The next agent phase is complete at the feature level.
- Remaining work is future iteration and broader browser validation, not core implementation.

## Session 2026-03-20 - Agent Hardening Pass

### Planned Workstreams

1. Add atomic agent trace persistence so query/session/source records are committed consistently.
2. Enforce prompt and context budgets so agent requests stay bounded as the corpus grows.
3. Run focused tests after each hardening change.
4. Restart the API after the hardening changes are applied.
5. Perform live smoke validation against the agent chat and trace endpoints.

### Notes

- Keep this pass limited to reliability and traceability.
- Do not expand agent scope in this pass.
- Keep the implementation additive.

## Session 2026-03-20 - Hardening Update

### Completed Milestones

- [x] Prompt budgets enforced.
- [x] Prompt-context metrics persisted.
- [x] Assistant metadata now carries `query_run_id`.
- [x] Focused tests passed (`21 passed`).

### Verification Notes

- [ ] Live API smoke validation is in progress.

### Notes

- This pass remains focused on reliability and traceability.
- The remaining work is live validation, not additional scope.

## Session 2026-03-20 - Operator Pass

### Completed Milestones

- [x] Session leases and serialization are in place for agent state.
- [x] Live busy-session rejection verified.

### Next Step

- Add replay controls plus aggregate agent metrics and latency monitoring to the admin UI.

### Verification Notes

- [x] Focused agent tests passed (`5 passed`).
- [x] Lease enforcement paths are present in the live agent stack.

## Session 2026-03-23 - Answer Quality and Config Surface

### Completed Milestones

- [x] Backend-owned citations are now the source of truth for displayed answer sources.
- [x] Retrieval reranking and context assembly were tightened to improve answer grounding.
- [x] Agent runtime config is now editable from the admin surface, including prompt, model, temperature, and timeout controls.
- [x] Answer-quality behavior was improved without expanding agent scope beyond read-only QA.

### Notes

- Keep the agent answer path read-only and traceable.
- Prefer backend rendering for source snippets instead of model-authored citation text.
- Treat runtime config changes as additive, persisted operator controls rather than code edits.

## 2026-03-23 20:53:26 +02:00 - Implementation Pass

- Runtime agent config editing was added for prompt, model, temperature, and timeout controls.
- Source display is backend-owned so citation snippets come from stored chunks, not model-authored quotes.
- Session token flow was kept explicit in the user-agent path so sessions can continue across turns.
- Upload handling was hardened around workspace-scoped paths and filename sanitization.
- Next step: run minimal smoke tests for the agent UI, runtime-config edits, citation rendering, and upload ingest.

## 2026-03-24 - Multimodal Continuation

- The multimodal ingestion/agent pass resumed after the token interruption.
- Multimodal ingestion completion is now in progress.
- Current focus is page/image extraction, OCR/vision enrichment, asset-to-chunk linkage, and downstream retrieval visibility.
- Patched agent multimodal retrieval so assets are retrieved from the asset collection, budgeted into the prompt, and can be cited via `used_asset_ids`.
- Patched page-image persistence so `page_image` assets are kept even when vision returns no OCR or summary text.
- Patched admin and user UIs to surface page assets, chunk-linked assets, and asset citations.
- Minimal `py_compile` validation passed on the touched runtime files.
- Fixed fresh-database schema ordering issues in `sql/schema.sql` for `chunk_asset_links` and `agent_query_sources`.
- Brought up the local stack successfully after recreating volumes.
- Ran a 5-page multimodal ingest on `honeybeeitsnatur00cowa_bw.pdf` and validated: `pages=5`, `page_assets=14`, `indexed_assets=14`, `accepted_chunks=1`.
- Verified the agent can return asset-backed citations with image URLs and `used_asset_ids` on a document-scoped query.
- Patched review-state logic so asset-backed answers are not mislabeled as chunk-only.
- Found a multimodal ingest blocker: `SourceDocument.content_hash` for PDFs currently hashes raw extracted text only, which can collide for weak-text or scanned PDFs.
- The fix in progress is to compute a deterministic PDF file-content hash with a page-range salt for PDF ingest while keeping text ingest unchanged.
- The PDF content-hash fix is implemented and live-validated.
- PDFs now use a deterministic file-byte hash plus page-range salt at ingest time, and rebuilds preserve the stored hash.
- Smoke-tested `honeybeeitsnatur00cowa_bw.pdf` (`0` raw chars) and `howtokeepbeeshan00comsiala.pdf` under tenant `hash-smoke-20260324`; both produced distinct document hashes and successful multimodal ingests.
- Retrieval work has started.
- The old retrieval eval only measured raw Chroma hits and did not reflect the live agent retrieval path.
- A tenant-scoping bug was found in asset retrieval where `_build_context_bundle` used the runtime config tenant fallback instead of the actual query tenant.
- Both issues are being fixed before rerank tuning.
- Retrieval eval now runs against `AgentService.inspect_retrieval` instead of raw Chroma.
- Asset retrieval is tenant-scoped correctly, direct asset hits are reranked, and search breadth is configurable via `max_search_k` / `max_asset_search_k`.
- Visual queries route through `multimodal_focus`.
- The judged eval tenant `retrieval-eval-20260324` currently passes `4/4` queries, with the latest output written to `data/evaluation/latest-admin-eval.json`.
- Admin retrieval-eval paths are restricted to `data/evaluation` JSON files only.
- Visual asset-link trace rows now require the linked chunk to remain in the final bundle.
- `py_compile` passed for `api.py` and `agent.py`.
- The live API restarted cleanly.
- Positive retrieval eval still passed `4/4` on `retrieval_small_queries.json`.
- Forbidden read test for `/admin/api/retrieval/evaluation?path=../api.py` now returns HTTP `400`.
- Starting a security remediation pass based on the user's findings.
- Scope for this pass: admin/ingest auth gate, tenant-bound session handling, secret/config sanitization, abstention leak fix, path validation, then review-decision validation, replay token flow, config upper bounds, and retrieval-eval gating.
- Compose ports now bind to `127.0.0.1`, and Postgres env values are compose-variable driven.
- Public agent queries now force the shared tenant, while admin/internal replay and inspect paths can use trusted tenant/session reuse.
- KG entity search is tenant-filtered.
- Agent runtime `api_key_override` is split from normal config and hidden from config responses.
- Persisted agent payloads redact secret-like fields.
- Abstained answers now return the generic abstention text instead of model-authored content.
- Direct PDF ingest resolves Windows paths safely inside `/app`.
- Runtime config has upper bounds.
- User like/dislike no longer updates admin correctness counts.
- Admin review decisions are validated.
- Retrieval eval now uses `expected_focus`-derived terms when explicit `expected_terms` are absent.
- Live checks passed: `health` ok, invalid review returns `400`, public tenant coercion persisted the query under `shared`, retrieval eval remained `4/4`.
- A second security/correctness remediation pass started on `2026-03-24`.
- Target items: normalize abstention/review reasons, improve weak-text multimodal retrievability, replace page-wide chunk-asset links with evidence-based linking, and continue admin/control-plane hardening.
- Added `ADMIN_API_TOKEN` in `.env`.
- Switched user session token storage to `sessionStorage`.
- Added session token max-age enforcement.
- Normalized abstention reason codes.
- Made chunk-asset links evidence-based instead of page-wide.
- Strengthened weak-text/page-image asset indexing.
- Compacted persisted trace payloads.
- Removed `page_context` fallback from chunk-asset linking.
- Added stale indexed-job auto-finalization in `repository.create_job`.
- Restarted the API.
- Rebuilt the six currently loaded sample PDFs so live data now uses the stricter asset-linking rules.
- A new pass started on `2026-03-25` to expand the operator UI and runtime configurability beyond the current answer-model controls.
- Target areas: ingestion, chunking/validation, vision/multimodal, review, KG, and retrieval/runtime knobs.
- Finished wiring the system runtime env editor.
- Added grouped `.env` load/save/reset support.
- Secret fields are redacted.
- Compile and live validation are now running.
- System runtime env editor finished.
- Grouped `.env` load/save/reset added.
- Secret fields are masked with `<unchanged>`.
- Env path is displayed as `E:\\n8n to python\\.env`.
- Control-plane now hard-fails closed when the admin token is unset.
- Live validation passed: compile, restart, `401` without token, `GET`/`PUT` system config, and admin HTML controls present.
- A retrieval hardening pass is in progress.
- Scope for this pass: replay fidelity, final-context trace accuracy, citation grounding, public asset citations, prompt-budget enforcement, session scope, asset reindex consistency, asset linking heuristics, document-collapse tuning, and retrieval-eval quality.
- Live retrieval eval and API validation are underway.
- Remaining focus is evidence grounding, replay fidelity, prompt-budget enforcement, and trace correctness.
- Retrieval hardening completion: live eval is now `4/4` on `retrieval_small_queries.json`.
- Visual queries no longer cross-mix chunk and asset documents.
- Replay now preserves `request_scope`.
- Traces persist final trimmed source selections.
- Added `scope_document_filenames` support in retrieval eval.
- Next task: admin-console expansion for full editability and operator visibility.
- Focus for the next pass: admin-only edit endpoints plus UI editors for persisted records and runtime config.
- Admin generic editor backend and UI are implemented.
- Added allowlisted load/save/delete/resync paths for `document`, `source`, `page`, `chunk`, `asset`, `asset_link`, `kg_entity`, `kg_assertion`, `kg_raw`, `agent_session`, and `agent_pattern`.
- Added an operations-panel record editor in the admin UI and editor buttons from detail screens.
- Validation is now running.
- Fixed an editor-path bug where chunk validation-only edits could overwrite `quality_score` and `reasons` with defaults. The repository now preserves existing validation fields when they are omitted from admin patch payloads.
- Admin console now includes a generic direct record editor in `Operations`.
- Added backend load/save/delete/resync routes and allowlisted patch support for `document`, `source`, `page`, `chunk`, `asset`, `chunk_asset_link`, `kg_entity`, `kg_assertion`, `kg_raw_extraction`, `agent_session`, and `agent_query_pattern`.
- Added vector resync for `document`, `chunk`, and `asset`, and wired detail screens to prefill the editor.
- Live smoke checks passed for `document`/`source`/`page`/`chunk`/`asset`/`link`/`kg raw` load/save and `chunk`/`asset` resync after API restart.
- User reported that the UI is not live. Checking container status and HTTP responses on `localhost:18000` now; the concrete failure will be fixed and the outcome recorded.
- User reports that the wrong API instance is being used. Auditing `compose.yaml` and all running containers to identify the intended project API and correct the target and launch path.
- Patched admin UI auth and error handling. When `/admin/api` calls return `401` or `503`, the page now shows an explicit admin-token-required notice instead of appearing dead or wrong.
- User identified host-port collision confusion between API stacks.
- Fix in progress: move this workspace compose services to high host ports, restart the `n8ntopython` stack, and verify admin, user, Chroma, and Postgres on the new ports.
- New combined task accepted.
- Scope for this pass: `1)` finish the host-port migration to high ports and fix remaining admin UI issues, including auto-refresh and the unknown Chroma collection state, `2)` normalize LLM secret and config handling so non-embedding model keys are not hidden in ad hoc stores, and `3)` perform the ontology pass by reading the books and wiring the updated ontology into the codebase without starting ingestion.
- Current pass started on `2026-03-25`.
- Focus: fix Docker host-port conflicts, fix admin UI live refresh and Chroma collection handling, normalize LLM secret and config handling, and revise the ontology from the books without starting ingestion.
- Confirmed high host ports are active: API `38100`, Chroma `38101`, Postgres `35432`.
- Identified the remaining admin issue: `admin_ui.py` still hardcodes the old Chroma collection names `kb_chunks_v1` and `kb_assets_v1`, and the parity-note logic still checks against `kb_chunks_v1`, which explains the unknown-collection problem.
- Confirmed current model-key handling: settings supports separate embedding, KG, review, vision, and agent keys, but `.env` mostly leaves them unset and the code falls back to `EMBEDDING_API_KEY`.
- Patching UI and config handling now, then moving to ontology revision without running ingestion.
- Identified the concrete admin bugs to patch now: `admin_ui.py` still has hardcoded collection options, and `reloadAllViews` uses `Promise.all`, so one failing view stops live refresh.
- Patching dynamic collection options, resilient refresh, and a provider-key source summary so the UI shows the actual env and override fallback chain.
- Ontology pass follows immediately after.
- Backend and UI patches are in.
- Added provider-key source reporting, Chroma collection default flags, dynamic collection handling in the admin UI, resilient reload logic, and fixed duplicate KG cleanup so orphan entities are pruned on chunk deletes.
- Added explicit env keys for `CHROMA_ASSET_COLLECTION`, `KG_API_KEY`, `REVIEW_API_KEY`, and `AGENT_API_KEY`.
- Starting the ontology pass now by mining recurring concepts from the local PDF corpus; no ingestion run will be started.
- Reviewer found three real regressions after the first pass.
- `admin/api/chroma/records` still allows only the two configured collections even though the UI now lists all Chroma collections.
- The singularization helper mangles the ontology alias `nucleus` into `nucleu`.
- The new ontology hierarchy made hive-type and brood-stage assertions fail validation.
- Patches for all three are now in progress.
- Moved the live stack to high host ports and confirmed the old `18000` binding is gone: API `38100`, Chroma `38101`, Postgres `35432`.
- Fixed admin UI Chroma handling by removing hardcoded collection names and adding dynamic defaults plus resilient live refresh.
- Added provider-key source reporting and explicit env keys for `KG_API_KEY`, `REVIEW_API_KEY`, `VISION_API_KEY`, `AGENT_API_KEY`, and `CHROMA_ASSET_COLLECTION`.
- Added an ontology editor to the admin UI and new `/admin/api/ontology` endpoints.
- Revised `data/beecore.ttl` from the local books and updated `kg.py` to parse `skos:altLabel` and use ontology aliases for tagging and canonicalization.
- Fixed duplicate KG cleanup pruning.
- Validated `/health`, `/admin`, `/admin/api/chroma/collections`, `/admin/api/system/config`, and `/admin/api/ontology` on port `38100`.
- Reviewer re-check returned no findings.
- New pass started to implement dynamic `top_k` selection for the agent answer path.
- Scope: stop forcing `8` by default in the user UI, add backend question-type-driven `top_k` selection when unset, keep manual override support, surface the effective `top_k` in responses and UI, and run minimal validation only.
- Patched dynamic `top_k` into the runtime config and answer path.
- The user UI no longer forces `8`; a blank value now means auto-selection.
- Responses now surface `effective_top_k`, `top_k_source`, `question_type`, and `retrieval_mode`.
- Running two minimal live queries now to verify that definition and procedure questions choose different `top_k` values.
- Evaluated the dynamic `top_k` strategy.
- The current rule-based classifier is brittle for narrative beekeeping questions that do not start with explicit cue phrases.
- Recommendation: replace primary question-type and `top_k` routing with a low-cost structured classifier model, keeping heuristic rules as fallback.
- Official OpenAI docs were checked: `GPT-5 nano` is positioned for simple instruction-following and classification, and the docs recommend `GPT-5.4 nano` as the newer speed and cost-sensitive starting point.
- No code changed in this update; this is a design decision and next-step recommendation.
- Began implementation of a cheap router model for query classification and `top_k` selection.
- Patched `agent_runtime` defaults and coercion to add `router_system_prompt`, and fixed `router_max_completion_tokens` coercion to be integer-based.
- Currently patching `api.py` to expose router settings in the editable env and config surfaces and in the key-source display.
- Patched `query_router` to use the runtime-configurable `router_system_prompt` and richer structured output fields.
- Updated agent metrics and prompt payloads to carry routing metadata.
- Updated the public agent UI to show router source and confidence, and to accept `Top K` overrides up to `24`.
- Running syntax validation next.
- Completed the cheap query-router rollout.
- The live API restarted on [http://localhost:38100](http://localhost:38100).
- Runtime config now exposes `router_enabled`, `provider`, `base_url`, `model`, `reasoning_effort`, `prompt_version`, `router_system_prompt`, `router_temperature`, `router_max_completion_tokens`, `router_timeout_seconds`, and `router_confidence_threshold`.
- Query routing is now model-backed via `query_router.py`, with heuristic fallback when the router is disabled, low-confidence, or errors.
- Live validation: a narrative procedure query returned `question_type=procedure`, `top_k=4`, `top_k_source=router`, `routing.model=gpt-5.4-nano`.
- Live validation: a narrative comparison query returned `question_type=comparison`, `top_k=6`, `top_k_source=router`, `routing.model=gpt-5.4-nano`.
- The admin config API now shows the new router fields and the router key source.
- UI updates: the user app status note now includes router source and confidence, and the `Top K` override input allows up to `24`.
- Starting multimodal evidence refinement.
- Targets for this pass: improve asset `search_text` composition, make chunk-asset links stricter and evidence-based, tune visual and bundle-aware retrieval selection, and strengthen retrieval evaluation for asset-backed queries before any wider ingest.
- Reviewer findings were accepted.
- Additional fixes are now being implemented: retrieval will start consuming `router.requires_visual` and `router.document_spread`, visual-reference links will require stronger evidence, and retrieval evaluation will score selected source rows and link confidence instead of only loose concatenated keyword hits.
- Continuing multimodal evidence refinement.
- Current focus: rerun live retrieval evaluation after stricter chunk-asset linking, improved asset `search_text`, router-driven visual retrieval settings, and bundle-aware asset and chunk compression.
- Final validation results and any follow-up fixes will be recorded after the rerun.
- Live multimodal retrieval evaluation rerun after rebuild passed `4/4` with `asset_queries_passed=1` on tenant `retrieval-eval-20260324`.
- Document `417bcab0-2b89-412c-be5e-ecf035b8916b` was rebuilt successfully with `pages=5`, `page_assets=5`, and `accepted_chunks=1`.
- Retrieval inspect for the visual query showed `question_type=visual_lookup`, retrieval mode `multimodal_focus`, selected asset hits and a `text_overlap` asset link with confidence `0.95`, `linked_chunk_ids` present, and the prompt asset payload populated.
- Final multimodal refinement results:
- Non-page assets no longer inherit `page_summary` or `page_terms` into retrieval text. `PageAsset.search_text` now uses asset-type-specific role terms and keeps page summary and terms only for `page_image` assets.
- `visual_reference` chunk-asset links now require non-`page_image` assets, at least two shared terms, label overlap, and `overlap_ratio >= 0.12`.
- Retrieval evaluation now records `row_term_coverage`, `row_focus_hit`, and `asset_row_term_coverage`, and requires row-level support instead of only whole-bundle keyword hits.
- Rebuilt honeybee document `417bcab0-2b89-412c-be5e-ecf035b8916b`.
- Restarted the API.
- Admin retrieval evaluation on tenant `retrieval-eval-20260324` now passes `4/4` with `asset_queries_passed=1` on port `38100`.
- Starting the agent-improvements pass from reviewer feedback.
- Scope for this pass: implement session memory compaction with evidence anchors, claim-level verification, make feedback and patterns operational or simplify semantics, tighten multimodal provenance further, clean up the KG event contract mismatch, upgrade evaluation to judged agent checks, add router caching by normalized query signature, and move browser session handling toward a stronger cookie-based model.
- Implementation continued on `2026-03-27` for agent memory compaction, claim verification, router caching, cookie sessions, judged evaluation, and KG event contract cleanup.
- The main patch set compiled, but live validation is currently blocked because the Docker daemon is unreachable from the shell and `localhost:38100` is down.
- Continuing the pass on `2026-03-28`: adding a browser-scoped user profile layer tied to agent memory and sessions, then running live validation through Docker if available.
- The log remains open for the next update.
- Browser-scoped user profiles were added.
- Added the new `agent_profiles` table and linked profiles to agent sessions.
- Added profile runtime config and UI editing.
- The admin agent tab now shows profiles.
- The schema was applied live, and the API is live on `localhost:38100`.
- Validated `GET` and `PUT /agent/profile`, `POST /agent/chat` with profile and memory persistence, and admin profile/session/run visibility.
- Included the serializer fix for UUID responses.
- New task started: add a Supabase-style lazy-loaded `Database` tab to the admin UI with table introspection, schema display, paginated row browsing, and live refresh for the selected table only.
- The database browser tab was added to the admin UI.
- It lazy-loads public-schema relations and only loads schema plus paginated rows for the selected relation while the `Database` tab is open.
- Added endpoints `GET /admin/api/db/relations` and `GET /admin/api/db/relations/{relation_name}`.
- No full-table preload is performed.
- Starting the final ontology and ingestion-layer check before a controlled corpus reset and fresh re-ingest.
- The planned reset scope includes ingestion, KG, and vector data plus agent sessions, summaries, and profiles, while keeping config and ontology.
- `2026-03-28` final ontology and ingestion preflight is green.
- Starting a controlled reset of ingestion, KG, vector, and agent-derived state, including session memories and profiles, while preserving config and ontology.
- Commands, before/after counts, and final fresh-ingest totals are being captured in this log as the reset and re-ingest proceed.
- Reset completed and verified clean.
- Postgres ingestion, KG, session, and profile tables are empty; Chroma chunk and asset collections are both at zero; `data/page_assets` was cleared.
- Starting fresh ingestion with KG over all workspace-root PDFs on `2026-03-28`.
- Added `tools/reingest_all_pdfs.py` as the detached batch runner for fresh corpus ingestion.
- It writes progress and summary JSON under `data/logs` and ingests all workspace-root PDFs with `tenant=shared`, `document_class=book`, multimodal extraction, vector indexing, and KG enabled.
- Fresh re-ingest is running detached in `n8ntopython-api-1` via `tools/reingest_all_pdfs.py`.
- The first file is `4-H-1059-W.pdf`.
- Current live counts mid-file: `documents=1`, `pages=36`, `assets=65`, `chunks=196`, `accepted=128`, `kg_raw=25`, `kg_assertions=23`, `kg_review=0`.
- Progress file: `data/logs/reingest-progress.json`.
- Stdout log: `data/logs/reingest-20260328.log`.
- Log correction: stopped the first detached re-ingest after review found that the runner was using raw-text identity.
- Patched `tools/reingest_all_pdfs.py` to use file-byte PDF hashing and to fail non-zero on partial-corpus errors.
- Re-ran the clean reset successfully; all ingestion, KG, session, and profile tables plus both Chroma collections are back to zero.
- Relaunching the fresh corpus ingest now.
- Corrected re-ingest is running.
- Verified that document identity now uses file-level hashing; for example `4-H-1059-W.pdf` is stored with `content_hash sha256:0770ef3f6e55e5db73966518f0bc5887d9d33b2d23d5e448d065c78807cb03e5`.
- Current live mid-file counts: `documents=1`, `pages=36`, `assets=65`, `chunks=196`, `accepted=129`, `kg_raw=23`, `kg_assertions=17`, `kg_validated=8`.
- The run remains on file `1/13` and is still progressing through KG.
- While the live re-ingest continues untouched, four non-disruptive tasks are running in parallel: `(1)` monitoring ingest progress, `(2)` drafting the next ontology revision in separate files only, `(3)` preparing a stronger judged eval set, and `(4)` reviewing the current retrieval and agent code for the next pass.
- No restarts, schema changes, or live pipeline mutations are being made during this phase.
- Created non-disruptive draft artifacts during the live ingest: `data/beecore.next-draft.ttl`, `docs/ontology-next-draft-notes.md`, `data/evaluation/agent_gold_queries_draft.json`, and `docs/agent-next-pass-review.md`.
- The JSON draft validated successfully with `12` rows.
- Current ingest snapshot: `documents=1`, `pages=36`, `assets=65`, `chunks=196`, `accepted=129`, `review=31`, `kg_assertions=92`, `kg_validated=35`.
- The progress file still reports `current_file=4-H-1059-W.pdf`.
- Merged the accepted relation-safe ontology additions into the live file `E:\n8n to python\data\beecore.ttl` while the detached ingest kept running.
- The ontology version was bumped to `0.3.1`.
- Added live classes and properties include `SugarSyrup`, `CandyFeed`, `BeeBread`, `SectionHoney`, `BroodNest`, `DroneComb`, `QueenCup`, `SectionBox`, `BeeEscape`, `HoneyHouse`, `BeeSpace`, `VentilationManagement`, `PreventingRobbing`, `feedsWith`, `usesComponent`, `occupiesBroodNest`, `storesResource`, and `maintainsBeeSpace`.
- Event-model concepts such as `HoneyFlow` and `ColonyProcess` remain draft-only in `data/beecore.next-draft.ttl`.
- Validation against `/app/data/beecore.ttl` succeeded with `classes=102` and `predicates=27`.
- The ingest progressed to file `2/13` (`A_Practical_Manual_of_Beekeeping.pdf`) during the merge, confirming no disruption.
- Log correction: the user switched from wait-for-finish to immediate restart (`option 2`).
- The current ingest is being interrupted, the API container is being restarted to load the updated live ontology, the approved ingestion, KG, vector, session, and profile state is being re-cleared, and a fresh corpus ingest is being started again from the beginning.
- Executed `option 2`.
- Restarted `n8ntopython-api-1` immediately and confirmed that the updated ontology is loaded in memory with `classes=102` and `predicates=27`, including `SugarSyrup`, `BeeSpace`, `feedsWith`, and `maintainsBeeSpace`.
- Re-cleared the approved ingestion, KG, vector, session, and profile state and started a fresh corpus ingest from zero.
- The new run started at `2026-03-28T11:04:54Z` with `current_file=4-H-1059-W.pdf`.
- Early snapshot: `documents=1`, `pages=0`, `assets=0`, `chunks=0`, `accepted=0`, `kg_assertions=0`.
- The detached runner process is active in `n8ntopython-api-1`.
- While the live ontology-aware ingest continues, an offline implementation pass is running for the memory and summarisation system plus post-ingest validation assets.
- No restart, schema apply, or live deployment is being performed during this phase.
- Completed the offline memory and summarisation improvement pass on disk.
- Changes include a richer session memory structure (`resolved_threads`, `user_preferences`, `topic_keywords`, `scope_signature`, `summary_version`), recent-message-aware summarisation inputs, stronger fallback memory and profile builders, tighter budget trimming, profile `communication_style` and `learning_goal` support, and a post-ingest validation runbook at `docs/post-ingest-validation-runbook.md`.
- Reviewed findings about soft budget ceilings, coarse fallback facts, and transient-goal leakage were addressed.
- `py_compile` passed on `agent.py`, `agent_runtime.py`, and `settings.py`.
- No restart or deployment was performed during the active ingest.
- Starting a read-only code review pass during the active ingest.
- Focus areas: recent agent memory and profile changes, the ontology and re-ingest path, and current operational risks.
- No live mutations are being made during this review.
- Starting a read-only code review during the live ingest.
- Scope: `src/bee_ingestion/agent.py`, `src/bee_ingestion/agent_runtime.py`, `src/bee_ingestion/settings.py`, `tools/reingest_all_pdfs.py`, `tools/wait_then_rerun_with_live_ontology.py`, and ontology/runtime coupling.
- No code changes, no restart, and no database mutation are being made during this review.
- Code review findings were prepared.
- Main issues identified: `(1)` `_compact_recent_messages()` with `limit <= 0` returns the full history, `(2)` the new memory and profile runtime knobs are not clamped, `(3)` agent query still fetches only the last `10` messages so larger memory and profile recent-message settings are ineffective, `(4)` the ontology rerun watcher proceeds on `completed_with_errors` and can wipe failed-run state, `(5)` progress JSON writes are non-atomic while the watcher reads raw JSON, creating a race, and `(6)` the watcher marks `completed` immediately after the detached relaunch without verifying that the new ingest actually started.
- No code changes were applied in this review update.
- Applying safe on-disk fixes during the live ingest.
- Scope: clamp memory and profile runtime knobs, fix the recent-message slicing bug, remove the dead `10`-message cap from agent memory and profile fetch, make progress-file writes atomic, and harden watcher terminal-state logic and relaunch verification.
- No restart or live-process interruption is being performed during this fix pass.
- Applied safe on-disk fixes during the live ingest.
- Patched files: `src/bee_ingestion/agent.py`, `src/bee_ingestion/agent_runtime.py`, `tools/reingest_all_pdfs.py`, and `tools/wait_then_rerun_with_live_ontology.py`.
- Fixed the recent-message negative and zero slicing bug, clamped the new memory and profile runtime knobs, removed the hardcoded `10`-message cap from memory and profile fetch, made re-ingest progress writes atomic, made the watcher fail closed on `completed_with_errors`, added fresh-progress verification after detached relaunch, and made PDF discovery recursive with exclusions.
- Verified with `py_compile` only; no restart was performed.
- Reset requested.
- Action plan: stop the live detached ingest, clear corpus, KG, vector, session, profile, and runtime-data state, clear progress and log artifacts plus derived page assets, then bring the API back up.
- Code, source PDFs, `.env`, and ontology files will remain on disk.
- The user changed direction before the reset, and no destructive reset actions were executed.
- Starting a read-only final analysis of the ingestion pipeline covering chunking, multimodal extraction, validation, KG, vector indexing, replay and reset behavior, and operational tooling.
- A prioritized improvement map will be returned from this analysis.
- Starting a broad ingestion fix pass on disk only.
- Scope: document identity and re-ingest behavior, asset indexing selectivity, failure cleanup and atomicity compensation, multimodal triggering, evidence-backed asset linking, multimodal-aware validation, asset-derived KG support, reset and rebuild consistency, and batch-ingest gating.
- No restart will be performed while the current ingest remains active.
- Completed the broad on-disk ingestion fix pass without restarting the live stack.
- Patched files: `src/bee_ingestion/repository.py`, `src/bee_ingestion/service.py`, `src/bee_ingestion/multimodal.py`, `src/bee_ingestion/validation.py`, `src/bee_ingestion/chroma_store.py`, and `tools/reingest_all_pdfs.py`.
- Changes included canonical document reuse on re-ingest by stable identity, broader reset-scope consistency, page-asset file cleanup hooks, source-row persistence of final multimodal normalized text and metrics, asset pagination for reindex, page-asset `search_text` persistence on reindex and resync, ontology hot-reload by file mtime, smarter page-level vision triggering, `page_image` asset creation only for visual pages, explicit vision failure accounting, stricter page-image indexability, richer chunk-asset metadata, multimodal-aware validation softening, asset-augmented KG extraction, failure cleanup that removes partial Chroma and KG publishes, Chroma reset verification, manifest support plus fail-fast acceptance gates in batch re-ingest, and nested PDF relative filenames.
- Verified with `py_compile` only.
- No restart was performed; the active ingest is still running on the previously loaded code.
- Starting the destructive restart, reset, and re-ingest as requested.
- Plan: stop the current ingest by restarting the API container, truncate the prior ingestion, KG, vector, session, and profile scope, clear derived page assets and progress logs, verify the empty state, then relaunch full re-ingest from zero on the patched code.
- The user requested a full restart from zero.
- The active ingest is being stopped, the approved ingestion, KG, vector, and agent-derived state is being cleared, and detached ingest is being relaunched on the patched code.
- Completion: the API container was restarted to stop the old ingest and load the patched code, the approved ingestion, KG, and agent-derived tables were truncated, both Chroma collections were reset to zero, derived `page_assets` and log artifacts were cleared, and detached ingest was relaunched.
- Fresh progress snapshot: `started_at=2026-03-28T14:13:46.189098+00:00`, `status=running`, `total_files=13`, `completed_files=0`, `current_file=4-H-1059-W.pdf`.
- The ingest ended with `completed_with_errors`.
- Current task: patch the PyMuPDF image-save crash in multimodal extraction, then run a broader ingestion failure-mode scan before any rerun.
- Starting a deep failure-point review across every Python code file in `src/bee_ingestion` and `tools`, with one read-only reviewer subagent per file as requested by the user.
- Goal: find code-rooted failure points and budget-waste risks, not external-provider outages.
- We are in a deep per-file failure scan using the workspace skills `agentic-ai-builder` and `api-security-best-practices`.
- `kg.py` review returned these findings:
- `(1)` KG extraction falls back to embedding credentials and base URL and can exfiltrate corpus text or ontology to the wrong host if KG config is unset; it should require an explicit KG endpoint and key with allowlisted HTTPS only.
- `(2)` Evidence spans and excerpts are not verified against chunk text, so fabricated provenance can survive.
- `(3)` Public extraction APIs return normalized but unvalidated candidates if called directly; the safe path should validate and prune by default.
- `(4)` Duplicate mention, relation, and evidence IDs are not rejected.
- `(5)` Source text is concatenated into the same instruction-carrying prompt, so prompt-injection-like control text in chunks can steer extraction.
- The user explicitly corrected us to use workspace skills from `E:`, and the whole sweep is now following that requirement.
- `cli.py` review completed with these findings:
- High: bare `--reset-data` with no `--document-id` performs a whole-project wipe because `document_id=None` is treated as a global reset; it needs an explicit `--all` plus strong confirmation.
- Medium: maintenance-mode flags are independent booleans resolved by first-match order, so incompatible combinations silently run the first branch.
- Medium: `--file` and `--text` are not mutually exclusive, causing inconsistent identity and unexpected local file reads or hashing.
- Medium: ingest defaults to tenant `shared` and maintenance paths are unscoped, so omissions can cross tenant boundaries.
- The reviewer was reassigned to `repository.py`.
- `query_router.py` review completed with these findings:
- High: the cache key can be too generic because `pattern_signature` is trusted even for low-signal signatures, so unrelated short or noisy queries can share a cached route.
- High: cached routing payloads are returned without revalidation or coercion, so malformed or stale cache rows can inject invalid `question_type`, `top_k`, `document_spread`, or `requires_visual`.
- Medium-High: invalid `document_spread` narrows to `single`, which is the unsafe direction.
- Medium-High: provider handling disables the router for non-exact `openai` values such as `auto`.
- Medium: a broad exception handler hides internal bugs behind `fallback:error` and leaks exception text into reasons.
- Medium: structured content responses are not parsed.
- Medium: there is no router input-size cap.
- Low-Medium: silent credential and base-URL fallback across subsystems weakens boundaries.
- Low: a malformed cache timestamp can bypass expiry.
- `Kant` was reassigned to `retrieval_eval.py`.
- `multimodal.py` review completed with these findings:
- High: model OCR text is promoted into canonical merged page text too aggressively, collapsing provenance and letting hallucinated OCR become first-class retrieval text.
- High: vision endpoint and key fallback crosses subsystem boundaries and sends page images to whichever base URL wins, with no scheme or host validation.
- Medium-High: full-page visuals can disappear entirely from assets on text-heavy pages because `page_image` asset creation is conditional.
- Medium-High: render, file, or parse exceptions still abort whole documents in some paths instead of degrading per page or asset.
- Medium: there are no render or request byte or pixel ceilings before base64 upload, so large PDFs can cause memory blowups and high spend.
- The reviewer was reassigned to `service.py`.
- `models.py` review completed with these findings:
- High: `PageAsset.asset_path` and `DocumentPage.page_image_path` are raw filesystem paths; combined with editable admin flows and image endpoints, a poisoned row can become arbitrary local file serving and host-path leakage.
- High: `PageAsset.search_text` assumes several metadata keys are `list[str]`, but metadata is an unvalidated JSON bag; malformed values can raise or corrupt tokenization during asset save, embed, or retrieval.
- Medium: `Chunk.metadata` is another untyped bag even though ranking, validation, and serialization assume numeric and list contracts.
- Medium: `ChunkAssetLink.confidence` is not bounded or checked for finiteness, so edited or bad rows can skew evidence ranking.
- The reviewer was reassigned to `validation.py`.
- `pipeline.py` review completed with these findings:
- High: `kg_validated` exists as a stage but not as a durable job status, so a crash during KG work leaves the job at `indexed` and stale-job recovery can wrongly finalize an unvalidated run.
- Medium: stage validation only checks stage name and outcome independently, not that the pair and job transition are coherent.
- Low: `quarantine` is a terminal job status and stage outcome, but there is no quarantine stage or event.
- Low: `registered` exists in the job machine but not in stage names, so immutable stage logs cannot represent job creation or claim.
- The reviewer was reassigned to `settings.py`.
- `retrieval_eval.py` review completed with these findings:
- High: `scope_document_filenames` fails open to the full tenant corpus when resolution returns no ids.
- High: under-specified rows pass too easily because most expectations default to trivially true or zero.
- High: term and focus scoring counts `asset_link.payload.shared_terms` as if it were evidence text.
- Medium-High: asset expectation is bundle-level and too weak to prove that the correct asset was retrieved.
- Medium-High: `expected_focus` can pass via coarse term overlap without the phrase appearing.
- Medium-High: document checks use substring filename matching.
- Medium-High: file-supplied `tenant_id` is trusted with `trusted_tenant=True`.
- Medium: filename resolution is capped and ambiguity-prone.
- Medium: term hits use raw substring matching.
- Medium: query rows are not schema-validated.
- Low-Medium: short focus terms are dropped.
- Low: the helper can read arbitrary caller-supplied paths if it is reused outside the hardened wrapper.
- `Kant` was reassigned to `reviewer.py`.
- `validation.py` review completed with these findings:
- High: OCR noise scoring is ASCII-centric and can reject legitimate Unicode-rich text as corruption.
- High: very short but asset-linked captions or labels are still rejected or reviewed, which drops visual evidence from retrieval and KG because only accepted chunks flow downstream.
- Medium: visual support is a hard binary threshold at `0.75`.
- Medium: direct `int()` and `float()` conversion over free-form metadata can raise and abort validation.
- Medium: the validator trusts mutable metadata fields for accept or reject decisions.
- Medium: the repetition heuristic is too naive and will over-review normal prose.
- Medium: structured list chunks are judged like prose.
- Low: unknown document classes are penalized.
- Low: heading-only chunks are hard-rejected instead of being retained as low-weight anchors or review items.
- The reviewer was reassigned to `tools/reingest_all_pdfs.py`.
- The deep failure scan report was written to `E:\n8n to python\docs\deep-failure-scan-20260330.md`.
- It consolidates all per-file findings from the subagent sweep plus the local fallback review of `tools/wait_then_rerun_with_live_ontology.py`.
- Top themes: loose outbound trust boundaries, incomplete tenant and identity binding, destructive workflows that are not staged, multimodal provenance that is still too permissive, and evaluation paths that over-certify broken states.
- Reviewed the latest reviewer-agent recommendations against the current code.
- Agreement summary: claim-level verification, session memory compaction, evidence-backed multimodal linking, richer judged evaluation, and router-result caching are valid next-step improvements.
- Partial agreement: query-pattern feedback should either be made operational in routing/review or the UI wording should be downgraded so it does not imply active learning.
- Conditional/deferred: cookie-based browser sessions matter if the app moves beyond local/internal use; for current localhost-only operation it is a hardening step, not the top product-quality blocker.
- Also agree that `candidate_events` should either become first-class persisted KG records or be removed from the extraction contract until the event layer is actually implemented.
- Began fixing the deep failure scan in priority order.
- Outbound trust boundaries hardened:
- `settings.py` now validates all outbound model base URLs, resolves workspace paths, and exposes allowlist/private-host controls.
- `agent_runtime.py` now validates runtime-config provider values and base URLs before they can be persisted.
- `embedding.py` now validates embedding response cardinality, ordering, and vector shape before accepting vectors.
- `query_router.py` no longer borrows KG or embedding credentials, validates cached routes, sanitizes router error fallbacks, and stops caching low-signal query signatures.
- `reviewer.py` now requires explicit `REVIEW_API_KEY`, uses structured JSON prompt input, and validates reviewer output locally.
- `kg.py` now requires explicit `KG_API_KEY` and uses only `KG_BASE_URL`.
- `multimodal.py` now requires explicit `VISION_API_KEY` and uses only `VISION_BASE_URL`.
- `agent.py` answer, claim-verifier, memory, and profile paths no longer borrow KG or embedding secrets; claim-verifier failure output is sanitized.
- Data exposure reduced:
- `repository.py` now redacts dangerous columns in the generic admin relation browser, broadens secret-key redaction, stores redacted traces instead of raw trace payloads, avoids exposing session token hashes from `get_agent_session`, and redacts query detail payloads on read.
- `api.py` now reflects the real credential chains in system-config payloads, exposes outbound-host controls in the admin system config, and constrains asset image serving to the page-assets root instead of trusting DB paths.
- Destructive workflow safety improved:
- `cli.py` now makes maintenance modes and input sources mutually exclusive, prevents bare global reset, and requires explicit `--all --confirm-reset-all`.
- `reingest_all_pdfs.py` now validates manifest entries and root containment, preserves committed ingest results on gate failures, removes the unsafe `no_indexed_assets` gate, and reports aborted runs distinctly.
- `wait_then_rerun_with_live_ontology.py` now uses a lock file, atomic state writes, append logging, and ingest wait timeouts/stale-progress detection.
- `service.py` now enforces containment for page-asset deletion, stops swallowing deletion errors, verifies stored PDF replayability before rebuild reset, and only opens multimodal PDFs from allowed workspace roots.
- All touched files compiled successfully with `python -m py_compile`.
- Live runtime validation was not completed in this pass because the API container was not running at the time of the changes.
- Continued the next hardening batch with workspace-skill guidance (`codex-orchestration`, `api-security-best-practices`).
- Tenant and identity binding tightened:
- `repository.py` now supports tenant-scoped session/profile/query detail lookups, tenant-scoped token verification, and session/profile token age based on dedicated issued-at timestamps instead of generic `updated_at`.
- `agent.py` now uses tenant-scoped session/profile/message/memory lookups for the public agent flow.
- `api.py` public agent endpoints now verify session/profile cookies against the public tenant, and public feedback is bound to the active session instead of accepting any bare `query_run_id`.
- Destructive service operations tightened further:
- `repository.py` now exposes advisory-lock helpers.
- `service.py` now serializes existing-document ingest, rebuild, revalidate, reindex, delete, reset, and KG replay through advisory locks.
- Existing-document rebuild/re-ingest failures now leave the document row in place and reset pipeline state instead of deleting the document object entirely.
- Browser/admin hardening continued:
- `admin_ui.py` now stores the admin token in `sessionStorage` instead of `localStorage`.
- `admin_ui.py` status badge rendering now sanitizes class names and escapes displayed values.
- Fatal boot error rendering now uses DOM text insertion instead of HTML insertion.
- Runtime secret storage hardened:
- Added `RUNTIME_SECRET_ENCRYPTION_KEY` to settings and admin system-config secret handling.
- Added `cryptography` dependency in `pyproject.toml`.
- `repository.py` now encrypts `agent_runtime_secrets.api_key_override` before persistence and decrypts on read, while still supporting legacy plaintext reads.
- `.env` and `.env.example` were updated for the new runtime secret encryption key.
- Multimodal trust tightened again:
- `service.py` no longer indexes page-image assets only because vision ran; page-image indexing now requires meaningful OCR/description or substantive terms.
- `service.py` removed the weak single-chunk labeled-overlap fallback for chunk-to-asset links, leaving only stronger evidence-backed link paths.
- Re-ran `python -m py_compile` over `settings.py`, `repository.py`, `agent.py`, `api.py`, `service.py`, and `admin_ui.py`; the compile pass succeeded.
- Updated the cross-agent coordination thread in `E:\n8n to python\codereview agent communication\chat.txt` with the new patch status and requested a fresh review pass.
- Continued the autonomous hardening pass from the reviewer feedback and worker subreviews.
- Public agent surface tightened again:
- `api.py` now rejects non-public `tenant_id` values on `/agent/query` and `/agent/chat`, forces the public tenant into the service call, and reloads updated public profiles with an explicit tenant filter.
- Grounding quality tightened:
- `agent.py` claim-verifier failures now fail closed instead of silently passing via the old lexical pass.
- `agent.py` verifier results now require non-empty claim coverage, enough local clause coverage, at least one supported claim, and zero unsupported claims to pass.
- `agent.py` lexical fallback is now claim-level instead of answer-level bag-of-words, and `_grounding_terms` now keeps short domain/numeric tokens such as `AFB`, `SHB`, `24h`, `10%`, and `3:1`.
- `agent.py` review-state derivation now forces `needs_review` for lexical fallback, verifier errors, unsupported claims, and weak grounding support.
- Memory/profile quality tightened:
- `agent.py` prompt profile injection now whitelists summary fields and no longer falls back to dumping raw profile-row fields into `user_profile`.
- `agent.py` session/profile refresh now reload the saved rows with explicit tenant filters.
- `agent.py` fallback session memory now stores evidence-backed facts from cited chunks/assets instead of storing assistant answer sentences as stable facts.
- `agent.py` fallback session goals can roll forward when the latest user turn diverges from the previous goal.
- `agent.py` fallback preference/topic merging is more recency-aware, and `_normalize_string_list(limit=0)` now correctly returns an empty list.
- `agent.py` fallback profile no longer promotes transient session scope, open threads, or session document ids into the long-lived browser profile.
- Multimodal trust tightened again:
- `service.py` now resolves the page-asset root against the workspace root consistently.
- `service.py` page-asset cleanup now uses a resolved root/target containment check.
- `service.py` asset indexing, asset linking, and KG augmentation now rely on deterministic OCR/non-generic-label anchors instead of model-generated visual descriptions alone.
- `agent.py` asset grounding rows and public asset citations now use trusted OCR/label text only, and prompt asset payloads distinguish deterministic text from generated descriptions.
- Ran `python -m py_compile` over `src/bee_ingestion/agent.py`, `src/bee_ingestion/api.py`, and `src/bee_ingestion/service.py`; the compile pass succeeded.
- Read the latest shared-chat updates and answered the new `frontend designer` auth/product questions in `E:\n8n to python\codereview agent communication\chat.txt`.
- Backend/product decisions recorded there:
- public product shell becomes `login -> chat` only
- public web auth should use dedicated auth APIs with secure first-party session cookies
- guest fallback is not part of the MVP public product path
- identity/membership should live in a separate auth schema from corpus/runtime data
- MVP user access should be admin-provisioned, not self-service signup
- Upgraded `tools/code_review_chat_watcher.py` from a reviewer-only poller into a bidirectional coordinator:
- it now debounces the shared chat file, takes a lock, tracks builder/reviewer/external trigger hashes separately, and records visible `(watcher)` failure notes when a builder follow-up is needed but no builder worker can be launched.
- added loop support in `tools/code_review_chat_watcher.py` and `tools/run_code_review_watcher.ps1` so the watcher can stay alive and poll continuously while the machine is on.
- validated the coordinator path by running the watcher once:
- it detected the latest `(agent builder)` update and wrote the expected `(code reviewer)` failure notice because the review backend still hits `HTTP 429`.
- it then detected the newer `(code reviewer)` update as a builder trigger and wrote a `(watcher)` operational note because `AGENT_BUILDER_TRIGGER_COMMAND` is not configured.
- re-ran `python -m py_compile` for `tools/code_review_chat_watcher.py`; the compile pass succeeded.
- Removed the intrusive shared-chat watcher tooling at the user's request:
- deleted the scheduled task `BeeCodeReviewWatcher15m`
- deleted `tools/code_review_chat_watcher.py`
- deleted `tools/run_code_review_watcher.ps1`
- removed the related watcher state artifacts from `codereview agent communication`, leaving only `chat.txt`
## 2026-04-02 - Sensor user-ownership and agent telemetry integration

- Added auth-user-owned sensor storage and public sensor APIs.
- Bound agent profiles to authenticated users and passed `auth_user_id` through the public agent path.
- Added sensor evidence into the agent prompt/citation/fallback path.
- Added bounded and idempotent sensor reading ingest using `reading_hash`.
- Added `sensor_readings` DB-level validity check requiring numeric or text value.
- Added sensor-only answer mode so telemetry questions can bypass vector embeddings.
- Added sensor cleanup on auth user disable/delete.
- Validated live:
  - authenticated sensor create/read
  - duplicate ingest dedupe
  - sensor context payload
  - sensor-backed agent answer fallback with reading provenance
  - cross-user agent session reuse rejection

## 2026-04-05 - Sensor place/hive ownership pass

- Added first-class `user_places` / `user_hives` models and repository APIs on disk.
- Extended `user_sensors` ownership to link a sensor to:
  - authenticated user
  - place
  - hive
- Added API contracts for:
  - `GET/POST /places`
  - `GET /places/{place_id}`
  - `GET /places/{place_id}/hives`
  - `GET/POST /hives`
  - `GET /hives/{hive_id}`
  - updated `POST /sensors` to accept `place_id` and `hive_id`
- Tightened route handling:
  - repository `ValueError` now maps to `400` on the new place/hive/sensor routes
  - added explicit `sensor.read` / `sensor.write` permissions in auth defaults
  - sensor/place/hive routes now require sensor permissions, not only `chat.use`
- Tightened repository integrity:
  - owner-scoped joins for place/hive lookups on sensor reads
  - canonical `resolved_place_name` / `resolved_hive_name` now override stale denormalized sensor labels in read paths
  - `build_user_sensor_context()` now uses per-sensor/per-metric SQL windowing instead of a single global recent-reading slice
- Tightened agent telemetry behavior:
  - sensor context is no longer auto-included for generic authenticated questions
  - sensor context now requires telemetry/place/hive intent
- Tightened schema on disk:
  - removed contradictory simple place/hive FK definitions in favor of owner-scoped constraints
  - added `idx_user_sensors_owner_ref`
  - added owner-scoped `fk_sensor_readings_owned_sensor`
  - added sync triggers so place/hive updates propagate to linked sensors
- Compile checks passed for:
  - `auth_store.py`
  - `repository.py`
  - `api.py`
  - `agent.py`
- Live blocker:
  - Docker Desktop is open but `com.docker.service` remains stopped from this shell
  - schema apply, API restart, and live place/hive/sensor smoke tests are still pending

## 2026-04-07 - Live sensor topology rollout

- Applied `sql/schema.sql` to the running `n8ntopython-postgres-1` instance with `psql -f`.
- Restarted `n8ntopython-api-1` to load:
  - fixed `_require_authenticated_sensor_user()` recursion
  - explicit `query_mode` handling (`auto/general/sensor`)
  - sensor-specific system prompt support
- Verified live DB objects:
  - `user_places`
  - `user_hives`
  - `user_sensors`
  - `sensor_readings`
  - triggers:
    - `trg_enforce_user_sensor_topology`
    - `trg_sync_user_sensors_from_place`
    - `trg_sync_user_sensors_from_hive`
- Ran full live smoke path:
  - created auth user through admin API
  - logged in through public auth
  - created place `6`
  - created hive `5` under place `6`
  - created sensor `1` bound to hive `5` / place `6`
  - ingested 4 readings
  - verified `/sensors/context`
  - verified `/agent/query` in `sensor` mode returned sensor-backed answer with sensor citation
  - verified `/agent/query` in `general` mode abstained and did not use sensor citations
- Rebuilt `frontend/dist` so the React app now exposes the new answer-mode selector.
- Patched admin replay to carry the original `auth_user_id`, so sensor-mode replays can reproduce sensor context instead of silently dropping it.

## 2026-04-07 - Final ingest launch
- Captured pre-reset overview to data/logs/final-ingest-pre-overview-20260407.json
- Captured DB restore point to data/backups/bee_ingestion_pre_final_ingest_20260407.dump
- Reset corpus/KG/agent-history state via /admin/api/reset
- Wrote manifest data/reingest-manifest.json for the 13 root PDFs

## 2026-04-10 - Ingestion-only hardening and fresh relaunch
- Patched ingestion safety in `src/bee_ingestion/service.py`, `repository.py`, `pipeline.py`, `multimodal.py`, `api.py`, `frontend/src/features/chat/PublicChatPage.tsx`, and `tools/reingest_all_pdfs.py`.
- Fixed:
  - bad helper call in chunk/asset relinking
  - stale-progress reporting and heartbeat visibility in the batch runner
  - unsafe terminal stage ordering between `kg_validated` and `indexed`
  - `page_area` NameError in PDF multimodal extraction
  - job lease expiry drift by adding active lease renewal plus fatal lease-loss checks before writes
  - fresh-ingest failure visibility by preserving failed document rows instead of deleting them immediately
  - KG review documents being published to Chroma
  - public sensor-mode authorization and session/profile bootstrap permission mismatches
- Rebuilt the API image twice to ensure the running container imported `/app/src/...` rather than the stale wheel in `site-packages`.
- Reset live ingest scope again while preserving auth and sensor topology:
  - Postgres corpus/KG/query-run tables truncated to zero
  - Chroma chunk/assets collections recreated at zero
  - `data/page_assets` and `data/logs/reingest-*` cleared
- Relaunched the batch on a clean baseline.
- Current live state:
  - batch status `running`
  - current file `4-H-1059-W.pdf`
  - `ingestion_jobs.status = processing`
  - lease is actively renewing in Postgres
  - page-assets are being rendered forward on disk (`page-0006` observed on the new document root)
- Remaining ingestion-specific gap after this pass:
  - the first persisted stage boundary still occurs only after the full multimodal extraction pass, so DB stage counts remain at zero while long PDFs are still being processed even though the run is genuinely alive.

## 2026-04-13 - Ingest timing, revalidation, and Chroma admin fixes
- Patched live ingest tracking so the batch now records real subphase progress in `data/logs/reingest-progress.json`:
  - explicit `phase`
  - `phase_detail`
  - `phase_metrics`
  - `last_progress_at`
  - `current_document_id`
  - `current_job_id`
- Patched `src/bee_ingestion/service.py` to use split stage timing with:
  - true stage start rows (`status=running`, `finished_at=NULL`)
  - true stage finish timestamps
  - per-phase progress emission for:
    - preparing
    - parsing
    - chunking
    - validating
    - KG extraction
    - embedding / indexing
- Added post-call lease checks before KG persistence and before vector writes so long provider calls cannot publish after lease loss.
- Made `revalidate_document()` non-destructive-first:
  - compute validations and KG results first
  - embed accepted chunks first
  - replace document KG transactionally only after the full replacement set is ready
- Patched `src/bee_ingestion/repository.py` stage ordering so running rows sort ahead of completed rows in admin views.
- Patched the Chroma admin contract:
  - collection metadata is now explicitly set for chunk and asset collections
  - `/admin/api/chroma/collections` returns actual collection metadata
  - Chroma records in the admin UI now fall back to `item.id` as the canonical source id
  - empty/error states in the Chroma records table are now explicit instead of rendering as blank rows
- Restarted the API container and reset ingest-related state again without starting a new ingest:
  - `documents=0`
  - `chunks=0`
  - `pages=0`
  - `page_assets=0`
  - `kg_assertions=0`
  - `ingestion_jobs=0`
  - Chroma chunk/assets collections both reset to zero with non-empty collection metadata
  - `data/page_assets` cleared
  - `data/logs/reingest-progress.json` and `reingest-summary.json` removed

## 2026-04-13 - Fresh ingest staged publish

- Added staged/ready publish-state handling for Chroma chunk and asset records in `src/bee_ingestion/chroma_store.py`.
- Fresh ingest now publishes vectors in two steps: staged write, then ready promotion in `src/bee_ingestion/service.py`.
- Retrieval now filters to `publish_state=ready`, so partially published fresh-ingest vectors stay invisible to the agent.
- Added admin stale-progress coercion in `src/bee_ingestion/api.py`.
- Extended `tests/test_service.py` to the current stage and revalidation contract.
- Validation:
  - `docker exec n8ntopython-api-1 python -m pytest /app/tests/test_service.py -q` -> `13 passed`
  - staged-vector smoke test: staged search returns `0`, ready search returns `1`
  - fresh-ingest smoke test: stored Chroma records end in `publish_state=ready`
  - final cleanup check: Postgres and Chroma back to zero

## 2026-04-13 - Review path staged publish

- Hardened chunk review accept/reject in `src/bee_ingestion/service.py`.
- Accept now follows `staged vector -> DB validation/metadata -> KG persist -> ready promotion`.
- Reject/review removal now follows `hide live vector -> DB/KG cleanup -> delete vector`, with rollback to the original review state if the DB step fails.
- Extended `tests/test_service.py` to cover staged promotion and rollback on failed review acceptance.
- Validation:
  - `docker exec n8ntopython-api-1 python -m pytest /app/tests/test_service.py -q -k "review_chunk or auto_review_chunks or revalidate_document"` -> `7 passed`
  - `docker exec n8ntopython-api-1 python -m pytest /app/tests/test_service.py -q` -> `14 passed`
  - API restarted and `/health` returned `{"status":"ok"}`
# 2026-04-15 - React admin migration phase 1

- expanded `frontend/src/lib/api/admin.ts` to cover legacy admin operational endpoints
- added migrated React admin operational sections in `frontend/src/features/admin/AdminExtendedSections.tsx`
- wired new sections into `frontend/src/features/admin/AdminPage.tsx`
- documented the migration plan, implemented scope, and parity gaps in `docs/admin-react-migration.md`
- validated with `npm run build` in `frontend`

# 2026-04-15 - React admin migration completion

- restored and completed `frontend/src/features/admin/AdminExtendedSections.tsx`
- added React admin coverage for:
  - corpus
  - chunks
  - knowledge graph
  - chroma
  - agent workbench
  - operations
- added React surfaces for direct editor flows and manual ingest helpers using existing backend endpoints
- changed `src/bee_ingestion/api.py` so `/admin` now redirects to the React admin at `/app/control`
- preserved the legacy admin as fallback at `/admin/legacy`
- validation:
  - `npm run build` in `frontend`
  - `python -m py_compile E:\n8n to python\src\bee_ingestion\api.py`

# 2026-04-15 - Retrieval hardening slice 1

- updated `src/bee_ingestion/agent.py` retrieval planning:
  - normal text-first queries no longer always issue asset-vector searches
  - `procedure` and `explanation` retrieval plans now enable KG augmentation
  - `inspect_retrieval()` now accepts `auth_user_id` so sensor-aware inspection is possible
- updated `src/bee_ingestion/repository.py` retrieval support:
  - chunk record loaders now expose `kg_assertion_count`
  - KG assertion loading now supports per-chunk quotas
  - KG evidence loading now supports per-assertion quotas
- updated chunk compression scoring in `src/bee_ingestion/agent.py` so KG bonus is driven by real KG presence instead of generic metadata presence
- refreshed `tests/test_agent.py` to match the current agent contract and cover the new retrieval behavior
- validation:
  - `docker exec n8ntopython-api-1 python -m pytest /app/tests/test_agent.py -q` -> `9 passed`
  - `python -m py_compile` for `agent.py`, `repository.py`, and `tests/test_agent.py`

# 2026-04-15 - Retrieval hardening slice 2

- kept KG evidence in the answer prompt, but made it first-class in `src/bee_ingestion/agent.py`:
  - added `used_evidence_ids` to the answer schema and response coercion path
  - added `allowed_evidence_ids` to the prompt contract
  - added KG evidence citations and grounding support so prompt evidence is accountable instead of prompt-only context
  - propagated KG evidence into final response metadata and persisted query-source selection
- improved KG entity search ranking in `src/bee_ingestion/repository.py`:
  - ranking now uses weighted lexical relevance from canonical names, entity types, object literals, and KG evidence excerpts
  - entity search now returns `relevance_score` and orders by it before popularity
- extended `tests/test_agent.py` with evidence-citation coverage
- validation:
  - `docker exec n8ntopython-api-1 python -m pytest /app/tests/test_agent.py -q` -> `11 passed`
  - `python -m py_compile` for `agent.py`, `repository.py`, and `tests/test_agent.py`
  - live repository smoke on the ingested corpus returned ranked KG entities with non-zero `relevance_score`

# 2026-04-15 - Retrieval hardening slice 3

- added hybrid lexical+dense chunk retrieval in `src/bee_ingestion/agent.py` and `src/bee_ingestion/repository.py`
  - chunk candidates now merge Chroma dense hits with Postgres lexical hits before reranking
  - if embeddings fail but lexical candidates exist, retrieval continues in lexical-only mode instead of failing the whole question
- added hybrid lexical+dense asset retrieval in `src/bee_ingestion/agent.py` and `src/bee_ingestion/repository.py`
  - asset candidates now merge Chroma asset hits with Postgres lexical asset hits before reranking
  - source payloads now preserve match-source information for dense vs lexical diagnostics
- extended `tests/test_agent.py` with:
  - lexical chunk fallback coverage
  - embedding-failure lexical fallback coverage
  - lexical asset candidate coverage for visual retrieval
- validation:
  - `docker exec n8ntopython-api-1 python -m pytest /app/tests/test_agent.py -q` -> `14 passed`
  - `python -m py_compile` for `agent.py`, `repository.py`, and `tests/test_agent.py`
  - live smoke on the corpus:
    - lexical-only chunk retrieval worked with forced embedding failure
    - lexical asset search returned real asset ids from the ingested corpus

# 2026-04-15 - Retrieval hardening slice 4

- added persistent exact-query embedding cache across requests:
  - new runtime flags in `src/bee_ingestion/settings.py` and `src/bee_ingestion/agent_runtime.py`
  - new Postgres cache table in `sql/schema.sql`
  - new repository cache methods in `src/bee_ingestion/repository.py`
  - `src/bee_ingestion/agent.py` now reuses cached query embeddings before calling the embedder
- kept this separate from router-pattern caching:
  - router cache stays pattern-level
  - embedding cache is exact-normalized-query keyed and model/base-url scoped
- extended `tests/test_agent.py` with embedding-cache reuse coverage
- validation:
  - `docker exec n8ntopython-api-1 python -m pytest /app/tests/test_agent.py -q` -> `15 passed`
  - `python -m py_compile` for `agent.py`, `repository.py`, `agent_runtime.py`, `settings.py`, and `tests/test_agent.py`
  - live smoke:
    - repeated `inspect_retrieval()` reused the cached embedding
    - cache row existed in Postgres with `dimensions=3072` and `cache_hits=1`
