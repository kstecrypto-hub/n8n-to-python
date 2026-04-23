# Deep Failure Scan - 2026-03-30

Scope: read-only failure review over the codebase, emphasizing code-rooted failure modes, ingestion reliability, agent safety, and API/security boundaries.

Skills used:
- `E:\n8n to python\skills\agentic-ai-builder\SKILL.md`
- `E:\n8n to python\skills\api-security-best-practices\SKILL.md`

Method:
- One subagent review per code file where the agent system returned a usable result.
- Local fallback review for `tools/wait_then_rerun_with_live_ontology.py` after repeated empty agent completions and one usage-limit failure.

## Files With No Findings

- `E:\n8n to python\src\bee_ingestion\__init__.py`

## Findings By File

### `E:\n8n to python\src\bee_ingestion\admin_ui.py`

- High: DOM XSS risk in status badge rendering via dynamic `innerHTML`.
- High: `escapeHtml()` is incomplete for quoted attributes and `data-*` injection contexts.
- Medium: admin token is persisted in `localStorage`.
- Medium: API console can send `X-Admin-Token` to arbitrary operator-supplied paths/URLs.
- Medium: destructive UI actions lack strong confirmation/versioning.
- Low: auth/outage handling can leave stale privileged state visible and blur `401` vs `503`.

### `E:\n8n to python\src\bee_ingestion\agent.py`

- High: profile-token reuse is not tenant-bound.
- High: runtime-configurable base URLs can exfiltrate prompts, evidence, memory, and profile summaries.
- Medium-High: claim verifier failure falls back to lexical grounding and can leak verifier exception text.
- Medium: prompt-derived session memory/profile state can persist prompt injection.
- Medium: trusted bypass flags and raw retrieval-inspection internals are too easy to misuse.
- Medium: no hard cap on question length or `document_ids` count before expensive work.

### `E:\n8n to python\src\bee_ingestion\agent_eval.py`

- High: filename-scoped eval can silently widen to the whole corpus.
- Medium: per-row `tenant_id` override plus `trusted_tenant=True` allows wrong-tenant eval.
- Medium: multi-document expectations can pass on partial coverage.
- Medium: `needs_review` answers can still pass.
- Medium: abstention checks are too weak and can pass degraded fallback abstentions.

### `E:\n8n to python\src\bee_ingestion\agent_runtime.py`

- High: all model base URLs are arbitrary unvalidated strings.
- Medium: provider fields are free-form and can silently disable safeguards.
- Medium: raw `int()`/`float()` coercions can hard-fail config parsing.
- Low: unknown config keys survive coercion and can be persisted silently.

### `E:\n8n to python\src\bee_ingestion\agent_ui.py`

- High: session reset can claim success even when server reset fails.
- High: the UI can silently resume hidden server-side context without rehydrating prior turns.
- Medium: arbitrary image URLs are rendered directly in the browser.
- Medium: backend detail strings are exposed directly to users.
- Medium: bootstrap failures are swallowed while the UI reports a healthy ready state.

### `E:\n8n to python\src\bee_ingestion\api.py`

- High: image endpoints trust DB-backed asset paths and can become arbitrary local file serving.
- High: the control plane is guarded by one shared admin token with no operator identity/scope.
- Medium: `/admin/api/reset` is one-call destructive with no staged confirmation or rollback.
- Medium: generic admin editing can desynchronize Postgres and Chroma.
- Medium: public feedback writes are not ownership-bound to the caller.
- Medium: public agent routes lack API-layer throttling and input-size caps.
- Medium: public endpoints reflect raw agent error text.

### `E:\n8n to python\src\bee_ingestion\chroma_store.py`

- High: destructive collection reset can leave chunk and asset collections in mixed state.
- Medium-High: tenant scope is optional at the vector-store boundary.
- Medium: `get_or_create_collection()` masks integrity drift as empty results.
- Medium: batch upserts do not pre-assert cardinality or compensate mid-batch failures.

### `E:\n8n to python\src\bee_ingestion\chunking.py`

- High: provenance span calculation can move backward or misbind repeated text.
- High: oversized atomic units can become oversized chunks with no hard split path.
- Medium-High: raw text can forge page sentinels and poison citations.
- Medium: substring front/back-matter heuristics can relabel ordinary body text.
- Medium: merged headings can carry wrong page provenance.

### `E:\n8n to python\src\bee_ingestion\cli.py`

- High: bare `--reset-data` can wipe the entire dataset if `--document-id` is omitted.
- Medium: incompatible maintenance mode flags are silently resolved by first-match order.
- Medium: `--file` and `--text` are not mutually exclusive and can create inconsistent identity.
- Medium: tenant scoping defaults are unsafe and maintenance paths are unscoped.

### `E:\n8n to python\src\bee_ingestion\embedding.py`

- High: embedding base URL is unvalidated and can exfiltrate text with bearer credentials.
- High: embedding response shape/order/cardinality is trusted without local checks.
- Medium: partial batch failure reporting is too opaque for efficient recovery.
- Medium: batching has no byte/token ceilings beyond item count.

### `E:\n8n to python\src\bee_ingestion\kg.py`

- High: KG extraction can fall back to embedding credentials/base URL.
- High: evidence spans/excerpts are not verified against source chunk text.
- High: public extraction APIs can return normalized but unvalidated graph candidates.
- Medium: duplicate mention/relation/evidence IDs are not rejected.
- Medium: source text is concatenated directly into instruction-carrying prompts.

### `E:\n8n to python\src\bee_ingestion\models.py`

- High: raw filesystem paths are stored on page/asset models.
- High: `PageAsset.search_text` assumes typed metadata that is actually unvalidated JSON.
- Medium: `Chunk.metadata` is an untyped bag even though downstream logic expects stable numeric/list fields.
- Medium: `ChunkAssetLink.confidence` is unbounded and unchecked for finite `[0,1]`.

### `E:\n8n to python\src\bee_ingestion\multimodal.py`

- High: model OCR is promoted into canonical merged page text too aggressively.
- High: vision endpoint/key fallback crosses subsystem boundaries and lacks host validation.
- Medium-High: full-page visuals can disappear entirely from assets on text-heavy pages.
- Medium-High: some render/file/parse failures still abort whole documents.
- Medium: there are no pixel/byte ceilings before render/base64 upload.

### `E:\n8n to python\src\bee_ingestion\pipeline.py`

- High: `kg_validated` is not a durable job status, so KG crashes are indistinguishable from safe post-index state.
- Medium: stage validation does not check coherent stage/outcome/job-transition tuples.
- Low: quarantine is not represented as a distinct stage/event.
- Low: job creation/claim is not represented in the immutable stage log.

### `E:\n8n to python\src\bee_ingestion\query_router.py`

- High: low-signal query signatures can poison route-cache reuse across unrelated queries.
- High: cached routes are returned without revalidation.
- Medium-High: invalid `document_spread` narrows unsafely to `single`.
- Medium-High: provider handling disables the router for non-exact `openai` values such as `auto`.
- Medium: broad exception handling hides internal errors behind `fallback:error` and leaks raw text.
- Medium: structured content responses are not parsed.
- Medium: router input has no hard size cap.
- Low-Medium: router credential/base-url fallback crosses subsystem boundaries.
- Low: malformed cache timestamps can bypass expiry.

### `E:\n8n to python\src\bee_ingestion\repository.py`

- High: generic admin relation browsing is an unrestricted data dump over public tables.
- High: runtime API key overrides are stored plaintext and broadly retrievable.
- High: agent traces and source payloads are stored with sanitization instead of real redaction.
- High: session/query writes are not consistently tenant-bound.
- High: agent read APIs are broadly unscoped and can expose token hashes and full traces.
- High: source-text getters expose raw content by bare IDs with no tenant predicate.
- Medium: profile/session getters and mutators operate on raw UUIDs without tenant checks.
- Medium: session creation/profile attach do not verify tenant compatibility.
- Medium: provenance/link writes trust caller-supplied IDs without ownership checks.
- Medium: document `tenant_id` edits do not migrate child rows.
- Medium: KG entities are upserted globally across tenants.
- Medium: delete cleanup and KG orphan pruning are split across transactions.
- Medium: token expiry uses mutable `updated_at` and plain equality checks.
- Low: sensitive-key redaction is too narrow.
- Low: several list helpers default to unscoped enumeration.
- Low: multiple whole-database destructive helpers have overlapping blast radii.
- Low: `delete_document` is defined twice.

### `E:\n8n to python\src\bee_ingestion\retrieval_eval.py`

- High: filename-scoped rows fail open to whole-tenant retrieval.
- High: under-specified rows pass too easily.
- High: linkage metadata is scored as if it were evidence text.
- Medium-High: asset expectations are bundle-level and too weak.
- Medium-High: focus expectations can pass via coarse term overlap.
- Medium-High: document checks use substring filename matching.
- Medium-High: file-supplied `tenant_id` is trusted with `trusted_tenant=True`.
- Medium: filename resolution is capped and ambiguity-prone.
- Medium: term hits use raw substring matching.
- Medium: query rows are not schema-validated.
- Low-Medium: short focus terms are dropped.
- Low: helper can read arbitrary caller-supplied paths if reused outside hardened wrappers.

### `E:\n8n to python\src\bee_ingestion\reviewer.py`

- High: review config fails open to KG/embedding credentials and base URLs.
- High: chunk text and metadata are injected raw into the review prompt.
- Medium-High: review is anchored to upstream heuristic output.
- Medium-High: reviewer output is not locally validated against enums/required fields.
- Medium: malformed confidence is silently coerced to `0.5`.
- Medium: review prompt fields have no local size caps.
- Low: raw provider/refusal/parse details leak in exception messages.

### `E:\n8n to python\src\bee_ingestion\service.py`

- High: rebuild/reingest of existing documents deletes last-known-good derived state before the new run is proven.
- High: mutating document operations have no shared lock/lease and can race.
- High: delete/reset paths destroy derived state before DB rows.
- High: page-asset file cleanup can path-escape and swallows deletion failures.
- Medium-High: reindex can desynchronize chunk-asset links and metadata for non-accepted chunks.
- Medium-High: accept paths can partially commit validation/indexing before KG/review provenance finishes.
- Medium-High: `revalidate(..., rerun_kg=True)` can leave partially rebuilt or empty KG.
- Medium-High: `reprocess_kg()` can stop halfway with no durable batch failure marker.
- Medium: rebuild can silently downgrade multimodal docs to text-only if the original PDF is missing.
- Medium: replay trusts persisted PDF paths without revalidating containment.
- Medium: cleanup paths can mask the primary ingest error.
- Medium: manual accept/reject changes lack explicit review-run provenance.
- Low-Medium: relink-without-KG-rerun can leave KG pointing at stale visual-link state.
- Low: derived `quality_flags` can accumulate stale reasons.
- Low: ontology can hot-reload mid-batch and mix ontology versions within one operation.

### `E:\n8n to python\src\bee_ingestion\settings.py`

- High: all outbound model base URLs are unvalidated.
- High: vision defaults on and silently widens egress to raw image upload.
- High: agent/router/memory/profile/verifier stacks default on and inherit broader secrets/base URLs.
- High: KG extraction defaults to `auto` and can silently enable KG model calls from embedding credentials.
- Medium: review can auto-enable from broader keys.
- Medium: public agent defaults to tenant `shared`.
- Medium: paths are cwd-relative and frozen in an import-time singleton.
- Medium: critical lease/threshold/token-age settings lack bounds validation.
- Low: default Postgres credentials are predictable.
- Low: dummy embeddings can make the system appear healthy while retrieval is fake.

### `E:\n8n to python\src\bee_ingestion\validation.py`

- High: OCR noise scoring is ASCII-centric and can reject legitimate Unicode-rich text.
- High: short but asset-linked captions/labels can still be rejected or reviewed, removing visual evidence downstream.
- Medium: visual support is a hard binary threshold.
- Medium: metadata coercions can raise and abort validation.
- Medium: validator trusts mutable metadata fields as authoritative.
- Medium: repetition heuristic is too naive.
- Medium: structured list chunks are judged like prose.
- Low: unknown document classes are penalized.
- Low: heading-only chunks are hard-rejected instead of retained as low-weight anchors or review items.

### `E:\n8n to python\tools\reingest_all_pdfs.py`

- High: manifest entries are resolved without schema/root-containment checks.
- High: post-ingest quality gates can convert a committed rebuild into an `error` record after state mutation.
- High: `page_assets > 0 and indexed_assets <= 0` is an unsafe success gate.
- Medium: small-document success gates have blind spots.
- Medium: manifest coverage is distorted by dropped invalid entries and duplicates.
- Medium: the runner is not resumable.
- Medium: fail-fast runs are reported as `completed_with_errors` instead of a clear aborted state.
- Medium: tenant and document class are hard-coded.
- Low: corpus root is hard-coded with no layout validation.

### `E:\n8n to python\tools\wait_then_rerun_with_live_ontology.py`

- High: there is no single-run lock. Multiple watcher instances can race, each waiting on the same progress file and then performing duplicate reset/restart/relaunch cycles.
- High: the destructive reset happens before the new run is proven. If Postgres truncate succeeds but Chroma reset, filesystem cleanup, API restart, or relaunch fails, the corpus is wiped and no replacement run exists.
- Medium-High: `_wait_for_current_ingest()` has no timeout or dead-run detection. If the detached ingest dies without publishing a terminal status, or the progress file stays malformed/missing, the watcher loops forever.
- Medium: the watcher declares `completed` as soon as it sees fresh `running` progress from the new ingest. It does not verify the relaunched job stays alive or survives the first file.
- Medium: state and log writes are non-atomic read/overwrite operations, so concurrent watcher runs or readers can see truncated JSON or clobbered log lines.
- Medium: runtime targeting is hard-coded (`ROOT`, container names, health URL) and not validated against the actual workspace/stack, so the script can reset or restart the wrong environment.

## Highest-Impact Themes

1. Outbound trust boundaries are too loose.
   - Multiple subsystems accept arbitrary base URLs and silently fall back across credential domains.
2. Tenant and identity binding is incomplete.
   - Sessions, profiles, traces, source reads, evals, and vector lookups are too often caller-asserted.
3. Destructive operations are not staged.
   - Reset, rebuild, delete, reindex, revalidate, and watcher flows frequently destroy current state before replacement is proven.
4. Multimodal provenance remains too permissive.
   - OCR promotion, weak link assumptions, and disappearing full-page visuals still distort evidence quality.
5. Evaluation paths still over-certify.
   - Retrieval/agent eval defaults allow broken or under-specified cases to pass.
