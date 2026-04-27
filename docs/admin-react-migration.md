# React Admin Migration

## Decision

The long-term admin target is the React frontend in:

- `E:\n8n to python\frontend\src\features\admin\AdminPage.tsx`

The legacy admin in:

- `E:\n8n to python\src\bee_ingestion\admin_ui.py`

remains unchanged in phase 1 and acts as the reference implementation plus fallback.

## Why this direction

- The React app is already the primary user-facing frontend.
- The React admin already owns accounts, runtime, rate limits, table browsing, and SQL.
- The legacy admin still owns the operational corpus surfaces.
- Keeping both as long-term primaries would preserve two frontend stacks and two UX models.

## Subagent findings used for this pass

### Legacy admin capability map

Mapped from:

- `E:\n8n to python\src\bee_ingestion\admin_ui.py`
- `E:\n8n to python\src\bee_ingestion\api.py`

Critical legacy surfaces:

- Documents and document bundle operations
- Chunk review and chunk detail
- KG entities/assertions/raw extraction inspection
- Chroma collections, records, and parity
- Ingest runner and pipeline operations

### React admin structure map

Mapped from:

- `E:\n8n to python\frontend\src\features\admin\AdminPage.tsx`
- `E:\n8n to python\frontend\src\lib\api\admin.ts`

Key conclusion:

- The React shell and auth model were already suitable.
- The migration seam was the API client first, then additive sections.
- No legacy template rewrite was needed for phase 1.

## Implemented in phase 1

### React admin sections added

Added new top-level React admin sections:

- `Corpus`
- `Chunks`
- `Knowledge graph`
- `Chroma`
- `Operations`

These are now wired into:

- `E:\n8n to python\frontend\src\features\admin\AdminPage.tsx`
- `E:\n8n to python\frontend\src\features\admin\AdminExtendedSections.tsx`

### React admin API surface expanded

Added React-side bindings for existing backend endpoints in:

- `E:\n8n to python\frontend\src\lib\api\admin.ts`

Covered endpoint groups:

- `/admin/api/documents`
- `/admin/api/documents/{document_id}/bundle`
- `/admin/api/documents/{document_id}/rebuild`
- `/admin/api/documents/{document_id}/revalidate`
- `/admin/api/documents/{document_id}/reindex`
- `/admin/api/documents/{document_id}/reprocess-kg`
- `/admin/api/documents/{document_id}/delete`
- `/admin/api/chunks`
- `/admin/api/chunks/{chunk_id}`
- `/admin/api/chunks/{chunk_id}/decision`
- `/admin/api/chunks/review/auto`
- `/admin/api/metadata/chunks`
- `/admin/api/kg/entities`
- `/admin/api/kg/entities/{entity_id}`
- `/admin/api/kg/assertions`
- `/admin/api/kg/raw`
- `/admin/api/chroma/collections`
- `/admin/api/chroma/records`
- `/admin/api/chroma/parity`
- `/admin/api/system/ingest-progress`
- `/admin/api/system/processes`
- `/admin/api/activity/stages`
- `/admin/api/activity/reviews`
- `/admin/api/ontology`
- `/admin/api/system/reingest/start`
- `/admin/api/system/reingest/resume`
- `/admin/api/system/reingest/stop`
- `/admin/api/retrieval/evaluate`
- `/admin/api/retrieval/evaluation`
- `/admin/api/agent/evaluate`
- `/admin/api/agent/evaluation`
- `/admin/api/reset`

## What works now

In the React admin:

- browse documents and inspect bundle data
- trigger rebuild, revalidate, reindex, KG replay, delete
- browse chunks and make manual chunk decisions
- auto-review chunks for a document or filtered chunk set
- inspect KG entities, assertions, and raw extraction rows
- inspect Chroma collections, records, and parity
- monitor ingest runner state and recent stage/review runs
- start, resume, stop, or reset the ingest pipeline
- load and save the ontology
- run retrieval and agent evaluations from the React admin

## Phase 2 completion

Added the remaining operator surfaces to the React admin:

- `Agent`
  - sessions
  - runs
  - profiles
  - reviews
  - patterns
  - run review
  - run replay
- `Operations`
  - ingest start, resume, stop, reset
  - ontology load and save
  - retrieval and agent evaluations
  - direct editor:
    - `/admin/api/editor/load`
    - `/admin/api/editor/save`
    - `/admin/api/editor/delete`
    - `/admin/api/editor/resync`
  - manual ingest helpers:
    - `/ingest/text`
    - `/ingest/pdf`
    - `/admin/api/uploads/ingest`

## Primary admin route

The primary admin route is now the React admin:

- `/admin` -> redirects to `/app/control`

The legacy admin remains available as fallback/reference only:

- `/admin/legacy`

## Validation

Build verification:

- `npm run build` in `E:\n8n to python\frontend`

Result:

- TypeScript build passed
- Vite production build passed

Backend validation:

- `python -m py_compile E:\n8n to python\src\bee_ingestion\api.py`

## Remaining refinement work

The admin migration is complete enough for one primary admin UI. Remaining work is refinement, not parity blockers:

1. richer drilldown and linked-image UX
2. more polished table views instead of JSON-heavy operator panels
3. eventual retirement of `admin_ui.py` if the fallback is no longer needed
