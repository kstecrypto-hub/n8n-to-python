# Bee Frontend And Tenant Architecture

## Decision

- Use `npm` with `Vite + React + TypeScript` for the new frontend.
- Keep FastAPI as the only browser-to-server boundary.
- Keep the frontend same-origin with the API so the current cookie-based public session model continues to work without CORS or token leakage complexity.

Why `npm` is the right default here:

- the repository has no existing JavaScript package-manager convention to preserve
- the current UI is inline HTML inside Python files, so maintainability is the real bottleneck
- `npm` is already available in the workspace and is the lowest-friction choice for `Vite`

## Current Runtime Reality

The current backend already supports:

- first-party auth sessions through `/auth/*` backed by a separate local identity database
- public browser-scoped agent sessions through HttpOnly-ish cookie flow in `/agent/*`
- public browser-scoped agent profiles through `/agent/profile`
- admin control-plane access through `X-Admin-Token` on `/admin/api/*` and `/ingest/*`
- tenant scoping on corpus, sessions, runs, profiles, runtime config, and runtime secrets

The current backend does not yet support:

- real tenant membership tables
- per-user org membership or invitation flow
- role enforcement beyond public-vs-admin trust boundaries

That distinction matters. The frontend should present the recommended tenant system as an architectural blueprint, not claim that the backend already enforces it.

## Frontend Boundary

The frontend must use APIs only.

Public app:

- `GET /auth/session`
- `POST /auth/login`
- `POST /auth/logout`
- `GET /agent/session`
- `POST /agent/session/reset`
- `GET /agent/profile`
- `PUT /agent/profile`
- `POST /agent/query`
- `POST /agent/chat`
- `POST /agent/runs/{query_run_id}/feedback`
- `GET /agent/assets/{asset_id}/image`

Operator app:

- `GET /admin/api/*`
- `POST /admin/api/*`
- `PUT /admin/api/*`
- `DELETE /admin/api/*`
- `POST /ingest/*`

Rules:

- the public frontend must never assume it can choose any tenant; the backend fixes public traffic to the configured public tenant
- the public login page is login-only; self-service signup is disabled by default for the MVP
- the operator frontend can expose tenant scope selectors, but those are filters and config scopes, not trust boundaries
- admin token handling stays separate from the public cookie session model

## Recommended Tenant Model

Use three layers:

1. Platform layer
2. Tenant layer
3. User membership layer

### Platform Layer

Platform-level concerns:

- tenant creation and suspension
- global model policy
- platform audit access
- billing and quota policy
- cross-tenant incident response

These should not live in the public corpus database.

### Tenant Layer

A tenant is one isolated hive or workspace.

Recommended tenant-owned resources:

- documents
- chunks, assets, KG rows, and vectors
- runtime config overrides
- chat sessions, profiles, runs, and reviews
- member-scoped UI preferences

### Membership Layer

Recommended future tables:

- `users`
- `identities`
- `tenants`
- `tenant_memberships`
- `tenant_invitations`
- `service_accounts`
- `api_clients`
- `auth_sessions`
- `audit_events`

## Separate Database Recommendation

Keep the current knowledge data plane where it is:

- Postgres for corpus, runtime traces, and tenant-scoped agent data
- Chroma for vectors

Add a separate identity database for:

- user accounts
- tenant memberships
- invitations
- SSO/OIDC identities
- refresh sessions
- MFA state
- auth audit trails

Reason:

- auth data has a different retention, risk, and operational lifecycle than corpus retrieval data
- this avoids mixing identity writes with ingestion and retrieval workloads
- it makes eventual external auth or SSO integration much cleaner

Practical recommendation:

- keep a separate Postgres database for identity and tenant membership
- keep Chroma separate as it already is
- do not let the browser talk to either database directly

Current implementation note:

- the current login-first MVP stores public auth in a dedicated Postgres identity database, inside the `auth` schema
- the admin UI can inspect both the application database and the identity database without changing the browser contract
- the auth store remains DSN-driven, so the identity database can later move to its own managed host without rewriting the frontend
- public self-service registration is disabled by default
- provision users from the server side until membership management lands in the admin console

## Recommended Roles

### `platform_owner`

- create, suspend, and recover tenants
- manage platform-wide model policy and quotas
- access platform audit logs
- no routine corpus editing inside every tenant by default

### `tenant_admin`

- manage tenant members and invitations
- manage tenant runtime config
- view tenant sessions, runs, reviews, and metrics
- approve destructive ingest actions
- cannot act across other tenants

### `knowledge_curator`

- upload, ingest, revalidate, rebuild, reindex, and reprocess KG for tenant documents
- review evidence quality and citation drift
- cannot manage tenant members or platform billing

### `review_analyst`

- inspect sessions, runs, reviews, citations, and feedback
- replay runs for debugging
- annotate or approve/reject answer quality
- cannot change runtime secrets or delete documents

### `member`

- use chat within assigned tenant scope
- manage own profile and browser session
- submit answer feedback
- cannot access admin or ingest endpoints

### `guest`

- not active in the current MVP public product
- if enabled later, it should sit behind an explicit feature flag
- would allow public-tenant access only, with no uploads, no config, and no admin visibility

## Frontend Permission Matrix

| Capability | guest | member | review_analyst | knowledge_curator | tenant_admin | platform_owner |
|---|---:|---:|---:|---:|---:|---:|
| Chat with agent | yes | yes | yes | yes | yes | yes |
| Update own profile | yes | yes | yes | yes | yes | yes |
| Submit answer feedback | yes | yes | yes | yes | yes | yes |
| View tenant runs/reviews | no | no | yes | yes | yes | yes |
| Replay runs | no | no | yes | yes | yes | yes |
| Upload or ingest documents | no | no | no | yes | yes | yes |
| Rebuild or delete documents | no | no | no | limited | yes | yes |
| Edit runtime config | no | no | no | limited | yes | yes |
| Edit runtime secrets | no | no | no | no | yes | yes |
| Manage tenant membership | no | no | no | no | yes | yes |
| Create or suspend tenants | no | no | no | no | no | yes |

`limited` means allowed only with explicit destructive-action safeguards, confirmation UI, and server-side authorization.

## Frontend Route Model

Recommended SPA routes:

- `/app`
- `/app/chat`

Meaning:

- `/app` is the login screen
- `/app/chat` is the user-facing chat surface after successful login
- `/admin` stays separate as the review console and is intentionally unlinked from the public shell

## UI Design Direction

Design language:

- honeycomb cards instead of flat dashboard tiles
- amber, wax, charcoal, and pollen accents instead of generic dark SaaS colors
- a dedicated evidence rail for citations and supporting assets
- a clear separation between public chat flow and operator control flow
- auth entry framed as hive access, but without obscuring the real trust boundaries

UX constraints:

- public mode should be login-first and then collapse to chat-only
- operator mode should feel scoped, auditable, and harder to misuse
- destructive actions must stay in the legacy admin console or future operator components with confirmation gates
- public self-service signup is disabled for the MVP; users are provisioned server-side

## API And Security Constraints

- public login remains secure-cookie/session based
- public chat remains protected by the authenticated first-party session plus the agent's own browser-scoped chat/profile cookies
- operator mode remains token-header based until real tenant membership auth exists
- never trust tenant ids, role labels, or document scope from the browser alone
- keep tenant checks in repository queries, not only in route handlers
- once tenant membership auth is implemented, use secure cookie sessions for the first-party web app instead of exposing long-lived bearer tokens to the browser

## Open Decisions To Confirm

These are the main product decisions still worth confirming before backend auth work starts:

1. Should `knowledge_curator` be allowed to edit runtime prompts, or only ingestion/retrieval data?
2. Should `review_analyst` be allowed to replay runs in production, or only in staging/admin review mode?
3. Should tenant admins be able to see raw prompt and model payloads, or is that platform-owner-only?
4. Guest chat should remain off in the main product shell unless it is deliberately reintroduced later behind an explicit feature flag.
