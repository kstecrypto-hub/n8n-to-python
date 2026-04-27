# Database Schema

## Short answer

Yes. The main application database is PostgreSQL.

The current storage layout is split across three stores:

- Application PostgreSQL: main corpus, ingestion, knowledge graph, agent runtime, and operator-facing relational data.
- Identity PostgreSQL: user accounts, login sessions, roles, and explicit permissions.
- Chroma: vector embeddings for chunks and page assets.

## Active stores

### 1. Application PostgreSQL

- Default DSN in code: `postgresql://bee:bee@localhost:5432/bee_ingestion`
- Docker Compose mapping for local development: `127.0.0.1:35432 -> 5432`
- Source of truth schema file: `sql/schema.sql`

Postgres is the primary operational store. If something is part of ingestion state, KG state, agent runtime, or the admin-visible relational model, it belongs here first.

### 2. Identity PostgreSQL

- Default DSN in code: `postgresql://bee_auth:bee_auth@localhost:5432/bee_identity`
- Docker Compose mapping for local development: `127.0.0.1:35433 -> 5432`
- Source of truth schema file: `sql/identity_schema.sql`

This database is isolated from ingestion and retrieval workloads so account data, session state, and permission writes have their own operational boundary.

### 3. Chroma

- Local Docker mapping: `127.0.0.1:38101 -> 8000`
- Default local path setting: `data/chroma`

Chroma stores derived vector indexes, not the main operational source of truth.

## PostgreSQL schema

### Ingestion and source documents

- `documents`
  Stable document identity by tenant, source type, filename, content hash, parser/ocr metadata, and status.
- `document_sources`
  Raw and normalized source text plus extraction metrics and metadata for replayability.
- `ingestion_jobs`
  Document-level job lifecycle, versions, lease ownership, and processing status.
- `ingestion_stage_runs`
  Per-stage execution records for a job, including attempts, metrics, and failures.
- `parsed_blocks`
  Parsed structural text blocks with page and character offsets.
- `document_pages`
  Per-page extracted text, OCR text, merged text, and page metadata.
- `page_assets`
  Page-level visual assets with OCR/description/search text, hashes, and asset metadata.
- `document_chunks`
  Retrieval chunks with section path, page bounds, parser/chunker versions, metadata, and status.
- `chunk_validations`
  Validation decision, quality score, and reasons for a chunk.
- `chunk_asset_links`
  Relationships between text chunks and page assets, including confidence and metadata.
- `chunk_review_runs`
  Model review history for chunk-role or quality decisions, with payload and confidence.

### Agent memory, sessions, and responses

- `agent_profiles`
  Long-lived user or persona profile state, summaries, tokens, and source-model provenance.
- `agent_sessions`
  Conversation sessions, status, claims, leases, and session token hash.
- `agent_session_memories`
  Rolling session-level summarized memory for a conversation.
- `agent_messages`
  Persisted chat messages for a session.
- `agent_query_runs`
  User queries, normalized forms, answer payloads, confidence, abstentions, prompt/raw/final payloads, and review state.
- `agent_answer_reviews`
  Human or operator review decisions for a query run.
- `agent_query_patterns`
  Query-pattern learning and router cache state by tenant and signature.
- `agent_query_sources`
  Evidence/source records linked to a query run.

### Agent runtime configuration

- `agent_runtime_configs`
  Tenant-scoped JSON runtime overrides for the agent.
- `agent_runtime_secrets`
  Tenant-scoped secret overrides such as encrypted API key replacements.

### Knowledge graph

- `kg_raw_extractions`
  Raw ontology extraction payload per chunk before final acceptance.
- `kg_entities`
  Canonicalized KG entities.
- `kg_assertions`
  Predicate assertions connecting entities or literals back to chunks and documents.
- `kg_assertion_evidence`
  Evidence rows tied to assertions.

## PostgreSQL relationships and design intent

### Document pipeline flow

- `documents` is the root record.
- `document_sources`, `ingestion_jobs`, `parsed_blocks`, `document_pages`, `page_assets`, and `document_chunks` all hang off document identity.
- `chunk_validations`, `chunk_asset_links`, `chunk_review_runs`, and `kg_raw_extractions` refine chunk-level output after chunk creation.

### Agent flow

- `agent_sessions` owns `agent_messages` and `agent_session_memories`.
- `agent_query_runs` optionally attaches to a session.
- `agent_answer_reviews` and `agent_query_sources` attach to a query run.
- `agent_query_patterns` summarizes repeated query behavior by tenant and signature.

### Runtime/config flow

- `agent_runtime_configs` and `agent_runtime_secrets` are tenant-scoped.
- They are intentionally separate so editable runtime JSON and secret values do not share the same storage model.

## Identity PostgreSQL schema

Public login identity now lives in the dedicated `bee_identity` Postgres database, inside the `auth` schema.

### `auth.auth_users`

- `user_id`
- `email`
- `display_name`
- `tenant_id`
- `role`
- `status`
- `permissions_json`
- `password_hash`
- `password_salt`
- `password_iterations`
- `created_at`
- `updated_at`
- `last_login_at`

Purpose:

- Stores browser-login identities for the public chat app.
- Keeps role and tenant membership attached to the account record.
- Stores explicit permission assignments alongside the coarse role label.
- Supports admin-provisioned users only in the current product flow.

### `auth.auth_sessions`

- `auth_session_id`
- `user_id`
- `tenant_id`
- `session_token_hash`
- `created_at`
- `last_seen_at`
- `expires_at`
- `revoked_at`

Purpose:

- Stores first-party authenticated web sessions for the public product.
- Session tokens are delivered via HttpOnly cookies; only token hashes are persisted.

## Chroma collections

The code currently treats Chroma as a derived search index.

- chunk collection: `kb_chunks_t3large_v1`
- asset collection: `kb_assets_v1`

These collections are fed from the relational source-of-truth records in Postgres and can be rebuilt if needed.

## Tenant model

The project is currently tenant-aware mostly through `tenant_id` columns rather than isolated Postgres schemas per tenant.

Current pattern:

- corpus/runtime records live in shared application Postgres tables with a `tenant_id` field where relevant
- public auth accounts live in the dedicated identity Postgres database and also carry `tenant_id`
- runtime overrides and learned query patterns are tenant-scoped explicitly

## Important implementation note

The operator console now exposes:

- application Postgres relation browsing and row CRUD across the operational schemas
- identity Postgres browsing for `auth.auth_users` and `auth.auth_sessions`
- SQL execution for advanced Postgres changes against either database
- auth account CRUD against the dedicated identity Postgres database
- rate-limit configuration
- tenant runtime config editing

That means the admin frontend now manages two Postgres control-plane databases plus Chroma as a derived vector index.
