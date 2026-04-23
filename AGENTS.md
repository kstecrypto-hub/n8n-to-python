# Workspace Skills

## Workspace Defaults

- Prefer local workspace skills on `E:` by default for matching tasks.
- Keep all project work, created files, edits, outputs, searches, and inputs on `E:` only unless the user explicitly grants a one-time exception.
- Do not download, create, modify, move, or read project files on `C:`.
- Do not rely on `C:` system skills or `C:` workspace paths for normal task execution in this workspace.

## Skills

A skill is a set of local instructions stored in a `SKILL.md` file. The skills below are available in this workspace and should be considered for tasks that match their descriptions.

### Available skills

- `1claw`: HSM-backed secret management and EVM transaction signing with 1Claw. Use when Codex needs to discover, fetch, store, rotate, delete, describe, share, or grant access to secrets in a 1Claw vault; inspect available vaults or secret paths; work with env bundles; or simulate and submit transactions through 1Claw without exposing raw secret values or private keys in conversation context. (file: `E:\n8n to python\skills\1claw\SKILL.md`)
- `api-security-best-practices`: Secure API design and hardening guidance for REST, GraphQL, and WebSocket backends. Use when designing new endpoints, securing existing APIs, implementing authentication or authorization, adding request validation, rate limiting, secret handling, file upload protection, or reviewing code for OWASP API Top 10 risks and common injection vulnerabilities. (file: `E:\n8n to python\skills\api-security-best-practices\SKILL.md`)
- `chroma-local-ingestion`: Local ChromaDB vector-ingestion skill. Use when designing or implementing local Chroma collections, chunking strategy, embedding contracts, metadata schemas, deterministic chunk ids, persistence paths, update/delete flow, and retrieval evaluation for agent knowledge retrieval. (file: `E:\n8n to python\skills\chroma-local-ingestion\SKILL.md`)
- `codex-orchestration`: General-purpose orchestration for Codex. Use when a task benefits from decomposition, `update_plan`, parallel scouting or validation, or background `codex exec` workers. Best for messy, multi-step, high-uncertainty, or review-heavy work where one agent should coordinate and synthesize. (file: `E:\n8n to python\skills\codex-orchestration\SKILL.md`)
- `docker-agent-runtime`: Local Docker runtime skill for agent systems. Use when containerizing the app, worker, Chroma, Postgres, graph stores, queues, and related services with Dockerfiles, Compose, health checks, volumes, networking, and local runbooks. (file: `E:\n8n to python\skills\docker-agent-runtime\SKILL.md`)
- `kg-ingestion-architect`: Knowledge-graph ingestion architecture skill. Use when designing or implementing ontology-constrained extraction, provenance-aware graph ingestion, canonicalization, validation, entity resolution, and durable KG write pipelines from text or documents. (file: `E:\n8n to python\skills\kg-ingestion-architect\SKILL.md`)
- `postgres-db`: PostgreSQL database operations skill. Use when executing SQL queries, inspecting schema, exporting table structure, creating backups, restoring databases, or checking PostgreSQL performance and size metrics. (file: `E:\n8n to python\skills\postgres-db\SKILL.md`)
- `rag-ingest`: RAG ingestion skill for writing already-parsed body text into a vector store. Use when chunking clean text, generating embeddings, and writing vectors plus metadata for retrieval, without handling crawling or summarization. (file: `E:\n8n to python\skills\rag-ingest\SKILL.md`)
- `rag-ingestion-pipeline`: Research-backed RAG ingestion pipeline design skill. Use when designing or improving normalization, chunking, ontology or KG enrichment, metadata, provenance, embedding and indexing strategy, update workflows, or ingestion evaluation for RAG, GraphRAG, or ontology-backed retrieval systems. (file: `E:\n8n to python\skills\rag-ingestion-pipeline\SKILL.md`)

## How to use skills

- Discovery: The list above is the skills available in this workspace. Skill bodies live on disk at the listed paths.
- Trigger rules: If the user names a skill with `$skill-name` or plain text, or the task clearly matches a skill description, use that skill for the turn. Multiple mentions mean use the minimal set that covers the task.
- Default preference: Check these workspace skills first before relying on any external or system-level skill list.
- Progressive disclosure:
  1. Open the referenced `SKILL.md`.
  2. Read only enough to follow the workflow.
  3. Load referenced files from `references/`, `scripts/`, or `assets/` only when they are needed.
- Coordination:
  - Announce which skill or skills you are using and why in one short line.
  - If a skill cannot be applied cleanly, say so briefly and continue with the next-best approach.
