# Agent Stack Blueprints

## Purpose
Use this file to map Docker design to agent ingestion systems.

## Common local stack

### Minimal
- `app`: API/UI
- `worker`: ingestion and processing
- `vector`: Chroma or equivalent

### Standard
- `app`
- `worker`
- `db`
- `redis`
- `vector`
- `graph`

### Extended local AI stack
- `app`
- `worker`
- `db`
- `redis`
- `vector`
- `graph`
- `model`
- `observability` profile

## Recommended service responsibilities

### app
- serves HTTP/API/UI
- accepts ingest requests
- reads from backends but should not own background processing loops unless intentionally simplified

### worker
- performs document loading, chunking, embedding, KG extraction, indexing, retries
- can be rebuilt/restarted independently of the app

### vector
- persists embeddings and metadata
- receives stable IDs from the application layer

### graph
- persists entities, relations, provenance, graph metadata

### redis / queue
- handles background job decoupling, retries, locks, or cache where useful

## Local developer ergonomics
A good local stack should let a developer:
- boot everything with one Compose command
- inspect logs by service
- reset selected state intentionally
- change app code without destroying database state
- know exactly where data lives

## Example delivery pattern
When asked to build the runtime, deliver:
1. service inventory
2. Dockerfile per buildable service
3. `compose.yaml`
4. `.env.example`
5. named volume definitions
6. health checks
7. runbook with first boot, rebuild, logs, reset, teardown
