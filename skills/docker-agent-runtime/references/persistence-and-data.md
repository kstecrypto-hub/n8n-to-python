# Persistence and Data

## Purpose
Use this file to design durable state and reset behavior.

## First principle
Container filesystems are not your durability layer. Durable local state must be mapped intentionally.

## Choose the storage mechanism

### Named volumes
Use for durable service-managed state:
- Postgres data
- Redis append-only or durable state if used
- Chroma persistence directory
- Neo4j data and logs

Advantages:
- Docker-managed
- more portable across hosts than raw bind paths
- better default for service data

### Bind mounts
Use for host-managed content:
- source code
- prompt/config files
- input documents to ingest
- exported artifacts

Advantages:
- visible and editable from the host
- ideal for live local development

## Reset policy
Always document which reset each command performs:
- restart containers only
- recreate containers but keep volumes
- delete volumes and lose data
- prune images/build cache

## Local agent-ingestion examples
- `/data/inbox` bind-mounted from host for incoming documents
- named volume for vector store state
- named volume for graph database state
- bind mount for source tree in development
- optional bind mount for exported results

## Id strategy and persistence relationship
If a service persists embeddings, chunks, graph nodes, or derived artifacts, the runtime plan must define stable IDs. Docker does not solve data identity; the application layer must.

## Anti-patterns
- writing important state into `/tmp` and assuming it survives recreation
- bind mounting an entire home directory into a container
- sharing one data directory among unrelated services without a clear reason
