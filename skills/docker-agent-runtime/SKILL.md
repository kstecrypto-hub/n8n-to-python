---
name: docker-agent-runtime
description: teach chatgpt how to design, set up, and operate a local docker runtime for agent systems and ingestion pipelines. use when codex needs to containerize applications, build dockerfiles, create compose stacks, wire services together, persist local data, debug container startup, or produce reproducible local environments for agents, vector stores, graph databases, workers, api services, and supporting infrastructure. this skill is for building the runtime and orchestration layer correctly, not for generic docker tutorials.
---

# Docker Agent Runtime

Use this skill to build the local runtime environment for an agent system. Treat Docker as infrastructure for reproducibility, isolation, dependency control, service wiring, and data persistence.

## Working mode

Follow this order unless the user clearly asks for only one part:

1. Identify the stack shape: app, api, worker, scheduler, vector store, graph db, cache, database, reverse proxy, observability tools.
2. Decide what belongs in containers and what should remain on the host.
3. Design image boundaries and container responsibilities.
4. Design persistence, networking, startup order, health checks, and secrets handling.
5. Produce Dockerfiles, `compose.yaml`, `.env.example`, and run/debug commands.
6. Verify local developer ergonomics: rebuild cycle, logs, ports, volumes, and recovery paths.

## Output contract

Unless the user asks otherwise, produce these sections:

1. **Runtime architecture**
2. **Container boundaries**
3. **Dockerfile plan**
4. **Compose plan**
5. **Persistence and networking plan**
6. **Startup and health strategy**
7. **Local runbook**
8. **Failure modes and debugging steps**

## Required mental model

- A Dockerfile defines how to build an image.
- An image is an immutable build artifact.
- A container is a running instance of an image.
- Compose defines a multi-container application and how services interact.
- Volumes and bind mounts solve different problems: persistence versus live local file access.
- Networks define service reachability; service names are the default internal DNS names in Compose.
- Health is not the same as “process started”. Many local failures are dependency readiness problems.

## Decision rules

### 1) Choose runtime topology

Use [references/runtime-model.md](references/runtime-model.md) first.

Default to separate services when components have different lifecycles or dependencies, for example:
- `api`
- `worker`
- `scheduler`
- `db`
- `redis`
- `chroma`
- `neo4j`
- `ollama` or another local model runtime

Do **not** merge unrelated responsibilities into one container unless the user explicitly wants a simplified demo.

### 2) Choose installation/setup stance

Use [references/local-setup.md](references/local-setup.md).

Prefer:
- Docker Desktop on Mac and Windows
- Docker Desktop or Docker Engine + Compose plugin on Linux
- Compose, not ad hoc long `docker run` commands, for anything beyond a trivial one-container demo

### 3) Write Dockerfiles correctly

Use [references/dockerfiles.md](references/dockerfiles.md).

Default expectations:
- deterministic base image choice
- minimal runtime image
- explicit working directory
- dependency installation separated from app copy where cache helps
- non-root user when practical
- `.dockerignore`
- clear entrypoint/command behavior

### 4) Write Compose correctly

Use [references/compose-patterns.md](references/compose-patterns.md).

Default expectations:
- one `compose.yaml` for the local stack
- named services with clear purpose
- health checks for stateful dependencies
- `depends_on` with conditions when readiness matters
- profiles for optional tooling
- named volumes for stateful data stores
- bind mounts for source code only when live editing is desired

### 5) Design storage correctly

Use [references/persistence-and-data.md](references/persistence-and-data.md).

Default expectations:
- use named volumes for databases, vector stores, graph stores, and cache persistence
- use bind mounts for local source code, configs, or ingest folders that the host edits directly
- separate ephemeral scratch data from durable state
- define backup/reset behavior explicitly

### 6) Design networking and exposure correctly

Use [references/networking-and-security.md](references/networking-and-security.md).

Default expectations:
- internal service-to-service communication by service name
- expose only ports the host actually needs
- prefer internal-only networks for back-end services when possible
- avoid assuming `localhost` from one container reaches another container

### 7) Debug before rewriting

Use [references/operations-and-debugging.md](references/operations-and-debugging.md).

When something fails, classify it first:
- build failure
- container exits immediately
- dependency not ready
- wrong port binding
- wrong hostname
- missing volume or permission issue
- stale image or stale volume state
- environment/config mismatch

## Agent-stack defaults

When the user wants a local agent ingestion system, assume a pattern like:
- application service for API/UI
- ingestion worker service
- queue/cache service when needed
- one or more data backends such as Chroma, Neo4j, Postgres, Redis
- optional local model service
- optional observability/debug profile

Use [references/agent-stack-blueprints.md](references/agent-stack-blueprints.md) for composition patterns.

## Constraints to enforce

- keep everything runnable locally unless the user explicitly requests cloud dependencies
- prefer reproducibility over convenience hacks
- prefer declarative Compose configuration over manual setup notes
- make data directories and reset behavior explicit
- never hide important runtime assumptions; state ports, volumes, env vars, and dependency order clearly

## References

- [references/runtime-model.md](references/runtime-model.md)
- [references/local-setup.md](references/local-setup.md)
- [references/dockerfiles.md](references/dockerfiles.md)
- [references/compose-patterns.md](references/compose-patterns.md)
- [references/persistence-and-data.md](references/persistence-and-data.md)
- [references/networking-and-security.md](references/networking-and-security.md)
- [references/operations-and-debugging.md](references/operations-and-debugging.md)
- [references/agent-stack-blueprints.md](references/agent-stack-blueprints.md)
- [references/prompts-and-scaffolds.md](references/prompts-and-scaffolds.md)
