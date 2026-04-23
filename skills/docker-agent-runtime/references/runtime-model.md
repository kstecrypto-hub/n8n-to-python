# Runtime Model

## Purpose
Use this file to reason about what Docker is doing in an agent system.

## Core concepts

### Dockerfile
A build recipe. It defines how to turn source code and dependencies into an image.

### Image
An immutable artifact. It should contain everything needed to run one responsibility.

### Container
A running process tree created from an image plus runtime configuration.

### Compose
A declarative description of multiple services, their builds/images, ports, networks, volumes, environment, and dependency behavior.

### Volume
Docker-managed persistent storage. Prefer this for stateful backends.

### Bind mount
A host path mounted into a container. Prefer this for local source code or host-managed data.

### Network
An isolated communication plane. Compose usually provides service-name DNS automatically.

## How to think about an agent stack
An agent system is rarely one process. It usually includes:
- user-facing app or API
- ingestion worker
- storage backends
- model runtime or provider adapter
- cache / queue / scheduler
- optional tracing and dashboards

Model each as a service if it has:
- different runtime dependencies
- different scaling needs
- different restart behavior
- state that must be isolated

## Anti-patterns
- one giant container for the entire stack
- putting durable database files inside a container filesystem without a volume
- using hostnames like `localhost` between containers
- storing secrets directly in Dockerfiles
- treating `docker compose up` success as proof of readiness

## Good default decomposition
- `api`: HTTP interface, auth, request handling
- `worker`: ingestion, chunking, embeddings, extraction
- `db`: relational metadata store
- `redis`: queue/cache
- `vector`: Chroma or another vector store
- `graph`: Neo4j or another graph store
- `model`: optional local inference runtime
