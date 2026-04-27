# Networking and Security

## Purpose
Use this file to reason about service communication and local exposure.

## Compose networking defaults
Compose usually creates an app network and each service joins it. Services can reach each other by service name.

## Practical rules
- from `api`, connect to `db:5432`, not `localhost:5432`
- publish ports only for services the host needs to access
- keep purely internal backends off host-exposed ports unless debugging requires otherwise

## Suggested exposure model for local agent stacks
Publish to host only when needed:
- API/UI port
- optional Chroma or Neo4j admin port if the user needs direct inspection
- optional observability UI

Keep internal only when not needed externally:
- Redis
- worker-only internal services
- private backends

## Secrets and environment
For local work:
- place variable placeholders in `.env.example`
- use local `.env` or equivalent ignored files for real values
- do not bake secrets into images
- do not hard-code secrets in compose examples unless clearly fake placeholders

## Internal-only networks
Use an internal backend network when you want to make it explicit that a database or vector store should only be reachable from other containers.

## Anti-patterns
- assuming published ports are required for container-to-container communication
- mixing real credentials into committed Compose files
- exposing databases publicly on all interfaces without need
