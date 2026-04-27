# Compose Patterns

## Purpose
Use this file to design the multi-service runtime.

## What Compose is for
Compose declares the running system: services, images or build contexts, ports, volumes, networks, env vars, health checks, and startup relationships.

## Default design rules
- use one service per major responsibility
- give services stable, meaningful names
- keep shared configuration explicit
- prefer `.env` inputs and `environment` blocks over hidden assumptions
- expose only required host ports

## Startup order and readiness
A started container may still be unusable.
For dependencies that need readiness, add:
- a `healthcheck` on the dependency service
- long-form `depends_on` with `condition: service_healthy` on the dependent service

Use this for databases, caches, vector stores, and graph stores when the app starts immediately.

## Profiles
Use profiles for optional services such as:
- tracing UIs
- admin tools
- debug shells
- migration jobs
- notebook environments

Keep the default boot path minimal.

## Volumes in Compose
Use named volumes for:
- database data
- vector database persistence
- graph database persistence
- caches that should survive restarts

Use bind mounts for:
- application source code during development
- local ingest directories
- config files intentionally edited on the host

## Networks in Compose
Default to one app network unless isolation requirements justify more.
Use multiple networks when:
- only selected services should see a backend
- a reverse proxy should talk to frontend services but not databases
- you want an internal-only backend network

## Port rules
- internal container-to-container traffic uses service name plus container port
- host-to-container traffic uses published host ports
- do not use `localhost` inside one container to reach a sibling container

## Reconciliation behavior
Running `docker compose up` again is expected after config changes. Compose recreates changed services while preserving mounted volumes.

## Anti-patterns
- relying entirely on short `depends_on` when readiness matters
- publishing every backend port to the host “just in case”
- mixing dev bind mounts with production assumptions without saying so
