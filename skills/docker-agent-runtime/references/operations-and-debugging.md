# Operations and Debugging

## Purpose
Use this file when the stack does not build, start, or behave correctly.

## Failure classification

### Build failure
Check:
- base image exists for host architecture
- dependency install step
- copied paths and `.dockerignore`
- missing build args
- stale cache or wrong target stage

### Container exits immediately
Check:
- command/entrypoint
- missing required env vars
- file permissions
- expected files missing from image
- process crashes visible in logs

### Service starts but app fails to connect
Check:
- wrong hostname (`localhost` misuse)
- wrong port
- dependency not ready yet
- network separation
- credentials/config mismatch

### Data seems lost
Check:
- volume not mounted where the service writes
- using ephemeral container path instead of persistent path
- stack recreated with `down -v`

### Code changes not reflected
Check:
- bind mount missing in dev mode
- image not rebuilt
- wrong working directory or copied path
- old container still running

## Default debugging commands to include in runbooks
- `docker compose ps`
- `docker compose logs -f <service>`
- `docker compose config`
- `docker compose exec <service> sh`
- `docker inspect <container>`
- `docker volume ls`
- `docker network ls`

## Recovery patterns
- `docker compose up --build`
- `docker compose restart <service>`
- `docker compose down`
- `docker compose down -v` only when intentional data reset is acceptable
- `docker system prune` only with care

## Readiness guidance
When dependency startup is the issue, prefer health checks and `depends_on` conditions over brittle sleep loops.
