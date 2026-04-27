# Local Setup

## Goal
Select the correct local Docker setup before designing the stack.

## Preferred installation stance

### Mac and Windows
Default to Docker Desktop.
Reason: it includes Docker Engine, Docker CLI, and Docker Compose and is the most straightforward local developer setup.

### Linux
Default to either:
- Docker Desktop, or
- Docker Engine plus the Docker Compose plugin

For modern local multi-service work, treat Compose as required.
Avoid relying on the legacy standalone Compose binary except when maintaining an older environment.

## What Codex should verify in a local setup plan
- Docker is installed
- daemon is running
- `docker version` works
- `docker compose version` works
- builder is available
- local disk paths for bind mounts exist
- ports chosen for the stack are free
- reset/rebuild commands are documented

## Local-only expectations
When the user wants everything local:
- do not assume cloud registries, managed databases, or hosted queues
- prefer local images and public base images pulled once
- persist backend data via named volumes
- keep configuration in `.env` or `.env.local` style files, not hard-coded in compose

## Suggested preflight checklist
1. Confirm Docker and Compose availability.
2. Confirm the host OS and architecture if base images might be architecture-sensitive.
3. Confirm which services need durable state.
4. Confirm which host directories must be mounted.
5. Confirm which ports are exposed to the host.
6. Confirm expected run commands for first boot, rebuild, teardown, and reset.
