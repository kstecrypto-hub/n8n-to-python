# Dockerfiles

## Purpose
Use this file when writing or reviewing Dockerfiles for the local agent stack.

## Principles
- one Dockerfile per buildable service unless there is a strong reason to share
- small, deterministic runtime image
- predictable build cache behavior
- clear separation of build-time and run-time concerns

## Default structure
1. choose a base image intentionally
2. set `WORKDIR`
3. copy dependency manifests first
4. install dependencies
5. copy application source
6. set non-root user where practical
7. define `CMD` or `ENTRYPOINT`

## What to optimize for
- repeatable local builds
- understandable failure points
- fast rebuilds during development
- minimal runtime dependencies

## Use multi-stage builds when
- you compile assets or binaries
- you need a heavier build environment than runtime environment
- you want to exclude compilers and caches from the final image

## Python service guidance
- pin the Python base image tag intentionally
- install only required system packages
- use a dependency lock or explicit requirements file
- avoid copying the entire repo before dependency installation if cache matters
- include `PYTHONUNBUFFERED=1` for clearer logs when helpful

## Node service guidance
- copy package manifests before source
- install dependencies deterministically
- be explicit about production versus development installs

## `.dockerignore`
Always reason about `.dockerignore`.
Exclude at least:
- `.git`
- local virtual environments
- large data artifacts not needed for the build
- caches
- secrets
- host-specific temporary files

## Command design
Prefer a single clear startup command.
If startup requires migrations, asset generation, or waiting logic, either:
- encode that explicitly in entrypoint logic, or
- separate it into dedicated services/jobs in Compose.

## Anti-patterns
- copying secrets into the image
- installing tools “just in case”
- running multiple unrelated daemons in one container
- using `latest` blindly for critical images
