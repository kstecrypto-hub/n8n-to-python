# Prompts and Scaffolds

## Use this response pattern

```markdown
# Local Docker runtime plan

## 1. Runtime architecture
[services, responsibilities, dependencies]

## 2. Dockerfiles
[per-service build strategy]

## 3. Compose design
[services, ports, volumes, networks, health checks]

## 4. Persistence plan
[what uses bind mounts vs named volumes]

## 5. Environment plan
[required variables and where they belong]

## 6. Runbook
[first boot, rebuild, logs, shell access, reset]

## 7. Failure modes
[most likely local issues and fixes]
```

## Good instruction phrasing for Codex
- “Build a fully local Docker runtime for this agent stack.”
- “Use Compose as the source of truth for orchestration.”
- “Add health checks and readiness-aware dependencies.”
- “Persist stateful backends with named volumes.”
- “Use bind mounts only where host editing is required.”
- “Document exact commands for build, run, logs, reset, and teardown.”

## What to avoid in answers
- generic Docker tutorials detached from the stack
- long lists of commands without architecture reasoning
- advice that mixes dev and production behavior without saying so
- solutions that hide important persistence or networking assumptions
