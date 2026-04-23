---
name: 1claw
description: HSM-backed secret management and EVM transaction signing with 1Claw. Use when Codex needs to discover, fetch, store, rotate, delete, describe, share, or grant access to secrets in a 1Claw vault; inspect available vaults or secret paths; work with env bundles; or simulate and submit transactions through 1Claw without exposing raw secret values or private keys in conversation context.
---

# 1Claw

## Overview

Use 1Claw to work with secrets just in time, keep secret material out of chat responses, and rely on vault policies instead of broad implicit access. Use this skill's reference when you need exact MCP parameters, REST endpoints, SDK methods, supported chains, or billing and error details.

## Workflow

1. Check whether the task needs discovery, metadata, the decrypted value, or a write operation.
2. Prefer metadata-first operations before value reads:
   - Use `list_vaults` before creating a vault.
   - Use `list_secrets` to discover candidate paths.
   - Use `describe_secret` before `get_secret` when you only need existence, type, version, or expiry.
3. Fetch secret values only immediately before the downstream API call that needs them.
4. Never echo, summarize, log, or persist raw secret values in files or responses unless the user explicitly asks for secure storage in 1Claw itself.
5. After regenerating a credential at a provider, immediately store the replacement with `rotate_and_store` or `put_secret`.
6. For one-off access, prefer `share_secret`; for ongoing scoped access, prefer `grant_access`.
7. For EVM actions, simulate before signing or submitting. Treat signing keys as HSM-managed and never attempt to extract raw private keys.

## Task Map

- Discover available vaults or candidate secret paths:
  Use `list_vaults` and `list_secrets`.
- Check whether a secret exists or inspect non-sensitive metadata:
  Use `describe_secret`.
- Read a credential for immediate use:
  Use `get_secret`.
- Store or update a secret value:
  Use `put_secret`.
- Rotate a known existing secret after regeneration:
  Use `rotate_and_store`.
- Read a multi-variable configuration bundle:
  Use `get_env_bundle`.
- Remove a secret:
  Use `delete_secret`.
- Grant continuing access to a vault path:
  Use `grant_access`.
- Share a secret temporarily:
  Use `share_secret`.
- Create a new vault:
  Use `create_vault`, but only after checking `list_vaults`.
- Simulate or submit EVM transactions:
  Use `simulate_transaction` or `submit_transaction`.

## Guardrails

- Treat access as deny-by-default. A valid agent credential does not imply permission to every vault or path.
- If 403 occurs, assume a missing policy, vault binding restriction, or transaction guardrail violation and tell the user what access appears to be missing.
- If 401 occurs, re-authenticate or let the MCP server refresh the token.
- Surface 402 quota or billing issues to the user rather than retrying blindly.
- If Intents API is enabled, expect raw reads of `private_key` and `ssh_key` secrets to be blocked.
- Do not invent secret paths. Discover them first when unclear.

## References

- Read [references/1claw-reference.md](./references/1claw-reference.md) when you need:
  - MCP tool parameters and expected behavior
  - REST endpoints and auth flows
  - SDK method mapping
  - supported chains
  - access control, Intents API, Shroud, CMEK, billing, and error codes
