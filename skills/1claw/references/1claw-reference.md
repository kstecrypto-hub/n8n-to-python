# 1Claw Reference

## Service Endpoints

- API base URL: `https://api.1claw.xyz`
- Shroud: `https://shroud.1claw.xyz`
- MCP endpoint: `https://mcp.1claw.xyz/mcp`
- Dashboard: `https://1claw.xyz`
- Docs: `https://docs.1claw.xyz`

## Setup

### Self-enrollment

Use when the agent does not have credentials yet. Enrollment sends credentials to the human.

```bash
curl -s -X POST https://api.1claw.xyz/v1/agents/enroll \
  -H "Content-Type: application/json" \
  -d '{"name":"my-agent","human_email":"human@example.com"}'
```

```ts
import { AgentsResource } from "@1claw/sdk";

await AgentsResource.enroll("https://api.1claw.xyz", {
  name: "my-agent",
  human_email: "human@example.com",
});
```

```bash
npx @1claw/cli agent enroll my-agent --email human@example.com
```

### MCP server

Only the API key is required. Agent ID and vault are auto-discovered unless overridden.

```json
{
  "mcpServers": {
    "1claw": {
      "command": "npx",
      "args": ["-y", "@1claw/mcp"],
      "env": {
        "ONECLAW_AGENT_API_KEY": "<agent-api-key>"
      }
    }
  }
}
```

Optional env vars:

- `ONECLAW_AGENT_ID`
- `ONECLAW_VAULT_ID`

Hosted HTTP streaming mode:

- URL: `https://mcp.1claw.xyz/mcp`
- Header `Authorization: Bearer <agent-jwt>`
- Header `X-Vault-ID: <vault-uuid>`

### TypeScript SDK

```bash
npm install @1claw/sdk
```

```ts
import { createClient } from "@1claw/sdk";

const client = createClient({
  baseUrl: "https://api.1claw.xyz",
  apiKey: process.env.ONECLAW_AGENT_API_KEY,
});
```

### Direct REST API

Exchange the agent API key for a JWT, or use `1ck_` and `ocv_` keys directly as Bearer tokens.

```bash
RESP=$(curl -s -X POST https://api.1claw.xyz/v1/auth/agent-token \
  -H "Content-Type: application/json" \
  -d '{"api_key":"<key>"}')
TOKEN=$(echo "$RESP" | jq -r .access_token)
AGENT_ID=$(echo "$RESP" | jq -r .agent_id)

curl -H "Authorization: Bearer $TOKEN" https://api.1claw.xyz/v1/vaults
```

## Authentication

### Agent auth flow

1. A human creates or enrolls the agent and receives `agent_id` plus `api_key` for `api_key` auth, or just `agent_id` for mTLS/OIDC agents.
2. Agents receive an Ed25519 SSH keypair automatically. The public key is attached to the agent record, and the private key lives in the `__agent-keys` vault.
3. Exchange credentials with `POST /v1/auth/agent-token` using `{ "api_key": "<key>" }` or `{ "agent_id": "<uuid>", "api_key": "<key>" }`.
4. Use `Authorization: Bearer <jwt>` for later requests.
5. JWT scopes derive from policies. If no policies exist, access is effectively zero.
6. JWT TTL is about one hour by default, and the MCP server refreshes shortly before expiry.

### API key auth

- `1ck_` personal keys and `ocv_` agent keys can be used directly as Bearer tokens.

## MCP Tools

### `list_secrets`

List metadata only. Never returns secret values.

- `prefix` optional string path filter

### `get_secret`

Read the decrypted value for immediate use.

- `path` required string

### `put_secret`

Create or update a secret version.

- `path` required string
- `value` required string
- `type` optional string, default `api_key`
- `metadata` optional object
- `expires_at` optional ISO 8601 string
- `max_access_count` optional number, `0` means unlimited

Supported `type` values:

- `api_key`
- `password`
- `private_key`
- `certificate`
- `file`
- `note`
- `ssh_key`
- `env_bundle`

### `delete_secret`

Soft-delete a secret.

- `path` required string

### `describe_secret`

Read non-sensitive metadata only.

- `path` required string

### `rotate_and_store`

Create a new version for an existing secret.

- `path` required string
- `value` required string

### `get_env_bundle`

Fetch an `env_bundle` and parse `KEY=VALUE` lines as JSON.

- `path` required string

### `create_vault`

Create a vault.

- `name` required string
- `description` optional string

### `list_vaults`

List accessible vaults.

### `grant_access`

Create a vault path policy for a user or agent.

- `vault_id` required UUID
- `principal_type` required `user` or `agent`
- `principal_id` required UUID
- `permissions` optional string array, default `["read"]`
- `secret_path_pattern` optional string, default `**`

### `share_secret`

Create a temporary share.

- `secret_id` required UUID
- `recipient_type` required `user`, `agent`, `anyone_with_link`, or `creator`
- `recipient_id` required for `user` and `agent`
- `expires_at` required ISO 8601 string
- `max_access_count` optional number, default `5`

### `simulate_transaction`

Run an EVM transaction simulation without signing.

- `to` required address
- `value` required ETH amount string
- `chain` required chain name or chain ID
- `data` optional hex calldata
- `signing_key_path` optional, default `keys/{chain}-signer`
- `gas_limit` optional number, default `21000`

### `submit_transaction`

Submit an EVM transaction for signing and optional broadcast.

- `to` required address
- `value` required ETH amount string
- `chain` required chain name or chain ID
- `data` optional hex calldata
- `signing_key_path` optional, default `keys/{chain}-signer`
- `nonce` optional number
- `gas_price` optional wei string
- `gas_limit` optional number, default `21000`
- `max_fee_per_gas` optional wei string
- `max_priority_fee_per_gas` optional wei string
- `simulate_first` optional boolean, default `true`

## REST API Quick Reference

Base URL: `https://api.1claw.xyz`

### Auth

- `POST /v1/auth/token`
- `POST /v1/auth/agent-token`
- `POST /v1/auth/google`
- `POST /v1/auth/signup`
- `POST /v1/auth/verify-email`
- `POST /v1/auth/mfa/verify`
- `GET /v1/auth/me`
- `PATCH /v1/auth/me`
- `DELETE /v1/auth/me`
- `DELETE /v1/auth/token`
- `POST /v1/auth/change-password`

### Vaults

- `POST /v1/vaults`
- `GET /v1/vaults`
- `GET /v1/vaults/{id}`
- `DELETE /v1/vaults/{id}`
- `POST /v1/vaults/{id}/cmek`
- `DELETE /v1/vaults/{id}/cmek`
- `POST /v1/vaults/{id}/cmek-rotate`
- `GET /v1/vaults/{id}/cmek-rotate/{job_id}`

### Secrets

- `PUT /v1/vaults/{id}/secrets/{path}`
- `GET /v1/vaults/{id}/secrets/{path}`
- `DELETE /v1/vaults/{id}/secrets/{path}`
- `GET /v1/vaults/{id}/secrets?prefix=...`

### Agents

- `POST /v1/agents`
- `GET /v1/agents`
- `GET /v1/agents/{id}`
- `GET /v1/agents/me`
- `PATCH /v1/agents/{id}`
- `DELETE /v1/agents/{id}`
- `POST /v1/agents/{id}/rotate-key`
- `POST /v1/agents/{id}/rotate-identity-keys`

### Policies

- `POST /v1/vaults/{id}/policies`
- `GET /v1/vaults/{id}/policies`
- `PUT /v1/vaults/{id}/policies/{pid}`
- `DELETE /v1/vaults/{id}/policies/{pid}`

### Sharing

- `POST /v1/secrets/{id}/share`
- `GET /v1/shares/outbound`
- `GET /v1/shares/inbound`
- `POST /v1/shares/{id}/accept`
- `POST /v1/shares/{id}/decline`
- `DELETE /v1/share/{id}`
- `GET /v1/share/{id}`

### Intents API

- `POST /v1/agents/{id}/transactions`
- `GET /v1/agents/{id}/transactions`
- `GET /v1/agents/{id}/transactions/{txid}`
- `POST /v1/agents/{id}/transactions/simulate`
- `POST /v1/agents/{id}/transactions/simulate-bundle`

### Audit, billing, chains, and misc

- `GET /v1/audit/events`
- `GET /v1/billing/subscription`
- `GET /v1/billing/credits/balance`
- `GET /v1/billing/credits/transactions`
- `PATCH /v1/billing/overage-method`
- `GET /v1/billing/usage`
- `GET /v1/billing/history`
- `GET /v1/chains`
- `GET /v1/chains/{name_or_id}`
- `GET /v1/health`
- `GET /v1/health/hsm`
- `POST/GET/DELETE /v1/auth/api-keys[/{id}]`
- `GET/POST/DELETE /v1/security/ip-rules[/{id}]`
- `GET/PATCH/DELETE /v1/org/members[/{id}]`

## SDK Method Map

- `client.vaults.create`, `get`, `list`, `delete`
- `client.secrets.set`, `get`, `list`, `delete`, `rotate`
- `client.agents.create`, `get`, `list`, `update`, `delete`, `rotateKey`
- `client.agents.submitTransaction`, `simulateTransaction`, `simulateBundle`, `getTransaction`, `listTransactions`
- `client.access.grantAgent`, `grantHuman`, `listGrants`, `update`, `revoke`
- `client.sharing.create`, `access`, `listOutbound`, `listInbound`, `accept`, `decline`, `revoke`
- `client.audit.query`
- `client.billing.usage`, `history`
- `client.auth.login`, `agentToken`, `logout`
- `client.apiKeys.create`, `list`, `revoke`
- `client.chains.list`, `get`
- `client.org.listMembers`, `updateMemberRole`, `removeMember`

## OpenAPI

```bash
npm install @1claw/openapi-spec
```

```bash
npx openapi-typescript node_modules/@1claw/openapi-spec/openapi.yaml -o ./types.ts
openapi-generator generate -i node_modules/@1claw/openapi-spec/openapi.yaml -g python -o ./oneclaw-py
oapi-codegen -package oneclaw node_modules/@1claw/openapi-spec/openapi.yaml > oneclaw.go
```

## Supported Chains

- `ethereum` `1`
- `base` `8453`
- `optimism` `10`
- `arbitrum-one` `42161`
- `polygon` `137`
- `sepolia` `11155111`
- `base-sepolia` `84532`

## Access Control and Security Notes

- Policies use glob path patterns such as `api-keys/*`, `db/**`, and `**`.
- Permissions are `read` and `write`. Delete follows write permission.
- Optional conditions may include time windows or IP allowlists.
- `vault_ids` can further restrict an agent to specific vaults.
- `token_ttl_seconds` controls JWT lifetime.
- If `intents_api_enabled` is true, transaction signing is allowed but direct reads of `private_key` and `ssh_key` secrets are blocked.
- Default signing key path is `keys/{chain}-signer`.
- Use an `Idempotency-Key` header on transaction submission for replay protection.
- Server-side nonce serialization prevents duplicate nonces when `nonce` is omitted.
- Transaction GET endpoints redact `signed_tx` unless `include_signed_tx=true` is requested.
- Guardrails may restrict destination addresses, transaction value, daily spend, and allowed chains.
- Shroud can enforce prompt-injection, data-exfiltration, and PII protections in a TEE.
- CMEK vaults return encrypted blobs that require client-side decryption with the customer-managed key.

## Share With the Human

To share a secret back to the human who created or enrolled the agent, use `recipient_type: "creator"` and do not provide an email or user ID.

## Error Handling

- `400` bad request, check payload format
- `401` not authenticated, refresh or re-authenticate
- `402` quota exhausted or payment required, surface to the user
- `403` missing permission, vault restriction, or guardrail violation
- `404` not found, confirm path or ID
- `405` wrong method
- `409` conflict, such as duplicate resource
- `410` expired or exhausted secret
- `413` payload too large
- `422` validation error or reverted simulation
- `429` rate limited, wait and retry

## Best Practices

1. Fetch secrets just in time.
2. Never echo secret values.
3. Prefer metadata or discovery calls before value reads.
4. Rotate immediately after regenerating a credential.
5. Prefer `grant_access` for durable sharing and `share_secret` for temporary access.
6. Simulate before signing.
7. Check for existing vaults before creating new ones.
8. Surface billing and quota issues rather than retrying automatically.
