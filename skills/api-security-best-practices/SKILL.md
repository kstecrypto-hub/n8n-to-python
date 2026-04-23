---
name: api-security-best-practices
description: Secure API design and hardening guidance for REST, GraphQL, and WebSocket backends. Use when designing new endpoints, securing existing APIs, implementing authentication or authorization, adding request validation, rate limiting, secret handling, file upload protection, or reviewing code for OWASP API Top 10 risks and common injection vulnerabilities.
---

# API Security Best Practices

## Overview

Use this skill to turn broad security asks into concrete API changes, reviews, and checklists. Focus on practical protections that fit the stack already in use instead of proposing generic middleware dumps.

## Workflow

1. Identify the surface area.
   Determine the API style, trust boundaries, caller types, sensitive data, and whether the task is implementation, review, or remediation.
2. Pick the right identity model.
   Choose session cookies, JWTs, OAuth 2.0/OIDC, service credentials, or API keys based on first-party vs third-party use, browser vs server clients, and revocation requirements.
3. Enforce authorization at the resource level.
   Check ownership, tenant boundaries, role or permission gates, and field-level exposure for every read and write path.
4. Validate every input path.
   Validate path params, query params, headers, body payloads, uploaded files, and outbound URLs. Prefer allowlists, typed schemas, and parameterized queries.
5. Add abuse protection.
   Apply endpoint-aware rate limits, auth throttles, idempotency where needed, pagination or size limits, and safe error handling.
6. Protect data and observability.
   Minimize returned fields, scrub secrets from logs, use secure headers, and avoid leaking internals through error responses.
7. Verify with targeted tests.
   Add or recommend tests for unauthorized access, malformed input, brute force attempts, overbroad field access, and rate-limit behavior.

## Decision Rules

- Prefer server-side sessions or secure cookies for first-party browser apps when feasible.
- Prefer short-lived access tokens plus rotating refresh tokens when stateless auth is required.
- Treat API keys as application identity, not end-user identity.
- Separate authentication from authorization; being logged in is never enough.
- Deny by default when tenant, owner, or scope checks are ambiguous.
- Use schema validation close to the transport boundary before business logic runs.
- Use parameterized queries or ORM filters for all data access.
- Return generic auth errors and avoid revealing whether an account exists.
- Rate-limit login, password reset, OTP, token refresh, search, export, and write-heavy endpoints more strictly than general reads.

## Common Deliverables

- Secure endpoint implementations and middleware
- Auth flow recommendations with tradeoffs
- Validation schemas and sanitization rules
- Rate-limiting and throttling configuration
- API security review findings mapped to concrete code paths
- Audit checklists aligned to OWASP API Top 10 themes

## Reference Guide

Read [references/api-security-guide.md](./references/api-security-guide.md) when you need implementation patterns, example middleware, checklists, or review prompts for:

- JWT login and verification
- RBAC and object-level authorization
- Zod-style request validation
- SQL injection prevention
- Rate limiting and auth throttling
- Helmet and transport security defaults
- API review checklists and OWASP mapping
