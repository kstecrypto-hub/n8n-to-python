# API Security Guide

## Contents

- Authentication and authorization
- Input validation and injection defenses
- Rate limiting and abuse controls
- Data protection and error handling
- Security review checklist
- OWASP API Top 10 mapping

## Authentication and authorization

Use authentication to establish identity, then perform separate authorization checks for each operation.

Pick the auth mechanism by client type:

- Use secure server-side sessions or HttpOnly cookies for first-party web apps when possible.
- Use OAuth 2.0 or OpenID Connect for delegated third-party access.
- Use short-lived JWT access tokens with rotating refresh tokens for stateless APIs that need them.
- Use API keys for service or application identity, not as a substitute for user auth.

JWT guidance:

- Keep access tokens short-lived.
- Include issuer and audience checks.
- Do not put secrets, PII, or mutable authorization state in the token body.
- Support refresh token rotation and revocation.
- Store refresh tokens server-side or in a revocation-aware store.

Authorization guidance:

- Check object ownership on every resource fetch, update, and delete.
- Enforce tenant scoping in every query, not just in routing.
- Restrict writable and readable fields separately.
- Validate admin-only and back-office actions with explicit role or permission checks.

Example review prompt:

```text
For each endpoint, answer:
1. Who can call it?
2. Which resource instances can they access?
3. Which fields can they read or mutate?
4. What happens if tenant or owner IDs are tampered with?
```

## Input validation and injection defenses

Validate all external input before business logic:

- Path parameters
- Query strings
- Headers
- JSON or form bodies
- File uploads
- Webhook payloads
- Outbound URLs used for fetches or callbacks

Rules:

- Prefer typed schemas such as Zod or Joi at the HTTP boundary.
- Use allowlists for enums, sort keys, filter fields, and upload types.
- Enforce request size limits and pagination caps.
- Sanitize or reject HTML when rich text is allowed.
- Never build SQL, shell commands, or template expressions by concatenating user input.

SQL injection prevention:

```js
const userId = Number(req.params.id);
if (!Number.isInteger(userId)) {
  return res.status(400).json({ error: "Invalid user ID" });
}

const user = await prisma.user.findUnique({
  where: { id: userId },
  select: { id: true, email: true, name: true }
});
```

Request validation pattern:

```js
const { z } = require("zod");

const createUserSchema = z.object({
  email: z.string().email(),
  password: z.string().min(12),
  name: z.string().min(2).max(100)
});

function validateBody(schema) {
  return (req, res, next) => {
    const result = schema.safeParse(req.body);
    if (!result.success) {
      return res.status(400).json({
        error: "Validation failed",
        details: result.error.flatten()
      });
    }
    req.body = result.data;
    next();
  };
}
```

File upload checks:

- Enforce MIME type and extension allowlists.
- Validate size limits before processing.
- Generate server-side filenames.
- Scan files if the risk profile requires it.
- Do not trust client-provided content type metadata.

SSRF checks for user-supplied URLs:

- Allow only required protocols.
- Resolve and block private or link-local IP ranges if the use case requires public fetches only.
- Apply timeouts, redirect limits, and response size limits.

## Rate limiting and abuse controls

Apply different controls for different endpoint classes:

- Auth endpoints: strict per IP and per account throttles.
- Search and exports: lower burst limits plus pagination caps.
- Writes and side effects: idempotency keys where retries are possible.
- Public APIs: quotas by key, tenant, or account.

Recommended behaviors:

- Return standard rate-limit headers.
- Use a centralized store such as Redis when running multiple instances.
- Track suspicious patterns separately from normal product analytics.
- Avoid locking out users permanently because of attacker-triggered failures.

Example Express rate limiting:

```js
const rateLimit = require("express-rate-limit");

const apiLimiter = rateLimit({
  windowMs: 15 * 60 * 1000,
  max: 100,
  standardHeaders: true,
  legacyHeaders: false
});

const authLimiter = rateLimit({
  windowMs: 15 * 60 * 1000,
  max: 5,
  skipSuccessfulRequests: true
});

app.use("/api", apiLimiter);
app.use("/api/auth/login", authLimiter);
```

## Data protection and error handling

Protect data in transit and at rest:

- Require HTTPS and secure cookies.
- Encrypt especially sensitive data at rest where appropriate.
- Keep secrets in environment or secret managers, not source control.
- Minimize returned fields and exclude password hashes, tokens, internal flags, and hidden metadata.

Error handling rules:

- Return generic authentication errors such as `Invalid credentials`.
- Hide stack traces and database internals in production.
- Log enough for diagnosis, but redact tokens, secrets, and personal data.
- Normalize authorization failures to avoid revealing whether a resource exists when that matters.

Secure headers:

- Use `helmet` or the framework equivalent.
- Configure HSTS when HTTPS is fully in place.
- Set a restrictive CORS policy.
- Deny framing unless embedding is explicitly required.

Example:

```js
const helmet = require("helmet");

app.use(helmet({
  frameguard: { action: "deny" },
  hidePoweredBy: true,
  noSniff: true
}));
```

## Security review checklist

Authentication and authorization:

- Is every non-public endpoint authenticated?
- Is authorization enforced per resource and per action?
- Are tenant boundaries enforced in database queries?
- Are high-risk actions protected by stricter permissions or re-auth when needed?

Validation and injection:

- Are all request inputs validated with typed schemas or strict parsing?
- Are query builders, ORM filters, sort fields, and raw SQL protected from user-controlled injection?
- Are uploads restricted by size and type?
- Are outbound URLs validated against SSRF risks?

Abuse protection:

- Are login, reset, OTP, and token endpoints throttled?
- Are expensive endpoints bounded by quotas, pagination, and timeouts?
- Are write operations resilient to retries and duplicate submissions?

Data exposure:

- Do responses exclude sensitive or internal-only fields?
- Are logs redacted?
- Do error messages avoid leaking stack traces, SQL text, or account existence?

Operations:

- Are dependencies current enough for known security fixes?
- Are secrets rotated and scoped?
- Are alerts or logs available for repeated auth failures and anomalous traffic?

## OWASP API Top 10 mapping

Use these prompts during reviews:

1. Broken object level authorization
   Verify that resource IDs cannot be swapped to access another user's data.
2. Broken authentication
   Check token validation, session handling, password reset, MFA, and credential stuffing resistance.
3. Broken object property level authorization
   Ensure hidden or admin-only fields are not readable or writable through mass assignment.
4. Unrestricted resource consumption
   Bound request rate, payload size, concurrency, and expensive query paths.
5. Broken function level authorization
   Verify that privileged endpoints enforce role or permission checks server-side.
6. Unrestricted access to sensitive business flows
   Protect checkout, signup abuse, referral abuse, and other high-value workflows.
7. Server-side request forgery
   Validate outbound destinations and restrict network reachability where possible.
8. Security misconfiguration
   Review headers, CORS, environment config, debug flags, and default credentials.
9. Improper inventory management
   Identify undocumented, deprecated, or shadow endpoints and versions.
10. Unsafe consumption of APIs
   Validate and sanitize data from third-party APIs before trusting or rendering it.
