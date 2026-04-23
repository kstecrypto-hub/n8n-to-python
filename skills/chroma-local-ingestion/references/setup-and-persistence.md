# Setup and Persistence

## Table of contents
1. Local installation choices
2. Embedded local persistence
3. Local server mode
4. Ephemeral mode
5. Persistence path discipline
6. Process and concurrency constraints
7. Collection lifecycle rules
8. Local-only embedding guidance

## 1. Local installation choices

For local Python usage, the baseline installation is the Chroma package.

Default local-first modes to consider:
- embedded in-memory for short-lived testing
- embedded persistent for normal local development or local production prototypes
- local server if the architecture needs an explicit service boundary

## 2. Embedded local persistence

Embedded persistent mode is the default choice when everything must run locally and the application can read and write the persistence path directly.

Mental model:
- the app constructs a `PersistentClient`
- a local directory is used as the persistence path
- collections and their records are stored there
- the same application process performs ingestion and retrieval

Use this mode for:
- desktop tools
- local copilots
- single-process ingestion workers
- local prototypes that need persistence between runs

## 3. Local server mode

A locally hosted Chroma server is appropriate when:
- multiple local clients need to connect
- the app wants a network boundary
- different processes or languages should not touch the persistence path directly

In this mode:
- the server runs locally
- the application typically connects via HTTP client
- persistence still remains local to the machine

## 4. Ephemeral mode

In-memory or ephemeral mode is useful for:
- tests
- quick experiments
- debug runs where persistence should be discarded

Do not design the main ingestion pipeline around ephemeral mode if the agent needs durable retrieval memory.

## 5. Persistence path discipline

A local path is part of the architecture, not an afterthought.

Define explicitly:
- where data lives
- who owns that path
- whether it is environment-specific
- backup/cleanup policy
- migration policy for re-indexes or collection replacements

Avoid hidden defaults when reproducibility matters.

## 6. Process and concurrency constraints

Local Chroma usage has an important operational constraint: thread usage is easier than multi-process shared writers on the same persistence path.

Design assumptions:
- multiple threads in one process are usually easier to reason about than multiple independent processes writing to the same local path
- if the system needs true multi-process writing or service separation, prefer a local server boundary instead of many independent embedded writers

Do not casually point many independent writers at the same embedded persistence path and assume it will behave like a multi-writer distributed database.

## 7. Collection lifecycle rules

Important lifecycle facts for implementation:
- collection names are unique within a database
- collection identity should reflect a real retrieval boundary
- once embeddings have been written, effective embedding dimensionality must stay compatible
- changing embedding model strategy is typically a re-index event, not an in-place toggle

Treat collection creation as a schema decision.

## 8. Local-only embedding guidance

If everything must remain local, avoid cloud embedding APIs in the ingestion design.

Local options include:
- Chroma’s local default embedding path where appropriate
- an embedding model run locally in the application
- a local embedding server reachable on localhost

The key rule is consistency:
- same embedding model family for ingestion and query
- same dimensionality
- same normalization and text preparation assumptions

If the embedding model changes materially, create a new collection and rebuild.
