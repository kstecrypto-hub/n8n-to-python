# Bee Ingestion Pipeline

Local-first ingestion pipeline for:

- source registration
- parsing and logical chunking
- chunk validation and quarantine
- Chroma indexing
- staged knowledge-graph extraction and validation

This project rebuilds ingestion only. It does not include the chat agent.

Internal admin UI:

- API health: `http://localhost:18000/health`
- Admin dashboard: `http://localhost:18000/admin`
