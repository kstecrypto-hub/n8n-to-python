# Final Ingest Checklist

Date: 2026-04-07

## Inputs
- Ontology assumed finalized
- Corpus source: root PDF set under `E:\n8n to python`
- Runner: `tools/reingest_all_pdfs.py`

## Preconditions
- Containers up
- API health `200`
- Pre-reset overview captured
- Pre-reset DB dump captured

## Reset scope
- Corpus ingestion tables
- KG tables
- Agent replay/history/memory/profile state in main app DB
- Chroma chunk and asset collections
- Derived page asset files
- Keep auth users, auth sessions, sensors, hives, places, and sensor readings intact

## Validation targets
- Runner reaches terminal success
- `completed_files = total_files`
- `error_files = 0`
- Post-ingest overview captured
- Chroma parity checked
- Agent query still works for corpus questions
- Sensor path still works after corpus ingest
