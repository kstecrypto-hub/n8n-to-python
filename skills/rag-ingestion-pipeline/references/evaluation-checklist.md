# Evaluation Checklist

Use this checklist before declaring the ingestion pipeline done.

## 1. Extraction Quality

- Are OCR, layout, and table extraction errors low enough for downstream chunking?
- Were headers, footers, and boilerplate removed without deleting meaningful content?
- Are page spans and document hierarchy preserved?

## 2. Chunk Quality

- Are chunks semantically coherent?
- Do chunk boundaries preserve entity and relation continuity?
- Are temporal or workflow sequences preserved?
- Are atomic evidence units retrievable on their own?
- Do parent, child, previous, and next links exist where needed?

## 3. Metadata Quality

- Does every chunk have stable IDs and provenance?
- Are canonical terms and surface terms both available when needed?
- Are time, location, confidence, and uncertainty fields present in domains that need them?
- Are quality flags present for noisy or low-confidence extracts?

## 4. Ontology Or KG Quality

- Are aliases and acronyms mapped correctly?
- Are entity and relation nodes linked back to chunks?
- Is graph construction incremental and update-friendly?
- Are workflow or timeline nodes aligned with real source evidence?

## 5. Retrieval Quality

Measure retrieval directly.

Preferred metrics:

- `nDCG@10`
- `Recall@10`
- `Recall@100`
- `Success@100`

Add task metrics when relevant:

- exact match
- completeness
- faithfulness
- traceability
- relevance judged by domain experts

## 6. Regression Suite

Test at least:

- alias or acronym queries
- cross-section queries
- multi-hop queries
- temporal queries
- ambiguous entity queries
- low-resource or sparse terminology queries

## 7. Update Safety

- Can new data be ingested without full reprocessing?
- Can ontology, embedder, and chunker versions be tracked?
- Can bad enrichment or bad canonicalization be rolled back?

## 8. Ship Gate

Do not ship if any of these fail:

- provenance is missing
- chunk boundaries destroy core relations
- ontology enrichment adds uncontrolled noise
- retrieval metrics improved only on easy queries
- updates require destructive reindexing by default
