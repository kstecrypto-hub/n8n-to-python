# Prompts and Output Contracts

## Table of contents
1. Why contracts matter
2. Default extraction contract
3. Prompt pattern for entity/relation extraction
4. Prompt pattern for ontology mapping
5. Prompt pattern for entity resolution adjudication
6. Guardrails for LLM-based extraction
7. What Codex should generate for the user

## 1. Why contracts matter

A common ingestion failure is asking an LLM to “extract a knowledge graph” in free text and then trying to parse whatever comes back. Do not do that.

Use strict intermediate contracts so the downstream pipeline can validate and repair outputs.

## 2. Default extraction contract

Use or adapt this structure:

```json
{
  "source_id": "string",
  "segment_id": "string",
  "mentions": [
    {
      "mention_id": "string",
      "text": "string",
      "type_hint": "string",
      "start": 0,
      "end": 0,
      "confidence": 0.0
    }
  ],
  "candidate_entities": [
    {
      "candidate_id": "string",
      "mention_ids": ["string"],
      "proposed_type": "string",
      "canonical_name": "string",
      "external_ids": [],
      "confidence": 0.0
    }
  ],
  "candidate_relations": [
    {
      "relation_id": "string",
      "subject_candidate_id": "string",
      "predicate_text": "string",
      "object_candidate_id": "string",
      "qualifiers": {},
      "confidence": 0.0
    }
  ],
  "candidate_events": [],
  "evidence": [
    {
      "evidence_id": "string",
      "supports": ["relation_id"],
      "excerpt": "string"
    }
  ]
}
```

## 3. Prompt pattern for entity/relation extraction

Prompt the model to extract candidates, not final truth.

Suggested pattern:

```text
You are extracting candidate graph objects from one source segment.
Return JSON only and follow the schema exactly.

Rules:
- Extract only what is directly supported by the segment.
- Separate mentions from canonical entities.
- Do not merge entities unless the segment itself provides a stable identifier.
- Preserve relation text exactly in predicate_text before normalization.
- Include qualifiers such as date, role, dosage, location, negation, or uncertainty when present.
- If evidence is weak, lower confidence instead of inventing details.
```

## 4. Prompt pattern for ontology mapping

After candidate extraction, run a controlled mapping step.

```text
Map the extracted candidate types and predicate_text values to the allowed ontology inventory.

Allowed classes:
[...]

Allowed predicates:
[...]

Rules:
- Use only allowed class and predicate names.
- If no reliable mapping exists, mark the item as unmapped.
- Do not create new ontology terms.
- Preserve the original extracted text separately.
```

## 5. Prompt pattern for entity resolution adjudication

Use this only for ambiguous cases, not the entire corpus.

```text
Given two candidate entity records and their evidence contexts, decide whether they refer to the same canonical entity.
Return one of: same_entity, different_entity, insufficient_evidence.
Explain the decision in structured fields, not long prose.
```

Require the model to cite the exact signals it used:
- identifier match
- alias match
- context match
- type mismatch
- conflicting attributes

## 6. Guardrails for LLM-based extraction

- Never let the model invent ontology terms during production mapping.
- Prefer schema-constrained JSON output.
- Keep the prompt versioned.
- Log raw model output before repair.
- Validate every response before graph writes.
- Quarantine malformed or unsupported outputs.
- Use deterministic post-processors for dates, units, ids, and enums.

## 7. What Codex should generate for the user

When asked to implement the ingestion mechanism, generate some or all of the following:

- pipeline architecture diagram in text form
- typed JSON schemas for intermediate stages
- normalization rules
- ontology mapping table
- pseudocode or actual code scaffold
- graph write examples for the chosen stack
- test fixtures
- acceptance criteria
- failure-mode checklist

The implementation should be explicit about:
- ids
- provenance
- confidence
- resolution policy
- update/replay behavior
- validation before persistence
