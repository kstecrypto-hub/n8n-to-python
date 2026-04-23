---
name: kg-ingestion-architect
description: design and implement automatic knowledge graph ingestion mechanisms for agents from text, documents, tables, events, and semi-structured data. use when chatgpt needs to explain how knowledge graphs work only insofar as needed to build them, or when it must create entity extraction, relation extraction, ontology alignment, canonicalization, provenance tracking, validation, incremental ingestion, and persistence logic for neo4j, amazon neptune, rdf/owl, or hybrid graph pipelines.
---

# KG Ingestion Architect

Build the ingestion mechanism, not just the graph model.

This skill is for tasks where the target system must automatically convert source material into a durable, queryable knowledge graph that an agent can trust. Teach the implementation through the minimum theory required: what a knowledge graph is, what entities and relations are, how ontologies constrain meaning, and how extraction, normalization, and persistence fit into one pipeline.

## Working mode

Use this sequence unless the user explicitly asks for something narrower:

1. **Clarify the graph objective**
   Determine what the agent must do with the graph: retrieval, reasoning, memory, recommendations, event tracking, lineage, compliance, biomedical linking, or tool orchestration.
2. **Choose graph style**
   Decide whether the problem is best modeled as a property graph, RDF/OWL graph, or a hybrid.
3. **Define the ingestion contract**
   Specify the input types, extracted objects, identifiers, ontology/schema mapping rules, provenance model, confidence model, and output format.
4. **Design the extraction pipeline**
   Break the ingestion flow into parsing, segmentation, extraction, normalization, entity resolution, validation, and upsert.
5. **Generate implementation artifacts**
   Produce architecture, schemas, extraction prompts, pseudocode, code scaffolds, storage logic, and test cases.
6. **Add graph-quality controls**
   Include idempotency, duplicate handling, evidence linking, temporal semantics, and monitoring.

## Decision logic

### 1) Decide what kind of graph is needed

Use a **property graph** when the agent mainly needs operational traversal, application-centric queries, flexible node/edge properties, and easier implementation in systems like Neo4j or Neptune property graph mode.

Use **RDF/OWL** when the agent needs interoperable semantics, explicit vocabularies, formal classes and properties, ontology reuse, linked data integration, or SPARQL-based reasoning.

Use a **hybrid design** when operational ingestion and application queries are easiest in a property graph, but canonical exchange semantics or ontology-backed validation need RDF/OWL.

Do not force RDF because the topic sounds semantic. Do not force property graphs because engineering feels simpler. Choose based on retrieval, reasoning, interoperability, validation, and maintenance requirements.

### 2) Decide what the graph stores

Separate these layers clearly:

- **World entities**: persons, organizations, drugs, systems, contracts, devices, concepts, places, genes, papers, policies.
- **Events/facts**: admissions, transactions, measurements, deployments, approvals, diagnoses, claims, authorship, observations.
- **Documents and source artifacts**: files, pages, paragraphs, messages, database rows, APIs, transcripts.
- **Ontology/schema objects**: classes, predicates, allowed types, constraints, controlled vocabularies.
- **Evidence and provenance**: exact span, source URI, extraction job, model version, timestamp, confidence.

A common failure is collapsing all layers into one. Do not represent documents, entities, and claims as if they are the same thing.

### 3) Decide extraction granularity

Choose the smallest unit that preserves meaning and provenance:

- For free text: often sentence, clause, or passage.
- For PDFs or reports: section + paragraph + sentence span.
- For tables: row or cell range, plus header context.
- For event streams: one event record.
- For APIs: one returned object or one logical fact bundle.

The extraction unit controls provenance quality, deduplication behavior, and reprocessing cost.

## Required output structure

When using this skill, produce results in this default structure unless the user asks for code only:

1. **Objective and graph type**
2. **Conceptual model**
   - entities
   - relations
   - events/facts
   - evidence/provenance
   - ontology/schema layer
3. **Ingestion architecture**
4. **Extraction contract**
5. **Canonicalization and entity resolution rules**
6. **Storage model**
7. **Validation and quality checks**
8. **Implementation plan or code scaffold**
9. **Failure modes and mitigations**

## Non-negotiable design rules

- Always model **provenance**. Every important graph assertion should be traceable to source evidence.
- Prefer **idempotent upserts** over blind inserts.
- Distinguish **canonical entities** from **mentions** extracted from raw text.
- Distinguish **relations in the world** from **claims in a document**.
- Make temporal assumptions explicit. Facts often have valid time and ingestion time.
- Separate **ontology alignment** from **entity resolution**. They are related but not the same operation.
- Record extraction confidence, but do not treat confidence as truth.
- Preserve source identifiers and external vocabulary identifiers when available.
- Use stable node identifiers. Human-readable names are labels, not primary keys.
- Design for reprocessing. Source parsers, ontologies, and extraction prompts will change.

## Delivery guidance

### If the user asks for theory

Explain the theory only to support implementation. Use the reference files below:

- `references/concepts-and-modeling.md`
- `references/ontology-and-schema.md`

### If the user asks for an ingestion mechanism or automation

Use these references first:

- `references/ingestion-pipeline.md`
- `references/extraction-normalization.md`
- `references/implementation-patterns.md`
- `references/prompts-and-output-contracts.md`

### If the user asks for examples

Provide examples in the target stack when possible:

- property graph: node labels, relationship types, Cypher, upsert rules
- rdf/owl: classes, object/datatype properties, IRIs, SHACL/SPARQL examples
- hybrid: canonical mapping between both

## Minimum conceptual model to teach before coding

Before generating code, make sure the answer is grounded in these concepts:

- A **knowledge graph** is a structured representation of entities, relations, events, and evidence arranged as nodes and edges or as subject-predicate-object assertions.
- An **entity** is a thing with identity that persists across mentions or records.
- A **relation** links entities or entities to values and must have typed meaning.
- An **ontology** defines the semantic vocabulary: classes, properties, domain/range expectations, and sometimes logical constraints.
- **Extraction** finds candidate entities, relations, attributes, and events from source material.
- **Canonicalization** converts inconsistent surface forms into normalized values.
- **Entity resolution** decides whether two mentions refer to the same real-world entity.
- **Persistence** stores the accepted graph assertions in a durable graph model.
- **Validation** checks structural, semantic, and operational correctness.

If any of these are missing, the answer is usually too shallow to implement a reliable ingestion mechanism.

## Implementation preferences

When generating code or scaffolding:

- Prefer explicit schemas and typed JSON contracts between stages.
- Prefer small, testable pipeline stages over one monolithic extractor.
- Prefer deterministic normalization before LLM-based reasoning where possible.
- Prefer storing both raw extraction output and accepted graph assertions.
- Prefer a reviewable intermediate representation before final graph writes for high-stakes domains.

## What to avoid

Avoid these anti-patterns unless the user explicitly requests them:

- dumping raw triples without provenance
- using names as unique identifiers
- merging all aliases automatically
- treating ontology classes as the same thing as extracted nodes
- encoding every noun phrase as an entity
- building relation types from arbitrary free text without normalization
- skipping mention-to-entity linking
- hiding uncertainty instead of modeling it
- writing directly to the production graph without staging or validation

## Reference map

- `references/concepts-and-modeling.md`: what knowledge graphs are, how entities, relations, events, facts, mentions, and evidence differ, and how to model them for agents
- `references/ontology-and-schema.md`: ontology, schema, RDF/OWL, property graphs, SHACL-like thinking, and mapping rules
- `references/ingestion-pipeline.md`: end-to-end ingestion architecture and lifecycle
- `references/extraction-normalization.md`: extraction design, canonicalization, entity resolution, confidence, and provenance
- `references/implementation-patterns.md`: implementation templates, persistence patterns, and stack-specific guidance
- `references/prompts-and-output-contracts.md`: prompt patterns and intermediate JSON contracts for extraction agents
