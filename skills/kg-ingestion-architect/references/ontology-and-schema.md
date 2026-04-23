# Ontology and Schema

## Table of contents
1. What ontology means in practice
2. Ontology vs schema vs extraction schema
3. Classes, properties, and constraints
4. Mapping extracted data to ontology
5. Property graph schema design
6. RDF/OWL design basics
7. SHACL-style validation mindset
8. Controlled vocabularies and external identifiers
9. Minimal ontology strategy for ingestion
10. Anti-patterns

## 1. What ontology means in practice

In implementation terms, an ontology is the semantic contract that says what kinds of things exist in the graph, what they can be related to, and what those relations mean.

Ontology answers questions like:
- What counts as a `Person` vs an `Organization`?
- Can `TREATS` connect `Drug -> Disease`, or also `Procedure -> Symptom`?
- Is `worksFor` the same as `employedBy`, or should one normalize to the other?
- Are `DiagnosisEvent` and `Condition` different classes?

Without ontology or schema, extraction pipelines drift. The same input may produce inconsistent node types and edge names over time.

## 2. Ontology vs schema vs extraction schema

These are related but different layers.

### Ontology
Semantic model of classes, properties, and meaning.

Examples:
- `Person`, `Organization`, `Publication`
- `authorOf`, `memberOf`, `locatedIn`
- domain/range expectations
- subclass relationships

### Storage schema
How the graph database stores those things.

Examples:
- Neo4j labels and constraints
- RDF namespaces and named graphs
- edge properties for confidence and provenance

### Extraction schema
The intermediate contract the extractor must produce.

Examples:
```json
{
  "mentions": [...],
  "candidate_entities": [...],
  "candidate_relations": [...],
  "events": [...],
  "evidence": [...]
}
```

The extraction schema should be stricter than free-form natural language and usually simpler than the full ontology.

## 3. Classes, properties, and constraints

### Classes
Classes define types of nodes or individuals.

Examples:
- `Person`
- `Organization`
- `Drug`
- `Disease`
- `Study`
- `ObservationEvent`

### Properties
Properties define valid relations or attributes.

Examples:
- object-like: `AUTHORED_BY`, `TREATS`, `PART_OF`
- literal-like: `hasName`, `hasDate`, `hasDose`

### Constraints
Constraints reduce ambiguity.

Examples:
- `TREATS` should usually connect treatment entities to conditions.
- `Person` should not have `hasDosage` unless the model explicitly supports that.
- `publishedOn` should be a date or datetime.

Constraints can be implemented through:
- code-level validation
- Neo4j constraints and application rules
- SHACL-like rule sets
- ontology domain/range expectations

## 4. Mapping extracted data to ontology

Ontology alignment is the step that transforms raw extracted candidates into accepted graph vocabulary.

Example raw extraction:
- entity type: `company`
- relation: `works at`

Ontology-mapped output:
- class: `Organization`
- predicate: `EMPLOYED_BY`

Typical mapping tasks:
- synonym collapse: `company`, `business`, `firm` -> `Organization`
- predicate normalization: `is ceo of`, `heads`, `leads` -> maybe `HAS_EXECUTIVE_ROLE` plus role node
- event normalization: `was diagnosed with` -> `DiagnosisEvent`

Do not let the extractor invent arbitrary class names or predicates at write time. Map candidates into a controlled inventory.

## 5. Property graph schema design

A useful property graph schema for ingestion usually contains:

### Canonical entity labels
- `Person`
- `Organization`
- `Product`
- `Document`
- `Concept`

### Mention/evidence labels
- `Mention`
- `SourceSpan`
- `ExtractionRun`

### Event labels
- `DeploymentEvent`
- `DiagnosisEvent`
- `TransactionEvent`

### Common relation families
- world facts: `WORKS_FOR`, `TREATS`, `LOCATED_IN`
- source layer: `MENTIONS`, `HAS_SPAN`, `EXTRACTED_FROM`
- provenance layer: `ASSERTED_IN`, `EVIDENCED_BY`, `GENERATED_BY_RUN`
- identity layer: `REFERS_TO`, `SAME_AS_CANDIDATE`

### Constraint examples
- unique constraint on canonical ids
- index on normalized names and external ids
- unique key for source objects using source system ids

## 6. RDF/OWL design basics

When designing RDF/OWL-oriented ingestion:

- Use stable IRIs for canonical resources.
- Keep namespaces explicit.
- Distinguish classes from individuals.
- Use object properties for entity-to-entity links.
- Use datatype properties for literal values.
- Consider named graphs for provenance or source partitioning.

Simple pattern:
- `ex:person_123 rdf:type ex:Person`
- `ex:paper_456 ex:authoredBy ex:person_123`
- `ex:paper_456 ex:title "Knowledge Graphs in Practice"`

OWL can encode richer semantics, but the ingestion system should not depend on full logical inference unless the use case truly requires it.

## 7. SHACL-style validation mindset

Even if SHACL is not implemented directly, think in SHACL-like terms:

- what node shapes exist?
- what predicates are allowed?
- what value types are required?
- what cardinalities make sense?
- what cross-field dependencies must hold?

Examples:
- `Person` must have at least one identifier or normalized name.
- `DiagnosisEvent` must have patient, condition, and date or evidence span.
- `TREATS` should not point to a literal string.

## 8. Controlled vocabularies and external identifiers

When a domain has external vocabularies, prefer linking to them rather than inventing local meanings.

Examples:
- SNOMED CT
- ICD
- MeSH
- UMLS CUIs
- ORCID
- DOI
- GEO identifiers
- Wikidata Q IDs

Preserve both:
- local canonical id
- external vocabulary ids

This supports reconciliation, interoperability, and re-ingestion from better data sources later.

## 9. Minimal ontology strategy for ingestion

Do not start with an encyclopedic ontology if the user asked for an operational ingestion mechanism. Start minimal:

1. Identify the agent’s key questions and actions.
2. Identify the smallest durable class set.
3. Define the allowed relation inventory.
4. Define evidence/provenance objects.
5. Define the normalization rules.
6. Expand only when repeated source patterns justify it.

A minimal, stable ontology is usually better than a comprehensive but unusable one.

## 10. Anti-patterns

- confusing label names with ontology design
- creating a new predicate for every phrasing variant
- allowing extractor-generated free-text types into production
- overloading one class to represent entity, mention, and source row
- adopting a massive ontology with no mapping strategy
