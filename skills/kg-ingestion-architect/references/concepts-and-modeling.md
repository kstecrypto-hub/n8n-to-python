# Concepts and Modeling

## Table of contents
1. What a knowledge graph is
2. Why agents need a graph instead of raw chunks alone
3. Core modeling objects
4. Entities, mentions, values, events, and facts
5. Relation semantics
6. Property graphs vs RDF/OWL
7. How documents and evidence fit into the model
8. Modeling principles for automatic ingestion
9. Common mistakes

## 1. What a knowledge graph is

A knowledge graph is not merely “data in nodes and edges.” It is a structured semantic memory system in which identity, meaning, and links are explicit enough that machines can traverse, validate, and reuse them.

At minimum, a knowledge graph has:

- **nodes or subjects/objects** representing things, events, documents, or concepts
- **typed edges or predicates** representing meaningfully named relationships
- **identifiers** that distinguish one thing from another
- **a schema or ontology layer** that constrains interpretation
- **evidence/provenance** connecting graph assertions back to their source material

For agents, a knowledge graph matters when raw vector retrieval is insufficient. The graph lets the system answer questions like:

- What is connected to what?
- Is this the same entity as before?
- Which claim came from which source?
- What changed over time?
- Which facts are inferred, extracted, or manually verified?
- Which tools or workflows should run based on the graph state?

## 2. Why agents need a graph instead of raw chunks alone

Vector search works well for semantic recall, but it does not guarantee identity, explicit relation semantics, or cross-source consolidation. A graph adds:

- **identity**: many mentions can resolve to one canonical entity
- **structure**: relations are typed instead of implied by nearby text
- **provenance**: every accepted fact can point to source evidence
- **constraints**: classes and allowed predicates narrow ambiguity
- **reasoning hooks**: traversal, inference, path constraints, lineage, explainability
- **incremental memory**: new ingested data can update or extend the same canonical object

The correct pattern for many agents is not graph *instead of* retrieval, but graph *plus* retrieval. The graph stores structured memory and relations; the raw text store preserves nuance and long-form context.

## 3. Core modeling objects

A practical ingestion-oriented graph usually needs five distinct object types:

### A. Canonical entities
These are the durable real-world or conceptual things the agent should remember across documents.

Examples:
- person
- organization
- drug
- disease
- server
- policy
- course
- device
- contract

### B. Mentions
A mention is the appearance of an entity in a specific source span.

Examples:
- “IBM” in paragraph 4
- “International Business Machines” in page 2
- “Dr. Smith” in a transcript line

A mention is not automatically a canonical entity. Mentions must often be resolved.

### C. Values/attributes
Some extracted items are not entities but literal values.

Examples:
- date of birth
- dosage amount
- version number
- latitude
- risk score

Do not reify every value into a node unless the use case requires it.

### D. Events or observations
Many important facts are event-shaped rather than entity-shaped.

Examples:
- patient admitted
- contract signed
- model deployed
- user logged in
- study reported outcome

Event modeling often produces better temporal accuracy than flattening everything into direct entity-to-entity relations.

### E. Evidence/provenance objects
Evidence connects an extracted or asserted fact to where it came from.

Examples:
- source document id
- page number
- section heading
- character span
- row id
- API response id
- ingestion job id
- extractor model version

## 4. Entities, mentions, values, events, and facts

A reliable ingestion mechanism should distinguish these clearly.

### Entity
A thing with identity that can persist across multiple sources.

Example:
- Canonical entity: `Organization#ibm`

### Mention
A source-local surface form that may refer to an entity.

Example:
- Mention text: “IBM Corp.” from document A
- Mention text: “International Business Machines” from document B

Both might link to the same canonical entity, but only after resolution.

### Fact
A machine-usable assertion.

Examples:
- `DrugX TREATS DiseaseY`
- `Paper123 AUTHORED_BY Person789`
- `Server42 HAS_IP "10.0.0.1"`

### Event
A fact with time, participants, and possibly state transition.

Examples:
- `DeploymentEvent#2026-03-16-app42`
- `DiagnosisEvent#encounter-abc`

When time matters, event nodes are often superior to direct edges.

## 5. Relation semantics

A relation must have typed meaning. “related_to” is rarely enough.

Good relation design:
- `AUTHORED_BY`
- `LOCATED_IN`
- `PART_OF`
- `TREATS`
- `MENTIONS`
- `EVIDENCED_BY`
- `OBSERVED_IN`

Weak relation design:
- `CONNECTED_TO`
- `ASSOCIATED_WITH`
- `LINKED`

Use generic relations only when the source truly does not justify a stronger predicate and when downstream logic can tolerate that ambiguity.

Relation design rules:
- Prefer domain-specific verbs or predicates.
- Define direction intentionally.
- Record provenance for non-trivial assertions.
- Separate factual relations from mention/evidence relations.

## 6. Property graphs vs RDF/OWL

### Property graph

A property graph has nodes and edges with properties on both.

Strengths:
- operational simplicity
- intuitive traversal
- easy application integration
- natural fit for agent memory and workflow state
- convenient for storing provenance and metadata on edges

Typical concepts:
- labels: `Person`, `Organization`, `Document`
- relation types: `WORKS_FOR`, `MENTIONS`, `CITES`
- properties: `name`, `source_id`, `confidence`

### RDF/OWL

RDF represents assertions as triples: subject, predicate, object.
OWL extends RDF with richer ontology semantics.

Strengths:
- explicit standardized semantics
- ontology reuse and linked data integration
- IRI-based identity
- SPARQL querying
- compatibility with formal validation and reasoning ecosystems

Typical concepts:
- class
- individual
- object property
- datatype property
- IRI
- domain/range
- subclass

### Practical selection rule

For agent ingestion systems, the question is not which is philosophically superior. The question is which representation best matches the agent’s retrieval, reasoning, and maintenance needs.

## 7. How documents and evidence fit into the model

Do not ingest structured facts while discarding the documents that justified them. Model at least some of the source layer.

Useful source objects:
- `Document`
- `Section`
- `Paragraph`
- `Table`
- `Row`
- `Message`
- `APIResponse`
- `File`

Useful evidence relations:
- `MENTIONED_IN`
- `EVIDENCED_BY`
- `EXTRACTED_FROM`
- `DERIVED_FROM`
- `HAS_SOURCE_SPAN`

In high-trust systems, represent facts as accepted assertions linked to evidence rather than as unsupported direct edges only.

## 8. Modeling principles for automatic ingestion

1. **Canonical identity must survive re-ingestion**.
2. **Mentions must remain inspectable** for debugging and provenance.
3. **Normalization must happen before merging** when possible.
4. **Ontology alignment must constrain relation naming**.
5. **Event modeling should be considered whenever time or state change matters**.
6. **Every graph write should be explainable from intermediate extraction output**.
7. **A source span should be enough to re-check the claim**.

## 9. Common mistakes

- Treating chunks as entities.
- Storing only text embeddings and calling it a knowledge graph.
- Turning every adjective or noun phrase into a node.
- Skipping entity resolution and creating one node per mention.
- Using one catch-all edge type for everything.
- Ignoring time and overwrite semantics.
- Failing to preserve provenance.
- Equating ontology classes with extracted instance nodes.
