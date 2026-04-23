# Extraction, Canonicalization, and Resolution

## Table of contents
1. Extraction design principles
2. What to extract
3. Deterministic extraction vs model-based extraction
4. Canonicalization
5. Entity resolution
6. Confidence and review policies
7. Provenance modeling
8. Choosing assertion granularity
9. Failure modes

## 1. Extraction design principles

Extraction should produce structured candidates under a controlled contract. The extractor’s job is not to invent ontology freely or silently merge entities. The extractor should expose enough intermediate structure that downstream normalization and validation can inspect it.

Use the extraction stage to answer:
- what mentions were found?
- what candidate entities do those mentions suggest?
- what candidate relations or events are expressed?
- what literal values were found?
- what evidence spans support each candidate?

## 2. What to extract

A good default extraction target includes:

- mention spans
- mention types
- candidate canonical type
- relation/event candidates
- literal attributes
- qualifiers (time, dosage, location, role, status)
- negation/speculation if relevant
- source evidence
- confidence per candidate

In technical or biomedical domains, qualifiers and negation are often essential.

Examples:
- “may cause” should not be normalized as a confirmed causal relation
- “rule out pneumonia” should not produce a positive diagnosis fact

## 3. Deterministic extraction vs model-based extraction

Use deterministic methods where the domain permits them:
- IDs
- dates
- units
- email addresses
- URLs
- structured table columns
- enumerated categories

Use model-based or LLM-based extraction where semantics are less rigid:
- relation interpretation
- event structure
- cross-clause references
- domain-role assignment
- contextual disambiguation

Best practice is hybrid extraction:
- deterministic preprocessing and normalization
- model-based semantic extraction
- deterministic post-validation

## 4. Canonicalization

Canonicalization standardizes representations before graph write.

Normalize at least:
- whitespace and punctuation
- Unicode variants
- case when appropriate
- dates and times
- units and measures
- known aliases
- namespace prefixes
- identifier formats

Examples:
- “03/04/25” -> unambiguous ISO form only with locale/context resolution
- “mg.” -> `mg`
- “I.B.M.” -> `IBM`
- “U.S.A.” -> `United States` or external id if policy defines it

Canonicalization is not the same as entity resolution. It prepares candidates for comparison.

## 5. Entity resolution

Entity resolution decides whether two mentions or candidates refer to the same canonical object.

Signals:
- exact external identifier
- normalized name equality
- alias list
- contextual co-occurrence
- same document reference id
- surrounding relations
- type compatibility
- embedding similarity
- ontology-linked ID

Recommended strategy:
1. exact-id match first
2. deterministic alias match second
3. contextual similarity and structural evidence third
4. LLM or learned adjudication only for ambiguous cases
5. unresolved candidate remains unmerged until confidence improves

Never auto-merge solely because two names are similar if the merge is hard to reverse.

## 6. Confidence and review policies

Confidence should be attached to extraction output, not confused with correctness.

Useful confidence levels:
- extraction confidence
- resolution confidence
- ontology-mapping confidence
- acceptance status

Policy example:
- high confidence + valid schema -> auto-accept
- medium confidence -> stage for review or delayed merge
- low confidence -> quarantine or keep as unresolved mention

## 7. Provenance modeling

For every accepted assertion, retain enough evidence to reconstruct why it exists.

Minimum provenance fields:
- source id
- segment id or row id
- source span or cell coordinates when possible
- extraction run id
- extractor version
- timestamp

Better provenance fields:
- mention ids
- pre-normalized surface forms
- prompt or rule version
- reviewer decision if human-reviewed

## 8. Choosing assertion granularity

There are three common write targets:

### Direct fact
Example: `DrugX TREATS DiseaseY`

Good when:
- assertion is stable
- qualifiers are few
- temporal details are minimal

### Reified assertion node
Create a fact/assertion node with metadata.

Good when:
- confidence matters
- multiple evidence items matter
- qualifiers matter
- conflicting sources exist

### Event model
Create an event node.

Good when:
- the fact has time
- participants play roles
- state changes matter
- source phrasing is event-centric

Choose the lightest structure that preserves the downstream semantics.

## 9. Failure modes

- false entity splits: same real entity becomes many canonical nodes
- false merges: distinct entities collapse into one node
- qualifier loss: title/date/negation disappears during normalization
- evidence loss: graph assertion cannot be traced back
- predicate drift: same relation written with multiple names
- ontology drift: changing type inventory without migration plan
