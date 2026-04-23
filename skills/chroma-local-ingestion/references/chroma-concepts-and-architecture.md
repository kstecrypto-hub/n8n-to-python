# Chroma Concepts and Architecture

## Table of contents
1. What a vector store is
2. What Chroma is actually doing
3. Chroma data model
4. Collections as the core abstraction
5. Embedded local mode vs local server mode
6. Retrieval semantics
7. What belongs in Chroma vs outside Chroma
8. Common misunderstandings

## 1. What a vector store is

A vector store is a retrieval system built around embeddings. Instead of retrieving only by exact string match or exact key, it retrieves items that are semantically close in embedding space.

For an agent pipeline, a vector store usually answers: “Which chunks are most relevant to this query?”

The vector store is not automatically a knowledge graph, a transactional database, or a full document management system. It is a retrieval component.

## 2. What Chroma is actually doing

Chroma stores and indexes embeddings together with associated records. At a practical level, it helps the application:

- persist embeddings locally or behind a local server
- associate embeddings with ids, documents, and metadata
- run nearest-neighbor retrieval for semantic queries
- apply metadata or document filters where appropriate
- fetch records directly by id or filter without similarity ranking

When the collection has an embedding function attached, Chroma can embed documents on write and text queries on retrieval. When the collection has no embedding function, the application must supply embeddings directly.

## 3. Chroma data model

A usable mental model is:

- **client**: how the application connects to Chroma
- **collection**: the core storage and query unit
- **records within a collection**: ids + documents + metadata + embeddings
- **query**: semantic similarity retrieval over embeddings
- **get**: direct retrieval by ids and/or filters without similarity ranking

This means the application must think carefully about:
- which collection a record belongs to
- how ids are generated
- what metadata will be filterable
- how documents or payload text are stored
- whether embeddings are generated inside Chroma or upstream

## 4. Collections as the core abstraction

Collections are the fundamental unit of storage and querying in Chroma.

A collection embodies a retrieval contract:
- item type
- embedding function strategy
- embedding dimensionality once data exists
- distance/similarity behavior as configured
- metadata conventions
- intended query workload

A collection should therefore not be treated as an arbitrary bucket. Create collections according to real operational boundaries.

Typical collection patterns:
- one collection per agent domain
- one collection per tenant/customer
- one collection per modality
- one collection per embedding strategy or corpus version

Do not mix incompatible embeddings or inconsistent content types into one collection unless the retrieval design explicitly justifies it.

## 5. Embedded local mode vs local server mode

### Embedded local mode

Use an embedded persistent client when the application can access the local storage path directly and does not need a separate service process.

Benefits:
- simplest local setup
- no network hop
- good fit for single-app pipelines

### Local server mode

Use a locally running Chroma server when different local components or languages need to communicate over HTTP or when a service boundary is intentional.

Benefits:
- cleaner service separation
- easier cross-process access model
- useful for local development environments with multiple clients

If the user wants everything local and minimal, embedded persistent mode is usually the default answer.

## 6. Retrieval semantics

Chroma supports two conceptually different retrieval styles:

### Semantic retrieval
Use `.query` when you want nearest-neighbor similarity search over embeddings.

### Direct retrieval
Use `.get` when you want records by id or by filters without semantic ranking.

The ingestion pipeline must support both:
- semantic retrieval for agent context assembly
- direct retrieval for debugging, audits, updates, and maintenance

## 7. What belongs in Chroma vs outside Chroma

Good candidates for Chroma storage:
- chunk text
- metadata needed for filtering and provenance
- embeddings
- stable chunk ids

Usually better outside Chroma:
- raw source files
- complex parsing artifacts
- write-ahead ingestion logs
- authoritative business records
- graph-structured canonical knowledge

A healthy architecture often uses Chroma beside a file store, relational store, or graph store.

## 8. Common misunderstandings

- thinking a vector store alone is a full memory architecture
- storing only documents and no metadata
- failing to define chunk ids deterministically
- assuming all embedding models are interchangeable within one collection
- confusing `.get` with `.query`
- assuming retrieval quality comes from the database alone rather than chunking + metadata + embeddings + query design
