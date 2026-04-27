# Local Implementation Patterns

## Table of contents
1. Default architecture
2. Embedded local pattern
3. Local server pattern
4. Python-first scaffold guidance
5. Testing strategy
6. Local ops checklist

## 1. Default architecture

Recommended local-first architecture:

- source readers
- parser/normalizer
- chunk builder
- metadata builder
- deterministic id generator
- embedder
- Chroma writer
- retrieval service
- evaluation harness

Keep each stage testable outside the agent loop.

## 2. Embedded local pattern

Use when one local application owns ingestion and retrieval.

Typical flow:
1. open local persistence path
2. create or get collection
3. generate chunks and metadata
4. embed or let collection embed
5. write batches
6. run test queries

This is usually the simplest correct answer for a fully local agent stack.

## 3. Local server pattern

Use when components need localhost HTTP boundaries.

Typical flow:
1. run local Chroma server on the machine
2. application connects through HTTP client
3. keep persistence path owned by the server process
4. centralize ingestion writes through one service path

Prefer this when process separation is important.

## 4. Python-first scaffold guidance

When writing code, generate modules roughly like:
- `loaders/`
- `parsers/`
- `chunking.py`
- `metadata.py`
- `embeddings.py`
- `vector_store.py`
- `ingest.py`
- `query.py`
- `eval_retrieval.py`

Helpful responsibilities:
- `vector_store.py`: client and collection operations only
- `ingest.py`: orchestration only
- `embeddings.py`: local embedding configuration only
- `eval_retrieval.py`: benchmark or regression queries

## 5. Testing strategy

Test at multiple levels:

### Unit tests
- chunk boundary generation
- metadata generation
- deterministic id generation
- update/delete selection logic

### Integration tests
- local persistent path works across runs
- one ingest run writes expected count
- repeated ingest does not duplicate stable chunks
- delete by source metadata works

### Retrieval tests
- known queries retrieve expected sources
- filters exclude wrong tenant/domain
- collection migration to a new embedding version works

## 6. Local ops checklist

Before calling the pipeline complete, verify:
- local persistence path is explicit
- collection name strategy is documented
- embedding model choice is documented
- same embedding strategy is used at query time
- chunk ids are deterministic
- metadata supports retrieval filters
- update/delete behavior is defined
- re-index policy is defined
- retrieval evaluation exists
