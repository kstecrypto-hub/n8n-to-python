---
name: postgres-db
description: PostgreSQL 数据库操作技能。用于执行SQL查询、表管理、备份恢复、性能监控等数据库操作。触发条件：用户提到 PostgreSQL、postgres、数据库查询、SQL查询、表结构、数据库备份等。
---

# PostgreSQL Database Skill

## Overview

This skill provides PostgreSQL database operations for query execution, schema inspection, backup and restore flows, and basic performance monitoring.

## Workflow

1. Identify whether the task is read-only inspection, data change, schema change, backup, restore, or performance review.
2. Prefer read-only inspection first:
   - Use `scripts/query.py` for SQL reads and targeted diagnostics.
   - Use `scripts/schema_export.py` for table, index, foreign key, view, and trigger inspection.
3. Treat mutating SQL, schema changes, and restore operations as higher risk:
   - Confirm the target database and scope when destructive impact is possible.
   - Prefer creating a backup before major writes or restore work.
4. Use environment variables or explicit flags for PostgreSQL connection settings.
5. Keep outputs practical:
   - JSON or CSV for machine-readable results.
   - Markdown for human-readable schema summaries.

## Capabilities

### 1. SQL Query Execution (`scripts/query.py`)

Execute SQL queries against PostgreSQL databases with support for:
- SELECT queries with formatted output
- INSERT, UPDATE, and DELETE operations
- Explicit commit control for mutating statements
- Query result export to JSON or CSV
- EXPLAIN and EXPLAIN ANALYZE support

### 2. Schema Export (`scripts/schema_export.py`)

Export database schema information:
- Table structures with columns, defaults, nullability, and constraints
- Indexes and foreign keys
- Views and triggers
- Export to JSON or Markdown format

### 3. Database Backup (`scripts/backup.py`)

Database backup and restore operations:
- Full database backup using `pg_dump`
- Table-specific backup
- Restore support using `pg_restore` or `psql`
- Backup rotation management

### 4. Performance Monitoring

Monitor database performance with SQL via `scripts/query.py`:
- Query execution plans with `EXPLAIN ANALYZE`
- Index usage statistics
- Table size and row counts
- Connection activity and lock inspection

## Usage

### Query Database

```bash
python scripts/query.py --dbname mydb --query "SELECT * FROM users LIMIT 10"
```

### Export Schema

```bash
python scripts/schema_export.py --dbname mydb --output schema.json
```

### Backup Database

```bash
python scripts/backup.py --dbname mydb --backup-dir /backups
```

## Requirements

- PostgreSQL client tools: `psql`, `pg_dump`, and optionally `pg_restore`
- Python 3.8+
- `psycopg` or `psycopg2`

## Configuration

Set environment variables:
- `PGHOST` for database host
- `PGPORT` for database port, default `5432`
- `PGDATABASE` for database name
- `PGUSER` for database user
- `PGPASSWORD` for database password
