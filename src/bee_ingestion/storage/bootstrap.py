from __future__ import annotations

import psycopg

from src.bee_ingestion.settings import settings


def ensure_app_storage_compatibility(dsn: str | None = None) -> None:
    """Apply explicit compatibility/bootstrap DDL for the application database.

    This remains a temporary compatibility shim for the production schema. It is
    intentionally explicit so repository construction stays side-effect free.
    """

    resolved_dsn = str(dsn or settings.postgres_dsn or "").strip()
    if not resolved_dsn:
        raise ValueError("Postgres DSN is required for application storage bootstrap")

    with psycopg.connect(resolved_dsn) as conn:
        with conn.cursor() as cur:
            cur.execute("ALTER TABLE IF EXISTS agent_profiles ADD COLUMN IF NOT EXISTS profile_token_issued_at timestamptz")
            cur.execute("ALTER TABLE IF EXISTS agent_sessions ADD COLUMN IF NOT EXISTS auth_user_id text")
            cur.execute("ALTER TABLE IF EXISTS agent_sessions ADD COLUMN IF NOT EXISTS workspace_kind text NOT NULL DEFAULT 'general'")
            cur.execute("ALTER TABLE IF EXISTS agent_sessions ADD COLUMN IF NOT EXISTS session_token_issued_at timestamptz")
            cur.execute("ALTER TABLE IF EXISTS sensor_readings ADD COLUMN IF NOT EXISTS reading_hash text NOT NULL DEFAULT ''")
            cur.execute(
                """
                CREATE TABLE IF NOT EXISTS corpus_snapshots (
                  snapshot_id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
                  tenant_id text NOT NULL DEFAULT 'shared',
                  snapshot_kind text NOT NULL,
                  summary text NOT NULL DEFAULT '',
                  document_id uuid,
                  job_id uuid,
                  metrics_json jsonb NOT NULL DEFAULT '{}'::jsonb,
                  metadata_json jsonb NOT NULL DEFAULT '{}'::jsonb,
                  created_at timestamptz NOT NULL DEFAULT now()
                )
                """
            )
            cur.execute(
                """
                CREATE TABLE IF NOT EXISTS document_synopses (
                  document_id uuid PRIMARY KEY REFERENCES documents(document_id) ON DELETE CASCADE,
                  tenant_id text NOT NULL,
                  title text NOT NULL DEFAULT '',
                  synopsis_text text NOT NULL DEFAULT '',
                  accepted_chunk_count integer NOT NULL DEFAULT 0,
                  section_count integer NOT NULL DEFAULT 0,
                  source_stage text NOT NULL DEFAULT 'chunks_validated',
                  synopsis_version text NOT NULL DEFAULT 'extractive-v1',
                  metadata_json jsonb NOT NULL DEFAULT '{}'::jsonb,
                  created_at timestamptz NOT NULL DEFAULT now(),
                  updated_at timestamptz NOT NULL DEFAULT now()
                )
                """
            )
            cur.execute(
                """
                CREATE TABLE IF NOT EXISTS document_section_synopses (
                  section_id text PRIMARY KEY,
                  document_id uuid NOT NULL REFERENCES documents(document_id) ON DELETE CASCADE,
                  tenant_id text NOT NULL,
                  parent_section_id text REFERENCES document_section_synopses(section_id) ON DELETE CASCADE,
                  section_path text[] NOT NULL DEFAULT '{}',
                  section_level integer NOT NULL DEFAULT 0,
                  section_title text NOT NULL DEFAULT '',
                  page_start integer,
                  page_end integer,
                  char_start integer,
                  char_end integer,
                  first_chunk_id text REFERENCES document_chunks(chunk_id) ON DELETE SET NULL,
                  last_chunk_id text REFERENCES document_chunks(chunk_id) ON DELETE SET NULL,
                  accepted_chunk_count integer NOT NULL DEFAULT 0,
                  total_chunk_count integer NOT NULL DEFAULT 0,
                  synopsis_text text NOT NULL DEFAULT '',
                  synopsis_version text NOT NULL DEFAULT 'extractive-v1',
                  metadata_json jsonb NOT NULL DEFAULT '{}'::jsonb,
                  created_at timestamptz NOT NULL DEFAULT now(),
                  updated_at timestamptz NOT NULL DEFAULT now(),
                  UNIQUE (document_id, section_path)
                )
                """
            )
            cur.execute("ALTER TABLE IF EXISTS documents ADD COLUMN IF NOT EXISTS latest_corpus_snapshot_id uuid")
            cur.execute("ALTER TABLE IF EXISTS ingestion_jobs ADD COLUMN IF NOT EXISTS completed_corpus_snapshot_id uuid")
            cur.execute("ALTER TABLE IF EXISTS agent_query_runs ADD COLUMN IF NOT EXISTS corpus_snapshot_id uuid")
            cur.execute("ALTER TABLE IF EXISTS agent_query_sources ADD COLUMN IF NOT EXISTS corpus_snapshot_id uuid")
            cur.execute("ALTER TABLE IF EXISTS agent_messages ADD COLUMN IF NOT EXISTS profile_id uuid")
            cur.execute("ALTER TABLE IF EXISTS agent_messages ADD COLUMN IF NOT EXISTS auth_user_id text")
            cur.execute("ALTER TABLE IF EXISTS agent_query_runs ADD COLUMN IF NOT EXISTS profile_id uuid")
            cur.execute("ALTER TABLE IF EXISTS agent_query_runs ADD COLUMN IF NOT EXISTS auth_user_id text")
            cur.execute("ALTER TABLE IF EXISTS agent_session_memories ADD COLUMN IF NOT EXISTS profile_id uuid")
            cur.execute("ALTER TABLE IF EXISTS agent_session_memories ADD COLUMN IF NOT EXISTS auth_user_id text")
            cur.execute(
                """
                CREATE TABLE IF NOT EXISTS agent_query_embeddings (
                  tenant_id text NOT NULL,
                  query_hash text NOT NULL,
                  normalized_query text NOT NULL,
                  cache_identity text NOT NULL,
                  embedding_json jsonb NOT NULL,
                  embedding_dimensions integer NOT NULL DEFAULT 0,
                  cache_hits integer NOT NULL DEFAULT 0,
                  cached_at timestamptz NOT NULL DEFAULT now(),
                  created_at timestamptz NOT NULL DEFAULT now(),
                  updated_at timestamptz NOT NULL DEFAULT now(),
                  PRIMARY KEY (tenant_id, query_hash, cache_identity)
                )
                """
            )
            cur.execute("CREATE INDEX IF NOT EXISTS idx_agent_sessions_owner ON agent_sessions(tenant_id, auth_user_id, updated_at DESC)")
            cur.execute("CREATE INDEX IF NOT EXISTS idx_agent_sessions_owner_kind ON agent_sessions(tenant_id, auth_user_id, workspace_kind, updated_at DESC)")
            cur.execute("CREATE INDEX IF NOT EXISTS idx_agent_messages_session_created_desc ON agent_messages(session_id, created_at DESC)")
            cur.execute("CREATE INDEX IF NOT EXISTS idx_agent_messages_owner_created_desc ON agent_messages(auth_user_id, created_at DESC)")
            cur.execute("CREATE INDEX IF NOT EXISTS idx_agent_query_runs_owner_created_desc ON agent_query_runs(auth_user_id, created_at DESC)")
            cur.execute("CREATE INDEX IF NOT EXISTS idx_agent_session_memories_owner_updated_desc ON agent_session_memories(auth_user_id, updated_at DESC)")
            cur.execute(
                """
                UPDATE agent_messages m
                SET profile_id = s.profile_id,
                    auth_user_id = s.auth_user_id
                FROM agent_sessions s
                WHERE s.session_id = m.session_id
                  AND (m.profile_id IS DISTINCT FROM s.profile_id OR m.auth_user_id IS DISTINCT FROM s.auth_user_id)
                """
            )
            cur.execute(
                """
                UPDATE agent_query_runs q
                SET profile_id = s.profile_id,
                    auth_user_id = s.auth_user_id
                FROM agent_sessions s
                WHERE s.session_id = q.session_id
                  AND (q.profile_id IS DISTINCT FROM s.profile_id OR q.auth_user_id IS DISTINCT FROM s.auth_user_id)
                """
            )
            cur.execute(
                """
                UPDATE agent_session_memories m
                SET profile_id = s.profile_id,
                    auth_user_id = s.auth_user_id
                FROM agent_sessions s
                WHERE s.session_id = m.session_id
                  AND (m.profile_id IS DISTINCT FROM s.profile_id OR m.auth_user_id IS DISTINCT FROM s.auth_user_id)
                """
            )
            cur.execute("CREATE UNIQUE INDEX IF NOT EXISTS idx_sensor_readings_sensor_hash ON sensor_readings(sensor_id, reading_hash)")
            cur.execute(
                """
                CREATE INDEX IF NOT EXISTS page_assets_tenant_document_page_asset_idx
                ON page_assets (tenant_id, document_id, page_number, asset_index)
                """
            )
            cur.execute(
                """
                CREATE INDEX IF NOT EXISTS document_chunks_tenant_document_chunk_idx
                ON document_chunks (tenant_id, document_id, chunk_index)
                """
            )
            cur.execute(
                """
                CREATE INDEX IF NOT EXISTS document_chunks_status_tenant_document_idx
                ON document_chunks (status, tenant_id, document_id, chunk_index)
                """
            )
            cur.execute(
                """
                CREATE INDEX IF NOT EXISTS chunk_validations_status_chunk_idx
                ON chunk_validations (status, chunk_id)
                """
            )
            cur.execute(
                """
                CREATE INDEX IF NOT EXISTS kg_assertions_chunk_status_confidence_idx
                ON kg_assertions (chunk_id, status, confidence DESC, created_at DESC)
                """
            )
            cur.execute(
                """
                CREATE INDEX IF NOT EXISTS kg_assertions_subject_status_confidence_idx
                ON kg_assertions (subject_entity_id, status, confidence DESC, created_at DESC)
                """
            )
            cur.execute(
                """
                CREATE INDEX IF NOT EXISTS kg_assertions_object_status_confidence_idx
                ON kg_assertions (object_entity_id, status, confidence DESC, created_at DESC)
                """
            )
            cur.execute(
                """
                CREATE INDEX IF NOT EXISTS kg_assertions_document_status_confidence_idx
                ON kg_assertions (document_id, status, confidence DESC, created_at DESC)
                """
            )
            cur.execute(
                """
                CREATE INDEX IF NOT EXISTS kg_assertion_evidence_assertion_created_idx
                ON kg_assertion_evidence (assertion_id, created_at DESC)
                """
            )
            cur.execute("SELECT EXISTS (SELECT 1 FROM pg_extension WHERE extname = 'pg_trgm')")
            has_pg_trgm = bool(cur.fetchone()[0])
            if has_pg_trgm:
                cur.execute(
                    """
                    CREATE INDEX IF NOT EXISTS kg_entities_canonical_name_trgm_idx
                    ON kg_entities USING gin (canonical_name gin_trgm_ops)
                    """
                )
                cur.execute(
                    """
                    CREATE INDEX IF NOT EXISTS kg_entities_type_trgm_idx
                    ON kg_entities USING gin (entity_type gin_trgm_ops)
                    """
                )
                cur.execute(
                    """
                    CREATE INDEX IF NOT EXISTS kg_assertions_object_literal_trgm_idx
                    ON kg_assertions USING gin ((COALESCE(object_literal, '')) gin_trgm_ops)
                    """
                )
                cur.execute(
                    """
                    CREATE INDEX IF NOT EXISTS kg_assertion_evidence_excerpt_trgm_idx
                    ON kg_assertion_evidence USING gin (excerpt gin_trgm_ops)
                    """
                )
                cur.execute(
                    """
                    CREATE INDEX IF NOT EXISTS document_synopses_title_trgm_idx
                    ON document_synopses USING gin ((COALESCE(title, '')) gin_trgm_ops)
                    """
                )
                cur.execute(
                    """
                    CREATE INDEX IF NOT EXISTS document_synopses_text_trgm_idx
                    ON document_synopses USING gin (synopsis_text gin_trgm_ops)
                    """
                )
                cur.execute(
                    """
                    CREATE INDEX IF NOT EXISTS document_section_synopses_title_trgm_idx
                    ON document_section_synopses USING gin ((COALESCE(section_title, '')) gin_trgm_ops)
                    """
                )
                cur.execute(
                    """
                    CREATE INDEX IF NOT EXISTS document_section_synopses_text_trgm_idx
                    ON document_section_synopses USING gin (synopsis_text gin_trgm_ops)
                    """
                )
                cur.execute(
                    """
                    CREATE INDEX IF NOT EXISTS page_assets_search_text_trgm_idx
                    ON page_assets USING gin (search_text gin_trgm_ops)
                    """
                )
                cur.execute(
                    """
                    CREATE INDEX IF NOT EXISTS page_assets_label_trgm_idx
                    ON page_assets USING gin ((COALESCE(metadata_json->>'label', '')) gin_trgm_ops)
                    """
                )
                cur.execute(
                    """
                    CREATE INDEX IF NOT EXISTS page_assets_asset_type_trgm_idx
                    ON page_assets USING gin ((COALESCE(asset_type, '')) gin_trgm_ops)
                    """
                )
                cur.execute(
                    """
                    CREATE INDEX IF NOT EXISTS document_chunks_text_trgm_idx
                    ON document_chunks USING gin (text gin_trgm_ops)
                    """
                )
                cur.execute(
                    """
                    CREATE INDEX IF NOT EXISTS document_chunks_title_trgm_idx
                    ON document_chunks USING gin ((COALESCE(metadata_json->>'title', '')) gin_trgm_ops)
                    """
                )
                cur.execute(
                    """
                    CREATE INDEX IF NOT EXISTS document_chunks_section_title_trgm_idx
                    ON document_chunks USING gin ((COALESCE(metadata_json->>'section_title', '')) gin_trgm_ops)
                    """
                )
                cur.execute(
                    """
                    CREATE INDEX IF NOT EXISTS document_chunks_section_heading_trgm_idx
                    ON document_chunks USING gin ((COALESCE(metadata_json->>'section_heading', '')) gin_trgm_ops)
                    """
                )
            cur.execute(
                """
                CREATE INDEX IF NOT EXISTS document_synopses_tenant_updated_idx
                ON document_synopses (tenant_id, updated_at DESC)
                """
            )
            cur.execute(
                """
                CREATE INDEX IF NOT EXISTS document_section_synopses_document_parent_idx
                ON document_section_synopses (document_id, parent_section_id)
                """
            )
            cur.execute(
                """
                CREATE INDEX IF NOT EXISTS document_section_synopses_document_level_idx
                ON document_section_synopses (document_id, section_level)
                """
            )
            cur.execute(
                """
                CREATE INDEX IF NOT EXISTS idx_corpus_snapshots_tenant_created
                ON corpus_snapshots(tenant_id, created_at DESC)
                """
            )
            cur.execute(
                """
                CREATE INDEX IF NOT EXISTS idx_corpus_snapshots_document_created
                ON corpus_snapshots(document_id, created_at DESC)
                """
            )
            cur.execute(
                """
                CREATE INDEX IF NOT EXISTS idx_agent_query_embeddings_lookup
                ON agent_query_embeddings(tenant_id, query_hash, cached_at DESC)
                """
            )
        conn.commit()
