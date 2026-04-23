CREATE EXTENSION IF NOT EXISTS pgcrypto;
CREATE EXTENSION IF NOT EXISTS pg_trgm;

CREATE TABLE IF NOT EXISTS documents (
  document_id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  tenant_id text NOT NULL,
  source_type text NOT NULL,
  filename text NOT NULL,
  content_hash text NOT NULL,
  document_class text NOT NULL DEFAULT 'note',
  parser_version text,
  ocr_engine text,
  ocr_model text,
  status text NOT NULL DEFAULT 'registered',
  created_at timestamptz NOT NULL DEFAULT now(),
  updated_at timestamptz NOT NULL DEFAULT now()
);

CREATE UNIQUE INDEX IF NOT EXISTS documents_tenant_hash_idx
  ON documents (tenant_id, content_hash);

CREATE TABLE IF NOT EXISTS document_sources (
  source_id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  document_id uuid NOT NULL REFERENCES documents(document_id) ON DELETE CASCADE,
  raw_text text NOT NULL,
  normalized_text text,
  extraction_metrics_json jsonb NOT NULL DEFAULT '{}'::jsonb,
  metadata_json jsonb NOT NULL DEFAULT '{}'::jsonb,
  created_at timestamptz NOT NULL DEFAULT now()
);

ALTER TABLE document_sources
  ADD COLUMN IF NOT EXISTS normalized_text text,
  ADD COLUMN IF NOT EXISTS extraction_metrics_json jsonb NOT NULL DEFAULT '{}'::jsonb;

CREATE TABLE IF NOT EXISTS ingestion_jobs (
  job_id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  document_id uuid NOT NULL REFERENCES documents(document_id) ON DELETE CASCADE,
  status text NOT NULL DEFAULT 'registered',
  extractor_version text NOT NULL DEFAULT 'unknown',
  normalizer_version text NOT NULL DEFAULT 'unknown',
  parser_version text NOT NULL,
  chunker_version text NOT NULL,
  validator_version text NOT NULL DEFAULT 'unknown',
  embedding_version text NOT NULL,
  kg_version text NOT NULL,
  claimed_by text,
  claimed_at timestamptz,
  lease_expires_at timestamptz,
  created_at timestamptz NOT NULL DEFAULT now(),
  updated_at timestamptz NOT NULL DEFAULT now()
);

ALTER TABLE ingestion_jobs
  ADD COLUMN IF NOT EXISTS extractor_version text NOT NULL DEFAULT 'unknown',
  ADD COLUMN IF NOT EXISTS normalizer_version text NOT NULL DEFAULT 'unknown',
  ADD COLUMN IF NOT EXISTS validator_version text NOT NULL DEFAULT 'unknown',
  ADD COLUMN IF NOT EXISTS claimed_by text,
  ADD COLUMN IF NOT EXISTS claimed_at timestamptz,
  ADD COLUMN IF NOT EXISTS lease_expires_at timestamptz;

CREATE UNIQUE INDEX IF NOT EXISTS ingestion_jobs_active_document_idx
  ON ingestion_jobs (document_id)
  WHERE status NOT IN ('completed', 'review', 'failed', 'quarantined');

CREATE TABLE IF NOT EXISTS ingestion_stage_runs (
  stage_run_id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  job_id uuid NOT NULL REFERENCES ingestion_jobs(job_id) ON DELETE CASCADE,
  document_id uuid NOT NULL REFERENCES documents(document_id) ON DELETE CASCADE,
  stage_name text NOT NULL,
  status text NOT NULL,
  attempt integer NOT NULL DEFAULT 1,
  worker_version text,
  input_version text,
  error_code text,
  error_message text,
  metrics_json jsonb NOT NULL DEFAULT '{}'::jsonb,
  started_at timestamptz NOT NULL DEFAULT now(),
  finished_at timestamptz
);

CREATE TABLE IF NOT EXISTS corpus_snapshots (
  snapshot_id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  tenant_id text NOT NULL DEFAULT 'shared',
  snapshot_kind text NOT NULL,
  summary text NOT NULL DEFAULT '',
  document_id uuid REFERENCES documents(document_id) ON DELETE SET NULL,
  job_id uuid REFERENCES ingestion_jobs(job_id) ON DELETE SET NULL,
  metrics_json jsonb NOT NULL DEFAULT '{}'::jsonb,
  metadata_json jsonb NOT NULL DEFAULT '{}'::jsonb,
  created_at timestamptz NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_corpus_snapshots_tenant_created
  ON corpus_snapshots (tenant_id, created_at DESC);

CREATE INDEX IF NOT EXISTS idx_corpus_snapshots_document_created
  ON corpus_snapshots (document_id, created_at DESC);

ALTER TABLE documents
  ADD COLUMN IF NOT EXISTS latest_corpus_snapshot_id uuid REFERENCES corpus_snapshots(snapshot_id) ON DELETE SET NULL;

ALTER TABLE ingestion_jobs
  ADD COLUMN IF NOT EXISTS completed_corpus_snapshot_id uuid REFERENCES corpus_snapshots(snapshot_id) ON DELETE SET NULL;

CREATE TABLE IF NOT EXISTS parsed_blocks (
  block_id text PRIMARY KEY,
  document_id uuid NOT NULL REFERENCES documents(document_id) ON DELETE CASCADE,
  page integer,
  section_path text[] NOT NULL DEFAULT '{}',
  block_type text NOT NULL,
  char_start integer,
  char_end integer,
  text text NOT NULL
);

CREATE TABLE IF NOT EXISTS document_pages (
  document_id uuid NOT NULL REFERENCES documents(document_id) ON DELETE CASCADE,
  page_number integer NOT NULL,
  extracted_text text NOT NULL DEFAULT '',
  ocr_text text NOT NULL DEFAULT '',
  merged_text text NOT NULL DEFAULT '',
  page_image_path text,
  metadata_json jsonb NOT NULL DEFAULT '{}'::jsonb,
  created_at timestamptz NOT NULL DEFAULT now(),
  updated_at timestamptz NOT NULL DEFAULT now(),
  PRIMARY KEY (document_id, page_number)
);

CREATE TABLE IF NOT EXISTS page_assets (
  asset_id text PRIMARY KEY,
  document_id uuid NOT NULL REFERENCES documents(document_id) ON DELETE CASCADE,
  tenant_id text NOT NULL,
  page_number integer NOT NULL,
  asset_index integer NOT NULL DEFAULT 0,
  asset_type text NOT NULL,
  bbox_json jsonb,
  asset_path text NOT NULL,
  content_hash text NOT NULL,
  ocr_text text NOT NULL DEFAULT '',
  description_text text NOT NULL DEFAULT '',
  search_text text NOT NULL DEFAULT '',
  metadata_json jsonb NOT NULL DEFAULT '{}'::jsonb,
  created_at timestamptz NOT NULL DEFAULT now(),
  updated_at timestamptz NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS page_assets_document_page_idx
  ON page_assets (document_id, page_number);

CREATE INDEX IF NOT EXISTS page_assets_tenant_document_page_asset_idx
  ON page_assets (tenant_id, document_id, page_number, asset_index);

CREATE INDEX IF NOT EXISTS page_assets_search_text_trgm_idx
  ON page_assets USING gin (search_text gin_trgm_ops);

CREATE INDEX IF NOT EXISTS page_assets_label_trgm_idx
  ON page_assets USING gin ((COALESCE(metadata_json->>'label', '')) gin_trgm_ops);

CREATE INDEX IF NOT EXISTS page_assets_asset_type_trgm_idx
  ON page_assets USING gin ((COALESCE(asset_type, '')) gin_trgm_ops);

CREATE TABLE IF NOT EXISTS document_chunks (
  chunk_id text PRIMARY KEY,
  document_id uuid NOT NULL REFERENCES documents(document_id) ON DELETE CASCADE,
  tenant_id text NOT NULL,
  chunk_index integer NOT NULL,
  page_start integer,
  page_end integer,
  section_path text[] NOT NULL DEFAULT '{}',
  prev_chunk_id text,
  next_chunk_id text,
  char_start integer,
  char_end integer,
  content_type text NOT NULL DEFAULT 'text',
  text text NOT NULL,
  parser_version text NOT NULL,
  chunker_version text NOT NULL,
  content_hash text NOT NULL,
  metadata_json jsonb NOT NULL DEFAULT '{}'::jsonb,
  status text NOT NULL DEFAULT 'pending',
  created_at timestamptz NOT NULL DEFAULT now(),
  updated_at timestamptz NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS document_chunks_tenant_document_chunk_idx
  ON document_chunks (tenant_id, document_id, chunk_index);

CREATE INDEX IF NOT EXISTS document_chunks_status_tenant_document_idx
  ON document_chunks (status, tenant_id, document_id, chunk_index);

CREATE INDEX IF NOT EXISTS document_chunks_text_trgm_idx
  ON document_chunks USING gin (text gin_trgm_ops);

CREATE INDEX IF NOT EXISTS document_chunks_title_trgm_idx
  ON document_chunks USING gin ((COALESCE(metadata_json->>'title', '')) gin_trgm_ops);

CREATE INDEX IF NOT EXISTS document_chunks_section_title_trgm_idx
  ON document_chunks USING gin ((COALESCE(metadata_json->>'section_title', '')) gin_trgm_ops);

CREATE INDEX IF NOT EXISTS document_chunks_section_heading_trgm_idx
  ON document_chunks USING gin ((COALESCE(metadata_json->>'section_heading', '')) gin_trgm_ops);

CREATE TABLE IF NOT EXISTS chunk_validations (
  chunk_id text PRIMARY KEY REFERENCES document_chunks(chunk_id) ON DELETE CASCADE,
  status text NOT NULL,
  quality_score numeric(5,4) NOT NULL,
  reasons jsonb NOT NULL DEFAULT '[]'::jsonb,
  created_at timestamptz NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS chunk_validations_status_chunk_idx
  ON chunk_validations (status, chunk_id);

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
);

CREATE INDEX IF NOT EXISTS document_synopses_tenant_updated_idx
  ON document_synopses (tenant_id, updated_at DESC);

CREATE INDEX IF NOT EXISTS document_synopses_title_trgm_idx
  ON document_synopses USING gin ((COALESCE(title, '')) gin_trgm_ops);

CREATE INDEX IF NOT EXISTS document_synopses_text_trgm_idx
  ON document_synopses USING gin (synopsis_text gin_trgm_ops);

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
);

CREATE INDEX IF NOT EXISTS document_section_synopses_document_parent_idx
  ON document_section_synopses (document_id, parent_section_id);

CREATE INDEX IF NOT EXISTS document_section_synopses_document_level_idx
  ON document_section_synopses (document_id, section_level);

CREATE INDEX IF NOT EXISTS document_section_synopses_title_trgm_idx
  ON document_section_synopses USING gin ((COALESCE(section_title, '')) gin_trgm_ops);

CREATE INDEX IF NOT EXISTS document_section_synopses_text_trgm_idx
  ON document_section_synopses USING gin (synopsis_text gin_trgm_ops);

CREATE TABLE IF NOT EXISTS chunk_asset_links (
  link_id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  chunk_id text NOT NULL REFERENCES document_chunks(chunk_id) ON DELETE CASCADE,
  asset_id text NOT NULL REFERENCES page_assets(asset_id) ON DELETE CASCADE,
  link_type text NOT NULL,
  confidence numeric(5,4) NOT NULL DEFAULT 1.0,
  metadata_json jsonb NOT NULL DEFAULT '{}'::jsonb,
  created_at timestamptz NOT NULL DEFAULT now(),
  UNIQUE (chunk_id, asset_id, link_type)
);

CREATE TABLE IF NOT EXISTS chunk_review_runs (
  review_run_id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  chunk_id text NOT NULL REFERENCES document_chunks(chunk_id) ON DELETE CASCADE,
  document_id uuid NOT NULL REFERENCES documents(document_id) ON DELETE CASCADE,
  provider text NOT NULL,
  model text NOT NULL,
  prompt_version text NOT NULL,
  decision text NOT NULL,
  confidence numeric(5,4) NOT NULL,
  detected_role text NOT NULL,
  reason text NOT NULL,
  payload jsonb NOT NULL DEFAULT '{}'::jsonb,
  created_at timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS agent_profiles (
  profile_id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  tenant_id text NOT NULL DEFAULT 'shared',
  auth_user_id text,
  display_name text,
  status text NOT NULL DEFAULT 'active',
  profile_token_hash text,
  profile_token_issued_at timestamptz,
  summary_json jsonb NOT NULL DEFAULT '{}'::jsonb,
  summary_text text NOT NULL DEFAULT '',
  source_provider text,
  source_model text,
  prompt_version text,
  created_at timestamptz NOT NULL DEFAULT now(),
  updated_at timestamptz NOT NULL DEFAULT now()
);

ALTER TABLE IF EXISTS agent_profiles
  ADD COLUMN IF NOT EXISTS auth_user_id text;

CREATE UNIQUE INDEX IF NOT EXISTS idx_agent_profiles_tenant_auth_user
  ON agent_profiles(tenant_id, auth_user_id)
  WHERE auth_user_id IS NOT NULL;

CREATE TABLE IF NOT EXISTS agent_sessions (
  session_id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  tenant_id text NOT NULL DEFAULT 'shared',
  auth_user_id text,
  profile_id uuid REFERENCES agent_profiles(profile_id) ON DELETE SET NULL,
  workspace_kind text NOT NULL DEFAULT 'general',
  title text,
  status text NOT NULL DEFAULT 'active',
  session_token_hash text,
  session_token_issued_at timestamptz,
  claimed_by text,
  claimed_at timestamptz,
  lease_expires_at timestamptz,
  created_at timestamptz NOT NULL DEFAULT now(),
  updated_at timestamptz NOT NULL DEFAULT now()
);

ALTER TABLE IF EXISTS agent_sessions
  ADD COLUMN IF NOT EXISTS auth_user_id text;

ALTER TABLE IF EXISTS agent_sessions
  ADD COLUMN IF NOT EXISTS profile_id uuid REFERENCES agent_profiles(profile_id) ON DELETE SET NULL;

ALTER TABLE IF EXISTS agent_sessions
  ADD COLUMN IF NOT EXISTS workspace_kind text NOT NULL DEFAULT 'general';

ALTER TABLE IF EXISTS agent_sessions
  ADD COLUMN IF NOT EXISTS session_token_hash text;

ALTER TABLE IF EXISTS agent_profiles
  ADD COLUMN IF NOT EXISTS profile_token_issued_at timestamptz;

ALTER TABLE IF EXISTS agent_sessions
  ADD COLUMN IF NOT EXISTS session_token_issued_at timestamptz;

ALTER TABLE IF EXISTS agent_sessions
  ADD COLUMN IF NOT EXISTS claimed_by text;

ALTER TABLE IF EXISTS agent_sessions
  ADD COLUMN IF NOT EXISTS claimed_at timestamptz;

ALTER TABLE IF EXISTS agent_sessions
  ADD COLUMN IF NOT EXISTS lease_expires_at timestamptz;

CREATE INDEX IF NOT EXISTS idx_agent_sessions_owner
  ON agent_sessions(tenant_id, auth_user_id, updated_at DESC);

CREATE INDEX IF NOT EXISTS idx_agent_sessions_owner_kind
  ON agent_sessions(tenant_id, auth_user_id, workspace_kind, updated_at DESC);

CREATE TABLE IF NOT EXISTS agent_session_memories (
  session_id uuid PRIMARY KEY REFERENCES agent_sessions(session_id) ON DELETE CASCADE,
  profile_id uuid REFERENCES agent_profiles(profile_id) ON DELETE SET NULL,
  auth_user_id text,
  summary_json jsonb NOT NULL DEFAULT '{}'::jsonb,
  summary_text text NOT NULL DEFAULT '',
  source_provider text,
  source_model text,
  prompt_version text,
  created_at timestamptz NOT NULL DEFAULT now(),
  updated_at timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS agent_messages (
  message_id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  session_id uuid NOT NULL REFERENCES agent_sessions(session_id) ON DELETE CASCADE,
  profile_id uuid REFERENCES agent_profiles(profile_id) ON DELETE SET NULL,
  auth_user_id text,
  role text NOT NULL,
  content text NOT NULL,
  metadata_json jsonb NOT NULL DEFAULT '{}'::jsonb,
  created_at timestamptz NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_agent_messages_session_created_desc
  ON agent_messages(session_id, created_at DESC);

CREATE INDEX IF NOT EXISTS idx_agent_messages_owner_created_desc
  ON agent_messages(auth_user_id, created_at DESC);

CREATE TABLE IF NOT EXISTS agent_query_runs (
  query_run_id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  session_id uuid REFERENCES agent_sessions(session_id) ON DELETE SET NULL,
  profile_id uuid REFERENCES agent_profiles(profile_id) ON DELETE SET NULL,
  auth_user_id text,
  tenant_id text NOT NULL DEFAULT 'shared',
  question text NOT NULL,
  normalized_query text NOT NULL,
  query_signature text,
  query_keywords jsonb NOT NULL DEFAULT '[]'::jsonb,
  question_type text NOT NULL DEFAULT 'fact',
  retrieval_mode text NOT NULL DEFAULT 'chunks_only',
  status text NOT NULL DEFAULT 'completed',
  answer text,
  confidence numeric(5,4) NOT NULL DEFAULT 0,
  abstained boolean NOT NULL DEFAULT false,
  abstain_reason text,
  provider text,
  model text,
  prompt_version text,
  error_message text,
  metrics_json jsonb NOT NULL DEFAULT '{}'::jsonb,
  prompt_payload jsonb NOT NULL DEFAULT '{}'::jsonb,
  raw_response_payload jsonb NOT NULL DEFAULT '{}'::jsonb,
  final_response_payload jsonb NOT NULL DEFAULT '{}'::jsonb,
  review_status text NOT NULL DEFAULT 'unreviewed',
  review_reason text,
  reviewed_at timestamptz,
  reviewed_by text,
  corpus_snapshot_id uuid REFERENCES corpus_snapshots(snapshot_id) ON DELETE SET NULL,
  created_at timestamptz NOT NULL DEFAULT now()
);

ALTER TABLE agent_query_runs
  ADD COLUMN IF NOT EXISTS corpus_snapshot_id uuid REFERENCES corpus_snapshots(snapshot_id) ON DELETE SET NULL;

CREATE INDEX IF NOT EXISTS idx_agent_query_runs_owner_created_desc
  ON agent_query_runs(auth_user_id, created_at DESC);

CREATE INDEX IF NOT EXISTS idx_agent_session_memories_owner_updated_desc
  ON agent_session_memories(auth_user_id, updated_at DESC);

CREATE TABLE IF NOT EXISTS user_places (
  place_id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  tenant_id text NOT NULL DEFAULT 'shared',
  auth_user_id text NOT NULL,
  external_place_id text NOT NULL,
  place_name text NOT NULL,
  status text NOT NULL DEFAULT 'active',
  metadata_json jsonb NOT NULL DEFAULT '{}'::jsonb,
  created_at timestamptz NOT NULL DEFAULT now(),
  updated_at timestamptz NOT NULL DEFAULT now(),
  UNIQUE (tenant_id, auth_user_id, external_place_id)
);

CREATE INDEX IF NOT EXISTS idx_user_places_owner
  ON user_places(tenant_id, auth_user_id, status, updated_at DESC);

CREATE UNIQUE INDEX IF NOT EXISTS idx_user_places_owner_ref
  ON user_places(place_id, tenant_id, auth_user_id);

CREATE TABLE IF NOT EXISTS user_hives (
  hive_id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  tenant_id text NOT NULL DEFAULT 'shared',
  auth_user_id text NOT NULL,
  external_hive_id text NOT NULL,
  hive_name text NOT NULL,
  place_id uuid,
  status text NOT NULL DEFAULT 'active',
  metadata_json jsonb NOT NULL DEFAULT '{}'::jsonb,
  created_at timestamptz NOT NULL DEFAULT now(),
  updated_at timestamptz NOT NULL DEFAULT now(),
  UNIQUE (tenant_id, auth_user_id, external_hive_id)
);

CREATE INDEX IF NOT EXISTS idx_user_hives_owner
  ON user_hives(tenant_id, auth_user_id, status, updated_at DESC);

CREATE UNIQUE INDEX IF NOT EXISTS idx_user_hives_owner_ref
  ON user_hives(hive_id, tenant_id, auth_user_id);

CREATE UNIQUE INDEX IF NOT EXISTS idx_user_hives_owner_place_ref
  ON user_hives(hive_id, place_id, tenant_id, auth_user_id);

CREATE TABLE IF NOT EXISTS user_sensors (
  sensor_id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  tenant_id text NOT NULL DEFAULT 'shared',
  auth_user_id text NOT NULL,
  external_sensor_id text NOT NULL,
  sensor_name text NOT NULL,
  sensor_type text NOT NULL DEFAULT 'environment',
  place_id uuid,
  hive_id uuid,
  hive_name text,
  location_label text,
  status text NOT NULL DEFAULT 'active',
  metadata_json jsonb NOT NULL DEFAULT '{}'::jsonb,
  created_at timestamptz NOT NULL DEFAULT now(),
  updated_at timestamptz NOT NULL DEFAULT now(),
  UNIQUE (tenant_id, auth_user_id, external_sensor_id)
);

ALTER TABLE IF EXISTS user_sensors
  ADD COLUMN IF NOT EXISTS place_id uuid;

ALTER TABLE IF EXISTS user_sensors
  ADD COLUMN IF NOT EXISTS hive_id uuid;

CREATE INDEX IF NOT EXISTS idx_user_sensors_place
  ON user_sensors(tenant_id, auth_user_id, place_id, updated_at DESC);

CREATE INDEX IF NOT EXISTS idx_user_sensors_hive
  ON user_sensors(tenant_id, auth_user_id, hive_id, updated_at DESC);

CREATE INDEX IF NOT EXISTS idx_user_sensors_owner
  ON user_sensors(tenant_id, auth_user_id, status, created_at DESC);

CREATE UNIQUE INDEX IF NOT EXISTS idx_user_sensors_owner_ref
  ON user_sensors(sensor_id, tenant_id, auth_user_id);

DO $$
DECLARE
  constraint_name text;
BEGIN
  FOR constraint_name IN
    SELECT con.conname
    FROM pg_constraint con
    WHERE con.contype = 'f'
      AND con.conrelid = 'user_hives'::regclass
      AND con.confrelid = 'user_places'::regclass
      AND con.conkey = ARRAY[
        (SELECT attnum FROM pg_attribute WHERE attrelid = 'user_hives'::regclass AND attname = 'place_id')
      ]
  LOOP
    EXECUTE format('ALTER TABLE user_hives DROP CONSTRAINT %I', constraint_name);
  END LOOP;
END $$;

DO $$
DECLARE
  constraint_name text;
BEGIN
  FOR constraint_name IN
    SELECT con.conname
    FROM pg_constraint con
    WHERE con.contype = 'f'
      AND con.conrelid = 'user_sensors'::regclass
      AND con.confrelid = 'user_places'::regclass
      AND con.conkey = ARRAY[
        (SELECT attnum FROM pg_attribute WHERE attrelid = 'user_sensors'::regclass AND attname = 'place_id')
      ]
  LOOP
    EXECUTE format('ALTER TABLE user_sensors DROP CONSTRAINT %I', constraint_name);
  END LOOP;
END $$;

DO $$
DECLARE
  constraint_name text;
BEGIN
  FOR constraint_name IN
    SELECT con.conname
    FROM pg_constraint con
    WHERE con.contype = 'f'
      AND con.conrelid = 'user_sensors'::regclass
      AND con.confrelid = 'user_hives'::regclass
      AND con.conkey = ARRAY[
        (SELECT attnum FROM pg_attribute WHERE attrelid = 'user_sensors'::regclass AND attname = 'hive_id')
      ]
  LOOP
    EXECUTE format('ALTER TABLE user_sensors DROP CONSTRAINT %I', constraint_name);
  END LOOP;
END $$;

DO $$
BEGIN
  IF NOT EXISTS (
    SELECT 1 FROM pg_constraint
    WHERE conname = 'fk_user_hives_owned_place'
      AND conrelid = 'user_hives'::regclass
  ) THEN
    ALTER TABLE user_hives
      ADD CONSTRAINT fk_user_hives_owned_place
      FOREIGN KEY (place_id, tenant_id, auth_user_id)
      REFERENCES user_places(place_id, tenant_id, auth_user_id)
      ON DELETE NO ACTION;
  END IF;
END $$;

DO $$
BEGIN
  IF NOT EXISTS (
    SELECT 1 FROM pg_constraint
    WHERE conname = 'fk_user_sensors_owned_place'
      AND conrelid = 'user_sensors'::regclass
  ) THEN
    ALTER TABLE user_sensors
      ADD CONSTRAINT fk_user_sensors_owned_place
      FOREIGN KEY (place_id, tenant_id, auth_user_id)
      REFERENCES user_places(place_id, tenant_id, auth_user_id)
      ON DELETE NO ACTION;
  END IF;
END $$;

DO $$
BEGIN
  IF NOT EXISTS (
    SELECT 1 FROM pg_constraint
    WHERE conname = 'fk_user_sensors_owned_hive'
      AND conrelid = 'user_sensors'::regclass
  ) THEN
    ALTER TABLE user_sensors
      ADD CONSTRAINT fk_user_sensors_owned_hive
      FOREIGN KEY (hive_id, tenant_id, auth_user_id)
      REFERENCES user_hives(hive_id, tenant_id, auth_user_id)
      ON DELETE NO ACTION;
  END IF;
END $$;

CREATE OR REPLACE FUNCTION enforce_user_sensor_topology()
RETURNS trigger AS $$
DECLARE
  hive_place_id uuid;
BEGIN
  IF NEW.hive_id IS NULL THEN
    RETURN NEW;
  END IF;

  SELECT place_id
  INTO hive_place_id
  FROM user_hives
  WHERE hive_id = NEW.hive_id
    AND tenant_id = NEW.tenant_id
    AND auth_user_id = NEW.auth_user_id;

  IF NOT FOUND THEN
    RAISE EXCEPTION 'Hive does not belong to the sensor owner';
  END IF;

  IF hive_place_id IS NULL THEN
    IF NEW.place_id IS NOT NULL THEN
      RAISE EXCEPTION 'Hive is not assigned to the selected place';
    END IF;
    RETURN NEW;
  END IF;

  IF NEW.place_id IS NULL THEN
    NEW.place_id := hive_place_id;
    RETURN NEW;
  END IF;

  IF NEW.place_id <> hive_place_id THEN
    RAISE EXCEPTION 'Sensor place_id must match hive place_id';
  END IF;

  RETURN NEW;
END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS trg_enforce_user_sensor_topology ON user_sensors;
CREATE TRIGGER trg_enforce_user_sensor_topology
BEFORE INSERT OR UPDATE ON user_sensors
FOR EACH ROW
EXECUTE FUNCTION enforce_user_sensor_topology();

CREATE OR REPLACE FUNCTION sync_user_sensors_from_place()
RETURNS trigger AS $$
BEGIN
  UPDATE user_sensors
  SET location_label = NEW.place_name,
      updated_at = now()
  WHERE tenant_id = NEW.tenant_id
    AND auth_user_id = NEW.auth_user_id
    AND place_id = NEW.place_id;
  RETURN NEW;
END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS trg_sync_user_sensors_from_place ON user_places;
CREATE TRIGGER trg_sync_user_sensors_from_place
AFTER UPDATE OF place_name ON user_places
FOR EACH ROW
WHEN (OLD.place_name IS DISTINCT FROM NEW.place_name)
EXECUTE FUNCTION sync_user_sensors_from_place();

CREATE OR REPLACE FUNCTION sync_user_sensors_from_hive()
RETURNS trigger AS $$
DECLARE
  resolved_place_name text;
BEGIN
  IF NEW.place_id IS NOT NULL THEN
    SELECT place_name
    INTO resolved_place_name
    FROM user_places
    WHERE place_id = NEW.place_id
      AND tenant_id = NEW.tenant_id
      AND auth_user_id = NEW.auth_user_id;
  ELSE
    resolved_place_name := NULL;
  END IF;

  UPDATE user_sensors
  SET place_id = NEW.place_id,
      hive_name = NEW.hive_name,
      location_label = resolved_place_name,
      updated_at = now()
  WHERE tenant_id = NEW.tenant_id
    AND auth_user_id = NEW.auth_user_id
    AND hive_id = NEW.hive_id;
  RETURN NEW;
END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS trg_sync_user_sensors_from_hive ON user_hives;
CREATE TRIGGER trg_sync_user_sensors_from_hive
AFTER UPDATE OF place_id, hive_name ON user_hives
FOR EACH ROW
WHEN (OLD.place_id IS DISTINCT FROM NEW.place_id OR OLD.hive_name IS DISTINCT FROM NEW.hive_name)
EXECUTE FUNCTION sync_user_sensors_from_hive();

CREATE TABLE IF NOT EXISTS sensor_readings (
  reading_id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  sensor_id uuid NOT NULL,
  tenant_id text NOT NULL DEFAULT 'shared',
  auth_user_id text NOT NULL,
  reading_hash text NOT NULL DEFAULT '',
  observed_at timestamptz NOT NULL,
  metric_name text NOT NULL,
  unit text,
  numeric_value double precision,
  text_value text,
  quality_score double precision,
  metadata_json jsonb NOT NULL DEFAULT '{}'::jsonb,
  CONSTRAINT chk_sensor_readings_value_present CHECK (
    numeric_value IS NOT NULL OR COALESCE(BTRIM(text_value), '') <> ''
  ),
  created_at timestamptz NOT NULL DEFAULT now()
);

ALTER TABLE IF EXISTS sensor_readings
  ADD COLUMN IF NOT EXISTS reading_hash text NOT NULL DEFAULT '';

DO $$
BEGIN
  IF NOT EXISTS (
    SELECT 1
    FROM pg_constraint
    WHERE conname = 'chk_sensor_readings_value_present'
      AND conrelid = 'sensor_readings'::regclass
  ) THEN
    ALTER TABLE sensor_readings
      ADD CONSTRAINT chk_sensor_readings_value_present
      CHECK (numeric_value IS NOT NULL OR COALESCE(BTRIM(text_value), '') <> '');
  END IF;
END $$;

CREATE INDEX IF NOT EXISTS idx_sensor_readings_owner_time
  ON sensor_readings(tenant_id, auth_user_id, observed_at DESC);

CREATE INDEX IF NOT EXISTS idx_sensor_readings_sensor_metric_time
  ON sensor_readings(sensor_id, metric_name, observed_at DESC);

CREATE UNIQUE INDEX IF NOT EXISTS idx_sensor_readings_sensor_hash
  ON sensor_readings(sensor_id, reading_hash);

DO $$
BEGIN
  IF NOT EXISTS (
    SELECT 1 FROM pg_constraint
    WHERE conname = 'fk_sensor_readings_owned_sensor'
      AND conrelid = 'sensor_readings'::regclass
  ) THEN
    ALTER TABLE sensor_readings
      ADD CONSTRAINT fk_sensor_readings_owned_sensor
      FOREIGN KEY (sensor_id, tenant_id, auth_user_id)
      REFERENCES user_sensors(sensor_id, tenant_id, auth_user_id)
      ON DELETE CASCADE;
  END IF;
END $$;

ALTER TABLE IF EXISTS agent_query_runs
  ADD COLUMN IF NOT EXISTS query_signature text;

ALTER TABLE IF EXISTS agent_query_runs
  ADD COLUMN IF NOT EXISTS query_keywords jsonb NOT NULL DEFAULT '[]'::jsonb;

ALTER TABLE IF EXISTS agent_query_runs
  ADD COLUMN IF NOT EXISTS prompt_payload jsonb NOT NULL DEFAULT '{}'::jsonb;

ALTER TABLE IF EXISTS agent_query_runs
  ADD COLUMN IF NOT EXISTS raw_response_payload jsonb NOT NULL DEFAULT '{}'::jsonb;

ALTER TABLE IF EXISTS agent_query_runs
  ADD COLUMN IF NOT EXISTS final_response_payload jsonb NOT NULL DEFAULT '{}'::jsonb;

ALTER TABLE IF EXISTS agent_query_runs
  ADD COLUMN IF NOT EXISTS review_status text NOT NULL DEFAULT 'unreviewed';

ALTER TABLE IF EXISTS agent_query_runs
  ADD COLUMN IF NOT EXISTS review_reason text;

ALTER TABLE IF EXISTS agent_query_runs
  ADD COLUMN IF NOT EXISTS reviewed_at timestamptz;

ALTER TABLE IF EXISTS agent_query_runs
  ADD COLUMN IF NOT EXISTS reviewed_by text;

CREATE TABLE IF NOT EXISTS agent_answer_reviews (
  review_id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  query_run_id uuid NOT NULL REFERENCES agent_query_runs(query_run_id) ON DELETE CASCADE,
  decision text NOT NULL,
  reviewer text NOT NULL DEFAULT 'admin',
  notes text,
  payload jsonb NOT NULL DEFAULT '{}'::jsonb,
  created_at timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS agent_runtime_configs (
  tenant_id text PRIMARY KEY,
  settings_json jsonb NOT NULL DEFAULT '{}'::jsonb,
  updated_by text,
  created_at timestamptz NOT NULL DEFAULT now(),
  updated_at timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS agent_runtime_secrets (
  tenant_id text PRIMARY KEY,
  api_key_override text,
  updated_by text,
  created_at timestamptz NOT NULL DEFAULT now(),
  updated_at timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS agent_query_patterns (
  tenant_id text NOT NULL,
  pattern_signature text NOT NULL,
  keywords_json jsonb NOT NULL DEFAULT '[]'::jsonb,
  example_query text,
  approved_count integer NOT NULL DEFAULT 0,
  rejected_count integer NOT NULL DEFAULT 0,
  needs_review_count integer NOT NULL DEFAULT 0,
  total_feedback_count integer NOT NULL DEFAULT 0,
  last_query_run_id uuid REFERENCES agent_query_runs(query_run_id) ON DELETE SET NULL,
  last_feedback_at timestamptz,
  last_feedback_by text,
  created_at timestamptz NOT NULL DEFAULT now(),
  updated_at timestamptz NOT NULL DEFAULT now(),
  PRIMARY KEY (tenant_id, pattern_signature)
);

ALTER TABLE IF EXISTS agent_query_patterns
  ADD COLUMN IF NOT EXISTS router_cache_json jsonb NOT NULL DEFAULT '{}'::jsonb;

ALTER TABLE IF EXISTS agent_query_patterns
  ADD COLUMN IF NOT EXISTS router_cached_at timestamptz;

ALTER TABLE IF EXISTS agent_query_patterns
  ADD COLUMN IF NOT EXISTS router_cache_hits integer NOT NULL DEFAULT 0;

ALTER TABLE IF EXISTS agent_query_patterns
  ADD COLUMN IF NOT EXISTS router_model text;

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
);

CREATE INDEX IF NOT EXISTS idx_agent_query_embeddings_lookup
  ON agent_query_embeddings (tenant_id, query_hash, cached_at DESC);

DO $$
BEGIN
  IF EXISTS (
    SELECT 1
    FROM information_schema.tables
    WHERE table_schema = 'public' AND table_name = 'agent_runtime_config'
  ) THEN
    INSERT INTO agent_runtime_configs (tenant_id, settings_json, updated_by, created_at, updated_at)
    SELECT 'shared', config_json, updated_by, created_at, updated_at
    FROM agent_runtime_config
    ON CONFLICT (tenant_id) DO NOTHING;

    DROP TABLE agent_runtime_config;
  END IF;
END $$;

CREATE TABLE IF NOT EXISTS kg_raw_extractions (
  extraction_id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  chunk_id text NOT NULL REFERENCES document_chunks(chunk_id) ON DELETE CASCADE,
  document_id uuid NOT NULL REFERENCES documents(document_id) ON DELETE CASCADE,
  payload jsonb NOT NULL,
  status text NOT NULL DEFAULT 'raw',
  created_at timestamptz NOT NULL DEFAULT now()
);

CREATE UNIQUE INDEX IF NOT EXISTS kg_raw_extractions_chunk_idx
  ON kg_raw_extractions (chunk_id);

CREATE TABLE IF NOT EXISTS kg_entities (
  entity_id text PRIMARY KEY,
  canonical_name text NOT NULL,
  entity_type text NOT NULL,
  source text,
  created_at timestamptz NOT NULL DEFAULT now(),
  updated_at timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS kg_assertions (
  assertion_id text PRIMARY KEY,
  document_id uuid NOT NULL REFERENCES documents(document_id) ON DELETE CASCADE,
  chunk_id text NOT NULL REFERENCES document_chunks(chunk_id) ON DELETE CASCADE,
  subject_entity_id text NOT NULL REFERENCES kg_entities(entity_id) ON DELETE RESTRICT,
  predicate text NOT NULL,
  object_entity_id text,
  object_literal text,
  confidence numeric(5,4) NOT NULL,
  qualifiers jsonb NOT NULL DEFAULT '{}'::jsonb,
  status text NOT NULL DEFAULT 'accepted',
  created_at timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS kg_assertion_evidence (
  evidence_id text PRIMARY KEY,
  assertion_id text NOT NULL REFERENCES kg_assertions(assertion_id) ON DELETE CASCADE,
  excerpt text NOT NULL,
  start_offset integer,
  end_offset integer,
  created_at timestamptz NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS kg_assertions_chunk_status_confidence_idx
  ON kg_assertions (chunk_id, status, confidence DESC, created_at DESC);

CREATE INDEX IF NOT EXISTS kg_assertions_subject_status_confidence_idx
  ON kg_assertions (subject_entity_id, status, confidence DESC, created_at DESC);

CREATE INDEX IF NOT EXISTS kg_assertions_object_status_confidence_idx
  ON kg_assertions (object_entity_id, status, confidence DESC, created_at DESC);

CREATE INDEX IF NOT EXISTS kg_assertions_document_status_confidence_idx
  ON kg_assertions (document_id, status, confidence DESC, created_at DESC);

CREATE INDEX IF NOT EXISTS kg_assertion_evidence_assertion_created_idx
  ON kg_assertion_evidence (assertion_id, created_at DESC);

CREATE INDEX IF NOT EXISTS kg_entities_canonical_name_trgm_idx
  ON kg_entities USING gin (canonical_name gin_trgm_ops);

CREATE INDEX IF NOT EXISTS kg_entities_type_trgm_idx
  ON kg_entities USING gin (entity_type gin_trgm_ops);

CREATE INDEX IF NOT EXISTS kg_assertions_object_literal_trgm_idx
  ON kg_assertions USING gin ((COALESCE(object_literal, '')) gin_trgm_ops);

CREATE INDEX IF NOT EXISTS kg_assertion_evidence_excerpt_trgm_idx
  ON kg_assertion_evidence USING gin (excerpt gin_trgm_ops);

CREATE TABLE IF NOT EXISTS agent_query_sources (
  source_link_id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  query_run_id uuid NOT NULL REFERENCES agent_query_runs(query_run_id) ON DELETE CASCADE,
  source_kind text NOT NULL,
  source_id text NOT NULL,
  document_id uuid REFERENCES documents(document_id) ON DELETE SET NULL,
  chunk_id text REFERENCES document_chunks(chunk_id) ON DELETE SET NULL,
  assertion_id text REFERENCES kg_assertions(assertion_id) ON DELETE SET NULL,
  entity_id text REFERENCES kg_entities(entity_id) ON DELETE SET NULL,
  rank integer,
  score numeric(12,6),
  selected boolean NOT NULL DEFAULT false,
  payload jsonb NOT NULL DEFAULT '{}'::jsonb,
  corpus_snapshot_id uuid REFERENCES corpus_snapshots(snapshot_id) ON DELETE SET NULL,
  created_at timestamptz NOT NULL DEFAULT now()
);

ALTER TABLE agent_query_sources
  ADD COLUMN IF NOT EXISTS corpus_snapshot_id uuid REFERENCES corpus_snapshots(snapshot_id) ON DELETE SET NULL;

CREATE OR REPLACE VIEW chunk_metadata_view AS
SELECT
  dc.chunk_id,
  dc.document_id,
  d.filename,
  d.document_class,
  d.tenant_id,
  dc.chunk_index,
  dc.page_start,
  dc.page_end,
  dc.prev_chunk_id,
  dc.next_chunk_id,
  dc.parser_version,
  dc.chunker_version,
  COALESCE(cv.status, dc.status) AS validation_status,
  cv.quality_score,
  cv.reasons,
  dc.metadata_json ->> 'chunk_role' AS chunk_role,
  dc.metadata_json ->> 'section_title' AS section_title,
  dc.metadata_json ->> 'document_class' AS metadata_document_class,
  dc.metadata_json -> 'ontology_classes' AS ontology_classes,
  dc.metadata_json
FROM document_chunks dc
JOIN documents d ON d.document_id = dc.document_id
LEFT JOIN chunk_validations cv ON cv.chunk_id = dc.chunk_id;
