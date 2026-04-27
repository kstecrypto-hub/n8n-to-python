CREATE SCHEMA IF NOT EXISTS auth;

CREATE TABLE IF NOT EXISTS auth.auth_users (
  user_id text PRIMARY KEY,
  email text NOT NULL UNIQUE,
  display_name text,
  tenant_id text NOT NULL DEFAULT 'shared',
  role text NOT NULL DEFAULT 'member',
  status text NOT NULL DEFAULT 'active',
  permissions_json jsonb NOT NULL DEFAULT '[]'::jsonb,
  password_hash text NOT NULL,
  password_salt text NOT NULL,
  password_iterations integer NOT NULL,
  created_at timestamptz NOT NULL DEFAULT now(),
  updated_at timestamptz NOT NULL DEFAULT now(),
  last_login_at timestamptz
);

CREATE INDEX IF NOT EXISTS auth_users_tenant_idx
  ON auth.auth_users (tenant_id, role, status, created_at DESC);

CREATE TABLE IF NOT EXISTS auth.auth_sessions (
  auth_session_id text PRIMARY KEY,
  user_id text NOT NULL REFERENCES auth.auth_users(user_id) ON DELETE CASCADE,
  tenant_id text NOT NULL DEFAULT 'shared',
  session_token_hash text NOT NULL,
  created_at timestamptz NOT NULL,
  last_seen_at timestamptz NOT NULL,
  expires_at timestamptz NOT NULL,
  revoked_at timestamptz
);

CREATE INDEX IF NOT EXISTS auth_sessions_user_idx
  ON auth.auth_sessions (user_id, expires_at);

CREATE INDEX IF NOT EXISTS auth_sessions_active_idx
  ON auth.auth_sessions (tenant_id, revoked_at, expires_at);
