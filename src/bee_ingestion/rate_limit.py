from __future__ import annotations

from collections import deque
from datetime import datetime, timedelta, timezone
from threading import Lock
from time import monotonic

import psycopg
from psycopg import sql

from src.bee_ingestion.settings import settings


def _utcnow() -> datetime:
    return datetime.now(timezone.utc)


class SlidingWindowRateLimiter:
    def __init__(self, dsn: str | None = None, *, schema_name: str | None = None) -> None:
        self._lock = Lock()
        self._hits: dict[str, deque[float]] = {}
        self._dsn = str(dsn or settings.auth_postgres_dsn or "").strip()
        self._schema_name = str(schema_name or settings.auth_postgres_schema or "auth").strip()
        self._table_ready = False
        self._last_global_cleanup = 0.0

    def _connect(self) -> psycopg.Connection:
        conn = psycopg.connect(self._dsn, autocommit=True)
        with conn.cursor() as cur:
            cur.execute(sql.SQL("SET search_path TO {}").format(sql.Identifier(self._schema_name)))
        return conn

    def _ensure_table(self) -> None:
        if not self._dsn or self._table_ready:
            return
        with self._connect() as conn:
            with conn.cursor() as cur:
                cur.execute(sql.SQL("CREATE SCHEMA IF NOT EXISTS {}").format(sql.Identifier(self._schema_name)))
                cur.execute(
                    """
                    CREATE TABLE IF NOT EXISTS api_rate_limit_hits (
                      hit_id bigserial PRIMARY KEY,
                      bucket_key text NOT NULL,
                      hit_at timestamptz NOT NULL DEFAULT now()
                    )
                    """
                )
                cur.execute(
                    """
                    CREATE INDEX IF NOT EXISTS api_rate_limit_hits_bucket_time_idx
                      ON api_rate_limit_hits (bucket_key, hit_at)
                    """
                )
        self._table_ready = True

    def _check_memory(self, key: str, *, limit: int, window_seconds: int) -> int | None:
        now = monotonic()
        cutoff = now - window_seconds
        with self._lock:
            bucket = self._hits.setdefault(key, deque())
            while bucket and bucket[0] <= cutoff:
                bucket.popleft()
            if len(bucket) >= limit:
                oldest = bucket[0]
                return max(1, int(window_seconds - (now - oldest)))
            bucket.append(now)
            if not bucket:
                self._hits.pop(key, None)
        return None

    def _check_postgres(self, key: str, *, limit: int, window_seconds: int) -> int | None:
        self._ensure_table()
        now = _utcnow()
        cutoff = now - timedelta(seconds=window_seconds)
        with self._connect() as conn:
            with conn.cursor() as cur:
                now_monotonic = monotonic()
                if now_monotonic - self._last_global_cleanup >= 300:
                    cur.execute(
                        "DELETE FROM api_rate_limit_hits WHERE hit_at <= %s",
                        (now - timedelta(days=2),),
                    )
                    self._last_global_cleanup = now_monotonic
                cur.execute(
                    "DELETE FROM api_rate_limit_hits WHERE bucket_key = %s AND hit_at <= %s",
                    (key, cutoff),
                )
                cur.execute(
                    """
                    SELECT hit_at
                    FROM api_rate_limit_hits
                    WHERE bucket_key = %s
                    ORDER BY hit_at ASC
                    LIMIT %s
                    """,
                    (key, limit),
                )
                rows = cur.fetchall()
                if len(rows) >= limit:
                    oldest = rows[0][0]
                    retry_after = max(1, int(window_seconds - (now - oldest).total_seconds()))
                    return retry_after
                cur.execute(
                    "INSERT INTO api_rate_limit_hits (bucket_key, hit_at) VALUES (%s, %s)",
                    (key, now),
                )
        return None

    def check(self, key: str, *, limit: int, window_seconds: int) -> int | None:
        normalized_key = str(key or "").strip()
        if not normalized_key or limit <= 0 or window_seconds <= 0:
            return None
        if not self._dsn:
            return self._check_memory(normalized_key, limit=limit, window_seconds=window_seconds)
        return self._check_postgres(normalized_key, limit=limit, window_seconds=window_seconds)

    def clear(self) -> None:
        with self._lock:
            self._hits.clear()
        if not self._dsn:
            return
        self._ensure_table()
        with self._connect() as conn:
            with conn.cursor() as cur:
                cur.execute("TRUNCATE TABLE api_rate_limit_hits")
