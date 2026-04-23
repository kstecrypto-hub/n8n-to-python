from __future__ import annotations

import argparse
import json

from src.bee_ingestion.auth_store import AuthStore
from src.bee_ingestion.settings import settings


def main() -> None:
    parser = argparse.ArgumentParser(description="Migrate auth data into the dedicated Postgres identity database.")
    parser.add_argument(
        "--source",
        choices=("shared-postgres", "sqlite"),
        default="shared-postgres",
        help="Source location for the auth data.",
    )
    parser.add_argument(
        "--sqlite-path",
        default=settings.auth_legacy_sqlite_path,
        help="Path to the legacy SQLite auth database when --source=sqlite.",
    )
    parser.add_argument(
        "--source-dsn",
        default=str(settings.postgres_dsn),
        help="Source Postgres DSN when --source=shared-postgres.",
    )
    parser.add_argument(
        "--source-schema",
        default=settings.auth_postgres_schema,
        help="Source Postgres auth schema when --source=shared-postgres.",
    )
    parser.add_argument(
        "--dsn",
        default=str(settings.auth_postgres_dsn or settings.postgres_dsn),
        help="Target Postgres DSN for the dedicated identity database.",
    )
    parser.add_argument(
        "--schema",
        default=settings.auth_postgres_schema,
        help="Target Postgres schema name for auth tables in the identity database.",
    )
    parser.add_argument(
        "--skip-sessions",
        action="store_true",
        help="Import users only and skip existing web sessions.",
    )
    args = parser.parse_args()

    target_store = AuthStore(dsn=args.dsn, schema_name=args.schema)
    if args.source == "sqlite":
        result = target_store.import_from_sqlite(args.sqlite_path, include_sessions=not args.skip_sessions)
    else:
        source_store = AuthStore(dsn=args.source_dsn, schema_name=args.source_schema)
        result = target_store.import_from_store(source_store, include_sessions=not args.skip_sessions)
    print(
        json.dumps(
            {
                "source": args.source,
                "sqlite_path": args.sqlite_path if args.source == "sqlite" else None,
                "source_dsn": args.source_dsn if args.source == "shared-postgres" else None,
                "source_schema": args.source_schema if args.source == "shared-postgres" else None,
                "target_dsn": args.dsn,
                "target_schema": args.schema,
                "users_imported": result.get("users", 0),
                "sessions_imported": result.get("sessions", 0),
            },
            indent=2,
        )
    )


if __name__ == "__main__":
    main()
