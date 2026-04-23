import argparse
import csv
import json
import os
import sys
from pathlib import Path


def load_driver():
    try:
        import psycopg  # type: ignore

        return "psycopg", psycopg
    except ImportError:
        try:
            import psycopg2  # type: ignore

            return "psycopg2", psycopg2
        except ImportError as exc:
            raise SystemExit(
                "PostgreSQL driver not found. Install `psycopg` or `psycopg2`."
            ) from exc


def parse_args():
    parser = argparse.ArgumentParser(description="Run PostgreSQL queries.")
    parser.add_argument("--host", default=os.getenv("PGHOST", "localhost"))
    parser.add_argument("--port", default=os.getenv("PGPORT", "5432"))
    parser.add_argument("--dbname", default=os.getenv("PGDATABASE"))
    parser.add_argument("--user", default=os.getenv("PGUSER"))
    parser.add_argument("--password", default=os.getenv("PGPASSWORD"))
    parser.add_argument("--query", help="SQL query text.")
    parser.add_argument("--file", help="Path to a .sql file.")
    parser.add_argument(
        "--format",
        choices=("table", "json", "csv"),
        default="table",
        help="Output format for row-returning queries.",
    )
    parser.add_argument("--output", help="Write results to a file.")
    parser.add_argument(
        "--commit",
        action="store_true",
        help="Commit mutating statements. Without this flag, mutating statements are rejected.",
    )
    parser.add_argument(
        "--fetch",
        action="store_true",
        help="Force fetching rows even for statements such as EXPLAIN ANALYZE.",
    )
    args = parser.parse_args()

    if not args.dbname:
        parser.error("--dbname is required or set PGDATABASE")
    if not args.user:
        parser.error("--user is required or set PGUSER")
    if bool(args.query) == bool(args.file):
        parser.error("Provide exactly one of --query or --file")
    return args


def read_query(args):
    if args.query:
        return args.query.strip()
    return Path(args.file).read_text(encoding="utf-8").strip()


def is_mutating(query):
    first = query.lstrip().split(None, 1)[0].lower()
    return first in {
        "insert",
        "update",
        "delete",
        "create",
        "alter",
        "drop",
        "truncate",
        "grant",
        "revoke",
        "comment",
    }


def connect(driver_name, driver, args):
    if driver_name == "psycopg":
        return driver.connect(
            host=args.host,
            port=args.port,
            dbname=args.dbname,
            user=args.user,
            password=args.password,
            autocommit=False,
        )
    return driver.connect(
        host=args.host,
        port=args.port,
        dbname=args.dbname,
        user=args.user,
        password=args.password,
    )


def rows_to_table(columns, rows):
    widths = [len(str(col)) for col in columns]
    for row in rows:
        for i, value in enumerate(row):
            widths[i] = max(widths[i], len("" if value is None else str(value)))

    def fmt_row(values):
        return " | ".join(
            ("" if value is None else str(value)).ljust(widths[i])
            for i, value in enumerate(values)
        )

    header = fmt_row(columns)
    divider = "-+-".join("-" * width for width in widths)
    body = "\n".join(fmt_row(row) for row in rows)
    return f"{header}\n{divider}\n{body}" if body else f"{header}\n{divider}"


def encode_rows(columns, rows, fmt):
    if fmt == "json":
        return json.dumps([dict(zip(columns, row)) for row in rows], indent=2, default=str)
    if fmt == "csv":
        from io import StringIO

        buffer = StringIO()
        writer = csv.writer(buffer)
        writer.writerow(columns)
        writer.writerows(rows)
        return buffer.getvalue()
    return rows_to_table(columns, rows)


def main():
    args = parse_args()
    query = read_query(args)
    if is_mutating(query) and not args.commit:
        raise SystemExit(
            "Mutating statement detected. Re-run with --commit if you intend to apply changes."
        )

    driver_name, driver = load_driver()
    conn = connect(driver_name, driver, args)
    try:
        with conn.cursor() as cur:
            cur.execute(query)
            has_rows = cur.description is not None
            if has_rows or args.fetch:
                rows = cur.fetchall()
                columns = [col[0] for col in cur.description]
                rendered = encode_rows(columns, rows, args.format)
            else:
                rendered = f"Statement executed successfully. Rows affected: {cur.rowcount}"

            if is_mutating(query):
                conn.commit()
            else:
                conn.rollback()

            if args.output:
                Path(args.output).write_text(rendered, encoding="utf-8")
            else:
                sys.stdout.write(rendered)
                if not rendered.endswith("\n"):
                    sys.stdout.write("\n")
    finally:
        conn.close()


if __name__ == "__main__":
    main()
