import argparse
import json
import os
from collections import defaultdict
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
    parser = argparse.ArgumentParser(description="Export PostgreSQL schema.")
    parser.add_argument("--host", default=os.getenv("PGHOST", "localhost"))
    parser.add_argument("--port", default=os.getenv("PGPORT", "5432"))
    parser.add_argument("--dbname", default=os.getenv("PGDATABASE"))
    parser.add_argument("--user", default=os.getenv("PGUSER"))
    parser.add_argument("--password", default=os.getenv("PGPASSWORD"))
    parser.add_argument("--schema", default="public")
    parser.add_argument("--output", required=True)
    parser.add_argument("--format", choices=("json", "markdown"))
    args = parser.parse_args()
    if not args.dbname:
        parser.error("--dbname is required or set PGDATABASE")
    if not args.user:
        parser.error("--user is required or set PGUSER")
    return args


def connect(driver_name, driver, args):
    if driver_name == "psycopg":
        return driver.connect(
            host=args.host,
            port=args.port,
            dbname=args.dbname,
            user=args.user,
            password=args.password,
            autocommit=True,
        )
    conn = driver.connect(
        host=args.host,
        port=args.port,
        dbname=args.dbname,
        user=args.user,
        password=args.password,
    )
    conn.autocommit = True
    return conn


def fetch_all(cur, query, params):
    cur.execute(query, params)
    columns = [desc[0] for desc in cur.description]
    return [dict(zip(columns, row)) for row in cur.fetchall()]


def gather_schema(cur, schema_name):
    tables = fetch_all(
        cur,
        """
        SELECT table_name
        FROM information_schema.tables
        WHERE table_schema = %s
          AND table_type = 'BASE TABLE'
        ORDER BY table_name
        """,
        (schema_name,),
    )
    columns = fetch_all(
        cur,
        """
        SELECT
          table_name,
          column_name,
          data_type,
          is_nullable,
          column_default,
          ordinal_position
        FROM information_schema.columns
        WHERE table_schema = %s
        ORDER BY table_name, ordinal_position
        """,
        (schema_name,),
    )
    indexes = fetch_all(
        cur,
        """
        SELECT tablename AS table_name, indexname AS index_name, indexdef
        FROM pg_indexes
        WHERE schemaname = %s
        ORDER BY tablename, indexname
        """,
        (schema_name,),
    )
    foreign_keys = fetch_all(
        cur,
        """
        SELECT
          tc.table_name,
          tc.constraint_name,
          kcu.column_name,
          ccu.table_name AS foreign_table_name,
          ccu.column_name AS foreign_column_name
        FROM information_schema.table_constraints tc
        JOIN information_schema.key_column_usage kcu
          ON tc.constraint_name = kcu.constraint_name
         AND tc.table_schema = kcu.table_schema
        JOIN information_schema.constraint_column_usage ccu
          ON ccu.constraint_name = tc.constraint_name
         AND ccu.table_schema = tc.table_schema
        WHERE tc.constraint_type = 'FOREIGN KEY'
          AND tc.table_schema = %s
        ORDER BY tc.table_name, tc.constraint_name
        """,
        (schema_name,),
    )
    views = fetch_all(
        cur,
        """
        SELECT table_name AS view_name, view_definition
        FROM information_schema.views
        WHERE table_schema = %s
        ORDER BY table_name
        """,
        (schema_name,),
    )
    triggers = fetch_all(
        cur,
        """
        SELECT
          event_object_table AS table_name,
          trigger_name,
          action_timing,
          event_manipulation,
          action_statement
        FROM information_schema.triggers
        WHERE trigger_schema = %s
        ORDER BY event_object_table, trigger_name
        """,
        (schema_name,),
    )

    grouped = defaultdict(
        lambda: {"columns": [], "indexes": [], "foreign_keys": [], "triggers": []}
    )
    for row in columns:
        grouped[row["table_name"]]["columns"].append(row)
    for row in indexes:
        grouped[row["table_name"]]["indexes"].append(row)
    for row in foreign_keys:
        grouped[row["table_name"]]["foreign_keys"].append(row)
    for row in triggers:
        grouped[row["table_name"]]["triggers"].append(row)

    return {
        "schema": schema_name,
        "tables": [
            {
                "table_name": table["table_name"],
                **grouped[table["table_name"]],
            }
            for table in tables
        ],
        "views": views,
    }


def render_markdown(data):
    lines = [f"# Schema Export: {data['schema']}", ""]
    for table in data["tables"]:
        lines.append(f"## Table `{table['table_name']}`")
        lines.append("")
        lines.append("| Column | Type | Nullable | Default |")
        lines.append("| --- | --- | --- | --- |")
        for column in table["columns"]:
            lines.append(
                f"| {column['column_name']} | {column['data_type']} | "
                f"{column['is_nullable']} | {column['column_default'] or ''} |"
            )
        lines.append("")

        if table["indexes"]:
            lines.append("### Indexes")
            for index in table["indexes"]:
                lines.append(f"- `{index['index_name']}`: `{index['indexdef']}`")
            lines.append("")

        if table["foreign_keys"]:
            lines.append("### Foreign Keys")
            for fk in table["foreign_keys"]:
                lines.append(
                    f"- `{fk['constraint_name']}`: `{fk['column_name']}` -> "
                    f"`{fk['foreign_table_name']}.{fk['foreign_column_name']}`"
                )
            lines.append("")

        if table["triggers"]:
            lines.append("### Triggers")
            for trigger in table["triggers"]:
                lines.append(
                    f"- `{trigger['trigger_name']}`: {trigger['action_timing']} "
                    f"{trigger['event_manipulation']} -> `{trigger['action_statement']}`"
                )
            lines.append("")

    if data["views"]:
        lines.append("## Views")
        lines.append("")
        for view in data["views"]:
            lines.append(f"### `{view['view_name']}`")
            lines.append("```sql")
            lines.append(view["view_definition"] or "")
            lines.append("```")
            lines.append("")

    return "\n".join(lines).rstrip() + "\n"


def main():
    args = parse_args()
    fmt = args.format or ("markdown" if Path(args.output).suffix.lower() == ".md" else "json")
    driver_name, driver = load_driver()
    conn = connect(driver_name, driver, args)
    try:
        with conn.cursor() as cur:
            data = gather_schema(cur, args.schema)
    finally:
        conn.close()

    output_path = Path(args.output)
    if fmt == "markdown":
        output_path.write_text(render_markdown(data), encoding="utf-8")
    else:
        output_path.write_text(json.dumps(data, indent=2), encoding="utf-8")


if __name__ == "__main__":
    main()
