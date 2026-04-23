import argparse
import os
import shutil
import subprocess
from datetime import datetime
from pathlib import Path


def parse_args():
    parser = argparse.ArgumentParser(description="Backup or restore PostgreSQL databases.")
    parser.add_argument("--host", default=os.getenv("PGHOST", "localhost"))
    parser.add_argument("--port", default=os.getenv("PGPORT", "5432"))
    parser.add_argument("--dbname", default=os.getenv("PGDATABASE"))
    parser.add_argument("--user", default=os.getenv("PGUSER"))
    parser.add_argument("--password", default=os.getenv("PGPASSWORD"))
    parser.add_argument("--backup-dir", required=True)
    parser.add_argument("--table", action="append", default=[], help="Specific table to back up.")
    parser.add_argument(
        "--format",
        choices=("plain", "custom"),
        default="custom",
        help="Backup format for pg_dump.",
    )
    parser.add_argument("--restore", help="Restore from a backup file.")
    parser.add_argument(
        "--rotate",
        type=int,
        default=0,
        help="Keep only the newest N backups for this database.",
    )
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()
    if not args.dbname:
        parser.error("--dbname is required or set PGDATABASE")
    if not args.user:
        parser.error("--user is required or set PGUSER")
    return args


def require_tool(name):
    if shutil.which(name):
        return name
    raise SystemExit(f"Required PostgreSQL tool not found in PATH: {name}")


def base_env(password):
    env = os.environ.copy()
    if password:
        env["PGPASSWORD"] = password
    return env


def make_backup_path(args):
    backup_dir = Path(args.backup_dir)
    backup_dir.mkdir(parents=True, exist_ok=True)
    stamp = datetime.now().strftime("%Y%m%d-%H%M%S")
    suffix = ".sql" if args.format == "plain" else ".dump"
    return backup_dir / f"{args.dbname}-{stamp}{suffix}"


def run_command(command, env, dry_run):
    print(" ".join(command))
    if dry_run:
        return
    subprocess.run(command, check=True, env=env)


def rotate_backups(backup_dir, dbname, keep):
    if keep <= 0:
        return
    files = sorted(
        Path(backup_dir).glob(f"{dbname}-*"),
        key=lambda path: path.stat().st_mtime,
        reverse=True,
    )
    for old_file in files[keep:]:
        old_file.unlink()


def do_backup(args):
    pg_dump = require_tool("pg_dump")
    output_path = make_backup_path(args)
    command = [
        pg_dump,
        "--host",
        args.host,
        "--port",
        str(args.port),
        "--username",
        args.user,
        "--dbname",
        args.dbname,
        "--file",
        str(output_path),
    ]
    if args.format == "custom":
        command.extend(["--format", "custom"])
    for table in args.table:
        command.extend(["--table", table])
    run_command(command, base_env(args.password), args.dry_run)
    if not args.dry_run:
        rotate_backups(args.backup_dir, args.dbname, args.rotate)
    print(f"Backup ready: {output_path}")


def do_restore(args):
    restore_path = Path(args.restore)
    if not restore_path.exists():
        raise SystemExit(f"Backup file not found: {restore_path}")

    env = base_env(args.password)
    if restore_path.suffix == ".sql":
        psql = require_tool("psql")
        command = [
            psql,
            "--host",
            args.host,
            "--port",
            str(args.port),
            "--username",
            args.user,
            "--dbname",
            args.dbname,
            "--file",
            str(restore_path),
        ]
    else:
        pg_restore = require_tool("pg_restore")
        command = [
            pg_restore,
            "--host",
            args.host,
            "--port",
            str(args.port),
            "--username",
            args.user,
            "--dbname",
            args.dbname,
            "--clean",
            "--if-exists",
            str(restore_path),
        ]
    run_command(command, env, args.dry_run)
    print(f"Restore completed from: {restore_path}")


def main():
    args = parse_args()
    if args.restore:
        do_restore(args)
    else:
        do_backup(args)


if __name__ == "__main__":
    main()
