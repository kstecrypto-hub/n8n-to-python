from __future__ import annotations

import argparse
import getpass
import json

from src.bee_ingestion.auth_store import AuthStore


def main() -> None:
    parser = argparse.ArgumentParser(description="Provision login users for the public Hive Signal app.")
    parser.add_argument("--email", required=True, help="Email address for the user to create.")
    parser.add_argument("--password", help="Password for the user. If omitted, an interactive prompt is used.")
    parser.add_argument("--display-name", help="Optional display name shown in the chat UI.")
    parser.add_argument("--tenant-id", default="shared", help="Tenant id for the user. Defaults to the public tenant.")
    args = parser.parse_args()

    password = args.password or getpass.getpass("Password: ")
    store = AuthStore()
    user = store.create_user(
        args.email,
        password,
        display_name=args.display_name,
        tenant_id=args.tenant_id,
    )
    print(
        json.dumps(
            {
                "user_id": user.get("user_id"),
                "email": user.get("email"),
                "display_name": user.get("display_name"),
                "tenant_id": user.get("tenant_id"),
                "role": user.get("role"),
            },
            indent=2,
        )
    )


if __name__ == "__main__":
    main()
