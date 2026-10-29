import argparse
import json
import sys

import uuid
from datetime import datetime, timezone

import psycopg

from aster import lake, obs
from aster.config import Config
from aster.ingestion import ingest
from aster.source import create_database, migrate, seed


def connect(config):
    return psycopg.connect(
        **config.postgres_kwargs(), autocommit=True, connect_timeout=5
    )


def new_run_id():
    return f"{datetime.now(timezone.utc):%Y%m%dT%H%M%SZ}-{uuid.uuid4().hex[:8]}"


def dispatch(args, config, run_id):
    if args.command == "migrate":
        with connect(config) as connection:
            migrate(connection)
        return {"migrated": True}
    if args.command == "postgres-init":
        with connect(config) as connection:
            created = create_database(connection, args.database)
        return {"database": args.database, "created": created}
    if args.command == "seed":
        with connect(config) as connection:
            seed(connection, args.size, args.seed)
        return {"size": args.size, "seed": args.seed}
    if args.command == "extract":
        s3 = lake.s3_client(config)
        obs.configure_logging("aster", run_id, config.loki_url)
        with connect(config) as connection:
            rows = ingest(connection, s3, config, run_id)
        return {"rows": rows}
    raise ValueError(f"Unknown command {args.command}")


def build_parser():
    parser = argparse.ArgumentParser(prog="aster")
    parser.add_argument("--run-id", default=None)
    commands = parser.add_subparsers(dest="command", required=True)
    commands.add_parser("migrate")
    init = commands.add_parser("postgres-init")
    init.add_argument("--database", default="airflow")
    seed_parser = commands.add_parser("seed")
    seed_parser.add_argument(
        "--size", choices=("small", "medium", "large"), default="small"
    )
    seed_parser.add_argument("--seed", type=int, default=42)
    commands.add_parser("extract")
    return parser


def main(argv=None):
    args = build_parser().parse_args(argv)
    config = Config.from_env()
    run_id = args.run_id or new_run_id()
    try:
        payload = dispatch(args, config, run_id)
    except Exception as error:
        obs.log(
            "command_failed",
            command=args.command,
            error=type(error).__name__,
            detail=str(error)[:4000],
        )
        print(
            json.dumps(
                {
                    "status": "failed",
                    "command": args.command,
                    "run_id": run_id,
                    "error": {"type": type(error).__name__, "message": str(error)},
                },
                default=str,
                sort_keys=True,
            )
        )
        return 1
    print(
        json.dumps(
            {"status": "ok", "command": args.command, "run_id": run_id, **payload},
            default=str,
            sort_keys=True,
        )
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
