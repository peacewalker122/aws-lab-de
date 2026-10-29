import argparse
import json
import sys

from aster import lake, metrics, obs, pipeline
from aster.config import Config
from aster.source import create_database, migrate, seed


def format_table(rows):
    if not rows:
        return "(no rows)"
    columns = list(rows[0])
    widths = {
        column: max(len(str(column)), *(len(str(row[column])) for row in rows))
        for column in columns
    }
    header = "  ".join(str(column).ljust(widths[column]) for column in columns)
    lines = [header, "  ".join("-" * widths[column] for column in columns)]
    for row in rows:
        lines.append(
            "  ".join(str(row[column]).ljust(widths[column]) for column in columns)
        )
    return "\n".join(lines)


def dispatch(args, config, run_id):
    s3 = lake.s3_client(config)
    if args.command == "migrate":
        with pipeline.connect_postgres(config) as connection:
            migrate(connection)
        return {"migrated": True}
    if args.command == "postgres-init":
        with pipeline.connect_postgres(config) as connection:
            created = create_database(connection, args.database)
        return {"database": args.database, "created": created}
    if args.command == "seed":
        with pipeline.connect_postgres(config) as connection:
            seed(connection, args.size, args.seed)
        return {"size": args.size, "seed": args.seed}
    if args.command == "query":
        connection = metrics.warehouse_connection(config)
        try:
            pointer = (
                lake.get_json(
                    s3, config.gold_bucket, f"gold/backfills/{args.gold_run}/manifest.json"
                )
                if args.gold_run
                else None
            )
            pointer = metrics.create_gold_views(connection, s3, config, pointer)
            names = metrics.METRICS if args.metric == "all" else (args.metric,)
            results = {name: metrics.run_metric(connection, name) for name in names}
        finally:
            connection.close()
        if args.format == "table":
            for name, rows in results.items():
                print(f"== {name}")
                print(format_table(rows))
        return {"gold_run_id": pointer["run_id"], "metrics": results}
    pipeline.prepare(config, run_id)
    if args.command == "extract":
        return pipeline.execute_stage(
            "extract", run_id, s3, config, pipeline.extract, s3, config, run_id
        )
    if args.command == "validate-raw":
        return pipeline.execute_stage(
            "validate_raw", run_id, s3, config, pipeline.validate_raw, s3, config
        )
    if args.command == "transform-silver":
        return pipeline.execute_stage(
            "transform_silver",
            run_id,
            s3,
            config,
            pipeline.transform_silver,
            s3,
            config,
            run_id,
        )
    if args.command == "validate-silver":
        return pipeline.execute_stage(
            "validate_silver",
            run_id,
            s3,
            config,
            pipeline.validate_silver,
            s3,
            config,
        )
    if args.command == "build-gold":
        window = pipeline.parse_window(args.start, args.end) if args.start else None
        return pipeline.execute_stage(
            "build_gold",
            run_id,
            s3,
            config,
            pipeline.make_gold,
            s3,
            config,
            run_id,
            window,
        )
    if args.command == "validate-gold":
        return pipeline.execute_stage(
            "validate_gold",
            run_id,
            s3,
            config,
            pipeline.validate_gold_stage,
            s3,
            config,
            run_id,
        )
    if args.command == "publish-run":
        return pipeline.execute_stage(
            "publish_run", run_id, s3, config, pipeline.publish, s3, config, run_id
        )
    if args.command == "pipeline":
        window = pipeline.parse_window(args.start, args.end) if args.start else None
        stages = pipeline.run_pipeline(config, run_id, window)
        return {"stages": sorted(stages)}
    if args.command == "backfill":
        stages = pipeline.run_backfill(
            config, run_id, *pipeline.parse_window(args.start, args.end)
        )
        return {"stages": sorted(stages)}
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
    for name in (
        "extract",
        "validate-raw",
        "transform-silver",
        "validate-silver",
        "validate-gold",
        "publish-run",
    ):
        commands.add_parser(name)
    gold = commands.add_parser("build-gold")
    gold.add_argument("--start", default=None)
    gold.add_argument("--end", default=None)
    pipeline_parser = commands.add_parser("pipeline")
    pipeline_parser.add_argument("--start", default=None)
    pipeline_parser.add_argument("--end", default=None)
    backfill = commands.add_parser("backfill")
    backfill.add_argument("--start", required=True)
    backfill.add_argument("--end", required=True)
    query = commands.add_parser("query")
    query.add_argument("--metric", choices=("all", *metrics.METRICS), default="all")
    query.add_argument("--format", choices=("json", "table"), default="json")
    query.add_argument("--gold-run", default=None)
    return parser


def main(argv=None):
    args = build_parser().parse_args(argv)
    config = Config.from_env()
    run_id = args.run_id or pipeline.new_run_id()
    try:
        payload = dispatch(args, config, run_id)
    except Exception as error:
        obs.log(
            "command_failed",
            command=args.command,
            error=type(error).__name__,
            detail=str(error)[:4000],
        )
        obs.flush_tracing()
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
