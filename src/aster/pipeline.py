import json
import time
import uuid
from datetime import date, datetime, timezone

import psycopg

from psycopg import sql

from aster import lake, metrics, obs, quality
from aster.ingestion import ingest
from aster.transform import build_gold, build_silver


def new_run_id():
    return f"{datetime.now(timezone.utc):%Y%m%dT%H%M%SZ}-{uuid.uuid4().hex[:8]}"


def prepare(config, run_id, service="aster"):
    obs.configure_logging(service, run_id, config.loki_url)
    obs.configure_tracing(config.otlp_endpoint)


def connect_postgres(config):
    connection = psycopg.connect(
        **config.postgres_kwargs(), autocommit=True, connect_timeout=5
    )
    connection.execute(
        sql.SQL("SET search_path TO {}").format(sql.Identifier(config.postgres_schema))
    )
    return connection


def record_stage(s3, config, run_id, name, payload):
    lake.put_json(
        s3,
        config.artifacts_bucket,
        f"runs/{run_id}/stages/{name}.json",
        payload,
    )


def load_stages(s3, config, run_id):
    stages = {}
    prefix = f"runs/{run_id}/stages/"
    for key in lake.list_keys(s3, config.artifacts_bucket, prefix):
        name = key[len(prefix) : -len(".json")]
        stages[name] = lake.get_json(s3, config.artifacts_bucket, key)
    return stages


def execute_stage(name, run_id, s3, config, function, *args, **kwargs):
    started = time.perf_counter()
    with obs.span(name, run_id=run_id):
        try:
            result = function(*args, **kwargs)
        except Exception as error:
            obs.log(
                "stage_failed",
                stage=name,
                error=type(error).__name__,
                detail=str(error)[:2000],
                duration_ms=round((time.perf_counter() - started) * 1000, 3),
            )
            raise
    record = {
        **result,
        "stage": name,
        "duration_ms": round((time.perf_counter() - started) * 1000, 3),
        "completed_at": datetime.now(timezone.utc).isoformat(),
    }
    record_stage(s3, config, run_id, name, record)
    obs.log("stage_completed", stage=name, duration_ms=record["duration_ms"])
    return record


def extract(s3, config, run_id):
    with connect_postgres(config) as connection:
        rows = ingest(connection, s3, config, run_id)
    obs.log("extract_completed", rows=rows, datasets=len(rows))
    return {"rows": rows}


def validate_raw(s3, config):
    with connect_postgres(config) as connection:
        checks = quality.validate_raw(s3, config, connection)
    quality.require(checks)
    obs.log("raw_validated", checks=[check.as_dict() for check in checks])
    return {"checks": [check.as_dict() for check in checks]}


def transform_silver(s3, config, run_id):
    result = build_silver(s3, config, run_id)
    return {
        "silver_run_id": result.run_id,
        "datasets": {
            name: {"rows": info["row_count"], "rejected": info["rejected_count"]}
            for name, info in result.datasets.items()
        },
    }


def validate_silver(s3, config):
    checks = quality.validate_silver(s3, config)
    quality.require(checks)
    obs.log("silver_validated", checks=[check.as_dict() for check in checks])
    return {"checks": [check.as_dict() for check in checks]}


def make_gold(s3, config, run_id, window=None):
    silver_pointer = lake.committed_pointer(s3, config.silver_bucket, "silver")
    if silver_pointer is None:
        raise ValueError("No committed silver dataset is available")
    result = build_gold(s3, config, run_id, silver_pointer, window)
    return {
        "gold_run_id": result.run_id,
        "tables": {name: info["row_count"] for name, info in result.tables.items()},
        "window": list(window) if window else None,
    }


def validate_gold_stage(s3, config, run_id):
    stages = load_stages(s3, config, run_id)
    window = stages.get("build_gold", {}).get("window")
    pointer = (
        lake.get_json(
            s3, config.gold_bucket, f"gold/backfills/{run_id}/manifest.json"
        )
        if window
        else None
    )
    checks = quality.validate_gold(s3, config, pointer=pointer, window=window)
    quality.require(checks)
    obs.log("gold_validated", checks=[check.as_dict() for check in checks])
    return {"checks": [check.as_dict() for check in checks]}


def gold_pointer_for_run(s3, config, run_id, stages):
    window = stages.get("build_gold", {}).get("window")
    if window:
        return lake.get_json(
            s3, config.gold_bucket, f"gold/backfills/{run_id}/manifest.json"
        )
    return lake.committed_pointer(s3, config.gold_bucket, "gold")


def publish(s3, config, run_id, status="success"):
    stages = load_stages(s3, config, run_id)
    gold_pointer = gold_pointer_for_run(s3, config, run_id, stages)
    connection = metrics.warehouse_connection(config)
    try:
        metrics.create_gold_views(connection, s3, config, gold_pointer)
        results = metrics.run_all(connection)
    finally:
        connection.close()
    silver = stages.get("transform_silver", {}).get("datasets", {})
    row_totals = {
        "extracted": sum(stages.get("extract", {}).get("rows", {}).values()),
        "silver_rows": sum(item["rows"] for item in silver.values()),
        "rejected_rows": sum(item["rejected"] for item in silver.values()),
        "gold_rows": sum(stages.get("build_gold", {}).get("tables", {}).values()),
    }
    durations = {name: stage.get("duration_ms", 0) for name, stage in stages.items()}
    artifact = {
        "run_id": run_id,
        "status": status,
        "window": stages.get("build_gold", {}).get("window"),
        "created_at": datetime.now(timezone.utc).isoformat(),
        "silver_run_id": stages.get("transform_silver", {}).get("silver_run_id"),
        "gold_run_id": stages.get("build_gold", {}).get("gold_run_id"),
        "row_totals": row_totals,
        "stage_duration_ms": durations,
        "stages": stages,
    }
    lake.put_json(s3, config.artifacts_bucket, f"runs/{run_id}/run.json", artifact)
    lake.put_json(
        s3, config.artifacts_bucket, f"runs/{run_id}/metrics.json", results
    )
    pushed = obs.push_metrics(
        config.pushgateway_url,
        run_id,
        {
            "aster_rows_extracted": row_totals["extracted"],
            "aster_rows_silver": row_totals["silver_rows"],
            "aster_rows_rejected": row_totals["rejected_rows"],
            "aster_rows_gold": row_totals["gold_rows"],
            "aster_run_duration_ms": sum(durations.values()),
            "aster_run_success": 1 if status == "success" else 0,
        },
    )
    obs.log(
        "run_published",
        status=status,
        metrics=len(results),
        metrics_pushed=pushed,
        row_totals=row_totals,
    )
    obs.flush_tracing()
    return {"metrics": len(results), "artifact": artifact}


def run_pipeline(config, run_id, window=None):
    prepare(config, run_id)
    s3 = lake.s3_client(config)
    started = datetime.now(timezone.utc)
    plan = (
        ("extract", extract, (s3, config, run_id)),
        ("validate_raw", validate_raw, (s3, config)),
        ("transform_silver", transform_silver, (s3, config, run_id)),
        ("validate_silver", validate_silver, (s3, config)),
        ("build_gold", make_gold, (s3, config, run_id, window)),
        ("validate_gold", validate_gold_stage, (s3, config, run_id)),
        ("publish_run", publish, (s3, config, run_id)),
    )
    try:
        for name, function, arguments in plan:
            execute_stage(name, run_id, s3, config, function, *arguments)
        obs.log(
            "pipeline_succeeded",
            duration_ms=round(
                (datetime.now(timezone.utc) - started).total_seconds() * 1000, 3
            ),
        )
    except Exception as error:
        try:
            lake.put_json(
                s3,
                config.artifacts_bucket,
                f"runs/{run_id}/run.json",
                {
                    "run_id": run_id,
                    "status": "failed",
                    "created_at": datetime.now(timezone.utc).isoformat(),
                    "error": {
                        "type": type(error).__name__,
                        "message": str(error)[:2000],
                    },
                    "stages": load_stages(s3, config, run_id),
                },
            )
            obs.push_metrics(
                config.pushgateway_url,
                run_id,
                {"aster_run_success": 0, "aster_run_duration_ms": 0},
            )
        finally:
            obs.log(
                "pipeline_failed",
                error=type(error).__name__,
                detail=str(error)[:2000],
            )
            obs.flush_tracing()
        raise
    return load_stages(s3, config, run_id)


def run_backfill(config, run_id, start, end):
    prepare(config, run_id)
    s3 = lake.s3_client(config)
    execute_stage("build_gold", run_id, s3, config, make_gold, s3, config, run_id, (start, end))
    execute_stage("validate_gold", run_id, s3, config, validate_gold_stage, s3, config, run_id)
    execute_stage("publish_run", run_id, s3, config, publish, s3, config, run_id)
    return load_stages(s3, config, run_id)


def parse_window(start, end):
    start_date = date.fromisoformat(start)
    end_date = date.fromisoformat(end)
    if start_date > end_date:
        raise ValueError("Window start must not be after window end")
    return start_date, end_date


def json_dumps(payload):
    return json.dumps(payload, default=str, sort_keys=True)
