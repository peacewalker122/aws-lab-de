import contextlib
import json
import time
import uuid
from datetime import date, datetime, timezone

import psycopg

from psycopg import sql

from aster import lake, metrics, obs, quality
from aster.faults import fail
from aster.ingestion import ingest
from aster.transform import (
    GOLD_COMMITTED_KEY,
    SILVER_COMMITTED_KEY,
    SILVER_GATE_REMEDY,
    backfill_pointer_key,
    build_gold,
    build_silver,
    silver_content,
    silver_content_digest,
    staged_pointer_key,
    staged_silver_pointer_key,
)


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


@contextlib.contextmanager
def pipeline_lock(config):
    connection = psycopg.connect(
        **config.postgres_kwargs(), autocommit=True, connect_timeout=5
    )
    scope = f"aster-pipeline:{config.postgres_schema}"
    try:
        connection.execute("SELECT pg_advisory_lock(hashtext(%s))", (scope,))
        obs.log("pipeline_lock_acquired", scope=scope)
        yield
    finally:
        obs.log("pipeline_lock_released", scope=scope)
        connection.close()


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
    try:
        if name in {"publish_run", "publish_silver"}:
            fail(f"{name}-stage-record")
        record_stage(s3, config, run_id, name, record)
    except Exception as error:
        if name == "publish_run":
            published = published_gold_for_run(s3, config, run_id)
            if published:
                record_published_artifact_failure(
                    s3,
                    config,
                    run_id,
                    published_gold_pointer_key(published),
                    published.get("published_at"),
                    load_stages(s3, config, run_id),
                    error,
                )
        obs.log("stage_record_failed", stage=name, error=type(error).__name__)
        raise
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
        "state": "staged",
        "staged_pointer": staged_silver_pointer_key(run_id),
        "datasets": {
            name: {"rows": info["row_count"], "rejected": info["rejected_count"]}
            for name, info in result.datasets.items()
        },
    }


def staged_silver_pointer(s3, config, run_id):
    return lake.get_json(s3, config.silver_bucket, staged_silver_pointer_key(run_id))


def require_silver_ownership(pointer, run_id):
    if pointer.get("run_id") != run_id:
        raise ValueError(f"Staged silver pointer does not belong to run {run_id}")
    for dataset, info in pointer.get("datasets", {}).items():
        prefix = f"silver/{dataset}/run={run_id}/"
        keys = info.get("objects", []) + info.get("rejected_objects", [])
        if any(not key.startswith(prefix) for key in keys):
            raise ValueError(
                f"Silver objects for {dataset} do not belong to run {run_id}"
            )


def validate_silver_stage(s3, config, run_id):
    stages = load_stages(s3, config, run_id)
    build_stage = stages.get("transform_silver") or {}
    if build_stage.get("silver_run_id") != run_id:
        raise ValueError(
            f"Run {run_id} has no silver output of its own; the transform record names "
            f"{build_stage.get('silver_run_id')}"
        )
    pointer = staged_silver_pointer(s3, config, run_id)
    if pointer is None:
        raise ValueError(
            f"Run {run_id} has no staged silver pointer at "
            f"{staged_silver_pointer_key(run_id)}"
        )
    require_silver_ownership(pointer, run_id)
    checks = quality.validate_silver(s3, config, pointer=pointer)
    quality.require(checks)
    obs.log("silver_validated", checks=[check.as_dict() for check in checks])
    return {
        "checks": [check.as_dict() for check in checks],
        "passed": True,
        "state": "validated",
        "staged_pointer": staged_silver_pointer_key(run_id),
        "validation": {
            "run_id": run_id,
            "content_digest": silver_content_digest(pointer),
            "validated_at": datetime.now(timezone.utc).isoformat(),
        },
    }


def require_silver_gate(s3, config, run_id, stages):
    gate = stages.get("validate_silver") or {}
    validation = gate.get("validation") or {}
    if gate.get("passed") is not True:
        raise ValueError(
            f"Run {run_id} has no passing silver validation; publication is blocked until "
            "validate-silver succeeds for the staged silver output"
        )
    pointer = staged_silver_pointer(s3, config, run_id)
    if pointer is None:
        raise ValueError(
            f"Run {run_id} has no staged silver pointer at "
            f"{staged_silver_pointer_key(run_id)}"
        )
    require_silver_ownership(pointer, run_id)
    if validation.get("run_id") != run_id:
        raise ValueError(f"Silver validation does not belong to run {run_id}")
    if silver_content_digest(pointer) != validation.get("content_digest"):
        raise ValueError(
            f"Staged silver pointer for run {run_id} changed after validation; "
            "re-run validate-silver before publishing"
        )
    return pointer, validation


def publish_silver(s3, config, run_id):
    stages = load_stages(s3, config, run_id)
    pointer, validation = require_silver_gate(s3, config, run_id, stages)
    committed = lake.committed_pointer(s3, config.silver_bucket, "silver")
    if committed and committed.get("run_id") != run_id:
        if lake.parse_instant(committed["created_at"]) >= lake.parse_instant(
            pointer["created_at"]
        ):
            raise ValueError(
                f"Committed silver belongs to run {committed.get('run_id')}, which was "
                f"staged after run {run_id}; refusing to publish another run's snapshot"
            )
    checks = quality.verify_snapshot_objects(
        s3,
        config,
        config.silver_bucket,
        quality.silver_snapshot_entries(pointer),
        "published.silver",
    )
    quality.require(checks)
    lake.put_json(
        s3,
        config.silver_bucket,
        SILVER_COMMITTED_KEY,
        {
            **silver_content(pointer),
            "state": "published",
            "published_at": datetime.now(timezone.utc).isoformat(),
            "validation": validation,
        },
    )
    obs.log("silver_published", run_id=run_id, pointer=SILVER_COMMITTED_KEY)
    return {
        "state": "published",
        "published_pointer": SILVER_COMMITTED_KEY,
        "silver_run_id": run_id,
        "checks": [check.as_dict() for check in checks],
    }


def require_gated_silver(pointer):
    validation = pointer.get("validation") or {}
    if validation.get("run_id") != pointer.get("run_id") or validation.get(
        "content_digest"
    ) != silver_content_digest(pointer):
        raise ValueError(
            f"Committed silver run {pointer.get('run_id')} is not a validated snapshot; "
            "gold cannot be built from it. " + SILVER_GATE_REMEDY
        )
    return pointer


def make_gold(s3, config, run_id, window=None):
    silver_pointer = lake.committed_pointer(s3, config.silver_bucket, "silver")
    if silver_pointer is None:
        raise ValueError("No committed silver dataset is available")
    require_gated_silver(silver_pointer)
    quality.require(
        quality.verify_snapshot_objects(
            s3,
            config,
            config.silver_bucket,
            quality.silver_snapshot_entries(silver_pointer),
            "silver.snapshot",
        )
    )
    result = build_gold(s3, config, run_id, silver_pointer, window)
    return {
        "gold_run_id": result.run_id,
        "silver_run_id": silver_pointer["run_id"],
        "tables": {name: info["row_count"] for name, info in result.tables.items()},
        "window": list(window) if window else None,
        "state": "staged",
        "staged_pointer": staged_pointer_key(run_id),
    }


def staged_gold_pointer(s3, config, run_id):
    return lake.get_json(s3, config.gold_bucket, staged_pointer_key(run_id))


def published_gold_pointer_key(pointer):
    if pointer.get("window"):
        return backfill_pointer_key(pointer["run_id"])
    return GOLD_COMMITTED_KEY


def validate_gold_stage(s3, config, run_id):
    stages = load_stages(s3, config, run_id)
    build_stage = stages.get("build_gold") or {}
    if build_stage.get("gold_run_id") != run_id:
        raise ValueError(
            f"Run {run_id} has no gold output of its own; the build record names "
            f"{build_stage.get('gold_run_id')}"
        )
    pointer = staged_gold_pointer(s3, config, run_id)
    if pointer is None:
        raise ValueError(
            f"Run {run_id} has no staged gold pointer at {staged_pointer_key(run_id)}"
        )
    checks = quality.validate_gold(
        s3, config, pointer=pointer, window=build_stage.get("window")
    )
    quality.require(checks)
    obs.log("gold_validated", checks=[check.as_dict() for check in checks])
    return {
        "checks": [check.as_dict() for check in checks],
        "passed": True,
        "state": "validated",
        "staged_pointer": staged_pointer_key(run_id),
        "pointer_digest": lake.json_digest(pointer),
    }


def require_publish_gate(s3, config, run_id, stages):
    gate = stages.get("validate_gold") or {}
    if gate.get("passed") is not True:
        raise ValueError(
            f"Run {run_id} has no passing gold validation; publication is blocked until "
            "validate-gold succeeds for the staged output"
        )
    pointer = staged_gold_pointer(s3, config, run_id)
    if pointer is None:
        raise ValueError(
            f"Run {run_id} has no staged gold pointer at {staged_pointer_key(run_id)}"
        )
    if lake.json_digest(pointer) != gate.get("pointer_digest"):
        raise ValueError(
            f"Staged gold pointer for run {run_id} changed after validation; "
            "re-run validate-gold before publishing"
        )
    return pointer


def require_run_ownership(s3, config, run_id, stages, pointer):
    build_stage = stages.get("build_gold") or {}
    if build_stage.get("gold_run_id") != run_id:
        raise ValueError(
            f"Run {run_id} has no gold output of its own; the build record names "
            f"{build_stage.get('gold_run_id')}"
        )
    silver_stage = stages.get("transform_silver")
    if silver_stage and pointer.get("silver_run_id") != silver_stage.get(
        "silver_run_id"
    ):
        raise ValueError(
            f"Run {run_id} built gold from silver run "
            f"{pointer.get('silver_run_id')} but transformed silver "
            f"{silver_stage.get('silver_run_id')}"
        )
    committed_silver = lake.committed_pointer(s3, config.silver_bucket, "silver")
    committed_silver_run = (committed_silver or {}).get("run_id")
    if committed_silver_run != pointer.get("silver_run_id"):
        raise ValueError(
            f"Run {run_id} built gold from silver run {pointer.get('silver_run_id')} "
            f"but the committed silver snapshot is {committed_silver_run}; "
            "rebuild gold before publishing"
        )
    committed_gold = lake.committed_pointer(s3, config.gold_bucket, "gold")
    if committed_gold and committed_gold.get("run_id") != run_id:
        published_after = lake.parse_instant(
            committed_gold["created_at"]
        ) >= lake.parse_instant(pointer["created_at"])
        if published_after:
            raise ValueError(
                f"Committed gold output belongs to run {committed_gold.get('run_id')}, "
                f"which was published after run {run_id}; refusing to publish "
                "another run's dataset"
            )
    return pointer


def verify_publication_inputs(s3, config, pointer):
    entries = {**pointer["tables"], "revenue_trend_seed": pointer["trend_seed"]}
    checks = quality.verify_snapshot_objects(
        s3, config, config.gold_bucket, entries, "published.gold"
    )
    checks.extend(
        quality.verify_snapshot_objects(
            s3,
            config,
            config.silver_bucket,
            quality.silver_snapshot_entries(pointer["silver"]),
            "published.gold.silver",
        )
    )
    quality.require(checks)
    return checks


def published_gold_for_run(s3, config, run_id):
    staged = staged_gold_pointer(s3, config, run_id)
    if staged is None:
        return None
    published = lake.get_json(
        s3, config.gold_bucket, published_gold_pointer_key(staged)
    )
    if published and published.get("run_id") == run_id:
        return published
    return None


def record_published_artifact_failure(
    s3, config, run_id, published_key, published_at, stages, error
):
    record = {
        "run_id": run_id,
        "status": "failed",
        "gold_state": "published",
        "gold_pointer": published_key,
        "staged_pointer": staged_pointer_key(run_id),
        "published_at": published_at,
        "error": {"type": type(error).__name__, "message": str(error)[:2000]},
        "detail": (
            "The gold pointer was committed for this run; only the run artifacts "
            "failed to write"
        ),
        "stages": stages,
    }
    lake.put_json(s3, config.artifacts_bucket, f"runs/{run_id}/run.json", record)
    obs.log(
        "run_artifact_failed_after_publication",
        run_id=run_id,
        pointer=published_key,
        error=type(error).__name__,
    )
    return record


def publish(s3, config, run_id, status="success"):
    stages = load_stages(s3, config, run_id)
    pointer = require_publish_gate(s3, config, run_id, stages)
    require_run_ownership(s3, config, run_id, stages, pointer)
    connection = metrics.warehouse_connection(config)
    try:
        metrics.create_gold_views(connection, s3, config, pointer)
        results = metrics.run_all(connection)
    finally:
        connection.close()
    verify_publication_inputs(s3, config, pointer)
    published_at = datetime.now(timezone.utc).isoformat()
    published_key = published_gold_pointer_key(pointer)
    lake.put_json(
        s3,
        config.gold_bucket,
        published_key,
        {**pointer, "state": "published", "published_at": published_at},
    )
    obs.log("gold_published", run_id=run_id, pointer=published_key)
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
        "gold_state": "published",
        "gold_pointer": published_key,
        "staged_pointer": staged_pointer_key(run_id),
        "window": stages.get("build_gold", {}).get("window"),
        "created_at": datetime.now(timezone.utc).isoformat(),
        "silver_run_id": stages.get("transform_silver", {}).get("silver_run_id"),
        "gold_run_id": stages.get("build_gold", {}).get("gold_run_id"),
        "row_totals": row_totals,
        "stage_duration_ms": durations,
        "stages": stages,
    }
    try:
        lake.put_json(
            s3, config.artifacts_bucket, f"runs/{run_id}/metrics.json", results
        )
        fail("publish-artifact-write")
        lake.put_json(s3, config.artifacts_bucket, f"runs/{run_id}/run.json", artifact)
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
    except Exception as error:
        record_published_artifact_failure(
            s3, config, run_id, published_key, published_at, stages, error
        )
        raise
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
    with pipeline_lock(config):
        return _run_pipeline(config, run_id, window)


def _run_pipeline(config, run_id, window=None):
    s3 = lake.s3_client(config)
    started = datetime.now(timezone.utc)
    plan = (
        ("extract", extract, (s3, config, run_id)),
        ("validate_raw", validate_raw, (s3, config)),
        ("transform_silver", transform_silver, (s3, config, run_id)),
        ("validate_silver", validate_silver_stage, (s3, config, run_id)),
        ("publish_silver", publish_silver, (s3, config, run_id)),
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
        published = published_gold_for_run(s3, config, run_id)
        failure = {
            "run_id": run_id,
            "status": "failed",
            "gold_state": "published" if published else "unpublished",
            "created_at": datetime.now(timezone.utc).isoformat(),
            "error": {
                "type": type(error).__name__,
                "message": str(error)[:2000],
            },
            "stages": load_stages(s3, config, run_id),
        }
        if published:
            failure["gold_pointer"] = published_gold_pointer_key(published)
            failure["published_at"] = published.get("published_at")
        try:
            lake.put_json(
                s3,
                config.artifacts_bucket,
                f"runs/{run_id}/run.json",
                failure,
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
    with pipeline_lock(config):
        s3 = lake.s3_client(config)
        execute_stage(
            "build_gold",
            run_id,
            s3,
            config,
            make_gold,
            s3,
            config,
            run_id,
            (start, end),
        )
        execute_stage(
            "validate_gold", run_id, s3, config, validate_gold_stage, s3, config, run_id
        )
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
