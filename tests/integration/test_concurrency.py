import json
import threading
import time

import pytest

from aster import lake, metrics, pipeline
from aster.source import migrate, seed

pytestmark = pytest.mark.integration

GOLD_TABLES = (
    "dim_customer",
    "dim_subscription",
    "fact_payments",
    "fact_service_events",
)


def seeded(config):
    with pipeline.connect_postgres(config) as connection:
        migrate(connection)
        seed(connection, "small", 42)


def run_keys(config, s3, prefix):
    return sorted(
        key
        for key in lake.list_keys(s3, config.gold_bucket, f"{prefix}/")
        if key.endswith(".parquet")
    )


def run_gold_pointer(config, s3, run_id):
    tables = {}
    for table in GOLD_TABLES:
        keys = run_keys(config, s3, f"gold/{table}/run={run_id}")
        assert keys, table
        tables[table] = {"objects": keys}
    seed_keys = run_keys(config, s3, f"gold/revenue_trend_seed/run={run_id}")
    assert seed_keys
    return {"run_id": run_id, "tables": tables, "trend_seed": {"objects": seed_keys}}


def metrics_for_pointer(config, s3, pointer):
    connection = metrics.duckdb_connect(config)
    try:
        metrics.create_gold_views(connection, s3, config, pointer)
        return json.loads(
            json.dumps(
                {
                    name: metrics.run_metric(connection, name)
                    for name in metrics.METRICS
                },
                default=str,
            )
        )
    finally:
        connection.close()


def test_overlapping_pipelines_serialize_and_publish_own_output(
    lake_env, monkeypatch
):
    config, s3 = lake_env.config, lake_env.s3
    seeded(config)
    run_ids = ("concurrent-a", "concurrent-b")
    finished = {}
    stage_intervals = []
    recorded = threading.Lock()
    errors = []
    execute_stage = pipeline.execute_stage

    def recording_execute_stage(name, run_id, *args, **kwargs):
        started = time.monotonic()
        try:
            return execute_stage(name, run_id, *args, **kwargs)
        finally:
            with recorded:
                stage_intervals.append((run_id, name, started, time.monotonic()))

    monkeypatch.setattr(pipeline, "execute_stage", recording_execute_stage)

    def run(run_id):
        try:
            pipeline.run_pipeline(config, run_id)
        except Exception as error:
            errors.append((run_id, error))
        finally:
            finished[run_id] = time.monotonic()

    threads = [threading.Thread(target=run, args=(run_id,)) for run_id in run_ids]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join()

    assert errors == []
    assert sorted(name for _, name, _, _ in stage_intervals) == sorted(
        ["extract", "validate_raw", "transform_silver", "validate_silver",
         "build_gold", "validate_gold", "publish_run"] * 2
    )
    for index, (run_id, name, start, end) in enumerate(stage_intervals):
        for other_run, other_name, other_start, other_end in stage_intervals[index + 1 :]:
            if other_run == run_id:
                continue
            assert end <= other_start or other_end <= start, (
                run_id,
                name,
                other_run,
                other_name,
                stage_intervals,
            )
    for run_id in run_ids:
        artifact = lake.get_json(
            s3, config.artifacts_bucket, f"runs/{run_id}/run.json"
        )
        assert artifact["status"] == "success"
        assert artifact["silver_run_id"] == run_id
        assert artifact["gold_run_id"] == run_id
        stored = lake.get_json(
            s3, config.artifacts_bucket, f"runs/{run_id}/metrics.json"
        )
        assert stored == metrics_for_pointer(
            config, s3, run_gold_pointer(config, s3, run_id)
        )
    last_finished = max(run_ids, key=finished.__getitem__)
    assert (
        lake.committed_pointer(s3, config.gold_bucket, "gold")["run_id"]
        == last_finished
    )
    assert (
        lake.committed_pointer(s3, config.silver_bucket, "silver")["run_id"]
        == last_finished
    )


def test_publish_refuses_a_run_that_lost_the_publication_race(lake_env):
    config, s3 = lake_env.config, lake_env.s3
    seeded(config)
    pipeline.run_pipeline(config, "race-silver")
    for run_id in ("race-a", "race-b"):
        pipeline.execute_stage(
            "build_gold", run_id, s3, config, pipeline.make_gold, s3, config, run_id
        )
        pipeline.execute_stage(
            "validate_gold",
            run_id,
            s3,
            config,
            pipeline.validate_gold_stage,
            s3,
            config,
            run_id,
        )
    pipeline.publish(s3, config, "race-b")
    with pytest.raises(ValueError, match="belongs to run race-b"):
        pipeline.publish(s3, config, "race-a")
    assert lake.get_json(s3, config.artifacts_bucket, "runs/race-a/run.json") is None
    assert (
        lake.committed_pointer(s3, config.gold_bucket, "gold")["run_id"] == "race-b"
    )


def test_publish_refuses_gold_built_from_another_silver_run(lake_env):
    config, s3 = lake_env.config, lake_env.s3
    seeded(config)
    pipeline.run_pipeline(config, "silver-a")
    pipeline.execute_stage(
        "build_gold",
        "silver-b",
        s3,
        config,
        pipeline.make_gold,
        s3,
        config,
        "silver-b",
    )
    pipeline.execute_stage(
        "transform_silver",
        "silver-b",
        s3,
        config,
        pipeline.transform_silver,
        s3,
        config,
        "silver-b",
    )
    pipeline.execute_stage(
        "validate_gold",
        "silver-b",
        s3,
        config,
        pipeline.validate_gold_stage,
        s3,
        config,
        "silver-b",
    )
    with pytest.raises(ValueError, match="built gold from silver run silver-a"):
        pipeline.publish(s3, config, "silver-b")
    assert (
        lake.get_json(s3, config.artifacts_bucket, "runs/silver-b/run.json") is None
    )
