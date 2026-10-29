import hashlib
import io
import json
import threading
import uuid

import duckdb
import psycopg
import pyarrow.parquet as pq
import pytest
from psycopg import sql

from aster import lake
from aster.ingestion import extract_batch, ingest
from aster.source import migrate, seed

pytestmark = pytest.mark.integration


def sql_list(values):
    return ", ".join("'" + value.replace("'", "''") + "'" for value in values)


def connect(config):
    connection = psycopg.connect(
        **config.postgres_kwargs(), autocommit=True, connect_timeout=5
    )
    connection.execute(
        sql.SQL("SET search_path TO {}").format(sql.Identifier(config.postgres_schema))
    )
    return connection


def warehouse(config):
    connection = duckdb.connect()
    connection.execute("LOAD httpfs")
    connection.execute(f"SET s3_endpoint='{config.s3_host()}'")
    connection.execute("SET s3_use_ssl=false")
    connection.execute("SET s3_url_style='path'")
    connection.execute("SET s3_access_key_id='test'")
    connection.execute("SET s3_secret_access_key='test'")
    connection.execute(f"SET s3_region='{config.region}'")
    return connection


def seeded(config):
    with connect(config) as connection:
        migrate(connection)
        seed(connection, "small", 42)


def raw_objects(config, s3, dataset, manifests=None):
    manifests = manifests or lake.raw_manifests(s3, config.raw_bucket, dataset)
    return [
        lake.s3_uri(config.raw_bucket, item["key"])
        for manifest in manifests
        for item in manifest["objects"]
    ]


def test_manifest_contract_and_checkpoint(lake_env):
    config, s3 = lake_env.config, lake_env.s3
    seeded(config)
    with connect(config) as connection:
        totals = ingest(connection, s3, config, "manifest-1")
        checkpoint = connection.execute(
            "SELECT cursor_time, cursor_id FROM extraction_checkpoints WHERE dataset = 'service_events'"
        ).fetchone()
    assert totals == {
        "customers": 100,
        "subscriptions": 100,
        "payments": 100,
        "service_events": 1000,
    }
    manifests = lake.raw_manifests(s3, config.raw_bucket, "service_events")
    assert len(manifests) == 4
    for manifest in manifests:
        assert manifest["schema_version"] == 2
        assert manifest["status"] == "complete"
        assert manifest["dataset"] == "service_events"
        assert manifest["record_count"] == 250
        assert manifest["cursor"]["column"] == "ingested_at"
        assert manifest["cursor"]["start"] <= manifest["cursor"]["end"]
        assert manifest["objects"][0]["key"].startswith(
            "service_events/ingest_date="
        )
        payload = lake.read_bytes(s3, config.raw_bucket, manifest["objects"][0]["key"])
        assert hashlib.sha256(payload).hexdigest() == manifest["objects"][0]["sha256"]
        assert manifest["objects"][0]["bytes"] == len(payload)
        assert pq.read_table(io.BytesIO(payload)).num_rows == 250
    assert manifests[-1]["cursor"]["end"][0] == checkpoint[0].isoformat()
    assert manifests[-1]["cursor"]["end"][1] == str(checkpoint[1])
    with connect(config) as connection:
        assert ingest(connection, s3, config, "manifest-2") == dict.fromkeys(
            ("customers", "subscriptions", "payments", "service_events"), 0
        )


def test_replay_after_checkpoint_rollback_is_idempotent(lake_env):
    config, s3 = lake_env.config, lake_env.s3
    seeded(config)
    with connect(config) as connection:
        ingest(connection, s3, config, "replay-1")
        before = sorted(lake.list_keys(s3, lake_env.buckets["raw"], ""))
        connection.execute(
            "UPDATE extraction_checkpoints SET cursor_time = '0001-01-01 00:00:00+00', "
            "cursor_id = '00000000-0000-0000-0000-000000000000'"
        )
        replay = ingest(connection, s3, config, "replay-2")
        assert replay == {
            "customers": 100,
            "subscriptions": 100,
            "payments": 100,
            "service_events": 1000,
        }
    assert sorted(lake.list_keys(s3, lake_env.buckets["raw"], "")) == before
    for dataset in ("customers", "subscriptions", "payments", "service_events"):
        for manifest in lake.raw_manifests(s3, config.raw_bucket, dataset):
            payload = lake.read_bytes(
                s3, config.raw_bucket, manifest["objects"][0]["key"]
            )
            assert (
                hashlib.sha256(payload).hexdigest()
                == manifest["objects"][0]["sha256"]
            )


def test_update_produces_new_version_and_latest_wins(lake_env):
    config, s3 = lake_env.config, lake_env.s3
    seeded(config)
    with connect(config) as connection:
        ingest(connection, s3, config, "update-1")
        customer = connection.execute(
            "SELECT customer_id FROM customers ORDER BY customer_id LIMIT 1"
        ).fetchone()[0]
        connection.execute(
            "UPDATE customers SET region = 'east' WHERE customer_id = %s", (customer,)
        )
        assert ingest(connection, s3, config, "update-2")["customers"] == 1
    manifests = lake.raw_manifests(s3, config.raw_bucket, "customers")
    assert len(manifests) == 2
    connection = warehouse(config)
    try:
        rows = connection.execute(
            f"SELECT customer_id, region FROM read_parquet([{sql_list(raw_objects(config, s3, 'customers'))}]) "
            "QUALIFY row_number() OVER (PARTITION BY customer_id ORDER BY updated_at DESC) = 1 "
            "AND customer_id = ?",
            [str(customer)],
        ).fetchall()
    finally:
        connection.close()
    assert rows == [(str(customer), "east")]


def test_interruption_rolls_back_checkpoint_but_keeps_committed_batch(lake_env):
    config, s3 = lake_env.config, lake_env.s3
    seeded(config)
    with connect(config) as connection:
        ingest(connection, s3, config, "interrupt-1")
        customer = connection.execute(
            "SELECT customer_id FROM customers ORDER BY customer_id LIMIT 1"
        ).fetchone()[0]
        connection.execute(
            "UPDATE customers SET region = 'south' WHERE customer_id = %s", (customer,)
        )
        with pytest.raises(RuntimeError, match="interruption"):
            with connection.transaction():
                assert (
                    extract_batch(
                        connection, s3, config.raw_bucket, "customers", 250, "interrupt-2"
                    )
                    == 1
                )
                raise RuntimeError("interruption")
        checkpoint_after_failure = connection.execute(
            "SELECT cursor_time FROM extraction_checkpoints WHERE dataset = 'customers'"
        ).fetchone()[0]
        keys = sorted(lake.list_keys(s3, lake_env.buckets["raw"], "customers/"))
        assert len(keys) == 4
        assert (
            extract_batch(
                connection, s3, config.raw_bucket, "customers", 250, "interrupt-3"
            )
            == 1
        )
        checkpoint_after_recovery = connection.execute(
            "SELECT cursor_time FROM extraction_checkpoints WHERE dataset = 'customers'"
        ).fetchone()[0]
    assert checkpoint_after_recovery > checkpoint_after_failure
    assert sorted(lake.list_keys(s3, lake_env.buckets["raw"], "customers/")) == keys
    assert len(lake.raw_manifests(s3, config.raw_bucket, "customers")) == 2


def test_cursor_ties_are_ordered_by_primary_key(lake_env):
    config, s3 = lake_env.config, lake_env.s3
    seeded(config)
    tied = [uuid.UUID(int=index * 7919 + 1, version=4) for index in range(5)]
    with connect(config) as connection:
        connection.execute("ALTER TABLE customers DISABLE TRIGGER customers_cursor")
        with connection.transaction():
            for customer in tied:
                connection.execute(
                    "INSERT INTO customers (customer_id, name, region, created_at, updated_at) "
                    "VALUES (%s, %s, 'tie', '2024-06-01 00:00:00+00', '2024-06-01 00:00:00+00')",
                    (customer, f"Tie {customer}"),
                )
        connection.execute("ALTER TABLE customers ENABLE TRIGGER customers_cursor")
    with connect(config) as connection:
        for _ in range(len(tied)):
            assert (
                extract_batch(
                    connection, s3, config.raw_bucket, "customers", 1, "ties-1"
                )
                == 1
            )
        checkpoint = connection.execute(
            "SELECT cursor_time, cursor_id FROM extraction_checkpoints WHERE dataset = 'customers'"
        ).fetchone()
        assert ingest(connection, s3, config, "ties-2")["customers"] == 100
    manifests = lake.raw_manifests(s3, config.raw_bucket, "customers")
    tie_ids = [
        manifest["cursor"]["end"][1]
        for manifest in manifests
        if manifest["cursor"]["end"][0] == "2024-06-01T00:00:00+00:00"
    ]
    assert sorted(tie_ids) == sorted(str(item) for item in tied)
    assert len(set(tie_ids)) == len(tied)
    assert str(checkpoint[1]) == str(sorted(tied)[-1])


def test_concurrent_writers_are_captured_exactly_once(lake_env):
    config, s3 = lake_env.config, lake_env.s3
    seeded(config)
    errors = []

    def writer(index):
        try:
            with connect(config) as connection:
                for step in range(25):
                    connection.execute(
                        "UPDATE customers SET region = %s "
                        "WHERE customer_id = (SELECT customer_id FROM customers ORDER BY customer_id LIMIT 1 OFFSET %s)",
                        (f"region-{index}-{step}", index * 7),
                    )
                for step in range(5):
                    connection.execute(
                        "INSERT INTO customers (customer_id, name, region, created_at, updated_at) "
                        "VALUES (%s, %s, 'north', now(), now())",
                        (uuid.uuid4(), f"Concurrent {index}-{step}"),
                    )
        except Exception as error:
            errors.append(error)

    threads = [threading.Thread(target=writer, args=(index,)) for index in range(3)]
    for thread in threads:
        thread.start()
    with connect(config) as connection:
        while any(thread.is_alive() for thread in threads):
            ingest(connection, s3, config, "concurrent")
        ingest(connection, s3, config, "concurrent")
        source = dict(
            connection.execute("SELECT customer_id::text, region FROM customers").fetchall()
        )
    for thread in threads:
        thread.join()
    assert errors == []
    assert len(source) == 115
    connection = warehouse(config)
    try:
        rows = connection.execute(
            f"SELECT customer_id, region FROM read_parquet([{sql_list(raw_objects(config, s3, 'customers'))}]) "
            "QUALIFY row_number() OVER (PARTITION BY customer_id ORDER BY updated_at DESC, filename DESC) = 1"
        ).fetchall()
    finally:
        connection.close()
    assert dict(rows) == source
    manifests = lake.raw_manifests(s3, config.raw_bucket, "customers")
    for manifest in manifests:
        assert manifest["status"] == "complete"
        assert json.loads(json.dumps(manifest))["batch_id"] == manifest["batch_id"]
