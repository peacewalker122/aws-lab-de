import io
import json
import uuid
from datetime import datetime, timezone

import pyarrow as pa
import pyarrow.parquet as pq
from psycopg import sql
from psycopg.rows import dict_row

from aster import lake
from aster.faults import hit
from aster.obs import log

SCHEMA_VERSION = 2
ZERO_CURSOR = ("0001-01-01 00:00:00+00", "00000000-0000-0000-0000-000000000000")
DATASETS = {
    "customers": ("updated_at", "customer_id"),
    "subscriptions": ("updated_at", "subscription_id"),
    "payments": ("updated_at", "payment_id"),
    "service_events": ("ingested_at", "event_id"),
}


def _serialize(value):
    if isinstance(value, uuid.UUID):
        return str(value)
    if isinstance(value, dict):
        return json.dumps(value, sort_keys=True)
    return value


def _checkpoint(connection, dataset):
    connection.execute(
        "INSERT INTO extraction_checkpoints VALUES (%s, %s, %s) ON CONFLICT DO NOTHING",
        (dataset, ZERO_CURSOR[0], ZERO_CURSOR[1]),
    )
    return connection.execute(
        "SELECT cursor_time, cursor_id FROM extraction_checkpoints WHERE dataset = %s FOR UPDATE",
        (dataset,),
    ).fetchone()


def _manifest_identity(manifest):
    return {
        key: manifest[key]
        for key in (
            "dataset",
            "schema_version",
            "batch_id",
            "record_count",
            "cursor",
            "objects",
        )
    }


def extract_batch(connection, s3, bucket, dataset, batch_size, run_id):
    cursor_column, id_column = DATASETS[dataset]
    started = datetime.now(timezone.utc)
    with connection.transaction():
        connection.execute(
            sql.SQL("LOCK TABLE {} IN SHARE MODE").format(sql.Identifier(dataset))
        )
        previous = _checkpoint(connection, dataset)
        statement = sql.SQL(
            "SELECT * FROM {} WHERE ({}, {}) > (%s, %s) ORDER BY {}, {} LIMIT %s"
        ).format(
            sql.Identifier(dataset),
            sql.Identifier(cursor_column),
            sql.Identifier(id_column),
            sql.Identifier(cursor_column),
            sql.Identifier(id_column),
        )
        with connection.cursor(row_factory=dict_row) as cursor:
            rows = cursor.execute(statement, (*previous, batch_size)).fetchall()
        if not rows:
            return 0
        records = [
            {key: _serialize(value) for key, value in row.items()} for row in rows
        ]
        buffer = io.BytesIO()
        pq.write_table(
            pa.Table.from_pylist(records), buffer, compression="zstd", version="2.6"
        )
        data = buffer.getvalue()
        checksum = lake.sha256_hex(data)
        start, start_id = rows[0][cursor_column], rows[0][id_column]
        end, end_id = rows[-1][cursor_column], rows[-1][id_column]
        batch_id = lake.sha256_hex(
            f"{dataset}|{SCHEMA_VERSION}|{checksum}|{end.isoformat()}|{end_id}".encode()
        )
        prefix = f"{dataset}/ingest_date={end.astimezone(timezone.utc).date().isoformat()}/batch={batch_id}"
        data_key = f"{prefix}/data.parquet"
        manifest_key = f"{prefix}/manifest.json"
        now = datetime.now(timezone.utc).isoformat()
        manifest = {
            "schema_version": SCHEMA_VERSION,
            "dataset": dataset,
            "batch_id": batch_id,
            "run_id": run_id,
            "status": "complete",
            "cursor": {
                "column": cursor_column,
                "start": [start.isoformat(), str(start_id)],
                "end": [end.isoformat(), str(end_id)],
                "parent": [previous[0].isoformat(), str(previous[1])],
            },
            "record_count": len(rows),
            "objects": [{"key": data_key, "sha256": checksum, "bytes": len(data)}],
            "created_at": now,
            "completed_at": now,
        }
        existing = lake.get_json(s3, bucket, manifest_key)
        if existing is not None and _manifest_identity(existing) != _manifest_identity(
            manifest
        ):
            raise ValueError(f"Conflicting raw manifest {manifest_key}")
        if existing is None:
            s3.put_object(Bucket=bucket, Key=data_key, Body=data)
        stored = lake.read_bytes(s3, bucket, data_key)
        if lake.sha256_hex(stored) != checksum:
            raise ValueError(f"S3 checksum mismatch for {data_key}")
        hit("ingest-after-data")
        if existing is None:
            lake.put_json(s3, bucket, manifest_key, manifest)
        if lake.get_json(s3, bucket, manifest_key) != (
            existing if existing is not None else manifest
        ):
            raise ValueError(f"Invalid raw manifest {manifest_key}")
        hit("ingest-after-manifest")
        connection.execute(
            "UPDATE extraction_checkpoints SET cursor_time = %s, cursor_id = %s WHERE dataset = %s",
            (end, end_id, dataset),
        )
    log(
        "raw_batch_committed",
        dataset=dataset,
        batch_id=batch_id,
        rows=len(rows),
        bytes=len(data),
        cursor_end=manifest["cursor"]["end"],
        checkpoint_after=[end.isoformat(), str(end_id)],
        replayed=existing is not None,
        duration_ms=round(
            (datetime.now(timezone.utc) - started).total_seconds() * 1000, 3
        ),
    )
    return len(rows)


def ingest(connection, s3, config, run_id):
    totals = {}
    for dataset in DATASETS:
        count = 0
        while batch_count := extract_batch(
            connection,
            s3,
            config.raw_bucket,
            dataset,
            config.batch_size,
            run_id,
        ):
            count += batch_count
        totals[dataset] = count
    return totals
