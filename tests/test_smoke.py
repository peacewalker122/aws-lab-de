import os
import uuid
from urllib.parse import urlsplit

import boto3
import duckdb
import pyarrow as pa
import pyarrow.parquet as pq
import psycopg
import pytest
from botocore.config import Config


def test_duckdb_reads_parquet(tmp_path):
    path = tmp_path / "fixture.parquet"
    pq.write_table(pa.table({"amount": [10, 20, 30]}), path)
    with duckdb.connect() as connection:
        assert connection.execute(
            "SELECT sum(amount) FROM read_parquet(?)", [str(path)]
        ).fetchone() == (60,)


@pytest.mark.integration
def test_s3_roundtrip():
    endpoint = os.environ.get("AWS_ENDPOINT_URL", "http://127.0.0.1:4566")
    url = urlsplit(endpoint)
    assert (
        url.scheme == "http" and url.hostname in {"127.0.0.1", "localhost"} and url.port
    ), "Local endpoint required"
    client = boto3.client(
        "s3",
        endpoint_url=endpoint,
        region_name=os.environ.get("AWS_DEFAULT_REGION", "us-east-1"),
        aws_access_key_id="test",
        aws_secret_access_key="test",
        config=Config(s3={"addressing_style": "path"}),
    )
    environment = os.environ.get("TF_VAR_environment", "local")
    for zone in ("raw", "silver", "gold", "artifacts"):
        client.head_bucket(Bucket=f"aster-{environment}-{zone}")
    bucket = os.environ.get("ASTER_BUCKET", "aster-local-m0-spike")
    key = f"smoke/{uuid.uuid4()}.txt"
    payload = b"aster compatibility smoke test\n"
    try:
        client.put_object(Bucket=bucket, Key=key, Body=payload)
        assert client.get_object(Bucket=bucket, Key=key)["Body"].read() == payload
    finally:
        client.delete_object(Bucket=bucket, Key=key)


@pytest.mark.integration
def test_postgres_connection():
    with psycopg.connect(
        host=os.environ.get("POSTGRES_HOST", "127.0.0.1"),
        port=int(os.environ.get("POSTGRES_PORT", "5432")),
        dbname="aster",
        user="aster",
        password=os.environ["POSTGRES_PASSWORD"],
        connect_timeout=5,
    ) as connection:
        assert connection.execute("SELECT current_database()").fetchone() == ("aster",)
