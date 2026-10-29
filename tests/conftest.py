import os
import subprocess
import sys
import uuid
from pathlib import Path
from types import SimpleNamespace

import psycopg
import pytest
from psycopg import sql

from aster import lake
from aster.config import Config

PROJECT_ROOT = Path(__file__).resolve().parents[1]


@pytest.fixture(scope="session")
def base_config():
    if not os.environ.get("POSTGRES_PASSWORD"):
        pytest.skip("local Aster environment is not configured")
    return Config.from_env()


@pytest.fixture
def lake_env(base_config):
    tag = uuid.uuid4().hex[:12]
    schema = f"t_{tag}"
    s3 = lake.s3_client(base_config)
    buckets = {
        zone: f"aster-test-{tag}-{zone}"
        for zone in ("raw", "silver", "gold", "artifacts")
    }
    for bucket in buckets.values():
        s3.create_bucket(Bucket=bucket)
    with psycopg.connect(**base_config.postgres_kwargs(), autocommit=True) as connection:
        connection.execute(sql.SQL("CREATE SCHEMA {}").format(sql.Identifier(schema)))
    config = base_config.with_overrides(
        postgres_schema=schema,
        raw_bucket=buckets["raw"],
        silver_bucket=buckets["silver"],
        gold_bucket=buckets["gold"],
        artifacts_bucket=buckets["artifacts"],
        batch_size=250,
        loki_url="",
        otlp_endpoint="",
        pushgateway_url="",
    )
    yield SimpleNamespace(config=config, s3=s3, buckets=buckets, schema=schema)
    for bucket in buckets.values():
        keys = lake.list_keys(s3, bucket, "")
        if keys:
            lake.delete_keys(s3, bucket, keys)
        s3.delete_bucket(Bucket=bucket)
    with psycopg.connect(**base_config.postgres_kwargs(), autocommit=True) as connection:
        connection.execute(
            sql.SQL("DROP SCHEMA {} CASCADE").format(sql.Identifier(schema))
        )


def aster_env(config, extra=None):
    env = dict(os.environ)
    env.update(
        {
            "POSTGRES_HOST": config.postgres_host,
            "POSTGRES_PORT": str(config.postgres_port),
            "POSTGRES_DATABASE": config.postgres_database,
            "POSTGRES_USER": config.postgres_user,
            "POSTGRES_PASSWORD": config.postgres_password,
            "ASTER_POSTGRES_SCHEMA": config.postgres_schema,
            "AWS_ENDPOINT_URL": config.endpoint,
            "AWS_DEFAULT_REGION": config.region,
            "ASTER_RAW_BUCKET": config.raw_bucket,
            "ASTER_SILVER_BUCKET": config.silver_bucket,
            "ASTER_GOLD_BUCKET": config.gold_bucket,
            "ASTER_ARTIFACTS_BUCKET": config.artifacts_bucket,
            "ASTER_BATCH_SIZE": str(config.batch_size),
            "ASTER_LOKI_URL": config.loki_url,
            "ASTER_OTLP_ENDPOINT": config.otlp_endpoint,
            "ASTER_PUSHGATEWAY_URL": config.pushgateway_url,
            "PYTHONPATH": str(PROJECT_ROOT / "src"),
        }
    )
    if extra:
        env.update(extra)
    return env


def run_aster(config, *arguments, extra_env=None, check=True):
    return subprocess.run(
        [sys.executable, "-m", "aster", *arguments],
        cwd=PROJECT_ROOT,
        env=aster_env(config, extra_env),
        capture_output=True,
        text=True,
        check=check,
    )
