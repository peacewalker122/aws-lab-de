import os
import re
from dataclasses import dataclass, replace
from urllib.parse import urlsplit


def _local_http_endpoint(value, variable):
    parsed = urlsplit(value)
    if (
        parsed.scheme != "http"
        or parsed.hostname not in {"localhost", "127.0.0.1"}
        or not parsed.port
    ):
        raise ValueError(f"{variable} must be a loopback HTTP URL with a port")
    return value


@dataclass(frozen=True)
class Config:
    postgres_host: str
    postgres_port: int
    postgres_database: str
    postgres_user: str
    postgres_password: str
    postgres_schema: str
    endpoint: str
    region: str
    raw_bucket: str
    silver_bucket: str
    gold_bucket: str
    artifacts_bucket: str
    batch_size: int
    loki_url: str
    otlp_endpoint: str
    pushgateway_url: str

    @classmethod
    def from_env(cls):
        endpoint = _local_http_endpoint(
            os.environ.get("AWS_ENDPOINT_URL", "http://127.0.0.1:4566"),
            "AWS_ENDPOINT_URL",
        )
        size = int(os.environ.get("ASTER_BATCH_SIZE", "1000"))
        if size <= 0:
            raise ValueError("ASTER_BATCH_SIZE must be positive")
        environment = os.environ.get("TF_VAR_environment", "local")
        schema = os.environ.get("ASTER_POSTGRES_SCHEMA", "public")
        if not re.fullmatch(r"[a-z_][a-z0-9_]{0,62}", schema):
            raise ValueError("ASTER_POSTGRES_SCHEMA must be a lowercase identifier")

        return cls(
            postgres_host=os.environ.get("POSTGRES_HOST", "127.0.0.1"),
            postgres_port=int(os.environ.get("POSTGRES_PORT", "5432")),
            postgres_database=os.environ.get("POSTGRES_DATABASE", "aster"),
            postgres_user=os.environ.get("POSTGRES_USER", "aster"),
            postgres_password=os.environ["POSTGRES_PASSWORD"],
            postgres_schema=schema,
            endpoint=endpoint,
            region=os.environ.get("AWS_DEFAULT_REGION", "us-east-1"),
            raw_bucket=os.environ.get("ASTER_RAW_BUCKET", f"aster-{environment}-raw"),
            silver_bucket=os.environ.get(
                "ASTER_SILVER_BUCKET", f"aster-{environment}-silver"
            ),
            gold_bucket=os.environ.get("ASTER_GOLD_BUCKET", f"aster-{environment}-gold"),
            artifacts_bucket=os.environ.get(
                "ASTER_ARTIFACTS_BUCKET", f"aster-{environment}-artifacts"
            ),
            batch_size=size,
            loki_url=_optional_url("ASTER_LOKI_URL"),
            otlp_endpoint=_optional_url("ASTER_OTLP_ENDPOINT"),
            pushgateway_url=_optional_url("ASTER_PUSHGATEWAY_URL"),
        )

    def with_overrides(self, **changes):
        return replace(self, **changes)

    def postgres_kwargs(self):
        return {
            "host": self.postgres_host,
            "port": self.postgres_port,
            "dbname": self.postgres_database,
            "user": self.postgres_user,
            "password": self.postgres_password,
        }

    def s3_host(self):
        parsed = urlsplit(self.endpoint)
        return f"{parsed.hostname}:{parsed.port}"


def _optional_url(variable):
    value = os.environ.get(variable, "").strip()
    return _local_http_endpoint(value, variable) if value else ""
