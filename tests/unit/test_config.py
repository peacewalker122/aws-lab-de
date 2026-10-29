import os

import pytest

from aster.config import Config


def test_endpoint_must_be_loopback(monkeypatch):
    monkeypatch.setenv("POSTGRES_PASSWORD", "test")
    monkeypatch.setenv("AWS_ENDPOINT_URL", "http://example.com:4566")
    with pytest.raises(ValueError, match="loopback"):
        Config.from_env()


def test_requires_postgres_password(monkeypatch):
    monkeypatch.delenv("POSTGRES_PASSWORD", raising=False)
    with pytest.raises(KeyError):
        Config.from_env()


def test_bucket_defaults_follow_environment(monkeypatch):
    monkeypatch.setenv("POSTGRES_PASSWORD", "test")
    monkeypatch.setenv("TF_VAR_environment", "ci")
    monkeypatch.delenv("ASTER_RAW_BUCKET", raising=False)
    monkeypatch.delenv("ASTER_SILVER_BUCKET", raising=False)
    config = Config.from_env()
    assert config.raw_bucket == "aster-ci-raw"
    assert config.silver_bucket == "aster-ci-silver"
    assert config.gold_bucket == "aster-ci-gold"
    assert config.artifacts_bucket == "aster-ci-artifacts"


def test_schema_identifier_is_validated(monkeypatch):
    monkeypatch.setenv("POSTGRES_PASSWORD", "test")
    monkeypatch.setenv("ASTER_POSTGRES_SCHEMA", "bad schema")
    with pytest.raises(ValueError, match="ASTER_POSTGRES_SCHEMA"):
        Config.from_env()


def test_batch_size_must_be_positive(monkeypatch):
    monkeypatch.setenv("POSTGRES_PASSWORD", "test")
    monkeypatch.setenv("ASTER_BATCH_SIZE", "0")
    with pytest.raises(ValueError, match="ASTER_BATCH_SIZE"):
        Config.from_env()


def test_optional_observability_urls_default_to_disabled(monkeypatch):
    monkeypatch.setenv("POSTGRES_PASSWORD", "test")
    monkeypatch.delenv("ASTER_LOKI_URL", raising=False)
    monkeypatch.setenv("ASTER_OTLP_ENDPOINT", "http://localhost:4318")
    config = Config.from_env()
    assert config.loki_url == ""
    assert config.otlp_endpoint == "http://localhost:4318"
    assert os.environ["ASTER_OTLP_ENDPOINT"] == "http://localhost:4318"
