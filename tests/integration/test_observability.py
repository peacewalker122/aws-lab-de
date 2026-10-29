import json
import os
import time
import urllib.error
import urllib.parse
import urllib.request
import uuid

import pytest

from aster import pipeline
from aster.source import migrate, seed

pytestmark = [pytest.mark.integration, pytest.mark.observability]

LOKI = "http://127.0.0.1:3100"
TEMPO = "http://127.0.0.1:3200"
PROMETHEUS = "http://127.0.0.1:19090"
PUSHGATEWAY = "http://127.0.0.1:9091"
GRAFANA = "http://127.0.0.1:3000"


def fetch(url, method="GET", body=None, headers=None, auth=None):
    request = urllib.request.Request(
        url, data=body, method=method, headers=headers or {}
    )
    if auth:
        import base64

        request.add_header(
            "Authorization",
            "Basic " + base64.b64encode(auth.encode()).decode(),
        )
    with urllib.request.urlopen(request, timeout=10) as response:
        return response.status, response.read()


def ready(url):
    try:
        return fetch(url)[0] == 200
    except (urllib.error.URLError, TimeoutError, ConnectionError):
        return False


def require_stack():
    if not all(
        ready(url)
        for url in (
            LOKI + "/ready",
            TEMPO + "/ready",
            PROMETHEUS + "/-/ready",
            PUSHGATEWAY + "/-/ready",
            GRAFANA + "/api/health",
        )
    ):
        pytest.skip("LGTM stack is not running; run `make monitoring-up`")


def wait_for(predicate, timeout=60, interval=2):
    deadline = time.time() + timeout
    while time.time() < deadline:
        result = predicate()
        if result:
            return result
        time.sleep(interval)
    return None


def test_monitoring_stack_is_ready_and_provisioned():
    require_stack()
    status, body = fetch(GRAFANA + "/api/datasources", auth="admin:" + os.environ["GF_SECURITY_ADMIN_PASSWORD"])
    assert status == 200
    datasources = {item["type"]: item["url"] for item in json.loads(body)}
    assert datasources == {
        "loki": "http://loki:3100",
        "prometheus": "http://prometheus:9090",
        "tempo": "http://tempo:3200",
    }
    status, body = fetch(
        GRAFANA + "/api/search?query=Aster",
        auth="admin:" + os.environ["GF_SECURITY_ADMIN_PASSWORD"],
    )
    assert status == 200
    assert any(item["uid"] == "aster-pipeline" for item in json.loads(body))


def test_pipeline_telemetry_reaches_all_backends(lake_env, base_config):
    require_stack()
    config = lake_env.config.with_overrides(
        loki_url=base_config.loki_url,
        otlp_endpoint=base_config.otlp_endpoint,
        pushgateway_url=base_config.pushgateway_url,
    )
    assert config.loki_url and config.otlp_endpoint and config.pushgateway_url
    run_id = f"obs-{uuid.uuid4().hex[:8]}"
    with pipeline.connect_postgres(config) as connection:
        migrate(connection)
        seed(connection, "small", 42)
    pipeline.run_pipeline(config, run_id)

    query = urllib.parse.quote(f'aster_rows_extracted{{run_id="{run_id}"}}')
    prometheus = wait_for(
        lambda: (
            json.loads(fetch(PROMETHEUS + "/api/v1/query?query=" + query)[1])["data"][
                "result"
            ]
        ),
        timeout=90,
    )
    assert prometheus, "Prometheus did not scrape the pushed metrics"
    assert prometheus[0]["value"][1] == "1300"
    assert prometheus[0]["metric"]["job"] == "aster_pipeline"

    loki_query = urllib.parse.urlencode(
        {
            "query": f'{{app="aster", run_id="{run_id}"}}',
            "limit": "100",
            "start": str(int((time.time() - 3600) * 1e9)),
        }
    )
    loki = wait_for(
        lambda: json.loads(fetch(LOKI + "/loki/api/v1/query_range?" + loki_query)[1])[
            "data"
        ]["result"],
        timeout=60,
    )
    assert loki
    messages = [line for stream in loki for _, line in stream["values"]]
    assert any("pipeline_succeeded" in message for message in messages)
    assert any("raw_batch_committed" in message for message in messages)
    assert all(run_id in message for message in messages)

    tempo = wait_for(
        lambda: json.loads(
            fetch(TEMPO + "/api/search?tags=" + urllib.parse.quote("service.name=aster") + "&limit=20")[1]
        )["traces"],
        timeout=90,
    )
    assert tempo, "Tempo has no traces for the aster service"
    span_names = set()
    for trace in tempo:
        payload = json.loads(fetch(TEMPO + f"/api/traces/{trace['traceID']}")[1])
        for resource in payload.get("batches", []):
            for scope in resource.get("scopeSpans", []):
                span_names.update(
                    span["name"] for span in scope.get("spans", [])
                )
    assert {"transform_silver", "build_gold", "publish_run"} <= span_names
