import json
import logging
import urllib.request
from contextlib import contextmanager
from datetime import datetime, timezone

from opentelemetry import trace
from opentelemetry.exporter.otlp.proto.http.trace_exporter import OTLPSpanExporter
from opentelemetry.sdk.resources import Resource
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import BatchSpanProcessor
from opentelemetry.trace import Status, StatusCode

SERVICE = "aster"
LOGGER_NAME = "aster"
TRACES_PATH = "/v1/traces"
PUSH_PATH = "/metrics/job/aster_pipeline"

_provider = None


class _JsonFormatter(logging.Formatter):
    def format(self, record):
        payload = {
            "timestamp": datetime.fromtimestamp(
                record.created, timezone.utc
            ).isoformat(),
            "level": record.levelname.lower(),
            "service": getattr(record, "service", SERVICE),
            "run_id": getattr(record, "run_id", None),
            "message": record.getMessage(),
        }
        payload.update(getattr(record, "fields", {}))
        if record.exc_info:
            payload["error"] = self.formatException(record.exc_info)
        return json.dumps(payload, default=str, sort_keys=True)


class _RunContext(logging.Filter):
    def __init__(self, service, run_id):
        super().__init__()
        self.service = service
        self.run_id = run_id

    def filter(self, record):
        record.service = self.service
        if getattr(record, "run_id", None) is None:
            record.run_id = self.run_id
        return True


class _LokiHandler(logging.Handler):
    def __init__(self, url, service, run_id):
        super().__init__()
        self.url = url.rstrip("/") + "/loki/api/v1/push"
        self.labels = {"app": SERVICE, "service": service, "run_id": run_id or "none"}

    def emit(self, record):
        payload = {
            "streams": [
                {
                    "stream": self.labels,
                    "values": [[str(int(record.created * 1e9)), self.format(record)]],
                }
            ]
        }
        request = urllib.request.Request(
            self.url,
            data=json.dumps(payload).encode(),
            headers={"Content-Type": "application/json"},
            method="POST",
        )
        try:
            urllib.request.urlopen(request, timeout=3).read()
        except Exception:
            self.handleError(record)


def configure_logging(service=SERVICE, run_id=None, loki_url=""):
    logger = logging.getLogger(LOGGER_NAME)
    logger.setLevel(logging.INFO)
    logger.propagate = False
    for handler in list(logger.handlers):
        logger.removeHandler(handler)
        handler.close()
    formatter = _JsonFormatter()
    context = _RunContext(service, run_id)
    console = logging.StreamHandler()
    console.setFormatter(formatter)
    console.addFilter(context)
    logger.addHandler(console)
    if loki_url:
        loki = _LokiHandler(loki_url, service, run_id)
        loki.setFormatter(formatter)
        loki.addFilter(context)
        logger.addHandler(loki)
    return logger


def logger():
    return logging.getLogger(LOGGER_NAME)


def log(event, **fields):
    logger().info(event, extra={"fields": fields})


def configure_tracing(endpoint):
    global _provider
    if not endpoint:
        return False
    provider = TracerProvider(resource=Resource.create({"service.name": SERVICE}))
    provider.add_span_processor(
        BatchSpanProcessor(
            OTLPSpanExporter(endpoint=endpoint.rstrip("/") + TRACES_PATH, timeout=5)
        )
    )
    trace.set_tracer_provider(provider)
    _provider = provider
    return True


def flush_tracing():
    if _provider is not None:
        _provider.force_flush(5000)


@contextmanager
def span(name, **attributes):
    tracer = trace.get_tracer(SERVICE)
    with tracer.start_as_current_span(name, attributes=attributes) as current:
        try:
            yield current
        except Exception as error:
            current.record_exception(error)
            current.set_status(Status(StatusCode.ERROR, type(error).__name__))
            raise


def push_metrics(url, run_id, values):
    if not url:
        return False
    body = "\n".join(
        line
        for name, value in sorted(values.items())
        for line in (
            f"# TYPE {name} gauge",
            f'{name}{{run_id="{run_id}"}} {value}',
        )
    )
    base = url.rstrip("/") + PUSH_PATH
    try:
        urllib.request.urlopen(
            urllib.request.Request(base, method="DELETE"), timeout=3
        ).read()
    except Exception:
        pass
    try:
        urllib.request.urlopen(
            urllib.request.Request(
                base,
                data=(body + "\n").encode(),
                headers={"Content-Type": "text/plain"},
                method="PUT",
            ),
            timeout=3,
        ).read()
        return True
    except Exception as error:
        logger().warning(
            "metrics_push_failed", extra={"fields": {"error": str(error)}}
        )
        return False
