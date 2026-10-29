from datetime import date
from decimal import Decimal

import duckdb
import pytest

from aster import metrics

CUSTOMERS = [
    (1, "c1", "north", "2025-01-01"),
    (2, "c2", "south", "2025-01-01"),
    (3, "c3", "west", "2025-01-01"),
]
SUBSCRIPTIONS = [
    (1, "s1", "basic", Decimal("19.00"), "active"),
    (2, "s2", "pro", Decimal("49.00"), "active"),
    (3, "s3", "enterprise", Decimal("199.00"), "paused"),
]
PAYMENTS = [
    ("p1", 1, 1, Decimal("19.00"), "2025-01-01", "succeeded"),
    ("p2", 1, 1, Decimal("19.00"), "2025-01-01", "succeeded"),
    ("p3", 2, 2, Decimal("49.00"), "2025-01-02", "succeeded"),
    ("p4", 2, 2, Decimal("49.00"), "2025-01-02", "failed"),
    ("p5", 3, 3, Decimal("199.00"), "2025-01-07", "succeeded"),
    ("p6", 3, 3, Decimal("199.00"), "2025-01-09", "refunded"),
]
EVENTS = [
    ("e1", 1, "2025-01-01", "request", 100, 200),
    ("e2", 2, "2025-01-01", "request", 200, 500),
    ("e3", 1, "2025-01-02", "request", 300, 200),
    ("e4", 3, "2025-01-02", "request", 400, 200),
    ("e5", 2, "2025-01-02", "request", 500, 503),
]


def metric(connection, name):
    return [
        tuple(
            value.isoformat() if isinstance(value, date) else value
            for value in row.values()
        )
        for row in metrics.run_metric(connection, name)
    ]


@pytest.fixture
def connection():
    with duckdb.connect() as warehouse:
        warehouse.execute(
            "CREATE TABLE dim_customer (customer_key BIGINT, customer_id VARCHAR, region VARCHAR, created_date DATE)"
        )
        warehouse.execute(
            "CREATE TABLE dim_subscription (subscription_key BIGINT, subscription_id VARCHAR, plan_name VARCHAR, monthly_price DECIMAL(12,2), status VARCHAR)"
        )
        warehouse.execute(
            "CREATE TABLE fact_payments (payment_id VARCHAR, customer_key BIGINT, subscription_key BIGINT, amount DECIMAL(12,2), payment_date DATE, payment_status VARCHAR)"
        )
        warehouse.execute(
            "CREATE TABLE fact_service_events (event_id VARCHAR, customer_key BIGINT, event_date DATE, event_type VARCHAR, duration_ms INTEGER, status_code INTEGER)"
        )
        warehouse.executemany(
            "INSERT INTO dim_customer VALUES (?, ?, ?, ?::DATE)", CUSTOMERS
        )
        warehouse.executemany(
            "INSERT INTO dim_subscription VALUES (?, ?, ?, ?, ?)", SUBSCRIPTIONS
        )
        warehouse.executemany("INSERT INTO fact_payments VALUES (?, ?, ?, ?, ?::DATE, ?)", PAYMENTS)
        warehouse.executemany(
            "INSERT INTO fact_service_events VALUES (?, ?, ?::DATE, ?, ?, ?)", EVENTS
        )
        yield warehouse


def test_daily_successful_payment_revenue(connection):
    assert metric(connection, "daily_successful_payment_revenue") == [
        ("2025-01-01", Decimal("38.00"), 2),
        ("2025-01-02", Decimal("49.00"), 1),
        ("2025-01-07", Decimal("199.00"), 1),
    ]


def test_daily_active_customers(connection):
    assert metric(connection, "daily_active_customers") == [
        ("2025-01-01", 2),
        ("2025-01-02", 3),
    ]


def test_revenue_by_region(connection):
    assert metric(connection, "revenue_by_region") == [
        ("north", Decimal("38.00"), 2),
        ("south", Decimal("49.00"), 1),
        ("west", Decimal("199.00"), 1),
    ]


def test_revenue_by_plan(connection):
    assert metric(connection, "revenue_by_plan") == [
        ("basic", Decimal("38.00"), 2),
        ("enterprise", Decimal("199.00"), 1),
        ("pro", Decimal("49.00"), 1),
    ]


def test_service_error_rate_by_day_region(connection):
    rows = metric(connection, "service_error_rate_by_day_region")
    assert rows == [
        ("2025-01-01", "north", 1, 0, 0.0),
        ("2025-01-01", "south", 1, 1, 1.0),
        ("2025-01-02", "north", 1, 0, 0.0),
        ("2025-01-02", "south", 1, 1, 1.0),
        ("2025-01-02", "west", 1, 0, 0.0),
    ]


def test_p95_service_duration_by_day(connection):
    rows = metric(connection, "p95_service_duration_by_day")
    assert [row[0] for row in rows] == ["2025-01-01", "2025-01-02"]
    assert [row[1] for row in rows] == [2, 3]
    assert rows[0][2] == pytest.approx(195.0)
    assert rows[1][2] == pytest.approx(490.0)


def test_daily_event_volume(connection):
    assert metric(connection, "daily_event_volume") == [
        ("2025-01-01", 2),
        ("2025-01-02", 3),
    ]


def test_seven_day_revenue_trend(connection):
    assert metric(connection, "seven_day_revenue_trend") == [
        ("2025-01-01", Decimal("38.00")),
        ("2025-01-02", Decimal("87.00")),
        ("2025-01-03", Decimal("87.00")),
        ("2025-01-04", Decimal("87.00")),
        ("2025-01-05", Decimal("87.00")),
        ("2025-01-06", Decimal("87.00")),
        ("2025-01-07", Decimal("286.00")),
    ]


def test_every_metric_is_covered():
    assert len(metrics.METRICS) == 8
