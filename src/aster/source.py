import random
import uuid
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from pathlib import Path

from psycopg import sql
from psycopg.types.json import Jsonb

MIGRATION = Path(__file__).resolve().parents[2] / "sql/migrations/001_source.sql"
SIZES = {"small": 1_000, "medium": 100_000, "large": 1_000_000}
ANCHOR = datetime(2025, 1, 1, tzinfo=timezone.utc)


def migrate(connection):
    with connection.transaction():
        connection.execute(MIGRATION.read_text())


def create_database(connection, name):
    exists = connection.execute(
        "SELECT 1 FROM pg_database WHERE datname = %s", (name,)
    ).fetchone()
    if not exists:
        connection.execute(
            sql.SQL("CREATE DATABASE {}").format(sql.Identifier(name))
        )
    return not exists


def _identifier(rng):
    return uuid.UUID(int=rng.getrandbits(128), version=4)


def _source_rows(size, seed_value):
    count = SIZES[size]
    rng = random.Random(seed_value)
    customers = [_identifier(rng) for _ in range(100)]
    subscriptions = [_identifier(rng) for _ in customers]
    customer_rows = []
    for i, customer in enumerate(customers):
        created = ANCHOR + timedelta(days=rng.randrange(180))
        customer_rows.append(
            (customer, f"Customer {i + 1}", ("north", "south", "west")[i % 3], created)
        )
    subscription_rows = []
    for i, (subscription, customer) in enumerate(
        zip(subscriptions, customers, strict=True)
    ):
        created = ANCHOR + timedelta(days=rng.randrange(180))
        subscription_rows.append(
            (
                subscription,
                customer,
                ("basic", "pro", "enterprise")[i % 3],
                Decimal("19.00") if i % 3 == 0 else Decimal("49.00") if i % 3 == 1 else Decimal("199.00"),
                ("active", "active", "paused")[i % 3],
                created,
            )
        )
    payment_rows = []
    for i, subscription in enumerate(subscriptions):
        created = ANCHOR + timedelta(days=rng.randrange(365))
        succeeded = i % 10 != 0
        payment_rows.append(
            (
                _identifier(rng),
                subscription,
                Decimal("19.00") if i % 3 == 0 else Decimal("49.00") if i % 3 == 1 else Decimal("199.00"),
                "USD",
                "succeeded" if succeeded else "failed",
                created if succeeded else None,
                created,
            )
        )
    event_rows = []
    for i in range(count):
        event_time = ANCHOR + timedelta(seconds=rng.randrange(365 * 86400))
        event_rows.append(
            (
                _identifier(rng),
                customers[rng.randrange(len(customers))],
                "request",
                event_time,
                rng.randrange(10, 3000),
                500 if i % 20 == 0 else 200,
                Jsonb({"source": "synthetic"}),
            )
        )
    return customer_rows, subscription_rows, payment_rows, event_rows


def seed(connection, size, seed_value):
    customers, subscriptions, payments, events = _source_rows(size, seed_value)
    with connection.transaction():
        connection.execute(
            "LOCK TABLE customers, subscriptions, payments, service_events IN EXCLUSIVE MODE"
        )
        if any(
            connection.execute(f"SELECT EXISTS (SELECT 1 FROM {table})").fetchone()[0]
            for table in ("customers", "subscriptions", "payments", "service_events")
        ):
            raise ValueError(
                "Seed requires empty source tables; existing data was not changed"
            )
        with connection.cursor().copy(
            "COPY customers (customer_id, name, region, created_at, updated_at) FROM STDIN"
        ) as copy:
            for customer, name, region, created in customers:
                copy.write_row((customer, name, region, created, created))
        with connection.cursor().copy(
            "COPY subscriptions (subscription_id, customer_id, plan_name, monthly_price, status, created_at, updated_at) FROM STDIN"
        ) as copy:
            for subscription, customer, plan, price, status, created in subscriptions:
                copy.write_row(
                    (subscription, customer, plan, price, status, created, created)
                )
        with connection.cursor().copy(
            "COPY payments (payment_id, subscription_id, amount, currency, status, paid_at, created_at, updated_at) FROM STDIN"
        ) as copy:
            for payment, subscription, amount, currency, status, paid, created in payments:
                copy.write_row(
                    (
                        payment,
                        subscription,
                        amount,
                        currency,
                        status,
                        paid,
                        created,
                        created,
                    )
                )
        with connection.cursor().copy(
            "COPY service_events (event_id, customer_id, event_type, event_time, ingested_at, duration_ms, status_code, payload) FROM STDIN"
        ) as copy:
            for event, customer, event_type, event_time, duration, status, payload in events:
                copy.write_row(
                    (
                        event,
                        customer,
                        event_type,
                        event_time,
                        event_time,
                        duration,
                        status,
                        payload,
                    )
                )
