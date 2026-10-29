CREATE TABLE IF NOT EXISTS customers (
    customer_id uuid PRIMARY KEY,
    name text NOT NULL,
    region text NOT NULL,
    created_at timestamptz NOT NULL,
    updated_at timestamptz NOT NULL
);

CREATE TABLE IF NOT EXISTS subscriptions (
    subscription_id uuid PRIMARY KEY,
    customer_id uuid NOT NULL REFERENCES customers(customer_id),
    plan_name text NOT NULL,
    monthly_price numeric(12, 2) NOT NULL,
    status text NOT NULL,
    created_at timestamptz NOT NULL,
    updated_at timestamptz NOT NULL
);

CREATE TABLE IF NOT EXISTS payments (
    payment_id uuid PRIMARY KEY,
    subscription_id uuid NOT NULL REFERENCES subscriptions(subscription_id),
    amount numeric(12, 2) NOT NULL,
    currency text NOT NULL,
    status text NOT NULL,
    paid_at timestamptz,
    created_at timestamptz NOT NULL,
    updated_at timestamptz NOT NULL
);

CREATE TABLE IF NOT EXISTS service_events (
    event_id uuid PRIMARY KEY,
    customer_id uuid NOT NULL REFERENCES customers(customer_id),
    event_type text NOT NULL,
    event_time timestamptz NOT NULL,
    ingested_at timestamptz NOT NULL,
    duration_ms integer,
    status_code integer,
    payload jsonb NOT NULL
);

CREATE TABLE IF NOT EXISTS source_cursor_clock (
    singleton boolean PRIMARY KEY DEFAULT true CHECK (singleton),
    last_time timestamptz NOT NULL
);

INSERT INTO source_cursor_clock (singleton, last_time)
VALUES (true, '-infinity') ON CONFLICT DO NOTHING;

CREATE OR REPLACE FUNCTION assign_source_cursor() RETURNS trigger AS $$
DECLARE
    next_time timestamptz;
BEGIN
    UPDATE source_cursor_clock
    SET last_time = greatest(clock_timestamp(), last_time + interval '1 microsecond')
    WHERE singleton = true
    RETURNING last_time INTO next_time;
    IF TG_TABLE_NAME = 'service_events' THEN
        NEW.ingested_at := next_time;
    ELSE
        NEW.updated_at := next_time;
    END IF;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION reject_source_removal() RETURNS trigger AS $$
BEGIN
    RAISE EXCEPTION 'Source records are append-only or update-only; deletion is unsupported';
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION reject_event_update() RETURNS trigger AS $$
BEGIN
    RAISE EXCEPTION 'Service events are append-only';
END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS customers_cursor ON customers;
CREATE TRIGGER customers_cursor BEFORE INSERT OR UPDATE ON customers FOR EACH ROW EXECUTE FUNCTION assign_source_cursor();
DROP TRIGGER IF EXISTS subscriptions_cursor ON subscriptions;
CREATE TRIGGER subscriptions_cursor BEFORE INSERT OR UPDATE ON subscriptions FOR EACH ROW EXECUTE FUNCTION assign_source_cursor();
DROP TRIGGER IF EXISTS payments_cursor ON payments;
CREATE TRIGGER payments_cursor BEFORE INSERT OR UPDATE ON payments FOR EACH ROW EXECUTE FUNCTION assign_source_cursor();
DROP TRIGGER IF EXISTS events_cursor ON service_events;
CREATE TRIGGER events_cursor BEFORE INSERT ON service_events FOR EACH ROW EXECUTE FUNCTION assign_source_cursor();
DROP TRIGGER IF EXISTS events_no_update ON service_events;
CREATE TRIGGER events_no_update BEFORE UPDATE ON service_events FOR EACH ROW EXECUTE FUNCTION reject_event_update();
DROP TRIGGER IF EXISTS customers_no_delete ON customers;
CREATE TRIGGER customers_no_delete BEFORE DELETE ON customers FOR EACH ROW EXECUTE FUNCTION reject_source_removal();
DROP TRIGGER IF EXISTS subscriptions_no_delete ON subscriptions;
CREATE TRIGGER subscriptions_no_delete BEFORE DELETE ON subscriptions FOR EACH ROW EXECUTE FUNCTION reject_source_removal();
DROP TRIGGER IF EXISTS payments_no_delete ON payments;
CREATE TRIGGER payments_no_delete BEFORE DELETE ON payments FOR EACH ROW EXECUTE FUNCTION reject_source_removal();
DROP TRIGGER IF EXISTS events_no_delete ON service_events;
CREATE TRIGGER events_no_delete BEFORE DELETE ON service_events FOR EACH ROW EXECUTE FUNCTION reject_source_removal();

CREATE TABLE IF NOT EXISTS extraction_checkpoints (
    dataset text PRIMARY KEY,
    cursor_time timestamptz NOT NULL,
    cursor_id uuid NOT NULL
);

CREATE INDEX IF NOT EXISTS customers_cursor_idx ON customers (updated_at, customer_id);
CREATE INDEX IF NOT EXISTS subscriptions_cursor_idx ON subscriptions (updated_at, subscription_id);
CREATE INDEX IF NOT EXISTS payments_cursor_idx ON payments (updated_at, payment_id);
CREATE INDEX IF NOT EXISTS events_cursor_idx ON service_events (ingested_at, event_id);
