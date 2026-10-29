# Metric semantics

All timestamps are stored in UTC. Silver datasets store naive UTC timestamps produced from the source `timestamptz` values. Fact table dates are UTC calendar dates.

Raw batches are append-only. Silver deduplicates raw rows by business identity, keeping the row with the highest source cursor (`updated_at` for mutable tables, `ingested_at` for events). Gold is rebuilt from the committed silver snapshot into run-specific object prefixes. `build_gold` writes only `gold/_staged/<run-id>.json`, which embeds the exact silver pointer used for the build; after `validate_gold` passes against that immutable snapshot, `publish_run` writes `gold/_committed.json` (full rebuild) or `gold/backfills/<run-id>/manifest.json` (windowed backfill). The committed pointer is the publication marker; a staged pointer is never queryable as committed gold.

| Surrogate key | Source | Ordering |
|---|---|---|
| `dim_customer.customer_key` | `row_number() OVER (ORDER BY customer_id)` | stable for a given silver snapshot |
| `dim_subscription.subscription_key` | `row_number() OVER (ORDER BY subscription_id)` | stable for a given silver snapshot |

`fact_payments.payment_date` is the UTC date of `paid_at`; it is null for payments that were never paid. `fact_service_events.event_date` is the UTC date of `event_time` (event time, not ingestion time). Facts are partitioned by `payment_month` and `event_month`.

## 1. Daily successful payment revenue

`sql/analytics/daily_successful_payment_revenue.sql`. Filters `payment_status = 'succeeded'` and a non-null `payment_date`, then sums `amount` per UTC payment date. Failed, pending and refunded payments are excluded. Payment totals are transaction amounts, not subscription MRR.

## 2. Daily active customers

`sql/analytics/daily_active_customers.sql`. Counts distinct `customer_key` values with at least one service event on the UTC event date. Service events are the activity signal; payments do not mark a customer active.

## 3. Revenue by customer region

`sql/analytics/revenue_by_region.sql`. Same revenue filter as metric 1, grouped by the region of the event's customer in `dim_customer`. A region without successful payments produces no row.

## 4. Revenue by subscription plan

`sql/analytics/revenue_by_plan.sql`. Same revenue filter as metric 1, grouped by `dim_subscription.plan_name`.

## 5. Service error rate by day and region

`sql/analytics/service_error_rate_by_day_region.sql`. Groups events by UTC event date and customer region. `error_count` counts `status_code >= 500`; `error_rate` is `error_count / event_count`. Events without a status code count as non-errors.

## 6. p95 service event duration by day

`sql/analytics/p95_service_duration_by_day.sql`. Uses DuckDB `quantile_cont(duration_ms, 0.95)` (linear interpolation) over non-null durations per UTC event date, plus the number of measured events.

## 7. Daily event volume

`sql/analytics/daily_event_volume.sql`. Counts service events per UTC event date.

## 8. Seven-day revenue trend

`sql/analytics/seven_day_revenue_trend.sql`. Reports the trailing inclusive seven-day successful revenue sum (`ROWS BETWEEN 6 PRECEDING AND CURRENT ROW`) for every date on a continuous daily calendar between the first and last successful payment date. Days without revenue contribute zero. The calendar is published as the `revenue_trend_seed` gold dataset (`calendar_date`, `is_lookback`, `prior_revenue`), derived from the committed silver payment snapshot.

A windowed backfill writes a second `revenue_trend_seed` clipped to the requested window and marks the six days before the window start as lookback rows. Lookback revenue participates in the rolling sum but lookback rows are never returned, so a backfilled trend equals a full rebuild restricted to the same dates, including the first day of the window. When the window does not overlap the successful payment range the seed and the trend are empty rather than fabricated zero rows.

## Data quality gates

Quality validation runs before publication and fails the run with a nonzero exit code:

- Raw: manifest completeness, object checksums, cursor monotonicity, and source reconciliation (append-only events must match exactly; mutable tables require every current key to be captured with at least one version).
- Silver: primary-key uniqueness, required fields, checksum verification, quarantine counts, and `accepted + rejected = distinct raw keys`.
- Gold: row counts and checksums, surrogate-key uniqueness, and referential integrity of facts.

Malformed rows are quarantined under `silver/<dataset>/run=<run>/rejected.parquet` with a `_reject_reason` column. Reasons include missing required fields, invalid statuses, negative amounts or durations, succeeded payments without `paid_at`, timestamps out of order, and unknown foreign keys.

Gold validation also reconciles against the silver snapshot embedded in the staged pointer, not against whatever silver is committed when validation runs, and it fences a build whose silver snapshot has been superseded. A committed gold pointer without `revenue_trend_seed`, `silver`, or `silver_run_id` is reported as a legacy pointer: validation fails with the missing keys and an explicit rebuild command, and the existing gold objects are never deleted.
