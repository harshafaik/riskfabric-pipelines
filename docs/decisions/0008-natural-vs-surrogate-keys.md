# 0008 — Facts join dimensions on natural keys

## Context

`dim_customer` is SCD2 and exposes `customer_sk`; `dim_card` and `dim_merchant` expose `card_sk`
and `merchant_sk`. `fact_transaction` carries `customer_id`, `card_id`, `merchant_id` and
`event_date`, not the surrogate keys. Two designs are possible: join the as-of surrogate keys
into the fact, or keep natural keys and let consumers use the validity range.

## Decision

Keep natural keys in the fact. Test referential integrity with `relationships` against the
dimensions' natural keys. Do not perform an as-of join into `fact_transaction` yet.

## Why

An as-of join on `transaction_ts` between `valid_from` and `valid_to` changes the incremental
model and its lookback semantics. With one customer snapshot in the fixture, every
`customer_sk` is the current version, so the join would add complexity with no behavioural
difference today.

The natural-key relationship tests catch the failure that matters at this layer: a fact row
whose dimension does not exist. They do not pretend to solve point-in-time attribution.

Consumers that need point-in-time attribution can join `dim_customer` on the validity range.
The range is explicit, tested for uniqueness, and documented in `schema.yml`.

## Consequences

A consumer joining `fact_transaction` to `dim_customer` on `customer_id` alone will duplicate
fact rows once multiple versions exist. That is a documented trap, not a guard.

Surrogate keys are not exercised end to end. `customer_sk` is tested for uniqueness only; there
is no test that a fact row resolves to exactly one customer version.

The fact grain is unaffected: one row per `transaction_id`.

## Revisit

When a second customer snapshot lands and the SCD2 change path has a test, add the as-of join
and the surrogate keys to the fact. That change should be a new decision with a measured
row-count and build-time delta, not an edit to this one.

## Related

- `models/marts/fact_transaction.sql`
- `models/marts/dim_customer.sql`
- `models/marts/schema.yml`
- `docs/components/transforms.md`
