-- Anomaly guard: a day's fraud rate must sit within three standard deviations
-- of the window mean. This is the "over-flagging" class of bug: a calibration
-- change that silently doubles the alert volume instead of failing a key test.
with daily as (
    select
        event_date,
        count(*) as transaction_count,
        sum(case when is_fraud then 1 else 0 end) as fraud_count,
        sum(case when is_fraud then 1 else 0 end) * 1.0 / count(*) as fraud_rate
    from {{ ref('fact_transaction') }}
    group by event_date
),

bounds as (
    select
        avg(fraud_rate) as mean_rate,
        stddev_samp(fraud_rate) as std_rate
    from daily
)

select
    daily.event_date,
    daily.transaction_count,
    daily.fraud_count,
    daily.fraud_rate,
    bounds.mean_rate,
    bounds.std_rate
from daily
cross join bounds
where bounds.std_rate > 0
  and daily.fraud_rate > bounds.mean_rate + 3 * bounds.std_rate
