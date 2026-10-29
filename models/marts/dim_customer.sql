with snapshots as (
    select
        customer_id,
        name,
        age,
        email,
        state,
        location,
        location_type,
        home_latitude,
        home_longitude,
        home_h3r5,
        home_h3r7,
        credit_score,
        case
            when credit_score < 600 then 'subprime'
            when credit_score < 700 then 'near_prime'
            when credit_score < 800 then 'prime'
            else 'super_prime'
        end as credit_band,
        monthly_spend,
        customer_risk_score,
        is_fraud,
        primary_ua,
        secondary_ua,
        isp,
        ip_subnet,
        registration_date,
        snapshot_date
    from {{ ref('stg_customers') }}
),

changes as (
    select
        *,
        lag(state) over w as previous_state,
        lag(credit_band) over w as previous_credit_band,
        lag(snapshot_date) over w as previous_snapshot_date
    from snapshots
    window w as (partition by customer_id order by snapshot_date)
),

version_starts as (
    select *
    from changes
    where previous_snapshot_date is null
       or state is distinct from previous_state
       or credit_band is distinct from previous_credit_band
),

versions as (
    select
        *,
        lead(snapshot_date) over (
            partition by customer_id order by snapshot_date
        ) as next_snapshot_date
    from version_starts
)

select
    md5(concat_ws('|', customer_id, cast(snapshot_date as varchar))) as customer_sk,
    customer_id,
    name,
    age,
    email,
    state,
    location,
    location_type,
    home_latitude,
    home_longitude,
    home_h3r5,
    home_h3r7,
    credit_score,
    credit_band,
    monthly_spend,
    customer_risk_score,
    is_fraud,
    registration_date,
    snapshot_date as valid_from,
    cast(
        coalesce(next_snapshot_date - interval 1 day, date '9999-12-31') as date
    ) as valid_to,
    next_snapshot_date is null as is_current
from versions
