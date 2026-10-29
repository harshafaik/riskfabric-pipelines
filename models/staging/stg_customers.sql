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
    monthly_spend,
    customer_risk_score,
    is_fraud,
    primary_ua,
    secondary_ua,
    isp,
    ip_subnet,
    registration_date,
    event_date as snapshot_date
from read_parquet(
    '{{ var("bronze_root") }}/customers/event_date=*/*.parquet',
    union_by_name = true,
    hive_partitioning = false
)
