with bounds as (
    select
        min(event_date) as min_day,
        max(event_date) as max_day
    from {{ ref('stg_transactions') }}
),

spine as (
    select unnest(generate_series(min_day, max_day, interval 1 day))::date as date_day
    from bounds
)

select
    cast(strftime(date_day, '%Y%m%d') as integer) as date_key,
    date_day,
    extract(year from date_day) as year,
    extract(quarter from date_day) as quarter,
    extract(month from date_day) as month,
    extract(day from date_day) as day_of_month,
    extract(isodow from date_day) as iso_day_of_week,
    extract(isodow from date_day) in (6, 7) as is_weekend
from spine
