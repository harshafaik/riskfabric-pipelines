select
    md5(merchant_id) as merchant_sk,
    merchant_id,
    arg_max(merchant_name, event_date) as merchant_name,
    arg_max(merchant_category, event_date) as merchant_category,
    arg_max(mcc, event_date) as mcc,
    arg_max(merchant_country, event_date) as merchant_country,
    min(event_date) as first_seen_date,
    max(event_date) as last_seen_date
from {{ ref('stg_transactions') }}
group by merchant_id
