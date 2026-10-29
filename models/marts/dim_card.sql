select
    md5(card_id) as card_sk,
    card_id,
    arg_min(account_id, event_date) as account_id,
    arg_min(customer_id, event_date) as customer_id,
    min(event_date) as first_seen_date,
    max(event_date) as last_seen_date
from {{ ref('stg_transactions') }}
group by card_id
