{{ config(
    materialized='incremental',
    unique_key='transaction_id',
    incremental_strategy='delete+insert'
) }}

with ordered as (
    select
        *,
        lag(transaction_ts) over (
            partition by card_id
            order by transaction_ts, transaction_id
        ) as previous_transaction_ts
    from {{ ref('stg_transactions') }}
),

enriched as (
    select
        md5(transaction_id) as transaction_sk,
        transaction_id,
        event_date,
        transaction_ts,
        customer_id,
        card_id,
        merchant_id,
        account_id,
        amount,
        currency,
        transaction_channel,
        card_present,
        status,
        auth_status,
        failure_reason,
        is_fraud,
        chargeback,
        chargeback_days,
        location_lat,
        location_long,
        h3_r7,
        previous_transaction_ts,
        case
            when previous_transaction_ts is null then null
            else date_diff('second', previous_transaction_ts, transaction_ts)
        end as time_since_last_transaction_seconds
    from ordered
)

select *
from enriched
{% if is_incremental() %}
where event_date >= (select max(event_date) from {{ this }}) - interval 3 day
{% endif %}
