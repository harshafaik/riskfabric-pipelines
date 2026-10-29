-- Regression guard: ordering bugs once produced negative intervals when the
-- sort happened after a join. A non-null interval must never be negative.
select
    transaction_id,
    card_id,
    time_since_last_transaction_seconds
from {{ ref('fact_transaction') }}
where time_since_last_transaction_seconds < 0
