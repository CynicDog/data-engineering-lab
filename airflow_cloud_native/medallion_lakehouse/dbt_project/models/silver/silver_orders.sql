{{ config(location=silver_location()) }}

-- Silver orders: cleaned, deduped, typed. One row per order_id.
--
-- - Status values normalized to a closed set (Silver-level contract).
-- - Late-arriving retries (same order_id, later event_ts) keep the latest row.
-- - Negative amounts dropped — they're refunds and belong in a separate fact.

with src as (
    select * from {{ source('bronze', 'orders') }}
    where amount >= 0
),

normalized as (
    select
        cast(order_id as varchar) as order_id,
        cast(customer_id as varchar) as customer_id,
        case lower(trim(status))
            when 'new' then 'new'
            when 'created' then 'new'
            when 'paid' then 'paid'
            when 'shipped' then 'shipped'
            when 'cancelled' then 'cancelled'
            when 'canceled' then 'cancelled'
            when 'refunded' then 'refunded'
            else 'unknown'
        end as status,
        cast(amount as double) as amount,
        cast(event_ts as timestamp) as event_ts,
        cast(ingest_ts as timestamp) as ingest_ts,
        row_number() over (
            partition by order_id
            order by event_ts desc, ingest_ts desc
        ) as rn
    from src
)

select
    order_id,
    customer_id,
    status,
    amount,
    event_ts,
    ingest_ts
from normalized
where rn = 1
