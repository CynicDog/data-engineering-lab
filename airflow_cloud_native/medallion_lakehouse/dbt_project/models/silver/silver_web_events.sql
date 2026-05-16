{{ config(location=silver_location()) }}

-- Silver web_events: typed + sessionized.
--
-- A session = consecutive events for the same customer with < 30 min gap.
-- This is the kind of derivation that makes Silver expensive but cheap to
-- reuse — every Gold mart that needs sessions reads them from here.

with src as (
    select * from {{ source('bronze', 'web_events') }}
),

typed as (
    select
        cast(event_id as varchar) as event_id,
        cast(customer_id as varchar) as customer_id,
        cast(product_id as varchar) as product_id,
        lower(cast(action as varchar)) as action,
        cast(event_ts as timestamp) as event_ts,
        cast(ingest_ts as timestamp) as ingest_ts
    from src
),

with_gaps as (
    select
        *,
        coalesce(
            date_diff(
                'minute',
                lag(event_ts) over (partition by customer_id order by event_ts),
                event_ts
            ),
            0
        ) as minutes_since_prev
    from typed
),

with_sessions as (
    select
        *,
        sum(case when minutes_since_prev > 30 then 1 else 0 end)
            over (partition by customer_id order by event_ts) as session_seq
    from with_gaps
)

select
    event_id,
    customer_id,
    product_id,
    action,
    event_ts,
    customer_id || ':' || cast(session_seq as varchar) as session_id,
    ingest_ts
from with_sessions
