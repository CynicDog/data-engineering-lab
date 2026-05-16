{{ config(location=silver_location()) }}

-- Silver customers: latest known state per customer_id.
--
-- The upstream CDC dump can include multiple snapshots; we keep the most
-- recent one. SCD-2 history is intentionally out of scope here — see the
-- README "where this would grow next" section.

with src as (
    select * from {{ source('bronze', 'customers') }}
),

ranked as (
    select
        cast(customer_id as varchar) as customer_id,
        cast(name as varchar) as name,
        upper(cast(country as varchar)) as country,
        cast(signed_up_at as timestamp) as signed_up_at,
        cast(ingest_ts as timestamp) as ingest_ts,
        row_number() over (
            partition by customer_id
            order by ingest_ts desc
        ) as rn
    from src
)

select customer_id, name, country, signed_up_at, ingest_ts
from ranked
where rn = 1
