{{ config(location=silver_location()) }}

with src as (
    select * from {{ source('bronze', 'products') }}
),

ranked as (
    select
        cast(product_id as varchar) as product_id,
        cast(name as varchar) as name,
        lower(cast(category as varchar)) as category,
        cast(list_price as double) as list_price,
        cast(ingest_ts as timestamp) as ingest_ts,
        row_number() over (
            partition by product_id
            order by ingest_ts desc
        ) as rn
    from src
)

select product_id, name, category, list_price, ingest_ts
from ranked
where rn = 1
