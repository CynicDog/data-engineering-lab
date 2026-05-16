{{ config(location=gold_location()) }}

-- Funnel + conversion per product. Joins web_events to itself, which is why
-- Silver pre-types and pre-sessionizes — Gold stays narrow and readable.

with events as (
    select * from {{ ref('silver_web_events') }}
),

products as (
    select * from {{ ref('silver_products') }}
),

per_product as (
    select
        product_id,
        count(*) filter (where action = 'view') as views,
        count(*) filter (where action = 'click') as clicks,
        count(*) filter (where action = 'add_to_cart') as add_to_carts,
        count(*) filter (where action = 'purchase') as purchases,
        count(distinct session_id) as sessions,
        count(distinct customer_id) as unique_visitors
    from events
    where product_id is not null
    group by product_id
)

select
    p.product_id,
    p.name as product_name,
    p.category,
    p.list_price,
    pp.views,
    pp.clicks,
    pp.add_to_carts,
    pp.purchases,
    pp.sessions,
    pp.unique_visitors,
    case when pp.views > 0
         then pp.purchases::double / pp.views
         else null
    end as view_to_purchase_rate
from products p
left join per_product pp using (product_id)
