{{ config(location=gold_location()) }}

-- Daily revenue × product category — the canonical "exec dashboard" mart.
-- Pre-aggregated so BI queries are cheap regardless of how many orders exist.

with orders as (
    select * from {{ ref('silver_orders') }}
    where status in ('paid', 'shipped')
),

products as (
    select * from {{ ref('silver_products') }}
),

-- We don't have order line items in this toy dataset, so attribute order
-- amount to product via the most recent web_event with action='purchase'
-- for the same customer within 24h before the order.
purchase_attribution as (
    select
        o.order_id,
        o.customer_id,
        o.amount,
        date_trunc('day', o.event_ts) as order_day,
        (
            select product_id
            from {{ ref('silver_web_events') }} we
            where we.customer_id = o.customer_id
              and we.action = 'purchase'
              and we.event_ts <= o.event_ts
              and we.event_ts >= o.event_ts - interval 1 day
            order by we.event_ts desc
            limit 1
        ) as product_id
    from orders o
)

select
    pa.order_day,
    coalesce(p.category, 'unknown') as category,
    count(distinct pa.order_id) as order_count,
    count(distinct pa.customer_id) as customer_count,
    sum(pa.amount) as revenue
from purchase_attribution pa
left join products p using (product_id)
group by pa.order_day, coalesce(p.category, 'unknown')
order by pa.order_day, category
