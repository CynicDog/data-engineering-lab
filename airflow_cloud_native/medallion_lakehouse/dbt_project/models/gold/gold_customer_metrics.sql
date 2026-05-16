{{ config(location=gold_location()) }}

-- One row per customer. The shape an analyst expects to see in a BI tool.
-- All joins go through Silver — no Bronze references in Gold.

with orders as (
    select * from {{ ref('silver_orders') }}
),

customers as (
    select * from {{ ref('silver_customers') }}
),

events as (
    select * from {{ ref('silver_web_events') }}
),

per_customer_orders as (
    select
        customer_id,
        count(*) filter (where status in ('paid', 'shipped')) as completed_orders,
        sum(amount) filter (where status in ('paid', 'shipped')) as lifetime_revenue,
        max(event_ts) filter (where status in ('paid', 'shipped')) as last_order_at
    from orders
    group by customer_id
),

per_customer_events as (
    select
        customer_id,
        count(distinct session_id) as session_count,
        max(event_ts) as last_seen_at
    from events
    group by customer_id
)

select
    c.customer_id,
    c.name,
    c.country,
    c.signed_up_at,
    coalesce(o.completed_orders, 0) as completed_orders,
    coalesce(o.lifetime_revenue, 0.0) as lifetime_revenue,
    o.last_order_at,
    coalesce(e.session_count, 0) as session_count,
    e.last_seen_at,
    case
        when o.last_order_at is null then 'never_purchased'
        when o.last_order_at >= current_timestamp - interval 30 day then 'active'
        when o.last_order_at >= current_timestamp - interval 90 day then 'dormant'
        else 'churned'
    end as lifecycle_stage
from customers c
left join per_customer_orders o using (customer_id)
left join per_customer_events e using (customer_id)
