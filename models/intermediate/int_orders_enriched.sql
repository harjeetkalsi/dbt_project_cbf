{{ config(
    materialized='table',
    tags=['intermediate', 'orders'],
    partition_by={
      'field': 'order_date',
      'data_type': 'date',
      'granularity': 'month'
    },
    cluster_by=['customer_id', 'order_status']
) }}

/*
  INTERMEDIATE MODEL: int_orders_enriched
  
  Purpose: Enrich orders with aggregated item data and customer info
  
  Joins:
  - stg_orders (base)
  - stg_order_items (aggregated)
  - stg_customers (customer details)
*/

with orders as (
    select * from {{ ref('stg_orders') }}
),

order_items as (
    select * from {{ ref('stg_order_items') }}
),

customers as (
    select * from {{ ref('stg_customers') }}
),

order_totals as (
    select
        order_id,
        count(*) as item_count,
        count(distinct product_id) as unique_products,
        sum(item_price) as subtotal,
        sum(item_freight) as freight_total,
        sum(item_total) as order_total,
        avg(item_price) as avg_item_price,
        max(item_price) as max_item_price
    from order_items
    group by 1
),

enriched as (
    select
        o.order_id,
        o.order_status,
        o.ordered_at,
        o.order_date,
        o.approved_at,
        o.shipped_at,
        o.delivered_at,
        o.estimated_delivery_at,
        o.delivery_days,
        o.is_late_delivery,
        o.days_late,

        o.customer_id,
        c.customer_unique_id,
        c.customer_city,
        c.customer_state,
        c.customer_region,

        coalesce(ot.item_count, 0) as item_count,
        coalesce(ot.unique_products, 0) as unique_products,
        coalesce(ot.subtotal, 0) as subtotal,
        coalesce(ot.freight_total, 0) as freight_total,
        coalesce(ot.order_total, 0) as order_total,
        coalesce(ot.avg_item_price, 0) as avg_item_price,
        coalesce(ot.max_item_price, 0) as max_item_price,

        case
            when coalesce(ot.order_total, 0) < 50 then 'Small'
            when coalesce(ot.order_total, 0) < 200 then 'Medium'
            when coalesce(ot.order_total, 0) < 500 then 'Large'
            else 'Premium'
        end as order_size_bucket,

        {{ generate_date_parts('o.order_date') }}

    from orders o
    left join customers c on o.customer_id = c.customer_id
    left join order_totals ot on o.order_id = ot.order_id
)

select * from enriched