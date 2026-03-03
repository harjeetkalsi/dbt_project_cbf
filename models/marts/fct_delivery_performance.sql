{{
  config(
    materialized='table',
    tags=['marts', 'facts', 'operations'],
    partition_by={
      'field': 'order_date',
      'data_type': 'date',
      'granularity': 'month'
    },
    cluster_by=['customer_region', 'is_late_delivery', 'order_size_bucket'],
    on_schema_change='append_new_columns'
  )
}}

-- Purpose: One row per order to analyse delivery performance by region, month, and order size bucket.
-- Grain: 1 row = 1 order_id
--
-- Why partition_by order_date (month):
-- Most reporting is time-based (monthly trends). Partitioning reduces scanned data when filtering by date ranges.
--
-- Why cluster_by customer_region, is_late_delivery, order_size_bucket:
-- Common filters for Ops dashboards: region, late vs on-time, and bucket. Clustering speeds these filters up.

with orders as (
    select
        order_id,
        customer_id,
        order_status,
        order_date,
        customer_region,
        order_size_bucket,      -- already exists in int_orders_enriched
        delivery_days,
        is_late_delivery,
        days_late
    from {{ ref('int_orders_enriched') }}
),

final as (
    select
        -- Keys
        order_id,
        customer_id,

        -- Dimensions for analysis
        order_date,
        customer_region,
        order_size_bucket,
        order_status,

        -- Delivery metrics
        delivery_days,
        is_late_delivery,
        days_late,

        -- Derived metrics
        case when is_late_delivery then 1 else 0 end as late_delivery_flag,

        case
            when order_status = 'canceled' then 'Canceled'
            when order_status != 'delivered' then 'In Progress'
            when order_status = 'delivered' and is_late_delivery then 'Late'
            when order_status = 'delivered' and not is_late_delivery then 'On Time'
            else 'Unknown'
        end as delivery_category,

        current_timestamp() as _loaded_at
    from orders
)

select * from final