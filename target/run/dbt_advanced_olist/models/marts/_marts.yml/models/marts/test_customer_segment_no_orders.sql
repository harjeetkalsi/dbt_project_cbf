-- Build actual result given inputs
with dbt_internal_unit_test_actual as (
  select
    `customer_id`,`customer_segment`, '''actual''' as `actual_or_expected`
  from (
    

/*
  MART: dim_customers
  
  Purpose: Customer dimension with lifetime metrics and segmentation
  
  Business Logic:
  - One row per customer
  - Lifetime value calculated from delivered orders only
  - Customer segments based on value and frequency
  
  Data Contract: Enforced in _marts.yml
*/

with  __dbt__cte__int_orders_enriched as (

-- Fixture for int_orders_enriched
select safe_cast(null as STRING) as `order_id`, safe_cast(null as STRING) as `order_status`, safe_cast(null as TIMESTAMP) as `ordered_at`, safe_cast(null as DATE) as `order_date`, safe_cast(null as TIMESTAMP) as `approved_at`, safe_cast(null as TIMESTAMP) as `shipped_at`, safe_cast(null as TIMESTAMP) as `delivered_at`, safe_cast(null as TIMESTAMP) as `estimated_delivery_at`, safe_cast(null as INT64) as `delivery_days`, safe_cast(null as BOOLEAN) as `is_late_delivery`, safe_cast(null as INT64) as `days_late`, safe_cast(null as STRING) as `customer_id`, safe_cast(null as STRING) as `customer_unique_id`, safe_cast(null as STRING) as `customer_city`, safe_cast(null as STRING) as `customer_state`, safe_cast(null as STRING) as `customer_region`, safe_cast(null as INT64) as `item_count`, safe_cast(null as INT64) as `unique_products`, safe_cast(null as NUMERIC) as `subtotal`, safe_cast(null as NUMERIC) as `freight_total`, safe_cast(null as NUMERIC) as `order_total`, safe_cast(null as NUMERIC) as `avg_item_price`, safe_cast(null as NUMERIC) as `max_item_price`, safe_cast(null as STRING) as `order_size_bucket`, safe_cast(null as INT64) as `order_year`, safe_cast(null as INT64) as `order_month`, safe_cast(null as INT64) as `order_quarter`, safe_cast(null as INT64) as `order_week`, safe_cast(null as INT64) as `order_day_of_week`, safe_cast(null as BOOLEAN) as `is_weekend`
    limit 0
),  __dbt__cte__stg_customers as (

-- Fixture for stg_customers
select safe_cast('''c3''' as STRING) as `customer_id`, safe_cast('''cu3''' as STRING) as `customer_unique_id`, safe_cast('''brasilia''' as STRING) as `customer_city`, safe_cast('''DF''' as STRING) as `customer_state`, safe_cast('''70000''' as STRING) as `customer_zip_code`, safe_cast('''Central-West''' as STRING) as `customer_region`
), orders as (
    select * from __dbt__cte__int_orders_enriched
    where order_status = 'delivered'  -- Only count completed orders
),

customers as (
    select * from __dbt__cte__stg_customers
),

-- Calculate customer metrics
customer_metrics as (
    select
        customer_id,
        customer_unique_id,
        
        -- Order counts
        count(distinct order_id) as lifetime_orders,
        
        -- Revenue metrics
        sum(order_total) as lifetime_revenue,
        avg(order_total) as avg_order_value,
        max(order_total) as max_order_value,
        sum(item_count) as lifetime_items,
        
        -- Time metrics
        min(order_date) as first_order_date,
        max(order_date) as last_order_date,
        date_diff(max(order_date), min(order_date), day) as customer_lifespan_days,
        
        -- Delivery metrics
        avg(delivery_days) as avg_delivery_days,
        sum(case when is_late_delivery then 1 else 0 end) as late_deliveries,
        
        -- Favorite region (most orders)
        max(customer_region) as primary_region

    from orders
    group by 1, 2
),

-- Build final dimension
final as (
    select
        -- Surrogate Key
        to_hex(md5(cast(coalesce(cast(c.customer_id as string), '_dbt_utils_surrogate_key_null_') as string))) as customer_key,
        
        -- Natural Keys
        c.customer_id,
        c.customer_unique_id,
        
        -- Location
        c.customer_city,
        c.customer_state,
        c.customer_zip_code,
        c.customer_region,
        
        -- Metrics (with defaults for customers without orders)
        coalesce(m.lifetime_orders, 0) as lifetime_orders,
        coalesce(m.lifetime_revenue, 0) as lifetime_revenue,
        coalesce(m.avg_order_value, 0) as avg_order_value,
        coalesce(m.max_order_value, 0) as max_order_value,
        coalesce(m.lifetime_items, 0) as lifetime_items,
        m.first_order_date,
        m.last_order_date,
        coalesce(m.customer_lifespan_days, 0) as customer_lifespan_days,
        coalesce(m.avg_delivery_days, 0) as avg_delivery_days,
        coalesce(m.late_deliveries, 0) as late_deliveries,
        
        -- Customer Segmentation
        case
            when coalesce(m.lifetime_revenue, 0) >= 400 and coalesce(m.lifetime_orders, 0) >= 3 then 'VIP'
            when coalesce(m.lifetime_revenue, 0) >= 300 then 'High Value'
            when coalesce(m.lifetime_revenue, 0) >= 100 then 'Medium Value'
            when coalesce(m.lifetime_revenue, 0) > 0 then 'Low Value'
            else 'No Orders'
        end as customer_segment,
        
        -- Frequency Classification
        case
            when coalesce(m.lifetime_orders, 0) >= 5 then 'Frequent'
            when coalesce(m.lifetime_orders, 0) >= 2 then 'Repeat'
            when coalesce(m.lifetime_orders, 0) = 1 then 'One-Time'
            else 'Never Purchased'
        end as purchase_frequency,
        
        -- Risk: Late delivery rate
        case
            when coalesce(m.lifetime_orders, 0) = 0 then 0
            else round(coalesce(m.late_deliveries, 0) * 100.0 / m.lifetime_orders, 2)
        end as late_delivery_rate,
        
        -- Metadata
        current_timestamp() as _loaded_at

    from customers c
    left join customer_metrics m on c.customer_id = m.customer_id
)

select * from final
  ) _dbt_internal_unit_test_actual
),
-- Build expected result
dbt_internal_unit_test_expected as (
  select
    `customer_id`, `customer_segment`, '''expected''' as `actual_or_expected`
  from (
    select safe_cast('''c3''' as STRING) as `customer_id`, safe_cast('''No Orders''' as STRING) as `customer_segment`
  ) _dbt_internal_unit_test_expected
)
-- Union actual and expected results
select * from dbt_internal_unit_test_actual
union all
select * from dbt_internal_unit_test_expected