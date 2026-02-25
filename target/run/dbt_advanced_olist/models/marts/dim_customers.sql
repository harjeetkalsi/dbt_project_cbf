
  
    

    create or replace table `arcane-pillar-485809-b6`.`raw_olist_marts`.`dim_customers`
        
  (
    customer_key string,
    customer_id string not null,
    customer_unique_id string,
    customer_city string,
    customer_state string,
    customer_zip_code string,
    customer_region string,
    lifetime_orders int64,
    lifetime_revenue numeric,
    avg_order_value numeric,
    max_order_value numeric,
    lifetime_items int64,
    first_order_date date,
    last_order_date date,
    customer_lifespan_days int64,
    avg_delivery_days float64,
    late_deliveries int64,
    customer_segment string,
    purchase_frequency string,
    late_delivery_rate float64,
    _loaded_at timestamp
    
    )

      
    partition by date_trunc(first_order_date, month)
    cluster by customer_state, customer_segment

    
    OPTIONS()
    as (
      
    select customer_key, customer_id, customer_unique_id, customer_city, customer_state, customer_zip_code, customer_region, lifetime_orders, lifetime_revenue, avg_order_value, max_order_value, lifetime_items, first_order_date, last_order_date, customer_lifespan_days, avg_delivery_days, late_deliveries, customer_segment, purchase_frequency, late_delivery_rate, _loaded_at
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

with orders as (
    select * from `arcane-pillar-485809-b6`.`raw_olist_intermediate`.`int_orders_enriched`
    where order_status = 'delivered'  -- Only count completed orders
),

customers as (
    select * from `arcane-pillar-485809-b6`.`raw_olist_staging`.`stg_customers`
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
            when coalesce(m.lifetime_revenue, 0) >= 500 and coalesce(m.lifetime_orders, 0) >= 3 then 'VIP'
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
    ) as model_subq
    );
  