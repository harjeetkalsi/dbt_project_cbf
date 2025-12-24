
  
    

    create or replace table `big-query-dbt-481111`.`dbt_dev_yourname_marts`.`fct_orders`
        
  (
    order_id string not null primary key not enforced,
    customer_id string,
    order_status string,
    order_date date,
    ordered_at timestamp,
    approved_at timestamp,
    shipped_at timestamp,
    delivered_at timestamp,
    estimated_delivery_at timestamp,
    order_year int64,
    order_month int64,
    order_quarter int64,
    order_week int64,
    order_day_of_week int64,
    is_weekend boolean,
    customer_city string,
    customer_state string,
    customer_region string,
    primary_category string,
    primary_category_group string,
    item_count int64,
    unique_products int64,
    subtotal numeric,
    freight_total numeric,
    order_total numeric,
    avg_item_price numeric,
    delivery_days int64,
    is_late_delivery boolean,
    days_late int64,
    order_size_bucket string,
    delivery_sla_status string,
    _loaded_at timestamp
    
    )

      
    partition by date_trunc(order_date, month)
    cluster by order_status, customer_state

    
    OPTIONS()
    as (
      
    select order_id, customer_id, order_status, order_date, ordered_at, approved_at, shipped_at, delivered_at, estimated_delivery_at, order_year, order_month, order_quarter, order_week, order_day_of_week, is_weekend, customer_city, customer_state, customer_region, primary_category, primary_category_group, item_count, unique_products, subtotal, freight_total, order_total, avg_item_price, delivery_days, is_late_delivery, days_late, order_size_bucket, delivery_sla_status, _loaded_at
    from (
        

/*
  MART: fct_orders
  
  Purpose: Order fact table for analytics and BI
  
  Features:
  - Incremental materialization for large datasets
  - Partitioned by order_date for BigQuery efficiency
  - Clustered for common filter patterns
  
  Grain: One row per order
*/

with orders as (
    select * from `big-query-dbt-481111`.`dbt_dev_yourname_intermediate`.`int_orders_enriched`
),

products as (
    select * from `big-query-dbt-481111`.`dbt_dev_yourname_intermediate`.`int_products_enriched`
),

order_items as (
    select * from `big-query-dbt-481111`.`dbt_dev_yourname_staging`.`stg_order_items`
),

-- Get primary category per order (highest value item)
order_categories as (
    select
        oi.order_id,
        p.category_name,
        p.category_group,
        row_number() over (
            partition by oi.order_id 
            order by oi.item_total desc
        ) as rank
    from order_items oi
    left join products p on oi.product_id = p.product_id
),

primary_category as (
    select
        order_id,
        category_name as primary_category,
        category_group as primary_category_group
    from order_categories
    where rank = 1
),

-- Build fact table
final as (
    select
        -- Keys
        o.order_id,
        o.customer_id,
        
        -- Order Details
        o.order_status,
        o.order_date,
        o.ordered_at,
        o.approved_at,
        o.shipped_at,
        o.delivered_at,
        o.estimated_delivery_at,
        
        -- Time dimensions
        o.order_year,
        o.order_month,
        o.order_quarter,
        o.order_week,
        o.order_day_of_week,
        o.is_weekend,
        
        -- Customer Location
        o.customer_city,
        o.customer_state,
        o.customer_region,
        
        -- Product Info
        coalesce(pc.primary_category, 'Unknown') as primary_category,
        coalesce(pc.primary_category_group, 'Other') as primary_category_group,
        
        -- Measures
        o.item_count,
        o.unique_products,
        o.subtotal,
        o.freight_total,
        o.order_total,
        o.avg_item_price,
        
        -- Delivery Metrics
        o.delivery_days,
        o.is_late_delivery,
        o.days_late,
        
        -- Derived Classifications
        o.order_size_bucket,
        
        -- SLA Compliance
        case
            when o.order_status = 'delivered' and not o.is_late_delivery then 'Met'
            when o.order_status = 'delivered' and o.is_late_delivery then 'Missed'
            when o.order_status = 'canceled' then 'Canceled'
            else 'In Progress'
        end as delivery_sla_status,
        
        -- Metadata
        current_timestamp() as _loaded_at

    from orders o
    left join primary_category pc on o.order_id = pc.order_id
)

select * from final
    ) as model_subq
    );
  