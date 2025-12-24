

/*
  STAGING MODEL: stg_orders
  
  Purpose: Clean and standardize raw order data
  Source: Olist Brazilian E-commerce (10K orders sample)
  
  Transformations:
  - Rename columns to consistent naming convention
  - Cast timestamps to proper types
  - Calculate delivery metrics
  - Add late delivery flag
*/

with source as (
    select * from `big-query-dbt-481111`.`raw_olist`.`orders`
),

renamed as (
    select
        -- Primary Key
        order_id,
        
        -- Foreign Keys
        customer_id,
        
        -- Order Status
        order_status,
        
        -- Timestamps (cast to proper types)
        cast(order_purchase_timestamp as timestamp) as ordered_at,
        cast(order_approved_at as timestamp) as approved_at,
        cast(order_delivered_carrier_date as timestamp) as shipped_at,
        cast(order_delivered_customer_date as timestamp) as delivered_at,
        cast(order_estimated_delivery_date as timestamp) as estimated_delivery_at,
        
        -- Derived: Order date (for partitioning)
        date(cast(order_purchase_timestamp as timestamp)) as order_date,
        
        -- Derived: Delivery time in days
        case 
            when order_delivered_customer_date is not null 
            then date_diff(
                date(cast(order_delivered_customer_date as timestamp)),
                date(cast(order_purchase_timestamp as timestamp)),
                day
            )
        end as delivery_days,
        
        -- Derived: Is delivery late?
        case
            when order_delivered_customer_date is not null 
                and cast(order_delivered_customer_date as timestamp) > cast(order_estimated_delivery_date as timestamp)
            then true
            else false
        end as is_late_delivery,
        
        -- Derived: Days late (if late)
        case
            when order_delivered_customer_date is not null 
                and cast(order_delivered_customer_date as timestamp) > cast(order_estimated_delivery_date as timestamp)
            then date_diff(
                date(cast(order_delivered_customer_date as timestamp)),
                date(cast(order_estimated_delivery_date as timestamp)),
                day
            )
            else 0
        end as days_late

    from source
)

select * from renamed

-- Limit in dev for faster iteration
limit 1000