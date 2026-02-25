/*
  SINGULAR TEST: assert_delivery_dates_logical
  
  Purpose: Delivery dates should follow logical order
  
  Business Rules:
  - approved_at >= ordered_at
  - shipped_at >= approved_at (if exists)
  - delivered_at >= shipped_at (if exists)
  
  Test PASSES if query returns 0 rows
*/

with orders as (
    select
        order_id,
        ordered_at,
        approved_at,
        shipped_at,
        delivered_at
    from `arcane-pillar-485809-b6`.`raw_olist_staging`.`stg_orders`
),

date_violations as (
    select
        order_id,
        case
            when approved_at < ordered_at then 'Approved before ordered'
            when shipped_at < approved_at then 'Shipped before approved'
            when delivered_at < shipped_at then 'Delivered before shipped'
            when delivered_at < ordered_at then 'Delivered before ordered'
        end as violation_type,
        ordered_at,
        approved_at,
        shipped_at,
        delivered_at
    from orders
    where 
        (approved_at is not null and approved_at < ordered_at)
        or (shipped_at is not null and approved_at is not null and shipped_at < approved_at)
        or (delivered_at is not null and shipped_at is not null and delivered_at < shipped_at)
        or (delivered_at is not null and delivered_at < ordered_at)
)

select * from date_violations