

/*
  SINGULAR TEST: assert_orders_have_items
  Purpose: Every order should have at least one line item
  Business Rule: An order without items indicates data integrity issue
  Test PASSES if this query returns 0 rows
  Test FAILS if any orders exist without items
  
  Note: Sample data may have orphaned records - set to warn
*/
with orders as (
    select order_id from `big-query-dbt-481111`.`dbt_dev_yourname_staging`.`stg_orders`
),
order_items as (
    select distinct order_id from `big-query-dbt-481111`.`dbt_dev_yourname_staging`.`stg_order_items`
),
orders_without_items as (
    select
        o.order_id
    from orders o
    left join order_items oi on o.order_id = oi.order_id
    where oi.order_id is null
)
select
    order_id,
    'Order has no items' as failure_reason
from orders_without_items