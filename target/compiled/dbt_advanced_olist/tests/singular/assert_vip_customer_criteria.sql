/*
  SINGULAR TEST: assert_vip_customer_criteria
  
  Purpose: VIP customers must meet the defined criteria
  
  Business Rule: VIP = lifetime_revenue >= 500 AND lifetime_orders >= 3
  
  Test PASSES if query returns 0 rows
*/

with vip_customers as (
    select
        customer_id,
        customer_segment,
        lifetime_revenue,
        lifetime_orders
    from `big-query-dbt-481111`.`dbt_dev_yourname_marts`.`dim_customers`
    where customer_segment = 'VIP'
),

invalid_vips as (
    select
        customer_id,
        lifetime_revenue,
        lifetime_orders,
        'VIP but does not meet criteria' as violation
    from vip_customers
    where lifetime_revenue < 500
       or lifetime_orders < 3
),

should_be_vips as (
    select
        customer_id,
        customer_segment,
        lifetime_revenue,
        lifetime_orders,
        'Should be VIP but is not' as violation
    from `big-query-dbt-481111`.`dbt_dev_yourname_marts`.`dim_customers`
    where lifetime_revenue >= 500
      and lifetime_orders >= 3
      and customer_segment != 'VIP'
)

select * from invalid_vips
union all
select customer_id, lifetime_revenue, lifetime_orders, violation from should_be_vips