/*
  SINGULAR TEST: assert_order_total_matches_items
  
  Purpose: Order total should equal sum of item totals (within tolerance)
  
  Business Rule: Order total = sum(item_price + item_freight) for all items
  
  Tolerance: 1 BRL to account for rounding
  
  Test PASSES if query returns 0 rows
*/

with order_totals_from_items as (
    select
        order_id,
        sum(item_total) as calculated_total
    from {{ ref('stg_order_items') }}
    group by 1
),

order_totals_from_orders as (
    select
        order_id,
        order_total
    from {{ ref('int_orders_enriched') }}
),

mismatches as (
    select
        o.order_id,
        o.order_total as reported_total,
        i.calculated_total,
        abs(o.order_total - i.calculated_total) as difference
    from order_totals_from_orders o
    inner join order_totals_from_items i on o.order_id = i.order_id
    where abs(o.order_total - i.calculated_total) > 1  -- 1 BRL tolerance
)

select
    order_id,
    reported_total,
    calculated_total,
    round(difference, 2) as difference_brl,
    'Order total does not match sum of items' as failure_reason
from mismatches
