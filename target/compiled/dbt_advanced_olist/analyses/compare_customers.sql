-- ============================================================
-- ANALYSIS: Compare Model Versions with Audit Helper
-- ============================================================
-- 
-- Use this analysis to compare two versions of a model.
-- Great for validating refactoring or debugging issues.
--
-- Run: dbt compile --select compare_customers
-- Then copy the compiled SQL to BigQuery
-- ============================================================



-- Compare current vs itself (in real use, compare to old version)












with a as (

    
select

  

   
    `customer_key` 
    
      , 
     
   
    `customer_id` 
    
      , 
     
   
    `customer_unique_id` 
    
      , 
     
   
    `customer_city` 
    
      , 
     
   
    `customer_state` 
    
      , 
     
   
    `customer_zip_code` 
    
      , 
     
   
    `customer_region` 
    
      , 
     
   
    `lifetime_orders` 
    
      , 
     
   
    `lifetime_revenue` 
    
      , 
     
   
    `avg_order_value` 
    
      , 
     
   
    `max_order_value` 
    
      , 
     
   
    `lifetime_items` 
    
      , 
     
   
    `first_order_date` 
    
      , 
     
   
    `last_order_date` 
    
      , 
     
   
    `customer_lifespan_days` 
    
      , 
     
   
    `avg_delivery_days` 
    
      , 
     
   
    `late_deliveries` 
    
      , 
     
   
    `customer_segment` 
    
      , 
     
   
    `purchase_frequency` 
    
      , 
     
   
    `late_delivery_rate` 
     
  



from `arcane-pillar-485809-b6`.`raw_olist_marts`.`dim_customers`


),

b as (

    
select

  

   
    `customer_key` 
    
      , 
     
   
    `customer_id` 
    
      , 
     
   
    `customer_unique_id` 
    
      , 
     
   
    `customer_city` 
    
      , 
     
   
    `customer_state` 
    
      , 
     
   
    `customer_zip_code` 
    
      , 
     
   
    `customer_region` 
    
      , 
     
   
    `lifetime_orders` 
    
      , 
     
   
    `lifetime_revenue` 
    
      , 
     
   
    `avg_order_value` 
    
      , 
     
   
    `max_order_value` 
    
      , 
     
   
    `lifetime_items` 
    
      , 
     
   
    `first_order_date` 
    
      , 
     
   
    `last_order_date` 
    
      , 
     
   
    `customer_lifespan_days` 
    
      , 
     
   
    `avg_delivery_days` 
    
      , 
     
   
    `late_deliveries` 
    
      , 
     
   
    `customer_segment` 
    
      , 
     
   
    `purchase_frequency` 
    
      , 
     
   
    `late_delivery_rate` 
     
  



from `arcane-pillar-485809-b6`.`raw_olist_marts`.`dim_customers`


),

a_intersect_b as (

    select * from a
    

    intersect distinct


    select * from b

),

a_except_b as (

    select * from a
    

    except distinct


    select * from b

),

b_except_a as (

    select * from b
    

    except distinct


    select * from a

),

all_records as (

    select
        *,
        true as in_a,
        true as in_b
    from a_intersect_b

    union all

    select
        *,
        true as in_a,
        false as in_b
    from a_except_b

    union all

    select
        *,
        false as in_a,
        true as in_b
    from b_except_a

),

summary_stats as (

    select

        in_a,
        in_b,
        count(*) as count

    from all_records
    group by 1, 2

),

final as (

    select

        *,
        round(100.0 * count / sum(count) over (), 2) as percent_of_total

    from summary_stats
    order by in_a desc, in_b desc

)

select * from final




```

-- ============================================================
-- INTERPRETATION:
-- 
-- in_a | in_b | count | meaning
-- -----|------|-------|--------
-- true | true | 9000  | Matching rows (good!)
-- true | false| 100   | Rows only in A (deleted in B?)
-- false| true | 50    | Rows only in B (new in B?)
--
-- ============================================================