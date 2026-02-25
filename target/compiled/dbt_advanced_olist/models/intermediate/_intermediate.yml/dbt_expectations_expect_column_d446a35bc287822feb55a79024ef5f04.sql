






    with grouped_expression as (
    select
        
        
    
  
( 1=1 and item_count >= 0 and item_count <= 100
)
 as expression


    from `arcane-pillar-485809-b6`.`raw_olist_intermediate`.`int_orders_enriched`
    

),
validation_errors as (

    select
        *
    from
        grouped_expression
    where
        not(expression = true)

)

select *
from validation_errors







