






    with grouped_expression as (
    select
        
        
    
  
( 1=1 and delivery_days >= 0 and delivery_days <= 180
)
 as expression


    from `arcane-pillar-485809-b6`.`raw_olist_staging`.`stg_orders`
    where
        delivery_days is not null
    
    

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







