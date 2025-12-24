






    with grouped_expression as (
    select
        
        
    
  
( 1=1 and delivery_days >= 0 and delivery_days <= 180
)
 as expression


    from `big-query-dbt-481111`.`dbt_dev_yourname_staging`.`stg_orders`
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







