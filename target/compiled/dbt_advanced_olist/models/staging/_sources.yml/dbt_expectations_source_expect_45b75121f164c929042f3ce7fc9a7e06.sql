






    with grouped_expression as (
    select
        
        
    
  
( 1=1 and price >= 0 and price <= 10000
)
 as expression


    from `big-query-dbt-481111`.`raw_olist`.`order_items`
    

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







