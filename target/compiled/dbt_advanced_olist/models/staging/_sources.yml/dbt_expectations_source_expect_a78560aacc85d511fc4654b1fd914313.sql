






    with grouped_expression as (
    select
        
        
    
  
( 1=1 and product_weight_g >= 0 and product_weight_g <= 100000
)
 as expression


    from `big-query-dbt-481111`.`raw_olist`.`products`
    

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







