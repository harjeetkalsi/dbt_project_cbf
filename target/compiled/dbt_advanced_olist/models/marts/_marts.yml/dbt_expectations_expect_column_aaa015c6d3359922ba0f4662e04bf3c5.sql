






    with grouped_expression as (
    select
        
        
    
  
( 1=1 and late_delivery_rate >= 0 and late_delivery_rate <= 100
)
 as expression


    from `big-query-dbt-481111`.`dbt_dev_yourname_marts`.`dim_customers`
    

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







