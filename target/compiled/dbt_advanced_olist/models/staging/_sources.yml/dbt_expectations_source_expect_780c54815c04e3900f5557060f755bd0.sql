




    with grouped_expression as (
    select
        
        
    
  

    length(
        customer_state
    ) = 2 as expression


    from `big-query-dbt-481111`.`raw_olist`.`customers`
    

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




