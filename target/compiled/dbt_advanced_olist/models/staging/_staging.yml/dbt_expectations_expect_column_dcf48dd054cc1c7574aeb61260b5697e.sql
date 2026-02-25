




    with grouped_expression as (
    select
        
        
    
  

    length(
        customer_state
    ) = 2 as expression


    from `arcane-pillar-485809-b6`.`raw_olist_staging`.`stg_customers`
    

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




