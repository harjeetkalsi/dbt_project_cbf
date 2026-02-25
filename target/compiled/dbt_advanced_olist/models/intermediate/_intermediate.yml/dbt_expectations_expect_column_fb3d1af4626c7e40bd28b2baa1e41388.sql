






    with grouped_expression as (
    select
        
        
    
  
( 1=1 and listing_quality_score >= 0 and listing_quality_score <= 100
)
 as expression


    from `arcane-pillar-485809-b6`.`raw_olist_intermediate`.`int_products_enriched`
    

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







