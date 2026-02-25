
    
    

with all_values as (

    select
        order_size_bucket as value_field,
        count(*) as n_records

    from `arcane-pillar-485809-b6`.`raw_olist_marts`.`fct_orders`
    group by order_size_bucket

)

select *
from all_values
where value_field not in (
    'Small','Medium','Large','Premium'
)


