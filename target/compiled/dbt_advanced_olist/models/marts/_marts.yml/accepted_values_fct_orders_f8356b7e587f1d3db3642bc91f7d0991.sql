
    
    

with all_values as (

    select
        order_size_bucket as value_field,
        count(*) as n_records

    from `big-query-dbt-481111`.`dbt_dev_yourname_marts`.`fct_orders`
    group by order_size_bucket

)

select *
from all_values
where value_field not in (
    'Small','Medium','Large','Premium'
)


