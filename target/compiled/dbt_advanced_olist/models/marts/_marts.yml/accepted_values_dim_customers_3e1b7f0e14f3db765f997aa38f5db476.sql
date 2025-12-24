
    
    

with all_values as (

    select
        customer_segment as value_field,
        count(*) as n_records

    from `big-query-dbt-481111`.`dbt_dev_yourname_marts`.`dim_customers`
    group by customer_segment

)

select *
from all_values
where value_field not in (
    'VIP','High Value','Medium Value','Low Value','No Orders'
)


