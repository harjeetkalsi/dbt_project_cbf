
    
    

with all_values as (

    select
        customer_region as value_field,
        count(*) as n_records

    from `big-query-dbt-481111`.`dbt_dev_yourname_staging`.`stg_customers`
    group by customer_region

)

select *
from all_values
where value_field not in (
    'Southeast','South','Northeast','Central-West','North'
)


