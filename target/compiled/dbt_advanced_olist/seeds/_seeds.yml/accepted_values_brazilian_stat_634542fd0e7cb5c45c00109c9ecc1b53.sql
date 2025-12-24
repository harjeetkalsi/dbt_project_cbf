
    
    

with all_values as (

    select
        region as value_field,
        count(*) as n_records

    from `big-query-dbt-481111`.`dbt_dev_yourname_seeds`.`brazilian_states`
    group by region

)

select *
from all_values
where value_field not in (
    'North','Northeast','Central-West','Southeast','South'
)


