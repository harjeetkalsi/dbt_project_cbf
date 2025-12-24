
    
    

with all_values as (

    select
        purchase_frequency as value_field,
        count(*) as n_records

    from `big-query-dbt-481111`.`dbt_dev_yourname_marts`.`dim_customers`
    group by purchase_frequency

)

select *
from all_values
where value_field not in (
    'Frequent','Repeat','One-Time','Never Purchased'
)


