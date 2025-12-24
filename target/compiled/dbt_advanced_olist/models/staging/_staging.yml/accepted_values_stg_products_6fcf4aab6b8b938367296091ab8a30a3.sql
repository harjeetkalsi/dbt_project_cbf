
    
    

with all_values as (

    select
        size_category as value_field,
        count(*) as n_records

    from (select * from `big-query-dbt-481111`.`dbt_dev_yourname_staging`.`stg_products` where size_category is not null) dbt_subquery
    group by size_category

)

select *
from all_values
where value_field not in (
    'Small','Medium','Large','Extra Large'
)


