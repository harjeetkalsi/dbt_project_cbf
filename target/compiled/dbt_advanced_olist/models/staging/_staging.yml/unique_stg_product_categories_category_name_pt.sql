
    
    

with dbt_test__target as (

  select category_name_pt as unique_field
  from `big-query-dbt-481111`.`dbt_dev_yourname_staging`.`stg_product_categories`
  where category_name_pt is not null

)

select
    unique_field,
    count(*) as n_records

from dbt_test__target
group by unique_field
having count(*) > 1


