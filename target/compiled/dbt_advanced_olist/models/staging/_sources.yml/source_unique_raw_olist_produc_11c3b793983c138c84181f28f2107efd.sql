
    
    

with dbt_test__target as (

  select string_field_0 as unique_field
  from `big-query-dbt-481111`.`raw_olist`.`product_category_name_translation`
  where string_field_0 is not null

)

select
    unique_field,
    count(*) as n_records

from dbt_test__target
group by unique_field
having count(*) > 1


