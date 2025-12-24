
    
    

with dbt_test__target as (

  select status_code as unique_field
  from `big-query-dbt-481111`.`dbt_dev_yourname_seeds`.`order_status_codes`
  where status_code is not null

)

select
    unique_field,
    count(*) as n_records

from dbt_test__target
group by unique_field
having count(*) > 1


