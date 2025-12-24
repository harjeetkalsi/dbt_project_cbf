
    
    

with dbt_test__target as (

  select state_code as unique_field
  from `big-query-dbt-481111`.`dbt_dev_yourname_seeds`.`brazilian_states`
  where state_code is not null

)

select
    unique_field,
    count(*) as n_records

from dbt_test__target
group by unique_field
having count(*) > 1


