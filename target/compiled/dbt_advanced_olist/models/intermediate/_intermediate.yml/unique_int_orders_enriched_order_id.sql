
    
    

with dbt_test__target as (

  select order_id as unique_field
  from `arcane-pillar-485809-b6`.`raw_olist_intermediate`.`int_orders_enriched`
  where order_id is not null

)

select
    unique_field,
    count(*) as n_records

from dbt_test__target
group by unique_field
having count(*) > 1


