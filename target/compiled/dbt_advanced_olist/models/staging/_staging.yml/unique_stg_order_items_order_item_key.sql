
    
    

with dbt_test__target as (

  select order_item_key as unique_field
  from `arcane-pillar-485809-b6`.`raw_olist_staging`.`stg_order_items`
  where order_item_key is not null

)

select
    unique_field,
    count(*) as n_records

from dbt_test__target
group by unique_field
having count(*) > 1


