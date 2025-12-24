
    
    

with child as (
    select order_id as from_field
    from `big-query-dbt-481111`.`raw_olist`.`order_items`
    where order_id is not null
),

parent as (
    select order_id as to_field
    from `big-query-dbt-481111`.`raw_olist`.`orders`
)

select
    from_field

from child
left join parent
    on child.from_field = parent.to_field

where parent.to_field is null


