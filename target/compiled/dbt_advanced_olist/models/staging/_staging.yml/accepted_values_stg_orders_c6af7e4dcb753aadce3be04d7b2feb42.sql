
    
    

with all_values as (

    select
        order_status as value_field,
        count(*) as n_records

    from `arcane-pillar-485809-b6`.`raw_olist_staging`.`stg_orders`
    group by order_status

)

select *
from all_values
where value_field not in (
    'delivered','shipped','processing','canceled','invoiced','unavailable','created','approved'
)


