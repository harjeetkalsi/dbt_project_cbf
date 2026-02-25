
    
    

with all_values as (

    select
        delivery_sla_status as value_field,
        count(*) as n_records

    from `arcane-pillar-485809-b6`.`raw_olist_marts`.`fct_orders`
    group by delivery_sla_status

)

select *
from all_values
where value_field not in (
    'Met','Missed','Canceled','In Progress'
)


