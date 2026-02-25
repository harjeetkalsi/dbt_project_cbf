
    
    

with all_values as (

    select
        customer_region as value_field,
        count(*) as n_records

    from `arcane-pillar-485809-b6`.`raw_olist_staging`.`stg_customers`
    group by customer_region

)

select *
from all_values
where value_field not in (
    'Southeast','South','Northeast','Central-West','North'
)


