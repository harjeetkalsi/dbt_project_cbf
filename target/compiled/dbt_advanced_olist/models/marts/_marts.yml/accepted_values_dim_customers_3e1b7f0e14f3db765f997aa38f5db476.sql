
    
    

with all_values as (

    select
        customer_segment as value_field,
        count(*) as n_records

    from `arcane-pillar-485809-b6`.`raw_olist_marts`.`dim_customers`
    group by customer_segment

)

select *
from all_values
where value_field not in (
    'VIP','High Value','Medium Value','Low Value','No Orders'
)


