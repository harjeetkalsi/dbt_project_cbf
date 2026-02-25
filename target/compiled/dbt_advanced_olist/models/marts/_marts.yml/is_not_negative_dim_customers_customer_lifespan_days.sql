

select
    customer_lifespan_days as failing_value,
    count(*) as row_count
from `arcane-pillar-485809-b6`.`raw_olist_marts`.`dim_customers`
where customer_lifespan_days < 0
group by 1

