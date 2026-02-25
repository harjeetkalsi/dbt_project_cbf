

select
    lifetime_orders as failing_value,
    count(*) as row_count
from `arcane-pillar-485809-b6`.`raw_olist_marts`.`dim_customers`
where lifetime_orders < 0
group by 1

