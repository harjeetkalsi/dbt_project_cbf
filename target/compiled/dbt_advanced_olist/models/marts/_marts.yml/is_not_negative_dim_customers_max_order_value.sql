

select
    max_order_value as failing_value,
    count(*) as row_count
from `arcane-pillar-485809-b6`.`raw_olist_marts`.`dim_customers`
where max_order_value < 0
group by 1

