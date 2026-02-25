

select
    freight_total as failing_value,
    count(*) as row_count
from `arcane-pillar-485809-b6`.`raw_olist_marts`.`fct_orders`
where freight_total < 0
group by 1

