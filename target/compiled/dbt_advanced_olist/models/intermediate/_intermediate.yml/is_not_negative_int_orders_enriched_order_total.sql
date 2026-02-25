

select
    order_total as failing_value,
    count(*) as row_count
from `arcane-pillar-485809-b6`.`raw_olist_intermediate`.`int_orders_enriched`
where order_total < 0
group by 1

