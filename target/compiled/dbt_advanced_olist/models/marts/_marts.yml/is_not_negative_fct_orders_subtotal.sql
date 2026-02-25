

select
    subtotal as failing_value,
    count(*) as row_count
from `arcane-pillar-485809-b6`.`raw_olist_marts`.`fct_orders`
where subtotal < 0
group by 1

