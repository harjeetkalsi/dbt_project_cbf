

select
    item_total as failing_value,
    count(*) as row_count
from `arcane-pillar-485809-b6`.`raw_olist_staging`.`stg_order_items`
where item_total <= 0
   or item_total is null
group by 1

