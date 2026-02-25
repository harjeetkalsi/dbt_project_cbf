

select
    item_price as failing_value,
    count(*) as row_count
from `arcane-pillar-485809-b6`.`raw_olist_staging`.`stg_order_items`
where item_price <= 0
   or item_price is null
group by 1

