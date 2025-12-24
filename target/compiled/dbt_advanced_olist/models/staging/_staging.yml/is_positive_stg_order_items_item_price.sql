

select
    item_price as failing_value,
    count(*) as row_count
from `big-query-dbt-481111`.`dbt_dev_yourname_staging`.`stg_order_items`
where item_price <= 0
   or item_price is null
group by 1

