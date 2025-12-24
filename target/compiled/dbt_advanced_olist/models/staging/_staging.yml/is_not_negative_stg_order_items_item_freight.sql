

select
    item_freight as failing_value,
    count(*) as row_count
from `big-query-dbt-481111`.`dbt_dev_yourname_staging`.`stg_order_items`
where item_freight < 0
group by 1

