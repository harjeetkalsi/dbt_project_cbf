

select
    order_total as failing_value,
    count(*) as row_count
from `big-query-dbt-481111`.`dbt_dev_yourname_intermediate`.`int_orders_enriched`
where order_total < 0
group by 1

