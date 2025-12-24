

select
    freight_total as failing_value,
    count(*) as row_count
from `big-query-dbt-481111`.`dbt_dev_yourname_marts`.`fct_orders`
where freight_total < 0
group by 1

